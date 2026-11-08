import {
	type GenerationOf,
	type GenerationTracking,
	type IndexEntry,
	KVGuardError,
	type KVRead,
	type KVStore,
	type KVWrite,
	nextGenerationToken,
	positionsOf,
} from './store.js';

/**
 * In-memory KVStore for tests and Node usage.
 *
 * Reads see a copy-on-write snapshot taken when the transaction opens.
 * Writes buffer locally and apply atomically on commit. Single-writer:
 * concurrent write() calls queue.
 *
 * Values are copied the way IndexedDB copies them: commit stores a copy of
 * each value put, and a read returns a fresh copy of the committed value, so a
 * caller that changes an object it put or read does not change the store. A
 * write transaction's reads of its own puts return the object it put. As in
 * IndexedDB, a copy of a class instance is a plain object; a value
 * structuredClone cannot copy (a function) makes commit throw DataCloneError.
 */
export class MemoryKVStore implements KVStore, GenerationTracking {
	private map = new Map<string, unknown>();
	private writeLock: Promise<void> = Promise.resolve();
	/** Installed by trackGenerations; undefined until then. */
	private generationOf: GenerationOf | undefined;

	async read(): Promise<KVRead> {
		// Map snapshot is O(1)-ish copy-on-write semantics are simulated by
		// copying lazily: we copy the map only when a writer commits while a
		// reader is open. Simpler: copy now — client maps are small.
		return new MemoryRead(new Map(this.map));
	}

	async write(): Promise<KVWrite> {
		let unlock!: () => void;
		const prev = this.writeLock;
		this.writeLock = new Promise((resolve) => (unlock = resolve));
		await prev;
		return new MemoryWrite(this, new Map(this.map), unlock, this.generationOf);
	}

	trackGenerations(generationOf: GenerationOf): void {
		this.generationOf = generationOf;
	}

	async close(): Promise<void> {
		// nothing to release
	}

	/** @internal */
	applyCommit(next: Map<string, unknown>): void {
		this.map = next;
	}

	/** @internal The committed value of `key`, for a guard. */
	committed(key: string): unknown {
		return this.map.get(key);
	}
}

class MemoryRead implements KVRead {
	constructor(protected snapshot: Map<string, unknown>) {}

	async get(key: string): Promise<unknown | undefined> {
		return this.value(key, this.snapshot.get(key));
	}

	async getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		return keys.map((k) => this.value(k, this.snapshot.get(k)));
	}

	async has(key: string): Promise<boolean> {
		return this.snapshot.has(key);
	}

	async *scan(prefix: string): AsyncIterable<[string, unknown]> {
		const keys = [ ...this.snapshot.keys() ].filter((k) => k.startsWith(prefix)).sort();
		for (const k of keys) {
			yield [ k, this.value(k, this.snapshot.get(k)) ];
		}
	}

	async *scanRange(lo?: string, hi?: string): AsyncIterable<[string, unknown]> {
		const keys = [ ...this.snapshot.keys() ]
			.filter((k) => (lo === undefined || k >= lo) && (hi === undefined || k < hi))
			.sort();
		for (const k of keys) {
			yield [ k, this.value(k, this.snapshot.get(k)) ];
		}
	}

	/**
	 * The secondary index, emulated by walking the snapshot.
	 *
	 * There is no B-tree here to read a slice out of, so this is O(store) where
	 * IndexedDB's is O(result) — but so is `scanRange`, which filters every key
	 * for the same reason, and so is the scan an index read falls back to. The
	 * backend exists for tests and Node, where the constant is what it costs and
	 * the asymptotics of the two paths still match.
	 */
	async *indexScan(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): AsyncIterable<string> {
		for (const entry of this.indexEntriesIn(lo, hi, direction)) yield entry.key;
	}

	async *indexScanRows(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): AsyncIterable<IndexEntry> {
		yield* this.indexEntriesIn(lo, hi, direction);
	}

	private indexEntriesIn(
		lo: string | undefined,
		hi: string | undefined,
		direction: 'asc' | 'desc',
	): IndexEntry[] {
		const entries = this.indexEntries(lo, hi);
		return direction === 'desc' ? entries.reverse() : entries;
	}

	async indexSeek(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): Promise<{ position: string; key: string } | undefined> {
		const entries = this.indexEntries(lo, hi);
		const at = direction === 'desc' ? entries[entries.length - 1] : entries[0];
		return at === undefined ? undefined : { position: at.position, key: at.key };
	}

	/** The entries of a range, in the order an IDB index over the same records
	 * yields them. */
	private indexEntries(lo?: string, hi?: string): IndexEntry[] {
		const entries: IndexEntry[] = [];
		for (const [ k, v ] of this.snapshot) {
			for (const position of positionsOf(v)) {
				if ((lo === undefined || position >= lo) && (hi === undefined || position < hi)) {
					entries.push({ position, key: k, value: this.value(k, v) });
				}
			}
		}
		// Position order, ties broken on the record key — what an IDB index over
		// the same records yields, so a caller cannot tell the backends apart.
		entries.sort((a, b) =>
			a.position !== b.position
				? (a.position < b.position ? -1 : 1)
				: a.key !== b.key
				? (a.key < b.key ? -1 : 1)
				: 0
		);
		return entries;
	}

	async indexCount(lo?: string, hi?: string): Promise<number> {
		let n = 0;
		for (const v of this.snapshot.values()) {
			for (const position of positionsOf(v)) {
				if ((lo === undefined || position >= lo) && (hi === undefined || position < hi)) n++;
			}
		}
		return n;
	}

	async seek(lo?: string, hi?: string): Promise<string | undefined> {
		let least: string | undefined;
		for (const k of this.snapshot.keys()) {
			if (lo !== undefined && k < lo) continue;
			if (hi !== undefined && k >= hi) continue;
			if (least === undefined || k < least) least = k;
		}
		return least;
	}

	release(): void {
		// snapshot is garbage-collected
	}

	/**
	 * The value a read of `key` returns for `stored`. A committed value is
	 * cloned so that a caller who changes it does not change the store, which
	 * IndexedDB also guarantees.
	 */
	protected value(key: string, stored: unknown): unknown {
		return this.isOwnPut(key) ? stored : copyValue(stored);
	}

	/** Whether `key` holds a value this transaction put and has not committed. */
	protected isOwnPut(_key: string): boolean {
		return false;
	}
}

class MemoryWrite extends MemoryRead implements KVWrite {
	private done = false;
	/** Generation keys this transaction's writes fall under; stamped on commit. */
	private stampAt: Set<string> | undefined;
	/** Keys this transaction deleted, tracked only while generations are, since
	 * a deleted generation key must stay deleted (see commit). */
	private dropped: Set<string> | undefined;
	/** The values commit requires; see KVWrite.guard. */
	private guards: Map<string, unknown> | undefined;
	/** Keys this transaction put; commit clones their values. */
	private puts = new Set<string>();

	constructor(
		private store: MemoryKVStore,
		snapshot: Map<string, unknown>,
		private unlock: () => void,
		private generationOf?: GenerationOf,
	) {
		super(snapshot);
	}

	async put(key: string, value: unknown): Promise<void> {
		this.snapshot.set(key, value);
		this.puts.add(key);
		this.dropped?.delete(key);
		this.note(key);
	}

	async del(key: string): Promise<void> {
		this.snapshot.delete(key);
		this.puts.delete(key);
		if (this.generationOf !== undefined) (this.dropped ??= new Set()).add(key);
		this.note(key);
	}

	guard(key: string, value: unknown): void {
		(this.guards ??= new Map()).set(key, value);
	}

	protected override isOwnPut(key: string): boolean {
		return this.puts.has(key);
	}

	/** Records the generations `key` belongs to, if any. */
	private note(key: string): void {
		const at = this.generationOf?.(key);
		if (at === undefined) return;
		const stampAt = (this.stampAt ??= new Set());
		if (typeof at === 'string') stampAt.add(at);
		else for (const k of at) stampAt.add(k);
	}

	async commit(): Promise<void> {
		if (this.done) throw new Error('transaction already finished');
		this.done = true;
		// IndexedDB copies a value when the put request runs, which for a
		// buffered write is at commit, so a change the caller makes after put
		// but before commit is stored too.
		try {
			for (const key of this.puts) this.snapshot.set(key, copyValue(this.snapshot.get(key)));
		} catch (err) {
			this.unlock();
			throw err;
		}
		// The write lock is held from write() to here, so the committed map is
		// the one this transaction opened on; checked anyway, so the guard does
		// not depend on that.
		for (const [ key, value ] of this.guards ?? []) {
			if (this.store.committed(key) !== value) {
				this.unlock();
				throw new KVGuardError(key);
			}
		}
		if (this.stampAt !== undefined) {
			const token = nextGenerationToken();
			// A transaction that deleted the generation key keeps it deleted — a
			// workspace purge takes the rows and their generation together, and an
			// absent token already means "unknown" to every reader. The snapshot IS
			// the commit here, so the stamp lands inside it.
			for (const at of this.stampAt) {
				if (this.dropped?.has(at) !== true) this.snapshot.set(at, token);
			}
		}
		this.store.applyCommit(this.snapshot);
		this.unlock();
	}

	override release(): void {
		if (this.done) return;
		this.done = true;
		this.unlock();
	}
}

/**
 * A deep copy of `value` with the types IndexedDB stores. It is not
 * structuredClone, because under jsdom that makes typed arrays of another
 * realm, which then fail `instanceof Uint8Array`.
 */
function copyValue(value: unknown): unknown {
	if (value === null || typeof value !== 'object') {
		// A function or a symbol cannot be stored; structuredClone throws the
		// DataCloneError IndexedDB would.
		return typeof value === 'function' || typeof value === 'symbol' ? structuredClone(value) : value;
	}
	if (ArrayBuffer.isView(value)) {
		if (value instanceof DataView) {
			return new DataView(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
		}
		return (value as Uint8Array).slice();
	}
	if (value instanceof ArrayBuffer) return value.slice(0);
	if (Array.isArray(value)) return value.map(copyValue);
	if (value instanceof Map) {
		return new Map([ ...value ].map(([ k, v ]) => [ copyValue(k), copyValue(v) ]));
	}
	if (value instanceof Set) return new Set([ ...value ].map(copyValue));
	if (value instanceof Date) return new Date(value.getTime());
	const out: Record<string, unknown> = {};
	for (const [ k, v ] of Object.entries(value)) out[k] = copyValue(v);
	return out;
}
