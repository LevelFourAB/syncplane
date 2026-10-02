import {
	type GenerationOf,
	type GenerationTracking,
	type IndexEntry,
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
}

class MemoryRead implements KVRead {
	constructor(protected snapshot: Map<string, unknown>) {}

	async get(key: string): Promise<unknown | undefined> {
		return this.snapshot.get(key);
	}

	async getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		return keys.map((k) => this.snapshot.get(k));
	}

	async has(key: string): Promise<boolean> {
		return this.snapshot.has(key);
	}

	async *scan(prefix: string): AsyncIterable<[string, unknown]> {
		const keys = [ ...this.snapshot.keys() ].filter((k) => k.startsWith(prefix)).sort();
		for (const k of keys) {
			yield [ k, this.snapshot.get(k) ];
		}
	}

	async *scanRange(lo?: string, hi?: string): AsyncIterable<[string, unknown]> {
		const keys = [ ...this.snapshot.keys() ]
			.filter((k) => (lo === undefined || k >= lo) && (hi === undefined || k < hi))
			.sort();
		for (const k of keys) {
			yield [ k, this.snapshot.get(k) ];
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
	async *indexScan(lo?: string, hi?: string): AsyncIterable<string> {
		for (const entry of this.indexEntries(lo, hi)) yield entry.key;
	}

	async *indexScanRows(lo?: string, hi?: string): AsyncIterable<IndexEntry> {
		yield* this.indexEntries(lo, hi);
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
					entries.push({ position, key: k, value: v });
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
}

class MemoryWrite extends MemoryRead implements KVWrite {
	private done = false;
	/** Generation keys this transaction's writes fall under; stamped on commit. */
	private stampAt: Set<string> | undefined;
	/** Keys this transaction deleted, tracked only while generations are, since
	 * a deleted generation key must stay deleted (see commit). */
	private dropped: Set<string> | undefined;

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
		this.dropped?.delete(key);
		this.note(key);
	}

	async del(key: string): Promise<void> {
		this.snapshot.delete(key);
		if (this.generationOf !== undefined) (this.dropped ??= new Set()).add(key);
		this.note(key);
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
