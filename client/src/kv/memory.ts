import { type KVRead, type KVStore, type KVWrite, positionsOf } from './store.js';

/**
 * In-memory KVStore for tests and Node usage.
 *
 * Reads see a copy-on-write snapshot taken when the transaction opens.
 * Writes buffer locally and apply atomically on commit. Single-writer:
 * concurrent write() calls queue.
 */
export class MemoryKVStore implements KVStore {
	private map = new Map<string, unknown>();
	private writeLock: Promise<void> = Promise.resolve();

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
		return new MemoryWrite(this, new Map(this.map), unlock);
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
		for (const [ , k ] of this.indexPairs(lo, hi)) yield k;
	}

	async indexSeek(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): Promise<{ position: string; key: string } | undefined> {
		const pairs = this.indexPairs(lo, hi);
		const at = direction === 'desc' ? pairs[pairs.length - 1] : pairs[0];
		return at === undefined ? undefined : { position: at[0], key: at[1] };
	}

	/** The (position, record key) pairs of a range, in the order an IDB index
	 * over the same records yields them. */
	private indexPairs(lo?: string, hi?: string): [string, string][] {
		const pairs: [string, string][] = [];
		for (const [ k, v ] of this.snapshot) {
			for (const position of positionsOf(v)) {
				if ((lo === undefined || position >= lo) && (hi === undefined || position < hi)) {
					pairs.push([ position, k ]);
				}
			}
		}
		// Position order, ties broken on the record key — what an IDB index over
		// the same records yields, so a caller cannot tell the backends apart.
		pairs.sort((a, b) =>
			a[0] !== b[0] ? (a[0] < b[0] ? -1 : 1) : a[1] !== b[1] ? (a[1] < b[1] ? -1 : 1) : 0
		);
		return pairs;
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

	release(): void {
		// snapshot is garbage-collected
	}
}

class MemoryWrite extends MemoryRead implements KVWrite {
	private done = false;

	constructor(
		private store: MemoryKVStore,
		snapshot: Map<string, unknown>,
		private unlock: () => void,
	) {
		super(snapshot);
	}

	async put(key: string, value: unknown): Promise<void> {
		this.snapshot.set(key, value);
	}

	async del(key: string): Promise<void> {
		this.snapshot.delete(key);
	}

	async commit(): Promise<void> {
		if (this.done) throw new Error('transaction already finished');
		this.done = true;
		this.store.applyCommit(this.snapshot);
		this.unlock();
	}

	override release(): void {
		if (this.done) return;
		this.done = true;
		this.unlock();
	}
}
