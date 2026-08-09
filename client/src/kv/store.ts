/**
 * Pluggable transactional KV storage (Replicache-style).
 *
 * Values must be structured-cloneable (IndexedDB requirement). Keys are
 * ordered by code-unit string comparison; scans are prefix-based.
 *
 * Beyond the key-ordered map, a store maintains ONE secondary index over a
 * materialized field of the values it holds — see INDEX_FIELD and indexScan.
 * That is the whole of the extra surface, and it is what lets a client keep
 * secondary indexes without spending a KV record per indexed position.
 */

/**
 * Field of a stored value holding the secondary-index POSITIONS that value
 * occupies, and the name of the store-maintained index over it.
 *
 * A value that is an object carrying `ix: string[]` occupies one index position
 * per element; every other value — a primitive, an array, an object without the
 * field — occupies none. That rule is what keeps meta records, pending
 * mutations and overlay rows out of the index without a second object store.
 *
 * Present-but-not-a-string-array is not a third case, it is a bug: IndexedDB's
 * multiEntry index takes a non-array value as ONE key while a backend walking
 * its own map sees no list to walk, so the backends do not agree and neither is
 * wrong. Everything written through indexedRecord satisfies the contract, which
 * is why this is stated rather than defended against.
 *
 * The backend maintains the index itself: IndexedDB with a `multiEntry` index
 * over the field, MemoryKVStore by walking its map. So a row's positions cost
 * no KV record of their own — they ride on a record that is written anyway,
 * which is the point (see optimistic/index-maintain.ts for what fills them).
 *
 * The corollary every writer has to respect: a value written WITHOUT the field
 * occupies no positions. There is no separate keyspace left holding them, so
 * rewriting a record with a stale, partial or absent `ix` silently unindexes
 * the row rather than leaving an entry behind.
 */
export const INDEX_FIELD = 'ix';

const NO_POSITIONS: readonly string[] = [];

/**
 * The index positions a stored value occupies, in the order it lists them —
 * minus repeats, because the positions are a SET: IndexedDB's multiEntry index
 * stores one entry per distinct element, so a backend walking the raw list has
 * to collapse repeats the same way or the two would disagree on what a range
 * holds. (A repeated element of an indexed field encodes to the same position
 * twice, which is where a repeat comes from.)
 *
 * Empty for anything that is not an object carrying INDEX_FIELD as an array —
 * values reaching a store are not all records, so this has to tolerate
 * whatever a caller put there.
 */
export function positionsOf(value: unknown): readonly string[] {
	if (typeof value !== 'object' || value === null) return NO_POSITIONS;
	const ix = (value as Record<string, unknown>)[INDEX_FIELD];
	if (!Array.isArray(ix)) return NO_POSITIONS;
	const set = new Set(ix as string[]);
	return set.size === ix.length ? ix as string[] : [ ...set ];
}

export interface KVStore {
	/**
	 * Open a read snapshot. Must be released. MemoryKVStore makes this an
	 * intrinsic snapshot (copy-on-open); IDBKVStore does not — its reads are
	 * only snapshot-consistent when taken under the client's shared lock, which
	 * excludes them from concurrent writers (see Mutex / idb.ts).
	 */
	read(): Promise<KVRead>;
	/** Open a write transaction. Must be committed or released. */
	write(): Promise<KVWrite>;
	close(): Promise<void>;
}

export interface KVRead {
	get(key: string): Promise<unknown | undefined>;
	/**
	 * Reads several keys at once. The result is parallel to `keys`: one entry
	 * per requested key, `undefined` where the key is absent. Duplicate keys are
	 * answered independently.
	 *
	 * Semantically `Promise.all(keys.map(get))`; it exists because a backend can
	 * answer the whole batch in one transaction where per-key gets each open
	 * their own.
	 */
	getMany(keys: readonly string[]): Promise<(unknown | undefined)[]>;
	has(key: string): Promise<boolean>;
	/** Yields entries whose key starts with `prefix`, in ascending key order. */
	scan(prefix: string): AsyncIterable<[key: string, value: unknown]>;
	/**
	 * Yields entries with `lo <= key < hi`, in ascending key order. An undefined
	 * bound is open on that end.
	 *
	 * This exists for secondary-index range scans, which is the one read that
	 * genuinely needs a half-open range rather than a prefix: the whole point of
	 * an index is to read the slice a query bounds, and expressing that as a
	 * prefix scan plus a filter would walk everything the index was built to
	 * skip.
	 */
	scanRange(lo?: string, hi?: string): AsyncIterable<[key: string, value: unknown]>;
	/**
	 * Yields the KEY of every record holding an index position in [lo, hi), in
	 * position order — one yield per matching POSITION, not per record. An
	 * undefined bound is open on that end.
	 *
	 * Positions are ordered by code-unit string comparison, exactly as keys are,
	 * and a tie is broken on the record's key; a position ends with the row key
	 * it belongs to, so ties do not arise in practice (see query/indexkey.ts).
	 * A record occupying several positions in the range is therefore yielded
	 * several times, and a caller that wants rows dedupes (queryByIndex does).
	 *
	 * It is LAZY, and that is the reason it is a verb of its own rather than a
	 * scanRange over an index keyspace: an index read almost always wants the
	 * first few rows of a wide range, and a backend must not have paid for the
	 * tail a caller never pulled. Stopping the iteration stops the work.
	 */
	indexScan(lo?: string, hi?: string): AsyncIterable<string>;
	/**
	 * The number of index positions in [lo, hi) — how many entries an indexScan
	 * over the same range would yield, counted per POSITION exactly as indexScan
	 * yields. An undefined bound is open on that end.
	 *
	 * It exists because counting is the one range read that needs none of the
	 * range's contents: IndexedDB answers it from the B-tree in a single request,
	 * where walking the range materializes every key in it.
	 */
	indexCount(lo?: string, hi?: string): Promise<number>;
	release(): void;
}

export interface KVWrite extends KVRead {
	put(key: string, value: unknown): Promise<void>;
	del(key: string): Promise<void>;
	/** Atomically persist all writes. Implies release. */
	commit(): Promise<void>;
}

/**
 * A write transaction that counts the entries written through it. Puts and
 * deletes both count, once per call: a delete is work for the store too, and
 * writing the same key twice is work done twice.
 *
 * It exists so a transaction can report its own write amplification without
 * every helper it calls having to thread a counter through. The bootstrap swap
 * uses it (see Coordinator.doBootstrap, which reports the count on
 * BootstrapEvent) — how many entries a snapshot costs per row is the difference
 * between a swap that is fast and one that is not, and it is otherwise
 * invisible until it shows up as wall time.
 */
export class CountingWrite implements KVWrite {
	/** Entries written so far. Meaningful once the transaction has committed. */
	writes = 0;

	constructor(private inner: KVWrite) {}

	get(key: string): Promise<unknown | undefined> {
		return this.inner.get(key);
	}

	getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		return this.inner.getMany(keys);
	}

	has(key: string): Promise<boolean> {
		return this.inner.has(key);
	}

	scan(prefix: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scan(prefix);
	}

	scanRange(lo?: string, hi?: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scanRange(lo, hi);
	}

	indexScan(lo?: string, hi?: string): AsyncIterable<string> {
		return this.inner.indexScan(lo, hi);
	}

	indexCount(lo?: string, hi?: string): Promise<number> {
		return this.inner.indexCount(lo, hi);
	}

	async put(key: string, value: unknown): Promise<void> {
		this.writes++;
		await this.inner.put(key, value);
	}

	async del(key: string): Promise<void> {
		this.writes++;
		await this.inner.del(key);
	}

	async commit(): Promise<void> {
		await this.inner.commit();
	}

	release(): void {
		this.inner.release();
	}
}
