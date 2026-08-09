import { safeRun } from '../report.js';
import { INDEX_FIELD, type KVRead, type KVStore, type KVWrite, positionsOf } from './store.js';

export interface IDBKVStoreOptions {
	/**
	 * Called when open() is waiting on another connection that has not yet let
	 * an upgrade or delete through. Advisory, not a failure: the open is still
	 * pending and completes as soon as that connection closes. This is the hook
	 * for asking the user to close other tabs.
	 */
	onBlocked?: () => void;
}

/**
 * IndexedDB-backed KVStore. Single object store with out-of-line string keys,
 * carrying one `multiEntry` index over each record's INDEX_FIELD array — which
 * is what makes indexScan a B-tree read rather than a keyspace of its own.
 *
 * IDB transactions auto-commit as soon as the microtask queue drains, so a
 * KVWrite cannot hold a real IDB transaction across app `await`s. Instead,
 * writes buffer in memory and commit() applies them in one short-lived
 * readwrite transaction (atomic).
 *
 * A KVRead is likewise NOT a self-contained snapshot: each get/has/scan opens
 * its own short-lived transaction, so a multi-key logical read straddles
 * several of them. Snapshot consistency is provided one level up — the client
 * runs readers under a shared lock and writers under an exclusive one (see
 * Mutex), so no commit can land between the transactions a logical read spans.
 */
export class IDBKVStore implements KVStore {
	/** Reason the connection went away, once it has; undefined while usable. */
	private lost: string | undefined;

	private constructor(
		private db: IDBDatabase,
		private storeName: string,
	) {
		// A connection that ignores versionchange blocks the other context
		// indefinitely. That context need not be an upgrade — deleteDatabase
		// (devtools "clear site data", a storage reset elsewhere in the app)
		// fires versionchange too, which is why this matters while the version
		// is pinned at 1. Yield the connection and treat the store as gone.
		db.onversionchange = () => this.lose('another connection is upgrading or deleting the database');
		// The browser can also force a connection shut on its own, e.g. when it
		// evicts the origin's storage.
		db.onclose = () => this.lose('the browser closed the connection');
	}

	/**
	 * The IDB version is pinned at 1, and layout migrations deliberately do not
	 * go through it: a store predating the current layout is wiped in-band by
	 * the FORMAT_VERSION check (see keys.ts and SyncClient.ensureFormatVersion),
	 * which needs neither a schema upgrade nor cross-tab coordination.
	 *
	 * The secondary index is therefore declared HERE, at version 1, alongside the
	 * object store. ONE index serves every entity, every declared index and every
	 * workspace, forever: what discriminates them is the prefix of the position
	 * string, not the index's key path (see keys.ts indexPositionPrefix), so no
	 * schema change ever adds a second one and no schema change ever needs a
	 * version at all.
	 *
	 * The gap the pinning leaves is a store created before the index was
	 * declared: onupgradeneeded does not run for it, so it keeps an object store
	 * with no index and every indexScan fails against it. That is loud rather
	 * than silent, and it costs clearing the origin's storage — which is why
	 * bumping was not worth it. Bumping later would be a bigger step than it
	 * looks: every already-open connection must yield first (peers do, on
	 * versionchange — see the constructor), and onupgradeneeded would have to
	 * migrate existing contents rather than assume the empty-store case below.
	 */
	static async open(
		name: string,
		storeName = 'kv',
		opts: IDBKVStoreOptions = {},
	): Promise<IDBKVStore> {
		const db = await new Promise<IDBDatabase>((resolve, reject) => {
			const req = indexedDB.open(name, 1);
			req.onupgradeneeded = () => {
				if (!req.result.objectStoreNames.contains(storeName)) {
					// multiEntry: one index record per element of the array, so a row
					// occupying several positions is reachable from each of them.
					req.result.createObjectStore(storeName)
						.createIndex(INDEX_FIELD, INDEX_FIELD, { multiEntry: true });
				}
			};
			req.onsuccess = () => resolve(req.result);
			req.onerror = () => reject(req.error);
			// Another connection still holds the old version. The request is not
			// dead — it completes once that connection closes, which peers running
			// this code do on versionchange — so report a diagnosable wait instead
			// of rejecting a state that recovers on its own. (Rejecting would also
			// be unrecoverable in practice: SyncClient memoizes the open promise,
			// so one rejection would keep the page storeless for good.)
			req.onblocked = () => {
				console.warn(
					`sync: IndexedDB open of "${name}" is waiting on another connection to close. `
						+ `If this persists, close other tabs of this app.`,
				);
				if (opts.onBlocked) safeRun(opts.onBlocked, 'IDBKVStore onBlocked');
			};
		});
		return new IDBKVStore(db, storeName);
	}

	async read(): Promise<KVRead> {
		this.assertUsable();
		return new IDBRead(this.db, this.storeName);
	}

	async write(): Promise<KVWrite> {
		this.assertUsable();
		return new IDBWrite(this.db, this.storeName);
	}

	async close(): Promise<void> {
		this.lose('the store was closed');
	}

	/** Closes the connection once, keeping the first (most specific) reason. */
	private lose(reason: string): void {
		this.lost ??= reason;
		this.db.close();
	}

	/**
	 * Fails a dead store eagerly, with the reason it died. Without this, every
	 * transaction opened afterwards throws a bare InvalidStateError from inside
	 * IDB with nothing pointing at the cause. No separate signal is needed to
	 * reach the app: SyncClient already routes store failures onto the status
	 * channel as lastError.
	 */
	private assertUsable(): void {
		if (this.lost) throw new Error(`sync: IndexedDB store unavailable — ${this.lost}`);
	}
}

function requestDone<T>(req: IDBRequest<T>): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		req.onsuccess = () => resolve(req.result);
		req.onerror = () => reject(req.error);
	});
}

/**
 * Bounds a prefix scan exactly. IDB orders strings by UTF-16 code unit, so
 * every key starting with `prefix` sorts in [prefix, successor(prefix)) and
 * nothing else does — matching MemoryKVStore's `startsWith` filter for any key
 * content. Row keys come from user data, so "any key content" includes U+FFFF:
 * an inclusive `prefix + '\uffff'` upper bound would drop `${prefix}\uffffx`
 * from every scan while `get()` still found it, a divergence the memory-backed
 * unit suite could never see.
 */
/**
 * Whether [lo, hi) selects nothing.
 *
 * A collapsed range is legal input — a probe whose bounds met — but IDB cannot
 * represent one: `IDBKeyRange.bound` throws unless lower < upper, and the only
 * other value a range argument takes is null, which means UNBOUNDED and would
 * read the whole store. So the empty range is answered by not asking, which is
 * what every caller of boundRange has to do first.
 */
function emptyRange(lo?: string, hi?: string): boolean {
	return lo !== undefined && hi !== undefined && lo >= hi;
}

/**
 * A half-open [lo, hi) key range; null when both ends are open. The range must
 * not be empty (see emptyRange) — there is no IDBKeyRange for that. `loOpen`
 * excludes `lo` itself, which is how a chunked scan asks for "every key after
 * the last one yielded".
 */
function boundRange(lo?: string, hi?: string, loOpen = false): IDBKeyRange | null {
	if (lo === undefined && hi === undefined) return null;
	if (hi === undefined) return IDBKeyRange.lowerBound(lo!, loOpen);
	if (lo === undefined) return IDBKeyRange.upperBound(hi, true);
	return IDBKeyRange.bound(lo, hi, loOpen, true);
}

/**
 * The least string that sorts above every string starting with `s`: `s` with
 * its last code unit incremented, carrying past trailing U+FFFF. Undefined
 * when every code unit is U+FFFF and the carry runs off the front.
 */
function successor(s: string): string | undefined {
	for (let i = s.length - 1; i >= 0; i--) {
		const unit = s.charCodeAt(i);
		if (unit !== 0xffff) return s.slice(0, i) + String.fromCharCode(unit + 1);
	}
	return undefined;
}

/**
 * Positions one indexScan asks the store for before yielding anything, and the
 * factor it grows that request by when the caller wants more.
 *
 * A cursor would need no such guess, but a cursor cannot survive the app awaits
 * a caller does between yields (it reads each row it is handed), so the range
 * is taken in getAllKeys chunks instead. Growing geometrically bounds the
 * re-read: continuing past a chunk re-requests the positions already yielded
 * and skips them, so a caller that consumes N positions costs under 2N — while
 * the overwhelmingly common case, a bounded query wanting a page, costs exactly
 * one request that stops at the chunk instead of materializing the whole range.
 */
const INDEX_SCAN_CHUNK = 128;

/**
 * Entries one scan/scanRange request materializes before yielding, and the cap
 * requests grow toward.
 *
 * Like indexScan, a scan takes its range in bounded getAll requests rather
 * than a cursor (which cannot survive the app awaits between yields) or one
 * unbounded getAll (which materializes a range the caller may abandon after a
 * page — the index build reads BUILD_BATCH rows of an entity per batch, and an
 * unbounded request would read the whole remainder every time). Unlike
 * indexScan there is no re-read on continuation: primary keys are unique, so
 * the next request resumes EXCLUSIVELY after the last key yielded. Growing
 * geometrically keeps a full-entity scan at a handful of requests while an
 * early-stopping caller pays one small one.
 */
const SCAN_CHUNK = 256;
const SCAN_CHUNK_MAX = 8192;

/** Orders (position, key) pairs the way an IDB index does: by index key, ties
 * broken on the record's primary key. */
function comparePositions(a: readonly [string, string], b: readonly [string, string]): number {
	if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
	if (a[1] !== b[1]) return a[1] < b[1] ? -1 : 1;
	return 0;
}

class IDBRead implements KVRead {
	constructor(
		protected db: IDBDatabase,
		protected storeName: string,
	) {}

	protected store(mode: IDBTransactionMode = 'readonly'): IDBObjectStore {
		return this.db.transaction(this.storeName, mode).objectStore(this.storeName);
	}

	async get(key: string): Promise<unknown | undefined> {
		return requestDone(this.store().get(key));
	}

	async getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		if (keys.length === 0) return [];
		// All requests are issued before the first await, so they share ONE
		// transaction — which is the point: a page of N rows costs one
		// transaction rather than N.
		const store = this.store();
		return Promise.all(keys.map((k) => requestDone(store.get(k))));
	}

	async has(key: string): Promise<boolean> {
		const count = await requestDone(this.store().count(key));
		return count > 0;
	}

	async *scan(prefix: string): AsyncIterable<[string, unknown]> {
		// [prefix, successor(prefix)) selects exactly the keys `startsWith`
		// selects, for any key content including U+FFFF (see successor). An
		// all-U+FFFF prefix has no successor: no string sorts above it, so the
		// range is bounded below only.
		yield* this.ranged(prefix === '' ? undefined : prefix, successor(prefix));
	}

	async *scanRange(lo?: string, hi?: string): AsyncIterable<[string, unknown]> {
		if (emptyRange(lo, hi)) return;
		yield* this.ranged(lo, hi);
	}

	/**
	 * The shared body of scan and scanRange: [lo, hi) in SCAN_CHUNK-sized
	 * getAll requests, continuing exclusively after the last key yielded. Each
	 * request opens its own transaction; the client holds a shared lock across
	 * a logical read, so no commit lands between them (see the class docs).
	 */
	protected async *ranged(lo?: string, hi?: string): AsyncIterable<[string, unknown]> {
		let want = SCAN_CHUNK;
		let from = lo;
		let fromOpen = false;
		for (;;) {
			const store = this.store();
			const range = boundRange(from, hi, fromOpen);
			// Keys and values in two requests on ONE transaction, so they describe
			// the same rows.
			const [ keys, values ] = await Promise.all([
				requestDone(store.getAllKeys(range, want)),
				requestDone(store.getAll(range, want)),
			]);
			for (let i = 0; i < keys.length; i++) {
				yield [ keys[i] as string, values[i] ];
			}
			// Short of what was asked for means the range is exhausted.
			if (keys.length < want) return;
			from = keys[keys.length - 1] as string;
			fromOpen = true;
			if (want < SCAN_CHUNK_MAX) want *= 2;
		}
	}

	async *indexScan(lo?: string, hi?: string): AsyncIterable<string> {
		if (emptyRange(lo, hi)) return;
		const range = boundRange(lo, hi);
		// `getAllKeys` on an INDEX returns the PRIMARY keys of the matching
		// records, one per matching index entry, in index order — which is the
		// whole read path in one request, and it takes a count so the store stops
		// there rather than materializing the range and discarding the tail.
		let want = INDEX_SCAN_CHUNK;
		let yielded = 0;
		for (;;) {
			const keys = await requestDone(
				this.store().index(INDEX_FIELD).getAllKeys(range, want),
			) as string[];
			for (let i = yielded; i < keys.length; i++) yield keys[i]!;
			// Short of what was asked for means the range is exhausted. Equal means
			// there may be more, so ask for twice as much and skip what was already
			// yielded — successive requests return prefixes of the same ordering, so
			// the skip is exact. Each request opens its own transaction; the client
			// holds a shared lock across a logical read, so no commit lands between
			// them (see the class docs).
			if (keys.length < want) return;
			yielded = keys.length;
			want *= 2;
		}
	}

	async indexCount(lo?: string, hi?: string): Promise<number> {
		if (emptyRange(lo, hi)) return 0;
		const range = boundRange(lo, hi);
		const index = this.store().index(INDEX_FIELD);
		return requestDone(range === null ? index.count() : index.count(range));
	}

	release(): void {
		// short-lived transactions; nothing held
	}
}

class IDBWrite extends IDBRead implements KVWrite {
	/** Buffered writes: value, or tombstone symbol for deletes. */
	private pending = new Map<string, unknown>();
	private deleted = new Set<string>();
	private done = false;

	override async get(key: string): Promise<unknown | undefined> {
		if (this.deleted.has(key)) return undefined;
		if (this.pending.has(key)) return this.pending.get(key);
		return super.get(key);
	}

	override async getMany(keys: readonly string[]): Promise<(unknown | undefined)[]> {
		const out = new Array<unknown | undefined>(keys.length);
		const misses: number[] = [];
		for (let i = 0; i < keys.length; i++) {
			const k = keys[i]!;
			if (this.deleted.has(k)) out[i] = undefined;
			else if (this.pending.has(k)) out[i] = this.pending.get(k);
			else misses.push(i);
		}
		if (misses.length > 0) {
			const got = await super.getMany(misses.map((i) => keys[i]!));
			for (let j = 0; j < misses.length; j++) out[misses[j]!] = got[j];
		}
		return out;
	}

	override async has(key: string): Promise<boolean> {
		if (this.deleted.has(key)) return false;
		if (this.pending.has(key)) return true;
		return super.has(key);
	}

	override async *scan(prefix: string): AsyncIterable<[string, unknown]> {
		yield* this.mergeBuffered(super.scan(prefix), (k) => k.startsWith(prefix));
	}

	override async *scanRange(lo?: string, hi?: string): AsyncIterable<[string, unknown]> {
		yield* this.mergeBuffered(
			super.scanRange(lo, hi),
			(k) => (lo === undefined || k >= lo) && (hi === undefined || k < hi),
		);
	}

	/**
	 * The index read, with this transaction's uncommitted writes folded in.
	 *
	 * IDB maintains its index from COMMITTED records, so the inherited fast path
	 * would not see a position the caller just wrote — and MemoryKVStore, whose
	 * transaction is a mutated snapshot, would. A backend-visible difference in
	 * what a write transaction can read back is exactly the kind of divergence
	 * the memory-backed unit suite cannot catch, so the two agree here instead.
	 *
	 * It costs reading the range's VALUES rather than its keys, and it is not
	 * lazy. Nothing on a hot path reads an index inside a write transaction —
	 * queries take a read transaction (see query/execute.ts) — so the honest
	 * semantics are worth more here than the request-shaped ones above.
	 */
	override async *indexScan(lo?: string, hi?: string): AsyncIterable<string> {
		if (emptyRange(lo, hi)) return;
		const range = boundRange(lo, hi);
		const index = this.store().index(INDEX_FIELD);
		const [ keys, values ] = await Promise.all([
			requestDone(index.getAllKeys(range)),
			requestDone(index.getAll(range)),
		]);
		// Keyed by record, so a record holding several positions in the range is
		// collapsed to one entry here and expanded again below — which is also
		// what lets a buffered write REPLACE the committed record's positions
		// rather than add to them.
		const records = new Map<string, unknown>();
		for (let i = 0; i < keys.length; i++) records.set(keys[i] as string, values[i]);
		for (const [ k, v ] of this.pending) records.set(k, v);
		for (const k of this.deleted) records.delete(k);
		const pairs: [string, string][] = [];
		for (const [ k, v ] of records) {
			for (const position of positionsOf(v)) {
				if ((lo === undefined || position >= lo) && (hi === undefined || position < hi)) {
					pairs.push([ position, k ]);
				}
			}
		}
		pairs.sort(comparePositions);
		for (const [ , k ] of pairs) yield k;
	}

	/**
	 * Counted through this class's own indexScan rather than the store's index,
	 * for the same reason indexScan above folds the buffer in: the count must
	 * agree with what the scan would yield. Same cost note applies — nothing on
	 * a hot path counts an index inside a write transaction.
	 */
	override async indexCount(lo?: string, hi?: string): Promise<number> {
		let n = 0;
		for await (const _ of this.indexScan(lo, hi)) n++;
		return n;
	}

	/**
	 * Overlays this transaction's uncommitted writes and deletes onto a
	 * committed-state iteration, so a scan sees what the caller just wrote.
	 *
	 * A merge-join of the two sorted streams rather than a materialized map:
	 * the committed side is pulled only as far as the caller reads, which is
	 * what keeps a scan a caller abandons after a page — the index build reads
	 * one batch of an entity per transaction — from materializing the rest of
	 * the range. Both sides are key-ordered (the buffered side by sorting here,
	 * which is O(pending) and small), and pending/deleted are disjoint by
	 * construction (see put/del), so each committed key resolves in one step.
	 */
	private async *mergeBuffered(
		committed: AsyncIterable<[string, unknown]>,
		selects: (key: string) => boolean,
	): AsyncIterable<[string, unknown]> {
		const adds = [ ...this.pending.keys() ].filter(selects).sort();
		let next = 0;
		for await (const [ k, v ] of committed) {
			while (next < adds.length && adds[next]! < k) {
				yield [ adds[next]!, this.pending.get(adds[next]!) ];
				next++;
			}
			if (next < adds.length && adds[next] === k) {
				yield [ k, this.pending.get(k) ];
				next++;
				continue;
			}
			if (this.deleted.has(k)) continue;
			yield [ k, v ];
		}
		for (; next < adds.length; next++) {
			yield [ adds[next]!, this.pending.get(adds[next]!) ];
		}
	}

	async put(key: string, value: unknown): Promise<void> {
		this.deleted.delete(key);
		this.pending.set(key, value);
	}

	async del(key: string): Promise<void> {
		this.pending.delete(key);
		this.deleted.add(key);
	}

	async commit(): Promise<void> {
		if (this.done) throw new Error('transaction already finished');
		this.done = true;
		if (this.pending.size === 0 && this.deleted.size === 0) return;
		const tx = this.db.transaction(this.storeName, 'readwrite');
		const store = tx.objectStore(this.storeName);
		for (const [ k, v ] of this.pending) store.put(v, k);
		for (const k of this.deleted) store.delete(k);
		await new Promise<void>((resolve, reject) => {
			tx.oncomplete = () => resolve();
			tx.onerror = () => reject(tx.error);
			tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
		});
	}

	override release(): void {
		this.done = true;
		this.pending.clear();
		this.deleted.clear();
	}
}
