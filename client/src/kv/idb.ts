import { safeRun } from '../report.js';
import {
	type GenerationOf,
	type GenerationTracking,
	INDEX_FIELD,
	type IndexEntry,
	type KVRead,
	type KVStore,
	type KVWrite,
	nextGenerationToken,
	positionsOf,
} from './store.js';

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
export class IDBKVStore implements KVStore, GenerationTracking {
	/** Reason the connection went away, once it has; undefined while usable. */
	private lost: string | undefined;

	/** Installed by trackGenerations; undefined until then. */
	private generationOf: GenerationOf | undefined;

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
		return new IDBWrite(this.db, this.storeName, this.generationOf);
	}

	trackGenerations(generationOf: GenerationOf): void {
		this.generationOf = generationOf;
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
 * The same promise, marked as already having a rejection handler.
 *
 * A promise that is created now and awaited several turns later — or abandoned
 * when a consumer stops mid-scan — would otherwise surface a failed request as
 * an unhandled rejection on the page before anything can await it.
 */
function handled<T>(p: Promise<T>): Promise<T> {
	p.catch(() => {});
	return p;
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

/**
 * Entries one indexScanRows request materializes before yielding, and the cap
 * requests grow toward.
 *
 * Capped where indexScan's growth is not, because the two pay differently for a
 * continuation: indexScan re-requests the whole prefix it already yielded, so
 * only unbounded growth keeps that under a constant factor, while this one
 * resumes at the last position and re-reads nothing but a tie group. What grows
 * here is the records held in memory at once, and the cap is what bounds them.
 */
const INDEX_ROW_CHUNK = 256;
const INDEX_ROW_CHUNK_MAX = 8192;

/** One entry of an IDB index, as getAllRecords answers with it: `key` is the
 * index key, `primaryKey` the record's. */
interface IDBIndexRecord {
	key: string;
	primaryKey: string;
	value: unknown;
}

/**
 * `IDBIndex.getAllRecords`, declared here because TypeScript's lib.dom has no
 * definition for it yet. Engines that predate it are served by the paired
 * getAllKeys/getAll request instead, so every use is feature-detected.
 */
interface RecordReadingIndex extends IDBIndex {
	getAllRecords(
		options?: { query?: IDBKeyRange | null; count?: number },
	): IDBRequest<IDBIndexRecord[]>;
}

function readsRecords(index: IDBIndex): RecordReadingIndex | undefined {
	return 'getAllRecords' in index ? index as RecordReadingIndex : undefined;
}

/** Orders index entries the way an IDB index does: by index key, ties broken on
 * the record's primary key. */
function comparePositions(a: IndexEntry, b: IndexEntry): number {
	if (a.position !== b.position) return a.position < b.position ? -1 : 1;
	if (a.key !== b.key) return a.key < b.key ? -1 : 1;
	return 0;
}

/**
 * Which position each record of a paired getAllKeys/getAll chunk was returned
 * for, recovered from the record's own INDEX_FIELD list.
 *
 * getAllKeys on an index answers with PRIMARY keys, so the position is not in
 * the response. It is recoverable because the response is in position order: a
 * record's n-th appearance in the chunk is its n-th position inside [lo, hi).
 *
 * The single-position case — every entry of an index nothing fans out on —
 * builds no list at all. This runs once per entry of a bulk read, so a list per
 * record is a cost the read cannot absorb.
 */
function recoverEntries(
	keys: readonly string[],
	values: readonly unknown[],
	lo: string | undefined,
	hi: string | undefined,
): IndexEntry[] {
	const within = (p: string): boolean =>
		(lo === undefined || p >= lo) && (hi === undefined || p < hi);
	const entries: IndexEntry[] = [];
	// Only a record holding SEVERAL positions of the range needs a count of the
	// times it has been seen, and an index without fan-out gives none.
	let repeats: Map<string, number> | undefined;
	for (let i = 0; i < keys.length; i++) {
		const key = keys[i]!;
		const value = values[i];
		const ix = typeof value === 'object' && value !== null
			? (value as Record<string, unknown>)[INDEX_FIELD]
			: undefined;
		if (!Array.isArray(ix)) continue;
		let position: string | undefined;
		let several = false;
		for (const p of ix as string[]) {
			if (!within(p)) continue;
			if (position === undefined) position = p;
			else if (p !== position) {
				several = true;
				if (p < position) position = p;
			}
		}
		if (position === undefined) continue;
		if (several) {
			repeats ??= new Map();
			const n = repeats.get(key) ?? 0;
			repeats.set(key, n + 1);
			if (n > 0) {
				position = [ ...new Set(ix as string[]) ].filter(within).sort()[n];
				if (position === undefined) continue;
			}
		}
		entries.push({ position, key, value });
	}
	return entries;
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
	 * Up to `want` entries of [lo, hi) as two requests on ONE transaction, so
	 * the keys and the values describe the same rows.
	 *
	 * The two are handed back separately rather than joined, because the keys
	 * alone bound the next chunk: a caller that awaits them first can issue the
	 * next request while this chunk's values are still being deserialized.
	 */
	private scanChunk(
		lo: string | undefined,
		hi: string | undefined,
		want: number,
		loOpen: boolean,
	): { keys: Promise<string[]>; values: Promise<unknown[]> } {
		const store = this.store();
		const range = boundRange(lo, hi, loOpen);
		return {
			keys: handled(requestDone(store.getAllKeys(range, want)) as Promise<string[]>),
			values: handled(requestDone(store.getAll(range, want))),
		};
	}

	/**
	 * The shared body of scan and scanRange: [lo, hi) in SCAN_CHUNK-sized
	 * getAll requests, continuing exclusively after the last key yielded. Each
	 * request opens its own transaction; the client holds a shared lock across
	 * a logical read, so no commit lands between them (see the class docs).
	 *
	 * At most one chunk is read ahead, and only once the consumer has taken a
	 * whole one: the store is a different thread, so a scan that waits for a
	 * chunk to arrive before asking for the next one spends the deserialization
	 * of every chunk with the store idle and the store's read of every chunk
	 * with the main thread idle. A consumer that stops inside its first chunk
	 * still costs exactly the one request it took.
	 */
	protected async *ranged(lo?: string, hi?: string): AsyncIterable<[string, unknown]> {
		let want = SCAN_CHUNK;
		let chunk = this.scanChunk(lo, hi, want, false);
		let drained = false;
		for (;;) {
			const keys = await chunk.keys;
			// Short of what was asked for means the range is exhausted.
			const more = keys.length === want;
			const next = more && want < SCAN_CHUNK_MAX ? want * 2 : want;
			const last = more ? keys[keys.length - 1]! : undefined;
			// Placed before the values are awaited, which is where the overlap
			// comes from: this chunk's values are still crossing into the page
			// while the store is already reading the next chunk's.
			const ahead = last !== undefined && drained
				? this.scanChunk(last, hi, next, true)
				: undefined;
			const values = await chunk.values;
			for (let i = 0; i < keys.length; i++) {
				yield [ keys[i]!, values[i] ];
			}
			if (last === undefined) return;
			drained = true;
			chunk = ahead ?? this.scanChunk(last, hi, next, true);
			want = next;
		}
	}

	async *indexScan(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): AsyncIterable<string> {
		if (emptyRange(lo, hi)) return;
		if (direction === 'desc') {
			for await (const entry of this.indexBackward(lo, hi, false)) yield entry.key;
			return;
		}
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

	async *indexScanRows(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): AsyncIterable<IndexEntry> {
		if (emptyRange(lo, hi)) return;
		if (direction === 'desc') {
			yield* this.indexBackward(lo, hi, true);
			return;
		}
		let want = INDEX_ROW_CHUNK;
		let from = lo;
		// The last entry yielded, which the next request resumes AT rather than
		// after: a position can be held by several records and the request range
		// is over positions alone, so the range has to take the position again
		// and the entries at or before this one are dropped here.
		let last: IndexEntry | undefined;
		for (;;) {
			const chunk = await this.indexChunk(from, hi, want);
			let fresh = 0;
			for (const entry of chunk) {
				if (last !== undefined && comparePositions(entry, last) <= 0) continue;
				last = entry;
				fresh++;
				yield entry;
			}
			// Short of what was asked for means the range is exhausted. Each
			// request opens its own transaction; the client holds a shared lock
			// across a logical read, so no commit lands between them (see the
			// class docs).
			if (chunk.length < want) return;
			if (last !== undefined) from = last.position;
			// A whole chunk of already-yielded entries means the tie group at
			// `last` is longer than the request, and only a bigger request gets
			// past it — so that case grows past the cap rather than spinning.
			want = fresh === 0 ? want * 2 : Math.min(want * 2, INDEX_ROW_CHUNK_MAX);
		}
	}

	/**
	 * The entries of [lo, hi) from the high end down, a chunk per request,
	 * growing as indexScan's do. `values` decides whether each entry carries its
	 * record; without it `value` is undefined.
	 *
	 * Backward reads take a cursor rather than getAll*: a getAll request has no
	 * direction outside the newest engines. The cursor walks one chunk inside a
	 * single request and is dropped before the first yield, because it cannot
	 * survive the awaits a caller does between yields; the next chunk opens a
	 * fresh one at the last entry yielded.
	 */
	private async *indexBackward(
		lo: string | undefined,
		hi: string | undefined,
		values: boolean,
	): AsyncIterable<IndexEntry> {
		let want = INDEX_SCAN_CHUNK;
		let last: IndexEntry | undefined;
		for (;;) {
			const chunk = await this.backwardChunk(lo, hi, want, last, values);
			yield* chunk;
			if (chunk.length < want) return;
			last = chunk[chunk.length - 1]!;
			want = Math.min(want * 2, INDEX_ROW_CHUNK_MAX);
		}
	}

	/**
	 * Up to `want` entries of [lo, hi) read backward in one request, starting
	 * below `after` when it is set: below its position, or at its position with
	 * a lesser record key.
	 */
	private backwardChunk(
		lo: string | undefined,
		hi: string | undefined,
		want: number,
		after: IndexEntry | undefined,
		values: boolean,
	): Promise<IndexEntry[]> {
		// The resumed range keeps `after.position` itself, closed, because the
		// records sharing it with a lesser key are still to come.
		const range = after === undefined
			? boundRange(lo, hi)
			: lo === undefined
			? IDBKeyRange.upperBound(after.position)
			: IDBKeyRange.bound(lo, after.position);
		const index = this.store().index(INDEX_FIELD);
		const request = values ? index.openCursor(range, 'prev') : index.openKeyCursor(range, 'prev');
		return new Promise<IndexEntry[]>((resolve, reject) => {
			const out: IndexEntry[] = [];
			request.onerror = () => reject(request.error);
			request.onsuccess = () => {
				const cursor = request.result;
				if (cursor === null) {
					resolve(out);
					return;
				}
				const position = cursor.key as string;
				const key = cursor.primaryKey as string;
				if (after !== undefined && position === after.position && key >= after.key) {
					// Under 'prev' a tie runs from the greatest record key down, so
					// the ones already yielded come first. continuePrimaryKey skips
					// them in one step, landing on `after` itself, which the plain
					// continue below steps past.
					if (key > after.key) cursor.continuePrimaryKey(after.position, after.key);
					else cursor.continue();
					return;
				}
				out.push({ position, key, value: values ? (cursor as IDBCursorWithValue).value : undefined });
				if (out.length >= want) resolve(out);
				else cursor.continue();
			};
		});
	}

	/**
	 * Up to `want` entries of [lo, hi) in one request, in index order.
	 *
	 * getAllRecords answers with the index key, the primary key and the value
	 * together; where it is missing, the same chunk takes a paired
	 * getAllKeys/getAll on ONE transaction — so the two describe the same
	 * entries — and the positions are recovered from the records.
	 */
	protected async indexChunk(
		lo: string | undefined,
		hi: string | undefined,
		want: number,
	): Promise<IndexEntry[]> {
		const index = this.store().index(INDEX_FIELD);
		const range = boundRange(lo, hi);
		const full = readsRecords(index);
		if (full !== undefined) {
			const records = await requestDone(full.getAllRecords({ query: range, count: want }));
			return records.map((r) => ({ position: r.key, key: r.primaryKey, value: r.value }));
		}
		const [ keys, values ] = await Promise.all([
			requestDone(index.getAllKeys(range, want)) as Promise<string[]>,
			requestDone(index.getAll(range, want)),
		]);
		return recoverEntries(keys, values, lo, hi);
	}

	async indexSeek(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): Promise<{ position: string; key: string } | undefined> {
		if (emptyRange(lo, hi)) return undefined;
		// A key cursor rather than getAllKeys: the POSITION is what a skip-scan
		// resumes from, and getAllKeys on an index returns only the primary keys.
		// One step is all that is read, so the cursor never has to survive an
		// await the way a streaming scan's would.
		const cursor = await requestDone(
			this.store().index(INDEX_FIELD).openKeyCursor(
				boundRange(lo, hi),
				direction === 'desc' ? 'prev' : 'next',
			),
		);
		if (cursor === null) return undefined;
		return { position: cursor.key as string, key: cursor.primaryKey as string };
	}

	async indexCount(lo?: string, hi?: string): Promise<number> {
		if (emptyRange(lo, hi)) return 0;
		const range = boundRange(lo, hi);
		const index = this.store().index(INDEX_FIELD);
		return requestDone(range === null ? index.count() : index.count(range));
	}

	async seek(lo?: string, hi?: string): Promise<string | undefined> {
		if (emptyRange(lo, hi)) return undefined;
		// A key cursor rather than getKey: getKey refuses an open range, and one
		// cursor step reads exactly one key either way.
		const cursor = await requestDone(this.store().openKeyCursor(boundRange(lo, hi)));
		return cursor === null ? undefined : cursor.key as string;
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
	/** Generation keys the buffered writes fall under; stamped on commit. */
	private stampAt: Set<string> | undefined;

	constructor(db: IDBDatabase, storeName: string, private generationOf?: GenerationOf) {
		super(db, storeName);
	}

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
	override async *indexScan(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): AsyncIterable<string> {
		for (const entry of await this.indexEntriesIn(lo, hi, direction)) yield entry.key;
	}

	/** indexScanRows with this transaction's uncommitted writes folded in, for
	 * the reason indexScan above does. */
	override async *indexScanRows(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): AsyncIterable<IndexEntry> {
		yield* await this.indexEntriesIn(lo, hi, direction);
	}

	private async indexEntriesIn(
		lo: string | undefined,
		hi: string | undefined,
		direction: 'asc' | 'desc',
	): Promise<IndexEntry[]> {
		const entries = await this.indexEntries(lo, hi);
		return direction === 'desc' ? entries.reverse() : entries;
	}

	/**
	 * Counted through this class's own pairs rather than the store's index, for
	 * the same reason indexScan above folds the buffer in: the count must agree
	 * with what the scan would yield. Same cost note applies — nothing on a hot
	 * path counts an index inside a write transaction.
	 */
	override async indexCount(lo?: string, hi?: string): Promise<number> {
		return (await this.indexEntries(lo, hi)).length;
	}

	override async indexSeek(
		lo?: string,
		hi?: string,
		direction: 'asc' | 'desc' = 'asc',
	): Promise<{ position: string; key: string } | undefined> {
		const entries = await this.indexEntries(lo, hi);
		const at = direction === 'desc' ? entries[entries.length - 1] : entries[0];
		return at === undefined ? undefined : { position: at.position, key: at.key };
	}

	/**
	 * The store's least key that this transaction has not deleted, against the
	 * least key it has buffered — whichever is lower. Stepping past a deleted
	 * key resumes at the next key above it, so a run of deletes costs one
	 * lookup each.
	 */
	override async seek(lo?: string, hi?: string): Promise<string | undefined> {
		let buffered: string | undefined;
		for (const k of this.pending.keys()) {
			if (lo !== undefined && k < lo) continue;
			if (hi !== undefined && k >= hi) continue;
			if (buffered === undefined || k < buffered) buffered = k;
		}
		let from = lo;
		let stored: string | undefined;
		for (;;) {
			stored = await super.seek(from, hi);
			if (stored === undefined || !this.deleted.has(stored)) break;
			// `stored + '\u0000'` is the least string above `stored`, so keys
			// that extend the deleted one ('a/b' → 'a/b0') stay in range; the
			// prefix successor would step over them.
			from = stored + '\u0000';
		}
		if (buffered === undefined) return stored;
		if (stored === undefined) return buffered;
		return buffered < stored ? buffered : stored;
	}

	/** The entries of a range, in position order, with this transaction's
	 * uncommitted writes folded in. */
	private async indexEntries(lo?: string, hi?: string): Promise<IndexEntry[]> {
		if (emptyRange(lo, hi)) return [];
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
		const entries: IndexEntry[] = [];
		for (const [ k, v ] of records) {
			for (const position of positionsOf(v)) {
				if ((lo === undefined || position >= lo) && (hi === undefined || position < hi)) {
					entries.push({ position, key: k, value: v });
				}
			}
		}
		entries.sort(comparePositions);
		return entries;
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
		this.note(key);
	}

	async del(key: string): Promise<void> {
		this.pending.delete(key);
		this.deleted.add(key);
		this.note(key);
	}

	/** Records the generations `key` belongs to, if any. Noted as the write is
	 * buffered rather than gathered at commit, so a commit walks the keys it is
	 * about to write and nothing else. */
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
		if (this.pending.size === 0 && this.deleted.size === 0) return;
		const tx = this.db.transaction(this.storeName, 'readwrite');
		const store = tx.objectStore(this.storeName);
		for (const [ k, v ] of this.pending) store.put(v, k);
		for (const k of this.deleted) store.delete(k);
		if (this.stampAt !== undefined) {
			const token = nextGenerationToken();
			// A transaction that deleted the generation key keeps it deleted — a
			// workspace purge takes the rows and their generation together, and an
			// absent token already means "unknown" to every reader.
			for (const at of this.stampAt) {
				if (!this.deleted.has(at)) store.put(token, at);
			}
		}
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
		this.stampAt = undefined;
	}
}
