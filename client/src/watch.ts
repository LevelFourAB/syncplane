import { type DescMessage, fromBinary, type MessageShape } from '@bufbuild/protobuf';

import { COMPLETE, type Coverage, type RowSet, withCoverage } from './coverage.js';
import { debugVerifyTouchesEnv } from './env.js';
import { wsSegment } from './keys.js';
import type { KVRead, KVStore } from './kv/store.js';
import type { EntityRegistry } from './mutations.js';
import type { Mutex } from './mutex.js';
import { sortOrderedFields } from './ordering.js';
import { type CompiledQuery, runQuery } from './query/index.js';
import { readMergedRowWithVersion, type Row, scanMergedRows, type ScopedRow } from './read.js';
import { hydrate, type IncludeSpec, type RefClient } from './refs.js';
import { safeInvoke } from './report.js';
import type { TouchSet } from './touch.js';

/**
 * Receives the full result after every commit that moves it. The array also
 * carries `coverage` — how much of the entity the local store holds — because
 * a watcher over a bounded working set can be complete for the view it drives
 * and still not be the whole entity.
 */
export type WatchCallback<T> = (rows: RowSet<T>) => void;

/** Options shared by `watch` and `watchQuery`. */
export interface WatchOptions {
	/** Hydrate result rows against this include tree. */
	include?: IncludeSpec;
	/**
	 * Called if this watch cannot be established at all — the store or hub
	 * failed to come up. It fires once and no rows ever arrive, which is the
	 * difference a caller cannot see otherwise: a watch that is merely slow and
	 * one that is dead both look like "no callback yet". The same failure also
	 * lands on `status().lastError` (code `watch_failed`), which is account-wide
	 * and so cannot say which watch died.
	 *
	 * An unregistered entity is NOT reported here — that throws synchronously
	 * from the call, as every other schema-first read does.
	 */
	onError?: (error: unknown) => void;
}

interface Watcher {
	schema: DescMessage;
	entity: string;
	/**
	 * Workspace this watcher observes; undefined means every workspace in the
	 * store (the cross-workspace read the entity-first key layout makes cheap).
	 */
	workspaceId?: string;
	cb: WatchCallback<unknown>;
	/** When set, rows are filtered/ordered/sliced before the callback fires. */
	query?: CompiledQuery;
	/** When set, result rows are hydrated against this include tree. */
	include?: IncludeSpec;
	/**
	 * Entities this watcher's hydration reaches (own entity ∪ every entity
	 * reachable through `include`). A commit touching any of them re-fires the
	 * watcher, so a change to an included target re-emits.
	 */
	reaches: ReadonlySet<string>;
	/**
	 * Memo of the last emitted query result (ordered keys + raw row bytes + row
	 * versions + the coverage stamped on it) used to suppress redundant
	 * callbacks. Versions are part of the memo because Row.version drives
	 * compare-and-swap: a version-only change (e.g. an optimistic create getting
	 * server-acked to byte-identical confirmed bytes) must still be delivered, or
	 * watchers keep a stale 0n and issue bad expectedVersion guards. Coverage is
	 * part of it because it rides on the result: an entity that finishes
	 * hydrating with no matching rows changes nothing BUT the coverage, and
	 * suppressing that emit is exactly what leaves a UI saying "loading" over a
	 * result it now knows to be empty. Only maintained for plain (non-include)
	 * query watchers; include watchers over-fire (a changed target row cannot be
	 * seen in the base scan, so they re-emit on every reaching touch).
	 */
	lastEmit?: { keys: string[]; bytes: Uint8Array[]; versions: bigint[]; coverage: Coverage };
	/**
	 * Read stamp of the newest snapshot this watcher has been dispatched (0 =
	 * none yet). Reads are ordered by the mutex, but the callback runs OUTSIDE
	 * it — and for an include watcher, after an async hydrate — so two ordered
	 * reads can still reach the callback in either order. Comparing stamps drops
	 * the older one instead of letting it overwrite a newer view, and (for query
	 * watchers) instead of letting it poison `lastEmit` so the correcting emit is
	 * suppressed until the rows happen to move again.
	 */
	emittedAt: number;
}

/** One row of a scope snapshot, held against the bytes it was decoded from. */
interface SnapshotRow {
	/**
	 * The raw key suffix `scanMergedRows` sorts by — `<wsSegment>/<key>` for a
	 * cross-workspace scan, `<key>` for a scoped one. Kept per row because the
	 * incremental path has to splice new rows into exactly the position a full
	 * scan would have put them in.
	 */
	suffix: string;
	bytes: Uint8Array;
	row: Row<unknown>;
}

/**
 * The authoritative local view of one scan scope: every row a full
 * `scanMergedRows` would return, in the order it would return them.
 *
 * It is a cache only in the sense that it can always be rebuilt by scanning;
 * between commits it is maintained in place from the row keys the commit
 * reported, which is what keeps a keystroke-sized mutation from costing a full
 * entity read.
 */
interface ScopeSnapshot {
	byId: Map<string, SnapshotRow>;
	/** Row ids in `suffix` order — exactly the order scanMergedRows yields. */
	ordered: string[];
}

/**
 * Past this many touched rows, AND this fraction of the snapshot, one prefix
 * scan beats the point reads: each touched key costs two `tx.get`s
 * (readMergedRowWithVersion), while a scan amortizes the cursor over the whole
 * entity. The absolute floor keeps a small commit on the incremental path even
 * when the snapshot itself is tiny.
 */
const RESCAN_MIN_KEYS = 8;
const RESCAN_FRACTION = 0.25;

/** What one scan scope must do about a committed touch set. */
type ScopePlan =
	| { rescan: true }
	| { rescan: false; keys: { workspaceId: string; key: string }[] };

export interface WatcherHubOptions {
	/**
	 * Runs a full scan alongside every incremental update and throws if the two
	 * disagree — a self-check on the producers, not a production path.
	 *
	 * Incremental notification makes an incomplete touch set a CORRECTNESS bug:
	 * before it, a producer that forgot to report a row merely paid for a scan
	 * that healed it anyway; now the row silently stays stale in every watcher.
	 * Turning this on across a test suite is what proves no producer
	 * under-reports. Defaults to the SYNC_DEBUG_VERIFY_TOUCHES environment
	 * variable (see env.debugVerifyTouchesEnv), so a whole run can be put under
	 * the check without touching any construction site.
	 */
	debugVerifyTouches?: boolean;
	/**
	 * Reports how much of an entity the local store holds, so every emitted
	 * result can carry it. Absent ⇒ complete, which is what a hub built without
	 * a subscription set (tests, in-process facades) should say.
	 */
	coverageOf?: (entity: string, workspaceId?: string) => Coverage;
}

/**
 * Scan-based reactivity: after any commit that touches an entity, every
 * watcher that REACHES that entity (its own entity, or — for an include
 * watcher — any entity its references reach) is invoked with the full
 * (optionally query-filtered, optionally hydrated) row list of the merged view.
 *
 * The row list comes from a per-scope snapshot (see ScopeSnapshot). A commit
 * that names its rows patches that snapshot in place; one that only names an
 * entity rebuilds it by scanning. So the cost of observing tracks the size of
 * the CHANGE, not the size of the entity — which is what makes a working set
 * of tens of thousands of rows watchable at all.
 *
 * Decoded values and Row wrappers are shared between same-entity watchers AND
 * reused across commits, so callbacks must not mutate the rows they receive — a
 * mutation would leak into every later emit, not just the current one.
 */
export class WatcherHub {
	private watchers = new Set<Watcher>();
	private closed = false;
	/** Set by the client so include watchers can hydrate; absent ⇒ no hydration. */
	private refClient?: RefClient;
	/** Live snapshot per scan scope; dropped when the scope's last watcher goes. */
	private snapshots = new Map<string, ScopeSnapshot>();
	/** Monotonic stamp minted inside each read section; see Watcher.emittedAt. */
	private readSeq = 0;
	private readonly verifyTouches: boolean;

	constructor(
		private kv: KVStore,
		private registry: EntityRegistry,
		// Shared with the writers so a re-scan reads a snapshot consistent with
		// (never torn by) a concurrent commit. Must be the same instance writers
		// take exclusively.
		private mutex: Mutex,
		private opts: WatcherHubOptions = {},
	) {
		this.verifyTouches = opts.debugVerifyTouches ?? debugVerifyTouchesEnv();
	}

	/** Installs the resolver used to hydrate include watchers. */
	setRefClient(client: RefClient): void {
		this.refClient = client;
	}

	/** Drops all watchers; later watch() calls are no-ops, notify() is inert. */
	close(): void {
		this.closed = true;
		this.watchers.clear();
		this.snapshots.clear();
	}

	watch<D extends DescMessage>(
		schema: D,
		workspaceId: string | undefined,
		cb: WatchCallback<MessageShape<D>>,
		query?: CompiledQuery,
		include?: IncludeSpec,
	): () => void {
		if (this.closed) return () => {};
		const def = this.registry.bySchema(schema);
		const reaches = this.reachableEntities(def.name, include);
		const watcher: Watcher = {
			schema,
			entity: def.name,
			workspaceId,
			cb: cb as WatchCallback<unknown>,
			query,
			include,
			reaches,
			emittedAt: 0,
		};
		this.watchers.add(watcher);
		// Emit the initial snapshot asynchronously (also seeds the query memo).
		void this.emitTo(watcher);
		return () => {
			this.watchers.delete(watcher);
			this.pruneSnapshot(scanScope(watcher));
		};
	}

	/** Drops a scope's snapshot once no watcher needs that scan. */
	private pruneSnapshot(scope: string): void {
		for (const w of this.watchers) {
			if (scanScope(w) === scope) return;
		}
		this.snapshots.delete(scope);
	}

	/**
	 * Notifies every watcher a committed touch set reaches. The set carries
	 * (workspace, entity) pairs — with the changed row keys where the writer knew
	 * them — so a commit in one workspace never wakes a watcher bound to another,
	 * while a cross-workspace watcher is woken by any of them.
	 */
	async notify(touched: TouchSet): Promise<void> {
		if (this.closed) return;
		const pairs = [ ...touched.pairs() ];
		const toNotify: Watcher[] = [];
		// One update per distinct (entity, scope) pair the notified watchers need;
		// `undefined` scope is the cross-workspace scan, which is a different scan
		// from any single workspace's.
		const scopes = new Map<string, Watcher>();
		for (const w of this.watchers) {
			if (!reaches(w, pairs)) continue;
			toNotify.push(w);
			const scope = scanScope(w);
			if (!scopes.has(scope)) scopes.set(scope, w);
		}
		if (toNotify.length === 0) return;
		// One read tx per notify, shared across scopes and watchers. Held under a
		// shared lock so a concurrent commit cannot tear the reads — the point
		// reads of the incremental path need that just as much as a scan does.
		// Hydration (which takes its own shared reads) runs AFTER this tx closes,
		// never nested inside it.
		const snaps = new Map<string, ScopeSnapshot>();
		const at = await this.mutex.runShared(async () => {
			const tx = await this.kv.read();
			try {
				for (const [ scope, sample ] of scopes) {
					snaps.set(scope, await this.updateScope(tx, sample, pairs));
				}
				// Stamped inside the section: the order the mutex grants reads in
				// IS the order of the views they saw.
				return ++this.readSeq;
			} finally {
				tx.release();
			}
		});
		for (const w of toNotify) {
			await this.dispatch(w, snaps.get(scanScope(w))!, at);
		}
	}

	private async emitTo(watcher: Watcher): Promise<void> {
		if (!this.watchers.has(watcher)) return;
		const [ snap, at ] = await this.mutex.runShared(async () => {
			const tx = await this.kv.read();
			try {
				return [ await this.scanEntity(tx, watcher), ++this.readSeq ] as const;
			} finally {
				tx.release();
			}
		});
		await this.dispatch(watcher, snap, at);
	}

	/**
	 * Brings one scope's snapshot up to date with a committed touch set, either
	 * by patching the rows the commit named or by rescanning.
	 */
	private async updateScope(
		tx: KVRead,
		watcher: Watcher,
		pairs: readonly TouchedPair[],
	): Promise<ScopeSnapshot> {
		const scope = scanScope(watcher);
		const snap = this.snapshots.get(scope);
		const plan = planScope(watcher, snap, pairs);
		if (plan.rescan || snap === undefined) return this.scanEntity(tx, watcher);
		for (const { workspaceId, key } of plan.keys) {
			const id = rowId(workspaceId, key);
			const suffix = suffixOf(watcher, workspaceId, key);
			const prev = snap.byId.get(id);
			const merged = await readMergedRowWithVersion(
				tx,
				this.registry,
				watcher.entity,
				workspaceId,
				key,
			);
			if (merged === undefined) {
				// Compute the position BEFORE dropping the row: the binary search
				// resolves suffixes through byId.
				if (prev === undefined) continue;
				const at = lowerBound(snap, suffix);
				if (snap.ordered[at] === id) snap.ordered.splice(at, 1);
				snap.byId.delete(id);
				continue;
			}
			const row: ScopedRow = { v: merged.v, version: merged.version, workspaceId, key };
			if (prev === undefined) snap.ordered.splice(lowerBound(snap, suffix), 0, id);
			snap.byId.set(id, snapshotRow(watcher.schema, prev, suffix, row));
		}
		if (this.verifyTouches) await this.verifyScope(tx, watcher, snap);
		return snap;
	}

	/** Rebuilds a scope's snapshot from a full merged-view scan. */
	private async scanEntity(tx: KVRead, watcher: Watcher): Promise<ScopeSnapshot> {
		const merged = await scanMergedRows(tx, this.registry, watcher.entity, watcher.workspaceId);
		const scope = scanScope(watcher);
		const prev = this.snapshots.get(scope);
		const byId = new Map<string, SnapshotRow>();
		const ordered: string[] = [];
		for (const row of merged) {
			const id = rowId(row.workspaceId, row.key);
			ordered.push(id);
			byId.set(
				id,
				snapshotRow(
					watcher.schema,
					prev?.byId.get(id),
					suffixOf(watcher, row.workspaceId, row.key),
					row,
				),
			);
		}
		const snap: ScopeSnapshot = { byId, ordered };
		this.snapshots.set(scope, snap);
		return snap;
	}

	/**
	 * Asserts a patched snapshot still equals what a full scan would produce.
	 * Only reachable under debugVerifyTouches; a divergence means some commit
	 * path changed a row without reporting it.
	 */
	private async verifyScope(tx: KVRead, watcher: Watcher, snap: ScopeSnapshot): Promise<void> {
		const merged = await scanMergedRows(tx, this.registry, watcher.entity, watcher.workspaceId);
		const fail = (why: string): never => {
			throw new Error(
				`incremental watch snapshot diverged for scope ${scanScope(watcher)}: ${why}`
					+ ' — a commit path changed a row without reporting it (see TouchSet)',
			);
		};
		if (merged.length !== snap.ordered.length) {
			fail(`${snap.ordered.length} rows held, ${merged.length} in the store`);
		}
		for (let i = 0; i < merged.length; i++) {
			const want = merged[i]!;
			const have = snap.byId.get(snap.ordered[i]!);
			if (
				have === undefined
				|| have.row.key !== want.key
				|| have.row.workspaceId !== want.workspaceId
				|| have.row.version !== want.version
				|| !bytesEqual(have.bytes, want.v)
			) {
				fail(`row ${i} is ${have?.row.key ?? '<missing>'}, expected ${want.key}`);
			}
		}
	}

	private async dispatch(watcher: Watcher, snap: ScopeSnapshot, at: number): Promise<void> {
		if (!this.watchers.has(watcher) || at <= watcher.emittedAt) return;
		// Claimed before the callback (and before any hydrate), so a dispatch that
		// starts while this one is still hydrating knows it is the newer of the two.
		watcher.emittedAt = at;
		const rows = snap.ordered.map((id) => snap.byId.get(id)!.row);
		const coverage = this.opts.coverageOf?.(watcher.entity, watcher.workspaceId) ?? COMPLETE;
		if (!watcher.query) {
			// Plain watchers keep the original always-fire, full-array semantics.
			safeInvoke(watcher.cb, withCoverage(rows, coverage), 'watch callback');
			return;
		}
		const result = runQuery(rows, watcher.query, watcher.schema);
		if (watcher.include && this.refClient) {
			// Hydrate after the scan tx closed (own shared reads). Over-fire: a
			// changed target row is invisible to the base scan, so include watchers
			// re-emit on every reaching touch rather than memo-suppressing.
			const hydrated = await hydrate(
				this.refClient,
				this.registry,
				watcher.entity,
				result as Row<never>[],
				watcher.include,
			);
			// A newer dispatch may have claimed the watcher (and already fired)
			// while this one hydrated; its rows win.
			if (!this.watchers.has(watcher) || watcher.emittedAt !== at) return;
			safeInvoke(watcher.cb, withCoverage(hydrated, coverage), 'watch callback');
			return;
		}
		const keys = result.map((r) => rowId(r.workspaceId, r.key));
		// Non-null: every result row came out of the snapshot.
		const bytes = keys.map((k) => snap.byId.get(k)!.bytes);
		const versions = result.map((r) => r.version);
		if (watcher.lastEmit && sameEmit(watcher.lastEmit, keys, bytes, versions, coverage)) return;
		watcher.lastEmit = { keys, bytes, versions, coverage };
		safeInvoke(watcher.cb, withCoverage(result, coverage), 'watch callback');
	}

	/**
	 * Entities a watcher reaches: its own, plus every entity reachable by
	 * walking the include tree (forward target / reverse source) at each level.
	 */
	private reachableEntities(entity: string, include?: IncludeSpec): Set<string> {
		const out = new Set<string>([ entity ]);
		const walk = (from: string, spec?: IncludeSpec): void => {
			if (!spec) return;
			for (const [ relation, nested ] of Object.entries(spec)) {
				const fwd = this.registry.forwardRef(from, relation);
				const next = fwd ? fwd.targetEntity : this.registry.reverseRef(from, relation)?.sourceEntity;
				if (next === undefined) continue;
				out.add(next);
				if (nested !== true) walk(next, nested);
			}
		};
		walk(entity, include);
		return out;
	}
}

/** One (workspace, entity) pair a commit touched; `keys` undefined = wildcard. */
interface TouchedPair {
	workspaceId: string;
	entity: string;
	keys?: ReadonlySet<string>;
}

/**
 * Decides how a scope absorbs a touch set: patch the named rows, or rescan.
 *
 * Rescanning is required when there is no snapshot to patch, when any touching
 * pair is a wildcard, or when so many rows moved that point reads stop paying
 * off. An empty key list is the cheapest case of all: the commit reached this
 * watcher only through an INCLUDED entity, so the scope's own rows are untouched
 * and the snapshot already answers.
 */
function planScope(
	watcher: Watcher,
	snap: ScopeSnapshot | undefined,
	pairs: readonly TouchedPair[],
): ScopePlan {
	if (snap === undefined) return { rescan: true };
	const keys: { workspaceId: string; key: string }[] = [];
	for (const pair of pairs) {
		if (pair.entity !== watcher.entity) continue;
		if (watcher.workspaceId !== undefined && watcher.workspaceId !== pair.workspaceId) continue;
		if (pair.keys === undefined) return { rescan: true };
		for (const key of pair.keys) keys.push({ workspaceId: pair.workspaceId, key });
	}
	if (keys.length > RESCAN_MIN_KEYS && keys.length > snap.byId.size * RESCAN_FRACTION) {
		return { rescan: true };
	}
	return { rescan: false, keys };
}

/** Index of the first `ordered` entry whose suffix is not below `suffix`. */
function lowerBound(snap: ScopeSnapshot, suffix: string): number {
	let lo = 0;
	let hi = snap.ordered.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		// Plain string comparison, matching how scanMergedRows sorts its suffixes.
		if (snap.byId.get(snap.ordered[mid]!)!.suffix < suffix) lo = mid + 1;
		else hi = mid;
	}
	return lo;
}

/**
 * Builds a snapshot row, reusing the previous one where the bytes held.
 * Identical bytes decode to an identical value, and the comparison is far
 * cheaper than the decode it avoids — cheaper too than trusting the row
 * version, which would serve wrong data on a stale hit.
 *
 * The Row wrapper is reused as well, but only when the version also held:
 * version can move without the bytes moving (an optimistic create acked to
 * byte-identical confirmed bytes), and that difference is exactly what a
 * compare-and-swap guard reads.
 */
function snapshotRow(
	schema: DescMessage,
	prev: SnapshotRow | undefined,
	suffix: string,
	row: ScopedRow,
): SnapshotRow {
	if (prev !== undefined && bytesEqual(prev.bytes, row.v)) {
		if (prev.row.version === row.version) return prev;
		return { suffix, bytes: row.v, row: { ...prev.row, version: row.version } };
	}
	const value = decodeRow(schema, row.v);
	return {
		suffix,
		bytes: row.v,
		row: { key: row.key, value, workspaceId: row.workspaceId, version: row.version },
	};
}

function decodeRow(schema: DescMessage, bytes: Uint8Array): unknown {
	const value = fromBinary(schema, bytes);
	sortOrderedFields(schema, value);
	return value;
}

/**
 * The key suffix a row sorts by inside one watcher's scan — the part of the KV
 * key that follows the scan prefix, so it is byte-for-byte what scanMergedRows
 * ordered on.
 */
function suffixOf(watcher: Watcher, workspaceId: string, key: string): string {
	return watcher.workspaceId === undefined ? `${wsSegment(workspaceId)}/${key}` : key;
}

/**
 * Identity of a scan a watcher needs: its entity plus its workspace scope
 * (`*` for the cross-workspace scan). Two watchers sharing this share a scan.
 */
function scanScope(watcher: Watcher): string {
	return `${watcher.entity}\u001f${watcher.workspaceId ?? '*'}`;
}

/** Identity of a row inside one scan result. */
function rowId(workspaceId: string, key: string): string {
	return `${workspaceId}\u001f${key}`;
}

function reaches(watcher: Watcher, touched: readonly TouchedPair[]): boolean {
	for (const t of touched) {
		if (!watcher.reaches.has(t.entity)) continue;
		// A reference followed by `include` may point at a row in the same
		// workspace only (references never cross the partition), so a scoped
		// watcher matches only its own workspace.
		if (watcher.workspaceId === undefined || watcher.workspaceId === t.workspaceId) return true;
	}
	return false;
}

function sameEmit(
	last: { keys: string[]; bytes: Uint8Array[]; versions: bigint[]; coverage: Coverage },
	keys: string[],
	bytes: Uint8Array[],
	versions: bigint[],
	coverage: Coverage,
): boolean {
	if (!sameCoverage(last.coverage, coverage)) return false;
	if (last.keys.length !== keys.length) return false;
	for (let i = 0; i < keys.length; i++) {
		const a = last.bytes[i];
		const b = bytes[i];
		if (
			last.keys[i] !== keys[i]
			|| last.versions[i] !== versions[i]
			|| a === undefined
			|| b === undefined
			|| !bytesEqual(a, b)
		) {
			return false;
		}
	}
	return true;
}

/**
 * Whether two coverage reports say the same thing. Bounds are accumulated in a
 * fixed order by one function (coverageOfEntity), so comparing them positionally
 * is exact rather than merely cheap.
 */
function sameCoverage(a: Coverage, b: Coverage): boolean {
	if (a === b) return true;
	if (a.complete !== b.complete || a.limit !== b.limit) return false;
	if (a.bounds.length !== b.bounds.length) return false;
	return a.bounds.every((bound, i) => bound === b.bounds[i]);
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a === b) return true;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}
