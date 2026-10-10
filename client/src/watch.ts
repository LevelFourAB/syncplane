import { type DescMessage, fromBinary, type MessageShape } from '@bufbuild/protobuf';

import {
	COMPLETE,
	type Coverage,
	type PageInfo,
	type RelCoverageRef,
	type RowSet,
	withCoverage,
} from './coverage.js';
import { debugVerifyTouchesEnv } from './env.js';
import { compareWorkspaces, wsSegment } from './keys.js';
import type { KVRead, KVStore } from './kv/store.js';
import { type EntityRegistry, refQualified, refSegmentOf } from './mutations.js';
import type { Mutex } from './mutex.js';
import type { CompiledIndex } from './optimistic/index-maintain.js';
import { sortOrderedFields } from './ordering.js';
import type { Cond, PageWindow } from './query/ast.js';
import { OrderCursors, pageWindowOf } from './query/cursor.js';
import {
	type IndexedRow,
	type IndexReport,
	queryByIndex,
	type RowGroupPage,
} from './query/execute.js';
import { readGroups, readRowGroups, sameGroups, sameRowGroups } from './query/group-read.js';
import {
	collectRels,
	compareRows,
	type CompiledQuery,
	computeRelKeys,
	diffRelKeys,
	evalCond,
	FacetBuckets,
	type FacetTable,
	type Group,
	MAX_INDEX_PROBES,
	type RelCond,
	type RelKeys,
	type RelKeysDelta,
	type RelNode,
	type RelPassCache,
	relReplacement,
	relsArePositive,
	relTupleKey,
	rewriteRels,
	rewriteWithKeys,
	type RowGroup,
	runQuery,
	sameFacets,
	sameRelKeys,
	stubKeyTuples,
	type TouchedTarget,
	updateRelKeys,
} from './query/index.js';
import type { PreparedGroupQuery, PreparedGroupRowsQuery } from './query/wire.js';
import {
	type MergedRow,
	readMergedRowsWithVersion,
	type ReadScope,
	type Row,
	rowOf,
	scanMergedRows,
	type ScopedRow,
	type WorkspaceScope,
	workspacesWithRows,
} from './read.js';
import { hydrate, type Include, type RefClient } from './refs.js';
import { reportError, safeInvoke } from './report.js';
import type { QueryDeclineReason, UnstampedQueryEvent } from './sync/events.js';
import { queryEventOf } from './sync/events.js';
import type { TouchSet } from './touch.js';

/**
 * Receives the delivered page after every commit that moves it, and how it
 * moved.
 *
 * The array carries `coverage` — how much of the entity the local store holds,
 * because a watcher over a bounded working set can be complete for the view it
 * drives and still not be the whole entity — and `total`, the matches behind a
 * page cut by `offset`/`limit`.
 *
 * `delta` describes the same change as an edit script against the PREVIOUS
 * array, for a consumer maintaining its own list. Ignoring it is always
 * correct; the array is the whole answer either way.
 */
export type WatchCallback<
	T,
	Total extends number | undefined = number,
	Facets extends FacetTable | undefined = undefined,
> = (
	rows: RowSet<T, Total, Facets>,
	delta: WatchDelta<T>,
) => void;

/** The page of a result a watch delivers. Both bounds are row counts. */
export interface WatchWindow {
	/**
	 * Matching rows to skip.
	 * @defaultValue 0
	 */
	offset?: number;
	/** Rows to deliver at most. Absent delivers everything from `offset` on. */
	limit?: number;
}

/**
 * How the delivered rows changed since the previous callback.
 *
 * `splice` carries an edit script: apply the changes in order to a copy of the
 * previous array and it becomes the current one. `reset` means the previous
 * array cannot be transformed into this one cheaply and should be replaced —
 * the first callback, a window move, a commit that reported an entity without
 * its rows, and every callback on a watch with an `offset` or a `limit`
 * (translating match-set positions across a page boundary is not worth it when
 * the page is short enough to compare directly).
 */
export type WatchDelta<T> =
	| { kind: 'reset' }
	| { kind: 'splice'; changes: readonly RowSplice<T>[] };

/** One edit in a `splice` delta, at an index into the array being transformed. */
export interface RowSplice<T> {
	/** `update` replaces the row at `index`; the other two shift what follows. */
	op: 'insert' | 'remove' | 'update';
	index: number;
	/** The row inserted, removed, or put in place. */
	row: Row<T>;
}

/**
 * Controls a live watch.
 *
 * Every method is safe to call before the first callback arrives, and after
 * `close`.
 */
export interface WatchHandle {
	/** Stops delivering rows. Idempotent. */
	close(): void;
	/**
	 * Moves the delivered page over the result the watch already holds, and
	 * re-delivers it.
	 *
	 * Costs a slice, not a read: the rows are held in result order, so paging
	 * through a large result does not touch the store. A move that lands on the
	 * same rows delivers nothing. Fields left out are cleared, so `setWindow({})`
	 * delivers the whole result.
	 */
	setWindow(window: WatchWindow): void;
}

/** A handle to a watch that was never established (a closed client). */
const INERT: WatchHandle = Object.freeze({ close: () => {}, setWindow: () => {} });

/**
 * What one keyed-watch callback carries: the watched keys whose row moved, each
 * mapped to its current row, or to `undefined` where the key names no row in
 * the local store.
 *
 * Every callback is a MERGE into what the consumer already holds rather than a
 * replacement: a key is carried when it is first read and whenever it moves
 * after that, so a key absent from the map has not changed.
 */
export type KeyedRows<T> = ReadonlyMap<string, Row<T> | undefined>;

/**
 * Receives the watched keys that moved, together with how much of the entity
 * the local store holds.
 *
 * The coverage is the entity's, not the key's: while `hydrating` is among its
 * bounds an absent row means "not here yet" rather than "no such row". A
 * coverage change alone fires a callback, with an empty map.
 */
export type KeyedWatchCallback<T> = (rows: KeyedRows<T>, coverage: Coverage) => void;

/**
 * Controls a live keyed watch.
 *
 * Both methods are safe to call before the first callback arrives, and after
 * `close`.
 */
export interface KeyedWatchHandle {
	/** Stops delivering rows. Idempotent. */
	close(): void;
	/**
	 * Replaces the watched key set.
	 *
	 * Keys already watched keep the rows they hold and are not re-read; new ones
	 * are read in one batch and delivered on their own callback, so growing the
	 * set costs a point read per added key and nothing per key already in it.
	 * Dropped keys stop being reported immediately.
	 */
	setKeys(keys: Iterable<string>): void;
}

/**
 * Names one row of an entity: the workspace it lives in, and its key within
 * that workspace. A bare key does not identify a row outside a workspace scope.
 */
export interface RowRef {
	workspaceId: string;
	key: string;
}

/** One ref a cross-workspace keyed watch reports, with the row it now names. */
export interface ScopedKeyedRow<T> extends RowRef {
	/** Undefined where the ref names no row in the local store. */
	row: Row<T> | undefined;
}

/**
 * What one cross-workspace keyed-watch callback carries: the watched refs whose
 * row moved, each with its current row.
 *
 * Merges into what the consumer holds, as `KeyedRows` does: a ref is carried
 * when it is first read and whenever it moves after that, so a ref absent from
 * the batch has not changed. Order is unspecified.
 */
export type ScopedKeyedRows<T> = readonly ScopedKeyedRow<T>[];

/**
 * Receives the watched refs that moved, together with how much of the entity
 * the local store holds across every workspace — complete only where it is
 * complete in each of them (see KeyedWatchCallback for what the coverage says
 * about an absent row).
 */
export type ScopedKeyedWatchCallback<T> = (
	rows: ScopedKeyedRows<T>,
	coverage: Coverage,
) => void;

/**
 * Controls a live cross-workspace keyed watch.
 *
 * Both methods are safe to call before the first callback arrives, and after
 * `close`.
 */
export interface ScopedKeyedWatchHandle {
	/** Stops delivering rows. Idempotent. */
	close(): void;
	/** Replaces the watched ref set; see KeyedWatchHandle.setKeys for the cost. */
	setRefs(refs: Iterable<RowRef>): void;
}

/** A handle to a keyed watch that was never established (a closed client). */
const INERT_KEYED: ScopedKeyedWatchHandle = Object.freeze({
	close: () => {},
	setRefs: () => {},
});

/** Options for `watchRows`. A keyed watch has no result order to page through. */
export interface KeyedWatchOptions {
	/** Hydrate delivered rows against this include tree. */
	include?: Include;
	/** Called if this watch cannot be established at all; see WatchOptions.onError. */
	onError?: (error: unknown) => void;
}

/** Options shared by `watch` and `watchQuery`. */
export interface WatchOptions {
	/** Hydrate result rows against this include tree. */
	include?: Include;
	/**
	 * Page to deliver, in place of any `offset`/`limit` the query carries.
	 *
	 * The difference is that this one moves: the match set behind it is
	 * maintained whole, so WatchHandle.setWindow re-pages it without a read,
	 * while a bound compiled into the query is fixed for the life of the watch.
	 */
	window?: WatchWindow;
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

/**
 * One row a watcher holds, against the bytes it was decoded from.
 *
 * Rows are shared between the watchers of a scan scope and reused across
 * commits, so object identity means "this row has not moved since you last saw
 * it" — which is what the emit memo tests, and what lets the React bridge
 * re-render only the components whose own row changed.
 */
interface ResultRow {
	/** Identity within a scan scope (see rowId). */
	id: string;
	/**
	 * The key suffix a scan sorts by — `<wsSegment>/<key>` for a cross-workspace
	 * scan, `<key>` for a scoped one. It is the final tie-break of every result
	 * order (see comparatorFor), so a maintained result can binary-search to the
	 * position a scan would have put the row in.
	 */
	suffix: string;
	bytes: Uint8Array;
	row: Row<unknown>;
}

/**
 * The rows one watcher's query matches, in result order and BEFORE its offset
 * and limit — the full match set, not the page that gets emitted.
 *
 * Keeping all of the matches rather than the page is what spares `limit` a read
 * of its own: deleting a row from a full page pulls the next one in from
 * `ordered` instead of going back to the store to find out what it is.
 *
 * The cost is that memory tracks the MATCH set. A narrow query over a large
 * entity holds little; one with no predicate holds an entry per entity row,
 * which is the remaining O(entity) case.
 */
interface MatchSet {
	byId: Map<string, ResultRow>;
	ordered: ResultRow[];
	/**
	 * The facet counts of `ordered`, kept in step with it: a row spliced in is
	 * added, one spliced out removed, so a commit costs the rows it moved rather
	 * than a fold over the set. Undefined for a query naming no facet.
	 */
	facets?: FacetBuckets;
}

interface Watcher {
	schema: DescMessage;
	entity: string;
	/**
	 * Workspace this watcher observes; undefined means every workspace in the
	 * store (the cross-workspace read the entity-first key layout makes cheap),
	 * unless `workspaces` narrows it to a named set.
	 */
	workspaceId?: string;
	/**
	 * The workspaces a set-scoped watcher spans, in key-segment order; undefined
	 * for a watcher over one workspace or over all of them.
	 *
	 * A set naming exactly one workspace ALSO sets `workspaceId`, which is what
	 * puts it on the scoped read, scan-scope and index paths without a case of
	 * its own.
	 */
	workspaces?: readonly string[];
	/**
	 * The scan scope this watcher draws on (see scanScope). It is set once at
	 * registration, because the entity and workspaces it comes from do not
	 * change after that.
	 */
	scope: string;
	cb: WatchCallback<unknown>;
	/** When set, rows are filtered/ordered/sliced before the callback fires. */
	query?: CompiledQuery;
	/** Per-workspace predicate; see NamedWorkspaces.predicates. */
	predicates?: ReadonlyMap<string, Cond>;
	/**
	 * The query's cursor bounds. They bound the MATCH SET — a row outside them
	 * is not a match — so `total` counts what is inside them and `window` slices
	 * within that. Undefined when the query carries no cursors.
	 */
	bounds?: PageWindow;
	/** Mints the positions each emitted page reports. Undefined for a
	 * cross-workspace watcher, whose rows have no single ordering to name a
	 * position in. */
	cursors?: OrderCursors;
	/** When set, result rows are hydrated against this include tree. */
	include?: Include;
	/**
	 * Entities this watcher's hydration reaches (own entity ∪ every entity
	 * reachable through `include`). A commit touching any of them re-fires the
	 * watcher, so a change to an included target re-emits.
	 */
	reaches: ReadonlySet<string>;
	/**
	 * Entities among `reaches` a commit in ANY workspace wakes this watcher
	 * for: those reached through a workspace-qualified reference, whose rows
	 * may sit outside the watcher's own scope.
	 */
	anywhere?: ReadonlySet<string>;
	/** The total order `matches.ordered` is in; see comparatorFor. */
	compare: (a: ResultRow, b: ResultRow) => number;
	/**
	 * The page delivered out of `matches`. Seeded from the query's own
	 * offset/limit and moved by WatchHandle.setWindow, which is why it lives here
	 * and not in `query` — the match set is maintained whole, and the page is a
	 * slice taken at emit time.
	 */
	window: WatchWindow;
	/**
	 * Edits applied to `matches` since the last delivered callback, or 'reset'
	 * when no edit script describes the move.
	 *
	 * It accumulates rather than being rebuilt per commit because a callback can
	 * be dropped (a newer dispatch beat it), and the consumer is then behind by
	 * everything since the last one that actually landed.
	 */
	pending: RowSplice<unknown>[] | 'reset';
	/** Rows the query matches, maintained in place; undefined until first read. */
	matches?: MatchSet;
	/**
	 * The exact rows handed to the last callback. Compared by identity to catch
	 * the emits a changed-index test cannot rule out on its own — a rebuild that
	 * landed on the same page, or two dispatches racing to deliver one commit.
	 */
	lastWindow?: ResultRow[];
	/**
	 * The array handed to the last callback, before hydration. The next page is
	 * built from it rather than mapped out of `matches` (see `dirtyFrom`), so a
	 * delivery costs a copy plus the disturbed tail instead of a pass over the
	 * whole result.
	 */
	lastRows?: Row<unknown>[];
	/**
	 * Lowest result index disturbed since the last DELIVERED callback (Infinity
	 * for none), which is the index from which `lastWindow`/`lastRows` stop
	 * describing the current match set — everything before it is the same
	 * ResultRow the consumer already holds.
	 *
	 * It accumulates across dispatches for the reason `pending` does: a dispatch
	 * superseded by a newer one never delivers, and the rows it disturbed are
	 * still undelivered when the next one gets there.
	 */
	dirtyFrom: number;
	/**
	 * Matches behind the last emitted page. Like coverage, it rides on the result
	 * and so moves it: a row entering or leaving the match set past a full page
	 * changes nothing a reader can see EXCEPT this, and suppressing that emit
	 * leaves a scroller sized for a result that no longer exists.
	 */
	lastTotal?: number;
	/**
	 * Facets of the last emitted page, for the same reason as `lastTotal`: a
	 * row past a full page changing its value on an axis moves nothing a
	 * reader can see except these.
	 */
	lastFacets?: FacetTable;
	/**
	 * Coverage of the last emit. It rides on the result, so it moves the result:
	 * an entity that finishes hydrating with no matching rows changes nothing
	 * BUT this, and suppressing that emit is what leaves a UI saying "loading"
	 * over a result it now knows to be empty.
	 */
	lastCoverage?: Coverage;
	/**
	 * Read stamp of the newest view this watcher has been dispatched (0 = none
	 * yet). Reads are ordered by the mutex, but the callback runs OUTSIDE it —
	 * and for an include watcher, after an async hydrate — so two ordered reads
	 * can still reach the callback in either order. Comparing stamps drops the
	 * older one instead of letting it overwrite a newer view.
	 *
	 * It gates the CALLBACK only. The match set is updated inside the read
	 * section, where the mutex already orders it: dropping a delta there would
	 * lose a row change rather than merely deliver it late.
	 */
	emittedAt: number;
	/** Set when the query crosses references (`matches`); see RelWatchState. */
	rel?: RelWatchState;
}

/**
 * State of a watcher whose query crosses references. Such a query is not
 * row-local: a commit to a TARGET entity moves which base rows match without
 * touching them, so the watcher re-runs the pre-pass on those commits
 * (refreshRel) and rebuilds its match set when the membership keys moved —
 * and only then, which is what the sameRelKeys memo buys.
 */
interface RelWatchState {
	/** Every rel node of the query and its per-workspace predicates. */
	nodes: RelNode[];
	/** Wire names of the target entities, for reach tests. Includes the
	 * watcher's own entity for a self-reference. */
	targets: ReadonlySet<string>;
	/**
	 * The targets of workspace-qualified references among `targets`: their
	 * rows may sit in any workspace the client holds, so a commit to one
	 * reaches the watcher whatever workspace it lands in.
	 */
	anywhere: ReadonlySet<string>;
	/** The crossed refs in the form the coverage rule consumes. */
	coverage: readonly RelCoverageRef[];
	/** Matching target keys from the last pre-pass; unset before the first. */
	keys?: RelKeys;
	/** Rewritten per-workspace predicates, memoized against `keys` and cleared
	 * whenever they move. */
	whereByWs: Map<string, Cond | undefined>;
}

/**
 * A watcher over an explicit set of row keys rather than a result.
 *
 * It holds no order, no page and no total, and it never scans: keys are read as
 * point lookups when they join the set, and a commit re-reads only the ones it
 * named. A query-less Watcher answers the same question by deriving the whole
 * entity, which is what this exists to avoid.
 */
interface KeyWatcher {
	schema: DescMessage;
	entity: string;
	/**
	 * The workspace every watched ref lives in, or undefined for a watch whose
	 * refs may name any of them. It is only ever the scan scope and the touch
	 * filter — each ref carries its own workspace regardless.
	 */
	workspaceId?: string;
	/** The scan scope this watcher draws on; see Watcher.scope. */
	scope: string;
	/**
	 * The watched refs, by row id (see rowId). Every map below is keyed the same
	 * way, so a cross-workspace watcher holding the same key in two workspaces
	 * keeps them apart.
	 */
	refs: Map<string, RowRef>;
	/** Refs awaiting their first read; flushed as one batch (see scheduleAdds). */
	added: Set<string>;
	/** Whether a flush of `added` is already scheduled. */
	flushing: boolean;
	/** The rows behind the watched refs. A watched ref absent here has no row. */
	held: Map<string, ResultRow>;
	/**
	 * Refs changed since the last delivered callback, with the row they changed
	 * to. It accumulates for the same reason Watcher.pending does: a callback can
	 * be dropped by a newer dispatch, and these deliveries are diffs, so a
	 * dropped one would otherwise lose the refs it carried.
	 */
	pending: Map<string, ResultRow | undefined>;
	cb: ScopedKeyedWatchCallback<unknown>;
	include?: Include;
	/** Entities this watcher's hydration reaches; see Watcher.reaches. */
	reaches: ReadonlySet<string>;
	/**
	 * Entities among `reaches` a commit in ANY workspace wakes this watcher
	 * for: those reached through a workspace-qualified reference, whose rows
	 * may sit outside the watcher's own scope.
	 */
	anywhere?: ReadonlySet<string>;
	/** Coverage of the last emit; see Watcher.lastCoverage. */
	lastCoverage?: Coverage;
	/** Read stamp of the newest view dispatched; see Watcher.emittedAt. */
	emittedAt: number;
}

/**
 * Rows shared by every watcher of one scan scope — a decode cache keyed by row
 * id, held to within a constant factor of the union of what those watchers
 * match (see pruneScope).
 *
 * A cache in the strict sense: dropping an entry costs a decode and nothing
 * else. What it buys is that ten watchers over one entity decode a changed row
 * once between them, and that a row no watcher's result moved keeps the same
 * object across commits.
 */
interface ScopeRows {
	rows: Map<string, ResultRow>;
	/**
	 * Rows the last full scan of this scope returned; absent until one happens.
	 * Only input to the rescan heuristic (see RESCAN_MIN_KEYS).
	 */
	scanned?: number;
	/**
	 * Rows left by the last full prune walk; absent until one happens. What the
	 * next walk is triggered against (see pruneScope).
	 */
	prunedTo?: number;
	/** Full prune walks this scope has taken; reported by scopeStats. */
	fullPrunes: number;
}

/**
 * What one scan scope changed, in the form the watchers on it consume.
 *
 * The keyed form carries only the rows the commit named, each already read back
 * (`undefined` = gone), and every watcher splices them into its own result. The
 * rebuild form carries the whole scope in scan order, and every watcher
 * re-derives its result from scratch.
 */
type ScopeDelta =
	| { rebuild: true; rows: ResultRow[] }
	| { rebuild: false; changed: Map<string, ResultRow | undefined> };

/**
 * Past this many touched rows, AND this fraction of the scope's last known
 * size, one prefix scan beats the point reads: each touched key costs an
 * overlay and a confirmed record in the getMany readMergedRowsWithVersion
 * issues, while a scan amortizes the cursor over the whole entity. The absolute
 * floor keeps a small commit on the incremental path even when the entity
 * itself is tiny.
 *
 * A scope no scan has ever visited — one an index has served throughout — has
 * no size to compare against and stays on the point reads, which is the right
 * call: scanning to discover how big it is spends exactly what the index was
 * chosen to avoid.
 */
const RESCAN_MIN_KEYS = 8;
const RESCAN_FRACTION = 0.25;

/**
 * A scan scope's shared rows are walked in full once they hold more than this
 * many rows AND more than this multiple of what the last full walk left. The
 * floor keeps a small map on the walk, where it costs less than the bookkeeping
 * to avoid it; the multiple is what amortizes the walk of a large one over the
 * commits that grew it (see pruneScope).
 */
const PRUNE_MIN_ROWS = 64;
const PRUNE_GROWTH = 2;

export interface WatcherHubOptions {
	/**
	 * Runs a full scan alongside every maintained update and throws if the two
	 * disagree — a self-check on the producers, not a production path.
	 *
	 * Maintaining results makes an incomplete touch set a CORRECTNESS bug:
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
	coverageOf?: (
		entity: string,
		scope?: WorkspaceScope,
		rels?: readonly RelCoverageRef[],
	) => Coverage;
	/**
	 * An entity's compiled secondary indexes, so a query watcher's first read
	 * can be served from one instead of scanned. Absent ⇒ every read scans,
	 * which is what a hub built without the client's index cache should do.
	 */
	indexes?: (entity: string) => CompiledIndex[];
	/**
	 * Reports each watcher's first read, so live queries aggregate alongside
	 * one-shot ones. Only the first read: what follows is maintained per commit
	 * rather than re-read.
	 */
	onRead?: (event: UnstampedQueryEvent) => void;
}

/**
 * Live query results, maintained per watcher rather than recomputed per commit.
 *
 * Each watcher holds the rows its query matches, in the order the query puts
 * them in (see MatchSet). A commit arrives as the rows it changed; for each of
 * them the transition is decidable on the spot — whether it matched before is
 * what the match set already says, and where it sat is a binary search away —
 * so the commit costs one predicate evaluation and one splice per changed row,
 * whatever the entity's size. A commit naming no row of the scope, or naming
 * rows the query neither held nor now admits, costs nothing at all.
 *
 * Two things still cost the whole entity, both uncommon: a commit that reports
 * an entity without its rows (a bootstrap swap, a confirmed-state wipe) forces
 * a rescan, and a first read falls back to one when no index covers the query.
 *
 * Decoded values and Row wrappers are shared between the watchers of a scope
 * AND reused across commits, and the delivered array is what the next page is
 * built from, so callbacks must not mutate either — a mutation would leak into
 * every later emit, not just the current one.
 */
export class WatcherHub {
	private watchers = new Set<Watcher>();
	/** Keyed watchers, kept apart because nothing about a result applies to them. */
	private keyWatchers = new Set<KeyWatcher>();
	/** Grouped watchers, which hold no rows and re-read on every commit. */
	private groupWatchers = new Set<GroupWatcher>();
	private closed = false;
	/** Set by the client so include watchers can hydrate; absent ⇒ no hydration. */
	private refClient?: RefClient;
	/** Shared rows per scan scope; dropped when the scope's last watcher goes. */
	private scopes = new Map<string, ScopeRows>();
	/** Monotonic stamp minted inside each read section; see Watcher.emittedAt. */
	private readSeq = 0;
	private readonly verifyTouches: boolean;

	constructor(
		private kv: KVStore,
		private registry: EntityRegistry,
		// Shared with the writers so a read sees a store consistent with (never
		// torn by) a concurrent commit. Must be the same instance writers take
		// exclusively.
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
		this.keyWatchers.clear();
		this.groupWatchers.clear();
		this.scopes.clear();
	}

	/**
	 * Rows each scan scope's shared decode cache holds, keyed by scope, with the
	 * number of full prune walks that cache has taken.
	 *
	 * @internal Support for the specs that pin the cache bound; not client API.
	 */
	scopeStats(): Map<string, { rows: number; fullPrunes: number }> {
		const out = new Map<string, { rows: number; fullPrunes: number }>();
		for (const [ scope, scoped ] of this.scopes) {
			out.set(scope, { rows: scoped.rows.size, fullPrunes: scoped.fullPrunes });
		}
		return out;
	}

	watch<D extends DescMessage>(
		schema: D,
		scope: ReadScope,
		cb: WatchCallback<MessageShape<D>>,
		query?: CompiledQuery,
		include?: Include,
		window?: WatchWindow,
	): WatchHandle {
		if (this.closed) return INERT;
		const def = this.registry.bySchema(schema);
		const named = typeof scope === 'object' ? scope : undefined;
		const rel = this.relStateOf(schema, query, named?.predicates);
		const reach = this.reachableEntities(def.name, include, rel);
		const workspaces = named === undefined
			? undefined
			: [ ...new Set(named.workspaces) ].sort(compareWorkspaces);
		// A named set of one is read as that workspace: same scan prefix, same
		// shared rows, same index. What it does NOT inherit is the ordering a
		// scoped watch can name a position in — see `cursors` below.
		const workspaceId = workspaces === undefined
			? (typeof scope === 'string' ? scope : undefined)
			: (workspaces.length === 1 ? workspaces[0] : undefined);
		// A set keeps its cursors off even at size one, so that a list shrinking to
		// a single workspace does not silently change what the watch delivers.
		const cursors = workspaceId === undefined || named !== undefined
			? undefined
			: new OrderCursors(schema, query?.orderBy ?? []);
		if (cursors === undefined && (query?.after !== undefined || query?.before !== undefined)) {
			throw new Error(
				"watchQueryAll: after/before name a position in one workspace's ordering, which "
					+ 'a cross-workspace watch — over every workspace or over a named set of them '
					+ '— has no counterpart for',
			);
		}
		const watcher: Watcher = {
			schema,
			entity: def.name,
			workspaceId,
			workspaces,
			scope: scanScope({ entity: def.name, workspaceId, workspaces }),
			cb: cb as WatchCallback<unknown>,
			query,
			predicates: named?.predicates,
			bounds: query === undefined ? undefined : pageWindowOf(schema, def.name, query),
			cursors,
			include,
			reaches: reach.entities,
			anywhere: reach.anywhere,
			compare: comparatorFor(schema, query),
			window: window ?? { offset: query?.offset, limit: query?.limit },
			pending: 'reset',
			dirtyFrom: 0,
			emittedAt: 0,
			rel,
		};
		this.watchers.add(watcher);
		// Read the initial result asynchronously (also seeds the emit memo).
		void this.emitTo(watcher);
		return {
			close: () => {
				if (!this.watchers.delete(watcher)) return;
				this.pruneScope(watcher.scope);
			},
			setWindow: (window) => void this.moveWindow(watcher, window),
		};
	}

	/**
	 * Re-delivers one watcher's page over a new window. No read: the rows are
	 * already held in result order, so this is a slice and a comparison.
	 */
	private async moveWindow(watcher: Watcher, window: WatchWindow): Promise<void> {
		if (!this.watchers.has(watcher)) return;
		watcher.window = { offset: window.offset, limit: window.limit };
		watcher.pending = 'reset';
		await this.emit(watcher, 0, false, ++this.readSeq);
	}

	/**
	 * Live grouped read: emits the groups after every commit that touches the
	 * entity in this workspace, suppressing callbacks whose answer is unchanged.
	 *
	 * Unlike a row watcher this holds no match set and maintains nothing — a
	 * commit re-runs the read. A grouped read over an index costs O(groups)
	 * whatever the entity holds, and a count has no per-row identity for an
	 * incremental update to key on: a row moving between two groups changes both
	 * numbers, and nothing in a delivered group says which rows it was made of.
	 */
	watchGroups<D extends DescMessage>(
		schema: D,
		workspaceId: string,
		compiled: PreparedGroupQuery,
		cb: (groups: Group[]) => void,
		onError?: (error: unknown) => void,
	): WatchHandle {
		const def = this.registry.bySchema(schema);
		return this.addGroupWatcher<Group[]>({
			entity: def.name,
			workspaceId,
			read: (tx) =>
				readGroups(
					tx,
					this.registry,
					def,
					schema,
					this.opts.indexes?.(def.name) ?? [],
					compiled,
					workspaceId,
				),
			same: sameGroups,
			cb,
			onError,
			reaches: new Set([ def.name ]),
		});
	}

	/**
	 * Live grouped ROW read: emits the head of each group after every commit
	 * that moves one. See watchGroups for what a grouped watcher does and does
	 * not maintain.
	 *
	 * Rows are decoded per read rather than shared with the row watchers of the
	 * same scope, so a callback may keep what it receives.
	 */
	watchRowGroups<D extends DescMessage>(
		schema: D,
		workspaceId: string,
		compiled: PreparedGroupRowsQuery,
		cb: (groups: RowGroup<MessageShape<D>>[]) => void,
		onError?: (error: unknown) => void,
	): WatchHandle {
		const def = this.registry.bySchema(schema);
		return this.addGroupWatcher<RowGroupPage<MessageShape<D>>[]>({
			entity: def.name,
			workspaceId,
			read: (tx) =>
				readRowGroups(
					tx,
					this.registry,
					def,
					schema,
					this.opts.indexes?.(def.name) ?? [],
					compiled,
					workspaceId,
				),
			same: sameRowGroups,
			cb: (groups) => cb(groups.map((g) => ({ ...g, rows: g.rows.map((entry) => entry.row) }))),
			onError,
			reaches: new Set([ def.name ]),
		});
	}

	/** Registers one grouped watcher and gives it its first read. */
	private addGroupWatcher<R>(watcher: Omit<GroupWatcher<R>, 'last'>): WatchHandle {
		if (this.closed) return INERT;
		const held = watcher as GroupWatcher<unknown>;
		this.groupWatchers.add(held);
		// The first read is its own dispatch rather than part of a notify, so a
		// mounting consumer does not wait for the next commit to see anything.
		void this.dispatchGroups([ held ]);
		return {
			close: () => {
				this.groupWatchers.delete(held);
			},
			setWindow: () => {},
		};
	}

	/** Re-reads a set of grouped watchers and delivers whatever changed. */
	private async dispatchGroups(watchers: readonly GroupWatcher[]): Promise<void> {
		if (watchers.length === 0) return;
		let results: unknown[];
		try {
			results = await this.mutex.runShared(async () => {
				const tx = await this.kv.read();
				try {
					const out: unknown[] = [];
					for (const w of watchers) out.push(await w.read(tx));
					return out;
				} finally {
					tx.release();
				}
			});
		} catch (err) {
			for (const w of watchers) {
				if (w.onError) safeInvoke(w.onError, err, 'group watch error handler');
				else reportError('group watch', err);
			}
			return;
		}
		for (let i = 0; i < watchers.length; i++) {
			const w = watchers[i]!;
			const answer = results[i];
			// A watcher closed while the read was in flight has nothing to hear it.
			if (answer === undefined || !this.groupWatchers.has(w)) continue;
			if (w.last !== undefined && w.same(w.last, answer)) continue;
			w.last = answer;
			safeInvoke(w.cb, answer, 'group watch callback');
		}
	}

	/**
	 * Watches an explicit set of rows of one entity, named by ref.
	 *
	 * With a `workspaceId` every ref must name that workspace and the watcher
	 * shares its scan scope (and so its decode cache) with the other watchers of
	 * it; with none, the refs may span workspaces and the watcher draws on the
	 * cross-workspace scope instead.
	 *
	 * Nothing is scanned either way: the refs are read as point lookups in one
	 * batch, and a commit re-reads only the watched refs it named. Registration
	 * is cheap enough to start empty and grow through
	 * ScopedKeyedWatchHandle.setRefs, which is what a screen of row-by-key views
	 * does as its components mount.
	 */
	watchRefs<D extends DescMessage>(
		schema: D,
		workspaceId: string | undefined,
		refs: Iterable<RowRef>,
		cb: ScopedKeyedWatchCallback<MessageShape<D>>,
		include?: Include,
	): ScopedKeyedWatchHandle {
		if (this.closed) return INERT_KEYED;
		const def = this.registry.bySchema(schema);
		const initial = refMap(refs);
		const watcher: KeyWatcher = {
			schema,
			entity: def.name,
			workspaceId,
			scope: scanScope({ entity: def.name, workspaceId }),
			refs: initial,
			added: new Set(initial.keys()),
			flushing: false,
			held: new Map(),
			pending: new Map(),
			cb: cb as ScopedKeyedWatchCallback<unknown>,
			include,
			...reachOf(this.reachableEntities(def.name, include)),
			emittedAt: 0,
		};
		this.keyWatchers.add(watcher);
		this.scheduleAdds(watcher);
		return {
			close: () => {
				if (!this.keyWatchers.delete(watcher)) return;
				this.pruneScope(watcher.scope);
			},
			setRefs: (next) => this.setRefs(watcher, next),
		};
	}

	/** Replaces a keyed watcher's ref set; see ScopedKeyedWatchHandle.setRefs. */
	private setRefs(watcher: KeyWatcher, next: Iterable<RowRef>): void {
		if (!this.keyWatchers.has(watcher)) return;
		const wanted = refMap(next);
		const dropped = new Set<string>();
		for (const id of watcher.refs.keys()) {
			if (wanted.has(id)) continue;
			watcher.held.delete(id);
			watcher.pending.delete(id);
			watcher.added.delete(id);
			dropped.add(id);
		}
		let grew = false;
		for (const id of wanted.keys()) {
			if (watcher.refs.has(id)) continue;
			watcher.added.add(id);
			grew = true;
		}
		watcher.refs = wanted;
		// Released here rather than at the next commit: a screen swapping one long
		// ref list for another would otherwise hold both until something is written.
		if (dropped.size > 0) this.pruneScope(watcher.scope, dropped);
		if (grew) this.scheduleAdds(watcher);
	}

	/**
	 * Schedules the read of everything in `added`, coalescing the calls made
	 * within one turn into a single batch. A list of row views mounting together
	 * issues one setRefs per component; without this each would be its own read.
	 */
	private scheduleAdds(watcher: KeyWatcher): void {
		if (watcher.flushing) return;
		watcher.flushing = true;
		queueMicrotask(() => void this.flushAdds(watcher));
	}

	/** Reads the keys added since the last flush and delivers them. */
	private async flushAdds(watcher: KeyWatcher): Promise<void> {
		watcher.flushing = false;
		if (!this.keyWatchers.has(watcher)) return;
		const at = await this.mutex.runShared(async () => {
			// Drained inside the section rather than before it, so a verify pass
			// running concurrently never sees a key that is watched, unread and no
			// longer queued for a read.
			const tx = await this.kv.read();
			try {
				const ids = [ ...watcher.added ];
				if (ids.length === 0) return 0;
				watcher.added.clear();
				await this.readRefs(tx, watcher, ids, true);
				return ++this.readSeq;
			} finally {
				tx.release();
			}
		});
		// Stamp 0 means there was nothing to read; emitKeyed drops it.
		await this.emitKeyed(watcher, at, false);
	}

	/**
	 * Reads some of a keyed watcher's refs back and records what moved. `first`
	 * marks the read that answers a ref for the very first time, which is
	 * delivered whatever it found — including that the row is absent, the answer
	 * a consumer cannot infer from silence.
	 */
	private async readRefs(
		tx: KVRead,
		watcher: KeyWatcher,
		ids: readonly string[],
		first: boolean,
	): Promise<void> {
		const wanted: RowRef[] = [];
		const wantedIds: string[] = [];
		for (const id of ids) {
			const ref = watcher.refs.get(id);
			if (ref === undefined) continue;
			wanted.push(ref);
			wantedIds.push(id);
		}
		if (wanted.length === 0) return;
		const scoped = this.scopeRows(watcher.scope);
		const merged = await readMergedRowsWithVersion(
			tx,
			this.registry,
			watcher.entity,
			wanted,
		);
		for (let i = 0; i < wanted.length; i++) {
			const { workspaceId, key } = wanted[i]!;
			const id = wantedIds[i]!;
			const view = merged[i];
			if (view === undefined) {
				const had = watcher.held.delete(id);
				scoped.rows.delete(id);
				if (first || had) watcher.pending.set(id, undefined);
				continue;
			}
			const row = resultRow(
				watcher.schema,
				scoped.rows.get(id),
				id,
				suffixOf(watcher, workspaceId, key),
				{ v: view.v, version: view.version, workspaceId, key, hidden: view.hidden },
			);
			scoped.rows.set(id, row);
			// The same object means the same bytes and version, so nothing a
			// consumer holding this ref can see has moved.
			const moved = watcher.held.get(id) !== row;
			watcher.held.set(id, row);
			if (first || moved) watcher.pending.set(id, row);
		}
	}

	/**
	 * Hands one keyed watcher the refs that moved, unless nothing did.
	 *
	 * `force` re-delivers every ref it holds: an include watcher whose hydration
	 * targets moved has a changed view over rows that did not move themselves,
	 * which nothing in `pending` would report.
	 */
	private async emitKeyed(watcher: KeyWatcher, at: number, force: boolean): Promise<void> {
		if (!this.keyWatchers.has(watcher)) return;
		if (at <= watcher.emittedAt) return;
		// Claimed before the callback (and before any hydrate), so a dispatch that
		// starts while this one is still hydrating knows it is the newer of the two.
		watcher.emittedAt = at;
		const coverage = this.opts.coverageOf?.(watcher.entity, watcher.workspaceId) ?? COMPLETE;
		// Coverage rides on every delivery, so it moves one on its own: an entity
		// that finishes hydrating turns "not here yet" into "no such row" for every
		// absent key, and nothing else says so.
		const coverageMoved = watcher.lastCoverage === undefined
			|| !sameCoverage(watcher.lastCoverage, coverage);
		const taken = force ? forcedEntries(watcher) : [ ...watcher.pending ];
		if (taken.length === 0 && !coverageMoved) return;
		// One slot per taken ref, in the order taken, so the hydrate below can be
		// scattered back positionally. A ref dropped by a concurrent setRefs no
		// longer resolves and is skipped.
		const rows: ScopedKeyedRow<unknown>[] = [];
		const slots = new Map<string, number>();
		for (const [ id ] of taken) {
			const ref = watcher.refs.get(id);
			if (ref === undefined) continue;
			slots.set(id, rows.length);
			rows.push({ workspaceId: ref.workspaceId, key: ref.key, row: undefined });
		}
		const present = taken.filter((e): e is [string, ResultRow] =>
			e[1] !== undefined && slots.has(e[0])
		);
		if (watcher.include && this.refClient && present.length > 0) {
			// Hydrated after the read tx closed (own shared reads).
			const hydrated = await hydrate(
				this.refClient,
				this.registry,
				watcher.entity,
				present.map(([ , r ]) => r.row) as Row<never>[],
				watcher.include,
			);
			// A newer dispatch may have claimed the watcher (and already fired) while
			// this one hydrated; its rows win, and it carried these refs too — they
			// are still in `pending`, which this path has not touched.
			if (!this.keyWatchers.has(watcher) || watcher.emittedAt !== at) return;
			present.forEach(([ id ], i) => {
				rows[slots.get(id)!]!.row = hydrated[i] as Row<unknown>;
			});
		} else {
			for (const [ id, held ] of taken) {
				const slot = slots.get(id);
				if (slot !== undefined) rows[slot]!.row = held?.row;
			}
		}
		watcher.lastCoverage = coverage;
		// Only what this callback carries, and only if a later read has not already
		// replaced it: an edit that landed during the hydrate above belongs to a
		// dispatch that has yet to deliver it.
		for (const [ id, held ] of taken) {
			if (watcher.pending.has(id) && watcher.pending.get(id) === held) {
				watcher.pending.delete(id);
			}
		}
		safeInvoke(
			(r: ScopedKeyedRows<unknown>) => watcher.cb(r, coverage),
			rows as ScopedKeyedRows<unknown>,
			'watch callback',
		);
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
		// Grouped by scan scope: watchers that need the same read share it.
		const byScope = new Map<string, Watcher[]>();
		for (const w of this.watchers) {
			if (!reaches(w, pairs)) continue;
			const scope = w.scope;
			const group = byScope.get(scope);
			if (group === undefined) byScope.set(scope, [ w ]);
			else group.push(w);
		}
		const keyed: KeyWatcher[] = [];
		for (const w of this.keyWatchers) {
			if (reaches(w, pairs)) keyed.push(w);
		}
		// Grouped watchers hold nothing to update in the read section below, so
		// they re-read on their own rather than joining it.
		const grouped: GroupWatcher[] = [];
		for (const w of this.groupWatchers) {
			if (reaches(w, pairs)) grouped.push(w);
		}
		void this.dispatchGroups(grouped);
		if (byScope.size === 0 && keyed.length === 0) return;
		const pending: { watcher: Watcher; from: number; force: boolean }[] = [];
		const keyedPending: { watcher: KeyWatcher; force: boolean }[] = [];
		// One read tx per notify, shared across scopes and watchers, under a
		// shared lock so a concurrent commit cannot tear the reads. Match sets are
		// updated inside the section too: the mutex is the only thing ordering two
		// overlapping notifies, and a delta applied out of order would lose a row
		// change rather than merely deliver it late. Only the callback — and, for
		// an include watcher, the hydrate it needs — runs outside.
		const at = await this.mutex.runShared(async () => {
			const tx = await this.kv.read();
			// Passes are shared for the whole notify rather than per group: two
			// watchers crossing one relation ask the same question of the target
			// entity whatever their own entity or query is, and inside this
			// transaction they are entitled to one answer.
			const relPasses: RelPassCache = new Map();
			try {
				for (const [ scope, group ] of byScope) {
					// Rel watchers re-run their pre-pass first: a commit touching a
					// TARGET entity moves which base rows match without touching
					// them, which no keyed delta can carry. A pre-pass whose keys
					// did not move keeps the watcher on the incremental path.
					const movedRel = new Map<Watcher, RelKeysDelta | 'moved'>();
					for (const w of group) {
						if (w.rel === undefined || !touchesRel(w, pairs)) continue;
						const moved = await this.refreshRel(tx, w, relPasses, pairs);
						if (moved !== undefined) movedRel.set(w, moved);
					}
					const delta = await this.readDelta(tx, scope, group, pairs);
					let scopeRows = delta.rebuild ? delta.rows : undefined;
					// The rows this pass put in the scope's shared map, so the prune
					// below can look at those alone. A scan puts the whole entity
					// there instead, which no such list describes.
					let added: Set<string> | undefined = delta.rebuild
						? undefined
						: new Set(delta.changed.keys());
					for (const w of group) {
						let from: number;
						const moved = movedRel.get(w);
						// A membership move reaches rows the commit never named, so
						// the keyed delta alone cannot carry it. Where the move is
						// small and the query lets it be read backwards, the rows it
						// reaches are found and spliced; otherwise the match set is
						// re-derived from the whole scope.
						const reached = moved === undefined || delta.rebuild
							? undefined
							: await this.relReached(tx, w, moved, scope);
						if (delta.rebuild) {
							from = applyDelta(w, delta);
						} else if (reached !== undefined) {
							for (const id of reached.keys()) added?.add(id);
							from = applyDelta(w, {
								rebuild: false,
								changed: mergeChanged(delta.changed, reached),
							}, reached);
						} else if (moved !== undefined) {
							added = undefined;
							scopeRows ??= await this.scanScope(tx, group[0]!);
							w.matches = buildMatches(w, scopeRows);
							w.pending = 'reset';
							from = 0;
						} else {
							from = applyDelta(w, delta);
						}
						pending.push({
							watcher: w,
							from,
							force: touchesInclude(w, pairs),
						});
					}
					if (this.verifyTouches) {
						for (const w of group) {
							await this.verifyRelKeys(tx, w);
							await this.verifyMatches(tx, w);
						}
					}
					this.pruneScope(scope, added);
				}
				// After the scans above, so a keyed watcher sharing a scope with a
				// rebuilt result reuses the rows that rebuild just decoded.
				const prunes = new Map<string, Set<string>>();
				for (const w of keyed) {
					const refs = touchedRefs(w, pairs);
					await this.readRefs(tx, w, refs, false);
					keyedPending.push({ watcher: w, force: touchesInclude(w, pairs) });
					if (this.verifyTouches) await this.verifyKeys(tx, w);
					addCandidates(prunes, w.scope, refs);
				}
				for (const [ scope, ids ] of prunes) this.pruneScope(scope, ids);
				// Stamped inside the section: the order the mutex grants reads in
				// IS the order of the views they saw.
				return ++this.readSeq;
			} finally {
				tx.release();
			}
		});
		for (const p of pending) await this.emit(p.watcher, p.from, p.force, at);
		for (const p of keyedPending) await this.emitKeyed(p.watcher, at, p.force);
	}

	/**
	 * Updates every watcher a commit reaches from the rows the writer attached
	 * to it, reading nothing, and returns a function that delivers the callbacks
	 * — or undefined when some watcher it reaches needs a read after all and the
	 * caller has to fall back to {@link notify}.
	 *
	 * Call it from INSIDE the writer's exclusive KV section, immediately after
	 * the commit. That section is what orders this against every other update:
	 * `notify` gets the same ordering from its own shared acquisition, and two
	 * deltas applied out of order would lose a row change rather than merely
	 * deliver it late. Nothing here acquires the mutex — a nested exclusive
	 * acquire would deadlock (see mutex.ts) — so nothing here may read the
	 * store outside the one debug check that has the section's exclusion behind
	 * it.
	 *
	 * Declines — returns undefined without touching any watcher — unless every
	 * one of these holds:
	 *
	 *   - every pair the commit names is keyed AND carries a view for every key
	 *     (see TouchSet.rowWithView), so no row would have to be read;
	 *   - no query watcher it reaches crosses references (a pre-pass reads the
	 *     target entity), hydrates an `include` (which reads the rows it joins),
	 *     or is still without a match set to splice into;
	 *   - no keyed watcher it reaches hydrates an `include`.
	 *
	 * Grouped watchers hold nothing to update and re-read for themselves, here
	 * exactly as in `notify`.
	 */
	async notifyWithRows(touched: TouchSet): Promise<(() => Promise<void>) | undefined> {
		if (this.closed) return undefined;
		const pairs = [ ...touched.pairs() ];
		const byScope = new Map<string, Watcher[]>();
		for (const w of this.watchers) {
			if (!reaches(w, pairs)) continue;
			if (w.rel !== undefined || w.include !== undefined || w.matches === undefined) {
				return undefined;
			}
			const scope = w.scope;
			const group = byScope.get(scope);
			if (group === undefined) byScope.set(scope, [ w ]);
			else group.push(w);
		}
		const keyed: KeyWatcher[] = [];
		for (const w of this.keyWatchers) {
			if (!reaches(w, pairs)) continue;
			if (w.include !== undefined) return undefined;
			keyed.push(w);
		}
		const grouped: GroupWatcher[] = [];
		for (const w of this.groupWatchers) {
			if (reaches(w, pairs)) grouped.push(w);
		}
		const pending: { watcher: Watcher; from: number }[] = [];
		let at = 0;
		if (byScope.size > 0 || keyed.length > 0) {
			// Built only once something would consume it, and the last thing that
			// can decline: a commit reaching no result costs nothing either way.
			const handed = handedRows(pairs);
			if (handed === undefined) return undefined;
			for (const [ scope, group ] of byScope) {
				const changed = this.handedChanged(scope, group[0]!, handed);
				for (const w of group) {
					pending.push({ watcher: w, from: applyDelta(w, { rebuild: false, changed }) });
				}
				this.pruneScope(scope, new Set(changed.keys()));
			}
			const prunes = new Map<string, Set<string>>();
			for (const w of keyed) addCandidates(prunes, w.scope, this.handedRefs(w, handed));
			for (const [ scope, ids ] of prunes) this.pruneScope(scope, ids);
			// Stamped here for the reason notify stamps inside its read section.
			at = ++this.readSeq;
			if (this.verifyTouches) await this.verifyHanded(byScope, keyed);
		}
		return async () => {
			void this.dispatchGroups(grouped);
			// `force` is always false: an include watcher is what it exists for, and
			// this path declines those.
			for (const p of pending) await this.emit(p.watcher, p.from, false, at);
			for (const w of keyed) await this.emitKeyed(w, at, false);
		};
	}

	/**
	 * The scope delta for one group, built from handed-over rows rather than
	 * read — readDelta's keyed branch with the getMany taken out, down to which
	 * rows land in the scope's shared decode cache.
	 */
	private handedChanged(
		scope: string,
		sample: Watcher,
		handed: readonly HandedRow[],
	): Map<string, ResultRow | undefined> {
		const scoped = this.scopeRows(scope);
		const changed = new Map<string, ResultRow | undefined>();
		for (const { workspaceId, entity, key, view } of handed) {
			if (entity !== sample.entity || !inScope(sample, workspaceId)) continue;
			const id = rowId(workspaceId, key);
			if (view === undefined) {
				scoped.rows.delete(id);
				changed.set(id, undefined);
				continue;
			}
			const row = resultRow(
				sample.schema,
				scoped.rows.get(id),
				id,
				suffixOf(sample, workspaceId, key),
				{
					...view,
					workspaceId,
					key,
				},
			);
			scoped.rows.set(id, row);
			changed.set(id, row);
		}
		return changed;
	}

	/**
	 * Brings one keyed watcher's held rows up to date from handed-over rows,
	 * returning the watched refs the commit named — readRefs with the getMany
	 * taken out, for the refs it would have re-read (`first` is never in play:
	 * a ref answered for the first time comes through flushAdds, which reads).
	 */
	private handedRefs(watcher: KeyWatcher, handed: readonly HandedRow[]): string[] {
		const scoped = this.scopeRows(watcher.scope);
		const named: string[] = [];
		for (const { workspaceId, entity, key, view } of handed) {
			if (entity !== watcher.entity || !inScope(watcher, workspaceId)) continue;
			const id = rowId(workspaceId, key);
			if (!watcher.refs.has(id)) continue;
			named.push(id);
			if (view === undefined) {
				const had = watcher.held.delete(id);
				scoped.rows.delete(id);
				if (had) watcher.pending.set(id, undefined);
				continue;
			}
			const row = resultRow(
				watcher.schema,
				scoped.rows.get(id),
				id,
				suffixOf(watcher, workspaceId, key),
				{
					...view,
					workspaceId,
					key,
				},
			);
			scoped.rows.set(id, row);
			const moved = watcher.held.get(id) !== row;
			watcher.held.set(id, row);
			if (moved) watcher.pending.set(id, row);
		}
		return named;
	}

	/**
	 * Runs the divergence check over an update served from handed-over rows.
	 * Only reachable under debugVerifyTouches.
	 *
	 * Reads WITHOUT taking the mutex, which is sound only here: the caller holds
	 * it exclusively, and that excludes every reader and writer a shared
	 * acquisition would — which is also why acquiring it would deadlock.
	 */
	private async verifyHanded(
		byScope: ReadonlyMap<string, Watcher[]>,
		keyed: readonly KeyWatcher[],
	): Promise<void> {
		const tx = await this.kv.read();
		try {
			for (const group of byScope.values()) {
				for (const w of group) await this.verifyMatches(tx, w);
			}
			for (const w of keyed) await this.verifyKeys(tx, w);
		} finally {
			tx.release();
		}
	}

	/** Reads one watcher's result for the first time, from an index or a scan. */
	private async emitTo(watcher: Watcher): Promise<void> {
		if (!this.watchers.has(watcher)) return;
		const report: IndexReport = { candidates: 0 };
		const started = performance.now();
		let entered = started;
		const at = await this.mutex.runShared(async () => {
			entered = performance.now();
			const tx = await this.kv.read();
			try {
				// The first pre-pass, before anything evaluates the predicate.
				if (watcher.rel) await this.refreshRel(tx, watcher);
				watcher.matches = await this.seedFromIndex(tx, watcher, report)
					?? buildMatches(watcher, await this.scanScope(tx, watcher));
				if (this.verifyTouches) await this.verifyMatches(tx, watcher);
				this.pruneScope(watcher.scope);
				return ++this.readSeq;
			} finally {
				tx.release();
			}
		});
		const matched = watcher.matches?.ordered.length ?? 0;
		this.opts.onRead?.(
			queryEventOf({
				origin: 'watch',
				entity: watcher.entity,
				workspaceId: watcher.workspaceId,
				rows: matched,
				total: matched,
				durationMs: performance.now() - started,
				waitMs: entered - started,
			}, report),
		);
		await this.emit(watcher, 0, true, at);
	}

	/**
	 * Reads what a commit did to one scan scope: the named rows, read back
	 * through the scope's shared rows, or the whole scope when it has to be
	 * rescanned.
	 */
	private async readDelta(
		tx: KVRead,
		scope: string,
		group: readonly Watcher[],
		pairs: readonly TouchedPair[],
	): Promise<ScopeDelta> {
		const sample = group[0]!;
		const keys = plannedKeys(group, this.scopes.get(scope), pairs);
		if (keys === undefined) return { rebuild: true, rows: await this.scanScope(tx, sample) };
		const scoped = this.scopeRows(scope);
		const changed = new Map<string, ResultRow | undefined>();
		// One batched read for the whole commit rather than a point read per
		// changed row — this runs inside the shared lock every reader queues on,
		// so a 200-row pull would otherwise hold it for 400 sequential gets.
		const merged = await readMergedRowsWithVersion(tx, this.registry, sample.entity, keys);
		for (let i = 0; i < keys.length; i++) {
			const { workspaceId, key } = keys[i]!;
			const id = rowId(workspaceId, key);
			const view = merged[i];
			if (view === undefined) {
				scoped.rows.delete(id);
				changed.set(id, undefined);
				continue;
			}
			const row = resultRow(
				sample.schema,
				scoped.rows.get(id),
				id,
				suffixOf(sample, workspaceId, key),
				{ v: view.v, version: view.version, workspaceId, key, hidden: view.hidden },
			);
			scoped.rows.set(id, row);
			changed.set(id, row);
		}
		return { rebuild: false, changed };
	}

	/** Rebuilds a scope's shared rows from a full merged-view scan, in scan order. */
	private async scanScope(tx: KVRead, watcher: Watcher): Promise<ResultRow[]> {
		const merged = await scanMergedRows(tx, this.registry, watcher.entity, readScope(watcher));
		const scoped = this.scopeRows(watcher.scope);
		const prev = scoped.rows;
		const rows = new Map<string, ResultRow>();
		const ordered: ResultRow[] = [];
		for (const row of merged) {
			const id = rowId(row.workspaceId, row.key);
			const built = resultRow(
				watcher.schema,
				prev.get(id),
				id,
				suffixOf(watcher, row.workspaceId, row.key),
				row,
			);
			rows.set(id, built);
			ordered.push(built);
		}
		// Replaced in place rather than by a fresh ScopeRows, so the prune
		// bookkeeping survives a rescan of the rows it is about.
		scoped.rows = rows;
		scoped.scanned = ordered.length;
		return ordered;
	}

	/**
	 * Answers a query watcher's first read from a secondary index, or undefined
	 * when none serves it and a scan has to.
	 *
	 * The index is asked for every MATCH rather than one page, because the match
	 * set is what the watcher goes on to maintain — re-deriving a page after each
	 * commit would put a read back on the commit path, which is the cost this
	 * class exists to remove.
	 *
	 * A watcher spanning workspaces — a named set, or every one holding rows —
	 * walks one index per workspace and concatenates: each leg is a
	 * single-workspace query, which is the only read an index serves. One leg
	 * declining declines the whole seed — the scan that then serves it is the
	 * definition, and half a result from an index is not a result.
	 */
	private async seedFromIndex(
		tx: KVRead,
		watcher: Watcher,
		report: IndexReport,
	): Promise<MatchSet | undefined> {
		const indexes = this.opts.indexes;
		if (watcher.query === undefined) return declined(report, 'no-query');
		// A hub built without the client's index cache has none to consult, and
		// an entity outside the registry has none either.
		if (indexes === undefined) return declined(report, 'no-index');
		const def = this.registry.get(watcher.entity);
		if (def === undefined) return declined(report, 'no-index');
		// A watcher spanning every workspace names none, so its legs are the
		// workspaces holding rows — the prefixes the scan it stands in for visits.
		const workspaces = watcher.workspaceId !== undefined
			? [ watcher.workspaceId ]
			: watcher.workspaces ?? await workspacesWithRows(tx, watcher.entity);
		// A set naming no workspace has none to consult in.
		if (workspaces.length === 0) return declined(report, 'no-index');
		const legs: IndexedRow<unknown>[] = [];
		const names = new Set<string>();
		for (const ws of workspaces) {
			const leg: IndexReport = { candidates: 0 };
			const found = await queryByIndex(
				tx,
				this.registry,
				def,
				watcher.schema,
				indexes(watcher.entity),
				{ where: whereFor(watcher, ws), orderBy: watcher.query.orderBy },
				ws,
				watcher.bounds,
				leg,
			);
			report.candidates += leg.candidates;
			if (found === undefined) {
				report.declined = leg.declined;
				return undefined;
			}
			if (leg.index !== undefined) names.add(leg.index);
			legs.push(...found.rows as IndexedRow<unknown>[]);
		}
		// One read reports one index. Legs choose independently, so a set whose
		// per-workspace predicates planned differently reports each name it used.
		report.index = [ ...names ].join('+');
		const scoped = this.scopeRows(watcher.scope);
		const ordered = legs.map((entry) => {
			const id = rowId(entry.row.workspaceId, entry.row.key);
			const prev = scoped.rows.get(id);
			const row = prev !== undefined && bytesEqual(prev.bytes, entry.bytes)
					&& prev.row.version === entry.row.version && sameHidden(prev.row.hidden, entry.row.hidden)
				? prev
				: {
					id,
					suffix: suffixOf(watcher, entry.row.workspaceId, entry.row.key),
					bytes: entry.bytes,
					row: entry.row as Row<unknown>,
				};
			scoped.rows.set(id, row);
			return row;
		});
		// Re-sorted rather than trusted from the index: the binary searches that
		// maintain this set are sound against THIS comparator and no other.
		ordered.sort(watcher.compare);
		return matchSetOf(watcher, ordered);
	}

	/**
	 * Hands one watcher its current page, unless nothing it reports has moved.
	 *
	 * `from` is the lowest result index the commit disturbed (Infinity for none).
	 * Folded into `dirtyFrom`, it settles in one comparison whether a windowed
	 * watcher has to look at all — an insert, delete or move at or past
	 * `offset + limit` cannot change the rows before it — and bounds the rest of
	 * the work here to the disturbed tail. Only the rows — `total` and `coverage`
	 * ride on the result too, and each moves it on its own. `force` bypasses the
	 * memo: the first emit, and an include watcher whose hydration targets moved
	 * where its own rows did not.
	 */
	private async emit(watcher: Watcher, from: number, force: boolean, at: number): Promise<void> {
		if (!this.watchers.has(watcher) || watcher.matches === undefined) return;
		// Lowered before the stamp gate below: a dispatch a newer one supersedes
		// still disturbed these rows, and it is handed `from` exactly once.
		if (from < watcher.dirtyFrom) watcher.dirtyFrom = from;
		if (at <= watcher.emittedAt) return;
		// Claimed before the callback (and before any hydrate), so a dispatch that
		// starts while this one is still hydrating knows it is the newer of the two.
		watcher.emittedAt = at;
		const coverage = this.opts.coverageOf?.(watcher.entity, readScope(watcher), watcher.rel?.coverage)
			?? COMPLETE;
		const memoized = !force
			&& watcher.lastWindow !== undefined
			&& watcher.lastCoverage !== undefined
			&& sameCoverage(watcher.lastCoverage, coverage);
		const end = windowEnd(watcher.window);
		const total = watcher.matches.ordered.length;
		const facets = watcher.matches.facets?.result();
		const settled = memoized && total === watcher.lastTotal
			&& sameFacets(facets, watcher.lastFacets);
		// The O(1) gate. `pending` has to be empty for it: a callback dropped by
		// a newer dispatch leaves edits undelivered, and the consumer is behind
		// by them however little this commit moved.
		if (
			settled && watcher.dirtyFrom >= end && watcher.pending !== 'reset'
			&& watcher.pending.length === 0
		) {
			return;
		}
		const offset = watcher.window.offset ?? 0;
		const ordered = watcher.matches.ordered;
		const length = Math.max(0, Math.min(total, end) - offset);
		// Rows before `dirtyFrom` are the ResultRows the last callback carried, so
		// neither the cancellation check nor the new page has to look at them: the
		// check compares the tail, and the page is the last one's leading copy plus
		// that tail. Without a last delivery there is nothing to reuse.
		const last = watcher.lastWindow;
		const lastRows = watcher.lastRows;
		const keep = last === undefined || lastRows === undefined
			? 0
			: Math.min(Math.max(0, Math.min(watcher.dirtyFrom, end) - offset), length, last.length);
		if (settled && sameTail(last!, ordered, offset, keep, length)) {
			// The page the consumer holds is already this one, so whatever the
			// edits were, they cancelled out.
			watcher.pending = [];
			watcher.dirtyFrom = Infinity;
			return;
		}
		const delta: WatchDelta<unknown> = watcher.pending === 'reset'
			? { kind: 'reset' }
			: { kind: 'splice', changes: watcher.pending };
		// A fresh array every time: consumers compare deliveries by array identity.
		const window: ResultRow[] = keep > 0 ? last!.slice(0, keep) : [];
		const rows: Row<unknown>[] = keep > 0 ? lastRows!.slice(0, keep) : [];
		for (let i = keep; i < length; i++) {
			const held = ordered[offset + i]!;
			window.push(held);
			rows.push(held.row);
		}
		if (watcher.include && this.refClient) {
			// Hydrated after the read tx closed (own shared reads).
			const hydrated = await hydrate(
				this.refClient,
				this.registry,
				watcher.entity,
				rows as Row<never>[],
				watcher.include,
			);
			// A newer dispatch may have claimed the watcher (and already fired)
			// while this one hydrated; its rows win.
			if (!this.watchers.has(watcher) || watcher.emittedAt !== at) return;
			watcher.lastWindow = window;
			watcher.lastRows = rows;
			watcher.lastCoverage = coverage;
			watcher.lastTotal = total;
			watcher.lastFacets = facets;
			watcher.pending = [];
			watcher.dirtyFrom = Infinity;
			deliver(
				watcher,
				withCoverage(hydrated, coverage, pageInfo(watcher, rows, total, end, facets)),
				delta,
			);
			return;
		}
		watcher.lastWindow = window;
		watcher.lastRows = rows;
		watcher.lastCoverage = coverage;
		watcher.lastTotal = total;
		watcher.lastFacets = facets;
		watcher.pending = [];
		watcher.dirtyFrom = Infinity;
		deliver(
			watcher,
			withCoverage(rows, coverage, pageInfo(watcher, rows, total, end, facets)),
			delta,
		);
	}

	private scopeRows(scope: string): ScopeRows {
		let scoped = this.scopes.get(scope);
		if (scoped === undefined) {
			scoped = { rows: new Map(), fullPrunes: 0 };
			this.scopes.set(scope, scoped);
		}
		return scoped;
	}

	/**
	 * Drops rows no watcher of a scope holds, and the scope itself with its last
	 * watcher. This is what keeps the shared rows near the union of the live
	 * results: a rescan puts the whole entity in the map, and everything the
	 * queries reject comes back out of it here.
	 *
	 * `candidates` names the rows an operation added to the map and confines the
	 * work to them, so a commit costs what it changed rather than what the
	 * results hold. Without it — a rescan, a first read, a watcher closing — and
	 * whenever the map has grown past PRUNE_GROWTH times what the last full walk
	 * left, every row in the map is examined instead. The map therefore stays
	 * within max(PRUNE_MIN_ROWS, PRUNE_GROWTH x the union) even where candidates
	 * missed something, and the walks that hold that bound are amortized over the
	 * commits that grew the map.
	 */
	private pruneScope(scope: string, candidates?: ReadonlySet<string>): void {
		const holders: ReadonlyMap<string, ResultRow>[] = [];
		let live = false;
		for (const w of this.watchers) {
			if (w.scope !== scope) continue;
			live = true;
			if (w.matches) holders.push(w.matches.byId);
		}
		for (const w of this.keyWatchers) {
			if (w.scope !== scope) continue;
			live = true;
			holders.push(w.held);
		}
		if (!live) {
			this.scopes.delete(scope);
			return;
		}
		const scoped = this.scopes.get(scope);
		if (scoped === undefined) return;
		const ceiling = Math.max(PRUNE_MIN_ROWS, (scoped.prunedTo ?? 0) * PRUNE_GROWTH);
		if (candidates !== undefined && scoped.rows.size <= ceiling) {
			// Probed per holder rather than against a union set: building the union
			// is the O(map) work this path exists to skip.
			for (const id of candidates) {
				if (!holders.some((rows) => rows.has(id))) scoped.rows.delete(id);
			}
			return;
		}
		const held = new Set<string>();
		for (const rows of holders) {
			for (const id of rows.keys()) held.add(id);
		}
		for (const id of scoped.rows.keys()) {
			if (!held.has(id)) scoped.rows.delete(id);
		}
		scoped.prunedTo = scoped.rows.size;
		scoped.fullPrunes++;
	}

	/**
	 * Asserts a maintained result still equals what a scan would produce.
	 * Only reachable under debugVerifyTouches.
	 *
	 * This is the check the whole design rests on. A commit that changed a row
	 * without reporting it, a predicate whose verdict moved without the row's
	 * bytes moving, a comparator that disagrees with the sort `runQuery` does —
	 * each shows up here and nowhere else, because a maintained result that is
	 * quietly wrong still looks like an ordinary one.
	 */
	private async verifyMatches(tx: KVRead, watcher: Watcher): Promise<void> {
		if (watcher.matches === undefined) return;
		const merged = await scanMergedRows(tx, this.registry, watcher.entity, readScope(watcher));
		const bytes = new Map(merged.map((row) => [ rowId(row.workspaceId, row.key), row.v ]));
		const scanned: Row<unknown>[] = merged.map((row) =>
			rowOf(
				watcher.schema,
				row.key,
				decodeRow(watcher.schema, row.v),
				row.workspaceId,
				row.version,
				row.hidden,
			)
		);
		// The definition, minus the page: a match set is the whole result, and the
		// offset/limit slice happens on emit. A per-workspace predicate is applied
		// before the sort rather than inside it, which is the same set — `runQuery`
		// filters first — and leaves one query for the one order it has to be in.
		// A rel query takes the same pre-filter path whatever its scope: its
		// effective predicate is per workspace by construction.
		const perWorkspace = watcher.predicates !== undefined || watcher.rel !== undefined;
		const admitted = !perWorkspace
			? scanned
			: scanned.filter((row) => {
				const where = whereFor(watcher, row.workspaceId);
				return where === undefined || evalCond(row.value, where, watcher.schema);
			});
		const want = watcher.query === undefined ? admitted : runQuery(
			admitted,
			{
				where: perWorkspace ? undefined : watcher.query.where,
				orderBy: watcher.query.orderBy,
			},
			watcher.schema,
			watcher.bounds,
		);
		const have = watcher.matches.ordered;
		const describe = (r: Row<unknown>): string => `${r.workspaceId}/${r.key}@${r.version}`;
		const fail = (why: string): never => {
			throw new Error(
				`maintained watch result diverged for scope ${watcher.scope}: ${why}\n`
					+ `  held: [${have.map((r) => describe(r.row)).join(', ')}]\n`
					+ `  scan: [${want.map(describe).join(', ')}]\n`
					+ 'Either a commit path changed a row without reporting it (see TouchSet),'
					+ ' or the maintained order disagrees with runQuery.',
			);
		};
		if (have.length !== want.length) {
			fail(`${have.length} rows held, ${want.length} from the scan`);
		}
		for (let i = 0; i < want.length; i++) {
			const held = have[i]!;
			const row = want[i]!;
			if (
				held.row.key !== row.key
				|| held.row.workspaceId !== row.workspaceId
				|| held.row.version !== row.version
				|| !bytesEqual(held.bytes, bytes.get(held.id) ?? EMPTY)
			) {
				fail(`row ${i} is ${held.row.key}, expected ${row.key}`);
			}
		}
	}

	/**
	 * Asserts a keyed watcher's held rows still equal a point read of its keys.
	 * Only reachable under debugVerifyTouches; the counterpart of verifyMatches,
	 * and the check that catches a commit which changed a watched row without
	 * naming it in its TouchSet.
	 */
	private async verifyKeys(tx: KVRead, watcher: KeyWatcher): Promise<void> {
		const ids = [ ...watcher.refs.keys() ].filter((id) => !watcher.added.has(id));
		const refs = ids.map((id) => watcher.refs.get(id)!);
		const merged = await readMergedRowsWithVersion(tx, this.registry, watcher.entity, refs);
		const fail = (ref: RowRef, why: string): never => {
			throw new Error(
				`keyed watch diverged for ${watcher.entity}/${ref.workspaceId}/${ref.key}: ${why}\n`
					+ 'A commit path changed the row without reporting it (see TouchSet).',
			);
		};
		for (let i = 0; i < ids.length; i++) {
			const ref = refs[i]!;
			const want = merged[i];
			const have = watcher.held.get(ids[i]!);
			if (want === undefined) {
				if (have !== undefined) fail(ref, 'held a row the store no longer has');
				continue;
			}
			if (have === undefined) fail(ref, 'holds no row where the store has one');
			if (!bytesEqual(have!.bytes, want.v)) fail(ref, 'holds different bytes than the store');
			if (have!.row.version !== want.version) {
				fail(ref, `holds version ${have!.row.version}, the store has ${want.version}`);
			}
		}
	}

	/**
	 * Entities a watcher reaches: its own, plus every entity reachable by
	 * walking the include tree (forward target / reverse source) at each level,
	 * plus the target of every reference its query crosses.
	 */
	private reachableEntities(entity: string, include?: Include, rel?: RelWatchState): Reachable {
		const entities = new Set<string>([ entity ]);
		const anywhere = new Set<string>();
		// `fromAnywhere` says the holders themselves may sit in any workspace,
		// so everything they reach may too.
		const walk = (from: string, spec: Include | undefined, fromAnywhere: boolean): void => {
			if (!spec) return;
			for (const [ relation, nested ] of Object.entries(spec)) {
				if (relation.startsWith('$')) continue;
				// A polymorphic relation reaches one entity per case, and each of
				// them may carry the nested tree on.
				const cases = this.registry.forwardCasesOf(from, relation);
				const rev = cases.length > 0 ? undefined : this.registry.reverseRef(from, relation);
				const hops = cases.length > 0
					? cases.map((c) => ({ next: c.targetEntity, shape: c.shape }))
					: rev
					? [ { next: rev.sourceEntity, shape: rev.shape } ]
					: [];
				for (const { next, shape } of hops) {
					entities.add(next);
					const nextAnywhere = fromAnywhere || refQualified(shape);
					if (nextAnywhere) anywhere.add(next);
					if (typeof nested === 'object' && nested !== null) walk(next, nested, nextAnywhere);
				}
			}
		};
		walk(entity, include, false);
		for (const target of rel?.targets ?? []) entities.add(target);
		for (const target of rel?.anywhere ?? []) anywhere.add(target);
		return { entities, anywhere };
	}

	/**
	 * The rel state of a query watcher, or undefined for a row-local query —
	 * which is every watcher without a `matches` in its predicate, and keeps
	 * their paths untouched.
	 */
	private relStateOf(
		schema: DescMessage,
		query?: CompiledQuery,
		predicates?: ReadonlyMap<string, Cond>,
	): RelWatchState | undefined {
		const nodes = [
			...collectRels(schema, query?.where),
			...(predicates ? [ ...predicates.values() ].flatMap((where) => collectRels(schema, where)) : []),
		];
		if (nodes.length === 0) return undefined;
		const targets = new Set<string>();
		const anywhere = new Set<string>();
		const coverage: RelCoverageRef[] = [];
		for (const node of nodes) {
			const target = this.registry.bySchema(node.targetSchema).name;
			targets.add(target);
			const qualified = refQualified(node.shape);
			if (qualified) anywhere.add(target);
			coverage.push({
				targetEntity: target,
				...(node.fieldPath.length === 1 ? { refLocalName: refSegmentOf(node.field) } : {}),
				...(qualified ? { crossWorkspace: true } : {}),
			});
		}
		return { nodes, targets, anywhere, coverage, whereByWs: new Map() };
	}

	/**
	 * Brings a rel watcher's membership up to date and reports how it moved: a
	 * delta the caller can re-examine rows from, `'moved'` when it moved but no
	 * delta describes it, or undefined when it did not move. Any move drops the
	 * rewritten predicates, because a moved membership changes which base rows
	 * match without touching one of them.
	 *
	 * `pairs` lets the update be taken from the rows the commit named rather
	 * than from a fresh pass over the target entity, which is a point read per
	 * changed row instead of a query per watcher. It declines — and the pass
	 * serves — for a commit that named no keys, and for a watcher that has no
	 * membership yet to update.
	 *
	 * `shared` collects the passes so watchers crossing the same relation over
	 * the same scope read the target entity once between them. It must not
	 * outlive the read `tx` answers from: two watchers sharing a pass are
	 * claiming to have read the same view, which only one transaction gives.
	 */
	private async refreshRel(
		tx: KVRead,
		watcher: Watcher,
		shared?: RelPassCache,
		pairs?: readonly TouchedPair[],
	): Promise<RelKeysDelta | undefined | 'moved'> {
		const rel = watcher.rel!;
		const scope = readScope(watcher);
		const prev = rel.keys;
		const next = (pairs !== undefined && prev !== undefined
			? await updateRelKeys(
				tx,
				this.registry,
				rel.nodes,
				scope,
				prev,
				(entity) => touchedTargets(watcher, entity, pairs),
				shared,
			)
			: undefined)
			?? await computeRelKeys(
				tx,
				this.registry,
				rel.nodes,
				scope,
				this.opts.indexes,
				shared,
			);
		if (prev !== undefined && sameRelKeys(prev, next)) return undefined;
		rel.keys = next;
		rel.whereByWs.clear();
		if (prev === undefined) return 'moved';
		return diffRelKeys(prev, next) ?? 'moved';
	}

	/**
	 * The rows a membership move reaches — those that may have started or
	 * stopped matching without the commit naming them — or undefined for "no
	 * cheaper answer than re-deriving the whole match set".
	 *
	 * Both halves rest on the move being MONOTONE, which relsArePositive is the
	 * check for. A row can then only start matching because a key entered some
	 * node's set, so the entrants select the candidates; and it can only stop
	 * because a key left, so the leavers are looked for among the rows already
	 * held. Under a negation both implications reverse and this would miss rows,
	 * which is why such a query declines to the re-derivation.
	 *
	 * A move of any size is examined: the entrants are queried in batches of at
	 * most MAX_INDEX_PROBES keys, so the width of the move does not by itself
	 * decide the path. It declines when a batch has no index plan, when the
	 * query or the reference shape puts it outside the assumptions above, or
	 * when the watcher spans every workspace. It declines rather than scans — a
	 * scan of the base entity is what the re-derivation already does, and doing
	 * it here would be the same cost with a splice script on top.
	 */
	private async relReached(
		tx: KVRead,
		watcher: Watcher,
		moved: RelKeysDelta | 'moved',
		scope: string,
	): Promise<Map<string, ResultRow | undefined> | undefined> {
		const rel = watcher.rel;
		const matches = watcher.matches;
		const indexes = this.opts.indexes;
		if (moved === 'moved' || rel?.keys === undefined || matches === undefined) return undefined;
		if (indexes === undefined) return undefined;
		// A workspace-qualified reference decides a stub by the workspace it
		// names as well as its key, which neither the leavers' canonical keys nor
		// the entrants' membership test below carry; re-derive instead.
		if (rel.nodes.some((node) => refQualified(node.shape))) return undefined;
		const workspaces = watcher.workspaceId !== undefined
			? [ watcher.workspaceId ]
			: watcher.workspaces;
		// A watcher spanning every workspace re-derives from a scan instead:
		// enumerating the store's workspaces for a walk per leg is the seed's
		// cost again, on a path a membership move takes rarely.
		if (workspaces === undefined || workspaces.length === 0) return undefined;
		for (const ws of workspaces) {
			if (!relsArePositive(watcher.predicates?.get(ws) ?? watcher.query?.where)) return undefined;
		}
		const def = this.registry.get(watcher.entity);
		if (def === undefined) return undefined;
		const out = new Map<string, ResultRow | undefined>();
		// Rows that may have stopped matching: the ones already held whose stub
		// names a target that left. Held rows are in memory, so this costs no
		// read at all.
		for (const held of matches.ordered) {
			for (const node of rel.nodes) {
				const gone = moved.removed.get(node)?.get(held.row.workspaceId);
				if (gone === undefined || gone.size === 0) continue;
				const hit = stubKeyTuples(node, held.row.value).some((tuple) => {
					const key = relTupleKey(tuple);
					return key !== undefined && gone.has(key);
				});
				if (hit) {
					out.set(held.id, held);
					break;
				}
			}
		}
		// Rows that may have started matching: the query the watcher already
		// runs, with one node narrowed to the keys that just entered. Every
		// other node keeps its current membership, so what comes back is rows
		// matching NOW that owe it to this node's move.
		const scoped = this.scopeRows(scope);
		for (const node of rel.nodes) {
			for (const ws of workspaces) {
				const entered = moved.added.get(node)?.get(ws);
				if (entered === undefined || entered.length === 0) continue;
				const base = watcher.predicates?.get(ws) ?? watcher.query?.where;
				if (base === undefined) return undefined;
				// The entrants are split into batches the planner will still probe
				// for: an `in` wider than MAX_INDEX_PROBES has no plan, and one
				// query per batch is what keeps a wide move off the scan. The
				// batches partition the entrants, and the query carries only a
				// where — no ordering, no limit, no offset — so their union is
				// exactly what the single query would have returned. A paged query
				// could not be split this way.
				for (let at = 0; at < entered.length; at += MAX_INDEX_PROBES) {
					const batch = entered.slice(at, at + MAX_INDEX_PROBES);
					const replacements = new Map<RelCond, Cond>();
					for (const other of rel.nodes) {
						const keys = other === node
							? batch
							: [ ...(rel.keys.get(other)?.get(ws)?.values() ?? []) ];
						replacements.set(other.cond, relReplacement(other, keys));
					}
					const found = await queryByIndex(
						tx,
						this.registry,
						def,
						watcher.schema,
						indexes(watcher.entity),
						{ where: rewriteRels(base, replacements), total: false },
						ws,
					);
					// A batch can still decline — the other nodes' memberships are
					// rewritten into the same query and may themselves be wide.
					if (found === undefined) return undefined;
					for (const entry of found.rows as IndexedRow<unknown>[]) {
						const id = rowId(entry.row.workspaceId, entry.row.key);
						const prev = scoped.rows.get(id);
						const row = prev !== undefined && bytesEqual(prev.bytes, entry.bytes)
								&& prev.row.version === entry.row.version && sameHidden(prev.row.hidden, entry.row.hidden)
							? prev
							: {
								id,
								suffix: suffixOf(watcher, entry.row.workspaceId, entry.row.key),
								bytes: entry.bytes,
								row: entry.row as Row<unknown>,
							};
						scoped.rows.set(id, row);
						out.set(id, row);
					}
				}
			}
		}
		return out;
	}

	/**
	 * Asserts a maintained membership still equals what a pre-pass would find.
	 * Only reachable under debugVerifyTouches.
	 *
	 * The counterpart of verifyMatches for the half of a rel watcher's state
	 * that no scan of its own entity can check: a membership updated from the
	 * rows a commit named is only right if every commit that moved a target row
	 * named it, and a membership that is quietly wrong still produces an
	 * ordinary-looking result.
	 */
	private async verifyRelKeys(tx: KVRead, watcher: Watcher): Promise<void> {
		const rel = watcher.rel;
		if (rel?.keys === undefined) return;
		const want = await computeRelKeys(
			tx,
			this.registry,
			rel.nodes,
			readScope(watcher),
			this.opts.indexes,
		);
		if (sameRelKeys(rel.keys, want)) return;
		const describe = (keys: RelKeys) =>
			[ ...keys.values() ]
				.map((perWs) =>
					[ ...perWs ].map(([ ws, keyed ]) => `${ws}:[${[ ...keyed.keys() ].join(', ')}]`).join(' ')
				)
				.join(' | ');
		throw new Error(
			`maintained rel membership diverged for ${watcher.entity}: \n`
				+ `  held: ${describe(rel.keys)}\n`
				+ `  pass: ${describe(want)}\n`
				+ 'A commit changed a target row without naming it (see TouchSet).',
		);
	}
}

const EMPTY = new Uint8Array();

/** One (workspace, entity) pair a commit touched; `keys` undefined = wildcard. */
interface TouchedPair {
	workspaceId: string;
	entity: string;
	keys?: ReadonlySet<string>;
	/** Merged views the writer attached to some of `keys`; see TouchSet.pairs. */
	views?: ReadonlyMap<string, MergedRow | undefined>;
}

/** What a watcher's hydration and relations reach; see Watcher.reaches and Watcher.anywhere. */
interface Reachable {
	entities: Set<string>;
	anywhere: Set<string>;
}

/** A Reachable as the two watcher fields it fills. */
function reachOf(
	reach: Reachable,
): { reaches: ReadonlySet<string>; anywhere: ReadonlySet<string> } {
	return { reaches: reach.entities, anywhere: reach.anywhere };
}

/**
 * What deciding "does this commit reach you" needs of a watcher, satisfied by
 * both kinds.
 */
interface Reach {
	entity: string;
	workspaceId?: string;
	workspaces?: readonly string[];
	include?: Include;
	reaches: ReadonlySet<string>;
	/**
	 * Entities among `reaches` a commit in ANY workspace wakes this watcher
	 * for: those reached through a workspace-qualified reference, whose rows
	 * may sit outside the watcher's own scope.
	 */
	anywhere?: ReadonlySet<string>;
}

/** Indexes refs by row id, which is also what deduplicates a repeated ref. */
function refMap(refs: Iterable<RowRef>): Map<string, RowRef> {
	const out = new Map<string, RowRef>();
	for (const ref of refs) out.set(rowId(ref.workspaceId, ref.key), ref);
	return out;
}

/**
 * The watched refs a commit named, by row id.
 *
 * A pair that names an entity without its rows is a wildcard: the changed keys
 * are unknown, so every ref the watcher holds IN THAT PAIR'S WORKSPACE could
 * have moved. A cross-workspace watcher's refs in the other workspaces are
 * untouched by it, which is why this accumulates per pair rather than giving up
 * on the whole set.
 */
function touchedRefs(watcher: KeyWatcher, pairs: readonly TouchedPair[]): string[] {
	const out = new Set<string>();
	for (const pair of pairs) {
		if (pair.entity !== watcher.entity) continue;
		if (!inScope(watcher, pair.workspaceId)) continue;
		if (pair.keys === undefined) {
			for (const [ id, ref ] of watcher.refs) {
				if (ref.workspaceId === pair.workspaceId) out.add(id);
			}
			continue;
		}
		for (const key of pair.keys) {
			const id = rowId(pair.workspaceId, key);
			if (watcher.refs.has(id)) out.add(id);
		}
	}
	return [ ...out ];
}

/**
 * Adds one keyed watcher's prune candidates to those of its scope, so that one
 * update prunes each scope once. A prune visits every watcher of the hub, so
 * one prune per scope keeps a commit linear in the number of keyed watchers —
 * a screen of row views holds one watcher per row, all on one scope.
 *
 * A watcher with no candidates still adds its scope: an empty prune can still
 * start the full walk (see pruneScope).
 */
function addCandidates(
	into: Map<string, Set<string>>,
	scope: string,
	ids: Iterable<string>,
): void {
	let candidates = into.get(scope);
	if (candidates === undefined) {
		candidates = new Set();
		into.set(scope, candidates);
	}
	for (const id of ids) candidates.add(id);
}

/** Every ref a forced emit re-delivers: what is held, with pending changes over it. */
function forcedEntries(watcher: KeyWatcher): [string, ResultRow | undefined][] {
	const out = new Map<string, ResultRow | undefined>(watcher.held);
	for (const [ id, row ] of watcher.pending) out.set(id, row);
	return [ ...out ];
}

/**
 * Brings one watcher's match set up to date with a scope delta, returning the
 * lowest result index it disturbed (Infinity if it disturbed none).
 *
 * Nothing here reads the store, and nothing looks at a row the commit did not
 * name: whether a row matched before is what the match set already holds, and
 * where it sat is a binary search away.
 */
function applyDelta(
	watcher: Watcher,
	delta: ScopeDelta,
	recheck?: { has(id: string): boolean },
): number {
	if (delta.rebuild) {
		watcher.matches = buildMatches(watcher, delta.rows);
		watcher.pending = 'reset';
		return 0;
	}
	const matches = watcher.matches;
	// plannedKeys rebuilds rather than hand a keyed delta to a watcher with
	// nothing to splice into, so this is unreachable; it is not worth a throw.
	if (matches === undefined) return Infinity;
	let from = Infinity;
	for (const [ id, next ] of delta.changed) {
		const prev = matches.byId.get(id);
		// The same object means the same bytes, version and position — unless the
		// caller says this row's verdict may have moved without its bytes, which
		// is what a membership move does to a row it reaches.
		if (prev !== undefined && prev === next && recheck?.has(id) !== true) continue;
		const admitted = next !== undefined && admits(watcher, next);
		if (prev === undefined && !admitted) continue;
		let removedAt = -1;
		let insertedAt = -1;
		if (prev !== undefined) {
			let at = lowerBound(matches.ordered, watcher.compare, prev);
			// A fence, not a path: the comparator is a total order and `prev` is in
			// the array, so the search lands on it. Falling back to a linear scan
			// turns a comparator that somehow disagreed into slow rather than wrong.
			if (matches.ordered[at] !== prev) at = matches.ordered.indexOf(prev);
			if (at >= 0) {
				matches.ordered.splice(at, 1);
				matches.facets?.add(prev.row, -1);
				removedAt = at;
				if (at < from) from = at;
			}
			matches.byId.delete(id);
		}
		if (admitted) {
			insertedAt = lowerBound(matches.ordered, watcher.compare, next!);
			matches.ordered.splice(insertedAt, 0, next!);
			matches.byId.set(id, next!);
			matches.facets?.add(next!.row, 1);
			if (insertedAt < from) from = insertedAt;
		}
		// A row that left and re-entered at the same position was replaced, not
		// moved — one edit rather than two, and the one a list wants.
		if (removedAt >= 0 && removedAt === insertedAt) {
			record(watcher, { op: 'update', index: insertedAt, row: next!.row });
			continue;
		}
		if (removedAt >= 0) record(watcher, { op: 'remove', index: removedAt, row: prev!.row });
		if (insertedAt >= 0) record(watcher, { op: 'insert', index: insertedAt, row: next!.row });
	}
	return from;
}

/**
 * The commit's own changed rows over the ones a membership move reached. The
 * commit wins where both name a row: both were read in this transaction, and
 * the one that names it read it because it changed.
 */
function mergeChanged(
	changed: ReadonlyMap<string, ResultRow | undefined>,
	reached: ReadonlyMap<string, ResultRow | undefined>,
): Map<string, ResultRow | undefined> {
	const out = new Map(reached);
	for (const [ id, row ] of changed) out.set(id, row);
	return out;
}

/**
 * Appends one edit to a watcher's pending script, in the order it was applied.
 *
 * A watcher with a page gives up and reports `reset` instead: its indices are
 * into the match set, and translating them across an offset and a limit means
 * composing shifts over the page boundary for a consumer that can compare two
 * short arrays instead.
 */
function record(watcher: Watcher, splice: RowSplice<unknown>): void {
	if (watcher.pending === 'reset') return;
	if (watcher.window.limit !== undefined || (watcher.window.offset ?? 0) !== 0) {
		watcher.pending = 'reset';
		return;
	}
	watcher.pending.push(splice);
}

/** Hands one result to a watcher's callback, isolating anything it throws. */
function deliver(watcher: Watcher, rows: RowSet<unknown>, delta: WatchDelta<unknown>): void {
	safeInvoke((r: RowSet<unknown>) => watcher.cb(r, delta), rows, 'watch callback');
}

/**
 * Records why a first read never reached the index and returns the "scan
 * instead" answer, for the checks the hub makes before queryByIndex sees them.
 */
function declined(report: IndexReport, reason: QueryDeclineReason): undefined {
	report.declined = reason;
	return undefined;
}

/** Derives a watcher's whole match set from a scope, given in scan order. */
function buildMatches(watcher: Watcher, rows: readonly ResultRow[]): MatchSet {
	const ordered = watcher.query?.where === undefined && watcher.predicates === undefined
			&& watcher.bounds === undefined
		? rows.slice()
		: rows.filter((r) => admits(watcher, r));
	if (watcher.query?.orderBy && watcher.query.orderBy.length > 0) {
		ordered.sort(watcher.compare);
	}
	return matchSetOf(watcher, ordered);
}

/** A match set over rows already filtered and in result order, with its
 * facets folded when the query names any. */
function matchSetOf(watcher: Watcher, ordered: ResultRow[]): MatchSet {
	const set: MatchSet = { byId: new Map(ordered.map((r) => [ r.id, r ])), ordered };
	if (watcher.query?.facets !== undefined) {
		set.facets = new FacetBuckets(watcher.query.facets, watcher.schema);
		for (const r of ordered) set.facets.add(r.row, 1);
	}
	return set;
}

/**
 * Whether a watcher's query admits a row, under the predicate that row's own
 * workspace is read with. The cursor bounds are part of that and not a slice
 * taken afterwards: a row outside them is not a match, so it never enters the
 * maintained set and never reaches `total`.
 */
function admits(watcher: Watcher, row: ResultRow): boolean {
	const where = whereFor(watcher, row.row.workspaceId);
	if (where !== undefined && !evalCond(row.row.value, where, watcher.schema)) return false;
	return watcher.bounds === undefined || watcher.bounds.within(row.row.value, row.row.key);
}

/**
 * The total order a query's result is in, made explicit so a maintained result
 * can binary-search within it.
 *
 * `runQuery` filters rows that arrive in scan order and sorts what survives
 * with a STABLE sort, so a tie in `compareRows` resolves to scan order — the
 * key suffix. Without an order clause there is no sort at all and the suffix is
 * the whole order.
 */
function comparatorFor(
	schema: DescMessage,
	query?: CompiledQuery,
): (a: ResultRow, b: ResultRow) => number {
	const orderBy = query?.orderBy;
	if (orderBy === undefined || orderBy.length === 0) {
		return (a, b) => cmp(a.suffix, b.suffix);
	}
	return (a, b) => compareRows(a.row, b.row, orderBy, schema) || cmp(a.suffix, b.suffix);
}

/** Index of the first row not ordered below `target`. */
function lowerBound(
	ordered: readonly ResultRow[],
	compare: (a: ResultRow, b: ResultRow) => number,
	target: ResultRow,
): number {
	let lo = 0;
	let hi = ordered.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (compare(ordered[mid]!, target) < 0) lo = mid + 1;
		else hi = mid;
	}
	return lo;
}

/**
 * The rows a commit named for one scan scope, or undefined to mean "rescan".
 *
 * Rescanning is required when a watcher has no result to splice into, when any
 * touching pair is a wildcard, or when so many rows moved that the point reads
 * stop paying off. An empty list is the cheapest case of all: the commit
 * reached these watchers only through an INCLUDED entity, so their own rows are
 * untouched and nothing needs reading.
 */
function plannedKeys(
	group: readonly Watcher[],
	scoped: ScopeRows | undefined,
	pairs: readonly TouchedPair[],
): { workspaceId: string; key: string }[] | undefined {
	if (group.some((w) => w.matches === undefined)) return undefined;
	const sample = group[0]!;
	const keys: { workspaceId: string; key: string }[] = [];
	for (const pair of pairs) {
		if (pair.entity !== sample.entity) continue;
		if (!inScope(sample, pair.workspaceId)) continue;
		if (pair.keys === undefined) return undefined;
		for (const key of pair.keys) keys.push({ workspaceId: pair.workspaceId, key });
	}
	const size = scoped?.scanned;
	if (size !== undefined && keys.length > RESCAN_MIN_KEYS && keys.length > size * RESCAN_FRACTION) {
		return undefined;
	}
	return keys;
}

/** One changed row a commit named, with the merged view its writer attached. */
interface HandedRow {
	workspaceId: string;
	entity: string;
	key: string;
	view: MergedRow | undefined;
}

/**
 * Every row a commit named, with the view attached to it — or undefined as soon
 * as one came without a view, whether because the pair named no rows at all or
 * because the writer could not state that row's post-commit merged view.
 *
 * All or nothing: serving half a commit from handed-over rows and reading the
 * other half back would mix two views of the store into one update, and the
 * point of the handover is that there is no read to mix with.
 */
function handedRows(pairs: readonly TouchedPair[]): HandedRow[] | undefined {
	const out: HandedRow[] = [];
	for (const { workspaceId, entity, keys, views } of pairs) {
		if (keys === undefined || views === undefined) return undefined;
		for (const key of keys) {
			if (!views.has(key)) return undefined;
			out.push({ workspaceId, entity, key, view: views.get(key) });
		}
	}
	return out;
}

/**
 * Whether a commit reached a watcher through something other than its own
 * rows — a target it hydrates against through `include`. Such a change moves
 * the hydrated result while leaving the base rows alone, so nothing the match
 * set can report would notice it and the emit has to be forced.
 *
 * The watcher's own entity counts only outside its scope: inside it the
 * commit names base rows the match set reports on its own, while a
 * self-reference reached through a workspace-qualified include may point at
 * a row of the same entity in another workspace.
 */
function touchesInclude(watcher: Reach, pairs: readonly TouchedPair[]): boolean {
	if (watcher.include === undefined) return false;
	for (const t of pairs) {
		if (!watcher.reaches.has(t.entity)) continue;
		if (t.entity === watcher.entity) {
			if (!inScope(watcher, t.workspaceId) && watcher.anywhere?.has(t.entity)) return true;
			continue;
		}
		if (watcher.anywhere?.has(t.entity) || inScope(watcher, t.workspaceId)) return true;
	}
	return false;
}

/**
 * The rows of one target entity a commit named, inside the watcher's scope, or
 * undefined when a pair reached it without naming its rows — which is the
 * whole set as far as anything downstream can tell, so the caller has to ask
 * the entity rather than update from a list.
 */
function touchedTargets(
	watcher: Watcher,
	entity: string,
	pairs: readonly TouchedPair[],
): TouchedTarget[] | undefined {
	const out: TouchedTarget[] = [];
	for (const pair of pairs) {
		if (pair.entity !== entity || !inScope(watcher, pair.workspaceId)) continue;
		if (pair.keys === undefined) return undefined;
		for (const key of pair.keys) out.push({ workspaceId: pair.workspaceId, key });
	}
	return out;
}

/**
 * Whether a commit touched a target entity of a reference the watcher's query
 * crosses. Unlike touchesInclude the watcher's own entity is NOT excluded: a
 * self-reference makes the base entity its own target, and a commit to it
 * moves the membership keys exactly like any other target commit.
 */
function touchesRel(watcher: Watcher, pairs: readonly TouchedPair[]): boolean {
	const rel = watcher.rel;
	if (rel === undefined) return false;
	for (const t of pairs) {
		if (!rel.targets.has(t.entity)) continue;
		if (rel.anywhere.has(t.entity) || inScope(watcher, t.workspaceId)) return true;
	}
	return false;
}

/**
 * The page metadata one emit reports: the positions of its first and last row,
 * plus whether matches remain past the window.
 *
 * The window slices the match set by OFFSET even for a query carrying cursors —
 * the cursors bounded the set, `setWindow` moves inside what they left — so
 * `hasMore` is about the window's end and nothing else.
 */
function pageInfo(
	watcher: Watcher,
	rows: readonly Row<unknown>[],
	total: number,
	end: number,
	facets: FacetTable | undefined,
): PageInfo {
	// The maintained match set makes the count free, but a query that opted out
	// still gets undefined: what a watcher delivers must not depend on which
	// path — one-shot or maintained — happens to serve it.
	const info: PageInfo = {
		total: watcher.query?.total === false ? 'uncounted' : total,
		hasMore: end !== Infinity && total > end,
		...(facets === undefined ? {} : { facets }),
	};
	if (watcher.cursors === undefined || rows.length === 0) return info;
	const first = rows[0]!;
	const last = rows[rows.length - 1]!;
	info.startCursor = watcher.cursors.encode(watcher.entity, first.value, first.key);
	info.endCursor = watcher.cursors.encode(watcher.entity, last.value, last.key);
	return info;
}

/** One past the last result index the delivered page can see. */
function windowEnd(window: WatchWindow): number {
	return window.limit === undefined ? Infinity : (window.offset ?? 0) + window.limit;
}

/**
 * Whether the page `last` holds is still the page `ordered` gives, comparing
 * only from `keep` on — the caller has established that the rows before it are
 * the same objects (see Watcher.dirtyFrom).
 *
 * Identity is exact rather than merely cheap: a ResultRow survives only while
 * its bytes AND its version hold, and both matter — a version that moves under
 * identical bytes (an optimistic create acked to the same encoding) is what a
 * compare-and-swap guard reads.
 */
function sameTail(
	last: readonly ResultRow[],
	ordered: readonly ResultRow[],
	offset: number,
	keep: number,
	length: number,
): boolean {
	if (last.length !== length) return false;
	for (let i = keep; i < length; i++) {
		if (last[i] !== ordered[offset + i]) return false;
	}
	return true;
}

/**
 * Builds a row, reusing the previous one where the bytes held. Identical bytes
 * decode to an identical value, and the comparison is far cheaper than the
 * decode it avoids — cheaper too than trusting the row version, which would
 * serve wrong data on a stale hit.
 *
 * The Row wrapper is reused as well, but only when the version and the hidden
 * fields also held: version can move without the bytes moving (an optimistic
 * create acked to byte-identical confirmed bytes), and that difference is
 * exactly what a compare-and-swap guard reads. A field the server starts to
 * hide can leave the bytes as they were too, when its value was the zero
 * value.
 */
function resultRow(
	schema: DescMessage,
	prev: ResultRow | undefined,
	id: string,
	suffix: string,
	row: ScopedRow,
): ResultRow {
	if (prev !== undefined && bytesEqual(prev.bytes, row.v)) {
		const next = rowOf(schema, row.key, prev.row.value, row.workspaceId, row.version, row.hidden);
		if (prev.row.version === row.version && sameHidden(prev.row.hidden, next.hidden)) return prev;
		return { id, suffix, bytes: row.v, row: next };
	}
	const value = decodeRow(schema, row.v);
	return {
		id,
		suffix,
		bytes: row.v,
		row: rowOf(schema, row.key, value, row.workspaceId, row.version, row.hidden),
	};
}

/** Whether two rows hide the same fields. */
function sameHidden(a: readonly string[] | undefined, b: readonly string[] | undefined): boolean {
	if (a === b) return true;
	if (a === undefined || b === undefined || a.length !== b.length) return false;
	return a.every((name, i) => name === b[i]);
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
function suffixOf(
	watcher: { workspaceId?: string },
	workspaceId: string,
	key: string,
): string {
	return watcher.workspaceId === undefined ? `${wsSegment(workspaceId)}/${key}` : key;
}

/**
 * Identity of the rows a watcher draws on: its entity plus its workspace scope
 * as key segments — one, a comma-joined set, or `*` for the cross-workspace
 * scan. Watchers sharing this share a read and a decode cache.
 *
 * The three forms cannot collide. A segment is percent-encoded, so it never
 * holds the comma that joins a set; and `*` is not a legal workspace id (see
 * naming.ts) precisely because it would otherwise name the cross-workspace
 * scope of the same entity, leaving whichever watcher was sampled first to
 * decide the read both are then served from.
 */
function scanScope(
	watcher: { entity: string; workspaceId?: string; workspaces?: readonly string[] },
): string {
	const scope = watcher.workspaceId !== undefined
		? wsSegment(watcher.workspaceId)
		: watcher.workspaces?.map(wsSegment).join(',') ?? '*';
	return `${watcher.entity}\u001f${scope}`;
}

/**
 * The workspaces a watcher reads and reports coverage over, as a set of one, a
 * named set, or every workspace in the store. A set of one resolves to its
 * single-workspace form, so the scoped read path serves it unchanged.
 */
function readScope(
	watcher: { workspaceId?: string; workspaces?: readonly string[] },
): WorkspaceScope | undefined {
	return watcher.workspaceId ?? watcher.workspaces;
}

/**
 * Whether a workspace falls inside a watcher's scope — its own, one of the set
 * it named, or any at all for one spanning every workspace.
 */
function inScope(
	watcher: { workspaceId?: string; workspaces?: readonly string[] },
	workspaceId: string,
): boolean {
	if (watcher.workspaceId !== undefined) return watcher.workspaceId === workspaceId;
	return watcher.workspaces === undefined || watcher.workspaces.includes(workspaceId);
}

/**
 * The predicate one workspace's rows are tested against: the watcher's own,
 * unless it carries one chosen per workspace — and, for a query crossing
 * references, that base predicate rewritten row-local with the workspace's
 * membership keys from the last pre-pass (memoized until the keys move).
 */
function whereFor(watcher: Watcher, workspaceId: string): Cond | undefined {
	const base = watcher.predicates?.get(workspaceId) ?? watcher.query?.where;
	const rel = watcher.rel;
	if (rel === undefined || base === undefined) return base;
	if (!rel.whereByWs.has(workspaceId)) {
		rel.whereByWs.set(workspaceId, rewriteWithKeys(base, rel.keys ?? new Map(), workspaceId));
	}
	return rel.whereByWs.get(workspaceId);
}

/**
 * A live grouped read. It holds no rows and no match set: a commit touching the
 * entity re-runs the read, and `last` is what suppresses a callback that would
 * deliver the same answer again.
 */
/**
 * A watcher over a GROUPED read — counts, distinct values, or the head of each
 * group.
 *
 * It holds no match set and maintains nothing: a commit re-runs the read, and a
 * read whose answer is unchanged delivers nothing. The read itself is a closure
 * rather than a query on the watcher, because the shapes a grouped read answers
 * with have nothing in common past that rhythm — which is also why `same` comes
 * with it.
 */
interface GroupWatcher<R = unknown> {
	entity: string;
	workspaceId: string;
	/** Re-reads the answer inside the hub's shared read section. */
	read: (tx: KVRead) => Promise<R>;
	/** Whether two answers say the same thing, so an unchanged read is not
	 * delivered. */
	same: (a: R, b: R) => boolean;
	cb: (result: R) => void;
	onError?: (error: unknown) => void;
	/** The entities a commit must touch to reach this watcher — its own, since a
	 * grouped read crosses nothing. */
	reaches: Set<string>;
	/** The last delivered answer, or undefined before the first callback. */
	last?: R;
}

/** Identity of a row inside one scan scope. */
function rowId(workspaceId: string, key: string): string {
	return `${workspaceId}\u001f${key}`;
}

function reaches(watcher: Reach, touched: readonly TouchedPair[]): boolean {
	for (const t of touched) {
		if (!watcher.reaches.has(t.entity)) continue;
		// A reference followed by `include` points into the holder's own
		// workspace unless it is workspace-qualified, so a scoped watcher matches
		// only the workspaces it named — except for the entities such a
		// reference reaches, whose rows may sit in any workspace.
		if (watcher.anywhere?.has(t.entity) || inScope(watcher, t.workspaceId)) return true;
	}
	return false;
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

function cmp(a: string, b: string): number {
	return a < b ? -1 : a > b ? 1 : 0;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a === b) return true;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}
