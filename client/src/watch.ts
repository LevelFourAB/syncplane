import { type DescMessage, fromBinary, type MessageShape } from '@bufbuild/protobuf';

import { COMPLETE, type Coverage, type RowSet, withCoverage } from './coverage.js';
import { debugVerifyTouchesEnv } from './env.js';
import { wsSegment } from './keys.js';
import type { KVRead, KVStore } from './kv/store.js';
import type { EntityRegistry } from './mutations.js';
import type { Mutex } from './mutex.js';
import type { CompiledIndex } from './optimistic/index-maintain.js';
import { sortOrderedFields } from './ordering.js';
import { queryByIndex } from './query/execute.js';
import { compareRows, type CompiledQuery, evalCond, runQuery } from './query/index.js';
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
	/** The total order `matches.ordered` is in; see comparatorFor. */
	compare: (a: ResultRow, b: ResultRow) => number;
	/** Rows the query matches, maintained in place; undefined until first read. */
	matches?: MatchSet;
	/**
	 * The exact rows handed to the last callback. Compared by identity to catch
	 * the emits a changed-index test cannot rule out on its own — a rebuild that
	 * landed on the same page, or two dispatches racing to deliver one commit.
	 */
	lastWindow?: ResultRow[];
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
}

/**
 * Rows shared by every watcher of one scan scope — a decode cache keyed by row
 * id, pruned after each commit to the union of what those watchers match.
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
 * size, one prefix scan beats the point reads: each touched key costs two
 * `tx.get`s (readMergedRowWithVersion), while a scan amortizes the cursor over
 * the whole entity. The absolute floor keeps a small commit on the incremental
 * path even when the entity itself is tiny.
 *
 * A scope no scan has ever visited — one an index has served throughout — has
 * no size to compare against and stays on the point reads, which is the right
 * call: scanning to discover how big it is spends exactly what the index was
 * chosen to avoid.
 */
const RESCAN_MIN_KEYS = 8;
const RESCAN_FRACTION = 0.25;

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
	coverageOf?: (entity: string, workspaceId?: string) => Coverage;
	/**
	 * An entity's compiled secondary indexes, so a query watcher's first read
	 * can be served from one instead of scanned. Absent ⇒ every read scans,
	 * which is what a hub built without the client's index cache should do.
	 */
	indexes?: (entity: string) => CompiledIndex[];
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
 * an entity without its rows (a wholesale overlay clear, a bootstrap swap)
 * forces a rescan, and a first read falls back to one when no index covers the
 * query.
 *
 * Decoded values and Row wrappers are shared between the watchers of a scope
 * AND reused across commits, so callbacks must not mutate the rows they
 * receive — a mutation would leak into every later emit, not just the current
 * one.
 */
export class WatcherHub {
	private watchers = new Set<Watcher>();
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
		this.scopes.clear();
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
			compare: comparatorFor(schema, query),
			emittedAt: 0,
		};
		this.watchers.add(watcher);
		// Read the initial result asynchronously (also seeds the emit memo).
		void this.emitTo(watcher);
		return () => {
			this.watchers.delete(watcher);
			this.pruneScope(scanScope(watcher));
		};
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
			const scope = scanScope(w);
			const group = byScope.get(scope);
			if (group === undefined) byScope.set(scope, [ w ]);
			else group.push(w);
		}
		if (byScope.size === 0) return;
		const pending: { watcher: Watcher; from: number; force: boolean }[] = [];
		// One read tx per notify, shared across scopes and watchers, under a
		// shared lock so a concurrent commit cannot tear the reads. Match sets are
		// updated inside the section too: the mutex is the only thing ordering two
		// overlapping notifies, and a delta applied out of order would lose a row
		// change rather than merely deliver it late. Only the callback — and, for
		// an include watcher, the hydrate it needs — runs outside.
		const at = await this.mutex.runShared(async () => {
			const tx = await this.kv.read();
			try {
				for (const [ scope, group ] of byScope) {
					const delta = await this.readDelta(tx, scope, group, pairs);
					for (const w of group) {
						pending.push({
							watcher: w,
							from: applyDelta(w, delta),
							force: touchesInclude(w, pairs),
						});
					}
					if (this.verifyTouches) {
						for (const w of group) await this.verifyMatches(tx, w);
					}
					this.pruneScope(scope);
				}
				// Stamped inside the section: the order the mutex grants reads in
				// IS the order of the views they saw.
				return ++this.readSeq;
			} finally {
				tx.release();
			}
		});
		for (const p of pending) await this.emit(p.watcher, p.from, p.force, at);
	}

	/** Reads one watcher's result for the first time, from an index or a scan. */
	private async emitTo(watcher: Watcher): Promise<void> {
		if (!this.watchers.has(watcher)) return;
		const at = await this.mutex.runShared(async () => {
			const tx = await this.kv.read();
			try {
				watcher.matches = await this.seedFromIndex(tx, watcher)
					?? buildMatches(watcher, await this.scanScope(tx, watcher));
				if (this.verifyTouches) await this.verifyMatches(tx, watcher);
				this.pruneScope(scanScope(watcher));
				return ++this.readSeq;
			} finally {
				tx.release();
			}
		});
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
		for (const { workspaceId, key } of keys) {
			const id = rowId(workspaceId, key);
			const merged = await readMergedRowWithVersion(
				tx,
				this.registry,
				sample.entity,
				workspaceId,
				key,
			);
			if (merged === undefined) {
				scoped.rows.delete(id);
				changed.set(id, undefined);
				continue;
			}
			const row = resultRow(
				sample.schema,
				scoped.rows.get(id),
				id,
				suffixOf(sample, workspaceId, key),
				{ v: merged.v, version: merged.version, workspaceId, key },
			);
			scoped.rows.set(id, row);
			changed.set(id, row);
		}
		return { rebuild: false, changed };
	}

	/** Rebuilds a scope's shared rows from a full merged-view scan, in scan order. */
	private async scanScope(tx: KVRead, watcher: Watcher): Promise<ResultRow[]> {
		const merged = await scanMergedRows(tx, this.registry, watcher.entity, watcher.workspaceId);
		const scope = scanScope(watcher);
		const prev = this.scopes.get(scope)?.rows;
		const rows = new Map<string, ResultRow>();
		const ordered: ResultRow[] = [];
		for (const row of merged) {
			const id = rowId(row.workspaceId, row.key);
			const built = resultRow(
				watcher.schema,
				prev?.get(id),
				id,
				suffixOf(watcher, row.workspaceId, row.key),
				row,
			);
			rows.set(id, built);
			ordered.push(built);
		}
		this.scopes.set(scope, { rows, scanned: ordered.length });
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
	 */
	private async seedFromIndex(tx: KVRead, watcher: Watcher): Promise<MatchSet | undefined> {
		const indexes = this.opts.indexes;
		if (indexes === undefined || watcher.query === undefined) return undefined;
		if (watcher.workspaceId === undefined) return undefined;
		const def = this.registry.get(watcher.entity);
		if (def === undefined) return undefined;
		const found = await queryByIndex(
			tx,
			this.registry,
			def,
			watcher.schema,
			indexes(watcher.entity),
			{ where: watcher.query.where, orderBy: watcher.query.orderBy },
			watcher.workspaceId,
		);
		if (found === undefined) return undefined;
		const scoped = this.scopeRows(scanScope(watcher));
		const ordered = found.map((entry) => {
			const id = rowId(entry.row.workspaceId, entry.row.key);
			const prev = scoped.rows.get(id);
			const row = prev !== undefined && bytesEqual(prev.bytes, entry.bytes)
					&& prev.row.version === entry.row.version
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
		return { byId: new Map(ordered.map((r) => [ r.id, r ])), ordered };
	}

	/**
	 * Hands one watcher its current page, unless nothing the page can see moved.
	 *
	 * `from` is the lowest result index the commit disturbed (Infinity for none),
	 * which settles in one comparison whether a windowed watcher has to look at
	 * all: an insert, delete or move at or past `offset + limit` cannot change
	 * the rows before it. `force` bypasses the memo — the first emit, and an
	 * include watcher whose hydration targets moved where its own rows did not.
	 */
	private async emit(watcher: Watcher, from: number, force: boolean, at: number): Promise<void> {
		if (!this.watchers.has(watcher) || watcher.matches === undefined) return;
		if (at <= watcher.emittedAt) return;
		// Claimed before the callback (and before any hydrate), so a dispatch that
		// starts while this one is still hydrating knows it is the newer of the two.
		watcher.emittedAt = at;
		const coverage = this.opts.coverageOf?.(watcher.entity, watcher.workspaceId) ?? COMPLETE;
		const memoized = !force
			&& watcher.lastWindow !== undefined
			&& watcher.lastCoverage !== undefined
			&& sameCoverage(watcher.lastCoverage, coverage);
		const end = windowEnd(watcher.query);
		if (memoized && from >= end) return;
		const window = watcher.matches.ordered.slice(
			watcher.query?.offset ?? 0,
			end === Infinity ? undefined : end,
		);
		if (memoized && sameWindow(watcher.lastWindow!, window)) return;
		const rows = window.map((r) => r.row);
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
			watcher.lastCoverage = coverage;
			safeInvoke(watcher.cb, withCoverage(hydrated, coverage), 'watch callback');
			return;
		}
		watcher.lastWindow = window;
		watcher.lastCoverage = coverage;
		safeInvoke(watcher.cb, withCoverage(rows, coverage), 'watch callback');
	}

	private scopeRows(scope: string): ScopeRows {
		let scoped = this.scopes.get(scope);
		if (scoped === undefined) {
			scoped = { rows: new Map() };
			this.scopes.set(scope, scoped);
		}
		return scoped;
	}

	/**
	 * Drops rows no watcher of a scope holds, and the scope itself with its last
	 * watcher. This is what keeps the shared rows down to the union of the live
	 * results: a rescan puts the whole entity in the map, and everything the
	 * queries reject comes back out of it here.
	 */
	private pruneScope(scope: string): void {
		const held = new Set<string>();
		let live = false;
		for (const w of this.watchers) {
			if (scanScope(w) !== scope) continue;
			live = true;
			if (w.matches) { for (const id of w.matches.byId.keys()) held.add(id); }
		}
		if (!live) {
			this.scopes.delete(scope);
			return;
		}
		const scoped = this.scopes.get(scope);
		if (scoped === undefined) return;
		for (const id of scoped.rows.keys()) {
			if (!held.has(id)) scoped.rows.delete(id);
		}
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
		const merged = await scanMergedRows(tx, this.registry, watcher.entity, watcher.workspaceId);
		const bytes = new Map(merged.map((row) => [ rowId(row.workspaceId, row.key), row.v ]));
		const scanned: Row<unknown>[] = merged.map((row) => ({
			key: row.key,
			value: decodeRow(watcher.schema, row.v),
			workspaceId: row.workspaceId,
			version: row.version,
		}));
		// The definition, minus the page: a match set is the whole result, and the
		// offset/limit slice happens on emit.
		const want = watcher.query === undefined ? scanned : runQuery(
			scanned,
			{ where: watcher.query.where, orderBy: watcher.query.orderBy },
			watcher.schema,
		);
		const have = watcher.matches.ordered;
		const describe = (r: Row<unknown>): string => `${r.workspaceId}/${r.key}@${r.version}`;
		const fail = (why: string): never => {
			throw new Error(
				`maintained watch result diverged for scope ${scanScope(watcher)}: ${why}\n`
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

const EMPTY = new Uint8Array();

/** One (workspace, entity) pair a commit touched; `keys` undefined = wildcard. */
interface TouchedPair {
	workspaceId: string;
	entity: string;
	keys?: ReadonlySet<string>;
}

/**
 * Brings one watcher's match set up to date with a scope delta, returning the
 * lowest result index it disturbed (Infinity if it disturbed none).
 *
 * Nothing here reads the store, and nothing looks at a row the commit did not
 * name: whether a row matched before is what the match set already holds, and
 * where it sat is a binary search away.
 */
function applyDelta(watcher: Watcher, delta: ScopeDelta): number {
	if (delta.rebuild) {
		watcher.matches = buildMatches(watcher, delta.rows);
		return 0;
	}
	const matches = watcher.matches;
	// plannedKeys rebuilds rather than hand a keyed delta to a watcher with
	// nothing to splice into, so this is unreachable; it is not worth a throw.
	if (matches === undefined) return Infinity;
	let from = Infinity;
	for (const [ id, next ] of delta.changed) {
		const prev = matches.byId.get(id);
		// The same object means the same bytes, version and position.
		if (prev !== undefined && prev === next) continue;
		const admitted = next !== undefined && admits(watcher, next);
		if (prev === undefined && !admitted) continue;
		if (prev !== undefined) {
			let at = lowerBound(matches.ordered, watcher.compare, prev);
			// A fence, not a path: the comparator is a total order and `prev` is in
			// the array, so the search lands on it. Falling back to a linear scan
			// turns a comparator that somehow disagreed into slow rather than wrong.
			if (matches.ordered[at] !== prev) at = matches.ordered.indexOf(prev);
			if (at >= 0) {
				matches.ordered.splice(at, 1);
				if (at < from) from = at;
			}
			matches.byId.delete(id);
		}
		if (admitted) {
			const at = lowerBound(matches.ordered, watcher.compare, next!);
			matches.ordered.splice(at, 0, next!);
			matches.byId.set(id, next!);
			if (at < from) from = at;
		}
	}
	return from;
}

/** Derives a watcher's whole match set from a scope, given in scan order. */
function buildMatches(watcher: Watcher, rows: readonly ResultRow[]): MatchSet {
	const ordered = watcher.query?.where === undefined
		? rows.slice()
		: rows.filter((r) => admits(watcher, r));
	if (watcher.query?.orderBy && watcher.query.orderBy.length > 0) {
		ordered.sort(watcher.compare);
	}
	return { byId: new Map(ordered.map((r) => [ r.id, r ])), ordered };
}

/** Whether a watcher's predicate admits a row. */
function admits(watcher: Watcher, row: ResultRow): boolean {
	const where = watcher.query?.where;
	return where === undefined || evalCond(row.row.value, where, watcher.schema);
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
		if (sample.workspaceId !== undefined && sample.workspaceId !== pair.workspaceId) continue;
		if (pair.keys === undefined) return undefined;
		for (const key of pair.keys) keys.push({ workspaceId: pair.workspaceId, key });
	}
	const size = scoped?.scanned;
	if (size !== undefined && keys.length > RESCAN_MIN_KEYS && keys.length > size * RESCAN_FRACTION) {
		return undefined;
	}
	return keys;
}

/**
 * Whether a commit reached a watcher through something other than its own
 * entity — a target it hydrates against through `include`. Such a change moves
 * the hydrated result while leaving the base rows alone, so nothing the match
 * set can report would notice it and the emit has to be forced.
 */
function touchesInclude(watcher: Watcher, pairs: readonly TouchedPair[]): boolean {
	if (watcher.include === undefined) return false;
	for (const t of pairs) {
		if (t.entity === watcher.entity || !watcher.reaches.has(t.entity)) continue;
		if (watcher.workspaceId === undefined || watcher.workspaceId === t.workspaceId) return true;
	}
	return false;
}

/** One past the last result index the emitted page can see. */
function windowEnd(query?: CompiledQuery): number {
	return query?.limit === undefined ? Infinity : (query.offset ?? 0) + query.limit;
}

/**
 * Whether two pages hold the same rows. Identity is exact rather than merely
 * cheap: a ResultRow survives only while its bytes AND its version hold, and
 * both matter — a version that moves under identical bytes (an optimistic
 * create acked to the same encoding) is what a compare-and-swap guard reads.
 */
function sameWindow(a: readonly ResultRow[], b: readonly ResultRow[]): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * Builds a row, reusing the previous one where the bytes held. Identical bytes
 * decode to an identical value, and the comparison is far cheaper than the
 * decode it avoids — cheaper too than trusting the row version, which would
 * serve wrong data on a stale hit.
 *
 * The Row wrapper is reused as well, but only when the version also held:
 * version can move without the bytes moving (an optimistic create acked to
 * byte-identical confirmed bytes), and that difference is exactly what a
 * compare-and-swap guard reads.
 */
function resultRow(
	schema: DescMessage,
	prev: ResultRow | undefined,
	id: string,
	suffix: string,
	row: ScopedRow,
): ResultRow {
	if (prev !== undefined && bytesEqual(prev.bytes, row.v)) {
		if (prev.row.version === row.version) return prev;
		return { id, suffix, bytes: row.v, row: { ...prev.row, version: row.version } };
	}
	const value = decodeRow(schema, row.v);
	return {
		id,
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
 * (`*` for the cross-workspace scan). Two watchers sharing this share a read —
 * which is why `*` is not a legal workspace id (see naming.ts): a workspace
 * named `*` would collide with the cross-workspace scope of the same entity,
 * and whichever watcher was sampled first would decide the read both are then
 * served from.
 */
function scanScope(watcher: Watcher): string {
	return `${watcher.entity}\u001f${watcher.workspaceId ?? '*'}`;
}

/** Identity of a row inside one scan scope. */
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
