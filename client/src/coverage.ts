import type { Subscription } from './gen/syncplane/v1/patch_pb.js';
import type { QueryCursor } from './query/cursor.js';
import type { Row, WorkspaceScope } from './read.js';

/**
 * Why a local result may not be the whole answer.
 *
 * - `unsubscribed` — the client holds NOTHING for this entity: the
 *   subscription set is non-empty and does not name it. A local query returns
 *   an empty array, which is indistinguishable from "there are no matching
 *   rows" unless you look here.
 * - `predicate` — only rows matching a subscription predicate, or named by a
 *   subscription's key set, synced.
 * - `window` — the synced set is capped by rank (see `limit`), so rows exist
 *   that no local query can reach no matter how it is written. In particular a
 *   client holding the top N by one order cannot answer a query ordered by
 *   another.
 * - `hydrating` — the first snapshot is still downloading and this entity's
 *   rows have not all landed yet. The only TRANSIENT bound: it clears on its
 *   own as the snapshot arrives, entity by entity, and the result grows without
 *   anything else changing. This is what tells "no issues" apart from "the
 *   issues have not arrived yet" while a cold client fills up — see
 *   SyncStatus.hydration for the workspace-wide view of the same thing.
 * - `relation` — the query crosses a reference (`matches`) and the target
 *   entity's local rows are not provably enough to answer it: a holder row
 *   whose target never synced evaluates against a missing row and reads as a
 *   non-match. Absent when the target entity's coverage is complete, or when
 *   every subscription scoping the queried entity follows the crossed ref
 *   path — then each locally-held holder's targets are in the follow closure.
 */
export type CoverageBound = 'unsubscribed' | 'predicate' | 'window' | 'hydrating' | 'relation';

/**
 * One reference a query crosses (a `matches` condition), as the coverage rule
 * needs it: which entity the targets belong to, and — when the ref is a
 * top-level field — the name a subscription's follow path would call it.
 */
export interface RelCoverageRef {
	targetEntity: string;
	/**
	 * The ref field's localName when the rel path is a single segment, the form
	 * follow-spec segments use. Undefined for a ref nested inside a message,
	 * which no follow path can name — only complete target coverage covers it.
	 */
	refLocalName?: string;
	/**
	 * Set for a workspace-qualified reference (see (syncplane.v1.reference)):
	 * its stubs may point into any workspace the client holds, so the target
	 * entity must be complete in every one of them, and a follow spec — which
	 * the server resolves inside one workspace — covers nothing.
	 */
	crossWorkspace?: boolean;
}

/** What the local store provably holds for one entity. */
export interface Coverage {
	/** true when the local store provably holds every row of this entity. */
	complete: boolean;
	/** Why not, empty when complete. */
	bounds: CoverageBound[];
	/**
	 * The row cap, when a window is the only thing SCOPING the entity. Absent
	 * when a predicate also bounds it, because then no single number describes
	 * how much is missing. `hydrating` does not clear it: the cap still says
	 * what the entity converges to, it just has not got there yet.
	 */
	limit?: number;
}

/**
 * A result array that also reports how much of the entity it could see, and how
 * many rows matched before it was paged.
 *
 * It IS an array — `.map`, `.length`, spreads and destructuring all behave
 * exactly as before, and both extras are non-enumerable so they stay out of
 * `JSON.stringify`, `Object.keys` and structural equality checks. The type is
 * the only thing that widened.
 */
export type RowSet<T, Total extends number | undefined = number> = Row<T>[] & {
	readonly coverage: Coverage;
	/**
	 * Matching rows in the LOCAL store before the page was cut — what `length`
	 * would have been without `offset`/`limit`, and what sizes a scroller over a
	 * paged result. Equal to `length` when the read was not paged, and undefined
	 * when the query opted out with `total: false`.
	 *
	 * Local is the operative word: it counts what `coverage` admits, not what
	 * the server holds. A result bounded by a subscription reports the rows this
	 * device could see, and reads that number as a workspace-wide total. A query
	 * carrying `after`/`before` counts the matches INSIDE those bounds, since a
	 * row outside them is not a match of that query at all.
	 */
	readonly total: Total;
	/**
	 * Positions of the first and last row, for paging on: pass `endCursor` back
	 * as `after` for the next page, `startCursor` as `before` for the previous
	 * one. Both undefined when the result is empty.
	 */
	readonly startCursor?: QueryCursor;
	readonly endCursor?: QueryCursor;
	/**
	 * Whether matches remain past the end `limit` counted from — after
	 * `endCursor` for a forward page, before `startCursor` for a backward one.
	 * False for a read with no limit, which returned everything it matched.
	 */
	readonly hasMore: boolean;
};

/** The coverage of a client that holds everything — full sync. */
export const COMPLETE: Coverage = Object.freeze({ complete: true, bounds: [] });

/**
 * The coverage of a result that has not arrived yet — the placeholder a React
 * hook returns before its first watch callback fires. Not provably complete
 * (nothing has been read), with no bound identified (none has been evaluated).
 */
export const UNKNOWN: Coverage = Object.freeze({ complete: false, bounds: [] });

/**
 * A readonly result array carrying its coverage and total — the shape the React
 * bridge hands to components, which must not mutate what they render.
 */
export type ReadonlyRowSet<T, Total extends number | undefined = number> = readonly Row<T>[] & {
	readonly coverage: Coverage;
	readonly total: Total;
	readonly startCursor?: QueryCursor;
	readonly endCursor?: QueryCursor;
	readonly hasMore: boolean;
};

/**
 * What a read knows about the page it cut, beyond the rows themselves.
 *
 * `total` defaults to `rows.length`, which is correct for every unpaged read; a
 * paged one passes what it counted, or `'uncounted'` for a query that opted out
 * with `total: false` — the one value that makes the result's total undefined.
 * The cursors are omitted by a read that cannot mint them (a cross-workspace
 * query, whose positions would not be comparable against one workspace's
 * ordering).
 */
export interface PageInfo {
	total?: number | 'uncounted';
	startCursor?: QueryCursor;
	endCursor?: QueryCursor;
	hasMore?: boolean;
}

/**
 * Attaches a coverage report and the page's own metadata to a result array
 * without disturbing it. Every added property is non-enumerable, so the array
 * still behaves as one everywhere a caller uses it.
 */
export function withCoverage<T>(
	rows: Row<T>[],
	coverage: Coverage,
	page: PageInfo = {},
): RowSet<T> {
	const hidden = (value: unknown) => ({
		value,
		enumerable: false,
		writable: false,
		configurable: true,
	});
	return Object.defineProperties(rows, {
		coverage: hidden(coverage),
		total: hidden(page.total === 'uncounted' ? undefined : page.total ?? rows.length),
		startCursor: hidden(page.startCursor),
		endCursor: hidden(page.endCursor),
		hasMore: hidden(page.hasMore ?? false),
	}) as RowSet<T>;
}

/**
 * Coverage of one entity under one workspace's subscription set.
 *
 * The subscriptions of an entity are a UNION — a row syncs if any of them
 * admits it — so one unbounded subscription makes the entity complete no
 * matter what the others say, and the bounds otherwise accumulate across them.
 *
 * `follow` is deliberately not a bound: it only ever ADDS rows beyond the base
 * scope, so it can make a result larger than the bounds suggest but never
 * smaller.
 */
export function coverageOfEntity(subs: readonly Subscription[], entity: string): Coverage {
	// An empty set is full sync.
	if (subs.length === 0) return COMPLETE;
	const own = subs.filter((s) => s.entity === entity);
	if (own.length === 0) return { complete: false, bounds: [ 'unsubscribed' ] };
	const bounds: CoverageBound[] = [];
	let limit = 0;
	for (const sub of own) {
		if (sub.window) {
			if (!bounds.includes('window')) bounds.push('window');
			limit += sub.window.limit;
			continue;
		}
		// A key set bounds the entity the way a predicate does: to a set of
		// rows no local query can see past, and one no single number describes.
		if (sub.predicate || sub.keySet) {
			if (!bounds.includes('predicate')) bounds.push('predicate');
			continue;
		}
		// Neither bound: this subscription alone covers the whole entity.
		return COMPLETE;
	}
	// A row cap only describes the whole entity when nothing else bounds it;
	// mixed with a predicate the total is unbounded and reporting a number
	// would understate what is missing.
	return onlyWindow(bounds) ? { complete: false, bounds, limit } : { complete: false, bounds };
}

/**
 * Whether a row cap describes the whole entity — a window and nothing else
 * scoping it. `hydrating` is ignored here because it is not a scope: it says
 * the client has not finished collecting what the scope admits, not that the
 * scope admits something a number cannot describe.
 */
function onlyWindow(bounds: readonly CoverageBound[]): boolean {
	const scope = bounds.filter((b) => b !== 'hydrating');
	return scope.length === 1 && scope[0] === 'window';
}

/**
 * Adds the transient `hydrating` bound to a scope-derived coverage: the rows
 * this entity will hold are still arriving, whatever the scope says about how
 * many that eventually is.
 */
function hydrating(base: Coverage): Coverage {
	if (base.bounds.includes('hydrating')) return base;
	const bounds: CoverageBound[] = [ ...base.bounds, 'hydrating' ];
	return base.limit === undefined
		? { complete: false, bounds }
		: { complete: false, bounds, limit: base.limit };
}

/**
 * Names the entities of one workspace whose rows are still arriving in the
 * first snapshot — see Coordinator.hydrationOf. An absent workspace, or an
 * absent set, holds a snapshot already (or holds one being replaced atomically,
 * which readers never see mid-flight).
 */
export type HydrationSource = (workspaceId: string) => ReadonlySet<string> | undefined;

/**
 * Resolves coverage against THIS tab's own declared scope rather than the
 * peer-widened union the coordinator actually syncs.
 *
 * The union is always a superset (a full-sync tab widens it for everyone), so
 * own-scope coverage errs toward claiming LESS completeness than reality — the
 * safe direction — and it does not flap as peer tabs come and go.
 *
 * Hydration is folded in on top of that: a scope that would be complete is not
 * complete yet while the rows it admits are still being downloaded for the
 * first time.
 */
export class CoverageResolver {
	constructor(
		private scopes: () => ReadonlyMap<string, readonly Subscription[]>,
		/** Absent ⇒ nothing is ever reported as hydrating (in-process facades, tests). */
		private hydration?: HydrationSource,
	) {}

	/**
	 * Coverage of an entity in one workspace, in a named set of them, or — with
	 * no scope — across every workspace the client holds. Anything spanning more
	 * than one is complete only if it is complete in each, and a set naming none
	 * is complete because it admits nothing.
	 *
	 * `rels` names the references the read crosses (a query using `matches`);
	 * each is checked per workspace and an uncovered one adds the `'relation'`
	 * bound. Omit for a row-local read.
	 */
	coverageOf(entity: string, scope?: WorkspaceScope, rels?: readonly RelCoverageRef[]): Coverage {
		const scopes = this.scopes();
		if (typeof scope === 'object') {
			return mergeCoverage([ ...new Set(scope) ].map((ws) => this.coverageOf(entity, ws, rels)));
		}
		const workspaceId = scope;
		if (workspaceId !== undefined) {
			const subs = scopes.get(workspaceId);
			const base = subs === undefined ? COMPLETE : coverageOfEntity(subs, entity);
			const hydrated = this.hydration?.(workspaceId)?.has(entity) ? hydrating(base) : base;
			return this.withRelations(hydrated, entity, workspaceId, rels);
		}
		return mergeCoverage(
			[ ...scopes.keys() ].map((ws) => this.coverageOf(entity, ws, rels)),
		);
	}

	/**
	 * Folds the `'relation'` bound into a coverage report when any crossed
	 * reference is not provably answerable from local target rows in this
	 * workspace. The limit is dropped with it: a row cap only describes the
	 * whole answer while a window is the only thing bounding it.
	 */
	private withRelations(
		base: Coverage,
		entity: string,
		workspaceId: string,
		rels: readonly RelCoverageRef[] | undefined,
	): Coverage {
		if (rels === undefined || rels.length === 0) return base;
		if (rels.every((rel) => this.relationCovered(entity, workspaceId, rel))) return base;
		return { complete: false, bounds: [ ...base.bounds, 'relation' ] };
	}

	/**
	 * Whether one crossed reference is provably answerable locally, per the
	 * rule stated on the `'relation'` bound. An entity the workspace does not
	 * subscribe to at all passes vacuously — the base result is empty, and its
	 * own bounds already say why.
	 */
	private relationCovered(entity: string, workspaceId: string, rel: RelCoverageRef): boolean {
		// A workspace-qualified reference points wherever its stubs say, so its
		// targets have to be complete in every workspace the client holds.
		if (rel.crossWorkspace) {
			return [ ...this.scopes().keys() ].every((ws) => this.targetComplete(ws, rel.targetEntity));
		}
		if (!this.targetComplete(workspaceId, rel.targetEntity)) {
			// A follow spec covers the targets of locally-held holders — unless
			// the target is still downloading, when nothing does yet.
			if (this.hydration?.(workspaceId)?.has(rel.targetEntity)) return false;
			const subs = this.scopes().get(workspaceId);
			if (subs === undefined || rel.refLocalName === undefined) return false;
			const own = subs.filter((s) => s.entity === entity);
			return own.every((s) =>
				s.follow?.paths.some((p) => p.segments[0] === rel.refLocalName) ?? false
			);
		}
		return true;
	}

	/**
	 * Whether one workspace provably holds every row of a target entity: not
	 * still downloading it, and either unsubscribed altogether — a workspace
	 * with no scope holds everything — or covering the entity completely.
	 */
	private targetComplete(workspaceId: string, target: string): boolean {
		// Targets still downloading answer stubs with rows that are not here
		// yet, whatever the scope promises.
		if (this.hydration?.(workspaceId)?.has(target)) return false;
		const subs = this.scopes().get(workspaceId);
		if (subs === undefined) return true;
		return coverageOfEntity(subs, target).complete;
	}
}

/**
 * Folds per-workspace coverage into one verdict for a cross-workspace read:
 * complete only where every workspace is, bounds unioned, and the limit summed
 * only while every workspace is window-bounded (otherwise there is no single
 * cap to report).
 */
function mergeCoverage(parts: readonly Coverage[]): Coverage {
	if (parts.length === 0) return COMPLETE;
	const bounds: CoverageBound[] = [];
	let complete = true;
	let limit: number | undefined = 0;
	for (const part of parts) {
		if (part.complete) continue;
		complete = false;
		for (const b of part.bounds) {
			if (!bounds.includes(b)) bounds.push(b);
		}
		if (part.limit === undefined || limit === undefined) limit = undefined;
		else limit += part.limit;
	}
	if (complete) return COMPLETE;
	return onlyWindow(bounds) && limit !== undefined
		? { complete, bounds, limit }
		: { complete, bounds };
}
