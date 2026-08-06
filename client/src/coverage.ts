import type { Subscription } from './gen/syncplane/v1/patch_pb.js';
import type { Row } from './read.js';

/**
 * Why a local result may not be the whole answer.
 *
 * - `unsubscribed` — the client holds NOTHING for this entity: the
 *   subscription set is non-empty and does not name it. A local query returns
 *   an empty array, which is indistinguishable from "there are no matching
 *   rows" unless you look here.
 * - `predicate` — only rows matching a subscription predicate synced.
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
 */
export type CoverageBound = 'unsubscribed' | 'predicate' | 'window' | 'hydrating';

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
export type RowSet<T> = Row<T>[] & {
	readonly coverage: Coverage;
	/**
	 * Matching rows in the LOCAL store before `offset` and `limit` — what
	 * `length` would have been without them, and what sizes a scroller over a
	 * paged result. Equal to `length` when the read was not paged.
	 *
	 * Local is the operative word: it counts what `coverage` admits, not what
	 * the server holds. A result bounded by a subscription reports the rows this
	 * device could see, and reads that number as a workspace-wide total.
	 */
	readonly total: number;
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
export type ReadonlyRowSet<T> = readonly Row<T>[] & {
	readonly coverage: Coverage;
	readonly total: number;
};

/**
 * Attaches a coverage report and a match total to a result array without
 * disturbing it.
 *
 * @param total - Matches before paging. Defaults to `rows.length`, which is
 * correct for every unpaged read; a paged one must pass what it counted.
 */
export function withCoverage<T>(
	rows: Row<T>[],
	coverage: Coverage,
	total: number = rows.length,
): RowSet<T> {
	return Object.defineProperties(rows, {
		coverage: { value: coverage, enumerable: false, writable: false, configurable: true },
		total: { value: total, enumerable: false, writable: false, configurable: true },
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
		if (sub.predicate) {
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
	 * Coverage of an entity in one workspace, or — with no workspace — across
	 * every workspace the client holds, which is complete only if it is complete
	 * in each of them.
	 */
	coverageOf(entity: string, workspaceId?: string): Coverage {
		const scopes = this.scopes();
		if (workspaceId !== undefined) {
			const subs = scopes.get(workspaceId);
			const base = subs === undefined ? COMPLETE : coverageOfEntity(subs, entity);
			return this.hydration?.(workspaceId)?.has(entity) ? hydrating(base) : base;
		}
		return mergeCoverage(
			[ ...scopes.entries() ].map(([ ws, subs ]) => {
				const base = coverageOfEntity(subs, entity);
				return this.hydration?.(ws)?.has(entity) ? hydrating(base) : base;
			}),
		);
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
