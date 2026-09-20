// The envelope every watch-backed hook reports: rows, plus whether they have
// arrived yet and whether the subscription behind them died.
//
// It is stamped onto the result ARRAY rather than wrapped in a `{ data, ... }`
// object, exactly as `coverage` already is: `rows.map(...)`, spreads,
// destructuring and `toEqual([])` all keep working, and a caller that never
// asks about loading never has to unwrap anything. `loading` and `error` are
// non-enumerable, so they stay out of JSON.stringify, Object.keys and
// structural comparisons — the same contract withCoverage() documents.

import { type Coverage, type ReadonlyRowSet, UNKNOWN, withCoverage } from '../coverage.js';
import type { FacetGroup, FacetTable } from '../query/index.js';
import type { Row } from '../read.js';

/** Whether a watch-backed result has arrived, and why it will not. */
export interface WatchState {
	/**
	 * True until the first callback arrives. This is what tells "loaded and
	 * empty" apart from "not loaded yet" — both are `[]`. Always false on a
	 * suspense hook, which waits for the first callback instead of reporting it.
	 */
	readonly loading: boolean;
	/**
	 * True while this entity's rows are still arriving in the client's first
	 * snapshot: the result is live and readable, just not all of it is here yet.
	 * It clears on its own, and the hook re-renders with the rest.
	 *
	 * The distinction `loading` cannot make. A cold client applies its snapshot
	 * entity by entity, so a result can be non-empty and still incomplete, and —
	 * the case that misleads a UI — it can be EMPTY because this entity has not
	 * started downloading. `loading || hydrating` is the honest "do not render
	 * an empty state yet" test; a suspense hook, which never reports loading,
	 * has only this one.
	 *
	 * A mirror of `coverage.bounds.includes('hydrating')`, stamped here so a
	 * component reaching for load state does not have to know about coverage.
	 */
	readonly hydrating: boolean;
	/**
	 * Set when the subscription itself could not be established, so no rows will
	 * ever arrive (see WatchOptions.onError). Rows already delivered are kept:
	 * what is on screen stops tracking the store rather than vanishing.
	 */
	readonly error: Error | undefined;
}

/** A row set that also reports whether it has loaded (see WatchState). */
export type WatchResult<
	T,
	Total extends number | undefined = number,
	Facets extends FacetTable | undefined = undefined,
> =
	& ReadonlyRowSet<T, Total, Facets>
	& WatchState;

/**
 * Stamps loading/hydrating/error onto a row set in place, mirroring
 * withCoverage. `hydrating` is derived from the coverage the row set already
 * carries, so the two can never disagree.
 */
export function withState<T>(
	rows: ReadonlyRowSet<T>,
	loading: boolean,
	error: Error | undefined,
): WatchResult<T> {
	return Object.defineProperties(rows, {
		// Configurable so re-stamping a row set (a re-delivered array) replaces
		// the state instead of throwing.
		loading: { value: loading, enumerable: false, configurable: true },
		hydrating: {
			value: rows.coverage.bounds.includes('hydrating'),
			enumerable: false,
			configurable: true,
		},
		error: { value: error, enumerable: false, configurable: true },
	}) as WatchResult<T>;
}

/**
 * One row per requested key, in the order they were asked for, with the load
 * state stamped on (see WatchState). There is always exactly one slot per key,
 * including before anything has been read; a slot is `undefined` where the key
 * names no row, or — while `loading` or `hydrating` — where its answer has not
 * come back yet.
 *
 * There is no coverage or total on it, unlike WatchResult: a key set is not a
 * result cut from a larger one, so there is nothing for either to report.
 */
export type KeyedRowsResult<T> = readonly (Row<T> | undefined)[] & WatchState;

/**
 * Stamps loading/hydrating/error onto a row-per-key array in place. `hydrating`
 * is passed rather than derived, since the array carries no coverage to derive
 * it from — it belongs to the entity behind the keys.
 */
export function withKeyedState<T>(
	rows: (Row<T> | undefined)[],
	loading: boolean,
	hydrating: boolean,
	error: Error | undefined,
): KeyedRowsResult<T> {
	return Object.defineProperties(rows, {
		loading: { value: loading, enumerable: false, configurable: true },
		hydrating: { value: hydrating, enumerable: false, configurable: true },
		error: { value: error, enumerable: false, configurable: true },
	}) as unknown as KeyedRowsResult<T>;
}

/**
 * A frozen empty result. Its coverage is UNKNOWN: nothing has been read, so
 * claiming either completeness or a specific bound would be an invention.
 */
export function emptyResult<T>(
	loading: boolean,
	error: Error | undefined,
	coverage: Coverage = UNKNOWN,
	facets?: FacetTable,
): WatchResult<T> {
	return Object.freeze(
		withState(
			withCoverage<T>([], coverage, facets === undefined ? {} : { facets }) as ReadonlyRowSet<T>,
			loading,
			error,
		),
	);
}

/**
 * The facets of a result that has no rows yet: every axis the query named,
 * each empty. A component indexing `facets.<axis>` then reads an empty list
 * before the first delivery rather than a property of undefined.
 */
export function emptyFacets(axes: readonly string[]): FacetTable {
	const out: Record<string, readonly FacetGroup[]> = {};
	for (const axis of axes) out[axis] = [];
	return out;
}

/** Normalizes whatever a failing subscription threw into an Error. */
export function asError(err: unknown): Error {
	return err instanceof Error ? err : new Error(String(err));
}
