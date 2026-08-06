// Structural identity for the values that decide whether a watch subscription
// must be torn down and re-established: a CompiledQuery and/or an IncludeSpec.
//
// compileQuery() runs the DSL lambdas every render and returns a fresh
// CompiledQuery object, so reference identity is useless. Instead we serialize
// the plain data to a stable string with object keys sorted, so two
// structurally-equal queries hash equal regardless of key order or object
// identity. `Scalar` includes `bigint` (and limit/offset are plain numbers),
// which JSON.stringify cannot encode, so a replacer tags bigints.

import type { CompiledQuery } from '../query/index.js';
import type { IncludeSpec } from '../refs.js';
import type { WatchWindow } from '../watch.js';

/**
 * Deterministic JSON of an arbitrary plain-data value with object keys sorted
 * and bigints encoded losslessly. Arrays keep their order (it is meaningful for
 * orderBy / `in` value lists); only object property order is normalized.
 */
function stableStringify(value: unknown): string {
	if (typeof value === 'bigint') return `"b:${value.toString()}"`;
	if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
	if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
	const entries = Object.entries(value as Record<string, unknown>)
		.filter(([ , v ]) => v !== undefined)
		.sort(([ a ], [ b ]) => (a < b ? -1 : a > b ? 1 : 0));
	return `{${entries.map(([ k, v ]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(',')}}`;
}

/**
 * Identity string for a watch subscription. Folds the entity key, the compiled
 * query (omitted for whole-entity watches) and the include spec into one stable
 * string. Equal strings ⇒ the same underlying subscription; a change forces a
 * resubscribe and (for the suspense cache) a distinct cache cell.
 */
export function stableHash(
	entityKey: string,
	query?: CompiledQuery,
	include?: IncludeSpec,
	window?: WatchWindow,
): string {
	return stableStringify({
		e: entityKey,
		q: query ?? null,
		i: include ?? null,
		w: window ? { o: window.offset ?? 0, l: window.limit ?? null } : null,
	});
}
