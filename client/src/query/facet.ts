// The facets of a row query — Query.facets — as one fold over its match set.
//
// A facet is a grouped read (group.ts) answered from rows a query already
// holds: the same group values, the same bucketing key and the same order, so
// a facet and a groupCount over the same predicate never disagree. What differs
// is the walk. A grouped read walks for its groups; a facet is folded from the
// rows the query walked for its page, and a maintained result keeps it up to
// date from the rows a commit moved.

import type { DescMessage } from '@bufbuild/protobuf';

import type { Row } from '../read.js';
import type { CompiledFacet } from './ast.js';
import {
	compareGroupValues,
	groupKeyOf,
	type GroupValue,
	groupValuesOf,
	NO_GROUP,
} from './group.js';
import type { FacetGroup, FacetTable } from './types.js';

/**
 * The facet counts of a match set, maintained one row at a time.
 *
 * It is the fold behind Query.facets on every path: a one-shot read adds each
 * matching row once, and a maintained result adds the rows a commit brought in
 * and removes the ones it took out, so the counts track the match set at the
 * cost of the rows that moved. A group whose count reaches zero is dropped —
 * only values a row produces exist.
 *
 * Groups are keyed by value, and by workspace as well on an axis that keeps
 * workspaces apart, using the key a grouped read buckets under so that the two
 * surfaces agree on what one value is.
 */
export class FacetBuckets {
	private readonly axes: Map<string, FacetGroup>[];

	constructor(
		private readonly facets: readonly CompiledFacet[],
		private readonly schema?: DescMessage,
	) {
		this.axes = facets.map(() => new Map());
	}

	/** Adds (`n` = 1) or removes (`n` = -1) one row's contribution to every axis. */
	add(row: Row<unknown>, n: 1 | -1): void {
		for (let i = 0; i < this.facets.length; i++) {
			const facet = this.facets[i]!;
			const axis = this.axes[i]!;
			const ws = facet.perWorkspace ? row.workspaceId : undefined;
			for (const gv of groupValuesOf(row.value, facet.by, this.schema)) {
				const key = (ws === undefined ? '' : ws + '\u0000') + groupKeyOf(gv);
				const held = axis.get(key);
				if (held !== undefined) {
					held.count += n;
					if (held.count <= 0) axis.delete(key);
					continue;
				}
				if (n < 0) continue;
				const group: FacetGroup = { value: gv.found ? gv.value : undefined, count: n };
				if (ws !== undefined) group.workspaceId = ws;
				axis.set(key, group);
			}
		}
	}

	/**
	 * The facets as a result carries them: every axis under its name, its
	 * groups in ascending value order with the no-value group last, ties
	 * between workspaces broken by workspace id. Groups are copied out, so a
	 * later add() leaves a delivered table unchanged.
	 */
	result(): FacetTable {
		const out: Record<string, FacetGroup[]> = {};
		for (let i = 0; i < this.facets.length; i++) {
			const groups = [ ...this.axes[i]!.values() ].map((g) => ({ ...g }));
			groups.sort(compareFacetGroups);
			out[this.facets[i]!.name] = groups;
		}
		return out;
	}
}

function compareFacetGroups(a: FacetGroup, b: FacetGroup): number {
	const c = compareGroupValues(facetValueOf(a), facetValueOf(b));
	if (c !== 0) return c;
	const wa = a.workspaceId ?? '';
	const wb = b.workspaceId ?? '';
	return wa < wb ? -1 : wa > wb ? 1 : 0;
}

function facetValueOf(g: FacetGroup): GroupValue {
	return g.value === undefined ? NO_GROUP : { found: true, value: g.value };
}

/** The facets of a finished match set — FacetBuckets over `rows`, once. */
export function facetsOf(
	rows: readonly Row<unknown>[],
	facets: readonly CompiledFacet[],
	schema?: DescMessage,
): FacetTable {
	const buckets = new FacetBuckets(facets, schema);
	for (const row of rows) buckets.add(row, 1);
	return buckets.result();
}

/** Whether two facet tables carry the same groups with the same counts. */
export function sameFacets(a: FacetTable | undefined, b: FacetTable | undefined): boolean {
	if (a === b) return true;
	if (a === undefined || b === undefined) return false;
	const names = Object.keys(a);
	if (names.length !== Object.keys(b).length) return false;
	for (const name of names) {
		const x = a[name];
		const y = b[name];
		if (x === undefined || y === undefined || x.length !== y.length) return false;
		for (let i = 0; i < x.length; i++) {
			const p = x[i]!;
			const q = y[i]!;
			if (p.count !== q.count || p.workspaceId !== q.workspaceId) return false;
			if (p.value !== q.value && !(p.value !== p.value && q.value !== q.value)) return false;
		}
	}
	return true;
}
