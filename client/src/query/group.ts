// Grouping a match set instead of paging it — the TS mirror of
// engine/query/group.go.
//
// A grouped read partitions the matching rows by the value at one path and
// answers with the groups: their values alone (a filter dropdown, a facet
// axis), or each with the number of rows in it (a board column's badge).
//
// # What a group IS
//
// One group per DISTINCT value the path resolves to across the match set, plus
// one group for the rows it resolves to nothing on. Groups come back in
// ascending value order with NaN and then the no-value group last — the order
// an ascending order key on the same path puts rows in (see orderRank in
// evaluate.ts), so the two surfaces never disagree about where an unset field
// sits.
//
// A path ending at a REPEATED scalar groups per ELEMENT: a row holding three
// labels joins three groups, and the counts therefore do not sum to the number
// of matching rows. That is what a label facet asks for.
//
// Only groups the rows produce exist. A status no row currently holds cannot
// come back as zero: a read over rows has no way to learn the value set, and
// that domain belongs to the app (an enum, a status entity) to join against
// these counts.

import type { DescMessage } from '@bufbuild/protobuf';

import type { Cond, Path, Scalar } from './ast.js';
import { compareValues, resolvePath, scalarKey } from './evaluate.js';

/** One group of a grouped read. */
export interface Group {
	/**
	 * The value its rows share at the grouping path, `undefined` for the group
	 * of rows that hold none.
	 */
	value: Scalar | undefined;
	/** Matching rows in the group; 0 for a distinct read, which counts none. */
	count: number;
}

/** A fully-materialized grouped query, independent of the typed DSL. */
export interface CompiledGroupQuery {
	/** Path the match set is partitioned by. */
	by: Path;
	/** Whether to count the rows of each group, or only name the groups. */
	distinct: boolean;
	where?: Cond;
	/** Maximum groups to return, counted from the first in value order. */
	limit?: number;
}

/**
 * One group a row belongs to. `found` is separate from `value` because a
 * grouping path can legitimately resolve to nothing, and that is a group of its
 * own rather than an absence.
 */
export interface GroupValue {
	found: boolean;
	value?: Scalar;
}

/** The no-value group, which every row missing the path joins. */
export const NO_GROUP: GroupValue = { found: false };

/**
 * The groups a row belongs to: one per element for a repeated path, exactly one
 * otherwise. Never empty — a row with no value at the path joins the no-value
 * group, and so does one holding an EMPTY list, which keeps it out of no group
 * at all.
 *
 * A value the path resolves to that is not a scalar (a message, a map, bytes)
 * has no group: nothing can identify it, so such a row joins the no-value group
 * the way a query on it evaluates false.
 */
export function groupValuesOf(row: unknown, by: Path, schema?: DescMessage): GroupValue[] {
	const got = resolvePath(row, by, schema);
	if (!got.found) return [ NO_GROUP ];
	if (Array.isArray(got.value)) {
		const out: GroupValue[] = [];
		const seen = new Set<string>();
		for (const el of got.value as unknown[]) {
			if (!isScalar(el)) continue;
			const gv: GroupValue = { found: true, value: el };
			const key = groupKeyOf(gv);
			// A row listing the same element twice occupies ONE index position, so
			// the definition has to collapse repeats the same way or the two would
			// disagree on that row's contribution.
			if (seen.has(key)) continue;
			seen.add(key);
			out.push(gv);
		}
		return out.length > 0 ? out : [ NO_GROUP ];
	}
	return isScalar(got.value) ? [ { found: true, value: got.value } ] : [ NO_GROUP ];
}

function isScalar(v: unknown): v is Scalar {
	const t = typeof v;
	return t === 'string' || t === 'number' || t === 'bigint' || t === 'boolean';
}

/**
 * Identifies a group for bucketing: two values that compare equal share a key,
 * and no two that do not. An integral number and a bigint of the same magnitude
 * are one group, for the reason equalsScalar crosses them.
 */
export function groupKeyOf(v: GroupValue): string {
	if (!v.found) return '\u0000missing';
	if (typeof v.value === 'number' && Number.isNaN(v.value)) return '\u0000nan';
	// ±Infinity, which scalarKey declines because a membership test must not
	// match one across types. A group is only ever compared with itself.
	return 'v' + (scalarKey(v.value) ?? `f${String(v.value)}`);
}

/**
 * Orders two groups the way an ascending order key on the same path orders
 * their rows: real values by value, then NaN, then the no-value group. Zero
 * means the two are one group.
 */
export function compareGroupValues(a: GroupValue, b: GroupValue): number {
	const ra = groupRank(a);
	const rb = groupRank(b);
	if (ra !== rb) return ra < rb ? -1 : 1;
	if (ra !== 0) return 0;
	return compareValues(a.value, b.value) ?? 0;
}

/** real < NaN < missing, compared before any direction — see orderRank. */
function groupRank(v: GroupValue): number {
	if (!v.found) return 2;
	if (typeof v.value === 'number' && Number.isNaN(v.value)) return 1;
	return 0;
}

/**
 * Accumulates groups by value while a read runs, and orders them once at the
 * end.
 *
 * Insertion order does not reach the result: the bucketing key decides which
 * contributions land in one group, and the comparator decides the order they
 * come out in, so a walk that visits groups in a different order than a scan
 * still produces the same answer.
 */
export class GroupBuckets {
	private byKey = new Map<string, Group>();

	constructor(private distinct: boolean) {}

	/** How many groups have been seen. */
	get size(): number {
		return this.byKey.size;
	}

	/** Adds a group's contribution. A distinct read drops the number — the group
	 * exists because a row put it there, and that is all it reports. */
	add(v: GroupValue, n: number): void {
		const key = groupKeyOf(v);
		const held = this.byKey.get(key);
		if (held !== undefined) {
			held.count += this.distinct ? 0 : n;
			return;
		}
		this.byKey.set(key, { value: v.found ? v.value : undefined, count: this.distinct ? 0 : n });
	}

	/** The groups in ascending value order, cut to `limit`, plus whether the cut
	 * left any out. */
	result(limit?: number): { groups: Group[]; hasMore: boolean } {
		const groups = [ ...this.byKey.values() ].sort((a, b) =>
			compareGroupValues(valueOf(a), valueOf(b))
		);
		if (limit !== undefined && groups.length > limit) {
			return { groups: groups.slice(0, limit), hasMore: true };
		}
		return { groups, hasMore: false };
	}
}

/** A finished group read back as the GroupValue it was bucketed under. */
function valueOf(g: Group): GroupValue {
	return g.value === undefined ? NO_GROUP : { found: true, value: g.value };
}
