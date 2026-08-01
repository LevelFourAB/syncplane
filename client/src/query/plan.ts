// Choosing an index for a query — the TS mirror of engine/query/plan.go.
//
// The planner answers one question: can this index produce the rows this query
// wants, in the order it wants, by reading a RANGE rather than everything? If
// yes it returns the range and whatever part of the predicate the range does
// not already guarantee (the residual, re-checked per row). If no it returns
// undefined and the caller scans.
//
// Returning nothing is always correct. That is the property the whole design
// leans on: an index is an optimization, the scan is the definition, and the
// two must agree row for row. So the planner is deliberately conservative — it
// recognises a small set of shapes exactly rather than a large set
// approximately, and anything it does not recognise becomes a residual or a
// declined plan.
//
// The rule it implements is the classic one for an ordered key-value store:
//
//     equality prefix  →  at most one range component  →  ordering suffix
//
// Leading components pinned to a single value by `eq` (or by `in`, which
// becomes k separate probes) collapse to a fixed prefix. The next component may
// take a range from an inequality, or supply the ordering. Components after
// that can only serve the ordering, because a range has already spread the
// entries out and nothing further is contiguous.
//
// What is NOT servable, and why:
//
// - `or` at the top level. Serving it means merging several ranges and
//   de-duplicating, which is a different execution shape; the scan handles it.
// - `contains` / `endsWith`. Neither is a prefix of anything, so no range
//   bounds them. Full-text search is a different mechanism entirely.
// - `not` and presence. Both become residual.
//
// Repeated fields DO plan, through the multi-entry indexes indexkey.ts
// describes: `has` over a repeated scalar and `any` over a repeated message
// each pin a component that fans out over that list, because "some element
// equals v" is "this row has an entry at v". Two consequences to keep in mind —
// one row can appear in the range several times, so a caller must dedupe by row
// key, and an ordering can only be served from components the equality prefix
// has already passed (see orderServedBy).
//
// The shared vectors in testdata/indexkey (`probeCases`) pin the decisions
// against the Go planner, because a planner that chose differently would still
// return correct rows — just at a different cost — and nothing else would
// notice the divergence.

import type { Cond, OrderKey, Path, Scalar } from './ast.js';
import { type IndexComponent, IndexEncoder, upperBound } from './indexkey.js';

/** A byte range over an index: `lo` inclusive, `hi` exclusive. An undefined
 * bound is open on that end. */
export interface IndexRange {
	lo?: string;
	hi?: string;
}

/** A chosen index access path. */
export interface QueryPlan {
	/** The index to read, by name. */
	index: string;
	/**
	 * The ranges to read, in order. Usually one; an `in` over the leading
	 * component produces several, and a caller serving an ORDERED query from
	 * several ranges must merge them rather than concatenate.
	 */
	ranges: IndexRange[];
	/**
	 * The part of the predicate the ranges do not guarantee, re-checked against
	 * each fetched row. Undefined means the range is exact.
	 */
	residual?: Cond;
	/**
	 * Whether reading the ranges in order yields rows in the query's requested
	 * order. When false the caller must still sort, and the index bought only a
	 * narrower candidate set.
	 */
	ordered: boolean;
}

/** One candidate index: its name and the encoder over its components. */
export interface PlannableIndex {
	name: string;
	encoder: IndexEncoder;
}

/**
 * Bounds the fan-out of an `in` over a leading component. Past it the probes
 * cost more than the scan they replace, and the plan is declined.
 */
const MAX_INDEX_PROBES = 64;

/**
 * Picks an index for a predicate plus an ordering, or returns undefined to mean
 * "scan". Candidates are considered in order; the first exact-ordered match
 * wins, failing that the one that narrows the most.
 *
 * A query with neither a predicate nor an ordering is a full scan in key order,
 * which no index improves on — the row keyspace IS that index — so it declines
 * rather than reading a second copy of the same thing.
 */
export function planQuery(
	candidates: PlannableIndex[],
	where: Cond | undefined,
	orderBy: OrderKey[] | undefined,
): QueryPlan | undefined {
	if (candidates.length === 0) return undefined;
	const terms = flattenAnd(where);
	if (terms.length === 0 && (orderBy === undefined || orderBy.length === 0)) return undefined;

	let best: QueryPlan | undefined;
	let bestScore = -1;
	for (const c of candidates) {
		const got = planAgainst(c, terms, orderBy);
		if (!got) continue;
		if (got.plan.ordered && got.score > 0) return got.plan;
		if (got.score > bestScore) {
			best = got.plan;
			bestScore = got.score;
		}
	}
	return best;
}

/** Tries one index. `score` is how many components the range pins. */
function planAgainst(
	idx: PlannableIndex,
	terms: Cond[],
	orderBy: OrderKey[] | undefined,
): { plan: QueryPlan; score: number } | undefined {
	const comps = idx.encoder.components;
	if (comps.length === 0) return undefined;

	// Walk the components left to right, consuming equality terms while they
	// keep pinning. `used` marks the terms the range accounts for, so whatever
	// is left becomes the residual.
	const used = new Array<boolean>(terms.length).fill(false);
	let prefixes: string[] = [ '' ];
	let pinned = 0;

	for (const c of comps) {
		const found = equalityValuesFor(c, terms, used);
		if (!found) break;
		const next: string[] = [];
		let ok = true;
		for (const p of prefixes) {
			for (const v of found.values) {
				const encoded = encodeLiteral(idx, pinned, v);
				if (encoded === undefined) {
					ok = false;
					break;
				}
				next.push(p + encoded);
			}
			if (!ok) break;
		}
		if (!ok) break;
		// A term that only PARTLY reduces to this component (an any() with more
		// than one condition inside, a hasAll) still bounds the range, but the
		// range does not answer it — so it bounds and stays in the residual.
		used[found.index] = found.exact;
		prefixes = next;
		pinned++;
		// An `in` fans the prefix set out multiplicatively; stop pinning before
		// that becomes a fan of probes larger than the scan it replaces.
		if (prefixes.length > MAX_INDEX_PROBES) return undefined;
	}

	// The component after the equality prefix may take a range from an
	// inequality. Only one can: past it the entries are no longer contiguous.
	let ranges: IndexRange[] | undefined;
	let ranged = false;
	if (pinned < comps.length) {
		const bounds = inequalityBoundsFor(idx, pinned, terms, used);
		if (bounds) {
			used[bounds.index] = true;
			ranged = true;
			ranges = prefixes.map((p) => ({
				lo: bounds.lo === undefined ? p : p + bounds.lo,
				hi: bounds.hi === undefined ? upperBound(p) : p + bounds.hi,
			}));
		}
	}
	if (!ranges) {
		ranges = prefixes.map((p) => ({ lo: p, hi: upperBound(p) }));
	}

	// An index that pins nothing and bounds nothing reads the whole thing, in a
	// different order than the rows are stored — strictly worse than a scan
	// unless it supplies the ordering.
	let ordered = orderServedBy(comps, pinned, orderBy);
	if (pinned === 0 && !ranged && !ordered) return undefined;

	// Several ranges cannot be read in order without a merge, so a caller that
	// needs the ordering has to be told the plan does not supply it.
	if (ranges.length > 1 && orderBy !== undefined && orderBy.length > 0) ordered = false;

	return {
		plan: { index: idx.name, ranges, residual: residualOf(terms, used), ordered },
		score: pinned,
	};
}

/**
 * Whether reading the index in order yields the query's requested order. The
 * equality prefix contributes nothing — every row in the range agrees on those
 * components — so the order keys must line up with the components that FOLLOW
 * it, in sequence, direction included.
 */
function orderServedBy(
	comps: IndexComponent[],
	pinned: number,
	orderBy: OrderKey[] | undefined,
): boolean {
	if (orderBy === undefined || orderBy.length === 0) return false;
	if (pinned + orderBy.length > comps.length) return false;
	return orderBy.every((k, i) => {
		const c = comps[pinned + i]!;
		// A fan-out component holds one entry per element, so a row sits at
		// several places in this stretch of the index at once and walking it
		// yields no row order at all. Pinned it is harmless — the prefix has
		// already chosen the element — but past the prefix it is not an ordering,
		// and an order key can never name a repeated path anyway.
		if (c.multiAt >= 0) return false;
		return c.desc === (k.dir === 'desc') && samePath(c.path, k.path);
	});
}

const samePath = (a: Path, b: Path): boolean =>
	a.length === b.length && a.every((seg, i) => seg === b[i]);

/**
 * A term pinning this component to one or more values, and whether the range
 * then answers the term outright.
 *
 * A term that is not exact still narrows: the values it yields are a superset of
 * what it matches, so the range is a superset of the rows, and the term stays in
 * the residual to reject the rest. That is what lets a hasAll or a
 * multi-condition any() use an index at all.
 */
function equalityValuesFor(
	c: IndexComponent,
	terms: Cond[],
	used: boolean[],
): { values: Scalar[]; index: number; exact: boolean } | undefined {
	for (let i = 0; i < terms.length; i++) {
		if (used[i]) continue;
		const t = terms[i]!;
		if (t.kind === 'cmp' && t.op === 'eq' && samePath(t.path, c.path)) {
			return { values: [ t.value ], index: i, exact: true };
		}
		if (t.kind === 'in' && t.values.length > 0 && samePath(t.path, c.path)) {
			return { values: t.values, index: i, exact: true };
		}
		if (t.kind === 'rep') {
			const got = repEqualityFor(c, t);
			if (got) return { ...got, index: i };
		}
		if (t.kind === 'exists') {
			const got = existsEqualityFor(c, t);
			if (got) return { ...got, index: i };
		}
	}
	return undefined;
}

/**
 * Reads membership over a repeated scalar as a probe on a component that fans
 * out over that same list — "some element equals v" is exactly "this row has an
 * entry at v".
 *
 * hasAll pins on the FIRST value only. The rows holding all of the values are a
 * subset of the rows holding that one, so one narrow range plus a residual beats
 * a union of ranges over every value.
 */
function repEqualityFor(
	c: IndexComponent,
	t: Extract<Cond, { kind: 'rep' }>,
): { values: Scalar[]; exact: boolean } | undefined {
	if (c.multiAt !== c.path.length - 1 || !samePath(t.path, c.path)) return undefined;
	switch (t.op) {
		case 'has':
			return { values: [ t.value ], exact: true };
		case 'hasAny':
			return t.values.length > 0 ? { values: t.values, exact: true } : undefined;
		case 'hasAll':
			return t.values.length > 0 ? { values: [ t.values[0]! ], exact: false } : undefined;
		default:
			return undefined;
	}
}

/**
 * Reads an any() as a probe on a component that fans out over the same list: the
 * component's entries hold one value per element, so pinning one is "some
 * element has this value" — the existential itself.
 *
 * Only the sub-condition's own equality reduces. When it is a conjunction the
 * range pins one conjunct and the whole any() stays residual, because the rest
 * has to be re-checked against the SAME element and the range cannot say which
 * element it matched.
 */
function existsEqualityFor(
	c: IndexComponent,
	t: Extract<Cond, { kind: 'exists' }>,
): { values: Scalar[]; exact: boolean } | undefined {
	if (c.multiAt < 0 || !samePath(c.path.slice(0, c.multiAt + 1), t.path)) return undefined;
	const rest = c.path.slice(c.multiAt + 1);
	// A component storing the elements themselves has no sub-path for a
	// condition on element fields to match.
	if (rest.length === 0) return undefined;
	const reduce = (sub: Cond, exact: boolean): { values: Scalar[]; exact: boolean } | undefined => {
		if (sub.kind === 'cmp' && sub.op === 'eq' && samePath(sub.path, rest)) {
			return { values: [ sub.value ], exact };
		}
		if (sub.kind === 'in' && sub.values.length > 0 && samePath(sub.path, rest)) {
			return { values: sub.values, exact };
		}
		return undefined;
	};
	const direct = reduce(t.cond, true);
	if (direct) return direct;
	if (t.cond.kind === 'and') {
		for (const sub of t.cond.conds) {
			const got = reduce(sub, false);
			if (got) return got;
		}
	}
	return undefined;
}

/**
 * A term bounding this component from one or both sides. Only a single term is
 * consumed: two inequalities on one component (a BETWEEN) would need both, and
 * the second stays residual — correct if not optimal.
 *
 * `startsWith` is an inequality in disguise: every string with a given prefix
 * occupies one contiguous range, which is what the encoding's terminator
 * guarantees.
 */
function inequalityBoundsFor(
	idx: PlannableIndex,
	compIndex: number,
	terms: Cond[],
	used: boolean[],
): { lo?: string; hi?: string; index: number } | undefined {
	const c = idx.encoder.components[compIndex]!;
	for (let i = 0; i < terms.length; i++) {
		if (used[i]) continue;
		const t = terms[i]!;
		if (t.kind === 'cmp' && samePath(t.path, c.path)) {
			const bounds = cmpBounds(idx, compIndex, t.op, t.value);
			if (bounds) return { ...bounds, index: i };
		}
		if (t.kind === 'str' && t.op === 'startsWith' && samePath(t.path, c.path)) {
			const bounds = prefixBounds(c, t.value);
			if (bounds) return { ...bounds, index: i };
		}
	}
	return undefined;
}

/**
 * Encoded bounds for one inequality.
 *
 * Written against the ASCENDING encoding and then flipped for a desc
 * component, rather than written twice: `x > v` selects the entries above v
 * ascending and the entries below it descending, and getting that backwards
 * silently returns the complement of the answer.
 */
function cmpBounds(
	idx: PlannableIndex,
	compIndex: number,
	op: string,
	value: Scalar,
): { lo?: string; hi?: string } | undefined {
	const v = encodeLiteral(idx, compIndex, value);
	if (v === undefined) return undefined;
	// Every real value carries the present tag, so the bounds stay inside the
	// real values and never reach the NaN or missing tags — which is right: a
	// comparison is false against both.
	const lowest = TAG_PRESENT;
	const above = upperBound(v);
	const top = upperBound(lowest);
	const desc = idx.encoder.components[compIndex]!.desc;

	if (!desc) {
		switch (op) {
			case 'gt':
				return { lo: above, hi: top };
			case 'gte':
				return { lo: v, hi: top };
			case 'lt':
				return { lo: lowest, hi: v };
			case 'lte':
				return { lo: lowest, hi: above };
		}
		return undefined;
	}
	// Under desc the payload order is reversed, so "everything above v"
	// ascending is "everything below v" descending. Re-derive rather than
	// reversing the encoded strings, which is not the same operation.
	switch (op) {
		case 'gt':
			return { lo: lowest, hi: v };
		case 'gte':
			return { lo: lowest, hi: above };
		case 'lt':
			return { lo: above, hi: top };
		case 'lte':
			return { lo: v, hi: top };
	}
	return undefined;
}

/** The present tag, as a one-character key — the floor of every real value. */
const TAG_PRESENT = '\u0001';

/**
 * Bounds for a string prefix. The prefix's own ENTRY encoding is not a bound —
 * it carries a terminator, so it would match only the exact string — so the
 * prefix is encoded unterminated and the byte range above it taken.
 */
function prefixBounds(c: IndexComponent, prefix: string): { lo?: string; hi?: string } | undefined {
	if (c.kind !== 'string') return undefined;
	// Encode the prefix through a one-component string index and strip both the
	// tag and the two-byte terminator, leaving the raw payload.
	const raw = new IndexEncoder([ { ...c, desc: false } ])
		.encodePrefix([ prefix ])
		.slice(1, -2);
	if (!c.desc) {
		const lo = TAG_PRESENT + raw;
		return { lo, hi: upperBound(lo) };
	}
	const inv = invert(raw);
	return { lo: TAG_PRESENT + (upperBound(inv) ?? ''), hi: upperBound(TAG_PRESENT + inv) };
}

const invert = (s: string): string => {
	let out = '';
	for (let i = 0; i < s.length; i++) out += String.fromCharCode(~s.charCodeAt(i) & 0xff);
	return out;
};

/** Encodes one literal as this component's payload, tag and inversion
 * included — matching what the encoder writes for a row holding that value. */
function encodeLiteral(
	idx: PlannableIndex,
	compIndex: number,
	value: Scalar,
): string | undefined {
	const c = idx.encoder.components[compIndex]!;
	const coerced = coerceLiteral(c, value);
	if (coerced === undefined) return undefined;
	try {
		return new IndexEncoder([ c ]).encodePrefix([ coerced ]);
	} catch {
		return undefined;
	}
}

/**
 * Converts a query literal into the shape the column stores, mirroring what
 * compareValues does across the same kind pair. Undefined means no exact
 * conversion exists, which leaves the term residual — and a residual gives the
 * scan-equivalent answer, so declining here is always safe.
 */
function coerceLiteral(c: IndexComponent, v: Scalar): Scalar | undefined {
	switch (c.kind) {
		case 'string':
			return typeof v === 'string' ? v : undefined;
		case 'bool':
			return typeof v === 'boolean' ? v : undefined;
		case 'number':
			if (typeof v === 'number') return Number.isNaN(v) ? undefined : v;
			// A bigint literal against a number column compares as Number, with
			// the same precision loss `Number(bigint)` has — which is what the
			// evaluator does too, so the bound and the re-check agree.
			return typeof v === 'bigint' ? Number(v) : undefined;
		case 'bigint':
			if (typeof v === 'bigint') return fitsColumn(c, v) ? v : undefined;
			if (typeof v !== 'number' || !Number.isInteger(v)) {
				// A non-integral literal falls between two integers, so no stored
				// value equals it and no exact bound exists at this component.
				return undefined;
			}
			return fitsColumn(c, BigInt(v)) ? BigInt(v) : undefined;
	}
}

/**
 * Rejects a literal outside the column's 64-bit range. The encoder masks such a
 * value into range, which would place the bound somewhere arbitrary rather than
 * outside every entry.
 */
function fitsColumn(c: IndexComponent, v: bigint): boolean {
	if (c.unsigned) return v >= 0n && v < 1n << 64n;
	return v >= -(1n << 63n) && v < 1n << 63n;
}

/**
 * Splits a predicate into the terms a range may consume independently. Only a
 * top-level AND splits: inside an `or` or a `not` a term does not have to hold
 * for a matching row, so treating one as a bound would drop rows.
 */
function flattenAnd(where: Cond | undefined): Cond[] {
	if (!where) return [];
	return where.kind === 'and' ? [ ...where.conds ] : [ where ];
}

/** Rebuilds the predicate from the terms the range does not guarantee.
 * Undefined means the range is exact. */
function residualOf(terms: Cond[], used: boolean[]): Cond | undefined {
	const left = terms.filter((_, i) => !used[i]);
	if (left.length === 0) return undefined;
	if (left.length === 1) return left[0];
	return { kind: 'and', conds: left };
}
