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
//   bounds them. Searching text is a match condition's job, served by the
//   term-postings index (searchindex.ts) rather than by this planner.
// - `not`, presence, and `match`. All become residual.
//
// Repeated fields DO plan, through the multi-entry indexes indexkey.ts
// describes: `has` over a repeated scalar and `any` over a repeated message
// each pin a component that fans out over that list, because "some element
// equals v" is "this row has an entry at v". Two consequences to keep in mind —
// one row can appear in the range several times, so a caller must dedupe by row
// key, and an ordering can only be served from components the equality prefix
// has already passed (see orderServedBy).
//
// A MAP fan-out plans the same way with one addition: an index may carry the
// entry KEY as its own component (`custom.*.key`) beside a value component
// (`custom.*.value.number`), in whichever order it declared them. Two rules keep
// the pair honest, and both live in planAgainst:
//
// - Components of one map fan must pin from the SAME term. Pinning `team` and
//   ranging on `priority`'s value would answer a question nobody asked, so the
//   first component of a fan to pin fixes the term the rest must use (`onlyFor`).
//   A term may only pin a key if it IMPLIES that entry is present: isNotSet is
//   satisfied by rows with no such entry, which sit outside the range entirely.
// - A range on a VALUE component only answers a term that named an entry key if
//   a key component pinned that key too. On its own the range spans the value
//   under EVERY key, so the term stays residual rather than returning rows where
//   a different field holds the value. That is why exactness is decided after
//   the walk (see mapKeysPinned) rather than at the reduction.
//
// The two layouts are complementary. `[key, value]` clusters one field's values,
// so it serves equality, ranges and an ordering within that field. `[value, key]`
// clusters one value, so it serves "which field holds X" — cross-key or, with
// the key pinned second, one named field — but equality only.
//
// The shared vectors in testdata/indexkey (`probeCases`) pin the decisions
// against the Go planner, because a planner that chose differently would still
// return correct rows — just at a different cost — and nothing else would
// notice the divergence.

import type { Cond, OrderKey, Path, Scalar } from './ast.js';
import { equalsScalar } from './evaluate.js';
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
	/**
	 * The encoded equality-pinned components every entry in the range shares. A
	 * range component is NOT part of it: only the components the range fixes to
	 * a single value are.
	 *
	 * It is what an ordered plan composes a seek bound from — appending a
	 * cursor's components to it names the position the page resumes at.
	 * Undefined when the plan produced several ranges, which share no single
	 * prefix.
	 */
	prefix?: string;
	/**
	 * Whether, within any ONE of `ranges`, every position belongs to a
	 * different row. True when each component that fans out sits inside the
	 * equality prefix: pinned, an element contributes one fixed value, so a row
	 * cannot spread across the unpinned tail (repeats of the pinned element
	 * encode to the SAME position, which the store keeps once — see
	 * kv/store.ts positionsOf). Across several ranges a row can still repeat.
	 *
	 * It is what lets a caller read a range's position count as a row count.
	 */
	distinctPerRange: boolean;
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
 * The term this component is restricted to, or -1 for no restriction. Only a
 * component that fans out over a map is restricted, and only once some other
 * component of that fan has pinned: the two read one entry, so they have to
 * describe the same one.
 */
function onlyFor(c: IndexComponent, mapFanTerm: number): number {
	return hasMapFan(c) ? mapFanTerm : -1;
}

/** Whether the component takes one value per ENTRY of a map, as opposed to per
 * element of a list or one value per row. */
function hasMapFan(c: IndexComponent): boolean {
	return c.fans.some((f) => f.isMap);
}

/**
 * Whether comps[i] and comps[j] read the same map entry: the same chain of
 * collections up to and including the innermost map. It is what pairs a key
 * component with the value components of its own fan, now that a declaration may
 * put them in either order and other components between them.
 */
function sameMapFan(a: IndexComponent, b: IndexComponent): boolean {
	const ma = a.fans.filter((f) => f.isMap).at(-1);
	const mb = b.fans.filter((f) => f.isMap).at(-1);
	if (ma === undefined || mb === undefined) return false;
	return a.path.slice(0, ma.at + 1).join('.') === b.path.slice(0, mb.at + 1).join('.');
}

/**
 * Whether every entry key a term named is also fixed by a key component of the
 * equality prefix.
 *
 * This is the condition under which a range on a map-fan VALUE component answers
 * its term outright. Without it the range spans that value under every key, so
 * rows where a DIFFERENT field holds the value fall inside it — and marking the
 * term exact would return them. A term that named no key (an any() over the map,
 * which asks about entries rather than one of them) needs nothing pinned.
 */
function mapKeysPinned(keys: Scalar[], pinnedKeys: Map<number, Scalar>): boolean {
	return keys.every((k) => [ ...pinnedKeys.values() ].some((got) => equalsScalar(got, k)));
}

/**
 * What one term contributes to one component: the values that pin it, which term
 * they came from, whether the range then answers that term, and the map entry
 * keys the term named — which mapKeysPinned turns into the final exactness
 * verdict once the whole prefix is known.
 */
interface Reduction {
	values: Scalar[];
	index: number;
	exact: boolean;
	keys: Scalar[];
	/**
	 * Which sub-condition of an any() this reduction consumed, and how many there
	 * are in all. A conjunction is answered only once EVERY conjunct is consumed
	 * — by whichever components happen to consume them, since components of one
	 * fan read a single element position and so describe one element between them.
	 */
	conjunct?: number;
	conjuncts?: number;
}

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
	// keep pinning.
	//
	// Two bookkeeping arrays, because "the walk has consumed this term" and "the
	// range answers this term" are no longer the same thing. `taken` stops a term
	// being reused by a later component; `used` is what drops it from the
	// residual, and for a term that named a map entry key it cannot be decided
	// until the whole prefix is known.
	const taken = new Array<boolean>(terms.length).fill(false);
	const exact = new Array<boolean>(terms.length).fill(false);
	// The entry keys an exact reduction depends on the prefix pinning. Absent for
	// a key-agnostic term, which the range answers outright.
	const needKeys = new Map<number, Scalar[]>();
	// The conjuncts of a term's any() the range has consumed, and how many there
	// are. Absent means the term is not a conjunction and `exact` decides alone.
	const covered = new Map<number, Set<number>>();
	const conjuncts = new Map<number, number>();
	let prefixes: string[] = [ '' ];
	let pinned = 0;
	// The term the components of this index's map fan are restricted to. The
	// first such component to pin fixes it; every later one must reduce the same
	// term, so `custom.a.number` and `custom.b.text` can never be read as one
	// pinned pair. Deliberately coarse — one map fan per index — which can
	// decline a plan a finer rule would allow but can never accept a wrong one.
	// pinnedKeys records what each key component chose, which is what lets an
	// ordering trust a value component of the same fan.
	let mapFanTerm = -1;
	const pinnedKeys = new Map<number, Scalar>();

	for (let ci = 0; ci < comps.length; ci++) {
		const c = comps[ci]!;
		const found = equalityValuesFor(c, terms, taken, onlyFor(c, mapFanTerm));
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
		if (hasMapFan(c) && mapFanTerm < 0) mapFanTerm = found.index;
		if (c.keyComponent) pinnedKeys.set(ci, found.values[0]!);
		// A term that only PARTLY reduces to this component (an any() with more
		// than one condition inside, a hasAll) still bounds the range, but the
		// range does not answer it — so it bounds and stays in the residual. A
		// term that named an entry key is left available too, because the key
		// component of the same fan still has to consume it.
		taken[found.index] = found.exact && found.keys.length === 0;
		if (found.conjuncts !== undefined) {
			const seen = covered.get(found.index) ?? new Set<number>();
			seen.add(found.conjunct!);
			covered.set(found.index, seen);
			conjuncts.set(found.index, found.conjuncts);
			if (found.keys.length > 0) needKeys.set(found.index, found.keys);
		} else if (found.exact) {
			exact[found.index] = true;
			needKeys.set(found.index, found.keys);
		}
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
		const bounds = inequalityBoundsFor(
			idx,
			pinned,
			terms,
			taken,
			onlyFor(comps[pinned]!, mapFanTerm),
		);
		if (bounds) {
			taken[bounds.index] = true;
			exact[bounds.index] = true;
			needKeys.set(bounds.index, bounds.keys);
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

	// Exactness, now that the prefix is known: a reduction onto a map-fan value
	// component only answers its term if the keys that term named are pinned too.
	const used = exact.map((e, ti) => {
		// A conjunction is answered only when every conjunct is: the rest would
		// otherwise have to be re-checked against the same element, and the range
		// cannot say which element it matched.
		const total = conjuncts.get(ti);
		const answered = total === undefined ? e : (covered.get(ti)?.size ?? 0) === total;
		return answered && mapKeysPinned(needKeys.get(ti) ?? [], pinnedKeys);
	});

	// An index that pins nothing and bounds nothing reads the whole thing, in a
	// different order than the rows are stored — strictly worse than a scan
	// unless it supplies the ordering.
	let ordered = orderServedBy(comps, pinned, orderBy, pinnedKeys);
	if (pinned === 0 && !ranged && !ordered) return undefined;

	// Several ranges cannot be read in order without a merge, so a caller that
	// needs the ordering has to be told the plan does not supply it.
	if (ranges.length > 1 && orderBy !== undefined && orderBy.length > 0) ordered = false;

	return {
		plan: {
			index: idx.name,
			ranges,
			residual: residualOf(terms, used),
			ordered,
			...(prefixes.length === 1 ? { prefix: prefixes[0]! } : {}),
			distinctPerRange: comps.every((c, ci) => c.fans.length === 0 || ci < pinned),
		},
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
	pinnedKeys: Map<number, Scalar>,
): boolean {
	if (orderBy === undefined || orderBy.length === 0) return false;
	if (pinned + orderBy.length > comps.length) return false;
	return orderBy.every((k, i) => {
		// A relevance rank is computed per row, so no stored component can
		// supply it.
		if (k.relevance) return false;
		const j = pinned + i;
		const c = comps[j]!;
		if (c.desc !== (k.dir === 'desc')) return false;
		const keys = matchFanPath(c, k.path);
		if (keys === undefined) return false;
		if (keys.length === 0) {
			// A LIST fan-out component holds one entry per element, so a row sits
			// at several places in this stretch of the index at once and walking
			// it yields no row order at all. Pinned it is harmless — the prefix
			// has already chosen the element — but past the prefix it is not an
			// ordering.
			return c.fans.length === 0;
		}
		// A MAP fan is different, and this is the whole reason the two cases are
		// distinguished: a map holds AT MOST ONE value per key, so once the entry
		// key is pinned the row occupies exactly one position here and the
		// component IS an ordering.
		return mapLeafPinned(comps, pinned, j, keys, pinnedKeys);
	});
}

/**
 * Whether comps[j] is the leaf of a map fan whose entry key the equality prefix
 * has already fixed to the key the order path names.
 *
 * The key component is looked up BY FAN among the pinned prefix rather than
 * assumed to sit at j-1: one declared path is one component now, so an index may
 * put its key component anywhere — before the value, after it, or with other
 * components in between.
 */
function mapLeafPinned(
	comps: IndexComponent[],
	pinned: number,
	j: number,
	keys: Scalar[],
	pinnedKeys: Map<number, Scalar>,
): boolean {
	const c = comps[j]!;
	if (c.fans.length !== 1 || !c.fans[0]!.isMap || keys.length !== 1) return false;
	for (let i = 0; i < pinned; i++) {
		if (!comps[i]!.keyComponent || !sameMapFan(comps[i]!, c)) continue;
		const got = pinnedKeys.get(i);
		if (got !== undefined && equalsScalar(got, keys[0]!)) return true;
	}
	return false;
}

const samePath = (a: Path, b: Path): boolean =>
	a.length === b.length && a.every((seg, i) => seg === b[i]);

/**
 * Whether a condition path addresses the same values a component fans out over,
 * returning the map keys it named. A `*` step in the component matches whatever
 * key the term spelled; everything else must be equal.
 *
 * The two paths are walked with SEPARATE cursors: a `*` is one segment of the
 * component but two of the term (the map, then the key), so the indices are
 * only aligned until the first fan.
 */
function matchFanPath(c: IndexComponent, term: Path): Scalar[] | undefined {
	return matchFan(c, term, c.path.length, true);
}

/**
 * matchFanPath where the term may continue past where the component stops —
 * which is how an entry-key component, whose path ends at the map, matches a
 * term that goes on into the entry's value.
 */
function matchFanPrefix(c: IndexComponent, term: Path, upto: number): Scalar[] | undefined {
	return matchFan(c, term, upto, false);
}

function matchFan(
	c: IndexComponent,
	term: Path,
	upto: number,
	exact: boolean,
): Scalar[] | undefined {
	const mapFans = new Map(c.fans.filter((f) => f.isMap).map((f) => [ f.at, f.keyKind ]));
	const keys: Scalar[] = [];
	let t = 0;
	for (let i = 0; i < upto; i++) {
		if (t >= term.length || c.path[i] !== term[t]) return undefined;
		t++;
		if (!mapFans.has(i)) continue;
		// The key is the NEXT term segment; the component has none, because `*`
		// is the shape and the key is the data.
		const raw = term[t];
		if (raw === undefined) return undefined;
		keys.push(typedMapKey(mapFans.get(i), raw));
		t++;
	}
	if (exact && t !== term.length) return undefined;
	return keys;
}

/** A record key as the value the encoder stores, so a probe encodes what a row
 * wrote. Mirrors sortedMapKeys in indexkey.ts. */
function typedMapKey(kind: string | undefined, raw: string): Scalar {
	switch (kind) {
		case 'bigint':
			return BigInt(raw);
		case 'number':
			return Number(raw);
		case 'bool':
			return raw === 'true';
		default:
			return raw;
	}
}

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
	taken: boolean[],
	only: number,
): Reduction | undefined {
	for (let i = 0; i < terms.length; i++) {
		if (taken[i] || (only >= 0 && i !== only)) continue;
		const t = terms[i]!;
		if (t.kind === 'exists' && mapFanMatches(c, t.path)) {
			// An any() over a map reduces onto EITHER side of the entry, so it is
			// dispatched before the key/value split below rather than inside it.
			const got = mapExistsEqualityFor(c, t);
			if (got) return { ...got, index: i };
			continue;
		}
		if (c.keyComponent) {
			const key = mapKeyEqualityFor(c, t);
			// The key is pinned by the term but NOT answered by it — the term
			// still has to constrain the value beside it — so it is left untaken
			// and falls to the residual if nothing consumes it.
			if (key !== undefined) return { values: [ key ], index: i, exact: false, keys: [] };
			continue;
		}
		if (t.kind === 'cmp' && t.op === 'eq') {
			const keys = matchFanPath(c, t.path);
			if (keys !== undefined) return { values: [ t.value ], index: i, exact: true, keys };
		}
		if (t.kind === 'in' && t.values.length > 0) {
			const keys = matchFanPath(c, t.path);
			if (keys !== undefined) return { values: t.values, index: i, exact: true, keys };
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

/** Whether a condition path addresses exactly what this component stores. */
function fanPathEq(c: IndexComponent, term: Path): boolean {
	return matchFanPath(c, term) !== undefined;
}

/**
 * Pins a map's entry-key component from any term that addresses one entry of it.
 *
 * The rule that makes this sound is that the term must IMPLY THE KEY IS PRESENT:
 * a range pinned to key k contains only rows holding an entry at k, so a term
 * that some row without that entry could satisfy would have its rows dropped.
 * Every operator qualifies except isNotSet, which is exactly the one satisfied
 * by a row that has no such entry.
 */
function mapKeyEqualityFor(c: IndexComponent, t: Cond): Scalar | undefined {
	const path = termPath(t);
	if (path === undefined) return undefined;
	const keys = matchFanPrefix(c, path, c.path.length);
	if (keys === undefined || keys.length === 0) return undefined;
	// The component ends AT its map fan, so the key it stores is the last one
	// the prefix captured.
	return keys[keys.length - 1];
}

/** The row path a term addresses, or undefined for a term that has none
 * (and/or/not) or that a row lacking the addressed value could satisfy. */
function termPath(t: Cond): Path | undefined {
	switch (t.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'rep':
		case 'exists':
			return t.path;
		case 'present':
			// isNotSet is true for a row with no such entry at all, so it cannot
			// pin the key: the range would drop exactly the rows it matches.
			return t.op === 'isSet' ? t.path : undefined;
		default:
			return undefined;
	}
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
): Omit<Reduction, 'index'> | undefined {
	// The component's value must BE the fanned element (a repeated scalar), and
	// the term must name the same path — with a literal key wherever the
	// component fans out over a map.
	const innermost = c.fans.at(-1);
	if (
		c.keyComponent || innermost === undefined || innermost.isMap
		|| innermost.at !== c.path.length - 1
	) {
		return undefined;
	}
	const keys = matchFanPath(c, t.path);
	if (keys === undefined) return undefined;
	switch (t.op) {
		case 'has':
			return { values: [ t.value ], exact: true, keys };
		case 'hasAny':
			return t.values.length > 0 ? { values: t.values, exact: true, keys } : undefined;
		case 'hasAll':
			return t.values.length > 0 ? { values: [ t.values[0]! ], exact: false, keys } : undefined;
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
): Omit<Reduction, 'index'> | undefined {
	const innermost = c.fans.at(-1);
	// any() scopes an element of a LIST; a map fan has no element scope of its
	// own, and its entries are addressed by key instead.
	if (c.keyComponent || innermost === undefined || innermost.isMap) return undefined;
	const keys = matchFan(c, t.path, innermost.at + 1, true);
	if (keys === undefined) return undefined;
	const rest = c.path.slice(innermost.at + 1);
	// A component storing the elements themselves has no sub-path for a
	// condition on element fields to match.
	if (rest.length === 0) return undefined;
	const reduce = (sub: Cond, exact: boolean): Omit<Reduction, 'index'> | undefined => {
		if (sub.kind === 'cmp' && sub.op === 'eq' && samePath(sub.path, rest)) {
			return { values: [ sub.value ], exact, keys };
		}
		if (sub.kind === 'in' && sub.values.length > 0 && samePath(sub.path, rest)) {
			return { values: sub.values, exact, keys };
		}
		return undefined;
	};
	const direct = reduce(t.cond, true);
	if (direct) return direct;
	if (t.cond.kind === 'and') {
		const conds = t.cond.conds;
		for (let j = 0; j < conds.length; j++) {
			const got = reduce(conds[j]!, false);
			if (got) return { ...got, conjunct: j, conjuncts: conds.length };
		}
	}
	return undefined;
}

/** The two segments a sub-condition inside a map entry may start at. They are
 * the same words an index component's `*` selector uses. */
const ENTRY_KEY = 'key';
const ENTRY_VALUE = 'value';

/**
 * Whether a component fans out over exactly the map an any() addresses. The
 * term's path ends AT the map with no key — that is what makes it a question
 * about entries rather than about one of them — so the comparison is against the
 * component's fan prefix.
 */
function mapFanMatches(c: IndexComponent, term: Path): boolean {
	const m = c.fans.filter((f) => f.isMap).at(-1);
	if (m === undefined) return false;
	return samePath(c.path.slice(0, m.at + 1), term);
}

/**
 * Reads an any() over a MAP as a probe on the components that fan out over that
 * map.
 *
 * The entry is ONE position, so a sub-condition on `value…` reduces onto a value
 * component of that fan and one on `key` onto its key component. When the
 * sub-condition conjoins both, the two components together answer the whole
 * existential — because they read one entry rather than two, which is exactly
 * what makes the explicit entry scope worth having.
 *
 * This is the cross-key case the `[value, key]` layout exists for: with no key
 * constraint at all, a value component alone answers "some entry holds X", so
 * the reduction names no keys and the exactness guard has nothing to require.
 */
function mapExistsEqualityFor(
	c: IndexComponent,
	t: Extract<Cond, { kind: 'exists' }>,
): Omit<Reduction, 'index'> | undefined {
	const m = c.fans.filter((f) => f.isMap).at(-1)!;
	// What this component reads, stated as a path from the entry.
	const want: Path = c.keyComponent
		? [ ENTRY_KEY ]
		: [ ENTRY_VALUE, ...c.path.slice(m.at + 1) ];
	const direct = reduceEntryCond(t.cond, want, true);
	if (direct) return direct;
	if (t.cond.kind === 'and') {
		const conds = t.cond.conds;
		for (let j = 0; j < conds.length; j++) {
			const got = reduceEntryCond(conds[j]!, want, false);
			if (got) return { ...got, conjunct: j, conjuncts: conds.length };
		}
	}
	return undefined;
}

/** Reduces one sub-condition of an any() over a map onto the entry path a
 * component reads. */
function reduceEntryCond(
	sub: Cond,
	want: Path,
	exact: boolean,
): Omit<Reduction, 'index'> | undefined {
	if (sub.kind === 'cmp' && sub.op === 'eq' && samePath(sub.path, want)) {
		return { values: [ sub.value ], exact, keys: [] };
	}
	if (sub.kind === 'in' && sub.values.length > 0 && samePath(sub.path, want)) {
		return { values: sub.values, exact, keys: [] };
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
 *
 * The map entry keys the bounding term named come back with the bounds, for the
 * same reason an equality reduction returns them: a range over one custom
 * field's values only answers the term if the key is pinned as well.
 */
function inequalityBoundsFor(
	idx: PlannableIndex,
	compIndex: number,
	terms: Cond[],
	taken: boolean[],
	only: number,
): { lo?: string; hi?: string; index: number; keys: Scalar[] } | undefined {
	const c = idx.encoder.components[compIndex]!;
	for (let i = 0; i < terms.length; i++) {
		if (taken[i] || (only >= 0 && i !== only)) continue;
		const t = terms[i]!;
		if (t.kind === 'cmp') {
			const keys = matchFanPath(c, t.path);
			if (keys !== undefined) {
				const bounds = cmpBounds(idx, compIndex, t.op, t.value);
				if (bounds) return { ...bounds, index: i, keys };
			}
		}
		if (t.kind === 'str' && t.op === 'startsWith') {
			const keys = matchFanPath(c, t.path);
			if (keys !== undefined) {
				const bounds = prefixBounds(c, t.value);
				if (bounds) return { ...bounds, index: i, keys };
			}
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
