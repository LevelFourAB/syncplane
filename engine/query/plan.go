package query

import (
	"math/big"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Choosing an index for a query.
//
// The planner answers one question: can this index produce the rows this query
// wants, in the order it wants, by reading a RANGE rather than everything? If
// yes it returns the range and whatever part of the predicate the range does
// not already guarantee (the residual, re-checked per row). If no it returns
// nothing and the caller scans.
//
// Returning nothing is always correct. That is the property the whole design
// leans on: an index is an optimization, the scan is the definition, and the
// two must agree row for row. So the planner is deliberately conservative —
// it recognises a small set of shapes exactly rather than a large set
// approximately, and anything it does not recognise becomes a residual or a
// declined plan.
//
// The rule it implements is the classic one for an ordered key-value store:
//
//	equality prefix   →   at most one range component   →   ordering suffix
//
// Leading components pinned to a single value by `eq` (or by `in`, which
// becomes k separate probes) collapse to a fixed prefix. The next component may
// take a range from an inequality, or supply the ordering. Components after
// that can only serve the ordering, because a range has already spread the
// entries out and nothing further is contiguous.
//
// What is NOT servable, and why:
//
//   - `or` at the top level. Serving it means merging several ranges and
//     de-duplicating, which is a different execution shape; the scan handles it.
//   - `contains` / `endsWith`. Neither is a prefix of anything, so no range
//     bounds them. Full-text search is a different mechanism entirely.
//   - `not` and presence. Both become residual.
//
// Repeated fields DO plan, through the multi-entry indexes indexkey.go
// describes: `has` over a repeated scalar and `any` over a repeated message
// each pin a component that fans out over that list, because "some element
// equals v" is "this row has an entry at v". Two consequences to keep in mind
// — one row can appear in the range several times, so a caller must dedupe by
// row key, and an ordering can only be served from components the equality
// prefix has already passed (see orderServedBy).
//
// A MAP fan-out plans the same way with one addition: an index may carry the
// entry KEY as its own component (`custom.*.key`) beside a value component
// (`custom.*.value.number`), in whichever order it declared them. Two rules keep
// the pair honest, and both live in planAgainst:
//
//   - Components of one map fan must pin from the SAME term. Pinning `team` and
//     ranging on `priority`'s value would answer a question nobody asked, so the
//     first component of a fan to pin fixes the term the rest must use (`only`).
//     A term may only pin a key if it IMPLIES that entry is present: isNotSet is
//     satisfied by rows with no such entry, which sit outside the range entirely.
//   - A range on a VALUE component only answers a term that named an entry key
//     if a key component pinned that key too. On its own the range spans the
//     value under EVERY key, so the term stays residual rather than returning
//     rows where a different field holds the value. That is why exactness is
//     decided after the walk (see mapKeysPinned) rather than at the reduction.
//
// The two layouts are complementary. `[key, value]` clusters one field's values,
// so it serves equality, ranges and an ordering within that field. `[value, key]`
// clusters one value, so it serves "which field holds X" — cross-key or, with
// the key pinned second, one named field — but equality only.
//
// A single-component index still serves an ordering plus a limit: walk it in
// order, re-check the residual per candidate, stop once the limit is filled.
// That is bounded by how many candidates the residual rejects, not by the
// limit, which is the honest cost of the easy declaration surface and the
// reason a composite index exists.

// Plan is a chosen index access path.
type Plan struct {
	// Index names the index to read, matching the name the caller registered.
	Index string
	// Ranges are the byte ranges to read, in the order they must be read.
	// Usually one; an `in` over the leading component produces several, and a
	// caller serving an ORDERED query from several ranges must merge them
	// rather than concatenate.
	Ranges []Range
	// Residual is the part of the predicate the ranges do not guarantee, to be
	// re-checked against each fetched row. Nil means the range is exact and no
	// re-check is needed.
	Residual *Compiled
	// Ordered reports whether reading the ranges in order yields rows in the
	// query's requested order. When false the caller must still sort, and the
	// index bought only a narrower candidate set.
	Ordered bool
	// Prefix is the encoded equality-pinned components every entry in the range
	// shares. A range component is NOT part of it: only the components the
	// range fixes to a single value are.
	//
	// It is what an Ordered plan composes a seek bound from — appending a
	// cursor's components (Order.CursorPrefix) to it names the position in the
	// index the page resumes at. Nil when the plan produced several ranges,
	// which share no single prefix, and empty when it pinned nothing.
	Prefix []byte
}

// Range is a half-open byte range over an index: Lo inclusive, Hi exclusive. A
// nil bound is open on that end.
type Range struct {
	Lo, Hi []byte
}

// Index is what the planner needs to know about one candidate index. Callers
// hold their own richer type (the engine's compiledIndex) and adapt.
type Index struct {
	Name    string
	Encoder *IndexEncoder
}

// PlanQuery picks an index for a predicate plus an ordering, or returns nil to
// mean "scan". Candidates are considered in order and the first exact-ordered
// match wins; failing that, the one that narrows the most.
//
// A nil predicate with a nil/empty order is a full scan of the entity in key
// order, which no index improves on — the row keyspace IS that index — so it
// declines rather than reading a second copy of the same thing.
func PlanQuery(candidates []Index, pred *Compiled, order *Order) *Plan {
	if len(candidates) == 0 {
		return nil
	}
	terms := flattenAnd(pred)
	if len(terms) == 0 && (order == nil || len(order.keys) == 0) {
		return nil
	}
	var best *Plan
	var bestScore int
	for _, c := range candidates {
		p, score := planAgainst(c, terms, order)
		if p == nil {
			continue
		}
		if p.Ordered && score > 0 {
			// An ordered plan with at least one pinned component can serve the
			// query end to end; nothing later can beat that.
			return p
		}
		if best == nil || score > bestScore {
			best, bestScore = p, score
		}
	}
	return best
}

// planAgainst tries one index. score is how much the plan narrows — the number
// of components the range pins — and is only meaningful when the plan is
// non-nil.
func planAgainst(idx Index, terms []node, order *Order) (*Plan, int) {
	comps := idx.Encoder.Components()
	if len(comps) == 0 {
		return nil, 0
	}

	// Walk the components left to right, consuming equality terms while they
	// keep pinning.
	//
	// Two bookkeeping arrays, because "the walk has consumed this term" and "the
	// range answers this term" are no longer the same thing. `taken` stops a term
	// being reused by a later component; `answered` is what drops it from the
	// residual, and for a term that named a map entry key it cannot be decided
	// until the whole prefix is known.
	taken := make([]bool, len(terms))
	exact := make([]bool, len(terms))
	// needKeys[ti] are the entry keys an exact reduction of term ti depends on
	// the prefix pinning. Nil for a key-agnostic term, which is answered outright.
	needKeys := make([][]condValue, len(terms))
	// covered[ti] are the conjuncts of term ti's any() the range has consumed, and
	// conjuncts[ti] how many there are. Zero means the term is not a conjunction
	// and `exact` decides on its own.
	covered := make([]map[int]bool, len(terms))
	conjuncts := make([]int, len(terms))
	prefixes := [][]byte{nil} // the equality prefixes built so far (>1 for `in`)
	pinned := 0
	// The term the components of THIS index's map fan are restricted to. The
	// first such component to pin fixes it; every later one must reduce the same
	// term, so `custom.a.number` and `custom.b.text` can never be read as one
	// pinned pair. It is deliberately coarse — one map fan per index — which can
	// decline a plan a finer rule would allow (two value components of the same
	// entry, pinned by two terms naming the same key) but can never accept a
	// wrong one. pinnedKeys records what each key component chose, which is what
	// lets an ordering trust a value component of the same fan.
	mapFanTerm := -1
	pinnedKeys := map[int]condValue{}

	for ci, c := range comps {
		red, found := equalityValuesFor(c, terms, taken, onlyFor(c, mapFanTerm))
		if !found {
			break
		}
		next := make([][]byte, 0, len(prefixes)*len(red.values))
		ok := true
		for _, p := range prefixes {
			for _, v := range red.values {
				buf, appended := appendLiteral(append([]byte(nil), p...), c, v)
				if !appended {
					ok = false
					break
				}
				next = append(next, buf)
			}
			if !ok {
				break
			}
		}
		if !ok {
			break
		}
		if hasMapFan(c) && mapFanTerm < 0 {
			mapFanTerm = red.term
		}
		if c.isEntryKey() {
			pinnedKeys[ci] = red.values[0]
		}
		// A term that only PARTLY reduces to this component (an any() with more
		// than one condition inside, a hasAll) still bounds the range, but the
		// range does not answer it — so it bounds and stays in the residual. A
		// term that named an entry key is left available too, because the key
		// component of the same fan still has to consume it.
		taken[red.term] = red.exact && len(red.keys) == 0
		switch {
		case red.conjuncts > 0:
			if covered[red.term] == nil {
				covered[red.term] = make(map[int]bool, red.conjuncts)
			}
			covered[red.term][red.conjunct] = true
			conjuncts[red.term] = red.conjuncts
			if len(red.keys) > 0 {
				needKeys[red.term] = red.keys
			}
		case red.exact:
			exact[red.term] = true
			needKeys[red.term] = red.keys
		}
		prefixes = next
		pinned++
		// An `in` fans the prefix set out multiplicatively; stop pinning before
		// that becomes a fan of probes larger than the scan it replaces.
		if len(prefixes) > maxIndexProbes {
			return nil, 0
		}
	}

	// The component after the equality prefix may take a range from an
	// inequality. Only one can: past it the entries are no longer contiguous.
	var ranges []Range
	rangedComp := -1
	if pinned < len(comps) {
		c := comps[pinned]
		lo, hi, ti, keys, found := inequalityBoundsFor(c, terms, taken, onlyFor(c, mapFanTerm))
		if found {
			taken[ti] = true
			exact[ti] = true
			needKeys[ti] = keys
			rangedComp = pinned
			ranges = make([]Range, 0, len(prefixes))
			for _, p := range prefixes {
				ranges = append(ranges, Range{
					Lo: concatBound(p, lo, true),
					Hi: concatBound(p, hi, false),
				})
			}
		}
	}

	// Exactness, now that the prefix is known: a reduction onto a map-fan value
	// component only answers its term if the keys that term named are pinned too.
	used := make([]bool, len(terms))
	for ti := range terms {
		answered := exact[ti]
		if conjuncts[ti] > 0 {
			// A conjunction is answered only when every conjunct is: the rest
			// would otherwise have to be re-checked against the same element, and
			// the range cannot say which element it matched.
			answered = len(covered[ti]) == conjuncts[ti]
		}
		used[ti] = answered && mapKeysPinned(needKeys[ti], pinnedKeys)
	}
	if ranges == nil {
		ranges = make([]Range, 0, len(prefixes))
		for _, p := range prefixes {
			ranges = append(ranges, Range{Lo: p, Hi: UpperBound(p)})
		}
	}

	// An index that pins nothing and bounds nothing reads the whole thing, in a
	// different order than the rows are stored — strictly worse than a scan
	// unless it supplies the ordering.
	ordered := orderServedBy(comps, pinned, order, pinnedKeys)
	if pinned == 0 && rangedComp < 0 && !ordered {
		return nil, 0
	}

	residual := residualOf(terms, used)
	// Several ranges cannot be read in order without a merge, so a caller that
	// needs the ordering has to be told the plan does not supply it. Reporting
	// Ordered honestly here is what keeps the sort correct.
	if len(ranges) > 1 && order != nil && len(order.keys) > 0 {
		ordered = false
	}
	var prefix []byte
	if len(prefixes) == 1 {
		prefix = prefixes[0]
	}
	return &Plan{Index: idx.Name, Ranges: ranges, Residual: residual, Ordered: ordered, Prefix: prefix}, pinned
}

// maxIndexProbes bounds the fan-out of an `in` over a leading component. Past
// it the probes cost more than the scan they replace, and the plan is declined.
const maxIndexProbes = 64

// onlyFor is the term this component is restricted to, or -1 for no restriction.
// Only a component that fans out over a map is restricted, and only once some
// other component of that fan has already pinned: the two read one entry, so
// they have to describe the same one.
func onlyFor(c IndexComponent, mapFanTerm int) int {
	if hasMapFan(c) {
		return mapFanTerm
	}
	return -1
}

// hasMapFan reports whether the component takes one value per ENTRY of a map,
// as opposed to per element of a list or one value per row.
func hasMapFan(c IndexComponent) bool {
	_, ok := mapFanPrefix(c.fields)
	return ok
}

// mapFanPrefix is the path up to and including the INNERMOST map fan step — the
// scope a key component of the same fan stores the key of.
func mapFanPrefix(fields []pathStep) ([]pathStep, bool) {
	for i := len(fields) - 1; i >= 0; i-- {
		if fields[i].fan && fields[i].fd.IsMap() {
			return fields[:i+1], true
		}
	}
	return nil, false
}

// sameMapFan reports whether two components read the same map entry: the same
// chain of collections up to and including the innermost map. It is what pairs a
// key component with the value components of its own fan, now that a declaration
// may put them in either order and other components between them.
//
// The entry-key selector is ignored in the comparison — it is precisely what
// differs between the two — as is `keyed`, which an index path never has.
func sameMapFan(a, b IndexComponent) bool {
	pa, oka := mapFanPrefix(a.fields)
	pb, okb := mapFanPrefix(b.fields)
	return oka && okb && sameFanSteps(pa, pb)
}

// mapKeysPinned reports whether every entry key a term named is also fixed by a
// key component of the equality prefix.
//
// This is the condition under which a range on a map-fan VALUE component answers
// its term outright. Without it the range spans that value under every key, so
// rows where a DIFFERENT field holds the value fall inside it — and marking the
// term exact would return them. A term that named no key (an any() over the map,
// which asks about entries rather than one of them) needs nothing pinned.
func mapKeysPinned(keys []condValue, pinnedKeys map[int]condValue) bool {
	for _, k := range keys {
		found := false
		for _, got := range pinnedKeys {
			if equalsScalar(got, k) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// reduction is what one term contributes to one component: the values that pin
// it, which term they came from, whether the range then answers that term, and
// the map entry keys the term named — which mapKeysPinned turns into the final
// exactness verdict once the whole prefix is known.
type reduction struct {
	values []condValue
	term   int
	exact  bool
	keys   []condValue
	// conjunct is which sub-condition of an any() this reduction consumed, and
	// conjuncts how many there are in all. A conjunction is answered only once
	// EVERY conjunct is consumed — by whichever components happen to consume
	// them, since components of one fan read a single element position and so
	// describe one element between them.
	conjunct  int
	conjuncts int
}

// orderServedBy reports whether reading the index in order yields the query's
// requested order. The equality prefix contributes nothing to the ordering —
// every row in the range agrees on those components — so the order keys must
// line up with the components that FOLLOW it, in sequence, direction included.
//
// A query that asks for no order reports false rather than true. Any index
// would "serve" an empty order trivially, but this answer is also what decides
// whether supplying the ordering is reason enough to use an index that pins and
// bounds nothing — and for a caller that wanted no order it is not.
func orderServedBy(comps []IndexComponent, pinned int, order *Order, pinnedKeys map[int]condValue) bool {
	if order == nil || len(order.keys) == 0 {
		return false
	}
	if pinned+len(order.keys) > len(comps) {
		return false
	}
	for i, k := range order.keys {
		j := pinned + i
		c := comps[j]
		if c.desc != k.desc {
			return false
		}
		keys, ok := matchFanPath(c.fields, k.fields)
		if !ok {
			return false
		}
		if len(keys) == 0 {
			if c.Multi() {
				// A LIST fan-out component holds one entry per element, so a row
				// sits at several places in this stretch of the index at once and
				// walking it yields no row order at all. Pinned it is harmless —
				// the prefix has already chosen the element — but past the prefix
				// it is not an ordering.
				return false
			}
			continue
		}
		// A MAP fan is different, and this is the whole reason the two cases are
		// distinguished: a map holds AT MOST ONE value per key, so once the entry
		// key is pinned the row occupies exactly one position here and the
		// component IS an ordering. Unpinned it is not — and neither is a leaf
		// that also fans out over a list inside the entry.
		if !mapLeafPinned(comps, pinned, j, keys, pinnedKeys) {
			return false
		}
	}
	return true
}

// mapLeafPinned reports whether comps[j] is the leaf of a map fan whose entry
// key the equality prefix has already fixed to the key the order path names.
//
// The key component is looked up BY FAN among the pinned prefix rather than
// assumed to sit at j-1: one declared path is one component now, so an index may
// put its key component anywhere — before the value, after it, or with other
// components in between.
func mapLeafPinned(
	comps []IndexComponent,
	pinned, j int,
	keys []condValue,
	pinnedKeys map[int]condValue,
) bool {
	levels := fanLevels(comps[j].fields)
	if len(levels) != 1 || !levels[0].isMap || len(keys) != 1 {
		return false
	}
	for i := 0; i < pinned; i++ {
		if !comps[i].isEntryKey() || !sameMapFan(comps[i], comps[j]) {
			continue
		}
		if got, ok := pinnedKeys[i]; ok && equalsScalar(got, keys[0]) {
			return true
		}
	}
	return false
}

// equalityValuesFor finds a term pinning this component to one or more exact
// values. The second result is false when nothing pins it.
//
// A term that is not exact still narrows: the values it yields are a superset
// of what it matches, so the range is a superset of the rows, and the term
// stays in the residual to reject the rest. That is what lets a hasAll or a
// multi-condition any() use an index at all.
func equalityValuesFor(c IndexComponent, terms []node, taken []bool, only int) (reduction, bool) {
	for i, t := range terms {
		if taken[i] || (only >= 0 && i != only) {
			continue
		}
		if n, ok := t.(*mapExistsNode); ok {
			// An any() over a map reduces onto EITHER side of the entry, so it is
			// dispatched before the key/value split below rather than inside it.
			if red, ok := mapExistsEqualityFor(c, n); ok {
				red.term = i
				return red, true
			}
			continue
		}
		if c.isEntryKey() {
			if vals, ok := mapKeyEqualityFor(c, t); ok {
				// The key is pinned by the term but NOT answered by it — the term
				// still has to constrain the value beside it — so it is left
				// untaken and falls to the residual if nothing consumes it.
				return reduction{values: vals, term: i}, true
			}
			continue
		}
		switch n := t.(type) {
		case *cmpNode:
			if keys, ok := matchFanPath(c.fields, n.fields); ok && n.op == syncplanev1.CmpOp_CMP_OP_EQ {
				return reduction{values: []condValue{n.value}, term: i, exact: true, keys: keys}, true
			}
		case *inNode:
			if keys, ok := matchFanPath(c.fields, n.fields); ok && len(n.values) > 0 {
				return reduction{values: n.values, term: i, exact: true, keys: keys}, true
			}
		case *repNode:
			if red, ok := repEqualityFor(c, n); ok {
				red.term = i
				return red, true
			}
		case *existsNode:
			if red, ok := existsEqualityFor(c, n); ok {
				red.term = i
				return red, true
			}
		}
	}
	return reduction{}, false
}

// fanPathEq reports whether a condition path addresses exactly what this
// component stores — the same fields, with a literal key wherever the component
// fans out over a map.
func fanPathEq(c IndexComponent, term []pathStep) bool {
	_, ok := matchFanPath(c.fields, term)
	return ok
}

// mapKeyEqualityFor pins a map's entry-key component from any term that
// addresses one entry of it.
//
// The rule that makes this sound is that the term must IMPLY THE KEY IS PRESENT:
// a range pinned to key k contains only rows holding an entry at k, so a term
// that some row without that entry could satisfy would have its rows dropped.
// Every operator qualifies except isNotSet, which is exactly the one satisfied by
// a row that has no such entry.
func mapKeyEqualityFor(c IndexComponent, t node) ([]condValue, bool) {
	p, ok := termPath(t)
	if !ok {
		return nil, false
	}
	keys, ok := matchFanPrefix(c.fields, p)
	if !ok || len(keys) == 0 {
		return nil, false
	}
	// The component ends AT its map fan, so the key it stores is the last one
	// the prefix captured.
	return []condValue{keys[len(keys)-1]}, true
}

// termPath is the row path a term addresses, or false for a term that has none
// (and/or/not, which never bound a range on their own) or that a row lacking the
// addressed value could satisfy.
func termPath(t node) ([]pathStep, bool) {
	switch n := t.(type) {
	case *cmpNode:
		return n.fields, true
	case *inNode:
		return n.fields, true
	case *strNode:
		return n.fields, true
	case *repNode:
		return n.fields, true
	case *existsNode:
		return n.fields, true
	case *mapExistsNode:
		// The path ends at the MAP, with no key — which is what makes it a
		// question about entries rather than about one of them — so it cannot pin
		// a key component from here. mapExistsEqualityFor is what reads it.
		return n.fields, true
	case *presentNode:
		// isNotSet is true for a row with no such entry at all, so it cannot pin
		// the key: the range would drop exactly the rows it matches.
		return n.fields, n.op == syncplanev1.PresentOp_PRESENT_OP_IS_SET
	}
	return nil, false
}

// matchFanPath reports whether a condition path addresses the same values a
// component path fans out over, returning the map keys it named. `*` steps in
// the component match a keyed step in the term; everything else must be equal.
func matchFanPath(comp, term []pathStep) ([]condValue, bool) {
	if len(comp) != len(term) {
		return nil, false
	}
	return matchFanPrefix(comp, term)
}

// matchFanPrefix is matchFanPath where the component may stop short — which is
// how an entry-key component, whose path ends at the map, matches a term that
// continues into the entry's value.
func matchFanPrefix(comp, term []pathStep) ([]condValue, bool) {
	if len(term) < len(comp) {
		return nil, false
	}
	var keys []condValue
	for i := range comp {
		if comp[i].fd.FullName() != term[i].fd.FullName() {
			return nil, false
		}
		if comp[i].fan && comp[i].fd.IsMap() {
			if !term[i].keyed {
				return nil, false
			}
			keys = append(keys, extractSingular(comp[i].fd.MapKey(), term[i].mapKey.Value()))
			continue
		}
		if term[i].keyed {
			return nil, false
		}
	}
	return keys, true
}

// repEqualityFor reads membership over a repeated scalar as a probe on a
// component that fans out over that same list — "some element equals v" is
// exactly "this row has an entry at v".
//
// hasAll pins on the FIRST value only. The rows holding all of the values are a
// subset of the rows holding that one, so one narrow range plus a residual
// beats a union of ranges over every value.
func repEqualityFor(c IndexComponent, n *repNode) (reduction, bool) {
	// The component's value must BE the fanned element (a repeated scalar), and
	// the term must name the same path — with a literal key wherever the
	// component fans out over a map, so `custom.<key>.value.labels.values`
	// matches `custom.*.value.labels.values` only for the entry the range has
	// pinned.
	if len(fanRest(c.fields)) != 0 || c.isEntryKey() || !c.Multi() {
		return reduction{}, false
	}
	keys, ok := matchFanPath(c.fields, n.fields)
	if !ok {
		return reduction{}, false
	}
	switch n.op {
	case syncplanev1.RepOp_REP_OP_HAS:
		return reduction{values: n.values[:1], exact: true, keys: keys}, true
	case syncplanev1.RepOp_REP_OP_HAS_ANY:
		if len(n.values) == 0 {
			return reduction{}, false
		}
		return reduction{values: n.values, exact: true, keys: keys}, true
	case syncplanev1.RepOp_REP_OP_HAS_ALL:
		if len(n.values) == 0 {
			return reduction{}, false
		}
		return reduction{values: n.values[:1], keys: keys}, true
	}
	return reduction{}, false
}

// existsEqualityFor reads an any() as a probe on a component that fans out over
// the same list: the component's entries hold one value per element, so pinning
// one is "some element has this value" — the existential itself.
//
// Only the sub-condition's own equality reduces. When it is a conjunction the
// range pins one conjunct and the whole any() stays residual, because the rest
// has to be re-checked against the SAME element and the range cannot say which
// element it matched.
func existsEqualityFor(c IndexComponent, n *existsNode) (reduction, bool) {
	levels := fanLevels(c.fields)
	if len(levels) == 0 || c.isEntryKey() {
		return reduction{}, false
	}
	last := levels[len(levels)-1]
	if last.isMap {
		// any() scopes an element of a LIST; a map fan has no element scope of
		// its own, and its entries are addressed by key instead.
		return reduction{}, false
	}
	keys, ok := matchFanPath(c.fields[:len(c.fields)-len(fanRest(c.fields))], n.fields)
	if !ok {
		return reduction{}, false
	}
	rest := fanRest(c.fields)
	if len(rest) == 0 {
		// The component stores the elements themselves, so there is no sub-path
		// for a condition on element fields to match.
		return reduction{}, false
	}
	switch inner := n.cond.(type) {
	case *cmpNode:
		if inner.op == syncplanev1.CmpOp_CMP_OP_EQ && samePath(inner.fields, rest) {
			return reduction{values: []condValue{inner.value}, exact: true, keys: keys}, true
		}
	case *inNode:
		if len(inner.values) > 0 && samePath(inner.fields, rest) {
			return reduction{values: inner.values, exact: true, keys: keys}, true
		}
	case *andNode:
		for j, sub := range inner.conds {
			switch s := sub.(type) {
			case *cmpNode:
				if s.op == syncplanev1.CmpOp_CMP_OP_EQ && samePath(s.fields, rest) {
					return reduction{
						values: []condValue{s.value}, keys: keys,
						conjunct: j, conjuncts: len(inner.conds),
					}, true
				}
			case *inNode:
				if len(s.values) > 0 && samePath(s.fields, rest) {
					return reduction{
						values: s.values, keys: keys,
						conjunct: j, conjuncts: len(inner.conds),
					}, true
				}
			}
		}
	}
	return reduction{}, false
}

// mapExistsEqualityFor reads an any() over a MAP as a probe on the components
// that fan out over that map.
//
// The entry is ONE position, so a sub-condition on `value…` reduces onto a value
// component of that fan and one on `key` onto its key component. When the
// sub-condition conjoins both, the two components together answer the whole
// existential — because they read one entry rather than two, which is exactly
// what makes the explicit entry scope worth having.
//
// This is the cross-key case the `[value, key]` layout exists for: with no key
// constraint at all, a value component alone answers "some entry holds X", so
// the reduction names no keys and the exactness guard has nothing to require.
func mapExistsEqualityFor(c IndexComponent, n *mapExistsNode) (reduction, bool) {
	if !mapFanMatches(c, n.fields) {
		return reduction{}, false
	}
	// What this component reads, stated as a path from the entry.
	side := mapEntryValueNumber
	rest := fanRest(c.fields)
	if c.isEntryKey() {
		side, rest = mapEntryKeyNumber, nil
	}
	if red, ok := reduceEntryCond(n.cond, side, rest); ok {
		return red, true
	}
	if and, ok := n.cond.(*andNode); ok {
		for j, sub := range and.conds {
			if red, ok := reduceEntryCond(sub, side, rest); ok {
				red.exact = false
				red.conjunct, red.conjuncts = j, len(and.conds)
				return red, true
			}
		}
	}
	return reduction{}, false
}

// mapFanMatches reports whether a component fans out over exactly the map an
// any() addresses. The term's path ends AT the map with no key — that is what
// makes it a question about entries rather than about one of them — so the
// comparison is against the component's fan prefix, descriptor for descriptor.
func mapFanMatches(c IndexComponent, term []pathStep) bool {
	prefix, ok := mapFanPrefix(c.fields)
	if !ok || len(prefix) != len(term) {
		return false
	}
	for i := range prefix {
		if prefix[i].fd.FullName() != term[i].fd.FullName() || term[i].keyed {
			return false
		}
	}
	return true
}

// reduceEntryCond reduces one sub-condition of an any() over a map onto a
// component, given which side of the entry that component stores and the path
// below it.
//
// The sub-condition compiled against the map's synthetic entry type, so its
// first step is that type's key or value field — matched by number, the same way
// the evaluator resolves it.
func reduceEntryCond(sub node, side protoreflect.FieldNumber, rest []pathStep) (reduction, bool) {
	match := func(fields []pathStep) bool {
		return len(fields) > 0 && fields[0].fd.Number() == side && samePath(fields[1:], rest)
	}
	switch inner := sub.(type) {
	case *cmpNode:
		if inner.op == syncplanev1.CmpOp_CMP_OP_EQ && match(inner.fields) {
			return reduction{values: []condValue{inner.value}, exact: true}, true
		}
	case *inNode:
		if len(inner.values) > 0 && match(inner.fields) {
			return reduction{values: inner.values, exact: true}, true
		}
	}
	return reduction{}, false
}

// inequalityBoundsFor finds a term bounding this component from one or both
// sides. Only a single term is consumed: two inequalities on one component
// (a BETWEEN) would need both, and the second stays residual, which is correct
// if not optimal.
//
// startsWith is an inequality in disguise: every string with a given prefix
// occupies one contiguous range, which is exactly what the encoding's
// terminator guarantees.
//
// The map entry keys the bounding term named come back with the bounds, for the
// same reason an equality reduction returns them: a range over one custom
// field's values only answers the term if the key is pinned as well.
func inequalityBoundsFor(c IndexComponent, terms []node, taken []bool, only int) (lo, hi []byte, idx int, keys []condValue, found bool) {
	for i, t := range terms {
		if taken[i] || (only >= 0 && i != only) {
			continue
		}
		switch n := t.(type) {
		case *cmpNode:
			named, ok := matchFanPath(c.fields, n.fields)
			if !ok {
				continue
			}
			if lo, hi, ok := cmpBounds(c, n); ok {
				return lo, hi, i, named, true
			}
		case *strNode:
			named, ok := matchFanPath(c.fields, n.fields)
			if !ok || n.op != syncplanev1.StrOp_STR_OP_STARTS_WITH {
				continue
			}
			// The prefix's own encoding is not a bound — it carries a
			// terminator, so it would match only the exact string. Encode the
			// prefix WITHOUT terminating it and take the byte range above it.
			raw := appendIndexString(nil, n.value)
			raw = raw[:len(raw)-2] // drop the terminator
			pre := append([]byte{tagPresent}, raw...)
			if c.desc {
				// Under desc the payload is inverted, which reverses the range:
				// the successor becomes the lower bound.
				inv := append([]byte(nil), raw...)
				invert(inv)
				lo := append([]byte{tagPresent}, UpperBound(inv)...)
				hi := append([]byte{tagPresent}, inv...)
				return lo, UpperBound(hi), i, named, true
			}
			return pre, UpperBound(pre), i, named, true
		}
	}
	return nil, nil, -1, nil, false
}

// cmpBounds turns one inequality into encoded bounds on the component.
//
// The mapping is written against the ASCENDING encoding and then flipped for a
// desc component, rather than written twice: `x > v` selects the entries above
// v ascending and the entries below it descending, and getting that backwards
// silently returns the complement of the answer.
func cmpBounds(c IndexComponent, n *cmpNode) (lo, hi []byte, ok bool) {
	v, ok := literalBytes(c, n.value)
	if !ok {
		return nil, nil, false
	}
	// Every real value carries tagPresent, so the bounds stay inside the real
	// values and never reach the NaN or missing tags — which is exactly right:
	// a comparison is false against both.
	lowest := []byte{tagPresent}
	above := UpperBound(v)

	var loA, hiA []byte
	switch n.op {
	case syncplanev1.CmpOp_CMP_OP_GT:
		loA, hiA = above, UpperBound(lowest)
	case syncplanev1.CmpOp_CMP_OP_GTE:
		loA, hiA = v, UpperBound(lowest)
	case syncplanev1.CmpOp_CMP_OP_LT:
		loA, hiA = lowest, v
	case syncplanev1.CmpOp_CMP_OP_LTE:
		loA, hiA = lowest, above
	default:
		return nil, nil, false
	}
	if !c.desc {
		return loA, hiA, true
	}
	// Under desc the payload order is reversed, so "everything above v"
	// ascending is "everything below v" descending. Re-derive rather than
	// reversing the byte strings, which is not the same operation.
	switch n.op {
	case syncplanev1.CmpOp_CMP_OP_GT:
		return lowest, v, true
	case syncplanev1.CmpOp_CMP_OP_GTE:
		return lowest, above, true
	case syncplanev1.CmpOp_CMP_OP_LT:
		return above, UpperBound(lowest), true
	case syncplanev1.CmpOp_CMP_OP_LTE:
		return v, UpperBound(lowest), true
	}
	return nil, nil, false
}

// literalBytes encodes a query literal as this component's payload, or reports
// that it cannot be.
//
// A literal whose kind does not match the column is not an error and not a
// bound: it is a legitimate no-match the evaluator answers false to, so the
// term stays residual and the scan-equivalent answer comes out. The ONE case
// worth converting is a number against a bigint column, where a non-integral
// literal becomes an exact floor/ceil bound rather than a declined plan.
func literalBytes(c IndexComponent, v condValue) ([]byte, bool) {
	buf, ok := appendLiteral(nil, c, v)
	return buf, ok
}

// appendLiteral appends one literal to dst as this component's payload,
// tag included and inversion applied, matching what the encoder writes for a
// row holding that value.
func appendLiteral(dst []byte, c IndexComponent, v condValue) ([]byte, bool) {
	leaf := c.leafField()
	want := singularKind(leaf)
	rv, ok := coerceLiteral(want, leaf, v)
	if !ok {
		return nil, false
	}
	return appendComponent(dst, c, rv, true), true
}

// coerceLiteral converts a wire literal into the row-value shape the column
// stores, mirroring what compareScalar does across the same kind pair. It
// fails when no exact conversion exists, which leaves the term residual.
func coerceLiteral(want rvKind, leaf protoreflect.FieldDescriptor, v condValue) (rowValue, bool) {
	if v.kind == want {
		if want == rvNumber && isNaN(v) {
			// NaN has a tag of its own and matches nothing, so it is never a
			// useful bound.
			return rowValue{}, false
		}
		return v, true
	}
	switch {
	case want == rvBigint && v.kind == rvNumber:
		if isNaN(v) || !isIntegralFloat(v.num) {
			// A non-integral literal falls between two integers, so no stored
			// value can equal it and no exact bound exists at this component.
			return rowValue{}, false
		}
		b := bigFromFloat(v.num)
		if !fitsColumn(leaf, b) {
			return rowValue{}, false
		}
		return rowValue{kind: rvBigint, big: b}, true
	case want == rvNumber && v.kind == rvBigint:
		return rowValue{kind: rvNumber, num: floatFromBig(v.big)}, true
	}
	return rowValue{}, false
}

// fitsColumn rejects a literal outside the column's 64-bit range. The encoder
// truncates such a value into range, which would place the bound somewhere
// arbitrary rather than outside every entry.
func fitsColumn(leaf protoreflect.FieldDescriptor, b *big.Int) bool {
	if isUnsigned64(leaf) {
		return b.Sign() >= 0 && b.BitLen() <= 64
	}
	return b.BitLen() < 64 || (b.Sign() < 0 && b.BitLen() == 64 && b.Cmp(minInt64) == 0)
}

var minInt64 = new(big.Int).Lsh(big.NewInt(-1), 63)

// concatBound joins an equality prefix to a bound on the next component. A nil
// bound means the range is open on that side, which after a prefix means "the
// rest of this prefix" rather than "the rest of the index".
func concatBound(prefix, bound []byte, low bool) []byte {
	if bound == nil {
		if low {
			return prefix
		}
		return UpperBound(prefix)
	}
	return append(append([]byte(nil), prefix...), bound...)
}

// flattenAnd splits a predicate into the terms a range may consume
// independently. Only a top-level AND splits: inside an `or` or a `not` a term
// does not have to hold for a matching row, so treating one as a bound would
// drop rows.
func flattenAnd(pred *Compiled) []node {
	if pred == nil || pred.root == nil {
		return nil
	}
	if and, ok := pred.root.(*andNode); ok {
		out := make([]node, 0, len(and.conds))
		for _, c := range and.conds {
			out = append(out, c)
		}
		return out
	}
	return []node{pred.root}
}

// residualOf rebuilds the predicate from the terms the range does not
// guarantee. Nil means the range is exact.
func residualOf(terms []node, used []bool) *Compiled {
	var left []node
	for i, t := range terms {
		if !used[i] {
			left = append(left, t)
		}
	}
	switch len(left) {
	case 0:
		return nil
	case 1:
		return &Compiled{root: left[0]}
	default:
		return &Compiled{root: &andNode{conds: left}}
	}
}
