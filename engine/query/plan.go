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
	// keep pinning. `used` marks the terms the range accounts for, so whatever
	// is left becomes the residual.
	used := make([]bool, len(terms))
	prefixes := [][]byte{nil} // the equality prefixes built so far (>1 for `in`)
	pinned := 0

	for _, c := range comps {
		vals, ti, exact := equalityValuesFor(c, terms, used)
		if vals == nil {
			break
		}
		next := make([][]byte, 0, len(prefixes)*len(vals))
		ok := true
		for _, p := range prefixes {
			for _, v := range vals {
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
		// A term that only PARTLY reduces to this component (an any() with more
		// than one condition inside, a hasAll) still bounds the range, but the
		// range does not answer it — so it bounds and stays in the residual.
		used[ti] = exact
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
		lo, hi, ti, found := inequalityBoundsFor(c, terms, used)
		if found {
			used[ti] = true
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
	if ranges == nil {
		ranges = make([]Range, 0, len(prefixes))
		for _, p := range prefixes {
			ranges = append(ranges, Range{Lo: p, Hi: UpperBound(p)})
		}
	}

	// An index that pins nothing and bounds nothing reads the whole thing, in a
	// different order than the rows are stored — strictly worse than a scan
	// unless it supplies the ordering.
	ordered := orderServedBy(comps, pinned, order)
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
	return &Plan{Index: idx.Name, Ranges: ranges, Residual: residual, Ordered: ordered}, pinned
}

// maxIndexProbes bounds the fan-out of an `in` over a leading component. Past
// it the probes cost more than the scan they replace, and the plan is declined.
const maxIndexProbes = 64

// orderServedBy reports whether reading the index in order yields the query's
// requested order. The equality prefix contributes nothing to the ordering —
// every row in the range agrees on those components — so the order keys must
// line up with the components that FOLLOW it, in sequence, direction included.
//
// A query that asks for no order reports false rather than true. Any index
// would "serve" an empty order trivially, but this answer is also what decides
// whether supplying the ordering is reason enough to use an index that pins and
// bounds nothing — and for a caller that wanted no order it is not.
func orderServedBy(comps []IndexComponent, pinned int, order *Order) bool {
	if order == nil || len(order.keys) == 0 {
		return false
	}
	if pinned+len(order.keys) > len(comps) {
		return false
	}
	for i, k := range order.keys {
		c := comps[pinned+i]
		if c.desc != k.desc || !sameFields(c.fields, k.fields) {
			return false
		}
		if c.Multi() {
			// A fan-out component holds one entry per element, so a row sits at
			// several places in this stretch of the index at once and walking it
			// yields no row order at all. Pinned it is harmless — the prefix has
			// already chosen the element — but past the prefix it is not an
			// ordering, and an order key can never name a repeated path anyway.
			return false
		}
	}
	return true
}

func sameFields(a, b []protoreflect.FieldDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].FullName() != b[i].FullName() {
			return false
		}
	}
	return true
}

// equalityValuesFor finds a term pinning this component to one or more exact
// values, returning them, the term's position, and whether the range then
// answers the term outright. nil values mean nothing pins it.
//
// A term that is not exact still narrows: the values it yields are a superset
// of what it matches, so the range is a superset of the rows, and the term
// stays in the residual to reject the rest. That is what lets a hasAll or a
// multi-condition any() use an index at all.
func equalityValuesFor(c IndexComponent, terms []node, used []bool) ([]condValue, int, bool) {
	for i, t := range terms {
		if used[i] {
			continue
		}
		switch n := t.(type) {
		case *cmpNode:
			if n.op == syncplanev1.CmpOp_CMP_OP_EQ && sameFields(n.fields, c.fields) {
				return []condValue{n.value}, i, true
			}
		case *inNode:
			if len(n.values) > 0 && sameFields(n.fields, c.fields) {
				return n.values, i, true
			}
		case *repNode:
			if vals, exact, ok := repEqualityFor(c, n); ok {
				return vals, i, exact
			}
		case *existsNode:
			if vals, exact, ok := existsEqualityFor(c, n); ok {
				return vals, i, exact
			}
		}
	}
	return nil, -1, false
}

// repEqualityFor reads membership over a repeated scalar as a probe on a
// component that fans out over that same list — "some element equals v" is
// exactly "this row has an entry at v".
//
// hasAll pins on the FIRST value only. The rows holding all of the values are a
// subset of the rows holding that one, so one narrow range plus a residual
// beats a union of ranges over every value.
func repEqualityFor(c IndexComponent, n *repNode) (vals []condValue, exact, ok bool) {
	if !c.Multi() || c.multiAt != len(c.fields)-1 || !sameFields(n.fields, c.fields) {
		return nil, false, false
	}
	switch n.op {
	case syncplanev1.RepOp_REP_OP_HAS:
		return n.values[:1], true, true
	case syncplanev1.RepOp_REP_OP_HAS_ANY:
		if len(n.values) == 0 {
			return nil, false, false
		}
		return n.values, true, true
	case syncplanev1.RepOp_REP_OP_HAS_ALL:
		if len(n.values) == 0 {
			return nil, false, false
		}
		return n.values[:1], false, true
	}
	return nil, false, false
}

// existsEqualityFor reads an any() as a probe on a component that fans out over
// the same list: the component's entries hold one value per element, so pinning
// one is "some element has this value" — the existential itself.
//
// Only the sub-condition's own equality reduces. When it is a conjunction the
// range pins one conjunct and the whole any() stays residual, because the rest
// has to be re-checked against the SAME element and the range cannot say which
// element it matched.
func existsEqualityFor(c IndexComponent, n *existsNode) (vals []condValue, exact, ok bool) {
	if !c.Multi() || !sameFields(c.fields[:c.multiAt+1], n.fields) {
		return nil, false, false
	}
	rest := c.fields[c.multiAt+1:]
	if len(rest) == 0 {
		// The component stores the elements themselves, so there is no sub-path
		// for a condition on element fields to match.
		return nil, false, false
	}
	switch inner := n.cond.(type) {
	case *cmpNode:
		if inner.op == syncplanev1.CmpOp_CMP_OP_EQ && sameFields(inner.fields, rest) {
			return []condValue{inner.value}, true, true
		}
	case *inNode:
		if len(inner.values) > 0 && sameFields(inner.fields, rest) {
			return inner.values, true, true
		}
	case *andNode:
		for _, sub := range inner.conds {
			switch s := sub.(type) {
			case *cmpNode:
				if s.op == syncplanev1.CmpOp_CMP_OP_EQ && sameFields(s.fields, rest) {
					return []condValue{s.value}, false, true
				}
			case *inNode:
				if len(s.values) > 0 && sameFields(s.fields, rest) {
					return s.values, false, true
				}
			}
		}
	}
	return nil, false, false
}

// inequalityBoundsFor finds a term bounding this component from one or both
// sides. Only a single term is consumed: two inequalities on one component
// (a BETWEEN) would need both, and the second stays residual, which is correct
// if not optimal.
//
// startsWith is an inequality in disguise: every string with a given prefix
// occupies one contiguous range, which is exactly what the encoding's
// terminator guarantees.
func inequalityBoundsFor(c IndexComponent, terms []node, used []bool) (lo, hi []byte, idx int, found bool) {
	for i, t := range terms {
		if used[i] {
			continue
		}
		switch n := t.(type) {
		case *cmpNode:
			if !sameFields(n.fields, c.fields) {
				continue
			}
			if lo, hi, ok := cmpBounds(c, n); ok {
				return lo, hi, i, true
			}
		case *strNode:
			if n.op != syncplanev1.StrOp_STR_OP_STARTS_WITH || !sameFields(n.fields, c.fields) {
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
				return lo, UpperBound(hi), i, true
			}
			return pre, UpperBound(pre), i, true
		}
	}
	return nil, nil, -1, false
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
	leaf := c.fields[len(c.fields)-1]
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
