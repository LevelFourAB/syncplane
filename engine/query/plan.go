package query

import (
	"bytes"
	"fmt"
	"math"
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
// A top-level `or` plans as a UNION: each branch is planned on its own, with
// no ordering, and if EVERY branch resolves to an index the plan carries one
// (index, ranges) access path per branch — see Plan.Union. The executors'
// row-key dedupe already covers a row two branches both yield, so a union
// asks nothing of them beyond reading each branch's ranges from its own
// index. If ANY branch declines, the whole `or` declines: a union that scans
// for one branch is the scan with extra steps. A union is never Ordered — its
// ranges would need a cross-index merge — so the caller re-sorts, and a paged
// union walks its ranges whole rather than seeking.
//
// What is NOT servable, and why:
//
//   - `or` below the top level (inside an `and` term). The branches would have
//     to be intersected with the other terms first; the term stays residual,
//     which still narrows through whatever the other terms pin.
//   - `contains` / `endsWith`. Neither is a prefix of anything, so no range
//     bounds them. Searching text is a match condition's job, served by the
//     term-postings index (searchindex.go) rather than by this planner.
//   - `not` and `match`. Both become residual.
//
// Presence DOES plan, against a component whose path is exactly the term's and
// that does not fan out: the encoder writes every row into every dense index,
// with a rank tag ahead of the payload (real < NaN < missing), and both the
// encoder's "missing" and the evaluator's isNotSet come from the same
// resolution rule. So isNotSet pins the component to the missing rank — an
// equality in all but name, and the components after it still pin, range and
// order — while isSet bounds the next free component to the two set ranks. A
// fan-out component is excluded from both: an empty collection encodes as
// missing while presence on the collection itself reads as set, so the ranks
// and the evaluator disagree exactly there.
//
// A SPARSE index holds no entry whose leading component is missing, so it is
// a candidate only for a plan whose every range lies inside the present ranks
// of that component (see insidePresentRanks). isNotSet on it, and any walk
// that does not bound the leading component, decline to the next candidate or
// to the scan.
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
// A GROUPED read (PlanGroup, group.go) reads the same components under one
// extra shape: the component right after the equality prefix may be a PARTITION
// — neither pinned nor bounded to one value — which a walk seeks past one
// distinct value at a time. It shares walkEqualityPrefix with the row planner
// so that "what an equality prefix is" is decided in one place.
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
	// cursor to it names the position in the index the page resumes at, and
	// OrderSpansIndex says how much of that cursor the index can take. Nil when
	// the plan produced several ranges,
	// which share no single prefix, and empty when it pinned nothing.
	Prefix []byte
	// OrderSpansIndex reports whether the equality prefix and the order keys
	// together consume EVERY component of the index, so that appending a
	// cursor's whole entry — row-key tiebreak included — to Prefix names one
	// existing entry rather than something no entry starts with.
	//
	// It is what lets a seek bound land past the tie group at the boundary
	// instead of at the start of it, which is the difference between a deep page
	// costing O(page) and O(depth). False for an index carrying components past
	// the ordering, and for any plan that is not Ordered.
	OrderSpansIndex bool
	// Union, when non-empty, is a plan for a top-level `or`: one access path
	// per branch, each naming its own index, read through Branches. Index,
	// Ranges and Prefix are unset, Ordered is false, and Residual — nil only
	// when every branch's range was exact — is the whole predicate: a branch's
	// range can yield a row that fails that branch yet matches another, so no
	// branch-local re-check is correct.
	Union []PlanBranch

	// tieComps are the index components the equality prefix and the order keys
	// cover, for an Ordered plan whose index carries components past them; nil
	// otherwise. TieGroup cuts an entry at their end.
	tieComps []IndexComponent
}

// TieGroup returns the leading bytes of entry that every entry of the plan's
// index tying with it on the order keys shares, or nil when the plan has no
// tie group to finish: one that is not Ordered, or whose OrderSpansIndex
// holds. The result aliases entry.
//
// Past the order keys the index orders a tie by its remaining components,
// where Order.Less orders it by row key, so the entry that follows a full page
// in the walk can still sort before a row the page holds. A walk that stops
// when its page is full has to read on while the next entry returns the same
// tie group as the previous one, and can stop at the first that does not.
//
// It errors on bytes that are not an entry of the plan's index.
func (p *Plan) TieGroup(entry []byte) ([]byte, error) {
	if p.tieComps == nil {
		return nil, nil
	}
	at := 0
	for i, c := range p.tieComps {
		n, err := componentLen(c, entry[at:])
		if err != nil {
			return nil, fmt.Errorf("index entry: component %d: %w", i, err)
		}
		at += n
	}
	return entry[:at], nil
}

// PlanBranch is one access path of a union plan: the index to read and the
// ranges to read from it.
type PlanBranch struct {
	Index  string
	Ranges []Range
}

// Branches lists the access paths to read — the union's branches, or the plan
// itself as the single one — so executing any plan is one loop over it.
func (p *Plan) Branches() []PlanBranch {
	if len(p.Union) > 0 {
		return p.Union
	}
	return []PlanBranch{{Index: p.Index, Ranges: p.Ranges}}
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
// mean "scan". The narrowest exact-ordered match wins; failing that, the one
// that narrows the most. Ties keep the earliest-declared candidate.
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
	if len(terms) == 1 {
		if or, ok := terms[0].(*orNode); ok {
			// A union is preferred over what the fallback below could still
			// find — an index that only supplies the ordering, with the whole
			// `or` as residual. That walk costs a row read per rejection until
			// the page fills where the union costs O(branch matches), and with
			// no row counts to weigh the two, selective branches are the bet.
			if p := planUnion(candidates, or, pred, order); p != nil {
				return p
			}
		}
	}
	return bestPlan(candidates, terms, order)
}

// bestPlan picks an index for one set of AND-ed terms — PlanQuery's candidate
// loop, shared with the per-branch planning a union does.
func bestPlan(candidates []Index, terms []node, order *Order) *Plan {
	var best *Plan
	var bestScore int
	bestServes := false
	for _, c := range candidates {
		p, score := planAgainst(c, terms, order)
		if p == nil {
			continue
		}
		// An ordered plan with at least one pinned component serves the query
		// end to end. Those outrank everything, and among themselves the longer
		// equality prefix wins — a later `[status, priority, created]` narrows
		// strictly more than an earlier `[status, created]` even though both
		// are ordered. Elsewhere the score alone decides, so an ordered plan
		// that pins nothing still competes with narrower unordered ones rather
		// than trumping them.
		serves := p.Ordered && score > 0
		if best == nil || (serves && !bestServes) || (serves == bestServes && score > bestScore) {
			best, bestScore, bestServes = p, score, serves
		}
	}
	return best
}

// planUnion plans a top-level `or` as one access path per branch, or returns
// nil when any branch declines — a union that scans for one branch has become
// the scan with extra steps, so all-or-nothing is the rule.
//
// Branches are planned with no ordering: their ranges cannot be read in the
// query's order without a cross-index merge, so the caller re-sorts whatever
// ordering was asked for.
func planUnion(candidates []Index, or *orNode, pred *Compiled, order *Order) *Plan {
	branches := orBranches(or)
	if len(branches) == 0 {
		return nil
	}
	if len(branches) == 1 {
		// A one-branch `or` is its branch; planning it directly keeps the
		// ordering and the seek a plain predicate would have had.
		return bestPlan(candidates, branches[0], order)
	}
	out := make([]PlanBranch, 0, len(branches))
	ranges := 0
	exact := true
	for _, terms := range branches {
		p := bestPlan(candidates, terms, nil)
		if p == nil {
			return nil
		}
		// The same fan-out guard an `in` has, across the whole union: past it
		// the probes cost more than the scan they replace.
		ranges += len(p.Ranges)
		if ranges > maxIndexProbes {
			return nil
		}
		if p.Residual != nil {
			exact = false
		}
		out = append(out, PlanBranch{Index: p.Index, Ranges: p.Ranges})
	}
	var residual *Compiled
	if !exact {
		// The WHOLE predicate, not the branches' residuals: a row a branch's
		// range yields may fail that branch and still match another, so only
		// the full `or` is a correct per-row re-check.
		residual = pred
	}
	return &Plan{Union: out, Residual: residual}
}

// orBranches splits an `or` into one term list per branch: a branch that is an
// `and` contributes its conjuncts, a nested `or` flattens into more branches.
func orBranches(or *orNode) [][]node {
	var out [][]node
	for _, c := range or.conds {
		switch n := c.(type) {
		case *orNode:
			out = append(out, orBranches(n)...)
		case *andNode:
			out = append(out, append([]node(nil), n.conds...))
		default:
			out = append(out, []node{c})
		}
	}
	return out
}

// prefixWalk is what the leading component walk consumed: the encoded equality
// prefixes it built, how many components they pin, and the bookkeeping the
// exactness verdict is computed from once the whole prefix is known.
//
// It is shared by the row planner and the group planner, which agree exactly on
// what an equality prefix is and diverge only in what they do with the
// component that follows it.
type prefixWalk struct {
	// prefixes are the equality prefixes built so far — more than one when an
	// `in` fanned the walk out.
	prefixes [][]byte
	// pinned is how many leading components the prefixes fix.
	pinned int
	// taken marks a term the walk consumed, so a later component cannot reuse it.
	taken []bool
	// exact marks a term the range answers on its own, before the map-key rule
	// below is applied.
	exact []bool
	// needKeys[ti] are the entry keys an exact reduction of term ti depends on
	// the prefix pinning. Nil for a key-agnostic term, which is answered outright.
	needKeys [][]condValue
	// covered[ti] are the conjuncts of term ti's any() the range has consumed,
	// and conjuncts[ti] how many there are. Zero means the term is not a
	// conjunction and `exact` decides on its own.
	covered   []map[int]bool
	conjuncts []int
	// pinnedKeys records what each entry-key component chose, which is what lets
	// an ordering — or a partition — trust a value component of the same fan.
	pinnedKeys map[int]condValue
	// mapFanTerm is the term the components of this index's map fan are
	// restricted to, or -1 while none has pinned.
	mapFanTerm int
	// overflow reports that an `in` fanned the prefix set past maxIndexProbes,
	// which declines the whole plan rather than issuing the probes.
	overflow bool
}

// walkEqualityPrefix walks the components left to right, consuming equality
// terms while they keep pinning.
//
// Two bookkeeping arrays, because "the walk has consumed this term" and "the
// range answers this term" are no longer the same thing. `taken` stops a term
// being reused by a later component; `exact` feeds what drops it from the
// residual, and for a term that named a map entry key it cannot be decided
// until the whole prefix is known.
func walkEqualityPrefix(comps []IndexComponent, terms []node) prefixWalk {
	w := prefixWalk{
		prefixes:   [][]byte{nil},
		taken:      make([]bool, len(terms)),
		exact:      make([]bool, len(terms)),
		needKeys:   make([][]condValue, len(terms)),
		covered:    make([]map[int]bool, len(terms)),
		conjuncts:  make([]int, len(terms)),
		pinnedKeys: map[int]condValue{},
		// The first component of this index's map fan to pin fixes the term every
		// later one must reduce, so `custom.a.number` and `custom.b.text` can
		// never be read as one pinned pair. It is deliberately coarse — one map
		// fan per index — which can decline a plan a finer rule would allow (two
		// value components of the same entry, pinned by two terms naming the same
		// key) but can never accept a wrong one.
		mapFanTerm: -1,
	}
	for ci, c := range comps {
		red, found := equalityValuesFor(c, terms, w.taken, onlyFor(c, w.mapFanTerm))
		if !found {
			break
		}
		next := make([][]byte, 0, len(w.prefixes)*max(len(red.values), 1))
		ok := true
		for _, p := range w.prefixes {
			if red.missing {
				// The missing rank is a tag with no payload, so the "value" is
				// the tag itself — never inverted, whatever the direction.
				next = append(next, append(append([]byte(nil), p...), tagMissing))
				continue
			}
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
		if hasMapFan(c) && w.mapFanTerm < 0 {
			w.mapFanTerm = red.term
		}
		if c.isEntryKey() {
			w.pinnedKeys[ci] = red.values[0]
		}
		// A term that only PARTLY reduces to this component (an any() with more
		// than one condition inside, a hasAll) still bounds the range, but the
		// range does not answer it — so it bounds and stays in the residual. A
		// term that named an entry key is left available too, because the key
		// component of the same fan still has to consume it.
		w.taken[red.term] = red.exact && len(red.keys) == 0
		switch {
		case red.conjuncts > 0:
			if w.covered[red.term] == nil {
				w.covered[red.term] = make(map[int]bool, red.conjuncts)
			}
			w.covered[red.term][red.conjunct] = true
			w.conjuncts[red.term] = red.conjuncts
			if len(red.keys) > 0 {
				w.needKeys[red.term] = red.keys
			}
		case red.exact:
			w.exact[red.term] = true
			w.needKeys[red.term] = red.keys
		}
		w.prefixes = next
		w.pinned++
		// An `in` fans the prefix set out multiplicatively; stop pinning before
		// that becomes a fan of probes larger than the scan it replaces.
		if len(w.prefixes) > maxIndexProbes {
			w.overflow = true
			return w
		}
	}
	return w
}

// usedTerms is which terms the range answers outright, now that the prefix is
// known: a reduction onto a map-fan value component only answers its term if
// the keys that term named are pinned too.
func (w *prefixWalk) usedTerms() []bool {
	used := make([]bool, len(w.taken))
	for ti := range used {
		answered := w.exact[ti]
		if w.conjuncts[ti] > 0 {
			// A conjunction is answered only when every conjunct is: the rest
			// would otherwise have to be re-checked against the same element, and
			// the range cannot say which element it matched.
			answered = len(w.covered[ti]) == w.conjuncts[ti]
		}
		used[ti] = answered && mapKeysPinned(w.needKeys[ti], w.pinnedKeys)
	}
	return used
}

// planAgainst tries one index. score is how much the plan narrows — the number
// of components the range pins — and is only meaningful when the plan is
// non-nil.
func planAgainst(idx Index, terms []node, order *Order) (*Plan, int) {
	comps := idx.Encoder.Components()
	if len(comps) == 0 {
		return nil, 0
	}
	w := walkEqualityPrefix(comps, terms)
	if w.overflow {
		return nil, 0
	}

	// The component after the equality prefix may take a range from an
	// inequality. Only one can: past it the entries are no longer contiguous.
	var ranges []Range
	rangedComp := -1
	if w.pinned < len(comps) {
		c := comps[w.pinned]
		lo, hi, ti, keys, found := inequalityBoundsFor(c, terms, w.taken, onlyFor(c, w.mapFanTerm))
		if found {
			w.taken[ti] = true
			w.exact[ti] = true
			w.needKeys[ti] = keys
			// A second inequality on the same component (a BETWEEN) intersects
			// with the first: both encode as sub-ranges of the component's set
			// ranks, so the tighter bound on each side is their conjunction.
			// A third stays residual, which is correct if not optimal.
			if lo2, hi2, ti2, keys2, found2 := inequalityBoundsFor(c, terms, w.taken, onlyFor(c, w.mapFanTerm)); found2 {
				w.taken[ti2] = true
				w.exact[ti2] = true
				w.needKeys[ti2] = keys2
				lo = maxBytes(lo, lo2)
				hi = minBytes(hi, hi2)
			}
			rangedComp = w.pinned
			ranges = make([]Range, 0, len(w.prefixes))
			for _, p := range w.prefixes {
				ranges = append(ranges, Range{
					Lo: concatBound(p, lo, true),
					Hi: concatBound(p, hi, false),
				})
			}
		}
	}

	used := w.usedTerms()
	if ranges == nil {
		ranges = make([]Range, 0, len(w.prefixes))
		for _, p := range w.prefixes {
			ranges = append(ranges, Range{Lo: p, Hi: UpperBound(p)})
		}
	}

	// An index that pins nothing and bounds nothing reads the whole thing, in a
	// different order than the rows are stored — strictly worse than a scan
	// unless it supplies the ordering.
	ordered := orderServedBy(comps, w.pinned, order, w.pinnedKeys)
	if w.pinned == 0 && rangedComp < 0 && !ordered {
		return nil, 0
	}

	residual := residualOf(terms, used)
	// Several ranges cannot be read in order without a merge, so a caller that
	// needs the ordering has to be told the plan does not supply it. Reporting
	// Ordered honestly here is what keeps the sort correct.
	if len(ranges) > 1 && order != nil && len(order.keys) > 0 {
		ordered = false
	}
	if idx.Encoder.Sparse() && !insidePresentRanks(ranges) {
		return nil, 0
	}
	var prefix []byte
	if len(w.prefixes) == 1 {
		prefix = w.prefixes[0]
	}
	spans := false
	var tieComps []IndexComponent
	if ordered {
		spans = w.pinned+len(order.keys) == len(comps)
		if !spans {
			tieComps = comps[:w.pinned+len(order.keys)]
		}
	}
	return &Plan{
		Index:           idx.Name,
		Ranges:          ranges,
		Residual:        residual,
		Ordered:         ordered,
		Prefix:          prefix,
		OrderSpansIndex: spans,
		tieComps:        tieComps,
	}, w.pinned
}

// insidePresentRanks reports whether every range lies in [tagPresent,
// tagMissing) on the leading component. A sparse index leaves out exactly the
// entries that start with tagMissing, so inside that region it holds the same
// entries as the dense index, and outside it a range would miss rows. The check
// runs on the bytes the plan reads rather than on the terms that built them, so
// it holds for every way the planner reaches a range — isNotSet, an unbounded
// ordering walk, a union branch — without a rule per term.
func insidePresentRanks(ranges []Range) bool {
	for _, r := range ranges {
		if r.Lo == nil || bytes.Compare(r.Lo, []byte{tagPresent}) < 0 {
			return false
		}
		if r.Hi == nil || bytes.Compare(r.Hi, []byte{tagMissing}) > 0 {
			return false
		}
	}
	return true
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
	// missing pins the component to the MISSING rank tag instead of to values
	// — what an isNotSet term reduces to. values is empty then.
	missing bool
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
		case *presentNode:
			if red, ok := presenceEqualityFor(c, n); ok {
				red.term = i
				return red, true
			}
		}
	}
	return reduction{}, false
}

// presenceEqualityFor pins a component to the missing rank from an isNotSet
// term. The encoder's "missing" and the evaluator's "not found" are the same
// resolution rule over the same path, so the rank range IS the term — an
// equality on the tag rather than on a payload — and the components after it
// keep pinning, ranging and ordering.
//
// A component that fans out is excluded: an empty collection encodes as
// missing while the collection itself always resolves as found, so the ranks
// and the evaluator disagree exactly there. So is a term naming a map entry —
// a row without that entry holds no position for it, so no range contains it.
func presenceEqualityFor(c IndexComponent, n *presentNode) (reduction, bool) {
	if n.op != syncplanev1.PresentOp_PRESENT_OP_IS_NOT_SET || len(fanLevels(c.fields)) > 0 {
		return reduction{}, false
	}
	keys, ok := matchFanPath(c.fields, n.fields)
	if !ok || len(keys) > 0 {
		return reduction{}, false
	}
	return reduction{missing: true, exact: true}, true
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
		case *presentNode:
			// isSet is a rank range: every set row carries the present or NaN
			// tag, both below the missing tag, and the tag is never inverted —
			// so the bounds hold whatever the component's direction. The no-fan
			// rule from presenceEqualityFor holds here too, and isNotSet never
			// reaches this: the equality walk pins it first.
			if n.op != syncplanev1.PresentOp_PRESENT_OP_IS_SET || len(fanLevels(c.fields)) > 0 {
				continue
			}
			named, ok := matchFanPath(c.fields, n.fields)
			if !ok || len(named) > 0 {
				continue
			}
			return []byte{tagPresent}, []byte{tagMissing}, i, nil, true
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
				// Under desc the payload is inverted byte by byte. That reverses
				// the ORDER of the entries, but a string that starts with the
				// prefix still has an entry that starts with the inverted
				// prefix, so the range keeps the ascending shape.
				invert(pre[1:])
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
		if v, ok = neighbourBytes(c, n); !ok {
			return nil, nil, false
		}
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
		if want == rvBigint && !fitsColumn(leaf, v.big) {
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
		// The evaluator compares a double and an integer exactly, so a bound
		// built from a rounded literal would take in or leave out the double it
		// rounded to. An integer no double holds sits between two stored
		// values, and no exact bound exists at this component.
		f, acc := new(big.Float).SetInt(v.big).Float64()
		if acc != big.Exact {
			return rowValue{}, false
		}
		return rowValue{kind: rvNumber, num: f}, true
	}
	return rowValue{}, false
}

// neighbourBytes encodes the bound for an inequality whose literal is an
// integer no double holds, against a double column.
//
// No stored value equals such a literal, so each inequality is equivalent to
// one on a neighbouring double: x > L and x <= L hold exactly where they hold
// for the largest double below L, and x >= L and x < L exactly where they hold
// for the smallest double above L. The bound stays exact and needs no
// re-check, where a scan would read every row — and the literal is common: an
// integer nanosecond timestamp against a double column is almost never a
// double itself.
func neighbourBytes(c IndexComponent, n *cmpNode) ([]byte, bool) {
	if singularKind(c.leafField()) != rvNumber || n.value.kind != rvBigint {
		return nil, false
	}
	f, acc := new(big.Float).SetInt(n.value.big).Float64()
	if acc == big.Exact || math.IsInf(f, 0) {
		return nil, false
	}
	switch n.op {
	case syncplanev1.CmpOp_CMP_OP_GT, syncplanev1.CmpOp_CMP_OP_LTE:
		if acc == big.Above {
			f = math.Nextafter(f, math.Inf(-1))
		}
	case syncplanev1.CmpOp_CMP_OP_GTE, syncplanev1.CmpOp_CMP_OP_LT:
		if acc == big.Below {
			f = math.Nextafter(f, math.Inf(1))
		}
	default:
		return nil, false
	}
	return appendComponent(nil, c, rowValue{kind: rvNumber, num: f}, true), true
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

// maxBytes is the tighter of two inclusive lower bounds on one component; nil
// means open on that side.
func maxBytes(a, b []byte) []byte {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case bytes.Compare(a, b) < 0:
		return b
	default:
		return a
	}
}

// minBytes is the tighter of two exclusive upper bounds on one component; nil
// means open on that side.
func minBytes(a, b []byte) []byte {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case bytes.Compare(a, b) > 0:
		return b
	default:
		return a
	}
}

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
