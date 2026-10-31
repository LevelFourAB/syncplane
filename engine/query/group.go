package query

import (
	"fmt"
	"sort"
	"strconv"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Grouping a match set instead of paging it.
//
// A grouped read partitions the matching rows by the value at one path and
// answers with the groups — their values alone (distinct), or each with the
// number of rows in it (a counted facet, a board column badge). Both are the
// same walk with different per-group work, which is why they share a plan and
// a compiled key.
//
// # What a group IS
//
// One group per DISTINCT value the path resolves to across the match set, plus
// one group for the rows the path resolves to nothing on. Groups come back in
// ascending value order with NaN and then the no-value group last — the order
// an ascending order key on the same path puts rows in (see orderRank), so the
// two surfaces never disagree about where an unset field sits.
//
// A path ending at a REPEATED scalar groups per ELEMENT: a row holding three
// labels joins three groups, and the counts therefore do not sum to the number
// of matching rows. That is what a label facet asks for, and it is the reason
// Group.multi exists rather than the repeated leaf being rejected.
//
// Only groups the rows produce exist. A status no row currently holds cannot
// come back as zero, because a read over rows has no way to learn the value
// set — that domain belongs to the caller's schema (an enum, a status entity),
// and the caller joins it against these counts.
//
// # How an index serves it
//
// The row planner recognises `equality prefix → at most one range component →
// ordering suffix`. Grouping adds a fourth kind of component, between the
// prefix and whatever follows: a PARTITION component, neither pinned nor
// bounded to one value. The walk seeks to the first entry sharing the prefix,
// does its per-group work, then seeks PAST that component's value to the next
// distinct one — so the cost is O(groups × work) rather than O(rows), and for
// distinct the work is nothing at all.
//
// The index that serves it is `[eq-filter fields…, groupPath, …]` — the same
// index a board ordering by `[status, rank]` already wants declared.
//
// Declining stays correct, exactly as it does for a row query: with no such
// index the caller buckets a scan in memory, per the standing rule that an
// index is an optimization and the scan is the definition.

// Group is a compiled grouping key: one resolved path, plus what reading a
// row's group values out of it needs.
type Group struct {
	fields []pathStep
	// multi reports the path ends at a repeated scalar, so a row belongs to one
	// group per element it holds rather than to exactly one.
	multi bool
}

// CompileGroup validates a grouping path against the message descriptor and
// resolves it once.
//
// The leaf must be comparable — the same rule an order key obeys, and for the
// same reason: a group is identified by its value, so a leaf nothing can be
// compared against would make every row one group. A repeated scalar is the one
// exception, and groups per element.
func CompileGroup(desc protoreflect.MessageDescriptor, path []string) (*Group, error) {
	fields, err := resolveFields(desc, path)
	if err != nil {
		return nil, err
	}
	st := fields[len(fields)-1]
	if leaf := st.leaf(); !st.keyed && leaf.IsList() {
		if !comparableKind(singularKind(leaf)) {
			return nil, fmt.Errorf("path %v: %s cannot be grouped by; %s",
				path, describeField(leaf), indexListHint(path, leaf))
		}
		return &Group{fields: fields, multi: true}, nil
	}
	if err := requireComparable(path, fields); err != nil {
		return nil, err
	}
	return &Group{fields: fields, multi: false}, nil
}

// Multi reports whether one row can belong to several groups, which happens
// exactly when the path ends at a repeated field. Group counts sum to the
// number of matching rows only when it is false.
func (g *Group) Multi() bool { return g.multi }

// GroupValue is one group, identified by the value its rows share at the
// grouping path. The zero value is the group of rows holding no value there.
type GroupValue struct {
	v     rowValue
	found bool
}

// Values are the groups msg belongs to: one per element for a repeated path,
// exactly one otherwise. Never empty — a row with no value at the path belongs
// to the no-value group, and an EMPTY list does too, which is what keeps a row
// out of no group at all.
func (g *Group) Values(msg protoreflect.Message) []GroupValue {
	v, found := resolveValue(msg, g.fields)
	if !g.multi || !found || v.kind != rvList {
		return []GroupValue{{v: v, found: found}}
	}
	n := v.list.Len()
	if n == 0 {
		return []GroupValue{{}}
	}
	out := make([]GroupValue, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, GroupValue{v: extractSingular(v.elem, v.list.Get(i)), found: true})
	}
	return out
}

// Key identifies the group for bucketing: two values that compare equal share
// a key, and no two that do not. Numbers and bigints normalize onto one key for
// the reason equalsScalar crosses them — an integral double and a bigint of the
// same magnitude are one group.
func (gv GroupValue) Key() string {
	if !gv.found {
		return "\x00missing"
	}
	if isNaN(gv.v) {
		return "\x00nan"
	}
	if k, ok := scalarKey(gv.v); ok {
		return "v" + k
	}
	// ±Inf, which scalarKey declines because a membership test must not match
	// one across kinds. A group is only ever compared with itself, so the exact
	// binary form separates the two infinities and nothing else.
	return "f" + strconv.FormatFloat(gv.v.num, 'b', -1, 64)
}

// Proto is the group's value on the wire, nil for the no-value group.
func (gv GroupValue) Proto() *syncplanev1.QueryValue {
	if !gv.found {
		return nil
	}
	switch gv.v.kind {
	case rvString:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: gv.v.str}}
	case rvBool:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_BoolValue{BoolValue: gv.v.b}}
	case rvNumber:
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: gv.v.num}}
	case rvBigint:
		if gv.v.big.IsInt64() {
			return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: gv.v.big.Int64()}}
		}
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_UintValue{UintValue: gv.v.big.Uint64()}}
	default:
		// Unreachable: CompileGroup rejects a leaf no comparison reaches.
		return nil
	}
}

// CompareGroupValues orders two groups the way an ascending order key on the
// same path orders their rows: real values by value, then NaN, then the
// no-value group. Zero means the two are one group.
func CompareGroupValues(a, b GroupValue) int {
	ra, rb := orderRank(a.v, a.found), orderRank(b.v, b.found)
	if ra != rb {
		if ra < rb {
			return -1
		}
		return 1
	}
	if ra != rankReal {
		return 0
	}
	c, ok := compareScalar(a.v, b.v)
	if !ok {
		// Two rows disagreeing at runtime about a leaf's kind, which Key has
		// already kept apart; ties rather than errors, like Order.Compare.
		return 0
	}
	return c
}

// # Rows per group
//
// The same partition serves the head of each group: the entries inside one
// group's sub-range are already in the index's own order, so a walk that stops
// after N of them reads `groups × N` rows rather than every row of every group.
// That only holds when the components PAST the partition are the ordering the
// caller asked for, which is what GroupPlan.Ordered reports and what
// PlanGroupRows requires — with no such index the caller buckets a scan and
// sorts each bucket, at the usual cost of the definition.
//
// The index that serves it is `[eq-filter fields…, groupPath, order keys…]` —
// again the one a board ordering by `[status, rank]` already wants declared.

// GroupPlan is a chosen index access path for a grouped read: one range whose
// entries the walk partitions on a component the prefix left free.
type GroupPlan struct {
	// Index names the index to read, matching the name the caller registered.
	Index string
	// Range is the byte range holding every entry that can contribute a group.
	// A grouped plan has exactly ONE: several would have to be merged group by
	// group, and a row an `in` puts in two of them would be counted twice.
	Range Range
	// Residual is the part of the predicate the range does not guarantee, to be
	// re-checked against each fetched row. Nil means the range is exact, which
	// is what lets a count be read off entries with no row decode.
	Residual *Compiled
	// Desc reports that the partition component stores its values descending, so
	// the walk must read the range BACKWARD to yield groups in ascending value
	// order.
	Desc bool
	// EntriesAreRows reports that inside one group's sub-range every entry names
	// a distinct row, so counting entries counts rows. False when a component
	// PAST the partition fans out, which puts one row at several positions of
	// the same group.
	EntriesAreRows bool
	// Ordered reports that reading one group's sub-range forward yields its rows
	// in the requested order, so a walk after the head of a group can stop. It
	// is false whenever the caller asked for no order and the partition is not
	// the last component: the entries then sort by whatever follows it, where an
	// unordered read sorts by row key.
	Ordered bool

	// comps are the index's components up to and including the partition one —
	// what GroupPrefix walks to find where a group's shared prefix ends.
	comps []IndexComponent
}

// GroupPrefix is the leading bytes of an index entry that every entry of the
// same group shares: the equality-pinned components plus the partition one.
// Seeking to its upper bound is what skips the rest of a group.
//
// It errors on bytes that are not an entry of this index, which cannot happen
// for entries the store yielded and is reported rather than assumed away.
func (p *GroupPlan) GroupPrefix(entry []byte) ([]byte, error) {
	at := 0
	for i, c := range p.comps {
		n, err := componentLen(c, entry[at:])
		if err != nil {
			return nil, fmt.Errorf("index entry: component %d: %w", i, err)
		}
		at += n
	}
	return entry[:at], nil
}

// PlanGroup picks an index that can partition a predicate's match set by
// `group`, or returns nil to mean "bucket a scan instead". Candidates are
// considered in order and the one that narrows most wins; an exact plan, whose
// counts need no row reads, beats a residual-bearing one that pins as much.
func PlanGroup(candidates []Index, pred *Compiled, group *Group) *GroupPlan {
	return planGroup(candidates, pred, group, nil, false)
}

// PlanGroupRows picks an index that can answer the head of each group in
// `order`, or returns nil to mean "bucket a scan and sort each bucket".
//
// It is PlanGroup under one more requirement: the components past the partition
// must BE the requested order, and each entry of a group must name a distinct
// row. Without both, walking a group in index order says nothing about which of
// its rows come first, and the head of the group could only be found by reading
// all of them — which is what the scan already does.
func PlanGroupRows(candidates []Index, pred *Compiled, group *Group, order *Order) *GroupPlan {
	return planGroup(candidates, pred, group, order, true)
}

func planGroup(candidates []Index, pred *Compiled, group *Group, order *Order, rows bool) *GroupPlan {
	if len(candidates) == 0 || group == nil {
		return nil
	}
	terms := flattenAnd(pred)
	var best *GroupPlan
	bestScore := -1
	for _, c := range candidates {
		p, score := planGroupAgainst(c, terms, group, order)
		if p == nil {
			continue
		}
		if rows && (!p.Ordered || !p.EntriesAreRows) {
			continue
		}
		if score > bestScore {
			best, bestScore = p, score
		}
	}
	return best
}

// planGroupAgainst tries one index. The score ranks by how much the range
// narrows first and by what the walk then costs — an exact range needs no row
// read per entry, and entries that are rows need none per group either.
func planGroupAgainst(idx Index, terms []node, group *Group, order *Order) (*GroupPlan, int) {
	comps := idx.Encoder.Components()
	if len(comps) == 0 {
		return nil, 0
	}
	w := walkEqualityPrefix(comps, terms)
	if w.overflow || len(w.prefixes) != 1 {
		// Several prefixes are several ranges, and a row that an `in` puts in two
		// of them would be counted once per range. A row query dedupes by row
		// key; a count has no row in hand to dedupe by.
		return nil, 0
	}
	if w.pinned >= len(comps) {
		// Nothing left to partition on: every component is pinned to one value,
		// so the range holds exactly one group and the index says nothing the
		// predicate did not.
		return nil, 0
	}
	part := comps[w.pinned]
	keys, ok := matchFanPath(part.fields, group.fields)
	if !ok {
		return nil, 0
	}
	if len(keys) > 0 && !mapLeafPinned(comps, w.pinned, w.pinned, keys, w.pinnedKeys) {
		// A map-fan value component holds one value per ENTRY, so partitioning on
		// it without its key pinned would group `custom.priority` and
		// `custom.severity` together under whatever value each happened to hold.
		return nil, 0
	}
	// A row occupies one entry of the partition component per group it belongs
	// to. A LIST fan gives it one per element, which is exactly what a repeated
	// grouping path asks for; a map fan whose key the prefix has pinned gives it
	// one, like an ordinary field. Anything else and the entries are not the
	// groups the definition assigns, so the two would disagree row for row.
	if perElement := len(keys) == 0 && part.Multi(); perElement != group.multi {
		return nil, 0
	}

	// The partition component may still take a range from an inequality, which
	// bounds WHICH groups the walk visits. Every entry of one group shares that
	// component's payload, so a group falls wholly inside such a range or wholly
	// outside it, and clamping never splits one.
	prefix := w.prefixes[0]
	rng := Range{Lo: prefix, Hi: UpperBound(prefix)}
	if lo, hi, ti, bkeys, found := inequalityBoundsFor(part, terms, w.taken, onlyFor(part, w.mapFanTerm)); found {
		w.taken[ti] = true
		w.exact[ti] = true
		w.needKeys[ti] = bkeys
		rng = Range{Lo: concatBound(prefix, lo, true), Hi: concatBound(prefix, hi, false)}
	}

	if idx.Encoder.Sparse() && !insidePresentRanks([]Range{rng}) {
		// The walk would visit the groups the sparse index leaves out — the
		// missing one first among them — as if they were empty.
		return nil, 0
	}

	residual := residualOf(terms, w.usedTerms())
	entriesAreRows := true
	for ci, c := range comps {
		if c.Multi() && ci > w.pinned {
			entriesAreRows = false
			break
		}
	}
	plan := &GroupPlan{
		Index:          idx.Name,
		Range:          rng,
		Residual:       residual,
		Desc:           part.desc,
		EntriesAreRows: entriesAreRows,
		Ordered:        groupOrderServedBy(comps, w.pinned+1, order, w.pinnedKeys),
		comps:          comps[:w.pinned+1],
	}
	score := w.pinned * 4
	if residual == nil {
		score += 2
	}
	if entriesAreRows {
		score++
	}
	return plan, score
}

// groupOrderServedBy reports whether walking one group's sub-range forward
// yields its rows in the requested order. `after` is where the group's shared
// prefix ends — the equality-pinned components plus the partition.
//
// An empty order is the case orderServedBy deliberately answers false to, and
// here it has a real answer: rows with no order key of their own come back in
// row-key order, which every entry ends with, so the walk is that order exactly
// when nothing sits between the partition and it.
func groupOrderServedBy(comps []IndexComponent, after int, order *Order, pinnedKeys map[int]condValue) bool {
	if order == nil || len(order.keys) == 0 {
		return after == len(comps)
	}
	return orderServedBy(comps, after, order, pinnedKeys, false)
}

// ValueOf names the group a prefix from GroupPrefix stands for, by matching the
// row's own group values against the partition component's bytes inside it.
//
// A singular path has one value, so the match is a formality. A repeated one
// has several, and only this comparison says WHICH element put the row in this
// group — the entry carries the value and the row carries the list, and the
// encoding is what joins them.
//
// The second result is false when no value of the row encodes to these bytes,
// which means the entry outlived the value it was written for.
func (p *GroupPlan) ValueOf(g *Group, msg protoreflect.Message, groupPrefix []byte) (GroupValue, bool) {
	part := p.comps[len(p.comps)-1]
	shared := len(groupPrefix)
	for _, gv := range g.Values(msg) {
		var encoded []byte
		if !gv.found {
			encoded = appendComponent(nil, part, rowValue{}, false)
		} else {
			var ok bool
			encoded, ok = appendLiteral(nil, part, gv.v)
			if !ok {
				continue
			}
		}
		if shared >= len(encoded) && string(groupPrefix[shared-len(encoded):]) == string(encoded) {
			return gv, true
		}
	}
	return GroupValue{}, false
}

// GroupCount is one group of a finished grouped read.
type GroupCount struct {
	Value GroupValue
	// Count is the matching rows in the group. A distinct read never resolves
	// one and leaves it at zero.
	Count uint64
}

// SortGroups puts groups in the order the surface promises: ascending value,
// then NaN, then the no-value group.
//
// No two groups ever tie — CompareGroupValues returns 0 only for values
// GroupValue.Key already put in one bucket — so the result does not depend on
// the order they were bucketed in.
func SortGroups(groups []GroupCount) {
	sort.Slice(groups, func(i, j int) bool {
		return CompareGroupValues(groups[i].Value, groups[j].Value) < 0
	})
}
