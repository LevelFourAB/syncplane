package server

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// rowProjector clears in msg, a decoded row of info with key, the fields a
// guard hides from a caller. A nil rowProjector clears nothing.
type rowProjector func(info *entityInfo, key string, msg proto.Message) error

// apply runs p on msg, and does nothing for a nil p.
func (p rowProjector) apply(info *entityInfo, key string, msg proto.Message) error {
	if p == nil {
		return nil
	}
	return p(info, key, msg)
}

// guardRead is how the bound caller reads the units of one guard that can
// hide them: filter admits the rows on which the caller reads them, and is
// nil when the caller reads them on every row it reads.
type guardRead struct {
	units  []string
	filter readFilter
}

// guardReadsOf returns how the bound caller reads the units of each guard of
// info that can hide them, in guard order, and nil for an entity without
// one. In a schema that uses the access model it fails until bindAccess has
// run.
func (e *rowEmitter) guardReadsOf(info *entityInfo) ([]guardRead, error) {
	if !info.access.hidesFields() {
		return nil, nil
	}
	if e.caller == nil || e.access == nil {
		return nil, errAccessUnbound
	}
	if reads, ok := e.guardReads[info.name]; ok {
		return reads, nil
	}
	var reads []guardRead
	for i := range info.access.guards {
		g := &info.access.guards[i]
		if g.hides() {
			reads = append(reads, guardRead{units: g.units, filter: e.readersFilter(info, g.read)})
		}
	}
	if e.guardReads == nil {
		e.guardReads = map[string][]guardRead{}
	}
	e.guardReads[info.name] = reads
	return reads, nil
}

// mayHide reports whether a guard of info hides its units from the bound
// caller on at least some rows.
func (e *rowEmitter) mayHide(info *entityInfo) (bool, error) {
	reads, err := e.guardReadsOf(info)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(reads, func(g guardRead) bool { return g.filter != nil }), nil
}

// unitsHiddenSomewhere returns, sorted, the units of info that a guard hides
// from the bound caller on at least some rows: the units a request may not
// name in a condition, an order or a grouping, since evaluating one would
// tell the caller something about a value it does not read.
func (e *rowEmitter) unitsHiddenSomewhere(info *entityInfo) ([]string, error) {
	reads, err := e.guardReadsOf(info)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, g := range reads {
		if g.filter != nil {
			out = append(out, g.units...)
		}
	}
	sort.Strings(out)
	return out, nil
}

// hiddenUnits returns, sorted, the units of the row of info with key that a
// guard hides from the bound caller. row is the row's decoded value.
func (e *rowEmitter) hiddenUnits(info *entityInfo, key string, row proto.Message) ([]string, error) {
	reads, err := e.guardReadsOf(info)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, g := range reads {
		if g.filter == nil {
			continue
		}
		ok, err := g.filter(key, row)
		if err != nil {
			return nil, err
		}
		if !ok {
			out = append(out, g.units...)
		}
	}
	sort.Strings(out)
	return out, nil
}

// project clears in msg the units of the row of info with key that a guard
// hides from the bound caller, and returns them sorted.
func (e *rowEmitter) project(info *entityInfo, key string, msg proto.Message) ([]string, error) {
	hidden, err := e.hiddenUnits(info, key, msg)
	if err != nil || len(hidden) == 0 {
		return nil, err
	}
	if err := carryProtectedFields(info, hidden, nil, msg); err != nil {
		return nil, err
	}
	return hidden, nil
}

// rowChange builds the RowChange that sends row to the bound caller: a
// tombstone for a deleted row, and otherwise the row without the units a
// guard hides from the caller, which RowChange.hidden names. A row whose
// entity can hide nothing from the caller ships as stored.
func (e *rowEmitter) rowChange(row storage.Row) (*syncplanev1.RowChange, error) {
	rc := rowChange(row)
	if row.Deleted {
		return rc, nil
	}
	info, ok := e.srv.catalogPtr().entities[row.Entity]
	if !ok {
		return rc, nil
	}
	hides, err := e.mayHide(info)
	if err != nil || !hides {
		return rc, err
	}
	// classify decodes every row of an entity that can hide units, so a row
	// that reaches here decodes. One that does not anyway must not ship as
	// stored, which would show the units it hides.
	msg, err := info.decode(row.Value)
	if err != nil {
		return nil, fmt.Errorf("server: entity %q row %q: %w", row.Entity, row.Key, err)
	}
	hidden, err := e.project(info, row.Key, msg)
	if err != nil || len(hidden) == 0 {
		return rc, err
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	rc.Body = &syncplanev1.RowChange_Value{Value: value}
	rc.Hidden = hidden
	return rc, nil
}

// refuseHiddenPaths refuses a request that names, in a condition, an order,
// a grouping or a facet of entity info, a unit that a guard hides from the
// bound caller on some row. heads are the first segments of the paths the
// request names on rows of info. what names the part of the request in the
// error.
func (e *rowEmitter) refuseHiddenPaths(info *entityInfo, what string, heads []string) error {
	if len(heads) == 0 {
		return nil
	}
	hidden, err := e.unitsHiddenSomewhere(info)
	if err != nil || len(hidden) == 0 {
		return err
	}
	desc := info.msgType.Descriptor()
	for _, head := range heads {
		unit := head
		if fd := desc.Fields().ByName(protoreflect.Name(head)); fd != nil {
			unit = unitOf(fd)
		}
		if _, found := slices.BinarySearch(hidden, unit); found {
			return connect.NewError(connect.CodePermissionDenied, fmt.Errorf(
				"%s names field %q of entity %q, which the caller does not read on every row", what, head, info.name))
		}
	}
	return nil
}

// refuseHiddenMatch refuses a search condition on entity info when a guard
// hides a search field of info from the bound caller on some row.
func (e *rowEmitter) refuseHiddenMatch(info *entityInfo, what string) error {
	hidden, err := e.unitsHiddenSomewhere(info)
	if err != nil || len(hidden) == 0 {
		return err
	}
	if info.search == nil {
		return nil
	}
	for _, fd := range info.search.decl.Heads() {
		if unit := unitOf(fd); slices.Contains(hidden, unit) {
			return connect.NewError(connect.CodePermissionDenied, fmt.Errorf(
				"%s searches entity %q, whose search field %q the caller does not read on every row", what, info.name, unit))
		}
	}
	return nil
}

// refuseHiddenCond refuses a condition on rows of info that reads a unit a
// guard hides from the bound caller on some row, on those rows or on the rows
// a rel condition reaches. A search condition counts as reading every search
// field. what names the part of the request in the error.
func (e *rowEmitter) refuseHiddenCond(info *entityInfo, what string, cond *syncplanev1.Condition) error {
	if cond == nil {
		return nil
	}
	heads, match := condHeads(cond)
	if err := e.refuseHiddenPaths(info, what, heads); err != nil {
		return err
	}
	if match {
		if err := e.refuseHiddenMatch(info, what); err != nil {
			return err
		}
	}
	rels, err := query.ExtractRels(info.msgType.Descriptor(), cond)
	if err != nil {
		// The request fails on the same condition where it compiles.
		return nil
	}
	for _, node := range rels {
		target, err := e.srv.relTargetInfo(node)
		if err != nil {
			return nil
		}
		if err := e.refuseHiddenCond(target, what, node.Cond.GetCond()); err != nil {
			return err
		}
	}
	return nil
}

// condHeads returns the first segment of each path cond reads on the row, in
// the order cond names them, and whether cond holds a search condition on
// the row. An exists condition reads the collection at its path, and a rel
// condition the reference at its path: their inner conditions read elements
// and other rows.
func condHeads(cond *syncplanev1.Condition) (heads []string, match bool) {
	var walk func(c *syncplanev1.Condition)
	add := func(path []string) {
		if len(path) > 0 {
			heads = append(heads, path[0])
		}
	}
	walk = func(c *syncplanev1.Condition) {
		switch k := c.GetKind().(type) {
		case *syncplanev1.Condition_Cmp:
			add(k.Cmp.GetPath())
		case *syncplanev1.Condition_In:
			add(k.In.GetPath())
		case *syncplanev1.Condition_Str:
			add(k.Str.GetPath())
		case *syncplanev1.Condition_Rep:
			add(k.Rep.GetPath())
		case *syncplanev1.Condition_Present:
			add(k.Present.GetPath())
		case *syncplanev1.Condition_Exists:
			add(k.Exists.GetPath())
		case *syncplanev1.Condition_Rel:
			add(k.Rel.GetPath())
		case *syncplanev1.Condition_Match:
			match = true
		case *syncplanev1.Condition_And:
			for _, sub := range k.And.GetConds() {
				walk(sub)
			}
		case *syncplanev1.Condition_Or:
			for _, sub := range k.Or.GetConds() {
				walk(sub)
			}
		case *syncplanev1.Condition_Not:
			walk(k.Not)
		}
	}
	walk(cond)
	return heads, match
}

// refuseHiddenSubs refuses a subscription of the emitter that names a field a
// guard hides from the bound caller on some row: in its predicate, its
// window's order or a reference its follow spec walks. Which rows the client
// holds would tell it something about values it does not read. It runs once
// access is bound, since what a guard hides depends on it. A subscription the
// client holds already is not refused: it selects nothing while it names such
// a field (see blindSubs).
func (e *rowEmitter) refuseHiddenSubs() error {
	c := e.srv.catalogPtr()
	for _, sub := range e.subs {
		if e.subsAdmitted || slices.ContainsFunc(e.heldSubs, func(h *syncplanev1.Subscription) bool { return proto.Equal(h, sub) }) {
			continue
		}
		info, ok := c.entities[sub.GetEntity()]
		if !ok {
			continue
		}
		what := fmt.Sprintf("the subscription to %q", info.name)
		if err := e.refuseHiddenBase(info, sub, what); err != nil {
			return err
		}
		if err := e.refuseHiddenFollow(sub, what); err != nil {
			return err
		}
	}
	e.subsAdmitted = true
	return nil
}

// refuseHiddenBase refuses sub, a subscription to rows of info, when its
// predicate, its window's order or its relevance reads a field a guard hides
// from the bound caller on some row.
func (e *rowEmitter) refuseHiddenBase(info *entityInfo, sub *syncplanev1.Subscription, what string) error {
	if err := e.refuseHiddenCond(info, what, sub.GetPredicate()); err != nil {
		return err
	}
	var heads []string
	relevance := false
	for _, k := range sub.GetWindow().GetOrderBy() {
		relevance = relevance || k.GetRelevance()
		if len(k.GetPath()) > 0 {
			heads = append(heads, k.GetPath()[0])
		}
	}
	if err := e.refuseHiddenPaths(info, what, heads); err != nil {
		return err
	}
	if relevance {
		return e.refuseHiddenMatch(info, what)
	}
	return nil
}

// subBlind is what the bound caller's access hides of what one subscription
// names (see blindSubs).
type subBlind struct {
	// base is set when its predicate, its window's order or its relevance
	// reads a field hidden from the caller on some row: the subscription
	// selects no row, its window holds none, and it seeds no follow closure.
	base bool
	// follow is set when its follow spec walks a reference hidden from the
	// caller on some row: the subscription seeds no follow closure.
	follow bool
}

// blindSubs returns, by index into the emitter's subscriptions, what the
// bound access hides of what each one names, or nil when it hides nothing of
// any. A subscription the client held when it could read every field it
// names keeps syncing when that changes, and refuseHiddenSubs does not refuse
// it. It selects nothing the hidden values decide instead, so that which rows
// the client holds tells it nothing about them; a resync removes the rows it
// selected, and sends them again once the caller reads the field again. It is
// memoized per binding.
func (e *rowEmitter) blindSubs() ([]subBlind, error) {
	if e.caller == nil || len(e.subs) == 0 {
		return nil, nil
	}
	if e.blindKnown {
		return e.blind, nil
	}
	c := e.srv.catalogPtr()
	var out []subBlind
	for i, sub := range e.subs {
		info, ok := c.entities[sub.GetEntity()]
		if !ok {
			continue
		}
		base, err := isHiddenRefusal(e.refuseHiddenBase(info, sub, ""))
		if err != nil {
			return nil, err
		}
		follow, err := isHiddenRefusal(e.refuseHiddenFollow(sub, ""))
		if err != nil {
			return nil, err
		}
		if !base && !follow {
			continue
		}
		if out == nil {
			out = make([]subBlind, len(e.subs))
		}
		out[i] = subBlind{base: base, follow: follow}
	}
	e.blind, e.blindKnown = out, true
	return out, nil
}

// blindBase reports whether the bound access hides a field that subscription
// i of the emitter selects its rows by (see subBlind.base).
func (e *rowEmitter) blindBase(i int) (bool, error) {
	blind, err := e.blindSubs()
	if err != nil || blind == nil {
		return false, err
	}
	return blind[i].base, nil
}

// blindSeeds reports whether subscription sub of the emitter seeds no follow
// closure under the bound access (see subBlind).
func (e *rowEmitter) blindSeeds(sub *syncplanev1.Subscription) (bool, error) {
	blind, err := e.blindSubs()
	if err != nil || blind == nil {
		return false, err
	}
	for i, s := range e.subs {
		if s == sub {
			return blind[i].base || blind[i].follow, nil
		}
	}
	return false, nil
}

// blindEntity returns, for the subscriptions of the emitter to rows of
// entity, which ones the bound access makes select nothing, as a bit per
// subscription index. Two sides of a resync whose results differ select
// different rows of entity under the same anchors.
func (e *rowEmitter) blindEntity(entity string) (string, error) {
	blind, err := e.blindSubs()
	if err != nil || blind == nil {
		return "", err
	}
	var b strings.Builder
	for i, sub := range e.subs {
		if sub.GetEntity() == entity && (blind[i].base || blind[i].follow) {
			fmt.Fprintf(&b, "%d,", i)
		}
	}
	return b.String(), nil
}

// isHiddenRefusal reports whether err is a refusal of refuseHidden* for a
// field the caller does not read, and returns any other error.
func isHiddenRefusal(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if connect.CodeOf(err) == connect.CodePermissionDenied {
		return true, nil
	}
	return false, err
}

// refuseHiddenFollow refuses a follow spec that walks a reference field a
// guard hides from the bound caller on some row of the entity that holds it:
// a forward step reads the field on the row it leaves, and a reverse step on
// the rows it reaches. It walks the entities as followEntitiesOf does.
func (e *rowEmitter) refuseHiddenFollow(sub *syncplanev1.Subscription, what string) error {
	follow := sub.GetFollow()
	if follow == nil || len(follow.GetPaths()) == 0 {
		return nil
	}
	c := e.srv.catalogPtr()
	g, err := c.refGraphLazy()
	if err != nil {
		return nil
	}
	check := func(entity string, fd protoreflect.FieldDescriptor) error {
		info, ok := c.entities[entity]
		if !ok {
			return nil
		}
		return e.refuseHiddenPaths(info, what, []string{string(fd.Name())})
	}
	depth := max(int(follow.GetDepth()), 1)
	frontier := map[string]bool{sub.GetEntity(): true}
	seen := map[string]bool{}
	for round := 0; round < depth && len(frontier) > 0; round++ {
		next := map[string]bool{}
		for ent := range frontier {
			if seen[ent] {
				continue
			}
			seen[ent] = true
			for _, path := range follow.GetPaths() {
				cur := map[string]bool{ent: true}
				for _, seg := range path.GetSegments() {
					step := map[string]bool{}
					for from := range cur {
						for _, fr := range g.forwardBy(from, seg) {
							if err := check(from, fr.fd); err != nil {
								return err
							}
							step[fr.targetEntity] = true
						}
						if rr := g.reverseBy(from, seg); rr != nil {
							if err := check(rr.sourceEntity, rr.viaField); err != nil {
								return err
							}
							step[rr.sourceEntity] = true
						}
					}
					cur = step
				}
				for to := range cur {
					next[to] = true
				}
			}
		}
		frontier = next
	}
	return nil
}
