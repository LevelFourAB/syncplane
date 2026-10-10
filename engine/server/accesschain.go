package server

import (
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/storage"
)

// accessChains walks access_parent chains: from a row to its parents, to
// their parents, and on until rows without a parent. A row with several
// parents makes the rows above it a graph, which a walk visits breadth first.
// It reads each row's parents at most once, so a read that classifies many
// rows below the same ancestors pays one read per distinct ancestor rather
// than one per row and hop.
//
// A value reads one state of the rows. A read path makes one per read
// transaction; a write path drops it after a write to a row that can sit on a
// chain (see txAccess.noteWrite).
type accessChains struct {
	model *accessModel
	// get reads one row, failing with storage.ErrNotFound for a key that has
	// never held one.
	get func(entity, key string) (storage.Row, error)
	// maxDepth is Limits.MaxAccessDepth: a walk stops after this many hops on
	// every path.
	maxDepth int
	// report is told about a row on a chain that does not decode. The row
	// anchors at nothing above itself, so nothing can show that a permission
	// held above it reaches the rows below it.
	report func(storage.Row, error)
	edges  map[anchorRow]chainEdge
	// withins holds what the within checks read of each row read so far, for
	// the rows of entities a within entry names.
	withins map[anchorRow]rowWithin
	// overrides replace the parent edges of the rows they name, and
	// withinOverrides what the within checks read of them. The old side of a
	// resync reads with what moved rows held when its cursor was issued.
	overrides       map[anchorRow]chainEdge
	withinOverrides map[anchorRow]rowWithin
}

// rowWithin is what the within checks that name a row's entity read of the
// row: the value of each subject field, and the conditions it matches. The
// zero value is a row that does not exist, which passes no check.
type rowWithin struct {
	live bool
	// subjects maps each subject field a check reads, by name, to its value.
	subjects map[string]string
	// matched are the when specs of the checks whose condition the row
	// matches, sorted.
	matched []string
}

// withinStateOf returns what the within checks that name info read of row,
// nil for a row that does not exist.
func withinStateOf(model *accessModel, info *entityInfo, row protoreflect.Message) rowWithin {
	checks := model.withinOn[info.name]
	if row == nil || len(checks) == 0 {
		return rowWithin{}
	}
	st := rowWithin{live: true}
	for _, w := range checks {
		if w.callerIs != nil {
			if st.subjects == nil {
				st.subjects = map[string]string{}
			}
			st.subjects[string(w.callerIs.Name())] = row.Get(w.callerIs).String()
		}
		if w.when != nil && !slices.Contains(st.matched, w.whenSpec) && w.when.Eval(row) {
			st.matched = append(st.matched, w.whenSpec)
		}
	}
	slices.Sort(st.matched)
	return st
}

// passes reports whether the row passes w for a caller with subjects, which
// are sorted.
func (st rowWithin) passes(w *withinCheck, subjects []string) bool {
	if !st.live {
		return false
	}
	if w.callerIs != nil && !subjectMatches(subjects, st.subjects[string(w.callerIs.Name())]) {
		return false
	}
	if w.when != nil {
		if _, ok := slices.BinarySearch(st.matched, w.whenSpec); !ok {
			return false
		}
	}
	return true
}

// key renders st as a string that equals the key of another state exactly
// when the two are equal.
func (st rowWithin) key() string {
	names := make([]string, 0, len(st.subjects))
	for name := range st.subjects {
		names = append(names, name)
	}
	slices.Sort(names)
	values := make([]string, len(names))
	for i, name := range names {
		values[i] = name + "=" + st.subjects[name]
	}
	return fmt.Sprintf("%t%q%q", st.live, values, st.matched)
}

// equal reports whether st and o read the same of their rows.
func (st rowWithin) equal(o rowWithin) bool {
	if st.live != o.live || len(st.subjects) != len(o.subjects) || !slices.Equal(st.matched, o.matched) {
		return false
	}
	for k, v := range st.subjects {
		if w, ok := o.subjects[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// chainEdge is one row's access parent edges: the rows its access parents
// name, sorted by entity and key and without repeats (see edgeTo). It is
// empty when the row anchors at nothing above itself: its entity has no
// access parent, every reference is unset or names no key, or the row does
// not exist. Two edges compare with equal, since the rows are a slice.
type chainEdge struct {
	parents []anchorRow
}

// edgeTo returns the edge to parents, which it does not change.
func edgeTo(parents ...anchorRow) chainEdge {
	return chainEdge{parents: compactRows(slices.Clone(parents))}
}

// ok reports whether the row sits below at least one row.
func (e chainEdge) ok() bool {
	return len(e.parents) > 0
}

// equal reports whether e and o name the same rows.
func (e chainEdge) equal(o chainEdge) bool {
	return slices.Equal(e.parents, o.parents)
}

// key renders the rows e names as a string that equals the key of another
// edge exactly when the two edges are equal.
func (e chainEdge) key() string {
	return fmt.Sprintf("%q", e.parents)
}

// newAccessChains returns a walker over the rows tx reads.
func newAccessChains(model *accessModel, tx storage.ReadTx, maxDepth int, report func(storage.Row, error)) *accessChains {
	return newAccessChainsFrom(model, tx.GetRow, maxDepth, report)
}

// newAccessChainsFrom returns a walker over the rows get reads.
func newAccessChainsFrom(
	model *accessModel,
	get func(entity, key string) (storage.Row, error),
	maxDepth int,
	report func(storage.Row, error),
) *accessChains {
	return &accessChains{
		model:    model,
		get:      get,
		maxDepth: maxDepth,
		report:   report,
		edges:    map[anchorRow]chainEdge{},
		withins:  map[anchorRow]rowWithin{},
	}
}

// edgeOf returns the parent edge of a row of info whose value is row, nil for
// a row that does not exist.
func edgeOf(model *accessModel, info *entityInfo, row protoreflect.Message) chainEdge {
	if row == nil {
		return chainEdge{}
	}
	return chainEdge{parents: parentsOf(row, model.parents[info.name])}
}

// firstEdge returns the parent edge of a row whose value the caller holds.
func (c *accessChains) firstEdge(info *entityInfo, key string, row protoreflect.Message) chainEdge {
	if e, ok := c.overrides[anchorRow{entity: info.name, key: key}]; ok {
		return e
	}
	return edgeOf(c.model, info, row)
}

// edge returns the parent edge of the row at, reading the row the first time.
func (c *accessChains) edge(at anchorRow) (chainEdge, error) {
	if e, ok := c.overrides[at]; ok {
		return e, nil
	}
	if e, ok := c.edges[at]; ok {
		return e, nil
	}
	// A row of an entity without an access parent ends every chain, so the
	// common one-level case reads nothing.
	if len(c.model.parents[at.entity]) == 0 {
		c.edges[at] = chainEdge{}
		return chainEdge{}, nil
	}
	if err := c.load(at); err != nil {
		return chainEdge{}, err
	}
	return c.edges[at], nil
}

// withinState returns what the within checks read of the row at, reading the
// row the first time.
func (c *accessChains) withinState(at anchorRow) (rowWithin, error) {
	if st, ok := c.withinOverrides[at]; ok {
		return st, nil
	}
	if st, ok := c.withins[at]; ok {
		return st, nil
	}
	if err := c.load(at); err != nil {
		return rowWithin{}, err
	}
	return c.withins[at], nil
}

// load reads the row at and memoizes its parent edge and what the within
// checks read of it. A row that does not exist or does not decode has
// neither.
func (c *accessChains) load(at anchorRow) error {
	info := c.model.entities[at.entity]
	var msg protoreflect.Message
	if info != nil {
		row, err := c.get(at.entity, at.key)
		switch {
		case errors.Is(err, storage.ErrNotFound):
		case err != nil:
			return err
		case row.Deleted:
		default:
			decoded, err := info.decode(row.Value)
			if err != nil {
				c.report(row, err)
			} else {
				msg = decoded.ProtoReflect()
			}
		}
	}
	if info == nil || msg == nil {
		c.edges[at], c.withins[at] = chainEdge{}, rowWithin{}
		return nil
	}
	c.edges[at] = edgeOf(c.model, info, msg)
	c.withins[at] = withinStateOf(c.model, info, msg)
	return nil
}

// firstWithin returns what the within checks read of a row whose value the
// caller holds.
func (c *accessChains) firstWithin(info *entityInfo, key string, row protoreflect.Message) rowWithin {
	if st, ok := c.withinOverrides[anchorRow{entity: info.name, key: key}]; ok {
		return st
	}
	return withinStateOf(c.model, info, row)
}

// withinAll reports whether the row of info with key passes every within
// entry of rule for a caller with subjects, which are sorted: for each, the
// row itself or a row above it on some path of at most maxDepth hops is a row
// of the entry's entity that passes its checks. row is the row's decoded
// value, nil for a row that does not exist. It is the rule the client's
// withinAt mirrors.
func (c *accessChains) withinAll(rule accessRule, subjects []string, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	for _, entry := range rule.within {
		w := c.model.withins[entry.spec()]
		if w == nil {
			return false, nil
		}
		if w.target.name == info.name && c.firstWithin(info, key, row).passes(w, subjects) {
			continue
		}
		ok, err := c.withinAbove(w, subjects, c.firstEdge(info, key, row))
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// withinAllAt reports whether the row at passes every within entry of rule
// that rows at or above it can decide, as withinAll decides it for a row. An
// entry whose entity's rows cannot sit at or above at holds: what decides it
// for the rows below at sits between them and at, and does not change with
// at or the rows above it.
func (c *accessChains) withinAllAt(rule accessRule, subjects []string, at anchorRow) (bool, error) {
	return c.withinAllFrom(rule, subjects, at.entity,
		func() (rowWithin, error) { return c.withinState(at) },
		func() (chainEdge, error) { return c.edge(at) })
}

// withinAllOn is withinAllAt for the row of info with key whose value the
// caller holds, nil for a row that does not exist.
func (c *accessChains) withinAllOn(rule accessRule, subjects []string, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	return c.withinAllFrom(rule, subjects, info.name,
		func() (rowWithin, error) { return c.firstWithin(info, key, row), nil },
		func() (chainEdge, error) { return c.firstEdge(info, key, row), nil })
}

// withinAllFrom decides withinAllAt for a row of entity, given what the
// within checks read of the row and its parent edge.
func (c *accessChains) withinAllFrom(
	rule accessRule,
	subjects []string,
	entity string,
	self func() (rowWithin, error),
	edge func() (chainEdge, error),
) (bool, error) {
	for _, entry := range rule.within {
		w := c.model.withins[entry.spec()]
		if w == nil {
			return false, nil
		}
		if entity != w.target.name && !slices.Contains(c.model.below[w.target.name], entity) {
			continue
		}
		if entity == w.target.name {
			st, err := self()
			if err != nil {
				return false, err
			}
			if st.passes(w, subjects) {
				continue
			}
		}
		e, err := edge()
		if err != nil {
			return false, err
		}
		ok, err := c.withinAbove(w, subjects, e)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// withinAbove reports whether a row above the edge e, on any path of at most
// maxDepth hops, is a row of w's entity that passes w for a caller with
// subjects. It walks only the rows whose entity can sit below w's entity.
func (c *accessChains) withinAbove(w *withinCheck, subjects []string, e chainEdge) (bool, error) {
	target := w.target.name
	leads := func(at anchorRow) bool {
		return at.entity == target || slices.Contains(c.model.below[target], at.entity)
	}
	seen := map[anchorRow]struct{}{}
	var frontier []anchorRow
	for _, at := range e.parents {
		if leads(at) {
			seen[at] = struct{}{}
			frontier = append(frontier, at)
		}
	}
	for hops := 1; len(frontier) > 0; hops++ {
		for _, at := range frontier {
			if at.entity != target {
				continue
			}
			st, err := c.withinState(at)
			if err != nil {
				return false, err
			}
			if st.passes(w, subjects) {
				return true, nil
			}
		}
		if hops >= c.maxDepth {
			break
		}
		var next []anchorRow
		for _, at := range frontier {
			up, err := c.edge(at)
			if err != nil {
				return false, err
			}
			for _, p := range up.parents {
				if _, dup := seen[p]; !dup && leads(p) {
					seen[p] = struct{}{}
					next = append(next, p)
				}
			}
		}
		frontier = next
	}
	return false, nil
}

// holds reports whether a holds its permission at the row of info with key:
// at the workspace root, at the row itself, or at a row above it on any path
// of at most maxDepth hops. row is the row's decoded value, nil for a row that
// does not exist, which has no parent. It is the rule the client's canAt
// mirrors; testdata/access/can.json pins both.
func (c *accessChains) holds(a *permAnchors, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	if a == nil {
		return false, nil
	}
	if a.holds(info.name, key) {
		return true, nil
	}
	return c.holdsAbove(a, c.firstEdge(info, key, row))
}

// holdsAt reports whether a holds its permission at the row at: at the
// workspace root, at the row itself, or at a row above it.
func (c *accessChains) holdsAt(a *permAnchors, at anchorRow) (bool, error) {
	if a == nil {
		return false, nil
	}
	if a.holds(at.entity, at.key) {
		return true, nil
	}
	e, err := c.edge(at)
	if err != nil {
		return false, err
	}
	return c.holdsAbove(a, e)
}

// holdsAll reports whether each of anchors holds its permission at the row of
// info with key, as holds decides it for one. Each can hold at a different
// row above it.
func (c *accessChains) holdsAll(anchors []*permAnchors, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	for _, a := range anchors {
		if ok, err := c.holds(a, info, key, row); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// holdsAllAt reports whether each of anchors holds its permission at the row
// at, as holdsAt decides it for one.
func (c *accessChains) holdsAllAt(anchors []*permAnchors, at anchorRow) (bool, error) {
	for _, a := range anchors {
		if ok, err := c.holdsAt(a, at); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// holdsThroughEach reports whether a holds its permission at the row of info
// with key through each of the row's parents on its own: at the workspace
// root, at the row itself, or at that parent or a row above it. A row without
// a parent needs the root or the row. row is the row's decoded value, nil for
// a row that does not exist. It is the rule the client's canAtEach mirrors;
// testdata/access/can.json pins both.
func (c *accessChains) holdsThroughEach(a *permAnchors, info *entityInfo, key string, row protoreflect.Message) (bool, error) {
	if a == nil {
		return false, nil
	}
	if a.holds(info.name, key) {
		return true, nil
	}
	return c.holdsAtEach(a, c.firstEdge(info, key, row).parents)
}

// holdsAtEach reports whether a holds its permission at each of rows, as
// holdsAt decides it for one. It reports false for no rows.
func (c *accessChains) holdsAtEach(a *permAnchors, rows []anchorRow) (bool, error) {
	if len(rows) == 0 {
		return false, nil
	}
	for _, at := range rows {
		if ok, err := c.holdsAt(a, at); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// holdsAbove reports whether a holds its permission at a row above the edge
// e, on any path of at most maxDepth hops.
func (c *accessChains) holdsAbove(a *permAnchors, e chainEdge) (bool, error) {
	if len(a.rows) == 0 {
		return a.root, nil
	}
	frontier := e.parents
	// Allocated only once a level holds two rows, so a walk up a row with one
	// parent at each step allocates nothing. A row that two paths reach is
	// then walked once.
	var seen map[anchorRow]struct{}
	for hops := 1; len(frontier) > 0; hops++ {
		for _, at := range frontier {
			if a.holds(at.entity, at.key) {
				return true, nil
			}
		}
		if hops >= c.maxDepth {
			break
		}
		var next []anchorRow
		if len(frontier) == 1 {
			up, err := c.edge(frontier[0])
			if err != nil {
				return false, err
			}
			next = up.parents
		} else {
			for _, at := range frontier {
				up, err := c.edge(at)
				if err != nil {
					return false, err
				}
				next = append(next, up.parents...)
			}
		}
		if len(next) > 1 {
			if seen == nil {
				seen = make(map[anchorRow]struct{}, len(frontier)+len(next))
				for _, at := range frontier {
					seen[at] = struct{}{}
				}
			}
			// A new slice: next can be a row's cached edge.
			fresh := make([]anchorRow, 0, len(next))
			for _, at := range next {
				if _, dup := seen[at]; !dup {
					seen[at] = struct{}{}
					fresh = append(fresh, at)
				}
			}
			next = fresh
		}
		frontier = next
	}
	return a.root, nil
}
