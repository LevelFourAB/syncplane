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
	// overrides replace the parent edges of the rows they name. The old side
	// of a resync reads with the parents that moved rows had when its cursor
	// was issued.
	overrides map[anchorRow]chainEdge
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
	return &accessChains{model: model, get: get, maxDepth: maxDepth, report: report, edges: map[anchorRow]chainEdge{}}
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
	var e chainEdge
	// A row of an entity without an access parent ends every chain, so the
	// common one-level case reads nothing.
	if info := c.model.entities[at.entity]; info != nil && len(c.model.parents[at.entity]) > 0 {
		row, err := c.get(at.entity, at.key)
		switch {
		case errors.Is(err, storage.ErrNotFound):
		case err != nil:
			return chainEdge{}, err
		case row.Deleted:
		default:
			msg, err := info.decode(row.Value)
			if err != nil {
				c.report(row, err)
			} else {
				e = edgeOf(c.model, info, msg.ProtoReflect())
			}
		}
	}
	c.edges[at] = e
	return e, nil
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
