package server

import (
	"errors"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/storage"
)

// accessChains walks access_parent chains: from a row to its parent, to the
// parent's parent, and on until a row without a parent. It reads each row's
// parent at most once, so a read that classifies many rows below the same
// ancestors pays one read per distinct ancestor rather than one per row and
// hop.
//
// A value reads one state of the rows. A read path makes one per read
// transaction; a write path drops it after a write to a row that can sit on a
// chain (see txAccess.noteWrite).
type accessChains struct {
	model *accessModel
	// get reads one row, failing with storage.ErrNotFound for a key that has
	// never held one.
	get func(entity, key string) (storage.Row, error)
	// maxDepth is Limits.MaxAccessDepth: a walk stops after this many hops.
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

// chainEdge is one row's access parent edge. ok is false when the row anchors
// at nothing above itself: its entity has no access parent, its reference is
// unset or names no key, or the row does not exist.
type chainEdge struct {
	parent anchorRow
	ok     bool
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
	if row == nil || info.access == nil || info.access.parent == nil {
		return chainEdge{}
	}
	key, ok := parentKeyOf(row, info.access, model.parents[info.name])
	if !ok {
		return chainEdge{}
	}
	return chainEdge{parent: anchorRow{entity: info.access.parentEntity, key: key}, ok: true}
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
	if info := c.model.entities[at.entity]; info != nil && c.model.parents[at.entity] != nil {
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
// at the workspace root, at the row itself, or at a row up its chain within
// maxDepth hops. row is the row's decoded value, nil for a row that does not
// exist, which has no parent. It is the rule the client's canAt mirrors;
// testdata/access/can.json pins both.
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
// workspace root, at the row itself, or up its chain.
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

// holdsAbove reports whether a holds its permission at a row of the chain
// that starts with e, within maxDepth hops.
func (c *accessChains) holdsAbove(a *permAnchors, e chainEdge) (bool, error) {
	if len(a.rows) == 0 {
		return a.root, nil
	}
	for hops := 1; e.ok && hops <= c.maxDepth; hops++ {
		if a.holds(e.parent.entity, e.parent.key) {
			return true, nil
		}
		var err error
		if e, err = c.edge(e.parent); err != nil {
			return false, err
		}
	}
	return a.root, nil
}
