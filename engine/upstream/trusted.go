package upstream

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

// The origin refuses a PATCH from the link whose mask reaches a field only a
// trusted write changes there (server.EntityMeta.TrustedOnlyFields), and the
// refusal reverts every write of the entry. The local engine's trusted code
// may still compute such fields, so the link keeps those values local: the
// push leaves them out, and replay shows them until the origin's own row
// replaces them. The local schema decides which fields these are, as it
// decides everything else the link replays.
//
// The origin's trusted code usually computes such a field in a change of its
// own, after the write that caused it. The row the write itself produced at
// the origin leaves the field unset, and taking that row as it is would show
// the field empty until the computed change arrives. So when a write settles,
// each such unit the origin's row leaves unset keeps the write's local value,
// recorded in confirmedRow.kept. Rows the link's own later writes produce at
// the origin keep those values too. Any other origin change of the row
// replaces them, and so does a value the origin sets itself.

// trustedOnlyPath reports whether a PATCH mask path of entity writes into a
// field only a trusted write changes at the origin. The path's first segment
// names the unit, as the origin matches it.
func (b *bound) trustedOnlyPath(entity, path string) bool {
	head, _, _ := strings.Cut(path, ".")
	return slices.Contains(b.trustedOnly[entity], head)
}

// localOnly reports whether the origin takes nothing of a write from the
// link: a PATCH whose every mask path writes into a field only a trusted
// write changes at the origin.
func (b *bound) localOnly(entity string, kind syncplanev1.CrudOp, mask []string) bool {
	if kind != syncplanev1.CrudOp_CRUD_OP_PATCH || len(mask) == 0 || len(b.trustedOnly[entity]) == 0 {
		return false
	}
	for _, p := range mask {
		if !b.trustedOnlyPath(entity, p) {
			return false
		}
	}
	return true
}

// sendable returns op as the link pushes it: a PATCH without the mask paths
// that write into a field only a trusted write changes at the origin, and
// without those fields in its value. ok is false when nothing of op is left.
// Any other op is returned as it is.
func (b *bound) sendable(op entryOp) (_ entryOp, ok bool) {
	if op.kind != syncplanev1.CrudOp_CRUD_OP_PATCH || len(b.trustedOnly[op.entity]) == 0 {
		return op, true
	}
	mask := slices.DeleteFunc(slices.Clone(op.mask), func(p string) bool { return b.trustedOnlyPath(op.entity, p) })
	switch {
	case len(mask) == len(op.mask):
		return op, true
	case len(mask) == 0:
		return op, false
	}
	op.mask = mask
	// The origin reads only the masked paths of a PATCH value, so a value
	// that does not decode is sent as captured.
	if v, err := b.models[op.entity].ClearUnits(op.value, b.trustedOnly[op.entity]); err == nil {
		op.value = v
	}
	return op, true
}

// sendableOps returns the ops of an entry as the link pushes them (see
// sendable), without the ops nothing is left of.
func (b *bound) sendableOps(ops []entryOp) []entryOp {
	out := make([]entryOp, 0, len(ops))
	for _, op := range ops {
		if sop, ok := b.sendable(op); ok {
			out = append(out, sop)
		}
	}
	return out
}

// keepLocal records a CRUD write the origin takes nothing of (see localOnly)
// without a pending entry of its own. An entry takes the next origin
// mutation id, and the origin refuses the ids after one the link never
// pushes as a gap.
//
// An op on a row that pending entries touch joins the newest of them, so
// replay keeps its value on the row until that entry settles and the origin's
// row replaces it. On a row no pending entry touches, an elided confirmed
// record takes the new local row as its own, so the value stays until the
// origin's next change of the row replaces the record. seq is the version the
// write stored its rows at.
func keepLocal(b *bound, st stateTx, seq uint64, ops []server.CapturedOp) error {
	for _, op := range ops {
		ids, err := st.touched(op.Entity, op.Key)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			c, ok, err := st.confirmed(op.Entity, op.Key)
			if err != nil {
				return err
			}
			// Any other record already differs from the local row, and the
			// next recompute of the row writes the record back.
			if ok && c.elided && c.localVersion == op.PrevVersion {
				c.localVersion = seq
				for _, p := range op.Mask {
					if head, _, _ := strings.Cut(p, "."); !slices.Contains(c.kept, head) {
						c.kept = append(c.kept, head)
					}
				}
				slices.Sort(c.kept)
				if err := st.putConfirmed(op.Entity, op.Key, c); err != nil {
					return err
				}
			}
			continue
		}
		host, err := st.entry(slices.Max(ids))
		if err != nil {
			return err
		}
		if host == nil {
			return fmt.Errorf("upstream: %s %q is marked with pending entry %d, which is gone", op.Entity, op.Key, slices.Max(ids))
		}
		text, err := b.models[op.Entity].CaptureText(rebase.Op{Kind: op.Op, Value: op.Value, Mask: op.Mask}, op.After)
		if err != nil {
			return fmt.Errorf("upstream: capture %s %q: %w", op.Entity, op.Key, err)
		}
		host.ops = append(host.ops, entryOp{
			entity: op.Entity, key: op.Key, kind: op.Op,
			value: bytes.Clone(op.Value), mask: slices.Clone(op.Mask), text: text,
		})
		if err := st.putEntry(host); err != nil {
			return err
		}
	}
	return nil
}

// withheld returns the part of op the origin may not take from the link, as
// an op to replay on the origin's row, and the units it writes: the mask
// paths of a PATCH that write into a field only a trusted write changes at
// the origin, or such fields of a PUT, which the origin keeps at their stored
// value or clears on a create. ok is false when the origin can take all of
// op, and for a DELETE.
func (b *bound) withheld(op entryOp) (_ rebase.Op, units []string, ok bool) {
	closed := b.trustedOnly[op.entity]
	if len(closed) == 0 {
		return rebase.Op{}, nil, false
	}
	var mask []string
	switch op.kind {
	case syncplanev1.CrudOp_CRUD_OP_PATCH:
		for _, p := range op.mask {
			if head, _, _ := strings.Cut(p, "."); slices.Contains(closed, head) {
				mask = append(mask, p)
				if !slices.Contains(units, head) {
					units = append(units, head)
				}
			}
		}
	case syncplanev1.CrudOp_CRUD_OP_PUT:
		for _, u := range closed {
			if op.mask == nil || slices.Contains(op.mask, u) {
				units = append(units, u)
			}
		}
		// The units named outright replace the origin's, as the PUT replaces
		// them locally.
		mask = units
	}
	if len(mask) == 0 {
		return rebase.Op{}, nil, false
	}
	return rebase.Op{Kind: syncplanev1.CrudOp_CRUD_OP_PATCH, Value: op.value, Mask: mask, Text: op.text}, units, true
}

// keepWithheld gives the confirmed record of each row that the applied CRUD
// entry e writes the local values the origin did not take from it (see
// withheld), where the origin's row leaves them unset. seq is the sequence the
// origin applied e at. A record holds them only while it is a row one of the
// link's own writes produced: at seq, or at the sequence of another pending
// entry on the row. Call it after e leaves the queue and before its rows are
// recomputed.
func (t *txn) keepWithheld(e *entry, seq uint64) error {
	for _, row := range e.rows() {
		var (
			ops     []rebase.Op
			reached []string
		)
		for _, op := range e.ops {
			if op.entity != row.Entity || op.key != row.Key {
				continue
			}
			if op.kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
				ops, reached = nil, nil
				continue
			}
			if wop, units, ok := t.b.withheld(op); ok {
				ops = append(ops, wop)
				for _, u := range units {
					if !slices.Contains(reached, u) {
						reached = append(reached, u)
					}
				}
			}
		}
		if len(ops) == 0 {
			continue
		}
		if err := t.keepRow(row, seq, ops, reached); err != nil {
			return err
		}
	}
	return nil
}

// keepRow is keepWithheld for one row r: ops are the withheld parts of the
// entry's writes to r, and reached the units they write.
func (t *txn) keepRow(r RowRef, seq uint64, ops []rebase.Op, reached []string) error {
	c, ok, err := t.st.confirmed(r.Entity, r.Key)
	if err != nil || !ok || !c.present || c.elided || c.stale {
		return err
	}
	if c.version != seq {
		if own, err := t.ownVersion(r, c.version); err != nil || !own {
			return err
		}
	}
	m := t.b.models[r.Entity]
	local, err := m.Replay(rebase.Row{Present: true, Value: c.value}, ops)
	if err != nil || !local.Present {
		// The row stays as the origin confirmed it, which is what replay
		// of an op it cannot use would show anyway.
		return nil
	}
	// A unit the origin set itself keeps the origin's value. One it left
	// unset, or one that holds an earlier local value, takes this write's.
	set, err := m.SetUnits(c.value, reached)
	if err != nil {
		return err
	}
	take := slices.DeleteFunc(reached, func(u string) bool {
		return slices.Contains(set, u) && !slices.Contains(c.kept, u)
	})
	if err := keepUnits(m, &c, local.Value, take); err != nil {
		return err
	}
	return t.st.putConfirmed(r.Entity, r.Key, c)
}

// carryKept gives c, the record of an origin change of row r, the local
// values the previous record kept (see keepWithheld): prev is that record's
// row value and kept its units. It does so only when the change is a row one
// of the link's pending entries produced, and only for the units the change
// leaves unset. Any other change replaces them.
func (t *txn) carryKept(r RowRef, c *confirmedRow, prev []byte, kept []string) error {
	own, err := t.ownVersion(r, c.version)
	if err != nil || !own {
		return err
	}
	m := t.b.models[r.Entity]
	set, err := m.SetUnits(c.value, kept)
	if err != nil {
		return err
	}
	take := slices.DeleteFunc(slices.Clone(kept), func(u string) bool { return slices.Contains(set, u) })
	return keepUnits(m, c, prev, take)
}

// keepUnits copies the units take names from the row value local into c's
// value, and records in c.kept those that local sets.
func keepUnits(m *rebase.Model, c *confirmedRow, local []byte, take []string) error {
	if len(take) == 0 {
		return nil
	}
	value, err := m.CopyUnits(c.value, local, take)
	if err != nil {
		return err
	}
	set, err := m.SetUnits(local, take)
	if err != nil {
		return err
	}
	kept := slices.DeleteFunc(slices.Clone(c.kept), func(u string) bool { return slices.Contains(take, u) })
	kept = append(kept, set...)
	slices.Sort(kept)
	c.value, c.kept = value, kept
	return nil
}

// ownVersion reports whether version is the sequence the origin applied a
// pending entry on row r at.
func (t *txn) ownVersion(r RowRef, version uint64) (bool, error) {
	ids, err := t.st.touched(r.Entity, r.Key)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		res, ok, err := t.st.result(id)
		if err != nil {
			return false, err
		}
		if ok && res.seq == version && res.status != syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
			return true, nil
		}
	}
	return false, nil
}
