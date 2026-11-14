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
