package text

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Merge combines two encoded document states into one. It is the merger the
// engine installs for (syncplane.v1.field_behavior).text fields: a pure union of
// items (content immutable, tombstone flags OR-ed), mark operations and
// version vectors, re-encoded canonically — idempotent, commutative and
// associative, with no causal-delivery requirement because every shipped
// state is causally closed by construction.
//
// Ids are never reused, so a same-id item that disagrees on either side —
// different content, or a different position in the tree — is corrupt input
// (two writers shared a peer) and errors rather than silently picking a side.
// The same holds for a same-id mark op that disagrees on anything.
//
// The base is always a full document — stored rows never hold delta state. The
// incoming side may be either: a client that knows what the server already
// confirmed may PATCH with a fragment (fragment.go) carrying only what it holds
// beyond that, which applies as the same union and produces byte-identical
// output whenever the receiver covers the fragment's base. When it does not,
// the error is tagged ErrFragmentUnusable and the remedy is a full-state
// resend.
//
// Neither side may be a plain-string value (plain.go): the error is tagged
// ErrPlainStringUnresolved, and the remedy is to resolve the value under a
// writer identity rather than to resend it.
func Merge(base, incoming []byte) ([]byte, error) {
	if IsFragment(base) {
		return nil, fmt.Errorf("text: merge base is a fragment; stored state is always a full document")
	}
	if IsPlainString(base) {
		return nil, unresolved{fmt.Errorf(
			"text: merge base is a plain-string value; stored state is always a full document")}
	}
	if IsPlainString(incoming) {
		// Applying it means diffing against the base and minting the difference
		// under a writer identity, which no merger has: the peer belongs to
		// whoever applies the value. Resolving happens before the write (the
		// client's enqueue path, server.ApplyTextStrings), so reaching here
		// means a payload skipped that step.
		return nil, unresolved{fmt.Errorf(
			"text: incoming plain-string value must be resolved under a writer identity before a write")}
	}
	if IsFragment(incoming) {
		return ApplyFragmentBytes(base, incoming)
	}
	if bytes.Equal(base, incoming) {
		// Fast path; also canonicalizes nothing — equal inputs are already
		// whatever they are, and re-encoding would not change convergence.
		return base, nil
	}
	a, err := Decode(base, "")
	if err != nil {
		return nil, fmt.Errorf("text: merge base: %w", err)
	}
	if err := a.MergeEncoded(incoming, nil); err != nil {
		return nil, err
	}
	return a.Encode(), nil
}

// MergeEncoded merges incoming — full state or a fragment — into d in place.
// It is the body of Merge and MergeWithFloor with the base already decoded,
// for callers that keep documents live across writes rather than decoding
// stored bytes every time (engine/server's decoded-document cache). Callers
// holding only bytes should use Merge or MergeWithFloor.
//
// floor is the field's compaction floor, nil or empty for a field that was
// never compacted; see MergeWithFloor for what a floor decides.
//
// An incoming plain-string value is rejected with ErrPlainStringUnresolved.
//
// d MUST BE DISCARDED if this returns an error. The fragment path validates
// before it commits, but the full-state path does not, so a failed merge can
// leave d holding part of the incoming side. Encoding it would persist a state
// no replica ever produced.
func (d *Doc) MergeEncoded(incoming []byte, floor map[string]uint64) error {
	if IsPlainString(incoming) {
		return unresolved{fmt.Errorf(
			"text: incoming plain-string value must be resolved under a writer identity before a write")}
	}
	if IsFragment(incoming) {
		f, err := DecodeFragment(incoming)
		if err != nil {
			return err
		}
		if len(floor) == 0 {
			return d.ApplyFragment(f)
		}
		skip, err := d.purgedItems(floor, len(f.items), func(yield func(ID, bool) error) error {
			for i := range f.items {
				if err := yield(f.items[i].id, f.items[i].deleted); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		skipMarks := d.purgedMarks(floor, len(f.marks), func(yield func(ID)) {
			for i := range f.marks {
				yield(f.marks[i].id)
			}
		})
		if err := d.applyFragmentSkipping(f, skip, skipMarks); err != nil {
			return unusableUnlessStale(err)
		}
		return nil
	}
	o, err := Decode(incoming, "")
	if err != nil {
		return fmt.Errorf("text: merge incoming: %w", err)
	}
	if len(floor) == 0 {
		return d.mergeIn(o)
	}
	skip, err := d.purgedItems(floor, len(o.byID), func(yield func(ID, bool) error) error {
		for id, n := range o.byID {
			if err := yield(id, n.deleted); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	skipMarks := d.purgedMarks(floor, len(o.marks), func(yield func(ID)) {
		for _, m := range o.marks {
			yield(m.id)
		}
	})
	return d.mergeInSkipping(o, skip, skipMarks)
}

// MergeDocs merges other into d in place (d grows; other is not modified).
func (d *Doc) MergeDocs(other *Doc) error { return d.mergeIn(other) }

func (d *Doc) mergeIn(other *Doc) error { return d.mergeInSkipping(other, nil, nil) }

// mergeInSkipping is mergeIn with sets of incoming ids to leave out — the
// tombstones and mark operations a compaction already purged from d (see
// compact.go). Both are nil for every ordinary merge, and the paths then behave
// identically. Anything in the incoming side that DEPENDS on a skipped item
// cannot be carried over and reports ErrStateStale.
func (d *Doc) mergeInSkipping(other *Doc, skip, skipMarks map[ID]bool) error {
	// Items: add the missing ones (two passes so parents resolve regardless
	// of order), OR the tombstone flags on the common ones.
	newIDs := make([]ID, 0)
	for id, on := range other.byID {
		if skip[id] {
			continue
		}
		if mine, ok := d.byID[id]; ok {
			if mine.r != on.r {
				return fmt.Errorf("text: merge conflict: item %s:%d has different content on each side",
					id.Peer, id.Counter)
			}
			// Placement is decided once, by the writer that minted the id, and
			// travels with the item — so a disagreement means two writers shared
			// a peer. Checking it is what makes "ids are never reused" an
			// enforced invariant rather than an assumed one: unchecked, this case
			// merges silently with each side keeping its own parent, which makes
			// Merge non-commutative (the same two states merged in opposite
			// orders produce different text).
			if parentID(mine) != parentID(on) || mine.side != on.side {
				return fmt.Errorf("text: merge conflict: item %s:%d has different placement on each side",
					id.Peer, id.Counter)
			}
			if on.deleted {
				mine.deleted = true
			}
			continue
		}
		d.byID[id] = &node{id: id, r: on.r, deleted: on.deleted, side: on.side}
		newIDs = append(newIDs, id)
	}
	// Deterministic attach order (sorted inserts make the result order-
	// independent; this only stabilizes error behavior and iteration).
	sort.Slice(newIDs, func(i, j int) bool { return newIDs[i].less(newIDs[j]) })
	for _, id := range newIDs {
		n := d.byID[id]
		op := other.byID[id]
		if op.parent == other.root {
			n.parent = d.root
		} else {
			p, ok := d.byID[op.parent.id]
			if !ok {
				if skip[op.parent.id] {
					return staleRef("item "+id.Peer+":"+strconv.FormatUint(id.Counter, 10), op.parent.id)
				}
				return fmt.Errorf("text: merge: item %s:%d references unknown parent %s:%d",
					id.Peer, id.Counter, op.parent.id.Peer, op.parent.id.Counter)
			}
			n.parent = p
		}
		addChild(n.parent, n)
	}

	// Marks: union by op id. A same-id op is the same op (ids are unique per
	// writer), so first-seen wins; any disagreement is the same shared-peer
	// corruption the item check above catches, and mark ops allocate out of the
	// same counter space, so they are just as exposed to it.
	seen := make(map[ID]*markOp, len(d.marks))
	for _, m := range d.marks {
		seen[m.id] = m
	}
	for _, m := range other.marks {
		// Checked before the anchor test below: an op the floor says was
		// reclaimed is not stale state to reject, even when the items its
		// boundaries name were reclaimed along with it.
		if skipMarks[m.id] {
			continue
		}
		if mine, ok := seen[m.id]; ok {
			if mine.key != m.key || !sameMarkValue(mine.value, m.value) ||
				mine.start != m.start || mine.end != m.end || mine.expand != m.expand {
				return fmt.Errorf("text: merge conflict: mark op %s:%d differs on each side",
					m.id.Peer, m.id.Counter)
			}
			continue
		}
		// An anchor pointing at a skipped item would leave the merged document
		// unreadable (Decode and Marks both refuse an unresolvable anchor), so a
		// mark op that survives only because its boundary does is a stale write,
		// not a droppable one. Only reachable with a floor in play: without one,
		// both sides are complete documents whose anchors already resolved.
		for _, a := range []Anchor{m.start, m.end} {
			if (a.Kind == AnchorBefore || a.Kind == AnchorAfter) && skip[a.Item] {
				return staleRef("mark op "+m.id.Peer+":"+strconv.FormatUint(m.id.Counter, 10), a.Item)
			}
		}
		cp := *m
		d.marks = append(d.marks, &cp)
		seen[m.id] = &cp
	}

	for p, c := range other.vv {
		d.bumpVV(p, c)
	}
	if len(newIDs) > 0 {
		d.orderDirty = true
	}
	return nil
}

// parentID is the id of n's parent, or the zero ID for a child of the root
// sentinel — a value no real item can hold, since counters are never zero.
func parentID(n *node) ID {
	if n.parent == nil {
		return ID{}
	}
	return n.parent.id
}

// sameMarkValue compares two validated mark values (nil, bool, string or
// float64). Floats compare by bit pattern so a NaN — which Mark rejects but a
// hand-built encoding could still carry — equals itself instead of turning a
// document into a permanent merge failure.
func sameMarkValue(a, b any) bool {
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		return math.Float64bits(af) == math.Float64bits(bf)
	}
	return a == b
}
