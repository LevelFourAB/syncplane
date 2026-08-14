package text

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// Compaction: dropping the tombstones and mark operations a document no longer
// needs, and the floor that keeps replicas from putting them back.
//
// Everything else in this package grows monotonically — a deleted character
// keeps its item so merge can verify identity and anchors keep resolving, and a
// mark operation is never physically removed because removal is itself an
// operation. That is what makes Merge a pure union with no causal-delivery
// requirement, and it is also why a long-lived document never shrinks.
// Compaction is the escape hatch: a maintenance pass (the engine's own sweep,
// see engine/server/compact.go) drops the state that nothing can still
// refer to — tombstones nothing points at, mark ops that can no longer win
// anywhere (see deadMarks) — and records the document's version vector as that
// field's COMPACTION FLOOR.
//
// The floor is what makes the drop safe. A replica that still holds purged
// state can only have got it from a state the floor already covers, so "the
// incoming side carries an id the stored row lacks, at an id the floor
// dominates" identifies purged state exactly — nothing else can satisfy both
// halves. Item and mark ids come out of the same counter space, so ONE floor
// covers both. Writes are then split by what that state would do:
//
//   - The item arrives as a TOMBSTONE, or the id names a purged MARK OP: the
//     sender is carrying state the receiver deliberately reclaimed and nothing
//     is lost by leaving it out, so the write is applied with that id skipped.
//     Replicas that have not re-read the row keep working instead of being cut
//     off, which matters because compaction targets quiet rows precisely when
//     nobody is looking.
//   - The item arrives LIVE, or the write hangs new state off one (a child
//     item, a mark anchor): applying it would resurrect text the document
//     dropped, or leave a dangling reference. Rejected with ErrStateStale; the
//     remedy is to re-read the row and re-apply the edit against it, which
//     loses only the marks the stale side minted (see the README).
//
// A mark op the sender minted below the floor that the receiver never saw is
// indistinguishable from one it purged, and is dropped silently. That is the
// same trade the tombstone case already makes, and it needs the same thing to
// stay rare: writers push in order and re-read before editing again.
//
// The TypeScript mirror (client/src/text/compact.ts) implements the identical
// rules; testdata/text/vectors.json pins the compacted encodings byte-for-byte.

// ErrStateStale classifies a write that cannot be applied because it is built
// on state a compaction dropped. Unlike ErrFragmentUnusable — where the sender
// simply has to send more — no resend of the same bytes can succeed: the sender
// has to re-read the row first.
var ErrStateStale = errors.New("text: state predates the compaction floor")

// stale tags an error as ErrStateStale while reporting the underlying message
// verbatim, exactly as unusable does for ErrFragmentUnusable.
type stale struct{ error }

func (e stale) Unwrap() error { return e.error }

func (e stale) Is(target error) bool { return target == ErrStateStale }

// deadMarks reports the mark operations that can no longer affect resolution —
// in this document or in any document reachable from it by further edits — and
// which compaction may therefore drop. Nothing else may be dropped: a mark op
// is state, and merge is a union, so an op removed while it still matters comes
// straight back from the next replica that holds it.
//
// Two rules, both of which have to hold for EVERY FUTURE document and not just
// this one. That distinction is the whole difficulty: "shadowed today" is not
// sufficient, because ops with different expand policies diverge at their
// boundaries. Op A covering [3,7) with ExpandBoth is entirely shadowed by a
// higher-id B covering [3,7) with ExpandNone — until one character is typed at
// edge 3, which A covers and B does not, and A is revealed.
//
//  1. A is dead when a higher-id op on the same key has IDENTICAL anchors.
//     Identical anchors mean identical coverage under every future insert, and
//     the higher id wins every character they both cover, so A can never be the
//     winner anywhere. This is the toggle-the-same-range case — the bulk of
//     what a long editing session accumulates, and exactly what a collapsed
//     mark plus its cancelling unmark look like.
//
//  2. A group of surviving ops on one key that overlap or TOUCH each other,
//     and nothing else on that key, is dead when every op in it is nil-valued.
//     Removal ops exist only to shadow marks beneath them; with none left they
//     paint "removed" over a stretch where nothing paints anything, which is
//     what the absence of the ops paints too. Touching counts as overlapping
//     because an insert at the shared edge can fall inside both, so a group has
//     to be separated from the rest of the key by a real gap to be judged
//     alone.
//
// Ops whose anchors do not resolve are left alone, and so is every other op on
// their key: Compact cannot fail, and a document assembled from foreign parts
// is not one to start reclaiming state from.
//
// What this deliberately does NOT use is "covers nothing". A collapsed mark
// covers nothing permanently and by design (marks.go), so emptiness stops being
// a proxy for deadness the moment they exist — a rule written that way would
// reclaim every zero-width annotation in the document.
func (d *Doc) deadMarks() map[ID]bool {
	if len(d.marks) == 0 {
		return nil
	}
	d.ensureOrder()
	fullIndex := make(map[ID]int, len(d.order))
	for i, it := range d.order {
		fullIndex[it.id] = i
	}
	anchorPos := func(a Anchor) (int, bool) {
		switch a.Kind {
		case AnchorDocStart:
			return 0, true
		case AnchorDocEnd:
			return len(d.order), true
		}
		i, ok := fullIndex[a.Item]
		if !ok {
			return 0, false
		}
		if a.Kind == AnchorBefore {
			return i, true
		}
		return i + 1, true
	}

	dead := make(map[ID]bool)

	// Rule 1: per (key, start anchor, end anchor), everything but the highest
	// id goes.
	type shape struct {
		key        string
		start, end Anchor
	}
	top := make(map[shape]ID, len(d.marks))
	for _, m := range d.marks {
		s := shape{key: m.key, start: m.start, end: m.end}
		if cur, ok := top[s]; !ok || cur.less(m.id) {
			top[s] = m.id
		}
	}
	for _, m := range d.marks {
		if top[shape{key: m.key, start: m.start, end: m.end}] != m.id {
			dead[m.id] = true
		}
	}

	// Rule 2: group the survivors per key into runs of touching-or-overlapping
	// CLOSED edge intervals, and drop any run that is all removals.
	type interval struct {
		lo, hi int
		op     *markOp
	}
	perKey := make(map[string][]interval, len(d.marks))
	opaque := make(map[string]bool) // keys holding an op whose anchors don't resolve
	for _, m := range d.marks {
		if dead[m.id] {
			continue
		}
		lo, okLo := anchorPos(m.start)
		hi, okHi := anchorPos(m.end)
		if !okLo || !okHi {
			opaque[m.key] = true
			continue
		}
		if hi < lo {
			hi = lo // inverted anchors cover nothing; judge it as the point it is
		}
		perKey[m.key] = append(perKey[m.key], interval{lo: lo, hi: hi, op: m})
	}
	for key, ivs := range perKey {
		if opaque[key] {
			continue
		}
		// Total order, so both languages sweep the same sequence.
		sort.Slice(ivs, func(i, j int) bool {
			if ivs[i].lo != ivs[j].lo {
				return ivs[i].lo < ivs[j].lo
			}
			if ivs[i].hi != ivs[j].hi {
				return ivs[i].hi < ivs[j].hi
			}
			return ivs[i].op.id.less(ivs[j].op.id)
		})
		for i := 0; i < len(ivs); {
			j, reach, allNil := i, ivs[i].hi, ivs[i].op.value == nil
			for j+1 < len(ivs) && ivs[j+1].lo <= reach {
				j++
				if ivs[j].hi > reach {
					reach = ivs[j].hi
				}
				if ivs[j].op.value != nil {
					allNil = false
				}
			}
			if allNil {
				for _, iv := range ivs[i : j+1] {
					dead[iv.op.id] = true
				}
			}
			i = j + 1
		}
	}
	return dead
}

// Compact drops the mark operations that can no longer affect resolution and
// every tombstone the document can then no longer need, returning how many of
// each it removed. The version vector is left alone: it is the record of what
// this document has seen, and dropping entries from it would make the ids of
// purged items look unseen again (see issues/106).
//
// Mark ops go FIRST, and the set of anchored items is computed after they do:
// an op is the only thing keeping its boundary tombstones alive, so reclaiming
// one is what makes them reclaimable in the same pass. The reverse order
// reclaims nothing on the item side until the next pass.
//
// An item is purgeable when it is deleted, has no children, and no mark
// operation anchors to it. The three conditions are what keep the result a
// well-formed document rather than a lossy one:
//
//   - Children keep their parent: the Fugue tree places concurrent insertions
//     relative to the item they were typed next to, so an item somebody else
//     typed inside deleted text pins the deleted text in place.
//   - Anchored items keep marks resolvable. Dropping the item instead — and
//     with it the mark operation, as an earlier design did — silently loses
//     formatting whenever a mark's boundary anchor happened to point at deleted
//     text, which an ExpandBefore/ExpandAfter mark does routinely.
//
// Purging cascades, so a run of text that was typed and then deleted (a chain
// of right children) collapses entirely — the common case, and the one
// setString produces when a whole-value writer shortens a document. The result
// does not depend on visitation order, and the cost is linear in the document
// however deep the chain runs.
//
// What it does NOT reclaim, and cannot: a tombstone that still has a live
// descendant. Deleting the middle of a typed run leaves the characters between
// it and the run's surviving tail in place, because they are the tree positions
// that tail is defined by. Re-parenting the survivors is not an option — sibling
// order is by id, so a child moved up to its grandparent would sort into a
// different place in the text. Compaction therefore bounds a document by the
// live text plus the tombstones that position it, not by the live text alone.
func (d *Doc) Compact() (items, marks int) {
	if dead := d.deadMarks(); len(dead) > 0 {
		kept := make([]*markOp, 0, len(d.marks)-len(dead))
		for _, m := range d.marks {
			if !dead[m.id] {
				kept = append(kept, m)
			}
		}
		marks, d.marks = len(dead), kept
	}

	anchored := make(map[ID]bool, len(d.marks)*2)
	for _, m := range d.marks {
		for _, a := range []Anchor{m.start, m.end} {
			if a.Kind == AnchorBefore || a.Kind == AnchorAfter {
				anchored[a.Item] = true
			}
		}
	}
	purgeable := func(n *node) bool {
		return n.deleted && !anchored[n.id] && len(n.left) == 0 && len(n.right) == 0
	}
	// A worklist seeded with the leaves, re-testing only the parent each removal
	// exposes. Re-testing the whole item table once per round instead costs a
	// round per level, and linear typing builds a chain of right children as
	// deep as the run is long — quadratic on exactly the shape compaction
	// exists for.
	var queue []*node
	for _, n := range d.byID {
		if purgeable(n) {
			queue = append(queue, n)
		}
	}
	for len(queue) > 0 {
		n := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		parent := n.parent
		removeChild(parent, n)
		delete(d.byID, n.id)
		items++
		// The parent reaches the queue at most once: it qualifies only on the
		// removal that empties its last child list, and nothing puts children
		// back. The root sentinel is never deleted, so it never qualifies.
		if purgeable(parent) {
			queue = append(queue, parent)
		}
	}
	if items > 0 {
		d.orderDirty = true
	}
	return items, marks
}

// CouldReclaim reports whether Compact could free at least n items and mark
// operations from the encoded document. n <= 0 is true for anything; empty
// input — a field nobody has written — is false.
//
// The answer is necessary, never sufficient: it counts every tombstone and
// every mark op, where Compact frees only the ones nothing can still refer to.
// So false is a promise that compacting falls short of n, while true has to be
// confirmed by compacting.
//
// Anything that is not a well-formed stored document is an error, without the
// distinctions Decode draws between a fragment, an unresolved plain-string
// value and malformed bytes.
//
// Cost is proportional to the document's runs where Decode's is proportional to
// its characters, which is what makes it worth asking first.
func CouldReclaim(data []byte, n int) (bool, error) {
	if n <= 0 {
		return true, nil
	}
	if len(data) == 0 {
		return false, nil
	}
	r := &decoder{buf: data}
	v, err := r.byte()
	if err != nil {
		return false, err
	}
	if v != formatVersion {
		// Deliberately not Decode's three-way split on fragment and
		// plain-string values: this reads what is already stored as a row, and
		// those two cannot be.
		return false, fmt.Errorf("text: unsupported format version %d", v)
	}
	peerCount, err := r.count("peer count", 2)
	if err != nil {
		return false, err
	}
	for i := uint64(0); i < peerCount; i++ {
		if _, err := r.bytes(); err != nil {
			return false, err
		}
	}
	for i := uint64(0); i < peerCount; i++ {
		if _, err := r.uvarint(); err != nil {
			return false, err
		}
	}
	runCount, err := r.count("run count", 6)
	if err != nil {
		return false, err
	}
	found := 0
	for i := uint64(0); i < runCount; i++ {
		// Peer index and start counter, neither of which says anything about
		// what is reclaimable.
		for j := 0; j < 2; j++ {
			if _, err := r.uvarint(); err != nil {
				return false, err
			}
		}
		itemCount, err := r.uvarint()
		if err != nil {
			return false, err
		}
		flags, err := r.byte()
		if err != nil {
			return false, err
		}
		if flags&2 == 0 {
			// Parent peer index and counter, present only when the parent is
			// not the root.
			for j := 0; j < 2; j++ {
				if _, err := r.uvarint(); err != nil {
					return false, err
				}
			}
		}
		content, err := r.bytes()
		if err != nil {
			return false, err
		}
		if flags&1 == 0 {
			continue
		}
		// A code point is at least one byte, so a run never holds more items
		// than content bytes; Decode is what rejects a count that disagrees.
		// Clamping keeps an inflated one from claiming a reclaim the document
		// cannot have, and keeps the sum in range whatever the input says.
		if lim := uint64(len(content)); itemCount > lim {
			itemCount = lim
		}
		found += int(itemCount)
		if found >= n {
			return true, nil
		}
	}
	markCount, err := r.count("mark count", 7)
	if err != nil {
		return false, err
	}
	return found+int(markCount) >= n, nil
}

// removeChild unlinks c from its parent's child list.
func removeChild(parent, c *node) {
	list := &parent.right
	if c.side == sideL {
		list = &parent.left
	}
	for i, n := range *list {
		if n == c {
			*list = append((*list)[:i], (*list)[i+1:]...)
			return
		}
	}
}

// EncodeVV serializes a version vector for storage outside a document — the
// engine keeps one per compacted field as that field's floor. Peers are sorted,
// so the encoding is canonical like everything else here.
//
//	varint peerCount
//	  per peer, sorted by UTF-8 bytes: varint len, bytes, varint counter
func EncodeVV(vv map[string]uint64) []byte {
	peers := make([]string, 0, len(vv))
	for p := range vv {
		peers = append(peers, p)
	}
	sort.Strings(peers)
	e := &encoder{buf: make([]byte, 0, 8+16*len(peers))}
	e.uvarint(uint64(len(peers)))
	for _, p := range peers {
		e.str(p)
		e.uvarint(vv[p])
	}
	return e.buf
}

// DecodeVV parses an EncodeVV payload. Empty input is the empty vector — "no
// floor" — which is how an uncompacted field reads.
func DecodeVV(data []byte) (map[string]uint64, error) {
	vv := make(map[string]uint64)
	if len(data) == 0 {
		return vv, nil
	}
	r := &decoder{buf: data}
	// A peer costs at least two bytes: a zero-length name plus its counter.
	n, err := r.count("peer count", 2)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < n; i++ {
		p, err := r.str()
		if err != nil {
			return nil, err
		}
		if _, dup := vv[p]; dup {
			return nil, fmt.Errorf("text: duplicate peer %q in version vector", p)
		}
		c, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		vv[p] = c
	}
	if r.off != len(r.buf) {
		return nil, fmt.Errorf("text: %d trailing bytes", len(r.buf)-r.off)
	}
	return vv, nil
}

// MergeWithFloor is Merge against a field that has been compacted: floor is the
// version vector recorded at the last compaction (empty for a field that never
// was, in which case this is exactly Merge).
//
// See the package comment above for what the floor decides. The output is
// byte-identical to Merge's whenever the incoming side carries no purged state,
// which is every write from a replica that has read the row since.
func MergeWithFloor(base, incoming []byte, floor map[string]uint64) ([]byte, error) {
	if len(floor) == 0 {
		return Merge(base, incoming)
	}
	if IsFragment(base) {
		return nil, fmt.Errorf("text: merge base is a fragment; stored state is always a full document")
	}
	if bytes.Equal(base, incoming) {
		return base, nil
	}
	d, err := Decode(base, "")
	if err != nil {
		return nil, fmt.Errorf("text: merge base: %w", err)
	}
	if err := d.MergeEncoded(incoming, floor); err != nil {
		return nil, err
	}
	return d.Encode(), nil
}

// purgedItems classifies the incoming side's items against the floor: those the
// receiver lacks at an id the floor dominates were purged by a compaction.
// Tombstones among them are returned as the set to skip; a live one is the
// resurrection this whole mechanism exists to refuse.
func (d *Doc) purgedItems(
	floor map[string]uint64,
	n int,
	each func(yield func(id ID, deleted bool) error) error,
) (map[ID]bool, error) {
	var skip map[ID]bool
	err := each(func(id ID, deleted bool) error {
		if _, ok := d.byID[id]; ok {
			return nil
		}
		if floor[id.Peer] < id.Counter {
			return nil
		}
		if !deleted {
			return stale{fmt.Errorf(
				"text: item %s:%d was compacted away and cannot be restored",
				id.Peer, id.Counter)}
		}
		if skip == nil {
			skip = make(map[ID]bool, n)
		}
		skip[id] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return skip, nil
}

// purgedMarks classifies the incoming side's mark operations against the floor:
// those the receiver lacks at an id the floor dominates were dropped by a
// compaction, and re-adding them would undo the reclaim on every write until
// the sender re-reads the row.
//
// Unlike purgedItems there is no live/tombstone split to make — a mark op has
// no state that could make putting it back a resurrection rather than a
// regression — so every match is simply skipped.
func (d *Doc) purgedMarks(floor map[string]uint64, n int, each func(yield func(id ID))) map[ID]bool {
	have := make(map[ID]bool, len(d.marks))
	for _, m := range d.marks {
		have[m.id] = true
	}
	var skip map[ID]bool
	each(func(id ID) {
		if have[id] || floor[id.Peer] < id.Counter {
			return
		}
		if skip == nil {
			skip = make(map[ID]bool, n)
		}
		skip[id] = true
	})
	return skip
}

// staleRef reports the error for new state that hangs off an item compaction
// purged — a child of it, or a mark anchored to it. Nothing can be done with
// such a write except re-read the row and redo the edit.
func staleRef(kind string, id ID) error {
	return stale{fmt.Errorf("text: %s references item %s:%d, which was compacted away",
		kind, id.Peer, id.Counter)}
}

// unusableUnlessStale keeps a stale verdict distinct while tagging every other
// fragment failure the way ApplyFragment would have.
func unusableUnlessStale(err error) error {
	if errors.Is(err, ErrStateStale) {
		return err
	}
	return unusable{err}
}
