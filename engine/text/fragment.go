package text

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"unicode/utf8"
)

// Delta-state fragments: format-version 2 of the text encoding.
//
// A fragment is a partial document — the items, tombstone flips and mark
// operations one state carries beyond some base state, plus the source
// document's FULL version vector. Applying a fragment to any receiver that
// already holds the base (or more) produces exactly the same bytes as merging
// the full source state: fragments are a wire-size optimization, never a
// semantic one. Rows, bootstrap chunks and the Changes feed always carry full
// v1 documents; only replication payloads may carry fragments, and full state
// remains the universal fallback when a fragment cannot apply.
//
// The layout is v1's (see encode.go) with three differences:
//
//   - The leading byte is 0x02.
//   - The version-vector section carries the SOURCE document's full version
//     vector, not just the counters of the items present. The peer-table
//     derivation stays v1's (sorted vv keys), which therefore still covers
//     every parent and anchor peer a fragment item can reference, and unioning
//     the vv into the receiver reproduces the full-merge vv exactly.
//   - References may dangle: a run parent or a mark anchor may name an item
//     the fragment does not carry. v1's unknown-parent, reachability and
//     unknown-anchor rejections move from decode time to apply time, where
//     they resolve against the receiver's items too. Everything else — varint
//     ceiling, allocation guards, UTF-8 validity, duplicate ids, canonical
//     sort orders, trailing bytes — is still enforced at decode; fragments
//     arrive from untrusted clients just like full documents.
//
// A tombstone flip (an item the base holds live and the source holds deleted)
// travels as an ordinary item with deleted=1 and its full content: content is
// immutable and merge verifies identity by it, so re-shipping the content is
// what lets the receiver run the same conflict checks a full merge would.
const fragmentVersion = 2

// ErrFragmentUnusable classifies every way a delta-state fragment can fail on
// the receiving side: bytes that do not decode as a fragment, and a fragment
// whose ids or references cannot be reconciled with the state it arrived at.
// The remedy is the same in all of them — the sender ships full state instead —
// so replication paths branch on this rather than on the message. Failures of
// the receiver's own base state are NOT tagged: those are not the sender's to
// fix.
var ErrFragmentUnusable = errors.New("text: fragment unusable")

// unusable tags an error as ErrFragmentUnusable while reporting the underlying
// message verbatim — the conformance vectors pin those strings, so the
// classification has to ride alongside the message rather than in front of it.
type unusable struct{ error }

func (e unusable) Unwrap() error { return e.error }

func (e unusable) Is(target error) bool { return target == ErrFragmentUnusable }

// IsFragment reports whether b is fragment-encoded (leading byte 0x02).
// Empty input is not a fragment — it decodes as an empty full document.
func IsFragment(b []byte) bool {
	return len(b) > 0 && b[0] == fragmentVersion
}

// fragItem is one item carried by a fragment, detached from any tree: the
// parent is a plain id (zero ID = child of the root sentinel), resolved
// against the receiver at apply time.
type fragItem struct {
	id      ID
	r       rune
	deleted bool
	parent  ID
	side    int
}

// Fragment is a decoded delta-state payload. It is inert data: nothing can be
// read from or edited in a fragment, it can only be applied to a full
// document (Doc.ApplyFragment) or unioned with another fragment
// (MergeFragments).
type Fragment struct {
	vv    map[string]uint64
	items []fragItem
	marks []markOp
}

// VersionVector returns a copy of the fragment's version vector — the full
// knowledge of the document it was cut from.
func (f *Fragment) VersionVector() map[string]uint64 {
	out := make(map[string]uint64, len(f.vv))
	for p, c := range f.vv {
		out[p] = c
	}
	return out
}

// EncodeFragment encodes the state d carries beyond base: items base lacks,
// tombstone flips (live in base, deleted in d), mark ops base lacks, and d's
// full version vector. A nil or empty base yields a self-contained fragment
// carrying the whole document.
//
// base must be a causal ancestor of d — every item, tombstone and mark op in
// base present unchanged in d. Anything else means the two states diverged
// and no fragment of d can stand in for a merge of it.
func EncodeFragment(d, base *Doc) ([]byte, error) {
	f := &Fragment{vv: make(map[string]uint64, len(d.vv))}
	for p, c := range d.vv {
		f.vv[p] = c
	}

	if base != nil {
		if err := checkFragmentBase(d, base); err != nil {
			return nil, err
		}
	}
	hasItem := func(id ID) (*node, bool) {
		if base == nil {
			return nil, false
		}
		n, ok := base.byID[id]
		return n, ok
	}

	// Collected in document order rather than by iterating byID: map order is
	// randomized, and handing a fully shuffled slice to the sort below is what
	// made diffing a large document the most expensive operation on the client's
	// push path. Typed text is very nearly counter-ordered in the traversal, so
	// the sort lands on its nearly-sorted path.
	d.ensureOrder()
	ids := make([]ID, 0, len(d.byID))
	for _, n := range d.order {
		ids = append(ids, n.id)
	}
	slices.SortFunc(ids, func(a, b ID) int {
		if c := cmp.Compare(a.Counter, b.Counter); c != 0 {
			return c
		}
		return cmp.Compare(a.Peer, b.Peer)
	})
	for _, id := range ids {
		n := d.byID[id]
		if bn, ok := hasItem(id); ok && (bn.deleted || !n.deleted) {
			continue // base already carries this item in this (or a later) state
		}
		f.items = append(f.items, fragItem{
			id: id, r: n.r, deleted: n.deleted, parent: parentID(n), side: n.side,
		})
	}

	baseMarks := make(map[ID]bool)
	if base != nil {
		for _, m := range base.marks {
			baseMarks[m.id] = true
		}
	}
	for _, m := range d.marks {
		if !baseMarks[m.id] {
			f.marks = append(f.marks, *m)
		}
	}
	return f.Encode(), nil
}

// checkFragmentBase verifies base is a causal ancestor of d: everything base
// holds, d holds identically (tombstones may only have advanced).
func checkFragmentBase(d, base *Doc) error {
	err := fmt.Errorf("text: fragment: base is not an ancestor of doc")
	for p, c := range base.vv {
		if d.vv[p] < c {
			return err
		}
	}
	for id, bn := range base.byID {
		dn, ok := d.byID[id]
		if !ok || dn.r != bn.r || parentID(dn) != parentID(bn) || dn.side != bn.side {
			return err
		}
		if bn.deleted && !dn.deleted {
			return err
		}
	}
	mine := make(map[ID]*markOp, len(d.marks))
	for _, m := range d.marks {
		mine[m.id] = m
	}
	for _, bm := range base.marks {
		dm, ok := mine[bm.id]
		if !ok || dm.key != bm.key || !sameMarkValue(dm.value, bm.value) ||
			dm.start != bm.start || dm.end != bm.end || dm.expand != bm.expand {
			return err
		}
	}
	return nil
}

// Encode serializes the fragment canonically (same sort rules as Doc.Encode).
func (f *Fragment) Encode() []byte {
	peers := make([]string, 0, len(f.vv))
	for p := range f.vv {
		peers = append(peers, p)
	}
	sort.Strings(peers)
	peerIdx := make(map[string]uint64, len(peers))
	for i, p := range peers {
		peerIdx[p] = uint64(i)
	}

	e := &encoder{buf: make([]byte, 0, 64+8*len(f.items))}
	e.byte(fragmentVersion)
	e.uvarint(uint64(len(peers)))
	for _, p := range peers {
		e.str(p)
	}
	for _, p := range peers {
		e.uvarint(f.vv[p])
	}

	// Canonical maximal runs over the fragment's items: per peer in table
	// order, counters ascending, split where the run invariant breaks. The
	// invariant compares parent ids rather than node pointers — ids are unique,
	// so this is the same rule Doc.Encode applies.
	itemsByPeer := make(map[string][]fragItem, len(peers))
	for _, it := range f.items {
		itemsByPeer[it.id.Peer] = append(itemsByPeer[it.id.Peer], it)
	}
	type run struct {
		peerIdx uint64
		start   uint64
		items   []fragItem
	}
	var runs []run
	for _, p := range peers {
		items := itemsByPeer[p]
		slices.SortFunc(items, func(a, b fragItem) int { return cmp.Compare(a.id.Counter, b.id.Counter) })
		for i := 0; i < len(items); i++ {
			it := items[i]
			if i > 0 {
				prev := items[i-1]
				last := &runs[len(runs)-1]
				if it.id.Counter == prev.id.Counter+1 &&
					it.parent == prev.id && it.side == sideR &&
					it.deleted == prev.deleted {
					last.items = append(last.items, it)
					continue
				}
			}
			runs = append(runs, run{peerIdx: peerIdx[p], start: it.id.Counter, items: []fragItem{it}})
		}
	}

	e.uvarint(uint64(len(runs)))
	for _, r := range runs {
		e.uvarint(r.peerIdx)
		e.uvarint(r.start)
		e.uvarint(uint64(len(r.items)))
		head := r.items[0]
		var flags byte
		if head.deleted {
			flags |= 1
		}
		if head.parent == (ID{}) {
			flags |= 2
		}
		if head.side == sideR {
			flags |= 4
		}
		e.byte(flags)
		if flags&2 == 0 {
			e.uvarint(peerIdx[head.parent.Peer])
			e.uvarint(head.parent.Counter)
		}
		content := make([]byte, 0, len(r.items)*2)
		for _, it := range r.items {
			content = utf8.AppendRune(content, it.r)
		}
		e.bytes(content)
	}

	marks := make([]markOp, len(f.marks))
	copy(marks, f.marks)
	sort.Slice(marks, func(i, j int) bool {
		a, b := marks[i], marks[j]
		if pa, pb := peerIdx[a.id.Peer], peerIdx[b.id.Peer]; pa != pb {
			return pa < pb
		}
		return a.id.Counter < b.id.Counter
	})
	e.uvarint(uint64(len(marks)))
	for _, m := range marks {
		e.uvarint(peerIdx[m.id.Peer])
		e.uvarint(m.id.Counter)
		e.str(m.key)
		switch v := m.value.(type) {
		case nil:
			e.byte(0)
		case bool:
			if v {
				e.byte(2)
			} else {
				e.byte(1)
			}
		case string:
			e.byte(3)
			e.str(v)
		case float64:
			e.byte(4)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
			e.buf = append(e.buf, b[:]...)
		}
		for _, a := range []Anchor{m.start, m.end} {
			e.byte(byte(a.Kind))
			if a.Kind == AnchorBefore || a.Kind == AnchorAfter {
				e.uvarint(peerIdx[a.Item.Peer])
				e.uvarint(a.Item.Counter)
			}
		}
		e.byte(byte(m.expand))
	}
	return e.buf
}

// DecodeFragment parses a fragment payload. Structural validity is enforced
// exactly as for full documents; only reference resolution (parents, anchors)
// is deferred to apply time. The version vector must cover every item and
// mark op the fragment carries — it claims to be the writer's full knowledge,
// and the merged vv depends on that.
//
// Every failure is tagged ErrFragmentUnusable: there is nothing a receiver can
// do about malformed delta state except ask for the full document.
func DecodeFragment(data []byte) (*Fragment, error) {
	f, err := decodeFragment(data)
	if err != nil {
		return nil, unusable{err}
	}
	return f, nil
}

func decodeFragment(data []byte) (*Fragment, error) {
	r := &decoder{buf: data}
	v, err := r.byte()
	if err != nil {
		return nil, err
	}
	if v == formatVersion {
		return nil, fmt.Errorf("text: full document where fragment expected")
	}
	if v != fragmentVersion {
		return nil, fmt.Errorf("text: unsupported format version %d", v)
	}

	f := &Fragment{vv: make(map[string]uint64)}

	// A peer costs at least two bytes: a zero-length name plus its version
	// vector counter.
	peerCount, err := r.count("peer count", 2)
	if err != nil {
		return nil, err
	}
	peers := make([]string, peerCount)
	for i := range peers {
		if peers[i], err = r.str(); err != nil {
			return nil, err
		}
	}
	for i := range peers {
		c, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		f.vv[peers[i]] = c
	}
	peerAt := func(idx uint64) (string, error) {
		if idx >= peerCount {
			return "", fmt.Errorf("text: peer index %d out of range", idx)
		}
		return peers[idx], nil
	}

	// A run costs at least six bytes (see Decode).
	runCount, err := r.count("run count", 6)
	if err != nil {
		return nil, err
	}
	seenItems := make(map[ID]bool)
	for i := uint64(0); i < runCount; i++ {
		pIdx, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		p, err := peerAt(pIdx)
		if err != nil {
			return nil, err
		}
		start, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if start == 0 {
			// Reserved for the root sentinel (see Decode). Under peer "" it
			// is also the zero parent id, which here means "child of the root".
			return nil, fmt.Errorf("text: item %s:0 uses reserved counter 0", p)
		}
		itemCount, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if itemCount == 0 {
			return nil, fmt.Errorf("text: empty run")
		}
		flags, err := r.byte()
		if err != nil {
			return nil, err
		}
		deleted := flags&1 != 0
		rootParent := flags&2 != 0
		side := sideL
		if flags&4 != 0 {
			side = sideR
		}
		var parent ID
		if !rootParent {
			ppIdx, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			pp, err := peerAt(ppIdx)
			if err != nil {
				return nil, err
			}
			pc, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			parent = ID{Peer: pp, Counter: pc}
		}
		content, err := r.bytes()
		if err != nil {
			return nil, err
		}
		var seen uint64
		prevParent := parent
		prevSide := side
		for len(content) > 0 {
			ru, size := utf8.DecodeRune(content)
			if ru == utf8.RuneError && size <= 1 {
				return nil, fmt.Errorf("text: invalid UTF-8 in run content")
			}
			content = content[size:]
			if seen >= itemCount {
				return nil, fmt.Errorf("text: run content longer than item count")
			}
			id := ID{Peer: p, Counter: start + seen}
			if seenItems[id] {
				return nil, fmt.Errorf("text: duplicate item %s:%d", id.Peer, id.Counter)
			}
			seenItems[id] = true
			if f.vv[p] < id.Counter {
				return nil, fmt.Errorf("text: fragment vv does not cover item %s:%d", id.Peer, id.Counter)
			}
			f.items = append(f.items, fragItem{
				id: id, r: ru, deleted: deleted, parent: prevParent, side: prevSide,
			})
			prevParent, prevSide = id, sideR
			seen++
		}
		if seen != itemCount {
			return nil, fmt.Errorf("text: run content shorter than item count")
		}
	}

	// A mark costs at least seven bytes (see Decode).
	markCount, err := r.count("mark count", 7)
	if err != nil {
		return nil, err
	}
	decodeAnchor := func() (Anchor, error) {
		k, err := r.byte()
		if err != nil {
			return Anchor{}, err
		}
		kind := AnchorKind(k)
		switch kind {
		case AnchorDocStart, AnchorDocEnd:
			return Anchor{Kind: kind}, nil
		case AnchorBefore, AnchorAfter:
			pIdx, err := r.uvarint()
			if err != nil {
				return Anchor{}, err
			}
			p, err := peerAt(pIdx)
			if err != nil {
				return Anchor{}, err
			}
			c, err := r.uvarint()
			if err != nil {
				return Anchor{}, err
			}
			// The item may be absent from the fragment; resolved at apply.
			return Anchor{Kind: kind, Item: ID{Peer: p, Counter: c}}, nil
		default:
			return Anchor{}, fmt.Errorf("text: invalid anchor kind %d", k)
		}
	}
	seenMarks := make(map[ID]bool, markCount)
	for i := uint64(0); i < markCount; i++ {
		pIdx, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		p, err := peerAt(pIdx)
		if err != nil {
			return nil, err
		}
		c, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			return nil, fmt.Errorf("text: mark op %s:0 uses reserved counter 0", p)
		}
		id := ID{Peer: p, Counter: c}
		if seenMarks[id] {
			return nil, fmt.Errorf("text: duplicate mark op %s:%d", p, c)
		}
		seenMarks[id] = true
		if f.vv[p] < c {
			return nil, fmt.Errorf("text: fragment vv does not cover mark op %s:%d", p, c)
		}
		key, err := r.str()
		if err != nil {
			return nil, err
		}
		tag, err := r.byte()
		if err != nil {
			return nil, err
		}
		var value any
		switch tag {
		case 0:
			value = nil
		case 1:
			value = false
		case 2:
			value = true
		case 3:
			s, err := r.str()
			if err != nil {
				return nil, err
			}
			value = s
		case 4:
			if len(r.buf)-r.off < 8 {
				return nil, fmt.Errorf("text: truncated document")
			}
			fl := math.Float64frombits(binary.BigEndian.Uint64(r.buf[r.off:]))
			r.off += 8
			if math.IsNaN(fl) || math.IsInf(fl, 0) {
				return nil, fmt.Errorf("text: non-finite mark value")
			}
			value = fl
		default:
			return nil, fmt.Errorf("text: invalid mark value tag %d", tag)
		}
		start, err := decodeAnchor()
		if err != nil {
			return nil, err
		}
		end, err := decodeAnchor()
		if err != nil {
			return nil, err
		}
		ex, err := r.byte()
		if err != nil {
			return nil, err
		}
		if ex > byte(ExpandBoth) {
			return nil, fmt.Errorf("text: invalid expand %d", ex)
		}
		f.marks = append(f.marks, markOp{
			id: id, key: key, value: value, start: start, end: end, expand: Expand(ex),
		})
	}
	if r.off != len(r.buf) {
		return nil, fmt.Errorf("text: %d trailing bytes", len(r.buf)-r.off)
	}
	return f, nil
}

// ApplyFragment merges f into d: the same pure union Merge performs, with
// f's dangling references resolved against d. Validation is complete before
// the first mutation — a rejected fragment leaves d untouched, which is what
// lets the engine answer "resend full state" without a savepoint. Every
// failure is tagged ErrFragmentUnusable, since that answer is the only remedy.
func (d *Doc) ApplyFragment(f *Fragment) error {
	if err := d.applyFragmentSkipping(f, nil, nil); err != nil {
		return unusable{err}
	}
	return nil
}

// applyFragmentSkipping is ApplyFragment with sets of ids to leave out — the
// tombstones and mark operations a compaction purged from d (see compact.go).
// Both are nil for every ordinary apply. State in the fragment that DEPENDS on
// a skipped item reports ErrStateStale, which the caller must keep distinct
// from ErrFragmentUnusable: resending the same edit as a full document does not
// help.
func (d *Doc) applyFragmentSkipping(f *Fragment, skip, skipMarks map[ID]bool) error {
	// Classify items and run the same conflict checks as a full merge.
	newByID := make(map[ID]*fragItem)
	for i := range f.items {
		fi := &f.items[i]
		if skip[fi.id] {
			continue
		}
		if mine, ok := d.byID[fi.id]; ok {
			if mine.r != fi.r {
				return fmt.Errorf("text: merge conflict: item %s:%d has different content on each side",
					fi.id.Peer, fi.id.Counter)
			}
			if parentID(mine) != fi.parent || mine.side != fi.side {
				return fmt.Errorf("text: merge conflict: item %s:%d has different placement on each side",
					fi.id.Peer, fi.id.Counter)
			}
			continue
		}
		newByID[fi.id] = fi
	}

	// Every new item must reach the root through parents that exist — in the
	// receiver or in the fragment itself. This is decode's reachability check
	// relocated to apply time.
	const (
		unresolved = 0
		resolving  = 1
		reachable  = 2
	)
	state := make(map[ID]int, len(newByID))
	unreachableCount := 0
	for i := range f.items {
		fi := &f.items[i]
		if _, isNew := newByID[fi.id]; !isNew || state[fi.id] != unresolved {
			continue
		}
		path := []ID{}
		cur := fi
		verdict := reachable
		for {
			if state[cur.id] == reachable {
				break
			}
			if state[cur.id] == resolving {
				verdict = -1 // cycle: everything on the path is unreachable
				break
			}
			state[cur.id] = resolving
			path = append(path, cur.id)
			p := cur.parent
			if p == (ID{}) {
				break
			}
			if _, ok := d.byID[p]; ok {
				break
			}
			np, ok := newByID[p]
			if !ok {
				if skip[p] {
					return staleRef("item "+cur.id.Peer+":"+strconv.FormatUint(cur.id.Counter, 10), p)
				}
				return fmt.Errorf("text: item %s:%d references unknown parent %s:%d",
					cur.id.Peer, cur.id.Counter, p.Peer, p.Counter)
			}
			cur = np
		}
		for _, id := range path {
			if verdict == reachable {
				state[id] = reachable
			} else {
				unreachableCount++
			}
		}
	}
	if unreachableCount > 0 {
		return fmt.Errorf("text: %d of %d items unreachable from the root (parent cycle)",
			unreachableCount, len(f.items))
	}

	// Marks: same-id ops must agree (a full merge's check); new ops' anchors
	// must resolve against receiver ∪ fragment.
	mine := make(map[ID]*markOp, len(d.marks))
	for _, m := range d.marks {
		mine[m.id] = m
	}
	var newMarks []markOp
	for _, m := range f.marks {
		// Checked before the anchor test below: an op the floor says was
		// reclaimed is not stale state to reject, even when the items its
		// boundaries name were reclaimed along with it.
		if skipMarks[m.id] {
			continue
		}
		if dm, ok := mine[m.id]; ok {
			if dm.key != m.key || !sameMarkValue(dm.value, m.value) ||
				dm.start != m.start || dm.end != m.end || dm.expand != m.expand {
				return fmt.Errorf("text: merge conflict: mark op %s:%d differs on each side",
					m.id.Peer, m.id.Counter)
			}
			continue
		}
		for _, a := range []Anchor{m.start, m.end} {
			if a.Kind != AnchorBefore && a.Kind != AnchorAfter {
				continue
			}
			if _, ok := d.byID[a.Item]; ok {
				continue
			}
			if _, ok := newByID[a.Item]; ok {
				continue
			}
			if skip[a.Item] {
				return staleRef("mark op "+m.id.Peer+":"+strconv.FormatUint(m.id.Counter, 10), a.Item)
			}
			return fmt.Errorf("text: anchor references unknown item %s:%d", a.Item.Peer, a.Item.Counter)
		}
		newMarks = append(newMarks, m)
	}

	// Commit — everything below is infallible.
	newIDs := make([]ID, 0, len(newByID))
	for id, fi := range newByID {
		d.byID[id] = &node{id: id, r: fi.r, deleted: fi.deleted, side: fi.side}
		newIDs = append(newIDs, id)
	}
	for i := range f.items {
		fi := &f.items[i]
		if _, isNew := newByID[fi.id]; isNew || skip[fi.id] {
			continue
		}
		if fi.deleted {
			d.byID[fi.id].deleted = true
		}
	}
	sort.Slice(newIDs, func(i, j int) bool { return newIDs[i].less(newIDs[j]) })
	for _, id := range newIDs {
		n := d.byID[id]
		p := newByID[id].parent
		if p == (ID{}) {
			n.parent = d.root
		} else {
			n.parent = d.byID[p]
		}
		addChild(n.parent, n)
	}
	for i := range newMarks {
		cp := newMarks[i]
		d.marks = append(d.marks, &cp)
	}
	for p, c := range f.vv {
		d.bumpVV(p, c)
	}
	if len(newIDs) > 0 {
		d.orderDirty = true
	}
	return nil
}

// ApplyFragmentBytes applies an encoded fragment to an encoded full document
// and re-encodes canonically. When the fragment was cut against a base the
// receiver already covers, the result is byte-identical to a full-state
// Merge with the fragment's source document.
func ApplyFragmentBytes(full, frag []byte) ([]byte, error) {
	d, err := Decode(full, "")
	if err != nil {
		return nil, err
	}
	f, err := DecodeFragment(frag)
	if err != nil {
		return nil, err
	}
	if err := d.ApplyFragment(f); err != nil {
		return nil, err
	}
	return d.Encode(), nil
}

// MergeFragments unions two encoded fragments into one: union of items with
// tombstones OR-ed, union of mark ops, pointwise-max version vectors. Like
// Merge it is associative and commutative, so a span of per-write fragments
// can be folded into a single one in any order. Same-id disagreements are the
// same shared-peer corruption Merge rejects.
func MergeFragments(a, b []byte) ([]byte, error) {
	fa, err := DecodeFragment(a)
	if err != nil {
		return nil, err
	}
	fb, err := DecodeFragment(b)
	if err != nil {
		return nil, err
	}
	byID := make(map[ID]int, len(fa.items))
	for i := range fa.items {
		byID[fa.items[i].id] = i
	}
	var extraItems []fragItem
	for _, it := range fb.items {
		i, ok := byID[it.id]
		if !ok {
			extraItems = append(extraItems, it)
			continue
		}
		mine := &fa.items[i]
		if mine.r != it.r {
			return nil, fmt.Errorf("text: merge conflict: item %s:%d has different content on each side",
				it.id.Peer, it.id.Counter)
		}
		if mine.parent != it.parent || mine.side != it.side {
			return nil, fmt.Errorf("text: merge conflict: item %s:%d has different placement on each side",
				it.id.Peer, it.id.Counter)
		}
		if it.deleted {
			mine.deleted = true
		}
	}
	fa.items = append(fa.items, extraItems...)
	seenMarks := make(map[ID]int, len(fa.marks))
	for i := range fa.marks {
		seenMarks[fa.marks[i].id] = i
	}
	var extraMarks []markOp
	for _, m := range fb.marks {
		i, ok := seenMarks[m.id]
		if !ok {
			extraMarks = append(extraMarks, m)
			continue
		}
		mine := &fa.marks[i]
		if mine.key != m.key || !sameMarkValue(mine.value, m.value) ||
			mine.start != m.start || mine.end != m.end || mine.expand != m.expand {
			return nil, fmt.Errorf("text: merge conflict: mark op %s:%d differs on each side",
				m.id.Peer, m.id.Counter)
		}
	}
	fa.marks = append(fa.marks, extraMarks...)
	for p, c := range fb.vv {
		if fa.vv[p] < c {
			fa.vv[p] = c
		}
	}
	return fa.Encode(), nil
}
