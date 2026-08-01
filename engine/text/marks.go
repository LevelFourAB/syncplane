package text

import (
	"fmt"
	"math"
	"sort"
)

// AnchorKind says what an anchor is attached to.
type AnchorKind uint8

const (
	// AnchorDocStart pins to the start of the document.
	AnchorDocStart AnchorKind = 0
	// AnchorDocEnd pins to the end of the document.
	AnchorDocEnd AnchorKind = 1
	// AnchorBefore pins immediately before the referenced item.
	AnchorBefore AnchorKind = 2
	// AnchorAfter pins immediately after the referenced item.
	AnchorAfter AnchorKind = 3
)

// Anchor is a stable position in a document: a spot immediately before or
// after a specific item (or a document end). An anchor held INSIDE a document
// (a mark boundary) stays resolvable for the life of the document — across
// remote edits, deletions of the referenced item, and undo — because Compact
// keeps every item a mark anchors to.
//
// An anchor stored OUTSIDE its document (an undo entry, a presence broadcast)
// has no such protection: compaction can drop the item it names, and
// ResolveAnchor then reports it as unknown. Callers holding long-lived external
// anchors must treat that error as "the position is gone", not as corruption.
type Anchor struct {
	Kind AnchorKind
	// Item is set for AnchorBefore/AnchorAfter.
	Item ID
}

// AnchorAt returns an anchor for the caret position index (0..Len). kind
// picks which neighbour the anchor sticks to: AnchorBefore attaches to the
// item at index (drifting left stays put when text is inserted at the
// position), AnchorAfter attaches to the item at index-1 (the anchor follows
// text inserted at the position). Document edges fall back to
// AnchorDocStart/AnchorDocEnd.
func (d *Doc) AnchorAt(index int, kind AnchorKind) (Anchor, error) {
	l := d.Len()
	if index < 0 || index > l {
		return Anchor{}, fmt.Errorf("text: anchor index %d out of range [0..%d]", index, l)
	}
	switch kind {
	case AnchorBefore:
		if index == l {
			return Anchor{Kind: AnchorDocEnd}, nil
		}
		return Anchor{Kind: AnchorBefore, Item: d.visibleAt(index).id}, nil
	case AnchorAfter:
		if index == 0 {
			return Anchor{Kind: AnchorDocStart}, nil
		}
		return Anchor{Kind: AnchorAfter, Item: d.visibleAt(index - 1).id}, nil
	default:
		return Anchor{}, fmt.Errorf("text: AnchorAt needs AnchorBefore or AnchorAfter, got %d", kind)
	}
}

// ResolveAnchor maps an anchor back to a visible index (0..Len). A live item
// resolves exactly; a deleted item resolves to the nearest live neighbour on
// the anchor's side; an unknown item is an error — reachable only for an anchor
// kept outside the document across a compaction that purged its item.
func (d *Doc) ResolveAnchor(a Anchor) (int, error) {
	switch a.Kind {
	case AnchorDocStart:
		return 0, nil
	case AnchorDocEnd:
		return d.Len(), nil
	}
	n, ok := d.byID[a.Item]
	if !ok {
		return 0, fmt.Errorf("text: anchor references unknown item %s:%d", a.Item.Peer, a.Item.Counter)
	}
	d.ensureOrder()
	// Position in the full order, then convert per side.
	full := d.fullIndexOf(n)
	if full < 0 {
		return 0, fmt.Errorf("text: internal: item %s:%d not in order", a.Item.Peer, a.Item.Counter)
	}
	switch a.Kind {
	case AnchorBefore:
		// Nearest live item at or after n; its visible index (Len when none).
		idx := 0
		for _, it := range d.order[:full] {
			if !it.deleted {
				idx++
			}
		}
		return idx, nil
	case AnchorAfter:
		// Nearest live item at or before n; its visible index + 1 (0 when none).
		idx := 0
		for _, it := range d.order[:full+1] {
			if !it.deleted {
				idx++
			}
		}
		return idx, nil
	default:
		return 0, fmt.Errorf("text: invalid anchor kind %d", a.Kind)
	}
}

func (d *Doc) fullIndexOf(n *node) int {
	for i, it := range d.order {
		if it == n {
			return i
		}
	}
	return -1
}

// validateValue normalizes a mark value: nil, bool, string, or a number
// (coerced to float64). Non-finite numbers are rejected — NaN has no useful
// equality (it would compare unequal to itself and split Marks into one span
// per character) and the infinities have no counterpart in JSON. The decoders
// enforce the same rule, so a document can never carry one.
func validateValue(v any) (any, error) {
	var f float64
	switch x := v.(type) {
	case nil, bool, string:
		return v, nil
	case float64:
		f = x
	case float32:
		f = float64(x)
	case int:
		f = float64(x)
	case int64:
		f = float64(x)
	default:
		return nil, fmt.Errorf("text: unsupported mark value type %T", v)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("text: non-finite mark value")
	}
	return f, nil
}

// Mark applies key=value over the visible range [start, end). expand decides
// whether text later typed at a boundary joins the mark; it is baked into the
// anchor sides at creation time. The returned ID identifies the operation
// (higher-id operations on the same key win where ranges overlap).
//
// start == end is a COLLAPSED mark: it covers no character and Marks never
// reports it, but MarksAt does, and with ExpandBoth the first character typed
// at the position falls inside it and it becomes an ordinary span. That is the
// pending-format state an editor needs for "press Cmd-B, then type", and with
// ExpandNone the zero-width annotation that attaches a note to a position
// rather than a span. See collapsedAnchors.
func (d *Doc) Mark(start, end int, key string, value any, expand Expand) (ID, error) {
	if value == nil {
		return ID{}, fmt.Errorf("text: nil mark value is reserved for Unmark")
	}
	return d.markOpAt(start, end, key, value, expand)
}

// Unmark removes key over the visible range [start, end): it records a mark
// operation with a nil value and a higher id than everything seen, which wins
// resolution over the marks it covers. expand mirrors Mark — pass the same
// policy the mark was created with so boundary inserts don't resurrect it.
//
// It is also how a collapsed mark is cancelled: an unmark at the same caret
// with the same expand policy resolves to the same anchors, so it outranks the
// mark both while they cover nothing and after text typed at the position
// makes them cover it. Nothing is physically removed — Compact reclaims the
// pair once neither can affect resolution again (see compact.go).
func (d *Doc) Unmark(start, end int, key string, expand Expand) (ID, error) {
	return d.markOpAt(start, end, key, nil, expand)
}

func (d *Doc) markOpAt(start, end int, key string, value any, expand Expand) (ID, error) {
	if d.peer == "" {
		return ID{}, fmt.Errorf("text: document has no peer; SetPeer before editing")
	}
	if key == "" {
		return ID{}, fmt.Errorf("text: empty mark key")
	}
	l := d.Len()
	if start < 0 || end > l || start > end {
		return ID{}, fmt.Errorf("text: mark range [%d,%d) invalid for length %d", start, end, l)
	}
	value, err := validateValue(value)
	if err != nil {
		return ID{}, err
	}

	var startAnchor, endAnchor Anchor
	if start == end {
		if startAnchor, endAnchor, err = d.collapsedAnchors(start, expand); err != nil {
			return ID{}, err
		}
	} else {
		first := d.visibleAt(start)
		last := d.visibleAt(end - 1)
		// Anchor sides encode the expand policy: a non-expanding boundary
		// anchors to the covered item itself, an expanding one anchors to the
		// adjacent item in the full order (so an item inserted between them
		// falls inside).
		startAnchor = Anchor{Kind: AnchorBefore, Item: first.id}
		if expand == ExpandBefore || expand == ExpandBoth {
			if p := d.predecessor(first); p != nil {
				startAnchor = Anchor{Kind: AnchorAfter, Item: p.id}
			} else {
				startAnchor = Anchor{Kind: AnchorDocStart}
			}
		}
		endAnchor = Anchor{Kind: AnchorAfter, Item: last.id}
		if expand == ExpandAfter || expand == ExpandBoth {
			if s := d.successor(last); s != nil {
				endAnchor = Anchor{Kind: AnchorBefore, Item: s.id}
			} else {
				endAnchor = Anchor{Kind: AnchorDocEnd}
			}
		}
	}

	id := ID{Peer: d.peer, Counter: d.nextCounter()}
	d.marks = append(d.marks, &markOp{
		id:     id,
		key:    key,
		value:  value,
		start:  startAnchor,
		end:    endAnchor,
		expand: expand,
	})
	d.bumpVV(id.Peer, id.Counter)
	return id, nil
}

// caretNeighbours returns the two items an insert at visible index would land
// between: before is the item it attaches after (nil at the start of the
// document) and after is the one immediately following that in the FULL,
// tombstone-inclusive order (nil at the end).
//
// Insert places a new item strictly between the two — either as before's right
// child or as after's left child, depending on which children already exist —
// which is what makes an anchor pair built from them straddle whatever is
// typed at the position. The full-order neighbour is the right one to use, not
// the next VISIBLE item: anchoring past a stretch of tombstones would give a
// "collapsed" mark that spans them, which resolves to the same empty span but
// is no longer a point, and compaction would keep every one of those
// tombstones alive to hold an anchor that does not need them.
func (d *Doc) caretNeighbours(index int) (before, after *node) {
	d.ensureOrder()
	if index > 0 {
		before = d.visibleAt(index - 1)
	}
	if before == nil {
		if len(d.order) > 0 {
			return nil, d.order[0]
		}
		return nil, nil
	}
	return before, d.successor(before)
}

// collapsedAnchors builds the anchor pair for a zero-width mark at caret
// position index. Only two policies mean anything without a covered character,
// and they are the two states an editor actually has:
//
//   - ExpandBoth straddles the caret — after the item before it, before the
//     item after it — so the first character typed there falls inside and the
//     mark becomes an ordinary span. This is a pending format.
//   - ExpandNone pins both boundaries to the same edge, so the mark stays
//     zero-width for the life of the document while drifting with the text
//     around it. This is a note attached to a position.
//
// ExpandBefore and ExpandAfter name one side of a range that has no sides, so
// they are refused rather than silently resolved to one of the two above.
func (d *Doc) collapsedAnchors(index int, expand Expand) (Anchor, Anchor, error) {
	if expand != ExpandNone && expand != ExpandBoth {
		return Anchor{}, Anchor{}, fmt.Errorf(
			"text: collapsed mark at %d needs ExpandNone or ExpandBoth, got expand %d", index, expand)
	}
	before, after := d.caretNeighbours(index)
	start := Anchor{Kind: AnchorDocStart}
	if before != nil {
		start = Anchor{Kind: AnchorAfter, Item: before.id}
	}
	if expand == ExpandNone {
		return start, start, nil
	}
	end := Anchor{Kind: AnchorDocEnd}
	if after != nil {
		end = Anchor{Kind: AnchorBefore, Item: after.id}
	}
	return start, end, nil
}

// PendingMark is one key/value that applies at a caret position.
type PendingMark struct {
	Key   string
	Value any
}

// MarksAt reports what text inserted at the visible index (0..Len) would be
// marked with: the active formatting for a caret, and what the next character
// typed there comes out as.
//
// This is a different question from Marks, which reports what is painted on
// characters that already exist, and neither answers the other. A collapsed
// mark covers nothing and never appears in Marks, but reporting it is the
// whole point of this call. An ordinary mark appears in both exactly when its
// expand policy takes in text typed at the position — so the two agree about a
// caret in the middle of a bold run, and disagree at its edges, which is
// correct in both directions.
//
// Winners are decided the same way as in Marks (highest id per key), and a
// nil-valued winner reports no entry. The result is sorted by key.
func (d *Doc) MarksAt(index int) ([]PendingMark, error) {
	l := d.Len()
	if index < 0 || index > l {
		return nil, fmt.Errorf("text: index %d out of range [0..%d]", index, l)
	}
	if len(d.marks) == 0 {
		return nil, nil
	}
	d.ensureOrder()
	fullIndex := make(map[ID]int, len(d.order))
	for i, it := range d.order {
		fullIndex[it.id] = i
	}

	// Where a new item would land in the full order, and what every anchor
	// resolves to once it has: everything at or past that position shifts by
	// one, so an operation covers the new item exactly when its shifted range
	// contains it. Computing the shift instead of doing a trial insert is what
	// keeps this a read.
	at := 0
	if before, _ := d.caretNeighbours(index); before != nil {
		at = fullIndex[before.id] + 1
	}
	shifted := func(a Anchor) (int, error) {
		switch a.Kind {
		case AnchorDocStart:
			return 0, nil
		case AnchorDocEnd:
			return len(d.order) + 1, nil
		}
		i, ok := fullIndex[a.Item]
		if !ok {
			return 0, fmt.Errorf("text: anchor references unknown item %s:%d", a.Item.Peer, a.Item.Counter)
		}
		if i >= at {
			i++
		}
		if a.Kind == AnchorAfter {
			i++
		}
		return i, nil
	}

	ops := make([]*markOp, len(d.marks))
	copy(ops, d.marks)
	sort.Slice(ops, func(i, j int) bool { return ops[i].id.less(ops[j].id) })
	winner := make(map[string]any, len(ops))
	for _, op := range ops {
		s, err := shifted(op.start)
		if err != nil {
			return nil, err
		}
		e, err := shifted(op.end)
		if err != nil {
			return nil, err
		}
		if s <= at && at < e {
			winner[op.key] = op.value
		}
	}

	out := make([]PendingMark, 0, len(winner))
	for k, v := range winner {
		if v == nil {
			continue // an unmark wins here: the key is off, not pending
		}
		out = append(out, PendingMark{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Point is a resolved zero-width mark: one that covers no character, reported
// at the visible index it sits between.
type Point struct {
	Index int
	Key   string
	Value any
}

// Points resolves the zero-width marks over the current text — the marks Marks
// cannot report, because a span covering no characters is not a span.
//
// Both kinds of collapsed mark appear here (see collapsedAnchors): a note
// attached to a position, which stays zero-width for good, and a pending format
// that has not been typed into yet, which stops being a point the moment it
// covers a character. The document does not distinguish them beyond their
// anchors, so neither does this; callers that need to tell them apart use
// distinct keys, which is what the annotation pattern already does (one key per
// comment, see client/src/text/annotations.ts).
//
// Winners are decided per key AND position rather than per key alone: two notes
// on the same key at different places in the document are both real, whereas
// two operations on the same key at the same position are the same collision
// Marks resolves — highest id wins, nil-valued winner reports nothing, which is
// what lets an unmark at the caret cancel a point. Sorted by (Index, Key).
func (d *Doc) Points() ([]Point, error) {
	if len(d.marks) == 0 {
		return nil, nil
	}
	d.ensureOrder()
	fullIndex := make(map[ID]int, len(d.order))
	for i, it := range d.order {
		fullIndex[it.id] = i
	}
	anchorPos := func(a Anchor) (int, error) {
		switch a.Kind {
		case AnchorDocStart:
			return 0, nil
		case AnchorDocEnd:
			return len(d.order), nil
		}
		i, ok := fullIndex[a.Item]
		if !ok {
			return 0, fmt.Errorf("text: anchor references unknown item %s:%d", a.Item.Peer, a.Item.Counter)
		}
		if a.Kind == AnchorBefore {
			return i, nil
		}
		return i + 1, nil
	}
	visBefore := make([]int, len(d.order)+1)
	visible := 0
	for i, it := range d.order {
		visBefore[i] = visible
		if !it.deleted {
			visible++
		}
	}
	visBefore[len(d.order)] = visible

	ops := make([]*markOp, len(d.marks))
	copy(ops, d.marks)
	sort.Slice(ops, func(i, j int) bool { return ops[i].id.less(ops[j].id) })

	type slot struct {
		key string
		at  int
	}
	winner := make(map[slot]any)
	for _, op := range ops {
		s, err := anchorPos(op.start)
		if err != nil {
			return nil, err
		}
		e, err := anchorPos(op.end)
		if err != nil {
			return nil, err
		}
		if s != e {
			continue // covers something (or is inverted); Marks' business
		}
		winner[slot{key: op.key, at: s}] = op.value
	}

	out := make([]Point, 0, len(winner))
	for sl, v := range winner {
		if v == nil {
			continue
		}
		out = append(out, Point{Index: visBefore[sl.at], Key: sl.key, Value: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Index != out[j].Index {
			return out[i].Index < out[j].Index
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// Span is a resolved mark range over the visible text.
type Span struct {
	// Start and End are visible code-point indices, [Start, End).
	Start, End int
	Key        string
	// Value is the winning mark value (never nil — nil-valued winners mean
	// "unmarked" and produce no span).
	Value any
}

// Marks resolves every mark key over the current text: for each character and
// key, the covering operation with the highest id wins; nil-valued winners
// (removals) produce no span. Spans are maximal runs of equal value, sorted
// by (Start, End, Key).
//
// A mark whose anchor references an item this document does not hold is an
// error rather than a guess: the two implementations would otherwise paint
// different ranges for the same input (Go from the start of the document, TS
// nothing at all). Decode validates anchors and markOpAt builds them from live
// items, so this only fires on a document assembled in memory from foreign
// parts — MergeDocs with an unrelated replica, say.
//
// Cost is O(document + covered), not O(keys × document): the two full-order
// passes below are shared by every key, and each key then only touches the
// stretches its own operations cover. That distinction is what makes the
// annotation pattern (one mark key per comment — see
// client/src/text/annotations.ts) usable, since a document with C comments has
// C keys that each cover a few words. Callers re-resolve on every text change,
// so this runs on the render path.
func (d *Doc) Marks() ([]Span, error) {
	d.ensureOrder()
	if len(d.marks) == 0 {
		return nil, nil
	}

	// Full-order boundary position of every anchor: an anchor occupies the
	// edge just before (AnchorBefore) or just after (AnchorAfter) its item, so
	// coverage of a character at full index f is start <= f < end measured in
	// edge coordinates.
	fullIndex := make(map[ID]int, len(d.order))
	for i, it := range d.order {
		fullIndex[it.id] = i
	}
	anchorPos := func(a Anchor) (int, error) {
		switch a.Kind {
		case AnchorDocStart:
			return 0, nil
		case AnchorDocEnd:
			return len(d.order), nil
		}
		i, ok := fullIndex[a.Item]
		if !ok {
			return 0, fmt.Errorf("text: anchor references unknown item %s:%d", a.Item.Peer, a.Item.Counter)
		}
		if a.Kind == AnchorBefore {
			return i, nil
		}
		return i + 1, nil // AnchorAfter
	}

	// visBefore[f] is how many visible characters precede full-order position
	// f, so a full-order position converts to a visible index in O(1) instead
	// of by counting from the start of the document once per key.
	visBefore := make([]int, len(d.order)+1)
	visible := 0
	for i, it := range d.order {
		visBefore[i] = visible
		if !it.deleted {
			visible++
		}
	}
	visBefore[len(d.order)] = visible

	// Resolve every operation's range up front, in ascending id order, so an
	// unresolvable anchor is reported for the same operation it always was.
	ops := make([]*markOp, len(d.marks))
	copy(ops, d.marks)
	sort.Slice(ops, func(i, j int) bool { return ops[i].id.less(ops[j].id) })

	ranges := make([]opRange, 0, len(ops))
	byKey := make(map[string][]int, len(ops)) // key -> indices into ranges, id order
	for _, op := range ops {
		s, err := anchorPos(op.start)
		if err != nil {
			return nil, err
		}
		e, err := anchorPos(op.end)
		if err != nil {
			return nil, err
		}
		if e > len(d.order) {
			e = len(d.order)
		}
		if s >= e {
			continue // covers nothing; painting it would be a no-op
		}
		v := op.value
		if v == nil {
			v = removedSentinel{}
		}
		byKey[op.key] = append(byKey[op.key], len(ranges))
		ranges = append(ranges, opRange{value: v, lo: s, hi: e})
	}

	var spans []Span
	for key, idxs := range byKey {
		segs := mergeSegments(ranges, idxs, visBefore)
		// Bucket each operation into the one segment that contains it, keeping
		// id order within a bucket so the last painter still wins.
		buckets := make([][]int, len(segs))
		for _, ri := range idxs {
			lo := ranges[ri].lo
			si := sort.Search(len(segs), func(i int) bool { return segs[i].hi > lo })
			buckets[si] = append(buckets[si], ri)
		}

		for si, seg := range segs {
			paint := make([]any, seg.hi-seg.lo)
			// Paint in DESCENDING id order, never overwriting. The highest-id
			// operation covering a character wins, so filling from the top and
			// keeping the first value written gives the same result as filling
			// from the bottom and letting the last writer win — except each
			// character is written once instead of once per operation covering
			// it, and a segment that fills up stops the walk early.
			//
			// That is what bounds a long formatting history: toggling one mark
			// over a whole document N times costs one pass, not N. bucket is in
			// ascending id order, so this runs it backwards.
			bucket := buckets[si]
			unpainted := len(paint)
			for i := len(bucket) - 1; i >= 0 && unpainted > 0; i-- {
				value := ranges[bucket[i]].value
				covered := paint[ranges[bucket[i]].lo-seg.lo : ranges[bucket[i]].hi-seg.lo]
				for k := range covered {
					if covered[k] == nil {
						covered[k] = value
						unpainted--
					}
				}
			}

			// Walk the segment's visible characters, emitting maximal runs of
			// equal value.
			visIdx := visBefore[seg.lo]
			runStart, runValue := -1, any(nil)
			flush := func() {
				if runStart >= 0 && runValue != nil {
					spans = append(spans, Span{Start: runStart, End: visIdx, Key: key, Value: runValue})
				}
				runStart, runValue = -1, nil
			}
			for k, it := range d.order[seg.lo:seg.hi] {
				if it.deleted {
					continue
				}
				v := paint[k]
				if (removedSentinel{}) == v {
					v = nil
				}
				if runStart >= 0 && v != runValue {
					flush()
				}
				if runStart < 0 && v != nil {
					runStart, runValue = visIdx, v
				}
				visIdx++
			}
			flush()
		}
	}
	sort.Slice(spans, func(i, j int) bool {
		a, b := spans[i], spans[j]
		if a.Start != b.Start {
			return a.Start < b.Start
		}
		if a.End != b.End {
			return a.End < b.End
		}
		return a.Key < b.Key
	})
	return spans, nil
}

// opRange is one mark operation's resolved coverage: a half-open range of
// full-order positions and the value it paints there (removedSentinel for an
// unmark, so "painted as removed" stays distinct from "never painted").
type opRange struct {
	value  any
	lo, hi int
}

// segment is a stretch of the full order that one mark key's operations cover.
type segment struct{ lo, hi int }

// mergeSegments reduces one key's operation ranges to the disjoint stretches
// they cover, so resolution can skip everything between them.
//
// Two ranges join when they overlap, touch, OR are separated only by
// tombstones. The last case is the one that is easy to get wrong: the emit walk
// skips deleted characters, so a gap made entirely of them does not interrupt a
// run of equal value, and splitting there would report two spans where whole-
// document painting reports one.
func mergeSegments(ranges []opRange, idxs []int, visBefore []int) []segment {
	order := make([]int, len(idxs))
	copy(order, idxs)
	sort.Slice(order, func(i, j int) bool { return ranges[order[i]].lo < ranges[order[j]].lo })

	segs := make([]segment, 0, len(order))
	for _, ri := range order {
		r := ranges[ri]
		if n := len(segs); n > 0 &&
			(r.lo <= segs[n-1].hi || visBefore[r.lo] == visBefore[segs[n-1].hi]) {
			if r.hi > segs[n-1].hi {
				segs[n-1].hi = r.hi
			}
			continue
		}
		segs = append(segs, segment{lo: r.lo, hi: r.hi})
	}
	return segs
}

// removedSentinel distinguishes "painted with nil (removed)" from "never
// painted" in the per-character paint array.
type removedSentinel struct{}
