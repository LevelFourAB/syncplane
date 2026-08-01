package text

import "fmt"

// DeltaKind tags a DeltaOp.
type DeltaKind uint8

const (
	// DeltaRetain keeps N code points unchanged.
	DeltaRetain DeltaKind = 0
	// DeltaInsert inserts Text at the current position.
	DeltaInsert DeltaKind = 1
	// DeltaDelete removes N code points at the current position.
	DeltaDelete DeltaKind = 2
)

// DeltaOp is one step of a text delta, in visible code-point coordinates.
type DeltaOp struct {
	Kind DeltaKind
	// N is the length for retain/delete ops.
	N int
	// Text is the inserted content for insert ops.
	Text string
}

// Delta describes how to transform one document's visible text into
// another's, plus whether the mark spans it resolved may have moved.
type Delta struct {
	Ops []DeltaOp
	// MarksChanged reports whether the mark spans an editor resolved against
	// prev may have moved, so that a binding re-reads Marks() exactly when the
	// formatting it renders could be stale.
	//
	// Spans are resolved from anchors against the current item order, not
	// stored, so they move whenever the text does: an insert inside a bold run
	// extends it, a delete can empty it. The flag is therefore set when the
	// mark-op sets differ OR when the delta changes text at all and the
	// document carries marks. That over-reports (an edit far from every mark
	// re-resolves needlessly) but is never wrong in the direction that matters,
	// and a document with no marks — the common case — never pays.
	MarksChanged bool
}

// Diff computes the delta from prev to next by comparing item graphs, not
// strings: an item visible in both is a retain even if equal text moved
// around it, so an editor applying the delta in place keeps its selection.
// prev must be a causal ancestor of next (every prev item exists in next),
// which is always true for a document evolving by local edits and merges.
func Diff(prev, next *Doc) (Delta, error) {
	prev.ensureOrder()
	next.ensureOrder()

	var (
		delta    Delta
		prevSeen int
	)
	emit := func(kind DeltaKind, n int, text string) {
		if n == 0 && text == "" {
			return
		}
		if last := len(delta.Ops) - 1; last >= 0 && delta.Ops[last].Kind == kind {
			delta.Ops[last].N += n
			delta.Ops[last].Text += text
			return
		}
		delta.Ops = append(delta.Ops, DeltaOp{Kind: kind, N: n, Text: text})
	}
	for _, n := range next.order {
		p, inPrev := prev.byID[n.id]
		visPrev := inPrev && !p.deleted
		visNext := !n.deleted
		if visPrev {
			prevSeen++
		}
		switch {
		case visPrev && visNext:
			emit(DeltaRetain, 1, "")
		case visPrev && !visNext:
			emit(DeltaDelete, 1, "")
		case !visPrev && visNext:
			emit(DeltaInsert, 0, string(n.r))
		}
	}
	if prevSeen != prev.Len() {
		return Delta{}, fmt.Errorf("text: diff: prev is not an ancestor of next (%d of %d visible items found)",
			prevSeen, prev.Len())
	}
	// A trailing retain carries no information; canonical deltas drop it so
	// the vectors have one spelling.
	if last := len(delta.Ops) - 1; last >= 0 && delta.Ops[last].Kind == DeltaRetain {
		delta.Ops = delta.Ops[:last]
	}
	delta.MarksChanged = markOpsDiffer(prev, next) || (len(next.marks) > 0 && changesText(delta.Ops))
	return delta, nil
}

// markOpsDiffer reports whether the two documents hold different sets of mark
// operations. Comparing sets rather than counts matters because prev is only
// checked for item ancestry: a caller diffing two states that swapped one mark
// op for another would otherwise be told nothing changed.
func markOpsDiffer(prev, next *Doc) bool {
	if len(prev.marks) != len(next.marks) {
		return true
	}
	prevIDs := make(map[ID]struct{}, len(prev.marks))
	for _, m := range prev.marks {
		prevIDs[m.id] = struct{}{}
	}
	for _, m := range next.marks {
		if _, ok := prevIDs[m.id]; !ok {
			return true
		}
	}
	return false
}

// changesText reports whether ops do anything but retain.
func changesText(ops []DeltaOp) bool {
	for _, op := range ops {
		if op.Kind != DeltaRetain {
			return true
		}
	}
	return false
}

// maxDiffCost bounds the Myers edit-distance search SetString runs: when the
// old and new strings are further apart than this, the multi-hunk diff falls
// back to one replacement hunk. The value is part of the cross-language
// contract — the TypeScript mirror uses the identical constant, and the shared
// vectors pin behavior on both sides of the boundary — because the fallback
// changes which items survive and therefore the canonical encoding.
const maxDiffCost = 1024

// editHunk is one contiguous change: at code-point index pos into the old
// text, delete del points and insert ins.
type editHunk struct {
	pos int
	del int
	ins []rune
}

// myersHunks computes a minimal edit script from old to new_ with the greedy
// O(ND) Myers algorithm, coalesced into hunks. Returns ok=false when the edit
// distance exceeds maxDiffCost. Deterministic — including its down/right
// tie-break — since both language implementations must emit identical
// scripts (identical scripts mean identical item ids, which the vectors pin).
func myersHunks(old, new_ []rune) ([]editHunk, bool) {
	n, m := len(old), len(new_)
	maxD := n + m
	if maxD > maxDiffCost {
		maxD = maxDiffCost
	}
	v := make([]int, 2*maxD+1)
	offset := maxD
	// trace[d] holds V for diagonals -d..d after round d (index k+d).
	trace := make([][]int, 0, 16)
	found := -1
search:
	for d := 0; d <= maxD; d++ {
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[offset+k-1] < v[offset+k+1]) {
				x = v[offset+k+1]
			} else {
				x = v[offset+k-1] + 1
			}
			y := x - k
			for x < n && y < m && old[x] == new_[y] {
				x++
				y++
			}
			v[offset+k] = x
			if x >= n && y >= m {
				snapshot := make([]int, 2*d+1)
				copy(snapshot, v[offset-d:offset+d+1])
				trace = append(trace, snapshot)
				found = d
				break search
			}
		}
		snapshot := make([]int, 2*d+1)
		copy(snapshot, v[offset-d:offset+d+1])
		trace = append(trace, snapshot)
	}
	if found < 0 {
		return nil, false
	}

	// Backtrack from (n, m), collecting single-point ops in reverse order.
	type editOp struct {
		pos    int // old-text index the op applies at
		del    bool
		insert rune
	}
	ops := make([]editOp, 0, found)
	x, y := n, m
	for d := found; d > 0; d-- {
		vp := trace[d-1]
		at := func(k int) int { return vp[(d-1)+k] }
		k := x - y
		down := k == -d || (k != d && at(k-1) < at(k+1))
		var prevK int
		if down {
			prevK = k + 1
		} else {
			prevK = k - 1
		}
		prevX := at(prevK)
		prevY := prevX - prevK
		if down {
			ops = append(ops, editOp{pos: prevX, insert: new_[prevY]})
		} else {
			ops = append(ops, editOp{pos: prevX, del: true})
		}
		x, y = prevX, prevY
	}

	// Reverse into forward order and coalesce adjacent ops into hunks: an op
	// extends the current hunk when it sits exactly past the points the hunk
	// already deletes (retained points in between close it).
	var hunks []editHunk
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		if len(hunks) > 0 {
			h := &hunks[len(hunks)-1]
			if op.pos == h.pos+h.del {
				if op.del {
					h.del++
				} else {
					h.ins = append(h.ins, op.insert)
				}
				continue
			}
		}
		h := editHunk{pos: op.pos}
		if op.del {
			h.del = 1
		} else {
			h.ins = []rune{op.insert}
		}
		hunks = append(hunks, h)
	}
	return hunks, true
}

// SetString rewrites the document's visible text to s. The longest common
// prefix and suffix are kept, and the middle is patched with a minimal
// multi-hunk diff (Myers, over code points), so items — and the marks and
// anchors attached to them — survive everywhere the text did not actually
// change. This is the write path for whole-value writers — connectors, the
// admin form and the string-valued mutation APIs — so an external edit merges
// with concurrent client edits instead of clobbering the document.
//
// When the two strings are further apart than maxDiffCost, the middle is
// replaced as a single hunk instead; edits that large rewrite most of the
// text anyway, and the bound keeps the diff linear in practice.
//
// s must be valid UTF-8. The check belongs here and not only in Insert:
// converting to runes below already substitutes U+FFFD for an invalid byte, so
// by the time a hunk reaches Insert the loss has happened and the string it is
// handed is well-formed.
func (d *Doc) SetString(s string) error {
	if err := requireWellFormed("setString", s); err != nil {
		return err
	}
	old := []rune(d.PlainText())
	new_ := []rune(s)

	prefix := 0
	for prefix < len(old) && prefix < len(new_) && old[prefix] == new_[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(old)-prefix && suffix < len(new_)-prefix &&
		old[len(old)-1-suffix] == new_[len(new_)-1-suffix] {
		suffix++
	}
	oldMid := old[prefix : len(old)-suffix]
	newMid := new_[prefix : len(new_)-suffix]
	if len(oldMid) == 0 && len(newMid) == 0 {
		return nil
	}
	hunks, ok := myersHunks(oldMid, newMid)
	if !ok {
		hunks = []editHunk{{pos: 0, del: len(oldMid), ins: newMid}}
	}
	// Right to left, so earlier hunks' indices stay valid as later ones edit.
	for i := len(hunks) - 1; i >= 0; i-- {
		h := hunks[i]
		if h.del > 0 {
			if err := d.Delete(prefix+h.pos, h.del); err != nil {
				return err
			}
		}
		if len(h.ins) > 0 {
			if err := d.Insert(prefix+h.pos, string(h.ins)); err != nil {
				return err
			}
		}
	}
	return nil
}
