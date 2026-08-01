package text

import (
	"fmt"
	"math/rand"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// A mark anchored to an item the document does not hold cannot be built
// through the public API — Decode validates anchors and Mark builds them from
// live items — but MergeDocs takes in-memory documents that never passed
// through a decoder. The two implementations resolved it differently and
// silently (Go painted from the start of the document, TS painted nothing),
// so both now refuse; this reaches into the unexported mark op to produce the
// state.
var _ = Describe("Marks with a dangling anchor", func() {
	It("errors instead of painting a guessed range", func() {
		d := New("alice")
		Expect(d.Insert(0, "hello")).To(Succeed())
		_, err := d.Mark(0, 5, "bold", true, ExpandNone)
		Expect(err).NotTo(HaveOccurred())

		d.marks[0].start = Anchor{Kind: AnchorBefore, Item: ID{Peer: "ghost", Counter: 1}}
		_, err = d.Marks()
		Expect(err).To(MatchError("text: anchor references unknown item ghost:1"))

		d.marks[0].start = Anchor{Kind: AnchorDocStart}
		d.marks[0].end = Anchor{Kind: AnchorAfter, Item: ID{Peer: "ghost", Counter: 2}}
		_, err = d.Marks()
		Expect(err).To(MatchError("text: anchor references unknown item ghost:2"))
	})
})

// marksByWholeDocumentPainting is the definition Marks implements, written the
// obvious way: one paint array per key spanning the entire item order, painted
// in ascending id order, then read off in a single walk of the whole document.
//
// Marks itself does not do this — it confines each key to the stretches its own
// operations cover, which is what keeps a document with many keys (the
// annotation pattern) from costing keys × document. The two must agree exactly,
// so this stays as the reference the property below differentiates against.
func (d *Doc) marksByWholeDocumentPainting() ([]Span, error) {
	d.ensureOrder()
	if len(d.marks) == 0 {
		return nil, nil
	}
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

	ops := make([]*markOp, len(d.marks))
	copy(ops, d.marks)
	sort.Slice(ops, func(i, j int) bool { return ops[i].id.less(ops[j].id) })

	byKey := make(map[string][]any)
	for _, op := range ops {
		values := byKey[op.key]
		if values == nil {
			values = make([]any, len(d.order))
			byKey[op.key] = values
		}
		v := op.value
		if v == nil {
			v = removedSentinel{}
		}
		s, err := anchorPos(op.start)
		if err != nil {
			return nil, err
		}
		e, err := anchorPos(op.end)
		if err != nil {
			return nil, err
		}
		for f := s; f < e && f < len(d.order); f++ {
			values[f] = v
		}
	}

	var spans []Span
	for key, values := range byKey {
		visIdx := 0
		runStart, runValue := -1, any(nil)
		flush := func() {
			if runStart >= 0 && runValue != nil {
				spans = append(spans, Span{Start: runStart, End: visIdx, Key: key, Value: runValue})
			}
			runStart, runValue = -1, nil
		}
		for f, it := range d.order {
			if it.deleted {
				continue
			}
			v := values[f]
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

var _ = Describe("Marks segmentation property", func() {
	It("matches whole-document painting over random marks, unmarks and deletes", func() {
		const seeds = 40
		expands := []Expand{ExpandNone, ExpandBefore, ExpandAfter, ExpandBoth}
		// Few enough keys and values that operations genuinely overlap and
		// supersede each other rather than each getting its own stretch.
		keys := []string{"bold", "italic", "comment:1", "comment:2"}
		values := []any{true, "red", float64(2)}

		for seed := int64(0); seed < seeds; seed++ {
			rng := rand.New(rand.NewSource(seed))
			d := New("alice")
			Expect(d.Insert(0, "abcdefghijklmnopqrstuvwxyz")).To(Succeed())

			for round := 0; round < 40; round++ {
				l := d.Len()
				switch op := rng.Intn(10); {
				case op < 3 && l > 0: // delete, which leaves tombstones between marks
					start := rng.Intn(l)
					d.Delete(start, 1+rng.Intn(min(3, l-start)))
				case op < 5: // insert, so marks meet text that arrived after them
					d.Insert(rng.Intn(l+1), string(rune('A'+rng.Intn(26))))
				case l > 0: // mark or unmark a random range
					start := rng.Intn(l)
					end := start + 1 + rng.Intn(l-start)
					key := keys[rng.Intn(len(keys))]
					expand := expands[rng.Intn(len(expands))]
					if rng.Intn(4) == 0 {
						d.Unmark(start, end, key, expand)
					} else {
						d.Mark(start, end, key, values[rng.Intn(len(values))], expand)
					}
				}

				got, err := d.Marks()
				Expect(err).NotTo(HaveOccurred())
				want, err := d.marksByWholeDocumentPainting()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(want),
					"seed %d round %d: text %q", seed, round, d.PlainText())
			}
		}
	})
})
