package text_test

import (
	"encoding/binary"
	"math"
	"math/rand"
	"runtime"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

var _ = Describe("Doc", func() {
	It("inserts and reads plain text", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello")).To(Succeed())
		Expect(d.Insert(5, " world")).To(Succeed())
		Expect(d.Insert(5, ",")).To(Succeed())
		Expect(d.PlainText()).To(Equal("hello, world"))
		Expect(d.Len()).To(Equal(12))
	})

	It("deletes ranges", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		Expect(d.Delete(5, 6)).To(Succeed())
		Expect(d.PlainText()).To(Equal("hello"))
	})

	It("handles multi-byte code points", func() {
		d := text.New("a")
		Expect(d.Insert(0, "héllo")).To(Succeed())
		Expect(d.Insert(5, " 🎉")).To(Succeed())
		Expect(d.PlainText()).To(Equal("héllo 🎉"))
		Expect(d.Len()).To(Equal(7))
		Expect(d.Delete(6, 1)).To(Succeed())
		Expect(d.PlainText()).To(Equal("héllo "))
	})

	It("rejects out-of-range edits", func() {
		d := text.New("a")
		Expect(d.Insert(1, "x")).NotTo(Succeed())
		Expect(d.Insert(0, "ab")).To(Succeed())
		Expect(d.Delete(1, 2)).NotTo(Succeed())
		Expect(d.Delete(-1, 1)).NotTo(Succeed())
	})

	It("rejects edits without a peer", func() {
		d := text.New("")
		Expect(d.Insert(0, "x")).NotTo(Succeed())
	})

	// Items are code points, so a byte sequence that is not text has no
	// faithful representation. Storing U+FFFD in its place is the failure this
	// guards: the document would be well-formed, pass every later check, and
	// silently differ from what the writer handed over.
	It("rejects invalid UTF-8 rather than substituting U+FFFD", func() {
		d := text.New("a")
		err := d.Insert(0, "caf\xe9 latte") // latin-1 é
		Expect(err).To(MatchError("text: insert: invalid UTF-8 at byte 3"))
		Expect(d.PlainText()).To(BeEmpty(), "a rejected insert must not edit")

		Expect(d.Insert(0, "café latte")).To(Succeed())
		Expect(d.Insert(4, "\xff")).To(MatchError("text: insert: invalid UTF-8 at byte 0"))
		Expect(d.PlainText()).To(Equal("café latte"))
	})

	It("round-trips through Encode/Decode", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		Expect(d.Delete(2, 3)).To(Succeed())
		_, err := d.Mark(0, 4, "bold", true, text.ExpandAfter)
		Expect(err).NotTo(HaveOccurred())

		enc := d.Encode()
		d2, err := text.Decode(enc, "bob")
		Expect(err).NotTo(HaveOccurred())
		Expect(d2.PlainText()).To(Equal(d.PlainText()))
		spans, err := d.Marks()
		Expect(err).NotTo(HaveOccurred())
		Expect(d2.Marks()).To(Equal(spans))
		Expect(d2.Encode()).To(Equal(enc), "re-encode must be byte-identical")
	})

	It("decodes empty bytes to an empty document", func() {
		d, err := text.Decode(nil, "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(d.PlainText()).To(Equal(""))
	})

	It("exposes the version vector", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abc")).To(Succeed())
		Expect(d.VersionVector()).To(Equal(map[string]uint64{"a": 3}))
	})
})

// Decode runs on bytes a client supplied (a PATCH to a text field merges into
// the stored document), so a tiny body must not be able to make it allocate a
// large container from a count it never backs with data.
var _ = Describe("Decode of hostile input", func() {
	// header builds a document prefix whose counts are taken verbatim, so a
	// count can be set far beyond what the remaining bytes could describe.
	header := func(counts ...uint64) []byte {
		buf := []byte{1} // format version
		for _, c := range counts {
			buf = binary.AppendUvarint(buf, c)
		}
		return buf
	}

	DescribeTable("rejects a count the body cannot back",
		func(data []byte) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := text.Decode(data, "")
			runtime.ReadMemStats(&after)

			Expect(err).To(HaveOccurred())
			// TotalAlloc is cumulative, so this measures what the call asked
			// for regardless of when the GC hands it back.
			Expect(after.TotalAlloc-before.TotalAlloc).To(BeNumerically("<", 1<<20),
				"a %d-byte body must not allocate a megabyte", len(data))
		},
		Entry("peer count", header(1<<25)),
		Entry("peer count near the varint ceiling", header(1<<62)),
		Entry("run count", header(0, 1<<25)),
		Entry("mark count", header(0, 0, 1<<25)),
	)

	It("accepts counts that exactly saturate the remaining bytes", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hi")).To(Succeed())
		_, err := d.Mark(0, 2, "b", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, err = text.Decode(d.Encode(), "")
		Expect(err).NotTo(HaveOccurred())
	})
})

// Every edit takes a counter above every counter the document has seen, and the
// format carries none above 2^53−1. A version vector that reaches it leaves no
// counter for the next edit.
var _ = Describe("Edits at the counter ceiling", func() {
	const ceiling = 1<<53 - 1

	// seenUpTo is a document with no items whose version vector holds one peer
	// at counter, as another writer may leave it.
	seenUpTo := func(counter uint64) []byte {
		buf := []byte{1, 1, 7}
		buf = append(buf, "mallory"...)
		buf = binary.AppendUvarint(buf, counter)
		return append(buf, 0, 0) // no runs, no marks
	}

	open := func(data []byte) *text.Doc {
		GinkgoHelper()
		d, err := text.Decode(data, "alice")
		Expect(err).NotTo(HaveOccurred())
		return d
	}

	It("refuses an insert, and changes nothing", func() {
		d := open(seenUpTo(ceiling))
		before := d.Encode()

		Expect(d.Insert(0, "x")).To(MatchError(text.ErrCounterCeiling))
		Expect(d.Encode()).To(Equal(before))
	})

	It("refuses a mark, and changes nothing", func() {
		d := open(seenUpTo(ceiling))
		before := d.Encode()

		_, err := d.Mark(0, 0, "bold", true, text.ExpandBoth)
		Expect(err).To(MatchError(text.ErrCounterCeiling))
		Expect(d.Encode()).To(Equal(before))
	})

	It("uses the counters up to the ceiling, and its encoding decodes", func() {
		d := open(seenUpTo(ceiling - 2))

		Expect(d.Insert(0, "ab")).To(Succeed())
		Expect(text.Decode(d.Encode(), "")).Error().NotTo(HaveOccurred())
		Expect(d.Insert(2, "c")).To(MatchError(text.ErrCounterCeiling))
		Expect(d.PlainText()).To(Equal("ab"))
	})

	It("refuses an insert that needs more counters than are left", func() {
		d := open(seenUpTo(ceiling - 1))

		Expect(d.Insert(0, "ab")).To(MatchError(text.ErrCounterCeiling))
		Expect(d.PlainText()).To(BeEmpty())
	})
})

var _ = Describe("MaxCounter", func() {
	It("reports the highest counter of the version vector", func() {
		a := text.New("a")
		Expect(a.Insert(0, "hello")).To(Succeed())
		b := text.New("b")
		Expect(b.Insert(0, "hi")).To(Succeed())
		merged, err := text.Merge(a.Encode(), b.Encode())
		Expect(err).NotTo(HaveOccurred())

		Expect(text.MaxCounter(merged)).To(Equal(uint64(5)))
	})

	It("reports 0 for empty input, which decodes as an empty document", func() {
		Expect(text.MaxCounter(nil)).To(Equal(uint64(0)))
	})

	It("refuses a fragment", func() {
		d := text.New("a")
		Expect(d.Insert(0, "x")).To(Succeed())
		frag, err := text.EncodeFragment(d, nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(text.MaxCounter(frag)).Error().To(MatchError(ContainSubstring("unsupported format version 2")))
	})
})

// fork decodes a's current state as a new replica owned by peer.
func fork(d *text.Doc, peer string) *text.Doc {
	nd, err := text.Decode(d.Encode(), peer)
	Expect(err).NotTo(HaveOccurred())
	return nd
}

// converge merges the encodings both ways and verifies byte equality.
func converge(a, b *text.Doc) *text.Doc {
	ab, err := text.Merge(a.Encode(), b.Encode())
	Expect(err).NotTo(HaveOccurred())
	ba, err := text.Merge(b.Encode(), a.Encode())
	Expect(err).NotTo(HaveOccurred())
	Expect(ab).To(Equal(ba), "merge must be commutative")
	d, err := text.Decode(ab, "")
	Expect(err).NotTo(HaveOccurred())
	return d
}

var _ = Describe("Merge", func() {
	It("merges concurrent inserts at different positions", func() {
		a := text.New("a")
		Expect(a.Insert(0, "hello world")).To(Succeed())
		b := fork(a, "b")
		Expect(a.Insert(0, ">> ")).To(Succeed())
		Expect(b.Insert(11, " <<")).To(Succeed())
		Expect(converge(a, b).PlainText()).To(Equal(">> hello world <<"))
	})

	It("does not interleave concurrent forward typing at the same position", func() {
		base := text.New("seed")
		Expect(base.Insert(0, "[]")).To(Succeed())
		a := fork(base, "alice")
		b := fork(base, "bob")
		Expect(a.Insert(1, "alice")).To(Succeed())
		Expect(b.Insert(1, "bob")).To(Succeed())
		got := converge(a, b).PlainText()
		Expect(got).To(SatisfyAny(
			Equal("[alicebob]"), Equal("[bobalice]"),
		), "sequences must stay contiguous, got %q", got)
	})

	It("does not interleave concurrent backward typing at the same position", func() {
		a := text.New("alice")
		b := text.New("bob")
		// Each types their word by prepending one character at a time.
		for i := len("ecila") - 1; i >= 0; i-- {
			Expect(a.Insert(0, string("alice"[i]))).To(Succeed())
		}
		for i := len("bob") - 1; i >= 0; i-- {
			Expect(b.Insert(0, string("bob"[i]))).To(Succeed())
		}
		got := converge(a, b).PlainText()
		Expect(got).To(SatisfyAny(
			Equal("alicebob"), Equal("bobalice"),
		), "sequences must stay contiguous, got %q", got)
	})

	It("is idempotent", func() {
		a := text.New("a")
		Expect(a.Insert(0, "abc")).To(Succeed())
		enc := a.Encode()
		m, err := text.Merge(enc, enc)
		Expect(err).NotTo(HaveOccurred())
		Expect(m).To(Equal(enc))
	})

	It("merges concurrent delete and insert", func() {
		base := text.New("seed")
		Expect(base.Insert(0, "hello world")).To(Succeed())
		a := fork(base, "a")
		b := fork(base, "b")
		Expect(a.Delete(0, 6)).To(Succeed())    // "world"
		Expect(b.Insert(11, "!")).To(Succeed()) // "hello world!"
		Expect(converge(a, b).PlainText()).To(Equal("world!"))
	})

	It("keeps a delete-everything merge convergent", func() {
		base := text.New("seed")
		Expect(base.Insert(0, "abc")).To(Succeed())
		a := fork(base, "a")
		b := fork(base, "b")
		Expect(a.Delete(0, 3)).To(Succeed())
		Expect(b.Insert(3, "d")).To(Succeed())
		Expect(converge(a, b).PlainText()).To(Equal("d"))
	})

	// Two replicas sharing a peer is the one input Merge cannot converge, and
	// nothing in the encoding reveals it — so it has to be caught here, loudly.
	// (Callers avoid it: see the peer-uniqueness invariant on text.ID.)
	Describe("two replicas that shared a peer", func() {
		It("rejects the same id carrying different content", func() {
			base := text.New("seed")
			Expect(base.Insert(0, "AB")).To(Succeed())
			a := fork(base, "dup")
			b := fork(base, "dup")
			Expect(a.Insert(0, "x")).To(Succeed())
			Expect(b.Insert(0, "y")).To(Succeed())
			_, err := text.Merge(a.Encode(), b.Encode())
			Expect(err).To(MatchError(ContainSubstring("different content")))
		})

		It("rejects the same id placed differently", func() {
			base := text.New("seed")
			Expect(base.Insert(0, "AB")).To(Succeed())
			a := fork(base, "dup")
			b := fork(base, "dup")
			// Same character, different position: identical ids, different
			// parents. Unchecked this merges silently and non-commutatively —
			// "zAB" one way round, "ABz" the other.
			Expect(a.Insert(0, "z")).To(Succeed())
			Expect(b.Insert(2, "z")).To(Succeed())
			_, err := text.Merge(a.Encode(), b.Encode())
			Expect(err).To(MatchError(ContainSubstring("different placement")))
			_, err = text.Merge(b.Encode(), a.Encode())
			Expect(err).To(MatchError(ContainSubstring("different placement")))
		})

		It("rejects the same mark op id covering different keys or ranges", func() {
			base := text.New("seed")
			Expect(base.Insert(0, "hello")).To(Succeed())
			a := fork(base, "dup")
			b := fork(base, "dup")
			_, err := a.Mark(0, 5, "bold", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			_, err = b.Mark(0, 3, "bold", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			_, err = text.Merge(a.Encode(), b.Encode())
			Expect(err).To(MatchError(ContainSubstring("differs on each side")))
		})

		It("still accepts a mark op both sides genuinely share", func() {
			// The ordinary case the check must not break: one op, inherited by
			// two replicas that then diverge elsewhere.
			base := text.New("seed")
			Expect(base.Insert(0, "hello")).To(Succeed())
			_, err := base.Mark(0, 5, "bold", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			a := fork(base, "a")
			b := fork(base, "b")
			Expect(a.Insert(5, "X")).To(Succeed())
			Expect(b.Insert(0, "Y")).To(Succeed())
			merged := converge(a, b)
			Expect(merged.PlainText()).To(Equal("YhelloX"))
			Expect(merged.Marks()).To(HaveLen(1))
		})
	})

	Describe("ObserveVersion", func() {
		It("lets one peer edit a stale ancestor without re-spending counters", func() {
			// The connector's shape: edit #1 lands, edit #2 diffs against the
			// ancestor that predates it, then merges into the current state.
			cur := text.New("w")
			Expect(cur.Insert(0, "hello")).To(Succeed())
			ancestor := cur.Encode()
			Expect(cur.Insert(5, " there")).To(Succeed())

			next, err := text.Decode(ancestor, "w")
			Expect(err).NotTo(HaveOccurred())
			next.ObserveVersion(cur.VersionVector())
			Expect(next.Insert(0, "oh ")).To(Succeed())
			Expect(next.MergeDocs(cur)).To(Succeed())
			Expect(next.PlainText()).To(Equal("oh hello there"))
		})

		It("is what stops that from colliding", func() {
			cur := text.New("w")
			Expect(cur.Insert(0, "hello")).To(Succeed())
			ancestor := cur.Encode()
			Expect(cur.Insert(5, " there")).To(Succeed())

			// Same sequence without observing first: the ancestor's version
			// vector stops at "hello", so the new item re-uses a counter the
			// second edit already spent under this peer.
			next, err := text.Decode(ancestor, "w")
			Expect(err).NotTo(HaveOccurred())
			Expect(next.Insert(0, "oh ")).To(Succeed())
			Expect(next.MergeDocs(cur)).To(MatchError(ContainSubstring("merge conflict")))
		})
	})

	It("treats empty sides as empty documents", func() {
		a := text.New("a")
		Expect(a.Insert(0, "x")).To(Succeed())
		m, err := text.Merge(nil, a.Encode())
		Expect(err).NotTo(HaveOccurred())
		Expect(m).To(Equal(a.Encode()))
		m, err = text.Merge(a.Encode(), nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(m).To(Equal(a.Encode()))
	})
})

var _ = Describe("Marks", func() {
	It("resolves a simple mark", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		_, err := d.Mark(0, 5, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Marks()).To(Equal([]text.Span{{Start: 0, End: 5, Key: "bold", Value: true}}))
	})

	It("moves marks with the text", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		_, err := d.Mark(6, 11, "comment:c1", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Insert(0, ">> ")).To(Succeed())
		Expect(d.Marks()).To(Equal([]text.Span{{Start: 9, End: 14, Key: "comment:c1", Value: true}}))
	})

	It("does not expand a NONE mark at its boundaries", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abc")).To(Succeed())
		_, err := d.Mark(1, 2, "m", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Insert(2, "xy")).To(Succeed()) // right at the mark's end
		Expect(d.PlainText()).To(Equal("abxyc"))
		Expect(d.Marks()).To(Equal([]text.Span{{Start: 1, End: 2, Key: "m", Value: true}}))
	})

	It("expands an AFTER mark at its end boundary", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abc")).To(Succeed())
		_, err := d.Mark(1, 2, "m", true, text.ExpandAfter)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Insert(2, "xy")).To(Succeed())
		Expect(d.Marks()).To(Equal([]text.Span{{Start: 1, End: 4, Key: "m", Value: true}}))
	})

	It("removes marks with Unmark", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello")).To(Succeed())
		_, err := d.Mark(0, 5, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, err = d.Unmark(1, 4, "bold", text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Marks()).To(Equal([]text.Span{
			{Start: 0, End: 1, Key: "bold", Value: true},
			{Start: 4, End: 5, Key: "bold", Value: true},
		}))
	})

	It("lets overlapping keys coexist", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		_, err := d.Mark(0, 8, "comment:c1", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, err = d.Mark(3, 11, "comment:c2", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Marks()).To(Equal([]text.Span{
			{Start: 0, End: 8, Key: "comment:c1", Value: true},
			{Start: 3, End: 11, Key: "comment:c2", Value: true},
		}))
	})

	It("resolves same-key overlaps by highest op id", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abcdef")).To(Succeed())
		_, err := d.Mark(0, 6, "color", "red", text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, err = d.Mark(2, 4, "color", "blue", text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Marks()).To(Equal([]text.Span{
			{Start: 0, End: 2, Key: "color", Value: "red"},
			{Start: 2, End: 4, Key: "color", Value: "blue"},
			{Start: 4, End: 6, Key: "color", Value: "red"},
		}))
	})

	It("keeps marks attached across concurrent edits to the marked range", func() {
		base := text.New("seed")
		Expect(base.Insert(0, "hello world")).To(Succeed())
		a := fork(base, "a")
		b := fork(base, "b")
		_, err := a.Mark(6, 11, "comment:c1", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(b.Insert(6, "big ")).To(Succeed())
		m := converge(a, b)
		Expect(m.PlainText()).To(Equal("hello big world"))
		// The mark tracks its characters: "world" moved to [10,15) and the
		// text inserted before it stays outside (ExpandNone).
		Expect(m.Marks()).To(Equal([]text.Span{{Start: 10, End: 15, Key: "comment:c1", Value: true}}))
	})

	It("rejects an inverted range", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abc")).To(Succeed())
		_, err := d.Mark(2, 1, "bold", true, text.ExpandNone)
		Expect(err).To(MatchError("text: mark range [2,1) invalid for length 3"))
	})

	DescribeTable("rejects a one-sided expand policy on a collapsed mark",
		func(expand text.Expand) {
			d := text.New("a")
			Expect(d.Insert(0, "abc")).To(Succeed())
			_, err := d.Mark(1, 1, "bold", true, expand)
			Expect(err).To(MatchError(ContainSubstring("needs ExpandNone or ExpandBoth")))
		},
		Entry("before", text.ExpandBefore),
		Entry("after", text.ExpandAfter),
	)

	// Both implementations refuse these at creation and at decode, so a
	// document can never carry one; see the rejection vectors for the decode
	// half.
	DescribeTable("rejects a non-finite mark value",
		func(v any) {
			d := text.New("a")
			Expect(d.Insert(0, "abc")).To(Succeed())
			_, err := d.Mark(0, 3, "size", v, text.ExpandNone)
			Expect(err).To(MatchError("text: non-finite mark value"))
		},
		Entry("NaN", math.NaN()),
		Entry("+Inf", math.Inf(1)),
		Entry("-Inf", math.Inf(-1)),
		Entry("float32 +Inf", float32(math.Inf(1))),
	)
})

// Collapsed marks are the pending-format and point-annotation states: a mark
// operation whose range covers no character. They ride in the ordinary mark
// section of the encoding — no new anchor kind, no new expand value, no format
// version — which is why the round-trip check below matters as much as the
// resolution ones.
var _ = Describe("Collapsed marks", func() {
	Describe("as a pending format", func() {
		pending := func() *text.Doc {
			GinkgoHelper()
			d := text.New("a")
			Expect(d.Insert(0, "abcdef")).To(Succeed())
			_, err := d.Mark(3, 3, "bold", true, text.ExpandBoth)
			Expect(err).NotTo(HaveOccurred())
			return d
		}

		It("covers nothing until text is typed at the caret", func() {
			d := pending()
			Expect(d.Marks()).To(BeEmpty())
			Expect(d.MarksAt(3)).To(Equal([]text.PendingMark{{Key: "bold", Value: true}}))
			Expect(d.MarksAt(2)).To(BeEmpty())
			Expect(d.MarksAt(4)).To(BeEmpty())
		})

		It("becomes an ordinary span on the first character typed there", func() {
			d := pending()
			Expect(d.Insert(3, "X")).To(Succeed())
			Expect(d.Marks()).To(Equal([]text.Span{{Start: 3, End: 4, Key: "bold", Value: true}}))
			// And keeps taking in what follows, which is what makes it usable
			// as a pending format rather than a one-character one.
			Expect(d.Insert(4, "YZ")).To(Succeed())
			Expect(d.Marks()).To(Equal([]text.Span{{Start: 3, End: 6, Key: "bold", Value: true}}))
			Expect(d.PlainText()).To(Equal("abcXYZdef"))
		})

		It("is cancelled by an unmark at the same caret", func() {
			d := pending()
			_, err := d.Unmark(3, 3, "bold", text.ExpandBoth)
			Expect(err).NotTo(HaveOccurred())
			Expect(d.MarksAt(3)).To(BeEmpty())
			Expect(d.Insert(3, "X")).To(Succeed())
			Expect(d.Marks()).To(BeEmpty())
		})

		It("survives a round-trip through the version-1 encoding", func() {
			d := pending()
			enc := d.Encode()
			round, err := text.Decode(enc, "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(round.Encode()).To(Equal(enc))
			Expect(round.Marks()).To(BeEmpty())
			Expect(round.MarksAt(3)).To(Equal([]text.PendingMark{{Key: "bold", Value: true}}))
			Expect(round.Insert(3, "X")).To(Succeed())
			Expect(round.Marks()).To(Equal([]text.Span{{Start: 3, End: 4, Key: "bold", Value: true}}))
		})

		It("expands over text a concurrent replica typed at the caret", func() {
			base := text.New("seed")
			Expect(base.Insert(0, "abcdef")).To(Succeed())
			a := fork(base, "a")
			b := fork(base, "b")
			_, err := a.Mark(3, 3, "bold", true, text.ExpandBoth)
			Expect(err).NotTo(HaveOccurred())
			Expect(b.Insert(3, "XY")).To(Succeed())
			m := converge(a, b)
			Expect(m.PlainText()).To(Equal("abcXYdef"))
			Expect(m.Marks()).To(Equal([]text.Span{{Start: 3, End: 5, Key: "bold", Value: true}}))
		})
	})

	Describe("as a point annotation", func() {
		noted := func() *text.Doc {
			GinkgoHelper()
			d := text.New("a")
			Expect(d.Insert(0, "abcdef")).To(Succeed())
			_, err := d.Mark(2, 2, "note:n1", "look here", text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			return d
		}

		It("reports as a point rather than a span", func() {
			d := noted()
			Expect(d.Marks()).To(BeEmpty())
			Expect(d.Points()).To(Equal([]text.Point{{Index: 2, Key: "note:n1", Value: "look here"}}))
			// A note is not pending formatting: text typed at the position does
			// not take it on, which is the whole difference from ExpandBoth.
			Expect(d.MarksAt(2)).To(BeEmpty())
		})

		It("stays zero-width when text is typed at its position", func() {
			d := noted()
			Expect(d.Insert(2, "XY")).To(Succeed())
			Expect(d.PlainText()).To(Equal("abXYcdef"))
			Expect(d.Marks()).To(BeEmpty())
			// Anchored to the trailing edge of "ab", so the typed text lands
			// after the note and the note keeps its index.
			Expect(d.Points()).To(Equal([]text.Point{{Index: 2, Key: "note:n1", Value: "look here"}}))
		})

		It("drifts with the text in front of it", func() {
			d := noted()
			Expect(d.Insert(0, ">> ")).To(Succeed())
			Expect(d.Points()).To(Equal([]text.Point{{Index: 5, Key: "note:n1", Value: "look here"}}))
		})

		It("is removed by an unmark at the same position", func() {
			d := noted()
			_, err := d.Unmark(2, 2, "note:n1", text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			Expect(d.Points()).To(BeEmpty())
		})

		It("keeps two notes on the same key at different positions", func() {
			d := noted()
			_, err := d.Mark(5, 5, "note:n1", "and here", text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			Expect(d.Points()).To(Equal([]text.Point{
				{Index: 2, Key: "note:n1", Value: "look here"},
				{Index: 5, Key: "note:n1", Value: "and here"},
			}))
		})
	})

	Describe("MarksAt on ordinary marks", func() {
		It("follows the expand policy at each boundary", func() {
			d := text.New("a")
			Expect(d.Insert(0, "abcde")).To(Succeed())
			_, err := d.Mark(1, 3, "m", true, text.ExpandAfter)
			Expect(err).NotTo(HaveOccurred())
			active := []text.PendingMark{{Key: "m", Value: true}}
			Expect(d.MarksAt(1)).To(BeEmpty()) // start boundary does not expand
			Expect(d.MarksAt(2)).To(Equal(active))
			Expect(d.MarksAt(3)).To(Equal(active)) // end boundary does
			Expect(d.MarksAt(4)).To(BeEmpty())
		})

		It("reports a mark running to the end of the document", func() {
			d := text.New("a")
			Expect(d.Insert(0, "abc")).To(Succeed())
			_, err := d.Mark(0, 3, "m", true, text.ExpandBoth)
			Expect(err).NotTo(HaveOccurred())
			Expect(d.MarksAt(3)).To(Equal([]text.PendingMark{{Key: "m", Value: true}}))
		})

		It("rejects an out-of-range index", func() {
			d := text.New("a")
			Expect(d.Insert(0, "abc")).To(Succeed())
			_, err := d.MarksAt(4)
			Expect(err).To(MatchError("text: index 4 out of range [0..3]"))
		})
	})
})

var _ = Describe("Anchors", func() {
	It("round-trips through EncodeAnchor/DecodeAnchor", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello")).To(Succeed())
		for _, kind := range []text.AnchorKind{text.AnchorBefore, text.AnchorAfter} {
			a, err := d.AnchorAt(3, kind)
			Expect(err).NotTo(HaveOccurred())
			back, err := text.DecodeAnchor(text.EncodeAnchor(a))
			Expect(err).NotTo(HaveOccurred())
			Expect(back).To(Equal(a))
		}
	})

	It("survives remote inserts before the anchor", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		a, err := d.AnchorAt(6, text.AnchorBefore) // before "world"
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Insert(0, ">> ")).To(Succeed())
		idx, err := d.ResolveAnchor(a)
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(9))
	})

	It("falls back to the nearest live neighbour after deletion", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abcdef")).To(Succeed())
		before, err := d.AnchorAt(3, text.AnchorBefore) // before 'd'
		Expect(err).NotTo(HaveOccurred())
		after, err := d.AnchorAt(4, text.AnchorAfter) // after 'd'
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Delete(2, 2)).To(Succeed()) // delete "cd"
		Expect(d.PlainText()).To(Equal("abef"))
		idx, err := d.ResolveAnchor(before)
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(2)) // nearest live to the right: 'e'
		idx, err = d.ResolveAnchor(after)
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(2)) // nearest live to the left: 'b', +1
	})

	It("errors on unknown items", func() {
		d := text.New("a")
		_, err := d.ResolveAnchor(text.Anchor{
			Kind: text.AnchorBefore,
			Item: text.ID{Peer: "ghost", Counter: 1},
		})
		Expect(err).To(HaveOccurred())
	})

	It("resolves document edges", func() {
		d := text.New("a")
		Expect(d.Insert(0, "abc")).To(Succeed())
		start, err := d.AnchorAt(0, text.AnchorAfter)
		Expect(err).NotTo(HaveOccurred())
		Expect(start.Kind).To(Equal(text.AnchorDocStart))
		end, err := d.AnchorAt(3, text.AnchorBefore)
		Expect(err).NotTo(HaveOccurred())
		Expect(end.Kind).To(Equal(text.AnchorDocEnd))
		i, err := d.ResolveAnchor(start)
		Expect(err).NotTo(HaveOccurred())
		Expect(i).To(Equal(0))
		i, err = d.ResolveAnchor(end)
		Expect(err).NotTo(HaveOccurred())
		Expect(i).To(Equal(3))
	})
})

var _ = Describe("Diff", func() {
	It("reports remote inserts and deletes against retained items", func() {
		prev := text.New("a")
		Expect(prev.Insert(0, "hello world")).To(Succeed())
		next := fork(prev, "b")
		Expect(next.Insert(5, ",")).To(Succeed())
		Expect(next.Delete(7, 5)).To(Succeed()) // "world"
		Expect(next.Insert(7, "there")).To(Succeed())

		delta, err := text.Diff(prev, next)
		Expect(err).NotTo(HaveOccurred())
		Expect(next.PlainText()).To(Equal("hello, there"))
		Expect(delta.Ops).To(Equal([]text.DeltaOp{
			{Kind: text.DeltaRetain, N: 5},
			{Kind: text.DeltaInsert, Text: ","},
			{Kind: text.DeltaRetain, N: 1},
			{Kind: text.DeltaInsert, Text: "there"},
			{Kind: text.DeltaDelete, N: 5},
		}))
		Expect(delta.MarksChanged).To(BeFalse())
	})

	It("flags mark changes", func() {
		prev := text.New("a")
		Expect(prev.Insert(0, "abc")).To(Succeed())
		next := fork(prev, "b")
		_, err := next.Mark(0, 2, "m", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		delta, err := text.Diff(prev, next)
		Expect(err).NotTo(HaveOccurred())
		Expect(delta.Ops).To(BeEmpty())
		Expect(delta.MarksChanged).To(BeTrue())
	})

	It("flags an edit that moves a resolved span, with the mark set unchanged", func() {
		prev := text.New("a")
		Expect(prev.Insert(0, "abcdef")).To(Succeed())
		_, err := prev.Mark(1, 4, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		next := fork(prev, "b")
		Expect(next.Insert(2, "X")).To(Succeed())

		// Both sides hold exactly one mark op, but the span it resolves to grew.
		delta, err := text.Diff(prev, next)
		Expect(err).NotTo(HaveOccurred())
		Expect(delta.MarksChanged).To(BeTrue())
		marks, err := next.Marks()
		Expect(err).NotTo(HaveOccurred())
		Expect(marks).To(Equal([]text.Span{{Start: 1, End: 5, Key: "bold", Value: true}}))
	})

	It("leaves an unmarked document's edits unflagged", func() {
		prev := text.New("a")
		Expect(prev.Insert(0, "abc")).To(Succeed())
		next := fork(prev, "b")
		Expect(next.Insert(3, "d")).To(Succeed())

		delta, err := text.Diff(prev, next)
		Expect(err).NotTo(HaveOccurred())
		Expect(delta.MarksChanged).To(BeFalse())
	})

	It("flags a swapped mark op at an unchanged count", func() {
		base := text.New("a")
		Expect(base.Insert(0, "abc")).To(Succeed())
		prev := fork(base, "b")
		_, err := prev.Mark(0, 2, "m", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		next := fork(base, "c")
		_, err = next.Mark(1, 3, "m", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())

		delta, err := text.Diff(prev, next)
		Expect(err).NotTo(HaveOccurred())
		Expect(delta.Ops).To(BeEmpty())
		Expect(delta.MarksChanged).To(BeTrue())
	})

	It("rejects a prev that is not an ancestor", func() {
		a := text.New("a")
		Expect(a.Insert(0, "abc")).To(Succeed())
		b := text.New("b")
		Expect(b.Insert(0, "xyz")).To(Succeed())
		_, err := text.Diff(a, b)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("SetString", func() {
	It("applies a minimal middle edit", func() {
		d := text.New("a")
		Expect(d.Insert(0, "hello cruel world")).To(Succeed())
		Expect(d.SetString("hello kind world")).To(Succeed())
		Expect(d.PlainText()).To(Equal("hello kind world"))
	})

	It("merges with a concurrent edit instead of clobbering", func() {
		base := text.New("seed")
		Expect(base.Insert(0, "one two three")).To(Succeed())
		a := fork(base, "connector")
		b := fork(base, "client")
		Expect(a.SetString("one TWO three")).To(Succeed())
		Expect(b.Insert(13, " four")).To(Succeed())
		Expect(converge(a, b).PlainText()).To(Equal("one TWO three four"))
	})

	It("handles clearing and full replacement", func() {
		d := text.New("a")
		Expect(d.SetString("abc")).To(Succeed())
		Expect(d.PlainText()).To(Equal("abc"))
		Expect(d.SetString("")).To(Succeed())
		Expect(d.PlainText()).To(Equal(""))
		Expect(d.SetString("xyz")).To(Succeed())
		Expect(d.PlainText()).To(Equal("xyz"))
	})

	// The check has to live in SetString itself: converting to runes below
	// substitutes U+FFFD for an invalid byte, so an Insert-only guard would
	// only ever see well-formed strings and pass.
	It("rejects invalid UTF-8 without touching the document", func() {
		d := text.New("a")
		Expect(d.Insert(0, "café latte")).To(Succeed())
		before := d.Encode()

		err := d.SetString("caf\xe9 latte")
		Expect(err).To(MatchError("text: setString: invalid UTF-8 at byte 3"))
		Expect(d.PlainText()).To(Equal("café latte"))
		Expect(d.Encode()).To(Equal(before), "a rejected write must leave no operations behind")
	})

	It("keeps disjoint edits from touching the text between them", func() {
		d := text.New("a")
		Expect(d.Insert(0, "one two three four five")).To(Succeed())
		_, err := d.Mark(8, 13, "comment:c1", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.SetString("one TWO three FOUR five")).To(Succeed())
		spans, err := d.Marks()
		Expect(err).NotTo(HaveOccurred())
		Expect(spans).To(Equal([]text.Span{{Start: 8, End: 13, Key: "comment:c1", Value: true}}))
	})

	It("inserts exactly len(new)-LCS items over random string pairs", func() {
		// The Myers script is minimal, so the number of freshly inserted items
		// is len(new) − LCS(old, new): everything common survives in place.
		// Observable through the version vector — the only counters spent are
		// the inserted items'.
		alphabet := []rune("abc é🎉")
		lcs := func(a, b []rune) int {
			dp := make([]int, len(b)+1)
			for i := 1; i <= len(a); i++ {
				prev := 0
				for j := 1; j <= len(b); j++ {
					cur := dp[j]
					if a[i-1] == b[j-1] {
						dp[j] = prev + 1
					} else if dp[j-1] > dp[j] {
						dp[j] = dp[j-1]
					}
					prev = cur
				}
			}
			return dp[len(b)]
		}
		rng := rand.New(rand.NewSource(42))
		randString := func() []rune {
			out := make([]rune, rng.Intn(40))
			for i := range out {
				out[i] = alphabet[rng.Intn(len(alphabet))]
			}
			return out
		}
		for trial := 0; trial < 200; trial++ {
			old := randString()
			next := randString()
			d := text.New("alice")
			Expect(d.Insert(0, string(old))).To(Succeed())
			Expect(d.SetString(string(next))).To(Succeed())
			Expect(d.PlainText()).To(Equal(string(next)))
			inserted := d.VersionVector()["alice"] - uint64(len(old))
			Expect(inserted).To(Equal(uint64(len(next)-lcs(old, next))),
				"trial %d: %q -> %q", trial, string(old), string(next))
		}
	})
})

var _ = Describe("Convergence property", func() {
	It("converges N replicas over random ops merged in random orders", func() {
		const (
			replicas = 4
			rounds   = 30
			seeds    = 10
		)
		alphabet := []rune("abcdefghijklmnopqrstuvwxyzåäö🎉")
		for seed := int64(0); seed < seeds; seed++ {
			rng := rand.New(rand.NewSource(seed))
			docs := make([]*text.Doc, replicas)
			for i := range docs {
				docs[i] = text.New(string(rune('A' + i)))
			}
			for r := 0; r < rounds; r++ {
				for _, d := range docs {
					switch op := rng.Intn(10); {
					case op < 5: // insert
						var sb strings.Builder
						for n := 1 + rng.Intn(4); n > 0; n-- {
							sb.WriteRune(alphabet[rng.Intn(len(alphabet))])
						}
						Expect(d.Insert(rng.Intn(d.Len()+1), sb.String())).To(Succeed())
					case op < 7: // delete
						if l := d.Len(); l > 0 {
							idx := rng.Intn(l)
							Expect(d.Delete(idx, 1+rng.Intn(min(3, l-idx)))).To(Succeed())
						}
					case op < 9: // mark, and one time in three remove instead
						if l := d.Len(); l > 1 {
							s := rng.Intn(l - 1)
							e := s + 1 + rng.Intn(l-s-1)
							key := "k" + string(rune('0'+rng.Intn(3)))
							expand := text.Expand(rng.Intn(4))
							// Removal is the nil-valued op whose whole purpose is to
							// win resolution over the marks it covers, so it is the
							// part of the mark model most exposed to an
							// order-dependent bug — it belongs in the random mix, not
							// only in fixed examples.
							var err error
							if rng.Intn(3) == 0 {
								_, err = d.Unmark(s, e, key, expand)
							} else {
								_, err = d.Mark(s, e, key, rng.Intn(2) == 0, expand)
							}
							Expect(err).NotTo(HaveOccurred())
						}
					default: // occasionally sync a random pair mid-flight
						i, j := rng.Intn(replicas), rng.Intn(replicas)
						if i != j {
							merged, err := text.Merge(docs[i].Encode(), docs[j].Encode())
							Expect(err).NotTo(HaveOccurred())
							nd, err := text.Decode(merged, docs[i].Peer())
							Expect(err).NotTo(HaveOccurred())
							docs[i] = nd
						}
					}
				}
			}
			// Merge all states in two different random orders; both must
			// produce identical canonical bytes.
			mergeAll := func(order []int) []byte {
				acc := docs[order[0]].Encode()
				for _, i := range order[1:] {
					var err error
					acc, err = text.Merge(acc, docs[i].Encode())
					Expect(err).NotTo(HaveOccurred())
				}
				return acc
			}
			o1 := rng.Perm(replicas)
			o2 := rng.Perm(replicas)
			m1 := mergeAll(o1)
			m2 := mergeAll(o2)
			Expect(m1).To(Equal(m2), "seed %d: merge orders %v vs %v diverged", seed, o1, o2)

			// And re-encoding a decode is stable (canonical form).
			d, err := text.Decode(m1, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(d.Encode()).To(Equal(m1))
		}
	})
})
