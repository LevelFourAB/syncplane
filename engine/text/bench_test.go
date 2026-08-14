package text_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/levelfourab/syncplane/engine/text"
)

// Benchmarks for the per-write cost of a text document, broken down by stage
// so the terms can be attributed rather than lumped into one "merge" number.
//
// The shape being measured is one keystroke on an existing document:
//
//	local edit  = Len bounds check + visibleAt + tree insert
//	Encode      = the client's push() and the server's re-encode
//	Decode      = the server's read of the stored document
//	fragment    = what actually crosses the wire since delta state landed
//	Merge       = the whole server-side write closure's text work
//
// Sizes are code points of visible text; documents are built by typing so the
// canonical run structure matches what a real editing session produces.
//
// Compact and CouldReclaim are the exception: they are what the server's
// maintenance sweep pays per row it considers, not per write.

var benchSizes = []int{5000, 20000, 50000}

// benchDoc returns a document of n code points, typed linearly by one peer.
func benchDoc(peer string, n int) *text.Doc {
	d := text.New(peer)
	const chunk = 500
	body := strings.Repeat("a", chunk)
	for written := 0; written < n; written += chunk {
		size := chunk
		if remaining := n - written; remaining < size {
			size = remaining
			body = strings.Repeat("a", size)
		}
		if err := d.Insert(d.Len(), body); err != nil {
			panic(err)
		}
	}
	return d
}

func eachSize(b *testing.B, fn func(b *testing.B, n int)) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("%dcp", n), func(b *testing.B) { fn(b, n) })
	}
}

// BenchmarkEncode is the cost the client pays in push() and the server pays
// re-encoding the merged document — O(n log n), per write, unavoidable while
// the field is stored as whole-document bytes.
func BenchmarkEncode(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		d := benchDoc("w", n)
		b.SetBytes(int64(len(d.Encode())))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = d.Encode()
		}
	})
}

// BenchmarkDecode is the server's per-write read of the stored document, and
// the client's read on every remote change.
func BenchmarkDecode(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		state := benchDoc("w", n).Encode()
		b.SetBytes(int64(len(state)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := text.Decode(state, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkInsertOne is the local edit alone: the Len bounds check, the
// visibleAt lookup and the tree splice, with no encoding. Every insert marks
// the cached traversal dirty, so this also pays the next rebuild.
func BenchmarkInsertOne(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		d := benchDoc("w", n)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := d.Insert(n/2, "x"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkKeystrokeLocal is what the client actually runs per keystroke
// today: the edit, then a full encode for the PATCH value.
func BenchmarkKeystrokeLocal(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		d := benchDoc("w", n)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := d.Insert(n/2, "x"); err != nil {
				b.Fatal(err)
			}
			_ = d.Encode()
		}
	})
}

// BenchmarkMergeFullState is the pre-delta-state server cost: two whole
// documents decoded, unioned and re-encoded. This is the path the 102 table
// measured; clients no longer take it, but bootstrap, connectors, filtered
// entities and every fragment fallback still do.
func BenchmarkMergeFullState(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		base := benchDoc("w", n)
		stored := base.Encode()

		incomingDoc, err := text.Decode(stored, "c")
		if err != nil {
			b.Fatal(err)
		}
		if err := incomingDoc.Insert(n/2, "x"); err != nil {
			b.Fatal(err)
		}
		incoming := incomingDoc.Encode()

		b.SetBytes(int64(len(stored)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := text.Merge(stored, incoming); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMergeFragment is the server's write closure today: decode the
// stored document, apply a one-character fragment, re-encode. The fragment is
// O(edit); everything around it is O(document), which is the remaining cost
// 102 attributes to whole-document storage.
func BenchmarkMergeFragment(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		base := benchDoc("w", n)
		stored := base.Encode()

		next, err := text.Decode(stored, "c")
		if err != nil {
			b.Fatal(err)
		}
		if err := next.Insert(n/2, "x"); err != nil {
			b.Fatal(err)
		}
		frag, err := text.EncodeFragment(next, base)
		if err != nil {
			b.Fatal(err)
		}

		b.SetBytes(int64(len(stored)))
		b.ReportMetric(float64(len(frag)), "fragbytes")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := text.Merge(stored, frag); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkEncodeFragment is the client's wire-side diff: the confirmed row
// decoded and compared against the local document to produce the fragment.
func BenchmarkEncodeFragment(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		base := benchDoc("w", n)
		next, err := text.Decode(base.Encode(), "c")
		if err != nil {
			b.Fatal(err)
		}
		if err := next.Insert(n/2, "x"); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := text.EncodeFragment(next, base); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMarksByComments is mark resolution under the annotation pattern:
// one mark key per comment, each covering a few words (see
// client/src/text/annotations.ts). Resolution is O(document + covered), so the
// curve across comment counts should stay close to flat — a resolver that
// allocates or walks per key instead grows with keys × document, which is the
// shape this case exists to catch.
//
// It runs on the render path: a marked document re-resolves whenever its text
// changes, so this cost sits beside the keystroke cases above, not apart from
// them.
func BenchmarkMarksByComments(b *testing.B) {
	const n = 20000
	for _, comments := range []int{0, 20, 100, 400} {
		b.Run(fmt.Sprintf("%dcomments", comments), func(b *testing.B) {
			d := benchDoc("w", n)
			for i := 0; i < comments; i++ {
				start := (i * 37) % (n - 60)
				if _, err := d.Mark(start, start+50, fmt.Sprintf("comment:%d", i), true, text.ExpandNone); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := d.Marks(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarksWholeDocument is the other extreme: one key whose operations
// each span the entire document, so the covered stretch IS the document and
// nothing can be skipped. Superseded operations accumulate permanently —
// compaction purges tombstones, not mark operations — so this measures what a
// long formatting history costs.
func BenchmarkMarksWholeDocument(b *testing.B) {
	const n = 20000
	for _, ops := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("%dops", ops), func(b *testing.B) {
			d := benchDoc("w", n)
			for i := 0; i < ops; i++ {
				if _, err := d.Mark(0, n, "bold", true, text.ExpandNone); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := d.Marks(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPlainText is the read path every query evaluation, connector and
// change-feed consumer takes.
func BenchmarkPlainText(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		state := benchDoc("w", n).Encode()
		b.SetBytes(int64(len(state)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := text.PlainText(state); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCompact runs on the shape compaction exists for: a document typed
// and then deleted, so every tombstone in it is reclaimable and they form a
// chain of right children as deep as the run is long. That is the shape a purge
// re-testing every item per removal round degrades to quadratic on, so the
// numbers here are only meaningful across sizes.
func BenchmarkCompact(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		d := benchDoc("w", n)
		if err := d.Delete(0, n); err != nil {
			b.Fatal(err)
		}
		state := d.Encode()
		b.SetBytes(int64(len(state)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Compact mutates, so each iteration needs its own document; only
			// the compaction itself is timed.
			doc, err := text.Decode(state, "w")
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if items, _ := doc.Compact(); items != n {
				b.Fatalf("purged %d of %d items", items, n)
			}
		}
	})
}

// BenchmarkCouldReclaim is the gate the sweep opens a stored document with,
// read against BenchmarkDecode above: the difference is what a row it decides
// not to rewrite costs instead.
func BenchmarkCouldReclaim(b *testing.B) {
	eachSize(b, func(b *testing.B, n int) {
		state := benchDoc("w", n).Encode()
		b.SetBytes(int64(len(state)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			could, err := text.CouldReclaim(state, 1)
			if err != nil {
				b.Fatal(err)
			}
			if could {
				b.Fatal("a document with nothing deleted should not open")
			}
		}
	})
}
