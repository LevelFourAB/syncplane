package text_test

import (
	"errors"
	"math/rand"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/text"
)

// forked decodes d's current state as an independent replica under peer.
func forked(d *text.Doc, peer string) *text.Doc {
	GinkgoHelper()
	f, err := text.Decode(d.Encode(), peer)
	Expect(err).NotTo(HaveOccurred())
	return f
}

var _ = Describe("Compact", func() {
	It("drops a run that was typed and then deleted", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		Expect(d.Delete(5, 6)).To(Succeed())
		before := len(d.Encode())

		Expect(d.Compact()).To(Equal(6))
		Expect(d.PlainText()).To(Equal("hello"))
		Expect(d.Len()).To(Equal(5))
		Expect(len(d.Encode())).To(BeNumerically("<", before))

		// Still a well-formed, canonically encoded document.
		round, err := text.Decode(d.Encode(), "")
		Expect(err).NotTo(HaveOccurred())
		Expect(round.Encode()).To(Equal(d.Encode()))
		Expect(round.PlainText()).To(Equal("hello"))
	})

	It("keeps the version vector so purged ids stay accounted for", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "abc")).To(Succeed())
		Expect(d.Delete(0, 3)).To(Succeed())
		Expect(d.Compact()).To(Equal(3))
		Expect(d.VersionVector()).To(Equal(map[string]uint64{"alice": 3}))
		// A later edit still allocates past everything the document ever saw.
		Expect(d.Insert(0, "x")).To(Succeed())
		Expect(d.VersionVector()).To(Equal(map[string]uint64{"alice": 4}))
	})

	It("keeps deleted items that still position live text", func() {
		a := text.New("alice")
		Expect(a.Insert(0, "hello world")).To(Succeed())
		b := forked(a, "bob")
		// Bob types inside what Alice is about to delete.
		Expect(b.Insert(7, "X")).To(Succeed())
		Expect(a.Delete(5, 6)).To(Succeed())
		Expect(a.MergeDocs(b)).To(Succeed())
		Expect(a.PlainText()).To(Equal("helloX"))

		// The tail past Bob's item collapses; the three characters between it and
		// the live text stay, because they are what places it.
		Expect(a.Compact()).To(Equal(3))
		Expect(a.PlainText()).To(Equal("helloX"))
		_, err := text.Decode(a.Encode(), "")
		Expect(err).NotTo(HaveOccurred())
	})

	It("keeps a deleted item a mark anchors to", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		// ExpandBefore anchors the start to the PRECEDING item, which the delete
		// below tombstones — the case that used to lose the mark entirely.
		_, err := d.Mark(6, 11, "bold", true, text.ExpandBefore)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Delete(5, 1)).To(Succeed())

		Expect(d.Compact()).To(Equal(0))
		spans, err := d.Marks()
		Expect(err).NotTo(HaveOccurred())
		Expect(spans).To(HaveLen(1))
		Expect(spans[0].Key).To(Equal("bold"))
		Expect(d.PlainText()).To(Equal("helloworld"))
	})

	It("is idempotent and leaves an uncompactable document untouched", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "abc")).To(Succeed())
		before := d.Encode()
		Expect(d.Compact()).To(Equal(0))
		Expect(d.Encode()).To(Equal(before))

		// The middle of a typed run is pinned by the character after it; the
		// last one is a leaf and goes.
		Expect(d.Delete(1, 1)).To(Succeed())
		Expect(d.Compact()).To(Equal(0))
		// Deleting the run's last character frees it AND the one before it, in
		// the same pass — purging iterates to a fixpoint.
		Expect(d.Delete(1, 1)).To(Succeed())
		Expect(d.Compact()).To(Equal(2))
		once := d.Encode()
		Expect(d.Compact()).To(Equal(0))
		Expect(d.Encode()).To(Equal(once))
	})

	It("converges with a replica that compacted independently", func() {
		a := text.New("alice")
		Expect(a.Insert(0, "hello world")).To(Succeed())
		Expect(a.Delete(5, 6)).To(Succeed())
		b := forked(a, "bob")

		Expect(a.Compact()).To(Equal(6))
		Expect(b.Compact()).To(Equal(6))
		Expect(a.Encode()).To(Equal(b.Encode()))
	})
})

var _ = Describe("Version vector encoding", func() {
	It("round-trips canonically", func() {
		vv := map[string]uint64{"bob": 3, "alice": 9, "": 1}
		enc := text.EncodeVV(vv)
		back, err := text.DecodeVV(enc)
		Expect(err).NotTo(HaveOccurred())
		Expect(back).To(Equal(vv))
		Expect(text.EncodeVV(back)).To(Equal(enc), "encoding is not canonical")
	})

	It("decodes empty input as no floor", func() {
		vv, err := text.DecodeVV(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(vv).To(BeEmpty())
	})

	It("rejects trailing bytes", func() {
		enc := append(text.EncodeVV(map[string]uint64{"a": 1}), 0)
		_, err := text.DecodeVV(enc)
		Expect(err).To(MatchError("text: 1 trailing bytes"))
	})
})

// Mark operations are state like any other, so compaction may only drop the
// ones that can never win again — in this document or in any document reachable
// from it. See deadMarks for the two rules; these pin the line between them and
// what has to stay.
var _ = Describe("Compact of mark operations", func() {
	It("reclaims a mark and the unmark that turned it off", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello")).To(Succeed())
		_, err := d.Mark(0, 5, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, err = d.Unmark(0, 5, "bold", text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Marks()).To(BeEmpty())

		items, marks := d.Compact()
		Expect(items).To(Equal(0))
		Expect(marks).To(Equal(2))
		Expect(d.Marks()).To(BeEmpty())
		Expect(d.PlainText()).To(Equal("hello"))
	})

	It("reclaims a cancelled pending format and the item it pinned", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		Expect(d.Delete(5, 6)).To(Succeed())
		// A pending format dropped at the end of the surviving text anchors to
		// the first tombstone of the deleted run. The rest of the run collapses
		// behind it as usual; that one item is held back by the anchor alone.
		_, err := d.Mark(5, 5, "bold", true, text.ExpandBoth)
		Expect(err).NotTo(HaveOccurred())
		items, marks := d.Compact()
		Expect(items).To(Equal(5))
		Expect(marks).To(Equal(0))

		_, err = d.Unmark(5, 5, "bold", text.ExpandBoth)
		Expect(err).NotTo(HaveOccurred())
		items, marks = d.Compact()
		Expect(marks).To(Equal(2))
		Expect(items).To(Equal(1), "the last tombstone went with the ops that anchored it")
		Expect(d.PlainText()).To(Equal("hello"))
		Expect(d.Compact()).To(Equal(0), "nothing left to reclaim")
	})

	It("keeps a mark that still paints", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello")).To(Succeed())
		_, err := d.Mark(0, 5, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		items, marks := d.Compact()
		Expect(items).To(Equal(0))
		Expect(marks).To(Equal(0))
		Expect(d.Marks()).To(Equal([]text.Span{{Start: 0, End: 5, Key: "bold", Value: true}}))
	})

	It("keeps a point annotation, which covers nothing by design", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello")).To(Succeed())
		_, err := d.Mark(2, 2, "note:n1", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, marks := d.Compact()
		Expect(marks).To(Equal(0))
		Expect(d.Points()).To(Equal([]text.Point{{Index: 2, Key: "note:n1", Value: true}}))
	})

	It("keeps an unmark that still shadows a mark beneath it", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello")).To(Succeed())
		_, err := d.Mark(0, 5, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		_, err = d.Unmark(1, 4, "bold", text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		before, err := d.Marks()
		Expect(err).NotTo(HaveOccurred())

		_, marks := d.Compact()
		Expect(marks).To(Equal(0))
		Expect(d.Marks()).To(Equal(before))
	})

	// The rule is anchor identity, not "shadowed right now". These two cover
	// the same characters and the higher id wins every one of them, but their
	// boundaries expand differently, so one character typed at the start
	// boundary reveals the lower op. Dropping it would silently change the
	// document's future.
	It("keeps a shadowed mark whose boundaries expand differently", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "abcdef")).To(Succeed())
		_, err := d.Mark(2, 4, "bold", true, text.ExpandBoth)
		Expect(err).NotTo(HaveOccurred())
		_, err = d.Mark(2, 4, "bold", false, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Marks()).To(Equal([]text.Span{{Start: 2, End: 4, Key: "bold", Value: false}}))

		_, marks := d.Compact()
		Expect(marks).To(Equal(0))

		Expect(d.Insert(2, "X")).To(Succeed())
		Expect(d.Marks()).To(Equal([]text.Span{
			{Start: 2, End: 3, Key: "bold", Value: true},
			{Start: 3, End: 5, Key: "bold", Value: false},
		}))
	})

	It("keeps a removal that touches a mark it does not overlap yet", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "abcdef")).To(Succeed())
		_, err := d.Mark(0, 3, "bold", true, text.ExpandNone)
		Expect(err).NotTo(HaveOccurred())
		// [3,6) starts exactly where the mark ends: no overlap today, but a
		// character typed at the shared edge can fall inside both.
		_, err = d.Unmark(3, 6, "bold", text.ExpandBoth)
		Expect(err).NotTo(HaveOccurred())
		_, marks := d.Compact()
		Expect(marks).To(Equal(0))
	})

	It("leaves a document whose marks resolve identically", func() {
		d := text.New("alice")
		Expect(d.Insert(0, "hello world")).To(Succeed())
		for i := 0; i < 5; i++ {
			_, err := d.Mark(0, 5, "bold", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
		}
		_, err := d.Mark(6, 11, "italic", true, text.ExpandAfter)
		Expect(err).NotTo(HaveOccurred())
		before, err := d.Marks()
		Expect(err).NotTo(HaveOccurred())

		_, marks := d.Compact()
		Expect(marks).To(Equal(4), "only the newest of the identical bold ops survives")
		Expect(d.Marks()).To(Equal(before))

		// Still canonical, and still the document both languages would encode.
		enc := d.Encode()
		round, err := text.Decode(enc, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(round.Encode()).To(Equal(enc))
	})
})

var _ = Describe("MergeWithFloor", func() {
	// compacted returns a document whose deleted tail has been purged, plus the
	// floor recorded for it and a replica still holding the pre-compaction state.
	compacted := func() (server *text.Doc, floor map[string]uint64, stale *text.Doc) {
		server = text.New("alice")
		Expect(server.Insert(0, "hello world")).To(Succeed())
		Expect(server.Delete(5, 6)).To(Succeed())
		stale = forked(server, "bob")
		Expect(server.Compact()).To(Equal(6))
		return server, server.VersionVector(), stale
	}

	It("behaves exactly like Merge when there is no floor", func() {
		a := text.New("alice")
		Expect(a.Insert(0, "abc")).To(Succeed())
		b := forked(a, "bob")
		Expect(b.Insert(3, "def")).To(Succeed())
		withFloor, err := text.MergeWithFloor(a.Encode(), b.Encode(), nil)
		Expect(err).NotTo(HaveOccurred())
		plain, err := text.Merge(a.Encode(), b.Encode())
		Expect(err).NotTo(HaveOccurred())
		Expect(withFloor).To(Equal(plain))
	})

	It("skips purged tombstones the sender still carries", func() {
		server, floor, stale := compacted()
		// The stale replica edits inside the part that survived; its state still
		// holds the six tombstones the server dropped. (Index 2 lands between two
		// surviving items — an edit at the very end would instead attach to the
		// first purged item, which is the stale case exercised below.)
		Expect(stale.Insert(2, "!")).To(Succeed())

		out, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
		Expect(err).NotTo(HaveOccurred())
		merged, err := text.Decode(out, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(merged.PlainText()).To(Equal("he!llo"))
		// The tombstones did not come back.
		Expect(merged.Compact()).To(Equal(0))

		// Re-applying the same write is a no-op, and a second write from the
		// same stale replica still lands.
		again, err := text.MergeWithFloor(out, stale.Encode(), floor)
		Expect(err).NotTo(HaveOccurred())
		Expect(again).To(Equal(out))
	})

	It("skips purged tombstones arriving as a fragment", func() {
		server, floor, stale := compacted()
		base := forked(stale, "")
		Expect(stale.Insert(2, "!")).To(Succeed())
		frag, err := text.EncodeFragment(stale, base)
		Expect(err).NotTo(HaveOccurred())

		out, err := text.MergeWithFloor(server.Encode(), frag, floor)
		Expect(err).NotTo(HaveOccurred())
		viaFull, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(viaFull), "fragment apply diverged from full merge")
	})

	// A reclaimed mark op has to stay reclaimed. Merge is a union, so without
	// the floor deciding this the next write from any replica that still holds
	// the op puts it straight back, and the reclaim never sticks.
	Describe("purged mark operations", func() {
		// toggled returns a document whose bold-on/bold-off pair has been
		// reclaimed, plus its floor and a replica still holding both ops.
		toggled := func() (server *text.Doc, floor map[string]uint64, stale *text.Doc) {
			server = text.New("alice")
			Expect(server.Insert(0, "hello")).To(Succeed())
			_, err := server.Mark(0, 5, "bold", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			_, err = server.Unmark(0, 5, "bold", text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
			stale = forked(server, "bob")
			items, marks := server.Compact()
			Expect(items).To(Equal(0))
			Expect(marks).To(Equal(2))
			return server, server.VersionVector(), stale
		}

		It("skips the ops a sender still carries", func() {
			server, floor, stale := toggled()
			Expect(stale.Insert(5, "!")).To(Succeed())

			out, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
			Expect(err).NotTo(HaveOccurred())
			merged, err := text.Decode(out, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(merged.PlainText()).To(Equal("hello!"))
			Expect(merged.Marks()).To(BeEmpty())
			_, marks := merged.Compact()
			Expect(marks).To(Equal(0), "the reclaimed ops came back")

			// Idempotent, like every other merge.
			again, err := text.MergeWithFloor(out, stale.Encode(), floor)
			Expect(err).NotTo(HaveOccurred())
			Expect(again).To(Equal(out))
		})

		It("skips them when they arrive as a fragment", func() {
			server, floor, stale := toggled()
			base := forked(stale, "")
			Expect(stale.Insert(5, "!")).To(Succeed())
			frag, err := text.EncodeFragment(stale, base)
			Expect(err).NotTo(HaveOccurred())

			out, err := text.MergeWithFloor(server.Encode(), frag, floor)
			Expect(err).NotTo(HaveOccurred())
			viaFull, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal(viaFull), "fragment apply diverged from full merge")
		})

		// The op and the items its boundaries named can be reclaimed in the
		// same pass, which would make the anchor check below report the write
		// as stale if the op were not skipped first.
		It("skips an op whose anchor items were reclaimed with it", func() {
			server := text.New("alice")
			Expect(server.Insert(0, "hello world")).To(Succeed())
			Expect(server.Delete(5, 6)).To(Succeed())
			_, err := server.Mark(5, 5, "bold", true, text.ExpandBoth)
			Expect(err).NotTo(HaveOccurred())
			_, err = server.Unmark(5, 5, "bold", text.ExpandBoth)
			Expect(err).NotTo(HaveOccurred())
			stale := forked(server, "bob")
			items, marks := server.Compact()
			Expect(items).To(Equal(6))
			Expect(marks).To(Equal(2))
			floor := server.VersionVector()

			// Bob edits inside the surviving text, so nothing but the reclaimed
			// state is in play.
			Expect(stale.Insert(2, "!")).To(Succeed())
			out, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
			Expect(err).NotTo(HaveOccurred())
			merged, err := text.Decode(out, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(merged.PlainText()).To(Equal("he!llo"))
			Expect(merged.Marks()).To(BeEmpty())
		})

		It("still carries an op the floor does not cover", func() {
			server, floor, stale := toggled()
			// A mark bob mints AFTER forking is new state, not reclaimed state:
			// its counter is past the floor.
			_, err := stale.Mark(0, 5, "italic", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())

			out, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
			Expect(err).NotTo(HaveOccurred())
			merged, err := text.Decode(out, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(merged.Marks()).To(Equal([]text.Span{
				{Start: 0, End: 5, Key: "italic", Value: true},
			}))
		})
	})

	It("rejects a sender that still holds a purged item as live", func() {
		server := text.New("alice")
		Expect(server.Insert(0, "hello world")).To(Succeed())
		// The stale replica forks BEFORE the delete, so it holds the tail live.
		stale := forked(server, "bob")
		Expect(server.Delete(5, 6)).To(Succeed())
		Expect(server.Compact()).To(Equal(6))
		floor := server.VersionVector()

		_, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
		Expect(err).To(MatchError(text.ErrStateStale))
		Expect(err.Error()).To(ContainSubstring("was compacted away"))
	})

	It("rejects new text anchored to purged text", func() {
		server := text.New("alice")
		Expect(server.Insert(0, "hello world")).To(Succeed())
		// Bob types inside the tail before deleting it himself, so both sides
		// agree the tail is gone but Bob's item hangs off one of its characters.
		stale := forked(server, "bob")
		Expect(stale.Insert(8, "X")).To(Succeed()) // "hello woXrld"
		Expect(stale.Delete(5, 3)).To(Succeed())   // "helloXrld"
		Expect(stale.Delete(6, 3)).To(Succeed())   // "helloX"

		Expect(server.Delete(5, 6)).To(Succeed())
		Expect(server.Compact()).To(Equal(6))
		floor := server.VersionVector()

		_, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
		Expect(err).To(MatchError(text.ErrStateStale))
		Expect(err.Error()).To(ContainSubstring("which was compacted away"))
	})

	It("rejects an edit appended onto a purged item", func() {
		server, floor, stale := compacted()
		// Appending at the end attaches to the first item of the deleted run,
		// which the server purged — the everyday shape of a replica that has not
		// re-read the row since compaction.
		Expect(stale.Insert(stale.Len(), "!")).To(Succeed())
		_, err := text.MergeWithFloor(server.Encode(), stale.Encode(), floor)
		Expect(err).To(MatchError(text.ErrStateStale))
	})

	It("keeps ErrFragmentUnusable distinct from ErrStateStale", func() {
		server, floor, _ := compacted()
		_, err := text.MergeWithFloor(server.Encode(), []byte{0x02, 0xff}, floor)
		Expect(err).To(MatchError(text.ErrFragmentUnusable))
		Expect(errors.Is(err, text.ErrStateStale)).To(BeFalse())
	})

	// Compaction must never produce a third outcome: a write from a replica
	// holding pre-compaction state either applies (and then agrees with what the
	// uncompacted server would have stored) or is refused as stale. Anything
	// else — a merge conflict, an undecodable row, resurrected text — is a bug.
	It("either applies or reports stale, over random histories", func() {
		rng := rand.New(rand.NewSource(7))
		applied, refused := 0, 0
		for round := 0; round < 200; round++ {
			uncompacted := text.New("alice")
			for i := 0; i < 6; i++ {
				Expect(uncompacted.Insert(rng.Intn(uncompacted.Len()+1),
					strings.Repeat(string(rune('a'+rng.Intn(26))), 1+rng.Intn(3)))).To(Succeed())
			}
			if uncompacted.Len() > 2 {
				at := rng.Intn(uncompacted.Len() - 1)
				Expect(uncompacted.Delete(at, 1+rng.Intn(uncompacted.Len()-at-1))).To(Succeed())
			}
			replica := forked(uncompacted, "bob")
			Expect(replica.Insert(rng.Intn(replica.Len()+1), "!")).To(Succeed())

			server := forked(uncompacted, "")
			want := server.PlainText()
			server.Compact()
			Expect(server.PlainText()).To(Equal(want), "compaction changed the visible text")

			out, err := text.MergeWithFloor(server.Encode(), replica.Encode(), server.VersionVector())
			if err != nil {
				Expect(err).To(MatchError(text.ErrStateStale), "round %d", round)
				refused++
				continue
			}
			applied++
			merged, err := text.Decode(out, "")
			Expect(err).NotTo(HaveOccurred(), "round %d", round)
			// Same text the uncompacted server would hold, and nothing purged
			// came back.
			full, err := text.Merge(uncompacted.Encode(), replica.Encode())
			Expect(err).NotTo(HaveOccurred())
			fullDoc, err := text.Decode(full, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(merged.PlainText()).To(Equal(fullDoc.PlainText()), "round %d", round)
		}
		// The two branches are both worth having been exercised.
		Expect(applied).To(BeNumerically(">", 0))
		Expect(refused).To(BeNumerically(">", 0))
	})
})
