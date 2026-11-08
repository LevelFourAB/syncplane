package server

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
)

// A withheld cursor trades wire bytes now for re-delivered rows after a
// reopen, so the policy is only sound while the trade stays in profit. These
// pin that boundary directly: the black-box specs can show a cursor being
// skipped, but not the arithmetic that decides when skipping stops paying.
var _ = Describe("subscribe cursor checkpoints", func() {
	It("checkpoints every frame for a client that did not opt in", func() {
		sc := &subScope{emitter: &rowEmitter{}, cursorSent: true}
		Expect(sc.checkpointDue(cursorState{}, 1, 1<<20)).To(BeTrue())
	})

	It("checkpoints until the client holds a cursor at all", func() {
		// A scope opened without one has nothing to keep, so the first frame
		// must carry a cursor however little the rows on it weigh.
		sc := &subScope{emitter: &rowEmitter{}, sparseCursor: true}
		Expect(sc.checkpointDue(cursorState{}, 1, 1<<20)).To(BeTrue())
	})

	It("withholds while the rows sent since weigh less than the cursor", func() {
		sc := &subScope{emitter: &rowEmitter{}, sparseCursor: true, cursorSent: true, owedBytes: 100}
		Expect(sc.checkpointDue(cursorState{}, 100, 1000)).To(BeFalse())
	})

	It("checkpoints once the withheld rows reach the cursor's weight", func() {
		sc := &subScope{emitter: &rowEmitter{}, sparseCursor: true, cursorSent: true, owedBytes: 900}
		Expect(sc.checkpointDue(cursorState{}, 100, 1000)).To(BeTrue())
	})

	It("checkpoints a frame that moves the membership the client holds", func() {
		held := func(keys ...string) cursorState {
			return cursorState{window: &syncplanecursorv1.ClosureSet{Entries: []*syncplanecursorv1.ClosureSet_Entry{
				{Entity: "widget", Keys: keys},
			}}}
		}
		sc := &subScope{emitter: &rowEmitter{}, sparseCursor: true, cursorSent: true, sent: held("a", "b")}
		Expect(sc.checkpointDue(held("a", "b"), 100, 1000)).To(BeFalse())
		Expect(sc.checkpointDue(held("a", "c"), 100, 1000)).To(BeTrue())
	})

	It("checkpoints a frame of a delta resync", func() {
		sc := &subScope{emitter: &rowEmitter{}, sparseCursor: true, cursorSent: true}
		Expect(sc.checkpointDue(cursorState{resync: &syncplanecursorv1.ResyncState{}}, 100, 1000)).To(BeTrue())
		Expect(sc.checkpointDue(cursorState{resynced: true}, 100, 1000)).To(BeTrue())
	})

	It("checkpoints a frame that sends a row the scope can hide, created past the cursor", func() {
		sc := &subScope{emitter: &rowEmitter{newestHidable: 6}, sparseCursor: true, cursorSent: true, sent: cursorState{seq: 5}}
		Expect(sc.checkpointDue(cursorState{seq: 7}, 100, 1000)).To(BeTrue())
		sc.emitter.newestHidable = 5
		Expect(sc.checkpointDue(cursorState{seq: 7}, 100, 1000)).To(BeFalse())
	})

	It("checkpoints every frame while the cursor is smaller than a page", func() {
		// The unwindowed, non-following case: a cursor of a few dozen bytes
		// never covers the page it rides on, so the debt never accrues and the
		// stream behaves exactly as it did before sparse cursors existed.
		sc := &subScope{emitter: &rowEmitter{}, sparseCursor: true, cursorSent: true}
		for range 10 {
			Expect(sc.checkpointDue(cursorState{}, 200, 40)).To(BeTrue())
			sc.owedBytes = 0
		}
	})
})
