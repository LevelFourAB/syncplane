package server

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// A withheld cursor trades wire bytes now for re-delivered rows after a
// reopen, so the policy is only sound while the trade stays in profit. These
// pin that boundary directly: the black-box specs can show a cursor being
// skipped, but not the arithmetic that decides when skipping stops paying.
var _ = Describe("subscribe cursor checkpoints", func() {
	It("checkpoints every frame for a client that did not opt in", func() {
		sc := &subScope{cursorSent: true}
		Expect(sc.checkpointDue(1, 1<<20)).To(BeTrue())
	})

	It("checkpoints until the client holds a cursor at all", func() {
		// A scope opened without one has nothing to keep, so the first frame
		// must carry a cursor however little the rows on it weigh.
		sc := &subScope{sparseCursor: true}
		Expect(sc.checkpointDue(1, 1<<20)).To(BeTrue())
	})

	It("withholds while the rows sent since weigh less than the cursor", func() {
		sc := &subScope{sparseCursor: true, cursorSent: true, owedBytes: 100}
		Expect(sc.checkpointDue(100, 1000)).To(BeFalse())
	})

	It("checkpoints once the withheld rows reach the cursor's weight", func() {
		sc := &subScope{sparseCursor: true, cursorSent: true, owedBytes: 900}
		Expect(sc.checkpointDue(100, 1000)).To(BeTrue())
	})

	It("checkpoints every frame while the cursor is smaller than a page", func() {
		// The unwindowed, non-following case: a cursor of a few dozen bytes
		// never covers the page it rides on, so the debt never accrues and the
		// stream behaves exactly as it did before sparse cursors existed.
		sc := &subScope{sparseCursor: true, cursorSent: true}
		for range 10 {
			Expect(sc.checkpointDue(200, 40)).To(BeTrue())
			sc.owedBytes = 0
		}
	})
})
