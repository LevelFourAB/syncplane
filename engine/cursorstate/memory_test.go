package cursorstate_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/cursorstate"
)

func TestCursorState(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cursor State Suite")
}

var _ = Describe("Memory", func() {
	ctx := context.Background()

	It("round-trips an entry", func() {
		m := cursorstate.NewMemory(0)
		ref := cursorstate.Ref{ID: 7, Gen: 42}
		Expect(m.Put(ctx, "ws1", ref, []byte("state"))).To(Succeed())
		got, err := m.Get(ctx, "ws1", ref)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]byte("state")))
	})

	It("answers ErrNotFound for an entry never written", func() {
		m := cursorstate.NewMemory(0)
		_, err := m.Get(ctx, "ws1", cursorstate.Ref{ID: 1, Gen: 2})
		Expect(err).To(MatchError(cursorstate.ErrNotFound))
	})

	It("keeps entries apart by workspace and by ref", func() {
		m := cursorstate.NewMemory(0)
		Expect(m.Put(ctx, "ws1", cursorstate.Ref{ID: 1, Gen: 1}, []byte("a"))).To(Succeed())
		Expect(m.Put(ctx, "ws2", cursorstate.Ref{ID: 1, Gen: 1}, []byte("b"))).To(Succeed())
		Expect(m.Put(ctx, "ws1", cursorstate.Ref{ID: 1, Gen: 2}, []byte("c"))).To(Succeed())
		got, err := m.Get(ctx, "ws1", cursorstate.Ref{ID: 1, Gen: 1})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]byte("a")))
	})

	It("evicts the least recently used entries when the budget fills", func() {
		m := cursorstate.NewMemory(30)
		a, b, c := cursorstate.Ref{ID: 1, Gen: 1}, cursorstate.Ref{ID: 2, Gen: 1}, cursorstate.Ref{ID: 3, Gen: 1}
		Expect(m.Put(ctx, "ws", a, make([]byte, 10))).To(Succeed())
		Expect(m.Put(ctx, "ws", b, make([]byte, 10))).To(Succeed())
		// Touch a so b is the eviction candidate.
		_, err := m.Get(ctx, "ws", a)
		Expect(err).NotTo(HaveOccurred())
		Expect(m.Put(ctx, "ws", c, make([]byte, 20))).To(Succeed())
		_, err = m.Get(ctx, "ws", a)
		Expect(err).NotTo(HaveOccurred())
		_, err = m.Get(ctx, "ws", b)
		Expect(err).To(MatchError(cursorstate.ErrNotFound))
	})

	It("refuses an entry larger than the whole budget", func() {
		m := cursorstate.NewMemory(10)
		Expect(m.Put(ctx, "ws", cursorstate.Ref{ID: 1, Gen: 1}, make([]byte, 11))).NotTo(Succeed())
	})

	It("accepts a retried Put without double-counting the budget", func() {
		m := cursorstate.NewMemory(25)
		ref := cursorstate.Ref{ID: 1, Gen: 1}
		Expect(m.Put(ctx, "ws", ref, make([]byte, 10))).To(Succeed())
		Expect(m.Put(ctx, "ws", ref, make([]byte, 10))).To(Succeed())
		// A second distinct entry still fits, which it would not if the retry
		// had been charged twice.
		Expect(m.Put(ctx, "ws", cursorstate.Ref{ID: 2, Gen: 1}, make([]byte, 10))).To(Succeed())
		_, err := m.Get(ctx, "ws", ref)
		Expect(err).NotTo(HaveOccurred())
	})
})
