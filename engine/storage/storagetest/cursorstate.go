package storagetest

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	"github.com/levelfourab/syncplane/engine/storage"
)

// CursorState registers the behavior specs for an adapter that implements
// cursorstate.Provider. Registered separately from Conformance because the
// provider is optional; an adapter without one does not call this.
// newAdapter is called per spec and must return a fresh, empty store; it is
// closed afterwards.
func CursorState(name string, newAdapter func() storage.Adapter) bool {
	return Describe("cursorstate.Provider conformance: "+name, func() {
		var (
			ctx   context.Context
			store cursorstate.Store
		)

		BeforeEach(func() {
			ctx = context.Background()
			adapter := newAdapter()
			DeferCleanup(func() { Expect(adapter.Close()).To(Succeed()) })
			provider, ok := storage.As[cursorstate.Provider](adapter)
			Expect(ok).To(BeTrue(), "adapter must implement cursorstate.Provider to run this suite")
			store = provider.CursorStateStore()
		})

		It("round-trips an entry", func() {
			ref := cursorstate.Ref{ID: 7, Gen: 42}
			Expect(store.Put(ctx, "ws1", ref, []byte("state-bytes"))).To(Succeed())
			got, err := store.Get(ctx, "ws1", ref)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("state-bytes")))
		})

		It("answers ErrNotFound for an entry never written", func() {
			_, err := store.Get(ctx, "ws1", cursorstate.Ref{ID: 1, Gen: 2})
			Expect(err).To(MatchError(cursorstate.ErrNotFound))
		})

		It("keeps entries apart by workspace", func() {
			ref := cursorstate.Ref{ID: 7, Gen: 42}
			Expect(store.Put(ctx, "ws1", ref, []byte("one"))).To(Succeed())
			Expect(store.Put(ctx, "ws2", ref, []byte("two"))).To(Succeed())
			got, err := store.Get(ctx, "ws1", ref)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("one")))
		})

		It("accepts a retried Put of the same entry", func() {
			ref := cursorstate.Ref{ID: 7, Gen: 42}
			Expect(store.Put(ctx, "ws1", ref, []byte("state-bytes"))).To(Succeed())
			Expect(store.Put(ctx, "ws1", ref, []byte("state-bytes"))).To(Succeed())
			got, err := store.Get(ctx, "ws1", ref)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("state-bytes")))
		})

		It("stores refs with the uint64 high bit set", func() {
			// Refs are random uint64s; an implementation narrowing them to a
			// signed column must round-trip the conversion.
			ref := cursorstate.Ref{ID: ^uint64(0) - 1, Gen: 1 << 63}
			Expect(store.Put(ctx, "ws1", ref, []byte("high"))).To(Succeed())
			got, err := store.Get(ctx, "ws1", ref)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("high")))
		})

		It("keeps a lineage's most recent entry as older generations pile up", func() {
			// Pruning superseded generations is licensed, but the entry just
			// written — the one live cursors reference — must survive its own
			// Put's pruning, and so must other lineages' entries.
			other := cursorstate.Ref{ID: 99, Gen: 1}
			Expect(store.Put(ctx, "ws1", other, []byte("other-lineage"))).To(Succeed())
			var last cursorstate.Ref
			for gen := uint64(1); gen <= 10; gen++ {
				last = cursorstate.Ref{ID: 7, Gen: gen}
				Expect(store.Put(ctx, "ws1", last, []byte{byte(gen)})).To(Succeed())
			}
			got, err := store.Get(ctx, "ws1", last)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte{10}))
			got, err = store.Get(ctx, "ws1", other)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("other-lineage")))
		})
	})
}
