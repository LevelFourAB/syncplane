package storagetest

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Leases registers the behavior specs for an adapter that implements
// lease.Provider. Registered separately from Conformance because the provider
// is optional; an adapter without one does not call this.
//
// newShared returns two adapter handles that share one lease space, plus a
// cleanup that closes what backs them. For a shared-database adapter that is
// two opens of the same database — the two-node deployment the leases exist
// for; for a single-process adapter it is the same store handed back twice,
// which is the whole sharing that adapter's model permits.
func Leases(name string, newShared func() (a, b storage.Adapter, cleanup func())) bool {
	return Describe("lease.Provider conformance: "+name, func() {
		var (
			ctx    context.Context
			la, lb lease.Locker
		)

		BeforeEach(func() {
			ctx = context.Background()
			a, b, cleanup := newShared()
			DeferCleanup(cleanup)
			pa, ok := a.(lease.Provider)
			Expect(ok).To(BeTrue(), "adapter must implement lease.Provider to run this suite")
			pb, ok := b.(lease.Provider)
			Expect(ok).To(BeTrue(), "adapter must implement lease.Provider to run this suite")
			la, lb = pa.Leases(), pb.Leases()
		})

		It("excludes a second holder across handles", func() {
			release, ok, err := la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(release).NotTo(BeNil())

			second, ok, err := lb.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
			Expect(second).To(BeNil())
		})

		It("frees the lease on release", func() {
			release, ok, err := la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			release()

			again, ok, err := lb.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			again()
		})

		It("treats a repeated release as a no-op", func() {
			release, ok, err := la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			release()

			// Reacquired elsewhere; the stale holder's second release must not
			// free the new holder's lease.
			held, ok, err := lb.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			release()

			_, ok, err = la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
			held()
		})

		It("keeps roles independent", func() {
			_, ok, err := la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			_, ok, err = lb.TryAcquire(ctx, "indexes", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
		})

		It("keeps workspaces independent", func() {
			_, ok, err := la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			_, ok, err = lb.TryAcquire(ctx, "gc", "ws2")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
		})
	})
}

// LeasesNamespaced registers the isolation specs for namespaced stores sharing
// one database: a lease taken through one namespace must not exclude another
// namespace's holder, or one app's sweeps would starve every other app's.
// newPair returns two adapters derived from the same underlying store plus a
// cleanup that closes it.
func LeasesNamespaced(name string, newPair func() (a, b storage.Adapter, cleanup func())) bool {
	return Describe("lease.Provider namespacing: "+name, func() {
		It("keeps namespaces apart", func() {
			ctx := context.Background()
			a, b, cleanup := newPair()
			DeferCleanup(cleanup)
			la := a.(lease.Provider).Leases()
			lb := b.(lease.Provider).Leases()

			_, ok, err := la.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			_, ok, err = lb.TryAcquire(ctx, "gc", "ws1")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
		})
	})
}
