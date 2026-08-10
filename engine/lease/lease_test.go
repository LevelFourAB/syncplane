package lease_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/lease"
)

var _ = Describe("Local locker", func() {
	var (
		ctx context.Context
		l   *lease.Local
	)

	BeforeEach(func() {
		ctx = context.Background()
		l = lease.NewLocal()
	})

	It("excludes a second holder of the same (role, workspace)", func() {
		release, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(release).NotTo(BeNil())

		second, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		Expect(second).To(BeNil())
	})

	It("frees the lease on release", func() {
		release, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		release()

		again, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		again()
	})

	It("treats a repeated release as a no-op", func() {
		release, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		release()

		// Reacquired by someone else; the stale holder's second release must
		// not free their lease out from under them.
		_, ok, err = l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		release()

		_, ok, err = l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
	})

	It("keeps roles independent", func() {
		_, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		_, ok, err = l.TryAcquire(ctx, "indexes", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
	})

	It("keeps workspaces independent", func() {
		_, ok, err := l.TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		_, ok, err = l.TryAcquire(ctx, "gc", "ws2")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
	})

	It("does not conflate (role, workspace) pairs whose concatenation collides", func() {
		_, ok, err := l.TryAcquire(ctx, "a", "bc")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		_, ok, err = l.TryAcquire(ctx, "ab", "c")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
	})
})
