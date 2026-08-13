package otelstore_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/otelstore"
)

// plainAdapter satisfies storage.Adapter by embedding; the capability specs
// never touch the data path, so only Namespace is implemented.
type plainAdapter struct{ storage.Adapter }

func (a *plainAdapter) Namespace(string) storage.Adapter { return a }

// providerAdapter is plainAdapter plus cursorstate.Provider — the shape of
// sqlitestore and pgstore, reduced to the capability under test.
type providerAdapter struct {
	plainAdapter
	cs cursorstate.Store
}

func (a *providerAdapter) Namespace(string) storage.Adapter { return a }

func (a *providerAdapter) CursorStateStore() cursorstate.Store { return a.cs }

type fakeCursorStore struct{ cursorstate.Store }

// leaseAdapter is plainAdapter plus lease.Provider.
type leaseAdapter struct {
	plainAdapter
	locker lease.Locker
}

func (a *leaseAdapter) Namespace(string) storage.Adapter { return a }

func (a *leaseAdapter) Leases() lease.Locker { return a.locker }

// replicaAdapter is plainAdapter plus storage.ReplicaProvider.
type replicaAdapter struct {
	plainAdapter
	rr storage.ReplicaReader
}

func (a *replicaAdapter) Namespace(string) storage.Adapter { return a }

func (a *replicaAdapter) ReplicaReader() storage.ReplicaReader { return a.rr }

type fakeReplicaReader struct{ storage.ReplicaReader }

var _ = Describe("Capability discovery", func() {
	It("exposes the inner adapter's cursorstate.Provider", func() {
		inner := &providerAdapter{cs: &fakeCursorStore{}}
		wrapped, err := otelstore.Wrap(inner)
		Expect(err).NotTo(HaveOccurred())

		p, ok := storage.As[cursorstate.Provider](wrapped)
		Expect(ok).To(BeTrue())
		Expect(p.CursorStateStore()).To(BeIdenticalTo(inner.cs))
	})

	It("keeps the capability visible on a namespaced view", func() {
		inner := &providerAdapter{cs: &fakeCursorStore{}}
		wrapped, err := otelstore.Wrap(inner)
		Expect(err).NotTo(HaveOccurred())

		p, ok := storage.As[cursorstate.Provider](wrapped.Namespace("app"))
		Expect(ok).To(BeTrue())
		Expect(p.CursorStateStore()).To(BeIdenticalTo(inner.cs))
	})

	It("walks a chain of wrappers", func() {
		inner := &providerAdapter{cs: &fakeCursorStore{}}
		once, err := otelstore.Wrap(inner)
		Expect(err).NotTo(HaveOccurred())
		twice, err := otelstore.Wrap(once)
		Expect(err).NotTo(HaveOccurred())

		p, ok := storage.As[cursorstate.Provider](twice)
		Expect(ok).To(BeTrue())
		Expect(p.CursorStateStore()).To(BeIdenticalTo(inner.cs))
	})

	It("exposes the inner adapter's lease.Provider", func() {
		inner := &leaseAdapter{locker: lease.NewLocal()}
		wrapped, err := otelstore.Wrap(inner)
		Expect(err).NotTo(HaveOccurred())

		p, ok := storage.As[lease.Provider](wrapped)
		Expect(ok).To(BeTrue())
		Expect(p.Leases()).To(BeIdenticalTo(inner.locker))
	})

	It("exposes the inner adapter's storage.ReplicaProvider", func() {
		inner := &replicaAdapter{rr: &fakeReplicaReader{}}
		wrapped, err := otelstore.Wrap(inner)
		Expect(err).NotTo(HaveOccurred())

		p, ok := storage.As[storage.ReplicaProvider](wrapped)
		Expect(ok).To(BeTrue())
		Expect(p.ReplicaReader()).To(BeIdenticalTo(inner.rr))
	})

	It("keeps storage.ReplicaProvider visible on a namespaced view and through a chain", func() {
		inner := &replicaAdapter{rr: &fakeReplicaReader{}}
		once, err := otelstore.Wrap(inner)
		Expect(err).NotTo(HaveOccurred())
		twice, err := otelstore.Wrap(once)
		Expect(err).NotTo(HaveOccurred())

		p, ok := storage.As[storage.ReplicaProvider](twice.Namespace("app"))
		Expect(ok).To(BeTrue())
		Expect(p.ReplicaReader()).To(BeIdenticalTo(inner.rr))
	})

	It("does not invent a capability the inner adapter lacks", func() {
		wrapped, err := otelstore.Wrap(&plainAdapter{})
		Expect(err).NotTo(HaveOccurred())

		_, ok := storage.As[cursorstate.Provider](wrapped)
		Expect(ok).To(BeFalse())

		_, ok = storage.As[storage.ReplicaProvider](wrapped)
		Expect(ok).To(BeFalse())
	})
})
