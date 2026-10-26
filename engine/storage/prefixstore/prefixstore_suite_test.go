package prefixstore_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/prefixstore"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/storage/storagetest"
)

func TestPrefixstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Prefix Store Suite")
}

// owning gives a wrapped adapter a Close that reaches the store underneath, so
// a suite that closes what it is handed closes the database as well. Unwrap
// keeps the wrapper's own capabilities resolvable through storage.As.
type owning struct {
	storage.Adapter
	inner storage.Adapter
}

func (o owning) Close() error            { return o.inner.Close() }
func (o owning) Unwrap() storage.Adapter { return o.Adapter }

// A prefixed view must satisfy the whole adapter contract: every spec below
// the wrapper sees only its own partition, and nothing about the contract may
// change because the ids it stores are longer than the ones it was given.
var _ = storagetest.Conformance("prefixstore over sqlite", func() storage.Adapter {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return owning{Adapter: prefixstore.Wrap(store, "app/"), inner: store}
})

var _ = storagetest.CursorState("prefixstore over sqlite", func() storage.Adapter {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return owning{Adapter: prefixstore.Wrap(store, "app/"), inner: store}
})

var _ = Describe("Prefixed adapters over one store", func() {
	var (
		ctx  context.Context
		root storage.Adapter
		a, b storage.Adapter
	)

	BeforeEach(func() {
		ctx = context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		root = store
		a, b = prefixstore.Wrap(store, "a/"), prefixstore.Wrap(store, "b/")
	})

	write := func(s storage.Adapter, ws string, fn func(tx storage.WriteTx)) {
		GinkgoHelper()
		Expect(s.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			fn(tx)
			return nil
		})).To(Succeed())
	}

	put := func(s storage.Adapter, ws, key, value string) {
		GinkgoHelper()
		write(s, ws, func(tx storage.WriteTx) {
			seq, err := tx.NextSeq()
			Expect(err).NotTo(HaveOccurred())
			Expect(tx.PutRow("widget", key, []byte(value), seq, storage.Actor{})).To(Succeed())
		})
	}

	It("keeps rows of the same workspace id apart", func() {
		put(a, "ws1", "k1", "from-a")
		put(b, "ws1", "k1", "from-b")

		Expect(b.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("from-b")))
			return nil
		})).To(Succeed())
	})

	It("keeps sequences apart", func() {
		put(a, "ws1", "k1", "v")
		put(a, "ws1", "k2", "v")
		put(b, "ws1", "k1", "v")

		Expect(b.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			Expect(tx.CurrentSeq()).To(BeEquivalentTo(1), "each partition counts its own writes")
			return nil
		})).To(Succeed())
	})

	It("keeps client watermarks apart", func() {
		write(a, "ws1", func(tx storage.WriteTx) {
			Expect(tx.SetClientLastMutationID("c1", 7)).To(Succeed())
		})

		Expect(b.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			Expect(tx.ClientLastMutationID("c1")).To(BeEquivalentTo(0))
			return nil
		})).To(Succeed())
	})

	It("lists only its own workspaces, with the prefix stripped", func() {
		put(a, "ws1", "k", "v")
		put(a, "ws2", "k", "v")
		put(b, "ws3", "k", "v")

		Expect(a.ListWorkspaceIDs(ctx, storage.Range{})).To(ConsistOf("ws1", "ws2"))
		Expect(b.ListWorkspaceIDs(ctx, storage.Range{})).To(ConsistOf("ws3"))
		Expect(root.ListWorkspaceIDs(ctx, storage.Range{})).To(
			ConsistOf("a/ws1", "a/ws2", "b/ws3"), "the store underneath holds one flat space")
	})

	It("narrows the caller's own range to the partition", func() {
		put(a, "ws1", "k", "v")
		put(a, "ws2", "k", "v")

		Expect(a.ListWorkspaceIDs(ctx, storage.Range{After: "ws1"})).To(ConsistOf("ws2"))
		Expect(a.ListWorkspaceIDs(ctx, storage.Range{Before: "ws2"})).To(ConsistOf("ws1"))
	})

	It("bounds the work queries to the partition", func() {
		// A live row and no sequence: an allocation leaves a lineage record,
		// which the cutoff below would collect in a as well.
		write(a, "ws1", func(tx storage.WriteTx) {
			Expect(tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})).To(Succeed())
		})
		put(b, "ws1", "k", "v")
		write(b, "ws1", func(tx storage.WriteTx) {
			Expect(tx.DeleteRow("widget", "k", 2, storage.Actor{})).To(Succeed())
		})
		cutoffs := storage.GCCutoffs{Tombstones: time.Now().Add(time.Hour)}

		Expect(a.ListCollectableWorkspaceIDs(ctx, cutoffs, storage.Range{}, 10)).To(BeEmpty())
		Expect(b.ListCollectableWorkspaceIDs(ctx, cutoffs, storage.Range{}, 10)).To(ConsistOf("ws1"))
		Expect(a.ListStaleIndexWorkspaceIDs(ctx, "mark", storage.Range{}, 10)).To(ConsistOf("ws1"))
		Expect(a.ListPendingWorkspaceIDs(ctx, "indexer", storage.Range{}, 10)).To(ConsistOf("ws1"))
	})

	It("leases out of separate spaces", func() {
		la, ok := storage.As[lease.Provider](a)
		Expect(ok).To(BeTrue())
		lb, ok := storage.As[lease.Provider](b)
		Expect(ok).To(BeTrue())

		_, taken, err := la.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(taken).To(BeTrue())

		_, taken, err = lb.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(taken).To(BeTrue(), "the other partition's sweep must not be starved")

		_, taken, err = la.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(taken).To(BeFalse(), "within one partition the lease still excludes")
	})

	It("keeps cursor state apart", func() {
		pa, ok := storage.As[cursorstate.Provider](a)
		Expect(ok).To(BeTrue())
		pb, ok := storage.As[cursorstate.Provider](b)
		Expect(ok).To(BeTrue())

		ref := cursorstate.Ref{ID: 7, Gen: 42}
		Expect(pa.CursorStateStore().Put(ctx, "ws1", ref, []byte("from-a"))).To(Succeed())
		_, err := pb.CursorStateStore().Get(ctx, "ws1", ref)
		Expect(err).To(MatchError(cursorstate.ErrNotFound))
	})

	It("hides a capability it cannot partition", func() {
		// The catalog belongs to no workspace, so a partition has no view of
		// it to hand out. Resolving one here would give an app the whole
		// deployment's catalog under the name of its own.
		_, ok := storage.As[storage.Catalog](a)
		Expect(ok).To(BeFalse())
		_, ok = storage.As[storage.Catalog](root)
		Expect(ok).To(BeTrue())
	})

	It("returns the adapter unchanged for an empty prefix", func() {
		Expect(prefixstore.Wrap(root, "")).To(BeIdenticalTo(root))
	})
})
