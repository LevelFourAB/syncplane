package storagetest

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Replica registers the behavior specs for an adapter that implements
// storage.ReplicaProvider. Registered separately from Conformance because the
// capability is optional; an adapter without one does not call this.
// newAdapter is called per spec and must return a fresh, empty store with a
// replica configured — pointing the replica at the primary itself is enough,
// since the specs pin routing and the read contract, not lag. It is closed
// afterwards.
func Replica(name string, newAdapter func() storage.Adapter) bool {
	return Describe("storage.ReplicaProvider conformance: "+name, func() {
		var (
			ctx     context.Context
			adapter storage.Adapter
			reader  storage.ReplicaReader
		)

		routes := []storage.ReadRoute{
			storage.ReadRouteReplica,
			storage.ReadRouteFallbackStale,
			storage.ReadRouteFallbackError,
		}

		BeforeEach(func() {
			ctx = context.Background()
			adapter = newAdapter()
			DeferCleanup(func() { Expect(adapter.Close()).To(Succeed()) })
			p, ok := storage.As[storage.ReplicaProvider](adapter)
			Expect(ok).To(BeTrue(), "adapter must implement storage.ReplicaProvider to run this suite")
			reader = p.ReplicaReader()
			Expect(reader).NotTo(BeNil(), "the suite needs a replica configured")
		})

		// put writes value under widget/key through a and returns the sequence
		// the write took — the floor a caller who saw that write's ack holds.
		put := func(a storage.Adapter, ws, key, value string) uint64 {
			GinkgoHelper()
			var seq uint64
			Expect(a.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				var err error
				if seq, err = tx.NextSeq(); err != nil {
					return err
				}
				return tx.PutRow("widget", key, []byte(value), seq, storage.Actor{})
			})).To(Succeed())
			return seq
		}

		It("serves a read with no floor", func() {
			put(adapter, "ws1", "k1", "v1")
			route, err := reader.WithReadTxMinSeq(ctx, "ws1", 0, func(tx storage.ReadTx) error {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1")))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(route).To(BeElementOf(routes))
		})

		It("serves a read floored at a sequence the store has reached", func() {
			seq := put(adapter, "ws1", "k1", "v1")
			route, err := reader.WithReadTxMinSeq(ctx, "ws1", seq, func(tx storage.ReadTx) error {
				got, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeNumerically(">=", seq), "the snapshot must satisfy the floor")
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1")))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(route).To(BeElementOf(routes))
		})

		It("falls back for a floor above the head and still answers correctly", func() {
			// No snapshot anywhere satisfies the floor, so the adapter must
			// route to the primary — minSeq is a routing floor, not a bound on
			// what fn observes.
			seq := put(adapter, "ws1", "k1", "v1")
			route, err := reader.WithReadTxMinSeq(ctx, "ws1", seq+1, func(tx storage.ReadTx) error {
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v1")))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(route).To(Equal(storage.ReadRouteFallbackStale))
		})

		It("anchors a read at the primary's current sequence", func() {
			put(adapter, "ws1", "k1", "old")
			seq := put(adapter, "ws1", "k1", "new")
			route, err := reader.WithReadTxAnchored(ctx, "ws1", func(tx storage.ReadTx) error {
				got, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeNumerically(">=", seq))
				row, err := tx.GetRow("widget", "k1")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("new")), "an anchored read is read-your-writes")
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(route).To(BeElementOf(routes))
		})

		It("invokes fn at most once, even when it falls back", func() {
			// Streaming callers cannot re-run: everything the routing decision
			// does must complete before fn's first byte.
			seq := put(adapter, "ws1", "k1", "v1")
			boom := errors.New("boom")
			for _, minSeq := range []uint64{0, seq + 1} {
				calls := 0
				route, err := reader.WithReadTxMinSeq(ctx, "ws1", minSeq, func(tx storage.ReadTx) error {
					calls++
					return boom
				})
				Expect(err).To(MatchError(boom), "fn's error must surface")
				Expect(calls).To(Equal(1), "fn must run exactly once (minSeq %d)", minSeq)
				Expect(route).To(BeElementOf(routes))
			}
		})

		It("hands fn a transaction that cannot be widened to a write", func() {
			route, err := reader.WithReadTxMinSeq(ctx, "ws1", 0, func(tx storage.ReadTx) error {
				_, ok := tx.(storage.WriteTx)
				Expect(ok).To(BeFalse(), "a replica read tx must not satisfy storage.WriteTx")
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(route).To(BeElementOf(routes))
		})

	})
}
