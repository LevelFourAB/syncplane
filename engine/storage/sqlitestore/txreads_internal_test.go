package sqlitestore

import (
	"context"
	"errors"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// A write transaction keeps what it read of index_state, index_scopes and
// posting_scopes (see sqliteTx.forgetTxReads). These specs change those rows
// in the same transaction, also in a savepoint that rolls back, and check
// that the next call sees the rows as they are.
var _ = Describe("metadata a write transaction keeps", func() {
	var store *Store
	ctx := context.Background()
	rolledBack := errors.New("roll back")

	BeforeEach(func() {
		var err error
		store, err = Open(filepath.Join(GinkgoT().TempDir(), "txreads.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	})

	It("sees an index state that the transaction sets", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			Expect(tx.IndexStates("widget")).To(BeEmpty())
			Expect(tx.SetIndexState("widget", "by_label", "h1", true)).To(Succeed())
			Expect(tx.IndexStates("widget")).To(Equal([]storage.IndexStatus{
				{Index: "by_label", SpecHash: "h1", Ready: true},
			}))
			return nil
		})).To(Succeed())
	})

	It("sees an index state as it was after a savepoint rolls back", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			Expect(tx.SetIndexState("widget", "by_label", "h1", true)).To(Succeed())
			Expect(tx.IndexStates("widget")).To(HaveLen(1))
			Expect(tx.InSavepoint(func() error {
				Expect(tx.SetIndexState("widget", "by_label", "", false)).To(Succeed())
				Expect(tx.SetIndexState("widget", "by_title", "h2", true)).To(Succeed())
				Expect(tx.IndexStates("widget")).To(HaveLen(2))
				return rolledBack
			})).To(MatchError(rolledBack))
			Expect(tx.IndexStates("widget")).To(Equal([]storage.IndexStatus{
				{Index: "by_label", SpecHash: "h1", Ready: true},
			}))
			return nil
		})).To(Succeed())
	})

	It("gives a new index scope after a savepoint that made one rolls back", func() {
		entry := []storage.IndexEntry{{Index: "by_label", Key: []byte("a\x00\x00r1\x00\x00")}}
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			Expect(tx.PutRow("widget", "r1", []byte("v"), 1, storage.Actor{})).To(Succeed())
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutIndexEntries("widget", "r1", entry)).To(Succeed())
				return rolledBack
			})).To(MatchError(rolledBack))
			return tx.PutIndexEntries("widget", "r1", entry)
		})).To(Succeed())
		var keys []string
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			return tx.ScanIndex("widget", "by_label", nil, nil, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
				keys = append(keys, rowKey)
				return nil
			})
		})).To(Succeed())
		Expect(keys).To(Equal([]string{"r1"}))
	})

	It("gives a new posting scope after a savepoint that made one rolls back", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutPostings("widget", "search", "r1", nil, [][]byte{[]byte("t")})).To(Succeed())
				return rolledBack
			})).To(MatchError(rolledBack))
			return tx.PutPostings("widget", "search", "r1", nil, [][]byte{[]byte("t")})
		})).To(Succeed())
		var keys []string
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			return tx.ScanPostings("widget", "search", []storage.IndexRange{{Lo: []byte("t"), Hi: []byte("t\x00")}}, nil, 0,
				func(rowKey string) error {
					keys = append(keys, rowKey)
					return nil
				})
		})).To(Succeed())
		Expect(keys).To(Equal([]string{"r1"}))
	})

	It("gives a new posting scope after the transaction clears the index", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			Expect(tx.PutPostings("widget", "search", "r1", nil, [][]byte{[]byte("t")})).To(Succeed())
			Expect(tx.ClearPostings("widget", "search")).To(Succeed())
			return tx.PutPostings("widget", "search", "r2", nil, [][]byte{[]byte("t")})
		})).To(Succeed())
		var keys []string
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			return tx.ScanPostings("widget", "search", []storage.IndexRange{{Lo: []byte("t"), Hi: []byte("t\x00")}}, nil, 0,
				func(rowKey string) error {
					keys = append(keys, rowKey)
					return nil
				})
		})).To(Succeed())
		Expect(keys).To(Equal([]string{"r2"}))
	})
})
