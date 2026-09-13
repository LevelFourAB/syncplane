package pgstore_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// Every scan streams through a server-side cursor whose FETCH is one prepared
// statement per connection. Two scans of different column shapes that reach the
// same connection under the same cursor name must not share that statement's
// cached description — which is what a windowed pull does: it scans the change
// log in one transaction and an entity by version in the next.
var _ = Describe("Postgres scan cursors", func() {
	ctx := context.Background()
	const ws = "ws"

	It("scans of different shapes on one connection do not poison each other's FETCH", func() {
		// One connection, so every transaction below reuses its statement cache.
		store := open(pgstore.WithMaxConns(1))
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			seq, err := tx.NextSeq()
			if err != nil {
				return err
			}
			return tx.PutRow("issue", "i1", []byte("v"), seq, storage.Actor{})
		})).To(Succeed())

		// The change log's column set (eight columns), then an entity scan's
		// (six) — each transaction's first cursor, so both are named alike.
		var seen int
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanChanges(0, "", "", 0, func(storage.Row) error {
				seen++
				return nil
			})
		})).To(Succeed())
		Expect(seen).To(Equal(1))

		seen = 0
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanEntityByVersionDesc("issue", 10, func(storage.Row) error {
				seen++
				return nil
			})
		})).To(Succeed())
		Expect(seen).To(Equal(1))
	})
})
