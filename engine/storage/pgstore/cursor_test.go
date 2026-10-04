package pgstore_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// A scan larger than one batch streams through a server-side cursor whose
// FETCH is one prepared statement per connection. Two scans of different column
// shapes that reach the same connection under the same cursor name must not
// share that statement's cached description — which is what a windowed pull
// does: it scans the change log in one transaction and an entity by version in
// the next.
var _ = Describe("Postgres scan cursors", func() {
	ctx := context.Background()
	const ws = "ws"
	// More rows than one FETCH holds, so the cursor path has to page.
	const scanRows = 300

	// seed writes n rows of entity issue, keyed so key order is write order.
	seed := func(store storage.Adapter, n int) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := 0; i < n; i++ {
				seq, err := tx.NextSeq()
				if err != nil {
					return err
				}
				if err := tx.PutRow("issue", fmt.Sprintf("i%04d", i), []byte("v"), seq,
					storage.Actor{}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}

	It("scans of different shapes on one connection do not poison each other's FETCH", func() {
		// One connection, so every transaction below reuses its statement cache.
		store := open(pgstore.WithMaxConns(1))
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		seed(store, scanRows)

		// The change log's column set (eight columns), then an entity scan's
		// (six) — each transaction's first cursor, so both are named alike.
		var seen int
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanChanges(0, "", "", 0, func(storage.Row) error {
				seen++
				return nil
			})
		})).To(Succeed())
		Expect(seen).To(Equal(scanRows))

		seen = 0
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanEntityByVersionDesc("issue", 0, func(storage.Row) error {
				seen++
				return nil
			})
		})).To(Succeed())
		Expect(seen).To(Equal(scanRows))
	})

	// An unbounded scan reads its first batch directly and hands the rest to a
	// cursor that skips what that read returned. The sizes sit on each side of
	// the batch boundaries, where a skip one row off would repeat or drop a row.
	DescribeTable("an unbounded scan returns every row once, in key order",
		func(n, limit int) {
			store := open()
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			seed(store, n)

			want := make([]string, n)
			for i := range want {
				want[i] = fmt.Sprintf("i%04d", i)
			}
			got := []string{}
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				return tx.ScanEntityAfter("issue", "", limit, func(row storage.Row) error {
					got = append(got, row.Key)
					return nil
				})
			})).To(Succeed())
			Expect(got).To(Equal(want))
		},
		Entry("no rows", 0, 0),
		Entry("one row", 1, 0),
		Entry("exactly one batch", 256, 0),
		Entry("one row past a batch", 257, 0),
		Entry("exactly two batches", 512, 0),
		Entry("one row past two batches", 513, 0),
		Entry("one row past a batch, limited past the direct-scan cap", 257, 1<<20),
	)

	It("a write transaction's scan does not return rows its own callback writes", func() {
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		seed(store, scanRows)

		// The contract does not ask for this, because the engine writes after a
		// scan returns. It holds because the cursor is declared before the first
		// row reaches the callback, which also keeps a write from moving the rows
		// the cursor skips. Each callback writes a row that sorts after every
		// seeded one, so a cursor declared later would return those rows too.
		var seen int
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.ScanEntity("issue", func(row storage.Row) error {
				Expect(row.Key).To(HavePrefix("i"))
				seen++
				seq, err := tx.NextSeq()
				if err != nil {
					return err
				}
				return tx.PutRow("issue", "z"+row.Key, []byte("v"), seq, storage.Actor{})
			})
		})).To(Succeed())
		Expect(seen).To(Equal(scanRows))
	})

	// A bounded scan reads its rows in one statement and an unbounded one pages
	// through a cursor, and both owe the callback a connection it can read on.
	// One connection is what makes a result set left open fail loudly rather
	// than quietly borrow a second one from the pool.
	DescribeTable("a scan callback can read the transaction it is scanning",
		func(limit int) {
			store := open(pgstore.WithMaxConns(1))
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			seed(store, scanRows)

			var seen int
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				return tx.ScanEntityAfter("issue", "", limit, func(row storage.Row) error {
					seen++
					_, err := tx.GetRow(row.Entity, row.Key)
					return err
				})
			})).To(Succeed())
			Expect(seen).To(Equal(scanRows))
		},
		Entry("bounded, read in one statement", scanRows),
		Entry("bounded past the direct-scan cap, read through a cursor", 1<<20),
		Entry("unbounded, read through a cursor", 0),
	)
})
