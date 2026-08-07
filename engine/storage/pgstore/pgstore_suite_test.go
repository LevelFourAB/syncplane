package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
	"github.com/levelfourab/syncplane/engine/storage/storagetest"
)

func TestPgstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Store Suite")
}

var _ = AfterSuite(pgtest.Shutdown)

// open is the shared "fresh, empty store" every spec below builds on.
func open(opts ...pgstore.Option) *pgstore.Store {
	GinkgoHelper()
	store, err := pgstore.Open(context.Background(), pgtest.DSN(), opts...)
	Expect(err).NotTo(HaveOccurred())
	return store
}

var _ = storagetest.Conformance("postgres", func() storage.Adapter {
	return open()
})

// A namespaced store must satisfy the full adapter contract too.
var _ = storagetest.Conformance("postgres (namespaced)", func() storage.Adapter {
	return open(pgstore.WithNamespace("app1"))
})

// One root store, two derived namespaces sharing its pool. cleanup owns the
// real Close — derived stores' Close is a no-op.
var _ = storagetest.ConformanceNamespaced("postgres", func() (storage.Adapter, storage.Adapter, func()) {
	root := open()
	return root.Namespace("a"), root.Namespace("b"), func() {
		Expect(root.Close()).To(Succeed())
	}
})

var _ = Describe("Postgres migrations", func() {
	ctx := context.Background()

	It("applies every migration once and reopens onto the same data", func() {
		dsn := pgtest.DSN()

		store, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())

		// A second Open finds the ledger already at the head and applies
		// nothing; the data it left behind is still there.
		store, err = pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("v1")))
			return nil
		})).To(Succeed())

		conn, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
		var applied []int
		rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
		Expect(err).NotTo(HaveOccurred())
		for rows.Next() {
			var v int
			Expect(rows.Scan(&v)).To(Succeed())
			applied = append(applied, v)
		}
		rows.Close()
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(applied).To(Equal([]int{1, 2}), "every migration, each recorded exactly once")
	})

	It("lets several nodes Open the same database at once", func() {
		// The difference from sqlitestore, which must never open one file
		// twice: concurrent boots race to apply the same migration, and CREATE
		// TABLE is not idempotent, so migration runs under an advisory lock.
		dsn := pgtest.DSN()
		const nodes = 4
		done := make(chan error, nodes)
		for range nodes {
			go func() {
				store, err := pgstore.Open(ctx, dsn)
				if err != nil {
					done <- err
					return
				}
				done <- store.Close()
			}()
		}
		for range nodes {
			Expect(<-done).To(Succeed())
		}
	})
})

var _ = Describe("Postgres concurrency", func() {
	ctx := context.Background()

	It("commits writes to different workspaces concurrently", func() {
		// The property SQLite cannot have: its one writer connection serializes
		// every workspace against every other. Here wsA holds an open write
		// transaction while wsB's runs to completion — under a single-writer
		// adapter this spec deadlocks until the timeout.
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		holding := make(chan struct{})
		release := make(chan struct{})
		heldDone := make(chan error, 1)
		go func() {
			heldDone <- store.WithWriteTx(ctx, "wsA", func(tx storage.WriteTx) error {
				if err := tx.PutRow("widget", "k", []byte("a"), 1, storage.Actor{}); err != nil {
					return err
				}
				close(holding)
				<-release
				return nil
			})
		}()

		<-holding
		other := make(chan error, 1)
		go func() {
			other <- store.WithWriteTx(ctx, "wsB", func(tx storage.WriteTx) error {
				return tx.PutRow("widget", "k", []byte("b"), 1, storage.Actor{})
			})
		}()
		Eventually(other, 5*time.Second).Should(Receive(BeNil()),
			"a write to another workspace must not wait on wsA's open transaction")

		close(release)
		Eventually(heldDone, 5*time.Second).Should(Receive(BeNil()))
	})

	It("excludes writers of one workspace across separate store handles", func() {
		// Two Stores over one database stand in for two server nodes. The
		// advisory lock is what makes the contract's mutual exclusion hold
		// between processes, where SQLite's write lock only ever had to hold
		// within one.
		dsn := pgtest.DSN()
		nodeA, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(nodeA.Close()).To(Succeed()) })
		nodeB, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(nodeB.Close()).To(Succeed()) })

		holding := make(chan struct{})
		release := make(chan struct{})
		aDone := make(chan error, 1)
		go func() {
			aDone <- nodeA.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				// Read-only until released, mirroring the push path: exclusion
				// has to cover the reads before the first write, which is why
				// the lock is taken at BEGIN and not at first write.
				if _, err := tx.CurrentSeq(); err != nil {
					return err
				}
				close(holding)
				<-release
				return tx.SetClientLastMutationID("c", 1)
			})
		}()

		<-holding
		bDone := make(chan error, 1)
		go func() {
			bDone <- nodeB.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				last, err := tx.ClientLastMutationID("c")
				if err != nil {
					return err
				}
				return tx.SetClientLastMutationID("c", last+1)
			})
		}()
		Consistently(bDone, "300ms").ShouldNot(Receive(),
			"the other node's write transaction must wait for the workspace lock")

		close(release)
		Eventually(aDone, 5*time.Second).Should(Receive(BeNil()))
		Eventually(bDone, 5*time.Second).Should(Receive(BeNil()))

		nodeB.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			// 2, not 1: node B read the watermark node A had committed. A lost
			// update here would mean the two nodes ran their closures at once.
			id, err := tx.ClientLastMutationID("c")
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(BeEquivalentTo(2))
			return nil
		})
	})

	It("makes one node's commit visible to another", func() {
		dsn := pgtest.DSN()
		writer, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(writer.Close()).To(Succeed()) })
		reader, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(reader.Close()).To(Succeed()) })

		Expect(writer.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k", []byte("shared"), 1, storage.Actor{})
		})).To(Succeed())

		Expect(reader.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("shared")))
			return nil
		})).To(Succeed())
	})
})

var _ = Describe("Postgres transaction invariants", func() {
	ctx := context.Background()

	It("hands read closures a transaction that cannot be widened to a write", func() {
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
			// WithReadTx never commits, so a write reached this way would be
			// silently rolled back; the assertion must fail instead.
			_, ok := tx.(storage.WriteTx)
			Expect(ok).To(BeFalse(), "a read tx must not satisfy storage.WriteTx")
			return nil
		})).To(Succeed())
	})

	It("recovers a transaction aborted by a failed statement inside a savepoint", func() {
		// The difference from SQLite that could have bitten the engine: ANY
		// failed statement puts a Postgres transaction into an aborted state
		// where every later statement fails with 25P02. The engine continues
		// past an error inside a write transaction in exactly one place — the
		// push path's rejected mutation — and it does so inside InSavepoint,
		// whose ROLLBACK TO is what clears that state.
		//
		// The conformance suite's savepoint specs roll back on a Go-level error
		// with no failed statement behind it, so they never reach the aborted
		// state at all. This does: a NUL byte is not representable in a
		// Postgres text column, so the write fails in the server rather than in
		// the driver.
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			err := tx.InSavepoint(func() error {
				return tx.PutRow("widget", "bad\u0000key", []byte("v"), 1, storage.Actor{})
			})
			// It has to be the SERVER that rejected it. A driver-side rejection
			// would never send a statement, so the transaction would never
			// enter the aborted state and this spec would prove nothing.
			var pgErr *pgconn.PgError
			Expect(errors.As(err, &pgErr)).To(BeTrue(),
				"the write must fail in Postgres, not in pgx: got %v", err)

			// The transaction has to be usable again here, or the push path
			// would lose every mutation after a rejected one.
			return tx.PutRow("widget", "good", []byte("v"), 2, storage.Actor{})
		})).To(Succeed())

		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "good")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("v")))
			return nil
		})).To(Succeed())
	})

	// The shared suite pins re-entrant reads through a scan longer than one
	// FETCH. What is left here is the cursor loop's own early exit, which no
	// adapter-agnostic spec can aim at a batch boundary.
	Describe("scans larger than one cursor batch", func() {
		const rows = 700 // comfortably more than scanBatch

		seed := func(store storage.Adapter) {
			GinkgoHelper()
			Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				for i := range rows {
					key := fmt.Sprintf("k%04d", i)
					if err := tx.PutRow("widget", key, []byte(key), uint64(i+1), storage.Actor{}); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
		}

		It("stops a multi-batch scan when fn returns an error", func() {
			store := open()
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			seed(store)

			boom := fmt.Errorf("boom")
			var seen int
			err := store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				return tx.ScanEntity("widget", func(storage.Row) error {
					seen++
					if seen == 300 { // inside the second batch
						return boom
					}
					return nil
				})
			})
			Expect(err).To(MatchError(boom))
			Expect(seen).To(Equal(300), "the scan must stop at the error, not finish the batch")
		})
	})
})
