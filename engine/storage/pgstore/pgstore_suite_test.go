package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
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

// The replica pointed at the primary itself: always fresh, so the routing and
// read contracts are exercised without a second database. The fake-lagging
// specs in replica_test.go cover the stale side.
var _ = storagetest.Replica("postgres", func() storage.Adapter {
	dsn := pgtest.DSN()
	store, err := pgstore.Open(context.Background(), dsn, pgstore.WithReadReplica(dsn))
	Expect(err).NotTo(HaveOccurred())
	return store
})

// The full adapter contract must hold unchanged with a replica wired in.
var _ = storagetest.Conformance("postgres (replica configured)", func() storage.Adapter {
	dsn := pgtest.DSN()
	store, err := pgstore.Open(context.Background(), dsn, pgstore.WithReadReplica(dsn))
	Expect(err).NotTo(HaveOccurred())
	return store
})

var _ = storagetest.CursorState("postgres", func() storage.Adapter {
	return open()
})

// Two opens of one database stand in for two server nodes sharing a lease
// space — the deployment the leases exist for.
var _ = storagetest.Leases("postgres", func() (storage.Adapter, storage.Adapter, func()) {
	dsn := pgtest.DSN()
	a, err := pgstore.Open(context.Background(), dsn)
	Expect(err).NotTo(HaveOccurred())
	b, err := pgstore.Open(context.Background(), dsn)
	Expect(err).NotTo(HaveOccurred())
	return a, b, func() {
		Expect(a.Close()).To(Succeed())
		Expect(b.Close()).To(Succeed())
	}
})

var _ = storagetest.Catalog("postgres", func() storage.Adapter {
	return open()
})

var _ = Describe("Postgres leases", func() {
	ctx := context.Background()

	It("frees a node's leases when its store closes", func() {
		// The failover story: leases are session advisory locks, so a node
		// that crashes or shuts down releases everything it held the moment
		// its connection dies — no TTL to wait out.
		dsn := pgtest.DSN()
		holder, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		survivor, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(survivor.Close()).To(Succeed()) })

		_, ok, err := holder.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		_, ok, err = survivor.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())

		Expect(holder.Close()).To(Succeed())
		Eventually(func() bool {
			release, ok, err := survivor.Leases().TryAcquire(ctx, "gc", "ws1")
			if err != nil || !ok {
				return false
			}
			release()
			return true
		}, 5*time.Second).Should(BeTrue())
	})

	It("scopes leases to the schema, so deployments sharing a database stay apart", func() {
		// Two DSNs from pgtest differ only in search_path — the same shape as
		// a staging and a production deployment on one database server.
		a, err := pgstore.Open(ctx, pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(a.Close()).To(Succeed()) })
		b, err := pgstore.Open(ctx, pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(b.Close()).To(Succeed()) })

		_, ok, err := a.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		_, ok, err = b.Leases().TryAcquire(ctx, "gc", "ws1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
	})
})

// grantReadWriteRole creates a login role holding DML on the schema behind dsn
// and nothing that can change it, and returns dsn pointed at that role. What a
// node needs from the database is not observable on the superuser connection
// every other spec runs as.
func grantReadWriteRole(dsn string) string {
	GinkgoHelper()
	ctx := context.Background()
	exec := func(sql string) {
		GinkgoHelper()
		conn, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = conn.Close(ctx) }()
		_, err = conn.Exec(ctx, sql)
		Expect(err).NotTo(HaveOccurred())
	}

	conn, err := pgx.Connect(ctx, dsn)
	Expect(err).NotTo(HaveOccurred())
	var schema string
	Expect(conn.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema)).To(Succeed())
	Expect(conn.Close(ctx)).To(Succeed())

	const password = "restricted"
	role := pgtest.UniqueName("sp_dml")
	exec(fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password))
	// A role is cluster-wide, and DROP ROLE refuses while the grants below
	// still name it. Ginkgo unwinds this before pgtest drops the schema and
	// after the spec's own store.Close.
	DeferCleanup(func() {
		exec(fmt.Sprintf(`DROP OWNED BY %q`, role))
		exec(fmt.Sprintf(`DROP ROLE %q`, role))
	})
	exec(fmt.Sprintf(`GRANT USAGE ON SCHEMA %q TO %q`, schema, role))
	exec(fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %q TO %q`, schema, role))

	u, err := url.Parse(dsn)
	Expect(err).NotTo(HaveOccurred(), "the suite's DSN must be a URI to re-point at a role")
	u.User = url.UserPassword(role, password)
	return u.String()
}

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
		Expect(applied).To(Equal([]int{1}), "every migration, each recorded exactly once")
	})

	Describe("a ledger ahead of this binary", func() {
		// What a node finds after a deploy step migrated for the next release
		// and the roll has not reached it — or after a rollback. The step's own
		// DDL is beside the point here: what decides the boot is what the
		// ledger records about it, which is the only thing a binary that never
		// carried the step can read.
		recordStep := func(dsn string, additive bool) {
			GinkgoHelper()
			conn, err := pgx.Connect(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = conn.Close(ctx) }()
			_, err = conn.Exec(ctx,
				`INSERT INTO schema_migrations (version, additive) VALUES ($1, $2)`,
				pgstore.SchemaVersion()+1, additive)
			Expect(err).NotTo(HaveOccurred())
		}

		DescribeTable("serves a database whose later steps only added to it",
			func(opts ...pgstore.Option) {
				dsn := pgtest.DSN()
				Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())
				recordStep(dsn, true)

				store, err := pgstore.Open(ctx, dsn, opts...)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
				Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
					return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
				})).To(Succeed())
			},
			Entry("migrating at boot"),
			Entry("verifying at boot", pgstore.WithoutMigrations()),
		)

		DescribeTable("refuses a database whose later step removed something",
			func(opts ...pgstore.Option) {
				dsn := pgtest.DSN()
				Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())
				recordStep(dsn, false)

				_, err := pgstore.Open(ctx, dsn, opts...)
				Expect(err).To(MatchError(ContainSubstring("needs a binary at version")))
			},
			Entry("migrating at boot"),
			Entry("verifying at boot", pgstore.WithoutMigrations()),
		)

		It("applies nothing to a database ahead of it", func() {
			dsn := pgtest.DSN()
			Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())
			recordStep(dsn, true)

			Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())

			conn, err := pgx.Connect(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
			var highest int
			Expect(conn.QueryRow(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&highest)).To(Succeed())
			Expect(highest).To(Equal(pgstore.SchemaVersion() + 1))
		})
	})

	Describe("opened without them", func() {
		It("serves a database a deploy step migrated", func() {
			dsn := pgtest.DSN()
			Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())

			store, err := pgstore.Open(ctx, dsn, pgstore.WithoutMigrations())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
			})).To(Succeed())
		})

		It("refuses a database nothing has migrated, and creates nothing", func() {
			// The failure a deploy step's absence must produce: an empty
			// database is where migrate would have built the whole schema, so
			// leaving it untouched is the assertion, not just the error.
			dsn := pgtest.DSN()
			_, err := pgstore.Open(ctx, dsn, pgstore.WithoutMigrations())
			Expect(err).To(MatchError(ContainSubstring("has this database been migrated?")))

			conn, err := pgx.Connect(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
			var tables int
			Expect(conn.QueryRow(ctx,
				`SELECT count(*) FROM pg_tables WHERE schemaname = current_schema()`).
				Scan(&tables)).To(Succeed())
			Expect(tables).To(BeZero())
		})

		It("refuses a database whose ledger the deploy step has not filled", func() {
			// An empty ledger: the state between a deploy step creating it and
			// applying the first migration, and the one a bare Open would
			// silently complete on the node's behalf.
			dsn := pgtest.DSN()
			conn, err := pgx.Connect(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
			_, err = conn.Exec(ctx, `CREATE TABLE schema_migrations (
				version    INTEGER PRIMARY KEY,
				applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				additive   BOOLEAN NOT NULL DEFAULT TRUE
			)`)
			Expect(err).NotTo(HaveOccurred())

			_, err = pgstore.Open(ctx, dsn, pgstore.WithoutMigrations())
			Expect(err).To(MatchError(ContainSubstring("database schema at version 0")))
		})

		It("boots as a role that cannot change the schema", func() {
			// What the option is for. A node that migrates at boot needs CREATE
			// on the schema forever — Postgres checks the privilege before it
			// checks existence, so even the ledger's CREATE TABLE IF NOT EXISTS
			// is refused without it — and a deployment whose DDL is a deploy
			// step should not have to grant that to every node.
			dsn := pgtest.DSN()
			Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())

			restricted := grantReadWriteRole(dsn)
			store, err := pgstore.Open(ctx, restricted, pgstore.WithoutMigrations())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
			})).To(Succeed())

			_, err = pgstore.Open(ctx, restricted)
			Expect(err).To(MatchError(ContainSubstring("permission denied")))
		})
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

	It("fails the whole transaction when a statement inside a savepoint fails", func() {
		// The difference from SQLite the engine has to live with: ANY failed
		// statement puts a Postgres transaction into an aborted state where
		// every later statement fails with 25P02, and no savepoint here rewinds
		// out of it — one batch carries statements from savepoints since
		// released, so there is no single point to rewind to (see InSavepoint
		// and queue.go).
		//
		// Nothing the engine does needs otherwise. The one place it continues
		// past an error inside a write transaction is a rejected mutation, whose
		// error is a verdict on the mutation rather than a statement Postgres
		// refused; a storage error there fails the workspace batch. So this
		// pins the safe direction instead: the failure surfaces, and none of the
		// transaction lands.
		//
		// The conformance suite's savepoint specs roll back on a Go-level error
		// with no failed statement behind it, so they never reach the aborted
		// state at all. This does: a NUL byte is not representable in a
		// Postgres text column, so the write fails in the server rather than in
		// the driver.
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		err := store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			Expect(tx.InSavepoint(func() error {
				return tx.PutRow("widget", "bad\u0000key", []byte("v"), 1, storage.Actor{})
			})).To(Succeed(), "the write is queued, so nothing has failed yet")
			return tx.PutRow("widget", "good", []byte("v"), 2, storage.Actor{})
		})
		// It has to be the SERVER that rejected it. A driver-side rejection
		// would never send a statement, so the transaction would never enter
		// the aborted state and this spec would prove nothing.
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue(),
			"the write must fail in Postgres, not in pgx: got %v", err)

		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			_, err := tx.GetRow("widget", "good")
			Expect(err).To(MatchError(storage.ErrNotFound),
				"the write that followed the failure must not have landed either")
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
