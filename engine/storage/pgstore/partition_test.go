package pgstore_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
	"github.com/levelfourab/syncplane/engine/storage/storagetest"
	"github.com/levelfourab/syncplane/internal/pgtest"
)

// The whole contract again, on the other physical layout. Modulus 2 rather
// than a realistic 32: routing, ON CONFLICT and pruning behave the same at
// either, and the conformance suite builds a fresh store more than a hundred
// times — each one paying for five tables per remainder.
var _ = storagetest.Conformance("postgres (partitioned)", func() storage.Adapter {
	return open(pgstore.WithPartitions(2))
})

var _ = Describe("Postgres partitioning", func() {
	ctx := context.Background()

	connect := func(dsn string) *pgx.Conn {
		GinkgoHelper()
		conn, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
		return conn
	}

	// partitionCounts maps every partitioned table in the schema to how many
	// partitions it has. A table that is not partitioned is absent, so one
	// assertion covers both halves of the layout decision: which tables are
	// split, and that no other table is.
	partitionCounts := func(dsn string) map[string]int {
		GinkgoHelper()
		rows, err := connect(dsn).Query(ctx,
			`SELECT c.relname, (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid)
			 FROM pg_class c
			 JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = current_schema() AND c.relkind = 'p'`)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		counts := map[string]int{}
		for rows.Next() {
			var (
				name string
				n    int
			)
			Expect(rows.Scan(&name, &n)).To(Succeed())
			counts[name] = n
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		return counts
	}

	It("splits the workspace-scoped tables and leaves the rest whole", func() {
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn, pgstore.WithPartitions(4))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(partitionCounts(dsn)).To(Equal(map[string]int{
			"rows":             4,
			"field_deltas":     4,
			"idempotency":      4,
			"index_entries":    4,
			"consumer_shadows": 4,
		}))
	})

	It("creates nothing partitioned by default", func() {
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(partitionCounts(dsn)).To(BeEmpty())
	})

	It("keys the partitions by namespace and workspace, hashed", func() {
		// The key is what makes every statement in the adapter prune to one
		// partition: all of them bind both columns to equality, and none of
		// them can move a row across the boundary.
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn, pgstore.WithPartitions(2))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		conn := connect(dsn)
		for _, table := range []string{"rows", "field_deltas", "idempotency", "index_entries", "consumer_shadows"} {
			var (
				strategy string
				columns  []string
			)
			Expect(conn.QueryRow(ctx,
				`SELECT p.partstrat::text, array_agg(a.attname ORDER BY a.attnum)
				 FROM pg_partitioned_table p
				 JOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = ANY(p.partattrs)
				 WHERE p.partrelid = to_regclass($1)
				 GROUP BY p.partstrat`, table).Scan(&strategy, &columns)).To(Succeed())
			Expect(strategy).To(Equal("h"), "%s must be hash-partitioned", table)
			Expect(columns).To(Equal([]string{"namespace", "workspace_id"}), "partition key of %s", table)
		}
	})

	It("routes every written row into a partition", func() {
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn, pgstore.WithPartitions(2))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		for _, ws := range []string{"ws1", "ws2", "ws3"} {
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
			})).To(Succeed())
		}

		conn := connect(dsn)
		var total, onParent int
		Expect(conn.QueryRow(ctx,
			`SELECT count(*), count(*) FILTER (WHERE tableoid = to_regclass('rows'))
			 FROM rows`).Scan(&total, &onParent)).To(Succeed())
		Expect(total).To(Equal(3))
		Expect(onParent).To(BeZero(), "a partitioned parent holds no rows of its own")
	})

	It("adopts the layout it finds, whatever a later open asks for", func() {
		// The property that lets a deploy step choose the layout once and
		// every node boot with a bare Open: an option that arrived too late is
		// ignored, never obeyed, so no boot rewrites what the database has.
		dsn := pgtest.DSN()
		Expect(pgstore.Migrate(ctx, dsn, pgstore.WithPartitions(4))).To(Succeed())

		bare, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		Expect(bare.Close()).To(Succeed())
		Expect(partitionCounts(dsn)).To(HaveKeyWithValue("rows", 4))

		other, err := pgstore.Open(ctx, dsn, pgstore.WithPartitions(16))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(other.Close()).To(Succeed()) })
		Expect(partitionCounts(dsn)).To(HaveKeyWithValue("rows", 4))
	})

	It("accepts a database whose statements were applied by hand", func() {
		// The reason Statements is exported: a host that owns its DDL runs
		// these and its nodes boot onto the result. Open re-running any of
		// them would fail on an existing relation, so reaching a read at all
		// is the assertion.
		dsn := pgtest.DSN()
		stmts, err := pgstore.Statements(pgstore.WithPartitions(2))
		Expect(err).NotTo(HaveOccurred())
		conn := connect(dsn)
		for _, m := range stmts {
			_, err := conn.Exec(ctx, m.SQL)
			Expect(err).NotTo(HaveOccurred(), "migration %d", m.Version)
		}

		store, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(partitionCounts(dsn)).To(HaveKeyWithValue("rows", 2))
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("v")))
			return nil
		})).To(Succeed())
	})

	It("migrates an existing database to the head without touching its layout", func() {
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn, pgstore.WithPartitions(2))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(pgstore.Migrate(ctx, dsn)).To(Succeed())
		Expect(partitionCounts(dsn)).To(HaveKeyWithValue("rows", 2))
	})

	// No database needed: the modulus is rejected before anything connects,
	// which is why a DSN that leads nowhere is enough.
	DescribeTable("rejects a modulus that is not a power of two",
		func(n int) {
			const nowhere = "postgres://invalid.example:5432/nothing"
			_, err := pgstore.Statements(pgstore.WithPartitions(n))
			Expect(err).To(MatchError(ContainSubstring("power of two")))
			_, err = pgstore.Open(ctx, nowhere, pgstore.WithPartitions(n))
			Expect(err).To(MatchError(ContainSubstring("power of two")))
			Expect(pgstore.Migrate(ctx, nowhere, pgstore.WithPartitions(n))).
				To(MatchError(ContainSubstring("power of two")))
		},
		Entry("three", 3),
		Entry("a hundred", 100),
		Entry("negative", -8),
		Entry("past the ceiling", 2048),
	)
})
