package pgstore_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// These specs run against a FAKE lagging replica: pgstore judges freshness
// purely by the workspaces row's last_seq, so a second schema holding an older
// copy of the primary's history is indistinguishable from a standby that has
// not caught up — and unlike a real standby, the lag holds still. Distinct row
// values on the two sides make the served-from side observable through the
// bytes as well as the route.
var _ = Describe("Postgres read replica", func() {
	ctx := context.Background()

	put := func(store *pgstore.Store, ws, key, value string) uint64 {
		GinkgoHelper()
		var seq uint64
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			var err error
			if seq, err = tx.NextSeq(); err != nil {
				return err
			}
			return tx.PutRow("widget", key, []byte(value), seq, storage.Actor{})
		})).To(Succeed())
		return seq
	}

	type lagging struct {
		store      *pgstore.Store
		replicaDSN string
		// replicaSeq is the replica's head; primarySeq the primary's, above it.
		replicaSeq uint64
		primarySeq uint64
	}

	// openLagging seeds a primary whose ws1/k1 holds "fresh-v" at seq 2 and a
	// replica frozen at the shared prefix of that history: "stale-v" at seq 1.
	openLagging := func() lagging {
		GinkgoHelper()
		primaryDSN, replicaDSN := pgtest.DSN(), pgtest.DSN()

		seed, err := pgstore.Open(ctx, replicaDSN)
		Expect(err).NotTo(HaveOccurred())
		replicaSeq := put(seed, "ws1", "k1", "stale-v")
		Expect(seed.Close()).To(Succeed())

		seed, err = pgstore.Open(ctx, primaryDSN)
		Expect(err).NotTo(HaveOccurred())
		Expect(put(seed, "ws1", "k1", "stale-v")).To(Equal(replicaSeq))
		primarySeq := put(seed, "ws1", "k1", "fresh-v")
		Expect(seed.Close()).To(Succeed())

		store, err := pgstore.Open(ctx, primaryDSN, pgstore.WithReadReplica(replicaDSN))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		return lagging{store: store, replicaDSN: replicaDSN, replicaSeq: replicaSeq, primarySeq: primarySeq}
	}

	// readValue reads ws1/k1 through the reader and reports where from.
	readValue := func(read func(func(storage.ReadTx) error) (storage.ReadRoute, error)) (storage.ReadRoute, string) {
		GinkgoHelper()
		var value string
		route, err := read(func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			value = string(row.Value)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		return route, value
	}

	It("serves a floor the replica satisfies from the replica", func() {
		l := openLagging()
		reader := l.store.ReplicaReader()
		for _, minSeq := range []uint64{0, l.replicaSeq} {
			route, value := readValue(func(fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
				return reader.WithReadTxMinSeq(ctx, "ws1", minSeq, fn)
			})
			Expect(route).To(Equal(storage.ReadRouteReplica), "minSeq %d", minSeq)
			Expect(value).To(Equal("stale-v"), "the bytes must come from the replica")
		}
	})

	It("falls back to the primary for a floor the replica has not reached", func() {
		l := openLagging()
		reader := l.store.ReplicaReader()
		route, value := readValue(func(fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
			return reader.WithReadTxMinSeq(ctx, "ws1", l.primarySeq, fn)
		})
		Expect(route).To(Equal(storage.ReadRouteFallbackStale))
		Expect(value).To(Equal("fresh-v"), "a fallback read must serve the primary's bytes")
	})

	It("never serves an anchored read from behind the primary's head", func() {
		l := openLagging()
		reader := l.store.ReplicaReader()
		route, value := readValue(func(fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
			return reader.WithReadTxAnchored(ctx, "ws1", fn)
		})
		Expect(route).To(Equal(storage.ReadRouteFallbackStale))
		Expect(value).To(Equal("fresh-v"))
	})

	// snapshot replaces every table behind toDSN with a copy of fromDSN's, so
	// the copy is what a standby holds when it has replayed exactly up to now:
	// the same workspace generation, epoch included.
	snapshot := func(fromDSN, toDSN string) {
		GinkgoHelper()
		schemaOf := func(dsn string) string {
			GinkgoHelper()
			conn, err := pgx.Connect(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = conn.Close(ctx) }()
			var schema string
			Expect(conn.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema)).To(Succeed())
			return schema
		}
		from, to := schemaOf(fromDSN), schemaOf(toDSN)

		conn, err := pgx.Connect(ctx, fromDSN)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = conn.Close(ctx) }()
		rows, err := conn.Query(ctx, `
			SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND NOT c.relispartition`, from)
		Expect(err).NotTo(HaveOccurred())
		tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
		Expect(err).NotTo(HaveOccurred())
		Expect(tables).NotTo(BeEmpty())
		for _, table := range tables {
			src := pgx.Identifier{from, table}.Sanitize()
			dst := pgx.Identifier{to, table}.Sanitize()
			_, err := conn.Exec(ctx, `TRUNCATE `+dst)
			Expect(err).NotTo(HaveOccurred())
			_, err = conn.Exec(ctx, `INSERT INTO `+dst+` SELECT * FROM `+src)
			Expect(err).NotTo(HaveOccurred(), "copy %s", table)
		}
	}

	It("never serves an anchored read from a workspace generation the primary deleted", func() {
		// Given a replica that holds ws1 as it stood before the primary
		// deleted it, restored it and wrote it again
		primaryDSN, replicaDSN := pgtest.DSN(), pgtest.DSN()
		seed, err := pgstore.Open(ctx, primaryDSN)
		Expect(err).NotTo(HaveOccurred())
		for i := 0; i < 3; i++ {
			put(seed, "ws1", "k1", "deleted-v")
		}
		Expect(seed.Close()).To(Succeed())
		seed, err = pgstore.Open(ctx, replicaDSN)
		Expect(err).NotTo(HaveOccurred())
		Expect(seed.Close()).To(Succeed())
		snapshot(primaryDSN, replicaDSN)

		store, err := pgstore.Open(ctx, primaryDSN, pgstore.WithReadReplica(replicaDSN))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.DeleteWorkspace(ctx, "ws1")).To(Succeed())
		Expect(store.RestoreWorkspace(ctx, "ws1")).To(Succeed())
		Expect(put(store, "ws1", "k1", "fresh-v")).To(BeNumerically("<", 3),
			"the restored workspace restarts its sequence below the replica's")

		// When a caller reads its own write through an anchored read
		reader := store.ReplicaReader()
		route, value := readValue(func(fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
			return reader.WithReadTxAnchored(ctx, "ws1", fn)
		})

		// Then the read sees the primary's generation, not the deleted one
		Expect(value).To(Equal("fresh-v"), "the replica served rows of the deleted generation")
		Expect(route).NotTo(Equal(storage.ReadRouteReplica))
	})

	It("falls back to the primary when the replica breaks after Open", func() {
		l := openLagging()

		conn, err := pgx.Connect(ctx, l.replicaDSN)
		Expect(err).NotTo(HaveOccurred())
		_, err = conn.Exec(ctx, `DROP TABLE workspaces`)
		Expect(err).NotTo(HaveOccurred())
		Expect(conn.Close(ctx)).To(Succeed())

		reader := l.store.ReplicaReader()
		route, value := readValue(func(fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
			return reader.WithReadTxMinSeq(ctx, "ws1", l.replicaSeq, fn)
		})
		Expect(route).To(Equal(storage.ReadRouteFallbackError))
		Expect(value).To(Equal("fresh-v"))
	})

	It("answers the workspace lists from the replica", func() {
		primaryDSN, replicaDSN := pgtest.DSN(), pgtest.DSN()

		seed, err := pgstore.Open(ctx, replicaDSN)
		Expect(err).NotTo(HaveOccurred())
		put(seed, "ws1", "k1", "stale-v")
		Expect(seed.Close()).To(Succeed())

		seed, err = pgstore.Open(ctx, primaryDSN)
		Expect(err).NotTo(HaveOccurred())
		put(seed, "ws1", "k1", "stale-v")
		put(seed, "ws2", "k1", "fresh-only")
		Expect(seed.Close()).To(Succeed())

		store, err := pgstore.Open(ctx, primaryDSN, pgstore.WithReadReplica(replicaDSN))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		// ws2 exists only on the primary; a replica-served list may omit it,
		// which is the bounded staleness the contract licenses.
		ids, err := store.ListWorkspaceIDs(ctx, storage.Range{})
		Expect(err).NotTo(HaveOccurred())
		Expect(ids).To(Equal([]string{"ws1"}))

		pending, err := store.ListPendingWorkspaceIDs(ctx, "c", storage.Range{}, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(pending).To(Equal([]string{"ws1"}))
	})

	It("fails Open when the replica schema is unmigrated", func() {
		primaryDSN, replicaDSN := pgtest.DSN(), pgtest.DSN()
		_, err := pgstore.Open(ctx, primaryDSN, pgstore.WithReadReplica(replicaDSN))
		Expect(err).To(HaveOccurred(), "a replica without the schema cannot be a standby of this database")
	})
})
