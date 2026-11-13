package pgstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("Postgres names", func() {
	ctx := context.Background()

	// tracedStore returns store and a second handle on its database whose
	// pool counts round trips and keeps the statements on text_fields and
	// shadow_sets.
	tracedStore := func(opts ...Option) (dsn string, store, traced *Store, tracer *nameTracer) {
		GinkgoHelper()
		dsn = pgtest.DSN()
		store, err := Open(ctx, dsn, opts...)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		cfg, err := pgxpool.ParseConfig(dsn)
		Expect(err).NotTo(HaveOccurred())
		tracer = &nameTracer{}
		cfg.ConnConfig.Tracer = tracer
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(pool.Close)
		return dsn, store, &Store{pool: pool, scopes: newScopeCache(), partitioned: store.partitioned}, tracer
	}

	admin := func(dsn string) *pgx.Conn {
		GinkgoHelper()
		conn, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
		return conn
	}

	fieldNames := func(conn *pgx.Conn, ws string) []string {
		GinkgoHelper()
		rows, err := conn.Query(ctx,
			`SELECT field FROM text_fields WHERE workspace_id = $1 ORDER BY field`, ws)
		Expect(err).NotTo(HaveOccurred())
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		Expect(err).NotTo(HaveOccurred())
		return names
	}

	It("reads only the names a transaction uses, in the round trips it makes anyway", func() {
		_, store, traced, tracer := tracedStore()
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for i := range 50 {
				if err := tx.PutFieldDelta("doc", "k", fmt.Sprintf("notes.e%02d.body", i), uint64(i+1), []byte{1}); err != nil {
					return err
				}
				if err := tx.PutConsumerShadow(fmt.Sprintf("c%02d", i), "doc", "k", []byte{1}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())

		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			tracer.reset()
			_, err := tx.CurrentSeq()
			Expect(err).NotTo(HaveOccurred())
			Expect(tracer.taken()).To(BeEmpty(), "the state a transaction loads holds no names")

			// The read of the span, of its name and of the horizon is one
			// statement.
			tracer.reset()
			frags, complete, err := tx.FieldDeltas("doc", "k", "notes.e07.body", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(frags).To(Equal([][]byte{{1}}))
			Expect(tracer.trips()).To(Equal(1))
			tracer.reset()
			_, complete, err = tx.FieldDeltas("doc", "k", "unknown", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(tracer.trips()).To(Equal(1))

			// A write of a field the transaction has read is queued.
			tracer.reset()
			Expect(tx.PutFieldDelta("doc", "k", "notes.e07.body", 60, []byte{2})).To(Succeed())
			Expect(tracer.trips()).To(BeZero())

			tracer.reset()
			Expect(tx.ConsumerShadow("c03", "doc", "k")).To(Equal([]byte{1}))
			Expect(tracer.trips()).To(Equal(1))
			tracer.reset()
			Expect(tx.PutConsumerShadow("c03", "doc", "k", []byte{2})).To(Succeed())
			Expect(tx.DeleteConsumerShadow("c04", "doc", "k")).To(Succeed())
			Expect(tracer.trips()).To(BeZero())
			return nil
		})).To(Succeed())

		Expect(store.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			frags, _, err := tx.FieldDeltas("doc", "k", "notes.e07.body", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(frags).To(Equal([][]byte{{1}, {2}}))
			Expect(tx.ConsumerShadow("c03", "doc", "k")).To(Equal([]byte{2}))
			Expect(tx.ConsumerShadow("c04", "doc", "k")).To(BeNil())
			return nil
		})).To(Succeed())
	})

	It("deletes the name of a field when its last entry is pruned", func() {
		dsn, store, _, _ := tracedStore()
		conn := admin(dsn)
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for i, field := range []string{"old", "mixed", "mixed", "new"} {
				if err := tx.PutFieldDelta("doc", fmt.Sprintf("k%d", i), field, uint64(i+1), []byte{1}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		Expect(store.WithWriteTx(ctx, "b", func(tx storage.WriteTx) error {
			return tx.PutFieldDelta("doc", "k", "old", 1, []byte{1})
		})).To(Succeed())
		// The entries of "old" and the first of "mixed" age past the cutoff.
		_, err := conn.Exec(ctx,
			`UPDATE field_deltas SET updated_at = 0 WHERE seq IN (1, 2) AND wsn = (
			     SELECT scope FROM index_scopes WHERE workspace_id = 'a' AND entity = '' AND idx = '')`)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			horizon, err := tx.PruneFieldDeltas(time.Now().Add(-time.Hour))
			Expect(horizon).To(Equal(uint64(2)))
			return err
		})).To(Succeed())
		Expect(fieldNames(conn, "a")).To(Equal([]string{"mixed", "new"}))
		Expect(fieldNames(conn, "b")).To(Equal([]string{"old"}), "another workspace keeps its names")

		// The field takes a new name when it is written again.
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			return tx.PutFieldDelta("doc", "k0", "old", 5, []byte{2})
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			frags, complete, err := tx.FieldDeltas("doc", "k0", "old", 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(frags).To(Equal([][]byte{{2}}))
			return nil
		})).To(Succeed())
	})

	It("keeps an entry that the pruning transaction writes after the prune", func() {
		_, store, _, _ := tracedStore()
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			if err := tx.PutFieldDelta("doc", "k", "body", 1, []byte{1}); err != nil {
				return err
			}
			// The cutoff is ahead of every entry, so the prune deletes the
			// entry above and the name of its field.
			if _, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour)); err != nil {
				return err
			}
			return tx.PutFieldDelta("doc", "k", "body", 2, []byte{2})
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			frags, _, err := tx.FieldDeltas("doc", "k", "body", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(frags).To(Equal([][]byte{{2}}))
			return nil
		})).To(Succeed())
	})

	It("keeps an entry that a writer waiting on a prune writes", func() {
		dsn, store, _, _ := tracedStore(WithPartitions(4))
		conn := admin(dsn)
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			return tx.PutFieldDelta("doc", "k", "body", 1, []byte{1})
		})).To(Succeed())

		pruned, release := make(chan struct{}), make(chan struct{})
		pruneDone := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			pruneDone <- store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
				if _, err := tx.PruneFieldDeltas(time.Now().Add(time.Hour)); err != nil {
					return err
				}
				close(pruned)
				<-release
				return nil
			})
		}()
		Eventually(pruned).Should(BeClosed())
		writeDone := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			writeDone <- store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
				return tx.PutFieldDelta("doc", "k", "body", 2, []byte{2})
			})
		}()
		// The writer waits on the workspace lock that the prune holds.
		Eventually(func() (int, error) {
			var n int
			err := conn.QueryRow(ctx,
				`SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database
				 WHERE l.locktype = 'advisory' AND NOT l.granted AND d.datname = current_database()`,
			).Scan(&n)
			return n, err
		}).Should(Equal(1))
		close(release)
		Eventually(pruneDone).Should(Receive(BeNil()))
		Eventually(writeDone).Should(Receive(BeNil()))

		Expect(store.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			frags, _, err := tx.FieldDeltas("doc", "k", "body", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(frags).To(Equal([][]byte{{2}}))
			return nil
		})).To(Succeed())
		Expect(fieldNames(conn, "a")).To(Equal([]string{"body"}))
	})

	It("collects idle consumers in one round trip and keeps the names of the others", func() {
		_, store, traced, tracer := tracedStore()
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for _, c := range []string{"idle", "busy"} {
				if err := tx.PutConsumerShadow(c, "doc", "k", []byte{1}); err != nil {
					return err
				}
				if err := tx.SetConsumerPosition(c, storage.ConsumerPosition{Seq: 1}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())

		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			Expect(tx.ConsumerShadow("busy", "doc", "k")).To(Equal([]byte{1}))
			tracer.reset()
			removed, err := tx.CollectIdleConsumers(time.Now().Add(time.Hour), 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(BeZero())
			Expect(tracer.trips()).To(Equal(1), "nothing is idle")

			// Only "idle" goes: it has fallen behind sequence 2, and so has
			// "busy", which the position written below keeps.
			Expect(tx.SetConsumerPosition("busy", storage.ConsumerPosition{Seq: 5})).To(Succeed())
			tracer.reset()
			removed, err = tx.CollectIdleConsumers(time.Now().Add(time.Hour), 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(Equal(1))
			Expect(tracer.trips()).To(Equal(1))

			tracer.reset()
			Expect(tx.PutConsumerShadow("busy", "doc", "k2", []byte{2})).To(Succeed())
			Expect(tracer.trips()).To(BeZero(), "the name of a consumer that stays is still known")
			return nil
		})).To(Succeed())

		Expect(store.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
			Expect(tx.ConsumerShadow("idle", "doc", "k")).To(BeNil())
			Expect(tx.ConsumerShadow("busy", "doc", "k")).To(Equal([]byte{1}))
			Expect(tx.ConsumerShadow("busy", "doc", "k2")).To(Equal([]byte{2}))
			return nil
		})).To(Succeed())
	})
})

// nameTracer counts the round trips of a pool and keeps the statements on
// text_fields and shadow_sets that it sends or queues.
type nameTracer struct {
	tripTracer
}

func (tr *nameTracer) keep(sql string, args []any) {
	if strings.Contains(sql, "text_fields") || strings.Contains(sql, "shadow_sets") {
		tr.stmts = append(tr.stmts, tracedQuery{sql: sql, args: args})
	}
}

func (tr *nameTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n++
	tr.keep(d.SQL, d.Args)
	return ctx
}

func (tr *nameTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n++
	for _, q := range d.Batch.QueuedQueries {
		tr.keep(q.SQL, q.Arguments)
	}
	return ctx
}
