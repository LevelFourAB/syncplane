package pgstore

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("Postgres posting counts", func() {
	ctx := context.Background()

	for _, layout := range []struct {
		name string
		opts []Option
	}{{"unpartitioned", nil}, {"partitioned", []Option{WithPartitions(4)}}} {
		It("returns the counts and the row count in one round trip, with and without a cached scope ("+layout.name+")", func() {
			dsn := pgtest.DSN()
			store, err := Open(ctx, dsn, layout.opts...)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
				for _, row := range []string{"r1", "r2", "r3"} {
					if err := tx.PutPostings("doc", "search", row, nil, [][]byte{[]byte("common")}); err != nil {
						return err
					}
				}
				if err := tx.PutPostings("doc", "search", "r1", nil, [][]byte{[]byte("rare")}); err != nil {
					return err
				}
				return tx.DeletePostings("doc", "search", "r3", [][]byte{[]byte("common")})
			})).To(Succeed())

			cfg, err := pgxpool.ParseConfig(dsn)
			Expect(err).NotTo(HaveOccurred())
			tracer := &tripTracer{}
			cfg.ConnConfig.Tracer = tracer
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(pool.Close)
			traced := &Store{pool: pool, scopes: newScopeCache()}

			one := func(k string) storage.IndexRange {
				return storage.IndexRange{Lo: []byte(k), Hi: []byte(k + "\x00")}
			}
			ranges := []storage.IndexRange{one("common"), one("rare"), one("none")}
			for _, cache := range []string{"cold", "warm"} {
				Expect(traced.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
					// The first statement of a transaction can carry its BEGIN,
					// so one read goes first.
					_, err := tx.CurrentSeq()
					Expect(err).NotTo(HaveOccurred())
					tracer.reset()
					got, rows, err := tx.PostingCounts("doc", "search", ranges, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(Equal([]int{2, 1, 0}), cache)
					Expect(rows).To(Equal(2), cache)
					Expect(tracer.trips()).To(Equal(1), cache)

					tracer.reset()
					got, rows, err = tx.PostingCounts("doc", "unknown", ranges, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(Equal([]int{0, 0, 0}), cache)
					Expect(rows).To(BeZero(), cache)
					Expect(tracer.trips()).To(Equal(1), "an index with no scope, "+cache)
					return nil
				})).To(Succeed())
			}

			// A scope id that the cache holds stale, after a clear on another
			// store, costs one more round trip and gives the current answer.
			Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
				if err := tx.ClearPostings("doc", "search"); err != nil {
					return err
				}
				return tx.PutPostings("doc", "search", "r9", nil, [][]byte{[]byte("rare")})
			})).To(Succeed())
			Expect(traced.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
				got, rows, err := tx.PostingCounts("doc", "search", ranges, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal([]int{0, 1, 0}))
				Expect(rows).To(Equal(1))
				return nil
			})).To(Succeed())
		})
	}

	It("adds no round trip to a write", func() {
		dsn := pgtest.DSN()
		cfg, err := pgxpool.ParseConfig(dsn)
		Expect(err).NotTo(HaveOccurred())
		store, err := Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		tracer := &tripTracer{}
		cfg.ConnConfig.Tracer = tracer
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(pool.Close)
		traced := &Store{pool: pool, scopes: newScopeCache()}
		rollback := errors.New("roll back")
		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			Expect(tx.PutPostings("doc", "search", "r1", nil, [][]byte{[]byte("a")})).To(Succeed())
			return nil
		})).To(Succeed())
		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			Expect(tx.PutPostings("doc", "search", "r2", nil, [][]byte{[]byte("a")})).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "r1", [][]byte{[]byte("a")})).To(Succeed())
			t := tx.(*pgTx)
			tracer.reset()
			Expect(t.resolvePostings()).To(Succeed())
			Expect(tracer.trips()).To(Equal(1), "one read; the writes, the row count with them, wait in the queue")
			return rollback
		})).To(MatchError(rollback))
	})
})
