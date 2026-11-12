package pgstore

import (
	"context"
	"errors"
	"fmt"

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
		for _, format := range []struct {
			name     string
			pageRows int
		}{{"keys in pages", 128}, {"keys in pages and chunks", 1}} {
			It("returns the counts and the row count in one round trip, with and without a cached scope ("+layout.name+", "+format.name+")", func() {
				setPostingFormat(postingChunkMax, postingPageBudget, format.pageRows)
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
	}

	It("reads keys in pages in no more round trips than keys in one chunk", func() {
		// p is in a page, one in one chunk, multi in several chunks.
		setPostingFormat(8, postingPageBudget, 4)
		dsn := pgtest.DSN()
		store, err := Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for i := range 12 {
				keys := [][]byte{[]byte("multi")}
				if i < 2 {
					keys = append(keys, []byte("p"))
				}
				if i >= 7 {
					keys = append(keys, []byte("one"))
				}
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, keys); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		var chunks, pages int
		Expect(store.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM posting_chunks WHERE k = 'one'),
			(SELECT count(*) FROM posting_pages)`).Scan(&chunks, &pages)).To(Succeed())
		Expect([]int{chunks, pages}).To(Equal([]int{1, 1}))

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
		for _, c := range []struct {
			name      string
			all, anyR []storage.IndexRange
			rows      int
			trips     int
		}{
			{"a key in a page", []storage.IndexRange{one("p")}, nil, 2, 2},
			{"a key in one chunk", []storage.IndexRange{one("one")}, nil, 5, 2},
			// An empty result reads no row keys.
			{"a key in a page and a key in one chunk", []storage.IndexRange{one("p"), one("one")}, nil, 0, 1},
			{"a range over a page and a chunk", []storage.IndexRange{{Lo: []byte("o"), Hi: []byte("q")}}, nil, 7, 2},
			{"any of a page and a chunk", nil, []storage.IndexRange{one("p"), one("one")}, 7, 2},
			{"a key in several chunks", []storage.IndexRange{one("multi")}, nil, 12, 3},
			{"a key in a page and a key in several chunks", []storage.IndexRange{one("p"), one("multi")}, nil, 2, 3},
		} {
			Expect(traced.WithReadTx(ctx, "a", func(tx storage.ReadTx) error {
				_, err := tx.CurrentSeq()
				Expect(err).NotTo(HaveOccurred())
				// The first scan caches the scope id.
				Expect(tx.ScanPostings("doc", "search", c.all, c.anyR, 0, func(string) error { return nil })).To(Succeed())
				tracer.reset()
				n := 0
				Expect(tx.ScanPostings("doc", "search", c.all, c.anyR, 0, func(string) error { n++; return nil })).To(Succeed())
				Expect(n).To(Equal(c.rows), c.name)
				Expect(tracer.trips()).To(Equal(c.trips), c.name)
				return nil
			})).To(Succeed())
		}
	})

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

		// Page splits and merges, moves between pages and chunks, and two
		// indexes in one transaction still cost one read.
		was, wasRows, wasChunk := postingPageBudget, postingPageMaxRows, postingChunkMax
		postingPageBudget, postingPageMaxRows, postingChunkMax = 24, 4, 2
		DeferCleanup(func() { postingPageBudget, postingPageMaxRows, postingChunkMax = was, wasRows, wasChunk })
		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for i := range 10 {
				keys := [][]byte{[]byte("big"), []byte(fmt.Sprintf("k%d", i))}
				for _, idx := range []string{"search", "other"} {
					if err := tx.PutPostings("doc", idx, fmt.Sprintf("s%d", i), nil, keys); err != nil {
						return err
					}
				}
			}
			return nil
		})).To(Succeed())
		Expect(traced.WithWriteTx(ctx, "a", func(tx storage.WriteTx) error {
			for i := range 10 {
				for _, idx := range []string{"search", "other"} {
					if err := tx.DeletePostings("doc", idx, fmt.Sprintf("s%d", i), [][]byte{[]byte("big"), []byte(fmt.Sprintf("k%d", i))}); err != nil {
						return err
					}
					if err := tx.PutPostings("doc", idx, fmt.Sprintf("t%d", i), nil, [][]byte{[]byte(fmt.Sprintf("j%d", i))}); err != nil {
						return err
					}
				}
			}
			t := tx.(*pgTx)
			tracer.reset()
			Expect(t.resolvePostings()).To(Succeed())
			Expect(tracer.trips()).To(Equal(1), "one read for pages and chunks of both indexes")
			return rollback
		})).To(MatchError(rollback))
	})
})
