package pgstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The conformance suite pins what the posting methods return. These specs pin
// the chunk format under them: they make chunks small, so that a few rows
// split and merge them, and check the stored tuples as well as the answers.
var _ = Describe("Postgres posting chunks", func() {
	ctx := context.Background()

	// smallChunks makes chunks hold n row numbers for the rest of the spec.
	smallChunks := func(n int) {
		was := postingChunkMax
		postingChunkMax = n
		DeferCleanup(func() { postingChunkMax = was })
	}

	openStore := func(opts ...Option) *Store {
		GinkgoHelper()
		store, err := Open(ctx, pgtest.DSN(), opts...)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		return store
	}

	write := func(store *Store, ws string, fn func(tx storage.WriteTx) error) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, ws, fn)).To(Succeed())
	}

	scan := func(store *Store, ws string, all, anyR []storage.IndexRange, limit int) []string {
		GinkgoHelper()
		var out []string
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanPostings("doc", "search", all, anyR, limit, func(key string) error {
				out = append(out, key)
				return nil
			})
		})).To(Succeed())
		sort.Strings(out)
		return out
	}

	counts := func(store *Store, ws string, ranges []storage.IndexRange, max int) []int {
		GinkgoHelper()
		var out []int
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			out, _, err = tx.PostingCounts("doc", "search", ranges, max)
			return err
		})).To(Succeed())
		return out
	}

	one := func(k []byte) storage.IndexRange {
		return storage.IndexRange{Lo: k, Hi: append(slices.Clone(k), 0)}
	}

	It("keeps more rows under one key than several chunks hold, and splits and merges them", func() {
		smallChunks(4)
		store := openStore()
		term := []byte("t")
		var want []string
		for i := range 30 {
			key := fmt.Sprintf("r%02d", i)
			want = append(want, key)
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings("doc", "search", key, nil, [][]byte{term})
			})
			checkChunks(ctx, store)
		}
		Expect(scan(store, "ws", []storage.IndexRange{one(term)}, nil, 0)).To(Equal(want))
		Expect(counts(store, "ws", []storage.IndexRange{one(term)}, 0)).To(Equal([]int{30}))
		Expect(chunkCount(ctx, store)).To(BeNumerically(">=", 8))

		// Remove from the middle until the chunks merge, then all of them.
		for i := 5; i < 25; i++ {
			key := fmt.Sprintf("r%02d", i)
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.DeletePostings("doc", "search", key, [][]byte{term})
			})
			checkChunks(ctx, store)
		}
		Expect(scan(store, "ws", []storage.IndexRange{one(term)}, nil, 0)).To(HaveLen(10))
		Expect(chunkCount(ctx, store)).To(BeNumerically("<=", 5))
		for _, i := range []int{0, 1, 2, 3, 4, 25, 26, 27, 28, 29} {
			key := fmt.Sprintf("r%02d", i)
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings("doc", "search", key, [][]byte{term}, nil)
			})
			checkChunks(ctx, store)
		}
		Expect(chunkCount(ctx, store)).To(BeZero())
		Expect(counts(store, "ws", []storage.IndexRange{one(term)}, 0)).To(Equal([]int{0}))
	})

	It("merges a small chunk into the chunk before or after it", func() {
		smallChunks(8)
		store := openStore()
		term := []byte("t")
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 40 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, [][]byte{term}); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(chunkCount(ctx, store)).To(Equal(5))
		// One removal per transaction, so each one sees only its own chunk and
		// the chunks next to it.
		var left []string
		for i := range 40 {
			key := fmt.Sprintf("r%02d", i)
			if i%8 == 0 {
				left = append(left, key)
				continue
			}
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings("doc", "search", key, [][]byte{term}, nil)
			})
			checkChunks(ctx, store)
		}
		Expect(scan(store, "ws", []storage.IndexRange{one(term)}, nil, 0)).To(Equal(left))
		Expect(chunkCount(ctx, store)).To(BeNumerically("<=", 2))
	})

	It("splits a chunk in the middle when a row of it gains the key", func() {
		smallChunks(4)
		store := openStore()
		a, b := []byte("a"), []byte("b")
		// Rows 0..11 hold b; rows 0..11 get a in a second pass, in reverse,
		// so every insert lands below the last chunk.
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 12 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, [][]byte{b}); err != nil {
					return err
				}
			}
			return nil
		})
		for i := 11; i >= 0; i-- {
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, [][]byte{a})
			})
			checkChunks(ctx, store)
		}
		Expect(scan(store, "ws", []storage.IndexRange{one(a), one(b)}, nil, 0)).To(HaveLen(12))
	})

	It("stores keys bytewise, with 0x00 and 0xff bytes and at the longest length", func() {
		store := openStore()
		long := bytes.Repeat([]byte{0xff}, storage.MaxIndexEntryBytes)
		keys := [][]byte{{0x00}, {0x00, 0x00}, {0x61}, {0x61, 0x00}, {0xff}, {0xff, 0x00}, long}
		write(store, "ws", func(tx storage.WriteTx) error {
			for i, k := range keys {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%d", i), nil, [][]byte{k}); err != nil {
					return err
				}
			}
			return nil
		})
		for i, k := range keys {
			Expect(scan(store, "ws", []storage.IndexRange{one(k)}, nil, 0)).To(Equal([]string{fmt.Sprintf("r%d", i)}), "key %x", k)
		}
		Expect(counts(store, "ws", []storage.IndexRange{
			{Lo: []byte{0x00}, Hi: []byte{0x01}},
			{Lo: []byte{0xff}},
			{Hi: []byte{0x61, 0x00}},
			{},
		}, 0)).To(Equal([]int{2, 3, 3, 7}))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Lo: []byte{0xff}}}, 0)).To(Equal([]string{"r4", "r5", "r6"}))
	})

	It("keeps two workspaces apart and empties one on DeleteWorkspace", func() {
		store := openStore()
		for _, ws := range []string{"ws1", "ws2"} {
			write(store, ws, func(tx storage.WriteTx) error {
				return tx.PutPostings("doc", "search", "row-"+ws, nil, [][]byte{[]byte("x")})
			})
		}
		Expect(scan(store, "ws1", nil, []storage.IndexRange{{}}, 0)).To(Equal([]string{"row-ws1"}))
		Expect(store.DeleteWorkspace(ctx, "ws1")).To(Succeed())
		Expect(scan(store, "ws1", nil, []storage.IndexRange{{}}, 0)).To(BeEmpty())
		Expect(scan(store, "ws2", nil, []storage.IndexRange{{}}, 0)).To(Equal([]string{"row-ws2"}))
		var chunks, rows, scopes int
		Expect(store.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM posting_chunks), (SELECT count(*) FROM posting_rows),
			(SELECT count(*) FROM posting_scopes)`).Scan(&chunks, &rows, &scopes)).To(Succeed())
		Expect([]int{chunks, rows, scopes}).To(Equal([]int{1, 1, 1}), "only ws2's tuples are left")
	})

	It("rewinds logged calls with a savepoint, also after a read wrote them", func() {
		store := openStore()
		x, y := []byte("x"), []byte("y")
		write(store, "ws", func(tx storage.WriteTx) error {
			Expect(tx.PutPostings("doc", "search", "kept", nil, [][]byte{x})).To(Succeed())
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutPostings("doc", "search", "lost", nil, [][]byte{x})).To(Succeed())
				return errors.New("rejected")
			})).To(HaveOccurred())
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutPostings("doc", "search", "lost2", nil, [][]byte{y})).To(Succeed())
				// The read writes every logged call, also "kept" from before
				// the savepoint, and the rewind undoes that write.
				n, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{{}}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal([]int{2}))
				return errors.New("rejected")
			})).To(HaveOccurred())
			return tx.PutPostings("doc", "search", "also", nil, [][]byte{y})
		})
		Expect(scan(store, "ws", nil, []storage.IndexRange{{}}, 0)).To(Equal([]string{"also", "kept"}))
		checkChunks(ctx, store)
	})

	It("reads and writes the current scope when another node cleared the index", func() {
		// Each store caches scope ids. Two stores over one database stand in
		// for two nodes: b drops the scope a has cached.
		dsn := pgtest.DSN()
		a, err := Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(a.Close()).To(Succeed()) })
		b, err := Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(b.Close()).To(Succeed()) })
		x, y := []byte("x"), []byte("y")

		write(a, "ws", func(tx storage.WriteTx) error {
			return tx.PutPostings("doc", "search", "old", nil, [][]byte{x})
		})
		Expect(scan(a, "ws", nil, []storage.IndexRange{{}}, 0)).To(Equal([]string{"old"}))

		write(b, "ws", func(tx storage.WriteTx) error {
			if err := tx.ClearPostings("doc", "search"); err != nil {
				return err
			}
			return tx.PutPostings("doc", "search", "new", nil, [][]byte{y})
		})
		Expect(scan(a, "ws", nil, []storage.IndexRange{{}}, 0)).To(Equal([]string{"new"}))
		Expect(counts(a, "ws", []storage.IndexRange{one(x), one(y)}, 0)).To(Equal([]int{0, 1}))

		write(b, "ws", func(tx storage.WriteTx) error { return tx.ClearPostings("doc", "search") })
		write(a, "ws", func(tx storage.WriteTx) error {
			return tx.PutPostings("doc", "search", "again", [][]byte{y}, [][]byte{x})
		})
		Expect(scan(b, "ws", nil, []storage.IndexRange{{}}, 0)).To(Equal([]string{"again"}))

		Expect(b.DeleteWorkspace(ctx, "ws")).To(Succeed())
		Expect(counts(a, "ws", []storage.IndexRange{{}}, 0)).To(Equal([]int{0}))
		Expect(scan(a, "ws", nil, []storage.IndexRange{{}}, 0)).To(BeEmpty())
		checkChunks(ctx, a)
	})

	It("gives a row that comes back after DeletePostings a new number", func() {
		store := openStore()
		x, y := []byte("x"), []byte("y")
		write(store, "ws", func(tx storage.WriteTx) error {
			return tx.PutPostings("doc", "search", "r", nil, [][]byte{x, y})
		})
		write(store, "ws", func(tx storage.WriteTx) error {
			// held names only x: the stale y must not come back with the row.
			Expect(tx.DeletePostings("doc", "search", "r", [][]byte{x})).To(Succeed())
			return tx.PutPostings("doc", "search", "r", nil, [][]byte{x})
		})
		Expect(scan(store, "ws", []storage.IndexRange{one(x)}, nil, 0)).To(Equal([]string{"r"}))
		Expect(scan(store, "ws", []storage.IndexRange{one(y)}, nil, 0)).To(BeEmpty())
	})

	It("gives readers on a snapshot the chunks as they were while a writer changes them", func() {
		smallChunks(4)
		store := openStore()
		term := []byte("t")
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 20 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, [][]byte{term}); err != nil {
					return err
				}
			}
			return nil
		})
		var wg sync.WaitGroup
		stop := make(chan struct{})
		errs := make(chan error, 8)
		for range 4 {
			wg.Go(func() {
				defer GinkgoRecover()
				for {
					select {
					case <-stop:
						return
					default:
					}
					err := store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
						first, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{one(term)}, 0)
						if err != nil {
							return err
						}
						var seen int
						if err := tx.ScanPostings("doc", "search", []storage.IndexRange{one(term)}, nil, 0, func(string) error {
							seen++
							return nil
						}); err != nil {
							return err
						}
						if seen != first[0] {
							return fmt.Errorf("snapshot counted %d and scanned %d", first[0], seen)
						}
						return nil
					})
					if err != nil {
						errs <- err
						return
					}
				}
			})
		}
		rng := rand.New(rand.NewPCG(1, 2))
		for range 60 {
			write(store, "ws", func(tx storage.WriteTx) error {
				for range 3 {
					key := fmt.Sprintf("r%02d", rng.IntN(40))
					if rng.IntN(2) == 0 {
						if err := tx.PutPostings("doc", "search", key, nil, [][]byte{term}); err != nil {
							return err
						}
					} else if err := tx.PutPostings("doc", "search", key, [][]byte{term}, nil); err != nil {
						return err
					}
				}
				return nil
			})
		}
		close(stop)
		wg.Wait()
		close(errs)
		for err := range errs {
			Expect(err).NotTo(HaveOccurred())
		}
		checkChunks(ctx, store)
	})

	DescribeTable("agree with an in-memory model under random calls",
		func(seed uint64, opts ...Option) {
			smallChunks(4)
			store := openStore(opts...)
			checkModel(ctx, store, rand.New(rand.NewPCG(seed, seed*7+1)), 150)
		},
		Entry("unpartitioned", uint64(1)),
		Entry("unpartitioned, other seed", uint64(2)),
		Entry("partitioned", uint64(3), WithPartitions(2)),
	)
})

// postingModel is the posting index as the contract defines it.
type postingModel map[string]map[string]bool // row key -> posting key -> held

func (m postingModel) clone() postingModel {
	out := postingModel{}
	for r, ks := range m {
		c := map[string]bool{}
		for k := range ks {
			c[k] = true
		}
		out[r] = c
	}
	return out
}

func (m postingModel) put(row string, remove, add [][]byte) {
	ks := m[row]
	if ks == nil {
		ks = map[string]bool{}
		m[row] = ks
	}
	for _, k := range remove {
		delete(ks, string(k))
	}
	for _, k := range add {
		ks[string(k)] = true
	}
	if len(ks) == 0 {
		delete(m, row)
	}
}

func (m postingModel) held(row string) [][]byte {
	var out [][]byte
	for k := range m[row] {
		out = append(out, []byte(k))
	}
	return out
}

func inRange(k string, r storage.IndexRange) bool {
	return (r.Lo == nil || k >= string(r.Lo)) && (r.Hi == nil || k < string(r.Hi))
}

func (m postingModel) count(r storage.IndexRange) int {
	n := 0
	for _, ks := range m {
		for k := range ks {
			if inRange(k, r) {
				n++
			}
		}
	}
	return n
}

func (m postingModel) scan(all, anyR []storage.IndexRange) []string {
	var out []string
	for row, ks := range m {
		holds := func(r storage.IndexRange) bool {
			for k := range ks {
				if inRange(k, r) {
					return true
				}
			}
			return false
		}
		match := len(all) > 0
		for _, r := range all {
			match = match && holds(r)
		}
		for _, r := range anyR {
			match = match || holds(r)
		}
		if match {
			out = append(out, row)
		}
	}
	sort.Strings(out)
	return out
}

var modelKeys = [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0xff}, {0x61}, {0x61, 0x00}, {0x62}, {0xff}, {0xff, 0xff}}

func randomRange(rng *rand.Rand) storage.IndexRange {
	pick := func() []byte {
		if rng.IntN(5) == 0 {
			return nil
		}
		return modelKeys[rng.IntN(len(modelKeys))]
	}
	if rng.IntN(2) == 0 {
		k := modelKeys[rng.IntN(len(modelKeys))]
		return storage.IndexRange{Lo: k, Hi: append(slices.Clone(k), 0)}
	}
	return storage.IndexRange{Lo: pick(), Hi: pick()}
}

func randomKeys(rng *rand.Rand, max int) [][]byte {
	var out [][]byte
	for range rng.IntN(max + 1) {
		out = append(out, modelKeys[rng.IntN(len(modelKeys))])
	}
	return out
}

// checkModel runs random write transactions against store and against a
// model, and after each one compares reads and the stored chunks.
func checkModel(ctx context.Context, store *Store, rng *rand.Rand, txs int) {
	GinkgoHelper()
	workspaces := []string{"a", "b"}
	models := map[string]postingModel{"a": {}, "b": {}}
	rowKey := func() string { return fmt.Sprintf("row%02d", rng.IntN(24)) }
	for range txs {
		ws := workspaces[rng.IntN(len(workspaces))]
		next := models[ws].clone()
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for range 1 + rng.IntN(8) {
				switch op := rng.IntN(20); {
				case op < 12:
					row := rowKey()
					remove := randomKeys(rng, 2)
					if rng.IntN(2) == 0 {
						remove = append(remove, next.held(row)...)
					}
					add := randomKeys(rng, 3)
					if err := tx.PutPostings("doc", "search", row, remove, add); err != nil {
						return err
					}
					next.put(row, remove, add)
				case op < 16:
					row := rowKey()
					if err := tx.DeletePostings("doc", "search", row, next.held(row)); err != nil {
						return err
					}
					delete(next, row)
				case op < 17:
					if err := tx.ClearPostings("doc", "search"); err != nil {
						return err
					}
					next = postingModel{}
				case op < 19:
					// A rejected mutation: the calls inside are rewound.
					saved := next.clone()
					err := tx.InSavepoint(func() error {
						row := rowKey()
						add := randomKeys(rng, 3)
						if err := tx.PutPostings("doc", "search", row, nil, add); err != nil {
							return err
						}
						next.put(row, nil, add)
						if rng.IntN(2) == 0 {
							r := randomRange(rng)
							n, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{r}, 0)
							if err != nil {
								return err
							}
							Expect(n).To(Equal([]int{next.count(r)}))
						}
						return errors.New("rejected")
					})
					if err == nil || err.Error() != "rejected" {
						return fmt.Errorf("savepoint: %v", err)
					}
					next = saved
				default:
					// A read inside the write transaction sees its own calls.
					all, anyR := []storage.IndexRange{randomRange(rng)}, []storage.IndexRange{randomRange(rng)}
					var got []string
					if err := tx.ScanPostings("doc", "search", all, anyR, 0, func(k string) error {
						got = append(got, k)
						return nil
					}); err != nil {
						return err
					}
					sort.Strings(got)
					Expect(got).To(Equal(next.scan(all, anyR)))
				}
			}
			return nil
		})).To(Succeed())
		models[ws] = next
		checkChunks(ctx, store)

		for _, ws := range workspaces {
			m := models[ws]
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				ranges := []storage.IndexRange{randomRange(rng), randomRange(rng), randomRange(rng), {}}
				got, _, err := tx.PostingCounts("doc", "search", ranges, 0)
				Expect(err).NotTo(HaveOccurred())
				for i, r := range ranges {
					Expect(got[i]).To(Equal(m.count(r)), "range %x..%x", r.Lo, r.Hi)
				}
				capped, _, err := tx.PostingCounts("doc", "search", ranges, 2)
				Expect(err).NotTo(HaveOccurred())
				for i, r := range ranges {
					if want := m.count(r); want <= 2 {
						Expect(capped[i]).To(Equal(want))
					} else {
						Expect(capped[i]).To(BeNumerically(">", 2))
					}
				}
				var all, anyR []storage.IndexRange
				for range rng.IntN(3) {
					all = append(all, randomRange(rng))
				}
				for range rng.IntN(3) {
					anyR = append(anyR, randomRange(rng))
				}
				collect := func(limit int) []string {
					var out []string
					Expect(tx.ScanPostings("doc", "search", all, anyR, limit, func(k string) error {
						out = append(out, k)
						return nil
					})).To(Succeed())
					sort.Strings(out)
					return out
				}
				want := m.scan(all, anyR)
				Expect(collect(0)).To(Equal(want), "all %v any %v", all, anyR)
				limited := collect(3)
				Expect(limited).To(HaveLen(min(3, len(want))))
				for _, k := range limited {
					Expect(want).To(ContainElement(k))
				}
				Expect(collect(3)).To(Equal(limited), "a limited scan is stable")
				return nil
			})).To(Succeed())
		}
	}
}

func chunkCount(ctx context.Context, store *Store) int {
	GinkgoHelper()
	var n int
	Expect(store.pool.QueryRow(ctx, `SELECT count(*) FROM posting_chunks WHERE chunk >= 0`).Scan(&n)).To(Succeed())
	return n
}

// checkChunks checks the format of every stored chunk: each key with postings
// has a chunk 0, which is the only chunk that may be empty and only while the
// key has other chunks; a header exists exactly when a key has several chunks, and
// lists them and their total; each chunk holds sorted numbers within its span,
// as many as its n says, and no more than a chunk may hold; and each number
// has a row.
func checkChunks(ctx context.Context, store *Store) {
	GinkgoHelper()
	rows, err := store.pool.Query(ctx, `SELECT scope, k, chunk, n, ids FROM posting_chunks ORDER BY scope, k, chunk`)
	Expect(err).NotTo(HaveOccurred())
	type keyRef struct {
		scope int32
		k     string
	}
	type tuple struct {
		chunk int64
		n     int32
		ids   []byte
	}
	byKey := map[keyRef][]tuple{}
	var refs []keyRef
	for rows.Next() {
		var (
			ref keyRef
			k   []byte
			tp  tuple
		)
		Expect(rows.Scan(&ref.scope, &k, &tp.chunk, &tp.n, &tp.ids)).To(Succeed())
		ref.k = string(k)
		if _, ok := byKey[ref]; !ok {
			refs = append(refs, ref)
		}
		byKey[ref] = append(byKey[ref], tp)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	nums := map[int32]map[int64]bool{}
	numRows, err := store.pool.Query(ctx, `SELECT scope, num FROM posting_rows`)
	Expect(err).NotTo(HaveOccurred())
	for numRows.Next() {
		var (
			scope int32
			num   int64
		)
		Expect(numRows.Scan(&scope, &num)).To(Succeed())
		if nums[scope] == nil {
			nums[scope] = map[int64]bool{}
		}
		nums[scope][num] = true
	}
	Expect(numRows.Err()).NotTo(HaveOccurred())

	for _, ref := range refs {
		tuples := byKey[ref]
		var header *tuple
		if tuples[0].chunk == postingHeader {
			header = &tuples[0]
			tuples = tuples[1:]
		}
		Expect(tuples).NotTo(BeEmpty(), "key %x has only a header", ref.k)
		Expect(tuples[0].chunk).To(BeZero(), "key %x has no chunk 0", ref.k)
		var starts []int64
		total := 0
		for i, tp := range tuples {
			starts = append(starts, tp.chunk)
			ids, err := decodeIDs(tp.ids, tp.chunk, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).To(HaveLen(int(tp.n)), "key %x chunk %d", ref.k, tp.chunk)
			Expect(len(ids)).To(BeNumerically("<=", postingChunkMax))
			Expect(slices.IsSorted(ids)).To(BeTrue())
			Expect(slices.Compact(slices.Clone(ids))).To(HaveLen(len(ids)))
			if i > 0 || len(tuples) == 1 {
				// Only chunk 0 may stay empty, while the key has other chunks.
				Expect(ids).NotTo(BeEmpty(), "key %x chunk %d is empty", ref.k, tp.chunk)
			}
			for _, id := range ids {
				Expect(id).To(BeNumerically(">=", tp.chunk))
				if i+1 < len(tuples) {
					Expect(id).To(BeNumerically("<", tuples[i+1].chunk))
				}
				Expect(nums[ref.scope][id]).To(BeTrue(), "number %d of key %x has no row", id, ref.k)
			}
			total += len(ids)
		}
		if len(tuples) == 1 {
			Expect(header).To(BeNil(), "key %x has one chunk and a header", ref.k)
			continue
		}
		Expect(header).NotTo(BeNil(), "key %x has %d chunks and no header", ref.k, len(tuples))
		Expect(int(header.n)).To(Equal(total), "header total of key %x", ref.k)
		listed, err := decodeIDs(header.ids, 0, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(Equal(starts), "header chunk list of key %x", ref.k)
	}
}
