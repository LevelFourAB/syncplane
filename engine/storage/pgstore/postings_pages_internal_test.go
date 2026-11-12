package pgstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// These specs pin the page format: they make pages and the row limit of a
// page key small, so that a few rows split, merge and move them, and check
// the stored pages as well as the answers.
var _ = Describe("Postgres posting pages", func() {
	ctx := context.Background()

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

	put := func(store *Store, ws, row string, keys ...string) {
		GinkgoHelper()
		write(store, ws, func(tx storage.WriteTx) error {
			return tx.PutPostings("doc", "search", row, nil, byteKeys(keys...))
		})
	}

	scan := func(store *Store, ws string, all, anyR []storage.IndexRange) []string {
		GinkgoHelper()
		var out []string
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanPostings("doc", "search", all, anyR, 0, func(key string) error {
				out = append(out, key)
				return nil
			})
		})).To(Succeed())
		sort.Strings(out)
		return out
	}

	counts := func(store *Store, ws string, max int, ranges ...storage.IndexRange) []int {
		GinkgoHelper()
		var out []int
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			out, _, err = tx.PostingCounts("doc", "search", ranges, max)
			return err
		})).To(Succeed())
		return out
	}

	one := func(k string) storage.IndexRange {
		return storage.IndexRange{Lo: []byte(k), Hi: []byte(k + "\x00")}
	}
	prefix := func(p string) storage.IndexRange {
		hi := []byte(p)
		hi[len(hi)-1]++
		return storage.IndexRange{Lo: []byte(p), Hi: hi}
	}

	It("splits a page that passes the budget into parts of about equal size", func() {
		var entries []pageEntry
		for i := range 11 {
			entries = append(entries, pageEntry{k: fmt.Appendf(nil, "k%02d", i), nums: []int64{int64(i)}})
		}
		// Ten keys fill the budget to the byte.
		setPostingFormat(postingChunkMax, pageLen(entries[:10]), 128)
		store := openStore()
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 10 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, byteKeys(fmt.Sprintf("k%02d", i))); err != nil {
					return err
				}
			}
			return nil
		})
		pages := storedPages(ctx, store)
		Expect(pages).To(HaveLen(1), "a page at the budget does not split")
		Expect(pages[0].lo).To(BeEmpty(), "the first page starts at the empty key")
		Expect(pages[0].size).To(Equal(postingPageBudget))

		put(store, "ws", "r10", "k10")
		pages = storedPages(ctx, store)
		Expect(pages).To(HaveLen(2), "one byte past the budget splits")
		Expect(pages[0].lo).To(BeEmpty())
		Expect(pages[1].lo).To(Equal(pages[1].keys[0]), "a part after the first starts at its first key")
		for _, pg := range pages {
			Expect(pg.size).To(BeNumerically("<=", postingPageBudget))
		}
		Expect(pages[0].size-pages[1].size).To(BeNumerically("~", 0, 8), "parts of about equal size")
		Expect(slices.Concat(pages[0].keys, pages[1].keys)).To(HaveLen(11))
		checkChunks(ctx, store)
	})

	It("merges a page into the page before it when a removal leaves it below a quarter of the budget", func() {
		setPostingFormat(postingChunkMax, 64, 128)
		store := openStore()
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 24 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, byteKeys(fmt.Sprintf("k%02d", i))); err != nil {
					return err
				}
			}
			return nil
		})
		// The write splits the keys into k00..k11 and k12..k23. The removals
		// first make room in the first page, then shrink the second one
		// until it merges, then take the rest.
		order := []int{1, 2, 3, 4, 5, 6, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 0, 7, 8, 9, 10, 11, 12, 23}
		var merged, stayed int
		for n, i := range order {
			before := storedPages(ctx, store)
			gone := fmt.Appendf(nil, "k%02d", i)
			j := slices.IndexFunc(before, func(pg storedPage) bool {
				return slices.ContainsFunc(pg.keys, func(k []byte) bool { return bytes.Equal(k, gone) })
			})
			Expect(j).To(BeNumerically(">=", 0))
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.DeletePostings("doc", "search", fmt.Sprintf("r%02d", i), [][]byte{gone})
			})
			pg := before[j]
			left := slices.DeleteFunc(slices.Clone(pg.entries), func(e pageEntry) bool { return bytes.Equal(e.k, gone) })
			want := slices.Clone(before)
			switch size := pageLen(left); {
			case len(left) == 0:
				want = slices.Delete(want, j, j+1)
			case size < postingPageBudget/4 && j > 0 && before[j-1].size+size <= postingPageBudget:
				merged++
				want[j-1].keys = append(slices.Clone(before[j-1].keys), keysOf(left)...)
				want = slices.Delete(want, j, j+1)
			default:
				if size >= postingPageBudget/4 {
					stayed++
				}
				want[j].keys = keysOf(left)
			}
			after := storedPages(ctx, store)
			Expect(loKeys(after)).To(Equal(loKeys(want)), "after the removal of %s", gone)
			checkChunks(ctx, store)
			Expect(counts(store, "ws", 0, storage.IndexRange{})).To(Equal([]int{23 - n}))
		}
		Expect(storedPages(ctx, store)).To(BeEmpty())
		Expect(merged).To(BeNumerically(">", 0))
		Expect(stayed).To(BeNumerically(">", 0))
	})

	DescribeTable("moves a key to chunks past the row limit and back below half of it",
		func(chunkMax, backAt int) {
			setPostingFormat(chunkMax, postingPageBudget, 8)
			store := openStore()
			// Neighbours keep the page of t alive when t leaves it.
			put(store, "ws", "n1", "s", "u")
			where := func() string {
				GinkgoHelper()
				var chunks, inPage bool
				Expect(store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM posting_chunks WHERE k = 't')`).Scan(&chunks)).To(Succeed())
				for _, pg := range storedPages(ctx, store) {
					inPage = inPage || slices.ContainsFunc(pg.keys, func(k []byte) bool { return string(k) == "t" })
				}
				Expect(chunks && inPage).To(BeFalse(), "t is in a page and in chunks")
				switch {
				case chunks:
					return "chunks"
				case inPage:
					return "page"
				}
				return ""
			}
			for i := 1; i <= 10; i++ {
				put(store, "ws", fmt.Sprintf("r%02d", i), "t")
				want := "page"
				if i > 8 {
					want = "chunks"
				}
				Expect(where()).To(Equal(want), "with %d rows", i)
				Expect(counts(store, "ws", 0, one("t"))).To(Equal([]int{i}))
				checkChunks(ctx, store)
			}
			for i := 10; i >= 1; i-- {
				write(store, "ws", func(tx storage.WriteTx) error {
					return tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), byteKeys("t"), nil)
				})
				left := i - 1
				want := "chunks"
				switch {
				case left == 0:
					want = ""
				case left <= backAt:
					want = "page"
				}
				Expect(where()).To(Equal(want), "with %d rows", left)
				Expect(scan(store, "ws", []storage.IndexRange{one("t")}, nil)).To(HaveLen(left))
				checkChunks(ctx, store)
			}
			Expect(scan(store, "ws", nil, []storage.IndexRange{{}})).To(Equal([]string{"n1"}))
		},
		Entry("with one chunk", 256, 3),
		Entry("with several chunks", 2, 2),
	)

	It("moves a key with many new rows straight to chunks, in one transaction with page edits", func() {
		setPostingFormat(4, 32, 8)
		store := openStore()
		put(store, "ws", "n1", "a", "b", "c")
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 30 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, byteKeys("b", fmt.Sprintf("x%02d", i))); err != nil {
					return err
				}
			}
			return tx.PutPostings("doc", "search", "n1", byteKeys("a"), nil)
		})
		Expect(counts(store, "ws", 0, one("a"), one("b"), one("c"), prefix("x"))).To(Equal([]int{0, 31, 1, 30}))
		Expect(chunkCount(ctx, store)).To(BeNumerically(">=", 8))
		checkChunks(ctx, store)
	})

	It("reads keys in pages and keys in chunks in one range", func() {
		setPostingFormat(postingChunkMax, postingPageBudget, 4)
		store := openStore()
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 10 {
				keys := []string{"ab"}
				if i < 2 {
					keys = append(keys, "ac")
				}
				if i == 9 {
					keys = append(keys, "ad", "b")
				}
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%d", i), nil, byteKeys(keys...)); err != nil {
					return err
				}
			}
			return tx.PutPostings("doc", "search", "s", nil, byteKeys("ac"))
		})
		Expect(chunkCount(ctx, store)).To(Equal(1), "ab is in chunks")
		Expect(storedPages(ctx, store)).To(HaveLen(1), "ac, ad and b are in a page")
		Expect(counts(store, "ws", 0, prefix("a"), one("ab"), one("ac"), storage.IndexRange{}, prefix("b"))).To(Equal([]int{14, 10, 3, 15, 1}))
		capped := counts(store, "ws", 3, prefix("a"), one("ac"))
		Expect(capped[0]).To(BeNumerically(">", 3))
		Expect(capped[1]).To(Equal(3))
		Expect(scan(store, "ws", []storage.IndexRange{prefix("a")}, nil)).To(HaveLen(11))
		Expect(scan(store, "ws", []storage.IndexRange{one("ab"), one("ac")}, nil)).To(Equal([]string{"r0", "r1"}))
		Expect(scan(store, "ws", []storage.IndexRange{one("ac"), {Lo: []byte("ab"), Hi: []byte("ac")}}, nil)).To(Equal([]string{"r0", "r1"}))
		Expect(scan(store, "ws", []storage.IndexRange{one("ab")}, []storage.IndexRange{one("ac")})).To(HaveLen(11))
		Expect(scan(store, "ws", []storage.IndexRange{{Lo: []byte("ac")}}, []storage.IndexRange{one("ad")})).To(Equal([]string{"r0", "r1", "r9", "s"}))
	})

	It("finds the first and the last page of a scope, and ranges beyond them", func() {
		setPostingFormat(postingChunkMax, 20, 128)
		store := openStore()
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 30 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, byteKeys(fmt.Sprintf("k%02d", i))); err != nil {
					return err
				}
			}
			return nil
		})
		pages := storedPages(ctx, store)
		Expect(len(pages)).To(BeNumerically(">=", 6))
		Expect(pages[0].lo).To(BeEmpty())
		for i := range 30 {
			k := fmt.Sprintf("k%02d", i)
			Expect(counts(store, "ws", 0, one(k))).To(Equal([]int{1}), k)
			Expect(scan(store, "ws", []storage.IndexRange{one(k)}, nil)).To(Equal([]string{fmt.Sprintf("r%02d", i)}), k)
		}
		Expect(counts(store, "ws", 0,
			storage.IndexRange{Hi: []byte("k00")},
			storage.IndexRange{Lo: []byte("a"), Hi: []byte("k00")},
			storage.IndexRange{Lo: []byte("k29\x00")},
			storage.IndexRange{Lo: []byte("l")},
			storage.IndexRange{},
			storage.IndexRange{Lo: []byte("k10"), Hi: []byte("k20")},
			storage.IndexRange{Lo: []byte("k20"), Hi: []byte("k10")},
		)).To(Equal([]int{0, 0, 0, 0, 30, 10, 0}))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Lo: []byte("k29\x00")}, {Hi: []byte("k00")}})).To(BeEmpty())

		// A key below every key goes into the first page; a key above every
		// key goes into the last.
		put(store, "ws", "low", "a")
		put(store, "ws", "high", "z")
		pages = storedPages(ctx, store)
		Expect(string(pages[0].keys[0])).To(Equal("a"))
		last := pages[len(pages)-1]
		Expect(string(last.keys[len(last.keys)-1])).To(Equal("z"))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Hi: []byte("k00")}, {Lo: []byte("k29\x00")}})).To(Equal([]string{"high", "low"}))
		checkChunks(ctx, store)

		// The first page keeps the empty key as lo when its first key goes.
		write(store, "ws", func(tx storage.WriteTx) error {
			return tx.DeletePostings("doc", "search", "low", byteKeys("a"))
		})
		Expect(storedPages(ctx, store)[0].lo).To(BeEmpty())
		Expect(counts(store, "ws", 0, storage.IndexRange{Hi: []byte("k01")}, one("k00"))).To(Equal([]int{1, 1}))
		checkChunks(ctx, store)
	})

	It("keeps a page of one key that is larger than the budget", func() {
		setPostingFormat(postingChunkMax, 32, 128)
		store := openStore()
		long := strings.Repeat("m", 100)
		put(store, "ws", "r1", "a", "b", long, "n", "o")
		pages := storedPages(ctx, store)
		var alone bool
		for _, pg := range pages {
			if len(pg.keys) == 1 && string(pg.keys[0]) == long {
				alone = true
				Expect(pg.size).To(BeNumerically(">", postingPageBudget))
			}
		}
		Expect(alone).To(BeTrue(), "the long key has a page of its own")
		Expect(counts(store, "ws", 0, one(long), prefix("m"), storage.IndexRange{})).To(Equal([]int{1, 1, 5}))
		checkChunks(ctx, store)
	})

	It("stores the longest key in pages and chunks under the longest names, with 64-bit row numbers", func() {
		store := openStore()
		ws := strings.Repeat("w", naming.MaxAppIDBytes+1+naming.MaxWorkspaceIDBytes)
		entity := strings.Repeat("e", naming.MaxEntityNameLength)
		idx := strings.Repeat("i", naming.MaxEntityNameLength)
		rowKey := func(i int) string {
			return strings.Repeat("k", naming.MaxRowKeyBytes-4) + fmt.Sprintf("%04d", i)
		}
		// Bytes that do not compress; k2 shares all but its last byte with k1.
		rng := rand.New(rand.NewPCG(1, 2))
		random := func() []byte {
			k := make([]byte, storage.MaxIndexEntryBytes)
			for i := range k {
				k[i] = byte(rng.Uint32())
			}
			return k
		}
		k1, k3 := random(), random()
		k2 := slices.Clone(k1)
		k2[len(k2)-1] ^= 0x55
		keys := [][]byte{k1, k2, k3}
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.PutPostings(entity, idx, rowKey(0), nil, keys)
		})).To(Succeed())
		// Row numbers past 2^62 take varints of nine bytes.
		_, err := store.pool.Exec(ctx, `UPDATE posting_scopes SET next_row = $1`, int64(1)<<62)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := 1; i < postingPageMaxRows; i++ {
				if err := tx.PutPostings(entity, idx, rowKey(i), nil, keys); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		exact := func(k []byte) storage.IndexRange {
			return storage.IndexRange{Lo: k, Hi: append(slices.Clone(k), 0)}
		}
		check := func(n int) {
			GinkgoHelper()
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				got, rows, err := tx.PostingCounts(entity, idx, []storage.IndexRange{exact(k1), exact(k2), exact(k3), {}}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal([]int{n, n, n, 3 * n}))
				Expect(rows).To(Equal(n))
				var seen []string
				Expect(tx.ScanPostings(entity, idx, []storage.IndexRange{exact(k1), exact(k2), exact(k3)}, nil, 0, func(key string) error {
					seen = append(seen, key)
					return nil
				})).To(Succeed())
				Expect(seen).To(HaveLen(n))
				return nil
			})).To(Succeed())
			checkChunks(ctx, store)
		}
		check(postingPageMaxRows)
		Expect(chunkCount(ctx, store)).To(BeZero(), "every key is in a page")

		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.PutPostings(entity, idx, rowKey(9999), nil, keys)
		})).To(Succeed())
		check(postingPageMaxRows + 1)
		Expect(storedPages(ctx, store)).To(BeEmpty(), "every key moved to chunks")

		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := range postingPageMaxRows - 1 {
				if err := tx.DeletePostings(entity, idx, rowKey(i), keys); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		check(2)
		Expect(chunkCount(ctx, store)).To(BeZero(), "every key moved back to pages")
	})

	DescribeTable("returns an error for a corrupt page body, and still clears the index and deletes the workspace",
		func(body []byte) {
			store := openStore()
			setup := func(ws string) {
				GinkgoHelper()
				put(store, ws, "r1", "a", "b")
				put(store, ws, "r2", "b", "c")
				tag, err := store.pool.Exec(ctx, `UPDATE posting_pages SET body = $2
					WHERE scope = (SELECT scope FROM posting_scopes WHERE workspace_id = $1)`, ws, body)
				Expect(err).NotTo(HaveOccurred())
				Expect(tag.RowsAffected()).To(BeEquivalentTo(1))
			}
			left := func(ws string) []int {
				GinkgoHelper()
				var c, p, r, s int
				Expect(store.pool.QueryRow(ctx, `SELECT
					(SELECT count(*) FROM posting_chunks), (SELECT count(*) FROM posting_pages),
					(SELECT count(*) FROM posting_rows), (SELECT count(*) FROM posting_scopes WHERE workspace_id = $1)`, ws).Scan(&c, &p, &r, &s)).To(Succeed())
				return []int{c, p, r, s}
			}
			setup("ws")
			Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				_, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{{}}, 0)
				return err
			})).To(MatchError(errCorruptPage))
			Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				return tx.ScanPostings("doc", "search", nil, []storage.IndexRange{{}}, 0, func(string) error { return nil })
			})).To(MatchError(errCorruptPage))
			Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings("doc", "search", "r3", nil, byteKeys("b"))
			})).To(MatchError(errCorruptPage))

			write(store, "ws", func(tx storage.WriteTx) error { return tx.ClearPostings("doc", "search") })
			Expect(left("ws")).To(Equal([]int{0, 0, 0, 0}))

			setup("ws2")
			Expect(store.DeleteWorkspace(ctx, "ws2")).To(Succeed())
			Expect(left("ws2")).To(Equal([]int{0, 0, 0, 0}))
		},
		Entry("with a varint that does not end", []byte{0xff, 0xff}),
		Entry("with no entry", []byte{}),
		Entry("cut short", encodePage([]pageEntry{{k: []byte("a"), nums: []int64{1, 2}}})[:5]),
		Entry("with keys out of order", []byte{0x00, 0x01, 'b', 0x01, 0x01, 0x00, 0x01, 'a', 0x01, 0x01}),
		Entry("with a key of no rows", []byte{0x00, 0x01, 'a', 0x00}),
		Entry("with a shared prefix longer than the key before", []byte{0x00, 0x01, 'a', 0x01, 0x01, 0x05, 0x01, 'b', 0x01, 0x01}),
	)

	It("rewinds page edits with a savepoint, also after a read inside the savepoint wrote them", func() {
		setPostingFormat(2, 24, 4)
		store := openStore()
		model := postingModel{}
		base := func(tx storage.WriteTx, row string, keys ...string) error {
			model.put(row, nil, byteKeys(keys...))
			return tx.PutPostings("doc", "search", row, nil, byteKeys(keys...))
		}
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 6 {
				if err := base(tx, fmt.Sprintf("r%d", i), fmt.Sprintf("k%d", i), "shared"); err != nil {
					return err
				}
			}
			return nil
		})
		before := storedPages(ctx, store)
		write(store, "ws", func(tx storage.WriteTx) error {
			Expect(tx.InSavepoint(func() error {
				// k0 passes the row limit and moves to chunks; the new keys
				// split pages.
				for i := range 6 {
					Expect(tx.PutPostings("doc", "search", fmt.Sprintf("s%d", i), nil, byteKeys("k0", fmt.Sprintf("k0%d", i)))).To(Succeed())
				}
				return errors.New("rejected")
			})).To(HaveOccurred())
			Expect(tx.InSavepoint(func() error {
				Expect(tx.DeletePostings("doc", "search", "r1", byteKeys("k1", "shared"))).To(Succeed())
				for i := range 6 {
					Expect(tx.PutPostings("doc", "search", fmt.Sprintf("t%d", i), nil, byteKeys("k2", "a"+fmt.Sprint(i)))).To(Succeed())
				}
				// The read writes every logged call, and the rewind undoes
				// those writes.
				n, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{one("k2"), one("k1"), {}}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal([]int{7, 0, 22}))
				return errors.New("rejected")
			})).To(HaveOccurred())
			return nil
		})
		Expect(storedPages(ctx, store)).To(Equal(before))
		Expect(chunkCount(ctx, store)).To(Equal(3), "only shared, with 6 rows in chunks of 2, is in chunks")
		Expect(storedPostings(ctx, store, "ws")).To(Equal(model))
		write(store, "ws", func(tx storage.WriteTx) error {
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutPostings("doc", "search", "lost", nil, byteKeys("k3", "k4", "zz"))).To(Succeed())
				_, _, err := tx.PostingCounts("doc", "search", []storage.IndexRange{{}}, 0)
				Expect(err).NotTo(HaveOccurred())
				return errors.New("rejected")
			})).To(HaveOccurred())
			return base(tx, "kept", "k3", "zz")
		})
		Expect(storedPostings(ctx, store, "ws")).To(Equal(model))
		checkChunks(ctx, store)
	})

	It("gives readers on a snapshot the pages as they were while a writer splits, merges and moves them", func() {
		setPostingFormat(2, 24, 4)
		store := openStore()
		keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 16 {
				if err := tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, byteKeys(keys[i%len(keys)])); err != nil {
					return err
				}
			}
			return nil
		})
		var wg sync.WaitGroup
		stop := make(chan struct{})
		errs := make(chan error, 8)
		for g := range 4 {
			wg.Go(func() {
				defer GinkgoRecover()
				rng := rand.New(rand.NewPCG(uint64(g), 9))
				for {
					select {
					case <-stop:
						return
					default:
					}
					err := store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
						ranges := make([]storage.IndexRange, len(keys))
						for i, k := range keys {
							ranges[i] = one(k)
						}
						ranges = append(ranges, storage.IndexRange{})
						first, _, err := tx.PostingCounts("doc", "search", ranges, 0)
						if err != nil {
							return err
						}
						sum := 0
						for _, n := range first[:len(keys)] {
							sum += n
						}
						if sum != first[len(keys)] {
							return fmt.Errorf("snapshot counted %d in keys and %d in all", sum, first[len(keys)])
						}
						i := rng.IntN(len(keys))
						var seen int
						if err := tx.ScanPostings("doc", "search", []storage.IndexRange{ranges[i]}, nil, 0, func(string) error {
							seen++
							return nil
						}); err != nil {
							return err
						}
						if seen != first[i] {
							return fmt.Errorf("snapshot counted %d and scanned %d for %s", first[i], seen, keys[i])
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
		for range 80 {
			write(store, "ws", func(tx storage.WriteTx) error {
				for range 4 {
					row := fmt.Sprintf("r%02d", rng.IntN(24))
					k := byteKeys(keys[rng.IntN(len(keys))])
					if rng.IntN(2) == 0 {
						if err := tx.PutPostings("doc", "search", row, nil, k); err != nil {
							return err
						}
					} else if err := tx.PutPostings("doc", "search", row, k, nil); err != nil {
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
})

func byteKeys(ks ...string) [][]byte {
	out := make([][]byte, len(ks))
	for i, k := range ks {
		out[i] = []byte(k)
	}
	return out
}

// storedPage is one stored page, decoded.
type storedPage struct {
	lo      []byte
	keys    [][]byte
	entries []pageEntry
	size    int
}

// storedPages reads every page of the store in lo order.
func storedPages(ctx context.Context, store *Store) []storedPage {
	GinkgoHelper()
	rows, err := store.pool.Query(ctx, `SELECT lo, body FROM posting_pages ORDER BY scope, lo`)
	Expect(err).NotTo(HaveOccurred())
	var out []storedPage
	for rows.Next() {
		var lo, body []byte
		Expect(rows.Scan(&lo, &body)).To(Succeed())
		es, err := decodePage(body)
		Expect(err).NotTo(HaveOccurred())
		out = append(out, storedPage{lo: notNilBytes(lo), keys: keysOf(es), entries: es, size: len(body)})
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

func keysOf(es []pageEntry) [][]byte {
	out := make([][]byte, len(es))
	for i, e := range es {
		out[i] = e.k
	}
	return out
}

// loKeys gives each page as its lo and its keys, as text.
func loKeys(pages []storedPage) []string {
	out := make([]string, len(pages))
	for i, pg := range pages {
		out[i] = fmt.Sprintf("%q: %q", pg.lo, bytes.Join(pg.keys, []byte(" ")))
	}
	return out
}
