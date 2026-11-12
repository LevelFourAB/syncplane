package sqlitestore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("posting list format", func() {
	It("round-trips row numbers and a chunk directory", func() {
		nums := []int64{1, 2, 3, 130, 131, 70000, 1 << 40}
		got, err := decodeNums(appendNums(nil, nums), nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(nums))

		dir := []*postingChunk{{id: 7, hi: 600, n: 512}, {id: 3, hi: 1400, n: 300}}
		back, err := decodeDir(encodeDir(dir))
		Expect(err).NotTo(HaveOccurred())
		Expect(back).To(Equal(dir))
		Expect(encodeDir(nil)).To(BeNil())
		none, err := decodeDir(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(none).To(BeEmpty())
	})

	DescribeTable("refuses row numbers it cannot have written",
		func(b []byte) {
			_, err := decodeNums(b, nil)
			Expect(err).To(HaveOccurred())
		},
		Entry("a truncated varint", []byte{0x80}),
		Entry("a zero delta", []byte{0x01, 0x00}),
		Entry("a first number of 0", []byte{0x00}),
		Entry("an overflow", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x01}),
	)

	DescribeTable("refuses a chunk directory it cannot have written",
		func(b []byte) {
			_, err := decodeDir(b)
			Expect(err).To(HaveOccurred())
		},
		Entry("a zero count", []byte{0x00}),
		Entry("a count past the bytes", []byte{0x05, 0x01, 0x01, 0x01}),
		Entry("a truncated entry", []byte{0x01, 0x01, 0x80}),
		Entry("a chunk id of 0", []byte{0x01, 0x00, 0x05, 0x05}),
		Entry("a bound that does not grow", []byte{0x02, 0x01, 0x05, 0x05, 0x02, 0x00, 0x01}),
		Entry("more numbers than the range holds", []byte{0x01, 0x01, 0x05, 0x06}),
		Entry("an empty chunk", []byte{0x01, 0x01, 0x05, 0x00}),
		Entry("trailing bytes", []byte{0x01, 0x01, 0x05, 0x05, 0x00, 0x00, 0x00}),
	)

	It("refuses a chunk that disagrees with its directory entry", func() {
		c := &postingChunk{id: 1, hi: 5, n: 3}
		_, err := decodeChunk(c, appendNums(nil, []int64{1, 2}), nil)
		Expect(err).To(MatchError(ContainSubstring("holds 2 row numbers")))
		_, err = decodeChunk(c, appendNums(nil, []int64{1, 2, 4}), nil)
		Expect(err).To(MatchError(ContainSubstring("ends at 4")))
		got, err := decodeChunk(c, appendNums(nil, []int64{1, 2, 5}), nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]int64{1, 2, 5}))
	})

	It("refuses a head whose count or tail disagrees with its chunks", func() {
		Expect(checkHead(3, []*postingChunk{{id: 1, hi: 5, n: 2}}, []int64{6})).To(Succeed())
		Expect(checkHead(4, []*postingChunk{{id: 1, hi: 5, n: 2}}, []int64{6})).To(MatchError(ContainSubstring("counts 4")))
		Expect(checkHead(3, []*postingChunk{{id: 1, hi: 5, n: 2}}, []int64{5})).To(MatchError(ContainSubstring("tail starts at 5")))
	})
})

// chunkChurnCeiling bounds the chunk records of an index that rows came to
// and went from, where one row in ten stayed, as a multiple of the chunk
// records of a fresh build of the rows that stayed. It sits about 15% above
// the measured value, so a change that leaves small chunks unmerged fails
// here. Lower it when a change merges more.
const chunkChurnCeiling = 1.6

var _ = Describe("posting lists in the store", func() {
	var store *Store
	ctx := context.Background()
	const entity, index = "doc", "search"

	BeforeEach(func() {
		var err error
		store, err = Open(filepath.Join(GinkgoT().TempDir(), "postings.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	})

	write := func(fn func(tx storage.WriteTx) error) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, "ws1", fn)).To(Succeed())
	}
	rowKey := func(i int) string { return fmt.Sprintf("r%05d", i) }
	count := func(table string) int {
		GinkgoHelper()
		var n int
		Expect(store.read.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n)).To(Succeed())
		return n
	}
	// stored checks every stored record (see checkPostings) and returns the
	// row numbers of each key, from its page or its head, with the chunk sizes
	// of each key that has a head. The specs that use it write one scope.
	stored := func() (map[string][]int64, map[string][]int) {
		GinkgoHelper()
		idx := checkPostings(store)
		nums := map[string][]int64{}
		sizes := map[string][]int{}
		for ref, ns := range idx.nums {
			nums[ref.k] = ns
		}
		for ref, sz := range idx.sizes {
			sizes[ref.k] = sz
		}
		return nums, sizes
	}
	numOf := func(key string) int64 {
		GinkgoHelper()
		var n int64
		Expect(store.read.QueryRow(`SELECT num FROM posting_rows WHERE key = ?`, key).Scan(&n)).To(Succeed())
		return n
	}
	put := func(key string, remove, add []string) {
		GinkgoHelper()
		toBytes := func(ks []string) [][]byte {
			out := make([][]byte, len(ks))
			for i, k := range ks {
				out[i] = []byte(k)
			}
			return out
		}
		write(func(tx storage.WriteTx) error {
			return tx.PutPostings(entity, index, key, toBytes(remove), toBytes(add))
		})
	}

	It("gives rows dense numbers in the order they are first posted", func() {
		for i := 0; i < 5; i++ {
			put(rowKey(4-i), nil, []string{"a"})
		}
		for i := 0; i < 5; i++ {
			Expect(numOf(rowKey(4 - i))).To(BeEquivalentTo(i + 1))
		}
		nums, _ := stored()
		Expect(nums["a"]).To(Equal([]int64{1, 2, 3, 4, 5}))
	})

	It("moves a full tail into chunks and splits a chunk that grows past the cap", func() {
		const n = 1200
		write(func(tx storage.WriteTx) error {
			for i := 0; i < n; i++ {
				if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("all")}); err != nil {
					return err
				}
			}
			// The odd rows first: their numbers fill the tail, which moves
			// into a chunk, and then into the same chunk until it is full.
			for i := 1; i < n; i += 2 {
				if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("x")}); err != nil {
					return err
				}
			}
			return nil
		})
		nums, sizes := stored()
		Expect(nums["all"]).To(HaveLen(n))
		Expect(sizes["all"]).To(Equal([]int{512, 512}))
		Expect(nums["x"]).To(HaveLen(n / 2))
		Expect(sizes["x"]).To(Equal([]int{512}))

		// The even rows go into the full chunk, which splits.
		write(func(tx storage.WriteTx) error {
			for i := 0; i < 200; i += 2 {
				if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("x")}); err != nil {
					return err
				}
			}
			return nil
		})
		nums, sizes = stored()
		Expect(nums["x"]).To(HaveLen(n/2 + 100))
		Expect(len(sizes["x"])).To(BeNumerically(">=", 2))
		Expect(sizes["x"][0]).To(BeNumerically("<", 512), "the chunk split")
	})

	It("merges a chunk that falls below a quarter of the cap with its neighbour", func() {
		const n = 1100
		write(func(tx storage.WriteTx) error {
			for i := 0; i < n; i++ {
				if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("x")}); err != nil {
					return err
				}
			}
			return nil
		})
		_, sizes := stored()
		Expect(sizes["x"]).To(Equal([]int{512, 512}))
		// Empty the first chunk down to below a quarter, with the second
		// chunk shrunk first so that the two fit in one.
		write(func(tx storage.WriteTx) error {
			for i := 512; i < 512+300; i++ {
				if err := tx.PutPostings(entity, index, rowKey(i), [][]byte{[]byte("x")}, nil); err != nil {
					return err
				}
			}
			return nil
		})
		_, sizes = stored()
		Expect(sizes["x"]).To(Equal([]int{512, 212}))
		write(func(tx storage.WriteTx) error {
			for i := 0; i < 400; i++ {
				if err := tx.PutPostings(entity, index, rowKey(i), [][]byte{[]byte("x")}, nil); err != nil {
					return err
				}
			}
			return nil
		})
		nums, sizes := stored()
		Expect(sizes["x"]).To(Equal([]int{324}))
		Expect(nums["x"]).To(HaveLen(n - 700))
		Expect(count("posting_chunks")).To(Equal(1))
	})

	Describe("a chunk that a removal leaves below a quarter of the cap", func() {
		// Every key has a head, with a tail of up to 8 numbers and chunks of
		// up to 16. A chunk is small with 3 numbers or fewer. Row i has the
		// number i+1, so the rows 0 to 15 are the first chunk of x, and so on.
		BeforeEach(func() {
			setPostingFormat(postingPageBudget, 0)
			setPostingChunks(8, 16)
		})
		build := func(rows int) {
			GinkgoHelper()
			write(func(tx storage.WriteTx) error {
				for i := 0; i < rows; i++ {
					if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("x")}); err != nil {
						return err
					}
				}
				return nil
			})
		}
		// drop removes x from the rows from..to, a transaction per row, and
		// checks the stored records after each one.
		drop := func(from, to int) {
			GinkgoHelper()
			for i := from; i <= to; i++ {
				put(rowKey(i), []string{"x"}, nil)
				checkPostings(store).strict(store)
			}
		}
		// shape returns the chunk sizes of x and the size of its tail.
		shape := func() ([]int, int) {
			GinkgoHelper()
			nums, sizes := stored()
			tail := len(nums["x"])
			for _, n := range sizes["x"] {
				tail -= n
			}
			return sizes["x"], tail
		}
		chunks := func() []int {
			GinkgoHelper()
			sizes, _ := shape()
			return sizes
		}

		It("merges into the small chunk on its left when the chunk on its right is full", func() {
			build(48)
			Expect(chunks()).To(Equal([]int{16, 16, 16}))
			// The first chunk becomes small while the second one is full.
			drop(0, 12)
			Expect(chunks()).To(Equal([]int{3, 16, 16}))
			// The second chunk becomes small later. The third one is full.
			drop(16, 27)
			Expect(chunks()).To(Equal([]int{3, 4, 16}))
			drop(28, 28)
			Expect(chunks()).To(Equal([]int{6, 16}))
			Expect(count("posting_chunks")).To(Equal(2))
		})

		DescribeTable("takes the smaller neighbour when it fits in with both",
			func(left, right int, want []int) {
				build(48)
				drop(0, 15-left)
				drop(32, 47-right)
				Expect(chunks()).To(Equal([]int{left, 16, right}))
				drop(16, 28)
				Expect(chunks()).To(Equal(want))
			},
			Entry("the left one", 4, 6, []int{7, 6}),
			Entry("the right one", 6, 4, []int{6, 7}),
			Entry("the right one when the two are the same size", 5, 5, []int{5, 8}),
			Entry("the one that fits when the smaller one does not exist", 16, 9, []int{16, 12}),
			Entry("the left one when only it has room", 9, 16, []int{12, 16}),
		)

		It("stays when it fits in with neither neighbour", func() {
			build(48)
			drop(0, 1)
			drop(32, 33)
			drop(16, 28)
			Expect(chunks()).To(Equal([]int{14, 3, 14}))
			// A removal from another chunk makes room and merges nothing. The
			// next removal from the small chunk looks again.
			drop(2, 2)
			Expect(chunks()).To(Equal([]int{13, 3, 14}))
			drop(29, 29)
			Expect(chunks()).To(Equal([]int{15, 14}))
		})

		It("merges the first chunk to the right and the last chunk to the left", func() {
			build(48)
			drop(16, 23)
			Expect(chunks()).To(Equal([]int{16, 8, 16}))
			drop(0, 12)
			Expect(chunks()).To(Equal([]int{11, 16}))
			drop(32, 44)
			Expect(chunks()).To(Equal([]int{14}))
			// The only chunk of a key has no neighbour.
			drop(13, 15)
			drop(24, 31)
			Expect(chunks()).To(Equal([]int{3}))
			drop(45, 47)
			Expect(chunks()).To(BeEmpty())
			Expect(count("posting_heads")).To(BeZero())
			Expect(count("posting_chunks")).To(BeZero())
		})

		It("leaves the tail of the head alone", func() {
			build(53)
			sizes, tail := shape()
			Expect(sizes).To(Equal([]int{16, 16, 16}))
			Expect(tail).To(Equal(5))
			// The last chunk becomes small. It does not take the tail, and the
			// chunk before it is full.
			drop(32, 44)
			sizes, tail = shape()
			Expect(sizes).To(Equal([]int{16, 16, 3}))
			Expect(tail).To(Equal(5))
			// A removal from the tail merges nothing.
			drop(48, 48)
			sizes, tail = shape()
			Expect(sizes).To(Equal([]int{16, 16, 3}))
			Expect(tail).To(Equal(4))
			// A full tail goes into the last chunk when it fits, as before.
			for i := 53; i < 57; i++ {
				put(rowKey(i), nil, []string{"x"})
			}
			sizes, tail = shape()
			Expect(sizes).To(Equal([]int{16, 16, 11}))
			Expect(tail).To(BeZero())
			checkPostings(store).strict(store)
		})

		It("frees each chunk one time when the removal that merges also moves the key to a page", func() {
			// A key leaves its head below 8 rows. A chunk is small with one
			// number.
			setPostingFormat(postingPageBudget, 16)
			setPostingChunks(4, 8)
			put("n", nil, []string{"w", "y"})
			build(20)
			Expect(chunks()).To(Equal([]int{8, 8, 4}))
			drop(0, 4)
			drop(16, 16)
			drop(8, 13)
			Expect(chunks()).To(Equal([]int{3, 2, 3}))

			// The merge, without the write: the chunk that goes is freed and
			// is not in the directory any more.
			var ids []int64
			write(func(tx storage.WriteTx) error {
				t := tx.(*sqliteTx)
				h, err := t.loadPostingHead(1, []byte("x"))
				Expect(err).NotTo(HaveOccurred())
				for _, c := range h.chunks {
					ids = append(ids, c.id)
				}
				Expect(t.removePostingNum(h, numOf(rowKey(14)))).To(Succeed())
				Expect(h.chunks).To(HaveLen(2))
				Expect([]int64{h.chunks[0].id, h.chunks[1].id}).To(Equal(ids[:2]))
				Expect(h.freed).To(Equal(ids[2:]))
				return nil
			})
			Expect(chunks()).To(Equal([]int{3, 2, 3}), "the head was not saved")

			_, err := store.write.Exec(`
				CREATE TABLE chunk_log (id INTEGER NOT NULL);
				CREATE TRIGGER chunk_delete AFTER DELETE ON posting_chunks BEGIN INSERT INTO chunk_log VALUES (old.id); END;`)
			Expect(err).NotTo(HaveOccurred())
			drop(14, 14)
			rows, err := store.write.Query(`SELECT id FROM chunk_log ORDER BY id`)
			Expect(err).NotTo(HaveOccurred())
			var freed []int64
			for rows.Next() {
				var id int64
				Expect(rows.Scan(&id)).To(Succeed())
				freed = append(freed, id)
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			Expect(rows.Close()).To(Succeed())
			slices.Sort(ids)
			Expect(freed).To(Equal(ids))
			Expect(count("posting_chunks")).To(BeZero())
			Expect(count("posting_heads")).To(BeZero())
			nums, _ := stored()
			Expect(nums["x"]).To(HaveLen(7))
		})
	})

	// churn makes rounds of new rows and deletes all but one in ten of the
	// rows of each round: in the order they were made in, which empties the
	// chunks from the first to the last, or, when shuffled says so for the
	// round, in another order. Each row holds one key that all rows hold and
	// keys that a third and a seventh of the rows hold. It calls check after
	// each transaction and returns the rows that stay, with their keys.
	churn := func(rng *rand.Rand, rounds, perRound int, shuffled func(round int) bool, check func(model postingModel)) postingModel {
		GinkgoHelper()
		model := postingModel{}
		next := 0
		for round := range rounds {
			first := next
			write(func(tx storage.WriteTx) error {
				for ; next < first+perRound; next++ {
					keys := byteKeys("all", fmt.Sprintf("m%d", next%3), fmt.Sprintf("h%d", next%7))
					model.put(rowKey(next), nil, keys)
					if err := tx.PutPostings(entity, index, rowKey(next), nil, keys); err != nil {
						return err
					}
				}
				return nil
			})
			check(model)
			var gone []int
			for i := first; i < next; i++ {
				if i%10 != 3 {
					gone = append(gone, i)
				}
			}
			if shuffled(round) {
				rng.Shuffle(len(gone), func(i, j int) { gone[i], gone[j] = gone[j], gone[i] })
			}
			for len(gone) > 0 {
				n := min(len(gone), 1+rng.IntN(12))
				write(func(tx storage.WriteTx) error {
					for _, i := range gone[:n] {
						if err := tx.DeletePostings(entity, index, rowKey(i), model.held(rowKey(i))); err != nil {
							return err
						}
						delete(model, rowKey(i))
					}
					return nil
				})
				check(model)
				gone = gone[n:]
			}
		}
		return model
	}

	It("agrees with a model when rows come and go and one in ten stays", func() {
		// Chunks of up to 8 numbers, and keys that get a head at 13 rows and
		// lose it below 6, so that the rounds split and merge chunks and move
		// keys.
		setPostingChunks(4, 8)
		setPostingFormat(64, 12)
		rng := rand.New(rand.NewPCG(uint64(GinkgoRandomSeed()), 23))
		ranges := []storage.IndexRange{{}, {Lo: []byte("all"), Hi: []byte("all\x00")}, {Lo: []byte("h"), Hi: []byte("i")},
			{Lo: []byte("m1"), Hi: []byte("m1\x00")}, {Lo: []byte("h3")}}
		churn(rng, 6, 60, func(round int) bool { return round%2 == 1 }, func(model postingModel) {
			GinkgoHelper()
			checkPostings(store).strict(store)
			Expect(storedPostings(store, "ws1", entity, index)).To(Equal(model))
			Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				got, rows, err := tx.PostingCounts(entity, index, ranges, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(rows).To(Equal(len(model)))
				for i, r := range ranges {
					Expect(got[i]).To(Equal(model.count(r)), "range %d", i)
				}
				all := []storage.IndexRange{ranges[1], ranges[2], ranges[3]}
				var keys []string
				Expect(tx.ScanPostings(entity, index, all, nil, 0, func(key string) error {
					keys = append(keys, key)
					return nil
				})).To(Succeed())
				sort.Strings(keys)
				Expect(keys).To(Equal(model.scan(all, nil)))
				return nil
			})).To(Succeed())
		})
		_, sizes := stored()
		Expect(len(sizes["all"])).To(BeNumerically(">=", 2), "the key that all rows hold has chunks")
	})

	It("keeps the chunk records near those of a fresh build when rows come and go and one in ten stays", func() {
		// Every key has a head, and a chunk holds up to 32 numbers.
		setPostingFormat(postingPageBudget, 0)
		setPostingChunks(16, 32)
		model := churn(rand.New(rand.NewPCG(31, 7)), 10, 320, func(int) bool { return false }, func(postingModel) {})
		checkPostings(store).strict(store)
		Expect(model).To(HaveLen(320))
		churned := count("posting_chunks")

		fresh, err := Open(filepath.Join(GinkgoT().TempDir(), "fresh.db"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(fresh.Close()).To(Succeed()) }()
		rows := make([]string, 0, len(model))
		for row := range model {
			rows = append(rows, row)
		}
		sort.Strings(rows)
		Expect(fresh.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for _, row := range rows {
				if err := tx.PutPostings(entity, index, row, nil, model.held(row)); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		var built int
		Expect(fresh.read.QueryRow(`SELECT COUNT(*) FROM posting_chunks`).Scan(&built)).To(Succeed())
		Expect(built).To(BeNumerically(">=", 20))
		ratio := float64(churned) / float64(built)
		AddReportEntry("chunk records after churn", fmt.Sprintf("%d against %d of a fresh build, %.2fx", churned, built, ratio))
		Expect(ratio).To(BeNumerically("<=", chunkChurnCeiling), "%d chunk records against %d of a fresh build", churned, built)
	})

	It("keeps every head consistent under random adds and removes", func() {
		rng := rand.New(rand.NewPCG(uint64(GinkgoRandomSeed()), 7))
		const rows = 1500
		keys := []string{"a", "b", "c"}
		model := map[string]map[string]bool{}
		for _, k := range keys {
			model[k] = map[string]bool{}
		}
		for batch := 0; batch < 12; batch++ {
			write(func(tx storage.WriteTx) error {
				for op := 0; op < 600; op++ {
					k := keys[rng.IntN(len(keys))]
					// Rows of a narrow band per batch, so that one chunk
					// takes many changes and splits or merges.
					r := rowKey((batch*97 + rng.IntN(400)) % rows)
					if rng.IntN(5) < 3 {
						model[k][r] = true
						if err := tx.PutPostings(entity, index, r, nil, [][]byte{[]byte(k)}); err != nil {
							return err
						}
					} else {
						delete(model[k], r)
						if err := tx.PutPostings(entity, index, r, [][]byte{[]byte(k)}, nil); err != nil {
							return err
						}
					}
				}
				return nil
			})
			nums, _ := stored()
			for _, k := range keys {
				want := make([]int64, 0, len(model[k]))
				for r := range model[k] {
					want = append(want, numOf(r))
				}
				slices.Sort(want)
				if len(want) == 0 {
					Expect(nums).NotTo(HaveKey(k))
					continue
				}
				Expect(nums[k]).To(Equal(want), "key %s after batch %d", k, batch)
			}
		}
		write(func(tx storage.WriteTx) error {
			for _, k := range keys {
				members := make([]string, 0, len(model[k]))
				for r := range model[k] {
					members = append(members, r)
				}
				sort.Strings(members)
				for _, r := range members {
					if err := tx.PutPostings(entity, index, r, [][]byte{[]byte(k)}, nil); err != nil {
						return err
					}
				}
			}
			return nil
		})
		Expect(count("posting_heads")).To(BeZero())
		Expect(count("posting_chunks")).To(BeZero())
		Expect(count("posting_pages")).To(BeZero())
	})

	It("leaves no record behind when the index is cleared or the workspace deleted", func() {
		for _, ws := range []string{"ws1", "ws2"} {
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				for i := 0; i < 700; i++ {
					keys := [][]byte{[]byte("x"), []byte("y")}
					if i < 3 {
						keys = append(keys, []byte("rare"))
					}
					if err := tx.PutPostings(entity, index, rowKey(i), nil, keys); err != nil {
						return err
					}
				}
				return tx.PutPostings(entity, "other", "k", nil, [][]byte{[]byte("x")})
			})).To(Succeed())
		}
		Expect(count("posting_chunks")).To(Equal(4))
		Expect(count("posting_heads")).To(Equal(4))
		Expect(count("posting_pages")).To(Equal(4), "each index of each workspace has one page")
		write(func(tx storage.WriteTx) error { return tx.ClearPostings(entity, index) })
		Expect(count("posting_chunks")).To(Equal(2))
		Expect(count("posting_heads")).To(Equal(2), "ws2 keeps its two heads")
		Expect(count("posting_pages")).To(Equal(3), "ws2 keeps its page, and each workspace keeps the other index")
		Expect(count("posting_rows")).To(Equal(702))
		Expect(count("posting_nums")).To(Equal(702))
		Expect(count("posting_scopes")).To(Equal(3))
		Expect(store.DeleteWorkspace(ctx, "ws2")).To(Succeed())
		Expect(count("posting_chunks")).To(BeZero())
		Expect(count("posting_heads")).To(BeZero())
		Expect(count("posting_pages")).To(Equal(1))
		Expect(count("posting_rows")).To(Equal(1))
		Expect(count("posting_nums")).To(Equal(1))
		Expect(count("posting_scopes")).To(Equal(1))
		checkPostings(store)
	})

	It("gives a cleared index a new scope, so no old row number is read in it", func() {
		put("a", nil, []string{"x"})
		var before int64
		Expect(store.read.QueryRow(`SELECT id FROM posting_scopes`).Scan(&before)).To(Succeed())
		write(func(tx storage.WriteTx) error { return tx.ClearPostings(entity, index) })
		put("b", nil, []string{"x"})
		var after int64
		Expect(store.read.QueryRow(`SELECT id FROM posting_scopes`).Scan(&after)).To(Succeed())
		Expect(after).NotTo(Equal(before))
		Expect(numOf("b")).To(BeEquivalentTo(1))
	})

	Describe("the row key cache", func() {
		// Rows 0 and 39 under one key are far apart in number, so the scan
		// resolves them one by one, which is the path that uses the cache.
		scanKey := func(k string) []string {
			GinkgoHelper()
			var got []string
			Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				return tx.ScanPostings(entity, index, nil, []storage.IndexRange{{Lo: []byte(k), Hi: []byte(k + "\x00")}}, 0,
					func(rowKey string) error {
						got = append(got, rowKey)
						return nil
					})
			})).To(Succeed())
			sort.Strings(got)
			return got
		}
		BeforeEach(func() {
			write(func(tx storage.WriteTx) error {
				for i := 0; i < 40; i++ {
					if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("all")}); err != nil {
						return err
					}
				}
				for _, i := range []int{0, 39} {
					if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("a"), []byte("b")}); err != nil {
						return err
					}
				}
				return nil
			})
		})

		It("learns the rows a read resolves", func() {
			Expect(scanKey("a")).To(Equal([]string{rowKey(0), rowKey(39)}))
			key, ok := store.postingKeys.get(postingNum{1, numOf(rowKey(39))})
			Expect(ok).To(BeTrue())
			Expect(key).To(Equal(rowKey(39)))
		})

		It("never yields a forgotten row from the cache", func() {
			Expect(scanKey("a")).To(Equal([]string{rowKey(0), rowKey(39)}))
			num := numOf(rowKey(0))
			// The row is forgotten with b left in its head.
			write(func(tx storage.WriteTx) error {
				if err := tx.DeletePostings(entity, index, rowKey(0), [][]byte{[]byte("a"), []byte("all")}); err != nil {
					return err
				}
				var got []string
				if err := tx.ScanPostings(entity, index, nil, []storage.IndexRange{{Lo: []byte("b"), Hi: []byte("b\x00")}}, 0,
					func(rowKey string) error {
						got = append(got, rowKey)
						return nil
					}); err != nil {
					return err
				}
				Expect(got).To(Equal([]string{rowKey(39)}), "the transaction that forgot the row")
				return nil
			})
			_, ok := store.postingKeys.get(postingNum{1, num})
			Expect(ok).To(BeFalse(), "the commit removed the mapping")
			Expect(scanKey("b")).To(Equal([]string{rowKey(39)}))
		})

		It("never yields a forgotten row from the cache right after the commit", func() {
			Expect(scanKey("a")).To(Equal([]string{rowKey(0), rowKey(39)}))
			_, ok := store.postingKeys.get(postingNum{1, numOf(rowKey(0))})
			Expect(ok).To(BeTrue(), "the read learned the row")
			var between []string
			store.afterPostingCommit = func() { between = scanKey("b") }
			DeferCleanup(func() { store.afterPostingCommit = nil })
			write(func(tx storage.WriteTx) error {
				return tx.DeletePostings(entity, index, rowKey(0), [][]byte{[]byte("a")})
			})
			Expect(between).To(Equal([]string{rowKey(39)}), "a read between the commit and the last cache removal")
			Expect(scanKey("b")).To(Equal([]string{rowKey(39)}))
			_, ok = store.postingKeys.get(postingNum{1, numOf(rowKey(39))})
			Expect(ok).To(BeTrue(), "reads learn again once the commit is done")
		})

		It("does not learn a row that a commit forgot after the read began", func() {
			num := numOf(rowKey(0))
			Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				// The read's snapshot is taken here, with the row still held.
				if err := tx.ScanPostings(entity, index, nil, []storage.IndexRange{{}}, 1, func(string) error { return nil }); err != nil {
					return err
				}
				Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
					return tx.DeletePostings(entity, index, rowKey(0), [][]byte{[]byte("a")})
				})).To(Succeed())
				var got []string
				if err := tx.ScanPostings(entity, index, nil, []storage.IndexRange{{Lo: []byte("b"), Hi: []byte("b\x00")}}, 0,
					func(rowKey string) error {
						got = append(got, rowKey)
						return nil
					}); err != nil {
					return err
				}
				Expect(got).To(Equal([]string{rowKey(0), rowKey(39)}), "the read's snapshot still holds the row")
				return nil
			})).To(Succeed())
			_, ok := store.postingKeys.get(postingNum{1, num})
			Expect(ok).To(BeFalse())
			Expect(scanKey("b")).To(Equal([]string{rowKey(39)}))
		})
	})

	Describe("a corrupt record", func() {
		scan := func() error {
			return store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				return tx.ScanPostings(entity, index, []storage.IndexRange{{}}, nil, 0, func(string) error { return nil })
			})
		}
		BeforeEach(func() {
			write(func(tx storage.WriteTx) error {
				for i := 0; i < 600; i++ {
					if err := tx.PutPostings(entity, index, rowKey(i), nil, [][]byte{[]byte("x")}); err != nil {
						return err
					}
				}
				return nil
			})
			Expect(scan()).To(Succeed())
		})

		It("fails a read and a write of a head with a broken tail", func() {
			_, err := store.write.Exec(`UPDATE posting_heads SET tail = x'0100'`)
			Expect(err).NotTo(HaveOccurred())
			Expect(scan()).To(MatchError(ContainSubstring("tail")))
			err = store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, index, rowKey(0), [][]byte{[]byte("x")}, nil)
			})
			Expect(err).To(MatchError(ContainSubstring("posting key 78")))
		})

		It("fails a read of a head whose count disagrees", func() {
			_, err := store.write.Exec(`UPDATE posting_heads SET cnt = cnt + 1`)
			Expect(err).NotTo(HaveOccurred())
			err = store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, index, rowKey(0), nil, [][]byte{[]byte("x")})
			})
			Expect(err).To(MatchError(ContainSubstring("counts")))
		})

		It("fails a read of a chunk that is missing or broken", func() {
			_, err := store.write.Exec(`UPDATE posting_chunks SET nums = x'05'`)
			Expect(err).NotTo(HaveOccurred())
			Expect(scan()).To(MatchError(ContainSubstring("holds 1 row numbers")))
			_, err = store.write.Exec(`DELETE FROM posting_chunks`)
			Expect(err).NotTo(HaveOccurred())
			Expect(scan()).To(MatchError(ContainSubstring("missing")))
		})

		It("fails a read of a broken directory", func() {
			_, err := store.write.Exec(`UPDATE posting_heads SET dir = x'01'`)
			Expect(err).NotTo(HaveOccurred())
			Expect(scan()).To(MatchError(ContainSubstring("directory")))
		})

		expectNoPostingRecords := func() {
			GinkgoHelper()
			for _, table := range []string{"posting_scopes", "posting_rows", "posting_nums", "posting_pages", "posting_heads", "posting_chunks"} {
				Expect(count(table)).To(BeZero(), table)
			}
		}

		It("clears an index whose directory is broken", func() {
			_, err := store.write.Exec(`UPDATE posting_heads SET dir = x'01'`)
			Expect(err).NotTo(HaveOccurred())
			write(func(tx storage.WriteTx) error { return tx.ClearPostings(entity, index) })
			expectNoPostingRecords()
			put(rowKey(0), nil, []string{"x"})
			Expect(scan()).To(Succeed())
		})

		It("deletes a workspace whose directory and chunks are broken", func() {
			_, err := store.write.Exec(`UPDATE posting_heads SET dir = x'01'`)
			Expect(err).NotTo(HaveOccurred())
			_, err = store.write.Exec(`UPDATE posting_chunks SET nums = x'00'`)
			Expect(err).NotTo(HaveOccurred())
			Expect(store.DeleteWorkspace(ctx, "ws1")).To(Succeed())
			expectNoPostingRecords()
		})

	})
})
