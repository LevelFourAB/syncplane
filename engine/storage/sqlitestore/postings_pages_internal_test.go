package sqlitestore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("posting page format", func() {
	// Keys that share prefixes, a key that is all of the prefix of the next
	// one, the empty key, and the bytes 0x00 and 0xff.
	entries := []pageEntry{
		entryOf([]byte{}, 7),
		entryOf([]byte{0x00}, 1, 2, 3),
		entryOf([]byte{0x00, 0x00}, 1<<40),
		entryOf([]byte{0x00, 0xff}, 5, 900, 70000),
		entryOf([]byte("\x01abc\x00\x00"), 1, 3, 900),
		entryOf([]byte("\x01abc\x00\x00\x00"), 2),
		entryOf([]byte("\x01abd\x00\x00"), 5),
		entryOf([]byte("\x01b\x00\x00"), 1, 2, 3, 4, math.MaxInt64),
		entryOf([]byte{0xff}, 9),
		entryOf([]byte{0xff, 0xff}, 9, 10),
	}

	It("round-trips keys and row numbers", func() {
		body := encodePage(entries)
		Expect(body).To(HaveLen(pageLen(entries)))
		got, err := decodePage(body)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(entries))
		for i := range entries {
			one := encodePage(entries[i : i+1])
			Expect(one).To(HaveLen(pageEntryLen(nil, entries[i])))
			back, err := decodePage(one)
			Expect(err).NotTo(HaveOccurred())
			Expect(back).To(Equal(entries[i : i+1]))
		}
	})

	It("stores a key as the bytes it does not share with the key before it", func() {
		a := entryOf([]byte("\x01abc\x00\x00"), 1)
		b := entryOf([]byte("\x01abd\x00\x00"), 1)
		// The shared length, the rest length, three bytes of key, the count
		// and one row number.
		Expect(appendPageEntry(nil, a.k, b)).To(Equal([]byte{0x03, 0x03, 'd', 0x00, 0x00, 0x01, 0x01}))
		Expect(pageEntryLen(a.k, b)).To(Equal(7))
		Expect(appendPageEntry(nil, nil, b)).To(Equal([]byte{0x00, 0x06, 0x01, 'a', 'b', 'd', 0x00, 0x00, 0x01, 0x01}))
		// A key that continues the key before it stores only what it adds.
		c := entryOf([]byte("\x01abd\x00\x00\x00"), 1)
		Expect(appendPageEntry(nil, b.k, c)).To(Equal([]byte{0x06, 0x01, 0x00, 0x01, 0x01}))
	})

	It("walks the counts and the coded row numbers without decoding them, and stops when asked", func() {
		var (
			keys   []string
			counts []int
		)
		Expect(walkPage(encodePage(entries), func(k []byte, count int, nums []byte) bool {
			keys = append(keys, string(k))
			counts = append(counts, count)
			Expect(nums).To(Equal(entries[len(keys)-1].coded))
			return true
		})).To(Succeed())
		Expect(keys).To(Equal(stringKeys(keysOf(entries))))
		Expect(counts).To(Equal([]int{1, 3, 1, 3, 3, 1, 1, 5, 1, 2}))

		seen := 0
		Expect(walkPage(encodePage(entries), func([]byte, int, []byte) bool {
			seen++
			return seen < 3
		})).To(Succeed())
		Expect(seen).To(Equal(3))
	})

	It("round-trips random pages", func() {
		rng := rand.New(rand.NewPCG(uint64(GinkgoRandomSeed()), 3))
		alphabet := []byte{0x00, 0x01, 'a', 'b', 0xfe, 0xff}
		for range 200 {
			set := map[string]bool{}
			for range rng.IntN(40) + 1 {
				k := make([]byte, rng.IntN(6))
				for i := range k {
					k[i] = alphabet[rng.IntN(len(alphabet))]
				}
				set[string(k)] = true
			}
			var es []pageEntry
			for k := range set {
				var nums []int64
				num := int64(0)
				for range rng.IntN(5) + 1 {
					num += 1 + rng.Int64N(1<<uint(rng.IntN(40)))
					nums = append(nums, num)
				}
				es = append(es, entryOf([]byte(k), nums...))
			}
			slices.SortFunc(es, func(a, b pageEntry) int { return bytes.Compare(a.k, b.k) })
			body := encodePage(es)
			Expect(body).To(HaveLen(pageLen(es)))
			got, err := decodePage(body)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(es))
			for i, e := range got {
				Expect(numsOf(e)).To(Equal(numsOf(es[i])))
			}

			budget := 16 + rng.IntN(64)
			if len(body) <= budget {
				continue
			}
			parts := splitPage(es, budget)
			Expect(slices.Concat(parts...)).To(Equal(es))
			for _, part := range parts {
				Expect(part).NotTo(BeEmpty())
				if len(part) > 1 {
					Expect(pageLen(part)).To(BeNumerically("<=", budget))
				}
			}
		}
	})

	It("splits a page into parts of about equal size, with a large key alone", func() {
		var many []pageEntry
		for i := range 300 {
			many = append(many, entryOf([]byte{1, byte(i / 26), byte('a' + i%26), 0, 0}, int64(i+1), int64(i+8)))
		}
		parts := splitPage(many, 512)
		Expect(len(parts)).To(BeNumerically(">=", 2))
		Expect(slices.Concat(parts...)).To(Equal(many))
		sizes := make([]int, len(parts))
		for i, part := range parts {
			sizes[i] = pageLen(part)
			Expect(sizes[i]).To(BeNumerically("<=", 512))
		}
		Expect(slices.Max(sizes)-slices.Min(sizes)).To(BeNumerically("<=", 32), "sizes %v", sizes)

		long := entryOf(bytes.Repeat([]byte("m"), 100), 1)
		parts = splitPage([]pageEntry{entryOf([]byte("a"), 1), long, entryOf([]byte("n"), 1)}, 32)
		Expect(parts).To(HaveLen(3))
		Expect(parts[1]).To(Equal([]pageEntry{long}))
	})

	DescribeTable("refuses a page body it cannot have written",
		func(b []byte, what string) {
			_, err := decodePage(b)
			Expect(err).To(MatchError(ContainSubstring(what)))
		},
		Entry("no entry", []byte{}, "no entry"),
		Entry("a varint that does not end", []byte{0xff, 0xff}, "truncated key prefix length"),
		Entry("a key cut short", []byte{0x00, 0x05, 'a'}, "key of 5 bytes"),
		Entry("an entry cut short", encodePage([]pageEntry{entryOf([]byte("a"), 1, 2)})[:4], "row numbers"),
		Entry("keys out of order", []byte{0x00, 0x01, 'b', 0x01, 0x01, 0x00, 0x01, 'a', 0x01, 0x01}, "not above the key before it"),
		Entry("the same key two times", []byte{0x00, 0x01, 'a', 0x01, 0x01, 0x01, 0x00, 0x01, 0x01}, "not above the key before it"),
		Entry("a key of no rows", []byte{0x00, 0x01, 'a', 0x00}, "with 0 row numbers"),
		Entry("a shared prefix longer than the key before", []byte{0x00, 0x01, 'a', 0x01, 0x01, 0x05, 0x01, 'b', 0x01, 0x01}, "shares 5 bytes"),
	)

	DescribeTable("decodes the row numbers of a key when asked, and refuses the ones it cannot have written",
		func(b []byte) {
			es, err := decodePage(b)
			Expect(err).NotTo(HaveOccurred(), "the numbers of a key are not checked until they are decoded")
			Expect(es).To(HaveLen(1))
			_, err = es[0].nums()
			Expect(err).To(MatchError(ContainSubstring("key 61: row number delta 0")))
		},
		Entry("a row number of 0", []byte{0x00, 0x01, 'a', 0x01, 0x00}),
		Entry("a row number that does not grow", []byte{0x00, 0x01, 'a', 0x02, 0x05, 0x00}),
	)
})

// These specs make pages and the row limit of a page key small, so that a few
// rows split, merge and move them, and check the stored records as well as
// the answers.
var _ = Describe("posting pages in the store", func() {
	ctx := context.Background()
	const entity, index = "doc", "search"

	openStore := func() *Store {
		GinkgoHelper()
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "pages.db"))
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
			return tx.PutPostings(entity, index, row, nil, byteKeys(keys...))
		})
	}
	scan := func(store *Store, ws string, all, anyR []storage.IndexRange) []string {
		GinkgoHelper()
		var out []string
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanPostings(entity, index, all, anyR, 0, func(key string) error {
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
			out, _, err = tx.PostingCounts(entity, index, ranges, max)
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
	tableCount := func(store *Store, table string) int {
		GinkgoHelper()
		var n int
		Expect(store.read.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n)).To(Succeed())
		return n
	}
	// where reports whether key k of the only scope is in a page or has a
	// head.
	where := func(store *Store, k string) string {
		GinkgoHelper()
		idx := checkPostings(store)
		ref := storedKey{1, k}
		switch {
		case idx.heads[ref]:
			return "head"
		case idx.nums[ref] != nil:
			return "page"
		}
		return ""
	}

	It("keeps one key in one page, and many keys in one page", func() {
		store := openStore()
		put(store, "ws", "r1", "only")
		pages := storedPages(store)
		Expect(loKeys(pages)).To(Equal([]string{`"": "only"`}), "the first page starts at the empty key")
		Expect(tableCount(store, "posting_heads")).To(BeZero())

		var keys []string
		for i := range 40 {
			keys = append(keys, fmt.Sprintf("k%02d", i))
		}
		put(store, "ws", "r2", keys...)
		put(store, "ws", "r3", keys[:10]...)
		pages = storedPages(store)
		Expect(pages).To(HaveLen(1))
		Expect(pages[0].keys).To(HaveLen(41))
		Expect(tableCount(store, "posting_heads")).To(BeZero())
		Expect(counts(store, "ws", 0, one("only"), one("k00"), one("k39"), prefix("k"), storage.IndexRange{})).To(Equal([]int{1, 2, 1, 50, 51}))
		Expect(scan(store, "ws", []storage.IndexRange{one("k05")}, nil)).To(Equal([]string{"r2", "r3"}))
		Expect(scan(store, "ws", []storage.IndexRange{one("k05"), one("k20")}, nil)).To(Equal([]string{"r2"}))
		checkPostings(store).strict(store)
	})

	It("splits a page that passes the budget into parts of about equal size", func() {
		var entries []pageEntry
		for i := range 11 {
			entries = append(entries, entryOf(fmt.Appendf(nil, "k%02d", i), int64(i+1)))
		}
		// Ten keys fill the budget to the byte.
		setPostingFormat(pageLen(entries[:10]), 128)
		store := openStore()
		for i := range 10 {
			put(store, "ws", fmt.Sprintf("r%02d", i), fmt.Sprintf("k%02d", i))
		}
		pages := storedPages(store)
		Expect(pages).To(HaveLen(1), "a page at the budget does not split")
		Expect(pages[0].lo).To(BeEmpty())
		Expect(pages[0].size).To(Equal(postingPageBudget))

		put(store, "ws", "r10", "k10")
		pages = storedPages(store)
		Expect(pages).To(HaveLen(2), "one byte past the budget splits")
		Expect(pages[0].lo).To(BeEmpty())
		Expect(pages[1].lo).To(Equal(pages[1].keys[0]), "a part after the first starts at its first key")
		for _, pg := range pages {
			Expect(pg.size).To(BeNumerically("<=", postingPageBudget))
		}
		Expect(pages[0].size-pages[1].size).To(BeNumerically("~", 0, 8), "parts of about equal size")
		Expect(slices.Concat(pages[0].keys, pages[1].keys)).To(HaveLen(11))
		Expect(counts(store, "ws", 0, storage.IndexRange{}, one("k00"), one("k10"))).To(Equal([]int{11, 1, 1}))
		checkPostings(store).strict(store)
	})

	Describe("a page that a removal leaves below a quarter of the budget", func() {
		var store *Store
		key := func(i int) string { return fmt.Sprintf("k%02d", i) }
		// drop removes the keys from..to from the index, a call per key.
		drop := func(from, to int) {
			GinkgoHelper()
			for i := from; i <= to; i++ {
				write(store, "ws", func(tx storage.WriteTx) error {
					return tx.PutPostings(entity, index, "r1", byteKeys(key(i)), nil)
				})
				checkPostings(store).strict(store)
			}
		}
		keyRange := func(from, to int) string {
			var ks []string
			for i := from; i <= to; i++ {
				ks = append(ks, key(i))
			}
			return strings.Join(ks, " ")
		}
		BeforeEach(func() {
			setPostingFormat(64, 128)
			store = openStore()
			var keys []string
			for i := range 40 {
				keys = append(keys, key(i))
			}
			// One call, so one split: four pages of ten keys and 52 bytes
			// each. Two keys are 12 bytes, three are 17.
			put(store, "ws", "r1", keys...)
			pages := storedPages(store)
			Expect(loKeys(pages)).To(Equal([]string{
				`"": "` + keyRange(0, 9) + `"`,
				`"k10": "` + keyRange(10, 19) + `"`,
				`"k20": "` + keyRange(20, 29) + `"`,
				`"k30": "` + keyRange(30, 39) + `"`,
			}))
			for _, pg := range pages {
				Expect(pg.size).To(Equal(52))
			}
		})

		It("merges into the page before it when the two fit in the budget", func() {
			drop(20, 26)
			Expect(storedPages(store)).To(HaveLen(4), "three keys are a quarter of the budget or more")
			drop(27, 27)
			Expect(loKeys(storedPages(store))).To(Equal([]string{
				`"": "` + keyRange(0, 9) + `"`,
				`"k10": "` + keyRange(10, 19) + " k28 k29" + `"`,
				`"k30": "` + keyRange(30, 39) + `"`,
			}))
			Expect(counts(store, "ws", 0, storage.IndexRange{}, one("k28"), one("k19"), prefix("k2"))).To(Equal([]int{32, 1, 1, 2}))
			Expect(scan(store, "ws", nil, []storage.IndexRange{one("k29")})).To(Equal([]string{"r1"}))
		})

		It("takes the page after it when it is the first page of the scope", func() {
			// The second page gets room first.
			drop(10, 15)
			drop(0, 7)
			Expect(loKeys(storedPages(store))).To(Equal([]string{
				`"": "k08 k09 k16 k17 k18 k19"`,
				`"k20": "` + keyRange(20, 29) + `"`,
				`"k30": "` + keyRange(30, 39) + `"`,
			}))
			Expect(counts(store, "ws", 0, storage.IndexRange{}, prefix("k1"), one("k08"))).To(Equal([]int{26, 4, 1}))
		})

		It("takes the page after it when the page before it has no room", func() {
			// The first page grows past what the two keys left in the second
			// page fit in with it.
			put(store, "ws", "r2", key(0), key(1), key(2), key(3), key(4))
			drop(20, 25)
			drop(10, 17)
			Expect(loKeys(storedPages(store))).To(Equal([]string{
				`"": "` + keyRange(0, 9) + `"`,
				`"k10": "k18 k19 k26 k27 k28 k29"`,
				`"k30": "` + keyRange(30, 39) + `"`,
			}))
		})

		It("stays when it fits in with neither the page before it nor the page after it", func() {
			put(store, "ws", "r2", key(0), key(1), key(2), key(3), key(4), key(20), key(21), key(22), key(23), key(24))
			drop(10, 17)
			pages := storedPages(store)
			Expect(loKeys(pages)).To(Equal([]string{
				`"": "` + keyRange(0, 9) + `"`,
				`"k10": "k18 k19"`,
				`"k20": "` + keyRange(20, 29) + `"`,
				`"k30": "` + keyRange(30, 39) + `"`,
			}))
			Expect(pages[1].size).To(BeNumerically("<", postingPageBudget/4))
			// The page goes when its last key goes.
			drop(18, 19)
			Expect(storedPages(store)).To(HaveLen(3))
			Expect(counts(store, "ws", 0, storage.IndexRange{}, prefix("k1"))).To(Equal([]int{40, 0}))
		})

		It("merges past a page that the same transaction leaves with no key", func() {
			// dropAll removes the keys in one transaction.
			dropAll := func(ranges ...[2]int) {
				GinkgoHelper()
				write(store, "ws", func(tx storage.WriteTx) error {
					for _, r := range ranges {
						for i := r[0]; i <= r[1]; i++ {
							if err := tx.PutPostings(entity, index, "r1", byteKeys(key(i)), nil); err != nil {
								return err
							}
						}
					}
					return nil
				})
				checkPostings(store).strict(store)
			}
			// The second page goes, and the third merges into the first.
			dropAll([2]int{10, 19}, [2]int{20, 27})
			Expect(loKeys(storedPages(store))).To(Equal([]string{
				`"": "` + keyRange(0, 9) + " k28 k29" + `"`,
				`"k30": "` + keyRange(30, 39) + `"`,
			}))
			put(store, "ws", "r1", key(10), key(20), key(21), key(22), key(23), key(24), key(25), key(26), key(27))
			drop(0, 5)
			drop(30, 35)
			pages := storedPages(store)
			Expect(pages).To(HaveLen(3))
			// The page in the middle goes, and the first page takes the last.
			var middle [][2]int
			for _, k := range pages[1].keys {
				var i int
				_, err := fmt.Sscanf(string(k), "k%02d", &i)
				Expect(err).NotTo(HaveOccurred())
				middle = append(middle, [2]int{i, i})
			}
			first := stringKeys(pages[0].keys)
			var gone [][2]int
			for _, k := range first[:len(first)-2] {
				var i int
				_, err := fmt.Sscanf(k, "k%02d", &i)
				Expect(err).NotTo(HaveOccurred())
				gone = append(gone, [2]int{i, i})
			}
			dropAll(append(middle, gone...)...)
			Expect(loKeys(storedPages(store))).To(Equal([]string{
				`"": "` + strings.Join(append(first[len(first)-2:], stringKeys(pages[2].keys)...), " ") + `"`,
			}))
		})

		It("merges on and on while keys go in a random order, until no page is left", func() {
			rng := rand.New(rand.NewPCG(uint64(GinkgoRandomSeed()), 5))
			for left, i := range rng.Perm(40) {
				before := storedPages(store)
				drop(i, i)
				after := storedPages(store)
				Expect(len(after)).To(BeNumerically("<=", len(before)))
				var keys []string
				for j, pg := range after {
					keys = append(keys, stringKeys(pg.keys)...)
					if j > 0 && pg.size < postingPageBudget/4 {
						Expect(after[j-1].size).To(BeNumerically(">=", postingPageBudget/4), "two small pages next to each other: %v", loKeys(after))
					}
				}
				Expect(keys).To(HaveLen(39 - left))
				Expect(keys).NotTo(ContainElement(key(i)))
				Expect(counts(store, "ws", 0, storage.IndexRange{})).To(Equal([]int{39 - left}))
			}
			Expect(storedPages(store)).To(BeEmpty())
		})
	})

	It("gives a key a head past the row limit and moves it back below half of it", func() {
		setPostingFormat(postingPageBudget, 8)
		store := openStore()
		// Neighbours keep the page of t alive when t leaves it.
		put(store, "ws", "n1", "s", "u")
		add := func(i int) {
			GinkgoHelper()
			put(store, "ws", fmt.Sprintf("r%02d", i), "t")
		}
		remove := func(i int) {
			GinkgoHelper()
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, index, fmt.Sprintf("r%02d", i), byteKeys("t"), nil)
			})
		}
		check := func(rows int, want string) {
			GinkgoHelper()
			Expect(where(store, "t")).To(Equal(want), "with %d rows", rows)
			Expect(counts(store, "ws", 0, one("t"), storage.IndexRange{})).To(Equal([]int{rows, rows + 2}))
			Expect(scan(store, "ws", []storage.IndexRange{one("t")}, nil)).To(HaveLen(rows))
		}
		for i := 1; i <= 8; i++ {
			add(i)
			check(i, "page")
		}
		add(9)
		check(9, "head")
		// Between half of the limit and the limit the key stays where it is,
		// whatever the direction of the change.
		for range 3 {
			remove(9)
			check(8, "head")
			add(9)
			check(9, "head")
		}
		for i := 9; i >= 5; i-- {
			remove(i)
			check(i-1, "head")
		}
		remove(4)
		check(3, "page")
		for range 3 {
			add(4)
			check(4, "page")
			remove(4)
			check(3, "page")
		}
		for i := 4; i <= 8; i++ {
			add(i)
			check(i, "page")
		}
		add(9)
		check(9, "head")
		for i := 9; i >= 1; i-- {
			remove(i)
		}
		check(0, "")
		Expect(tableCount(store, "posting_heads")).To(BeZero())
		Expect(scan(store, "ws", nil, []storage.IndexRange{{}})).To(Equal([]string{"n1"}))
	})

	It("moves a key with a head and several chunks back to a page", func() {
		// A row limit above the size of a chunk, so that the head has chunks
		// when the key falls below half of the limit.
		const limit = 1500
		setPostingFormat(postingPageBudget, limit)
		store := openStore()
		put(store, "ws", "n1", "s", "u")
		row := func(i int) string { return fmt.Sprintf("r%04d", i) }
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range limit + 1 {
				if err := tx.PutPostings(entity, index, row(i), nil, byteKeys("t")); err != nil {
					return err
				}
			}
			return nil
		})
		idx := checkPostings(store)
		Expect(idx.heads[storedKey{1, "t"}]).To(BeTrue())
		Expect(len(idx.sizes[storedKey{1, "t"}])).To(BeNumerically(">=", 2))
		// The highest rows go, so the first chunks stay as they are.
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := limit; i >= limit/2; i-- {
				if err := tx.PutPostings(entity, index, row(i), byteKeys("t"), nil); err != nil {
					return err
				}
			}
			return nil
		})
		idx = checkPostings(store)
		Expect(idx.heads[storedKey{1, "t"}]).To(BeTrue(), "half of the limit keeps the head")
		Expect(len(idx.sizes[storedKey{1, "t"}])).To(BeNumerically(">=", 2))
		write(store, "ws", func(tx storage.WriteTx) error {
			return tx.PutPostings(entity, index, row(limit/2-1), byteKeys("t"), nil)
		})
		Expect(where(store, "t")).To(Equal("page"))
		Expect(tableCount(store, "posting_heads")).To(BeZero())
		Expect(tableCount(store, "posting_chunks")).To(BeZero(), "the chunks of the head went with it")
		Expect(counts(store, "ws", 0, one("t"))).To(Equal([]int{limit/2 - 1}))
		got := scan(store, "ws", []storage.IndexRange{one("t")}, nil)
		Expect(got).To(HaveLen(limit/2 - 1))
		Expect(got[0]).To(Equal(row(0)))
		Expect(got[len(got)-1]).To(Equal(row(limit/2 - 2)))
		checkPostings(store).strict(store)
	})

	It("rewinds a savepoint and a transaction in which one key got a head and another lost its head", func() {
		setPostingFormat(64, 4)
		store := openStore()
		row := func(i int) string { return fmt.Sprintf("r%d", i) }
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 5 {
				keys := byteKeys("down", "n")
				if i < 4 {
					keys = append(keys, []byte("up"))
				}
				if err := tx.PutPostings(entity, index, row(i), nil, keys); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(where(store, "up")).To(Equal("page"))
		Expect(where(store, "down")).To(Equal("head"))
		before := checkPostings(store)
		// move gives up a fifth row, which is one more than a page key has,
		// and leaves down with one row, which is less than half of that.
		move := func(tx storage.WriteTx) {
			GinkgoHelper()
			Expect(tx.PutPostings(entity, index, row(4), nil, byteKeys("up"))).To(Succeed())
			for i := range 4 {
				Expect(tx.PutPostings(entity, index, row(i), byteKeys("down"), nil)).To(Succeed())
			}
			n, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{one("up"), one("down")}, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal([]int{5, 1}))
		}
		expectBefore := func() {
			GinkgoHelper()
			after := checkPostings(store)
			after.strict(store)
			Expect(after.nums).To(Equal(before.nums))
			Expect(after.heads).To(Equal(before.heads))
			Expect(loKeys(after.pages)).To(Equal(loKeys(before.pages)))
		}

		write(store, "ws", func(tx storage.WriteTx) error {
			Expect(tx.InSavepoint(func() error {
				move(tx)
				return errRejected
			})).To(MatchError(errRejected))
			n, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{one("up"), one("down")}, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal([]int{4, 5}))
			return nil
		})
		expectBefore()

		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			move(tx)
			return errRejected
		})).To(MatchError(errRejected))
		expectBefore()

		write(store, "ws", func(tx storage.WriteTx) error {
			return tx.InSavepoint(func() error {
				move(tx)
				return nil
			})
		})
		Expect(where(store, "up")).To(Equal("head"))
		Expect(where(store, "down")).To(Equal("page"))
		Expect(counts(store, "ws", 0, one("up"), one("down"), one("n"))).To(Equal([]int{5, 1, 5}))
		checkPostings(store).strict(store)
	})

	Describe("a key that fails to move between a page and a head", func() {
		var store *Store
		row := func(i int) string { return fmt.Sprintf("r%d", i) }
		// headCount returns the count of the head of t, or -1 when t has
		// none.
		headCount := func() int {
			GinkgoHelper()
			var cnt int
			err := store.read.QueryRow(`SELECT cnt FROM posting_heads WHERE k = x'74'`).Scan(&cnt)
			if errors.Is(err, sql.ErrNoRows) {
				return -1
			}
			Expect(err).NotTo(HaveOccurred())
			return cnt
		}
		// setup gives t rows rows, with keys next to it in its page.
		setup := func(rows int) {
			GinkgoHelper()
			setPostingFormat(postingPageBudget, 4)
			store = openStore()
			put(store, "ws", "n", "s", "u")
			for i := range rows {
				put(store, "ws", row(i), "t")
			}
		}
		fail := func(when string) {
			GinkgoHelper()
			_, err := store.write.Exec(`CREATE TRIGGER no_head BEFORE ` + when + ` ON posting_heads
				BEGIN SELECT RAISE(ABORT, 'no head'); END`)
			Expect(err).NotTo(HaveOccurred())
		}

		It("keeps the key in its page when the head cannot be stored", func() {
			setup(4)
			Expect(where(store, "t")).To(Equal("page"))
			fail("INSERT")
			err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				// The fifth row gives t a head, which the trigger refuses. The
				// caller goes on.
				Expect(tx.PutPostings(entity, index, row(4), nil, byteKeys("t"))).To(MatchError(ContainSubstring("no head")))
				return nil
			})
			Expect(err).To(MatchError(ContainSubstring("no head")), "a move that failed stops the commit")
			Expect(where(store, "t")).To(Equal("page"))
			Expect(counts(store, "ws", 0, one("t"), storage.IndexRange{})).To(Equal([]int{4, 6}))
			checkPostings(store).strict(store)
		})

		It("keeps the head when the page of the key cannot be read", func() {
			setup(5)
			Expect(headCount()).To(Equal(5))
			_, err := store.write.Exec(`UPDATE posting_pages SET body = x'ffff'`)
			Expect(err).NotTo(HaveOccurred())
			err = store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				for i := range 3 {
					Expect(tx.PutPostings(entity, index, row(i), byteKeys("t"), nil)).To(Succeed())
				}
				// The fourth removal moves t to its page, which does not
				// decode. The caller goes on.
				Expect(tx.PutPostings(entity, index, row(3), byteKeys("t"), nil)).To(MatchError(ContainSubstring("posting page")))
				return nil
			})
			if err == nil {
				Expect(headCount()).To(Equal(2), "the removal that failed changed nothing")
			} else {
				Expect(headCount()).To(Equal(5))
			}
		})

		It("keeps the head when it cannot be deleted", func() {
			setup(5)
			fail("DELETE")
			err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				for i := range 3 {
					Expect(tx.PutPostings(entity, index, row(i), byteKeys("t"), nil)).To(Succeed())
				}
				Expect(tx.PutPostings(entity, index, row(3), byteKeys("t"), nil)).To(MatchError(ContainSubstring("no head")))
				return nil
			})
			Expect(err).To(MatchError(ContainSubstring("no head")), "a move that failed stops the commit")
			Expect(where(store, "t")).To(Equal("head"))
			Expect(counts(store, "ws", 0, one("t"), storage.IndexRange{})).To(Equal([]int{5, 7}))
			checkPostings(store).strict(store)
		})
	})

	It("gives a key with many new rows a head in one transaction with page edits", func() {
		setPostingFormat(32, 8)
		store := openStore()
		put(store, "ws", "n1", "a", "b", "c")
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 30 {
				if err := tx.PutPostings(entity, index, fmt.Sprintf("r%02d", i), nil, byteKeys("b", fmt.Sprintf("x%02d", i))); err != nil {
					return err
				}
			}
			return tx.PutPostings(entity, index, "n1", byteKeys("a"), nil)
		})
		Expect(counts(store, "ws", 0, one("a"), one("b"), one("c"), prefix("x"))).To(Equal([]int{0, 31, 1, 30}))
		Expect(where(store, "b")).To(Equal("head"))
		Expect(where(store, "c")).To(Equal("page"))
		checkPostings(store).strict(store)
	})

	It("reads keys in pages and keys with heads in one range", func() {
		setPostingFormat(postingPageBudget, 4)
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
				if err := tx.PutPostings(entity, index, fmt.Sprintf("r%d", i), nil, byteKeys(keys...)); err != nil {
					return err
				}
			}
			return tx.PutPostings(entity, index, "s", nil, byteKeys("ac"))
		})
		Expect(where(store, "ab")).To(Equal("head"))
		Expect(tableCount(store, "posting_heads")).To(Equal(1))
		Expect(storedPages(store)).To(HaveLen(1), "ac, ad and b are in a page")
		Expect(counts(store, "ws", 0, prefix("a"), one("ab"), one("ac"), storage.IndexRange{}, prefix("b"))).To(Equal([]int{14, 10, 3, 15, 1}))
		capped := counts(store, "ws", 3, prefix("a"), one("ac"))
		Expect(capped[0]).To(BeNumerically(">", 3))
		Expect(capped[1]).To(Equal(3))
		Expect(scan(store, "ws", []storage.IndexRange{prefix("a")}, nil)).To(HaveLen(11))
		Expect(scan(store, "ws", []storage.IndexRange{one("ab"), one("ac")}, nil)).To(Equal([]string{"r0", "r1"}))
		Expect(scan(store, "ws", []storage.IndexRange{one("ac"), {Lo: []byte("ab"), Hi: []byte("ac")}}, nil)).To(Equal([]string{"r0", "r1"}))
		Expect(scan(store, "ws", []storage.IndexRange{one("ab")}, []storage.IndexRange{one("ac")})).To(HaveLen(11))
		Expect(scan(store, "ws", []storage.IndexRange{{Lo: []byte("ac")}}, []storage.IndexRange{one("ad")})).To(Equal([]string{"r0", "r1", "r9", "s"}))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Hi: []byte("ac")}, prefix("b")})).To(HaveLen(10))
	})

	It("finds the first and the last page of a scope, and ranges beyond them", func() {
		setPostingFormat(20, 128)
		store := openStore()
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 30 {
				if err := tx.PutPostings(entity, index, fmt.Sprintf("r%02d", i), nil, byteKeys(fmt.Sprintf("k%02d", i))); err != nil {
					return err
				}
			}
			return nil
		})
		pages := storedPages(store)
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
			storage.IndexRange{Lo: []byte{}, Hi: []byte("k05")},
		)).To(Equal([]int{0, 0, 0, 0, 30, 10, 0, 5}))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Lo: []byte("k29\x00")}, {Hi: []byte("k00")}})).To(BeEmpty())

		// A key below every key goes into the first page; a key above every
		// key goes into the last.
		put(store, "ws", "low", "a")
		put(store, "ws", "high", "z")
		pages = storedPages(store)
		Expect(string(pages[0].keys[0])).To(Equal("a"))
		last := pages[len(pages)-1]
		Expect(string(last.keys[len(last.keys)-1])).To(Equal("z"))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Hi: []byte("k00")}, {Lo: []byte("k29\x00")}})).To(Equal([]string{"high", "low"}))
		checkPostings(store).strict(store)

		// The first page keeps the empty key as lo when its first key goes.
		write(store, "ws", func(tx storage.WriteTx) error {
			return tx.DeletePostings(entity, index, "low", byteKeys("a"))
		})
		Expect(storedPages(store)[0].lo).To(BeEmpty())
		Expect(counts(store, "ws", 0, storage.IndexRange{Hi: []byte("k01")}, one("k00"))).To(Equal([]int{1, 1}))

		// When the first page loses its last key, the scope has no page from
		// the empty key on, and a key below every page makes one.
		first := storedPages(store)[0]
		write(store, "ws", func(tx storage.WriteTx) error {
			for _, k := range first.keys {
				var i int
				_, err := fmt.Sscanf(string(k), "k%02d", &i)
				Expect(err).NotTo(HaveOccurred())
				if err := tx.DeletePostings(entity, index, fmt.Sprintf("r%02d", i), [][]byte{k}); err != nil {
					return err
				}
			}
			return nil
		})
		pages = storedPages(store)
		Expect(pages[0].lo).NotTo(BeEmpty())
		Expect(counts(store, "ws", 0, storage.IndexRange{Hi: pages[0].lo}, storage.IndexRange{})).To(Equal([]int{0, 31 - len(first.keys)}))
		others := loKeys(pages)
		put(store, "ws", "low", "a", "b")
		pages = storedPages(store)
		Expect(loKeys(pages[:1])).To(Equal([]string{`"": "a b"`}))
		Expect(loKeys(pages[1:])).To(Equal(others))
		Expect(scan(store, "ws", nil, []storage.IndexRange{{Hi: []byte("k")}})).To(Equal([]string{"low"}))
		checkPostings(store).strict(store)
	})

	It("keeps a page of one key that is larger than the budget", func() {
		setPostingFormat(32, 128)
		store := openStore()
		long := strings.Repeat("m", 100)
		put(store, "ws", "r1", "a", "b", long, "n", "o")
		var alone bool
		for _, pg := range storedPages(store) {
			if len(pg.keys) == 1 && string(pg.keys[0]) == long {
				alone = true
				Expect(pg.size).To(BeNumerically(">", postingPageBudget))
			}
		}
		Expect(alone).To(BeTrue(), "the long key has a page of its own")
		Expect(counts(store, "ws", 0, one(long), prefix("m"), storage.IndexRange{})).To(Equal([]int{1, 1, 5}))
		put(store, "ws", "r2", long, "mm")
		Expect(counts(store, "ws", 0, one(long), prefix("m"), storage.IndexRange{})).To(Equal([]int{2, 3, 7}))
		checkPostings(store).strict(store)
	})

	It("stores the longest key in pages and heads under the longest names, with 64-bit row numbers", func() {
		store := openStore()
		ws := strings.Repeat("w", naming.MaxAppIDBytes+1+naming.MaxWorkspaceIDBytes)
		ent := strings.Repeat("e", naming.MaxEntityNameLength)
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
			return tx.PutPostings(ent, idx, rowKey(0), nil, keys)
		})).To(Succeed())
		// Row numbers past 2^62 take varints of nine bytes.
		_, err := store.write.Exec(`UPDATE posting_scopes SET next_num = ?`, int64(1)<<62)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := 1; i < postingPageMaxRows; i++ {
				if err := tx.PutPostings(ent, idx, rowKey(i), nil, keys); err != nil {
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
				got, rows, err := tx.PostingCounts(ent, idx, []storage.IndexRange{exact(k1), exact(k2), exact(k3), {}}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal([]int{n, n, n, 3 * n}))
				Expect(rows).To(Equal(n))
				var seen []string
				Expect(tx.ScanPostings(ent, idx, []storage.IndexRange{exact(k1), exact(k2), exact(k3)}, nil, 0, func(key string) error {
					seen = append(seen, key)
					return nil
				})).To(Succeed())
				Expect(seen).To(HaveLen(n))
				return nil
			})).To(Succeed())
			checkPostings(store).strict(store)
		}
		check(postingPageMaxRows)
		Expect(tableCount(store, "posting_heads")).To(BeZero(), "every key is in a page")

		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.PutPostings(ent, idx, rowKey(9999), nil, keys)
		})).To(Succeed())
		check(postingPageMaxRows + 1)
		Expect(storedPages(store)).To(BeEmpty(), "every key has a head")

		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := range postingPageMaxRows - 1 {
				if err := tx.DeletePostings(ent, idx, rowKey(i), keys); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		check(2)
		Expect(tableCount(store, "posting_heads")).To(BeZero(), "every key moved back to a page")
	})

	Describe("the page reads and writes of a transaction", func() {
		var (
			store *Store
			keys  []string
		)
		// pageWrites returns the page records written since the last call, as
		// triggers on the table logged them.
		pageWrites := func() []string {
			GinkgoHelper()
			rows, err := store.write.Query(`SELECT op FROM page_log ORDER BY rowid`)
			Expect(err).NotTo(HaveOccurred())
			var out []string
			for rows.Next() {
				var op string
				Expect(rows.Scan(&op)).To(Succeed())
				out = append(out, op)
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			Expect(rows.Close()).To(Succeed())
			_, err = store.write.Exec(`DELETE FROM page_log`)
			Expect(err).NotTo(HaveOccurred())
			return out
		}
		// logged returns the number of page records tx has written.
		logged := func(tx storage.WriteTx) int {
			GinkgoHelper()
			var n int
			Expect(tx.(*sqliteTx).queryRow(`SELECT COUNT(*) FROM page_log`).Scan(&n)).To(Succeed())
			return n
		}
		BeforeEach(func() {
			setPostingFormat(64, 128)
			store = openStore()
			keys = nil
			for i := range 40 {
				keys = append(keys, fmt.Sprintf("k%02d", i))
			}
			put(store, "ws", "r1", keys...)
			Expect(len(storedPages(store))).To(BeNumerically(">=", 4))
			_, err := store.write.Exec(`
				CREATE TABLE page_log (op TEXT NOT NULL);
				CREATE TRIGGER page_insert AFTER INSERT ON posting_pages BEGIN INSERT INTO page_log VALUES ('insert'); END;
				CREATE TRIGGER page_update AFTER UPDATE ON posting_pages BEGIN INSERT INTO page_log VALUES ('update'); END;
				CREATE TRIGGER page_delete AFTER DELETE ON posting_pages BEGIN INSERT INTO page_log VALUES ('delete'); END;`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("writes a page one time for several keys of it", func() {
			pages := storedPages(store)
			Expect(len(pages[1].keys)).To(BeNumerically(">=", 3))
			inOne := stringKeys(pages[1].keys[:3])
			put(store, "ws", "r2", inOne...)
			Expect(pageWrites()).To(Equal([]string{"update"}))
			Expect(loKeys(storedPages(store))).To(Equal(loKeys(pages)))

			// Two keys of each of two pages.
			inTwo := append(stringKeys(pages[0].keys[:2]), stringKeys(pages[2].keys[:2])...)
			put(store, "ws", "r3", inTwo...)
			Expect(pageWrites()).To(Equal([]string{"update", "update"}))

			// Removals and additions to one page in one call.
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, index, "r2", byteKeys(inOne[0]), byteKeys(inOne[1], inOne[2]))
			})
			Expect(pageWrites()).To(Equal([]string{"update"}))
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.DeletePostings(entity, index, "r3", byteKeys(inTwo...))
			})
			Expect(pageWrites()).To(Equal([]string{"update", "update"}))

			// A call that changes nothing writes nothing.
			put(store, "ws", "r1", keys...)
			write(store, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, index, "r2", byteKeys("absent", keys[0]), nil)
			})
			Expect(pageWrites()).To(BeEmpty())
			checkPostings(store).strict(store)
		})

		It("writes a page one time for the calls of one transaction, at the commit", func() {
			pages := storedPages(store)
			first, second := stringKeys(pages[1].keys[:3]), stringKeys(pages[2].keys[:3])
			write(store, "ws", func(tx storage.WriteTx) error {
				for i, k := range first {
					Expect(tx.PutPostings(entity, index, fmt.Sprintf("s%d", i), nil, byteKeys(k, second[i]))).To(Succeed())
				}
				Expect(tx.DeletePostings(entity, index, "s0", byteKeys(first[0], second[0]))).To(Succeed())
				Expect(logged(tx)).To(BeZero(), "no page is written before the commit")
				return nil
			})
			Expect(pageWrites()).To(Equal([]string{"update", "update"}))
			Expect(counts(store, "ws", 0, one(first[0]), one(first[1]), one(second[2]))).To(Equal([]int{1, 2, 2}))
			checkPostings(store).strict(store)

			// A read of the postings in the transaction sees its calls, and
			// writes the pages they changed to do so.
			write(store, "ws", func(tx storage.WriteTx) error {
				Expect(tx.PutPostings(entity, index, "s3", nil, byteKeys(first[0], first[1]))).To(Succeed())
				Expect(logged(tx)).To(BeZero())
				n, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{one(first[0])}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal([]int{2}))
				Expect(logged(tx)).To(Equal(1))
				Expect(tx.PutPostings(entity, index, "s3", nil, byteKeys(first[2]))).To(Succeed())
				var rows []string
				Expect(tx.ScanPostings(entity, index, []storage.IndexRange{one(first[2])}, nil, 0, func(key string) error {
					rows = append(rows, key)
					return nil
				})).To(Succeed())
				sort.Strings(rows)
				Expect(rows).To(Equal([]string{"r1", "s2", "s3"}))
				Expect(logged(tx)).To(Equal(2))
				return nil
			})
			Expect(pageWrites()).To(Equal([]string{"update", "update"}))
			checkPostings(store).strict(store)
		})

		It("writes the page changes before a savepoint, and drops the changes of a savepoint that fails", func() {
			pages := storedPages(store)
			k := stringKeys(pages[1].keys[:4])
			write(store, "ws", func(tx storage.WriteTx) error {
				Expect(tx.PutPostings(entity, index, "kept", nil, byteKeys(k[0]))).To(Succeed())
				Expect(tx.InSavepoint(func() error {
					Expect(tx.PutPostings(entity, index, "lost", nil, byteKeys(k[0], k[1]))).To(Succeed())
					Expect(tx.PutPostings(entity, index, "kept", byteKeys(k[0]), byteKeys(k[2]))).To(Succeed())
					return errors.New("rejected")
				})).To(MatchError("rejected"))
				// The page is read again: what the savepoint held of it is
				// gone.
				Expect(tx.PutPostings(entity, index, "kept", nil, byteKeys(k[3]))).To(Succeed())
				Expect(tx.InSavepoint(func() error {
					return tx.PutPostings(entity, index, "also", nil, byteKeys(k[1]))
				})).To(Succeed())
				Expect(tx.InSavepoint(func() error {
					Expect(tx.PutPostings(entity, index, "lost", nil, byteKeys(k[2]))).To(Succeed())
					n, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{one(k[2])}, 0)
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal([]int{2}))
					return errors.New("rejected")
				})).To(MatchError("rejected"))
				return nil
			})
			Expect(counts(store, "ws", 0, one(k[0]), one(k[1]), one(k[2]), one(k[3]))).To(Equal([]int{2, 2, 1, 2}))
			Expect(scan(store, "ws", nil, []storage.IndexRange{one(k[0]), one(k[1]), one(k[3])})).To(Equal([]string{"also", "kept", "r1"}))
			Expect(pageWrites()).To(Equal([]string{"update", "update", "update"}), "one write before each savepoint")
			checkPostings(store).strict(store)
		})

		It("writes the page changes when the held pages take more bytes than the limit", func() {
			pages := storedPages(store)
			// One page of ten keys is held within the limit, and two are not.
			pageHeld := 0
			for _, e := range pages[0].entries {
				pageHeld += heldSize(e)
			}
			setPostingHeld(postingHeldPageKeys, pageHeld+pageHeld/2)
			write(store, "ws", func(tx storage.WriteTx) error {
				// A key of each page, three times.
				for n := range 3 {
					for _, pg := range pages {
						Expect(tx.PutPostings(entity, index, fmt.Sprintf("s%d", n), nil, [][]byte{pg.keys[0], pg.keys[1]})).To(Succeed())
					}
				}
				// Each second call found the page of the call before it held
				// and wrote both, so no page is held now.
				Expect(logged(tx)).To(Equal(3 * len(pages)))
				Expect(tx.(*sqliteTx).postingPages).To(BeEmpty())
				return nil
			})
			Expect(pageWrites()).To(HaveLen(3 * len(pages)))
			for _, pg := range pages {
				Expect(counts(store, "ws", 0, one(string(pg.keys[0])), one(string(pg.keys[1])))).To(Equal([]int{4, 4}))
			}
			checkPostings(store).strict(store)
		})

		It("cuts a held page that gets many keys, and stores the pages it would store uncut", func() {
			// load puts 400 new keys into a scope with no page, in one
			// transaction and in an order that is not the key order, takes
			// some out again, and returns the stored pages.
			load := func(heldPageKeys int) []string {
				GinkgoHelper()
				setPostingHeld(heldPageKeys, postingHeldMax)
				store := openStore()
				rng := rand.New(rand.NewPCG(5, 8))
				order := rng.Perm(400)
				model := postingModel{}
				write(store, "ws", func(tx storage.WriteTx) error {
					for call := 0; call < 40; call++ {
						var add []string
						for _, i := range order[call*10 : call*10+10] {
							add = append(add, fmt.Sprintf("n%03d", i))
						}
						row := fmt.Sprintf("r%d", call%7)
						model.put(row, nil, byteKeys(add...))
						Expect(tx.PutPostings(entity, index, row, nil, byteKeys(add...))).To(Succeed())
					}
					var gone []string
					for _, i := range order[100:160] {
						gone = append(gone, fmt.Sprintf("n%03d", i))
					}
					for n := range 7 {
						row := fmt.Sprintf("r%d", n)
						model.put(row, byteKeys(gone...), nil)
						Expect(tx.PutPostings(entity, index, row, byteKeys(gone...), nil)).To(Succeed())
					}
					pc := tx.(*sqliteTx).postingPages[1]
					held := 0
					for i, pg := range pc.pages {
						Expect(len(pg.entries)).To(BeNumerically("<=", heldPageKeys), "held page %d", i)
						Expect(pg.cut).To(Equal(i > 0), "every held page but the first is a part of it")
						held += len(pg.entries)
					}
					Expect(held).To(Equal(340))
					if heldPageKeys < 400 {
						Expect(len(pc.pages)).To(BeNumerically(">=", 340/heldPageKeys))
					} else {
						Expect(pc.pages).To(HaveLen(1))
					}
					return nil
				})
				checkPostings(store).strict(store)
				Expect(storedPostings(store, "ws", entity, index)).To(Equal(model))
				pages := storedPages(store)
				out := loKeys(pages)
				for i, pg := range pages {
					out[i] += fmt.Sprintf(" (%d bytes)", pg.size)
				}
				return out
			}
			uncut := load(1 << 20)
			Expect(len(uncut)).To(BeNumerically(">=", 20))
			Expect(load(8)).To(Equal(uncut))
			Expect(load(3)).To(Equal(uncut))
		})

		It("counts the bytes of the held pages as keys come and go", func() {
			pages := storedPages(store)
			write(store, "ws", func(tx storage.WriteTx) error {
				t := tx.(*sqliteTx)
				sum := func() int {
					n := 0
					for _, pg := range t.postingPages[1].pages {
						for _, e := range pg.entries {
							n += heldSize(e)
						}
					}
					return n
				}
				// New keys, a second row for a key, and a row and a key taken
				// out again.
				Expect(tx.PutPostings(entity, index, "s1", nil, byteKeys("k05x", "k15x", "k15y", string(pages[1].keys[0])))).To(Succeed())
				Expect(t.postingPages[1].held).To(Equal(sum()))
				Expect(tx.PutPostings(entity, index, "s1", byteKeys("k15x", string(pages[1].keys[0])), byteKeys(string(pages[0].keys[1])))).To(Succeed())
				Expect(t.postingPages[1].held).To(Equal(sum()))
				Expect(tx.DeletePostings(entity, index, "r1", byteKeys(keys...))).To(Succeed())
				Expect(t.postingPages[1].held).To(Equal(sum()))
				return nil
			})
			checkPostings(store).strict(store)
		})

		It("reads a page one time for several keys of it", func() {
			pages := storedPages(store)
			write(store, "ws", func(tx storage.WriteTx) error {
				t := tx.(*sqliteTx)
				reads := func() int {
					GinkgoHelper()
					return t.postingPages[1].reads
				}
				// Every key of one page, a new key between two of them, and a
				// new key the page would hold after its last key.
				between := string(pages[1].keys[0]) + "!"
				after := string(pages[2].lo[:len(pages[2].lo)-1]) + "\x01"
				Expect(tx.PutPostings(entity, index, "r2", nil, append(byteKeys(between, after), pages[1].keys...))).To(Succeed())
				Expect(reads()).To(Equal(1))
				// Later calls of the transaction find the page held.
				Expect(tx.PutPostings(entity, index, "r3", nil, pages[1].keys[:3])).To(Succeed())
				Expect(tx.PutPostings(entity, index, "r2", [][]byte{pages[1].keys[4], []byte(between)}, nil)).To(Succeed())
				Expect(reads()).To(Equal(1))
				Expect(tx.PutPostings(entity, index, "r3", [][]byte{pages[3].keys[1]}, [][]byte{pages[3].keys[0]})).To(Succeed())
				Expect(reads()).To(Equal(2))
				Expect(tx.DeletePostings(entity, index, "r3", append([][]byte{pages[3].keys[0]}, pages[1].keys[:3]...))).To(Succeed())
				Expect(reads()).To(Equal(2), "a page read before another page is still held")
				return nil
			})
			checkPostings(store).strict(store)
			Expect(scan(store, "ws", []storage.IndexRange{one(string(pages[1].keys[5]))}, nil)).To(Equal([]string{"r1", "r2"}))
			Expect(scan(store, "ws", nil, []storage.IndexRange{one(string(pages[1].keys[0])), one(string(pages[3].keys[0]))})).To(Equal([]string{"r1", "r2"}))
		})

		It("writes no page for a change to a key with a head", func() {
			setPostingFormat(64, 2)
			// r1 holds every key; two more rows give k00 a head.
			put(store, "ws", "r2", "k00")
			Expect(tableCount(store, "posting_heads")).To(BeZero())
			put(store, "ws", "r3", "k00")
			Expect(where(store, "k00")).To(Equal("head"))
			pageWrites()
			// A change to the head alone writes no page.
			put(store, "ws", "r4", "k00")
			Expect(pageWrites()).To(BeEmpty())
			Expect(counts(store, "ws", 0, one("k00"))).To(Equal([]int{4}))
		})
	})

	Describe("a write of held page changes that fails", func() {
		var (
			store *Store
			pages []storedPage
			// p holds the keys of each page, as text.
			p [][]string
		)
		BeforeEach(func() {
			setPostingFormat(64, 128)
			store = openStore()
			var keys []string
			for i := range 40 {
				keys = append(keys, fmt.Sprintf("k%02d", i))
			}
			// Four pages of ten keys. The second one does not decode, so a
			// write that merges the third page into it fails.
			put(store, "ws", "r1", keys...)
			pages = storedPages(store)
			Expect(pages).To(HaveLen(4))
			p = nil
			for _, pg := range pages {
				p = append(p, stringKeys(pg.keys))
			}
			res, err := store.write.Exec(`UPDATE posting_pages SET body = x'ffff' WHERE lo = ?`, pages[1].lo)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(BeEquivalentTo(1))
		})
		// shrink removes all keys but one of the third page from r1, which
		// leaves the page small. The change is held: no page is read for the
		// merge until the changes are stored.
		shrink := func(tx storage.WriteTx) {
			GinkgoHelper()
			Expect(tx.PutPostings(entity, index, "r1", byteKeys(p[2][:9]...), nil)).To(Succeed())
		}
		// expectUnchanged checks that the pages that decode hold what the
		// setup wrote, and that only r1 has a row number.
		expectUnchanged := func() {
			GinkgoHelper()
			Expect(tableCount(store, "posting_rows")).To(Equal(1))
			Expect(tableCount(store, "posting_nums")).To(Equal(1))
			after := storedPagesWhere(store, `lo <> ?`, pages[1].lo)
			Expect(loKeys(after)).To(Equal(loKeys([]storedPage{pages[0], pages[2], pages[3]})))
			for _, pg := range after {
				for _, e := range pg.entries {
					Expect(numsOf(e)).To(Equal([]int64{1}))
				}
			}
		}

		It("refuses the commit when the write before a savepoint failed and the caller went on", func() {
			err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				Expect(tx.PutPostings(entity, index, "r2", nil, byteKeys(p[3][0]))).To(Succeed())
				shrink(tx)
				ran := false
				spErr := tx.InSavepoint(func() error {
					ran = true
					return tx.PutPostings(entity, index, "r3", nil, byteKeys(p[3][1]))
				})
				Expect(spErr).To(MatchError(ContainSubstring("posting page")))
				Expect(ran).To(BeFalse(), "the savepoint did not start")
				// A caller that takes the error as a refusal and goes on.
				Expect(tx.PutPostings(entity, index, "r4", nil, byteKeys(p[3][2]))).To(MatchError(ContainSubstring("posting page")))
				_, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{one(p[3][0])}, 0)
				Expect(err).To(MatchError(ContainSubstring("posting page")))
				Expect(tx.ClearPostings(entity, index)).To(MatchError(ContainSubstring("posting page")))
				return nil
			})
			Expect(err).To(MatchError(ContainSubstring("posting page")))
			expectUnchanged()
		})

		It("stores nothing when the write at the commit fails", func() {
			err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				Expect(tx.PutPostings(entity, index, "r2", nil, byteKeys(p[3][0]))).To(Succeed())
				shrink(tx)
				return nil
			})
			Expect(err).To(MatchError(ContainSubstring("posting page")))
			expectUnchanged()
		})

		DescribeTable("refuses the commit when the write before a read failed and the caller went on",
			func(read func(tx storage.WriteTx) error) {
				err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
					Expect(tx.PutPostings(entity, index, "r2", nil, byteKeys(p[3][0]))).To(Succeed())
					shrink(tx)
					Expect(read(tx)).To(MatchError(ContainSubstring("posting page")))
					Expect(tx.DeletePostings(entity, index, "r1", byteKeys(p[0]...))).To(MatchError(ContainSubstring("posting page")))
					return nil
				})
				Expect(err).To(MatchError(ContainSubstring("posting page")))
				expectUnchanged()
			},
			Entry("PostingCounts", func(tx storage.WriteTx) error {
				_, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{{Lo: []byte("k30")}}, 0)
				return err
			}),
			Entry("ScanPostings", func(tx storage.WriteTx) error {
				return tx.ScanPostings(entity, index, nil, []storage.IndexRange{{Lo: []byte("k30")}}, 0, func(string) error { return nil })
			}),
		)

		It("refuses the commit when the write at the limit of held changes failed and the caller went on", func() {
			setPostingHeld(postingHeldPageKeys, 1)
			err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				// The limit makes this call store its changes, which fails.
				Expect(tx.PutPostings(entity, index, "r1", byteKeys(p[2][:9]...), nil)).To(MatchError(ContainSubstring("posting page")))
				return nil
			})
			Expect(err).To(MatchError(ContainSubstring("posting page")))
			expectUnchanged()
		})

		It("refuses the commit when the write before an inner savepoint failed and the outer one went on", func() {
			err := store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				Expect(tx.InSavepoint(func() error {
					shrink(tx)
					ran := false
					inner := func() error {
						ran = true
						return errRejected
					}
					// Neither inner savepoint starts, so neither is rewound,
					// and the failure stays.
					Expect(tx.InSavepoint(inner)).To(MatchError(ContainSubstring("posting page")))
					Expect(tx.InSavepoint(inner)).To(MatchError(ContainSubstring("posting page")))
					Expect(ran).To(BeFalse())
					return nil
				})).To(Succeed())
				Expect(tx.ScanPostings(entity, index, nil, nil, 0, func(string) error { return nil })).To(MatchError(ContainSubstring("posting page")))
				return nil
			})
			Expect(err).To(MatchError(ContainSubstring("posting page")))
			expectUnchanged()
		})

		It("goes on after a savepoint that failed to write is rewound", func() {
			write(store, "ws", func(tx storage.WriteTx) error {
				Expect(tx.PutPostings(entity, index, "r2", nil, byteKeys(p[3][0]))).To(Succeed())
				err := tx.InSavepoint(func() error {
					shrink(tx)
					Expect(tx.PutPostings(entity, index, "r3", nil, byteKeys(p[3][1]))).To(Succeed())
					_, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{{Lo: []byte("k30")}}, 0)
					return err
				})
				Expect(err).To(MatchError(ContainSubstring("posting page")))
				// The same failure inside a savepoint inside a savepoint, where
				// the inner one does not start.
				err = tx.InSavepoint(func() error {
					shrink(tx)
					return tx.InSavepoint(func() error { return errors.New("not run") })
				})
				Expect(err).To(MatchError(ContainSubstring("posting page")))
				Expect(tx.PutPostings(entity, index, "r4", nil, byteKeys(p[3][2], p[0][0]))).To(Succeed())
				n, rows, err := tx.PostingCounts(entity, index, []storage.IndexRange{one(p[3][0]), one(p[3][1]), one(p[3][2]), one(p[2][0])}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal([]int{2, 1, 2, 1}))
				Expect(rows).To(Equal(3))
				return nil
			})
			Expect(counts(store, "ws", 0, one(p[3][0]), one(p[3][1]), one(p[3][2]), one(p[0][0]), one(p[2][0]), one(p[2][9]))).
				To(Equal([]int{2, 1, 2, 2, 1, 1}))
			Expect(scan(store, "ws", nil, []storage.IndexRange{{Lo: []byte("k30")}})).To(Equal([]string{"r1", "r2", "r4"}))
			Expect(loKeys(storedPagesWhere(store, `lo <> ?`, pages[1].lo))).To(Equal(loKeys([]storedPage{pages[0], pages[2], pages[3]})))
		})
	})

	DescribeTable("returns an error for a corrupt page body, and still clears the index and deletes the workspace",
		func(body []byte) {
			store := openStore()
			setup := func(ws string) {
				GinkgoHelper()
				put(store, ws, "r1", "a", "b")
				put(store, ws, "r2", "b", "c")
				res, err := store.write.Exec(`UPDATE posting_pages SET body = ?
					WHERE scope = (SELECT id FROM posting_scopes WHERE workspace_id = ?)`, body, ws)
				Expect(err).NotTo(HaveOccurred())
				Expect(res.RowsAffected()).To(BeEquivalentTo(1))
			}
			left := func() []int {
				GinkgoHelper()
				var out []int
				for _, table := range []string{"posting_chunks", "posting_heads", "posting_pages", "posting_rows", "posting_nums", "posting_scopes"} {
					out = append(out, tableCount(store, table))
				}
				return out
			}
			setup("ws")
			Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				_, _, err := tx.PostingCounts(entity, index, []storage.IndexRange{{}}, 0)
				return err
			})).To(MatchError(ContainSubstring("posting page")))
			Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				return tx.ScanPostings(entity, index, nil, []storage.IndexRange{{}}, 0, func(string) error { return nil })
			})).To(MatchError(ContainSubstring("posting page")))
			Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, index, "r3", nil, byteKeys("b"))
			})).To(MatchError(ContainSubstring("posting page")))
			Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				return tx.DeletePostings(entity, index, "r1", byteKeys("a", "b"))
			})).To(MatchError(ContainSubstring("posting page")))

			write(store, "ws", func(tx storage.WriteTx) error { return tx.ClearPostings(entity, index) })
			Expect(left()).To(Equal([]int{0, 0, 0, 0, 0, 0}))
			put(store, "ws", "r1", "a")
			Expect(counts(store, "ws", 0, storage.IndexRange{})).To(Equal([]int{1}))

			setup("ws2")
			Expect(store.DeleteWorkspace(ctx, "ws2")).To(Succeed())
			Expect(left()).To(Equal([]int{0, 0, 1, 1, 1, 1}), "the records of ws stay")
		},
		Entry("with a varint that does not end", []byte{0xff, 0xff}),
		Entry("with no entry", []byte{}),
		Entry("cut short", encodePage([]pageEntry{entryOf([]byte("a"), 1, 2)})[:4]),
		Entry("with keys out of order", []byte{0x00, 0x01, 'b', 0x01, 0x01, 0x00, 0x01, 'a', 0x01, 0x01}),
		Entry("with a key of no rows", []byte{0x00, 0x01, 'a', 0x00}),
		Entry("with a shared prefix longer than the key before", []byte{0x00, 0x01, 'a', 0x01, 0x01, 0x05, 0x01, 'b', 0x01, 0x01}),
	)

	It("fails a scan of a page whose row numbers do not grow", func() {
		store := openStore()
		put(store, "ws", "r1", "a")
		put(store, "ws", "r2", "a")
		_, err := store.write.Exec(`UPDATE posting_pages SET body = ?`, []byte{0x00, 0x01, 'a', 0x02, 0x01, 0x00})
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
			return tx.ScanPostings(entity, index, []storage.IndexRange{one("a")}, nil, 0, func(string) error { return nil })
		})).To(MatchError(ContainSubstring("delta 0")))
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			return tx.PutPostings(entity, index, "r3", nil, byteKeys("a"))
		})).To(MatchError(ContainSubstring("delta 0")))
	})

	It("fails a write that finds a key in a page and with a head", func() {
		setPostingFormat(postingPageBudget, 4)
		store := openStore()
		for i := range 5 {
			put(store, "ws", fmt.Sprintf("r%d", i), "t")
		}
		put(store, "ws", "r0", "s")
		Expect(where(store, "t")).To(Equal("head"))
		_, err := store.write.Exec(`UPDATE posting_pages SET body = ?`,
			encodePage([]pageEntry{entryOf([]byte("s"), 1), entryOf([]byte("t"), 1)}))
		Expect(err).NotTo(HaveOccurred())
		// Four removals leave the head with one row, so the key moves.
		err = store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			for i := 1; i < 5; i++ {
				if err := tx.PutPostings(entity, index, fmt.Sprintf("r%d", i), byteKeys("t"), nil); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(err).To(MatchError(ContainSubstring("has a head and is in a page")))
	})

	It("keeps the pages of each workspace and each index apart", func() {
		setPostingFormat(48, 128)
		store := openStore()
		type scopeRef struct{ ws, index string }
		refs := []scopeRef{{"ws1", "search"}, {"ws1", "other"}, {"ws2", "search"}}
		model := map[scopeRef]postingModel{}
		putIn := func(ref scopeRef, row string, remove, add []string) {
			GinkgoHelper()
			if model[ref] == nil {
				model[ref] = postingModel{}
			}
			model[ref].put(row, byteKeys(remove...), byteKeys(add...))
			write(store, ref.ws, func(tx storage.WriteTx) error {
				return tx.PutPostings(entity, ref.index, row, byteKeys(remove...), byteKeys(add...))
			})
		}
		check := func() {
			GinkgoHelper()
			checkPostings(store).strict(store)
			for _, ref := range refs {
				Expect(storedPostings(store, ref.ws, entity, ref.index)).To(Equal(model[ref].nonEmpty()), "%v", ref)
				Expect(store.WithReadTx(ctx, ref.ws, func(tx storage.ReadTx) error {
					ranges := []storage.IndexRange{{}, prefix("k"), one("k03"), {Hi: []byte("k10")}, {Lo: []byte("k10")}}
					got, _, err := tx.PostingCounts(entity, ref.index, ranges, 0)
					Expect(err).NotTo(HaveOccurred())
					for i, r := range ranges {
						Expect(got[i]).To(Equal(model[ref].count(r)), "%v range %d", ref, i)
						var rows []string
						Expect(tx.ScanPostings(entity, ref.index, nil, []storage.IndexRange{r}, 0, func(key string) error {
							rows = append(rows, key)
							return nil
						})).To(Succeed())
						sort.Strings(rows)
						Expect(rows).To(Equal(model[ref].scan(nil, []storage.IndexRange{r})), "%v range %d", ref, i)
					}
					return nil
				})).To(Succeed())
			}
		}
		// The same keys in every scope, with other rows, so that each scope
		// has several pages with the same lo values. The keys of row x fill
		// the first page of each scope.
		aKeys := []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7"}
		for n, ref := range refs {
			putIn(ref, "x", nil, aKeys)
			for i := range 20 {
				putIn(ref, fmt.Sprintf("r%d-%02d", n, i), nil, []string{fmt.Sprintf("k%02d", i), fmt.Sprintf("k%02d", (i+n+1)%20)})
			}
		}
		check()
		pagesOf := func(scope int64) []storedPage {
			GinkgoHelper()
			return slices.DeleteFunc(storedPages(store), func(pg storedPage) bool { return pg.scope != scope })
		}
		first, third := loKeys(pagesOf(1)), loKeys(pagesOf(3))
		Expect(len(first)).To(BeNumerically(">=", 3))
		Expect(pagesOf(2)[0].keys).To(Equal(byteKeys(aKeys[:len(pagesOf(2)[0].keys)]...)), "the first page holds keys of row x only")

		// The second scope loses row x in one call, and with it its first
		// page, so the page that is now its first does not start at the
		// empty key. That page then loses all its keys but one, which makes
		// it small. It must not merge into the last page of the first scope,
		// which is the record before it in the table.
		write(store, "ws1", func(tx storage.WriteTx) error {
			return tx.DeletePostings(entity, "other", "x", byteKeys(aKeys...))
		})
		delete(model[refs[1]], "x")
		head := pagesOf(2)[0]
		Expect(head.lo).NotTo(BeEmpty())
		check()
		for _, k := range head.keys[:len(head.keys)-1] {
			for row, held := range model[refs[1]].clone() {
				if held[string(k)] {
					putIn(refs[1], row, []string{string(k)}, nil)
				}
			}
		}
		Expect(pagesOf(2)[0].lo).To(Equal(head.lo))
		check()
		Expect(loKeys(pagesOf(1))).To(Equal(first))
		Expect(loKeys(pagesOf(3))).To(Equal(third))
		// A key below the first page of the second scope makes a page there,
		// and does not go into the last page of the first scope.
		putIn(refs[1], "low", nil, []string{"a", "a0"})
		check()
		Expect(loKeys(pagesOf(1))).To(Equal(first))
		Expect(loKeys(pagesOf(3))).To(Equal(third))
		Expect(loKeys(pagesOf(2)[:1])).To(Equal([]string{`"": "a a0"`}))

		// Clearing one scope and deleting one workspace leave the third.
		write(store, "ws1", func(tx storage.WriteTx) error { return tx.ClearPostings(entity, "search") })
		model[refs[0]] = postingModel{}
		check()
		Expect(pagesOf(1)).To(BeEmpty())
		Expect(loKeys(pagesOf(3))).To(Equal(third))
		Expect(store.DeleteWorkspace(ctx, "ws1")).To(Succeed())
		Expect(pagesOf(2)).To(BeEmpty())
		Expect(loKeys(pagesOf(3))).To(Equal(third))
	})

	It("removes the pages of an index when it is cleared, and builds it again under a new scope", func() {
		setPostingFormat(32, 4)
		store := openStore()
		build := func() {
			GinkgoHelper()
			write(store, "ws", func(tx storage.WriteTx) error {
				for i := range 30 {
					if err := tx.PutPostings(entity, index, fmt.Sprintf("r%02d", i), nil, byteKeys("all", fmt.Sprintf("k%02d", i%12))); err != nil {
						return err
					}
				}
				return nil
			})
		}
		build()
		before := checkPostings(store)
		Expect(len(before.pages)).To(BeNumerically(">=", 3))
		Expect(before.heads).To(HaveLen(1))
		// A clear drops the page changes of its transaction with the pages.
		write(store, "ws", func(tx storage.WriteTx) error {
			if err := tx.PutPostings(entity, index, "r00", byteKeys("k00"), byteKeys("new", "k05")); err != nil {
				return err
			}
			if err := tx.ClearPostings(entity, index); err != nil {
				return err
			}
			n, rows, err := tx.PostingCounts(entity, index, []storage.IndexRange{{}}, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal([]int{0}))
			Expect(rows).To(BeZero())
			return nil
		})
		for _, table := range []string{"posting_pages", "posting_heads", "posting_chunks", "posting_rows", "posting_nums", "posting_scopes"} {
			Expect(tableCount(store, table)).To(BeZero(), table)
		}
		build()
		after := checkPostings(store)
		after.strict(store)
		Expect(loKeys(after.pages)).To(Equal(loKeys(before.pages)))
		for _, pg := range after.pages {
			Expect(pg.scope).To(BeEquivalentTo(2), "the pages belong to the new scope")
		}
		Expect(counts(store, "ws", 0, one("all"), prefix("k"))).To(Equal([]int{30, 30}))
	})

	It("stays small and right when most rows are deleted", func() {
		setPostingFormat(64, 64)
		store := openStore()
		rng := rand.New(rand.NewPCG(17, 5))
		const rows = 300
		model := postingModel{}
		rowKey := func(i int) string { return fmt.Sprintf("r%03d", i) }
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range rows {
				// Two common keys, which get heads, and keys that a few
				// rows hold.
				keys := byteKeys("common", fmt.Sprintf("t%03d", i), fmt.Sprintf("t%03d", rng.IntN(rows)), fmt.Sprintf("u%02d", i%100))
				if i%2 == 0 {
					keys = append(keys, []byte("even"))
				}
				model.put(rowKey(i), nil, keys)
				if err := tx.PutPostings(entity, index, rowKey(i), nil, keys); err != nil {
					return err
				}
			}
			return nil
		})
		full := checkPostings(store)
		full.strict(store)
		Expect(full.heads).To(HaveLen(2))
		fullBytes := full.pageBytes()
		order := rng.Perm(rows)
		deleteRows := func(batch []int) {
			GinkgoHelper()
			write(store, "ws", func(tx storage.WriteTx) error {
				for _, i := range batch {
					if err := tx.DeletePostings(entity, index, rowKey(i), model.held(rowKey(i))); err != nil {
						return err
					}
					delete(model, rowKey(i))
				}
				return nil
			})
			idx := checkPostings(store)
			idx.strict(store)
			Expect(storedPostings(store, "ws", entity, index)).To(Equal(model))
			for _, r := range []storage.IndexRange{{}, one("common"), one("even"), prefix("t"), prefix("u0"), {Lo: []byte("t1"), Hi: []byte("u5")}} {
				Expect(counts(store, "ws", 0, r)).To(Equal([]int{model.count(r)}))
				Expect(scan(store, "ws", nil, []storage.IndexRange{r})).To(Equal(model.scan(nil, []storage.IndexRange{r})))
			}
			Expect(scan(store, "ws", []storage.IndexRange{one("common"), prefix("u0")}, nil)).
				To(Equal(model.scan([]storage.IndexRange{one("common"), prefix("u0")}, nil)))
		}
		// 95% of the rows go, in batches.
		gone := order[:rows*95/100]
		for len(gone) > 0 {
			n := min(len(gone), 15)
			deleteRows(gone[:n])
			gone = gone[n:]
		}
		sparse := checkPostings(store)
		Expect(sparse.heads).To(BeEmpty(), "the common keys moved back to a page")
		Expect(tableCount(store, "posting_chunks")).To(BeZero())
		// The pages that are left hold a twentieth of the rows. A page that
		// a removal leaves below a quarter of the budget merges with a page
		// next to it, so no two such pages are next to each other.
		Expect(sparse.pageBytes()).To(BeNumerically("<", fullBytes/5), "%d pages", len(sparse.pages))
		Expect(len(sparse.pages)).To(BeNumerically("<", len(full.pages)/2))
		for i, pg := range sparse.pages[1:] {
			if before := sparse.pages[i]; pg.size < postingPageBudget/4 {
				Expect(before.size).To(BeNumerically(">=", postingPageBudget/4),
					"pages %q of %d bytes and %q of %d bytes are both small", before.lo, before.size, pg.lo, pg.size)
			}
		}
		deleteRows(order[rows*95/100:])
		for _, table := range []string{"posting_pages", "posting_heads", "posting_chunks", "posting_rows", "posting_nums"} {
			Expect(tableCount(store, table)).To(BeZero(), table)
		}
		Expect(counts(store, "ws", 0, storage.IndexRange{})).To(Equal([]int{0}))
	})

	It("reads a file that has a head for every key, and moves a key to a page at its next write", func() {
		path := filepath.Join(GinkgoT().TempDir(), "version9.db")
		store, err := Open(path)
		Expect(err).NotTo(HaveOccurred())
		model := postingModel{}
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			for i := range 300 {
				keys := byteKeys("common", fmt.Sprintf("t%02d", i%40))
				if i < 2 {
					keys = append(keys, []byte("rare"))
				}
				row := fmt.Sprintf("r%03d", i)
				model.put(row, nil, keys)
				if err := tx.PutPostings(entity, index, row, nil, keys); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		paged := checkPostings(store)
		Expect(store.Close()).To(Succeed())

		// The file as version 9 wrote it: no page table, and each key of a
		// page in a head of its own, with its numbers as the tail.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		for _, pg := range paged.pages {
			for _, e := range pg.entries {
				_, err := db.Exec(`INSERT INTO posting_heads (scope, k, cnt, dir, tail) VALUES (?, ?, ?, NULL, ?)`,
					pg.scope, e.k, e.n, e.coded)
				Expect(err).NotTo(HaveOccurred())
			}
		}
		_, err = db.Exec(`DROP TABLE posting_pages; PRAGMA user_version = 9;`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err = Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		verify := func() {
			GinkgoHelper()
			checkPostings(store)
			Expect(storedPostings(store, "ws", entity, index)).To(Equal(model))
			for _, r := range []storage.IndexRange{{}, one("common"), one("rare"), one("t07"), prefix("t"), prefix("t1"), {Lo: []byte("rare")}} {
				Expect(counts(store, "ws", 0, r)).To(Equal([]int{model.count(r)}))
				Expect(scan(store, "ws", nil, []storage.IndexRange{r})).To(Equal(model.scan(nil, []storage.IndexRange{r})))
			}
			all := []storage.IndexRange{one("common"), prefix("t0")}
			Expect(scan(store, "ws", all, nil)).To(Equal(model.scan(all, nil)))
		}
		Expect(tableCount(store, "posting_pages")).To(BeZero())
		Expect(tableCount(store, "posting_heads")).To(Equal(42))
		verify()

		// An add and a removal each move the small key they change, and
		// leave the other heads as they are.
		model.put("r299", nil, byteKeys("rare"))
		put(store, "ws", "r299", "rare")
		Expect(where(store, "rare")).To(Equal("page"))
		Expect(tableCount(store, "posting_heads")).To(Equal(41))
		verify()
		model.put("r007", byteKeys("t07"), nil)
		write(store, "ws", func(tx storage.WriteTx) error {
			return tx.PutPostings(entity, index, "r007", byteKeys("t07"), nil)
		})
		Expect(where(store, "t07")).To(Equal("page"))
		Expect(where(store, "t08")).To(Equal("head"))
		Expect(where(store, "common")).To(Equal("head"))
		Expect(tableCount(store, "posting_heads")).To(Equal(40))
		verify()

		// A deleted row moves each small key it held, and a new row moves
		// each small key it gets.
		held := model.held("r010")
		delete(model, "r010")
		write(store, "ws", func(tx storage.WriteTx) error { return tx.DeletePostings(entity, index, "r010", held) })
		Expect(where(store, "t10")).To(Equal("page"))
		var every []string
		for i := range 40 {
			every = append(every, fmt.Sprintf("t%02d", i))
		}
		model.put("new", nil, byteKeys(every...))
		put(store, "ws", "new", every...)
		Expect(tableCount(store, "posting_heads")).To(Equal(1), "only the common key keeps its head")
		verify()
		checkPostings(store).strict(store)
	})

	It("gives readers on a snapshot the pages as they were while a writer splits, merges and moves them", func() {
		setPostingFormat(24, 4)
		store := openStore()
		keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
		write(store, "ws", func(tx storage.WriteTx) error {
			for i := range 16 {
				if err := tx.PutPostings(entity, index, fmt.Sprintf("r%02d", i), nil, byteKeys(keys[i%len(keys)])); err != nil {
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
						first, _, err := tx.PostingCounts(entity, index, ranges, 0)
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
						if err := tx.ScanPostings(entity, index, []storage.IndexRange{ranges[i]}, nil, 0, func(string) error {
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
		for range 150 {
			write(store, "ws", func(tx storage.WriteTx) error {
				for range 4 {
					row := fmt.Sprintf("r%02d", rng.IntN(24))
					k := byteKeys(keys[rng.IntN(len(keys))])
					if rng.IntN(2) == 0 {
						if err := tx.PutPostings(entity, index, row, nil, k); err != nil {
							return err
						}
					} else if err := tx.PutPostings(entity, index, row, k, nil); err != nil {
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
		checkPostings(store)
	})

	DescribeTable("agree with an in-memory model under random calls",
		func(seed uint64, pageBudget, pageRows, heldPageKeys, heldBytes int) {
			setPostingFormat(pageBudget, pageRows)
			setPostingHeld(heldPageKeys, heldBytes)
			checkModel(ctx, openStore(), rand.New(rand.NewPCG(seed, seed*7+1)), 120)
		},
		// Small pages, and keys that get a head at 5 rows and lose it below
		// 2, so that the random calls split, merge and move often.
		Entry("with small pages", uint64(11), 48, 4, 128, 16<<20),
		Entry("with small pages, other seed", uint64(12), 48, 4, 128, 16<<20),
		Entry("with held pages cut at three keys", uint64(13), 48, 4, 2, 16<<20),
		Entry("with held pages cut at five keys, and larger pages", uint64(17), 160, 6, 4, 16<<20),
		Entry("with a write of the held pages at each call", uint64(18), 48, 4, 128, 0),
		Entry("with a write of the held pages at a few hundred bytes", uint64(19), 48, 4, 3, 700),
		Entry("with pages of one or two keys", uint64(14), 8, 2, 128, 16<<20),
		Entry("with a head for every key", uint64(15), 48, 0, 128, 16<<20),
		Entry("with the shipped format", uint64(16), 512, 128, 128, 16<<20),
	)

	DescribeTable("agrees with an in-memory model under random calls with a random seed",
		func(heldPageKeys, heldBytes int) {
			setPostingFormat(48, 4)
			setPostingHeld(heldPageKeys, heldBytes)
			seed := uint64(GinkgoRandomSeed())
			checkModel(ctx, openStore(), rand.New(rand.NewPCG(seed, seed*7+1)), 120)
		},
		Entry("with the shipped limits of held pages", 128, 16<<20),
		Entry("with small limits of held pages", 3, 900),
	)
})

func byteKeys(ks ...string) [][]byte {
	out := make([][]byte, len(ks))
	for i, k := range ks {
		out[i] = []byte(k)
	}
	return out
}

func stringKeys(ks [][]byte) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

// entryOf makes the page entry of k with the row numbers nums.
func entryOf(k []byte, nums ...int64) pageEntry { return newPageEntry(k, nums) }

// numsOf decodes the row numbers of e, which are ascending when they decode.
func numsOf(e pageEntry) []int64 {
	GinkgoHelper()
	nums, err := e.nums()
	Expect(err).NotTo(HaveOccurred())
	return nums
}

func keysOf(es []pageEntry) [][]byte {
	out := make([][]byte, len(es))
	for i, e := range es {
		out[i] = e.k
	}
	return out
}

// setPostingFormat sets the page budget and the most row numbers of a page
// key for the rest of the spec. A pageRows of 0 gives every key a head.
func setPostingFormat(pageBudget, pageRows int) {
	wasBudget, wasRows := postingPageBudget, postingPageMaxRows
	postingPageBudget, postingPageMaxRows = pageBudget, pageRows
	DeferCleanup(func() { postingPageBudget, postingPageMaxRows = wasBudget, wasRows })
}

// setPostingHeld sets the most keys of one held page and the most bytes of
// held pages for the rest of the spec.
func setPostingHeld(pageKeys, bytes int) {
	wasKeys, wasBytes := postingHeldPageKeys, postingHeldMax
	postingHeldPageKeys, postingHeldMax = pageKeys, bytes
	DeferCleanup(func() { postingHeldPageKeys, postingHeldMax = wasKeys, wasBytes })
}

// storedPage is one stored page, decoded.
type storedPage struct {
	scope   int64
	lo      []byte
	keys    [][]byte
	entries []pageEntry
	size    int
}

// storedPages reads every page of the store in scope and lo order.
func storedPages(store *Store) []storedPage {
	GinkgoHelper()
	return storedPagesWhere(store, `TRUE`)
}

// storedPagesWhere reads the pages of the store that match a condition, in
// scope and lo order.
func storedPagesWhere(store *Store, where string, args ...any) []storedPage {
	GinkgoHelper()
	rows, err := store.read.Query(`SELECT scope, lo, body FROM posting_pages WHERE `+where+` ORDER BY scope, lo`, args...)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()
	var out []storedPage
	for rows.Next() {
		var (
			pg       storedPage
			lo, body []byte
		)
		Expect(rows.Scan(&pg.scope, &lo, &body)).To(Succeed())
		es, err := decodePage(body)
		Expect(err).NotTo(HaveOccurred(), "scope %d page %x", pg.scope, lo)
		pg.lo, pg.keys, pg.entries, pg.size = notNilBytes(lo), keysOf(es), es, len(body)
		out = append(out, pg)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
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

// storedKey names one posting key of one scope.
type storedKey struct {
	scope int64
	k     string
}

// storedIndex is what checkPostings read.
type storedIndex struct {
	// nums holds the row numbers of every key, from its page or its head.
	nums map[storedKey][]int64
	// heads holds the keys that have a head, and sizes the sizes of their
	// chunks.
	heads map[storedKey]bool
	sizes map[storedKey][]int
	pages []storedPage
}

// checkPostings reads every head, chunk and page of the store, checks their
// format, and returns what they hold.
//
// Heads: the count is what the directory and the tail hold; each chunk holds
// the numbers its directory entry says, no more than a chunk may hold, all
// above the chunk before it; the tail is below its cap; no head is empty; and
// the chunk table holds exactly the chunks that the directories name.
//
// Pages: each page decodes and holds one key or more, in order, all at or
// above its lo and below the lo of the next page of the scope; a page of two
// keys or more is within the budget; each key has one to postingPageMaxRows
// ascending numbers; and no key is in a page and has a head.
func checkPostings(store *Store) storedIndex {
	GinkgoHelper()
	idx := storedIndex{nums: map[storedKey][]int64{}, heads: map[storedKey]bool{}, sizes: map[storedKey][]int{}}
	rows, err := store.read.Query(`SELECT scope, k, cnt, dir, tail FROM posting_heads ORDER BY scope, k`)
	Expect(err).NotTo(HaveOccurred())
	type head struct {
		ref storedKey
		h   postingHead
	}
	var heads []head
	for rows.Next() {
		var (
			hd           head
			k, dir, tail []byte
		)
		Expect(rows.Scan(&hd.ref.scope, &k, &hd.h.cnt, &dir, &tail)).To(Succeed())
		hd.ref.k = string(k)
		Expect(fillHead(&hd.h, dir, tail)).To(Succeed(), "head of key %x", k)
		heads = append(heads, hd)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	Expect(rows.Close()).To(Succeed())
	chunks := 0
	for _, hd := range heads {
		var all []int64
		for _, c := range hd.h.chunks {
			Expect(c.n).To(BeNumerically("<=", postingChunkCap))
			var (
				b     []byte
				scope int64
			)
			Expect(store.read.QueryRow(`SELECT scope, nums FROM posting_chunks WHERE id = ?`, c.id).Scan(&scope, &b)).To(Succeed())
			Expect(scope).To(Equal(hd.ref.scope), "chunk %d of key %x", c.id, hd.ref.k)
			got, err := decodeChunk(c, b, nil)
			Expect(err).NotTo(HaveOccurred())
			if len(all) > 0 {
				Expect(got[0]).To(BeNumerically(">", all[len(all)-1]))
			}
			all = append(all, got...)
			idx.sizes[hd.ref] = append(idx.sizes[hd.ref], c.n)
			chunks++
		}
		Expect(len(hd.h.tail)).To(BeNumerically("<", postingTailCap))
		all = append(all, hd.h.tail...)
		Expect(all).NotTo(BeEmpty(), "a head that holds nothing is deleted")
		idx.nums[hd.ref] = all
		idx.heads[hd.ref] = true
	}
	var stored int
	Expect(store.read.QueryRow(`SELECT COUNT(*) FROM posting_chunks`).Scan(&stored)).To(Succeed())
	Expect(stored).To(Equal(chunks), "no chunk record outlives its directory entry")

	idx.pages = storedPages(store)
	for i, pg := range idx.pages {
		Expect(pg.entries).NotTo(BeEmpty(), "scope %d page %x is empty", pg.scope, pg.lo)
		if len(pg.entries) > 1 {
			Expect(pg.size).To(BeNumerically("<=", postingPageBudget), "scope %d page %x", pg.scope, pg.lo)
		}
		Expect(bytes.Compare(pg.keys[0], pg.lo)).To(BeNumerically(">=", 0), "scope %d page %x starts below its lo", pg.scope, pg.lo)
		if i+1 < len(idx.pages) && idx.pages[i+1].scope == pg.scope {
			Expect(bytes.Compare(pg.keys[len(pg.keys)-1], idx.pages[i+1].lo)).To(BeNumerically("<", 0),
				"scope %d page %x reaches into the next page", pg.scope, pg.lo)
		}
		for j, e := range pg.entries {
			if j > 0 {
				Expect(bytes.Compare(pg.keys[j-1], e.k)).To(BeNumerically("<", 0))
			}
			nums := numsOf(e)
			Expect(nums).To(HaveLen(e.n), "page key %x", e.k)
			Expect(nums).NotTo(BeEmpty())
			Expect(len(nums)).To(BeNumerically("<=", postingPageMaxRows), "page key %x", e.k)
			ref := storedKey{pg.scope, string(e.k)}
			Expect(idx.heads[ref]).To(BeFalse(), "key %x is in a page and has a head", e.k)
			Expect(idx.nums).NotTo(HaveKey(ref), "key %x is in two pages", e.k)
			idx.nums[ref] = nums
		}
	}
	return idx
}

// strict checks what holds for a store that this version of the package
// wrote with one format, when every DeletePostings call named all the keys
// of its row: each row number names a row, and each head holds half of
// postingPageMaxRows numbers or more.
func (idx storedIndex) strict(store *Store) {
	GinkgoHelper()
	rows, err := store.read.Query(`SELECT scope, num FROM posting_nums`)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()
	named := map[postingNum]bool{}
	for rows.Next() {
		var n postingNum
		Expect(rows.Scan(&n.scope, &n.num)).To(Succeed())
		named[n] = true
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	for ref, nums := range idx.nums {
		for _, num := range nums {
			Expect(named[postingNum{ref.scope, num}]).To(BeTrue(), "number %d of key %x has no row", num, ref.k)
		}
		if idx.heads[ref] {
			Expect(len(nums)).To(BeNumerically(">=", postingPageMaxRows/2), "head of key %x", ref.k)
		}
	}
}

// pageBytes is the sum of the page body sizes.
func (idx storedIndex) pageBytes() int {
	n := 0
	for _, pg := range idx.pages {
		n += pg.size
	}
	return n
}

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

func (m postingModel) nonEmpty() postingModel {
	out := postingModel{}
	for r, ks := range m {
		if len(ks) > 0 {
			out[r] = ks
		}
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

// storedPostings reads the postings of one index of ws from the stored pages,
// heads, chunks and row numbers.
func storedPostings(store *Store, ws, entity, index string) postingModel {
	GinkgoHelper()
	out := postingModel{}
	var scope int64
	err := store.read.QueryRow(`SELECT id FROM posting_scopes WHERE workspace_id = ? AND entity = ? AND idx = ?`, ws, entity, index).Scan(&scope)
	if errors.Is(err, sql.ErrNoRows) {
		return out
	}
	Expect(err).NotTo(HaveOccurred())
	keyOf := map[int64]string{}
	rows, err := store.read.Query(`SELECT num, key FROM posting_nums WHERE scope = ?`, scope)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()
	for rows.Next() {
		var (
			num int64
			key string
		)
		Expect(rows.Scan(&num, &key)).To(Succeed())
		keyOf[num] = key
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	for ref, nums := range checkPostings(store).nums {
		if ref.scope != scope {
			continue
		}
		for _, n := range nums {
			key, ok := keyOf[n]
			Expect(ok).To(BeTrue(), "number %d of key %x has no row", n, ref.k)
			if out[key] == nil {
				out[key] = map[string]bool{}
			}
			out[key][ref.k] = true
		}
	}
	return out
}

// modelKeys are the posting keys of checkModel: 300 keys of up to five
// bytes, with shared prefixes, the bytes 0x00 and 0xff, and the empty key.
// The first modelHotKeys of them are picked much more often than the others,
// so that they gather rows and get heads, while the others fill pages.
var modelKeys = func() [][]byte {
	rng := rand.New(rand.NewPCG(3, 4))
	alphabet := []byte{0x00, 0x00, 'a', 'a', 'b', 'c', 0xff, 0xff}
	seen := map[string]bool{"": true}
	out := [][]byte{{}}
	for len(out) < 300 {
		k := make([]byte, 1+rng.IntN(5))
		for i := range k {
			k[i] = alphabet[rng.IntN(len(alphabet))]
		}
		if !seen[string(k)] {
			seen[string(k)] = true
			out = append(out, k)
		}
	}
	return out
}()

const modelHotKeys = 6

func randomKey(rng *rand.Rand) []byte {
	if rng.IntN(5) < 2 {
		return modelKeys[rng.IntN(modelHotKeys)]
	}
	return modelKeys[rng.IntN(len(modelKeys))]
}

func randomRange(rng *rand.Rand) storage.IndexRange {
	pick := func() []byte {
		if rng.IntN(5) == 0 {
			return nil
		}
		return randomKey(rng)
	}
	switch rng.IntN(4) {
	case 0, 1:
		k := randomKey(rng)
		return storage.IndexRange{Lo: k, Hi: append(slices.Clone(k), 0)}
	case 2:
		// The keys with one prefix of one or two bytes.
		k := randomKey(rng)
		if len(k) > 0 && k[0] != 0xff {
			lo := slices.Clone(k[:min(len(k), 1+rng.IntN(2))])
			hi := slices.Clone(lo)
			for i := len(hi) - 1; i >= 0; i-- {
				if hi[i] != 0xff {
					hi[i]++
					return storage.IndexRange{Lo: lo, Hi: hi[:i+1]}
				}
			}
		}
	}
	return storage.IndexRange{Lo: pick(), Hi: pick()}
}

func randomKeys(rng *rand.Rand, max int) [][]byte {
	var out [][]byte
	for range rng.IntN(max + 1) {
		out = append(out, randomKey(rng))
	}
	return out
}

// errRejected is what a savepoint of checkModel that fails returns.
var errRejected = errors.New("rejected")

// modelRun is one write transaction of checkModel: the transaction and the
// model as its calls so far leave it.
type modelRun struct {
	tx   storage.WriteTx
	rng  *rand.Rand
	next postingModel
}

func (r *modelRun) rowKey() string { return fmt.Sprintf("row%02d", r.rng.IntN(24)) }

// ops makes n random calls. A call can be a savepoint, which makes calls of
// its own, down to depth 0.
func (r *modelRun) ops(n, depth int) error {
	const entity, index = "doc", "search"
	rng, tx := r.rng, r.tx
	for range n {
		switch op := rng.IntN(48); {
		case op < 22:
			row := r.rowKey()
			remove := randomKeys(rng, 2)
			if rng.IntN(2) == 0 {
				remove = append(remove, r.next.held(row)...)
			}
			add := randomKeys(rng, 5)
			if err := tx.PutPostings(entity, index, row, remove, add); err != nil {
				return err
			}
			r.next.put(row, remove, add)
		case op < 25:
			// One hot key for many rows, or for none: the key gets a head,
			// or loses it.
			k := [][]byte{modelKeys[rng.IntN(modelHotKeys)]}
			add := rng.IntN(2) == 0
			for i := range 24 {
				row := fmt.Sprintf("row%02d", i)
				var err error
				if add {
					err = tx.PutPostings(entity, index, row, nil, k)
					r.next.put(row, nil, k)
				} else {
					err = tx.PutPostings(entity, index, row, k, nil)
					r.next.put(row, k, nil)
				}
				if err != nil {
					return err
				}
			}
		case op < 32:
			row := r.rowKey()
			if err := tx.DeletePostings(entity, index, row, r.next.held(row)); err != nil {
				return err
			}
			delete(r.next, row)
		case op < 33:
			if err := tx.ClearPostings(entity, index); err != nil {
				return err
			}
			clear(r.next)
		case op < 41 && depth > 0:
			// A savepoint that is kept, or one that is rewound with the
			// calls inside it, which stands for a rejected mutation.
			saved := r.next.clone()
			fail := rng.IntN(2) == 0
			err := tx.InSavepoint(func() error {
				if err := r.ops(1+rng.IntN(4), depth-1); err != nil {
					return err
				}
				if fail {
					return errRejected
				}
				return nil
			})
			switch {
			case fail && !errors.Is(err, errRejected):
				return fmt.Errorf("savepoint: %v", err)
			case fail:
				r.next = saved
			case err != nil:
				return err
			}
		case op < 44:
			ranges := []storage.IndexRange{randomRange(rng), randomRange(rng)}
			n, _, err := tx.PostingCounts(entity, index, ranges, 0)
			if err != nil {
				return err
			}
			Expect(n).To(Equal([]int{r.next.count(ranges[0]), r.next.count(ranges[1])}))
		default:
			// A read inside the write transaction sees its own calls.
			all, anyR := []storage.IndexRange{randomRange(rng)}, []storage.IndexRange{randomRange(rng)}
			var got []string
			if err := tx.ScanPostings(entity, index, all, anyR, 0, func(k string) error {
				got = append(got, k)
				return nil
			}); err != nil {
				return err
			}
			sort.Strings(got)
			Expect(got).To(Equal(r.next.scan(all, anyR)))
		}
	}
	return nil
}

// checkModel runs random write transactions against store and against a
// model, and after each one compares the stored records and the reads. The
// transactions hold savepoints that are kept and savepoints that are rewound,
// two deep, and one in ten returns an error and leaves nothing.
func checkModel(ctx context.Context, store *Store, rng *rand.Rand, txs int) {
	GinkgoHelper()
	const entity, index = "doc", "search"
	workspaces := []string{"a", "b"}
	models := map[string]postingModel{"a": {}, "b": {}}
	errAbort := errors.New("abort")
	// What the transactions made, to check that they did what the spec is
	// for: the most pages at one time, and the keys seen with a head and in
	// a page.
	var (
		mostPages       int
		headed, inPages = map[storedKey]bool{}, map[storedKey]bool{}
	)
	defer func() {
		GinkgoHelper()
		if postingPageMaxRows > 0 && postingPageBudget <= 64 {
			Expect(mostPages).To(BeNumerically(">=", 8), "the calls made few pages")
		}
		if postingPageMaxRows > 1 && postingPageMaxRows < 12 {
			moved := 0
			for ref := range headed {
				if inPages[ref] {
					moved++
				}
			}
			Expect(moved).To(BeNumerically(">=", 2), "few keys moved between a page and a head")
		}
	}()
	for range txs {
		ws := workspaces[rng.IntN(len(workspaces))]
		run := &modelRun{rng: rng, next: models[ws].clone()}
		abort := rng.IntN(10) == 0
		err := store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			run.tx = tx
			if err := run.ops(1+rng.IntN(8), 2); err != nil {
				return err
			}
			if abort {
				return errAbort
			}
			return nil
		})
		if abort {
			Expect(err).To(MatchError(errAbort))
		} else {
			Expect(err).NotTo(HaveOccurred())
			models[ws] = run.next
		}
		idx := checkPostings(store)
		idx.strict(store)
		mostPages = max(mostPages, len(idx.pages))
		for ref := range idx.nums {
			if idx.heads[ref] {
				headed[ref] = true
			} else {
				inPages[ref] = true
			}
		}
		for _, ws := range workspaces {
			Expect(storedPostings(store, ws, entity, index)).To(Equal(models[ws].nonEmpty()), "stored postings of %s", ws)
		}

		for _, ws := range workspaces {
			m := models[ws]
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				ranges := []storage.IndexRange{randomRange(rng), randomRange(rng), randomRange(rng), {}}
				got, _, err := tx.PostingCounts(entity, index, ranges, 0)
				Expect(err).NotTo(HaveOccurred())
				for i, r := range ranges {
					Expect(got[i]).To(Equal(m.count(r)), "range %x..%x", r.Lo, r.Hi)
				}
				capped, _, err := tx.PostingCounts(entity, index, ranges, 2)
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
					Expect(tx.ScanPostings(entity, index, all, anyR, limit, func(k string) error {
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
