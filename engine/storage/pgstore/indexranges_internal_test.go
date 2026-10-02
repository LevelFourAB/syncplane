package pgstore

import (
	"context"
	"fmt"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The conformance suite pins what ScanIndexRanges yields, but it cannot name
// the page size, so it cannot put a page boundary where the paging changes
// course: at the end of a range, inside one, or past ranges that hold nothing.
// These specs size the ranges by scanBatch and hold each one to what ScanIndex
// yields for it.
var _ = Describe("Postgres multi-range index reads", func() {
	ctx := context.Background()
	const ws = "ws"

	DescribeTable("yield what one ScanIndex per range yields",
		// sizes holds the entries of each range. Ranges named in unbounded have
		// no Hi, so they also hold the entries of every range after them.
		func(sizes []int, unbounded ...int) {
			store, err := Open(ctx, pgtest.DSN())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				for r, n := range sizes {
					for j := 0; j < n; j++ {
						// Every entry of a range starts with its byte, and many
						// share the whole entry, as the rows below one parent do,
						// so a page that resumes by entry alone repeats them.
						if err := tx.PutIndexEntries("issue", fmt.Sprintf("r%02d-%04d", r, j), []storage.IndexEntry{
							{Index: "by_parent", Key: []byte{byte(r + 1), byte(j % 3)}},
						}); err != nil {
							return err
						}
					}
				}
				return nil
			})).To(Succeed())

			ranges := make([]storage.IndexRange, len(sizes))
			for r := range sizes {
				ranges[r] = storage.IndexRange{Lo: []byte{byte(r + 1)}, Hi: []byte{byte(r + 2)}}
				if slices.Contains(unbounded, r) {
					ranges[r].Hi = nil
				}
			}
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				var want []string
				for i, r := range ranges {
					if err := tx.ScanIndex("issue", "by_parent", r.Lo, r.Hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
						want = append(want, fmt.Sprintf("%d:%s", i, rowKey))
						return nil
					}); err != nil {
						return err
					}
				}
				var got []string
				if err := tx.ScanIndexRanges("issue", "by_parent", ranges, func(i int, _ []byte, rowKey string) error {
					got = append(got, fmt.Sprintf("%d:%s", i, rowKey))
					return nil
				}); err != nil {
					return err
				}
				Expect(got).To(Equal(want))
				return nil
			})).To(Succeed())
		},
		Entry("a range that ends with the page", []int{scanBatch, 3, 2}),
		Entry("a range that ends one entry past the page", []int{scanBatch + 1, 3}),
		Entry("a range across several pages", []int{3, 2*scanBatch + 10, 3}),
		Entry("ranges that hold nothing around a page boundary", []int{0, scanBatch - 1, 0, 0, 2, 0}),
		Entry("small ranges across pages", []int{100, 100, 100, 100, 100, 100}),
		Entry("an unbounded range inside a run", []int{scanBatch, 5, scanBatch, 5}, 1),
		Entry("an unbounded range at each end", []int{4, scanBatch + 2, 4}, 0, 2),
	)

	It("yields an entry once for each range that holds it, across a page boundary", func() {
		store, err := Open(ctx, pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for j := 0; j < scanBatch-10; j++ {
				if err := tx.PutIndexEntries("issue", fmt.Sprintf("k%04d", j), []storage.IndexEntry{
					{Index: "by_parent", Key: []byte{0x01}},
				}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		same := storage.IndexRange{Lo: []byte{0x01}, Hi: []byte{0x02}}
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			perRange := make([]int, 3)
			if err := tx.ScanIndexRanges("issue", "by_parent", []storage.IndexRange{same, same, same}, func(i int, _ []byte, _ string) error {
				perRange[i]++
				return nil
			}); err != nil {
				return err
			}
			Expect(perRange).To(Equal([]int{scanBatch - 10, scanBatch - 10, scanBatch - 10}))
			return nil
		})).To(Succeed())
	})
})
