package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// postingSpecs registers the specs of posting indexes (WriteTx.PutPostings,
// DeletePostings, ClearPostings, ReadTx.PostingCounts, ScanPostings). The
// getters return the adapter and context of the running spec.
func postingSpecs(getStore func() storage.Adapter, getCtx func() context.Context) {
	write := func(ws string, fn func(storage.WriteTx)) {
		GinkgoHelper()
		Expect(getStore().WithWriteTx(getCtx(), ws, func(tx storage.WriteTx) error {
			fn(tx)
			return nil
		})).To(Succeed())
	}
	read := func(ws string, fn func(storage.ReadTx)) {
		GinkgoHelper()
		Expect(getStore().WithReadTx(getCtx(), ws, func(tx storage.ReadTx) error {
			fn(tx)
			return nil
		})).To(Succeed())
	}
	// Posting keys are opaque bytes. These are hand-written and include 0x00
	// and 0xff, which a text collation or a C string would mishandle.
	keys := func(ks ...string) [][]byte {
		out := make([][]byte, len(ks))
		for i, k := range ks {
			out[i] = []byte(k)
		}
		return out
	}
	scan := func(tx storage.ReadTx, entity, idx string, all, any []storage.IndexRange, limit int) []string {
		GinkgoHelper()
		got := []string{}
		Expect(tx.ScanPostings(entity, idx, all, any, limit, func(rowKey string) error {
			got = append(got, rowKey)
			return nil
		})).To(Succeed())
		sort.Strings(got)
		return got
	}
	counts := func(tx storage.ReadTx, entity, idx string, max int, ranges ...storage.IndexRange) []int {
		GinkgoHelper()
		got, _, err := tx.PostingCounts(entity, idx, ranges, max)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(len(ranges)))
		return got
	}
	// rowsIn is the row count PostingCounts returns, read with no ranges and
	// with one.
	rowsIn := func(tx storage.ReadTx, entity, idx string) int {
		GinkgoHelper()
		_, rows, err := tx.PostingCounts(entity, idx, nil, 0)
		Expect(err).NotTo(HaveOccurred())
		_, again, err := tx.PostingCounts(entity, idx, []storage.IndexRange{{}}, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(again).To(Equal(rows), "the row count does not depend on the ranges")
		return rows
	}
	r := func(rs ...storage.IndexRange) []storage.IndexRange { return rs }
	ex := func(k string) storage.IndexRange { return exactRange([]byte(k)) }
	pre := func(p string) storage.IndexRange { return prefixRange([]byte(p)) }

	It("reads an index that holds nothing as empty", func() {
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"), storage.IndexRange{})).To(Equal([]int{0, 0}))
			Expect(counts(tx, "doc", "search", 0)).To(BeEmpty())
			Expect(rowsIn(tx, "doc", "search")).To(BeZero())
			Expect(scan(tx, "doc", "search", r(ex("a")), r(ex("b")), 0)).To(BeEmpty())
		})
	})

	It("counts each row that got a posting once, until DeletePostings or ClearPostings removes it", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a", "b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r3", keys("a"), nil)).To(Succeed(),
				"a call that only removes gives no row a place in the index")
			Expect(rowsIn(tx, "doc", "search")).To(Equal(2), "the transaction reads its own writes")
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(2))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r2", keys("a", "b"), nil)).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, storage.IndexRange{})).To(Equal([]int{2}))
			Expect(rowsIn(tx, "doc", "search")).To(Equal(2),
				"a row whose postings PutPostings removed still counts")
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "r2", nil)).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "never", keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "r1", keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(BeZero(),
				"a deleted row does not count, also when held left a posting out")
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("c"))).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "r1", keys("c"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("d"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r4", nil, keys("d"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(2), "a deleted row that comes back counts once")
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.ClearPostings("doc", "search")).To(Succeed())
			Expect(rowsIn(tx, "doc", "search")).To(BeZero())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(BeZero())
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1), "a cleared index counts again from 0")
		})
	})

	It("counts a row whose postings one transaction adds and removes again", func() {
		// No read between the calls: an adapter that collects the calls of a
		// transaction sees them together.
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", keys("a"), nil)).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("a"))).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "r2", keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, storage.IndexRange{})).To(Equal([]int{0}))
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1),
				"r1 got a posting and no DeletePostings removed it; r2 was deleted")
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "r1", nil)).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(BeZero())
		})
	})

	It("counts and yields the rows of each key", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("apple", "pear"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("apple"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r3", nil, keys("plum"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("apple"), ex("pear"), ex("plum"), ex("fig"))).
				To(Equal([]int{2, 1, 1, 0}))
			Expect(scan(tx, "doc", "search", nil, r(ex("apple")), 0)).To(Equal([]string{"r1", "r2"}))
			Expect(scan(tx, "doc", "search", r(ex("apple")), nil, 0)).To(Equal([]string{"r1", "r2"}))
			Expect(scan(tx, "doc", "search", r(ex("plum")), nil, 0)).To(Equal([]string{"r3"}))
			Expect(scan(tx, "doc", "search", r(ex("fig")), nil, 0)).To(BeEmpty())
		})
	})

	It("counts (key, row) pairs over a range, so a row with two keys in it counts twice", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("pa", "pb"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("pa", "q"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0,
				pre("p"),
				storage.IndexRange{},
				storage.IndexRange{Lo: []byte("pb")},
				storage.IndexRange{Hi: []byte("pb")},
				storage.IndexRange{Lo: []byte("q"), Hi: []byte("p")},
			)).To(Equal([]int{3, 4, 2, 2, 0}))
			Expect(scan(tx, "doc", "search", nil, r(pre("p")), 0)).To(Equal([]string{"r1", "r2"}),
				"a scan yields each row once however many keys it holds in the range")
		})
	})

	It("may stop counting past max, and counts exactly up to it", func() {
		write("ws1", func(tx storage.WriteTx) {
			for i := 0; i < 20; i++ {
				Expect(tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, keys("many"))).To(Succeed())
			}
			Expect(tx.PutPostings("doc", "search", "r00", nil, keys("few"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r01", nil, keys("few"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			got := counts(tx, "doc", "search", 5, ex("many"), ex("few"), ex("none"), pre("m"))
			Expect(got[0]).To(BeNumerically(">", 5))
			Expect(got[1:3]).To(Equal([]int{2, 0}))
			Expect(got[3]).To(BeNumerically(">", 5))
			Expect(counts(tx, "doc", "search", 20, ex("many"))).To(Equal([]int{20}))
		})
	})

	It("adds and removes with set semantics", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a", "a", "b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", keys("zzz"), nil)).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"), ex("b"), ex("zzz"))).To(Equal([]int{1, 1, 0}))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", keys("a", "a"), nil)).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", keys("a"), nil)).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"), ex("b"))).To(Equal([]int{0, 1}))
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(BeEmpty())
			Expect(scan(tx, "doc", "search", nil, r(ex("b")), 0)).To(Equal([]string{"r1"}))
		})
	})

	It("removes before it adds, so a key in both is held after the call", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "held", nil, keys("x"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "held", keys("x"), keys("x"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "new", keys("x"), keys("x"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("x"))).To(Equal([]int{2}))
			Expect(scan(tx, "doc", "search", r(ex("x")), nil, 0)).To(Equal([]string{"held", "new"}))
		})
	})

	It("registers the workspace only for a call that adds", func() {
		write("ghost", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", keys("a"), nil)).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", nil, nil)).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "r1", keys("a"))).To(Succeed())
			Expect(tx.ClearPostings("doc", "search")).To(Succeed())
		})
		Expect(getStore().ListWorkspaceIDs(getCtx(), storage.Range{})).To(BeEmpty())
		write("ghost", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
		})
		Expect(getStore().ListWorkspaceIDs(getCtx(), storage.Range{})).To(ConsistOf("ghost"))
	})

	It("refuses an add to a deleted workspace", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
		})
		Expect(getStore().DeleteWorkspace(getCtx(), "ws1")).To(Succeed())
		err := getStore().WithWriteTx(getCtx(), "ws1", func(tx storage.WriteTx) error {
			return tx.PutPostings("doc", "search", "r1", nil, keys("a"))
		})
		Expect(err).To(MatchError(storage.ErrWorkspaceDeleted))
	})

	It("forgets a deleted row and lets the key come back", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a", "b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("a"))).To(Succeed())
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "r1", keys("a", "b"))).To(Succeed())
			Expect(tx.DeletePostings("doc", "search", "never", keys("a"))).To(Succeed(),
				"a row that holds nothing is not an error")
			Expect(tx.DeletePostings("doc", "search", "never", nil)).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"), ex("b"))).To(Equal([]int{1, 0}))
			Expect(scan(tx, "doc", "search", nil, r(ex("a"), ex("b")), 0)).To(Equal([]string{"r2"}))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("b"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(Equal([]string{"r2"}))
			Expect(scan(tx, "doc", "search", nil, r(ex("b")), 0)).To(Equal([]string{"r1"}))
		})
	})

	It("never yields a deleted row, even for a posting held left out", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a", "b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("b"))).To(Succeed())
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "r1", keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(ex("a"), ex("b")), 0)).To(Equal([]string{"r2"}))
			Expect(scan(tx, "doc", "search", r(ex("b")), nil, 0)).To(Equal([]string{"r2"}))
		})
	})

	It("clears one index and nothing else", func() {
		for _, ws := range []string{"ws1", "ws2"} {
			write(ws, func(tx storage.WriteTx) {
				Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
				Expect(tx.PutPostings("doc", "other", "r1", nil, keys("a"))).To(Succeed())
				Expect(tx.PutPostings("note", "search", "r1", nil, keys("a"))).To(Succeed())
				Expect(tx.SetIndexState("doc", "search", "spec", true)).To(Succeed())
			})
		}
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.ClearPostings("doc", "search")).To(Succeed())
			Expect(tx.ClearPostings("doc", "unknown")).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"))).To(Equal([]int{0}))
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(BeEmpty())
			Expect(scan(tx, "doc", "other", nil, r(ex("a")), 0)).To(Equal([]string{"r1"}))
			Expect(scan(tx, "note", "search", nil, r(ex("a")), 0)).To(Equal([]string{"r1"}))
			hash, ready, err := tx.IndexState("doc", "search")
			Expect(err).NotTo(HaveOccurred())
			Expect(hash).To(Equal("spec"), "a clear keeps the build state")
			Expect(ready).To(BeTrue())
		})
		read("ws2", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(Equal([]string{"r1"}))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("b"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"), ex("b"))).To(Equal([]int{1, 1}))
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(Equal([]string{"r2"}))
			Expect(scan(tx, "doc", "search", nil, r(ex("b")), 0)).To(Equal([]string{"r1"}))
		})
	})

	It("keeps workspaces, entities and index names apart", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "d1", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("doc", "title", "t1", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("note", "search", "n1", nil, keys("a"))).To(Succeed())
		})
		write("ws2", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "w2", nil, keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", r(ex("a")), nil, 0)).To(Equal([]string{"d1"}))
			Expect(scan(tx, "doc", "title", r(ex("a")), nil, 0)).To(Equal([]string{"t1"}))
			Expect(scan(tx, "note", "search", nil, r(ex("a")), 0)).To(Equal([]string{"n1"}))
			Expect(counts(tx, "doc", "search", 0, ex("a"))).To(Equal([]int{1}))
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1))
			Expect(rowsIn(tx, "doc", "title")).To(Equal(1))
			Expect(rowsIn(tx, "note", "search")).To(Equal(1))
		})
		read("ws2", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(storage.IndexRange{}), 0)).To(Equal([]string{"w2"}))
			Expect(scan(tx, "note", "search", nil, r(storage.IndexRange{}), 0)).To(BeEmpty())
		})
		write("ws2", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "d1", keys("a"))).To(Succeed())
			Expect(tx.ClearPostings("doc", "title")).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", r(ex("a")), nil, 0)).To(Equal([]string{"d1"}))
			Expect(scan(tx, "doc", "title", r(ex("a")), nil, 0)).To(Equal([]string{"t1"}))
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1))
			Expect(rowsIn(tx, "doc", "title")).To(Equal(1))
		})
		read("ws2", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1))
			Expect(rowsIn(tx, "doc", "title")).To(BeZero())
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "title", "t1", keys("a"))).To(Succeed())
			Expect(tx.ClearPostings("note", "search")).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1))
			Expect(rowsIn(tx, "doc", "title")).To(BeZero())
			Expect(rowsIn(tx, "note", "search")).To(BeZero())
		})
	})

	It("does not mix postings with the value entries of another index", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutIndexEntries("doc", "r1", []storage.IndexEntry{{Index: "by_x", Key: []byte("a")}})).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r2", nil, keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			var got []string
			Expect(tx.ScanIndex("doc", "by_x", nil, nil, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
				got = append(got, rowKey)
				return nil
			})).To(Succeed())
			Expect(got).To(Equal([]string{"r1"}))
			Expect(scan(tx, "doc", "search", nil, r(storage.IndexRange{}), 0)).To(Equal([]string{"r2"}))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeleteIndexEntries("doc", "r1")).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(storage.IndexRange{}), 0)).To(Equal([]string{"r2"}))
		})
	})

	It("compares keys bytewise, with 0x00 and 0xff bytes and a key of the maximum length", func() {
		long := bytes.Repeat([]byte{0xff}, storage.MaxIndexEntryBytes)
		longer := append(bytes.Repeat([]byte{0xff}, storage.MaxIndexEntryBytes-1), 0xfe)
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "zero", nil, keys("\x00", "a\x00b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "ab", nil, keys("ab"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "ff", nil, keys("\xff", "a\xff"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "long", nil, [][]byte{long, longer})).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0,
				ex("\x00"), ex("a\x00b"), pre("a"), pre("a\x00"), ex("\xff"),
				exactRange(long), exactRange(longer), pre("\xff"),
				storage.IndexRange{Lo: []byte("a\x00"), Hi: []byte("a\xff")},
			)).To(Equal([]int{1, 1, 3, 1, 1, 1, 1, 3, 2}))
			Expect(scan(tx, "doc", "search", nil, r(pre("a")), 0)).To(Equal([]string{"ab", "ff", "zero"}))
			Expect(scan(tx, "doc", "search", r(exactRange(long)), nil, 0)).To(Equal([]string{"long"}))
			Expect(scan(tx, "doc", "search", r(pre("\xff"), ex("a\xff")), nil, 0)).To(Equal([]string{"ff"}))
		})
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "long", [][]byte{long}, nil)).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, exactRange(long), exactRange(longer))).To(Equal([]int{0, 1}))
		})
	})

	It("stores a posting key at the size limit under the longest names", func() {
		// The same longest names the value-entry spec uses: an app-prefixed
		// workspace id (see prefixstore), and every name and key at its limit.
		ws := strings.Repeat("w", naming.MaxAppIDBytes+1+naming.MaxWorkspaceIDBytes)
		entity := strings.Repeat("e", naming.MaxEntityNameLength)
		idx := strings.Repeat("i", naming.MaxEntityNameLength)
		rowKey := strings.Repeat("k", naming.MaxRowKeyBytes)
		// Bytes that do not compress.
		k := make([]byte, storage.MaxIndexEntryBytes)
		_, _ = rand.New(rand.NewSource(1)).Read(k)
		write(ws, func(tx storage.WriteTx) {
			Expect(tx.PutPostings(entity, idx, rowKey, nil, [][]byte{k})).To(Succeed())
		})
		read(ws, func(tx storage.ReadTx) {
			Expect(counts(tx, entity, idx, 0, exactRange(k))).To(Equal([]int{1}))
			Expect(scan(tx, entity, idx, r(exactRange(k)), nil, 0)).To(Equal([]string{rowKey}))
		})
		write(ws, func(tx storage.WriteTx) {
			Expect(tx.DeletePostings(entity, idx, rowKey, [][]byte{k})).To(Succeed())
		})
		read(ws, func(tx storage.ReadTx) {
			Expect(scan(tx, entity, idx, nil, r(storage.IndexRange{}), 0)).To(BeEmpty())
		})
	})

	It("neither retains nor changes the slices it is given", func() {
		remove := keys("gone")
		add := keys("a", "b")
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "r1", remove, add)).To(Succeed())
		})
		Expect(remove).To(Equal(keys("gone")))
		Expect(add).To(Equal(keys("a", "b")))
		// Changing the slices after the call changes nothing stored.
		add[0][0] = 'z'
		add[1] = []byte("c")
		read("ws1", func(tx storage.ReadTx) {
			ranges := []storage.IndexRange{ex("a"), ex("b"), ex("c")}
			got, _, err := tx.PostingCounts("doc", "search", ranges, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]int{1, 1, 0}))
			Expect(ranges).To(Equal([]storage.IndexRange{ex("a"), ex("b"), ex("c")}))
			ranges[0] = ex("c")
			all := []storage.IndexRange{ex("a")}
			anyR := []storage.IndexRange{ex("b")}
			Expect(scan(tx, "doc", "search", all, anyR, 0)).To(Equal([]string{"r1"}))
			Expect(all).To(Equal([]storage.IndexRange{ex("a")}))
			Expect(anyR).To(Equal([]storage.IndexRange{ex("b")}))
		})
		held := keys("a", "b")
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.DeletePostings("doc", "search", "r1", held)).To(Succeed())
		})
		Expect(held).To(Equal(keys("a", "b")))
	})

	It("keeps postings when PurgeEntityRows removes the rows", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutRow("doc", "r1", []byte("v"), 1, storage.Actor{})).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "r1", nil, keys("a"))).To(Succeed())
		})
		write("ws1", func(tx storage.WriteTx) {
			removed, _, err := tx.PurgeEntityRows("doc", "", time.Time{}, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(Equal(1))
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("a"))).To(Equal([]int{1}))
		})
		// The contract allows the purged row to be yielded until the index is
		// cleared; a clear then removes it.
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.ClearPostings("doc", "search")).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(BeEmpty())
		})
	})

	It("intersects all, unions any, and yields each row once", func() {
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "ab", nil, keys("a", "b"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "abc", nil, keys("a", "b", "c"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "a", nil, keys("a"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "c", nil, keys("c"))).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "o", nil, keys("\x02"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", r(ex("a"), ex("b")), nil, 0)).To(Equal([]string{"ab", "abc"}))
			Expect(scan(tx, "doc", "search", r(ex("a"), ex("b"), ex("c")), nil, 0)).To(Equal([]string{"abc"}))
			Expect(scan(tx, "doc", "search", r(ex("a"), ex("missing")), nil, 0)).To(BeEmpty())
			Expect(scan(tx, "doc", "search", r(ex("a"), ex("b")), r(ex("\x02")), 0)).
				To(Equal([]string{"ab", "abc", "o"}))
			Expect(scan(tx, "doc", "search", r(ex("a"), ex("b")), r(ex("c"), ex("a")), 0)).
				To(Equal([]string{"a", "ab", "abc", "c"}), "a row in all and in any comes once")
			Expect(scan(tx, "doc", "search", nil, r(ex("a"), ex("c")), 0)).To(Equal([]string{"a", "ab", "abc", "c"}))
			Expect(scan(tx, "doc", "search", nil, nil, 0)).To(BeEmpty())
			Expect(scan(tx, "doc", "search", r(ex("a"), ex("a")), nil, 0)).To(Equal([]string{"a", "ab", "abc"}),
				"a repeated range does not change the result")
			Expect(scan(tx, "doc", "search", r(pre("")), nil, 0)).To(Equal([]string{"a", "ab", "abc", "c", "o"}))
			Expect(scan(tx, "doc", "search", r(storage.IndexRange{Lo: []byte("b"), Hi: []byte("a")}), r(ex("c")), 0)).
				To(Equal([]string{"abc", "c"}), "an empty range in all matches no row")
		})
	})

	It("yields at most limit rows, the same rows each time", func() {
		write("ws1", func(tx storage.WriteTx) {
			for i := 0; i < 30; i++ {
				ks := keys("a")
				if i%2 == 0 {
					ks = append(ks, []byte("b"))
				}
				Expect(tx.PutPostings("doc", "search", fmt.Sprintf("r%02d", i), nil, ks)).To(Succeed())
			}
			Expect(tx.PutPostings("doc", "search", "x", nil, keys("c"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			for _, q := range []struct{ all, any []storage.IndexRange }{
				{r(ex("a")), nil},
				{r(ex("a"), ex("b")), nil},
				{r(ex("a"), ex("b")), r(ex("c"))},
				{nil, r(ex("b"), ex("c"))},
			} {
				full := scan(tx, "doc", "search", q.all, q.any, 0)
				for _, limit := range []int{1, 7, len(full), len(full) + 5} {
					got := scan(tx, "doc", "search", q.all, q.any, limit)
					Expect(got).To(HaveLen(min(limit, len(full))))
					Expect(full).To(ContainElements(got))
					Expect(scan(tx, "doc", "search", q.all, q.any, limit)).To(Equal(got))
				}
				Expect(scan(tx, "doc", "search", q.all, q.any, -1)).To(Equal(full))
			}
		})
	})

	It("stops when fn returns an error, propagating it", func() {
		write("ws1", func(tx storage.WriteTx) {
			for i := 0; i < 5; i++ {
				Expect(tx.PutPostings("doc", "search", fmt.Sprintf("r%d", i), nil, keys("a"))).To(Succeed())
			}
		})
		read("ws1", func(tx storage.ReadTx) {
			stop := errors.New("stop")
			seen := 0
			err := tx.ScanPostings("doc", "search", r(ex("a")), nil, 0, func(string) error {
				seen++
				return stop
			})
			Expect(err).To(MatchError(stop))
			Expect(seen).To(Equal(1))
		})
	})

	It("handles more rows under one key than several chunks hold", func() {
		const n = 2000
		write("ws1", func(tx storage.WriteTx) {
			for i := 0; i < n; i++ {
				ks := keys("common")
				if i%100 == 7 {
					ks = append(ks, []byte("rare"))
				}
				Expect(tx.PutPostings("doc", "search", fmt.Sprintf("r%05d", i), nil, ks)).To(Succeed())
			}
		})
		write("ws1", func(tx storage.WriteTx) {
			for i := 0; i < n; i += 3 {
				Expect(tx.PutPostings("doc", "search", fmt.Sprintf("r%05d", i), keys("common"), nil)).To(Succeed())
			}
			Expect(tx.DeletePostings("doc", "search", "r00007", keys("common", "rare"))).To(Succeed())
		})
		want, rare := 0, []string{}
		for i := 0; i < n; i++ {
			if i%3 != 0 && i != 7 {
				want++
			}
			if i%100 == 7 && i != 7 {
				rare = append(rare, fmt.Sprintf("r%05d", i))
			}
		}
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("common"), ex("rare"))).To(Equal([]int{want, len(rare)}))
			Expect(scan(tx, "doc", "search", r(ex("common")), nil, 0)).To(HaveLen(want))
			var both []string
			for _, k := range rare {
				var i int
				_, _ = fmt.Sscanf(k, "r%05d", &i)
				if i%3 != 0 {
					both = append(both, k)
				}
			}
			Expect(scan(tx, "doc", "search", r(ex("common"), ex("rare")), nil, 0)).To(Equal(both))
			Expect(scan(tx, "doc", "search", r(ex("rare"), ex("common")), nil, 0)).To(Equal(both))
			Expect(scan(tx, "doc", "search", r(ex("common")), nil, 10)).To(HaveLen(10))
		})
		write("ws1", func(tx storage.WriteTx) {
			for i := 0; i < n; i++ {
				Expect(tx.PutPostings("doc", "search", fmt.Sprintf("r%05d", i), keys("common"), nil)).To(Succeed())
			}
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(counts(tx, "doc", "search", 0, ex("common"))).To(Equal([]int{0}))
			Expect(scan(tx, "doc", "search", nil, r(ex("common")), 0)).To(BeEmpty())
			Expect(scan(tx, "doc", "search", nil, r(ex("rare")), 0)).To(Equal(rare))
		})
	})

	It("rolls back the postings of a savepoint that fails", func() {
		boom := errors.New("boom")
		write("ws1", func(tx storage.WriteTx) {
			Expect(tx.PutPostings("doc", "search", "keep", nil, keys("a"))).To(Succeed())
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutPostings("doc", "search", "gone", nil, keys("a"))).To(Succeed())
				Expect(tx.PutPostings("doc", "search", "keep", keys("a"), keys("b"))).To(Succeed())
				Expect(rowsIn(tx, "doc", "search")).To(Equal(2))
				return boom
			})).To(MatchError(boom))
			Expect(rowsIn(tx, "doc", "search")).To(Equal(1))
			Expect(tx.InSavepoint(func() error {
				Expect(tx.DeletePostings("doc", "search", "keep", keys("a"))).To(Succeed())
				Expect(rowsIn(tx, "doc", "search")).To(BeZero())
				return boom
			})).To(MatchError(boom))
			Expect(tx.PutPostings("doc", "search", "after", nil, keys("a"))).To(Succeed())
		})
		read("ws1", func(tx storage.ReadTx) {
			Expect(rowsIn(tx, "doc", "search")).To(Equal(2))
			Expect(counts(tx, "doc", "search", 0, ex("a"), ex("b"))).To(Equal([]int{2, 0}))
			Expect(scan(tx, "doc", "search", nil, r(ex("a"), ex("b")), 0)).To(Equal([]string{"after", "keep"}))
		})
		err := getStore().WithWriteTx(getCtx(), "ws1", func(tx storage.WriteTx) error {
			Expect(tx.ClearPostings("doc", "search")).To(Succeed())
			Expect(tx.PutPostings("doc", "search", "other", nil, keys("a"))).To(Succeed())
			return boom
		})
		Expect(err).To(MatchError(boom))
		read("ws1", func(tx storage.ReadTx) {
			Expect(scan(tx, "doc", "search", nil, r(ex("a")), 0)).To(Equal([]string{"after", "keep"}))
			Expect(rowsIn(tx, "doc", "search")).To(Equal(2))
		})
	})

	It("matches an in-memory model under random operations", func() {
		seed := GinkgoRandomSeed()
		rng := rand.New(rand.NewSource(seed))
		alphabet := []byte{0x00, 0x01, 0x02, 'a', 'b', 0xfe, 0xff}
		pool := make([][]byte, 0, 30)
		seen := map[string]bool{}
		for len(pool) < 30 {
			k := make([]byte, 1+rng.Intn(3))
			for i := range k {
				k[i] = alphabet[rng.Intn(len(alphabet))]
			}
			if !seen[string(k)] {
				seen[string(k)] = true
				pool = append(pool, k)
			}
		}
		type ix struct{ entity, index string }
		indexes := []ix{{"doc", "search"}, {"doc", "title"}, {"note", "search"}}
		// model[index][row] is the set of keys the row holds.
		model := map[ix]map[string]map[string]bool{}
		// known[index] is the set of rows that got a posting and were not
		// deleted or cleared since, which the row count counts.
		known := map[ix]map[string]bool{}
		for _, i := range indexes {
			model[i] = map[string]map[string]bool{}
			known[i] = map[string]bool{}
		}
		rows := make([]string, 60)
		for i := range rows {
			rows[i] = fmt.Sprintf("row-%02d", i)
		}
		pick := func(n int) [][]byte {
			out := make([][]byte, n)
			for i := range out {
				// Skew to the first keys so that some keys hold many rows.
				out[i] = pool[min(rng.Intn(len(pool)), rng.Intn(len(pool)))]
			}
			return out
		}
		for batch := 0; batch < 25; batch++ {
			write("ws1", func(tx storage.WriteTx) {
				for op := 0; op < 20; op++ {
					i := indexes[rng.Intn(len(indexes))]
					row := rows[rng.Intn(len(rows))]
					held := model[i][row]
					switch p := rng.Intn(100); {
					case p < 2:
						Expect(tx.ClearPostings(i.entity, i.index)).To(Succeed())
						model[i] = map[string]map[string]bool{}
						known[i] = map[string]bool{}
					case p < 15:
						var hk [][]byte
						for k := range held {
							hk = append(hk, []byte(k))
						}
						Expect(tx.DeletePostings(i.entity, i.index, row, hk)).To(Succeed())
						delete(model[i], row)
						delete(known[i], row)
					default:
						remove := pick(rng.Intn(4))
						// In key order, so that a seed gives the same
						// operations in each run.
						heldKeys := make([]string, 0, len(held))
						for k := range held {
							heldKeys = append(heldKeys, k)
						}
						sort.Strings(heldKeys)
						for _, k := range heldKeys {
							if rng.Intn(3) == 0 {
								remove = append(remove, []byte(k))
							}
						}
						add := pick(rng.Intn(5))
						Expect(tx.PutPostings(i.entity, i.index, row, remove, add)).To(Succeed())
						if len(add) > 0 {
							known[i][row] = true
						}
						if held == nil {
							held = map[string]bool{}
						}
						for _, k := range remove {
							delete(held, string(k))
						}
						for _, k := range add {
							held[string(k)] = true
						}
						if len(held) == 0 {
							delete(model[i], row)
						} else {
							model[i][row] = held
						}
					}
				}
			})
		}

		modelCount := func(i ix, rg storage.IndexRange) int {
			n := 0
			for _, held := range model[i] {
				for k := range held {
					if inRange([]byte(k), rg) {
						n++
					}
				}
			}
			return n
		}
		modelScan := func(i ix, all, any []storage.IndexRange) []string {
			out := []string{}
			for row, held := range model[i] {
				hit := func(rg storage.IndexRange) bool {
					for k := range held {
						if inRange([]byte(k), rg) {
							return true
						}
					}
					return false
				}
				ok := len(all) > 0
				for _, rg := range all {
					ok = ok && hit(rg)
				}
				for _, rg := range any {
					ok = ok || hit(rg)
				}
				if ok {
					out = append(out, row)
				}
			}
			sort.Strings(out)
			return out
		}
		randRange := func() storage.IndexRange {
			switch rng.Intn(4) {
			case 0, 1:
				return exactRange(pool[rng.Intn(len(pool))])
			case 2:
				return prefixRange(pool[rng.Intn(len(pool))][:1])
			default:
				a, b := pool[rng.Intn(len(pool))], pool[rng.Intn(len(pool))]
				if bytes.Compare(a, b) > 0 {
					a, b = b, a
				}
				return storage.IndexRange{Lo: a, Hi: b}
			}
		}
		describe := func(rs []storage.IndexRange) string {
			parts := make([]string, len(rs))
			for i, rg := range rs {
				parts[i] = fmt.Sprintf("[%x,%x)", rg.Lo, rg.Hi)
			}
			return strings.Join(parts, " ")
		}
		read("ws1", func(tx storage.ReadTx) {
			for _, i := range indexes {
				ranges := []storage.IndexRange{{}}
				for _, k := range pool {
					ranges = append(ranges, exactRange(k), prefixRange(k[:1]))
				}
				want := make([]int, len(ranges))
				for j, rg := range ranges {
					want[j] = modelCount(i, rg)
				}
				Expect(counts(tx, i.entity, i.index, 0, ranges...)).To(Equal(want), "seed %d, %v", seed, i)
				Expect(rowsIn(tx, i.entity, i.index)).To(Equal(len(known[i])), "seed %d, %v", seed, i)
				capped := counts(tx, i.entity, i.index, 3, ranges...)
				for j := range ranges {
					if want[j] <= 3 {
						Expect(capped[j]).To(Equal(want[j]), "seed %d, %v, range %s", seed, i, describe(ranges[j:j+1]))
					} else {
						Expect(capped[j]).To(BeNumerically(">", 3), "seed %d, %v, range %s", seed, i, describe(ranges[j:j+1]))
					}
				}
				for q := 0; q < 60; q++ {
					all := make([]storage.IndexRange, rng.Intn(4))
					for j := range all {
						all[j] = randRange()
					}
					anyR := make([]storage.IndexRange, rng.Intn(3))
					for j := range anyR {
						anyR[j] = randRange()
					}
					msg := fmt.Sprintf("seed %d, %v, all %s, any %s", seed, i, describe(all), describe(anyR))
					full := modelScan(i, all, anyR)
					Expect(scan(tx, i.entity, i.index, all, anyR, 0)).To(Equal(full), msg)
					limit := 1 + rng.Intn(10)
					got := scan(tx, i.entity, i.index, all, anyR, limit)
					Expect(got).To(HaveLen(min(limit, len(full))), msg)
					Expect(full).To(ContainElements(got), msg)
					Expect(scan(tx, i.entity, i.index, all, anyR, limit)).To(Equal(got), msg)
				}
			}
		})
	})
}

// exactRange is the range that holds key and no other key.
func exactRange(key []byte) storage.IndexRange {
	return storage.IndexRange{Lo: key, Hi: append(append([]byte{}, key...), 0)}
}

// prefixRange is the range that holds every key that starts with p.
func prefixRange(p []byte) storage.IndexRange {
	hi := append([]byte{}, p...)
	for len(hi) > 0 && hi[len(hi)-1] == 0xff {
		hi = hi[:len(hi)-1]
	}
	if len(hi) == 0 {
		return storage.IndexRange{Lo: p}
	}
	hi[len(hi)-1]++
	return storage.IndexRange{Lo: p, Hi: hi}
}

// inRange reports whether k falls in rg, with the nil bounds of IndexRange.
func inRange(k []byte, rg storage.IndexRange) bool {
	if rg.Lo != nil && bytes.Compare(k, rg.Lo) < 0 {
		return false
	}
	return rg.Hi == nil || bytes.Compare(k, rg.Hi) < 0
}
