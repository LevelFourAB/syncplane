package server_test

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// postingRowsAdapter can report more rows in a posting index than it holds,
// which makes a search on a term in every row take the index path, and counts
// the posting scans of its read transactions.
type postingRowsAdapter struct {
	storage.Adapter
	inflate atomic.Bool
	scans   atomic.Int64
}

func (a *postingRowsAdapter) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	return a.Adapter.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		return fn(&postingRowsTx{ReadTx: tx, owner: a})
	})
}

type postingRowsTx struct {
	storage.ReadTx
	owner *postingRowsAdapter
}

func (t *postingRowsTx) PostingCounts(entity, index string, ranges []storage.IndexRange, max int) ([]int, int, error) {
	counts, rows, err := t.ReadTx.PostingCounts(entity, index, ranges, max)
	if t.owner.inflate.Load() {
		rows *= 100
	}
	return counts, rows, err
}

func (t *postingRowsTx) ScanPostings(entity, index string, all, any []storage.IndexRange, limit int, fn func(string) error) error {
	t.owner.scans.Add(1)
	return t.ReadTx.ScanPostings(entity, index, all, any, limit, fn)
}

var _ = Describe("Search on a term most rows hold", func() {
	const ws = "ws-search-decline"

	It("returns the same rows through the index and through the scan it declines to", func() {
		ctx := context.Background()
		store := &postingRowsAdapter{Adapter: newTestStore()}
		srv := mustNewServer(store, server.WithEntities(&testdatav1.Article{}), server.WithIndexBuild(0, 0))
		DeferCleanup(func() { _ = srv.Close() })

		var want []string
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			for i := range 40 {
				id := fmt.Sprintf("a%02d", i)
				want = append(want, id)
				a := &testdatav1.Article{Id: id, Title: "everywhere", Summary: fmt.Sprintf("word%d", i%3)}
				if err := tx.Put(a); err != nil {
					return err
				}
			}
			// A row without the term, so a scan has a row to reject.
			return tx.Put(&testdatav1.Article{Id: "other", Title: "elsewhere"})
		})).To(Succeed())
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())

		search := func() []string {
			GinkgoHelper()
			var got []string
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(&testdatav1.Article{}, query.Spec{Where: query.Match("everywhere ")},
					func(m proto.Message, _ uint64) error {
						got = append(got, m.(*testdatav1.Article).GetId())
						return nil
					})
			})).To(Succeed())
			sort.Strings(got)
			return got
		}

		store.inflate.Store(true)
		store.scans.Store(0)
		Expect(search()).To(Equal(want))
		Expect(store.scans.Load()).To(BeNumerically(">", 0), "the index path reads the postings")

		store.inflate.Store(false)
		store.scans.Store(0)
		Expect(search()).To(Equal(want))
		Expect(store.scans.Load()).To(BeZero(), "the declined search reads no postings")
	})
})
