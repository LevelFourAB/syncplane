package client_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/client"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Freshness floor", func() {
	It("asks for no freshness above the head of a restored workspace", func() {
		ctx := GinkgoT().Context()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "floor.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)
		floors := &floorRecorder{Adapter: store}
		srv, err := server.New(floors, server.WithEntities(&todov1.Todo{}))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(srv.Close)
		ts := httptest.NewServer(srv.HTTPHandler())
		DeferCleanup(ts.Close)
		c, err := client.New(ts.URL)
		Expect(err).NotTo(HaveOccurred())
		ws := c.Workspace("acme")
		for i := range 5 {
			_, err := ws.Put(ctx, &todov1.Todo{Id: fmt.Sprintf("t%d", i), Title: "old"})
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(srv.DeleteWorkspace(ctx, "acme")).To(Succeed())
		Expect(srv.RestoreWorkspace(ctx, "acme")).To(Succeed())
		Expect(srv.Apply(ctx, "acme", func(tx *server.ApplyTx) error {
			return tx.Put(&todov1.Todo{Id: "r1", Title: "restored"})
		})).To(Succeed())
		head, err := srv.CurrentSeq(ctx, "acme")
		Expect(err).NotTo(HaveOccurred())

		snap, err := ws.Bootstrap(ctx)
		Expect(err).NotTo(HaveOccurred())
		pull := func() {
			for _, err := range ws.Pull(ctx, snap.Cursor) {
				Expect(err).NotTo(HaveOccurred())
			}
		}
		// The first pull may still carry the floor of the history before the
		// restore; the server's answer tells the Client that history is gone.
		pull()
		mark := floors.count()
		pull()
		after := floors.since(mark)
		Expect(after).NotTo(BeEmpty())
		Expect(slices.Max(after)).To(BeNumerically("<=", head), "floors %v over head %d", after, head)
	})
})

// floorRecorder serves every read from the store itself and records the
// freshness floor each read through its ReplicaReader asked for.
type floorRecorder struct {
	storage.Adapter
	mu     sync.Mutex
	floors []uint64
}

func (r *floorRecorder) ReplicaReader() storage.ReplicaReader { return r }

func (r *floorRecorder) WithReadTxMinSeq(ctx context.Context, ws string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	r.mu.Lock()
	r.floors = append(r.floors, minSeq)
	r.mu.Unlock()
	return storage.ReadRouteReplica, r.Adapter.WithReadTx(ctx, ws, fn)
}

func (r *floorRecorder) WithReadTxAnchored(ctx context.Context, ws string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return storage.ReadRouteReplica, r.Adapter.WithReadTx(ctx, ws, fn)
}

func (r *floorRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.floors)
}

func (r *floorRecorder) since(mark int) []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.floors[mark:])
}
