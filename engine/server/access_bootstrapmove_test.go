package server_test

import (
	"context"
	gosync "sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// hookedReadStore runs a hook before a read transaction opens, chosen by
// what the transactions before it read: afterBegin before the first one
// after a transaction that read the store epoch (a Bootstrap's Begin), and
// afterEntity before the first one after a transaction that read rows of
// entity. Each hook runs once, outside any transaction of the store.
type hookedReadStore struct {
	storage.Adapter
	mu          gosync.Mutex
	entity      string
	afterBegin  func()
	afterEntity func()
	sawBegin    bool
	sawEntity   bool
	firing      bool
}

func (h *hookedReadStore) Unwrap() storage.Adapter { return h.Adapter }

func (h *hookedReadStore) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	h.mu.Lock()
	var hook func()
	if !h.firing {
		switch {
		case h.sawBegin && h.afterBegin != nil:
			hook, h.afterBegin = h.afterBegin, nil
		case h.sawEntity && h.afterEntity != nil:
			hook, h.afterEntity = h.afterEntity, nil
		}
		h.firing = hook != nil
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
		h.mu.Lock()
		h.firing = false
		h.mu.Unlock()
	}
	return h.Adapter.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		return fn(&hookedReadTx{ReadTx: tx, h: h})
	})
}

type hookedReadTx struct {
	storage.ReadTx
	h *hookedReadStore
}

func (t *hookedReadTx) StoreEpoch() (uint64, error) {
	t.h.mu.Lock()
	if !t.h.firing {
		t.h.sawBegin = true
	}
	t.h.mu.Unlock()
	return t.ReadTx.StoreEpoch()
}

func (t *hookedReadTx) saw(entity string) {
	t.h.mu.Lock()
	if !t.h.firing && entity == t.h.entity {
		t.h.sawEntity = true
	}
	t.h.mu.Unlock()
}

func (t *hookedReadTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	t.saw(entity)
	return t.ReadTx.GetRows(entity, keys, fn)
}

func (t *hookedReadTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	t.saw(entity)
	return t.ReadTx.ScanEntityAfter(entity, afterKey, limit, fn)
}

var _ = Describe("Access model bootstrap across a move", func() {
	It("removes a comment the snapshot shipped while its task sat for a moment in a project the caller reads", func() {
		// Given comments below tasks below projects, where guest reads p1
		// and t2, with comment c2 below it, sits in p2
		hooked := &hookedReadStore{Adapter: newTestStore(), entity: "comment"}
		f := newDeepFixtureOn(hooked)
		f.seedOrg()
		f.seedComments()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		moveTask := func(projectID string) {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&accessv1.Task{Id: "t2", Project: project(projectID)}, []string{"project"}, nil)
			})).To(Succeed())
		}
		// And t2 moves to p1 after the snapshot begins, and back to p2
		// after the snapshot reads the comments
		hooked.mu.Lock()
		hooked.sawBegin, hooked.sawEntity = false, false
		hooked.afterBegin = func() { moveTask("p1") }
		hooked.afterEntity = func() { moveTask("p2") }
		hooked.mu.Unlock()

		// When guest bootstraps, and pulls from the Begin cursor
		req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-guest"})
		as("guest", req.Header())
		stream, err := f.sync.Bootstrap(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		guest := newAccessReplica(f, "guest")
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				guest.cursor = body.Begin.GetCursor()
				guest.access = body.Begin.GetCallerAccess()
			case *syncplanev1.BootstrapResponse_Chunk_:
				for _, rc := range body.Chunk.GetRows() {
					guest.rows[rc.GetEntity()+"/"+rc.GetKey()] = true
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		hooked.mu.Lock()
		Expect(hooked.afterBegin).To(BeNil(), "t2 did not move into p1 during the snapshot")
		Expect(hooked.afterEntity).To(BeNil(), "t2 did not move back during the snapshot")
		hooked.mu.Unlock()
		guest.sync()

		// Then guest holds what a fresh pull shows it: t2 and c2 are in
		// p2, which guest does not read
		Expect(guest.view()).NotTo(ContainElement("comment/c2"))
		Expect(guest.view()).To(Equal(f.pull("guest")))
	})
})
