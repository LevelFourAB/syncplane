package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// staleDeletionCheck answers the gate's deletion check with "not deleted"
// whatever the store holds, which is what a request sees when DeleteWorkspace
// lands between its gate and its transaction.
type staleDeletionCheck struct {
	storage.Adapter
}

func (staleDeletionCheck) WorkspaceDeleted(context.Context, string) (bool, error) {
	return false, nil
}

var _ = Describe("Deleting a workspace", func() {
	const ws = "w-graph"

	var (
		ctx    context.Context
		srv    *server.Server
		store  storage.Adapter
		client syncplanev1connect.SyncServiceClient
	)

	serve := func(s *server.Server) {
		path, handler := s.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(clientID string, mutationID uint64, key string) *syncplanev1.WorkspaceResults {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation(clientID, mutationID, "todo", key, &todov1.Todo{Id: key, Title: key}),
		})))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetResults()).To(HaveLen(1))
		return resp.Msg.GetResults()[0]
	}

	pull := func(cursor []byte) (*syncplanev1.PullResponse, error) {
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c1", Cursor: cursor,
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	// revoked asserts that err is the permanent workspace denial a sync client
	// stops on.
	revoked := func(err error) {
		GinkgoHelper()
		var ce *connect.Error
		Expect(errors.As(err, &ce)).To(BeTrue(), "want a Connect error, got %v", err)
		Expect(ce.Code()).To(Equal(connect.CodePermissionDenied))
		Expect(ce.Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceAccessRevoked))
	}

	streamCtx := func() context.Context {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		DeferCleanup(cancel)
		return c
	}

	BeforeEach(func() {
		ctx = context.Background()
		srv, store = newTestServerStore(server.WithHeartbeat(time.Hour))
		serve(srv)
		Expect(push("c1", 1, "t1").GetErrorCode()).To(BeEmpty())
	})

	It("refuses every workspace-scoped RPC as permanently revoked", func() {
		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())

		res := push("c1", 2, "t2")
		Expect(res.GetErrorCode()).To(Equal(server.CodeWorkspaceAccessRevoked))

		_, err := pull(nil)
		revoked(err)

		_, err = client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: ws, Entity: "todo"}))
		revoked(err)

		boot, err := client.Bootstrap(streamCtx(), connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws, ClientId: "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		for boot.Receive() {
		}
		revoked(boot.Err())

		sub, err := client.Subscribe(streamCtx(), connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
		}))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = sub.Close() })
		Expect(sub.Receive()).To(BeTrue(), "stream error: %v", sub.Err())
		Expect(sub.Msg().GetDropped().GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED))
	})

	It("removes the workspace's rows and drops it from WorkspaceIDs", func() {
		Expect(srv.WorkspaceIDs(ctx)).To(ConsistOf(ws))

		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())

		Expect(srv.WorkspaceIDs(ctx)).To(BeEmpty())
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			_, err := tx.GetRow("todo", "t1")
			Expect(err).To(MatchError(storage.ErrNotFound))
			Expect(tx.ClientLastMutationID("c1")).To(BeZero())
			return nil
		})).To(Succeed())
	})

	It("refuses the in-process API with ErrWorkspaceDeleted", func() {
		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())

		err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
			return tx.Put(&todov1.Todo{Id: "t2", Title: "back"})
		})
		Expect(err).To(MatchError(server.ErrWorkspaceDeleted))
		_, denied := server.AsWorkspaceDenial(err)
		Expect(denied).To(BeTrue(), "the gate refuses it as a workspace denial")

		err = srv.View(ctx, ws, func(*server.ViewTx) error { return nil })
		Expect(err).To(MatchError(server.ErrWorkspaceDeleted))

		// A trusted call skips the gate, and the store refuses its write.
		err = srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&todov1.Todo{Id: "t2", Title: "back"})
		})
		Expect(err).To(MatchError(server.ErrWorkspaceDeleted))
		Expect(srv.WorkspaceIDs(ctx)).To(BeEmpty(), "the refused write brought nothing back")
	})

	It("refuses a push whose gate ran before the deletion landed", func() {
		srv = mustNewServer(staleDeletionCheck{store}, testEntities())
		serve(srv)
		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())

		res := push("c1", 2, "t2")
		Expect(res.GetErrorCode()).To(Equal(server.CodeWorkspaceAccessRevoked))
		Expect(store.ListWorkspaceIDs(ctx, storage.Range{})).To(BeEmpty())
	})

	It("drops the scope of a stream that is open when the workspace is deleted", func() {
		sub, err := client.Subscribe(streamCtx(), connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
		}))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = sub.Close() })
		for sub.Receive() && !sub.Msg().GetCaughtUp() {
		}
		Expect(sub.Err()).NotTo(HaveOccurred())

		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())

		// The heartbeat is an hour away, so the poke is what reaches the stream.
		// The reset sends the client to bootstrap, which the gate refuses.
		var dropped *syncplanev1.WorkspaceDropped
		for dropped == nil && sub.Receive() {
			dropped = sub.Msg().GetDropped()
		}
		Expect(dropped).NotTo(BeNil(), "stream error: %v", sub.Err())
		Expect(dropped.GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_RESET))
	})

	It("starts a new, empty workspace under the id after RestoreWorkspace", func() {
		before, err := pull(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())

		Expect(srv.RestoreWorkspace(ctx, ws)).To(Succeed())

		// The watermark went with the workspace, so the client's ids start over.
		Expect(push("c2", 1, "t2").GetErrorCode()).To(BeEmpty())
		Expect(srv.WorkspaceIDs(ctx)).To(ConsistOf(ws))
		after, err := pull(before.GetNextCursor())
		Expect(err).NotTo(HaveOccurred())
		Expect(after.GetReset_()).To(BeTrue(), "a cursor of the deleted workspace must not resume in the new one")
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			_, err := tx.GetRow("todo", "t1")
			Expect(err).To(MatchError(storage.ErrNotFound), "restoring brings back no data")
			return nil
		})).To(Succeed())
	})

	It("is idempotent and refuses an id that never held state", func() {
		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		Expect(srv.DeleteWorkspace(ctx, "w-never")).To(Succeed())

		resp, err := client.Push(ctx, connect.NewRequest(pushOf("w-never", []*syncplanev1.Mutation{
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeWorkspaceAccessRevoked))
	})

	It("requires a workspace id", func() {
		Expect(srv.DeleteWorkspace(ctx, "")).To(MatchError(ContainSubstring("workspace id is required")))
		Expect(srv.RestoreWorkspace(ctx, "")).To(MatchError(ContainSubstring("workspace id is required")))
	})
})
