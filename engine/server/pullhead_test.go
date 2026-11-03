package server_test

import (
	"context"
	"fmt"
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
)

// PullResponse.head_seq: the workspace's newest sequence, which a client
// subtracts its own position from to know how far behind it is.
var _ = Describe("Pull head sequence", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)
	connectTo := func(srv *server.Server) {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}
	push := func(muts ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		pushResults(resp.Msg)
	}
	pull := func(cursor []byte, limit uint32) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c1", Cursor: cursor, Limit: limit,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	BeforeEach(func() { ctx = context.Background() })

	It("reports the head on every page, so the distance shrinks to 0 as pages drain", func() {
		connectTo(newTestServer())
		for i := 1; i <= 5; i++ {
			// One push per write, so each takes a sequence of its own.
			push(putMutation("c1", uint64(i), "todo", fmt.Sprintf("t%d", i), &todov1.Todo{Id: fmt.Sprintf("t%d", i)}))
		}

		first := pull(nil, 2)
		Expect(first.GetHasMore()).To(BeTrue())
		head := first.GetHeadSeq()
		Expect(head).To(BeNumerically(">", first.GetSeq()), "a truncated page is behind the head")

		var last *syncplanev1.PullResponse
		for cursor := first.GetNextCursor(); ; {
			last = pull(cursor, 2)
			Expect(last.GetHeadSeq()).To(Equal(head))
			if !last.GetHasMore() {
				break
			}
			cursor = last.GetNextCursor()
		}
		Expect(last.GetSeq()).To(Equal(head), "the last page is up to date")

		idle := pull(last.GetNextCursor(), 2)
		Expect(idle.GetPatch().GetChanges()).To(BeEmpty())
		Expect(idle.GetHeadSeq()).To(Equal(idle.GetSeq()))
	})

	It("sends no head on a reset", func() {
		srv := newTestServer(server.WithGC(-time.Second, 0))
		connectTo(srv)
		push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
		before := pull(nil, 0)
		push(deleteMutation("c1", 2, "todo", "t1"))
		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

		resp := pull(before.GetNextCursor(), 0)
		Expect(resp.GetReset_()).To(BeTrue())
		Expect(resp.GetHeadSeq()).To(BeZero())
	})
})
