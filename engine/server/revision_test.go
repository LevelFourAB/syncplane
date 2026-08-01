package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Schema revision advertisement", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// clientFor mounts a server carrying the given revision (empty = none) on
	// an httptest server and returns a connected client.
	clientFor := func(rev string) syncplanev1connect.SyncServiceClient {
		var opts []server.Option
		if rev != "" {
			opts = append(opts, server.WithSchemaRevision(rev))
		}
		srv := newTestServer(opts...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	It("echoes the configured revision on pull", func() {
		client := clientFor("rev-7")
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetSchemaRevision()).To(Equal("rev-7"))
	})

	It("echoes the configured revision on bootstrap Begin", func() {
		client := clientFor("rev-7")
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		var begin *syncplanev1.BootstrapResponse_Begin
		for stream.Receive() {
			if b, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Begin_); ok {
				begin = b.Begin
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(begin).NotTo(BeNil())
		Expect(begin.GetSchemaRevision()).To(Equal("rev-7"))
	})

	It("echoes the configured revision on every stream frame", func() {
		// Carried on the envelope, not just inside a page, so a connected
		// client learns of a rolling deploy from a heartbeat alone.
		client := clientFor("rev-7")
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.Subscribe(streamCtx, connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(stream.Receive()).To(BeTrue())
		Expect(stream.Msg().GetSchemaRevision()).To(Equal("rev-7"))
	})

	It("advertises the revision on a push, so write-path skew is visible", func() {
		// The sync cycle pushes before it pulls: without this, the first cycle
		// after a deploy always writes against a skewed server before a
		// "block" policy could engage.
		client := clientFor("rev-7")
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetSchemaRevision()).To(Equal("rev-7"))
	})

	It("advertises the revision on a query", func() {
		client := clientFor("rev-7")
		resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "todo",
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetSchemaRevision()).To(Equal("rev-7"))
	})

	It("advertises an empty revision when none is configured", func() {
		client := clientFor("")
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetSchemaRevision()).To(BeEmpty())
	})
})
