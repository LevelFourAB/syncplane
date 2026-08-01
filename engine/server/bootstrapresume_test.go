package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// A snapshot delivered across two attempts: the first is cut short, the second
// hands back what it was given and is served the rest. What every spec here is
// really asserting is that the two attempts together equal one — the same rows,
// exactly once, under a cursor that still heals whatever raced them.
var _ = Describe("Bootstrap resume", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	startServer := func(srv interface {
		Handler() (string, http.Handler)
	}) syncplanev1connect.SyncServiceClient {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(workspace string, mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(workspace, mutations)))
		Expect(err).NotTo(HaveOccurred())
		pushResults(resp.Msg)
	}

	// attempt runs one Bootstrap and returns its Begin, the rows it delivered
	// and the resume token of the last chunk it took. `chunks` bounds how many
	// chunks it accepts before hanging up, standing in for a connection that
	// drops mid-snapshot; 0 means "take the whole stream".
	type attempt struct {
		begin *syncplanev1.BootstrapResponse_Begin
		rows  []*syncplanev1.RowChange
		token []byte
		ended bool
	}
	run := func(req *syncplanev1.BootstrapRequest, chunks int) attempt {
		GinkgoHelper()
		stream, err := client.Bootstrap(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(stream.Close()).To(Succeed()) }()
		var out attempt
		seen := 0
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				out.begin = body.Begin
			case *syncplanev1.BootstrapResponse_Chunk_:
				out.rows = append(out.rows, body.Chunk.GetRows()...)
				out.token = body.Chunk.GetResumeToken()
				seen++
				if chunks > 0 && seen == chunks {
					return out
				}
			case *syncplanev1.BootstrapResponse_End_:
				out.ended = true
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		return out
	}

	keysOf := func(rows []*syncplanev1.RowChange) []string {
		out := make([]string, 0, len(rows))
		for _, rc := range rows {
			out = append(out, rc.GetEntity()+"/"+rc.GetKey())
		}
		return out
	}

	BeforeEach(func() {
		ctx = context.Background()
		// Two rows per chunk, so a five-row workspace is interrupted several
		// chunks before the end and both entities are involved.
		client = startServer(newTestServer(server.WithChunkSize(2)))
		var muts []*syncplanev1.Mutation
		for i := 1; i <= 5; i++ {
			id := fmt.Sprintf("t%d", i)
			muts = append(muts, putMutation("c1", uint64(i), "todo", id, &todov1.Todo{Id: id}))
		}
		muts = append(muts, putMutation("c1", 6, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		push(ws, muts...)
	})

	It("serves only the rows an interrupted attempt did not deliver", func() {
		first := run(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 1)
		Expect(first.begin.GetResumed()).To(BeFalse())
		Expect(first.rows).To(HaveLen(2))
		Expect(first.token).NotTo(BeEmpty())

		second := run(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  first.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeTrue())
		Expect(second.ended).To(BeTrue())
		Expect(second.begin.GetCursor()).To(Equal(first.begin.GetCursor()),
			"a resumed snapshot keeps the cursor its first attempt minted")
		Expect(second.begin.GetSeq()).To(Equal(first.begin.GetSeq()))

		// Together the attempts deliver the whole workspace, each row once.
		Expect(keysOf(second.rows)).NotTo(ContainElements(keysOf(first.rows)))
		Expect(append(keysOf(first.rows), keysOf(second.rows)...)).To(ConsistOf(
			"todo/t1", "todo/t2", "todo/t3", "todo/t4", "todo/t5", "widget/w1"))
	})

	It("carries what earlier attempts delivered through a second interruption", func() {
		var (
			req  = &syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}
			all  []string
			last attempt
		)
		// Three attempts, each taking a single chunk, then a fourth for the
		// rest: the position has to accumulate across all of them.
		for i := 0; i < 3; i++ {
			last = run(req, 1)
			all = append(all, keysOf(last.rows)...)
			req = &syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
				ClientId:    "c1",
				Resume: &syncplanev1.BootstrapResume{
					Cursor: last.begin.GetCursor(),
					Token:  last.token,
				},
			}
			Expect(last.token).NotTo(BeEmpty())
		}
		final := run(req, 0)
		Expect(final.begin.GetResumed()).To(BeTrue())
		Expect(final.ended).To(BeTrue())
		all = append(all, keysOf(final.rows)...)
		Expect(all).To(ConsistOf(
			"todo/t1", "todo/t2", "todo/t3", "todo/t4", "todo/t5", "widget/w1"))
	})

	It("heals rows that changed between the attempts, via the re-issued cursor", func() {
		first := run(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 1)
		delivered := keysOf(first.rows)

		// A write landing between the attempts: it touches a row the first
		// attempt already shipped, so only the pull can carry it.
		push(ws, patchMutation("c1", 7, "todo", "t1", &todov1.Todo{Title: "edited"}, "title"))

		second := run(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  first.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeTrue())
		Expect(delivered).To(ContainElement("todo/t1"), "the edited row shipped in the first attempt")

		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Cursor:      second.begin.GetCursor(),
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetReset_()).To(BeFalse())
		changes := resp.Msg.GetPatch().GetChanges()
		Expect(keysOf(changes)).To(ConsistOf("todo/t1"))
		var todo todov1.Todo
		Expect(proto.Unmarshal(changes[0].GetValue(), &todo)).To(Succeed())
		Expect(todo.GetTitle()).To(Equal("edited"))
	})

	It("starts a fresh snapshot when the subscription set changed", func() {
		first := run(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 1)

		second := run(&syncplanev1.BootstrapRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Subscriptions: []*syncplanev1.Subscription{{Entity: "todo"}},
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  first.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeFalse(),
			"rows already delivered were chosen under a different scope")
		Expect(second.begin.GetCursor()).NotTo(Equal(first.begin.GetCursor()))
		Expect(keysOf(second.rows)).To(ConsistOf(
			"todo/t1", "todo/t2", "todo/t3", "todo/t4", "todo/t5"))
	})

	It("starts a fresh snapshot for a cursor from another store lineage", func() {
		push("ws-other", putMutation("c1", 1, "todo", "other", &todov1.Todo{Id: "other"}))
		foreign := run(&syncplanev1.BootstrapRequest{WorkspaceId: "ws-other", ClientId: "c1"}, 0)

		second := run(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Resume: &syncplanev1.BootstrapResume{
				Cursor: foreign.begin.GetCursor(),
				Token:  foreign.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeFalse())
		Expect(keysOf(second.rows)).To(ConsistOf(
			"todo/t1", "todo/t2", "todo/t3", "todo/t4", "todo/t5", "widget/w1"))
	})

	It("rejects a resume token it cannot parse", func() {
		first := run(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 1)
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  []byte{0xff, 0xff, 0xff},
			},
		}))
		Expect(err).NotTo(HaveOccurred())
		for stream.Receive() { //nolint:revive // draining is the point
		}
		Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
		Expect(stream.Close()).To(Succeed())
	})
})
