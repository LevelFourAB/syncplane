package server_test

import (
	"context"
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
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

var _ = Describe("Query", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	start := func(srv *server.Server) {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		_, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
	}

	query := func(req *syncplanev1.QueryRequest) []string {
		GinkgoHelper()
		req.WorkspaceId = ws
		resp, err := client.Query(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		keys := make([]string, 0, len(resp.Msg.GetRows()))
		for _, rc := range resp.Msg.GetRows() {
			Expect(rc.GetTombstone()).To(BeFalse(), "queries never return tombstones")
			keys = append(keys, rc.GetKey())
		}
		return keys
	}

	putWidget := func(id uint64, key string, count int32, label string) *syncplanev1.Mutation {
		return putMutation("c1", id, "widget", key, &testdatav1.Widget{Id: key, Count: count, Label: label})
	}

	seed := func() {
		push(
			putWidget(1, "w1", 30, "x"),
			putWidget(2, "w2", 10, "y"),
			putWidget(3, "w3", 20, "y"),
			putWidget(4, "w4", 20, "a"),
		)
	}

	countDesc := &syncplanev1.OrderKey{Path: []string{"count"}, Dir: syncplanev1.Direction_DIRECTION_DESC}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("with default permissions", func() {
		BeforeEach(func() {
			start(newTestServer())
			seed()
		})

		It("filters by predicate", func() {
			Expect(query(&syncplanev1.QueryRequest{
				Entity:    "widget",
				Predicate: countGte(20),
			})).To(Equal([]string{"w1", "w3", "w4"}))
		})

		It("returns everything without a predicate, in key order", func() {
			Expect(query(&syncplanev1.QueryRequest{Entity: "widget"})).
				To(Equal([]string{"w1", "w2", "w3", "w4"}))
		})

		It("orders with ties broken by the next key then the row key", func() {
			Expect(query(&syncplanev1.QueryRequest{
				Entity: "widget",
				OrderBy: []*syncplanev1.OrderKey{
					countDesc,
					{Path: []string{"label"}, Dir: syncplanev1.Direction_DIRECTION_ASC},
				},
			})).To(Equal([]string{"w1", "w4", "w3", "w2"}))
		})

		It("applies offset then limit after the sort", func() {
			Expect(query(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countDesc},
				Offset:  1,
				Limit:   2,
			})).To(Equal([]string{"w3", "w4"}))
		})

		It("returns empty for an offset past the result set", func() {
			Expect(query(&syncplanev1.QueryRequest{Entity: "widget", Offset: 100})).To(BeEmpty())
		})

		It("paginates through the full ordering one page at a time", func() {
			// offset+limit < the match count exercises the bounded top-k path:
			// each page is selected without buffering every row.
			var got []string
			for off := uint32(0); off < 4; off += 2 {
				got = append(got, query(&syncplanev1.QueryRequest{
					Entity:  "widget",
					OrderBy: []*syncplanev1.OrderKey{countDesc},
					Offset:  off,
					Limit:   2,
				})...)
			}
			Expect(got).To(Equal([]string{"w1", "w3", "w4", "w2"}))
		})

		It("rejects unknown entities", func() {
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "nope",
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects bad predicate paths", func() {
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "widget",
				Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"no_such_field"},
					Op:    syncplanev1.CmpOp_CMP_OP_EQ,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "x"}},
				}}},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects bad order paths", func() {
			_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "widget",
				OrderBy:     []*syncplanev1.OrderKey{{Path: []string{"nope"}, Dir: syncplanev1.Direction_DIRECTION_ASC}},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})
	})

	It("clamps the limit to MaxQueryRows", func() {
		start(newTestServer(server.WithLimits(server.Limits{MaxQueryRows: 2})))
		seed()
		Expect(query(&syncplanev1.QueryRequest{Entity: "widget", Limit: 1000})).To(HaveLen(2))
	})

	It("rejects offsets beyond MaxQueryOffset rather than clamping", func() {
		start(newTestServer(server.WithLimits(server.Limits{MaxQueryOffset: 5})))
		seed()
		// At the cap is allowed (an empty page past the result set).
		Expect(query(&syncplanev1.QueryRequest{Entity: "widget", Offset: 5})).To(BeEmpty())
		_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "widget",
			Offset:      6,
		}))
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		// The message names the knob, not just the fact: an operator reading
		// this in a log must learn what to change (issues/072).
		Expect(err).To(MatchError(ContainSubstring("offset 6, over MaxQueryOffset (5); raise it with server.WithLimits")))
	})

	It("always applies read filters", func() {
		start(newTestServer(server.WithPermissions(exampleapp.NewPermissions())))
		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "very secret"}),
		)
		resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "todo",
		}))
		Expect(err).NotTo(HaveOccurred())
		var titles []string
		for _, rc := range resp.Msg.GetRows() {
			var todo todov1.Todo
			Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
			titles = append(titles, todo.GetTitle())
		}
		Expect(titles).To(Equal([]string{"public"}))
	})
})
