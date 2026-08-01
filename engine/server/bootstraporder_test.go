package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// A cold client reads a snapshot as it lands, entity by entity, so the order
// entities go out in decides what the user sees first — and the order announced
// on Begin is what lets that client tell an entity still queued from one that
// shipped nothing. Both halves are asserted here: the announcement, and that
// the rows really follow it.
var _ = Describe("Bootstrap delivery order", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		path, handler := newTestServer().Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	// One row per entity, so the row stream itself spells out the delivery
	// order and a spec can compare it against the announcement.
	seedOneEach := func() {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "t"}),
			putMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			putMutation("c1", 3, "project", "p1", &testdatav1.Project{Id: "p1"}),
			putMutation("c1", 4, "user", "u1", &testdatav1.User{Id: "u1"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	// bootstrap returns the announced order and the entity of each row in the
	// order it arrived.
	bootstrap := func(req *syncplanev1.BootstrapRequest) (announced, rowOrder []string) {
		GinkgoHelper()
		req.WorkspaceId = ws
		req.ClientId = "c1"
		stream, err := client.Bootstrap(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				announced = body.Begin.GetEntities()
			case *syncplanev1.BootstrapResponse_Chunk_:
				for _, rc := range body.Chunk.GetRows() {
					if len(rowOrder) == 0 || rowOrder[len(rowOrder)-1] != rc.GetEntity() {
						rowOrder = append(rowOrder, rc.GetEntity())
					}
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		return announced, rowOrder
	}

	It("announces every entity in scope, and ships rows in that order", func() {
		seedOneEach()
		announced, rowOrder := bootstrap(&syncplanev1.BootstrapRequest{})
		// Full sync: everything registered, in the server's own (sorted) order.
		Expect(announced).To(Equal([]string{"issue", "pair", "project", "todo", "user", "widget"}))
		Expect(rowOrder).To(Equal([]string{"project", "todo", "user", "widget"}))
	})

	It("ships priority entities first, in the order asked for", func() {
		seedOneEach()
		announced, rowOrder := bootstrap(&syncplanev1.BootstrapRequest{
			PriorityEntities: []string{"user", "todo"},
		})
		Expect(announced).To(Equal([]string{"user", "todo", "issue", "pair", "project", "widget"}))
		Expect(rowOrder).To(Equal([]string{"user", "todo", "project", "widget"}))
	})

	It("serves a subscribed client in subscription order", func() {
		seedOneEach()
		announced, rowOrder := bootstrap(&syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{
				{Entity: "widget"},
				{Entity: "todo"},
				{Entity: "project"},
			},
		})
		Expect(announced).To(Equal([]string{"widget", "todo", "project"}))
		Expect(rowOrder).To(Equal([]string{"widget", "todo", "project"}))
	})

	It("lets priority reorder a subscription set", func() {
		seedOneEach()
		announced, _ := bootstrap(&syncplanev1.BootstrapRequest{
			PriorityEntities: []string{"todo"},
			Subscriptions: []*syncplanev1.Subscription{
				{Entity: "widget"},
				{Entity: "todo"},
			},
		})
		Expect(announced).To(Equal([]string{"todo", "widget"}))
	})

	It("ignores priority names the snapshot cannot ship", func() {
		seedOneEach()
		// "nope" is not registered and "widget" is outside the subscription set:
		// naming either is over-specified, not wrong, so the bootstrap serves the
		// scope it has rather than failing.
		announced, rowOrder := bootstrap(&syncplanev1.BootstrapRequest{
			PriorityEntities: []string{"nope", "widget", "todo"},
			Subscriptions:    []*syncplanev1.Subscription{{Entity: "todo"}},
		})
		Expect(announced).To(Equal([]string{"todo"}))
		Expect(rowOrder).To(Equal([]string{"todo"}))
	})

	It("puts follow-reachable entities after the subscribed ones", func() {
		seedOneEach()
		// `issue` is reachable only by following project's reverse relation, so
		// no request names it and it lands in the trailing sorted tier.
		announced, _ := bootstrap(&syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{{
				Entity: "project",
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}},
				},
			}},
		})
		Expect(announced).To(Equal([]string{"project", "issue"}))
	})
})

var _ = Describe("Bootstrap entity ordering", func() {
	It("is a reordering, never a filter", func() {
		// Belt and braces on the tier logic itself: whatever the request says,
		// the snapshot ships exactly the entities the scope admits.
		path, handler := newTestServer(server.WithSchemaRevision("r1")).Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		stream, err := client.Bootstrap(context.Background(), connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId:      ws,
			PriorityEntities: []string{"widget", "widget", "todo"},
		}))
		Expect(err).NotTo(HaveOccurred())
		var announced []string
		for stream.Receive() {
			if body, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Begin_); ok {
				announced = body.Begin.GetEntities()
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(announced).To(ConsistOf("issue", "pair", "project", "todo", "user", "widget"))
		Expect(announced[:2]).To(Equal([]string{"widget", "todo"}))
	})
})
