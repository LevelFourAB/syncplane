package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("BatchMutation", func() {
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

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	pull := func() *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Cursor:      nil,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	widgetAt := func(rc *syncplanev1.RowChange) *testdatav1.Widget {
		GinkgoHelper()
		var w testdatav1.Widget
		Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
		return &w
	}

	BeforeEach(func() {
		ctx = context.Background()
		start(newTestServer())
	})

	It("applies every sub-mutation atomically under one id and one sequence", func() {
		resp := push(batchMutation("c1", 1,
			crudPut("widget", "a", &testdatav1.Widget{Id: "a", Label: "A"}),
			crudPut("widget", "b", &testdatav1.Widget{Id: "b", Label: "B"}),
		))
		Expect(resp).To(HaveLen(1), "a batch is one mutation -> one result")
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		Expect(resp[0].GetSeq()).To(BeEquivalentTo(1))

		pulled := pull()
		changes := pulled.GetPatch().GetChanges()
		Expect(changes).To(HaveLen(2))
		for _, rc := range changes {
			Expect(rc.GetVersion()).To(BeEquivalentTo(1), "all rows of a batch share one version")
		}
		Expect(pulled.GetLastAckedMutationId()).To(BeEquivalentTo(1))
	})

	It("paginates across a batch's shared sequence without dropping rows", func() {
		// All three rows land at version 1. A page limit of 1 splits the group
		// at every boundary; before the (entity, key) tie-break the cursor
		// recorded a bare seq and the next pull's version>seq scan dropped the
		// rest of the group forever.
		push(batchMutation("c1", 1,
			crudPut("widget", "a", &testdatav1.Widget{Id: "a"}),
			crudPut("widget", "b", &testdatav1.Widget{Id: "b"}),
			crudPut("widget", "c", &testdatav1.Widget{Id: "c"}),
		))

		var (
			cursor []byte
			keys   []string
		)
		for {
			resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
				ClientId:    "c1",
				Cursor:      cursor,
				Limit:       1,
			}))
			Expect(err).NotTo(HaveOccurred())
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				Expect(rc.GetVersion()).To(BeEquivalentTo(1))
				keys = append(keys, rc.GetKey())
			}
			cursor = resp.Msg.GetNextCursor()
			if !resp.Msg.GetHasMore() {
				break
			}
		}
		Expect(keys).To(ConsistOf("a", "b", "c"), "every row of the split batch reaches the client exactly once")
	})

	It("applies sub-mutations in order so a later op sees an earlier one", func() {
		resp := push(batchMutation("c1", 1,
			crudPut("widget", "a", &testdatav1.Widget{Id: "a", Label: "first", Count: 1}),
			crudPatch("widget", "a", &testdatav1.Widget{Id: "a", Label: "second"}, "label"),
		))
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		changes := pull().GetPatch().GetChanges()
		Expect(changes).To(HaveLen(1), "both ops target the same key -> one row")
		w := widgetAt(changes[0])
		Expect(w.GetLabel()).To(Equal("second"), "the PATCH merged onto the PUT from the same batch")
		Expect(w.GetCount()).To(BeEquivalentTo(1), "unmasked fields from the PUT survive")
	})

	It("rolls the whole batch back when any sub-mutation is rejected", func() {
		resp := push(batchMutation("c1", 1,
			crudPut("widget", "a", &testdatav1.Widget{Id: "a", Label: "A"}),
			// PATCH of a row that does not exist -> delete-wins conflict.
			crudPatch("widget", "ghost", &testdatav1.Widget{Id: "ghost", Label: "x"}, "label"),
		))
		Expect(resp).To(HaveLen(1))
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(resp[0].GetErrorCode()).To(Equal(server.CodeConflict))
		// The rejection is still an ack: the watermark advanced.
		Expect(pull().GetPatch().GetChanges()).To(BeEmpty(), "the successful PUT half rolled back too")
		Expect(pull().GetLastAckedMutationId()).To(BeEquivalentTo(1))
	})

	It("consumes no sequence when rejected: the next mutation gets seq 1", func() {
		push(batchMutation("c1", 1,
			crudPut("widget", "a", &testdatav1.Widget{Id: "a"}),
			crudDelete("widget", ""), // empty key -> rejected
		))
		resp := push(putMutation("c1", 2, "widget", "b", &testdatav1.Widget{Id: "b"}))
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		Expect(resp[0].GetSeq()).To(BeEquivalentTo(1), "the rejected batch burned no sequence")
	})

	It("rejects an empty batch", func() {
		resp := push(batchMutation("c1", 1))
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(resp[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
	})

	It("rejects a batch whose sub-mutation count exceeds the per-push limit", func() {
		start(newTestServer(server.WithLimits(server.Limits{MaxMutationsPerPush: 2})))
		resp := push(batchMutation("c1", 1,
			crudPut("widget", "a", &testdatav1.Widget{Id: "a"}),
			crudPut("widget", "b", &testdatav1.Widget{Id: "b"}),
			crudPut("widget", "c", &testdatav1.Widget{Id: "c"}),
		))
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(resp[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		Expect(resp[0].GetErrorMessage()).To(Equal(
			"batch of 3 mutations, over MaxMutationsPerPush (2); raise it with server.WithLimits"))
	})

	It("acks a rejected batch alongside good neighbours in the same push", func() {
		resp := push(
			putMutation("c1", 1, "widget", "before", &testdatav1.Widget{Id: "before"}),
			batchMutation("c1", 2,
				crudPut("widget", "x", &testdatav1.Widget{Id: "x"}),
				crudPatch("widget", "ghost", &testdatav1.Widget{Id: "ghost"}, "label"),
			),
			putMutation("c1", 3, "widget", "after", &testdatav1.Widget{Id: "after"}),
		)
		Expect(resp).To(HaveLen(3))
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		Expect(resp[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(resp[2].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		pulled := pull()
		Expect(pulled.GetPatch().GetChanges()).To(HaveLen(2), "only the two neighbours wrote")
		Expect(pulled.GetLastAckedMutationId()).To(BeEquivalentTo(3))
	})
})
