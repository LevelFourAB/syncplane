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

// A oneof is one field: the group is what a mask names and what a write
// replaces. Everything that needs a field to be reliably THERE — a key, a
// keyed-list identity, a text document, a reference — cannot sit inside one,
// and registration says so rather than letting the schema half-work.
var _ = Describe("Oneofs", func() {
	Describe("registration", func() {
		It("rejects a key field inside a oneof", func() {
			_, err := server.New(newTestStore(),
				server.WithEntities(&testdatav1.OneofKeyEntity{}))
			Expect(err).To(MatchError(ContainSubstring(
				`key field "id" is a member of oneof "which"`)))
		})

		It("rejects a text field inside a oneof", func() {
			_, err := server.New(newTestStore(),
				server.WithEntities(&testdatav1.OneofTextEntity{}))
			Expect(err).To(MatchError(ContainSubstring(`text field "body" is inside oneof "which"`)))
		})

		// One level down is the same problem: the mask stops at the group, so
		// nothing below it ever reaches the merger.
		It("rejects a text field below a oneof member", func() {
			_, err := server.New(newTestStore(),
				server.WithEntities(&testdatav1.OneofNestedTextEntity{}))
			Expect(err).To(MatchError(ContainSubstring(
				`text field "holder.body" is inside oneof "which"`)))
		})

		It("rejects a reference inside a oneof", func() {
			_, err := server.New(newTestStore(),
				server.WithEntities(&testdatav1.Project{}, &testdatav1.User{},
					&testdatav1.OneofRefEntity{}))
			Expect(err).To(MatchError(ContainSubstring(
				`reference field "project" is a member of oneof "target"`)))
		})

		It("rejects a keyed-list identity inside the element's oneof", func() {
			_, err := server.New(newTestStore(),
				server.WithEntities(&testdatav1.OneofListIdentityEntity{}))
			Expect(err).To(MatchError(ContainSubstring(
				`"uid" is a member of oneof "which"`)))
		})

		It("rejects a merger on a oneof member", func() {
			Expect(newTestServerErr(
				server.WithMerger(&testdatav1.Widget{}, "choice_settings", concatDocs),
			)).To(MatchError(ContainSubstring(`"choice_settings" is inside oneof "choice"`)))
		})

		It("accepts an entity that merely has a oneof", func() {
			Expect(newTestServerErr()).To(Succeed())
		})
	})

	Describe("PATCH over the wire", func() {
		var (
			ctx    context.Context
			client syncplanev1connect.SyncServiceClient
		)

		BeforeEach(func() {
			ctx = context.Background()
			srv := newTestServer()
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		})

		push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
			GinkgoHelper()
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
			Expect(err).NotTo(HaveOccurred())
			return pushResults(resp.Msg)
		}

		getWidget := func(key string) *testdatav1.Widget {
			GinkgoHelper()
			resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
			}))
			Expect(err).NotTo(HaveOccurred())
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				if rc.GetEntity() == "widget" && rc.GetKey() == key && !rc.GetTombstone() {
					var w testdatav1.Widget
					Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
					return &w
				}
			}
			Fail("widget " + key + " not found")
			return nil
		}

		It("switches the case and leaves unmasked fields alone", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id:     "w1",
				Label:  "keep me",
				Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "first"},
			}))
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{
				Choice: &testdatav1.Widget_ChoiceNumber{ChoiceNumber: 42},
			}, "choice"))

			w := getWidget("w1")
			Expect(w.GetChoiceNumber()).To(BeEquivalentTo(42))
			Expect(w.GetChoiceText()).To(BeEmpty())
			Expect(w.GetLabel()).To(Equal("keep me"))
		})

		It("clears the group when the patch carries no case", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id:     "w1",
				Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "first"},
			}))
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{}, "choice"))
			Expect(getWidget("w1").GetChoice()).To(BeNil())
		})

		It("rejects a mask naming a member", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
			results := push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{
				Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "nope"},
			}, "choice_text"))
			Expect(results).To(HaveLen(1))
			Expect(results[0].GetStatus()).
				To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorMessage()).To(ContainSubstring(`member of oneof "choice"`))
		})
	})
})
