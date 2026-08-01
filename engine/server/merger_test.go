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

func concatDocs(base, incoming []byte) ([]byte, error) {
	var b, in testdatav1.Blob
	if err := proto.Unmarshal(base, &b); err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(incoming, &in); err != nil {
		return nil, err
	}
	b.Data = append(b.Data, in.Data...)
	b.Note += in.Note
	return proto.Marshal(&b)
}

var _ = Describe("Merge hooks", func() {
	// A bad WithMerger is New's error to report, so these assert on the whole
	// bring-up rather than on a registration call.
	Describe("WithMerger validation", func() {
		It("accepts opaque message and bytes fields", func() {
			Expect(newTestServerErr(
				server.WithMerger(&testdatav1.Widget{}, "doc", concatDocs),
				server.WithMerger(&testdatav1.Widget{}, "raw", concatDocs),
				server.WithMerger(&testdatav1.Widget{}, "settings.inner", concatDocs),
			)).To(Succeed())
		})

		It("rejects entities the server does not declare", func() {
			Expect(newTestServerErr(server.WithMerger(&testdatav1.Blob{}, "data", concatDocs))).
				To(MatchError(ContainSubstring(
					"is not a registered entity; register it first with server.WithEntities(&Blob{})")))
		})

		It("rejects non-opaque fields", func() {
			Expect(newTestServerErr(server.WithMerger(&testdatav1.Widget{}, "settings", concatDocs))).
				To(MatchError(ContainSubstring("not opaque")))
		})

		It("rejects unknown fields", func() {
			Expect(newTestServerErr(server.WithMerger(&testdatav1.Widget{}, "nope", concatDocs))).
				To(MatchError(ContainSubstring(`unknown field "nope"`)))
		})

		It("rejects a duplicate declaration", func() {
			Expect(newTestServerErr(
				server.WithMerger(&testdatav1.Widget{}, "doc", concatDocs),
				server.WithMerger(&testdatav1.Widget{}, "doc", concatDocs),
			)).To(MatchError(ContainSubstring("already registered")))
		})

		It("does not care whether the entity is declared before or after it", func() {
			store := newTestStore()
			_, err := server.New(store,
				server.WithMerger(&testdatav1.Widget{}, "doc", concatDocs),
				server.WithEntities(&testdatav1.Widget{}))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("PATCH with a registered merger", func() {
		var (
			ctx    context.Context
			client syncplanev1connect.SyncServiceClient
		)

		BeforeEach(func() {
			ctx = context.Background()
			srv := newTestServer(server.WithMerger(&testdatav1.Widget{}, "doc", concatDocs))
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		})

		push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
			GinkgoHelper()
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				mutations)))
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

		It("merges concurrent PATCHes so both contributions survive", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id:  "w1",
				Doc: &testdatav1.Blob{Data: []byte("A"), Note: "a"},
			}))
			// Two clients patch doc concurrently; the server linearizes them
			// and the merger folds each incoming value into the stored one.
			push(patchMutation("c2", 1, "widget", "w1", &testdatav1.Widget{
				Doc: &testdatav1.Blob{Data: []byte("B"), Note: "b"},
			}, "doc"))
			push(patchMutation("c3", 1, "widget", "w1", &testdatav1.Widget{
				Doc: &testdatav1.Blob{Data: []byte("C"), Note: "c"},
			}, "doc"))

			w := getWidget("w1")
			Expect(w.GetDoc().GetData()).To(Equal([]byte("ABC")))
			Expect(w.GetDoc().GetNote()).To(Equal("abc"))
		})

		It("keeps PUT replace-whole semantics", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id:  "w1",
				Doc: &testdatav1.Blob{Data: []byte("A")},
			}))
			push(putMutation("c1", 2, "widget", "w1", &testdatav1.Widget{
				Id:  "w1",
				Doc: &testdatav1.Blob{Data: []byte("B")},
			}))
			Expect(getWidget("w1").GetDoc().GetData()).To(Equal([]byte("B")))
		})

		It("patching other fields leaves the opaque field alone", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id:  "w1",
				Doc: &testdatav1.Blob{Data: []byte("A")},
			}))
			push(patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Label: "renamed"}, "label"))
			w := getWidget("w1")
			Expect(w.GetLabel()).To(Equal("renamed"))
			Expect(w.GetDoc().GetData()).To(Equal([]byte("A")))
		})
	})
})
