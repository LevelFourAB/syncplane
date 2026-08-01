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
)

// Reference projection: a stored row keeps only a referenced entity's key
// fields. The merge path is unchanged (refs are keyed message lists / singular
// messages); only the write-time projection at the marshal seam is new.
var _ = Describe("Reference projection", func() {
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
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	getIssue := func(key string) *testdatav1.Issue {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		Expect(err).NotTo(HaveOccurred())
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetEntity() == "issue" && rc.GetKey() == key && !rc.GetTombstone() {
				var i testdatav1.Issue
				Expect(proto.Unmarshal(rc.GetValue(), &i)).To(Succeed())
				return &i
			}
		}
		Fail("issue " + key + " not found")
		return nil
	}

	It("PUT strips a fat to-one stub down to its key field", func() {
		push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
			Id:    "i1",
			Title: "fix it",
			// A fat stub carrying a non-key field (name) — must not be stored.
			Project: &testdatav1.Project{Id: "p1", Name: "smuggled"},
		}))
		i := getIssue("i1")
		Expect(i.GetTitle()).To(Equal("fix it"))
		Expect(i.GetProject().GetId()).To(Equal("p1"))
		Expect(i.GetProject().GetName()).To(BeEmpty())
	})

	It("PUT projects to-many and composite-key refs, leaving self id intact", func() {
		push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
			Id: "i1",
			BlockedBy: []*testdatav1.Issue{
				{Id: "i2", Title: "noise"},
				{Id: "i3", Project: &testdatav1.Project{Id: "p9", Name: "x"}},
			},
			Pair: &testdatav1.Pair{Ns: "n", Id: "k", Label: "drop me"},
		}))
		i := getIssue("i1")
		Expect(i.GetId()).To(Equal("i1"))
		Expect(i.GetBlockedBy()).To(HaveLen(2))
		Expect(i.GetBlockedBy()[0].GetId()).To(Equal("i2"))
		Expect(i.GetBlockedBy()[0].GetTitle()).To(BeEmpty())
		Expect(i.GetBlockedBy()[1].GetId()).To(Equal("i3"))
		Expect(i.GetBlockedBy()[1].GetProject()).To(BeNil())
		// Composite-key ref keeps both key fields, drops the rest.
		Expect(i.GetPair().GetNs()).To(Equal("n"))
		Expect(i.GetPair().GetId()).To(Equal("k"))
		Expect(i.GetPair().GetLabel()).To(BeEmpty())
	})

	It("PATCH projects the merged result so a fat stub cannot smuggle fields", func() {
		push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{Id: "i1", Title: "t"}))
		push(patchMutation("c1", 2, "issue", "i1", &testdatav1.Issue{
			Project: &testdatav1.Project{Id: "p2", Name: "smuggled"},
		}, "project"))
		i := getIssue("i1")
		Expect(i.GetProject().GetId()).To(Equal("p2"))
		Expect(i.GetProject().GetName()).To(BeEmpty())
		Expect(i.GetTitle()).To(Equal("t"))
	})
})
