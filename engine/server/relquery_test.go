package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// userFilterPerms hides users whose name contains "hidden" from reads —
// what pins that a rel pre-pass runs under the TARGET entity's read filter.
type userFilterPerms struct {
	server.AllowAll
}

func (userFilterPerms) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if entity != "user" {
		return nil, nil
	}
	return func(_ string, row proto.Message) bool {
		u, ok := row.(*testdatav1.User)
		return !ok || !strings.Contains(u.GetName(), "hidden")
	}, nil
}

var _ = Describe("Query with relation conditions", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	start := func(s *server.Server) {
		srv = s
		path, handler := s.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
	}

	seed := func() {
		GinkgoHelper()
		_, pairKey, err := srv.EntityKey(&testdatav1.Pair{Ns: "a", Id: "p1"})
		Expect(err).NotTo(HaveOccurred())
		_, otherPairKey, err := srv.EntityKey(&testdatav1.Pair{Ns: "b", Id: "p1"})
		Expect(err).NotTo(HaveOccurred())
		push(
			putMutation("c1", 1, "user", "u1", &testdatav1.User{Id: "u1", Name: "Maya"}),
			putMutation("c1", 2, "user", "u2", &testdatav1.User{Id: "u2", Name: "Noor hidden"}),
			putMutation("c1", 3, "pair", pairKey, &testdatav1.Pair{Ns: "a", Id: "p1", Label: "keep"}),
			putMutation("c1", 4, "pair", otherPairKey, &testdatav1.Pair{Ns: "b", Id: "p1", Label: "other"}),
			putMutation("c1", 5, "issue", "i1", &testdatav1.Issue{
				Id: "i1", State: "open", Assignee: &testdatav1.User{Id: "u1"},
				Pair: &testdatav1.Pair{Ns: "a", Id: "p1"},
			}),
			putMutation("c1", 6, "issue", "i2", &testdatav1.Issue{
				Id: "i2", State: "open", Assignee: &testdatav1.User{Id: "u2"},
				Pair: &testdatav1.Pair{Ns: "b", Id: "p1"},
			}),
			putMutation("c1", 7, "issue", "i3", &testdatav1.Issue{
				Id: "i3", State: "done",
				BlockedBy: []*testdatav1.Issue{{Id: "i1"}},
			}),
			putMutation("c1", 8, "issue", "i4", &testdatav1.Issue{
				Id: "i4", State: "done", Assignee: &testdatav1.User{Id: "ghost"},
			}),
		)
	}

	queryIssues := func(cond *syncplanev1.Condition) ([]string, error) {
		resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "issue",
			Predicate:   cond,
		}))
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(resp.Msg.GetRows()))
		for _, rc := range resp.Msg.GetRows() {
			keys = append(keys, rc.GetKey())
		}
		return keys, nil
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("with default permissions", func() {
		BeforeEach(func() {
			start(newTestServer())
			seed()
		})

		It("answers a to-one semi-join", func() {
			Expect(queryIssues(query.Rel("assignee", query.Eq("name", "Maya")))).To(Equal([]string{"i1"}))
		})

		It("treats unset refs and orphaned stubs as no match", func() {
			// i3 has no assignee, i4's points at a user that does not exist;
			// neither matches however permissive the inner condition.
			Expect(queryIssues(query.Rel("assignee", query.StartsWith("id", "")))).To(Equal([]string{"i1", "i2"}))
		})

		It("answers a repeated-ref semi-join", func() {
			Expect(queryIssues(query.Rel("blocked_by", query.Eq("state", "open")))).To(Equal([]string{"i3"}))
		})

		It("compares composite keys as whole tuples", func() {
			// i2's pair shares the id but not the ns of the matching target.
			Expect(queryIssues(query.Rel("pair", query.Eq("label", "keep")))).To(Equal([]string{"i1"}))
		})

		It("composes with row-local conditions", func() {
			Expect(queryIssues(query.And(
				query.Eq("state", "open"),
				query.Not(query.Rel("assignee", query.Eq("name", "Maya"))),
			))).To(Equal([]string{"i2"}))
		})

		It("rejects a rel over a non-reference field", func() {
			_, err := queryIssues(query.Rel("title", query.Eq("name", "x")))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring("matches needs a reference field")))
		})

		It("rejects a rel nested inside a rel", func() {
			_, err := queryIssues(query.Rel("assignee", query.Rel("assignee", query.Eq("name", "x"))))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects a rel in a subscription predicate", func() {
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
				ClientId:    "c1",
				Subscriptions: []*syncplanev1.Subscription{{
					Entity:    "issue",
					Predicate: query.Rel("assignee", query.Eq("name", "Maya")),
				}},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring("matches is not supported here")))
		})

		It("shares one pass between conditions selecting the same targets", func() {
			// Both nodes cross into `user` with the same inner condition, so
			// the pre-pass runs once and both replacements are built from it.
			// The answer is what says the sharing kept them apart.
			Expect(queryIssues(query.Or(
				query.Rel("assignee", query.Eq("name", "Maya")),
				query.And(
					query.Eq("state", "done"),
					query.Rel("assignee", query.Eq("name", "Maya")),
				),
			))).To(Equal([]string{"i1"}))
		})

		It("serves the same semi-join through ViewTx.Query", func() {
			var keys []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				return tx.Query(&testdatav1.Issue{}, query.Spec{
					Where: query.Rel("assignee", query.Eq("name", "Maya")),
				}, func(msg proto.Message, _ uint64) error {
					keys = append(keys, msg.(*testdatav1.Issue).GetId())
					return nil
				})
			})).To(Succeed())
			Expect(keys).To(Equal([]string{"i1"}))
		})
	})

	// Both limits bound what one request can make the server read, since
	// nothing in the predicate's own size does.
	Describe("with the rel limits tightened", func() {
		It("rejects a predicate carrying more rel conditions than MaxRelConditions", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxRelConditions: 2})))
			seed()
			conds := make([]*syncplanev1.Condition, 3)
			for i := range conds {
				conds[i] = query.Rel("assignee", query.StartsWith("name", string(rune('A'+i))))
			}
			_, err := queryIssues(query.Or(conds...))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring("MaxRelConditions")))
		})

		It("fails a rel selecting more targets than MaxRelKeys", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxRelKeys: 1})))
			seed()
			// Truncating the key set would drop matching rows silently, so the
			// query fails instead.
			_, err := queryIssues(query.Rel("assignee", query.StartsWith("id", "u")))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
			Expect(err).To(MatchError(ContainSubstring("MaxRelKeys")))
		})
	})

	Describe("with a read filter on the target entity", func() {
		BeforeEach(func() {
			start(newTestServer(server.WithPermissions(userFilterPerms{})))
			seed()
		})

		It("runs the pre-pass under the target's read filter", func() {
			// u2 is hidden from this caller, so a predicate over users must not
			// see it — otherwise the predicate is an oracle for rows the caller
			// cannot read.
			Expect(queryIssues(query.Rel("assignee", query.Contains("name", "hidden")))).To(BeEmpty())
			Expect(queryIssues(query.Rel("assignee", query.StartsWith("id", "u")))).To(Equal([]string{"i1"}))
		})

		It("sees every target on a trusted read", func() {
			var keys []string
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(&testdatav1.Issue{}, query.Spec{
					Where: query.Rel("assignee", query.Contains("name", "hidden")),
				}, func(msg proto.Message, _ uint64) error {
					keys = append(keys, msg.(*testdatav1.Issue).GetId())
					return nil
				})
			})).To(Succeed())
			Expect(keys).To(Equal([]string{"i2"}))
		})
	})
})
