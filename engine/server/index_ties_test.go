package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// An ordered index walk that stops when the page is full must stop where the
// scan would. The scan breaks a tie on the order keys by row key; an index
// whose entry carries components past the order keys orders each tie group by
// those components instead. by_day_title is [day, title], the query orders by
// `day` alone, and inside each day the titles run against the row keys, so the
// two orders disagree inside every group.
var _ = Describe("Query served from an index with components past the order keys", func() {
	journalSpec := server.DynamicEntity{
		Name:      "journal",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "day", Number: 2, Kind: protoreflect.Int64Kind},
			{Name: "title", Number: 3, Kind: protoreflect.StringKind},
		},
		Indexes: []server.DynamicIndex{{
			Name: "by_day_title",
			Key:  []server.DynamicIndexKey{{Path: "day"}, {Path: "title"}},
		}},
	}

	type journal struct {
		id    string
		day   int64
		title string
	}
	// Each tie group is larger than the rows a page keeps, has_more probe
	// included, so a walk that stops when the page is full stops inside it.
	seed := []journal{
		{"j1", 1, "e"},
		{"j2", 1, "d"},
		{"j3", 1, "c"},
		{"j4", 1, "b"},
		{"j5", 1, "a"},
		{"j6", 2, "c"},
		{"j7", 2, "b"},
		{"j8", 2, "a"},
		{"j9", 3, "m"},
	}

	dayAsc := []*syncplanev1.OrderKey{{Path: []string{"day"}, Dir: syncplanev1.Direction_DIRECTION_ASC}}

	type answer struct {
		keys    []string
		hasMore bool
	}

	var ctx context.Context

	// serve builds a server over adapter, seeds it and builds its indexes.
	serve := func(adapter storage.Adapter, opts ...server.Option) *server.Server {
		GinkgoHelper()
		srv := mustNewServer(adapter, opts...)
		Expect(srv.RegisterDynamicEntity(journalSpec)).To(Succeed())
		mt, err := server.BuildDynamicMessageType(journalSpec)
		Expect(err).NotTo(HaveOccurred())
		muts := make([]*syncplanev1.Mutation, 0, len(seed))
		for i, row := range seed {
			muts = append(muts, putMutation("c1", uint64(i+1), "journal", row.id, buildNote(mt, map[string]any{
				"id": row.id, "day": row.day, "title": row.title,
			})))
		}
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		Expect(pushResults(resp.Msg)).To(HaveLen(len(muts)))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		return srv
	}

	ask := func(srv *server.Server, req *syncplanev1.QueryRequest) answer {
		GinkgoHelper()
		req.WorkspaceId = ws
		req.Entity = "journal"
		resp, err := srv.Query(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		keys := make([]string, 0, len(resp.Msg.GetRows()))
		for _, rc := range resp.Msg.GetRows() {
			keys = append(keys, rc.GetKey())
		}
		return answer{keys: keys, hasMore: resp.Msg.GetHasMore()}
	}

	scanOnly := server.WithLimits(server.Limits{DisableIndexes: true})

	BeforeEach(func() {
		ctx = context.Background()
	})

	DescribeTable("answers as the scan does",
		func(req func() *syncplanev1.QueryRequest, want []string) {
			counting := &indexWalkCountingAdapter{Adapter: newTestStore()}
			indexed := serve(counting)
			scanning := serve(newTestStore(), scanOnly)

			fromScan := ask(scanning, req())
			Expect(fromScan.keys).To(Equal(want))
			counting.reset()
			Expect(ask(indexed, req())).To(Equal(fromScan))
			Expect(counting.count()).To(BeNumerically(">", 0), "the index served the query")
		},
		Entry("at the limit", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: dayAsc, Limit: 2}
		}, []string{"j1", "j2"}),
		Entry("past an offset", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: dayAsc, Offset: 1, Limit: 1}
		}, []string{"j2"}),
		Entry("inside a range", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{
				OrderBy: dayAsc,
				Limit:   1,
				Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"day"},
					Op:    syncplanev1.CmpOp_CMP_OP_GT,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: 1}},
				}}},
			}
		}, []string{"j6"}),
		Entry("with a residual", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{
				OrderBy: dayAsc,
				Limit:   1,
				Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"title"},
					Op:    syncplanev1.CmpOp_CMP_OP_NEQ,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "c"}},
				}}},
			}
		}, []string{"j1"}),
	)

	It("walks the same pages by cursor as the scan does", func() {
		pageAll := func(srv *server.Server) []string {
			GinkgoHelper()
			var (
				got   []string
				after []byte
			)
			for {
				resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
					WorkspaceId: ws, Entity: "journal", OrderBy: dayAsc, Limit: 2, After: after,
				}))
				Expect(err).NotTo(HaveOccurred())
				for _, rc := range resp.Msg.GetRows() {
					got = append(got, rc.GetKey())
				}
				if !resp.Msg.GetHasMore() {
					return got
				}
				after = resp.Msg.GetEndCursor()
			}
		}
		indexed := serve(newTestStore())
		scanning := serve(newTestStore(), scanOnly)
		Expect(pageAll(scanning)).To(Equal([]string{"j1", "j2", "j3", "j4", "j5", "j6", "j7", "j8", "j9"}))
		Expect(pageAll(indexed)).To(Equal(pageAll(scanning)))
	})
})
