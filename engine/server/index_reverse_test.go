package server_test

import (
	"context"
	"fmt"
	"math"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// An ordering that runs against every component it uses is served by reading
// the index backward. That must return exactly what the scan returns, with
// three things in the way: the scan breaks a tie on the row key ascending
// while a backward read meets each tie group in descending key order, a NaN
// sorts last in either direction while its tag does not invert, and the walk
// has to stop near the limit rather than read the range.
var _ = Describe("Query served by reading an index backward", func() {
	// Every index is ascending except by_title_desc, and every query below
	// orders against it.
	spec := server.DynamicEntity{
		Name:      "journal",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "day", Number: 2, Kind: protoreflect.Int64Kind},
			{Name: "title", Number: 3, Kind: protoreflect.StringKind},
			{Name: "score", Number: 4, Kind: protoreflect.DoubleKind},
		},
		Indexes: []server.DynamicIndex{
			{Name: "by_day_title", Key: []server.DynamicIndexKey{{Path: "day"}, {Path: "title"}}},
			{Name: "by_score", Key: []server.DynamicIndexKey{{Path: "score"}}},
			{Name: "by_title_desc", Key: []server.DynamicIndexKey{{Path: "title", Desc: true}}},
		},
	}

	// Three rows per day, so a page boundary can fall inside a tie group; day 0
	// is what `day > 0` leaves out. Titles run against the row keys.
	const rows = 60

	desc := func(path string) *syncplanev1.OrderKey {
		return &syncplanev1.OrderKey{Path: []string{path}, Dir: syncplanev1.Direction_DIRECTION_DESC}
	}
	asc := func(path string) *syncplanev1.OrderKey {
		return &syncplanev1.OrderKey{Path: []string{path}, Dir: syncplanev1.Direction_DIRECTION_ASC}
	}

	var ctx context.Context

	serve := func(adapter storage.Adapter, opts ...server.Option) *server.Server {
		GinkgoHelper()
		srv := mustNewServer(adapter, opts...)
		Expect(srv.RegisterDynamicEntity(spec)).To(Succeed())
		mt, err := server.BuildDynamicMessageType(spec)
		Expect(err).NotTo(HaveOccurred())
		muts := make([]*syncplanev1.Mutation, 0, rows)
		for i := 0; i < rows; i++ {
			id := fmt.Sprintf("j%02d", i)
			score := float64(i%9 - 4)
			if i%13 == 0 {
				score = math.NaN()
			}
			muts = append(muts, putMutation("c1", uint64(i+1), "journal", id, buildNote(mt, map[string]any{
				"id": id, "day": int64(i % 20), "title": fmt.Sprintf("t%02d", rows-i), "score": score,
			})))
		}
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		Expect(pushResults(resp.Msg)).To(HaveLen(len(muts)))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		return srv
	}

	type answer struct {
		keys    []string
		hasMore bool
	}
	ask := func(srv *server.Server, req *syncplanev1.QueryRequest) answer {
		GinkgoHelper()
		req.WorkspaceId, req.Entity = ws, "journal"
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

	// walked bounds the entries read: the offset and the page, its has_more
	// probe, the rest of the tie group the page ends in (at most two more rows
	// here) and the entry the walk stops on. Zero leaves it unchecked.
	DescribeTable("answers as the scan does",
		func(req func() *syncplanev1.QueryRequest, walked int) {
			counting := &indexWalkCountingAdapter{Adapter: newTestStore()}
			indexed := serve(counting)
			scanning := serve(newTestStore(), scanOnly)

			fromScan := ask(scanning, req())
			counting.reset()
			Expect(ask(indexed, req())).To(Equal(fromScan))
			Expect(counting.count()).To(BeNumerically(">", 0), "the index served the query")
			if walked > 0 {
				Expect(counting.count()).To(BeNumerically("<=", walked))
			}
		},
		Entry("stops after the tie group the limit falls in", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{desc("day")}, Limit: 5}
		}, 9),
		Entry("the latest journals: a range, read backward to the limit", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{
				OrderBy: []*syncplanev1.OrderKey{desc("day")},
				Limit:   30,
				Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"day"},
					Op:    syncplanev1.CmpOp_CMP_OP_GT,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: 0}},
				}}},
			}
		}, 34),
		Entry("past an offset", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{desc("day")}, Offset: 7, Limit: 4}
		}, 15),
		Entry("an ascending order on a descending component", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{asc("title")}, Limit: 5}
		}, 9),
		Entry("two order keys, both against the index", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{desc("day"), desc("title")}, Limit: 5}
		}, 9),
		Entry("puts NaN after every real value", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{desc("score")}, Limit: rows}
		}, 0),
		Entry("cuts a page inside the NaN rank", func() *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{desc("score")}, Limit: 57}
		}, 0),
	)

	for _, step := range []uint32{1, 4, 7} {
		It(fmt.Sprintf("walks the same pages by cursor as the scan does, %d at a time", step), func() {
			order := []*syncplanev1.OrderKey{desc("day")}
			forward := func(srv *server.Server) []string {
				GinkgoHelper()
				var (
					got   []string
					after []byte
				)
				for {
					resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
						WorkspaceId: ws, Entity: "journal", OrderBy: order, Limit: step, After: after,
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
			backward := func(srv *server.Server) []string {
				GinkgoHelper()
				resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
					WorkspaceId: ws, Entity: "journal", OrderBy: order, Offset: rows - step, Limit: step,
				}))
				Expect(err).NotTo(HaveOccurred())
				var got []string
				for _, rc := range resp.Msg.GetRows() {
					got = append(got, rc.GetKey())
				}
				before := resp.Msg.GetStartCursor()
				for {
					resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
						WorkspaceId: ws, Entity: "journal", OrderBy: order, Limit: step, Before: before,
					}))
					Expect(err).NotTo(HaveOccurred())
					page := make([]string, 0, len(resp.Msg.GetRows()))
					for _, rc := range resp.Msg.GetRows() {
						page = append(page, rc.GetKey())
					}
					got = append(page, got...)
					if !resp.Msg.GetHasMore() {
						return got
					}
					before = resp.Msg.GetStartCursor()
				}
			}
			indexed := serve(newTestStore())
			scanning := serve(newTestStore(), scanOnly)
			want := forward(scanning)
			Expect(want).To(HaveLen(rows))
			Expect(forward(indexed)).To(Equal(want))
			Expect(backward(indexed)).To(Equal(want))
		})
	}
})
