package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"

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

var _ = Describe("Query facets", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	clientFor := func(srv *server.Server) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	// describe renders every facet of a response as "path: value=count …",
	// so two servers' answers compare as one string each.
	describe := func(resp *syncplanev1.QueryResponse) []string {
		out := make([]string, 0, len(resp.GetFacets()))
		for _, f := range resp.GetFacets() {
			line := fmt.Sprintf("%v:", f.GetPath())
			for _, g := range f.GetGroups() {
				v := "(none)"
				if g.GetValue() != nil {
					v = g.GetValue().String()
				}
				line += fmt.Sprintf(" %s=%d", v, g.GetCount())
			}
			out = append(out, line)
		}
		return out
	}

	facet := func(path ...string) *syncplanev1.QueryFacet {
		return &syncplanev1.QueryFacet{Path: path}
	}

	Describe("against the scan", func() {
		rnd := rand.New(rand.NewSource(20260920))
		states := []string{"open", "doing", "done"}
		var rows []*testdatav1.Issue
		for i := 0; i < 120; i++ {
			row := &testdatav1.Issue{
				Id:    fmt.Sprintf("i%03d", i),
				Title: fmt.Sprintf("t%d", i),
				Rank:  int32(rnd.Intn(50)),
			}
			if i%7 != 0 {
				row.State = states[rnd.Intn(len(states))]
			}
			for _, l := range []string{"ui", "bug", "perf"} {
				if rnd.Intn(3) == 0 {
					row.Labels = append(row.Labels, l)
				}
			}
			rows = append(rows, row)
		}

		load := func(srv *server.Server) {
			GinkgoHelper()
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				for _, r := range rows {
					if err := tx.Put(proto.Clone(r).(*testdatav1.Issue)); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
		}

		var scanning, indexed syncplanev1connect.SyncServiceClient

		BeforeEach(func() {
			scanSrv, _ := newTestServerStore(server.WithLimits(server.Limits{DisableIndexes: true}))
			load(scanSrv)
			scanning = clientFor(scanSrv)

			indexSrv, _ := newTestServerStore()
			load(indexSrv)
			Expect(indexSrv.BuildIndexes(ctx, ws)).To(Succeed())
			indexed = clientFor(indexSrv)
		})

		run := func(c syncplanev1connect.SyncServiceClient, req *syncplanev1.QueryRequest) *syncplanev1.QueryResponse {
			GinkgoHelper()
			req.WorkspaceId = ws
			req.Entity = "issue"
			resp, err := c.Query(ctx, connect.NewRequest(req))
			Expect(err).NotTo(HaveOccurred())
			return resp.Msg
		}

		// agree runs one request on both servers and hands back the scan's
		// answer, having checked the index arm reproduced it exactly.
		agree := func(req *syncplanev1.QueryRequest) *syncplanev1.QueryResponse {
			GinkgoHelper()
			want := run(scanning, proto.Clone(req).(*syncplanev1.QueryRequest))
			got := run(indexed, proto.Clone(req).(*syncplanev1.QueryRequest))
			Expect(describe(got)).To(Equal(describe(want)), "the index walk and the scan must agree")
			Expect(len(got.GetRows())).To(Equal(len(want.GetRows())))
			return want
		}

		sum := func(f *syncplanev1.QueryFacet) uint64 {
			var n uint64
			for _, g := range f.GetGroups() {
				n += g.GetCount()
			}
			return n
		}

		It("counts the match set on every axis, in request order", func() {
			resp := agree(&syncplanev1.QueryRequest{
				Predicate: query.Gt("rank", 25),
				Facets:    []*syncplanev1.QueryFacet{facet("state"), facet("labels")},
			})
			Expect(resp.GetFacets()).To(HaveLen(2))
			Expect(resp.GetFacets()[0].GetPath()).To(Equal([]string{"state"}))
			Expect(resp.GetFacets()[1].GetPath()).To(Equal([]string{"labels"}))
			// Unlimited: the rows ARE the match set, so a single-valued axis
			// sums to them.
			Expect(sum(resp.GetFacets()[0])).To(Equal(uint64(len(resp.GetRows()))))
		})

		It("describes the rows a page was cut from, not the page", func() {
			whole := agree(&syncplanev1.QueryRequest{
				Predicate: query.Eq("state", "open"),
				OrderBy:   []*syncplanev1.OrderKey{{Path: []string{"rank"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
				Facets:    []*syncplanev1.QueryFacet{facet("labels")},
			})
			paged := agree(&syncplanev1.QueryRequest{
				Predicate: query.Eq("state", "open"),
				OrderBy:   []*syncplanev1.OrderKey{{Path: []string{"rank"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
				Limit:     3,
				Offset:    2,
				Facets:    []*syncplanev1.QueryFacet{facet("labels")},
			})
			Expect(paged.GetRows()).To(HaveLen(3))
			Expect(paged.GetHasMore()).To(BeTrue())
			Expect(describe(paged)).To(Equal(describe(whole)))

			// A cursor bounds the match set itself, so the counts move with it.
			first := run(indexed, &syncplanev1.QueryRequest{
				Predicate: query.Eq("state", "open"),
				OrderBy:   []*syncplanev1.OrderKey{{Path: []string{"rank"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
				Limit:     5,
			})
			rest := agree(&syncplanev1.QueryRequest{
				Predicate: query.Eq("state", "open"),
				OrderBy:   []*syncplanev1.OrderKey{{Path: []string{"rank"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
				After:     first.GetEndCursor(),
				Facets:    []*syncplanev1.QueryFacet{facet("id")},
			})
			Expect(sum(rest.GetFacets()[0])).To(Equal(uint64(len(whole.GetRows()) - 5)))
		})

		It("counts a repeated path once per element and the empty list as no value", func() {
			resp := agree(&syncplanev1.QueryRequest{
				Facets: []*syncplanev1.QueryFacet{facet("labels")},
			})
			var withLabels, without int
			for _, r := range rows {
				if len(r.Labels) > 0 {
					withLabels++
				} else {
					without++
				}
			}
			groups := resp.GetFacets()[0].GetGroups()
			last := groups[len(groups)-1]
			Expect(last.GetValue()).To(BeNil(), "the no-value group comes last")
			Expect(last.GetCount()).To(Equal(uint64(without)))
			Expect(sum(resp.GetFacets()[0])).To(BeNumerically(">", uint64(withLabels)),
				"memberships, not rows")
		})

		It("answers none for a request naming none", func() {
			resp := agree(&syncplanev1.QueryRequest{Predicate: query.Eq("state", "open")})
			Expect(resp.GetFacets()).To(BeEmpty())
		})
	})

	Describe("refusals", func() {
		var c syncplanev1connect.SyncServiceClient

		BeforeEach(func() {
			srv, _ := newTestServerStore(server.WithLimits(server.Limits{MaxQueryFacets: 2}))
			c = clientFor(srv)
		})

		expectInvalid := func(req *syncplanev1.QueryRequest, text string) {
			GinkgoHelper()
			req.WorkspaceId = ws
			req.Entity = "issue"
			_, err := c.Query(ctx, connect.NewRequest(req))
			Expect(err).To(HaveOccurred())
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err.Error()).To(ContainSubstring(text))
		}

		It("refuses facets alongside group_by", func() {
			expectInvalid(&syncplanev1.QueryRequest{
				GroupBy:   []string{"state"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				Facets:    []*syncplanev1.QueryFacet{facet("labels")},
			}, "group_by")
		})

		It("refuses a path a grouped read would refuse", func() {
			expectInvalid(&syncplanev1.QueryRequest{
				Facets: []*syncplanev1.QueryFacet{facet("custom")},
			}, "facets[0]")
			expectInvalid(&syncplanev1.QueryRequest{
				Facets: []*syncplanev1.QueryFacet{{}},
			}, "needs a path")
		})

		It("caps the axes at MaxQueryFacets", func() {
			expectInvalid(&syncplanev1.QueryRequest{
				Facets: []*syncplanev1.QueryFacet{facet("state"), facet("labels"), facet("title")},
			}, "MaxQueryFacets")
		})
	})
})
