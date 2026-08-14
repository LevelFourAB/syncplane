package server_test

import (
	"context"
	"fmt"
	"math"
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

// A grouped read is served two ways — a skip-scan over an index, and a bucketed
// scan — and the two must agree group for group and count for count. So most of
// what follows is DIFFERENTIAL: the same rows and the same request run against
// a server with its indexes built and one with them disabled, which turns any
// divergence into a failure rather than into a plausible-looking answer.
var _ = Describe("Grouped queries", func() {
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

	// describe renders a response's groups as strings, so a mismatch names the
	// group and the count rather than printing two proto dumps.
	describe := func(resp *syncplanev1.QueryResponse) []string {
		out := make([]string, 0, len(resp.GetGroups()))
		for _, g := range resp.GetGroups() {
			v := "(none)"
			if g.GetValue() != nil {
				v = g.GetValue().String()
			}
			out = append(out, fmt.Sprintf("%s=%d", v, g.GetCount()))
		}
		return out
	}

	Describe("against the scan", func() {
		// Rows are randomized so the specs cover value distributions nobody
		// thought to write down — a group with one row, a group with hundreds,
		// a value only one row holds.
		rnd := rand.New(rand.NewSource(20260814))
		states := []string{"open", "doing", "done"}
		var rows []*testdatav1.Issue
		for i := 0; i < 120; i++ {
			row := &testdatav1.Issue{
				Id:     fmt.Sprintf("i%03d", i),
				Title:  fmt.Sprintf("t%d", i),
				Rank:   int32(rnd.Intn(50)),
				Custom: map[string]*testdatav1.FieldValue{},
			}
			if i%7 != 0 {
				// Every seventh row leaves `state` at its zero value, which is
				// the empty string rather than "missing" — an implicit-presence
				// scalar is always found.
				row.State = states[rnd.Intn(len(states))]
			}
			for _, l := range []string{"ui", "bug", "perf"} {
				if rnd.Intn(3) == 0 {
					row.Labels = append(row.Labels, l)
				}
			}
			if i%3 == 0 {
				row.Custom["priority"] = &testdatav1.FieldValue{
					Kind: &testdatav1.FieldValue_Number{Number: float64(rnd.Intn(4))},
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

		run := func(c syncplanev1connect.SyncServiceClient, req *syncplanev1.QueryRequest) []string {
			GinkgoHelper()
			req.WorkspaceId = ws
			req.Entity = "issue"
			resp, err := c.Query(ctx, connect.NewRequest(req))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetRows()).To(BeEmpty(), "a grouped read answers with groups, not rows")
			return describe(resp.Msg)
		}

		agree := func(req *syncplanev1.QueryRequest) []string {
			GinkgoHelper()
			want := run(scanning, proto.Clone(req).(*syncplanev1.QueryRequest))
			Expect(run(indexed, proto.Clone(req).(*syncplanev1.QueryRequest))).To(Equal(want),
				"the skip-scan and the bucketed scan must agree")
			return want
		}

		It("counts by a leading indexed component", func() {
			got := agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"state"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
			Expect(got).NotTo(BeEmpty())
		})

		It("names the distinct values of that component", func() {
			got := agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"state"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_DISTINCT,
			})
			for _, g := range got {
				Expect(g).To(HaveSuffix("=0"), "a distinct read reports no count")
			}
		})

		It("counts under a residual the range cannot express", func() {
			agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"state"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				Predicate: query.Gt("rank", 25),
			})
		})

		It("counts a repeated path once per element", func() {
			byLabel := agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"labels"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
			Expect(byLabel).NotTo(BeEmpty())
		})

		It("counts a map entry addressed by key", func() {
			agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"custom", "priority", "number"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
		})

		It("counts by a path no index reaches", func() {
			agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"title"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
		})

		It("caps the groups returned and reports the rest", func() {
			GinkgoHelper()
			req := &syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "issue",
				GroupBy:     []string{"rank"},
				Aggregate:   syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				Limit:       3,
			}
			for _, c := range []syncplanev1connect.SyncServiceClient{scanning, indexed} {
				resp, err := c.Query(ctx, connect.NewRequest(proto.Clone(req).(*syncplanev1.QueryRequest)))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Msg.GetGroups()).To(HaveLen(3))
				Expect(resp.Msg.GetHasMore()).To(BeTrue())
			}
			// And the capped page is the FIRST three of the uncapped answer,
			// which is what makes the ordering worth promising.
			full := agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"rank"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
			capped := agree(&syncplanev1.QueryRequest{
				GroupBy:   []string{"rank"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				Limit:     3,
			})
			Expect(capped).To(Equal(full[:3]))
		})
	})

	Describe("ordering and shape", func() {
		var client syncplanev1connect.SyncServiceClient

		BeforeEach(func() {
			srv, _ := newTestServerStore()
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				ratio := func(v float64) *testdatav1.Settings { return &testdatav1.Settings{Ratio: v} }
				return firstErr(
					tx.Put(&testdatav1.Widget{Id: "w1", Label: "b", Settings: ratio(2)}),
					tx.Put(&testdatav1.Widget{Id: "w2", Label: "a", Settings: ratio(1)}),
					tx.Put(&testdatav1.Widget{Id: "w3", Label: "a", Settings: ratio(math.NaN())}),
					// No settings at all, so settings.ratio resolves to nothing.
					tx.Put(&testdatav1.Widget{Id: "w4", Label: "c"}),
				)
			})).To(Succeed())
			client = clientFor(srv)
		})

		groups := func(req *syncplanev1.QueryRequest) *syncplanev1.QueryResponse {
			GinkgoHelper()
			req.WorkspaceId = ws
			req.Entity = "widget"
			resp, err := client.Query(ctx, connect.NewRequest(req))
			Expect(err).NotTo(HaveOccurred())
			return resp.Msg
		}

		It("orders groups by value with the no-value group last", func() {
			resp := groups(&syncplanev1.QueryRequest{
				GroupBy:   []string{"settings", "ratio"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
			Expect(describe(resp)).To(Equal([]string{
				"double_value:1=1", "double_value:2=1", "double_value:nan=1", "(none)=1",
			}))
		})

		It("counts rows sharing a value into one group", func() {
			resp := groups(&syncplanev1.QueryRequest{
				GroupBy:   []string{"label"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
			Expect(describe(resp)).To(Equal([]string{
				`string_value:"a"=2`, `string_value:"b"=1`, `string_value:"c"=1`,
			}))
		})

		It("reports no group for a value no row holds", func() {
			// The group DOMAIN is the app's; a read over rows can only report
			// the values that are there.
			resp := groups(&syncplanev1.QueryRequest{
				GroupBy:   []string{"label"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				Predicate: query.Eq("label", "zzz"),
			})
			Expect(resp.GetGroups()).To(BeEmpty())
		})
	})

	Describe("validation", func() {
		var client syncplanev1connect.SyncServiceClient

		BeforeEach(func() { client = clientFor(newTestServer()) })

		reject := func(req *syncplanev1.QueryRequest, msg string) {
			GinkgoHelper()
			req.WorkspaceId = ws
			req.Entity = "widget"
			_, err := client.Query(ctx, connect.NewRequest(req))
			Expect(err).To(HaveOccurred())
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err.Error()).To(ContainSubstring(msg))
		}

		It("rejects a group_by with no aggregate", func() {
			reject(&syncplanev1.QueryRequest{GroupBy: []string{"label"}}, "COUNT or DISTINCT")
		})

		It("rejects an aggregate with no group_by", func() {
			reject(&syncplanev1.QueryRequest{
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			}, "needs a group_by path")
		})

		It("rejects the paging fields a set of groups has no counterpart for", func() {
			reject(&syncplanev1.QueryRequest{
				GroupBy:   []string{"label"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				OrderBy:   []*syncplanev1.OrderKey{query.Asc("count")},
			}, "order_by")
			reject(&syncplanev1.QueryRequest{
				GroupBy:   []string{"label"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				Offset:    2,
			}, "offset")
			reject(&syncplanev1.QueryRequest{
				GroupBy:   []string{"label"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				After:     []byte{1, 2, 3},
			}, "after/before")
		})

		It("rejects a path nothing can be grouped by", func() {
			reject(&syncplanev1.QueryRequest{
				GroupBy:   []string{"settings"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			}, "not comparable")
		})
	})
})

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
