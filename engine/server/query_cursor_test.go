package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// indexWalkCountingAdapter counts the index entries a read walks. It is how the
// specs below observe a seek: the ROWS a cursor page returns are the same
// whether the walk started at the range's own bound or at the cursor, so only
// the entries it read apart the two.
type indexWalkCountingAdapter struct {
	storage.Adapter
	mu      sync.Mutex
	entries int
}

func (a *indexWalkCountingAdapter) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	return a.Adapter.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		return fn(&indexWalkCountingTx{ReadTx: tx, owner: a})
	})
}

func (a *indexWalkCountingAdapter) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = 0
}

func (a *indexWalkCountingAdapter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.entries
}

type indexWalkCountingTx struct {
	storage.ReadTx
	owner *indexWalkCountingAdapter
}

func (t *indexWalkCountingTx) ScanIndex(
	entity, index string,
	lo, hi []byte,
	limit int,
	order storage.ScanOrder,
	fn func(entry []byte, rowKey string) error,
) error {
	return t.ReadTx.ScanIndex(entity, index, lo, hi, limit, order, func(entry []byte, rowKey string) error {
		t.owner.mu.Lock()
		t.owner.entries++
		t.owner.mu.Unlock()
		return fn(entry, rowKey)
	})
}

var _ = Describe("Query paging by cursor", func() {
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

	pushWidgets := func(muts ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		// A rejected BATCH is not an RPC error, and a spec that seeded nothing
		// would go on to assert against an empty entity.
		Expect(pushResults(resp.Msg)).To(HaveLen(len(muts)))
	}

	run := func(req *syncplanev1.QueryRequest) *syncplanev1.QueryResponse {
		GinkgoHelper()
		req.WorkspaceId = ws
		resp, err := client.Query(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	keysOf := func(resp *syncplanev1.QueryResponse) []string {
		out := make([]string, 0, len(resp.GetRows()))
		for _, rc := range resp.GetRows() {
			out = append(out, rc.GetKey())
		}
		return out
	}

	countAsc := &syncplanev1.OrderKey{Path: []string{"count"}, Dir: syncplanev1.Direction_DIRECTION_ASC}
	labelAsc := &syncplanev1.OrderKey{Path: []string{"label"}, Dir: syncplanev1.Direction_DIRECTION_ASC}

	// Seven widgets, distinct counts, so the ordering is total before the
	// row-key tiebreak ever runs.
	seedWidgets := func() {
		muts := make([]*syncplanev1.Mutation, 0, 7)
		for i, key := range []string{"w1", "w2", "w3", "w4", "w5", "w6", "w7"} {
			muts = append(muts, putMutation("c1", uint64(i+1), "widget", key,
				&testdatav1.Widget{Id: key, Count: int32(i + 1)}))
		}
		pushWidgets(muts...)
	}

	// pageAll walks the whole ordering with cursors, `step` rows at a time.
	pageAll := func(step uint32) []string {
		GinkgoHelper()
		var (
			got   []string
			after []byte
		)
		for {
			resp := run(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countAsc},
				Limit:   step,
				After:   after,
			})
			got = append(got, keysOf(resp)...)
			if !resp.GetHasMore() {
				Expect(len(resp.GetRows()) > 0 || after == nil).To(BeTrue())
				return got
			}
			after = resp.GetEndCursor()
			Expect(after).NotTo(BeEmpty())
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		start(newTestServer())
		seedWidgets()
	})

	It("pages forward to the same rows the offset path returns", func() {
		var byOffset []string
		for off := uint32(0); off < 7; off += 2 {
			byOffset = append(byOffset, keysOf(run(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countAsc},
				Offset:  off,
				Limit:   2,
			}))...)
		}
		Expect(pageAll(2)).To(Equal(byOffset))
		Expect(byOffset).To(Equal([]string{"w1", "w2", "w3", "w4", "w5", "w6", "w7"}))
	})

	It("pages backward from a before cursor, taking the limit from that end", func() {
		last := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Offset:  5,
			Limit:   2,
		})
		Expect(keysOf(last)).To(Equal([]string{"w6", "w7"}))

		// before alone means "the page preceding this one": the rows nearest
		// the bound, still in the query's order.
		prev := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   2,
			Before:  last.GetStartCursor(),
		})
		Expect(keysOf(prev)).To(Equal([]string{"w4", "w5"}))
		Expect(prev.GetHasMore()).To(BeTrue())

		first := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   4,
			Before:  prev.GetStartCursor(),
		})
		Expect(keysOf(first)).To(Equal([]string{"w1", "w2", "w3"}))
		Expect(first.GetHasMore()).To(BeFalse(), "nothing sorts before w1")
	})

	It("holds the next page steady when a row is inserted ahead of the cursor", func() {
		// The bug keyset paging exists to fix. Both pages are taken with the
		// same limit against the same query; only the insert happens between.
		first := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   3,
		})
		Expect(keysOf(first)).To(Equal([]string{"w1", "w2", "w3"}))

		pushWidgets(putMutation("c1", 8, "widget", "w0", &testdatav1.Widget{Id: "w0", Count: -1}))

		Expect(keysOf(run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   3,
			After:   first.GetEndCursor(),
		}))).To(Equal([]string{"w4", "w5", "w6"}))

		// The same continuation by rank re-serves w3, because the insert
		// pushed every row one place along.
		Expect(keysOf(run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   3,
			Offset:  3,
		}))).To(Equal([]string{"w3", "w4", "w5"}))
	})

	It("reads a bounded range when both cursors are set", func() {
		all := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   2,
		})
		end := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Offset:  5,
			Limit:   1,
		})
		between := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   10,
			After:   all.GetEndCursor(),
			Before:  end.GetStartCursor(),
		})
		Expect(keysOf(between)).To(Equal([]string{"w3", "w4", "w5"}),
			"both bounds are exclusive, so neither w2 nor w6 is in the range")
		Expect(between.GetHasMore()).To(BeFalse())
	})

	It("reports has_more exactly at the boundary", func() {
		exact := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   7,
		})
		Expect(exact.GetRows()).To(HaveLen(7))
		Expect(exact.GetHasMore()).To(BeFalse())

		short := run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   6,
		})
		Expect(short.GetHasMore()).To(BeTrue())

		Expect(run(&syncplanev1.QueryRequest{
			Entity:  "widget",
			OrderBy: []*syncplanev1.OrderKey{countAsc},
			Limit:   6,
			After:   short.GetEndCursor(),
		}).GetHasMore()).To(BeFalse(), "one row left, and the probe sees no eighth")
	})

	It("leaves the cursors empty for an empty page", func() {
		resp := run(&syncplanev1.QueryRequest{
			Entity:    "widget",
			OrderBy:   []*syncplanev1.OrderKey{countAsc},
			Predicate: countGte(1000),
		})
		Expect(resp.GetRows()).To(BeEmpty())
		Expect(resp.GetStartCursor()).To(BeEmpty())
		Expect(resp.GetEndCursor()).To(BeEmpty())
		Expect(resp.GetHasMore()).To(BeFalse())
	})

	Describe("rejections", func() {
		cursor := func() []byte {
			GinkgoHelper()
			resp := run(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countAsc},
				Limit:   1,
			})
			Expect(resp.GetEndCursor()).NotTo(BeEmpty())
			return resp.GetEndCursor()
		}

		fails := func(req *syncplanev1.QueryRequest) error {
			GinkgoHelper()
			req.WorkspaceId = ws
			_, err := client.Query(ctx, connect.NewRequest(req))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			return err
		}

		It("refuses an offset alongside a cursor", func() {
			Expect(fails(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countAsc},
				Offset:  1,
				After:   cursor(),
			})).To(MatchError(ContainSubstring("offset cannot be combined with after/before")))
		})

		It("refuses a cursor minted under a different ordering", func() {
			Expect(fails(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{labelAsc},
				After:   cursor(),
			})).To(MatchError(ContainSubstring("different entity or ordering")))
		})

		It("refuses a cursor over MaxQueryCursorBytes", func() {
			cur := cursor()
			start(newTestServer(server.WithLimits(server.Limits{MaxQueryCursorBytes: 8})))
			seedWidgets()
			Expect(fails(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countAsc},
				After:   cur,
			})).To(MatchError(ContainSubstring("MaxQueryCursorBytes")))
		})

		It("refuses bytes that are not a cursor", func() {
			Expect(fails(&syncplanev1.QueryRequest{
				Entity:  "widget",
				OrderBy: []*syncplanev1.OrderKey{countAsc},
				After:   []byte("not a cursor at all, but long enough"),
			})).To(MatchError(ContainSubstring("after")))
		})
	})

	Describe("served from a secondary index", func() {
		// by_state_rank is [state, rank desc], so `state == open` pins the
		// leading component and `order by rank desc` is served by the one after
		// it — the shape that makes a plan Ordered, which is the only shape the
		// seek and the early stop apply to.
		var counting *indexWalkCountingAdapter

		openRanked := func(limit uint32, after []byte) *syncplanev1.QueryRequest {
			return &syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "issue",
				Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"state"},
					Op:    syncplanev1.CmpOp_CMP_OP_EQ,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "open"}},
				}}},
				OrderBy: []*syncplanev1.OrderKey{{Path: []string{"rank"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
				Limit:   limit,
				After:   after,
			}
		}

		seedIssues := func(srv *server.Server) {
			GinkgoHelper()
			muts := make([]*syncplanev1.Mutation, 0, 20)
			for i := 0; i < 20; i++ {
				key := "i" + string(rune('a'+i))
				state := "open"
				if i%2 == 1 {
					state = "closed"
				}
				muts = append(muts, putMutation("c1", uint64(i+1), "issue", key,
					&testdatav1.Issue{Id: key, State: state, Rank: int32(i)}))
			}
			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
			Expect(err).NotTo(HaveOccurred())
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		}

		pageIssues := func(step uint32) []string {
			GinkgoHelper()
			var (
				got   []string
				after []byte
			)
			for {
				resp, err := client.Query(ctx, connect.NewRequest(openRanked(step, after)))
				Expect(err).NotTo(HaveOccurred())
				got = append(got, keysOf(resp.Msg)...)
				if !resp.Msg.GetHasMore() {
					return got
				}
				after = resp.Msg.GetEndCursor()
			}
		}

		BeforeEach(func() {
			counting = &indexWalkCountingAdapter{Adapter: newTestStore()}
			srv := mustNewServer(counting, testEntities())
			start(srv)
			seedIssues(srv)
		})

		It("returns the same pages as the scan does", func() {
			indexed := pageIssues(3)

			scanning := mustNewServer(newTestStore(), testEntities(),
				server.WithLimits(server.Limits{DisableIndexes: true}))
			start(scanning)
			seedIssues(scanning)
			Expect(pageIssues(3)).To(Equal(indexed))
			Expect(indexed).To(HaveLen(10))
		})

		It("costs the same walk however deep the page is", func() {
			// Ten issues are open. Without the seek the walk to the last page
			// would pass all ten entries; without the early stop even the first
			// page would. With both, every page costs the same bounded walk,
			// and that constancy is the whole claim keyset paging makes.
			var (
				walks []int
				after []byte
			)
			for {
				counting.reset()
				resp, err := client.Query(ctx, connect.NewRequest(openRanked(2, after)))
				Expect(err).NotTo(HaveOccurred())
				walks = append(walks, counting.count())
				if !resp.Msg.GetHasMore() {
					break
				}
				after = resp.Msg.GetEndCursor()
			}
			Expect(walks).To(HaveLen(5), "ten open issues, two at a time")
			for i, n := range walks {
				// limit(2), the has_more probe, the entry the walk is stopped
				// on, and the boundary entry the seek deliberately admits for
				// the exact comparison to reject.
				Expect(n).To(BeNumerically("<=", 5), "page %d", i)
			}
		})
	})
})
