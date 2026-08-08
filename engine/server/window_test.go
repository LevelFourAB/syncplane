package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// countDesc orders widgets by count, highest first — the ordering every
// order_by window spec below uses.
func countDesc() []*syncplanev1.OrderKey {
	return []*syncplanev1.OrderKey{{Path: []string{"count"}, Dir: syncplanev1.Direction_DIRECTION_DESC}}
}

// windowSub builds a widget subscription bounded to the top `limit` rows by
// count (descending), optionally narrowed by a predicate first.
func windowSub(limit uint32, pred *syncplanev1.Condition) *syncplanev1.Subscription {
	return &syncplanev1.Subscription{
		Entity:    "widget",
		Predicate: pred,
		Window:    &syncplanev1.Window{OrderBy: countDesc(), Limit: limit},
	}
}

// recencySub bounds a widget subscription to the `limit` most recently
// written rows.
func recencySub(limit uint32) *syncplanev1.Subscription {
	return &syncplanev1.Subscription{
		Entity: "widget",
		Window: &syncplanev1.Window{ByRecency: true, Limit: limit},
	}
}

// Windowed subscriptions bound a client's working set by RANK, not just by
// attribute. Membership rides on the cursor as an exact key-set, so every
// spec here is ultimately about one question: does the row that fell off the
// tail actually leave the client?
var _ = Describe("Windowed subscriptions", func() {
	var (
		ctx     context.Context
		client  syncplanev1connect.SyncServiceClient
		nextMut uint64
	)

	startWith := func(opts ...server.Option) {
		srv := newTestServer(opts...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	BeforeEach(func() {
		ctx = context.Background()
		nextMut = 0
		startWith()
	})

	// putWidget writes one widget, allocating the next mutation id.
	putWidget := func(id string, count int32) {
		GinkgoHelper()
		nextMut++
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", nextMut, "widget", id, &testdatav1.Widget{Id: id, Count: count}),
		})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	delWidget := func(id string) {
		GinkgoHelper()
		nextMut++
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{{
			ClientId:   "c1",
			MutationId: nextMut,
			Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
				Entity: "widget", Key: id, Op: syncplanev1.CrudOp_CRUD_OP_DELETE,
			}},
		}})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	pull := func(cursor []byte, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c1", Cursor: cursor, Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	bootstrap := func(subs ...*syncplanev1.Subscription) (cursor []byte, rows []*syncplanev1.RowChange) {
		GinkgoHelper()
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws, ClientId: "c1", Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				cursor = body.Begin.GetCursor()
			case *syncplanev1.BootstrapResponse_Chunk_:
				rows = append(rows, body.Chunk.GetRows()...)
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(cursor).NotTo(BeNil())
		return cursor, rows
	}

	// drain pulls until the server stops asking for more, accumulating every
	// change. A resync answers has_more even with nothing left to send, so a
	// single pull is never enough to observe a completed scope change.
	drain := func(cursor []byte, subs ...*syncplanev1.Subscription) ([]byte, []*syncplanev1.RowChange) {
		GinkgoHelper()
		var all []*syncplanev1.RowChange
		for i := 0; i < 20; i++ {
			resp := pull(cursor, subs...)
			Expect(resp.GetReset_()).To(BeFalse(), "unexpected reset")
			all = append(all, resp.GetPatch().GetChanges()...)
			cursor = resp.GetNextCursor()
			if !resp.GetHasMore() {
				return cursor, all
			}
		}
		Fail("drain did not settle")
		return nil, nil
	}

	// valueKeys / tombstoneKeys project a change list to the keys of each kind,
	// which is what every assertion here is really about.
	valueKeys := func(changes []*syncplanev1.RowChange) []string {
		out := []string{}
		for _, c := range changes {
			if c.GetValue() != nil {
				out = append(out, c.GetKey())
			}
		}
		return out
	}
	tombstoneKeys := func(changes []*syncplanev1.RowChange) []string {
		out := []string{}
		for _, c := range changes {
			if c.GetTombstone() {
				out = append(out, c.GetKey())
			}
		}
		return out
	}

	Describe("Bootstrap", func() {
		It("ships exactly the window, not the whole entity", func() {
			putWidget("a", 10)
			putWidget("b", 30)
			putWidget("c", 20)
			putWidget("d", 5)

			_, rows := bootstrap(windowSub(2, nil))
			Expect(valueKeys(rows)).To(ConsistOf("b", "c"))
		})

		It("applies the predicate before the rank", func() {
			putWidget("a", 100) // excluded by the predicate despite ranking first
			putWidget("b", 30)
			putWidget("c", 20)
			putWidget("d", 15)

			// count < 50, top 2 by count.
			under50 := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path:  []string{"count"},
				Op:    syncplanev1.CmpOp_CMP_OP_LT,
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: 50}},
			}}}
			_, rows := bootstrap(windowSub(2, under50))
			Expect(valueKeys(rows)).To(ConsistOf("b", "c"))
		})

		It("ships everything when the window is larger than the entity", func() {
			putWidget("a", 1)
			putWidget("b", 2)
			_, rows := bootstrap(windowSub(10, nil))
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))
		})
	})

	Describe("Pull", func() {
		It("evicts the tail as a tombstone when a new row takes the head", func() {
			putWidget("a", 10)
			putWidget("b", 20)
			sub := windowSub(2, nil)
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))

			putWidget("c", 30) // pushes "a" (count 10) off the tail
			_, changes := drain(cursor, sub)
			Expect(valueKeys(changes)).To(ContainElement("c"))
			Expect(tombstoneKeys(changes)).To(ContainElement("a"))
		})

		It("refills from below when a member is deleted", func() {
			putWidget("a", 10)
			putWidget("b", 20)
			putWidget("c", 30)
			sub := windowSub(2, nil)
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("b", "c"))

			delWidget("c")
			_, changes := drain(cursor, sub)
			// "c" is gone and "a" rises into the freed slot.
			Expect(tombstoneKeys(changes)).To(ContainElement("c"))
			Expect(valueKeys(changes)).To(ContainElement("a"))
		})

		It("drops a member edited out of the predicate", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			sub := windowSub(5, countGte(10))
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))

			putWidget("a", 1) // still exists, no longer matches
			_, changes := drain(cursor, sub)
			Expect(tombstoneKeys(changes)).To(ContainElement("a"))
		})

		It("drops a member that merely sank below the rank boundary", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			sub := windowSub(2, nil)
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))

			// "b" sinks under "c" without leaving any predicate: only the RANK
			// changed, which is exactly what a predicate-only scope cannot see.
			putWidget("b", 5)
			_, changes := drain(cursor, sub)
			Expect(tombstoneKeys(changes)).To(ContainElement("b"))
			Expect(valueKeys(changes)).To(ContainElement("c"))
		})

		It("emits nothing when the window did not move", func() {
			putWidget("a", 10)
			sub := windowSub(2, nil)
			cursor, _ := bootstrap(sub)
			_, changes := drain(cursor, sub)
			Expect(changes).To(BeEmpty())
		})
	})

	Describe("by_recency", func() {
		It("holds the most recently written rows with no order fields", func() {
			putWidget("a", 1)
			putWidget("b", 1)
			putWidget("c", 1)
			_, rows := bootstrap(recencySub(2))
			Expect(valueKeys(rows)).To(ConsistOf("b", "c"))
		})

		It("re-ranks on write, evicting the least recent", func() {
			putWidget("a", 1)
			putWidget("b", 1)
			sub := recencySub(2)
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))

			putWidget("c", 1)
			_, changes := drain(cursor, sub)
			Expect(valueKeys(changes)).To(ContainElement("c"))
			Expect(tombstoneKeys(changes)).To(ContainElement("a"))
		})

		It("brings a re-touched row back to the head", func() {
			putWidget("a", 1)
			putWidget("b", 1)
			putWidget("c", 1)
			sub := recencySub(2)
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("b", "c"))

			putWidget("a", 2) // newest again; "b" is now the oldest of the three
			_, changes := drain(cursor, sub)
			Expect(valueKeys(changes)).To(ContainElement("a"))
			Expect(tombstoneKeys(changes)).To(ContainElement("b"))
		})
	})

	Describe("Validation", func() {
		It("rejects a limit above MaxWindowSize", func() {
			srv := newTestServer(server.WithLimits(server.Limits{MaxWindowSize: 5}))
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			c := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

			_, err := c.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId:   ws,
				Subscriptions: []*syncplanev1.Subscription{windowSub(6, nil)},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring(
				"window limit 6, over MaxWindowSize (5); raise it with server.WithLimits")))
		})

		It("rejects a zero limit", func() {
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId:   ws,
				Subscriptions: []*syncplanev1.Subscription{windowSub(0, nil)},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects by_recency combined with order_by", func() {
			sub := &syncplanev1.Subscription{
				Entity: "widget",
				Window: &syncplanev1.Window{ByRecency: true, OrderBy: countDesc(), Limit: 5},
			}
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, Subscriptions: []*syncplanev1.Subscription{sub},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects an order key on an unknown field", func() {
			sub := &syncplanev1.Subscription{
				Entity: "widget",
				Window: &syncplanev1.Window{
					OrderBy: []*syncplanev1.OrderKey{{
						Path: []string{"nope"}, Dir: syncplanev1.Direction_DIRECTION_ASC,
					}},
					Limit: 5,
				},
			}
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, Subscriptions: []*syncplanev1.Subscription{sub},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})
	})

	Describe("Scope changes", func() {
		It("delta-resyncs a widened limit instead of diffing membership", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			cursor, rows := bootstrap(windowSub(1, nil))
			Expect(valueKeys(rows)).To(ConsistOf("a"))

			// Widening needs rows the client never held: a membership diff cannot
			// produce them (they are at or below the cursor's seq and were never
			// sent), so the window has to sit in sub_hash and take the resync path.
			_, changes := drain(cursor, windowSub(3, nil))
			Expect(valueKeys(changes)).To(ConsistOf("b", "c"))
		})

		It("delta-resyncs a narrowed limit, tombstoning what left", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			cursor, rows := bootstrap(windowSub(3, nil))
			Expect(valueKeys(rows)).To(ConsistOf("a", "b", "c"))

			_, changes := drain(cursor, windowSub(1, nil))
			Expect(tombstoneKeys(changes)).To(ConsistOf("b", "c"))
		})

		It("resyncs a predicate change over a windowed entity", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 5)
			cursor, rows := bootstrap(windowSub(5, countGte(10)))
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))

			// Both windows are rebuilt per resync page: the old one (count >= 10)
			// and the new one (count >= 1).
			_, changes := drain(cursor, windowSub(5, countGte(1)))
			Expect(valueKeys(changes)).To(ConsistOf("c"))
		})

		It("resyncs from a windowed scope to an unwindowed one", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			cursor, rows := bootstrap(windowSub(1, nil))
			Expect(valueKeys(rows)).To(ConsistOf("a"))

			_, changes := drain(cursor, widgetSub(nil))
			Expect(valueKeys(changes)).To(ConsistOf("b", "c"))
		})

		It("keeps diffing correctly across the pull that follows a resync", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			cursor, _ := bootstrap(windowSub(1, nil))

			sub := windowSub(2, nil)
			cursor, _ = drain(cursor, sub)
			// The completion cursor must carry the NEW membership {a, b}; if it
			// carried the old {a}, this next pull would re-emit "b" as an
			// addition instead of settling.
			resp := pull(cursor, sub)
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
		})
	})

	Describe("Cursor compatibility", func() {
		It("serves a pre-window cursor unchanged", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			// A cursor issued for a plain predicate subscription: no window
			// anywhere in it. Adding the Window field must leave its sub_hash
			// identical, or every existing client would re-bootstrap on upgrade.
			sub := widgetSub(countGte(10))
			cursor, rows := bootstrap(sub)
			Expect(valueKeys(rows)).To(ConsistOf("a", "b"))

			resp := pull(cursor, sub)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetHasMore()).To(BeFalse())
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
		})

		It("carries the window key-set forward across a truncated page", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			sub := windowSub(3, nil)
			cursor, _ := bootstrap(sub)

			putWidget("a", 31)
			putWidget("b", 21)
			putWidget("c", 11)
			// limit 1 forces mid-page cursors, which must carry cur.window
			// through untouched — the diff only runs at a clean boundary.
			for i := 0; i < 10; i++ {
				resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
					WorkspaceId: ws, ClientId: "c1", Cursor: cursor,
					Subscriptions: []*syncplanev1.Subscription{sub}, Limit: 1,
				}))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Msg.GetReset_()).To(BeFalse())
				cursor = resp.Msg.GetNextCursor()
				if !resp.Msg.GetHasMore() {
					break
				}
			}
			// Settled: no spurious tombstones from a lost membership set.
			resp := pull(cursor, sub)
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
		})
	})

	Describe("With follow", func() {
		// A followed windowed subscription must seed from the WINDOW, not from
		// every predicate match — otherwise following expands the working set
		// right back past the bound the window exists to impose.
		It("seeds the closure from window members only", func() {
			push := func(muts ...*syncplanev1.Mutation) {
				GinkgoHelper()
				_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
				Expect(err).NotTo(HaveOccurred())
			}
			push(
				putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "one"}),
				putMutation("c1", 2, "project", "p2", &testdatav1.Project{Id: "p2", Name: "two"}),
				putMutation("c1", 3, "issue", "i1", &testdatav1.Issue{
					Id: "i1", Title: "b", Project: &testdatav1.Project{Id: "p1"},
				}),
				putMutation("c1", 4, "issue", "i2", &testdatav1.Issue{
					Id: "i2", Title: "a", Project: &testdatav1.Project{Id: "p2"},
				}),
			)

			// Top 1 issue by title descending ⇒ "i1" (title "b"), whose project
			// is p1. p2 is reachable only from the out-of-window issue.
			sub := &syncplanev1.Subscription{
				Entity: "issue",
				Window: &syncplanev1.Window{
					OrderBy: []*syncplanev1.OrderKey{{
						Path: []string{"title"}, Dir: syncplanev1.Direction_DIRECTION_DESC,
					}},
					Limit: 1,
				},
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			}
			_, rows := bootstrap(sub)
			keys := map[string][]string{}
			for _, r := range rows {
				if r.GetValue() != nil {
					keys[r.GetEntity()] = append(keys[r.GetEntity()], r.GetKey())
				}
			}
			Expect(keys["issue"]).To(ConsistOf("i1"))
			Expect(keys["project"]).To(ConsistOf("p1"))
		})
	})

	Describe("Cursor size", func() {
		It("records the window membership as a key-set on the cursor", func() {
			putWidget("a", 30)
			putWidget("b", 20)
			putWidget("c", 10)
			cursor, _ := bootstrap(windowSub(2, nil))

			var decoded syncplanecursorv1.Cursor
			Expect(proto.Unmarshal(cursor, &decoded)).To(Succeed())
			Expect(decoded.GetWindows()).To(HaveLen(1))
			state := decoded.GetWindows()[0]
			Expect(state.GetSubscription()).To(BeZero())
			Expect(state.GetKeys()).To(Equal([]string{"a", "b"}))
			// The tail's position rides with the keys: it is what lets the next
			// pull decide a changed row's membership by comparison instead of
			// resolving the window again.
			Expect(state.GetBoundary()).NotTo(BeEmpty())
		})
	})
})
