package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

func countGte(n float64) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
		Path:  []string{"count"},
		Op:    syncplanev1.CmpOp_CMP_OP_GTE,
		Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: n}},
	}}}
}

func widgetSub(pred *syncplanev1.Condition) *syncplanev1.Subscription {
	return &syncplanev1.Subscription{Entity: "widget", Predicate: pred}
}

var _ = Describe("Subscriptions and revocation", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
		perms  *exampleapp.Permissions
	)

	start := func(srv *server.Server) {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	pull := func(cursor []byte, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Cursor:        cursor,
			Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	bootstrap := func(subs ...*syncplanev1.Subscription) (cursor []byte, rows []*syncplanev1.RowChange) {
		GinkgoHelper()
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Subscriptions: subs,
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

	widgetKeys := func(rows []*syncplanev1.RowChange) []string {
		var out []string
		for _, rc := range rows {
			if rc.GetEntity() == "widget" && !rc.GetTombstone() {
				out = append(out, rc.GetKey())
			}
		}
		return out
	}

	tombstoneKeys := func(rows []*syncplanev1.RowChange) []string {
		var out []string
		for _, rc := range rows {
			if rc.GetTombstone() {
				out = append(out, rc.GetKey())
			}
		}
		return out
	}

	putWidget := func(id uint64, key string, count int32) *syncplanev1.Mutation {
		return putMutation("c1", id, "widget", key, &testdatav1.Widget{Id: key, Count: count})
	}

	BeforeEach(func() {
		ctx = context.Background()
		perms = exampleapp.NewPermissions()
		start(newTestServer(server.WithPermissions(perms)))
	})

	Describe("Bootstrap", func() {
		It("ships only rows matching the predicate and skips unsubscribed entities", func() {
			push(
				putWidget(1, "w-low", 5),
				putWidget(2, "w-high", 50),
				putMutation("c1", 3, "todo", "t1", &todov1.Todo{Id: "t1", Title: "todo row"}),
			)

			_, rows := bootstrap(widgetSub(countGte(10)))
			Expect(widgetKeys(rows)).To(Equal([]string{"w-high"}))
			for _, rc := range rows {
				Expect(rc.GetEntity()).To(Equal("widget"), "unsubscribed entities must not bootstrap")
			}
		})

		It("treats a subscription without a predicate as the whole entity", func() {
			push(putWidget(1, "w1", 1), putWidget(2, "w2", 100))
			_, rows := bootstrap(widgetSub(nil))
			Expect(widgetKeys(rows)).To(ConsistOf("w1", "w2"))
		})

		It("unions multiple subscriptions for the same entity", func() {
			push(putWidget(1, "w-low", 1), putWidget(2, "w-mid", 50), putWidget(3, "w-high", 500))
			_, rows := bootstrap(widgetSub(countGte(100)), widgetSub(&syncplanev1.Condition{
				Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"count"},
					Op:    syncplanev1.CmpOp_CMP_OP_LT,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: 10}},
				}},
			}))
			Expect(widgetKeys(rows)).To(ConsistOf("w-low", "w-high"))
		})

		It("rejects unknown entities", func() {
			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId:   ws,
				Subscriptions: []*syncplanev1.Subscription{{Entity: "nope"}},
			}))
			if err == nil {
				for stream.Receive() {
				}
				err = stream.Err()
			}
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("rejects bad predicate paths", func() {
			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
				Subscriptions: []*syncplanev1.Subscription{widgetSub(&syncplanev1.Condition{
					Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
						Path:  []string{"no_such_field"},
						Op:    syncplanev1.CmpOp_CMP_OP_EQ,
						Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "x"}},
					}},
				})},
			}))
			if err == nil {
				for stream.Receive() {
				}
				err = stream.Err()
			}
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})
	})

	Describe("Pull with subscriptions", func() {
		It("emits rows crossing INTO scope as values and OUT of scope as tombstones", func() {
			push(putWidget(1, "w1", 5))
			cursor, rows := bootstrap(widgetSub(countGte(10)))
			Expect(widgetKeys(rows)).To(BeEmpty())

			// Edit the row into scope.
			push(putWidget(2, "w1", 20))
			resp := pull(cursor, widgetSub(countGte(10)))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(widgetKeys(resp.GetPatch().GetChanges())).To(Equal([]string{"w1"}))
			cursor = resp.GetNextCursor()

			// Edit it back out of scope: the client must drop it.
			push(putWidget(3, "w1", 3))
			resp = pull(cursor, widgetSub(countGte(10)))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(tombstoneKeys(resp.GetPatch().GetChanges())).To(Equal([]string{"w1"}))
		})

		It("skips changes of unsubscribed entities entirely, including tombstones", func() {
			cursor, _ := bootstrap(widgetSub(nil))
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}),
				deleteMutation("c1", 2, "todo", "t1"),
				putWidget(3, "w1", 1),
			)
			resp := pull(cursor, widgetSub(nil))
			Expect(resp.GetReset_()).To(BeFalse())
			changes := resp.GetPatch().GetChanges()
			Expect(changes).To(HaveLen(1))
			Expect(changes[0].GetEntity()).To(Equal("widget"))
		})

		It("delta-resyncs instead of resetting when the subscription set changes", func() {
			// Full coverage of the delta lives in resync_test.go; this pins
			// that a scope change no longer answers reset=true.
			cursor, _ := bootstrap(widgetSub(countGte(10)))
			resp := pull(cursor, widgetSub(countGte(99)))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetHasMore()).To(BeTrue(), "a resync always needs a follow-up pull")
		})

		It("delta-resyncs into full sync when subscriptions disappear", func() {
			cursor, _ := bootstrap(widgetSub(countGte(10)))
			resp := pull(cursor)
			Expect(resp.GetReset_()).To(BeFalse())
		})

		It("does not reset when the same subscriptions arrive in a different order", func() {
			subA := widgetSub(countGte(10))
			subB := &syncplanev1.Subscription{Entity: "todo"}
			cursor, _ := bootstrap(subA, subB)
			resp := pull(cursor, subB, subA)
			Expect(resp.GetReset_()).To(BeFalse())
		})
	})

	// The snapshot pages read after Begin, so a row can be created between
	// the two. Whatever the pages saw, the snapshot plus the pull from its
	// Begin cursor must hold what a fresh bootstrap holds.
	Describe("Bootstrap followed by its first pull", func() {
		var hooked *hookedReadStore

		BeforeEach(func() {
			hooked = &hookedReadStore{Adapter: newTestStore()}
			start(mustNewServer(hooked, testEntities(), server.WithPermissions(perms), server.WithChunkSize(1)))
		})

		// latest holds the widgets the client holds after syncedKeys.
		var latest map[string]*testdatav1.Widget

		// syncedKeys bootstraps with created run right after Begin, runs
		// between before the first pull, and returns the keys the client then
		// holds.
		syncedKeys := func(created, between func(), subs ...*syncplanev1.Subscription) []string {
			GinkgoHelper()
			hooked.mu.Lock()
			hooked.afterBegin = created
			hooked.mu.Unlock()
			cursor, rows := bootstrap(subs...)
			hooked.mu.Lock()
			Expect(hooked.afterBegin).To(BeNil(), "nothing was created during the snapshot")
			hooked.mu.Unlock()
			between()
			resp := pull(cursor, subs...)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetHasMore()).To(BeFalse())
			held := map[string]bool{}
			latest = map[string]*testdatav1.Widget{}
			for _, rc := range append(rows, resp.GetPatch().GetChanges()...) {
				k := rc.GetEntity() + "/" + rc.GetKey()
				held[k] = !rc.GetTombstone()
				if rc.GetEntity() == "widget" && !rc.GetTombstone() {
					w := &testdatav1.Widget{}
					Expect(proto.Unmarshal(rc.GetValue(), w)).To(Succeed())
					latest[k] = w
				}
			}
			var out []string
			for k, live := range held {
				if live {
					out = append(out, k)
				}
			}
			sort.Strings(out)
			return out
		}

		It("removes a row created after Begin that then left the predicate", func() {
			push(putWidget(1, "a", 50))
			keys := syncedKeys(
				func() { push(putWidget(2, "x", 50)) },
				func() { push(putWidget(3, "x", 5)) },
				widgetSub(countGte(10)))
			Expect(keys).To(Equal([]string{"widget/a"}))
		})

		It("removes a row created after Begin that the read filter then hides", func() {
			push(putMutation("c1", 1, "todo", "a", &todov1.Todo{Id: "a", Title: "public"}))
			keys := syncedKeys(
				func() { push(putMutation("c1", 2, "todo", "x", &todov1.Todo{Id: "x", Title: "public"})) },
				func() { push(putMutation("c1", 3, "todo", "x", &todov1.Todo{Id: "x", Title: "now secret"})) })
			Expect(keys).To(Equal([]string{"todo/a"}))
		})

		It("delivers the latest value of a row created after Begin", func() {
			push(putWidget(1, "a", 50))
			keys := syncedKeys(
				func() { push(putWidget(2, "x", 50)) },
				func() { push(putWidget(3, "x", 60)) },
				widgetSub(countGte(10)))
			Expect(keys).To(Equal([]string{"widget/a", "widget/x"}))
			Expect(latest["widget/x"].GetCount()).To(BeEquivalentTo(60))
		})
	})

	Describe("Revocation", func() {
		It("tombstones a row whose edit makes the read filter deny it", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}))
			cursor, rows := bootstrap()
			Expect(rows).To(HaveLen(1))

			// The edit flips the row into the filtered set; the version bump
			// makes the next pull re-evaluate it.
			push(putMutation("c1", 2, "todo", "t1", &todov1.Todo{Id: "t1", Title: "now secret"}))
			resp := pull(cursor)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(tombstoneKeys(resp.GetPatch().GetChanges())).To(Equal([]string{"t1"}))
		})

		It("resets every cursor when the filter epoch bumps and old epochs are not evaluable", func() {
			// exampleapp's policy does not implement EpochReadFilters, so an
			// epoch bump keeps the pre-delta behavior: full reset. The delta
			// path for epoch changes is covered in resync_test.go.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}))
			cursor, _ := bootstrap()
			Expect(pull(cursor).GetReset_()).To(BeFalse())

			perms.BumpEpoch(ws)
			Expect(pull(cursor).GetReset_()).To(BeTrue())

			// Re-bootstrap issues a cursor under the new epoch.
			cursor, _ = bootstrap()
			Expect(pull(cursor).GetReset_()).To(BeFalse())
		})
	})

	Describe("Cursor compatibility", func() {
		It("accepts legacy bare-seq cursor bytes", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			first := pull(nil)
			Expect(first.GetPatch().GetChanges()).To(HaveLen(1))

			// A pre-upgrade cursor carries only seq; sub_hash/filter_epoch
			// decode as 0, which match a full-sync epoch-0 client.
			legacy, err := proto.Marshal(&syncplanecursorv1.Cursor{Seq: 1})
			Expect(err).NotTo(HaveOccurred())
			resp := pull(legacy)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
		})
	})
})
