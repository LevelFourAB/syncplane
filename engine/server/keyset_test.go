package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// keySub names widget rows by key.
func keySub(keys ...string) *syncplanev1.Subscription {
	return &syncplanev1.Subscription{
		Entity: "widget",
		KeySet: &syncplanev1.KeySet{Keys: keys},
	}
}

// A key-set subscription pins rows by name: the client says which rows it
// holds, and the server never has to walk the entity to agree. Every spec here
// pins both what the client was told and what it cost the store.
var _ = Describe("Key-set subscriptions", func() {
	var (
		ctx     context.Context
		client  syncplanev1connect.SyncServiceClient
		counter *readCountingAdapter
		nextMut uint64
	)

	start := func(opts ...server.Option) {
		counter = &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
		srv := mustNewServer(counter, append([]server.Option{testEntities()}, opts...)...)
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
		start()
	})

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	putWidget := func(id string, count int32) {
		GinkgoHelper()
		nextMut++
		push(putMutation("c1", nextMut, "widget", id, &testdatav1.Widget{Id: id, Count: count}))
	}

	delWidget := func(id string) {
		GinkgoHelper()
		nextMut++
		push(&syncplanev1.Mutation{
			ClientId:   "c1",
			MutationId: nextMut,
			Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
				Entity: "widget", Key: id, Op: syncplanev1.CrudOp_CRUD_OP_DELETE,
			}},
		})
	}

	seedWidgets := func(n int) {
		GinkgoHelper()
		for i := 1; i <= n; i++ {
			putWidget(fmt.Sprintf("w%d", i), int32(i))
		}
	}

	pullRaw := func(cursor []byte, subs ...*syncplanev1.Subscription) (*syncplanev1.PullResponse, error) {
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c1", Cursor: cursor, Subscriptions: subs,
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	pull := func(cursor []byte, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := pullRaw(cursor, subs...)
		Expect(err).NotTo(HaveOccurred())
		return resp
	}

	bootstrapRaw := func(subs ...*syncplanev1.Subscription) (cursor []byte, rows []*syncplanev1.RowChange, err error) {
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws, ClientId: "c1", Subscriptions: subs,
		}))
		if err != nil {
			return nil, nil, err
		}
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				cursor = body.Begin.GetCursor()
			case *syncplanev1.BootstrapResponse_Chunk_:
				rows = append(rows, body.Chunk.GetRows()...)
			}
		}
		return cursor, rows, stream.Err()
	}

	bootstrap := func(subs ...*syncplanev1.Subscription) ([]byte, []*syncplanev1.RowChange) {
		GinkgoHelper()
		cursor, rows, err := bootstrapRaw(subs...)
		Expect(err).NotTo(HaveOccurred())
		Expect(cursor).NotTo(BeNil())
		return cursor, rows
	}

	// drain follows has_more to the end without a reset, and returns
	// everything the client was told to apply.
	drain := func(cursor []byte, subs ...*syncplanev1.Subscription) ([]byte, []*syncplanev1.RowChange) {
		GinkgoHelper()
		var all []*syncplanev1.RowChange
		for range 20 {
			resp := pull(cursor, subs...)
			Expect(resp.GetReset_()).To(BeFalse(), "unexpected reset")
			all = append(all, resp.GetPatch().GetChanges()...)
			cursor = resp.GetNextCursor()
			if !resp.GetHasMore() {
				return cursor, all
			}
		}
		Fail("pull never drained")
		return nil, nil
	}

	valueKeys := func(rows []*syncplanev1.RowChange, entity string) []string {
		var out []string
		for _, rc := range rows {
			if rc.GetEntity() == entity && !rc.GetTombstone() {
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

	Describe("bootstrap", func() {
		It("ships the named rows and nothing else, without walking the entity", func() {
			seedWidgets(6)
			counter.reset()

			// w9 names no row: a key the store does not hold is simply absent.
			_, rows := bootstrap(keySub("w4", "w2", "w9"))
			Expect(valueKeys(rows, "widget")).To(Equal([]string{"w2", "w4"}), "key order, like the scan")
			Expect(counter.count("scanafter:widget")).To(BeZero(), "a key set never walks the entity")
			Expect(counter.count("batchkeys")).To(Equal(3), "one read per named key")
		})

		It("ships no rows for an empty set but keeps the entity subscribed", func() {
			seedWidgets(3)
			cursor, rows := bootstrap(&syncplanev1.Subscription{Entity: "widget", KeySet: &syncplanev1.KeySet{}})
			Expect(rows).To(BeEmpty())

			// Naming a row afterwards is a scope change, not a re-bootstrap.
			_, changes := drain(cursor, keySub("w1"))
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w1"}))
		})

		It("withholds a named row the read filter hides", func() {
			start(server.WithPermissions(newEpochPermissions("secret")))
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret plans"}),
			)
			_, rows := bootstrap(&syncplanev1.Subscription{
				Entity: "todo", KeySet: &syncplanev1.KeySet{Keys: []string{"t1", "t2"}},
			})
			Expect(valueKeys(rows, "todo")).To(Equal([]string{"t1"}), "naming a row does not grant it")
		})
	})

	Describe("pull", func() {
		It("delivers a change to a named row and says nothing about the rest", func() {
			seedWidgets(3)
			cursor, _ := bootstrap(keySub("w2"))

			putWidget("w2", 20)
			putWidget("w3", 30)
			resp := pull(cursor, keySub("w2"))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(valueKeys(resp.GetPatch().GetChanges(), "widget")).To(Equal([]string{"w2"}))
			// w3 changed too, but the client never held it: no tombstone, so
			// the pull discloses neither its key nor that it moved.
			Expect(tombstoneKeys(resp.GetPatch().GetChanges())).To(BeEmpty())
		})

		It("delivers the tombstone of a named row that was deleted", func() {
			seedWidgets(2)
			cursor, _ := bootstrap(keySub("w1", "w2"))

			delWidget("w1")
			resp := pull(cursor, keySub("w1", "w2"))
			Expect(tombstoneKeys(resp.GetPatch().GetChanges())).To(Equal([]string{"w1"}))
		})

		It("treats a reordered or repeated key list as the same scope", func() {
			seedWidgets(2)
			cursor, _ := bootstrap(keySub("w1", "w2"))
			counter.reset()

			resp := pull(cursor, keySub("w2", "w1", "w1"))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetHasMore()).To(BeFalse(), "no resync was started")
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
			Expect(counter.count("batchkeys")).To(BeZero(), "nothing was re-read")
		})

		It("follows references from the named rows only", func() {
			push(
				putMutation("c1", 1, "user", "u1", &testdatav1.User{Id: "u1", Name: "ann"}),
				putMutation("c1", 2, "user", "u2", &testdatav1.User{Id: "u2", Name: "bob"}),
				putMutation("c1", 3, "issue", "i1", &testdatav1.Issue{Id: "i1", Assignee: &testdatav1.User{Id: "u1"}}),
				putMutation("c1", 4, "issue", "i2", &testdatav1.Issue{Id: "i2", Assignee: &testdatav1.User{Id: "u2"}}),
			)
			counter.reset()
			resp := pull(nil, &syncplanev1.Subscription{
				Entity: "issue",
				KeySet: &syncplanev1.KeySet{Keys: []string{"i1"}},
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"assignee"}}}},
			})
			Expect(valueKeys(resp.GetPatch().GetChanges(), "issue")).To(Equal([]string{"i1"}))
			Expect(valueKeys(resp.GetPatch().GetChanges(), "user")).To(Equal([]string{"u1"}))
			Expect(counter.count("scanafter:issue")).To(BeZero(), "the seeds are the named rows")
		})
	})

	Describe("scope change", func() {
		It("resyncs the difference between the two sets without walking the entity", func() {
			seedWidgets(4)
			cursor, rows := bootstrap(keySub("w1", "w2"))
			Expect(valueKeys(rows, "widget")).To(Equal([]string{"w1", "w2"}))
			counter.reset()

			_, changes := drain(cursor, keySub("w2", "w3"))
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w3"}), "the row entering the set arrives")
			Expect(tombstoneKeys(changes)).To(Equal([]string{"w1"}), "the row leaving the set drops")
			Expect(counter.count("scanafter:widget")).To(BeZero())
			// The union of both sets, read once each; w4 is in neither and is
			// never touched.
			Expect(counter.count("batchkeys")).To(Equal(3))
		})

		It("reads nothing of an entity whose scope did not change", func() {
			// A hold names one more row of one entity. Every other entity in
			// the set keeps the scope it had, so the resync must not walk any
			// of them to learn that nothing of theirs moved.
			for i := 1; i <= 5; i++ {
				nextMut++
				id := fmt.Sprintf("t%d", i)
				push(putMutation("c1", nextMut, "todo", id, &todov1.Todo{Id: id, Title: id}))
			}
			seedWidgets(3)
			whole := &syncplanev1.Subscription{Entity: "todo"}
			cursor, rows := bootstrap(whole, keySub("w1"))
			Expect(valueKeys(rows, "todo")).To(HaveLen(5))
			counter.reset()

			_, changes := drain(cursor, whole, keySub("w1", "w2"))
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w2"}))
			Expect(tombstoneKeys(changes)).To(BeEmpty())
			Expect(counter.count("scanafter:todo")).To(BeZero(), "an unchanged whole-entity scope is not walked")
			Expect(counter.count("scanafter:widget")).To(BeZero())
			Expect(counter.count("batchkeys")).To(Equal(2), "the keys named on either side, once each")
		})

		It("resyncs from a predicate to a key set and back", func() {
			seedWidgets(3)
			countEq := func(n int32) *syncplanev1.Condition {
				return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path:  []string{"count"},
					Op:    syncplanev1.CmpOp_CMP_OP_EQ,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: int64(n)}},
				}}}
			}
			predSub := &syncplanev1.Subscription{Entity: "widget", Predicate: countEq(1)}
			cursor, rows := bootstrap(predSub)
			Expect(valueKeys(rows, "widget")).To(Equal([]string{"w1"}))

			cursor, changes := drain(cursor, keySub("w2"))
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w2"}))
			Expect(tombstoneKeys(changes)).To(Equal([]string{"w1"}))

			_, changes = drain(cursor, predSub)
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w1"}))
			Expect(tombstoneKeys(changes)).To(Equal([]string{"w2"}))
		})
	})

	Describe("validation", func() {
		invalid := func(sub *syncplanev1.Subscription) {
			GinkgoHelper()
			_, err := pullRaw(nil, sub)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument), "pull: %v", err)
			_, _, err = bootstrapRaw(sub)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument), "bootstrap: %v", err)
		}

		It("refuses a key set combined with a predicate", func() {
			sub := keySub("w1")
			sub.Predicate = &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path: []string{"count"}, Op: syncplanev1.CmpOp_CMP_OP_EQ,
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: 1}},
			}}}
			invalid(sub)
		})

		It("refuses a key set combined with a window", func() {
			sub := keySub("w1")
			sub.Window = &syncplanev1.Window{ByRecency: true, Limit: 1}
			invalid(sub)
		})

		It("refuses a key that no row could have", func() {
			invalid(keySub("w1", ""))
			invalid(keySub("w1\x1fw2"))
		})

		It("refuses a set above MaxSubscriptionKeys", func() {
			start(server.WithLimits(server.Limits{MaxSubscriptionKeys: 2}))
			seedWidgets(3)
			invalid(keySub("w1", "w2", "w3"))
			// Duplicates still count: the limit bounds the request, and the
			// request is what the client keeps sending.
			invalid(keySub("w1", "w1", "w1"))
			_, err := pullRaw(nil, keySub("w1", "w2"))
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
