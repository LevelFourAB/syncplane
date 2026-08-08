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

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// epochPermissions hides todos whose title contains the CURRENT epoch's
// hidden word. Past epochs stay evaluable via ReadFilterAt, which opts the
// server into delta resync for permission changes; forget() drops the
// history to exercise the reset fallback.
type epochPermissions struct {
	server.AllowAll
	words     []string // index = epoch
	forgotten bool
}

func newEpochPermissions(word string) *epochPermissions {
	return &epochPermissions{words: []string{word}}
}

func (p *epochPermissions) ReadFilter(_ context.Context, _ server.Principal, _ string, entity string) (server.RowFilter, error) {
	return p.filterAt(entity, uint64(len(p.words)-1))
}

func (p *epochPermissions) FilterEpoch(context.Context, server.Principal, string) (uint64, error) {
	return uint64(len(p.words) - 1), nil
}

func (p *epochPermissions) ReadFilterAt(_ context.Context, _ server.Principal, _ string, entity string, epoch uint64) (server.RowFilter, error) {
	if p.forgotten || epoch >= uint64(len(p.words)) {
		return nil, server.ErrEpochUnavailable
	}
	return p.filterAt(entity, epoch)
}

func (p *epochPermissions) filterAt(entity string, epoch uint64) (server.RowFilter, error) {
	if entity != "todo" {
		return nil, nil
	}
	word := p.words[epoch]
	return func(_ string, row proto.Message) bool {
		todo, ok := row.(*todov1.Todo)
		return !ok || !strings.Contains(todo.GetTitle(), word)
	}, nil
}

// setHiddenWord installs a new filter and bumps the epoch, like a membership
// change would in a real app.
func (p *epochPermissions) setHiddenWord(word string) { p.words = append(p.words, word) }

var _ = Describe("Delta resync", func() {
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

	pull := func(cursor []byte, limit uint32, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Cursor:        cursor,
			Limit:         limit,
			Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// drain follows has_more to the end, asserting the server never resets,
	// and returns everything the client was told to apply.
	drain := func(cursor []byte, subs ...*syncplanev1.Subscription) (changes []*syncplanev1.RowChange, drops []string, next []byte) {
		GinkgoHelper()
		for range 100 {
			resp := pull(cursor, 0, subs...)
			Expect(resp.GetReset_()).To(BeFalse(), "delta resync must not reset")
			changes = append(changes, resp.GetPatch().GetChanges()...)
			drops = append(drops, resp.GetPatch().GetDropEntities()...)
			cursor = resp.GetNextCursor()
			if !resp.GetHasMore() {
				return changes, drops, cursor
			}
		}
		Fail("pull never drained")
		return nil, nil, nil
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

	putWidget := func(id uint64, key string, count int32) *syncplanev1.Mutation {
		return putMutation("c1", id, "widget", key, &testdatav1.Widget{Id: key, Count: count})
	}

	putTodo := func(id uint64, key, title string) *syncplanev1.Mutation {
		return putMutation("c1", id, "todo", key, &todov1.Todo{Id: key, Title: title})
	}

	Describe("subscription changes", func() {
		BeforeEach(func() {
			ctx = context.Background()
			start(newTestServer())
		})

		It("widening a predicate ships only the newly visible rows", func() {
			push(putWidget(1, "w-low", 5), putWidget(2, "w-high", 50))
			cursor, rows := bootstrap(widgetSub(countGte(10)))
			Expect(valueKeys(rows, "widget")).To(Equal([]string{"w-high"}))

			changes, drops, _ := drain(cursor, widgetSub(countGte(1)))
			Expect(drops).To(BeEmpty())
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w-low"}), "rows already held must not be resent")
			Expect(tombstoneKeys(changes)).To(BeEmpty())
		})

		It("narrowing a predicate tombstones the rows that left scope", func() {
			push(putWidget(1, "w-low", 5), putWidget(2, "w-high", 50))
			cursor, _ := bootstrap(widgetSub(nil))

			changes, drops, _ := drain(cursor, widgetSub(countGte(10)))
			Expect(drops).To(BeEmpty())
			Expect(tombstoneKeys(changes)).To(Equal([]string{"w-low"}))
			Expect(valueKeys(changes, "widget")).To(BeEmpty(), "surviving rows must not be resent")
		})

		It("an entity leaving the set drops wholesale instead of tombstoning per row", func() {
			push(putWidget(1, "w1", 5), putWidget(2, "w2", 7), putTodo(3, "t1", "keep"))
			todoSub := &syncplanev1.Subscription{Entity: "todo"}
			cursor, _ := bootstrap(widgetSub(nil), todoSub)

			changes, drops, _ := drain(cursor, todoSub)
			Expect(drops).To(Equal([]string{"widget"}))
			Expect(changes).To(BeEmpty())
		})

		It("an entity joining the set arrives as values", func() {
			push(putWidget(1, "w1", 5), putTodo(2, "t1", "keep"))
			todoSub := &syncplanev1.Subscription{Entity: "todo"}
			cursor, _ := bootstrap(todoSub)

			changes, drops, _ := drain(cursor, todoSub, widgetSub(nil))
			Expect(drops).To(BeEmpty())
			Expect(valueKeys(changes, "widget")).To(Equal([]string{"w1"}))
			Expect(valueKeys(changes, "todo")).To(BeEmpty(), "rows already held must not be resent")
		})

		It("narrowing full sync to a subset drops the unsubscribed entities", func() {
			push(putWidget(1, "w1", 5), putTodo(2, "t1", "keep"))
			cursor, _ := bootstrap()

			changes, drops, _ := drain(cursor, widgetSub(nil))
			Expect(drops).To(Equal([]string{"activity", "comment", "historied", "historied_all", "issue", "ledger", "pair", "project", "todo", "user"}))
			Expect(changes).To(BeEmpty(), "widgets were already held under full sync")
		})

		It("widening a subset to full sync ships the missing entities", func() {
			push(putWidget(1, "w1", 5), putTodo(2, "t1", "keep"))
			cursor, _ := bootstrap(widgetSub(nil))

			changes, drops, _ := drain(cursor)
			Expect(drops).To(BeEmpty())
			Expect(valueKeys(changes, "todo")).To(Equal([]string{"t1"}))
			Expect(valueKeys(changes, "widget")).To(BeEmpty())
		})

		It("catches up on rows written after the cursor under the new scope", func() {
			push(putWidget(1, "w-low", 5))
			cursor, _ := bootstrap(widgetSub(countGte(10)))
			// Written after the cursor: resync skips it (version > cursor
			// seq); the catch-up pull that follows must deliver it.
			push(putWidget(2, "w-mid", 20))

			changes, _, _ := drain(cursor, widgetSub(countGte(1)))
			Expect(valueKeys(changes, "widget")).To(ConsistOf("w-low", "w-mid"))
		})

		It("paginates the resync under the pull limit", func() {
			muts := []*syncplanev1.Mutation{}
			for i := uint64(1); i <= 5; i++ {
				muts = append(muts, putWidget(i, string(rune('a'+i-1))+"-w", int32(i)))
			}
			push(muts...)
			cursor, rows := bootstrap(widgetSub(countGte(10)))
			Expect(rows).To(BeEmpty())

			var values []string
			pages := 0
			resp := pull(cursor, 2, widgetSub(countGte(1)))
			for {
				Expect(resp.GetReset_()).To(BeFalse())
				Expect(len(resp.GetPatch().GetChanges())).To(BeNumerically("<=", 2))
				values = append(values, valueKeys(resp.GetPatch().GetChanges(), "widget")...)
				pages++
				if !resp.GetHasMore() {
					break
				}
				Expect(pages).To(BeNumerically("<", 100), "pull never drained")
				resp = pull(resp.GetNextCursor(), 2, widgetSub(countGte(1)))
			}
			Expect(values).To(ConsistOf("a-w", "b-w", "c-w", "d-w", "e-w"))
			Expect(pages).To(BeNumerically(">", 1), "limit 2 must force multiple pages")
		})

		It("converges when rows change while the resync is paging", func() {
			push(putWidget(1, "w1", 5), putWidget(2, "w2", 5), putWidget(3, "w3", 5))
			cursor, rows := bootstrap(widgetSub(countGte(10)))
			Expect(rows).To(BeEmpty())

			// Simulate the client's confirmed state.
			state := map[string]bool{}
			apply := func(resp *syncplanev1.PullResponse) {
				for _, entity := range resp.GetPatch().GetDropEntities() {
					for k := range state {
						if strings.HasPrefix(k, entity+"/") {
							delete(state, k)
						}
					}
				}
				for _, rc := range resp.GetPatch().GetChanges() {
					k := rc.GetEntity() + "/" + rc.GetKey()
					if rc.GetTombstone() {
						delete(state, k)
					} else {
						state[k] = true
					}
				}
			}

			wide := widgetSub(countGte(1))
			resp := pull(cursor, 1, wide) // first resync page: w1 only
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetHasMore()).To(BeTrue())
			apply(resp)

			// Mid-resync edits: w2 leaves the NEW scope, w3 changes within it.
			push(putWidget(4, "w2", 0), putWidget(5, "w3", 50))

			cursor = resp.GetNextCursor()
			for range 100 {
				resp = pull(cursor, 1, wide)
				Expect(resp.GetReset_()).To(BeFalse())
				apply(resp)
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					break
				}
			}
			Expect(resp.GetHasMore()).To(BeFalse(), "pull never drained")
			Expect(state).To(Equal(map[string]bool{"widget/w1": true, "widget/w3": true}))
		})

		It("resets when the scope changes again mid-resync", func() {
			push(putWidget(1, "w1", 1), putWidget(2, "w2", 2), putWidget(3, "w3", 3))
			cursor, _ := bootstrap(widgetSub(countGte(10)))

			resp := pull(cursor, 1, widgetSub(countGte(1)))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetHasMore()).To(BeTrue())

			// The client's rows are now a mixture of both scopes; a THIRD
			// subscription set has no single diff and must reset.
			resp = pull(resp.GetNextCursor(), 1, widgetSub(countGte(2)))
			Expect(resp.GetReset_()).To(BeTrue())
		})

		It("resets for legacy cursors that lack the embedded subscription set", func() {
			push(putWidget(1, "w1", 50))
			cursor, _ := bootstrap(widgetSub(countGte(10)))

			var c syncplanecursorv1.Cursor
			Expect(proto.Unmarshal(cursor, &c)).To(Succeed())
			c.Subscriptions = nil // what a pre-upgrade server would have issued
			legacy, err := proto.Marshal(&c)
			Expect(err).NotTo(HaveOccurred())

			resp := pull(legacy, 0, widgetSub(countGte(1)))
			Expect(resp.GetReset_()).To(BeTrue())
		})
	})

	Describe("filter epoch changes", func() {
		var perms *epochPermissions

		BeforeEach(func() {
			ctx = context.Background()
			perms = newEpochPermissions("alpha")
			start(newTestServer(server.WithPermissions(perms)))
		})

		It("diffs old and new visibility instead of resetting", func() {
			push(
				putTodo(1, "t-alpha", "alpha thing"),
				putTodo(2, "t-beta", "beta thing"),
				putTodo(3, "t-plain", "plain thing"),
			)
			cursor, rows := bootstrap()
			Expect(valueKeys(rows, "todo")).To(ConsistOf("t-beta", "t-plain"))

			perms.setHiddenWord("beta")
			changes, drops, next := drain(cursor)
			Expect(drops).To(BeEmpty())
			Expect(tombstoneKeys(changes)).To(Equal([]string{"t-beta"}), "newly hidden rows arrive as removals")
			Expect(valueKeys(changes, "todo")).To(Equal([]string{"t-alpha"}), "newly visible rows arrive as values")

			// The settled cursor lives under the new epoch: pulls are normal
			// again and stay empty until something changes.
			resp := pull(next, 0)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
			Expect(resp.GetHasMore()).To(BeFalse())
		})

		It("handles a subscription and epoch change in the same pull", func() {
			push(
				putTodo(1, "t-alpha", "alpha thing"),
				putTodo(2, "t-plain", "plain thing"),
				putWidget(3, "w1", 5),
			)
			cursor, _ := bootstrap()

			perms.setHiddenWord("plain")
			changes, drops, _ := drain(cursor, &syncplanev1.Subscription{Entity: "todo"})
			Expect(drops).To(Equal([]string{"activity", "comment", "historied", "historied_all", "issue", "ledger", "pair", "project", "user", "widget"}))
			Expect(tombstoneKeys(changes)).To(Equal([]string{"t-plain"}))
			Expect(valueKeys(changes, "todo")).To(Equal([]string{"t-alpha"}))
		})

		It("resets when the old epoch is no longer evaluable", func() {
			push(putTodo(1, "t1", "plain thing"))
			cursor, _ := bootstrap()

			perms.setHiddenWord("beta")
			perms.forgotten = true
			resp := pull(cursor, 0)
			Expect(resp.GetReset_()).To(BeTrue())
		})
	})
})
