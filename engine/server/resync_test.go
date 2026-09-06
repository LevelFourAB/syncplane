package server_test

import (
	"context"
	"fmt"
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

	// applyPatch plays one patch onto a simulated client state keyed by
	// "entity/key", in the order the client applies it: entity drops first,
	// then row changes.
	applyPatch := func(state map[string]bool, patch *syncplanev1.Patch) {
		for _, entity := range patch.GetDropEntities() {
			for k := range state {
				if strings.HasPrefix(k, entity+"/") {
					delete(state, k)
				}
			}
		}
		for _, rc := range patch.GetChanges() {
			k := rc.GetEntity() + "/" + rc.GetKey()
			if rc.GetTombstone() {
				delete(state, k)
			} else {
				state[k] = true
			}
		}
	}

	// heldAfter simulates a client that bootstrapped rows and then applied
	// every patch drain returned, and reports the rows it holds.
	heldAfter := func(rows []*syncplanev1.RowChange, changes []*syncplanev1.RowChange, drops []string) map[string]bool {
		state := map[string]bool{}
		applyPatch(state, &syncplanev1.Patch{Changes: rows})
		applyPatch(state, &syncplanev1.Patch{DropEntities: drops, Changes: changes})
		return state
	}

	// followProject subscribes to all issues and follows issue→project, so
	// projects arrive through the closure whether or not they have a base
	// scope of their own.
	followProject := func() *syncplanev1.Subscription {
		return &syncplanev1.Subscription{
			Entity: "issue",
			Follow: &syncplanev1.FollowSpec{
				Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				Depth: 1,
			},
		}
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

		It("keeps a row that leaves base scope but stays follow-reachable", func() {
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "project", "B", &testdatav1.Project{Id: "B", Name: "beta"}),
				putMutation("c1", 3, "issue", "i1", &testdatav1.Issue{Id: "i1", Title: "one", Project: &testdatav1.Project{Id: "A"}}),
			)
			// project is in base scope AND reachable through the follow; the
			// closure the cursor carries already names project/A.
			cursor, rows := bootstrap(&syncplanev1.Subscription{Entity: "project"}, followProject())
			Expect(valueKeys(rows, "project")).To(Equal([]string{"A", "B"}))

			// project leaves base scope: A stays held through i1, B must go.
			changes, drops, next := drain(cursor, followProject())
			Expect(drops).To(BeEmpty())
			Expect(tombstoneKeys(changes)).To(Equal([]string{"B"}), "a row the closure holds must not be tombstoned")
			Expect(heldAfter(rows, changes, drops)).To(Equal(map[string]bool{"issue/i1": true, "project/A": true}))

			// An idle pull has nothing left to repair.
			resp := pull(next, 0, followProject())
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
		})

		It("delivers the closure of a follow spec the scope change introduced", func() {
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "project", "B", &testdatav1.Project{Id: "B", Name: "beta"}),
				putMutation("c1", 3, "issue", "i1", &testdatav1.Issue{Id: "i1", Title: "one", Project: &testdatav1.Project{Id: "A"}}),
			)
			// No follow yet, so the cursor carries no closure at all.
			cursor, rows := bootstrap(&syncplanev1.Subscription{Entity: "project"})
			Expect(valueKeys(rows, "project")).To(Equal([]string{"A", "B"}))

			// The follow is new: its closure must be walked from the new seeds,
			// not maintained from the (empty) set the cursor carried.
			changes, drops, next := drain(cursor, followProject())
			Expect(drops).To(BeEmpty())
			Expect(heldAfter(rows, changes, drops)).To(Equal(map[string]bool{"issue/i1": true, "project/A": true}))

			resp := pull(next, 0, followProject())
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
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
			Expect(drops).To(Equal([]string{"activity", "comment", "historied", "historied_all", "issue", "keyed_text", "ledger", "pair", "project", "task", "todo", "user"}))
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
			apply := func(resp *syncplanev1.PullResponse) { applyPatch(state, resp.GetPatch()) }

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
			Expect(drops).To(Equal([]string{"activity", "comment", "historied", "historied_all", "issue", "keyed_text", "ledger", "pair", "project", "task", "user", "widget"}))
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

	Describe("scan budget", func() {
		const liveRows = 1000

		// seedWidgets fills the workspace with liveRows widgets whose counts
		// run 1..liveRows, so a predicate on count selects a known suffix.
		seedWidgets := func() {
			GinkgoHelper()
			var batch []*syncplanev1.Mutation
			for i := 1; i <= liveRows; i++ {
				batch = append(batch, putWidget(uint64(i), fmt.Sprintf("w%04d", i), int32(i)))
				if len(batch) == 100 {
					push(batch...)
					batch = nil
				}
			}
			if len(batch) > 0 {
				push(batch...)
			}
		}

		// widen switches from a predicate matching nothing to one matching the
		// top five rows and drains the resync, returning how many pulls that
		// took and which widgets arrived.
		widen := func() (pulls int, keys []string) {
			GinkgoHelper()
			cursor, rows := bootstrap(widgetSub(countGte(liveRows + 1)))
			Expect(rows).To(BeEmpty())

			wide := widgetSub(countGte(liveRows - 4))
			for range 200 {
				resp := pull(cursor, 0, wide)
				pulls++
				Expect(resp.GetReset_()).To(BeFalse())
				keys = append(keys, valueKeys(resp.GetPatch().GetChanges(), "widget")...)
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					return pulls, keys
				}
			}
			Fail("pull never drained")
			return 0, nil
		}

		wantKeys := func() []string {
			var out []string
			for i := liveRows - 4; i <= liveRows; i++ {
				out = append(out, fmt.Sprintf("w%04d", i))
			}
			return out
		}

		BeforeEach(func() {
			ctx = context.Background()
		})

		It("finishes a low-delta resync over many rows in one page", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxPullChanges: 64})))
			seedWidgets()

			pulls, keys := widen()
			Expect(keys).To(ConsistOf(wantKeys()))
			// One page carries the whole diff and the completion cursor; the
			// second is the catch-up pull that always follows a resync. The
			// scan budget, not the page size, decides this: bounding the scan
			// by MaxPullChanges would cost liveRows/64 pages instead.
			Expect(pulls).To(Equal(2))
		})

		It("pages the scan when MaxResyncScan is lowered", func() {
			start(newTestServer(server.WithLimits(server.Limits{
				MaxPullChanges: 64,
				MaxResyncScan:  64,
			})))
			seedWidgets()

			pulls, keys := widen()
			Expect(keys).To(ConsistOf(wantKeys()))
			Expect(pulls).To(BeNumerically(">=", liveRows/64),
				"a 64-row scan budget must page the scan even though the diff fits one page")
		})
	})

	// A scope change only has to look at rows whose visibility can differ
	// between the two scopes. Where an index can name those rows the resync
	// reads them and nothing else; every other shape falls back to the scan.
	// The result is identical either way, which is why each spec here pins
	// both the patch and what it cost.
	Describe("candidate narrowing", func() {
		// outcome is one drained scope change: what the client was told to
		// apply, and how many times the resync walked the entity to work it
		// out.
		type outcome struct {
			values     []string
			tombstones []string
			scans      int
		}

		var counter *readCountingAdapter

		// startCounting serves a fresh workspace over a store that counts its
		// reads, and reports whether the entities' indexes were built.
		startCounting := func(buildIndexes bool, opts ...server.Option) {
			GinkgoHelper()
			counter = &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
			srv := mustNewServer(counter, append([]server.Option{testEntities()}, opts...)...)
			if buildIndexes {
				Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			}
			start(srv)
		}

		stateEq := func(state string) *syncplanev1.Condition {
			return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path:  []string{"state"},
				Op:    syncplanev1.CmpOp_CMP_OP_EQ,
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: state}},
			}}}
		}

		titleEq := func(title string) *syncplanev1.Condition {
			return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path:  []string{"title"},
				Op:    syncplanev1.CmpOp_CMP_OP_EQ,
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: title}},
			}}}
		}

		issueSub := func(pred *syncplanev1.Condition) *syncplanev1.Subscription {
			return &syncplanev1.Subscription{Entity: "issue", Predicate: pred}
		}

		// seedIssues writes three issues per state, titled after their state so
		// a predicate on the unindexed title selects the same rows a predicate
		// on the indexed state does.
		seedIssues := func(states ...string) {
			GinkgoHelper()
			var muts []*syncplanev1.Mutation
			id := uint64(0)
			for _, state := range states {
				for i := 1; i <= 3; i++ {
					id++
					key := fmt.Sprintf("i-%s-%d", state, i)
					muts = append(muts, putMutation("c1", id, "issue", key,
						&testdatav1.Issue{Id: key, State: state, Title: state}))
				}
			}
			push(muts...)
		}

		issueKeys := func(state string) []string {
			var out []string
			for i := 1; i <= 3; i++ {
				out = append(out, fmt.Sprintf("i-%s-%d", state, i))
			}
			return out
		}

		// switchScope bootstraps under from, drains the resync to to, and
		// reports the patch together with the entity walks it took.
		switchScope := func(entity string, from, to *syncplanev1.Subscription) outcome {
			GinkgoHelper()
			cursor, _ := bootstrap(from)
			counter.reset()
			changes, drops, _ := drain(cursor, to)
			Expect(drops).To(BeEmpty())
			return outcome{
				values:     valueKeys(changes, entity),
				tombstones: tombstoneKeys(changes),
				scans:      counter.count("scanafter:" + entity),
			}
		}

		BeforeEach(func() { ctx = context.Background() })

		It("switches an indexed predicate without walking the entity", func() {
			startCounting(true)
			seedIssues("open", "closed", "done")
			indexed := switchScope("issue", issueSub(stateEq("open")), issueSub(stateEq("closed")))

			// The same switch over the same rows, with the indexes unbuilt.
			startCounting(false)
			seedIssues("open", "closed", "done")
			scanned := switchScope("issue", issueSub(stateEq("open")), issueSub(stateEq("closed")))

			Expect(indexed.values).To(Equal(issueKeys("closed")), "rows entering scope arrive as values")
			Expect(indexed.tombstones).To(Equal(issueKeys("open")), "rows leaving scope are tombstoned")
			Expect(indexed.values).To(Equal(scanned.values), "both paths must produce the same patch")
			Expect(indexed.tombstones).To(Equal(scanned.tombstones), "both paths must produce the same patch")
			Expect(indexed.scans).To(BeZero(), "an indexed predicate must not walk the entity")
			Expect(scanned.scans).To(BeNumerically(">", 0), "without a built index the resync walks the entity")
		})

		It("names the candidates of both sides without reading their rows", func() {
			startCounting(true)
			seedIssues("open", "closed", "done")
			out := switchScope("issue", issueSub(stateEq("open")), issueSub(stateEq("closed")))

			Expect(out.values).To(Equal(issueKeys("closed")))
			Expect(out.tombstones).To(Equal(issueKeys("open")))
			// Six candidates — three leaving, three entering — and one read of
			// each: naming them costs no row read.
			Expect(counter.count("batchkeys")).To(Equal(6))
			Expect(counter.count("get")).To(BeZero())
		})

		It("leaves rows written after the cursor to the catch-up pull", func() {
			startCounting(true)
			seedIssues("open", "closed")
			cursor, _ := bootstrap(issueSub(stateEq("open")))
			// Written past the cursor's seq: the resync must not decide its
			// visibility, and the catch-up pull that follows delivers it.
			push(putMutation("c1", 7, "issue", "i-late",
				&testdatav1.Issue{Id: "i-late", State: "closed", Title: "closed"}))
			counter.reset()

			resp := pull(cursor, 0, issueSub(stateEq("closed")))
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(valueKeys(resp.GetPatch().GetChanges(), "issue")).To(Equal(issueKeys("closed")))
			Expect(counter.count("scanafter:issue")).To(BeZero())

			changes, _, _ := drain(resp.GetNextCursor(), issueSub(stateEq("closed")))
			Expect(valueKeys(changes, "issue")).To(Equal([]string{"i-late"}))
		})

		It("widens a recency window without walking the entity", func() {
			startCounting(true)
			// One push each, so the rows have distinct versions and the
			// recency ordering is the write order rather than a key tiebreak.
			for i := 1; i <= 8; i++ {
				push(putWidget(uint64(i), fmt.Sprintf("w%d", i), int32(i)))
			}

			out := switchScope("widget", recencySub(2), recencySub(4))
			Expect(out.values).To(ConsistOf("w5", "w6"), "the rows the widened window admits")
			Expect(out.tombstones).To(BeEmpty())
			Expect(out.scans).To(BeZero(), "a window-only scope resolves from its two membership sets")
		})

		It("walks the entity for a predicate no index serves", func() {
			startCounting(true)
			seedIssues("open", "closed", "done")
			out := switchScope("issue", issueSub(titleEq("open")), issueSub(titleEq("closed")))

			Expect(out.values).To(Equal(issueKeys("closed")))
			Expect(out.tombstones).To(Equal(issueKeys("open")))
			Expect(out.scans).To(BeNumerically(">", 0), "an unindexed predicate has only the scan")
		})

		It("pages an indexed switch under the pull limit without repeating a row", func() {
			startCounting(true)
			seedIssues("open", "closed")
			cursor, _ := bootstrap(issueSub(stateEq("open")))

			var values, tombs []string
			pages := 0
			for range 100 {
				resp := pull(cursor, 1, issueSub(stateEq("closed")))
				Expect(resp.GetReset_()).To(BeFalse())
				Expect(len(resp.GetPatch().GetChanges())).To(BeNumerically("<=", 1))
				values = append(values, valueKeys(resp.GetPatch().GetChanges(), "issue")...)
				tombs = append(tombs, tombstoneKeys(resp.GetPatch().GetChanges())...)
				pages++
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					break
				}
			}
			Expect(pages).To(BeNumerically(">", 1), "a limit of 1 must force several pages")
			Expect(values).To(Equal(issueKeys("closed")), "each row exactly once, in key order")
			Expect(tombs).To(Equal(issueKeys("open")), "each row exactly once, in key order")
		})

		It("falls back to the scan when the candidates outgrow MaxResyncScan", func() {
			startCounting(true, server.WithLimits(server.Limits{
				// Six candidates (three leaving, three entering) against a
				// budget of two: an index that names most of the entity is no
				// cheaper than walking it.
				MaxPullChanges: 2,
				MaxResyncScan:  2,
			}))
			seedIssues("open", "closed", "done")
			out := switchScope("issue", issueSub(stateEq("open")), issueSub(stateEq("closed")))

			Expect(out.values).To(Equal(issueKeys("closed")))
			Expect(out.tombstones).To(Equal(issueKeys("open")))
			Expect(out.scans).To(BeNumerically(">", 0), "an over-budget candidate set falls back to the scan")
		})
	})
})
