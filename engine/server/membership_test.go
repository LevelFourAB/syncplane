package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// readCountingAdapter counts the reads made under a read snapshot, by kind.
// What maintenance saves is invisible in a response — the same rows come back
// either way — so the specs that pin the saving count what it took to produce
// them. Writes go through WithWriteTx and are deliberately not counted.
type readCountingAdapter struct {
	storage.Adapter
	mu     sync.Mutex
	counts map[string]int
}

func (a *readCountingAdapter) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	return a.Adapter.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		return fn(&readCountingTx{ReadTx: tx, owner: a})
	})
}

func (a *readCountingAdapter) bump(kind string) { a.add(kind, 1) }

func (a *readCountingAdapter) add(kind string, n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts[kind] += n
}

func (a *readCountingAdapter) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts = map[string]int{}
}

func (a *readCountingAdapter) count(kind string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.counts[kind]
}

type readCountingTx struct {
	storage.ReadTx
	owner *readCountingAdapter
}

func (t *readCountingTx) GetRow(entity, key string) (storage.Row, error) {
	t.owner.bump("get")
	return t.ReadTx.GetRow(entity, key)
}

// GetRows counts the batch read twice over: "batch" is what a networked
// adapter pays a round trip for, and "batchkeys" is how many rows it resolved
// — the ratio between them is what batching buys, and neither number alone
// says it.
func (t *readCountingTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	t.owner.bump("batch")
	t.owner.add("batchkeys", len(keys))
	return t.ReadTx.GetRows(entity, keys, fn)
}

func (t *readCountingTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	t.owner.bump("scan")
	return t.ReadTx.ScanEntity(entity, fn)
}

func (t *readCountingTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	t.owner.bump("recency")
	return t.ReadTx.ScanEntityByVersionDesc(entity, limit, fn)
}

func (t *readCountingTx) ScanIndex(entity, index string, lo, hi []byte, limit int, order storage.ScanOrder, fn func([]byte, string) error) error {
	t.owner.bump("probe")
	return t.ReadTx.ScanIndex(entity, index, lo, hi, limit, order, fn)
}

// replica is one server under the differential script, together with the
// cursor its own pulls have reached. Two of them take the same mutations in
// the same order and are asked the same questions.
type replica struct {
	client syncplanev1connect.SyncServiceClient
	cursor []byte
}

func startReplica(opts ...server.Option) *replica {
	GinkgoHelper()
	return serve(newTestServer(opts...))
}

// startCountingReplica is startReplica over a store that counts its reads.
func startCountingReplica(opts ...server.Option) (*replica, *readCountingAdapter) {
	GinkgoHelper()
	store := &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
	srv := mustNewServer(store, append([]server.Option{testEntities()}, opts...)...)
	return serve(srv), store
}

func serve(srv *server.Server) *replica {
	GinkgoHelper()
	path, handler := srv.Handler()
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	DeferCleanup(ts.Close)
	return &replica{client: syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)}
}

func (r *replica) push(ctx context.Context, muts []*syncplanev1.Mutation) {
	GinkgoHelper()
	resp, err := r.client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
	Expect(err).NotTo(HaveOccurred())
	for _, res := range pushResults(resp.Msg) {
		Expect(res.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
			"mutation refused: %s", res.GetErrorMessage())
	}
}

func (r *replica) pull(ctx context.Context, subs []*syncplanev1.Subscription, limit int) *syncplanev1.PullResponse {
	GinkgoHelper()
	resp, err := r.client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: ws, ClientId: "c1", Cursor: r.cursor, Subscriptions: subs, Limit: uint32(limit),
	}))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.Msg.GetReset_()).To(BeFalse(), "unexpected reset")
	r.cursor = resp.Msg.GetNextCursor()
	return resp.Msg
}

// bootstrapRows is the live set a client starting from nothing would be handed,
// which is the independent oracle for what pulling should have converged on.
func (r *replica) bootstrapRows(ctx context.Context, subs []*syncplanev1.Subscription) []string {
	GinkgoHelper()
	stream, err := r.client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
		WorkspaceId: ws, ClientId: "c2", Subscriptions: subs,
	}))
	Expect(err).NotTo(HaveOccurred())
	view := map[string][]byte{}
	for stream.Receive() {
		chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_)
		if !ok {
			continue
		}
		for _, rc := range chunk.Chunk.GetRows() {
			view[rc.GetEntity()+"/"+rc.GetKey()] = rc.GetValue()
		}
	}
	Expect(stream.Err()).NotTo(HaveOccurred())
	return viewKeys(view)
}

// applyPatch folds one pull page into a client's view, exactly as a client
// would: a value replaces, a tombstone drops.
func applyPatch(view map[string][]byte, p *syncplanev1.Patch) {
	for _, rc := range p.GetChanges() {
		key := rc.GetEntity() + "/" + rc.GetKey()
		if rc.GetTombstone() {
			delete(view, key)
			continue
		}
		view[key] = rc.GetValue()
	}
}

func viewKeys(view map[string][]byte) []string {
	out := make([]string, 0, len(view))
	for k := range view {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// patchDigest renders a page as a comparable list. Versions are included
// because two pages naming the same rows at different versions are not the
// same page — one of them delivered a row the other did not.
func patchDigest(p *syncplanev1.Patch) []string {
	out := make([]string, 0, len(p.GetChanges()))
	for _, rc := range p.GetChanges() {
		if rc.GetTombstone() {
			out = append(out, fmt.Sprintf("-%s/%s@%d", rc.GetEntity(), rc.GetKey(), rc.GetVersion()))
			continue
		}
		out = append(out, fmt.Sprintf("+%s/%s@%d=%x", rc.GetEntity(), rc.GetKey(), rc.GetVersion(), rc.GetValue()))
	}
	sort.Strings(out)
	return out
}

// hidingPermissions withholds some rows from every read, so the specs run
// against a membership decided by a read filter and not only by a predicate.
type hidingPermissions struct{ server.AllowAll }

func (hidingPermissions) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	switch entity {
	case "widget":
		return func(_ string, row proto.Message) bool { return row.(*testdatav1.Widget).GetLabel() != "l0" }, nil
	case "issue":
		return func(_ string, row proto.Message) bool { return row.(*testdatav1.Issue).GetTitle() != "t0" }, nil
	}
	return nil, nil
}

// nameIs is the "name equals" predicate the seed-rule specs narrow a project
// subscription with.
func nameIs(v string) *syncplanev1.Condition {
	return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
		Path:  []string{"name"},
		Op:    syncplanev1.CmpOp_CMP_OP_EQ,
		Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: v}},
	}}}
}

// changeKeys names a page's rows and says which of them are tombstones, for
// specs about WHICH rows moved rather than what they now hold.
func changeKeys(p *syncplanev1.Patch) []string {
	out := make([]string, 0, len(p.GetChanges()))
	for _, rc := range p.GetChanges() {
		sign := "+"
		if rc.GetTombstone() {
			sign = "-"
		}
		out = append(out, sign+rc.GetEntity()+"/"+rc.GetKey())
	}
	return out
}

// randomRound is one batch of mutations over a small fixed key space, chosen so
// that windows fill, overflow, lose members and refill, and so that follow
// seeds enter and leave the base scope. The key space is deliberately smaller
// than the number of rounds: rows are rewritten, deleted and re-created, which
// is where membership sets drift apart if they drift at all.
func randomRound(rng *rand.Rand, nextMut *uint64, n int) []*syncplanev1.Mutation {
	muts := make([]*syncplanev1.Mutation, 0, n)
	next := func() uint64 { *nextMut++; return *nextMut }
	for i := 0; i < n; i++ {
		switch rng.Intn(12) {
		case 0, 1, 2:
			id := fmt.Sprintf("w%d", rng.Intn(8))
			muts = append(muts, putMutation("c1", next(), "widget", id,
				&testdatav1.Widget{Id: id, Count: int32(rng.Intn(20)), Label: fmt.Sprintf("l%d", rng.Intn(3))}))
		case 3:
			muts = append(muts, deleteMutation("c1", next(), "widget", fmt.Sprintf("w%d", rng.Intn(8))))
		case 4, 5:
			id := fmt.Sprintf("p%d", rng.Intn(4))
			muts = append(muts, putMutation("c1", next(), "project", id,
				&testdatav1.Project{Id: id, Name: fmt.Sprintf("n%d", rng.Intn(6))}))
		case 6:
			muts = append(muts, deleteMutation("c1", next(), "project", fmt.Sprintf("p%d", rng.Intn(4))))
		case 7, 8:
			id := fmt.Sprintf("i%d", rng.Intn(10))
			iss := &testdatav1.Issue{Id: id, Title: fmt.Sprintf("t%d", rng.Intn(5)), Rank: int32(rng.Intn(20))}
			// A fifth of the issues point at nothing, so a seed losing its last
			// follower and a follower losing its seed both happen.
			if p := rng.Intn(5); p < 4 {
				iss.Project = &testdatav1.Project{Id: fmt.Sprintf("p%d", p)}
			}
			// Two refs to the same target, so one issue can be held by two
			// relations at once and dropping it needs both to let go.
			if u := rng.Intn(4); u < 3 {
				iss.Assignee = &testdatav1.User{Id: fmt.Sprintf("u%d", u)}
			}
			if u := rng.Intn(4); u < 3 {
				iss.Reporter = &testdatav1.User{Id: fmt.Sprintf("u%d", u)}
			}
			muts = append(muts, putMutation("c1", next(), "issue", id, iss))
		case 10:
			id := fmt.Sprintf("u%d", rng.Intn(3))
			if rng.Intn(4) == 0 {
				muts = append(muts, deleteMutation("c1", next(), "user", id))
				break
			}
			muts = append(muts, putMutation("c1", next(), "user", id,
				&testdatav1.User{Id: id, Name: fmt.Sprintf("u%d", rng.Intn(4))}))
		case 9:
			// A batch writes several rows under ONE version, which is what puts
			// a page boundary inside a same-version group.
			a := fmt.Sprintf("w%d", rng.Intn(8))
			b := fmt.Sprintf("i%d", rng.Intn(10))
			muts = append(muts, batchMutation("c1", next(),
				crudPut("widget", a, &testdatav1.Widget{Id: a, Count: int32(rng.Intn(20))}),
				crudPut("issue", b, &testdatav1.Issue{Id: b, Rank: int32(rng.Intn(20)),
					Project: &testdatav1.Project{Id: fmt.Sprintf("p%d", rng.Intn(4))}})))
		default:
			muts = append(muts, deleteMutation("c1", next(), "issue", fmt.Sprintf("i%d", rng.Intn(10))))
		}
	}
	return muts
}

// Maintaining a bounded membership set from the rows that changed is an
// optimization over resolving it from scratch, so the two must answer
// identically however the workspace is driven — the same rule index use is
// held to (see Limits.DisableMembershipMaintenance). Every spec here drives two
// servers through one script and compares every page.
var _ = Describe("Membership maintenance", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// drive runs one script against both replicas and compares every page they
	// produce, then checks the view pulling converged on against a fresh
	// bootstrap — because two implementations agreeing is not by itself
	// evidence that either is right.
	//
	// limit is the per-pull row limit, so a caller can force pages to truncate
	// (and cursors to carry a sub-position, which is a case maintenance
	// declines); 0 takes the server's own.
	drive := func(seed int64, rounds, limit int, opts []server.Option, subs ...*syncplanev1.Subscription) {
		GinkgoHelper()
		maintained := startReplica(opts...)
		rebuilt := startReplica(append(append([]server.Option{}, opts...),
			server.WithLimits(server.Limits{DisableMembershipMaintenance: true}))...)
		rng := rand.New(rand.NewSource(seed))
		view := map[string][]byte{}
		var nextMut uint64
		for round := 0; round < rounds; round++ {
			muts := randomRound(rng, &nextMut, 1+rng.Intn(3))
			maintained.push(ctx, muts)
			rebuilt.push(ctx, muts)

			for page := 0; ; page++ {
				Expect(page).To(BeNumerically("<", 100), "round %d did not drain", round)
				got := maintained.pull(ctx, subs, limit)
				want := rebuilt.pull(ctx, subs, limit)
				Expect(patchDigest(got.GetPatch())).To(Equal(patchDigest(want.GetPatch())),
					"page %d of round %d differs (seed %d)", page, round, seed)
				Expect(got.GetSeq()).To(Equal(want.GetSeq()), "seq differs at round %d", round)
				Expect(got.GetHasMore()).To(Equal(want.GetHasMore()), "has_more differs at round %d", round)
				applyPatch(view, got.GetPatch())
				if !got.GetHasMore() {
					break
				}
			}
		}
		Expect(viewKeys(view)).To(Equal(maintained.bootstrapRows(ctx, subs)),
			"converged view differs from a fresh bootstrap (seed %d)", seed)
	}

	differential := func(seed int64, rounds int, subs ...*syncplanev1.Subscription) {
		GinkgoHelper()
		drive(seed, rounds, 0, nil, subs...)
	}

	// overSeeds runs one script shape under several fixed seeds. Fixed rather
	// than drawn from the suite's randomness, so a failure is reproducible from
	// the spec name alone.
	overSeeds := func(description string, body func(seed int64)) {
		args := []any{body}
		for _, seed := range []int64{1, 2, 3, 7, 11} {
			args = append(args, Entry(fmt.Sprintf("seed %d", seed), seed))
		}
		DescribeTable(description, args...)
	}

	overSeeds("an order_by window converges the same way it rebuilds",
		func(seed int64) { differential(seed, 120, windowSub(3, nil)) },
	)

	overSeeds("a by_recency window converges the same way it rebuilds",
		func(seed int64) { differential(seed, 120, recencySub(3)) },
	)

	overSeeds("a windowed subscription following a reverse relation converges the same way it rebuilds",
		func(seed int64) {
			differential(seed, 120, &syncplanev1.Subscription{
				Entity: "project",
				Window: &syncplanev1.Window{
					OrderBy: []*syncplanev1.OrderKey{{Path: []string{"name"}, Dir: syncplanev1.Direction_DIRECTION_ASC}},
					Limit:   2,
				},
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
			})
		},
	)

	overSeeds("a predicate-scoped subscription following a reverse relation converges the same way it rebuilds",
		func(seed int64) {
			differential(seed, 120, &syncplanev1.Subscription{
				Entity: "project",
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
			})
		},
	)

	overSeeds("a page that truncates converges the same way it rebuilds",
		func(seed int64) {
			// One row per page forces every cursor through the truncated path,
			// including the sub-position inside a batch's shared version, which
			// is a case maintenance declines and must decline identically.
			drive(seed, 40, 1, nil,
				windowSub(3, nil),
				&syncplanev1.Subscription{
					Entity: "project",
					Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
				})
		},
	)

	overSeeds("a read filter narrows both sets the same way it rebuilds",
		func(seed int64) {
			// Membership is decided against the read filter as well as the
			// predicate, so a filtered row must leave a window and a closure
			// exactly as an unmatched one does.
			drive(seed, 120, 0, []server.Option{server.WithPermissions(hidingPermissions{})},
				windowSub(3, nil),
				recencySub(3),
				&syncplanev1.Subscription{
					Entity: "project",
					Window: &syncplanev1.Window{
						OrderBy: []*syncplanev1.OrderKey{{Path: []string{"name"}, Dir: syncplanev1.Direction_DIRECTION_ASC}},
						Limit:   2,
					},
					Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
				})
		},
	)

	overSeeds("two subscriptions seeding one relation differently converge the same way they rebuild",
		func(seed int64) {
			// A window and a predicate over the SAME entity, both following the
			// same relation. Whether a project is a follow root is a different
			// question for each, asked about the same rows.
			issues := &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}}
			drive(seed, 120, 0, nil,
				&syncplanev1.Subscription{
					Entity: "project",
					Window: &syncplanev1.Window{
						OrderBy: []*syncplanev1.OrderKey{{Path: []string{"name"}, Dir: syncplanev1.Direction_DIRECTION_ASC}},
						Limit:   2,
					},
					Follow: issues,
				},
				&syncplanev1.Subscription{Entity: "project", Predicate: nameIs("n1"), Follow: issues},
			)
		},
	)

	overSeeds("two relations onto one entity converge the same way they rebuild",
		func(seed int64) {
			// An issue can be reached as an assignee's and as a reporter's, so
			// losing one relation must not drop a row the other still holds.
			drive(seed, 120, 0, nil, &syncplanev1.Subscription{
				Entity: "user",
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{
					{Segments: []string{"issuesByAssignee"}},
					{Segments: []string{"issuesByReporter"}},
				}},
			})
		},
	)

	// The specs above would all pass if maintenance never ran, so these pin
	// that it does — by the reads it does NOT make.
	Describe("what a maintained pull costs", func() {
		// Every project is inside the window and every one has followers, so a
		// rebuild asks all of them what points back and a maintained pull asks
		// only about what moved.
		const projects, perProject = 20, 5

		followingWindow := func() []*syncplanev1.Subscription {
			return []*syncplanev1.Subscription{{
				Entity: "project",
				Window: &syncplanev1.Window{
					OrderBy: []*syncplanev1.OrderKey{{Path: []string{"id"}, Dir: syncplanev1.Direction_DIRECTION_ASC}},
					Limit:   projects,
				},
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
			}}
		}

		seed := func(r *replica) uint64 {
			GinkgoHelper()
			var mut uint64
			var muts []*syncplanev1.Mutation
			for p := 0; p < projects; p++ {
				id := fmt.Sprintf("p%02d", p)
				mut++
				muts = append(muts, putMutation("c1", mut, "project", id, &testdatav1.Project{Id: id, Name: id}))
				for i := 0; i < perProject; i++ {
					key := fmt.Sprintf("i%02d-%d", p, i)
					mut++
					muts = append(muts, putMutation("c1", mut, "issue", key,
						&testdatav1.Issue{Id: key, Project: &testdatav1.Project{Id: id}}))
				}
			}
			r.push(ctx, muts)
			return mut
		}

		It("probes only the reverse relation of the seed that moved", func() {
			maintained, counter := startCountingReplica()
			mut := seed(maintained)
			subs := followingWindow()
			maintained.pull(ctx, subs, 0) // settle: the first pull has no state to maintain from

			// One new issue under one project. Nothing else about the window or
			// the closure can have moved.
			maintained.push(ctx, []*syncplanev1.Mutation{putMutation("c1", mut+1, "issue", "i00-new",
				&testdatav1.Issue{Id: "i00-new", Project: &testdatav1.Project{Id: "p00"}})})
			counter.reset()
			Expect(maintained.pull(ctx, subs, 0).GetPatch().GetChanges()).To(HaveLen(1))

			// A rebuild asks every seed what points at it. This decides the
			// changed issue's membership from its own ref field, and asks
			// nothing — the seed it names did not move, and no other seed
			// could have gained or lost a follower without a row changing.
			Expect(counter.count("probe") + counter.count("scan")).To(BeZero())
		})

		It("costs a probe per seed when it has to rebuild", func() {
			rebuilt, counter := startCountingReplica(
				server.WithLimits(server.Limits{DisableMembershipMaintenance: true}))
			mut := seed(rebuilt)
			subs := followingWindow()
			rebuilt.pull(ctx, subs, 0)

			rebuilt.push(ctx, []*syncplanev1.Mutation{putMutation("c1", mut+1, "issue", "i00-new",
				&testdatav1.Issue{Id: "i00-new", Project: &testdatav1.Project{Id: "p00"}})})
			counter.reset()
			rebuilt.pull(ctx, subs, 0)

			// The reference point the spec above is measured against: the walk
			// asks every seed what points at it, however little changed — one
			// index probe each where the ref index is ready, one scan of the
			// source entity each where it is not.
			Expect(counter.count("probe") + counter.count("scan")).To(BeNumerically(">=", projects))
		})

		It("reorders a recency window without reading the entity", func() {
			maintained, counter := startCountingReplica()
			var muts []*syncplanev1.Mutation
			for i := 0; i < 30; i++ {
				id := fmt.Sprintf("w%02d", i)
				muts = append(muts, putMutation("c1", uint64(i+1), "widget", id, &testdatav1.Widget{Id: id}))
			}
			maintained.push(ctx, muts)
			subs := []*syncplanev1.Subscription{recencySub(5)}
			maintained.pull(ctx, subs, 0)

			maintained.push(ctx, []*syncplanev1.Mutation{
				putMutation("c1", 31, "widget", "w00", &testdatav1.Widget{Id: "w00", Count: 1}),
			})
			counter.reset()
			// The rewritten row enters at the front and the weakest member
			// falls off, so the page is a value and a tombstone.
			Expect(changeKeys(maintained.pull(ctx, subs, 0).GetPatch())).To(ConsistOf(
				"+widget/w00", "-widget/w25"))

			// A recency window's ordering IS the workspace sequence, so a row
			// written since the cursor enters at the front and the tail falls
			// off a list the cursor already carries. Nothing is read to know it.
			Expect(counter.count("recency")).To(BeZero())
			Expect(counter.count("scan")).To(BeZero())
		})
	})

	overSeeds("several windows and a forward follow converge the same way they rebuild",
		func(seed int64) {
			differential(seed, 120,
				windowSub(2, nil),
				recencySub(4),
				&syncplanev1.Subscription{
					Entity: "issue",
					Window: &syncplanev1.Window{
						OrderBy: []*syncplanev1.OrderKey{{Path: []string{"rank"}, Dir: syncplanev1.Direction_DIRECTION_DESC}},
						Limit:   3,
					},
					// A forward segment is a shape maintenance declines, so this
					// pins that a declined closure still agrees with a
					// maintained window beside it.
					Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}}},
				})
		},
	)
})
