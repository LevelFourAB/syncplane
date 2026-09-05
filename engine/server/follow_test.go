package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// scanCountingAdapter counts full-entity scans made under a read snapshot.
// What a following pull costs is not visible in its response — the closure
// rebuild happens either way and returns the same rows — so the specs that
// pin the cost count the scans it makes. Writes go through WithWriteTx and are
// deliberately not counted.
type scanCountingAdapter struct {
	storage.Adapter
	mu    sync.Mutex
	scans map[string]int
}

func (a *scanCountingAdapter) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	return a.Adapter.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		return fn(&scanCountingTx{ReadTx: tx, owner: a})
	})
}

func (a *scanCountingAdapter) count(entity string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.scans[entity]
}

func (a *scanCountingAdapter) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.scans = map[string]int{}
}

type scanCountingTx struct {
	storage.ReadTx
	owner *scanCountingAdapter
}

func (t *scanCountingTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	t.owner.mu.Lock()
	t.owner.scans[entity]++
	t.owner.mu.Unlock()
	return t.ReadTx.ScanEntity(entity, fn)
}

// Reference-following subscriptions: a subscription's synced set is its base
// scope UNION a bounded closure reached by walking typed references. The
// closure is recomputed every pull and diffed against the cursor's stored
// key-set, so a re-point delivers the new target AND drops the old one.
var _ = Describe("Reference-following subscriptions", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
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
		startWith()
	})

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		_, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
	}

	// pull issues a Pull with the given cursor + subscriptions and returns the
	// response (so callers can thread NextCursor).
	pull := func(cursor []byte, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, Cursor: cursor, Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// values returns the live (non-tombstone) "entity/key" set of a patch.
	values := func(p *syncplanev1.Patch) []string {
		var out []string
		for _, rc := range p.GetChanges() {
			if !rc.GetTombstone() {
				out = append(out, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
		return out
	}
	tombstones := func(p *syncplanev1.Patch) []string {
		var out []string
		for _, rc := range p.GetChanges() {
			if rc.GetTombstone() {
				out = append(out, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
		return out
	}
	// projectName finds a project value in a patch and returns its name.
	projectName := func(p *syncplanev1.Patch, key string) string {
		for _, rc := range p.GetChanges() {
			if rc.GetEntity() == "project" && rc.GetKey() == key && !rc.GetTombstone() {
				var pr testdatav1.Project
				Expect(proto.Unmarshal(rc.GetValue(), &pr)).To(Succeed())
				return pr.GetName()
			}
		}
		return ""
	}

	idEq := func(id string) *syncplanev1.Condition {
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
			Path:  []string{"id"},
			Op:    syncplanev1.CmpOp_CMP_OP_EQ,
			Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: id}},
		}}}
	}

	issue := func(id, title, projectID string) *testdatav1.Issue {
		i := &testdatav1.Issue{Id: id, Title: title}
		if projectID != "" {
			i.Project = &testdatav1.Project{Id: projectID}
		}
		return i
	}

	// followProject subscribes to all issues and follows issue→project, so
	// projects arrive ONLY through the closure (issues are the base scope).
	followProject := func(depth uint32) *syncplanev1.Subscription {
		return &syncplanev1.Subscription{
			Entity: "issue",
			Follow: &syncplanev1.FollowSpec{
				Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				Depth: depth,
			},
		}
	}

	Describe("re-pointing a ref to an already-old target", func() {
		// THE case the plan glosses over: the incremental scan only sees rows
		// with version > cursor.seq. A holder re-pointed to an OLD target whose
		// row never re-appears in the scan must still be delivered (and the
		// now-unreferenced old target dropped). This is what the stored-keyset
		// closure diff exists to get right.
		It("delivers the newly-referenced old target and drops the old one", func() {
			// Two projects exist up front; only A is referenced.
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "project", "B", &testdatav1.Project{Id: "B", Name: "beta"}),
				putMutation("c1", 3, "issue", "i1", issue("i1", "first", "A")),
			)
			sub := followProject(1)

			// Initial pull: base issue + followed project A; B is unreferenced.
			r1 := pull(nil, sub)
			Expect(values(r1.GetPatch())).To(ConsistOf("issue/i1", "project/A"))
			Expect(values(r1.GetPatch())).NotTo(ContainElement("project/B"))

			// Re-point i1 from A to B. B's row is OLD (version 2 ≤ cursor.seq);
			// the incremental scan will see i1 but never B.
			push(putMutation("c1", 4, "issue", "i1", issue("i1", "first", "B")))

			r2 := pull(r1.GetNextCursor(), sub)
			// i1 (changed) and B (entered the closure, old row) arrive; A drops.
			Expect(values(r2.GetPatch())).To(ContainElements("issue/i1", "project/B"))
			Expect(tombstones(r2.GetPatch())).To(ContainElement("project/A"))
			Expect(projectName(r2.GetPatch(), "B")).To(Equal("beta"))
		})

		It("sends nothing for a changed target that was never in the closure", func() {
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "project", "B", &testdatav1.Project{Id: "B", Name: "beta"}),
				putMutation("c1", 3, "issue", "i1", issue("i1", "first", "A")),
			)
			sub := followProject(1)
			r1 := pull(nil, sub)
			Expect(values(r1.GetPatch())).To(ConsistOf("issue/i1", "project/A"))

			// B changes but nothing references it. The cursor's stored closure
			// proves the client never held it, so a tombstone would only
			// disclose that the row exists and just changed.
			push(putMutation("c1", 4, "project", "B", &testdatav1.Project{Id: "B", Name: "beta 2"}))
			r2 := pull(r1.GetNextCursor(), sub)
			Expect(r2.GetPatch().GetChanges()).To(BeEmpty())
		})
	})

	Describe("a changed follow-target that is only follow-visible", func() {
		// Failure-mode 2: the holder is unchanged, the TARGET edits a field. The
		// target is in a subscribed (followed) entity so the incremental scan
		// reaches it — but it has no base scope, so closure-aware classify is
		// what keeps it visible instead of tombstoning it.
		It("delivers the target's new value without re-pointing", func() {
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "first", "A")),
			)
			sub := followProject(1)
			r1 := pull(nil, sub)
			Expect(values(r1.GetPatch())).To(ConsistOf("issue/i1", "project/A"))

			// Edit A only.
			push(putMutation("c1", 3, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha2"}))
			r2 := pull(r1.GetNextCursor(), sub)
			Expect(projectName(r2.GetPatch(), "A")).To(Equal("alpha2"))
			Expect(tombstones(r2.GetPatch())).NotTo(ContainElement("project/A"))
		})
	})

	Describe("forward depth", func() {
		// blocked_by is a repeated self-reference: a dependency chain
		// i1→i2→i3. Depth bounds how far the closure walks it.
		BeforeEach(func() {
			blocked := func(id string, deps ...string) *testdatav1.Issue {
				i := &testdatav1.Issue{Id: id, Title: id}
				for _, d := range deps {
					i.BlockedBy = append(i.BlockedBy, &testdatav1.Issue{Id: d})
				}
				return i
			}
			push(
				putMutation("c1", 1, "issue", "i3", blocked("i3")),
				putMutation("c1", 2, "issue", "i2", blocked("i2", "i3")),
				putMutation("c1", 3, "issue", "i1", blocked("i1", "i2")),
			)
		})

		blockedSub := func(depth uint32) *syncplanev1.Subscription {
			return &syncplanev1.Subscription{
				Entity:    "issue",
				Predicate: idEq("i1"), // base scope: only i1; follow blocked_by from it
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"blockedBy"}}},
					Depth: depth,
				},
			}
		}

		It("depth 1 reaches only direct dependencies", func() {
			r := pull(nil, blockedSub(1))
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "issue/i2"))
		})
		It("depth 2 reaches transitive dependencies", func() {
			r := pull(nil, blockedSub(2))
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "issue/i2", "issue/i3"))
		})
	})

	Describe("reverse following", func() {
		// project→issues is the auto-named reverse of the single issue.project
		// forward ref (pluralized source). Subscribe a project, get its issues.
		reverseSub := &syncplanev1.Subscription{
			Entity:    "project",
			Predicate: nil, // whole project entity (only P exists below)
			Follow:    &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
		}

		It("delivers children of a subscribed parent", func() {
			push(
				putMutation("c1", 1, "project", "P", &testdatav1.Project{Id: "P", Name: "proj"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "one", "P")),
				putMutation("c1", 3, "issue", "i2", issue("i2", "two", "P")),
			)
			r := pull(nil, reverseSub)
			Expect(values(r.GetPatch())).To(ConsistOf("project/P", "issue/i1", "issue/i2"))
		})
	})

	Describe("multi-path hold/drop", func() {
		// A project P is referenced by two issues. P leaves the closure only
		// when NO issue references it — held by any path ⇒ stays.
		It("keeps a target held by another path, drops it only when fully unreferenced", func() {
			push(
				putMutation("c1", 1, "project", "P", &testdatav1.Project{Id: "P", Name: "proj"}),
				putMutation("c1", 2, "project", "Q", &testdatav1.Project{Id: "Q", Name: "other"}),
				putMutation("c1", 3, "issue", "i1", issue("i1", "one", "P")),
				putMutation("c1", 4, "issue", "i2", issue("i2", "two", "P")),
			)
			sub := followProject(1)
			r1 := pull(nil, sub)
			Expect(values(r1.GetPatch())).To(ContainElement("project/P"))

			// Re-point i1 to Q; i2 still references P, so P must stay.
			push(putMutation("c1", 5, "issue", "i1", issue("i1", "one", "Q")))
			r2 := pull(r1.GetNextCursor(), sub)
			Expect(values(r2.GetPatch())).To(ContainElement("project/Q"))
			Expect(tombstones(r2.GetPatch())).NotTo(ContainElement("project/P"))

			// Re-point i2 to Q as well; now P is unreferenced and drops.
			push(putMutation("c1", 6, "issue", "i2", issue("i2", "two", "Q")))
			r3 := pull(r2.GetNextCursor(), sub)
			Expect(tombstones(r3.GetPatch())).To(ContainElement("project/P"))
		})
	})

	Describe("cycle termination", func() {
		// blocked_by can form a cycle: i1↔i2. A visited-set must terminate the
		// walk; a generous depth must not loop forever.
		It("terminates on a forward cycle", func() {
			push(
				putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{Id: "i1", Title: "one", BlockedBy: []*testdatav1.Issue{{Id: "i2"}}}),
				putMutation("c1", 2, "issue", "i2", &testdatav1.Issue{Id: "i2", Title: "two", BlockedBy: []*testdatav1.Issue{{Id: "i1"}}}),
			)
			sub := &syncplanev1.Subscription{
				Entity:    "issue",
				Predicate: idEq("i1"),
				Follow:    &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"blockedBy"}}}, Depth: 4},
			}
			r := pull(nil, sub)
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "issue/i2"))
		})
	})

	Describe("independent path sets from a shared seed", func() {
		// Two subscriptions follow different relations from the SAME issue. The
		// per-subscription visited set must let each walk its own paths — a
		// shared visited set would drop the second subscription's target.
		It("follows each subscription's paths from a row both seed", func() {
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "user", "u1", &testdatav1.User{Id: "u1", Name: "ann"}),
				putMutation("c1", 3, "issue", "i1", &testdatav1.Issue{
					Id: "i1", Title: "one",
					Project:  &testdatav1.Project{Id: "A"},
					Assignee: &testdatav1.User{Id: "u1"},
				}),
			)
			followAssignee := &syncplanev1.Subscription{
				Entity: "issue",
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"assignee"}}}},
			}
			r := pull(nil, followProject(1), followAssignee)
			Expect(values(r.GetPatch())).To(ContainElements("project/A", "user/u1", "issue/i1"))
		})
	})

	Describe("follow-spec change without a reset", func() {
		// A follow-spec change is deliberately excluded from sub_hash: it must
		// NOT trigger a base-scope reset/resync. The closure diff against the
		// cursor's stored key-set reconciles it instead.
		It("widening depth delivers newly-reachable rows on the same cursor", func() {
			blocked := func(id string, deps ...string) *testdatav1.Issue {
				i := &testdatav1.Issue{Id: id, Title: id}
				for _, d := range deps {
					i.BlockedBy = append(i.BlockedBy, &testdatav1.Issue{Id: d})
				}
				return i
			}
			push(
				putMutation("c1", 1, "issue", "i3", blocked("i3")),
				putMutation("c1", 2, "issue", "i2", blocked("i2", "i3")),
				putMutation("c1", 3, "issue", "i1", blocked("i1", "i2")),
			)
			sub := func(depth uint32) *syncplanev1.Subscription {
				return &syncplanev1.Subscription{
					Entity:    "issue",
					Predicate: idEq("i1"),
					Follow:    &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"blockedBy"}}}, Depth: depth},
				}
			}
			r1 := pull(nil, sub(1))
			Expect(values(r1.GetPatch())).To(ConsistOf("issue/i1", "issue/i2"))

			// Same base predicate, deeper follow: no reset, i3 arrives.
			r2 := pull(r1.GetNextCursor(), sub(2))
			Expect(r2.GetReset_()).To(BeFalse())
			Expect(values(r2.GetPatch())).To(ContainElement("issue/i3"))
		})
	})

	Describe("what a following pull costs", func() {
		// The closure is rebuilt statelessly on every following pull, so the work
		// a pull does is invisible in the patch it returns — two pulls that
		// deliver nothing can cost a full scan of the base entity or nothing at
		// all. These specs count entity scans instead, because "does not scan"
		// is the whole claim (issues/112).
		var (
			counter *scanCountingAdapter
			cclient syncplanev1connect.SyncServiceClient
		)

		startCounting := func(opts ...server.Option) *server.Server {
			GinkgoHelper()
			counter = &scanCountingAdapter{Adapter: newTestStore(), scans: map[string]int{}}
			srv := mustNewServer(counter, append([]server.Option{testEntities()}, opts...)...)
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			cclient = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
			return srv
		}
		cpush := func(mutations ...*syncplanev1.Mutation) {
			GinkgoHelper()
			_, err := cclient.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
			Expect(err).NotTo(HaveOccurred())
		}
		cpull := func(cursor []byte, subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
			GinkgoHelper()
			resp, err := cclient.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, Cursor: cursor, Subscriptions: subs,
			}))
			Expect(err).NotTo(HaveOccurred())
			return resp.Msg
		}

		It("rebuilds nothing when the cursor already sits at the head", func() {
			startCounting()
			cpush(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "first", "A")),
			)
			sub := followProject(1)
			r1 := cpull(nil, sub)
			Expect(values(r1.GetPatch())).To(ConsistOf("issue/i1", "project/A"))

			// Nothing was written since that cursor, so there is nothing the
			// closure could have become: the cursor's key-set is carried forward
			// and the base entity is never touched.
			counter.reset()
			r2 := cpull(r1.GetNextCursor(), sub)
			Expect(r2.GetPatch().GetChanges()).To(BeEmpty())
			Expect(counter.count("issue")).To(BeZero(), "the seed scan must not run")

			// And the skip must not wedge the stream: the next write still
			// arrives, closure and all.
			cpush(putMutation("c1", 3, "project", "B", &testdatav1.Project{Id: "B", Name: "beta"}))
			cpush(putMutation("c1", 4, "issue", "i2", issue("i2", "second", "B")))
			r3 := cpull(r2.GetNextCursor(), sub)
			Expect(values(r3.GetPatch())).To(ContainElements("issue/i2", "project/B"))
		})

		It("seeds a windowed follow from the window instead of scanning again", func() {
			startCounting()
			cpush(
				putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "one"}),
				putMutation("c1", 2, "issue", "i1", &testdatav1.Issue{
					Id: "i1", Title: "b", Project: &testdatav1.Project{Id: "p1"},
				}),
				putMutation("c1", 3, "issue", "i2", &testdatav1.Issue{
					Id: "i2", Title: "a", Project: &testdatav1.Project{Id: "p1"},
				}),
			)
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
			counter.reset()
			r := cpull(nil, sub)
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "project/p1"))
			// One scan resolves the window; the follow seeds from the members it
			// produced rather than walking the entity a second time to find rows
			// it already holds.
			Expect(counter.count("issue")).To(Equal(1))
		})

		// A predicate that names its own rows is the shape a "one issue and
		// everything around it" subscription has, and the seed set is the whole
		// reason such a pull would otherwise read the entity end to end.
		It("seeds a key-equality follow without scanning the entity", func() {
			// No indexes built, and `id` carries none of its own: the equality
			// names the row outright.
			startCounting()
			cpush(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "first", "A")),
				putMutation("c1", 3, "issue", "i2", issue("i2", "second", "A")),
			)
			counter.reset()

			r := cpull(nil, &syncplanev1.Subscription{
				Entity:    "issue",
				Predicate: idEq("i1"),
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			})
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "project/A"))
			Expect(counter.count("issue")).To(BeZero(), "the pinned key names the seed")
		})

		It("seeds an indexed predicate without scanning the entity", func() {
			srv := startCounting()
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			cpush(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", &testdatav1.Issue{
					Id: "i1", Title: "first", State: "open",
					Project: &testdatav1.Project{Id: "A"},
				}),
				putMutation("c1", 3, "issue", "i2", &testdatav1.Issue{
					Id: "i2", Title: "second", State: "closed",
					Project: &testdatav1.Project{Id: "A"},
				}),
			)
			counter.reset()

			r := cpull(nil, &syncplanev1.Subscription{
				Entity:    "issue",
				Predicate: stringIs("state", "open"),
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			})
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "project/A"))
			Expect(counter.count("issue")).To(BeZero(), "the index names the seeds")
		})

		It("scans for seeds a predicate no index reaches", func() {
			// `title` carries no index, so nothing can name the matching rows
			// short of visiting them — and the answer must still be exact.
			startCounting()
			cpush(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "first", "A")),
				putMutation("c1", 3, "issue", "i2", issue("i2", "second", "A")),
			)
			counter.reset()

			r := cpull(nil, &syncplanev1.Subscription{
				Entity:    "issue",
				Predicate: titleIs("first"),
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			})
			Expect(values(r.GetPatch())).To(ConsistOf("issue/i1", "project/A"))
			Expect(counter.count("issue")).To(BeNumerically(">", 0))
		})

		It("drops a read-filtered seed, closure and all", func() {
			// The candidates a predicate names are a superset of the seeds: the
			// read filter is what cuts them down, and a seed it hides follows
			// nothing.
			startCounting(server.WithPermissions(hidingPermissions{}))
			cpush(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "t0", "A")),
			)
			counter.reset()

			r := cpull(nil, &syncplanev1.Subscription{
				Entity:    "issue",
				Predicate: idEq("i1"),
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			})
			Expect(values(r.GetPatch())).To(BeEmpty())
			Expect(counter.count("issue")).To(BeZero())
		})
	})

	Describe("bounds", func() {
		It("rejects a closure exceeding MaxClosureSize", func() {
			startWith(server.WithLimits(server.Limits{MaxClosureSize: 2}))
			push(
				putMutation("c1", 1, "project", "A", &testdatav1.Project{Id: "A"}),
				putMutation("c1", 2, "project", "B", &testdatav1.Project{Id: "B"}),
				putMutation("c1", 3, "project", "C", &testdatav1.Project{Id: "C"}),
				putMutation("c1", 4, "issue", "i1", issue("i1", "one", "A")),
				putMutation("c1", 5, "issue", "i2", issue("i2", "two", "B")),
				putMutation("c1", 6, "issue", "i3", issue("i3", "three", "C")),
			)
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, Subscriptions: []*syncplanev1.Subscription{followProject(1)},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
			Expect(err).To(MatchError(ContainSubstring(
				"over MaxClosureSize (2); raise it with server.WithLimits")))
		})

		It("rejects a follow depth above MaxFollowDepth", func() {
			startWith(server.WithLimits(server.Limits{MaxFollowDepth: 1}))
			push(putMutation("c1", 1, "issue", "i1", issue("i1", "one", "A")))
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, Subscriptions: []*syncplanev1.Subscription{followProject(3)},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring(
				"follow depth 3, over MaxFollowDepth (1); raise it with server.WithLimits")))
		})

		It("rejects a reverse fan-out exceeding MaxReverseFanOut", func() {
			startWith(server.WithLimits(server.Limits{MaxReverseFanOut: 1}))
			push(
				putMutation("c1", 1, "project", "P", &testdatav1.Project{Id: "P"}),
				putMutation("c1", 2, "issue", "i1", issue("i1", "one", "P")),
				putMutation("c1", 3, "issue", "i2", issue("i2", "two", "P")),
			)
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
				Subscriptions: []*syncplanev1.Subscription{{
					Entity: "project",
					Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
				}},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
			Expect(err).To(MatchError(ContainSubstring(
				"over MaxReverseFanOut (1); raise it with server.WithLimits")))
		})

		It("names the entity's relations when a follow segment is unknown", func() {
			startWith()
			_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
				Subscriptions: []*syncplanev1.Subscription{{
					Entity: "project",
					Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issue"}}}},
				}},
			}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			// Segment names are derived, not spelled out in the .proto, so the
			// error has to show the set to be debuggable (issues/072).
			Expect(err).To(MatchError(ContainSubstring(
				`follow segment "issue" is not a relation of entity "project" (relations: issues)`)))
		})
	})
})

// Forward refs and reverse relations share ONE segment namespace per entity, so
// a name can only mean one relation. The compiled test entities cannot collide
// (their names are fixed in the .proto), so this reaches for dynamic entities
// to build the schema that does: post.comments is a forward ref list, and the
// reverse of comment.post is auto-named pluralize("comment") = "comments" on
// post as well.
var _ = Describe("Follow segment namespace", func() {
	post := server.DynamicEntity{
		Name: "post", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "comments", Number: 2, Ref: "comment", Repeated: true},
		},
	}
	comment := server.DynamicEntity{
		Name: "comment", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "post", Number: 2, Ref: "post"},
		},
	}

	// Resolution is forward-first, so letting this through would make the
	// reverse half unreachable AND answer the segment with a different row set
	// than the schema's author has any way to tell it apart from — a silently
	// wrong subscription rather than an empty one. Rejecting the graph is the
	// only reading that keeps the namespace honest.
	It("rejects a reverse relation a ref field of the same name would shadow", func() {
		ctx := context.Background()
		srv := newBareServer()
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(post, comment))
		Expect(err).NotTo(HaveOccurred())

		_, err = srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity: "post",
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"comments"}}}},
			}},
		}))
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeFailedPrecondition))
		Expect(err).To(MatchError(ContainSubstring(
			`reverse relation "comments" on entity "post" collides with the reference field of that name; ` +
				`rename a ref field to disambiguate`)))
	})
})
