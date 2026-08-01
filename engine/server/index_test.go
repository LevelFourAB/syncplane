package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Secondary indexes are a pure optimization: every read that consults one has
// a scan fallback, and the two must give byte-identical answers. So the tests
// that matter are DIFFERENTIAL — the same scenario run twice, once served from
// the index and once from the scan — rather than assertions about the index
// itself, which would only pin what the implementation happens to do.
//
// The rest of the server suite is the other half of this: it never builds an
// index, so it exercises the fallback on every path, unchanged.
var _ = Describe("Secondary indexes", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// scenario runs one reference-following pull against a server, returning
	// the live rows it delivered. built decides whether the reverse hop is
	// served from the index or from the scan.
	//
	// Everything else — the schema, the rows, the subscription — is identical
	// between the two, which is what makes a difference in the result a bug
	// rather than a difference in setup.
	scenario := func(built bool, opts ...server.Option) ([]string, []string) {
		GinkgoHelper()
		srv, _ := newTestServerStore(opts...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "One"}),
			putMutation("c1", 2, "project", "p2", &testdatav1.Project{Id: "p2", Name: "Two"}),
			putMutation("c1", 3, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Title: "a", Project: &testdatav1.Project{Id: "p1"},
			}),
			putMutation("c1", 4, "issue", "i2", &testdatav1.Issue{
				Id: "i2", Title: "b", Project: &testdatav1.Project{Id: "p2"},
			}),
			putMutation("c1", 5, "issue", "i3", &testdatav1.Issue{
				Id: "i3", Title: "c", Project: &testdatav1.Project{Id: "p1"},
			}),
			// No project at all: must be absent from both paths.
			putMutation("c1", 6, "issue", "i4", &testdatav1.Issue{Id: "i4", Title: "d"}),
		})))
		Expect(err).NotTo(HaveOccurred())

		if built {
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		}

		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity: "project",
				Follow: &syncplanev1.FollowSpec{Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}}},
			}},
		}))
		Expect(err).NotTo(HaveOccurred())

		var live, dead []string
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetTombstone() {
				dead = append(dead, rc.GetEntity()+"/"+rc.GetKey())
			} else {
				live = append(live, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
		return live, dead
	}

	It("serves a reverse follow identically from the index and from a scan", func() {
		scanLive, scanDead := scenario(false)
		indexLive, indexDead := scenario(true)
		Expect(indexLive).To(ConsistOf(scanLive))
		Expect(indexDead).To(ConsistOf(scanDead))
		Expect(scanLive).To(ConsistOf(
			"project/p1", "project/p2", "issue/i1", "issue/i2", "issue/i3"),
			"i4 holds no project, so no path reaches it")
	})

	It("falls back to the scan when indexes are disabled, even once built", func() {
		// DisableIndexes must not change the answer — that is the whole point
		// of the switch, and what lets the rest of the suite run either way.
		disabled := server.Limits{DisableIndexes: true}
		live, dead := scenario(true, server.WithLimits(disabled))
		scanLive, scanDead := scenario(false)
		Expect(live).To(ConsistOf(scanLive))
		Expect(dead).To(ConsistOf(scanDead))
	})

	It("counts a followed row once when the reverse hop hits its budget", func() {
		// Same fallback hazard as the query path: the probe walk collects
		// matches as it goes and only then discovers it has considered more
		// candidates than the bound allows, so the scan that follows must not
		// re-collect what it already has. p1 holds two issues, so a budget of
		// one forces the fallback with a match already in hand — and a fan-out
		// bound of two, which those two issues sit exactly on, fails the pull
		// outright if the fallback double-counts them.
		live, dead := scenario(true, server.WithLimits(server.Limits{
			MaxIndexScanCandidates: 1,
			MaxReverseFanOut:       2,
		}))
		scanLive, scanDead := scenario(false)
		Expect(live).To(ConsistOf(scanLive))
		Expect(dead).To(ConsistOf(scanDead))
	})

	It("falls back to the scan while the index is not built", func() {
		// The default test server never runs a build pass, so this is the
		// state every other spec in the suite is in.
		live, _ := scenario(false)
		Expect(live).To(ContainElement("issue/i1"))
	})

	Describe("follow seeds", func() {
		// The other end of a following pull: before it can walk anything it has
		// to find the rows to walk FROM, and a predicated subscription narrows
		// those the same way a query does. Same differential rule — the seeds an
		// index finds and the seeds a scan finds must be the same seeds, which
		// here means the same closure.
		stateEq := func(state string) *syncplanev1.Condition {
			return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path:  []string{"state"},
				Op:    syncplanev1.CmpOp_CMP_OP_EQ,
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: state}},
			}}}
		}

		seeded := func(built bool, opts ...server.Option) []string {
			GinkgoHelper()
			srv, _ := newTestServerStore(opts...)
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

			issue := func(id, state, project string) *testdatav1.Issue {
				i := &testdatav1.Issue{Id: id, Title: id, State: state}
				if project != "" {
					i.Project = &testdatav1.Project{Id: project}
				}
				return i
			}
			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "One"}),
				putMutation("c1", 2, "project", "p2", &testdatav1.Project{Id: "p2", Name: "Two"}),
				putMutation("c1", 3, "project", "p3", &testdatav1.Project{Id: "p3", Name: "Three"}),
				putMutation("c1", 4, "issue", "i1", issue("i1", "open", "p1")),
				// Out of base scope, so p2 must stay out of the closure too.
				putMutation("c1", 5, "issue", "i2", issue("i2", "done", "p2")),
				putMutation("c1", 6, "issue", "i3", issue("i3", "open", "p3")),
				// In scope, holds no project: seeds the walk, reaches nothing.
				putMutation("c1", 7, "issue", "i4", issue("i4", "open", "")),
			})))
			Expect(err).NotTo(HaveOccurred())

			if built {
				Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			}

			resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws,
				Subscriptions: []*syncplanev1.Subscription{{
					Entity:    "issue",
					Predicate: stateEq("open"),
					Follow: &syncplanev1.FollowSpec{
						Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
					},
				}},
			}))
			Expect(err).NotTo(HaveOccurred())

			var live []string
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				if !rc.GetTombstone() {
					live = append(live, rc.GetEntity()+"/"+rc.GetKey())
				}
			}
			return live
		}

		It("finds the same seeds from the index as from a scan", func() {
			scanned := seeded(false)
			Expect(seeded(true)).To(ConsistOf(scanned))
			Expect(scanned).To(ConsistOf(
				"issue/i1", "issue/i3", "issue/i4", "project/p1", "project/p3"),
				"p2 is reachable only from an out-of-scope issue")
		})

		It("seeds identically when the walk gives up on its candidate budget", func() {
			// Three rows match `state = "open"`, so a budget of one gives up
			// with seeds already collected — the case where falling back is not
			// enough on its own and what was collected has to be dropped first.
			Expect(seeded(true, server.WithLimits(server.Limits{MaxIndexScanCandidates: 1}))).
				To(ConsistOf(seeded(false)))
		})
	})

	Describe("planned queries", func() {
		// Every query below runs twice against identical data — once with the
		// indexes built, once with them disabled — and the two results must be
		// element-for-element equal, order included. That is the entire test:
		// the planner is allowed to decline any query, but never to answer one
		// differently from the scan.
		//
		// Issue carries every declaration surface: `state` is marked
		// `indexed` (one component), `by_state_rank` is a declared composite
		// over (state, rank desc) — the filter-plus-different-sort shape a
		// single-field index cannot serve at O(limit) — and `labels` is a
		// repeated scalar, where one row takes one position per element.
		seed := func(srv *server.Server) {
			GinkgoHelper()
			ctx := context.Background()
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				for _, r := range []struct {
					id      string
					state   string
					rank    int32
					labels  []string
					blocked []string
				}{
					{"i1", "open", 5, []string{"bug", "ui"}, []string{"i2", "i3"}},
					{"i2", "open", 9, []string{"ui"}, nil},
					{"i3", "done", 3, []string{"bug"}, nil},
					// No labels: still reachable through by_label_rank, which is
					// what the missing tag on an empty list is for.
					{"i4", "open", 1, nil, nil},
					// A duplicate element collapses to one entry, so this row must
					// come back once and not twice.
					{"i5", "done", 7, []string{"ui", "ui"}, []string{"i2"}},
					{"i6", "", 4, []string{"bug", "ui"}, nil}, // implicit-presence empty string, not missing
				} {
					row := &testdatav1.Issue{
						Id: r.id, Title: r.id, State: r.state, Rank: r.rank, Labels: r.labels,
					}
					for _, id := range r.blocked {
						row.BlockedBy = append(row.BlockedBy, &testdatav1.Issue{Id: id})
					}
					if err := tx.Put(row); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
		}

		// answer runs one query and returns the row keys in result order.
		answer := func(built bool, spec query.Spec) []string {
			GinkgoHelper()
			ctx := context.Background()
			var opts []server.Option
			if !built {
				opts = append(opts, server.WithLimits(server.Limits{DisableIndexes: true}))
			}
			srv, _ := newTestServerStore(opts...)
			seed(srv)
			if built {
				Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			}
			var keys []string
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(&testdatav1.Issue{}, spec, func(msg proto.Message, _ uint64) error {
					keys = append(keys, msg.(*testdatav1.Issue).GetId())
					return nil
				})
			})).To(Succeed())
			return keys
		}

		same := func(name string, spec query.Spec, want []string) {
			GinkgoHelper()
			It(name, func() {
				scanned := answer(false, spec)
				indexed := answer(true, spec)
				Expect(indexed).To(Equal(scanned), "index and scan must agree exactly")
				Expect(scanned).To(Equal(want))
			})
		}

		same("equality on an indexed field",
			query.Spec{Where: query.Eq("state", "open")},
			[]string{"i1", "i2", "i4"})

		same("equality plus an ordering the composite index supplies",
			query.Spec{
				Where:   query.Eq("state", "open"),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("rank")},
			},
			[]string{"i2", "i1", "i4"})

		same("equality, ordering and a limit",
			query.Spec{
				Where:   query.Eq("state", "open"),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("rank")},
				Limit:   2,
			},
			[]string{"i2", "i1"})

		same("an offset past the first page",
			query.Spec{
				Where:   query.Eq("state", "open"),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("rank")},
				Offset:  1,
				Limit:   1,
			},
			[]string{"i1"})

		same("an ordering with no predicate",
			query.Spec{OrderBy: []*syncplanev1.OrderKey{query.Asc("state")}},
			[]string{"i6", "i3", "i5", "i1", "i2", "i4"})

		same("in over an indexed field becomes several probes",
			query.Spec{Where: query.In("state", "open", "done")},
			[]string{"i1", "i2", "i3", "i4", "i5"})

		same("an inequality bounds the range",
			query.Spec{
				Where:   query.Gt("rank", 4),
				OrderBy: []*syncplanev1.OrderKey{query.Asc("rank")},
			},
			[]string{"i1", "i5", "i2"})

		same("a residual term is re-checked after the range",
			query.Spec{Where: query.And(query.Eq("state", "open"), query.Gte("rank", 5))},
			[]string{"i1", "i2"})

		same("matching the empty string is not the same as matching nothing",
			query.Spec{Where: query.Eq("state", "")},
			[]string{"i6"})

		same("contains is never index-servable and falls back",
			query.Spec{Where: query.Contains("title", "i1")},
			[]string{"i1"})

		same("or is not servable in v1 and falls back",
			query.Spec{Where: query.Or(query.Eq("state", "done"), query.Eq("rank", 1))},
			[]string{"i3", "i4", "i5"})

		same("a predicate matching nothing",
			query.Spec{Where: query.Eq("state", "archived")},
			nil)

		// Fan-out: one row, several index entries. Every case here is a place
		// the index could return a row twice, or miss one whose list is empty.
		same("membership over a repeated field",
			query.Spec{Where: query.Has("labels", "bug")},
			[]string{"i1", "i3", "i6"})

		same("a row whose element repeats is still returned once",
			query.Spec{Where: query.Has("labels", "ui")},
			[]string{"i1", "i2", "i5", "i6"})

		same("membership plus the ordering the fan-out index supplies",
			query.Spec{
				Where:   query.Has("labels", "ui"),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("rank")},
			},
			[]string{"i2", "i5", "i1", "i6"})

		same("membership, ordering and a limit",
			query.Spec{
				Where:   query.Has("labels", "ui"),
				OrderBy: []*syncplanev1.OrderKey{query.Desc("rank")},
				Limit:   2,
			},
			[]string{"i2", "i5"})

		same("hasAny returns each matching row once, across several ranges",
			query.Spec{Where: query.HasAny("labels", "bug", "ui")},
			[]string{"i1", "i2", "i3", "i5", "i6"})

		same("hasAll re-checks what the range cannot pin",
			query.Spec{Where: query.HasAll("labels", "bug", "ui")},
			[]string{"i1", "i6"})

		same("isEmpty finds the row with no elements",
			query.Spec{Where: query.IsEmpty("labels")},
			[]string{"i4"})

		// A to-many reference is indexed by the engine with no declaration at
		// all — one entry per referenced key — which is what turns "who blocks
		// on i2" from a scan of the entity into a range read.
		same("membership over a to-many reference",
			query.Spec{Where: query.Has("blocked_by", "i2")},
			[]string{"i1", "i5"})

		same("any() over a to-many reference, which has spells more briefly",
			query.Spec{Where: query.Any("blocked_by", query.Eq("id", "i3"))},
			[]string{"i1"})

		same("hasAll over a to-many reference needs every key on one row",
			query.Spec{Where: query.HasAll("blocked_by", "i2", "i3")},
			[]string{"i1"})

		same("a predicate on another field still sees rows with an empty list",
			// by_label_rank pins nothing here, so its range spans every entry —
			// and i4, whose list is empty, must be in it.
			query.Spec{
				Where:   query.Eq("state", "open"),
				OrderBy: []*syncplanev1.OrderKey{query.Asc("rank")},
			},
			[]string{"i4", "i1", "i2"})

		same("a negated term stays residual",
			query.Spec{Where: query.Not(query.Eq("state", "open"))},
			[]string{"i3", "i5", "i6"})

		It("gives up on the candidate budget without emitting what the scan re-emits", func() {
			// The budget can only be exceeded by a walk that has already offered
			// rows, so falling back is not enough: everything the index path put
			// in front of the caller has to be taken back before the scan puts
			// it there again. Three "open" issues against a budget of two is the
			// smallest shape that trips it.
			srv, _ := newTestServerStore(server.WithLimits(server.Limits{MaxIndexScanCandidates: 2}))
			seed(srv)
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())

			var keys []string
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(
					&testdatav1.Issue{},
					query.Spec{Where: query.Eq("state", "open")},
					func(msg proto.Message, _ uint64) error {
						keys = append(keys, msg.(*testdatav1.Issue).GetId())
						return nil
					},
				)
			})).To(Succeed())
			Expect(keys).To(Equal([]string{"i1", "i2", "i4"}), "each matching row exactly once")
		})
	})

	Describe("declaration surface", func() {
		It("compiles both surfaces into indexes on the entity", func() {
			srv, store := newTestServerStore()
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			// The field mark and the declared composite both became real,
			// separately named indexes; the ref indexes are there too.
			for _, name := range []string{"__field_state", "by_state_rank", "__ref_project"} {
				Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
					_, ready, err := tx.IndexState("issue", name)
					Expect(err).NotTo(HaveOccurred())
					Expect(ready).To(BeTrue(), "index %q should exist and be built", name)
					return nil
				})).To(Succeed())
			}
		})

		// The dynamic surface mirrors the compiled one, so registering
		// through it is the cheapest way to assert what a bad declaration
		// does — and it also pins that the mirror actually reaches the same
		// validation.
		registerIndexed := func(fields []server.DynamicField, indexes ...server.DynamicIndex) error {
			srv, err := server.New(newTestStore())
			Expect(err).NotTo(HaveOccurred())
			return srv.RegisterDynamicEntity(server.DynamicEntity{
				Name:      "note",
				KeyFields: []string{"id"},
				Fields: append([]server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				}, fields...),
				Indexes: indexes,
			})
		}
		label := []server.DynamicField{{Name: "label", Number: 2, Kind: protoreflect.StringKind}}

		It("rejects an index name in the engine's reserved namespace", func() {
			err := registerIndexed(label, server.DynamicIndex{
				Name: "__mine", Key: []server.DynamicIndexKey{{Path: "label"}},
			})
			Expect(err).To(MatchError(ContainSubstring("reserved")))
		})

		It("rejects an index with no key components", func() {
			err := registerIndexed(label, server.DynamicIndex{Name: "empty"})
			Expect(err).To(MatchError(ContainSubstring("no key components")))
		})

		It("rejects an index over a collaborative text field", func() {
			err := registerIndexed(
				[]server.DynamicField{{Name: "body", Number: 2, Kind: protoreflect.BytesKind, Text: true}},
				server.DynamicIndex{Name: "by_body", Key: []server.DynamicIndexKey{{Path: "body"}}})
			Expect(err).To(MatchError(ContainSubstring("collaborative text field cannot be indexed")))
		})

		It("rejects a field-level mark on a collaborative text field", func() {
			err := registerIndexed([]server.DynamicField{
				{Name: "body", Number: 2, Kind: protoreflect.BytesKind, Text: true, Indexed: true},
			})
			Expect(err).To(MatchError(ContainSubstring("collaborative text field cannot be indexed")))
		})

		It("rejects an index over a leaf no comparison can reach", func() {
			err := registerIndexed(
				[]server.DynamicField{{Name: "raw", Number: 2, Kind: protoreflect.BytesKind}},
				server.DynamicIndex{Name: "by_raw", Key: []server.DynamicIndexKey{{Path: "raw"}}})
			Expect(err).To(MatchError(ContainSubstring("not comparable")))
		})

		It("accepts an index over a repeated scalar, which stores one entry per element", func() {
			err := registerIndexed(
				[]server.DynamicField{{Name: "tags", Number: 2, Kind: protoreflect.StringKind, Repeated: true}},
				server.DynamicIndex{Name: "by_tags", Key: []server.DynamicIndexKey{{Path: "tags"}}})
			Expect(err).To(Succeed())
		})

		It("rejects two indexes with the same name", func() {
			dup := server.DynamicIndex{Name: "dup", Key: []server.DynamicIndexKey{{Path: "label"}}}
			Expect(registerIndexed(label, dup, dup)).To(MatchError(ContainSubstring("declared twice")))
		})

		It("accepts a field-level mark, desugared to a one-component index", func() {
			Expect(registerIndexed([]server.DynamicField{
				{Name: "label", Number: 2, Kind: protoreflect.StringKind, Indexed: true},
			})).To(Succeed())
		})
	})

	Describe("maintenance", func() {
		var (
			srv    *server.Server
			store  storage.Adapter
			client syncplanev1connect.SyncServiceClient
		)

		BeforeEach(func() {
			srv, store = newTestServerStore()
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		})

		push := func(muts ...*syncplanev1.Mutation) {
			GinkgoHelper()
			_, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
			Expect(err).NotTo(HaveOccurred())
		}

		It("writes an entry when a row with a reference is created", func() {
			push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Project: &testdatav1.Project{Id: "p1"},
			}))
			Expect(entriesOf(store, "issue", "__ref_project")).To(Equal([]string{"i1"}))
		})

		It("moves the entry when a PATCH re-points the reference", func() {
			push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Project: &testdatav1.Project{Id: "p1"},
			}))
			before := indexKeysOf(store, "issue", "__ref_project")
			push(patchMutation("c1", 2, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Project: &testdatav1.Project{Id: "p2"},
			}, "project"))
			after := indexKeysOf(store, "issue", "__ref_project")
			Expect(after).To(HaveLen(1), "one row, one entry — the old one is replaced, not added to")
			Expect(after[0]).NotTo(Equal(before[0]), "a re-point moves the row in the index")
		})

		It("removes the entry when the row is deleted", func() {
			push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Project: &testdatav1.Project{Id: "p1"},
			}))
			push(deleteMutation("c1", 2, "issue", "i1"))
			Expect(entriesOf(store, "issue", "__ref_project")).To(BeEmpty(),
				"entries exist for live rows only")
		})

		It("indexes a row whose reference is unset, under the missing tag", func() {
			// Every live row occupies a position in every index of its entity,
			// including rows the index cannot say anything useful about. That
			// is what lets an ordered scan of the index BE the ordered scan of
			// the entity, missing values and all.
			push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{Id: "i1"}))
			Expect(entriesOf(store, "issue", "__ref_project")).To(Equal([]string{"i1"}))
		})

		It("marks an index ready only once the build has walked the entity", func() {
			push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Project: &testdatav1.Project{Id: "p1"},
			}))
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				_, ready, err := tx.IndexState("issue", "__ref_project")
				Expect(err).NotTo(HaveOccurred())
				Expect(ready).To(BeFalse(), "maintenance alone never claims coverage")
				return nil
			})).To(Succeed())

			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				hash, ready, err := tx.IndexState("issue", "__ref_project")
				Expect(err).NotTo(HaveOccurred())
				Expect(ready).To(BeTrue())
				Expect(hash).NotTo(BeEmpty())
				return nil
			})).To(Succeed())
		})

		It("backfills rows that predate the build", func() {
			// Written with maintenance live, so the entries are already there;
			// what the build adds is the CLAIM that they cover everything.
			// Re-running it must be a no-op rather than a duplication.
			push(putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{
				Id: "i1", Project: &testdatav1.Project{Id: "p1"},
			}))
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			Expect(entriesOf(store, "issue", "__ref_project")).To(Equal([]string{"i1"}))
		})
	})

	Describe("isolation between entities", func() {
		// Index names are derived from FIELD names, so any two entities marking
		// a field of the same name claim the same one — "__field_state" belongs
		// to `issue` and to the dynamic `note` below alike. An entry names a row
		// key and nothing else, so entries that are not partitioned by entity
		// hand one entity's row keys to the other's reader, which reads them in
		// ITS keyspace: same key, different row, and a predicate the row does
		// not satisfy.
		noteState := server.DynamicEntity{
			Name:      "note",
			KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "state", Number: 2, Kind: protoreflect.StringKind, Indexed: true},
			},
		}

		// Both rows take the key "i1" — in different entities, so they are
		// unrelated rows that a shared keyspace would conflate.
		seedCollision := func() (*server.Server, storage.Adapter) {
			GinkgoHelper()
			srv, store := newTestServerStore()
			Expect(srv.RegisterDynamicEntity(noteState)).To(Succeed())
			mt, err := server.BuildDynamicMessageType(noteState)
			Expect(err).NotTo(HaveOccurred())
			// Pushed rather than written through EntityTx: a mutation carries
			// encoded bytes, so the note decodes through the type the server
			// registered rather than this spec's separately built one.
			_, err = srv.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "issue", "i1", &testdatav1.Issue{Id: "i1", State: "closed"}),
				putMutation("c1", 2, "note", "i1", buildNote(mt, map[string]any{
					"id": "i1", "state": "open",
				})),
			})))
			Expect(err).NotTo(HaveOccurred())
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
			return srv, store
		}

		It("never answers one entity's query from another entity's entries", func() {
			srv, _ := seedCollision()
			var keys []string
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(
					&testdatav1.Issue{},
					query.Spec{Where: query.Eq("state", "open")},
					func(msg proto.Message, _ uint64) error {
						keys = append(keys, msg.(*testdatav1.Issue).GetId())
						return nil
					},
				)
			})).To(Succeed())
			Expect(keys).To(BeEmpty(),
				"the only issue is closed; the open row of the same key belongs to another entity")
		})

		It("scans only the entity's own entries", func() {
			_, store := seedCollision()
			Expect(entriesOf(store, "issue", "__field_state")).To(Equal([]string{"i1"}))
			Expect(entriesOf(store, "note", "__field_state")).To(Equal([]string{"i1"}))
			// Same name, same row key, two entries: which one a reader sees is
			// decided by the entity it asked for, not by the name alone.
			Expect(indexKeysOf(store, "issue", "__field_state")).NotTo(
				Equal(indexKeysOf(store, "note", "__field_state")))
		})
	})
})

// entriesOf returns the row keys one entity's index holds, in entry order.
// Reading an index directly is something no production path does; these exist
// because a differential test can only catch a maintenance bug that changes an
// ANSWER, and a stale or misfiled entry pointing at a row that is re-read and
// re-checked is invisible to one.
func entriesOf(store storage.Adapter, entity, index string) []string {
	GinkgoHelper()
	var out []string
	Expect(store.WithReadTx(context.Background(), ws, func(tx storage.ReadTx) error {
		return tx.ScanIndex(entity, index, nil, nil, 0, func(_ []byte, rowKey string) error {
			out = append(out, rowKey)
			return nil
		})
	})).To(Succeed())
	return out
}

// indexKeysOf returns the raw entry keys one entity's index holds.
func indexKeysOf(store storage.Adapter, entity, index string) []string {
	GinkgoHelper()
	var out []string
	Expect(store.WithReadTx(context.Background(), ws, func(tx storage.ReadTx) error {
		return tx.ScanIndex(entity, index, nil, nil, 0, func(entry []byte, _ string) error {
			out = append(out, string(entry))
			return nil
		})
	})).To(Succeed())
	return out
}
