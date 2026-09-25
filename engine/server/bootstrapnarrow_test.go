package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// A partial snapshot only has to ship the rows its scope admits. Where the
// server can name those rows without reading the rest — an index over the
// predicate, a key the predicate pins, a follow closure, a window's membership
// — it reads them and nothing else; every other shape walks the entity. The
// result is identical either way, which is why each spec here pins both the
// rows and what they cost.
var _ = Describe("Bootstrap candidate narrowing", func() {
	var (
		ctx     context.Context
		client  syncplanev1connect.SyncServiceClient
		counter *readCountingAdapter
	)

	serveOn := func(srv *server.Server) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	// startCounting serves a fresh workspace over a store that counts its
	// reads, and reports whether the entities' indexes were built.
	startCounting := func(buildIndexes bool, opts ...server.Option) *server.Server {
		GinkgoHelper()
		counter = &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
		srv := mustNewServer(counter, append([]server.Option{testEntities()}, opts...)...)
		if buildIndexes {
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		}
		client = serveOn(srv)
		return srv
	}

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	// attempt is one Bootstrap stream: what Begin announced, the rows it
	// delivered and the resume token of the last chunk it took.
	type attempt struct {
		begin *syncplanev1.BootstrapResponse_Begin
		rows  []*syncplanev1.RowChange
		token []byte
		ended bool
	}
	// run takes `chunks` chunks before hanging up, standing in for a dropped
	// connection; 0 takes the whole stream.
	run := func(c syncplanev1connect.SyncServiceClient, req *syncplanev1.BootstrapRequest, chunks int) attempt {
		GinkgoHelper()
		req.WorkspaceId = ws
		req.ClientId = "c1"
		stream, err := c.Bootstrap(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(stream.Close()).To(Succeed()) }()
		var out attempt
		seen := 0
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				out.begin = body.Begin
			case *syncplanev1.BootstrapResponse_Chunk_:
				out.rows = append(out.rows, body.Chunk.GetRows()...)
				out.token = body.Chunk.GetResumeToken()
				seen++
				if chunks > 0 && seen == chunks {
					return out
				}
			case *syncplanev1.BootstrapResponse_End_:
				out.ended = true
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		return out
	}

	bootstrap := func(subs ...*syncplanev1.Subscription) attempt {
		GinkgoHelper()
		return run(client, &syncplanev1.BootstrapRequest{Subscriptions: subs}, 0)
	}

	keysOf := func(rows []*syncplanev1.RowChange, entity string) []string {
		var out []string
		for _, rc := range rows {
			if rc.GetEntity() == entity {
				out = append(out, rc.GetKey())
			}
		}
		return out
	}

	stateEq := func(state string) *syncplanev1.Condition {
		return stringIs("state", state)
	}

	idEq := func(id string) *syncplanev1.Condition {
		return stringIs("id", id)
	}

	issueSub := func(pred *syncplanev1.Condition) *syncplanev1.Subscription {
		return &syncplanev1.Subscription{Entity: "issue", Predicate: pred}
	}

	// seedIssues writes three issues per state, titled t0..t2 within each so a
	// read filter can hide one of them.
	seedIssues := func(states ...string) {
		GinkgoHelper()
		var muts []*syncplanev1.Mutation
		id := uint64(0)
		for _, state := range states {
			for i := 0; i < 3; i++ {
				id++
				key := fmt.Sprintf("i-%s-%d", state, i)
				muts = append(muts, putMutation("c1", id, "issue", key,
					&testdatav1.Issue{Id: key, State: state, Title: fmt.Sprintf("t%d", i)}))
			}
		}
		push(muts...)
	}

	issueKeys := func(state string) []string {
		var out []string
		for i := 0; i < 3; i++ {
			out = append(out, fmt.Sprintf("i-%s-%d", state, i))
		}
		return out
	}

	BeforeEach(func() { ctx = context.Background() })

	It("ships an indexed predicate without walking the entity", func() {
		startCounting(true)
		seedIssues("open", "closed", "done")
		indexed := bootstrap(issueSub(stateEq("open")))
		indexedScans := counter.count("scanafter:issue")

		// The same scope over the same rows, with the indexes unbuilt.
		startCounting(false)
		seedIssues("open", "closed", "done")
		scanned := bootstrap(issueSub(stateEq("open")))
		scannedScans := counter.count("scanafter:issue")

		Expect(keysOf(indexed.rows, "issue")).To(Equal(issueKeys("open")))
		Expect(keysOf(scanned.rows, "issue")).To(Equal(issueKeys("open")),
			"both paths must ship the same rows")
		Expect(indexedScans).To(BeZero(), "an indexed predicate must not walk the entity")
		Expect(scannedScans).To(BeNumerically(">", 0),
			"without a built index the snapshot walks the entity")
	})

	It("reads each candidate row once, not once to name it and once to ship it", func() {
		startCounting(true)
		seedIssues("open", "closed", "done")
		counter.reset()

		out := bootstrap(issueSub(stateEq("open")))
		Expect(keysOf(out.rows, "issue")).To(Equal(issueKeys("open")))
		Expect(counter.count("batchkeys")).To(Equal(3),
			"naming the candidates costs no row read; only shipping them does")
		Expect(counter.count("get")).To(BeZero())
	})

	It("ships a predicate pinning the key without walking the entity", func() {
		// No indexes built, and the key field carries none of its own: the
		// equality names the row outright.
		startCounting(false)
		seedIssues("open", "closed")
		out := bootstrap(issueSub(idEq("i-open-1")))

		Expect(keysOf(out.rows, "issue")).To(Equal([]string{"i-open-1"}))
		Expect(counter.count("scanafter:issue")).To(BeZero())
	})

	It("ships a follow-only entity's closure without walking it", func() {
		startCounting(true)
		push(
			putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "one"}),
			putMutation("c1", 2, "project", "p2", &testdatav1.Project{Id: "p2", Name: "two"}),
		)
		var muts []*syncplanev1.Mutation
		for i := 0; i < 4; i++ {
			key := fmt.Sprintf("i-%d", i)
			project := "p1"
			if i%2 == 1 {
				project = "p2"
			}
			muts = append(muts, putMutation("c1", uint64(3+i), "issue", key,
				&testdatav1.Issue{Id: key, Project: &testdatav1.Project{Id: project}}))
		}
		push(muts...)
		counter.reset()

		// `issue` has no base scope of its own: the follow spec is the only
		// thing that reaches it, so its visible rows are exactly the closure.
		out := bootstrap(&syncplanev1.Subscription{
			Entity:    "project",
			Predicate: stringIs("id", "p1"),
			Follow: &syncplanev1.FollowSpec{
				Paths: []*syncplanev1.RefPath{{Segments: []string{"issuesByProject"}}},
			},
		})

		Expect(keysOf(out.rows, "project")).To(Equal([]string{"p1"}))
		Expect(keysOf(out.rows, "issue")).To(Equal([]string{"i-0", "i-2"}))
		Expect(counter.count("scanafter:issue")).To(BeZero(),
			"a follow-only entity ships its closure members and nothing else")
	})

	It("ships a windowed scope's members without walking the entity", func() {
		startCounting(true)
		for i := 1; i <= 5; i++ {
			push(putMutation("c1", uint64(i), "widget", fmt.Sprintf("w%d", i),
				&testdatav1.Widget{Id: fmt.Sprintf("w%d", i), Count: int32(i)}))
		}
		counter.reset()

		out := bootstrap(recencySub(2))
		Expect(keysOf(out.rows, "widget")).To(ConsistOf("w4", "w5"))
		Expect(counter.count("scanafter:widget")).To(BeZero(),
			"a window-only scope ships exactly its membership")
	})

	It("applies read filters on the candidate path", func() {
		startCounting(true, server.WithPermissions(hidingPermissions{}))
		seedIssues("open")
		counter.reset()

		out := bootstrap(issueSub(stateEq("open")))
		Expect(keysOf(out.rows, "issue")).To(Equal([]string{"i-open-1", "i-open-2"}),
			"the filter hides the row titled t0")
		Expect(counter.count("scanafter:issue")).To(BeZero())
	})

	It("serves only the rows an interrupted candidate walk did not deliver", func() {
		startCounting(true, server.WithChunkSize(2))
		seedIssues("open", "closed")
		counter.reset()

		first := run(client, &syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
		}, 1)
		Expect(first.rows).To(HaveLen(2))

		second := run(client, &syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  first.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeTrue())
		Expect(second.ended).To(BeTrue())
		Expect(keysOf(second.rows, "issue")).NotTo(ContainElements(keysOf(first.rows, "issue")))
		Expect(append(keysOf(first.rows, "issue"), keysOf(second.rows, "issue")...)).
			To(ConsistOf(issueKeys("open")))
		Expect(counter.count("scanafter:issue")).To(BeZero())
	})

	It("resumes a candidate walk on the scan path", func() {
		// Two servers over one store: the first narrows candidates, the second
		// has indexes turned off and can only scan. The resume position means
		// the same thing to both.
		store := &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
		counter = store
		indexed := mustNewServer(store, testEntities(), server.WithChunkSize(2))
		Expect(indexed.BuildIndexes(ctx, ws)).To(Succeed())
		client = serveOn(indexed)
		seedIssues("open", "closed")

		scanning := serveOn(mustNewServer(store, testEntities(), server.WithChunkSize(2),
			server.WithLimits(server.Limits{DisableIndexes: true})))

		first := run(client, &syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
		}, 1)
		Expect(first.rows).To(HaveLen(2))

		second := run(scanning, &syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  first.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeTrue())
		Expect(second.ended).To(BeTrue())
		Expect(append(keysOf(first.rows, "issue"), keysOf(second.rows, "issue")...)).
			To(ConsistOf(issueKeys("open")))
	})

	It("resumes a scan on the candidate path", func() {
		store := &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
		counter = store
		srv := mustNewServer(store, testEntities(), server.WithChunkSize(2))
		client = serveOn(srv)
		seedIssues("open", "closed")

		first := run(client, &syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
		}, 1)
		Expect(first.rows).To(HaveLen(2))

		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		second := run(client, &syncplanev1.BootstrapRequest{
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
			Resume: &syncplanev1.BootstrapResume{
				Cursor: first.begin.GetCursor(),
				Token:  first.token,
			},
		}, 0)
		Expect(second.begin.GetResumed()).To(BeTrue())
		Expect(append(keysOf(first.rows, "issue"), keysOf(second.rows, "issue")...)).
			To(ConsistOf(issueKeys("open")))
	})

	It("falls back to the scan when the candidates outgrow the budget", func() {
		startCounting(true, server.WithLimits(server.Limits{
			// Three candidates against a budget of two: an index naming most
			// of an entity is no cheaper than walking it.
			MaxPullChanges: 2,
			MaxResyncScan:  2,
		}))
		seedIssues("open", "closed")
		counter.reset()

		out := bootstrap(issueSub(stateEq("open")))
		Expect(keysOf(out.rows, "issue")).To(Equal(issueKeys("open")))
		Expect(counter.count("scanafter:issue")).To(BeNumerically(">", 0),
			"an over-budget candidate set falls back to the scan")
	})

	It("leaves a row written after Begin to the follow-up pull", func() {
		startCounting(true)
		seedIssues("open")
		out := bootstrap(issueSub(stateEq("open")))
		Expect(keysOf(out.rows, "issue")).To(Equal(issueKeys("open")))

		// A row entering the predicate's match set after the snapshot's
		// candidates were enumerated carries a version above the Begin seq, so
		// the pull from the Begin cursor delivers it.
		push(putMutation("c1", 4, "issue", "i-open-late",
			&testdatav1.Issue{Id: "i-open-late", State: "open"}))

		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Cursor:        out.begin.GetCursor(),
			Subscriptions: []*syncplanev1.Subscription{issueSub(stateEq("open"))},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetReset_()).To(BeFalse())
		Expect(keysOf(resp.Msg.GetPatch().GetChanges(), "issue")).To(Equal([]string{"i-open-late"}))
	})
})
