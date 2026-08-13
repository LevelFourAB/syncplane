package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// replicaCall records one read routed through the replica capability: which
// workspace, the freshness floor the server passed, and whether it asked for a
// primary-anchored snapshot instead of naming a floor itself.
type replicaCall struct {
	ws       string
	minSeq   uint64
	anchored bool
}

// fakeReplicaStore wraps a real adapter with a storage.ReplicaProvider whose
// reads delegate straight back to the same store. There is no second database:
// what the fake pins is the SERVER's routing — which paths route through the
// capability, with what floor — and, via reportStale, that a path keeps
// serving correct data while the adapter reports it fell back.
type fakeReplicaStore struct {
	storage.Adapter
	mu          sync.Mutex
	calls       []replicaCall
	reportStale bool
}

func (f *fakeReplicaStore) ReplicaReader() storage.ReplicaReader { return f }

func (f *fakeReplicaStore) record(c replicaCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *fakeReplicaStore) recorded() []replicaCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]replicaCall(nil), f.calls...)
}

func (f *fakeReplicaStore) route() storage.ReadRoute {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reportStale {
		return storage.ReadRouteFallbackStale
	}
	return storage.ReadRouteReplica
}

func (f *fakeReplicaStore) setStale(stale bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reportStale = stale
}

func (f *fakeReplicaStore) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func (f *fakeReplicaStore) WithReadTxMinSeq(ctx context.Context, ws string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	f.record(replicaCall{ws: ws, minSeq: minSeq})
	return f.route(), f.Adapter.WithReadTx(ctx, ws, fn)
}

func (f *fakeReplicaStore) WithReadTxAnchored(ctx context.Context, ws string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	f.record(replicaCall{ws: ws, anchored: true})
	return f.route(), f.Adapter.WithReadTx(ctx, ws, fn)
}

var _ = Describe("Replica read routing", func() {
	const ws = "ws1"
	var (
		ctx    context.Context
		fake   *fakeReplicaStore
		srv    *server.Server
		reader *sdkmetric.ManualReader
	)

	BeforeEach(func() {
		ctx = context.Background()
		fake = &fakeReplicaStore{Adapter: newTestStore()}
		reader = sdkmetric.NewManualReader()
		srv = mustNewServer(fake, testEntities(),
			server.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
		DeferCleanup(srv.Close)
	})

	// push applies n mutations with ids startID.. — sequential ids because the
	// per-client watermark refuses gaps and repeats across calls.
	push := func(startID, n int) {
		GinkgoHelper()
		muts := make([]*syncplanev1.Mutation, n)
		for i := range n {
			muts[i] = putMutation("c1", uint64(startID+i), "widget", "w1", &testdatav1.Widget{Id: "w1"})
		}
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		results := pushResults(resp.Msg)
		for _, r := range results {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	pull := func(cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			Cursor:      cursor,
			Subscriptions: []*syncplanev1.Subscription{
				{Entity: "widget"},
			},
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	It("floors a pull at its cursor's sequence", func() {
		push(1, 2)
		first := pull(nil)
		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: 0}),
			"an empty cursor carries no floor")
		Expect(first.GetReset_()).To(BeFalse())

		fake.reset()
		pull(first.GetNextCursor())
		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: 2}),
			"a resumed pull must not be served from behind its own cursor")
	})

	It("keeps serving correct pages when the adapter reports a fallback, and counts it", func() {
		push(1, 2)
		fake.setStale(true)
		resp := pull(nil)
		Expect(resp.GetReset_()).To(BeFalse())
		Expect(resp.GetPatch().GetChanges()).To(HaveLen(1),
			"a fallback read serves the primary's rows, coalesced by row")

		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(ctx, &rm)).To(Succeed())
		var points []metricdata.DataPoint[int64]
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name == "syncplane.replica.reads" {
					points = m.Data.(metricdata.Sum[int64]).DataPoints
				}
			}
		}
		want := attribute.String("syncplane.read_route", "fallback_stale")
		matched := false
		for _, p := range points {
			if v, ok := p.Attributes.Value(want.Key); ok && v == want.Value {
				matched = p.Value >= 1
			}
		}
		Expect(matched).To(BeTrue(), "the fallback must be visible on syncplane.replica.reads: %v", points)
	})

	It("floors the change feed at the caller's cursor", func() {
		push(1, 2)
		next, more, err := srv.Changes(ctx, ws, server.Cursor{}, 0, func(server.Change) error { return nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(more).To(BeFalse())
		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: 0}))

		// A snapshot from before the cursor was issued would read as "cursor
		// ahead of CurrentSeq" — a spurious ErrCursorTooOld. The floor is what
		// rules it out; resuming from the head must stay clean.
		fake.reset()
		_, _, err = srv.Changes(ctx, ws, next, 0, func(server.Change) error { return nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: next.Seq}))
		Expect(next.Seq).To(BeNumerically(">", 0))
	})

	It("anchors a bootstrap's Begin transaction and floors its page scans at the Begin seq", func() {
		push(1, 2)

		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			Subscriptions: []*syncplanev1.Subscription{
				{Entity: "widget"},
			},
		}))
		Expect(err).NotTo(HaveOccurred())
		var beginSeq uint64
		for stream.Receive() {
			if b := stream.Msg().GetBegin(); b != nil {
				beginSeq = b.GetSeq()
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(beginSeq).To(BeEquivalentTo(2))

		calls := fake.recorded()
		Expect(calls).To(ContainElement(replicaCall{ws: ws, anchored: true}),
			"the Begin cursor must be read-your-writes")
		Expect(calls).To(ContainElement(replicaCall{ws: ws, minSeq: beginSeq}),
			"page scans must not read from before the position Begin promised")
	})

	It("routes the index-readiness probe with no floor", func() {
		push(1, 1)
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: 0}),
			"the probe tolerates any staleness — the build's write txs re-check on the primary")
	})

	It("keeps position-comparing reads off the replica", func() {
		push(1, 1)
		fake.reset()

		_, _, err := srv.ConsumerPosition(ctx, ws, "c")
		Expect(err).NotTo(HaveOccurred())
		_, err = srv.ConsumerPositions(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		_, err = srv.ConsumerShadow(ctx, ws, "c", "widget", "w1")
		Expect(err).NotTo(HaveOccurred())
		_, err = srv.CurrentSeq(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error { return nil })).To(Succeed())

		Expect(fake.recorded()).To(BeEmpty(),
			"consumer positions, shadows, views and the lag denominator must read the primary")
	})

	It("raises a pull's floor to the wire's acked seq when it is past the cursor", func() {
		// The gap min_seq exists for: a cursor at seq 2 whose client has since
		// been acked writes up to seq 4. The cursor alone would admit a
		// snapshot missing those writes.
		push(1, 2)
		first := pull(nil)
		push(3, 2)

		fake.reset()
		resp, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			Cursor:        first.GetNextCursor(),
			Subscriptions: []*syncplanev1.Subscription{{Entity: "widget"}},
			MinSeq:        4,
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetReset_()).To(BeFalse())
		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: 4}))
	})

	It("raises a subscribe scope's floor to the scope's acked seq", func() {
		push(1, 2)
		first := pull(nil)
		push(3, 2)

		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		fake.reset()
		streamCtx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		stream, err := client.Subscribe(streamCtx, connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes: []*syncplanev1.WorkspaceScope{{
				WorkspaceId:   ws,
				Cursor:        first.GetNextCursor(),
				Subscriptions: []*syncplanev1.Subscription{{Entity: "widget"}},
				MinSeq:        4,
			}},
		}))
		Expect(err).NotTo(HaveOccurred())
		for stream.Receive() {
			if stream.Msg().GetCaughtUp() {
				break
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		cancel()
		Expect(stream.Close()).To(Succeed())

		Expect(fake.recorded()).To(ContainElement(replicaCall{ws: ws, minSeq: 4}),
			"the first page must be floored at the scope's acked seq, not its cursor")
	})
})
