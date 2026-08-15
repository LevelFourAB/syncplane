package effects_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/effects"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// init quiets the engine's default-logger output for the whole test binary so
// specs don't spam stderr.
func init() { slog.SetDefault(slog.New(slog.DiscardHandler)) }

func TestEffects(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Effects Suite")
}

// fixture is a server over a temp store plus a runtime with no consumers yet.
type fixture struct {
	srv *server.Server
	rt  *effects.Runtime
}

func newFixture(opts ...func(*effects.Config)) *fixture {
	GinkgoHelper()
	return newFixtureFull(nil, opts...)
}

// newFixtureFull builds the fixture over a server configured with srvOpts —
// specs that need GC (and with it a moving tombstone horizon) pass WithGC here.
func newFixtureFull(srvOpts []server.Option, opts ...func(*effects.Config)) *fixture {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "engine.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

	srv, err := server.New(store, append(srvOpts,
		server.WithEntities(&testdatav1.Widget{}, &testdatav1.Pair{}))...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })

	cfg := effects.Config{Server: srv}
	for _, opt := range opts {
		opt(&cfg)
	}
	rt, err := effects.New(cfg)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(rt.Close()).To(Succeed()) })
	return &fixture{srv: srv, rt: rt}
}

// put writes a widget through the trusted server API, the same linearization
// point a client push takes.
func (f *fixture) put(ctx context.Context, ws string, w *testdatav1.Widget) {
	GinkgoHelper()
	Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.Put(w)
	})).To(Succeed())
}

func (f *fixture) putPair(ctx context.Context, ws string, p *testdatav1.Pair) {
	GinkgoHelper()
	Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.Put(p)
	})).To(Succeed())
}

// collectGarbage runs a GC pass, advancing the tombstone horizon past every
// tombstone written so far. Specs that call it build the server with a negative
// retention, which puts the cutoff in the future so everything already written
// is immediately collectable.
func (f *fixture) collectGarbage(ctx context.Context, ws string) {
	GinkgoHelper()
	Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())
}

func (f *fixture) delete(ctx context.Context, ws, id string) {
	GinkgoHelper()
	Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.Delete(&testdatav1.Widget{}, id)
	})).To(Succeed())
}

// recorder collects the events a handler saw, and can be told to fail on a
// given key to exercise the wedge.
type recorder struct {
	mu     sync.Mutex
	seen   []effects.Event[*testdatav1.Widget]
	failOn map[string]error
}

func newRecorder() *recorder {
	return &recorder{failOn: make(map[string]error)}
}

func (r *recorder) handle(_ context.Context, ev effects.Event[*testdatav1.Widget]) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err, ok := r.failOn[ev.Key]; ok {
		return err
	}
	r.seen = append(r.seen, ev)
	return nil
}

func (r *recorder) events() []effects.Event[*testdatav1.Widget] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]effects.Event[*testdatav1.Widget](nil), r.seen...)
}

func (r *recorder) keys() []string {
	GinkgoHelper()
	var out []string
	for _, ev := range r.events() {
		out = append(out, ev.Key)
	}
	return out
}

func (r *recorder) fail(key string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failOn[key] = err
}

func (r *recorder) recover(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failOn, key)
}

// blocker is a handler that parks every delivery until it is released, and
// reports how many deliveries are parked in it at once. It is how the specs
// observe the worker pool: concurrency is only visible while handlers are still
// running.
type blocker struct {
	entered chan string
	release chan struct{}

	mu   sync.Mutex
	live int
	peak int
}

func newBlocker(capacity int) *blocker {
	return &blocker{entered: make(chan string, capacity), release: make(chan struct{})}
}

func (b *blocker) handle(_ context.Context, ev effects.Event[*testdatav1.Widget]) error {
	b.mu.Lock()
	b.live++
	if b.live > b.peak {
		b.peak = b.live
	}
	b.mu.Unlock()

	b.entered <- ev.Workspace
	<-b.release

	b.mu.Lock()
	b.live--
	b.mu.Unlock()
	return nil
}

// inFlight is how many deliveries are parked in the handler right now.
func (b *blocker) inFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.live
}

// highWater is the most deliveries that were ever parked at once.
func (b *blocker) highWater() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

// releaseAll lets every parked delivery through, and every later one straight
// past.
func (b *blocker) releaseAll() { close(b.release) }

// deafNotifier is a notifier that fans out per-workspace pokes but cannot
// report activity — the shape a shared notifier has before it grows the
// Activity extension, and the one that leaves the sweep as the only way to find
// work. It deliberately offers no Unwrap, so nothing recovers the extension off
// the notifier it delegates to.
type deafNotifier struct{ inner *pubsub.Local }

func newDeafNotifier() deafNotifier { return deafNotifier{inner: pubsub.NewLocal()} }

func (d deafNotifier) Notify(workspaceID string) { d.inner.Notify(workspaceID) }

func (d deafNotifier) Subscribe(workspaceID string) (<-chan struct{}, func()) {
	return d.inner.Subscribe(workspaceID)
}

func (d deafNotifier) SubscribeSet(workspaceIDs []string) pubsub.Set {
	return d.inner.SubscribeSet(workspaceIDs)
}
