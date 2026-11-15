package connect_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// replicaPair is an origin server over HTTP and a replica server linked to it
// with upstream, both with the record-mode test entities.
type replicaPair struct {
	origin  *server.Server
	replica *server.Server
	link    *upstream.Link

	mu       sync.Mutex
	rejected []upstream.Rejection
	// onRejected, when set, runs for each rejection after it is recorded.
	onRejected func(upstream.Rejection)
}

func newReplicaPair(ws string, originOpts ...server.Option) *replicaPair {
	GinkgoHelper()
	ctx := context.Background()
	pr := &replicaPair{}

	ostore, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "origin.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(ostore.Close)
	pr.origin, err = server.New(ostore, append([]server.Option{
		server.WithEntities(&testdatav1.Project{}, &testdatav1.Widget{}),
		server.WithHeartbeat(time.Second),
	}, originOpts...)...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(pr.origin.Close)
	ts := httptest.NewServer(pr.origin.HTTPHandler())
	DeferCleanup(func() {
		ts.CloseClientConnections()
		ts.Close()
	})

	rstore, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "replica.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(rstore.Close)
	transport := &http.Transport{}
	DeferCleanup(transport.CloseIdleConnections)
	pr.link, err = upstream.New(upstream.Config{
		OriginURL:  ts.URL,
		HTTPClient: &http.Client{Transport: transport},
		Bindings:   []upstream.Binding{{Prototype: &testdatav1.Project{}}, {Prototype: &testdatav1.Widget{}}},
		Logger:     slog.New(slog.DiscardHandler),
		RetryMin:   10 * time.Millisecond,
		RetryMax:   100 * time.Millisecond,
		OnRejected: func(r upstream.Rejection) {
			pr.mu.Lock()
			pr.rejected = append(pr.rejected, r)
			fn := pr.onRejected
			pr.mu.Unlock()
			if fn != nil {
				fn(r)
			}
		},
	})
	Expect(err).NotTo(HaveOccurred())
	pr.replica, err = server.New(rstore, append([]server.Option{
		server.WithEntities(&testdatav1.Project{}, &testdatav1.Widget{}),
	}, pr.link.ServerOptions()...)...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(pr.replica.Close)
	Expect(pr.link.Start(ctx, pr.replica)).To(Succeed())
	DeferCleanup(pr.link.Close)
	Expect(pr.link.AddWorkspace(ctx, ws)).To(Succeed())
	pr.flush(ws)
	return pr
}

// flush waits until the replica's writes are confirmed or rejected and the
// replica holds what the origin holds.
func (pr *replicaPair) flush(ws string) {
	GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	Expect(pr.link.Flush(ctx, ws)).To(Succeed())
}

func (pr *replicaPair) rejections() []upstream.Rejection {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return append([]upstream.Rejection(nil), pr.rejected...)
}

// recFixtureOn is a record-mode fixture over an existing server, with its own
// store and codec.
func recFixtureOn(srv *server.Server, opts ...func(*engineconnect.Config)) *recFixture {
	GinkgoHelper()
	f := &recFixture{
		srv:   srv,
		store: newMemStore(),
		codec: &lineCodec{},
		clock: &fakeClock{t: time.Unix(1_700_000_000, 0)},
		opts:  opts,
	}
	f.conn = f.connector()
	return f
}

var _ = Describe("Record mode on a replica", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("writes no row from an ingest of an EgressOnly type (acceptance 18)", func() {
		pr := newReplicaPair(ws)
		origin := recFixtureOn(pr.origin)
		origin.addRecord(ctx, ws, "p1", "Config", "a = 1")
		pr.flush(ws)

		var log overwriteLog
		f := recFixtureOn(pr.replica, egressOnly(&log))
		Eventually(func() map[string]string { return f.projects(ctx, ws) }).WithTimeout(10*time.Second).Should(HaveLen(1), "the replica holds the origin's record")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.content(ws, "Config.txt")).To(Equal("Config\na = 1"))
		seq := f.seq(ctx, ws)

		f.store.put(ws, "Config.txt", "Config\na = 2")
		f.store.put(ws, "Other.txt", "Other\nb = 1")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.conn.Touch(ws, "Config.txt", "Other.txt")
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		pr.flush(ws)

		Expect(f.seq(ctx, ws)).To(Equal(seq), "the replica wrote no row")
		Expect(origin.lines(ctx, ws, "p1")).To(Equal("Config\na = 1"), "the origin got no write")
		Expect(origin.projects(ctx, ws)).To(HaveLen(1))
		Expect(pr.rejections()).To(BeEmpty())
		Expect(f.content(ws, "Config.txt")).To(Equal("Config\na = 1"), "the store shows the engine content")
		Expect(log.all()).To(HaveLen(1))
		Expect(string(log.all()[0].Old)).To(Equal("Config\na = 2"))
	})
})
