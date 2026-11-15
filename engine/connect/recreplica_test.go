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
	. "github.com/onsi/gomega/gstruct"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
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
	// onRejected, when set, runs for each rejection before it is recorded.
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
			fn := pr.onRejected
			pr.mu.Unlock()
			if fn != nil {
				fn(r)
			}
			pr.mu.Lock()
			pr.rejected = append(pr.rejected, r)
			pr.mu.Unlock()
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

// denyForbidden refuses every write of a project named "forbidden".
type denyForbidden struct{ server.AllowAll }

func (denyForbidden) CanMutate(_ context.Context, _ server.Principal, _ string, w server.Write) error {
	if c := w.Crud; c != nil && c.GetEntity() == "project" && c.GetOp() != syncplanev1.CrudOp_CRUD_OP_DELETE {
		var p testdatav1.Project
		if err := proto.Unmarshal(c.GetValue(), &p); err == nil && p.GetName() == "forbidden" {
			return server.Reject(server.CodePermissionDenied, "forbidden name")
		}
	}
	return nil
}

var _ = Describe("Record mode on a replica", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("shows the engine content in the store and gives the refused content to the adopter when the origin refuses an ingest (acceptance 17)", func() {
		pr := newReplicaPair(ws, server.WithPermissions(denyForbidden{}))
		var log overwriteLog
		f := recFixtureOn(pr.replica, refusals(&log, 0))
		pr.onRejected = func(r upstream.Rejection) {
			rows := make([]engineconnect.RowRef, len(r.Rows))
			for i, row := range r.Rows {
				rows[i] = engineconnect.RowRef(row)
			}
			Expect(f.conn.Refused(ctx, r.Workspace, rows)).To(Succeed())
		}
		f.store.put(ws, "a.txt", "T\none")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		pr.flush(ws)
		key := f.key(ctx, ws, "a.txt")
		origin := recFixtureOn(pr.origin)
		Expect(origin.lines(ctx, ws, key)).To(Equal("T\none"))

		f.store.put(ws, "a.txt", "forbidden\none")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Eventually(pr.rejections).WithTimeout(10 * time.Second).Should(HaveLen(1))
		Expect(f.lines(ctx, ws, key)).To(Equal("T\none"), "the link reverted the rows")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())

		Expect(f.content(ws, "a.txt")).To(Equal("T\none"))
		Expect(log.all()).To(ConsistOf(MatchFields(IgnoreExtras, Fields{
			"Key": Equal(key), "Reason": Equal(engineconnect.OverwriteRefused),
			"Old": Equal([]byte("forbidden\none")), "New": Equal([]byte("T\none")),
		})))
		pr.flush(ws)
		Expect(origin.lines(ctx, ws, key)).To(Equal("T\none"))
	})

	It("keeps both text edits of two connectors with one WriterID and their own TextPeer (acceptance 16)", func() {
		pr := newReplicaPair(ws)
		peer := func(p string) func(*engineconnect.Config) {
			return func(cfg *engineconnect.Config) { cfg.TextPeer = p }
		}
		o := recFixtureOn(pr.origin, peer("files@origin"))
		r := recFixtureOn(pr.replica, peer("files@replica"), withRecords(func(rs *engineconnect.Records) {
			rs.FirstPass = engineconnect.EngineWins
		}))
		o.store.put(ws, "T.txt", "T\nhello world")
		Expect(o.conn.Reconcile(ctx, ws)).To(Succeed())
		key := o.key(ctx, ws, "T.txt")
		Eventually(func() string { return r.lines(ctx, ws, key) }).WithTimeout(10 * time.Second).Should(Equal("T\nhello world"))
		Expect(r.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(r.content(ws, "T.txt")).To(Equal("T\nhello world"))
		w := o.widgetWith(ctx, ws, key, "hello world")

		o.store.put(ws, "T.txt", "T\nhello brave world")
		r.store.put(ws, "T.txt", "T\nhello world!")
		Expect(o.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(r.conn.Reconcile(ctx, ws)).To(Succeed())
		pr.flush(ws)
		Eventually(func() string { return r.lines(ctx, ws, key) }).WithTimeout(10 * time.Second).Should(Equal("T\nhello brave world!"))
		Expect(o.lines(ctx, ws, key)).To(Equal("T\nhello brave world!"))
		Expect(o.conn.Flush(ctx, ws)).To(Succeed())
		Expect(r.conn.Flush(ctx, ws)).To(Succeed())
		Expect(o.content(ws, "T.txt")).To(Equal("T\nhello brave world!"))
		Expect(r.content(ws, "T.txt")).To(Equal("T\nhello brave world!"))

		// Each peer minted its own items: the document holds the original
		// text of the origin's peer and one edit of each.
		_, widgets := o.rows(ctx, ws, key)
		Expect(widgets).To(HaveLen(1))
		Expect(widgets[0].GetId()).To(Equal(w))
		doc, err := text.Decode(widgets[0].GetBody(), "check")
		Expect(err).NotTo(HaveOccurred())
		Expect(doc.VersionVector()).To(HaveKey("files@origin"))
		Expect(doc.VersionVector()).To(HaveKey("files@replica"))
		Expect(doc.VersionVector()).NotTo(HaveKey("connector:files"))
	})

	It("mints the text of a RowCodec under each connector's TextPeer, with no item id on both", func() {
		pr := newReplicaPair(ws)
		with := func(p string) func(*engineconnect.Config) {
			return func(cfg *engineconnect.Config) {
				cfg.TextPeer = p
				cfg.Records.Types[0].Codec = (&rowLines{}).codec()
			}
		}
		o := recFixtureOn(pr.origin, with("files@origin"))
		r := recFixtureOn(pr.replica, with("files@replica"), withRecords(func(rs *engineconnect.Records) {
			rs.FirstPass = engineconnect.EngineWins
		}))
		o.store.put(ws, "T.txt", "T\nhello world")
		Expect(o.conn.Reconcile(ctx, ws)).To(Succeed())
		key := o.key(ctx, ws, "T.txt")
		Eventually(func() string { return r.lines(ctx, ws, key) }).WithTimeout(10 * time.Second).Should(Equal("T\nhello world"))
		Expect(r.conn.Reconcile(ctx, ws)).To(Succeed())
		w := o.widgetWith(ctx, ws, key, "hello world")

		// Both stores edit the line at once, more than once, so each peer
		// mints items before it sees the other's.
		for _, edit := range [][2]string{{"hello brave world", "hello world!"}, {"hello brave new world", "hello world!?"}} {
			o.store.put(ws, "T.txt", "T\n"+edit[0])
			r.store.put(ws, "T.txt", "T\n"+edit[1])
			Expect(o.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(r.conn.Reconcile(ctx, ws)).To(Succeed())
			pr.flush(ws)
			Expect(o.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(r.conn.Reconcile(ctx, ws)).To(Succeed())
			pr.flush(ws)
		}
		want := "T\nhello brave new world!?"
		Eventually(func() string { return r.lines(ctx, ws, key) }).WithTimeout(10 * time.Second).Should(Equal(want))
		Expect(o.lines(ctx, ws, key)).To(Equal(want))
		Expect(o.conn.Flush(ctx, ws)).To(Succeed())
		Expect(r.conn.Flush(ctx, ws)).To(Succeed())
		Expect(o.content(ws, "T.txt")).To(Equal(want))
		Expect(r.content(ws, "T.txt")).To(Equal(want))
		Expect(pr.rejections()).To(BeEmpty())

		var docs [][]byte
		for _, srv := range []*server.Server{pr.origin, pr.replica} {
			var row testdatav1.Widget
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				_, _, err := tx.Get(&row, w)
				return err
			})).To(Succeed())
			docs = append(docs, row.GetBody())
		}
		doc, err := text.Decode(docs[0], "check")
		Expect(err).NotTo(HaveOccurred())
		Expect(doc.VersionVector()).To(HaveKey("files@origin"))
		Expect(doc.VersionVector()).To(HaveKey("files@replica"))
		Expect(doc.VersionVector()).NotTo(HaveKey("connector:files"))
		// An item id on two sides with different content fails the union.
		union, err := text.Merge(docs[0], docs[1])
		Expect(err).NotTo(HaveOccurred())
		Expect(text.PlainText(union)).To(Equal("hello brave new world!?"))
	})

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
