package upstream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/client"
	"github.com/levelfourab/syncplane/engine/fractional"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// The convergence harness drives random writes from every kind of writer a
// link has to reconcile (clients of the origin, the Go facade of each local
// engine, and clients of each local engine) while it injects network and
// process faults. At each checkpoint it heals every fault, waits for every
// link to flush, and checks that the origin, every local engine and every
// downstream replica hold the same rows, that no accepted write is lost, and
// that each origin rejection is reported once with the identity of its write.
//
// The schedule of operations and faults is a function of the seed. Timing is
// not, so a failing seed can need a few runs to fail again.
//
// Environment:
//
//	UPSTREAM_CONV_SEED=<n>    run only seed n
//	UPSTREAM_CONV_SEEDS=<n>   run n seeds (default 2, long mode 12)
//	UPSTREAM_CONV_OPS=<n>     operations per seed (default 90, long mode 400)
//	UPSTREAM_CONV_BASE=<n>    first seed when no seed is pinned (default 1;
//	                          long mode picks one from the clock)
//	UPSTREAM_CONV_LONG=1      long mode
//	UPSTREAM_CONV_TAIL=<n>    operations of the log a failure prints (default 80)
//	UPSTREAM_CONV_REPORT=1    print the statistics of each seed to stderr
//	UPSTREAM_CONV_TOLERATE_WT_RECEIPT=1
//	                          count a failed write-through receipt instead of
//	                          failing, to reach the checks after it
//
// Focus with: go tool ginkgo run -race --label-filter=convergence ./engine/upstream

const convWS = "conv"

func convEnvInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

func convPlan() (seeds []uint64, ops int) {
	long := os.Getenv("UPSTREAM_CONV_LONG") != ""
	n, ops, base := 2, 90, 1
	if long {
		n, ops, base = 12, 400, int(time.Now().UnixNano()%1_000_000)
	}
	n = convEnvInt("UPSTREAM_CONV_SEEDS", n)
	ops = convEnvInt("UPSTREAM_CONV_OPS", ops)
	base = convEnvInt("UPSTREAM_CONV_BASE", base)
	if s := os.Getenv("UPSTREAM_CONV_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err == nil {
			return []uint64{v}, ops
		}
	}
	for i := range n {
		seeds = append(seeds, uint64(base+i))
	}
	return seeds, ops
}

// ---------------------------------------------------------------------------
// Schema shared by the origin and the local engines.

func convEntities() []proto.Message {
	return []proto.Message{
		&testdatav1.Widget{}, &testdatav1.Pair{}, &testdatav1.Project{}, &testdatav1.User{}, &testdatav1.Issue{},
	}
}

var convTypes = map[string]func() proto.Message{
	"widget":  func() proto.Message { return &testdatav1.Widget{} },
	"pair":    func() proto.Message { return &testdatav1.Pair{} },
	"project": func() proto.Message { return &testdatav1.Project{} },
	"user":    func() proto.Message { return &testdatav1.User{} },
	"issue":   func() proto.Message { return &testdatav1.Issue{} },
}

// forbiddenField is the field whose value the origin's policy checks, per
// entity.
var forbiddenField = map[string]string{"widget": "label", "issue": "title", "project": "name"}

func isForbidden(s string) bool { return strings.HasPrefix(s, "X") }

// bump adds args.count to the count of widget args.id, and rejects with code
// "missing" when the widget does not exist.
func bump(_ context.Context, tx *server.EntityTx, args *testdatav1.Widget) error {
	w := &testdatav1.Widget{}
	ok, err := tx.Get(w, args.GetId())
	if err != nil {
		return err
	}
	if !ok {
		return server.Reject("missing", "no widget "+args.GetId())
	}
	return tx.Patch(&testdatav1.Widget{Id: w.GetId(), Count: w.GetCount() + args.GetCount()}, []string{"count"}, nil)
}

// stamp sets the label of widget args.id to args.label when the widget
// exists. Only the origin declares it.
func stamp(_ context.Context, tx *server.EntityTx, args *testdatav1.Widget) error {
	w := &testdatav1.Widget{}
	ok, err := tx.Get(w, args.GetId())
	if err != nil || !ok {
		return err
	}
	return tx.Patch(&testdatav1.Widget{Id: w.GetId(), Label: args.GetLabel()}, []string{"label"}, nil)
}

// xperm is the origin's policy: it rejects a write that sets the checked
// field of an entity to a value starting with "X", a bump by 13 and a stamp
// with such a label. While noAccess is set it admits nobody to any workspace.
type xperm struct {
	server.AllowAll
	noAccess atomic.Bool
}

func (p *xperm) CanAccessWorkspace(context.Context, server.Principal, string) error {
	if p.noAccess.Load() {
		return server.ErrNoAccess
	}
	return nil
}

func (p *xperm) CanMutate(_ context.Context, _ server.Principal, _ string, w server.Write) error {
	if c := w.Custom; c != nil {
		var a testdatav1.Widget
		if err := c.GetArgs().UnmarshalTo(&a); err != nil {
			return nil
		}
		if c.GetName() == "bump" && a.GetCount() == 13 {
			return server.Reject(server.CodePermissionDenied, "bump by 13")
		}
		if c.GetName() == "stamp" && isForbidden(a.GetLabel()) {
			return server.Reject(server.CodePermissionDenied, "forbidden stamp")
		}
		return nil
	}
	c := w.Crud
	if c == nil || c.GetOp() == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return nil
	}
	field, ok := forbiddenField[c.GetEntity()]
	if !ok {
		return nil
	}
	if c.GetOp() == syncplanev1.CrudOp_CRUD_OP_PATCH && !slices.Contains(c.GetFieldMask().GetPaths(), field) {
		return nil
	}
	msg := convTypes[c.GetEntity()]()
	if err := proto.Unmarshal(c.GetValue(), msg); err != nil {
		return nil
	}
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoName(field))
	if isForbidden(msg.ProtoReflect().Get(fd).String()) {
		return server.Reject(server.CodePermissionDenied, "forbidden "+field)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Transport faults.

// swapHandler serves through the handler it holds, and answers 503 while it
// holds none, so a server can restart behind one stable URL.
type swapHandler struct{ h atomic.Pointer[http.Handler] }

func (s *swapHandler) set(h http.Handler) {
	if h == nil {
		s.h.Store(nil)
		return
	}
	s.h.Store(&h)
}

func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h := s.h.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}
	http.Error(w, "down", http.StatusServiceUnavailable)
}

// chaos wraps a partition with faults on the read paths: it breaks the next
// Subscribe or Pull response after some bytes, or stalls the next Subscribe
// response (it stays open and goes silent).
type chaos struct {
	base        http.RoundTripper
	breakStream atomic.Int64
	breakPull   atomic.Int64
	stallStream atomic.Int64
	subscribes  atomic.Int64
	stalled     atomic.Int64

	mu     sync.Mutex
	stalls map[*stallBody]struct{}
}

// releaseStalls ends every stalled response, as a connection that has been
// silent for long enough is ended by a keepalive or a proxy.
func (c *chaos) releaseStalls() int {
	c.mu.Lock()
	stalls := c.stalls
	c.stalls = nil
	c.mu.Unlock()
	for b := range stalls {
		b.once.Do(func() { close(b.closed) })
	}
	return len(stalls)
}

func (c *chaos) stallActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.stalls) > 0
}

func (c *chaos) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	switch filepath.Base(req.URL.Path) {
	case "Subscribe":
		c.subscribes.Add(1)
		if n := c.breakStream.Swap(0); n > 0 {
			resp.Body = &brokenBody{ReadCloser: resp.Body, left: n}
		} else if n := c.stallStream.Swap(0); n > 0 {
			c.stalled.Add(1)
			sb := &stallBody{ReadCloser: resp.Body, left: n, ctx: req.Context(), closed: make(chan struct{})}
			c.mu.Lock()
			if c.stalls == nil {
				c.stalls = map[*stallBody]struct{}{}
			}
			c.stalls[sb] = struct{}{}
			c.mu.Unlock()
			resp.Body = sb
		}
	case "Pull":
		if n := c.breakPull.Swap(0); n > 0 {
			resp.Body = &brokenBody{ReadCloser: resp.Body, left: n}
		}
	}
	return resp, nil
}

// stallBody passes left bytes, then blocks until it is closed or its request
// ends: a connection that stays open and delivers nothing more.
type stallBody struct {
	io.ReadCloser
	left   int64
	ctx    context.Context
	once   sync.Once
	closed chan struct{}
}

func (b *stallBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		select {
		case <-b.closed:
		case <-b.ctx.Done():
		}
		return 0, errors.New("stalled stream closed")
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, err
}

func (b *stallBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return b.ReadCloser.Close()
}

// ---------------------------------------------------------------------------
// Origin.

type corigin struct {
	path  string
	opts  []server.Option
	perm  *xperm
	front *swapHandler
	ts    *httptest.Server
	store *sqlitestore.Store
	srv   *server.Server
}

func newCOrigin(extra ...server.Option) *corigin {
	GinkgoHelper()
	o := &corigin{path: filepath.Join(GinkgoT().TempDir(), "origin.db"), perm: &xperm{}, front: &swapHandler{}}
	o.opts = append([]server.Option{
		server.WithEntities(convEntities()...),
		server.Mutator("bump", bump),
		server.Mutator("stamp", stamp),
		server.WithPermissions(o.perm),
		server.WithHeartbeat(300 * time.Millisecond),
	}, extra...)
	o.ts = httptest.NewServer(o.front)
	o.open()
	DeferCleanup(func() {
		o.ts.CloseClientConnections()
		o.ts.Close()
		o.close()
	})
	return o
}

func (o *corigin) open() {
	GinkgoHelper()
	store, err := sqlitestore.Open(o.path)
	Expect(err).NotTo(HaveOccurred())
	srv, err := server.New(store, o.opts...)
	Expect(err).NotTo(HaveOccurred())
	o.store, o.srv = store, srv
	o.front.set(srv.HTTPHandler())
}

func (o *corigin) close() {
	if o.srv == nil {
		return
	}
	o.front.set(nil)
	o.ts.CloseClientConnections()
	Expect(o.srv.Close()).To(Succeed())
	Expect(o.store.Close()).To(Succeed())
	o.srv, o.store = nil, nil
}

func (o *corigin) head() uint64 {
	GinkgoHelper()
	seq, err := o.srv.CurrentSeq(context.Background(), convWS)
	Expect(err).NotTo(HaveOccurred())
	return seq
}

func (o *corigin) client(id string) *client.Workspace {
	GinkgoHelper()
	c, err := client.New(o.ts.URL, client.WithClientID(id), client.WithHTTPClient(&http.Client{Transport: &http.Transport{}}))
	Expect(err).NotTo(HaveOccurred())
	return c.Workspace(convWS)
}

// ---------------------------------------------------------------------------
// Local node.

type crej struct {
	upstream.Rejection
	gen int
}

type cnode struct {
	name  string
	o     *corigin
	path  string
	part  *partition
	chaos *chaos
	mut   func(*upstream.Config)
	extra []server.Option
	// beforeStart runs on each new local engine before the link starts.
	beforeStart func(*server.Server)
	front       *swapHandler
	ts          *httptest.Server

	store *sqlitestore.Store
	srv   *server.Server
	link  *upstream.Link
	gen   int
	// scoped is set while the workspace is linked with narrowed
	// subscriptions.
	scoped bool

	// closed is set from the return of Link.Close to the next start.
	closed atomic.Bool
	// late counts callbacks that ran while closed was set.
	late atomic.Int64

	mu         sync.Mutex
	rejections []crej
	resets     int
	revoked    int
	states     map[upstream.State]int
}

func newCNode(o *corigin, name string, mut func(*upstream.Config), extra ...server.Option) *cnode {
	GinkgoHelper()
	return newCNodeWith(o, name, mut, nil, extra...)
}

func newCNodeWith(o *corigin, name string, mut func(*upstream.Config), beforeStart func(*server.Server), extra ...server.Option) *cnode {
	GinkgoHelper()
	part := newPartition()
	n := &cnode{
		name: name, o: o, path: filepath.Join(GinkgoT().TempDir(), name+".db"),
		part: part, chaos: &chaos{base: part}, mut: mut, extra: extra, beforeStart: beforeStart,
		front: &swapHandler{}, states: map[upstream.State]int{},
	}
	n.ts = httptest.NewServer(n.front)
	n.start()
	DeferCleanup(func() {
		n.stop()
		n.ts.CloseClientConnections()
		n.ts.Close()
	})
	return n
}

func (n *cnode) config() upstream.Config {
	var bindings []upstream.Binding
	for _, m := range convEntities() {
		bindings = append(bindings, upstream.Binding{Prototype: m})
	}
	cfg := upstream.Config{
		OriginURL:           n.o.ts.URL,
		HTTPClient:          &http.Client{Transport: n.chaos},
		Bindings:            bindings,
		Logger:              slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelWarn})).With("node", n.name),
		RetryMin:            5 * time.Millisecond,
		RetryMax:            50 * time.Millisecond,
		WriteThroughTimeout: 3 * time.Second,
		OnRejected: func(r upstream.Rejection) {
			if n.closed.Load() {
				n.late.Add(1)
			}
			n.mu.Lock()
			n.rejections = append(n.rejections, crej{Rejection: r, gen: n.gen})
			n.mu.Unlock()
		},
		OnReset: func(string) {
			n.mu.Lock()
			n.resets++
			n.mu.Unlock()
		},
		OnAccessRevoked: func(string) {
			n.mu.Lock()
			n.revoked++
			n.mu.Unlock()
		},
		OnStatus: func(s upstream.Status) {
			n.mu.Lock()
			n.states[s.State]++
			n.mu.Unlock()
		},
	}
	if n.mut != nil {
		n.mut(&cfg)
	}
	return cfg
}

func (n *cnode) start() {
	GinkgoHelper()
	store, err := sqlitestore.Open(n.path)
	Expect(err).NotTo(HaveOccurred())
	link, err := upstream.New(n.config())
	Expect(err).NotTo(HaveOccurred())
	opts := append([]server.Option{
		server.WithEntities(convEntities()...),
		server.Mutator("bump", bump),
	}, n.extra...)
	srv, err := server.New(store, append(opts, link.ServerOptions()...)...)
	Expect(err).NotTo(HaveOccurred())
	if n.beforeStart != nil {
		n.beforeStart(srv)
	}
	n.mu.Lock()
	n.gen++
	n.mu.Unlock()
	n.closed.Store(false)
	Expect(link.Start(context.Background(), srv)).To(Succeed())
	n.store, n.srv, n.link = store, srv, link
	n.front.set(srv.HTTPHandler())
}

func (n *cnode) stop() {
	GinkgoHelper()
	if n.link == nil {
		return
	}
	n.front.set(nil)
	n.ts.CloseClientConnections()
	Expect(n.link.Close()).To(Succeed())
	n.closed.Store(true)
	n.part.base.(*http.Transport).CloseIdleConnections()
	Expect(n.srv.Close()).To(Succeed())
	Expect(n.store.Close()).To(Succeed())
	n.link, n.srv, n.store = nil, nil, nil
}

func (n *cnode) restart() {
	GinkgoHelper()
	n.stop()
	n.start()
}

func (n *cnode) ws() *upstream.Workspace { return n.link.Workspace(convWS) }

func (n *cnode) status() upstream.Status {
	st, _ := n.link.Status(convWS)
	return st
}

func (n *cnode) healAll() {
	n.part.Heal()
	n.part.blockPush.Store(false)
	n.part.losePushes.Store(0)
	n.part.breakBootstrap.Store(0)
	n.chaos.breakStream.Store(0)
	n.chaos.breakPull.Store(0)
	n.chaos.stallStream.Store(0)
	n.chaos.releaseStalls()
}

func (n *cnode) rejected() []crej {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.rejections)
}

func (n *cnode) flush(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return n.link.Flush(ctx, convWS)
}

func (n *cnode) client() *client.Workspace {
	GinkgoHelper()
	c, err := client.New(n.ts.URL, client.WithHTTPClient(&http.Client{Transport: &http.Transport{}}))
	Expect(err).NotTo(HaveOccurred())
	return c.Workspace(convWS)
}

// ---------------------------------------------------------------------------
// Row snapshots and their comparison.

type rowset map[string]map[string]proto.Message

func dumpRows(srv *server.Server, workspace string) rowset {
	GinkgoHelper()
	out := rowset{}
	ctx := server.Trusted(context.Background())
	for name, mk := range convTypes {
		out[name] = map[string]proto.Message{}
		Expect(srv.View(ctx, workspace, func(tx *server.ViewTx) error {
			after := ""
			for {
				last, more, err := tx.ScanPage(mk(), after, 200, func(msg proto.Message, _ uint64) error {
					_, key, err := srv.EntityKey(msg)
					if err != nil {
						return err
					}
					out[name][key] = msg
					return nil
				})
				if err != nil || !more {
					return err
				}
				after = last
			}
		})).To(Succeed())
	}
	return out
}

func textOf(b []byte) (string, string) {
	if len(b) == 0 {
		return "", "{}"
	}
	plain, err := text.PlainText(b)
	if err != nil {
		return "<undecodable: " + err.Error() + ">", ""
	}
	d, err := text.Decode(b, "")
	if err != nil {
		return plain, "<no doc>"
	}
	vv := d.VersionVector()
	keys := make([]string, 0, len(vv))
	for k := range vv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString("{")
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s:%d,", k, vv[k])
	}
	sb.WriteString("}")
	return plain, sb.String()
}

// diffMsg describes how b differs from a, "" when they hold the same row.
// Text fields compare by plain text and version vector.
func diffMsg(a, b proto.Message) string {
	a, b = proto.Clone(a), proto.Clone(b)
	var notes []string
	if wa, ok := a.(*testdatav1.Widget); ok {
		wb := b.(*testdatav1.Widget)
		pa, va := textOf(wa.GetBody())
		pb, vb := textOf(wb.GetBody())
		if pa != pb {
			notes = append(notes, fmt.Sprintf("body text %q != %q", pa, pb))
		} else if va != vb {
			notes = append(notes, fmt.Sprintf("body version vector %s != %s", va, vb))
		}
		wa.Body, wb.Body = nil, nil
	}
	if !proto.Equal(a, b) {
		notes = append(notes, fmt.Sprintf("%v != %v", a, b))
	}
	return strings.Join(notes, "; ")
}

func diffRows(aName string, a rowset, bName string, b rowset) []string {
	var out []string
	for ent := range convTypes {
		keys := map[string]bool{}
		for k := range a[ent] {
			keys[k] = true
		}
		for k := range b[ent] {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			ra, rb := a[ent][k], b[ent][k]
			switch {
			case ra == nil:
				out = append(out, fmt.Sprintf("%s/%s: only in %s: %v", ent, k, bName, rb))
			case rb == nil:
				out = append(out, fmt.Sprintf("%s/%s: only in %s: %v", ent, k, aName, ra))
			default:
				if d := diffMsg(ra, rb); d != "" {
					out = append(out, fmt.Sprintf("%s/%s: %s vs %s: %s", ent, k, aName, bName, d))
				}
			}
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Downstream replica: a client of a local engine that keeps a copy of the
// rows from Bootstrap and Pull, the way a browser does.

type downstream struct {
	node   *cnode
	w      *client.Workspace
	cursor []byte
	rows   map[string]map[string][]byte
	resets int
}

func newDownstream(n *cnode) *downstream {
	GinkgoHelper()
	d := &downstream{node: n, w: n.client()}
	d.bootstrap()
	return d
}

func (d *downstream) bootstrap() {
	GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	snap, err := d.w.Bootstrap(ctx)
	Expect(err).NotTo(HaveOccurred())
	rows := map[string]map[string][]byte{}
	for rc, err := range snap.Rows() {
		Expect(err).NotTo(HaveOccurred())
		if rows[rc.GetEntity()] == nil {
			rows[rc.GetEntity()] = map[string][]byte{}
		}
		rows[rc.GetEntity()][rc.GetKey()] = rc.GetValue()
	}
	d.rows, d.cursor = rows, snap.Cursor
}

func (d *downstream) catchUp() {
	GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for range 5 {
		reset := false
		for page, err := range d.w.Pull(ctx, d.cursor, client.WithNoDeltaEntities("widget")) {
			if errors.Is(err, client.ErrReset) {
				reset = true
				break
			}
			Expect(err).NotTo(HaveOccurred())
			for _, ent := range page.GetPatch().GetDropEntities() {
				delete(d.rows, ent)
			}
			for _, rc := range page.GetPatch().GetChanges() {
				if d.rows[rc.GetEntity()] == nil {
					d.rows[rc.GetEntity()] = map[string][]byte{}
				}
				if rc.GetTombstone() {
					delete(d.rows[rc.GetEntity()], rc.GetKey())
				} else {
					Expect(rc.GetTextDeltas()).To(BeEmpty())
					d.rows[rc.GetEntity()][rc.GetKey()] = rc.GetValue()
				}
			}
			if len(page.GetNextCursor()) > 0 {
				d.cursor = page.GetNextCursor()
			}
		}
		if !reset {
			return
		}
		d.resets++
		d.bootstrap()
	}
	Fail("downstream kept resetting")
}

func (d *downstream) rowset() rowset {
	GinkgoHelper()
	out := rowset{}
	for ent, mk := range convTypes {
		out[ent] = map[string]proto.Message{}
		for k, v := range d.rows[ent] {
			m := mk()
			Expect(proto.Unmarshal(v, m)).To(Succeed())
			out[ent][k] = m
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// The randomized world.

type uniqInsert struct {
	key       string
	label     string
	forbidden bool
	via       string // origin, facade, facade-wt, down
	node      int
	outcome   string // ok, rejected, unknown, failed
}

type facadeWrite struct {
	node      int
	gen       int
	seq       uint64
	desc      string
	forbidden bool
}

type world struct {
	seed    uint64
	r       *rand.Rand
	o       *corigin
	nodes   []*cnode
	writers []*client.Workspace
	downs   []*downstream

	log      []string
	opn      int
	uniq     int
	inserts  []uniqInsert
	facade   []facadeWrite
	stats    map[string]int
	gcWindow bool
	posKeys  []string
	// stallLeft counts down the operations until the stalled stream of each
	// node ends.
	stallLeft []int
}

func (w *world) logf(format string, args ...any) {
	w.log = append(w.log, fmt.Sprintf("#%d ", w.opn)+fmt.Sprintf(format, args...))
}

func (w *world) count(k string) { w.stats[k]++ }

func (w *world) tail() string {
	from := max(0, len(w.log)-convEnvInt("UPSTREAM_CONV_TAIL", 80))
	return fmt.Sprintf("seed %d, op log tail:\n%s", w.seed, strings.Join(w.log[from:], "\n"))
}

func (w *world) pick(xs ...string) string { return xs[w.r.IntN(len(xs))] }

func (w *world) chance(p float64) bool { return w.r.Float64() < p }

var (
	widgetKeys  = []string{"w0", "w1", "w2", "w3", "w4", "w5"}
	issueKeys   = []string{"i0", "i1", "i2", "i3"}
	projectKeys = []string{"p0", "p1", "p2"}
)

func (w *world) label(actor string) string {
	l := fmt.Sprintf("%s-%d", actor, w.opn)
	if w.chance(0.07) {
		l = "X" + l
	}
	return l
}

func (w *world) randWidget(id, actor string) *testdatav1.Widget {
	r := w.r
	x := &testdatav1.Widget{Id: id, Label: w.label(actor), Count: r.Int32N(50), Flag: r.IntN(2) == 0}
	if r.IntN(2) == 0 {
		x.Settings = &testdatav1.Settings{Color: w.pick("red", "green", "blue"), Size: r.Int32N(9)}
	}
	for _, t := range []string{"a", "b", "c", "d"} {
		if r.IntN(2) == 0 {
			x.Tags = append(x.Tags, t)
		}
	}
	for _, t := range []string{"q", "r"} {
		if r.IntN(3) == 0 {
			x.AtomicTags = append(x.AtomicTags, t)
		}
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if r.IntN(2) == 0 {
			if x.Attrs == nil {
				x.Attrs = map[string]string{}
			}
			x.Attrs[k] = fmt.Sprintf("%s-%d", actor, r.IntN(100))
		}
	}
	for _, u := range []string{"m1", "m2", "m3"} {
		if r.IntN(2) == 0 {
			x.Members = append(x.Members, &testdatav1.Member{Uid: u, Name: fmt.Sprintf("%s-%d", actor, r.IntN(100)), Role: r.Int32N(4)})
		}
	}
	for _, it := range []string{"it1", "it2", "it3"} {
		if r.IntN(2) == 0 {
			x.Items = append(x.Items, &testdatav1.Item{Id: it, Pos: w.posKeys[r.IntN(len(w.posKeys))], Text: fmt.Sprintf("%s-%d", actor, r.IntN(100))})
		}
	}
	if r.IntN(2) == 0 {
		s := fmt.Sprintf("maybe-%d", r.IntN(10))
		x.Maybe = &s
	}
	x.Ratio = float64(r.IntN(100)) / 4
	x.Big = r.Int64N(1 << 40)
	return x
}

var widgetPaths = []string{
	"label", "count", "flag", "settings", "settings.color", "settings.size", "tags", "atomic_tags",
	"attrs", "attrs.k1", "attrs.k2", "members", "members.m1", "members.m1.name", "members.m2.role",
	"items", "items.it1", "items.it2.text", "items.it1.pos", "maybe", "ratio", "big",
}

func overlaps(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+".")
}

func (w *world) mask(paths []string) []string {
	n := 1 + w.r.IntN(3)
	var out []string
	for range n * 3 {
		p := paths[w.r.IntN(len(paths))]
		if slices.ContainsFunc(out, func(q string) bool { return overlaps(p, q) }) {
			continue
		}
		out = append(out, p)
		if len(out) == n {
			break
		}
	}
	return out
}

func (w *world) randIssue(id, actor string) *testdatav1.Issue {
	r := w.r
	x := &testdatav1.Issue{Id: id, Title: w.label(actor), State: w.pick("open", "closed"), Rank: r.Int32N(20)}
	if r.IntN(3) != 0 {
		x.Project = &testdatav1.Project{Id: projectKeys[r.IntN(len(projectKeys))]}
	}
	for _, l := range []string{"bug", "ui", "perf"} {
		if r.IntN(2) == 0 {
			x.Labels = append(x.Labels, l)
		}
	}
	if r.IntN(3) == 0 {
		x.BlockedBy = append(x.BlockedBy, &testdatav1.Issue{Id: issueKeys[r.IntN(len(issueKeys))]})
	}
	return x
}

var issuePaths = []string{"title", "state", "rank", "labels", "project", "blocked_by"}

// wop is one CRUD operation, independent of who runs it.
type wop struct {
	kind     string // put, patch, delete
	msg      proto.Message
	mask     []string
	proto    proto.Message
	key      string
	expected *uint64
	text     *string
}

func (o wop) String() string {
	switch o.kind {
	case "delete":
		e, _ := client.EntityName(o.proto)
		return fmt.Sprintf("delete %s/%s", e, o.key)
	}
	e, k, _ := client.EntityKey(o.msg)
	s := fmt.Sprintf("%s %s/%s", o.kind, e, k)
	if o.mask != nil {
		s += fmt.Sprintf(" mask=%v", o.mask)
	}
	if o.expected != nil {
		s += fmt.Sprintf(" expect=%d", *o.expected)
	}
	if o.text != nil {
		s += fmt.Sprintf(" text=%q", *o.text)
	}
	if v := forbiddenValue(o.msg, o.mask); v != "" {
		s += " value=" + v
	}
	return s
}

func forbiddenValue(msg proto.Message, mask []string) string {
	if msg == nil {
		return ""
	}
	e, _ := client.EntityName(msg)
	f, ok := forbiddenField[e]
	if !ok {
		return ""
	}
	if mask != nil && !slices.Contains(mask, f) {
		return ""
	}
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoName(f))
	return msg.ProtoReflect().Get(fd).String()
}

func (o wop) forbidden() bool { return isForbidden(forbiddenValue(o.msg, o.mask)) }

// genOp makes one random CRUD operation on the shared rows.
func (w *world) genOp(actor string) wop {
	r := w.r
	switch x := r.IntN(100); {
	case x < 55:
		id := widgetKeys[r.IntN(len(widgetKeys))]
		switch y := r.IntN(10); {
		case y < 4:
			return wop{kind: "put", msg: w.randWidget(id, actor)}
		case y < 9:
			return wop{kind: "patch", msg: w.randWidget(id, actor), mask: w.mask(widgetPaths)}
		default:
			return wop{kind: "delete", proto: &testdatav1.Widget{}, key: id}
		}
	case x < 85:
		id := issueKeys[r.IntN(len(issueKeys))]
		switch y := r.IntN(10); {
		case y < 4:
			return wop{kind: "put", msg: w.randIssue(id, actor)}
		case y < 9:
			return wop{kind: "patch", msg: w.randIssue(id, actor), mask: w.mask(issuePaths)}
		default:
			return wop{kind: "delete", proto: &testdatav1.Issue{}, key: id}
		}
	default:
		id := projectKeys[r.IntN(len(projectKeys))]
		switch y := r.IntN(10); {
		case y < 5:
			return wop{kind: "put", msg: &testdatav1.Project{Id: id, Name: w.label(actor)}}
		case y < 9:
			return wop{kind: "patch", msg: &testdatav1.Project{Id: id, Name: w.label(actor)}, mask: []string{"name"}}
		default:
			return wop{kind: "delete", proto: &testdatav1.Project{}, key: id}
		}
	}
}

func editText(r *rand.Rand, cur, actor string) string {
	rs := []rune(cur)
	switch {
	case len(rs) > 0 && r.IntN(3) == 0:
		i := r.IntN(len(rs))
		j := min(len(rs), i+1+r.IntN(4))
		return string(rs[:i]) + string(rs[j:])
	default:
		i := r.IntN(len(rs) + 1)
		ins := fmt.Sprintf("<%s>", actor)
		return string(rs[:i]) + ins + string(rs[i:])
	}
}

// ---------------------------------------------------------------------------
// Executors.

func clientOp(o wop) client.Op {
	var opts []client.WriteOption
	if o.expected != nil {
		opts = append(opts, client.WithExpectedVersion(*o.expected))
	}
	if o.text != nil {
		opts = append(opts, client.WithText("body", *o.text))
	}
	switch o.kind {
	case "put":
		return client.Put(o.msg, opts...)
	case "patch":
		return client.Patch(o.msg, o.mask, opts...)
	default:
		return client.Delete(o.proto, o.key, opts...)
	}
}

func facadeTx(tx *server.ApplyTx, o wop, peer string) error {
	var opts []server.WriteOption
	if o.text != nil {
		opts = append(opts, server.WithTextString("body", *o.text), server.WithTextPeer(peer))
	}
	switch o.kind {
	case "put":
		if o.expected != nil {
			opts = append(opts, server.WithExpectedVersion(*o.expected))
		}
		return tx.Put(o.msg, opts...)
	case "patch":
		return tx.Patch(o.msg, o.mask, o.expected, opts...)
	default:
		return tx.Delete(o.proto, o.key)
	}
}

// actor is one writer of the world.
type actor struct {
	name string
	kind string // origin, facade, down
	node int
	cw   *client.Workspace
}

func (w *world) actors() []actor {
	var out []actor
	for i, cw := range w.writers {
		out = append(out, actor{name: fmt.Sprintf("o%d", i), kind: "origin", node: -1, cw: cw})
	}
	for i := range w.nodes {
		out = append(out, actor{name: fmt.Sprintf("f%d", i), kind: "facade", node: i})
	}
	for i, d := range w.downs {
		out = append(out, actor{name: fmt.Sprintf("d%d", i), kind: "down", node: slices.Index(w.nodes, d.node), cw: d.w})
	}
	return out
}

func (w *world) facadeCtx(a actor) (context.Context, bool) {
	ctx := context.Background()
	if w.chance(0.12) {
		return upstream.WriteThrough(ctx), true
	}
	return ctx, false
}

// run executes ops as actor a and records the outcome.
func (w *world) run(a actor, desc string, ops ...wop) {
	forbidden := slices.ContainsFunc(ops, wop.forbidden)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	switch a.kind {
	case "origin", "down":
		var cops []client.Op
		for _, o := range ops {
			cops = append(cops, clientOp(o))
		}
		res, err := a.cw.Apply(ctx, cops...)
		w.logf("%s %s %v -> seq=%d err=%v", a.name, desc, ops, res.Seq, err)
		w.outcome(a.kind, err)
	case "facade":
		n := w.nodes[a.node]
		fctx, through := w.facadeCtx(a)
		rc, err := n.ws().Apply(fctx, func(tx *server.ApplyTx) error {
			for _, o := range ops {
				if err := facadeTx(tx, o, "facade-"+n.name); err != nil {
					return err
				}
			}
			return nil
		})
		w.logf("%s(wt=%v) %s %v -> seq=%d fwd=%v err=%v", a.name, through, desc, ops, rc.Seq, rc.Forwarded, err)
		w.outcome(a.kind, err)
		if err == nil && !through && rc.Forwarded && rc.Seq != 0 {
			w.facade = append(w.facade, facadeWrite{node: a.node, gen: n.gen, seq: rc.Seq, desc: fmt.Sprint(ops), forbidden: forbidden})
		}
		if through && err == nil && rc.Forwarded {
			// A write-through write the origin accepted is decided already.
			wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
			werr := rc.Wait(wctx)
			wcancel()
			if werr != nil && n.chaos.stallActive() {
				// The receipt waits for the silent stream; the stall spec
				// covers that.
				w.count("wt-wait:stalled")
			} else if werr != nil && os.Getenv("UPSTREAM_CONV_TOLERATE_WT_RECEIPT") != "" {
				w.count("wt-wait:failed")
				w.logf("write-through receipt failed: %v", werr)
			} else {
				Expect(werr).To(Succeed(), "write-through receipt of an applied write\n%s", w.tail())
			}
		}
		if through && forbidden && err == nil {
			Fail(fmt.Sprintf("write-through write the origin must refuse committed: %v\n%s", ops, w.tail()))
		}
	}
}

func (w *world) outcome(kind string, err error) {
	switch {
	case err == nil:
		w.count(kind + ":ok")
	default:
		code := client.CodeOf(err)
		if rej, ok := server.AsRejection(err); ok {
			code = rej.Code
		}
		if errors.Is(err, upstream.ErrOriginUnavailable) {
			code = "origin_unavailable"
		}
		if code == "" {
			code = "error"
		}
		w.count(kind + ":" + code)
	}
}

func (w *world) invoke(a actor, name string, args *testdatav1.Widget) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	forbidden := (name == "bump" && args.GetCount() == 13) || (name == "stamp" && isForbidden(args.GetLabel()))
	switch a.kind {
	case "origin", "down":
		res, err := a.cw.Invoke(ctx, name, args)
		w.logf("%s invoke %s(%s,%d,%q) -> seq=%d err=%v", a.name, name, args.GetId(), args.GetCount(), args.GetLabel(), res.Seq, err)
		w.outcome(a.kind, err)
	case "facade":
		n := w.nodes[a.node]
		fctx, through := w.facadeCtx(a)
		rc, err := n.ws().Invoke(fctx, name, args)
		w.logf("%s(wt=%v) invoke %s(%s,%d,%q) -> seq=%d fwd=%v err=%v", a.name, through, name, args.GetId(), args.GetCount(), args.GetLabel(), rc.Seq, rc.Forwarded, err)
		w.outcome(a.kind, err)
		if err == nil && !through && rc.Forwarded && rc.Seq != 0 {
			w.facade = append(w.facade, facadeWrite{node: a.node, gen: n.gen, seq: rc.Seq, desc: "invoke " + name, forbidden: forbidden})
		}
	}
}

// readWidget reads a widget as actor a sees it.
func (w *world) readWidget(a actor, key string) (*testdatav1.Widget, uint64, bool) {
	ctx := context.Background()
	switch a.kind {
	case "facade":
		row, ok, err := upstream.Get[*testdatav1.Widget](ctx, w.nodes[a.node].ws(), key)
		if err != nil || !ok {
			return nil, 0, false
		}
		return row.Value, row.Version, true
	default:
		row, ok, err := client.Get[*testdatav1.Widget](ctx, a.cw, key)
		if err != nil || !ok {
			return nil, 0, false
		}
		return row.Value, row.Version, true
	}
}

func (w *world) readIssue(a actor, key string) (uint64, bool) {
	ctx := context.Background()
	switch a.kind {
	case "facade":
		row, ok, err := upstream.Get[*testdatav1.Issue](ctx, w.nodes[a.node].ws(), key)
		return row.Version, err == nil && ok
	default:
		row, ok, err := client.Get[*testdatav1.Issue](ctx, a.cw, key)
		return row.Version, err == nil && ok
	}
}

// step runs one random operation.
func (w *world) step() {
	w.opn++
	acts := w.actors()
	a := acts[w.r.IntN(len(acts))]
	r := w.r
	switch x := r.IntN(100); {
	case x < 18: // single put/patch/delete
		o := w.genOp(a.name)
		w.count("op:" + o.kind)
		w.run(a, "crud", o)
	case x < 30: // patch widget
		o := wop{kind: "patch", msg: w.randWidget(widgetKeys[r.IntN(len(widgetKeys))], a.name), mask: w.mask(widgetPaths)}
		w.count("op:patch")
		w.run(a, "patch", o)
	case x < 38: // recreate
		key := widgetKeys[r.IntN(len(widgetKeys))]
		w.count("op:recreate")
		w.run(a, "recreate", wop{kind: "delete", proto: &testdatav1.Widget{}, key: key}, wop{kind: "put", msg: w.randWidget(key, a.name)})
	case x < 46: // batch
		n := 2 + r.IntN(3)
		var ops []wop
		for range n {
			ops = append(ops, w.genOp(a.name))
		}
		w.count("op:batch")
		w.run(a, "batch", ops...)
	case x < 55: // CAS
		w.count("op:cas")
		if r.IntN(2) == 0 {
			key := widgetKeys[r.IntN(len(widgetKeys))]
			_, v, ok := w.readWidget(a, key)
			if !ok {
				v = 0
			}
			if r.IntN(5) == 0 {
				v++ // a stale guess
			}
			if ok {
				w.run(a, "cas", wop{kind: "patch", msg: w.randWidget(key, a.name), mask: w.mask(widgetPaths), expected: &v})
			} else {
				w.run(a, "cas-create", wop{kind: "put", msg: w.randWidget(key, a.name), expected: &v})
			}
		} else {
			key := issueKeys[r.IntN(len(issueKeys))]
			v, ok := w.readIssue(a, key)
			if ok {
				w.run(a, "cas", wop{kind: "patch", msg: w.randIssue(key, a.name), mask: w.mask(issuePaths), expected: &v})
			} else {
				w.run(a, "cas-create", wop{kind: "put", msg: w.randIssue(key, a.name), expected: &v})
			}
		}
	case x < 64: // custom mutation
		key := widgetKeys[r.IntN(len(widgetKeys))]
		if r.IntN(2) == 0 {
			w.count("op:bump")
			w.invoke(a, "bump", &testdatav1.Widget{Id: key, Count: []int32{1, 2, 13}[r.IntN(3)]})
		} else {
			w.count("op:stamp")
			w.invoke(a, "stamp", &testdatav1.Widget{Id: key, Label: w.label(a.name)})
		}
	case x < 82: // text edit
		key := widgetKeys[r.IntN(len(widgetKeys))]
		cur, _, ok := w.readWidget(a, key)
		plain := ""
		if ok {
			plain, _ = textOf(cur.GetBody())
		}
		s := editText(r, plain, a.name)
		w.count("op:text")
		if ok && r.IntN(4) != 0 {
			w.run(a, "text", wop{kind: "patch", msg: &testdatav1.Widget{Id: key}, text: &s})
		} else {
			x := w.randWidget(key, a.name)
			w.run(a, "text-put", wop{kind: "put", msg: x, text: &s})
		}
	default: // unique insert
		w.uniq++
		key := fmt.Sprintf("u%03d", w.uniq)
		forbidden := r.IntN(4) == 0
		lbl := "uniq-" + key
		if forbidden {
			lbl = "X" + lbl
		}
		ins := uniqInsert{key: key, label: lbl, forbidden: forbidden, via: a.kind, node: a.node}
		msg := &testdatav1.Widget{Id: key, Label: lbl}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		var err error
		switch a.kind {
		case "facade":
			fctx, through := w.facadeCtx(a)
			if through {
				ins.via = "facade-wt"
			}
			var rc upstream.Receipt
			rc, err = w.nodes[a.node].ws().Put(fctx, msg)
			if err == nil && !through {
				w.facade = append(w.facade, facadeWrite{node: a.node, gen: w.nodes[a.node].gen, seq: rc.Seq, desc: "insert " + key, forbidden: forbidden})
			}
		default:
			_, err = a.cw.Put(ctx, msg)
		}
		w.outcome(a.kind, err)
		switch {
		case err == nil:
			ins.outcome = "ok"
		case errors.Is(err, upstream.ErrOriginUnavailable), errors.Is(err, client.ErrUnconfirmed):
			ins.outcome = "unknown"
		default:
			ins.outcome = "failed:" + err.Error()
		}
		w.logf("%s insert %s label=%s via=%s -> %s", a.name, key, lbl, ins.via, ins.outcome)
		w.count("op:insert")
		w.inserts = append(w.inserts, ins)
	}
	w.fault()
}

// fault injects at most one fault after an operation.
func (w *world) fault() {
	r := w.r
	for i, left := range w.stallLeft {
		if left > 0 {
			w.stallLeft[i]--
			if left == 1 && w.nodes[i].chaos.releaseStalls() > 0 {
				w.logf("fault: stalled stream of %s ends", w.nodes[i].name)
			}
		}
	}
	n := w.nodes[r.IntN(len(w.nodes))]
	ni := slices.Index(w.nodes, n)
	switch x := r.IntN(1000); {
	case x < 70:
		if n.part.down.Load() {
			n.part.Heal()
			w.logf("fault: heal %s", n.name)
		} else {
			n.part.Cut()
			w.logf("fault: cut %s", n.name)
		}
		w.count("fault:partition")
	case x < 110:
		if w.gcWindow {
			return
		}
		k := int64(1 + r.IntN(2))
		n.part.losePushes.Store(k)
		w.logf("fault: lose %d push responses of %s", k, n.name)
		w.count("fault:lose-push")
	case x < 150:
		b := int64(20 + r.IntN(2000))
		n.chaos.breakStream.Store(b)
		w.logf("fault: break next stream of %s after %d bytes", n.name, b)
		w.count("fault:break-stream")
	case x < 170:
		b := int64(20 + r.IntN(600))
		n.chaos.breakPull.Store(b)
		w.logf("fault: break next pull of %s after %d bytes", n.name, b)
		w.count("fault:break-pull")
	case x < 185:
		b := int64(50 + r.IntN(800))
		n.part.breakBootstrap.Store(b)
		w.logf("fault: break next bootstrap of %s after %d bytes", n.name, b)
		w.count("fault:break-bootstrap")
	case x < 215:
		w.logf("fault: restart %s", n.name)
		n.restart()
		w.count("fault:restart")
	case x < 235:
		ctx := context.Background()
		if n.scoped {
			Expect(n.link.SetSubscriptions(ctx, convWS)).To(Succeed())
			n.scoped = false
			w.logf("fault: %s subscribes to everything", n.name)
		} else {
			Expect(n.link.SetSubscriptions(ctx, convWS,
				&syncplanev1.Subscription{Entity: "widget", Predicate: query.Eq("flag", true)},
				&syncplanev1.Subscription{Entity: "issue", Predicate: query.Eq("state", "open")},
				&syncplanev1.Subscription{Entity: "project"},
			)).To(Succeed())
			n.scoped = true
			w.logf("fault: %s narrows its subscriptions", n.name)
		}
		w.count("fault:subscriptions")
	case x < 245:
		w.logf("fault: stall next stream of %s", n.name)
		n.chaos.stallStream.Store(int64(20 + r.IntN(200)))
		w.stallLeft[ni] = 3 + r.IntN(8)
		w.count("fault:stall-stream")
	case x < 270:
		d := w.downs[r.IntN(len(w.downs))]
		d.catchUp()
		w.logf("downstream of %s caught up", d.node.name)
	}
	_ = ni
}

// gcReset makes the origin collect tombstones past the cursor of a node it
// cut off, so that node has to bootstrap again.
func (w *world) gcReset() {
	n := w.nodes[w.r.IntN(len(w.nodes))]
	n.part.Cut()
	key := widgetKeys[w.r.IntN(len(widgetKeys))]
	ctx := context.Background()
	_, err := w.writers[0].Delete(ctx, &testdatav1.Widget{}, key)
	Expect(err).NotTo(HaveOccurred())
	_, err = w.writers[0].Put(ctx, w.randWidget(widgetKeys[w.r.IntN(len(widgetKeys))], "gc"))
	if err != nil && client.CodeOf(err) != server.CodePermissionDenied {
		Expect(err).NotTo(HaveOccurred())
	}
	time.Sleep(5 * time.Millisecond)
	Expect(w.o.srv.CollectGarbage(ctx, convWS)).To(Succeed())
	w.gcWindow = true
	w.logf("fault: %s cut off while the origin collected tombstones (deleted %s)", n.name, key)
	w.count("fault:gc-reset")
}

// quiesce heals every fault and waits until every link has flushed and
// caught up with the origin.
func (w *world) quiesce() {
	ctx := context.Background()
	for _, n := range w.nodes {
		n.healAll()
		if n.scoped {
			Expect(n.link.SetSubscriptions(ctx, convWS)).To(Succeed())
			n.scoped = false
		}
	}
	for _, d := range w.downs {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := d.w.Confirm(cctx)
		cancel()
		w.logf("downstream confirm: %v", err)
	}
	for _, cw := range w.writers {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := cw.Confirm(cctx)
		cancel()
		Expect(err == nil || client.CodeOf(err) != "").To(BeTrue(), "%v", err)
	}
	for round := 0; ; round++ {
		before := w.o.head()
		for _, n := range w.nodes {
			err := n.flush(30 * time.Second)
			Expect(err).NotTo(HaveOccurred(), "flush %s: status %+v\n%s", n.name, n.status(), w.tail())
		}
		after := w.o.head()
		caught := true
		for _, n := range w.nodes {
			st := n.status()
			if st.OriginSeq != after || st.Pending != 0 || st.State != upstream.StateLive {
				caught = false
			}
		}
		if before == after && caught {
			break
		}
		if round > 30 {
			var sts []string
			for _, n := range w.nodes {
				sts = append(sts, fmt.Sprintf("%s: %+v", n.name, n.status()))
			}
			Fail(fmt.Sprintf("links never settled: origin head %d, %s\n%s", after, strings.Join(sts, "; "), w.tail()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	w.gcWindow = false
}

// check compares every replica with the origin and checks the bookkeeping of
// every write recorded so far.
func (w *world) check() {
	origin := dumpRows(w.o.srv, convWS)
	for _, n := range w.nodes {
		local := dumpRows(n.srv, convWS)
		d := diffRows("origin", origin, n.name, local)
		Expect(d).To(BeEmpty(), "local engine %s diverged from the origin\n%s", n.name, w.tail())
	}
	for i, d := range w.downs {
		d.catchUp()
		diff := diffRows(d.node.name, dumpRows(d.node.srv, convWS), fmt.Sprintf("d%d", i), d.rowset())
		Expect(diff).To(BeEmpty(), "downstream replica d%d diverged from %s\n%s", i, d.node.name, w.tail())
	}

	// Each rejection is reported once.
	for _, n := range w.nodes {
		Expect(n.late.Load()).To(BeZero(), "%s: OnRejected ran after Close returned\n%s", n.name, w.tail())
		seen := map[uint64]crej{}
		seenSeq := map[uint64]crej{}
		for _, r := range n.rejected() {
			Expect(r.Workspace).To(Equal(convWS))
			if prev, dup := seen[r.MutationID]; dup {
				Fail(fmt.Sprintf("%s reported mutation %d twice: %+v and %+v\n%s", n.name, r.MutationID, prev, r, w.tail()))
			}
			seen[r.MutationID] = r
			if r.LocalSeq != 0 {
				if prev, dup := seenSeq[r.LocalSeq]; dup {
					Fail(fmt.Sprintf("%s reported local seq %d twice: %+v and %+v\n%s", n.name, r.LocalSeq, prev, r, w.tail()))
				}
				seenSeq[r.LocalSeq] = r
			}
			Expect([]string{server.CodePermissionDenied, server.CodeConflict, "missing"}).To(ContainElement(r.Code),
				"%s reported an unexpected rejection code: %+v\n%s", n.name, r, w.tail())
		}
	}

	// The facade's receipts agree with the reports.
	ctx := context.Background()
	for _, f := range w.facade {
		n := w.nodes[f.node]
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := n.link.Await(actx, convWS, f.seq)
		cancel()
		var reported []crej
		for _, r := range n.rejected() {
			if r.LocalSeq == f.seq {
				reported = append(reported, r)
			}
		}
		var rej *upstream.RejectedError
		switch {
		case err == nil:
			Expect(reported).To(BeEmpty(), "%s: write %d (%s) awaited as applied but reported as rejected\n%s", n.name, f.seq, f.desc, w.tail())
			Expect(f.forbidden).To(BeFalse(), "%s: write %d (%s) the origin must refuse awaited as applied\n%s", n.name, f.seq, f.desc, w.tail())
		case errors.As(err, &rej):
			Expect(reported).To(HaveLen(1), "%s: write %d (%s) awaited as rejected (%v) but reported %d times\n%s", n.name, f.seq, f.desc, err, len(reported), w.tail())
			Expect(reported[0].MutationID).To(Equal(rej.MutationID))
			Expect(reported[0].Code).To(Equal(rej.Code))
		case errors.Is(err, upstream.ErrUnknownWrite):
			// The link forgets decided writes after a while; the report is
			// what remains to check.
			w.count("await:unknown")
			if f.forbidden {
				Expect(reported).To(HaveLen(1), "%s: write %d (%s) the origin must refuse, forgotten by Await, reported %d times\n%s", n.name, f.seq, f.desc, len(reported), w.tail())
			} else {
				Expect(len(reported)).To(BeNumerically("<=", 1), "%s: write %d (%s) reported %d times\n%s", n.name, f.seq, f.desc, len(reported), w.tail())
			}
		default:
			Fail(fmt.Sprintf("%s: await of write %d (%s, gen %d of %d): %v\n%s", n.name, f.seq, f.desc, f.gen, n.gen, err, w.tail()))
		}
	}

	// No accepted insert is lost, and every refused one is gone and reported
	// once by the node that took it.
	for _, ins := range w.inserts {
		row := origin["widget"][ins.key]
		mentions := 0
		if ins.node >= 0 {
			for _, r := range w.nodes[ins.node].rejected() {
				if slices.Contains(r.Rows, upstream.RowRef{Entity: "widget", Key: ins.key}) {
					mentions++
				}
			}
		}
		switch {
		case ins.forbidden:
			Expect(row).To(BeNil(), "refused insert %+v is on the origin\n%s", ins, w.tail())
			if ins.outcome == "ok" && (ins.via == "facade" || ins.via == "down") {
				Expect(mentions).To(Equal(1), "refused insert %+v reported %d times\n%s", ins, mentions, w.tail())
			} else {
				Expect(mentions).To(BeZero(), "insert %+v refused synchronously but also reported\n%s", ins, w.tail())
			}
		case ins.outcome == "ok":
			Expect(row).NotTo(BeNil(), "accepted insert %+v is lost\n%s", ins, w.tail())
			Expect(row.(*testdatav1.Widget).GetLabel()).To(Equal(ins.label))
			Expect(mentions).To(BeZero(), "accepted insert %+v reported as rejected\n%s", ins, w.tail())
		}
	}
}

func newWorld(seed uint64) *world {
	GinkgoHelper()
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	keys, err := fractional.NKeysBetween("", "", 5)
	Expect(err).NotTo(HaveOccurred())
	w := &world{seed: seed, r: r, stats: map[string]int{}, posKeys: keys}
	w.o = newCOrigin(server.WithGC(time.Millisecond, 0))
	nNodes := 1 + r.IntN(2)
	for i := range nNodes {
		n := newCNode(w.o, fmt.Sprintf("n%d", i), nil)
		Expect(n.link.AddWorkspace(context.Background(), convWS)).To(Succeed())
		w.nodes = append(w.nodes, n)
	}
	for i := range 2 {
		w.writers = append(w.writers, w.o.client(fmt.Sprintf("origin-writer-%d-%d", seed, i)))
	}
	for _, n := range w.nodes {
		Expect(n.flush(20 * time.Second)).To(Succeed())
	}
	for i := range 1 + r.IntN(2) {
		w.downs = append(w.downs, newDownstream(w.nodes[i%len(w.nodes)]))
	}
	w.stats["nodes"] = nNodes
	w.stallLeft = make([]int, nNodes)
	w.stats["downstreams"] = len(w.downs)
	return w
}

func (w *world) summary() string {
	keys := make([]string, 0, len(w.stats))
	for k := range w.stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, w.stats[k]))
	}
	rejections, resets := 0, 0
	for _, n := range w.nodes {
		rejections += len(n.rejected())
		n.mu.Lock()
		resets += n.resets
		n.mu.Unlock()
	}
	dresets := 0
	for _, d := range w.downs {
		dresets += d.resets
	}
	parts = append(parts, fmt.Sprintf("reported-rejections=%d", rejections), fmt.Sprintf("link-resets=%d", resets), fmt.Sprintf("downstream-resets=%d", dresets))
	return fmt.Sprintf("seed %d: %s", w.seed, strings.Join(parts, " "))
}

// runWorld runs ops random operations of seed, with a checkpoint every 30.
func runWorld(seed uint64, ops int) {
	AddReportEntry("seed", seed)
	GinkgoWriter.Printf("convergence seed %d, %d ops\n", seed, ops)
	w := newWorld(seed)
	DeferCleanup(func() {
		s := w.summary()
		AddReportEntry("stats", s)
		if os.Getenv("UPSTREAM_CONV_REPORT") != "" {
			fmt.Fprintf(os.Stderr, "%s failed=%v\n", s, CurrentSpecReport().Failed())
		}
		if CurrentSpecReport().Failed() {
			GinkgoWriter.Println(w.tail())
		}
	})
	checkpoint := 30
	for i := 1; i <= ops; i++ {
		w.step()
		if i%checkpoint == 0 || i == ops {
			w.logf("checkpoint")
			w.quiesce()
			w.check()
			w.count("checkpoints")
			if w.chance(0.5) && i != ops {
				w.gcReset()
			}
		}
	}
}

var _ = Describe("Convergence", Label("convergence"), func() {
	seeds, ops := convPlan()
	for _, seed := range seeds {
		It(fmt.Sprintf("converges under random writes and faults (seed %d)", seed), func() {
			runWorld(seed, ops)
		})
	}

	// Seeds that found a bug no smaller spec reproduces. Timing decides
	// whether a seed fails, so each runs its schedule a few times.
	It("settles every write-through receipt (seed 9)", func() {
		for range 3 {
			runWorld(9, 150)
		}
	})
})

func protoName(s string) protoreflect.Name { return protoreflect.Name(s) }
