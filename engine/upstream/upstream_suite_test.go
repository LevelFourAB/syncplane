package upstream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream"
)

func init() { slog.SetDefault(slog.New(slog.DiscardHandler)) }

func TestUpstream(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(10 * time.Second)
	SetDefaultEventuallyPollingInterval(10 * time.Millisecond)
	RunSpecs(t, "Upstream Suite")
}

const ws = "ws1"

// partition is a RoundTripper that can cut the link off from the origin: a
// cut fails every new request and ends every open response, streams
// included.
type partition struct {
	base   http.RoundTripper
	down   atomic.Bool
	mu     sync.Mutex
	bodies map[*trackedBody]struct{}
	// pushes counts Push requests that reached the origin.
	pushes atomic.Int64
	// blockPush fails Push requests only.
	blockPush atomic.Bool
	// losePushes is how many Push responses to lose: the origin handles the
	// request, and the link sees a failure in transit.
	losePushes atomic.Int64
	// onLost runs after the origin handled a Push whose response is lost,
	// before the link sees the failure.
	onLost atomic.Pointer[func()]
	// breakBootstrap ends the next Bootstrap response after this many bytes.
	breakBootstrap atomic.Int64
	// bootstraps counts Bootstrap requests.
	bootstraps atomic.Int64
	// blockReads fails Pull and Subscribe requests only.
	blockReads atomic.Bool
}

func newPartition() *partition {
	return &partition{base: &http.Transport{}, bodies: map[*trackedBody]struct{}{}}
}

func (p *partition) RoundTrip(req *http.Request) (*http.Response, error) {
	if p.down.Load() {
		return nil, errors.New("partitioned")
	}
	if base := filepath.Base(req.URL.Path); (base == "Pull" || base == "Subscribe") && p.blockReads.Load() {
		return nil, errors.New("reads blocked")
	}
	if filepath.Base(req.URL.Path) == "Push" {
		if p.blockPush.Load() {
			return nil, errors.New("push blocked")
		}
		p.pushes.Add(1)
	}
	resp, err := p.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if filepath.Base(req.URL.Path) == "Push" && p.losePushes.Add(-1) >= 0 {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if f := p.onLost.Load(); f != nil {
			(*f)()
		}
		return nil, errors.New("response lost")
	}
	if filepath.Base(req.URL.Path) == "Bootstrap" {
		p.bootstraps.Add(1)
		if n := p.breakBootstrap.Swap(0); n > 0 {
			resp.Body = &brokenBody{ReadCloser: resp.Body, left: n}
		}
	}
	tb := &trackedBody{ReadCloser: resp.Body, p: p}
	p.mu.Lock()
	p.bodies[tb] = struct{}{}
	p.mu.Unlock()
	resp.Body = tb
	return resp, nil
}

// Cut fails every request from now on and ends the open ones.
func (p *partition) Cut() {
	p.down.Store(true)
	p.mu.Lock()
	bodies := p.bodies
	p.bodies = map[*trackedBody]struct{}{}
	p.mu.Unlock()
	for b := range bodies {
		_ = b.ReadCloser.Close()
	}
}

func (p *partition) Heal() { p.down.Store(false) }

type trackedBody struct {
	io.ReadCloser
	p *partition
}

func (b *trackedBody) Close() error {
	b.p.mu.Lock()
	delete(b.p.bodies, b)
	b.p.mu.Unlock()
	return b.ReadCloser.Close()
}

// origin is the authoritative server, over HTTP.
type origin struct {
	srv   *server.Server
	ts    *httptest.Server
	store storage.Adapter
}

func completeAll(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
	var todos []*todov1.Todo
	if err := tx.Scan(&todov1.Todo{}, func(msg proto.Message) error {
		todos = append(todos, msg.(*todov1.Todo))
		return nil
	}); err != nil {
		return err
	}
	for _, t := range todos {
		t.Completed = true
		if err := tx.Put(t); err != nil {
			return err
		}
	}
	return nil
}

func newOrigin(opts ...server.Option) *origin {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "origin.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(store.Close)
	srv, err := server.New(store, append([]server.Option{
		server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}),
		server.WithMutator("complete_all", &todov1.CompleteAllArgs{}, completeAll),
		server.WithHeartbeat(time.Second),
	}, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(srv.Close)
	ts := httptest.NewServer(srv.HTTPHandler())
	DeferCleanup(func() {
		ts.CloseClientConnections()
		ts.Close()
	})
	return &origin{srv: srv, ts: ts, store: store}
}

// put writes rows on the origin, as another client of it would.
func (o *origin) put(msgs ...proto.Message) {
	GinkgoHelper()
	Expect(o.srv.Apply(context.Background(), ws, func(tx *server.ApplyTx) error {
		for _, m := range msgs {
			if err := tx.Put(m); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
}

func (o *origin) patch(msg proto.Message, mask ...string) {
	GinkgoHelper()
	Expect(o.srv.Apply(context.Background(), ws, func(tx *server.ApplyTx) error {
		return tx.Patch(msg, mask, nil)
	})).To(Succeed())
}

func (o *origin) todo(key string) *todov1.Todo {
	GinkgoHelper()
	return getRow[*todov1.Todo](o.srv, key)
}

func getRow[T proto.Message](srv *server.Server, key string) T {
	GinkgoHelper()
	var zero T
	msg := zero.ProtoReflect().New().Interface().(T)
	var found bool
	Expect(srv.View(context.Background(), ws, func(tx *server.ViewTx) error {
		var err error
		found, _, err = tx.Get(msg, key)
		return err
	})).To(Succeed())
	if !found {
		return zero
	}
	return msg
}

// local is a local engine linked to an origin, restartable over one store
// file.
type local struct {
	o    *origin
	path string
	part *partition
	mut  func(*upstream.Config)
	opts []server.Option

	store *sqlitestore.Store
	srv   *server.Server
	link  *upstream.Link
	ts    *httptest.Server

	mu         sync.Mutex
	rejections []upstream.Rejection
	resets     []string
	revoked    []string
}

func newLocal(o *origin, mut func(*upstream.Config), opts ...server.Option) *local {
	GinkgoHelper()
	n := &local{o: o, path: filepath.Join(GinkgoT().TempDir(), "local.db"), part: newPartition(), mut: mut, opts: opts}
	n.start()
	DeferCleanup(n.stop)
	return n
}

func (n *local) config() upstream.Config {
	cfg := upstream.Config{
		OriginURL:  n.o.ts.URL,
		HTTPClient: &http.Client{Transport: n.part},
		Bindings: []upstream.Binding{
			{Prototype: &todov1.Todo{}},
			{Prototype: &todov1.Note{}},
			{Prototype: &todov1.Annotation{}, Direction: upstream.PullOnly},
		},
		Logger:   slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelDebug})),
		RetryMin: 10 * time.Millisecond,
		RetryMax: 100 * time.Millisecond,
		OnRejected: func(r upstream.Rejection) {
			n.mu.Lock()
			n.rejections = append(n.rejections, r)
			n.mu.Unlock()
		},
		OnReset: func(ws string) {
			n.mu.Lock()
			n.resets = append(n.resets, ws)
			n.mu.Unlock()
		},
		OnAccessRevoked: func(ws string) {
			n.mu.Lock()
			n.revoked = append(n.revoked, ws)
			n.mu.Unlock()
		},
	}
	if n.mut != nil {
		n.mut(&cfg)
	}
	return cfg
}

func (n *local) start() {
	GinkgoHelper()
	store, err := sqlitestore.Open(n.path)
	Expect(err).NotTo(HaveOccurred())
	link, err := upstream.New(n.config())
	Expect(err).NotTo(HaveOccurred())
	opts := append([]server.Option{
		server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}, &testdatav1.Widget{}),
	}, n.opts...)
	srv, err := server.New(store, append(opts, link.ServerOptions()...)...)
	Expect(err).NotTo(HaveOccurred())
	Expect(link.Start(context.Background(), srv)).To(Succeed())
	n.store, n.srv, n.link = store, srv, link
	n.ts = httptest.NewServer(srv.HTTPHandler())
}

func (n *local) stop() {
	if n.link == nil {
		return
	}
	n.ts.CloseClientConnections()
	n.ts.Close()
	Expect(n.link.Close()).To(Succeed())
	n.part.base.(*http.Transport).CloseIdleConnections()
	Expect(n.srv.Close()).To(Succeed())
	Expect(n.store.Close()).To(Succeed())
	n.link, n.srv, n.store, n.ts = nil, nil, nil, nil
}

func (n *local) restart() {
	GinkgoHelper()
	n.stop()
	n.start()
}

func (n *local) ws() *upstream.Workspace { return n.link.Workspace(ws) }

func (n *local) add(subs ...any) {
	GinkgoHelper()
	Expect(n.link.AddWorkspace(context.Background(), ws)).To(Succeed())
}

func (n *local) flush() {
	GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	Expect(n.link.Flush(ctx, ws)).To(Succeed(), func() string { return fmt.Sprintf("status %+v", n.status()) })
}

func (n *local) todo(key string) *todov1.Todo {
	GinkgoHelper()
	return getRow[*todov1.Todo](n.srv, key)
}

func (n *local) put(msgs ...proto.Message) upstream.Receipt {
	GinkgoHelper()
	rc, err := n.ws().Apply(context.Background(), func(tx *server.ApplyTx) error {
		for _, m := range msgs {
			if err := tx.Put(m); err != nil {
				return err
			}
		}
		return nil
	})
	Expect(err).NotTo(HaveOccurred())
	return rc
}

func (n *local) patch(msg proto.Message, mask ...string) upstream.Receipt {
	GinkgoHelper()
	rc, err := n.ws().Patch(context.Background(), msg, mask, nil)
	Expect(err).NotTo(HaveOccurred())
	return rc
}

func (n *local) rejected() []upstream.Rejection {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]upstream.Rejection(nil), n.rejections...)
}

func (n *local) status() upstream.Status {
	st, _ := n.link.Status(ws)
	return st
}

func title(t *todov1.Todo) string { return t.GetTitle() }

func marshal(m proto.Message) ([]byte, error) { return proto.Marshal(m) }

// pushDirect pushes one mutation to the origin under the given client id.
func pushDirect(o *origin, clientID string, id uint64, crud *syncplanev1.CrudMutation) *syncplanev1.MutationResult {
	GinkgoHelper()
	resp, err := o.srv.Push(context.Background(), connect.NewRequest(&syncplanev1.PushRequest{
		Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: []*syncplanev1.Mutation{{
			ClientId: clientID, MutationId: id, Payload: &syncplanev1.Mutation_Crud{Crud: crud},
		}}}},
	}))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(BeEmpty())
	return resp.Msg.GetResults()[0].GetResults()[0]
}

// linkGoroutines counts the goroutines running link code.
func linkGoroutines() int {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if isLinkGoroutine(g) {
			n++
		}
	}
	return n
}

func dumpLinkGoroutines() string {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if isLinkGoroutine(g) {
			out = append(out, g)
		}
	}
	return strings.Join(out, "\n\n")
}

// isLinkGoroutine reports a goroutine running a frame of the link package.
func isLinkGoroutine(stack string) bool {
	for _, line := range strings.Split(stack, "\n") {
		if strings.HasPrefix(line, "github.com/levelfourab/syncplane/engine/upstream.") {
			return true
		}
	}
	return false
}

func anyOf(m proto.Message) (*anypb.Any, error) { return anypb.New(m) }

func connectRequest[T any](msg *T) *connect.Request[T] { return connect.NewRequest(msg) }

// brokenBody fails once left bytes were read.
type brokenBody struct {
	io.ReadCloser
	left int64
}

func (b *brokenBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errors.New("connection reset")
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, err
}
