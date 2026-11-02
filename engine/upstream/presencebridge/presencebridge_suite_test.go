package presencebridge_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/client"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

func TestPresenceBridge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Presence Bridge Suite")
}

// announce is the room announce interval of every engine in the suite, short
// enough that a spec can watch a record lapse.
const announce = 50 * time.Millisecond

var discard = slog.New(slog.DiscardHandler)

// startServer serves an engine over a SQLite store in the spec's temp dir. The
// X-User header names the caller's subject.
func startServer(name string, opts ...server.Option) (*server.Server, *httptest.Server) {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), name+".db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(store.Close)
	srv, err := server.New(store, append([]server.Option{
		server.WithAuthenticator(server.AuthenticatorFunc(func(_ context.Context, h http.Header) (server.Principal, error) {
			return server.Principal{Subject: h.Get("X-User")}, nil
		})),
		server.WithRoomAnnounce(announce),
		server.WithHeartbeat(50 * time.Millisecond),
		server.WithLogger(discard),
		server.WithEntities(&todov1.Todo{}),
	}, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(srv.Close)
	ts := httptest.NewServer(srv.HTTPHandler())
	DeferCleanup(ts.Close)
	return srv, ts
}

// asUser is a Connect interceptor that names the caller's subject.
type asUser string

func (u asUser) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("X-User", string(u))
		return next(ctx, req)
	}
}

func (u asUser) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("X-User", string(u))
		return conn
	}
}

func (u asUser) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// userClient returns a client of the server at ts that calls as user.
func userClient(ts *httptest.Server, user string) *client.Client {
	GinkgoHelper()
	c, err := client.New(ts.URL, client.WithHTTPClient(ts.Client()), client.WithInterceptors(asUser(user)))
	Expect(err).NotTo(HaveOccurred())
	return c
}

// cutter is an HTTP transport whose connection to the server can be cut: cut
// ends every request in flight and refuses new ones until restore.
type cutter struct {
	base http.RoundTripper

	mu     sync.Mutex
	down   bool
	ctx    context.Context
	cancel context.CancelFunc
}

func newCutter(base http.RoundTripper) *cutter {
	c := &cutter{base: base}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *cutter) RoundTrip(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	down, cut := c.down, c.ctx
	c.mu.Unlock()
	if down {
		return nil, errors.New("connection refused")
	}
	ctx, cancel := context.WithCancel(req.Context())
	context.AfterFunc(cut, cancel)
	return c.base.RoundTrip(req.WithContext(ctx))
}

func (c *cutter) cut() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down = true
	c.cancel()
}

func (c *cutter) restore() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down = false
	c.ctx, c.cancel = context.WithCancel(context.Background())
}

// roster follows one room through a Watch stream, the way a client renders
// it.
type roster struct {
	mu       sync.Mutex
	ready    bool
	members  map[string]string
	messages []string
}

// follow opens a Watch on room until ctx ends.
func follow(ctx context.Context, room *client.Room) *roster {
	r := &roster{members: make(map[string]string)}
	go func() {
		defer GinkgoRecover()
		for msg, err := range room.Watch(ctx) {
			if err != nil {
				return
			}
			r.apply(msg)
		}
	}()
	Eventually(r.isReady).Should(BeTrue(), "the room's snapshot must arrive")
	return r
}

func (r *roster) apply(msg *syncplanev1.WatchResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	member := func(p *syncplanev1.Participant) string {
		return p.GetSubject() + "/" + string(p.GetPresence().GetState())
	}
	switch body := msg.GetBody().(type) {
	case *syncplanev1.WatchResponse_Snapshot:
		r.ready = true
		clear(r.members)
		for _, p := range body.Snapshot.GetParticipants() {
			r.members[p.GetSessionId()] = member(p)
		}
	case *syncplanev1.WatchResponse_Updated:
		r.members[body.Updated.GetSessionId()] = member(body.Updated)
	case *syncplanev1.WatchResponse_Left:
		delete(r.members, body.Left)
	case *syncplanev1.WatchResponse_Message:
		r.messages = append(r.messages,
			body.Message.GetSenderSessionId()+"/"+body.Message.GetSenderSubject()+"/"+string(body.Message.GetPayload()))
	}
}

func (r *roster) isReady() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready
}

// Members returns session id to "subject/state".
func (r *roster) Members() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.members))
	for k, v := range r.members {
		out[k] = v
	}
	return out
}

// Messages returns every broadcast received, as "sender/subject/payload".
func (r *roster) Messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.messages...)
}
