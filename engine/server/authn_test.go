package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// recordingPerms captures the Principal each hook was handed, so a spec can
// assert what the engine passed rather than what the hook could have dug out
// of ctx itself. It embeds AllowAll: only the hooks it records are overridden,
// which also exercises the partial-implementation pattern (issue 070).
type recordingPerms struct {
	server.AllowAll
	mu       sync.Mutex
	mutate   []server.Principal
	read     []server.Principal
	epoch    []server.Principal
	rejectFn func(p server.Principal) error
}

func (p *recordingPerms) CanMutate(_ context.Context, pr server.Principal, _ string, _ server.Write) error {
	p.mu.Lock()
	p.mutate = append(p.mutate, pr)
	p.mu.Unlock()
	if p.rejectFn != nil {
		return p.rejectFn(pr)
	}
	return nil
}

func (p *recordingPerms) ReadFilter(_ context.Context, pr server.Principal, _, _ string) (server.RowFilter, error) {
	p.mu.Lock()
	p.read = append(p.read, pr)
	p.mu.Unlock()
	return nil, nil
}

func (p *recordingPerms) FilterEpoch(_ context.Context, pr server.Principal, _ string) (uint64, error) {
	p.mu.Lock()
	p.epoch = append(p.epoch, pr)
	p.mu.Unlock()
	return 0, nil
}

func (p *recordingPerms) seen() (mutate, read, epoch []server.Principal) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]server.Principal(nil), p.mutate...),
		append([]server.Principal(nil), p.read...),
		append([]server.Principal(nil), p.epoch...)
}

var _ = Describe("Authentication seam (issues 062, 063)", func() {
	const ws = "w1"

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
		perms  *recordingPerms
	)

	// start mounts srv and points a client at it. header, when non-nil, is
	// applied to every request the helpers below send.
	start := func(srv *server.Server, header http.Header) {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL,
			connect.WithInterceptors(headerInterceptor(header)))
	}

	push := func() ([]*syncplanev1.MutationResult, error) {
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			[]*syncplanev1.Mutation{
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hello"}),
			})))
		if err != nil {
			return nil, err
		}
		return pushResults(resp.Msg), nil
	}

	pullAs := func(clientID string) (*syncplanev1.PullResponse, error) {
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: clientID,
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	pull := func() (*syncplanev1.PullResponse, error) { return pullAs("c1") }

	bearer := func(token string) http.Header {
		h := http.Header{}
		h.Set("Authorization", "Bearer "+token)
		return h
	}

	BeforeEach(func() {
		ctx = context.Background()
		perms = &recordingPerms{}
	})

	Describe("with no authenticator installed", func() {
		BeforeEach(func() {
			start(newTestServer(server.WithPermissions(perms)), nil)
		})

		It("admits the request and hands the hooks the anonymous principal", func() {
			_, err := push()
			Expect(err).NotTo(HaveOccurred())
			_, err = pull()
			Expect(err).NotTo(HaveOccurred())

			mutate, _, epoch := perms.seen()
			Expect(mutate).To(HaveLen(1))
			Expect(mutate[0].IsAnonymous()).To(BeTrue())
			Expect(mutate[0]).To(Equal(server.Principal{}))
			Expect(epoch).NotTo(BeEmpty())
			Expect(epoch[0].IsAnonymous()).To(BeTrue())
		})

		It("ignores a credential it was never configured to read", func() {
			start(newTestServer(server.WithPermissions(perms)), bearer("anything"))
			_, err := push()
			Expect(err).NotTo(HaveOccurred())

			mutate, _, _ := perms.seen()
			Expect(mutate[0].IsAnonymous()).To(BeTrue(),
				"a bearer token means nothing without an Authenticator")
		})
	})

	Describe("with an authenticator installed", func() {
		alice := server.Principal{
			Subject: "alice",
			Issuer:  "test",
			Claims:  map[string]any{"role": "admin"},
		}

		authn := func() server.Authenticator {
			return server.StaticTokenAuthenticator(map[string]server.Principal{
				"alice-token": alice,
			})
		}

		It("hands every hook the verified principal", func() {
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(authn()),
			), bearer("alice-token"))

			_, err := push()
			Expect(err).NotTo(HaveOccurred())
			_, err = pull()
			Expect(err).NotTo(HaveOccurred())

			mutate, read, epoch := perms.seen()
			Expect(mutate).To(HaveLen(1))
			Expect(mutate[0]).To(Equal(alice))
			Expect(mutate[0].IsAnonymous()).To(BeFalse())
			role, ok := mutate[0].StringClaim("role")
			Expect(ok).To(BeTrue())
			Expect(role).To(Equal("admin"))

			Expect(epoch).NotTo(BeEmpty())
			Expect(epoch[0]).To(Equal(alice))
			for _, p := range read {
				Expect(p).To(Equal(alice))
			}
		})

		It("rejects a bad credential as unauthenticated, before the handler runs", func() {
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(authn()),
			), bearer("wrong-token"))

			_, err := push()
			Expect(err).To(HaveOccurred())
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnauthenticated))

			mutate, _, epoch := perms.seen()
			Expect(mutate).To(BeEmpty(), "authentication must run before CanMutate")
			Expect(epoch).To(BeEmpty())
		})

		It("rejects a missing credential as unauthenticated", func() {
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(authn()),
			), nil)

			_, err := pull()
			Expect(err).To(HaveOccurred())
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnauthenticated))
		})

		subscribe := func() *connect.ServerStreamForClient[syncplanev1.SubscribeResponse] {
			GinkgoHelper()
			stream, err := client.Subscribe(ctx, connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c1",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
			}))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = stream.Close() })
			return stream
		}

		It("rejects the Subscribe stream at open on a bad credential", func() {
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(authn()),
			), bearer("wrong-token"))

			stream := subscribe()
			Expect(stream.Receive()).To(BeFalse(), "stream must not yield before authenticating")
			Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeUnauthenticated))
		})

		It("admits the Subscribe stream on a good credential", func() {
			// The counterpart to the spec above: it proves the rejection there
			// is the server reading the stream's credential and disliking it,
			// not the credential failing to ride a streaming request at all. A
			// short heartbeat gives the open stream something to deliver.
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(authn()),
				server.WithHeartbeat(20*time.Millisecond),
			), bearer("alice-token"))

			stream := subscribe()
			Expect(stream.Receive()).To(BeTrue(), "authenticated stream must deliver its catch-up page")
			Expect(stream.Err()).NotTo(HaveOccurred())
		})

		It("surfaces a verifier outage as internal, not as unauthenticated", func() {
			// A client must not read "identity provider is down" as "your
			// credential is bad" — that would have it discard a valid session.
			broken := server.AuthenticatorFunc(
				func(context.Context, http.Header) (server.Principal, error) {
					return server.Principal{}, errors.New("JWKS endpoint unreachable")
				})
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(broken),
			), bearer("alice-token"))

			_, err := push()
			Expect(err).To(HaveOccurred())
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInternal))
		})

		It("admits an anonymous caller when the authenticator says so", func() {
			anon := server.AuthenticatorFunc(
				func(context.Context, http.Header) (server.Principal, error) {
					return server.Principal{}, nil
				})
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(anon),
			), nil)

			_, err := push()
			Expect(err).NotTo(HaveOccurred())

			mutate, _, _ := perms.seen()
			Expect(mutate[0].IsAnonymous()).To(BeTrue())
		})

		It("lets a policy reject the anonymous caller", func() {
			perms.rejectFn = func(p server.Principal) error {
				if p.IsAnonymous() {
					return server.Reject(server.CodePermissionDenied, "sign in to make changes")
				}
				return nil
			}
			anon := server.AuthenticatorFunc(
				func(context.Context, http.Header) (server.Principal, error) {
					return server.Principal{}, nil
				})
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(anon),
			), nil)

			// A policy "no" is an ack, not an RPC error — the client drops the
			// mutation rather than retrying it forever.
			resp, err := push()
			Expect(err).NotTo(HaveOccurred())
			Expect(resp[0].GetStatus()).
				To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(resp[0].GetErrorCode()).To(Equal(server.CodePermissionDenied))
		})
	})

	Describe("StaticTokenAuthenticator", func() {
		authenticate := func(a server.Authenticator, header http.Header) (server.Principal, error) {
			return a.Authenticate(context.Background(), header)
		}

		It("rejects every request when configured with no tokens", func() {
			a := server.StaticTokenAuthenticator(nil)
			_, err := authenticate(a, bearer("anything"))
			Expect(err).To(MatchError(server.ErrUnauthenticated))
		})

		It("rejects an empty configured token rather than admitting a blank one", func() {
			a := server.StaticTokenAuthenticator(map[string]server.Principal{
				"": {Subject: "ghost"},
			})
			_, err := authenticate(a, bearer(""))
			Expect(err).To(MatchError(server.ErrUnauthenticated))
		})

		It("rejects a non-bearer Authorization header", func() {
			a := server.StaticTokenAuthenticator(map[string]server.Principal{"t": {Subject: "s"}})
			h := http.Header{}
			h.Set("Authorization", "Basic dXNlcjpwYXNz")
			_, err := authenticate(a, h)
			Expect(err).To(MatchError(server.ErrUnauthenticated))
		})

		It("copies its token map so a later mutation cannot change who is admitted", func() {
			tokens := map[string]server.Principal{"t1": {Subject: "s1"}}
			a := server.StaticTokenAuthenticator(tokens)
			tokens["t2"] = server.Principal{Subject: "intruder"}

			_, err := authenticate(a, bearer("t2"))
			Expect(err).To(MatchError(server.ErrUnauthenticated))

			p, err := authenticate(a, bearer("t1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(p.Subject).To(Equal("s1"))
		})
	})

	Describe("Principal ctx contract", func() {
		It("reports absence distinctly from an anonymous principal", func() {
			_, ok := server.PrincipalFrom(context.Background())
			Expect(ok).To(BeFalse())

			p, ok := server.PrincipalFrom(
				server.WithPrincipal(context.Background(), server.Principal{}))
			Expect(ok).To(BeTrue(), "an explicitly-set anonymous principal is present")
			Expect(p.IsAnonymous()).To(BeTrue())
		})

		It("round-trips a principal set by an app's own middleware", func() {
			want := server.Principal{Subject: "bob", Issuer: "middleware"}
			got, ok := server.PrincipalFrom(server.WithPrincipal(context.Background(), want))
			Expect(ok).To(BeTrue())
			Expect(got).To(Equal(want))
		})

		It("reads claims only at their declared type", func() {
			p := server.Principal{Claims: map[string]any{"role": "admin", "seats": 3}}

			role, ok := p.StringClaim("role")
			Expect(ok).To(BeTrue())
			Expect(role).To(Equal("admin"))

			_, ok = p.StringClaim("seats")
			Expect(ok).To(BeFalse(), "a non-string claim must not coerce")

			_, ok = p.StringClaim("missing")
			Expect(ok).To(BeFalse())

			seats, ok := p.Claim("seats")
			Expect(ok).To(BeTrue())
			Expect(seats).To(Equal(3))
		})

		It("is safe to read on a principal with no claims", func() {
			var p server.Principal
			_, ok := p.Claim("anything")
			Expect(ok).To(BeFalse())
			_, ok = p.StringClaim("anything")
			Expect(ok).To(BeFalse())
		})
	})

	Describe("read filters", func() {
		It("closes over the principal for the row scan", func() {
			// The filter runs after ReadFilter returned, during the snapshot
			// scan, so it must capture the principal rather than expect one.
			filtering := &principalScopedPerms{}
			srv := newTestServer(
				server.WithPermissions(filtering),
				server.WithAuthenticator(server.StaticTokenAuthenticator(
					map[string]server.Principal{
						"alice-token": {Subject: "alice"},
						"bob-token":   {Subject: "bob"},
					})),
			)
			start(srv, bearer("alice-token"))
			_, err := push()
			Expect(err).NotTo(HaveOccurred())

			resp, err := pull()
			Expect(err).NotTo(HaveOccurred())
			Expect(visibleTitles(resp.GetPatch().GetChanges())).To(ConsistOf("hello"))

			// Same server, same rows, different caller: the row-scan filter must
			// follow the principal, not the workspace.
			start(srv, bearer("bob-token"))
			resp, err = pullAs("c2")
			Expect(err).NotTo(HaveOccurred())
			Expect(visibleTitles(resp.GetPatch().GetChanges())).To(BeEmpty(),
				"bob may not read alice's rows")
		})
	})
})

// principalScopedPerms hides every row from anyone but alice, deciding inside
// the returned RowFilter — the placement that only works if the filter closed
// over the principal.
type principalScopedPerms struct{ server.AllowAll }

func (principalScopedPerms) ReadFilter(_ context.Context, p server.Principal, _, _ string) (server.RowFilter, error) {
	return func(string, proto.Message) bool { return p.Subject == "alice" }, nil
}

// visibleTitles returns the titles of non-tombstone todo changes.
func visibleTitles(changes []*syncplanev1.RowChange) []string {
	GinkgoHelper()
	var out []string
	for _, rc := range changes {
		if rc.GetTombstone() {
			continue
		}
		var todo todov1.Todo
		Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
		out = append(out, todo.GetTitle())
	}
	return out
}

// headerInterceptor stamps fixed headers onto every outgoing request, standing
// in for a client that carries a credential. It covers streaming calls too, so
// a spec can assert what the server does with a stream's credential rather
// than with its absence. A nil header sends none.
func headerInterceptor(header http.Header) connect.Interceptor {
	return &clientHeaderInterceptor{header: header}
}

type clientHeaderInterceptor struct{ header http.Header }

func (i *clientHeaderInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		applyHeader(req.Header(), i.header)
		return next(ctx, req)
	}
}

func (i *clientHeaderInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		applyHeader(conn.RequestHeader(), i.header)
		return conn
	}
}

// WrapStreamingHandler is a no-op: this interceptor is client-side only.
func (i *clientHeaderInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func applyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Set(k, v)
		}
	}
}
