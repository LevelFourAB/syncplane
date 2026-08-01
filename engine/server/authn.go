package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"
)

// ErrUnauthenticated is the sentinel an Authenticator returns to reject a
// request outright. The engine maps it (and anything wrapping it) to Connect's
// unauthenticated code, which is HTTP 401.
//
// Wrap it to explain the failure — errors.Join or fmt.Errorf("%w") both work:
//
//	return Principal{}, fmt.Errorf("%w: token expired", server.ErrUnauthenticated)
//
// The wrapped message is logged but never sent to the client: a credential
// verifier's reasons are an oracle, so the wire carries a fixed message.
var ErrUnauthenticated = errors.New("server: unauthenticated")

// Authenticator turns a request's headers into a verified Principal. It is the
// engine's authentication seam: install one with WithAuthenticator and the
// server runs it ahead of every sync RPC, stashing the result where the
// permission hooks receive it.
//
// The engine calls Authenticate on the public sync surface only (Server.Handler
// and, through it, Manager.SyncHandler). It never runs for trusted server-side
// paths — Server.Mutate, Server.Invoke, the admin service — which have no
// request to authenticate; those are gated by reachability and, for the admin
// service, by RegisterAuthorizer.
//
// # Contract
//
//   - Return a Principal with a non-empty Subject to admit an identified
//     caller.
//   - Return the zero Principal and a nil error to admit an ANONYMOUS caller.
//     Do this deliberately: it hands the request to the permission hooks with
//     no identity, and the default AllowAll policy allows everything.
//   - Return ErrUnauthenticated (or an error wrapping it) to reject the
//     request as 401.
//   - Return any other error to fail the request as an infrastructure fault
//     (internal, HTTP 500) — the right outcome when the identity provider is
//     unreachable, since answering "unauthenticated" would let a client treat a
//     transient outage as a credential problem and discard its session.
//
// Authenticate runs on the request's hot path, before the handler and before
// any storage work. Keep it fast: verify a signature locally and cache
// introspection results rather than calling out per request.
//
// Implementations must be safe for concurrent use.
type Authenticator interface {
	Authenticate(ctx context.Context, header http.Header) (Principal, error)
}

// AuthenticatorFunc adapts a plain function to Authenticator.
type AuthenticatorFunc func(ctx context.Context, header http.Header) (Principal, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, header http.Header) (Principal, error) {
	return f(ctx, header)
}

// WithAuthenticator installs the authentication seam: a is run before every
// sync RPC and its Principal is put on the handler's ctx, where the Permissions
// hooks receive it.
//
// With no authenticator (the default) nothing authenticates and every request
// reaches the hooks as the anonymous zero Principal — the embed-behind-your-own
// -auth posture the engine has always had. That is safe only when the sync
// surface is unreachable by untrusted traffic. Authenticating in your own HTTP
// middleware is equally supported; have it call WithPrincipal so the hooks see
// the identity.
//
// Installing an authenticator does not by itself authorize anything: it
// establishes WHO is calling, and the Permissions policy decides what they may
// do. A server exposed to untrusted traffic needs both — the default AllowAll
// allows every caller, authenticated or not.
func WithAuthenticator(a Authenticator) Option {
	return func(s *Server) error { s.authn = a; return nil }
}

// authInterceptor runs the Authenticator and puts its Principal on the ctx the
// handler sees. It wraps both unary and streaming handlers, so the long-lived
// Subscribe stream authenticates at open exactly like a unary call.
//
// Note what this does NOT do: a stream authenticated at open stays open on the
// credential it presented, and a credential that expires mid-stream is not
// re-checked. AUTHORIZATION is re-checked — Subscribe re-runs the workspace
// gate on every heartbeat and drops a revoked scope — so this covers the
// narrower case of a session that ends without membership changing.
// Deployments needing prompt credential expiry should bound stream lifetime.
type authInterceptor struct {
	authn Authenticator
}

func (i *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, err := i.authenticate(ctx, req.Header())
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingClient is a no-op: the engine only serves streams, never calls
// them, so there is no client side to authenticate.
func (i *authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx, err := i.authenticate(ctx, conn.RequestHeader())
		if err != nil {
			return err
		}
		return next(ctx, conn)
	}
}

// authenticate runs the authenticator and returns the ctx the handler should
// see. Rejections become unauthenticated; every other error is an
// infrastructure fault and stays internal (see the Authenticator contract).
// The client-visible message is fixed either way — the authenticator's own
// wording is left to the logging interceptor, which records the full error.
func (i *authInterceptor) authenticate(ctx context.Context, header http.Header) (context.Context, error) {
	p, err := i.authn.Authenticate(ctx, header)
	if err != nil {
		if errors.Is(err, ErrUnauthenticated) {
			return nil, connect.NewError(connect.CodeUnauthenticated,
				fmt.Errorf("authentication failed: %w", err))
		}
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("authenticating request: %w", err))
	}
	return WithPrincipal(ctx, p), nil
}

// StaticTokenAuthenticator admits a request iff it carries
// "Authorization: Bearer <token>" for one of the given tokens, and returns that
// token's Principal. It is the smallest useful Authenticator: fixed API keys
// for a handful of trusted callers (a backend service, a CLI, an integration
// test), with no identity provider in the loop.
//
// It is NOT a login system. Tokens are long-lived shared secrets with no
// expiry, no rotation and no revocation short of restarting with a different
// map, so it does not fit end-user traffic — that wants a real credential
// (a signed JWT verified against a JWKS, or a session cookie your middleware
// resolves) behind the same Authenticator interface.
//
// An empty map, an empty token, or a request with no bearer token all reject
// with ErrUnauthenticated, so a misconfigured deployment fails closed rather
// than admitting everyone as anonymous.
//
// The returned Principal is the caller's, verbatim; give each token a distinct
// Subject so policies and logs can tell callers apart. A Principal with an
// empty Subject would admit that token's holder as anonymous, which is almost
// never intended.
func StaticTokenAuthenticator(tokens map[string]Principal) Authenticator {
	// Copy so a later mutation of the caller's map can't silently change who is
	// admitted, and so the lookup below needs no lock.
	own := make(map[string]Principal, len(tokens))
	for t, p := range tokens {
		own[t] = p
	}
	return staticTokenAuthenticator{tokens: own}
}

type staticTokenAuthenticator struct{ tokens map[string]Principal }

func (a staticTokenAuthenticator) Authenticate(_ context.Context, header http.Header) (Principal, error) {
	const prefix = "Bearer "
	auth := header.Get("Authorization")
	if !strings.HasPrefix(auth, prefix) {
		return Principal{}, fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	}
	got := []byte(strings.TrimPrefix(auth, prefix))
	// Compare against every configured token in constant time rather than
	// indexing the map: a map lookup's timing varies with the presented value,
	// which leaks whether a guess shares a prefix/hash with a real token. The
	// loop is O(tokens), which is the point — this authenticator is for a
	// handful of static keys, not a user table.
	var match *Principal
	for t, p := range a.tokens {
		if t == "" {
			continue
		}
		if subtle.ConstantTimeCompare(got, []byte(t)) == 1 {
			match = &p
		}
	}
	if match == nil {
		return Principal{}, fmt.Errorf("%w: invalid bearer token", ErrUnauthenticated)
	}
	return *match, nil
}
