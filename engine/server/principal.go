package server

import "context"

// Principal is the verified identity of an RPC's caller — the engine's shared
// vocabulary for "who is calling". An Authenticator produces one per request
// (see authn.go), the engine stashes it on the request ctx, and every
// Permissions hook receives it explicitly so a policy can never accidentally
// authorize as nobody.
//
// It is deliberately small and transport-agnostic: the engine never interprets
// a Principal beyond checking Subject for emptiness, so apps are free to carry
// whatever their policies authorize on. It is passed by value and must be
// treated as immutable — hooks run concurrently across in-flight RPCs and must
// not write to Claims.
//
// A zero Principal is the anonymous caller: no authenticator is installed, or
// one deliberately admitted an unauthenticated request. Policies decide what
// anonymous may do; the engine's own default (AllowAll) allows everything, so
// a server exposed to untrusted traffic must install both an Authenticator and
// a Permissions policy that rejects the zero Principal.
type Principal struct {
	// Subject is the stable, issuer-scoped identity of the caller (a user id,
	// a service account name). It is the field policies key on. Empty exactly
	// when the caller is anonymous — an Authenticator that returns a Principal
	// without a Subject is admitting an unauthenticated request.
	Subject string
	// Issuer names the authority that vouched for Subject: an OIDC issuer URL,
	// an API-key realm, whatever the Authenticator uses to scope its subjects.
	// Two principals from different issuers may share a Subject and are not the
	// same caller, so a policy spanning issuers must compare both.
	Issuer string
	// Claims carries the verified attributes a policy may authorize on (roles,
	// tenant, email, group membership). Values are whatever the Authenticator
	// decoded — for JWT-shaped credentials, typically the JSON types
	// (string, float64, bool, []any, map[string]any).
	//
	// Only put VERIFIED attributes here. The engine treats every claim as
	// trustworthy because an Authenticator vouched for it; copying unverified
	// request headers in makes them authorization inputs.
	//
	// Nil for principals that carry no attributes. Never written after the
	// Authenticator returns.
	Claims map[string]any
}

// IsAnonymous reports whether the caller was not authenticated. It is the
// check a deny-by-default policy leads with.
func (p Principal) IsAnonymous() bool { return p.Subject == "" }

// Claim returns the named claim and whether it was present. Safe on a nil
// Claims map.
func (p Principal) Claim(name string) (any, bool) {
	v, ok := p.Claims[name]
	return v, ok
}

// StringClaim returns the named claim when it is present AND a string. A claim
// of another type reports false rather than a coerced value, so a policy
// comparing against it cannot be fooled by a credential that typed the claim
// unexpectedly.
func (p Principal) StringClaim(name string) (string, bool) {
	s, ok := p.Claims[name].(string)
	return s, ok
}

// principalKey is the unexported ctx key for the request principal. It is a
// distinct type so nothing outside this package can collide with or forge it.
type principalKey struct{}

// WithPrincipal returns a ctx carrying p as the caller's verified identity.
//
// The engine's authentication interceptor calls this for you (see
// WithAuthenticator). Call it directly when authentication happens in your own
// HTTP middleware instead — that is the supported hand-off, and the only one:
// the permission hooks read the principal the engine finds here, so a
// middleware that stashes an identity under its own ctx key is invisible to
// them.
//
// It is also how a server-side caller says who it is acting as. The in-process
// API — Apply, Mutate, Invoke, View — authorizes against the principal it finds
// here, so a background job or an API handler that authenticated for itself
// puts the resulting identity on the ctx and is then held to the same policy a
// sync client is. A ctx with no principal authorizes as anonymous.
//
// A ctx principal reaches only what runs SYNCHRONOUSLY within the writing ctx:
// the permission hooks, mutator bodies, post-commit work the writer itself
// performs. It does not reach the change feed — a feed consumer drains on its
// own ctx, on its own schedule, so it never sees the writing ctx at all. What
// crosses that boundary is the actor recorded on the row (see storage.Actor and
// Change.Actor), which is derived from this principal at write time.
//
// Work with no caller to authorize marks its ctx with Trusted instead, which
// skips the hooks; the two compose, so a ctx can carry a principal for
// attribution while a single call opts out of enforcement.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the ctx's verified principal and whether one was set.
//
// Permission hooks do not need this — they are handed the principal as an
// argument precisely so they cannot forget it. Use it in code that only has a
// ctx: mutators, post-commit hooks, change-feed consumers.
//
// The false case means no authentication ran for this ctx, which is NOT the
// same as an authenticated-but-anonymous caller (a Principal an Authenticator
// deliberately returned with no Subject). Code that only cares whether it has
// an identity should ignore the bool and call IsAnonymous on the result, which
// is correct in both cases.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// principalOf returns the ctx's principal, or the zero (anonymous) Principal
// when none was set. It is what the engine's hook call sites use: an RPC that
// ran without an authenticator authorizes as anonymous rather than as a
// special "unknown" case every policy would have to handle separately.
func principalOf(ctx context.Context) Principal {
	p, _ := PrincipalFrom(ctx)
	return p
}
