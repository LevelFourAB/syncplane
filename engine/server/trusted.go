package server

import (
	"context"
	"fmt"

	"github.com/levelfourab/syncplane/engine/storage"
)

// trustedKey is the unexported ctx key marking system work. It is a distinct
// type so nothing outside this package can collide with or forge it — the
// bypass must be reachable only through Trusted.
type trustedKey struct{}

// Trusted marks ctx as system work, exempting it from the permission hooks.
//
// The in-process write and read APIs — Apply, Mutate, Invoke, View — authorize
// against the ctx principal by default, so an app handler that forgot to
// authenticate is denied rather than silently admitted. Machinery that has no
// caller to authorize — a connector reconciling an external source, a
// migration, a cron job, the admin UI on its private listener — has no
// principal to offer and would be denied by any real policy. It says so here:
//
//	srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error { … })
//
// The engine short-circuits on this marker and never consults Permissions at
// all, rather than synthesizing a system Principal for policies to recognize. A
// policy that has to know about an engine-internal identity is a policy that
// can get it wrong, and the failure would be silent; this way the bypass is one
// greppable call at the site that takes it.
//
// Forgetting it fails SAFE: unmarked system code authorizes as the anonymous
// principal, which a deny-by-default policy refuses, so the connector's first
// reconcile pass errors loudly instead of writing unauthorized. That is the
// whole reason the bypass is opt-in rather than the default.
//
// It marks the ctx, not the server, so it does not leak: a ctx derived from a
// request keeps the request's principal for attribution while exempting this
// one call.
func Trusted(ctx context.Context) context.Context {
	return context.WithValue(ctx, trustedKey{}, true)
}

// isTrusted reports whether ctx was marked by Trusted. It is what every
// in-process entry point checks before resolving a principal or running a hook.
func isTrusted(ctx context.Context) bool {
	t, _ := ctx.Value(trustedKey{}).(bool)
	return t
}

// gate is the entry check every in-process API runs: it decides whether this
// call is authorized at all and, when it is, admits the caller to the
// workspace. It returns the principal that subsequent write and read hooks
// authorize against, and whether enforcement applies.
//
// It runs before any transaction opens, so a denied caller performs no storage
// work — the same placement, and for the same reason, as enterWorkspace on the
// RPC path (see workspaceaccess.go).
//
// Unlike that path it returns the policy's error unwrapped rather than mapping
// it onto a Connect code: an in-process caller recovers a settled denial with
// AsWorkspaceDenial and sees a plain error for a hook that could not decide,
// instead of having to unpick a transport status it never asked for.
func (s *Server) gate(ctx context.Context, workspaceID string) (p Principal, enforced bool, err error) {
	if isTrusted(ctx) {
		return Principal{}, false, nil
	}
	p = principalOf(ctx)
	if err := s.perms.CanAccessWorkspace(ctx, p, workspaceID); err != nil {
		return p, true, fmt.Errorf("server: workspace %q: %w", workspaceID, err)
	}
	return p, true, nil
}

// writeAuthorizer returns the per-write hook an EntityTx runs, or nil when the
// call is trusted. Shared by Mutate and MutateIdempotent, whose closures
// discover their writes inside the transaction and so authorize as they go.
func (s *Server) writeAuthorizer(ctx context.Context, p Principal, workspaceID string, enforced bool) func(Write) error {
	if !enforced {
		return nil
	}
	return func(w Write) error {
		return s.perms.CanMutate(ctx, p, workspaceID, w)
	}
}

// readGate resolves the read filters for an in-process read and returns the
// per-row visibility predicate the transaction applies. A nil predicate means
// everything live is visible, which is what a Trusted call gets.
//
// It reuses the same rowEmitter that pull, bootstrap and the Query RPC classify
// rows through, so "visible" means one thing across every read path rather than
// being reimplemented here.
//
// Every registered entity's filter is warmed BEFORE the snapshot opens. The
// Permissions contract requires ReadFilter to be resolved outside the read
// transaction so a slow hook cannot pin the snapshot, and unlike Query — which
// reads exactly one entity and warms exactly one filter — an in-process closure
// may touch any entity, so there is nothing narrower to warm. That costs one
// ReadFilter call per registered entity per View/Apply; the hook is contractually
// a cached lookup, which is what keeps it cheap.
func (s *Server) readGate(ctx context.Context, workspaceID string, enforced bool) (func(storage.Row) (bool, error), error) {
	if !enforced {
		return nil, nil
	}
	emitter, err := newRowEmitter(ctx, s, workspaceID, nil)
	if err != nil {
		return nil, err
	}
	for _, name := range s.catalogPtr().entityNames() {
		if _, err := emitter.filterOf(name); err != nil {
			return nil, err
		}
	}
	return func(row storage.Row) (bool, error) {
		class, err := emitter.classify(row)
		if err != nil {
			return false, err
		}
		return class == rowVisible, nil
	}, nil
}
