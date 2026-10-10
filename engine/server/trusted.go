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

// Trusted marks ctx as system work, exempting it from the permission hooks
// and from the access rules.
//
// A trusted call holds every permission, and EntityTx.Can reports true for
// it. Every write path still refuses an invalid grant row, trusted ones
// included: a row with an empty subject, an undeclared role, or no access
// parent on a grant entity that declares one.
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
//
// In a schema that uses the access model it also resolves the caller's access
// as enterWorkspace does, and refuses a caller that holds no permission in the
// workspace when the schema declares a role.
func (s *Server) gate(ctx context.Context, workspaceID string) (adm admission, enforced bool, err error) {
	return s.gateTo(ctx, workspaceID, false)
}

// gateServed is gate for a call that one transaction of s.store serves. As
// enterWorkspaceServed does, it can leave the grant check to that transaction
// and set admission.verdict, and the call then keeps to the same rules. The
// gate's error that verdict.settle returns is the one gate returns.
func (s *Server) gateServed(ctx context.Context, workspaceID string) (adm admission, enforced bool, err error) {
	return s.gateTo(ctx, workspaceID, s.grantsInServingTx(true))
}

// gateTo runs gate. leaveGrants leaves the grant check to the transaction that
// serves the call.
func (s *Server) gateTo(ctx context.Context, workspaceID string, leaveGrants bool) (adm admission, enforced bool, err error) {
	if isTrusted(ctx) {
		return admission{}, false, nil
	}
	adm.principal = principalOf(ctx)
	if err := s.catalogPtr().accessErr; err != nil {
		return adm, true, err
	}
	model := s.catalogPtr().access
	if err := s.perms.CanAccessWorkspace(ctx, adm.principal, workspaceID); err != nil {
		return adm, true, fmt.Errorf("server: workspace %q: %w", workspaceID, err)
	}
	if model == nil {
		if adm.creator, err = s.creatorOf(ctx, adm.principal, workspaceID); err != nil {
			return adm, true, err
		}
		return adm, true, nil
	}
	if adm.caller, err = s.resolveCaller(ctx, adm.principal, workspaceID, model); err != nil {
		return adm, true, err
	}
	adm.creator = adm.caller.primary
	wrap := func(err error) error { return fmt.Errorf("server: workspace %q: %w", workspaceID, err) }
	if leaveGrants && model.requiresPermission() {
		adm.verdict = &pendingVerdict{srv: s, ws: workspaceID, caller: adm.caller, wrap: wrap}
		return adm, true, nil
	}
	if adm.access, err = s.admitAccess(ctx, workspaceID, adm.caller); err != nil {
		return adm, true, wrap(err)
	}
	return adm, true, nil
}

// writeAuthorizer returns the per-write hook an EntityTx runs and the write gate
// its writes resolve protected fields and row checks through, both nil when the
// call is trusted. Shared by Mutate and MutateIdempotent, whose closures discover their
// writes inside the transaction and so authorize as they go.
//
// The two travel together because they describe one request's enforcement, but
// they are handed to EntityTx separately: Apply's apply pass carries the gate
// with no authorizer, having authorized its intents in an earlier pass.
func (s *Server) writeAuthorizer(
	ctx context.Context,
	adm admission,
	workspaceID string,
	enforced bool,
) (func(Write) error, *writeGate) {
	if !enforced {
		return nil, nil
	}
	g := s.newWriteGate(ctx, adm, workspaceID, enforced)
	return func(w Write) error {
		return s.authorizeWrite(ctx, adm.principal, workspaceID, g, w)
	}, g
}

// readGate resolves the read filters for an in-process read and returns the
// per-row visibility predicate the transaction applies. A nil predicate means
// everything live is visible, which is what a Trusted call gets.
//
// bind must run first inside the read transaction: it resolves the caller's
// access in the access model from that snapshot. It is never nil.
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
func (s *Server) readGate(
	ctx context.Context,
	workspaceID string,
	adm admission,
	enforced bool,
) (visible func(storage.Row) (bool, error), bind func(storage.ReadTx) error, err error) {
	if !enforced {
		return nil, func(storage.ReadTx) error { return nil }, nil
	}
	emitter, err := newRowEmitter(ctx, s, workspaceID, nil)
	if err != nil {
		return nil, nil, err
	}
	emitter.admit(adm)
	// In-process reads are a READ path: an app handler that scans a server-only
	// entity asked for it by name, and withholding it here would only make the
	// entity unreadable from the process that owns it. The read filter it
	// carries still applies.
	emitter.includeServerOnly = true
	for _, name := range s.catalogPtr().entityNames() {
		if _, err := emitter.appFilterOf(name); err != nil {
			return nil, nil, err
		}
	}
	return func(row storage.Row) (bool, error) {
		class, err := emitter.classify(row)
		if err != nil {
			return false, err
		}
		return class == rowVisible, nil
	}, emitter.bindAccess, nil
}
