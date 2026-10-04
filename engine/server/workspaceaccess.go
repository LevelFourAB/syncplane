package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/storage"
)

// enterWorkspace is the first thing every workspace-scoped RPC does after
// validating that workspace_id is present: it resolves the caller's principal
// and runs the workspace-access gate, returning the principal for the handler
// to pass down to the remaining hooks.
//
// Placement is the whole point. It runs before the handler opens a read
// snapshot, subscribes to the notifier, resolves FilterEpoch or touches a
// client watermark row, so a refused caller performs no storage work and
// leaves no observable trace in a workspace they were denied — no watermark
// row, no notifier subscription, no epoch lookup. Those side effects are how a
// gate placed one step too late still leaks the existence of a workspace.
//
// A settled denial becomes permission_denied tagged CodeWorkspaceAccessRevoked;
// a failure to decide becomes internal, so a client never reads an outage as
// revocation. Errors already carrying a Connect code (a policy that built one
// itself) pass through untouched.
//
// A schema that breaks a rule of the access model fails every request here
// with failed_precondition, before the application's hook runs. In a schema
// that uses the model, the gate then resolves the caller's subjects and global
// roles. When the schema declares a role, it also reads the caller's grants
// from the primary and refuses a caller that holds no permission in the
// workspace, after the application's hook admitted it.
func (s *Server) enterWorkspace(ctx context.Context, workspaceID string) (admission, error) {
	return s.admitTo(ctx, workspaceID, false)
}

// enterWorkspaceServed is enterWorkspace for a request that one transaction
// serves. onPrimary reports that the transaction reads the primary in every
// configuration: a write transaction, or a read transaction of s.store. A read
// transaction that readTx or readTxAnchored routes does not.
//
// Where that transaction reads the primary and no hook of the application runs
// before it (see grantsInServingTx), the gate leaves its grant check to the
// transaction and sets admission.verdict. The handler then keeps to three
// rules. Its transaction reaches the verdict before it reads anything else
// (rowEmitter.bindAccess and txAccess.begin do). Work before that transaction
// that a refused caller must not cause calls verdict.reachApart first. Every
// outcome of the handler goes through verdict.settle or verdict.refusal, so a
// refused caller gets the gate's error whatever else failed first.
func (s *Server) enterWorkspaceServed(ctx context.Context, workspaceID string, onPrimary bool) (admission, error) {
	return s.admitTo(ctx, workspaceID, s.grantsInServingTx(onPrimary))
}

// admitTo runs the workspace gate. leaveGrants leaves the grant check to the
// transaction that serves the request (see enterWorkspaceServed).
func (s *Server) admitTo(ctx context.Context, workspaceID string, leaveGrants bool) (admission, error) {
	adm := admission{principal: principalOf(ctx)}
	model, err := s.catalogPtr().accessModelOrErr()
	if err != nil {
		return adm, err
	}
	if err := s.perms.CanAccessWorkspace(ctx, adm.principal, workspaceID); err != nil {
		return adm, workspaceAccessError(err)
	}
	if model == nil {
		return adm, nil
	}
	if adm.caller, err = s.resolveCaller(ctx, adm.principal, model); err != nil {
		return adm, workspaceAccessError(err)
	}
	if leaveGrants && model.requiresPermission() {
		adm.verdict = &pendingVerdict{srv: s, ws: workspaceID, caller: adm.caller, wrap: workspaceAccessError}
		return adm, nil
	}
	if adm.access, err = s.admitAccess(ctx, workspaceID, adm.caller); err != nil {
		return adm, workspaceAccessError(err)
	}
	return adm, nil
}

// grantsInServingTx reports whether the workspace gate may leave its grant
// check to the transaction that serves a request. onPrimary is as for
// enterWorkspaceServed.
func (s *Server) grantsInServingTx(onPrimary bool) bool {
	// Between the gate and its transaction a handler calls FilterEpoch,
	// ReadFilter and CanMutate. The default policy's hooks do nothing, so with
	// it the handler's work up to the transaction is the engine's own, which a
	// refused caller cannot observe. An application's hooks would run for a
	// caller the grants refuse.
	if _, ok := s.perms.(AllowAll); !ok {
		return false
	}
	// A replica that has not seen a new grant must not refuse a caller, so a
	// read that can run on a replica keeps the gate's own read of the primary.
	return onPrimary || s.replicaReads == nil
}

// admission is what the workspace gate resolved for one request.
type admission struct {
	principal Principal
	// caller is the caller's identity in the access model, and access what
	// it held when the gate read its grants from the primary. Both are nil
	// for a schema that does not use the model. A read path binds the
	// caller's access again inside its own snapshot (rowEmitter.bindAccess),
	// and a write path inside its write transaction (txAccess.begin); both
	// use access when their transaction reads the workspace at the position
	// the gate read it at. access also serves the checks that read no rows,
	// such as a room's anchor.
	caller *accessCaller
	access *accessSet
	// verdict is the grant check the gate left to the transaction that
	// serves the request, nil when the gate made it (see
	// enterWorkspaceServed). access is then nil.
	verdict *pendingVerdict
}

// pendingVerdict is the grant check the workspace gate leaves to the
// transaction that serves a request. Every copy of the request's admission
// shares it; it is not safe for concurrent use.
type pendingVerdict struct {
	srv    *Server
	ws     string
	caller *accessCaller
	// wrap maps an error of the gate onto what the entry point reports.
	wrap func(error) error
	// set is what the caller held where the verdict was reached, nil until
	// then. err is the gate's error from the last attempt that failed.
	set *accessSet
	err error
}

// reach reaches the verdict in tx unless it is reached already, and returns
// what the caller holds. It reads the grants from tx and refuses a caller that
// holds no permission, as admitAccess does from a transaction of its own; the
// error it returns is a gateError. The set records the position of tx, so a
// later transaction at that position uses it (see accessIn).
func (v *pendingVerdict) reach(tx storage.ReadTx) (*accessSet, error) {
	if v.set != nil {
		return v.set, nil
	}
	return v.record(admitted(accessIn(tx, v.caller, v.srv.limits, nil)))
}

// reachApart reaches the verdict, unless it is reached already, in a read
// transaction of its own on the primary, as enterWorkspace does. The error it
// returns is a gateError. A nil v does nothing.
func (v *pendingVerdict) reachApart(ctx context.Context) error {
	if v == nil || v.set != nil {
		return nil
	}
	_, err := v.record(v.srv.admitAccess(ctx, v.ws, v.caller))
	return err
}

func (v *pendingVerdict) record(set *accessSet, err error) (*accessSet, error) {
	if err != nil {
		v.err = v.wrap(err)
		return nil, &gateError{v.err}
	}
	v.set, v.err = set, nil
	return set, nil
}

// refusal returns the gate's error that a request ending with err, nil for a
// success, reports in place of err, or nil when the gate admits the caller.
// It is the error a gateError in the chain of err carries. Otherwise, when no
// attempt reached the verdict, refusal reaches it with reachApart, so a
// refused caller gets the gate's error whatever failed first; it does not when
// ctx is done, and err then stands. A nil v returns nil.
func (v *pendingVerdict) refusal(ctx context.Context, err error) error {
	if v == nil {
		return nil
	}
	var ge *gateError
	switch {
	case errors.As(err, &ge):
		return ge.err
	case v.set != nil:
		return nil
	case v.err != nil:
		return v.err
	case ctx.Err() != nil:
		return nil
	}
	if v.reachApart(ctx) != nil {
		return v.err
	}
	return nil
}

// settle returns what a request ending with err reports: the gate's error
// where refusal returns one, and err otherwise.
func (v *pendingVerdict) settle(ctx context.Context, err error) error {
	if gerr := v.refusal(ctx, err); gerr != nil {
		return gerr
	}
	return err
}

// gateError carries the workspace gate's error out of the transaction that
// reached the verdict. refusal takes it out of the chain again, so the handler
// reports the error as the gate built it.
type gateError struct{ err error }

func (e *gateError) Error() string { return e.err.Error() }
func (e *gateError) Unwrap() error { return e.err }

// workspaceAccessError maps a CanAccessWorkspace verdict onto the wire.
func workspaceAccessError(err error) error {
	if d, ok := AsWorkspaceDenial(err); ok {
		ce := connect.NewError(connect.CodePermissionDenied, d)
		ce.Meta().Set(ErrorCodeMetadataKey, CodeWorkspaceAccessRevoked)
		return ce
	}
	if connect.CodeOf(err) != connect.CodeUnknown {
		// The policy chose its own transport code; respect it.
		return err
	}
	return connect.NewError(connect.CodeInternal,
		fmt.Errorf("resolving workspace access: %w", err))
}
