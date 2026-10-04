package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
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
	if adm.access, err = s.admitAccess(ctx, workspaceID, adm.caller); err != nil {
		return adm, workspaceAccessError(err)
	}
	return adm, nil
}

// admission is what the workspace gate resolved for one request.
type admission struct {
	principal Principal
	// caller is the caller's identity in the access model, and access what
	// it held when the gate read its grants from the primary. Both are nil
	// for a schema that does not use the model. A read path binds the
	// caller's access again inside its own snapshot (rowEmitter.bindAccess);
	// access serves the checks that read no rows, such as a room's anchor.
	caller *accessCaller
	access *accessSet
}

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
