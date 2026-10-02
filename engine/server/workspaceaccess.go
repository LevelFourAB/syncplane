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
// with failed_precondition, before the application's hook runs.
func (s *Server) enterWorkspace(ctx context.Context, workspaceID string) (Principal, error) {
	p := principalOf(ctx)
	if _, err := s.catalogPtr().accessModelOrErr(); err != nil {
		return p, err
	}
	if err := s.perms.CanAccessWorkspace(ctx, p, workspaceID); err != nil {
		return p, workspaceAccessError(err)
	}
	return p, nil
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
