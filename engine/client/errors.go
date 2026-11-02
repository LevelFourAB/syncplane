package client

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

// Machine-readable codes the server reports on a rejected mutation, on a
// failed workspace batch, and in the "syncplane-error-code" metadata of an
// RPC error. Applications may define more codes of their own.
const (
	CodePermissionDenied       = "permission_denied"
	CodeAccessDenied           = "access_denied"
	CodeInvalidArgument        = "invalid_argument"
	CodeConflict               = "conflict"
	CodeTextFullStateRequired  = "text_full_state_required"
	CodeTextStateStale         = "text_state_stale"
	CodeWorkspaceAccessRevoked = "workspace_access_revoked"
	CodeWorkspaceNoAccess      = "workspace_no_access"
	CodeMutationIDGap          = "mutation_id_gap"
	CodeInternal               = "internal"
	CodeMutationFailed         = "mutation_failed"
)

// errorCodeMetadataKey is the response-metadata key of the code on an RPC
// error.
const errorCodeMetadataKey = "syncplane-error-code"

// RejectedError reports a mutation the server rejected. A rejection is final:
// the server wrote nothing for the mutation and will give the same answer to
// every resend.
type RejectedError struct {
	Workspace  string
	MutationID uint64
	// Code is one of the Code constants or an application-defined code.
	Code string
	// Message is human-readable detail for logs. Branch on Code instead.
	Message string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("client: workspace %q: mutation %d rejected: %s: %s", e.Workspace, e.MutationID, e.Code, e.Message)
}

// WorkspaceError reports a push the server refused as a whole for one
// workspace, so nothing of it applied. Code is CodeWorkspaceAccessRevoked,
// CodeWorkspaceNoAccess, CodeMutationFailed, CodeInvalidArgument or another
// workspace-scoped code. A Subscribe stream that drops a workspace for lost
// access reports the same type.
type WorkspaceError struct {
	Workspace string
	Code      string
	Message   string
}

func (e *WorkspaceError) Error() string {
	return fmt.Sprintf("client: workspace %q: %s: %s", e.Workspace, e.Code, e.Message)
}

// ErrUnconfirmed is wrapped by a write error when the client could not learn
// whether the server applied the mutation: the context ended, every attempt
// failed in transit or with a transient server failure, or the server refused
// a resend of a mutation that an earlier attempt may have applied. The error
// also wraps the last failure, which can be a *WorkspaceError or a
// *connect.Error, so test for ErrUnconfirmed before those. The mutation stays
// queued in the Client, which sends it again, under the same mutation id,
// before the next write to the workspace. Workspace.Confirm sends it again at
// once and reports its outcome.
var ErrUnconfirmed = errors.New("client: mutation outcome unknown")

// ErrClientIDInUse is returned by a write when the server reports a new
// mutation as already applied. Another writer advanced the watermark of this
// client id, so two live Clients share one id. The mutation was not applied.
var ErrClientIDInUse = errors.New("client: client id in use by another writer")

// ErrReset is returned by Pull, and reported by Subscribe, when a cursor can
// no longer continue: it predates the collected tombstones, or its scope
// changed in a way the server cannot resynchronize. Bootstrap again to get a
// new cursor.
var ErrReset = errors.New("client: cursor reset; bootstrap again")

// CodeOf returns the machine-readable code err carries: the Code of a
// *RejectedError or *WorkspaceError, or the "syncplane-error-code" metadata of
// a *connect.Error. It returns "" when err carries none.
func CodeOf(err error) string {
	var rej *RejectedError
	if errors.As(err, &rej) {
		return rej.Code
	}
	var ws *WorkspaceError
	if errors.As(err, &ws) {
		return ws.Code
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Meta().Get(errorCodeMetadataKey)
	}
	return ""
}
