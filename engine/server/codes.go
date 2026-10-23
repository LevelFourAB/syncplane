package server

// Machine-readable rejection codes shared between server and client.
// The TS client mirrors these in client/src/codes.ts; keep both in sync.
// Applications may define their own codes, but should prefer these for the
// common cases so generic client UI can react uniformly.
const (
	// CodePermissionDenied rejects a mutation the caller may not perform.
	CodePermissionDenied = "permission_denied"
	// CodeAccessDenied rejects a mutation the schema's access model refuses:
	// no access rule of the entity admits the write for the caller, the caller
	// grants a role it does not hold, a move needs a read permission the caller
	// lacks, a mutator's MutatorAccess does not hold, or the entity has no
	// rule for the operation. The application's own permission hooks refuse
	// with CodePermissionDenied, except the default row check: on an entity of
	// the access model, a write to a row ReadFilter hides gets CodeAccessDenied,
	// as a write to a missing row does. The message names only what the caller
	// sent; the server logs why at debug level, as "access refused".
	CodeAccessDenied = "access_denied"
	// CodeInvalidArgument rejects a mutation whose payload is semantically
	// invalid for the application.
	CodeInvalidArgument = "invalid_argument"
	// CodeConflict rejects a mutation that lost a business-level conflict
	// (e.g. uniqueness, state machine).
	CodeConflict = "conflict"
	// CodeTextFullStateRequired rejects a mutation whose collaborative-text
	// PATCH carried a delta-state fragment the server could not apply — the
	// fragment was cut against a state this row is not (or no longer) built on.
	// Nothing is wrong with the write itself: the client re-sends the same edit
	// carrying the full document and it lands. Like every rejection it consumes
	// no sequence and rolls the savepoint back, so the row is untouched.
	CodeTextFullStateRequired = "text_full_state_required"
	// CodeTextStateStale rejects a mutation whose collaborative-text value is
	// built on state a compaction has dropped (see engine/text.Compact): it
	// would put back characters the document purged, or hangs new text off one
	// of them. Resending the same bytes cannot help — unlike
	// CodeTextFullStateRequired, which asks for MORE of the same state. The
	// client has to re-read the row and re-apply its edit against it, which
	// costs the marks the stale side had minted. Consumes no sequence.
	CodeTextStateStale = "text_state_stale"
	// CodeWorkspaceAccessRevoked reports that the caller may not access the
	// workspace and that this is PERMANENT — the answer will not change by
	// retrying. Unlike the codes above it never rides on a MutationResult: it
	// denies a whole workspace (see DenyWorkspace), so it arrives as an RPC
	// error on the single-workspace RPCs, as a WorkspaceResults.error_code on
	// Push, and as a WorkspaceDropped frame on Subscribe. A client seeing it
	// should stop syncing that workspace and discard its local copy rather than
	// back off and retry.
	CodeWorkspaceAccessRevoked = "workspace_access_revoked"
	// CodeWorkspaceNoAccess reports that the caller holds no permission in the
	// workspace's access model. Unlike CodeWorkspaceAccessRevoked it is not
	// permanent: a grant row written later admits the caller. It arrives where
	// CodeWorkspaceAccessRevoked does, and as a WorkspaceDropped frame with
	// reason NO_ACCESS on Subscribe. A client seeing it keeps its local copy and
	// its queue, and retries with backoff. A Pull from a cursor issued while
	// the caller held a permission gets it only after a page that tombstones
	// the rows the caller read and sets has_more.
	CodeWorkspaceNoAccess = "workspace_no_access"
)

// Workspace-scoped failure codes. These appear on WorkspaceResults.error_code,
// where a failure belongs to ONE workspace of a multi-workspace request and
// must not be reported as a failure of the request. CodeWorkspaceAccessRevoked
// above is used the same way.
const (
	// CodeMutationIDGap reports that a batch's mutation ids were not contiguous
	// with the server's watermark for that client, so NOTHING in the batch was
	// applied. The client resends in order from its watermark. Deterministic:
	// retrying the identical batch fails identically.
	CodeMutationIDGap = "mutation_id_gap"
	// CodeInternal reports a failure the caller cannot act on and that may not
	// persist — a storage error, or a permission hook that could not decide.
	// Retry with backoff. It never means "denied": a denial is always typed.
	CodeInternal = "internal"
)

// ErrorCodeMetadataKey is the response-metadata key carrying a machine-readable
// code (one of the constants above) on an RPC-level error. Connect exposes it
// to clients as error metadata; the TS client reads it off ConnectError.
//
// RPC errors carry a Connect code, which is coarse — several conditions map to
// permission_denied — so this key is what lets a client branch on WHICH denial
// it hit without matching on message text.
const ErrorCodeMetadataKey = "syncplane-error-code"
