/**
 * Machine-readable failure codes.
 *
 * Two families live here. The first is issued by the SERVER and mirrors
 * engine/server/codes.go — keep both in sync. The second is issued by the
 * CLIENT for failures the server never sees (a locally dropped mutation, a
 * store that could not be opened); those have no Go counterpart.
 *
 * Which code can arrive on which channel:
 *
 * | Failure                              | Channel                        | Code                       |
 * | ------------------------------------ | ------------------------------ | -------------------------- |
 * | Mutation refused by the app's rules  | `onMutationRejected`           | `permission_denied`, `invalid_argument`, `conflict`, or an app-defined code |
 * | Text PATCH's delta state did not fit | (handled internally, re-sent)  | `text_full_state_required` |
 * | Text write predates a compaction     | (handled internally, re-applied) | `text_state_stale`       |
 * | Mutation dropped after repeated      | `onMutationRejected`           | `poison_dropped`           |
 * | deterministic push failures          |                                |                            |
 * | Mutation discarded by a purge        | `onMutationRejected` + the     | `workspace_purged`         |
 * |                                      | `removeWorkspace` result       |                            |
 * | Mutation lost to a format upgrade    | `onMutationRejected` +         | `format_upgraded`          |
 * |                                      | `status().lastError`           |                            |
 * | Sync cycle failed                    | `status().lastError`           | `unavailable`, `internal`, or the server's code |
 * | Watch registration failed            | `status().lastError`           | `watch_failed`             |
 * | Text handle cannot merge remote state | `TextHandle.status()` +       | `text_merge_failed`        |
 * |                                      | `TextHandle.onError`           |                            |
 * | Access to a workspace withdrawn      | `status().workspaces[id].access` | (no code — `'revoked'`)  |
 * | Server/client schema revisions differ | `status().schemaSkew`         | (no code — see SchemaSkewInfo) |
 *
 * The two `status()` fields at the bottom are standing conditions rather than
 * failures: unlike `lastError` they survive a successful sync cycle, because
 * neither clears by retrying.
 */

/** The caller may not perform this mutation. */
export const CodePermissionDenied = 'permission_denied';
/** The mutation payload is semantically invalid for the application. */
export const CodeInvalidArgument = 'invalid_argument';
/** The mutation lost a business-level conflict (e.g. uniqueness). */
export const CodeConflict = 'conflict';

/**
 * A collaborative-text PATCH carried a delta-state fragment the server could
 * not apply, because the row is not built on the state the fragment was cut
 * against. Nothing is wrong with the edit — the coordinator re-sends it
 * carrying the full document, and pins the row to full state for the rest of
 * the session, so apps normally never see this code.
 */
export const CodeTextFullStateRequired = 'text_full_state_required';

/**
 * A collaborative-text write was built on state the server has compacted away:
 * applying it would put back characters the document dropped, or it hangs new
 * text off one of them. Resending the same bytes cannot help — unlike
 * `text_full_state_required`, which asks for more of the same state.
 *
 * The coordinator recovers: it captures the visible text of the affected
 * fields, re-bootstraps onto the server's current state, and re-applies that
 * text as a fresh edit. What the recovery cannot carry over is formatting the
 * stale side had applied on top of the dropped characters — those mark
 * operations are lost. A live `TextHandle` heals itself the moment it sees the
 * compacted state (it rebuilds instead of merging), so apps normally never see
 * this code; it surfaces only when the recovery could not read the text back.
 */
export const CodeTextStateStale = 'text_state_stale';

/**
 * The caller may not access the workspace, permanently — retrying will not
 * change the answer.
 *
 * Unlike the codes above it never rides on a mutation result, because it denies
 * a whole workspace rather than one write. Where it arrives depends on how many
 * workspaces the call named: as an RPC error on the single-workspace RPCs
 * (Pull, Bootstrap, Query), as `WorkspaceResults.errorCode` on Push, and as a
 * `WorkspaceDropped` frame with reason ACCESS_REVOKED on Subscribe — because
 * failing those requests outright would deny the client every other workspace
 * in the same call.
 *
 * A client seeing it stops syncing that workspace and surfaces
 * `status().workspaces[id].access === 'revoked'` rather than backing off
 * against a verdict that will never change. It does not delete the local copy
 * on its own — `removeWorkspace(id, { purge: true })` is how an app decides to.
 */
export const CodeWorkspaceAccessRevoked = 'workspace_access_revoked';

/**
 * A push batch's mutation ids were not contiguous with the server's watermark
 * for this client, so NOTHING in the batch applied, and that workspace's queue
 * stopped moving until the two line up again. The client recovers either way:
 * ids at or below the watermark are dropped by the next pull's rebase, and a
 * queue stranded above it is renumbered onto the watermark and resent.
 *
 * Also surfaces on `status().lastError` for the cycle that saw it — the queue
 * did not move, which is not something a client should report as a healthy
 * sync.
 *
 * Workspace-scoped, like the code above: it appears on `WorkspaceResults`, not
 * on any single mutation.
 */
export const CodeMutationIdGap = 'mutation_id_gap';

/**
 * A failure the caller cannot act on and that may not persist — a storage
 * error, or a permission hook that could not decide. Retry with backoff. It
 * never means "denied": a denial is always typed.
 */
export const CodeInternal = 'internal';

/**
 * Response-metadata key carrying one of the codes above on an RPC-level error.
 * Mirrors ErrorCodeMetadataKey in engine/server/codes.go.
 *
 * A Connect code alone is too coarse to act on — several conditions map to
 * `permission_denied` — so this is how a caller tells which denial it hit
 * without matching on message text:
 *
 * ```ts
 * if (err instanceof ConnectError && err.code === Code.PermissionDenied) {
 *   if (err.metadata.get(ErrorCodeMetadataKey) === CodeWorkspaceAccessRevoked) {
 *     // access is gone for good
 *   }
 * }
 * ```
 */
export const ErrorCodeMetadataKey = 'syncplane-error-code';

/*
 * Client-issued codes. These describe something the client did to a mutation or
 * to its own store, so they have no counterpart in engine/server/codes.go and
 * never travel over the wire.
 */

/**
 * The client itself dropped a mutation whose push the server kept failing the
 * same way (the poison-queue backstop in the coordinator's push), as opposed to
 * the server rejecting it. Lets apps tell a client-side dead-letter apart from
 * a normal "no". `errorMessage` carries the failure the server last reported.
 */
export const CodePoisonDropped = 'poison_dropped';

/**
 * The mutation was queued for a workspace that was purged
 * (`removeWorkspace(id, { purge: true })`) and can never be sent now. Reported
 * once per discarded mutation; the same mutations are also returned from
 * `removeWorkspace`, which is the channel to use when the app wants to keep the
 * unsent work rather than merely learn that it is gone.
 */
export const CodeWorkspacePurged = 'workspace_purged';

/**
 * The mutation was queued in a store written by a different local layout
 * version and could not be salvaged during the upgrade, so it was dropped. Also
 * summarised (as a count) on `status().lastError`.
 */
export const CodeFormatUpgraded = 'format_upgraded';

/** A watch could not be registered because the local store failed to open. */
export const CodeWatchFailed = 'watch_failed';

/**
 * A live `TextHandle` could not reconcile its document with state that arrived
 * on the row watch — the bytes did not decode, the merge found the same id
 * carrying different content or placement (two writers shared a peer), or the
 * incoming state was not an ancestor of the merged result.
 *
 * Terminal for that one handle: it stops merging and refuses edits, because a
 * document that cannot take in remote state will never converge again and the
 * text on screen is stale from then on. Nothing else is affected — the row, the
 * sync loop and every other handle carry on — and the app recovers by closing
 * the handle and opening a new one, which rebuilds from the stored row. Arrives
 * on `TextHandle.status()` and `TextHandle.onError`.
 */
export const CodeTextMergeFailed = 'text_merge_failed';

/**
 * The server could not be reached, or did not answer in time. Transient by
 * definition — the sync loop retries with backoff. `SyncErrorInfo.transportCode`
 * carries the exact Connect code when a finer distinction is needed.
 */
export const CodeUnavailable = 'unavailable';

/**
 * Codes that can ride on a rejected mutation. Apps may define their own on the
 * server (a mutator returns whatever code it likes), which is why the fields
 * carrying this widen it with `(string & {})` — unknown codes still typecheck,
 * known ones autocomplete. Handle it with a `switch` and a `default`.
 */
export type RejectionCode =
	| typeof CodePermissionDenied
	| typeof CodeInvalidArgument
	| typeof CodeConflict
	| typeof CodePoisonDropped
	| typeof CodeWorkspacePurged
	| typeof CodeFormatUpgraded;

/**
 * Codes that can appear on `status().lastError`. A failed sync cycle reports
 * the server's own code when the response carried one (in
 * ErrorCodeMetadataKey), and otherwise classifies the transport failure as
 * `unavailable` or `internal`.
 */
export type SyncErrorCode =
	| typeof CodePermissionDenied
	| typeof CodeInvalidArgument
	| typeof CodeWorkspaceAccessRevoked
	| typeof CodeMutationIdGap
	| typeof CodeInternal
	| typeof CodeUnavailable
	| typeof CodeFormatUpgraded
	| typeof CodeWatchFailed;
