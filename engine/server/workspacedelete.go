package server

import (
	"context"
	"fmt"

	"github.com/levelfourab/syncplane/engine/storage"
)

// ErrWorkspaceDeleted reports that DeleteWorkspace deleted the workspace and
// RestoreWorkspace has not restored it since. Test for it with errors.Is.
//
// The workspace gate refuses every request to a deleted workspace with a
// WorkspaceDenial that wraps it, so AsWorkspaceDenial also matches, and a sync
// client receives the permanent denial (CodeWorkspaceAccessRevoked). A Trusted
// call skips the gate: its write fails with an error that wraps
// ErrWorkspaceDeleted but is not a WorkspaceDenial, and its read sees an empty
// workspace.
var ErrWorkspaceDeleted = storage.ErrWorkspaceDeleted

// errWorkspaceDeletedDenial is the gate's verdict on a deleted workspace.
var errWorkspaceDeletedDenial error = &WorkspaceDenial{
	Message: "the workspace was deleted",
	cause:   ErrWorkspaceDeleted,
}

// DeleteWorkspace removes every piece of state the workspace holds: rows and
// tombstones, client watermarks and rejection verdicts, the collaborative-text
// delta log and compaction floors, index entries, idempotency records, effect
// consumer positions and shadows, and the workspace's sequence and store epoch.
// It does this in one storage transaction, after any write in flight commits.
// With WithBlobStore it then deletes the workspace's blobs and staged
// uploads. When that fails, it returns an error and the next blob GC pass of
// WithGC deletes them.
//
// The id then stays refused until RestoreWorkspace:
//
//   - A sync client's next request gets the permanent workspace denial
//     (permission_denied with CodeWorkspaceAccessRevoked), and the client
//     stops syncing the workspace. An open Subscribe stream drops the
//     workspace with a reset, and the client's next bootstrap gets the denial.
//   - Apply, Mutate, Invoke and View return an error that matches
//     ErrWorkspaceDeleted. See ErrWorkspaceDeleted for Trusted calls.
//   - WorkspaceIDs does not list it, and no background pass visits it.
//
// Deleting a workspace that holds no state still refuses its id. Deleting a
// deleted workspace changes nothing. Neither is an error.
//
// It runs no permission hook: expose it only to callers the application has
// authorized itself. It must not be called from inside a mutator or another
// write transaction, and returns an error when ctx shows that it is.
//
// State kept outside the storage adapter stays: a cursor-state store set with
// WithCursorStateStore keeps its entries until it evicts them, and presence
// rooms keep their ephemeral sessions until the sessions end.
func (s *Server) DeleteWorkspace(ctx context.Context, workspaceID string) error {
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: DeleteWorkspace called re-entrantly inside a write transaction (workspace %q): it opens a write transaction that would deadlock against the running one", workspaceID)
	}
	if err := s.store.DeleteWorkspace(ctx, workspaceID); err != nil {
		return fmt.Errorf("server: delete workspace %q: %w", workspaceID, err)
	}
	// Streams on the workspace drain on a poke, find the workspace reset under
	// their cursor, and send the client to bootstrap, which the gate refuses.
	// Without it they would wait for the heartbeat re-check.
	s.notifier.Notify(workspaceID)
	// After the rows: the deletion mark is what lets blob GC finish the job
	// when this fails, and an upload admitted before the mark can still commit
	// a blob after this call.
	if s.blobs != nil {
		if err := s.blobs.DeleteWorkspace(ctx, s.blobKey(workspaceID)); err != nil {
			return fmt.Errorf("server: delete blobs of workspace %q: %w", workspaceID, err)
		}
	}
	return nil
}

// RestoreWorkspace lifts the refusal DeleteWorkspace put on the id. It
// restores no data: the next write starts a new, empty workspace under the id,
// with a new store epoch, so a client that still holds a cursor of the deleted
// workspace re-bootstraps instead of resuming. Restoring a workspace that is
// not deleted is not an error.
//
// It runs no permission hook, and must not be called from inside a mutator or
// another write transaction, exactly as DeleteWorkspace.
func (s *Server) RestoreWorkspace(ctx context.Context, workspaceID string) error {
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: RestoreWorkspace called re-entrantly inside a write transaction (workspace %q): it opens a write transaction that would deadlock against the running one", workspaceID)
	}
	if err := s.store.RestoreWorkspace(ctx, workspaceID); err != nil {
		return fmt.Errorf("server: restore workspace %q: %w", workspaceID, err)
	}
	return nil
}

// refuseDeleted returns errWorkspaceDeletedDenial when the workspace is
// deleted, and an error wrapped as a failure to decide when the adapter cannot
// say.
//
// The gate asks this on every request rather than leaving it to the
// transaction that serves it. A read transaction sees a deleted workspace as an
// empty one, so a pull would answer with a reset instead of the denial, and a
// Query with no rows. Only writes are refused inside the transaction, which is
// what closes the race between this check and a deletion that lands after it.
func (s *Server) refuseDeleted(ctx context.Context, workspaceID string) error {
	deleted, err := s.store.WorkspaceDeleted(ctx, workspaceID)
	if err != nil {
		return fmt.Errorf("server: reading whether workspace %q is deleted: %w", workspaceID, err)
	}
	if deleted {
		return errWorkspaceDeletedDenial
	}
	return nil
}
