package server

import (
	"context"
	"fmt"

	"github.com/levelfourab/syncplane/engine/blob"
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
// WithGC deletes them. A RestoreWorkspace that lands before the blobs are gone
// stops the deletion: the blobs the restored workspace uploads stay, and blob
// GC treats the blobs left from before as unreferenced.
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
		if err := s.deleteWorkspaceBlobs(ctx, workspaceID); err != nil {
			return fmt.Errorf("server: delete blobs of workspace %q: %w", workspaceID, err)
		}
	}
	return nil
}

// deleteWorkspaceBlobs removes every committed blob and staged upload of a
// workspace that DeleteWorkspace marked. It stops, without an error, when the
// workspace is no longer marked.
//
// RestoreWorkspace can land while the walk runs, and the blobs uploaded after
// it belong to the restored workspace. So each step deletes inside the
// workspace's write exclusion, which RestoreWorkspace also takes, and reads the
// mark there first. A step holds the exclusion for one batch, not for the whole
// walk: on an adapter with one writer for all workspaces, a long walk would
// stop every write for its duration.
func (s *Server) deleteWorkspaceBlobs(ctx context.Context, workspaceID string) error {
	key := s.blobKey(workspaceID)
	var digests []blob.Digest
	for d, err := range s.blobs.List(ctx, key) {
		if err != nil {
			return err
		}
		digests = append(digests, d)
	}
	var staged []blob.Digest
	for up, err := range s.blobs.ListStaged(ctx, key) {
		if err != nil {
			return err
		}
		staged = append(staged, up.Manifest.Digest)
	}
	steps := []struct {
		digests []blob.Digest
		remove  func(context.Context, string, blob.Digest) error
	}{
		{digests, s.blobs.Delete},
		{staged, s.blobs.Abort},
	}
	for _, step := range steps {
		for start := 0; start < len(step.digests); start += blobGCBatch {
			batch := step.digests[start:min(start+blobGCBatch, len(step.digests))]
			deleted, err := s.whileDeleted(ctx, workspaceID, func() error {
				for _, d := range batch {
					if err := step.remove(ctx, key, d); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil || !deleted {
				return err
			}
		}
	}
	// The walk left little or nothing: an upload admitted before the mark can
	// have committed since the listing. This also removes what the blob store
	// keeps for the workspace itself.
	_, err := s.whileDeleted(ctx, workspaceID, func() error {
		return s.blobs.DeleteWorkspace(ctx, key)
	})
	return err
}

// whileDeleted runs fn inside the workspace's write exclusion when the
// workspace is still marked deleted there, and reports whether it ran fn.
func (s *Server) whileDeleted(ctx context.Context, workspaceID string, fn func() error) (bool, error) {
	ran := false
	err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		ran = false
		// A statement of the transaction comes first: an adapter can take the
		// exclusion with it (pgstore does), and the mark must be read after.
		if _, err := tx.CurrentSeq(); err != nil {
			return err
		}
		// The mark is read outside the transaction, from the committed state,
		// which RestoreWorkspace cannot change while the exclusion is held.
		deleted, err := s.store.WorkspaceDeleted(ctx, workspaceID)
		if err != nil || !deleted {
			return err
		}
		ran = true
		return fn()
	})
	return ran, err
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
