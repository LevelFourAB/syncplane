package blobbridge

import (
	"context"
	"errors"
	"io"
	"iter"
	"time"

	"github.com/levelfourab/syncplane/engine/blob"
)

var _ blob.Store = (*Bridge)(nil)

// Begin stages an upload in the wrapped store.
func (b *Bridge) Begin(ctx context.Context, workspaceID string, m blob.Manifest) error {
	return b.inner.Begin(ctx, workspaceID, m)
}

// Staged reads a staged upload from the wrapped store.
func (b *Bridge) Staged(ctx context.Context, workspaceID string, digest blob.Digest) (blob.Upload, []int, error) {
	return b.inner.Staged(ctx, workspaceID, digest)
}

// PutChunk stores a chunk in the wrapped store.
func (b *Bridge) PutChunk(ctx context.Context, workspaceID string, digest blob.Digest, index int, r io.Reader) error {
	return b.inner.PutChunk(ctx, workspaceID, digest, index, r)
}

// Complete commits a staged upload in the wrapped store. While Run runs, a
// commit queues the blob for upload to the origin.
func (b *Bridge) Complete(ctx context.Context, workspaceID string, digest blob.Digest) error {
	err := b.inner.Complete(ctx, workspaceID, digest)
	if err == nil {
		if st := b.running(ctx); st != nil {
			st.sched.check(workspaceID, []blob.Digest{digest})
		}
	}
	return err
}

// Abort removes a staged upload from the wrapped store.
func (b *Bridge) Abort(ctx context.Context, workspaceID string, digest blob.Digest) error {
	return b.inner.Abort(ctx, workspaceID, digest)
}

// ListStaged lists the staged uploads of the wrapped store.
func (b *Bridge) ListStaged(ctx context.Context, workspaceID string) iter.Seq2[blob.Upload, error] {
	return b.inner.ListStaged(ctx, workspaceID)
}

// Stat returns a committed blob's Info from the wrapped store, or
// blob.ErrNotFound.
//
// While Run runs, a blob the wrapped store does not hold starts a fetch from
// the origin. Stat then waits up to the read wait (see WithReadWait) for the
// fetch to commit the blob, and after that up to StageWait, or until ctx
// ends, for the fetch to stage it. A staged blob reads as being uploaded
// (Staged reports it), and the local engine notifies the workspace when it
// commits.
func (b *Bridge) Stat(ctx context.Context, workspaceID string, digest blob.Digest) (blob.Info, error) {
	info, err := b.inner.Stat(ctx, workspaceID, digest)
	if !errors.Is(err, blob.ErrNotFound) {
		return info, err
	}
	st := b.running(ctx)
	if st == nil {
		return info, err
	}
	settled, staged, ok := st.sched.want(workspaceID, digest, b.cfg.patience)
	if !ok {
		return info, err
	}
	if b.cfg.readWait > 0 && wait(ctx, settled, b.cfg.readWait) {
		return b.inner.Stat(ctx, workspaceID, digest)
	}
	wait(ctx, staged, StageWait)
	return b.inner.Stat(ctx, workspaceID, digest)
}

// wait waits up to d for ch to close, and reports whether it closed.
func wait(ctx context.Context, ch <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
	case <-ctx.Done():
	}
	return false
}

// Open opens a committed blob of the wrapped store.
func (b *Bridge) Open(ctx context.Context, workspaceID string, digest blob.Digest) (io.ReadSeekCloser, error) {
	return b.inner.Open(ctx, workspaceID, digest)
}

// SetUnreferencedSince sets the mark of a committed blob in the wrapped store.
func (b *Bridge) SetUnreferencedSince(ctx context.Context, workspaceID string, digest blob.Digest, t time.Time) error {
	return b.inner.SetUnreferencedSince(ctx, workspaceID, digest, t)
}

// Delete removes a committed blob from the wrapped store.
func (b *Bridge) Delete(ctx context.Context, workspaceID string, digest blob.Digest) error {
	return b.inner.Delete(ctx, workspaceID, digest)
}

// List lists the committed blobs of the wrapped store.
func (b *Bridge) List(ctx context.Context, workspaceID string) iter.Seq2[blob.Digest, error] {
	return b.inner.List(ctx, workspaceID)
}

// ListWorkspaces lists the workspaces of the wrapped store.
func (b *Bridge) ListWorkspaces(ctx context.Context) iter.Seq2[string, error] {
	return b.inner.ListWorkspaces(ctx)
}

// DeleteWorkspace removes a workspace from the wrapped store.
func (b *Bridge) DeleteWorkspace(ctx context.Context, workspaceID string) error {
	return b.inner.DeleteWorkspace(ctx, workspaceID)
}

// Close closes the wrapped store. It does not stop Run.
func (b *Bridge) Close() error { return b.inner.Close() }
