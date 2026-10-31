// Package prefixstore partitions one storage adapter between several servers
// by giving each its own prefix of the workspace-id space.
//
// A storage adapter has one flat space of workspaces and no notion of who
// groups them. A host that serves several applications from one database needs
// exactly one thing of it — that no application can read or write another's
// workspaces — and a prefix delivers it: the host hands each application a
// wrapped adapter, that application's servers go on using their own bare
// workspace ids, and every id reaching the database carries the prefix.
//
//	store := sqlitestore.Open(path)
//	srv, err := server.New(prefixstore.Wrap(store, "acme/"))
//
// The prefix must be one no workspace id can begin with, or two applications
// share a partition. A trailing separator outside the id alphabet is what
// guarantees that; the engine's Manager uses "<appID>/", and `/` is not a legal
// workspace-id character.
//
// # What the prefix reaches
//
// Every workspace id crossing the seam, in both directions: the ids passed to
// the transaction methods, the Range each work query scans, and the ids those
// queries return, which come back stripped. The optional capabilities keyed by
// workspace — leases, cursor state, replica reads — are wrapped the same way,
// so background passes and pull cursors are partitioned with the data.
//
// The server-global catalog is NOT reachable through a wrapped adapter. It
// belongs to no workspace and therefore to no partition; a host that keeps
// state there uses the adapter it wrapped.
package prefixstore

import (
	"context"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Wrap returns a view of inner in which every workspace id is prefixed, so
// that servers built on it cannot see workspaces outside the prefix. An empty
// prefix returns inner unchanged.
//
// Close on the returned adapter is a no-op: inner keeps its handle, and the
// caller that opened it closes it.
//
// The view is not a storage.As chain. A capability it does not know how to
// prefix stays invisible rather than resolving to an unpartitioned one, so
// storage.As answers false for anything added after this package rather than
// handing out a view that reaches every partition.
func Wrap(inner storage.Adapter, prefix string) storage.Adapter {
	if prefix == "" {
		return inner
	}
	s := &store{inner: inner, prefix: prefix, bound: upperBound(prefix)}
	if p, ok := storage.As[lease.Provider](inner); ok {
		s.leases = leaseLocker{inner: p.Leases(), prefix: prefix}
	} else {
		// The same fallback a Server makes for an adapter that provides no
		// locker, taken here so that Leases never answers nil.
		s.leases = leaseLocker{inner: lease.NewLocal(), prefix: prefix}
	}
	if p, ok := storage.As[cursorstate.Provider](inner); ok {
		if st := p.CursorStateStore(); st != nil {
			s.cursors = cursorStore{inner: st, prefix: prefix}
		}
	}
	if p, ok := storage.As[storage.ReplicaProvider](inner); ok {
		if r := p.ReplicaReader(); r != nil {
			s.replica = replicaReader{inner: r, prefix: prefix}
		}
	}
	return s
}

// store is the prefixed view. bound is the exclusive upper end of the prefix's
// run of ids, computed once because every work query needs it.
type store struct {
	inner  storage.Adapter
	prefix string
	bound  string

	leases  lease.Locker
	cursors cursorstate.Store
	replica storage.ReplicaReader
}

// WithReadTx runs the read transaction against the prefixed workspace.
func (s *store) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	return s.inner.WithReadTx(ctx, s.prefix+workspaceID, fn)
}

// WithWriteTx runs the write transaction against the prefixed workspace.
func (s *store) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	return s.inner.WithWriteTx(ctx, s.prefix+workspaceID, fn)
}

// DeleteWorkspace deletes the prefixed workspace.
func (s *store) DeleteWorkspace(ctx context.Context, workspaceID string) error {
	return s.inner.DeleteWorkspace(ctx, s.prefix+workspaceID)
}

// RestoreWorkspace lifts the deletion mark of the prefixed workspace.
func (s *store) RestoreWorkspace(ctx context.Context, workspaceID string) error {
	return s.inner.RestoreWorkspace(ctx, s.prefix+workspaceID)
}

// WorkspaceDeleted reports the deletion mark of the prefixed workspace.
func (s *store) WorkspaceDeleted(ctx context.Context, workspaceID string) (bool, error) {
	return s.inner.WorkspaceDeleted(ctx, s.prefix+workspaceID)
}

// ListWorkspaceIDs enumerates the workspaces inside the prefix.
func (s *store) ListWorkspaceIDs(ctx context.Context, rng storage.Range) ([]string, error) {
	ids, err := s.inner.ListWorkspaceIDs(ctx, s.scope(rng))
	return s.strip(ids), err
}

// ListPendingWorkspaceIDs pages the consumer's pending workspaces inside the
// prefix.
func (s *store) ListPendingWorkspaceIDs(ctx context.Context, consumer string, rng storage.Range, limit int) ([]string, error) {
	ids, err := s.inner.ListPendingWorkspaceIDs(ctx, consumer, s.scope(rng), limit)
	return s.strip(ids), err
}

// ListCollectableWorkspaceIDs pages the collectable workspaces inside the
// prefix.
func (s *store) ListCollectableWorkspaceIDs(ctx context.Context, cutoffs storage.GCCutoffs, rng storage.Range, limit int) ([]string, error) {
	ids, err := s.inner.ListCollectableWorkspaceIDs(ctx, cutoffs, s.scope(rng), limit)
	return s.strip(ids), err
}

// ListStaleIndexWorkspaceIDs pages the stale-index workspaces inside the
// prefix.
func (s *store) ListStaleIndexWorkspaceIDs(ctx context.Context, mark string, rng storage.Range, limit int) ([]string, error) {
	ids, err := s.inner.ListStaleIndexWorkspaceIDs(ctx, mark, s.scope(rng), limit)
	return s.strip(ids), err
}

// ListCompactableWorkspaceIDs pages the compactable workspaces inside the
// prefix.
func (s *store) ListCompactableWorkspaceIDs(ctx context.Context, minPurged int, rng storage.Range, limit int) ([]string, error) {
	ids, err := s.inner.ListCompactableWorkspaceIDs(ctx, minPurged, s.scope(rng), limit)
	return s.strip(ids), err
}

// Close is a no-op; the wrapped adapter owns the handle.
func (s *store) Close() error { return nil }

// Leases returns the prefixed locker, so two partitions leasing the same role
// and workspace id do not exclude each other.
func (s *store) Leases() lease.Locker { return s.leases }

// CursorStateStore returns the prefixed cursor-state store, or nil when the
// wrapped adapter provides none.
func (s *store) CursorStateStore() cursorstate.Store {
	if s.cursors == nil {
		return nil
	}
	return s.cursors
}

// ReplicaReader returns the prefixed replica reader, or nil when the wrapped
// adapter has no replica configured.
func (s *store) ReplicaReader() storage.ReplicaReader {
	if s.replica == nil {
		return nil
	}
	return s.replica
}

// scope narrows rng to the prefix's run of ids. An unset end of the caller's
// range becomes the end of the run rather than the end of the store, which is
// what keeps a query that matches nothing from scanning past the partition.
func (s *store) scope(rng storage.Range) storage.Range {
	out := storage.Range{After: s.prefix, Before: s.bound}
	if rng.After != "" {
		out.After = s.prefix + rng.After
	}
	if rng.Before != "" {
		out.Before = s.prefix + rng.Before
	}
	return out
}

// strip removes the prefix from ids read back. Ids outside the prefix are
// dropped rather than returned unprefixed: the range already excludes them, so
// one appearing here is an adapter answering a wider question than it was
// asked, and a partition is the wrong place to report it.
//
// Into a new slice rather than over the one it was given: nothing in the
// contract says the ids an adapter returns are the caller's to rewrite.
func (s *store) strip(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if ws, mine := naming.CutWorkspacePrefix(id, s.prefix); mine {
			out = append(out, ws)
		}
	}
	return out
}

// leaseLocker prefixes the workspace half of every lease.
type leaseLocker struct {
	inner  lease.Locker
	prefix string
}

func (l leaseLocker) TryAcquire(ctx context.Context, role, workspaceID string) (func(), bool, error) {
	return l.inner.TryAcquire(ctx, role, l.prefix+workspaceID)
}

// cursorStore prefixes the workspace an entry is stored under.
type cursorStore struct {
	inner  cursorstate.Store
	prefix string
}

func (c cursorStore) Get(ctx context.Context, workspaceID string, ref cursorstate.Ref) ([]byte, error) {
	return c.inner.Get(ctx, c.prefix+workspaceID, ref)
}

func (c cursorStore) Put(ctx context.Context, workspaceID string, ref cursorstate.Ref, state []byte) error {
	return c.inner.Put(ctx, c.prefix+workspaceID, ref, state)
}

// replicaReader prefixes the workspace of a replica read.
type replicaReader struct {
	inner  storage.ReplicaReader
	prefix string
}

func (r replicaReader) WithReadTxMinSeq(ctx context.Context, workspaceID string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return r.inner.WithReadTxMinSeq(ctx, r.prefix+workspaceID, minSeq, fn)
}

func (r replicaReader) WithReadTxAnchored(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return r.inner.WithReadTxAnchored(ctx, r.prefix+workspaceID, fn)
}

// upperBound returns the first id that sorts after every id beginning with
// prefix: the prefix with its last byte below 0xff incremented and everything
// after it dropped. A prefix of nothing but 0xff bytes has no such id and
// returns "", which Range reads as "to the end of the store".
func upperBound(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}
