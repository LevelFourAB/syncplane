package sqlitestore

import (
	"context"

	"github.com/levelfourab/syncplane/engine/lease"
)

// Leases returns a process-local lease.Locker, making the adapter a
// lease.Provider: a server constructed on this store dedups its background
// work with no configuration. Process-local is the whole lease space this
// adapter can have — a database file is opened by at most one process (see the
// package comment), so there is no other node to exclude.
func (s *Store) Leases() lease.Locker {
	return nsLocker{local: s.leases, ns: s.ns}
}

// nsLocker scopes the store-wide locker to one namespace by folding the
// namespace into the role, separator included so ("a", "bc") and ("ab", "c")
// stay distinct pairs.
type nsLocker struct {
	local *lease.Local
	ns    string
}

func (l nsLocker) TryAcquire(ctx context.Context, role, workspaceID string) (func(), bool, error) {
	return l.local.TryAcquire(ctx, l.ns+"\x00"+role, workspaceID)
}
