package sqlitestore

import "github.com/levelfourab/syncplane/engine/lease"

// Leases returns a process-local lease.Locker, making the adapter a
// lease.Provider: a server constructed on this store dedups its background
// work with no configuration. Process-local is the whole lease space this
// adapter can have — a database file is opened by at most one process (see the
// package comment), so there is no other node to exclude.
func (s *Store) Leases() lease.Locker {
	return s.leases
}
