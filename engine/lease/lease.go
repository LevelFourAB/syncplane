// Package lease hands out per-workspace work leases: advisory try-locks that
// let one holder of a (role, workspace) pair proceed while everyone else
// skips.
//
// Background consumers — the engine's GC and index sweeps, an effects runtime,
// application-side workers — are idempotent, so several nodes running them
// against one store duplicate work rather than corrupt it, and for a consumer
// with outward side effects each duplicate is user-visible. A lease narrows
// that duplication to the windows the primitive cannot close.
//
// A lease is advisory deduplication, not mutual exclusion to build
// correctness on. It can be lost without the holder noticing — a dropped
// database connection releases every lease its session held — and a Locker
// that cannot reach its backend reports an error, which callers treat as
// "proceed without the lease". Work guarded by a lease must therefore stay
// exactly as idempotent as work that runs unleased.
//
// Roles are free-form strings scoped by the deployment sharing a Locker. The
// engine takes "gc" and "indexes" for its own sweeps and "effects/<consumer>"
// for effect runtimes; applications leasing their own per-workspace work
// should pick names outside those.
package lease

import (
	"context"
	"sync"
)

// Locker hands out leases. Implementations must be safe for concurrent use.
type Locker interface {
	// TryAcquire takes the lease on (role, workspaceID) if nothing holds it,
	// and reports ok=false — with a nil release — when something does. It
	// never blocks waiting for a holder.
	//
	// The returned release gives the lease back; calling it more than once is
	// a no-op. An error means the Locker could not reach its backend, not
	// that the lease is held: callers whose work is idempotent should proceed
	// as if unleased.
	TryAcquire(ctx context.Context, role, workspaceID string) (release func(), ok bool, err error)
}

// Provider is implemented by storage adapters that supply a Locker covering
// every process sharing the store, so those processes lease out of one space.
// A server constructed without server.WithLeases asks its adapter for one, so
// an adapter that implements Provider gives every server built on it shared
// leases with no configuration.
type Provider interface {
	// Leases returns the adapter-backed Locker. The adapter retains ownership
	// and closes it with the adapter itself.
	Leases() Locker
}

// NewLocal returns a process-local Locker: a lease excludes only holders in
// the same process. It is the locker for a store a single process owns —
// sqlitestore's is one — and the fallback a server takes when its adapter
// provides none, where it dedups nothing between processes.
func NewLocal() *Local {
	return &Local{held: make(map[localKey]struct{})}
}

// Local is the process-local Locker returned by NewLocal. It is safe for
// concurrent use; the zero value is not usable.
type Local struct {
	mu   sync.Mutex
	held map[localKey]struct{}
}

type localKey struct {
	role string
	ws   string
}

// TryAcquire takes the lease if no holder in this process has it. It never
// returns an error and ignores ctx: there is no backend to reach.
func (l *Local) TryAcquire(_ context.Context, role, workspaceID string) (func(), bool, error) {
	k := localKey{role: role, ws: workspaceID}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, taken := l.held[k]; taken {
		return nil, false, nil
	}
	l.held[k] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.held, k)
			l.mu.Unlock()
		})
	}, true, nil
}
