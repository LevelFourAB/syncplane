package server

import (
	"context"
	"log/slog"
)

// The engine's lease roles, one per background sweep. The lease package
// documents the naming convention; consumers outside this package pick names
// that collide with neither.
const (
	leaseRoleGC      = "gc"
	leaseRoleIndexes = "indexes"
)

// tryLease takes role's lease on a workspace for one background pass,
// reporting whether the pass should run. A held lease means another process is
// doing this exact work, so run=false and the pass is skipped; the release is
// nil then and must not be called.
//
// A locker error yields run=true with a no-op release: a lease dedups
// idempotent maintenance, it is not a correctness gate, and erring toward
// "skip" would let a broken lease backend silently starve GC on every node at
// once.
func (s *Server) tryLease(ctx context.Context, role, workspaceID string) (release func(), run bool) {
	release, ok, err := s.leases.TryAcquire(ctx, role, workspaceID)
	if err != nil {
		s.log().LogAttrs(ctx, slog.LevelWarn, "lease unavailable; proceeding unleased",
			slog.String("role", role),
			slog.String("workspace", workspaceID),
			slog.Any("err", err))
		return func() {}, true
	}
	if !ok {
		return nil, false
	}
	return release, true
}
