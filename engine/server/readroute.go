package server

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
)

// readTx runs fn over a read snapshot at least as fresh as minSeq, routed to
// the adapter's read replica when it offers one (storage.ReplicaProvider) and
// to the primary otherwise. minSeq 0 accepts any staleness.
//
// Only paths whose freshness need is expressible as a workspace-sequence floor
// route through here — a cursor's seq, a client's acked seq, or zero for
// reads that re-check against the primary anyway. Reads that compare positions
// across transactions (consumer positions, lag denominators) and everything
// touching cursor state stay on s.store.WithReadTx: served from a snapshot of
// an earlier moment, they would report regressions that never happened.
func (s *Server) readTx(ctx context.Context, workspaceID string, minSeq uint64, fn func(storage.ReadTx) error) error {
	if s.replicaReads == nil {
		return s.store.WithReadTx(ctx, workspaceID, fn)
	}
	route, err := s.replicaReads.WithReadTxMinSeq(ctx, workspaceID, minSeq, fn)
	s.tel().replicaReads.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrReadRoute.String(string(route))))
	return err
}

// readTxAnchored is readTx floored at the primary's current sequence —
// read-your-writes for callers holding no sequence of their own.
func (s *Server) readTxAnchored(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	if s.replicaReads == nil {
		return s.store.WithReadTx(ctx, workspaceID, fn)
	}
	route, err := s.replicaReads.WithReadTxAnchored(ctx, workspaceID, fn)
	s.tel().replicaReads.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrReadRoute.String(string(route))))
	return err
}
