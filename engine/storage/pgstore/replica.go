package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/storage"
)

// openReplica builds the read-replica pool and verifies the schema behind it.
//
// migrate never runs here: the replica is expected to be a read-only standby,
// and the migration path's session advisory lock and DDL are exactly what a
// standby refuses. The probe below is the substitute — a standby that has
// replayed the primary's migrations answers with the primary's version, and
// anything else (an unrelated database, an empty one, a standby still
// replaying a fresh migration) fails Open rather than serving reads from the
// wrong schema.
//
// It is the PRIMARY's version that is compared, not this binary's. The two
// differ whenever a node serves a database migrated ahead of it, which is a
// state the ledger licenses (see serveFloor); comparing against the binary
// instead would refuse the replica in exactly that case, and it would be
// answering a question about the binary while claiming to be about the standby.
func openReplica(ctx context.Context, dsn string, maxConns int32, primaryVersion int) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: parse replica dsn: %w", err)
	}
	if maxConns > 0 {
		poolCfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: connect replica: %w", err)
	}
	version, err := schemaVersion(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: probe replica schema (is the replica DSN a standby of this database?): %w", err)
	}
	if version != primaryVersion {
		pool.Close()
		return nil, fmt.Errorf("pgstore: replica schema at version %d, primary at %d (is the replica DSN a standby of this database?)",
			version, primaryVersion)
	}
	return pool, nil
}

// replicaReader implements storage.ReplicaReader over the store's replica
// pool, falling back to the primary through the store's own WithReadTx.
type replicaReader struct {
	s *Store
}

// ReplicaReader returns the store's replica-capable reader, or nil when Open
// was not given WithReadReplica (see storage.ReplicaProvider).
func (s *Store) ReplicaReader() storage.ReplicaReader {
	if s.replica == nil {
		return nil
	}
	return replicaReader{s: s}
}

func (r replicaReader) WithReadTxMinSeq(ctx context.Context, workspaceID string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	s := r.s
	// The same isolation as WithReadTx: fn's contract is one consistent
	// snapshot wherever it runs.
	tx, err := s.replica.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return storage.ReadRouteFallbackError, s.WithReadTx(ctx, workspaceID, fn)
	}
	if minSeq > 0 {
		// Judged INSIDE the open transaction, so the answer holds for the
		// snapshot fn would read — a check on a separate connection could pass
		// against a fresher snapshot than the transaction's own.
		var seq int64
		err := tx.QueryRow(ctx,
			`SELECT last_seq FROM workspaces WHERE workspace_id = $1`,
			workspaceID).Scan(&seq)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			seq = 0
		case err != nil:
			_ = tx.Rollback(ctx)
			return storage.ReadRouteFallbackError, s.WithReadTx(ctx, workspaceID, fn)
		}
		if uint64(seq) < minSeq {
			_ = tx.Rollback(ctx)
			return storage.ReadRouteFallbackStale, s.WithReadTx(ctx, workspaceID, fn)
		}
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return storage.ReadRouteReplica, fn(readOnlyTx{&pgTx{tx: tx, ctx: ctx, ws: workspaceID}})
}

func (r replicaReader) WithReadTxAnchored(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	s := r.s
	// A bare point read, not a transaction: the connection goes back to the
	// pool before the (possibly long) read runs, so anchoring costs the
	// primary one statement rather than a held connection.
	var seq int64
	err := s.pool.QueryRow(ctx,
		`SELECT last_seq FROM workspaces WHERE workspace_id = $1`,
		workspaceID).Scan(&seq)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		seq = 0
	case err != nil:
		return storage.ReadRouteFallbackError, s.WithReadTx(ctx, workspaceID, fn)
	}
	return r.WithReadTxMinSeq(ctx, workspaceID, uint64(seq), fn)
}
