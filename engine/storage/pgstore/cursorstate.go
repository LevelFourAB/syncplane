package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/cursorstate"
)

// The table stays bounded without a background sweeper, by two rules applied
// on every Put. A lineage keeps only its most recent entries: older
// generations can only be presented by a retried request or a briefly
// concurrent tab, both of which sit within the last few. And a workspace
// drops entries older than cursorStateRetention wholesale, which is what
// clears lineages whose client never came back. The retention is therefore
// the ceiling on how long a client can stay away and still resume without a
// re-bootstrap — sized to the same weeks-not-minutes order of magnitude
// WithGC's retention guidance uses, since past either bound the client is
// reset all the same. A Get of an entry older than cursorStateRenewAfter
// renews it, so an entry that a client keeps presenting does not age out
// while the client is active.
const (
	cursorStateKeepPerLineage = 4
	cursorStateRetention      = 30 * 24 * time.Hour
	cursorStateRenewAfter     = cursorStateRetention / 4
)

// CursorStateStore returns a cursorstate.Store backed by this database, making
// the adapter a cursorstate.Provider: a server constructed on this store gets
// cursors that reference their membership sets instead of restating them, and
// — because the entries are in the shared database — a client can present its
// cursor to any node. Entries are pruned on write (see the constants above);
// a client presenting a pruned entry is reset, exactly as one below the
// tombstone horizon is.
func (s *Store) CursorStateStore() cursorstate.Store {
	return &cursorStateStore{s: s}
}

type cursorStateStore struct {
	s *Store
}

func (c *cursorStateStore) Get(ctx context.Context, workspaceID string, ref cursorstate.Ref) ([]byte, error) {
	var (
		state []byte
		aged  bool
	)
	err := c.s.pool.QueryRow(ctx, `
		SELECT state, written_at < now() - make_interval(secs => $4) FROM cursor_state
		WHERE workspace_id = $1 AND id = $2 AND gen = $3`,
		workspaceID, int64(ref.ID), int64(ref.Gen), cursorStateRenewAfter.Seconds()).Scan(&state, &aged)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, cursorstate.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: cursor state: %w", err)
	}
	// Renewed on a read only once it has aged, so a client that pulls often
	// costs one write per renewal period, not one per pull. A failed renewal
	// costs nothing now: the next Get tries again.
	if aged {
		_, _ = c.s.pool.Exec(ctx, `
			UPDATE cursor_state SET written_at = now()
			WHERE workspace_id = $1 AND id = $2 AND gen = $3`,
			workspaceID, int64(ref.ID), int64(ref.Gen))
	}
	return state, nil
}

func (c *cursorStateStore) Put(ctx context.Context, workspaceID string, ref cursorstate.Ref, state []byte) error {
	// ON CONFLICT DO NOTHING: a Put over an existing ref is a retry carrying
	// the same bytes, so either copy serves.
	if _, err := c.s.pool.Exec(ctx, `
		INSERT INTO cursor_state (workspace_id, id, gen, state)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workspace_id, id, gen) DO NOTHING`,
		workspaceID, int64(ref.ID), int64(ref.Gen), state); err != nil {
		return fmt.Errorf("pgstore: cursor state: %w", err)
	}
	// Pruning failures surface as Put failures even though the entry landed:
	// the server then inlines one cursor it need not have, and the operator
	// sees a put_failure instead of a silently growing table.
	if _, err := c.s.pool.Exec(ctx, `
		DELETE FROM cursor_state
		WHERE workspace_id = $1 AND id = $2 AND (written_at, gen) NOT IN (
			SELECT written_at, gen FROM cursor_state
			WHERE workspace_id = $1 AND id = $2
			ORDER BY written_at DESC, gen DESC LIMIT $3
		)`,
		workspaceID, int64(ref.ID), cursorStateKeepPerLineage); err != nil {
		return fmt.Errorf("pgstore: cursor state: prune lineage: %w", err)
	}
	if _, err := c.s.pool.Exec(ctx, `
		DELETE FROM cursor_state
		WHERE workspace_id = $1 AND written_at < now() - make_interval(secs => $2)`,
		workspaceID, cursorStateRetention.Seconds()); err != nil {
		return fmt.Errorf("pgstore: cursor state: prune aged: %w", err)
	}
	return nil
}
