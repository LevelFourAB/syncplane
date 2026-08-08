package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
// reset all the same.
const (
	cursorStateKeepPerLineage = 4
	cursorStateRetention      = 30 * 24 * time.Hour
)

// CursorStateStore returns a cursorstate.Store backed by this database, making
// the adapter a cursorstate.Provider: a server constructed on this store gets
// cursors that reference their membership sets instead of restating them, and
// the entries survive restarts with the rest of the database. Entries are
// pruned on write (see the constants above); a client presenting a pruned
// entry is reset, exactly as one below the tombstone horizon is.
func (s *Store) CursorStateStore() cursorstate.Store {
	return &cursorStateStore{s: s}
}

type cursorStateStore struct {
	s *Store
}

func (c *cursorStateStore) Get(ctx context.Context, workspaceID string, ref cursorstate.Ref) ([]byte, error) {
	var state []byte
	err := c.s.read.QueryRowContext(ctx, `
		SELECT state FROM cursor_state
		WHERE namespace = ? AND workspace_id = ? AND id = ? AND gen = ?`,
		c.s.ns, workspaceID, int64(ref.ID), int64(ref.Gen)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, cursorstate.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: cursor state: %w", err)
	}
	return state, nil
}

func (c *cursorStateStore) Put(ctx context.Context, workspaceID string, ref cursorstate.Ref, state []byte) error {
	now := time.Now().Unix()
	// ON CONFLICT DO NOTHING: a Put over an existing ref is a retry carrying
	// the same bytes, so either copy serves.
	if _, err := c.s.write.ExecContext(ctx, `
		INSERT INTO cursor_state (namespace, workspace_id, id, gen, state, written_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (namespace, workspace_id, id, gen) DO NOTHING`,
		c.s.ns, workspaceID, int64(ref.ID), int64(ref.Gen), state, now); err != nil {
		return fmt.Errorf("sqlitestore: cursor state: %w", err)
	}
	// Pruning failures surface as Put failures even though the entry landed:
	// the server then inlines one cursor it need not have, and the operator
	// sees a put_failure instead of a silently growing table.
	if _, err := c.s.write.ExecContext(ctx, `
		DELETE FROM cursor_state
		WHERE namespace = ?1 AND workspace_id = ?2 AND id = ?3 AND rowid NOT IN (
			SELECT rowid FROM cursor_state
			WHERE namespace = ?1 AND workspace_id = ?2 AND id = ?3
			ORDER BY written_at DESC, rowid DESC LIMIT ?4
		)`,
		c.s.ns, workspaceID, int64(ref.ID), cursorStateKeepPerLineage); err != nil {
		return fmt.Errorf("sqlitestore: cursor state: prune lineage: %w", err)
	}
	if _, err := c.s.write.ExecContext(ctx, `
		DELETE FROM cursor_state
		WHERE namespace = ? AND workspace_id = ? AND written_at < ?`,
		c.s.ns, workspaceID, now-int64(cursorStateRetention/time.Second)); err != nil {
		return fmt.Errorf("sqlitestore: cursor state: prune aged: %w", err)
	}
	return nil
}
