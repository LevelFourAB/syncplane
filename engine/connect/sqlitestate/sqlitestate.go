// Package sqlitestate implements connect.State on SQLite via
// modernc.org/sqlite (pure Go, no CGO) — the shipped default for connector
// bookkeeping. The database is connector-owned and fully reconstructible:
// deleting it costs one full reconcile, never data.
package sqlitestate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// schema is the immutable baseline; structural evolution happens through
// migrations indexed by PRAGMA user_version, mirroring sqlitestore.
const schema = `
CREATE TABLE IF NOT EXISTS shadows (
	workspace_id   TEXT NOT NULL,
	entity         TEXT NOT NULL,
	key            TEXT NOT NULL,
	source_fp      TEXT NOT NULL,
	engine_version INTEGER NOT NULL,
	base           BLOB,
	PRIMARY KEY (workspace_id, entity, key)
);
CREATE TABLE IF NOT EXISTS egress_cursors (
	workspace_id TEXT PRIMARY KEY,
	seq          INTEGER NOT NULL,
	entity       TEXT NOT NULL DEFAULT '',
	key          TEXT NOT NULL DEFAULT ''
);
`

var migrations = []string{
	// 1: the ingress half of the bookkeeping — where an incremental source's
	// change feed has been consumed to (source_sync), and what plain text the
	// source last showed per collaborative-text field (shadows.base_text, a
	// JSON object). base_text is NULL on rows written before it existed;
	// connect falls back to rendering the base document for those.
	`
	CREATE TABLE IF NOT EXISTS source_sync (
		workspace_id   TEXT PRIMARY KEY,
		cursor         TEXT NOT NULL DEFAULT '',
		last_full_sync INTEGER NOT NULL DEFAULT 0
	);
	ALTER TABLE shadows ADD COLUMN base_text BLOB;
	`,
}

// Store implements connect.State.
type Store struct {
	db *sql.DB
}

var _ connect.State = (*Store)(nil)

// Open opens (creating if necessary) the connector state database at path.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Bookkeeping volume is low; one connection serializes writes the same
	// way sqlitestore's writer pool does and keeps the model trivial.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlitestate: apply schema: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("sqlitestate: read user_version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf("sqlitestate: database user_version %d is newer than this binary (max %d)", version, len(migrations))
	}
	for ; version < len(migrations); version++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[version]); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlitestate: migration %d: %w", version+1, err)
		}
		if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, version+1)); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlitestate: migration %d: bump user_version: %w", version+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlitestate: migration %d: commit: %w", version+1, err)
		}
	}
	return nil
}

// encodeTexts serializes a shadow's per-field plain text. Nil and empty both
// store as NULL, so the "no record" case has exactly one representation.
func encodeTexts(texts map[string]string) ([]byte, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	return json.Marshal(texts)
}

func decodeTexts(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var texts map[string]string
	if err := json.Unmarshal(raw, &texts); err != nil {
		return nil, fmt.Errorf("sqlitestate: decode base_text: %w", err)
	}
	return texts, nil
}

func (s *Store) Shadow(ctx context.Context, workspaceID, entity, key string) (*connect.Shadow, error) {
	var (
		sh       connect.Shadow
		baseText []byte
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT source_fp, engine_version, base, base_text FROM shadows
		 WHERE workspace_id = ? AND entity = ? AND key = ?`,
		workspaceID, entity, key,
	).Scan(&sh.SourceFP, &sh.EngineVersion, &sh.Base, &baseText)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if sh.BaseText, err = decodeTexts(baseText); err != nil {
		return nil, err
	}
	return &sh, nil
}

func (s *Store) PutShadow(ctx context.Context, workspaceID, entity, key string, sh connect.Shadow) error {
	baseText, err := encodeTexts(sh.BaseText)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO shadows (workspace_id, entity, key, source_fp, engine_version, base, base_text)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, entity, key) DO UPDATE SET
		   source_fp = excluded.source_fp,
		   engine_version = excluded.engine_version,
		   base = excluded.base,
		   base_text = excluded.base_text`,
		workspaceID, entity, key, sh.SourceFP, sh.EngineVersion, sh.Base, baseText,
	)
	return err
}

func (s *Store) DeleteShadow(ctx context.Context, workspaceID, entity, key string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM shadows WHERE workspace_id = ? AND entity = ? AND key = ?`,
		workspaceID, entity, key,
	)
	return err
}

func (s *Store) ScanShadows(ctx context.Context, workspaceID string, fn func(entity, key string, sh connect.Shadow) error) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT entity, key, source_fp, engine_version, base, base_text FROM shadows
		 WHERE workspace_id = ? ORDER BY entity, key`,
		workspaceID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var entity, key string
		var sh connect.Shadow
		var baseText []byte
		if err := rows.Scan(&entity, &key, &sh.SourceFP, &sh.EngineVersion, &sh.Base, &baseText); err != nil {
			return err
		}
		if sh.BaseText, err = decodeTexts(baseText); err != nil {
			return err
		}
		if err := fn(entity, key, sh); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) Cursor(ctx context.Context, workspaceID string) (server.Cursor, error) {
	var c server.Cursor
	err := s.db.QueryRowContext(ctx,
		`SELECT seq, entity, key FROM egress_cursors WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&c.Seq, &c.Entity, &c.Key)
	if err == sql.ErrNoRows {
		return server.Cursor{}, nil
	}
	if err != nil {
		return server.Cursor{}, err
	}
	return c, nil
}

func (s *Store) SetCursor(ctx context.Context, workspaceID string, c server.Cursor) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO egress_cursors (workspace_id, seq, entity, key)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (workspace_id) DO UPDATE SET
		   seq = excluded.seq, entity = excluded.entity, key = excluded.key`,
		workspaceID, c.Seq, c.Entity, c.Key,
	)
	return err
}

func (s *Store) SourceSync(ctx context.Context, workspaceID string) (connect.SourceSync, error) {
	var (
		out   connect.SourceSync
		nanos int64
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT cursor, last_full_sync FROM source_sync WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&out.Cursor, &nanos)
	if err == sql.ErrNoRows {
		return connect.SourceSync{}, nil
	}
	if err != nil {
		return connect.SourceSync{}, err
	}
	if nanos != 0 {
		out.LastFullSync = time.Unix(0, nanos)
	}
	return out, nil
}

func (s *Store) SetSourceSync(ctx context.Context, workspaceID string, sync connect.SourceSync) error {
	var nanos int64
	if !sync.LastFullSync.IsZero() {
		nanos = sync.LastFullSync.UnixNano()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO source_sync (workspace_id, cursor, last_full_sync)
		 VALUES (?, ?, ?)
		 ON CONFLICT (workspace_id) DO UPDATE SET
		   cursor = excluded.cursor, last_full_sync = excluded.last_full_sync`,
		workspaceID, sync.Cursor, nanos,
	)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}
