package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/storage"
)

// pgTx implements storage.WriteTx (and, narrowed by readOnlyTx, storage.ReadTx)
// over one pgx transaction.
//
// ctx is held on the struct because storage.ReadTx's methods take none: the
// engine scopes cancellation at WithReadTx/WithWriteTx, so every statement this
// transaction issues belongs to that one context and there is no second
// lifetime for a field to confuse it with.
type pgTx struct {
	tx  pgx.Tx
	ctx context.Context
	ns  string
	ws  string
	// savepoint nesting depth; names savepoints sp1, sp2, ...
	spDepth int
	// cursorSeq names scan cursors uniquely within the transaction.
	cursorSeq int
	// registered memoizes register's INSERT so a write burst does not repeat
	// it. Cleared on savepoint rollback, which may have undone that INSERT.
	registered bool
}

// register creates the workspaces row, and is called by every mutating method
// before it writes. Registration is deliberately lazy: workspace_id is
// unauthenticated wire input, so a transaction that turns out to write nothing
// must not leave behind a row that ListWorkspaceIDs — and with it every
// background pass — iterates forever.
//
// It mints the store epoch here and never touches it again (ON CONFLICT DO
// NOTHING): the epoch must stay stable for the workspace's lifetime so cursors
// keep matching across restarts.
func (t *pgTx) register() error {
	if t.registered {
		return nil
	}
	epoch, err := newStoreEpoch()
	if err != nil {
		return err
	}
	if _, err := t.tx.Exec(t.ctx,
		`INSERT INTO workspaces (namespace, workspace_id, store_epoch) VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING`,
		t.ns, t.ws, epoch,
	); err != nil {
		return err
	}
	t.registered = true
	return nil
}

func (t *pgTx) CurrentSeq() (uint64, error) {
	var seq int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT last_seq FROM workspaces WHERE namespace = $1 AND workspace_id = $2`, t.ns, t.ws,
	).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint64(seq), err
}

func (t *pgTx) StoreEpoch() (uint64, error) {
	// Stored as a signed BIGINT because Postgres has no unsigned one;
	// reinterpret the bits as the unsigned epoch. The value is opaque — only
	// equality matters.
	var epoch int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT store_epoch FROM workspaces WHERE namespace = $1 AND workspace_id = $2`, t.ns, t.ws,
	).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint64(epoch), err
}

func (t *pgTx) TombstoneHorizon() (uint64, error) {
	var h int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT tombstone_horizon FROM workspaces WHERE namespace = $1 AND workspace_id = $2`, t.ns, t.ws,
	).Scan(&h)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint64(h), err
}

func (t *pgTx) GetRow(entity, key string) (storage.Row, error) {
	row := storage.Row{Entity: entity, Key: key}
	var version int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT value, version, deleted, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4`,
		t.ns, t.ws, entity, key,
	).Scan(&row.Value, &version, &row.Deleted,
		&row.Actor.Subject, &row.Actor.System)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.Row{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.Row{}, err
	}
	row.Version = uint64(version)
	return row, nil
}

// decodeEntityRow reads the column set the two key-ordered entity scans select.
// It runs with the result set open, so it only Scans (see scanCursor).
func decodeEntityRow(entity string) func(pgx.Rows) (storage.Row, error) {
	return func(rows pgx.Rows) (storage.Row, error) {
		r := storage.Row{Entity: entity}
		var version int64
		if err := rows.Scan(&r.Key, &r.Value, &version,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return storage.Row{}, err
		}
		r.Version = uint64(version)
		return r, nil
	}
}

func (t *pgTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	return t.ScanEntityAfter(entity, "", 0, fn)
}

func (t *pgTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	return scanCursor(t,
		`SELECT key, value, version, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND NOT deleted AND key > $4
		 ORDER BY key
		 LIMIT $5`,
		[]any{t.ns, t.ws, entity, afterKey, limitArg(limit)},
		decodeEntityRow(entity), fn)
}

func (t *pgTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	// ORDER BY matches rows_by_entity_version exactly (version DESC, key ASC,
	// partial on NOT deleted), so this reads the index prefix and stops at the
	// limit instead of sorting the entity.
	return scanCursor(t,
		`SELECT key, value, version, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND NOT deleted
		 ORDER BY version DESC, key
		 LIMIT $4`,
		[]any{t.ns, t.ws, entity, limitArg(limit)},
		decodeEntityRow(entity), fn)
}

func (t *pgTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	decode := func(rows pgx.Rows) (storage.Row, error) {
		var r storage.Row
		var version int64
		if err := rows.Scan(&r.Entity, &r.Key, &r.Value, &version, &r.Deleted,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return storage.Row{}, err
		}
		r.Version = uint64(version)
		if r.Deleted {
			r.Value = nil
		}
		return r, nil
	}
	const cols = `SELECT entity, key, value, version, deleted,
	                     actor_subject, actor_system
	              FROM rows `

	if afterEntity == "" {
		// A clean version boundary: everything strictly past afterSeq. This
		// cannot be folded into the row comparison below — every row carries a
		// non-empty entity, so (version, entity, key) > (afterSeq, '', '') would
		// also take the rows AT afterSeq.
		return scanCursor(t,
			cols+`WHERE namespace = $1 AND workspace_id = $2 AND version > $3
			      ORDER BY version, entity, key
			      LIMIT $4`,
			[]any{t.ns, t.ws, int64(afterSeq), limitArg(limit)},
			decode, fn)
	}
	// Resume inside a same-version group. The row-value comparison is the whole
	// tie-break in one predicate — Postgres compares it lexicographically,
	// bytewise for entity and key because both columns are COLLATE "C" — where
	// SQLite needs the disjunction spelled out.
	return scanCursor(t,
		cols+`WHERE namespace = $1 AND workspace_id = $2
		        AND (version, entity, key) > ($3::bigint, $4::text, $5::text)
		      ORDER BY version, entity, key
		      LIMIT $6`,
		[]any{t.ns, t.ws, int64(afterSeq), afterEntity, afterKey, limitArg(limit)},
		decode, fn)
}

func (t *pgTx) ClientLastMutationID(clientID string) (uint64, error) {
	var id int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT last_mutation_id FROM clients
		 WHERE namespace = $1 AND workspace_id = $2 AND client_id = $3`,
		t.ns, t.ws, clientID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint64(id), err
}

func (t *pgTx) ClientRejections(clientID string) ([]storage.ClientRejection, error) {
	var blob []byte
	err := t.tx.QueryRow(t.ctx,
		`SELECT rejections FROM clients
		 WHERE namespace = $1 AND workspace_id = $2 AND client_id = $3`,
		t.ns, t.ws, clientID,
	).Scan(&blob)
	if errors.Is(err, pgx.ErrNoRows) || len(blob) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var recs []storage.ClientRejection
	if err := json.Unmarshal(blob, &recs); err != nil {
		return nil, fmt.Errorf("pgstore: decode rejections for client %q: %w", clientID, err)
	}
	return recs, nil
}

func (t *pgTx) IdempotencyRecord(key string) (storage.IdempotencyRecord, bool, error) {
	var (
		rec storage.IdempotencyRecord
		seq int64
	)
	err := t.tx.QueryRow(t.ctx,
		`SELECT seq, result FROM idempotency
		 WHERE namespace = $1 AND workspace_id = $2 AND key = $3`,
		t.ns, t.ws, key,
	).Scan(&seq, &rec.Result)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return storage.IdempotencyRecord{}, false, err
	}
	rec.Seq = uint64(seq)
	// The column is NOT NULL, so "no result" round-trips as an empty blob;
	// normalize it back to nil so callers see what they recorded.
	if len(rec.Result) == 0 {
		rec.Result = nil
	}
	return rec, true, nil
}

func (t *pgTx) PutIdempotencyRecord(key string, rec storage.IdempotencyRecord) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO idempotency (namespace, workspace_id, key, seq, result, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, key)
		 DO UPDATE SET seq = excluded.seq, result = excluded.result, updated_at = excluded.updated_at`,
		t.ns, t.ws, key, int64(rec.Seq), notNilBytes(rec.Result), time.Now().UnixMilli(),
	)
	return err
}

func (t *pgTx) CollectIdempotencyKeys(cutoff time.Time) (int, error) {
	tag, err := t.tx.Exec(t.ctx,
		`DELETE FROM idempotency WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3`,
		t.ns, t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) ConsumerPosition(consumer string) (storage.ConsumerPosition, bool, error) {
	pos := storage.ConsumerPosition{Consumer: consumer}
	var (
		seq     int64
		updated int64
	)
	err := t.tx.QueryRow(t.ctx,
		`SELECT seq, entity, key, updated_at FROM consumer_positions
		 WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3`,
		t.ns, t.ws, consumer,
	).Scan(&seq, &pos.Entity, &pos.Key, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ConsumerPosition{}, false, nil
	}
	if err != nil {
		return storage.ConsumerPosition{}, false, err
	}
	pos.Seq = uint64(seq)
	pos.UpdatedAt = time.UnixMilli(updated)
	return pos, true, nil
}

func (t *pgTx) ListConsumerPositions() ([]storage.ConsumerPosition, error) {
	rows, err := t.tx.Query(t.ctx,
		`SELECT consumer, seq, entity, key, updated_at FROM consumer_positions
		 WHERE namespace = $1 AND workspace_id = $2 ORDER BY consumer`,
		t.ns, t.ws,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.ConsumerPosition
	for rows.Next() {
		var (
			pos     storage.ConsumerPosition
			seq     int64
			updated int64
		)
		if err := rows.Scan(&pos.Consumer, &seq, &pos.Entity, &pos.Key, &updated); err != nil {
			return nil, err
		}
		pos.Seq = uint64(seq)
		pos.UpdatedAt = time.UnixMilli(updated)
		out = append(out, pos)
	}
	return out, rows.Err()
}

func (t *pgTx) SetConsumerPosition(consumer string, p storage.ConsumerPosition) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO consumer_positions (namespace, workspace_id, consumer, seq, entity, key, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (namespace, workspace_id, consumer)
		 DO UPDATE SET seq = excluded.seq, entity = excluded.entity,
		               key = excluded.key, updated_at = excluded.updated_at`,
		t.ns, t.ws, consumer, int64(p.Seq), p.Entity, p.Key, time.Now().UnixMilli(),
	)
	return err
}

func (t *pgTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	var snapshot []byte
	err := t.tx.QueryRow(t.ctx,
		`SELECT snapshot FROM consumer_shadows
		 WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3 AND entity = $4 AND key = $5`,
		t.ns, t.ws, consumer, entity, key,
	).Scan(&snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (t *pgTx) PutConsumerShadow(consumer, entity, key string, snapshot []byte) error {
	if err := t.register(); err != nil {
		return err
	}
	// The column is NOT NULL, so a nil snapshot stores as an empty blob and
	// reads back as one. A consumer that wants "no shadow" deletes it.
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO consumer_shadows (namespace, workspace_id, consumer, entity, key, snapshot)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, consumer, entity, key)
		 DO UPDATE SET snapshot = excluded.snapshot`,
		t.ns, t.ws, consumer, entity, key, notNilBytes(snapshot),
	)
	return err
}

func (t *pgTx) DeleteConsumerShadow(consumer, entity, key string) error {
	_, err := t.tx.Exec(t.ctx,
		`DELETE FROM consumer_shadows
		 WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3 AND entity = $4 AND key = $5`,
		t.ns, t.ws, consumer, entity, key,
	)
	return err
}

func (t *pgTx) DeleteConsumer(consumer string) error {
	if _, err := t.tx.Exec(t.ctx,
		`DELETE FROM consumer_shadows WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3`,
		t.ns, t.ws, consumer,
	); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`DELETE FROM consumer_positions WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3`,
		t.ns, t.ws, consumer,
	)
	return err
}

func (t *pgTx) CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (int, error) {
	// Shadows first, while the positions that name their consumers still exist:
	// the reverse order would leave every collected consumer's shadows behind
	// with nothing left to identify them by.
	if _, err := t.tx.Exec(t.ctx,
		`DELETE FROM consumer_shadows
		 WHERE namespace = $1 AND workspace_id = $2 AND consumer IN (
		     SELECT consumer FROM consumer_positions
		     WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3 AND seq < $4
		 )`,
		t.ns, t.ws, cutoff.UnixMilli(), int64(belowSeq),
	); err != nil {
		return 0, err
	}
	tag, err := t.tx.Exec(t.ctx,
		`DELETE FROM consumer_positions
		 WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3 AND seq < $4`,
		t.ns, t.ws, cutoff.UnixMilli(), int64(belowSeq),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) FieldDeltas(entity, key, field string, afterSeq uint64) ([][]byte, bool, error) {
	var horizon int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT field_delta_horizon FROM workspaces WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws,
	).Scan(&horizon)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	// The span starts where the caller's knowledge ends; anything pruned at or
	// above that point is a hole the fold cannot see.
	if afterSeq < uint64(horizon) {
		return nil, false, nil
	}
	rows, err := t.tx.Query(t.ctx,
		`SELECT frag FROM field_deltas
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4 AND field = $5
		   AND seq > $6
		 ORDER BY seq`,
		t.ns, t.ws, entity, key, field, int64(afterSeq),
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var frags [][]byte
	for rows.Next() {
		var frag []byte
		if err := rows.Scan(&frag); err != nil {
			return nil, false, err
		}
		if len(frag) == 0 {
			// A gap marker: the span is not expressible as deltas. Drain nothing
			// further — the caller must ship full state.
			return nil, false, nil
		}
		frags = append(frags, frag)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return frags, true, nil
}

func (t *pgTx) PutFieldDelta(entity, key, field string, seq uint64, frag []byte) error {
	if err := t.register(); err != nil {
		return err
	}
	// A gap marker must round-trip as an empty (not NULL) blob: the column is
	// NOT NULL and the read path distinguishes empty from present.
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO field_deltas (namespace, workspace_id, entity, key, field, seq, frag, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (namespace, workspace_id, entity, key, field, seq)
		 DO UPDATE SET frag = excluded.frag, updated_at = excluded.updated_at`,
		t.ns, t.ws, entity, key, field, int64(seq), notNilBytes(frag), time.Now().UnixMilli(),
	)
	return err
}

func (t *pgTx) PruneFieldDeltas(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxPruned int64
	if err := t.tx.QueryRow(t.ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM field_deltas
		 WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3`,
		t.ns, t.ws, cutoffMillis,
	).Scan(&maxPruned); err != nil {
		return 0, err
	}
	if maxPruned > 0 {
		if _, err := t.tx.Exec(t.ctx,
			`DELETE FROM field_deltas WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3`,
			t.ns, t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// GREATEST keeps the horizon monotonic even if an older cutoff runs
		// after a newer one, exactly as CollectTombstones does.
		if _, err := t.tx.Exec(t.ctx,
			`UPDATE workspaces SET field_delta_horizon = GREATEST(field_delta_horizon, $3)
			 WHERE namespace = $1 AND workspace_id = $2`,
			t.ns, t.ws, maxPruned,
		); err != nil {
			return 0, err
		}
	}
	var horizon int64
	err := t.tx.QueryRow(t.ctx,
		`SELECT field_delta_horizon FROM workspaces WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws,
	).Scan(&horizon)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint64(horizon), err
}

func (t *pgTx) TextFloor(entity, key, field string) ([]byte, error) {
	var vv []byte
	err := t.tx.QueryRow(t.ctx,
		`SELECT vv FROM text_floors
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4 AND field = $5`,
		t.ns, t.ws, entity, key, field,
	).Scan(&vv)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return vv, err
}

func (t *pgTx) PutTextFloor(entity, key, field string, vv []byte) error {
	if err := t.register(); err != nil {
		return err
	}
	if len(vv) == 0 {
		_, err := t.tx.Exec(t.ctx,
			`DELETE FROM text_floors
			 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4 AND field = $5`,
			t.ns, t.ws, entity, key, field,
		)
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO text_floors (namespace, workspace_id, entity, key, field, vv)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, entity, key, field)
		 DO UPDATE SET vv = excluded.vv`,
		t.ns, t.ws, entity, key, field, vv,
	)
	return err
}

func (t *pgTx) ScanIndex(
	entity, index string,
	lo, hi []byte,
	limit int,
	order storage.ScanOrder,
	fn func(entry []byte, rowKey string) error,
) error {
	// The bounds are optional on both ends, and a "compare only if bound"
	// predicate would defeat the index, so the WHERE clause is built to match
	// what was given. k is BYTEA, which Postgres compares bytewise — the order
	// the entry encoding is built for, and the reason this must never become a
	// text column.
	var where strings.Builder
	where.WriteString(`namespace = $1 AND workspace_id = $2 AND entity = $3 AND idx = $4`)
	args := []any{t.ns, t.ws, entity, index}
	if lo != nil {
		args = append(args, lo)
		fmt.Fprintf(&where, ` AND k >= $%d`, len(args))
	}
	if hi != nil {
		args = append(args, hi)
		fmt.Fprintf(&where, ` AND k < $%d`, len(args))
	}
	args = append(args, limitArg(limit))
	// The row key is part of the sort so that two entries sharing a key come
	// out in a fixed order; reversing the walk has to reverse it too, or the
	// descending scan would not be the exact mirror of the ascending one.
	orderBy := `k, key`
	if order == storage.ScanDesc {
		orderBy = `k DESC, key DESC`
	}
	query := fmt.Sprintf(
		`SELECT k, key FROM index_entries WHERE %s ORDER BY %s LIMIT $%d`, where.String(), orderBy, len(args))

	type indexRow struct {
		entry  []byte
		rowKey string
	}
	return scanCursor(t, query, args,
		func(rows pgx.Rows) (indexRow, error) {
			var r indexRow
			if err := rows.Scan(&r.entry, &r.rowKey); err != nil {
				return indexRow{}, err
			}
			return r, nil
		},
		func(r indexRow) error { return fn(r.entry, r.rowKey) })
}

func (t *pgTx) IndexState(entity, index string) (string, bool, error) {
	var (
		specHash string
		ready    bool
	)
	err := t.tx.QueryRow(t.ctx,
		`SELECT spec_hash, ready FROM index_state
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND idx = $4`,
		t.ns, t.ws, entity, index,
	).Scan(&specHash, &ready)
	if errors.Is(err, pgx.ErrNoRows) {
		// An index nothing has built yet, which is the normal state right after
		// registration — not an error.
		return "", false, nil
	}
	return specHash, ready, err
}

func (t *pgTx) SetIndexState(entity, index, specHash string, ready bool) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO index_state (namespace, workspace_id, entity, idx, spec_hash, ready)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, entity, idx)
		 DO UPDATE SET spec_hash = excluded.spec_hash, ready = excluded.ready`,
		t.ns, t.ws, entity, index, specHash, ready,
	)
	return err
}

func (t *pgTx) PutIndexEntries(entity, key string, entries []storage.IndexEntry) error {
	if len(entries) == 0 {
		// Replace-with-nothing IS the delete, but a row that has never had
		// entries must not register the workspace on its account.
		return t.DeleteIndexEntries(entity, key)
	}
	if err := t.register(); err != nil {
		return err
	}
	// Replace-set: clear first, then insert. index_entries_by_row makes the
	// delete a range scan of this row's entries rather than of the index.
	if _, err := t.tx.Exec(t.ctx,
		`DELETE FROM index_entries
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4`,
		t.ns, t.ws, entity, key,
	); err != nil {
		return err
	}

	var sql strings.Builder
	sql.WriteString(`INSERT INTO index_entries (namespace, workspace_id, entity, idx, k, key) VALUES `)
	args := make([]any, 0, len(entries)*6)
	for i, e := range entries {
		if i > 0 {
			sql.WriteString(", ")
		}
		base := i * 6
		fmt.Fprintf(&sql, "($%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6)
		args = append(args, t.ns, t.ws, entity, e.Index, notNilBytes(e.Key), key)
	}
	// DO NOTHING rather than failing: two components of one index can encode to
	// the same entry for one row only if the caller passed a duplicate, and
	// failing the whole write on that would turn a harmless caller bug into a
	// stuck write path. There are no non-key columns, so there is nothing an
	// update would change.
	sql.WriteString(` ON CONFLICT DO NOTHING`)
	_, err := t.tx.Exec(t.ctx, sql.String(), args...)
	return err
}

func (t *pgTx) DeleteIndexEntries(entity, key string) error {
	// No register(): removing entries a row never had must leave no trace, the
	// same reason a write closure that writes nothing does not create a
	// workspace.
	_, err := t.tx.Exec(t.ctx,
		`DELETE FROM index_entries
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4`,
		t.ns, t.ws, entity, key,
	)
	return err
}

func (t *pgTx) NextSeq() (uint64, error) {
	if err := t.register(); err != nil {
		return 0, err
	}
	// The UPDATE takes a row lock, but the workspace advisory lock has already
	// made this transaction the only writer of the row; this is a read-modify-
	// write in one statement rather than a concurrency control of its own.
	var seq int64
	err := t.tx.QueryRow(t.ctx,
		`UPDATE workspaces SET last_seq = last_seq + 1
		 WHERE namespace = $1 AND workspace_id = $2 RETURNING last_seq`,
		t.ns, t.ws,
	).Scan(&seq)
	return uint64(seq), err
}

func (t *pgTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO rows (namespace, workspace_id, entity, key, value, version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8, $9)
		 ON CONFLICT (namespace, workspace_id, entity, key)
		 DO UPDATE SET value = excluded.value, version = excluded.version, deleted = FALSE,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ns, t.ws, entity, key, value, int64(version), time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
	return err
}

func (t *pgTx) DeleteRow(entity, key string, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO rows (namespace, workspace_id, entity, key, value, version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES ($1, $2, $3, $4, NULL, $5, TRUE, $6, $7, $8)
		 ON CONFLICT (namespace, workspace_id, entity, key)
		 DO UPDATE SET value = NULL, version = excluded.version, deleted = TRUE,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ns, t.ws, entity, key, int64(version), time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
	return err
}

func (t *pgTx) SetClientLastMutationID(clientID string, mutationID uint64) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO clients (namespace, workspace_id, client_id, last_mutation_id, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (namespace, workspace_id, client_id)
		 DO UPDATE SET last_mutation_id = excluded.last_mutation_id, updated_at = excluded.updated_at`,
		t.ns, t.ws, clientID, int64(mutationID), time.Now().UnixMilli(),
	)
	return err
}

func (t *pgTx) SetClientRejections(clientID string, recs []storage.ClientRejection) error {
	if err := t.register(); err != nil {
		return err
	}
	var blob []byte
	if len(recs) > 0 {
		var err error
		if blob, err = json.Marshal(recs); err != nil {
			return fmt.Errorf("pgstore: encode rejections for client %q: %w", clientID, err)
		}
	}
	_, err := t.tx.Exec(t.ctx,
		`INSERT INTO clients (namespace, workspace_id, client_id, rejections, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (namespace, workspace_id, client_id)
		 DO UPDATE SET rejections = excluded.rejections, updated_at = excluded.updated_at`,
		t.ns, t.ws, clientID, blob, time.Now().UnixMilli(),
	)
	return err
}

// InSavepoint runs fn inside a nested savepoint.
//
// It carries more weight here than under SQLite. Any failed statement puts a
// Postgres transaction into an aborted state where every later statement fails
// with 25P02 until it rewinds to a savepoint — so ROLLBACK TO is not merely how
// the engine undoes a rejected mutation's writes, it is the only way a
// transaction continues past a storage error at all. A caller that swallows an
// error and keeps writing outside a savepoint gets a loud, immediate failure
// rather than a silent one, which is the safe direction for the difference to
// fall.
func (t *pgTx) InSavepoint(fn func() error) error {
	t.spDepth++
	name := fmt.Sprintf("sp%d", t.spDepth)
	defer func() { t.spDepth-- }()
	if _, err := t.tx.Exec(t.ctx, `SAVEPOINT `+name); err != nil {
		return err
	}
	if err := fn(); err != nil {
		// The rewind may undo register's INSERT (a rejected mutation that was
		// this transaction's only write), so re-run it on the next write. When
		// the INSERT predates the savepoint the extra statement is a harmless
		// ON CONFLICT DO NOTHING.
		t.registered = false
		// ROLLBACK TO rewinds the writes but keeps the savepoint open;
		// RELEASE discards it.
		if _, rbErr := t.tx.Exec(t.ctx, `ROLLBACK TO `+name); rbErr != nil {
			return errors.Join(err, fmt.Errorf("pgstore: rollback savepoint: %w", rbErr))
		}
		if _, rbErr := t.tx.Exec(t.ctx, `RELEASE `+name); rbErr != nil {
			return errors.Join(err, fmt.Errorf("pgstore: release savepoint: %w", rbErr))
		}
		return err
	}
	if _, err := t.tx.Exec(t.ctx, `RELEASE `+name); err != nil {
		return fmt.Errorf("pgstore: release savepoint: %w", err)
	}
	return nil
}

func (t *pgTx) CollectTombstones(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxDeleted int64
	if err := t.tx.QueryRow(t.ctx,
		`SELECT COALESCE(MAX(version), 0) FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND deleted AND updated_at < $3`,
		t.ns, t.ws, cutoffMillis,
	).Scan(&maxDeleted); err != nil {
		return 0, err
	}
	if maxDeleted > 0 {
		if _, err := t.tx.Exec(t.ctx,
			`DELETE FROM rows
			 WHERE namespace = $1 AND workspace_id = $2 AND deleted AND updated_at < $3`,
			t.ns, t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// GREATEST keeps the horizon monotonic even if an older cutoff runs
		// after a newer one.
		if _, err := t.tx.Exec(t.ctx,
			`UPDATE workspaces SET tombstone_horizon = GREATEST(tombstone_horizon, $3)
			 WHERE namespace = $1 AND workspace_id = $2`,
			t.ns, t.ws, maxDeleted,
		); err != nil {
			return 0, err
		}
	}
	return t.TombstoneHorizon()
}

// PurgeEntityRows hard-deletes the selected rows of one entity and the index
// entries they occupied. Entries go first so no statement can observe an entry
// whose row this call has already removed; both are in the caller's
// transaction, so a rollback keeps them together either way.
//
// The prefix becomes a half-open key RANGE rather than a LIKE: every text column
// here is COLLATE "C" (see the package comment), so a range is the same bytewise
// comparison every other key predicate makes — and, unlike LIKE, it can use the
// primary-key index and needs no escaping of wildcards.
//
// A limit turns both statements into the same `key IN (SELECT … ORDER BY key
// LIMIT n)` subquery — Postgres has no DELETE … LIMIT, and the subquery is what
// the row delete needs anyway. The ORDER BY is load-bearing rather than cosmetic:
// the two subqueries run against the same rows (the entries delete does not touch
// them) and must select the SAME set, or entries outlive their row or die before
// it. A total order over a unique key is what makes them agree.
//
// more is "this call hit its limit", which the interface allows: the drain that
// exactly emptied the selection costs one more no-op call, which is cheaper than
// probing for a next row on every chunk.
func (t *pgTx) PurgeEntityRows(entity, keyPrefix string, before time.Time, limit int) (int, bool, error) {
	where, args := purgeWhere(t.ns, t.ws, entity, keyPrefix, before)
	selected, rowsWhere := where, where
	if limit > 0 {
		args = append(args, limit)
		selected = fmt.Sprintf("%s ORDER BY key LIMIT $%d", where, len(args))
		// The outer predicate reuses $1..$3, which purgeWhere always numbers as
		// namespace, workspace and entity.
		rowsWhere = `namespace = $1 AND workspace_id = $2 AND entity = $3 AND key IN (
		   SELECT key FROM rows WHERE ` + selected + `)`
	}
	if _, err := t.tx.Exec(t.ctx,
		`DELETE FROM index_entries
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key IN (
		   SELECT key FROM rows WHERE `+selected+`
		 )`,
		args...,
	); err != nil {
		return 0, false, err
	}
	tag, err := t.tx.Exec(t.ctx, `DELETE FROM rows WHERE `+rowsWhere, args...)
	if err != nil {
		return 0, false, err
	}
	n := int(tag.RowsAffected())
	return n, limit > 0 && n == limit, nil
}

// purgeWhere builds PurgeEntityRows' row predicate and its arguments. The first
// three placeholders are shared with the enclosing index_entries statement, so
// the numbering starts where that one's does.
//
// Both narrowings are optional: an empty prefix bounds nothing, and a zero
// `before` means no age bound at all — which is NOT what its UnixMilli() would
// say, so the branch is load-bearing rather than an optimization.
func purgeWhere(ns, ws, entity, keyPrefix string, before time.Time) (string, []any) {
	where := "namespace = $1 AND workspace_id = $2 AND entity = $3"
	args := []any{ns, ws, entity}
	next := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if keyPrefix != "" {
		where += " AND key >= " + next(keyPrefix)
		// Both halves are needed: ">= prefix" alone also matches every key that
		// merely sorts after it. The upper bound is missing only for a prefix of
		// nothing but 0xff bytes, which no UTF-8 key can contain — so in
		// practice this branch always runs.
		if hi, bounded := prefixUpperBound(keyPrefix); bounded {
			where += " AND key < " + next(hi)
		}
	}
	if !before.IsZero() {
		where += " AND updated_at < " + next(before.UnixMilli())
	}
	return where, args
}

// prefixUpperBound returns the least string greater than every string starting
// with prefix, and whether one exists. Increment the last byte that can be
// incremented and drop everything after it; a prefix of nothing but 0xff bytes
// has no upper bound.
func prefixUpperBound(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

func (t *pgTx) CollectIdleClients(cutoff time.Time) (int, error) {
	tag, err := t.tx.Exec(t.ctx,
		`DELETE FROM clients WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3`,
		t.ns, t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// notNilBytes maps a nil slice onto the empty blob a NOT NULL bytea column
// stores. pgx encodes a nil []byte as SQL NULL, which those columns reject —
// and the distinction the engine cares about is empty versus absent, which the
// row's presence already carries.
func notNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
