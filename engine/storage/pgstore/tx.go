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
	// spPending names the savepoint whose InSavepoint has returned but which is
	// still established in Postgres, or "" for none. See InSavepoint.
	spPending string
	// cursorSeq names scan cursors uniquely within the transaction.
	cursorSeq int
	// registered memoizes register's INSERT so a write burst does not repeat
	// it. Restored on savepoint rollback, which may have undone that INSERT.
	registered bool
	// seq is the highest workspace sequence NextSeq has handed out, and
	// seqPersisted the value the workspaces row holds. They diverge between an
	// in-memory allocation and the write-back flush makes before COMMIT.
	// seqLoaded says whether either means anything yet.
	seq          uint64
	seqPersisted uint64
	seqLoaded    bool
	// ws* memoize the workspaces row's three position scalars, which
	// loadWorkspace reads together. wsLoaded false means "not read yet, or
	// invalidated by a write of this transaction's own".
	wsLoaded  bool
	wsSeq     uint64
	wsHorizon uint64
	wsEpoch   uint64
	// queued holds the write-only statements this transaction has not sent yet,
	// in the order they were written; sends counts the batches that have gone
	// out, and failed holds the first error one of them returned. See queue.go.
	// A read transaction never fills any of them.
	queued []queuedStmt
	sends  int
	failed error
}

// loadWorkspace reads last_seq, tombstone_horizon and store_epoch in one
// statement and keeps them for the rest of the transaction. Every pull reads
// all three, so the memo turns three round trips into one.
//
// Holding them is exact rather than approximate. A read transaction is
// REPEATABLE READ, so its snapshot cannot change under it; a write transaction
// holds the workspace advisory lock, so nothing else writes the row while it
// runs, and the three places where the transaction moves the row itself —
// register minting the epoch, NextSeq claiming a sequence, CollectTombstones
// raising the horizon — drop the memo or answer from their own state.
//
// A workspace with no row yet reads as all zeroes, which is the value each
// scalar has on the row register would create for it.
func (t *pgTx) loadWorkspace() error {
	if t.wsLoaded {
		return nil
	}
	// store_epoch is stored as a signed BIGINT because Postgres has no unsigned
	// one; reinterpret the bits. The value is opaque — only equality matters.
	var seq, horizon, epoch int64
	err := t.queryRow(
		`SELECT last_seq, tombstone_horizon, store_epoch FROM workspaces
		 WHERE namespace = $1 AND workspace_id = $2`, t.ns, t.ws,
	).Scan(&seq, &horizon, &epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		seq, horizon, epoch = 0, 0, 0
	case err != nil:
		return err
	}
	t.wsSeq, t.wsHorizon, t.wsEpoch = uint64(seq), uint64(horizon), uint64(epoch)
	t.wsLoaded = true
	return nil
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
	if err := t.queue(
		`INSERT INTO workspaces (namespace, workspace_id, store_epoch) VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING`,
		t.ns, t.ws, epoch,
	); err != nil {
		return err
	}
	// The row may have just been minted, and with it the store epoch a reader
	// earlier in this transaction saw as absent.
	t.wsLoaded = false
	t.registered = true
	return nil
}

func (t *pgTx) CurrentSeq() (uint64, error) {
	if t.seqLoaded {
		// Sequences NextSeq served from memory count as allocated: a caller
		// inside this transaction must see what it just took, whether or not
		// the write-back has happened yet. This also outranks the memo, whose
		// last_seq predates the allocation whenever it was read first.
		return t.seq, nil
	}
	if err := t.loadWorkspace(); err != nil {
		return 0, err
	}
	return t.wsSeq, nil
}

func (t *pgTx) StoreEpoch() (uint64, error) {
	if err := t.loadWorkspace(); err != nil {
		return 0, err
	}
	return t.wsEpoch, nil
}

func (t *pgTx) TombstoneHorizon() (uint64, error) {
	if err := t.loadWorkspace(); err != nil {
		return 0, err
	}
	return t.wsHorizon, nil
}

func (t *pgTx) GetRow(entity, key string) (storage.Row, error) {
	row := storage.Row{Entity: entity, Key: key}
	var version, created int64
	err := t.queryRow(
		`SELECT value, version, created_version, deleted, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4`,
		t.ns, t.ws, entity, key,
	).Scan(&row.Value, &version, &created, &row.Deleted,
		&row.Actor.Subject, &row.Actor.System)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.Row{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.Row{}, err
	}
	row.Version = uint64(version)
	row.CreatedVersion = uint64(created)
	return row, nil
}

// GetRows answers the whole batch with one statement. This is the method the
// adapter exists to make cheap: the engine resolves an index walk's candidates
// through it, and a point read per candidate would be a round trip per
// candidate on a walk that is otherwise a single range read.
//
// The result set is drained and closed before the first callback runs, for the
// reason scanCursor buffers — fn may read this transaction, and pgx cannot run
// a second query on a connection whose result set is still open.
func (t *pgTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	if len(keys) == 0 {
		return nil
	}
	byKey, err := t.readRowsByKey(entity, keys)
	if err != nil {
		return err
	}
	// Yield in the caller's order, not the one Postgres returned: keys carries
	// the index walk's order, which is the row order the caller has to produce.
	for _, key := range keys {
		row, ok := byKey[key]
		if !ok {
			continue // no row: the batch form of ErrNotFound
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) readRowsByKey(entity string, keys []string) (map[string]storage.Row, error) {
	rows, err := t.query(
		`SELECT key, value, version, created_version, deleted, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = ANY($4)`,
		t.ns, t.ws, entity, keys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byKey := make(map[string]storage.Row, len(keys))
	for rows.Next() {
		r := storage.Row{Entity: entity}
		var version, created int64
		if err := rows.Scan(&r.Key, &r.Value, &version, &created, &r.Deleted,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return nil, err
		}
		r.Version = uint64(version)
		r.CreatedVersion = uint64(created)
		byKey[r.Key] = r
	}
	return byKey, rows.Err()
}

// decodeEntityRow reads the column set the two key-ordered entity scans select.
// It runs with the result set open, so it only Scans (see scanCursor).
func decodeEntityRow(entity string) func(pgx.Rows) (storage.Row, error) {
	return func(rows pgx.Rows) (storage.Row, error) {
		r := storage.Row{Entity: entity}
		var version, created int64
		if err := rows.Scan(&r.Key, &r.Value, &version, &created,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return storage.Row{}, err
		}
		r.Version = uint64(version)
		r.CreatedVersion = uint64(created)
		return r, nil
	}
}

func (t *pgTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	return t.ScanEntityAfter(entity, "", 0, fn)
}

func (t *pgTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	return scanRows(t,
		`SELECT key, value, version, created_version, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND NOT deleted AND key > $4
		 ORDER BY key
		 LIMIT $5`,
		[]any{t.ns, t.ws, entity, afterKey, limitArg(limit)}, limit,
		decodeEntityRow(entity), fn)
}

func (t *pgTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	// ORDER BY matches rows_by_entity_version exactly (version DESC, key ASC,
	// partial on NOT deleted), so this reads the index prefix and stops at the
	// limit instead of sorting the entity.
	return scanRows(t,
		`SELECT key, value, version, created_version, actor_subject, actor_system
		 FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND NOT deleted
		 ORDER BY version DESC, key
		 LIMIT $4`,
		[]any{t.ns, t.ws, entity, limitArg(limit)}, limit,
		decodeEntityRow(entity), fn)
}

func (t *pgTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	decode := func(rows pgx.Rows) (storage.Row, error) {
		var r storage.Row
		var version, created int64
		if err := rows.Scan(&r.Entity, &r.Key, &r.Value, &version, &created, &r.Deleted,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return storage.Row{}, err
		}
		r.Version = uint64(version)
		r.CreatedVersion = uint64(created)
		if r.Deleted {
			r.Value = nil
		}
		return r, nil
	}
	const cols = `SELECT entity, key, value, version, created_version, deleted,
	                     actor_subject, actor_system
	              FROM rows `

	if afterEntity == "" {
		// A clean version boundary: everything strictly past afterSeq. This
		// cannot be folded into the row comparison below — every row carries a
		// non-empty entity, so (version, entity, key) > (afterSeq, '', '') would
		// also take the rows AT afterSeq.
		return scanRows(t,
			cols+`WHERE namespace = $1 AND workspace_id = $2 AND version > $3
			      ORDER BY version, entity, key
			      LIMIT $4`,
			[]any{t.ns, t.ws, int64(afterSeq), limitArg(limit)}, limit,
			decode, fn)
	}
	// Resume inside a same-version group. The row-value comparison is the whole
	// tie-break in one predicate — Postgres compares it lexicographically,
	// bytewise for entity and key because both columns are COLLATE "C" — where
	// SQLite needs the disjunction spelled out.
	return scanRows(t,
		cols+`WHERE namespace = $1 AND workspace_id = $2
		        AND (version, entity, key) > ($3::bigint, $4::text, $5::text)
		      ORDER BY version, entity, key
		      LIMIT $6`,
		[]any{t.ns, t.ws, int64(afterSeq), afterEntity, afterKey, limitArg(limit)}, limit,
		decode, fn)
}

func (t *pgTx) ClientLastMutationID(clientID string) (uint64, error) {
	var id int64
	err := t.queryRow(
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
	err := t.queryRow(
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
	err := t.queryRow(
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
	return t.queue(
		`INSERT INTO idempotency (namespace, workspace_id, key, seq, result, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, key)
		 DO UPDATE SET seq = excluded.seq, result = excluded.result, updated_at = excluded.updated_at`,
		t.ns, t.ws, key, int64(rec.Seq), notNilBytes(rec.Result), time.Now().UnixMilli(),
	)
}

func (t *pgTx) CollectIdempotencyKeys(cutoff time.Time) (int, error) {
	tag, err := t.exec(
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
	err := t.queryRow(
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
	rows, err := t.query(
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
	return t.queue(
		`INSERT INTO consumer_positions (namespace, workspace_id, consumer, seq, entity, key, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (namespace, workspace_id, consumer)
		 DO UPDATE SET seq = excluded.seq, entity = excluded.entity,
		               key = excluded.key, updated_at = excluded.updated_at`,
		t.ns, t.ws, consumer, int64(p.Seq), p.Entity, p.Key, time.Now().UnixMilli(),
	)
}

func (t *pgTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	var snapshot []byte
	err := t.queryRow(
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
	return t.queue(
		`INSERT INTO consumer_shadows (namespace, workspace_id, consumer, entity, key, snapshot)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, consumer, entity, key)
		 DO UPDATE SET snapshot = excluded.snapshot`,
		t.ns, t.ws, consumer, entity, key, notNilBytes(snapshot),
	)
}

func (t *pgTx) DeleteConsumerShadow(consumer, entity, key string) error {
	return t.queue(
		`DELETE FROM consumer_shadows
		 WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3 AND entity = $4 AND key = $5`,
		t.ns, t.ws, consumer, entity, key,
	)
}

func (t *pgTx) DeleteConsumer(consumer string) error {
	if err := t.queue(
		`DELETE FROM consumer_shadows WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3`,
		t.ns, t.ws, consumer,
	); err != nil {
		return err
	}
	return t.queue(
		`DELETE FROM consumer_positions WHERE namespace = $1 AND workspace_id = $2 AND consumer = $3`,
		t.ns, t.ws, consumer,
	)
}

func (t *pgTx) CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (int, error) {
	// Shadows first, while the positions that name their consumers still exist:
	// the reverse order would leave every collected consumer's shadows behind
	// with nothing left to identify them by.
	if _, err := t.exec(
		`DELETE FROM consumer_shadows
		 WHERE namespace = $1 AND workspace_id = $2 AND consumer IN (
		     SELECT consumer FROM consumer_positions
		     WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3 AND seq < $4
		 )`,
		t.ns, t.ws, cutoff.UnixMilli(), int64(belowSeq),
	); err != nil {
		return 0, err
	}
	tag, err := t.exec(
		`DELETE FROM consumer_positions
		 WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3 AND seq < $4`,
		t.ns, t.ws, cutoff.UnixMilli(), int64(belowSeq),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) FieldDeltaHorizon() (uint64, error) {
	var horizon int64
	err := t.queryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws,
	).Scan(&horizon)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(horizon), nil
}

func (t *pgTx) FieldDeltas(entity, key, field string, afterSeq uint64) ([][]byte, bool, error) {
	horizon, err := t.FieldDeltaHorizon()
	if err != nil {
		return nil, false, err
	}
	// The span starts where the caller's knowledge ends; anything pruned at or
	// above that point is a hole the fold cannot see.
	if afterSeq < uint64(horizon) {
		return nil, false, nil
	}
	rows, err := t.query(
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
	return t.queue(
		`INSERT INTO field_deltas (namespace, workspace_id, entity, key, field, seq, frag, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (namespace, workspace_id, entity, key, field, seq)
		 DO UPDATE SET frag = excluded.frag, updated_at = excluded.updated_at`,
		t.ns, t.ws, entity, key, field, int64(seq), notNilBytes(frag), time.Now().UnixMilli(),
	)
}

func (t *pgTx) PruneFieldDeltas(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxPruned int64
	if err := t.queryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM field_deltas
		 WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3`,
		t.ns, t.ws, cutoffMillis,
	).Scan(&maxPruned); err != nil {
		return 0, err
	}
	if maxPruned > 0 {
		if _, err := t.exec(
			`DELETE FROM field_deltas WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3`,
			t.ns, t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// GREATEST keeps the horizon monotonic even if an older cutoff runs
		// after a newer one, exactly as CollectTombstones does.
		if _, err := t.exec(
			`UPDATE workspaces SET field_delta_horizon = GREATEST(field_delta_horizon, $3)
			 WHERE namespace = $1 AND workspace_id = $2`,
			t.ns, t.ws, maxPruned,
		); err != nil {
			return 0, err
		}
	}
	var horizon int64
	err := t.queryRow(
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
	err := t.queryRow(
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
		return t.queue(
			`DELETE FROM text_floors
			 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4 AND field = $5`,
			t.ns, t.ws, entity, key, field,
		)
	}
	return t.queue(
		`INSERT INTO text_floors (namespace, workspace_id, entity, key, field, vv)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, entity, key, field)
		 DO UPDATE SET vv = excluded.vv`,
		t.ns, t.ws, entity, key, field, vv,
	)
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
	return scanRows(t, query, args, limit,
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
	err := t.queryRow(
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

// SetIndexBuildMark updates the workspace row and never calls register: the
// mark says "this workspace's indexes are built", which is vacuously true of a
// workspace that holds nothing, and registering one here would let a
// BuildIndexes call on an arbitrary id mint a workspace every later pass
// iterates.
func (t *pgTx) SetIndexBuildMark(mark string) error {
	return t.queue(
		`UPDATE workspaces SET index_build_mark = $3 WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws, mark,
	)
}

// SetTextCompactMark updates the workspace row and never calls register, for
// SetIndexBuildMark's reason: a workspace holding no rows has no text to
// compact, and a sweep must not mint one.
func (t *pgTx) SetTextCompactMark(mark storage.TextCompactMark) error {
	return t.queue(
		`UPDATE workspaces
		    SET text_compact_seq = $3, text_compact_entity = $4, text_compact_key = $5,
		        text_compact_min_purged = $6
		  WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws, int64(mark.Seq), mark.Entity, mark.Key, int32(mark.MinPurged),
	)
}

func (t *pgTx) TextCompactMark() (storage.TextCompactMark, error) {
	var m storage.TextCompactMark
	var seq int64
	var minPurged int32
	err := t.queryRow(
		`SELECT text_compact_seq, text_compact_entity, text_compact_key, text_compact_min_purged
		   FROM workspaces WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws,
	).Scan(&seq, &m.Entity, &m.Key, &minPurged)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.TextCompactMark{}, nil
	}
	if err != nil {
		return storage.TextCompactMark{}, err
	}
	m.Seq, m.MinPurged = uint64(seq), int(minPurged)
	return m, nil
}

func (t *pgTx) SetIndexState(entity, index, specHash string, ready bool) error {
	if err := t.register(); err != nil {
		return err
	}
	return t.queue(
		`INSERT INTO index_state (namespace, workspace_id, entity, idx, spec_hash, ready)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, workspace_id, entity, idx)
		 DO UPDATE SET spec_hash = excluded.spec_hash, ready = excluded.ready`,
		t.ns, t.ws, entity, index, specHash, ready,
	)
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
	// Replace-set by DIFFERENCE, not by clear-and-reinsert. A row carries an
	// entry per index and per element of every indexed list, while the PATCH
	// that triggered the write usually moves one or two of them; re-inserting
	// the unchanged rest leaves a dead tuple per entry for vacuum and dirties
	// a page per btree the entry sits in. The new set arrives as two parallel
	// arrays so the whole replacement stays one statement whatever the fan-out.
	idxs := make([]string, len(entries))
	ks := make([][]byte, len(entries))
	for i, e := range entries {
		idxs[i], ks[i] = e.Index, notNilBytes(e.Key)
	}
	// Both directions ride in one data-modifying CTE, which is one round trip
	// where two statements were two. The DELETE runs whether or not the main
	// query reads it, and neither half sees the other's effect on the table —
	// which is safe here because the DELETE takes only entries OUTSIDE the new
	// set and the INSERT adds only entries inside it, so the rows they touch
	// are disjoint by construction.
	//
	// DO NOTHING rather than failing: it absorbs both the entries that are
	// already stored and a caller that passed the same entry twice — two
	// components of one index can encode to the same entry for one row only
	// that way, and failing the whole write on a harmless caller bug would
	// stick the write path. There are no non-key columns, so there is nothing
	// an update would change.
	return t.queue(
		`WITH want AS (SELECT u.idx, u.k FROM unnest($5::text[], $6::bytea[]) AS u(idx, k)),
		      stale AS (
		          DELETE FROM index_entries
		          WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4
		            AND (idx, k) NOT IN (SELECT w.idx, w.k FROM want w)
		      )
		 INSERT INTO index_entries (namespace, workspace_id, entity, idx, k, key)
		 SELECT $1::text, $2::text, $3::text, w.idx, w.k, $4::text FROM want w
		 ON CONFLICT DO NOTHING`,
		t.ns, t.ws, entity, key, idxs, ks,
	)
}

func (t *pgTx) DeleteIndexEntries(entity, key string) error {
	// No register(): removing entries a row never had must leave no trace, the
	// same reason a write closure that writes nothing does not create a
	// workspace.
	return t.queue(
		`DELETE FROM index_entries
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key = $4`,
		t.ns, t.ws, entity, key,
	)
}

// NextSeq claims the first sequence with a statement and every later one from
// memory, so a push of N mutations costs one allocation statement rather than
// N. The workspace advisory lock has already made this transaction the only
// writer of the workspaces row, so nothing else can allocate against it in
// between and the row only has to hold the final value: flush writes it back
// before COMMIT, which is what keeps the invariant that a committed transaction
// persists every sequence it allocated.
func (t *pgTx) NextSeq() (uint64, error) {
	if err := t.register(); err != nil {
		return 0, err
	}
	if t.seqLoaded {
		t.seq++
		return t.seq, nil
	}
	// The first call reads and claims in one statement, so a transaction that
	// allocates exactly one sequence costs exactly one statement.
	var seq int64
	if err := t.queryRow(
		`UPDATE workspaces SET last_seq = last_seq + 1
		 WHERE namespace = $1 AND workspace_id = $2 RETURNING last_seq`,
		t.ns, t.ws,
	).Scan(&seq); err != nil {
		return 0, err
	}
	t.seq, t.seqPersisted, t.seqLoaded = uint64(seq), uint64(seq), true
	return t.seq, nil
}

// flush writes back what NextSeq allocated in memory. The store calls it after
// the write closure returns successfully and before COMMIT; it is a no-op for a
// transaction that allocated at most one sequence.
func (t *pgTx) flush() error {
	if !t.seqLoaded || t.seq == t.seqPersisted {
		return nil
	}
	if err := t.queue(
		`UPDATE workspaces SET last_seq = $3 WHERE namespace = $1 AND workspace_id = $2`,
		t.ns, t.ws, int64(t.seq),
	); err != nil {
		return err
	}
	t.seqPersisted = t.seq
	return nil
}

func (t *pgTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	// created_version stays out of the conflict SET: the stored value is the
	// key's original creation, which every overwrite keeps (see storage.Row).
	return t.queue(
		`INSERT INTO rows (namespace, workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, $8, $9, $10)
		 ON CONFLICT (namespace, workspace_id, entity, key)
		 DO UPDATE SET value = excluded.value, version = excluded.version, deleted = FALSE,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ns, t.ws, entity, key, value, int64(version), int64(version), time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
}

func (t *pgTx) DeleteRow(entity, key string, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	return t.queue(
		`INSERT INTO rows (namespace, workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES ($1, $2, $3, $4, NULL, $5, $6, TRUE, $7, $8, $9)
		 ON CONFLICT (namespace, workspace_id, entity, key)
		 DO UPDATE SET value = NULL, version = excluded.version, deleted = TRUE,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ns, t.ws, entity, key, int64(version), int64(version), time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
}

func (t *pgTx) SetClientLastMutationID(clientID string, mutationID uint64) error {
	if err := t.register(); err != nil {
		return err
	}
	return t.queue(
		`INSERT INTO clients (namespace, workspace_id, client_id, last_mutation_id, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (namespace, workspace_id, client_id)
		 DO UPDATE SET last_mutation_id = excluded.last_mutation_id, updated_at = excluded.updated_at`,
		t.ns, t.ws, clientID, int64(mutationID), time.Now().UnixMilli(),
	)
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
	return t.queue(
		`INSERT INTO clients (namespace, workspace_id, client_id, rejections, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (namespace, workspace_id, client_id)
		 DO UPDATE SET rejections = excluded.rejections, updated_at = excluded.updated_at`,
		t.ns, t.ws, clientID, blob, time.Now().UnixMilli(),
	)
}

// InSavepoint runs fn inside a nested savepoint.
//
// The rewind covers what fn wrote, not what the server refused. A statement
// Postgres rejects aborts the whole transaction — every later statement fails
// with 25P02 — and no savepoint here recovers it: writes travel in batches that
// may span savepoints already released, so there is no single point to rewind
// to. Such an error fails the transaction, loudly and at once, which is the
// safe direction for the difference to fall. It costs nothing the engine uses:
// the one place it continues past an error inside a write transaction is a
// rejected mutation, which is a verdict on the mutation rather than a statement
// the server refused.
//
// # Deferred release
//
// A finished savepoint is left established rather than released on the spot:
// the next call at the same depth releases it and opens the replacement, and
// COMMIT releases the last one. That is what makes a push of N mutations cost N
// savepoint statements rather than 2N. It holds at most one deferred savepoint
// per nesting level, which keeps a long batch far below the ~64 concurrent
// subtransactions past which Postgres spills the subxact cache and slows every
// reader of the rows the transaction touches.
//
// Statements issued between a deferred savepoint and its release run inside
// that subtransaction. They still survive it, because the release always
// precedes the ROLLBACK TO of the savepoint that replaces it.
//
// # Rewinding the queue instead of the server
//
// Both the savepoint statements and the writes they bracket go through the
// write-behind queue (see queue.go), which gives the common rewind a cheaper
// form than ROLLBACK TO: if nothing has been sent since this savepoint was
// queued, the server has never seen any of it, and dropping the queue back to
// where the savepoint starts is the whole undo. A push that rejects a mutation
// pays nothing for it. Once something HAS been sent the savepoint exists on the
// server, so the queued remainder is dropped and a ROLLBACK TO is queued in its
// place — which lands before anything written afterwards, in order.
func (t *pgTx) InSavepoint(fn func() error) error {
	t.spDepth++
	name := fmt.Sprintf("sp%d", t.spDepth)
	defer func() { t.spDepth-- }()

	// Where this savepoint's entries begin, and how many batches had gone out
	// when it opened: together they say whether the rewind below can be the
	// queue's own.
	mark, sends, pending := len(t.queued), t.sends, t.spPending

	// Two entries rather than one `RELEASE spN; SAVEPOINT spN` string: a batch
	// speaks the extended query protocol, which carries one statement per
	// message. Queued back to back they still cost no extra round trip.
	if pending == name {
		if err := t.queue(`RELEASE ` + name); err != nil {
			return err
		}
	}
	if err := t.queue(`SAVEPOINT ` + name); err != nil {
		return err
	}
	// Any deeper deferred savepoint was released along with this name (RELEASE
	// takes everything established above it), and the new one is active rather
	// than deferred.
	t.spPending = ""

	// NextSeq allocates from memory, so the rewind below has to undo the
	// allocation the way ROLLBACK TO undoes the statements — a rejected
	// mutation must leave the next one the sequence it took, or the log would
	// gap. registered rides along for the same reason: the rewind may undo
	// register's INSERT.
	seq, seqPersisted, seqLoaded, registered := t.seq, t.seqPersisted, t.seqLoaded, t.registered

	if err := fn(); err != nil {
		t.seq, t.seqPersisted, t.seqLoaded, t.registered = seq, seqPersisted, seqLoaded, registered
		// The rewind may undo a register or a CollectTombstones, so the memo is
		// dropped rather than restored: what fn saw is not what the row holds now,
		// and what preceded fn may not be either.
		t.wsLoaded = false
		switch {
		case t.failed != nil:
			// A queued statement has already failed, which aborts the whole
			// transaction in Postgres. No savepoint rewinds that — the batch it
			// failed in may have carried statements from savepoints since
			// released — so the transaction stays dead and WithWriteTx rolls it
			// back. err is the failure itself wherever fn observed it.
			return err
		case t.sends == sends:
			// Nothing has left this process since the SAVEPOINT was queued, so
			// every statement fn wrote is still in the queue above mark, and
			// truncating is an exact undo. The RELEASE of the savepoint this one
			// replaced goes with it, leaving that one deferred exactly as it was.
			t.queued = t.queued[:mark]
			t.spPending = pending
			return err
		default:
			// A send empties the queue, so everything still in it was queued by
			// fn and must be dropped rather than sent: ROLLBACK TO would only
			// undo it again. The savepoint itself went out with that send.
			t.queued = t.queued[:0]
			// ROLLBACK TO rewinds the writes but leaves the savepoint
			// established, which is exactly the deferred state — so the rewind
			// costs one statement and the release rides on whatever comes next.
			if rbErr := t.queue(`ROLLBACK TO ` + name); rbErr != nil {
				return errors.Join(err, fmt.Errorf("pgstore: rollback savepoint: %w", rbErr))
			}
			t.spPending = name
			return err
		}
	}
	t.spPending = name
	return nil
}

func (t *pgTx) CollectTombstones(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxDeleted int64
	if err := t.queryRow(
		`SELECT COALESCE(MAX(version), 0) FROM rows
		 WHERE namespace = $1 AND workspace_id = $2 AND deleted AND updated_at < $3`,
		t.ns, t.ws, cutoffMillis,
	).Scan(&maxDeleted); err != nil {
		return 0, err
	}
	if maxDeleted > 0 {
		if _, err := t.exec(
			`DELETE FROM rows
			 WHERE namespace = $1 AND workspace_id = $2 AND deleted AND updated_at < $3`,
			t.ns, t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// GREATEST keeps the horizon monotonic even if an older cutoff runs
		// after a newer one.
		if _, err := t.exec(
			`UPDATE workspaces SET tombstone_horizon = GREATEST(tombstone_horizon, $3)
			 WHERE namespace = $1 AND workspace_id = $2`,
			t.ns, t.ws, maxDeleted,
		); err != nil {
			return 0, err
		}
		t.wsLoaded = false
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
	if _, err := t.exec(
		`DELETE FROM index_entries
		 WHERE namespace = $1 AND workspace_id = $2 AND entity = $3 AND key IN (
		   SELECT key FROM rows WHERE `+selected+`
		 )`,
		args...,
	); err != nil {
		return 0, false, err
	}
	tag, err := t.exec(`DELETE FROM rows WHERE `+rowsWhere, args...)
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

func (t *pgTx) ExpireClientRejections(cutoff time.Time) (int, error) {
	// updated_at is deliberately left alone: it is what CollectIdleClients
	// measures idleness by, and expiring verdicts is not client activity.
	tag, err := t.exec(
		`UPDATE clients SET rejections = NULL
		 WHERE namespace = $1 AND workspace_id = $2 AND updated_at < $3 AND rejections IS NOT NULL`,
		t.ns, t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) CollectIdleClients(cutoff time.Time) (int, error) {
	tag, err := t.exec(
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
