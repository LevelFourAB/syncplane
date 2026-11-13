package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/storage"
)

// pgTx implements storage.WriteTx (and, narrowed by readOnlyTx, storage.ReadTx)
// over one transaction on a connection it holds for its whole life.
//
// ctx is held on the struct because storage.ReadTx's methods take none: the
// engine scopes cancellation at WithReadTx/WithWriteTx, so every statement this
// transaction issues belongs to that one context and there is no second
// lifetime for a field to confuse it with.
type pgTx struct {
	conn *pgxpool.Conn
	ctx  context.Context
	ns   string
	ws   string
	// opening counts the statements beginTx queued to open the transaction.
	opening int
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
	// seqStart is the first sequence this transaction allocated, meaningful
	// while seqLoaded, and mark the lineage mark its sequences carry once it
	// commits (see storage.ReadTx.SeqLineage). flush records the pair.
	seqStart uint64
	mark     uint64
	// ws* memoize the workspaces row's three position scalars, which
	// loadWorkspace reads together, and the newest committed lineage record
	// (wsLineageStart 0 for none) read in the same statement. wsLoaded false
	// means "not read yet, or invalidated by a write of this transaction's
	// own".
	wsLoaded       bool
	wsSeq          uint64
	wsHorizon      uint64
	wsEpoch        uint64
	wsLineageStart uint64
	wsLineageMark  uint64
	// indexStates memoizes every index_state row of the workspace (see
	// IndexState). nil means "not read yet, or dropped by a savepoint rewind".
	indexStates map[indexKey]indexState
	// queued holds the write-only statements this transaction has not sent yet,
	// in the order they were written; sends counts the batches that have gone
	// out, and failed holds the first error one of them returned. See queue.go.
	// A read transaction uses them only to open a scan cursor.
	queued []queuedStmt
	sends  int
	failed error
	// postingLog holds the PutPostings and DeletePostings calls of the
	// transaction in order, and postingDone counts the calls at its start
	// that resolvePostings has written. See postings.go.
	postingLog  []postingOp
	postingDone int
	// scopes is the store's cache of posting scope ids, nil for none.
	scopes *scopeCache
	// partitioned reports that the database has the partitioned layout.
	partitioned bool
	// entryLog holds PutIndexEntries and DeleteIndexEntries calls of the
	// transaction in order: the calls that are not written, and before them
	// the written calls that the rewind of an open savepoint can make pending
	// again. entryDone counts the written calls, and entryPending the calls
	// after them and their entries. See index.go.
	entryLog     []entryOp
	entryDone    int
	entryPending int
	// indexScopes memoizes every index_scopes row of the workspace (see
	// loadIndexScopes). nil means "not read yet, or dropped by a savepoint
	// rewind".
	indexScopes map[indexKey]int32
	// names memoizes every text_fields and shadow_sets row of the workspace
	// (see loadNames). nil means "not read yet, or dropped by a savepoint
	// rewind or by a delete of names".
	names map[nameKey]int32
}

// The statements that open a transaction. beginTx queues them rather than
// sending them, so they travel with the transaction's first statement.
var (
	// REPEATABLE READ, not the default: a scan's callback issues further reads
	// on the transaction, and under READ COMMITTED each of them would take its
	// own snapshot.
	beginRead  = queuedStmt{sql: `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`}
	beginWrite = queuedStmt{sql: `BEGIN`}
)

// beginTx acquires a connection from pool and opens a transaction for ws on
// it. Nothing reaches the server here: the opening statements wait in the
// queue for the transaction's first send, which saves the round trip BEGIN
// would take on its own, and a transaction that issues no statement costs
// none. The caller must call end.
func beginTx(ctx context.Context, pool *pgxpool.Pool, ws string, opening ...queuedStmt) (*pgTx, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	t := &pgTx{conn: conn, ctx: ctx, ws: ws, opening: len(opening)}
	t.queued = append(t.queued, opening...)
	return t, nil
}

// commitTimeout bounds the wait for the reply to COMMIT, which does not end
// with the transaction's context.
const commitTimeout = 10 * time.Second

// commit sends what is still queued and COMMIT as one batch. It returns the
// error of a statement in the batch that failed, and pgx.ErrTxCommitRollback
// for a COMMIT the server turned into a rollback.
//
// A transaction that has sent nothing and queued nothing past its opening has
// nothing on the server to commit, so it sends nothing, and fails only when
// its context has ended.
//
// A context that has ended before COMMIT is sent fails the commit. Once
// COMMIT is sent, the end of the context does not stop the wait for its
// reply; commitTimeout bounds that wait.
func (t *pgTx) commit() error {
	if t.failed != nil {
		return t.failed
	}
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if t.sends == 0 && len(t.queued) == t.opening {
		return nil
	}
	// The server can commit after a cancelled context made the client give up
	// on the reply, and the caller then sees a failure for a write that
	// landed. Waiting out the reply closes that window; what remains is a lost
	// connection, where no client can learn the outcome.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.ctx), commitTimeout)
	defer cancel()
	callerCtx := t.ctx
	t.ctx = ctx
	defer func() { t.ctx = callerCtx }()
	tag, err := t.exec(`COMMIT`)
	if err != nil {
		return err
	}
	if tag.String() == "ROLLBACK" {
		return pgx.ErrTxCommitRollback
	}
	return nil
}

// end rolls back whatever transaction the connection still has open and
// returns the connection to its pool. It runs after commit as well as after a
// failure or a panic.
//
// The server's own transaction status decides, not this struct's: it is right
// after a batch that failed half way, and it is idle when the opening
// statements never left the queue. A ROLLBACK that fails leaves the status as
// it was, and the pool closes a connection released in a transaction or in
// the middle of a result instead of reusing it.
func (t *pgTx) end() {
	if t.conn.Conn().PgConn().TxStatus() != 'I' {
		_, _ = t.conn.Exec(t.ctx, `ROLLBACK`)
	}
	t.conn.Release()
}

// loadWorkspace reads last_seq, tombstone_horizon, store_epoch and the newest
// lineage record in one statement and keeps them for the rest of the
// transaction. Every pull reads all four, so the memo turns four round trips
// into one.
//
// Holding them is exact rather than approximate. A read transaction is
// REPEATABLE READ, so its snapshot cannot change under it; a write transaction
// holds the workspace advisory lock, so nothing else writes them while it
// runs, and the places where the transaction moves them itself — register
// minting the epoch, NextSeq claiming a sequence, CollectTombstones raising
// the horizon, PruneSeqLineage deleting records — drop the memo or answer from
// their own state. The lineage record this transaction adds is written by
// flush, after the last read.
//
// A workspace with no row yet reads as all zeroes, which is the value each
// scalar has on the row register would create for it.
func (t *pgTx) loadWorkspace() error {
	if t.wsLoaded {
		return nil
	}
	return t.loadState()
}

// loadIndexStates reads every index_state row of the workspace into the memo,
// unless it holds them already.
func (t *pgTx) loadIndexStates() error {
	if t.indexStates != nil {
		return nil
	}
	return t.loadState()
}

// loadIndexScopes reads every index_scopes row of the workspace into the
// memo, unless it holds them already.
//
// Holding the set is exact for loadWorkspace's reason: a read transaction's
// snapshot cannot change, and a write transaction holds the workspace lock,
// under which only the transaction itself adds a row (see makeIndexScopes).
func (t *pgTx) loadIndexScopes() error {
	if t.indexScopes != nil {
		return nil
	}
	return t.loadState()
}

// loadNames reads every index_scopes, text_fields and shadow_sets row of the
// workspace into the memos, unless they hold them already. It holds them
// exactly, for the reason loadIndexScopes gives.
func (t *pgTx) loadNames() error {
	if t.names != nil && t.indexScopes != nil {
		return nil
	}
	return t.loadState()
}

// loadState fills whichever of the workspace memo, the index state memo, the
// index scope memo and the memo of names is empty, all in one round trip. Most
// transactions that need one of them need the others — a request reads its
// position and probes indexes, a write of a row reads its index states before
// it writes its entries, and a pull or a write of a text field reads its
// names — and each further read costs the server less than the round trip it
// saves when it is needed.
func (t *pgTx) loadState() error {
	var reads []queuedStmt
	if !t.wsLoaded {
		reads = append(reads, queuedStmt{
			// The newest lineage record rides along because a pull asks for
			// the mark of the head sequence on every cursor it issues, and that
			// record is the one that answers it.
			sql: `SELECT w.last_seq, w.tombstone_horizon, w.store_epoch,
			             COALESCE(l.start_seq, 0), COALESCE(l.mark, 0)
			      FROM workspaces w
			      LEFT JOIN LATERAL (
			        SELECT start_seq, mark FROM seq_lineage
			        WHERE workspace_id = w.workspace_id
			        ORDER BY start_seq DESC LIMIT 1
			      ) l ON TRUE
			      WHERE w.workspace_id = $1`,
			args: []any{t.ws},
		})
	}
	if t.indexStates == nil {
		reads = append(reads, queuedStmt{
			sql:  `SELECT entity, idx, spec_hash, ready FROM index_state WHERE workspace_id = $1`,
			args: []any{t.ws},
		})
	}
	if t.indexScopes == nil {
		reads = append(reads, queuedStmt{
			sql:  `SELECT entity, idx, scope FROM index_scopes WHERE workspace_id = $1`,
			args: []any{t.ws},
		})
	}
	if t.names == nil {
		reads = append(reads, queuedStmt{sql: namesSQL, args: []any{t.ws}})
	}
	br, err := t.sendWith(reads...)
	if err != nil {
		return err
	}
	if err := t.decodeState(br); err != nil {
		_ = br.Close()
		return err
	}
	return br.Close()
}

// decodeState reads the results loadState asked for from br, in its order.
func (t *pgTx) decodeState(br pgx.BatchResults) error {
	if !t.wsLoaded {
		// store_epoch is stored as a signed BIGINT because Postgres has no
		// unsigned one; reinterpret the bits. The value is opaque — only
		// equality matters.
		var seq, horizon, epoch, lineageStart, lineageMark int64
		err := br.QueryRow().Scan(&seq, &horizon, &epoch, &lineageStart, &lineageMark)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			seq, horizon, epoch, lineageStart, lineageMark = 0, 0, 0, 0, 0
		case err != nil:
			return err
		}
		t.wsSeq, t.wsHorizon, t.wsEpoch = uint64(seq), uint64(horizon), uint64(epoch)
		t.wsLineageStart, t.wsLineageMark = uint64(lineageStart), uint64(lineageMark)
		t.wsLoaded = true
	}
	if t.indexStates == nil {
		rows, err := br.Query()
		if err != nil {
			return err
		}
		states := make(map[indexKey]indexState)
		for rows.Next() {
			var (
				k  indexKey
				st indexState
			)
			if err := rows.Scan(&k.entity, &k.index, &st.specHash, &st.ready); err != nil {
				rows.Close()
				return err
			}
			states[k] = st
		}
		// Closed here and not deferred: the batch gives the next result only
		// when this one is closed.
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		t.indexStates = states
	}
	if t.indexScopes == nil {
		rows, err := br.Query()
		if err != nil {
			return err
		}
		scopes := make(map[indexKey]int32)
		for rows.Next() {
			var (
				k     indexKey
				scope int32
			)
			if err := rows.Scan(&k.entity, &k.index, &scope); err != nil {
				rows.Close()
				return err
			}
			scopes[k] = scope
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		t.indexScopes = scopes
	}
	if t.names == nil {
		names, err := decodeNames(br)
		if err != nil {
			return err
		}
		t.names = names
	}
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
		`INSERT INTO workspaces (workspace_id, store_epoch) VALUES ($1, $2)
		 ON CONFLICT DO NOTHING`,
		t.ws, epoch,
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

func (t *pgTx) SeqLineage(seq uint64) (uint64, error) {
	if seq == 0 {
		return 0, nil
	}
	current, err := t.CurrentSeq()
	if err != nil || seq > current {
		return 0, err
	}
	// This transaction's own allocations have no row until flush.
	if t.seqLoaded && seq >= t.seqStart {
		return t.mark, nil
	}
	if err := t.loadWorkspace(); err != nil {
		return 0, err
	}
	switch {
	case t.wsLineageStart == 0:
		// No record at all, so none at or below seq either.
		return 0, nil
	case seq >= t.wsLineageStart:
		return t.wsLineageMark, nil
	}
	var mark int64
	err = t.queryRow(
		`SELECT mark FROM seq_lineage WHERE workspace_id = $1 AND start_seq <= $2
		 ORDER BY start_seq DESC LIMIT 1`,
		t.ws, int64(seq),
	).Scan(&mark)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint64(mark), err
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
		 WHERE workspace_id = $1 AND entity = $2 AND key = $3`,
		t.ws, entity, key,
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
		 WHERE workspace_id = $1 AND entity = $2 AND key = ANY($3)`,
		t.ws, entity, keys,
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
		 WHERE workspace_id = $1 AND entity = $2 AND NOT deleted AND key > $3
		 ORDER BY key
		 LIMIT $4`,
		[]any{t.ws, entity, afterKey, limitArg(limit)}, limit,
		decodeEntityRow(entity), fn)
}

func (t *pgTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	// ORDER BY matches rows_by_entity_version exactly (version DESC, key ASC,
	// partial on NOT deleted), so this reads the index prefix and stops at the
	// limit instead of sorting the entity.
	return scanRows(t,
		`SELECT key, value, version, created_version, actor_subject, actor_system
		 FROM rows
		 WHERE workspace_id = $1 AND entity = $2 AND NOT deleted
		 ORDER BY version DESC, key
		 LIMIT $3`,
		[]any{t.ws, entity, limitArg(limit)}, limit,
		decodeEntityRow(entity), fn)
}

// CountRows implements storage.RowCounter in one round trip. The filter
// matches the partial index rows_by_entity_version, so Postgres can answer
// with an index-only scan and does not read row values.
func (t *pgTx) CountRows(entities []string) ([]uint64, error) {
	out := make([]uint64, len(entities))
	if len(entities) == 0 {
		return out, nil
	}
	at := make(map[string][]int, len(entities))
	for i, e := range entities {
		at[e] = append(at[e], i)
	}
	rows, err := t.query(
		`SELECT entity, count(*) FROM rows
		 WHERE workspace_id = $1 AND entity = ANY($2) AND NOT deleted
		 GROUP BY entity`,
		t.ws, entities,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			entity string
			n      int64
		)
		if err := rows.Scan(&entity, &n); err != nil {
			return nil, err
		}
		for _, i := range at[entity] {
			out[i] = uint64(n)
		}
	}
	return out, rows.Err()
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
			cols+`WHERE workspace_id = $1 AND version > $2
			      ORDER BY version, entity, key
			      LIMIT $3`,
			[]any{t.ws, int64(afterSeq), limitArg(limit)}, limit,
			decode, fn)
	}
	// Resume inside a same-version group. The row-value comparison is the whole
	// tie-break in one predicate — Postgres compares it lexicographically,
	// bytewise for entity and key because both columns are COLLATE "C" — where
	// SQLite needs the disjunction spelled out.
	return scanRows(t,
		cols+`WHERE workspace_id = $1
		        AND (version, entity, key) > ($2::bigint, $3::text, $4::text)
		      ORDER BY version, entity, key
		      LIMIT $5`,
		[]any{t.ws, int64(afterSeq), afterEntity, afterKey, limitArg(limit)}, limit,
		decode, fn)
}

func (t *pgTx) ClientLastMutationID(clientID string) (uint64, error) {
	var id int64
	err := t.queryRow(
		`SELECT last_mutation_id FROM clients
		 WHERE workspace_id = $1 AND client_id = $2`,
		t.ws, clientID,
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
		 WHERE workspace_id = $1 AND client_id = $2`,
		t.ws, clientID,
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

func (t *pgTx) ClientAppliedMutations(clientID string) ([]storage.AppliedMutation, error) {
	var blob []byte
	err := t.queryRow(
		`SELECT applied FROM clients
		 WHERE workspace_id = $1 AND client_id = $2`,
		t.ws, clientID,
	).Scan(&blob)
	if errors.Is(err, pgx.ErrNoRows) || len(blob) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var recs []storage.AppliedMutation
	if err := json.Unmarshal(blob, &recs); err != nil {
		return nil, fmt.Errorf("pgstore: decode applied mutations for client %q: %w", clientID, err)
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
		 WHERE workspace_id = $1 AND key = $2`,
		t.ws, key,
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
		`INSERT INTO idempotency (workspace_id, key, seq, result, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (workspace_id, key)
		 DO UPDATE SET seq = excluded.seq, result = excluded.result, updated_at = excluded.updated_at`,
		t.ws, key, int64(rec.Seq), notNilBytes(rec.Result), time.Now().UnixMilli(),
	)
}

func (t *pgTx) CollectIdempotencyKeys(cutoff time.Time) (int, error) {
	tag, err := t.exec(
		`DELETE FROM idempotency WHERE workspace_id = $1 AND updated_at < $2`,
		t.ws, cutoff.UnixMilli(),
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
		 WHERE workspace_id = $1 AND consumer = $2`,
		t.ws, consumer,
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
		 WHERE workspace_id = $1 ORDER BY consumer`,
		t.ws,
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
		`INSERT INTO consumer_positions (workspace_id, consumer, seq, entity, key, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (workspace_id, consumer)
		 DO UPDATE SET seq = excluded.seq, entity = excluded.entity,
		               key = excluded.key, updated_at = excluded.updated_at`,
		t.ws, consumer, int64(p.Seq), p.Entity, p.Key, time.Now().UnixMilli(),
	)
}

func (t *pgTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	wsn, sid, ok, err := t.nameIDs(nameKey{shadowSet, consumer, entity}, false)
	if err != nil || !ok {
		return nil, err
	}
	var snapshot []byte
	err = t.queryRow(
		`SELECT snapshot FROM consumer_shadows WHERE sid = $1 AND wsn = $2 AND key = $3`,
		sid, wsn, key,
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
	wsn, sid, _, err := t.nameIDs(nameKey{shadowSet, consumer, entity}, true)
	if err != nil {
		return err
	}
	// The column is NOT NULL, so a nil snapshot stores as an empty blob and
	// reads back as one. A consumer that wants "no shadow" deletes it.
	return t.queue(
		`INSERT INTO consumer_shadows (wsn, sid, key, snapshot)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (sid, wsn, key)
		 DO UPDATE SET snapshot = excluded.snapshot`,
		wsn, sid, key, notNilBytes(snapshot),
	)
}

func (t *pgTx) DeleteConsumerShadow(consumer, entity, key string) error {
	wsn, sid, ok, err := t.nameIDs(nameKey{shadowSet, consumer, entity}, false)
	if err != nil || !ok {
		return err
	}
	return t.queue(
		`DELETE FROM consumer_shadows WHERE sid = $1 AND wsn = $2 AND key = $3`,
		sid, wsn, key,
	)
}

func (t *pgTx) DeleteConsumer(consumer string) error {
	wsn, sids, ok, err := t.shadowSetIDs(consumer)
	if err != nil {
		return err
	}
	if ok && len(sids) > 0 {
		if err := t.queue(
			`DELETE FROM consumer_shadows WHERE sid = ANY($1::int4[]) AND wsn = $2`,
			sids, wsn,
		); err != nil {
			return err
		}
		if err := t.queue(
			`DELETE FROM shadow_sets WHERE workspace_id = $1 AND consumer = $2`,
			t.ws, consumer,
		); err != nil {
			return err
		}
		for k := range t.names {
			if k.kind == shadowSet && k.a == consumer {
				delete(t.names, k)
			}
		}
	}
	return t.queue(
		`DELETE FROM consumer_positions WHERE workspace_id = $1 AND consumer = $2`,
		t.ws, consumer,
	)
}

func (t *pgTx) CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (int, error) {
	if err := t.loadNames(); err != nil {
		return 0, err
	}
	// Shadows and their sets first, while the positions that name their
	// consumers still exist: the reverse order would leave every collected
	// consumer's shadows behind with nothing left to identify them by. A
	// workspace without a number holds no shadow.
	if wsn, ok := t.indexScopes[indexKey{}]; ok {
		if _, err := t.exec(
			`DELETE FROM consumer_shadows s USING shadow_sets n
			 WHERE s.sid = n.id AND s.wsn = $4 AND n.workspace_id = $1 AND n.consumer IN (
			     SELECT consumer FROM consumer_positions
			     WHERE workspace_id = $1 AND updated_at < $2 AND seq < $3
			 )`,
			t.ws, cutoff.UnixMilli(), int64(belowSeq), wsn,
		); err != nil {
			return 0, err
		}
		if _, err := t.exec(
			`DELETE FROM shadow_sets
			 WHERE workspace_id = $1 AND consumer IN (
			     SELECT consumer FROM consumer_positions
			     WHERE workspace_id = $1 AND updated_at < $2 AND seq < $3
			 )`,
			t.ws, cutoff.UnixMilli(), int64(belowSeq),
		); err != nil {
			return 0, err
		}
		t.names = nil
	}
	tag, err := t.exec(
		`DELETE FROM consumer_positions
		 WHERE workspace_id = $1 AND updated_at < $2 AND seq < $3`,
		t.ws, cutoff.UnixMilli(), int64(belowSeq),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) FieldDeltaHorizon() (uint64, error) {
	var horizon int64
	err := t.queryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE workspace_id = $1`,
		t.ws,
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
	wsn, fid, ok, err := t.nameIDs(nameKey{textField, entity, field}, false)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// No entry of the field was ever written: an empty span.
		return nil, true, nil
	}
	rows, err := t.query(
		`SELECT frag FROM field_deltas
		 WHERE fid = $1 AND wsn = $2 AND key = $3 AND seq > $4
		 ORDER BY seq`,
		fid, wsn, key, int64(afterSeq),
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
	wsn, fid, _, err := t.nameIDs(nameKey{textField, entity, field}, true)
	if err != nil {
		return err
	}
	// A gap marker must round-trip as an empty (not NULL) blob: the column is
	// NOT NULL and the read path distinguishes empty from present.
	return t.queue(
		`INSERT INTO field_deltas (seq, updated_at, wsn, fid, key, frag)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (fid, wsn, key, seq)
		 DO UPDATE SET frag = excluded.frag, updated_at = excluded.updated_at`,
		int64(seq), time.Now().UnixMilli(), wsn, fid, key, notNilBytes(frag),
	)
}

func (t *pgTx) PruneFieldDeltas(cutoff time.Time) (uint64, error) {
	if err := t.loadIndexScopes(); err != nil {
		return 0, err
	}
	// A workspace without a number holds no entry.
	wsn, hasEntries := t.indexScopes[indexKey{}]
	cutoffMillis := cutoff.UnixMilli()
	var maxPruned int64
	if hasEntries {
		if err := t.queryRow(
			`SELECT COALESCE(MAX(seq), 0) FROM field_deltas
			 WHERE wsn = $1 AND updated_at < $2`,
			wsn, cutoffMillis,
		).Scan(&maxPruned); err != nil {
			return 0, err
		}
	}
	if maxPruned > 0 {
		if _, err := t.exec(
			`DELETE FROM field_deltas WHERE wsn = $1 AND updated_at < $2`,
			wsn, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// GREATEST keeps the horizon monotonic even if an older cutoff runs
		// after a newer one, exactly as CollectTombstones does.
		if _, err := t.exec(
			`UPDATE workspaces SET field_delta_horizon = GREATEST(field_delta_horizon, $2)
			 WHERE workspace_id = $1`,
			t.ws, maxPruned,
		); err != nil {
			return 0, err
		}
	}
	var horizon int64
	err := t.queryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE workspace_id = $1`,
		t.ws,
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
		 WHERE workspace_id = $1 AND entity = $2 AND key = $3 AND field = $4`,
		t.ws, entity, key, field,
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
			 WHERE workspace_id = $1 AND entity = $2 AND key = $3 AND field = $4`,
			t.ws, entity, key, field,
		)
	}
	return t.queue(
		`INSERT INTO text_floors (workspace_id, entity, key, field, vv)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (workspace_id, entity, key, field)
		 DO UPDATE SET vv = excluded.vv`,
		t.ws, entity, key, field, vv,
	)
}

// indexKey names one (entity, index) pair of index_state.
type indexKey struct {
	entity, index string
}

// indexState is what index_state records for one indexKey.
type indexState struct {
	specHash string
	ready    bool
}

// IndexState answers from one read of every index state of the workspace,
// made on the first call and kept for the rest of the transaction. A request
// asks for several pairs, and the table holds one row per pair the schema
// declares, so the whole set costs what one point read costs.
//
// Holding the set is exact for loadWorkspace's reason. A read transaction's
// snapshot cannot change, and only a write transaction of this workspace
// writes index_state; such a transaction holds the workspace lock, so inside
// one the only writer is the transaction itself, and SetIndexState keeps the
// memo current.
func (t *pgTx) IndexState(entity, index string) (string, bool, error) {
	if err := t.loadIndexStates(); err != nil {
		return "", false, err
	}
	// A pair with no row is an index nothing has built yet, which is the
	// normal state right after registration — not an error.
	st := t.indexStates[indexKey{entity, index}]
	return st.specHash, st.ready, nil
}

// IndexStates answers from the memo IndexState reads, so it costs no round
// trip in a transaction that has read the workspace.
func (t *pgTx) IndexStates(entity string) ([]storage.IndexStatus, error) {
	if err := t.loadIndexStates(); err != nil {
		return nil, err
	}
	out := []storage.IndexStatus{}
	for k, st := range t.indexStates {
		if k.entity == entity {
			out = append(out, storage.IndexStatus{Index: k.index, SpecHash: st.specHash, Ready: st.ready})
		}
	}
	slices.SortFunc(out, func(a, b storage.IndexStatus) int { return strings.Compare(a.Index, b.Index) })
	return out, nil
}

// SetIndexBuildMark updates the workspace row and never calls register: the
// mark says "this workspace's indexes are built", which is vacuously true of a
// workspace that holds nothing, and registering one here would let a
// BuildIndexes call on an arbitrary id mint a workspace every later pass
// iterates.
func (t *pgTx) SetIndexBuildMark(mark string) error {
	return t.queue(
		`UPDATE workspaces SET index_build_mark = $2 WHERE workspace_id = $1`,
		t.ws, mark,
	)
}

// SetTextCompactMark updates the workspace row and never calls register, for
// SetIndexBuildMark's reason: a workspace holding no rows has no text to
// compact, and a sweep must not mint one.
func (t *pgTx) SetTextCompactMark(mark storage.TextCompactMark) error {
	return t.queue(
		`UPDATE workspaces
		    SET text_compact_seq = $2, text_compact_entity = $3, text_compact_key = $4,
		        text_compact_min_purged = $5
		  WHERE workspace_id = $1`,
		t.ws, int64(mark.Seq), mark.Entity, mark.Key, int32(mark.MinPurged),
	)
}

func (t *pgTx) TextCompactMark() (storage.TextCompactMark, error) {
	var m storage.TextCompactMark
	var seq int64
	var minPurged int32
	err := t.queryRow(
		`SELECT text_compact_seq, text_compact_entity, text_compact_key, text_compact_min_purged
		   FROM workspaces WHERE workspace_id = $1`,
		t.ws,
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
	if err := t.queue(
		`INSERT INTO index_state (workspace_id, entity, idx, spec_hash, ready)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (workspace_id, entity, idx)
		 DO UPDATE SET spec_hash = excluded.spec_hash, ready = excluded.ready`,
		t.ws, entity, index, specHash, ready,
	); err != nil {
		return err
	}
	// The upsert has not reached the server yet. If it fails there, it fails
	// the whole transaction, which then cannot commit anything decided on the
	// memo.
	if t.indexStates != nil {
		t.indexStates[indexKey{entity, index}] = indexState{specHash: specHash, ready: ready}
	}
	return nil
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
		 WHERE workspace_id = $1 RETURNING last_seq`,
		t.ws,
	).Scan(&seq); err != nil {
		return 0, err
	}
	t.seq, t.seqPersisted, t.seqLoaded = uint64(seq), uint64(seq), true
	t.seqStart = t.seq
	if t.mark == 0 {
		mark, err := newLineageMark()
		if err != nil {
			return 0, err
		}
		t.mark = uint64(mark)
	}
	return t.seq, nil
}

// flush writes back what NextSeq allocated in memory and records the lineage
// mark of those sequences. The store calls it after the write closure returns
// successfully and before COMMIT. Every statement is queued, so they travel
// with COMMIT.
//
// A transaction that registered the workspace also gets the deletion guard: a
// statement that fails with a unique violation on deleted_workspaces_pkey when
// the workspace carries a deletion mark, by inserting a copy of the mark's own
// row, and inserts nothing otherwise. The failure aborts the transaction, so
// nothing it wrote commits, and deletedGuardError reports it as
// storage.ErrWorkspaceDeleted. A plain SELECT of the mark would cost a round
// trip of its own; the guard rides in the batch COMMIT sends anyway. It sees
// every mark that matters: the workspace lock, taken before anything else in
// the transaction, is also what DeleteWorkspace holds.
func (t *pgTx) flush() error {
	if err := t.resolveEntries(); err != nil {
		return err
	}
	if err := t.resolvePostings(); err != nil {
		return err
	}
	if t.registered {
		if err := t.queue(
			`INSERT INTO deleted_workspaces (workspace_id, deleted_at)
			 SELECT workspace_id, deleted_at FROM deleted_workspaces WHERE workspace_id = $1`,
			t.ws,
		); err != nil {
			return err
		}
	}
	if !t.seqLoaded {
		return nil
	}
	if t.seq != t.seqPersisted {
		if err := t.queue(
			`UPDATE workspaces SET last_seq = $2 WHERE workspace_id = $1`,
			t.ws, int64(t.seq),
		); err != nil {
			return err
		}
		t.seqPersisted = t.seq
	}
	return t.queue(
		`INSERT INTO seq_lineage (workspace_id, start_seq, mark, written_at) VALUES ($1, $2, $3, $4)`,
		t.ws, int64(t.seqStart), int64(t.mark), time.Now().UnixMilli(),
	)
}

func (t *pgTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	// created_version stays out of the conflict SET: the stored value is the
	// key's original creation, which every overwrite keeps (see storage.Row).
	return t.queue(
		`INSERT INTO rows (workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8, $9)
		 ON CONFLICT (workspace_id, entity, key)
		 DO UPDATE SET value = excluded.value, version = excluded.version, deleted = FALSE,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ws, entity, key, value, int64(version), int64(version), time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
}

func (t *pgTx) DeleteRow(entity, key string, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	return t.queue(
		`INSERT INTO rows (workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES ($1, $2, $3, NULL, $4, $5, TRUE, $6, $7, $8)
		 ON CONFLICT (workspace_id, entity, key)
		 DO UPDATE SET value = NULL, version = excluded.version, deleted = TRUE,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ws, entity, key, int64(version), int64(version), time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
}

func (t *pgTx) SetClientLastMutationID(clientID string, mutationID uint64) error {
	if err := t.register(); err != nil {
		return err
	}
	return t.queue(
		`INSERT INTO clients (workspace_id, client_id, last_mutation_id, updated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (workspace_id, client_id)
		 DO UPDATE SET last_mutation_id = excluded.last_mutation_id, updated_at = excluded.updated_at`,
		t.ws, clientID, int64(mutationID), time.Now().UnixMilli(),
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
		`INSERT INTO clients (workspace_id, client_id, rejections, updated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (workspace_id, client_id)
		 DO UPDATE SET rejections = excluded.rejections, updated_at = excluded.updated_at`,
		t.ws, clientID, blob, time.Now().UnixMilli(),
	)
}

func (t *pgTx) SetClientAppliedMutations(clientID string, recs []storage.AppliedMutation) error {
	if err := t.register(); err != nil {
		return err
	}
	var blob []byte
	if len(recs) > 0 {
		var err error
		if blob, err = json.Marshal(recs); err != nil {
			return fmt.Errorf("pgstore: encode applied mutations for client %q: %w", clientID, err)
		}
	}
	return t.queue(
		`INSERT INTO clients (workspace_id, client_id, applied, updated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (workspace_id, client_id)
		 DO UPDATE SET applied = excluded.applied, updated_at = excluded.updated_at`,
		t.ws, clientID, blob, time.Now().UnixMilli(),
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
	// The posting calls fn logged go with the rewind. When fn made
	// resolvePostings write calls logged before the savepoint as well, the
	// rewind also undoes those writes, so the calls are pending again.
	postingLen, postingDone := len(t.postingLog), t.postingDone
	// The same holds for the index entry calls.
	entryLen, entryDone, entryPending := len(t.entryLog), t.entryDone, t.entryPending

	if err := fn(); err != nil {
		t.seq, t.seqPersisted, t.seqLoaded, t.registered = seq, seqPersisted, seqLoaded, registered
		t.postingLog, t.postingDone = t.postingLog[:postingLen], postingDone
		t.entryLog, t.entryDone, t.entryPending = t.entryLog[:entryLen], entryDone, entryPending
		// The rewind may undo a register, a CollectTombstones, a SetIndexState
		// or a new index scope, so the memos are dropped rather than restored:
		// what fn saw is not what the tables hold now, and what preceded fn may
		// not be either.
		t.wsLoaded = false
		t.indexStates = nil
		t.indexScopes = nil
		t.names = nil
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

func (t *pgTx) PruneSeqLineage(cutoff time.Time) (int, error) {
	if cutoff.IsZero() {
		return 0, nil
	}
	// By sequence rather than by age alone: a record that survived among
	// pruned older ones would answer for their sequences too, since a lookup
	// takes the greatest start_seq at or below the sequence it asks about.
	// The newest record stays whatever its age: it covers the head, and in an
	// idle workspace the head is old too.
	tag, err := t.exec(
		`DELETE FROM seq_lineage WHERE workspace_id = $1 AND start_seq <= (
			SELECT MAX(start_seq) FROM seq_lineage WHERE workspace_id = $1 AND written_at < $2
		) AND start_seq < (
			SELECT MAX(start_seq) FROM seq_lineage WHERE workspace_id = $1
		)`,
		t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	// The memo may name a record this just deleted.
	t.wsLoaded = false
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) CollectTombstones(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxDeleted int64
	if err := t.queryRow(
		`SELECT COALESCE(MAX(version), 0) FROM rows
		 WHERE workspace_id = $1 AND deleted AND updated_at < $2`,
		t.ws, cutoffMillis,
	).Scan(&maxDeleted); err != nil {
		return 0, err
	}
	if maxDeleted > 0 {
		if _, err := t.exec(
			`DELETE FROM rows
			 WHERE workspace_id = $1 AND deleted AND updated_at < $2`,
			t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// GREATEST keeps the horizon monotonic even if an older cutoff runs
		// after a newer one.
		if _, err := t.exec(
			`UPDATE workspaces SET tombstone_horizon = GREATEST(tombstone_horizon, $2)
			 WHERE workspace_id = $1`,
			t.ws, maxDeleted,
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
// A limit makes both statements select through the same `SELECT key … ORDER BY
// key LIMIT n` subquery — Postgres has no DELETE … LIMIT, and the subquery is
// what the row delete needs anyway. The ORDER BY is load-bearing rather than cosmetic:
// the two subqueries run against the same rows (the entries delete does not touch
// them) and must select the SAME set, or entries outlive their row or die before
// it. A total order over a unique key is what makes them agree.
//
// more is "this call hit its limit", which the interface allows: the drain that
// exactly emptied the selection costs one more no-op call, which is cheaper than
// probing for a next row on every chunk.
func (t *pgTx) PurgeEntityRows(entity, keyPrefix string, before time.Time, limit int) (int, bool, error) {
	// The logged index entry calls go first: the delete below must take the
	// entries they give the selected rows.
	if err := t.resolveEntries(); err != nil {
		return 0, false, err
	}
	if err := t.loadIndexScopes(); err != nil {
		return 0, false, err
	}
	where, args := purgeWhere(t.ws, entity, keyPrefix, before)
	selected, rowsWhere := where, where
	if limit > 0 {
		args = append(args, limit)
		selected = fmt.Sprintf("%s ORDER BY key LIMIT $%d", where, len(args))
		// The outer predicate reuses $1 and $2, which purgeWhere always numbers
		// as workspace and entity.
		rowsWhere = `workspace_id = $1 AND entity = $2 AND key IN (
		   SELECT key FROM rows WHERE ` + selected + `)`
	}
	wsn, hasEntries := t.indexScopes[indexKey{}]
	ent, entityHasEntries := t.indexScopes[indexKey{entity: entity}]
	// A workspace or an entity without an id holds no entry.
	if hasEntries && entityHasEntries {
		// tgt holds the row ids before either delete runs. It finds them with a
		// probe per selected row and the deletes take them as an array, for
		// the reasons indexPutSQL gives.
		w, e := len(args)+1, len(args)+2
		restore := t.customPlanIfPartitioned()
		_, err := t.exec(fmt.Sprintf(
			`WITH tgt AS MATERIALIZED (
			     SELECT r.rid FROM (SELECT key FROM rows WHERE %[3]s) s
			     CROSS JOIN LATERAL (
			       SELECT x.rid FROM index_rows x WHERE x.ent = $%[2]d AND x.wsn = $%[1]d AND x.key = s.key LIMIT 1
			     ) r
			 ),
			 gone AS (
			     DELETE FROM index_rows x WHERE x.wsn = $%[1]d AND x.rid = ANY (ARRAY(SELECT t.rid FROM tgt t))
			 )
			 DELETE FROM index_entries e WHERE e.wsn = $%[1]d AND e.rid = ANY (ARRAY(SELECT t.rid FROM tgt t))`,
			w, e, selected),
			append(slices.Clone(args), wsn, ent)...,
		)
		restore()
		if err != nil {
			return 0, false, err
		}
	}
	tag, err := t.exec(`DELETE FROM rows WHERE `+rowsWhere, args...)
	if err != nil {
		return 0, false, err
	}
	n := int(tag.RowsAffected())
	return n, limit > 0 && n == limit, nil
}

// purgeWhere builds PurgeEntityRows' row predicate and its arguments. The first
// two placeholders are shared with the statements that enclose it, so
// the numbering starts where that one's does.
//
// Both narrowings are optional: an empty prefix bounds nothing, and a zero
// `before` means no age bound at all — which is NOT what its UnixMilli() would
// say, so the branch is load-bearing rather than an optimization.
func purgeWhere(ws, entity, keyPrefix string, before time.Time) (string, []any) {
	where := "workspace_id = $1 AND entity = $2"
	args := []any{ws, entity}
	next := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if keyPrefix != "" {
		where += " AND key >= " + next(keyPrefix)
		// Both halves are needed: ">= prefix" alone also matches every key that
		// merely sorts after it. The upper bound is missing only for a prefix of
		// nothing but U+10FFFF, and every key at or above such a prefix starts
		// with it.
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
// with prefix, and whether one exists. Replace the last code point that has a
// successor with that successor and drop everything after it; a prefix of
// nothing but U+10FFFF has no upper bound.
//
// Postgres receives the bound as TEXT, so it must be valid UTF-8, and one more
// than the last byte often is not (U+043F is d0 bf). UTF-8 byte order is
// code-point order, so the next code point is also the least bound bytewise.
func prefixUpperBound(prefix string) (string, bool) {
	for end := len(prefix); end > 0; {
		r, size := utf8.DecodeLastRuneInString(prefix[:end])
		end -= size
		switch r {
		case utf8.MaxRune:
			continue
		case 0xd7ff:
			// U+D800 to U+DFFF are surrogates, which UTF-8 cannot encode.
			return prefix[:end] + "\ue000", true
		default:
			return prefix[:end] + string(r+1), true
		}
	}
	return "", false
}

func (t *pgTx) ExpireClientRejections(cutoff time.Time) (int, error) {
	// updated_at is deliberately left alone: it is what CollectIdleClients
	// measures idleness by, and expiring verdicts is not client activity.
	tag, err := t.exec(
		`UPDATE clients SET rejections = NULL
		 WHERE workspace_id = $1 AND updated_at < $2 AND rejections IS NOT NULL`,
		t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (t *pgTx) CollectIdleClients(cutoff time.Time) (int, error) {
	tag, err := t.exec(
		`DELETE FROM clients WHERE workspace_id = $1 AND updated_at < $2`,
		t.ws, cutoff.UnixMilli(),
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
