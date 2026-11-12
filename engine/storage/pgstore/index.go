package pgstore

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Value index entries (see storage.WriteTx.PutIndexEntries) are stored with
// int ids in place of the workspace id, the entity and index names and the row
// key, which would otherwise repeat in every tuple and in both indexes of it.
//
// index_scopes gives an int id to each workspace, entity and index that holds
// entries. The id of a workspace is its workspace number, wsn: index_rows and
// index_entries carry it, and a partitioned layout hashes them by it, so every
// entry of a workspace is in one partition. index_rows gives each row that
// holds entries a row id, rid, from the sequence index_row_ids. index_entries
// holds one tuple per entry: wsn, the id of the index (scope), the entry k,
// the row id and rk.
//
// The ids of index_scopes are INTEGER, from the sequence of the posting scopes,
// and none is used again. A workspace takes one, and one more for each entity
// and each index that holds entries, so the sequence gives out 2^31 ids in
// the life of the database and then fails the write that asks for the next.
// Row ids are BIGINT.
//
// rk says where the row key of an entry is. The engine ends an entry with the
// row key and two zero bytes (see query.IndexEncoder), so for such an entry rk
// is the byte length of the row key negated, zero or less, and a scan cuts the
// row key out of k. For any other entry rk is the row id, above zero, and a
// scan reads the row key from index_rows: entries a caller makes by hand, and
// the entries of a row key that the engine's encoding writes in another form
// than the key itself, which is every key with a code point above U+FFFF.
//
// PutIndexEntries and DeleteIndexEntries do not reach the server when they are
// called. They go to entryLog, and resolveEntries writes every logged call of
// the transaction with one statement per indexRowsPerStatement rows: before
// COMMIT (see flush), and before anything in this transaction reads or deletes
// stored entries. One statement plans, locks and starts once for all its rows;
// a statement per row pays that per row, and with a partitioned layout it is
// most of what a row costs.
//
// Every statement on index_rows and index_entries binds wsn to equality as a
// parameter, so a partitioned layout prunes it to one partition. The ids come
// from indexScopes, the transaction's memo of the workspace's index_scopes
// rows.

const (
	// indexEntryEnd is the two bytes that end a string in an entry, and so
	// follow the row key at the end of one.
	indexEntryEnd = "\x00\x00"

	// indexRowsPerStatement is the most rows one statement of resolveEntries
	// replaces or deletes, and indexEntriesPerStatement the entry count at
	// which it starts another statement. They bound the arrays a statement
	// carries and what the server holds of them in memory at once. A row is
	// never split, so a statement holds more entries when one row does.
	indexRowsPerStatement    = 500
	indexEntriesPerStatement = 16384

	// maxLoggedEntries caps the calls and entries the transaction holds
	// unwritten between them, as maxQueuedStatements caps its statements. A
	// log that reaches it is written and sent on the spot.
	maxLoggedEntries = 32768
)

// The plan cache of Postgres chooses for each prepared statement between a
// custom plan, made again on every call, and a generic plan, made once. Left
// alone, its choice for the statements on the index tables depends on what the
// first transactions of the connection held, and both wrong choices cost:
//
//   - Unpartitioned, a custom plan costs the planning on every call, which
//     for a transaction of one row is about as much as running the statement.
//   - Partitioned, a generic plan locks every partition of every table it
//     names before it prunes to the one of the workspace. A write then waits
//     for an exclusive lock that another session holds on any partition, and
//     its locks fill the lock table of the server. A custom plan prunes when
//     it plans and locks the partitions of the workspace alone.
//
// So resolveEntries queues the plan mode of the layout before its statements
// (see entryPlanMode) and defaultPlanMode after them. The statements then run
// on that kind of plan from their first call, and the statements after them
// in the transaction run in the default mode of the session again. Each is
// SET LOCAL, so none outlives the transaction, and a savepoint rewind takes
// them back with the statements between them.
//
// The reads of the index tables leave the plan cache alone on an unpartitioned
// layout, and force a custom plan on a partitioned one, for the same reason
// (see customPlanIfPartitioned).
const (
	forceGenericPlan = `SET LOCAL plan_cache_mode = force_generic_plan`
	forceCustomPlan  = `SET LOCAL plan_cache_mode = force_custom_plan`
	defaultPlanMode  = `SET LOCAL plan_cache_mode TO DEFAULT`
)

// entryPlanMode is the statement that sets the plan mode for the statements of
// resolveEntries on the layout of the database.
func (t *pgTx) entryPlanMode() string {
	if t.partitioned {
		return forceCustomPlan
	}
	return forceGenericPlan
}

// customPlanIfPartitioned queues forceCustomPlan on a partitioned layout, for
// the statements on the index tables that the caller sends next, and returns
// the function that queues defaultPlanMode after them. The function does
// nothing on its second call, and nothing at all on an unpartitioned layout.
// The caller must call it before a statement that is not on the index tables
// reaches the queue.
func (t *pgTx) customPlanIfPartitioned() (restore func()) {
	if !t.partitioned {
		return func() {}
	}
	// An error here is the failure of the transaction, which the statement
	// the caller sends next reports.
	_ = t.queue(forceCustomPlan)
	done := false
	return func() {
		if !done {
			done = true
			_ = t.queue(defaultPlanMode)
		}
	}
}

const (
	// indexPutSQL replaces the entries of the rows ($2 entity id, $3 row key)
	// of workspace number $1 by the entries ($4 position of the row in $2 and
	// $3, $5 index id, $6 entry, $7 rk, or NULL for the row id). A row without
	// a row id takes a new one.
	//
	// It replaces by difference: have is what the rows hold, and only the
	// entries of it that want lacks are deleted, and only the entries of want
	// that it lacks are inserted. A row carries an entry per index and per
	// element of every indexed list, while the write that came here moved one
	// or two of them, and a deleted and inserted tuple per unchanged entry
	// would leave a dead tuple each and dirty a page of both indexes.
	// Filtering the INSERT against have is what makes an unchanged entry
	// free: an insert of the whole set under ON CONFLICT DO NOTHING would
	// descend the primary key once per entry only to discard it.
	//
	// Every part reads the same snapshot, so inp does not see the rows ins
	// adds; fresh carries their ids to want. The DELETE and the INSERT touch
	// different tuples for the same reason.
	//
	// The generic plan must not depend on the statistics: a plan made while
	// the tables were empty runs on them when they are large. So no part
	// joins index_rows or index_entries, where the planner may pick a hash
	// join over a scan of a whole table. The row ids come from a probe with
	// LIMIT 1 per row, and the entries from an index condition on an array of
	// row ids.
	//
	// The DELETE names the stale tuples by their tuple id, which costs a
	// fetch per stale entry where the row-id index would read every entry of
	// the rows again. A tuple id of have names one tuple of the partition of
	// wsn for as long as the statement runs: entries are never updated, and
	// the workspace lock keeps every other writer of the workspace out. The
	// condition on rid does no filtering then. It is there for the plan: with
	// it the planner has an index condition on the rows of this statement
	// also when it does not scan by tuple id, as on empty tables, where it
	// would otherwise scan the table.
	indexPutSQL = `WITH inp AS MATERIALIZED (
	SELECT u.ord::int4 AS ord, u.ent, u.key, r.rid
	FROM unnest($2::int4[], $3::text[]) WITH ORDINALITY AS u(ent, key, ord)
	LEFT JOIN LATERAL (
		SELECT x.rid FROM index_rows x WHERE x.ent = u.ent AND x.wsn = $1 AND x.key = u.key LIMIT 1
	) r ON TRUE
),
fresh AS MATERIALIZED (
	SELECT i.ord, i.ent, i.key, nextval('index_row_ids') AS rid FROM inp i WHERE i.rid IS NULL
),
ins AS (
	INSERT INTO index_rows (wsn, ent, rid, key) SELECT $1, f.ent, f.rid, f.key FROM fresh f
),
want AS MATERIALIZED (
	SELECT COALESCE(i.rid, f.rid) AS rid, u.scope, u.k, u.rk
	FROM unnest($4::int4[], $5::int4[], $6::bytea[], $7::int8[]) AS u(ord, scope, k, rk)
	JOIN inp i ON i.ord = u.ord
	LEFT JOIN fresh f ON f.ord = u.ord
),
have AS MATERIALIZED (
	SELECT x.ctid AS tid, x.rid, x.scope, x.k FROM index_entries x
	WHERE x.wsn = $1 AND x.rid = ANY (ARRAY(SELECT i.rid FROM inp i WHERE i.rid IS NOT NULL))
),
stale AS (
	DELETE FROM index_entries e
	WHERE e.wsn = $1 AND e.rid = ANY (ARRAY(SELECT i.rid FROM inp i WHERE i.rid IS NOT NULL))
	  AND e.ctid = ANY (ARRAY(
		SELECT h.tid FROM have h
		WHERE NOT EXISTS (SELECT 1 FROM want w WHERE w.rid = h.rid AND w.scope = h.scope AND w.k = h.k)))
)
INSERT INTO index_entries (wsn, scope, rk, rid, k)
SELECT $1, w.scope, COALESCE(w.rk, w.rid), w.rid, w.k FROM want w
WHERE NOT EXISTS (SELECT 1 FROM have h WHERE h.rid = w.rid AND h.scope = w.scope AND h.k = w.k)`

	// indexDeleteSQL deletes the rows ($2 entity id, $3 row key) of workspace
	// number $1 from index_rows, and their entries. A row that holds no entry
	// is not in index_rows, and nothing happens for it.
	indexDeleteSQL = `WITH tgt AS MATERIALIZED (
	SELECT r.rid FROM unnest($2::int4[], $3::text[]) AS u(ent, key)
	CROSS JOIN LATERAL (
		SELECT x.rid FROM index_rows x WHERE x.ent = u.ent AND x.wsn = $1 AND x.key = u.key LIMIT 1
	) r
),
gone AS (
	DELETE FROM index_rows x WHERE x.wsn = $1 AND x.rid = ANY (ARRAY(SELECT t.rid FROM tgt t))
)
DELETE FROM index_entries e WHERE e.wsn = $1 AND e.rid = ANY (ARRAY(SELECT t.rid FROM tgt t))`

	// indexScopesSQL gives the names ($2 entity, $3 index) of workspace $1 a
	// scope id each and returns them. The caller holds the workspace lock and
	// names only what the workspace does not hold, so a name the table holds
	// already fails on the primary key instead of taking a second id.
	indexScopesSQL = `INSERT INTO index_scopes (workspace_id, entity, idx, scope)
SELECT $1, u.entity, u.idx, nextval('posting_scope_ids')::int4
FROM unnest($2::text[], $3::text[]) AS u(entity, idx)
RETURNING entity, idx, scope`

	// indexRowKeysSQL reads the row keys of the row ids $2 of workspace number
	// $1.
	indexRowKeysSQL = `SELECT rid, key FROM index_rows WHERE wsn = $1 AND rid = ANY($2::int8[])`
)

// entryOp is one PutIndexEntries or DeleteIndexEntries call that the
// transaction has not written yet.
type entryOp struct {
	entity, key string
	// indexes and keys are the entries of a PutIndexEntries call, in the
	// order of the call. keys are copies the transaction owns. Both are nil
	// for a DeleteIndexEntries call.
	indexes []string
	keys    [][]byte
}

// endsWithRowKey reports whether the entry k ends with rowKey and
// indexEntryEnd.
func endsWithRowKey(k []byte, rowKey string) bool {
	n := len(k) - len(indexEntryEnd)
	return n >= len(rowKey) && string(k[n:]) == indexEntryEnd && string(k[n-len(rowKey):n]) == rowKey
}

// entryRowKey cuts the row key out of the entry k, whose rk column holds rk,
// zero or less: the byte length of the row key negated.
func entryRowKey(k []byte, rk int64) (string, error) {
	end := len(k) - len(indexEntryEnd)
	if end < 0 || rk > 0 || rk < -int64(end) {
		return "", fmt.Errorf("pgstore: row key length %d does not fit an index entry of %d bytes", -rk, len(k))
	}
	return string(k[end+int(rk) : end]), nil
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
	if t.failed != nil {
		return t.failed
	}
	op := entryOp{entity: entity, key: key, indexes: make([]string, len(entries))}
	keys := make([][]byte, len(entries))
	for i, e := range entries {
		op.indexes[i], keys[i] = e.Index, e.Key
	}
	// Copies, because the statement that carries them is built later, when
	// the caller may have reused its buffers.
	op.keys = cloneKeys(keys)
	return t.logEntries(op)
}

func (t *pgTx) DeleteIndexEntries(entity, key string) error {
	if t.failed != nil {
		return t.failed
	}
	// No register(): removing entries a row never had must leave no trace,
	// the same reason a write closure that writes nothing does not create a
	// workspace.
	return t.logEntries(entryOp{entity: entity, key: key})
}

func (t *pgTx) logEntries(op entryOp) error {
	t.entryLog = append(t.entryLog, op)
	// A call counts as well as its entries, so that a run of deletes, which
	// hold none, also reaches the cap.
	t.entryPending += 1 + len(op.keys)
	if t.entryPending < maxLoggedEntries {
		return nil
	}
	if err := t.resolveEntries(); err != nil {
		return err
	}
	return t.send()
}

// resolveEntries writes the logged calls that are not written yet: it queues
// the statements that replace and delete their rows. It reads only when the
// memo of index scopes is not loaded, or when a call names an entity or an
// index of the workspace for the first time.
//
// Every reader of index_rows and index_entries in a write transaction calls it
// first, so that the read sees the transaction's own calls.
func (t *pgTx) resolveEntries() error {
	if t.failed != nil {
		return t.failed
	}
	ops := t.entryLog[t.entryDone:]
	if len(ops) == 0 {
		return nil
	}
	if err := t.loadIndexScopes(); err != nil {
		return err
	}

	// The last call of a row decides what the row holds, so the earlier ones
	// of this run have nothing to write.
	type rowRef struct{ entity, key string }
	last := make(map[rowRef]int, len(ops))
	for i := range ops {
		last[rowRef{ops[i].entity, ops[i].key}] = i
	}
	var (
		puts, dels []*entryOp
		missing    []indexKey
		asked      = map[indexKey]struct{}{}
	)
	need := func(k indexKey) {
		if _, ok := t.indexScopes[k]; ok {
			return
		}
		if _, ok := asked[k]; !ok {
			asked[k] = struct{}{}
			missing = append(missing, k)
		}
	}
	for i := range ops {
		op := &ops[i]
		if last[rowRef{op.entity, op.key}] != i {
			continue
		}
		if op.keys == nil {
			dels = append(dels, op)
			continue
		}
		puts = append(puts, op)
		need(indexKey{})
		need(indexKey{entity: op.entity})
		for j, index := range op.indexes {
			// A row names each of its indexes many times in a row.
			if j == 0 || index != op.indexes[j-1] {
				need(indexKey{entity: op.entity, index: index})
			}
		}
	}
	if len(missing) > 0 {
		if err := t.makeIndexScopes(missing); err != nil {
			return err
		}
	}
	t.entryDone, t.entryPending = len(t.entryLog), 0
	if t.spDepth == 0 {
		// Only the rewind of a savepoint makes a written call pending again,
		// so outside one the log can let go of the calls and their entries.
		// Deferred, because the statements below are built from ops.
		defer func() {
			clear(t.entryLog)
			t.entryLog, t.entryDone = t.entryLog[:0], 0
		}()
	}
	wsn, ok := t.indexScopes[indexKey{}]
	if !ok {
		// Only deletes, in a workspace that holds no entry.
		return nil
	}

	var stmts []queuedStmt
	for start := 0; start < len(dels); start += indexRowsPerStatement {
		chunk := dels[start:min(start+indexRowsPerStatement, len(dels))]
		ents, keys := make([]int32, 0, len(chunk)), make([]string, 0, len(chunk))
		for _, op := range chunk {
			// An entity without an id holds no entry.
			if ent, ok := t.indexScopes[indexKey{entity: op.entity}]; ok {
				ents, keys = append(ents, ent), append(keys, op.key)
			}
		}
		if len(keys) > 0 {
			stmts = append(stmts, queuedStmt{sql: indexDeleteSQL, args: []any{wsn, ents, keys}})
		}
	}
	for start := 0; start < len(puts); {
		end, entries := start, 0
		for end < len(puts) && end-start < indexRowsPerStatement && (end == start || entries < indexEntriesPerStatement) {
			entries += len(puts[end].keys)
			end++
		}
		stmts = append(stmts, t.indexPutStmt(wsn, puts[start:end], entries))
		start = end
	}
	if len(stmts) == 0 {
		return nil
	}
	if err := t.queue(t.entryPlanMode()); err != nil {
		return err
	}
	for _, s := range stmts {
		if err := t.queue(s.sql, s.args...); err != nil {
			return err
		}
	}
	return t.queue(defaultPlanMode)
}

// indexPutStmt builds indexPutSQL for the calls ops, which hold entries
// entries between them.
func (t *pgTx) indexPutStmt(wsn int32, ops []*entryOp, entries int) queuedStmt {
	type entry struct {
		scope int32
		k     []byte
	}
	var (
		ents   = make([]int32, len(ops))
		keys   = make([]string, len(ops))
		ords   = make([]int32, 0, entries)
		scopes = make([]int32, 0, entries)
		ks     = make([][]byte, 0, entries)
		rks    = make([]*int64, 0, entries)
		row    []entry
	)
	for i, op := range ops {
		ents[i], keys[i] = t.indexScopes[indexKey{entity: op.entity}], op.key
		row = row[:0]
		var (
			scope int32
			index string
		)
		for j, k := range op.keys {
			if j == 0 || op.indexes[j] != index {
				index = op.indexes[j]
				scope = t.indexScopes[indexKey{entity: op.entity, index: index}]
			}
			row = append(row, entry{scope: scope, k: notNilBytes(k)})
		}
		// The caller may hand in the same entry twice — two components of one
		// index can encode to the same entry for one row — and the primary key
		// would refuse the second copy. Sorting brings the copies together.
		slices.SortFunc(row, func(a, b entry) int {
			if a.scope != b.scope {
				return int(a.scope) - int(b.scope)
			}
			return bytes.Compare(a.k, b.k)
		})
		suffix := -int64(len(op.key))
		for j, e := range row {
			if j > 0 && e.scope == row[j-1].scope && bytes.Equal(e.k, row[j-1].k) {
				continue
			}
			ords, scopes, ks = append(ords, int32(i+1)), append(scopes, e.scope), append(ks, e.k)
			if endsWithRowKey(e.k, op.key) {
				rks = append(rks, &suffix)
			} else {
				rks = append(rks, nil)
			}
		}
	}
	return queuedStmt{sql: indexPutSQL, args: []any{wsn, ents, keys, ords, scopes, ks, rks}}
}

// makeIndexScopes gives each of names a scope id and adds it to the memo. It
// costs a round trip, once for each name in the life of a workspace.
func (t *pgTx) makeIndexScopes(names []indexKey) error {
	entities, indexes := make([]string, len(names)), make([]string, len(names))
	for i, n := range names {
		entities[i], indexes[i] = n.entity, n.index
	}
	rows, err := t.query(indexScopesSQL, t.ws, entities, indexes)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			k     indexKey
			scope int32
		)
		if err := rows.Scan(&k.entity, &k.index, &scope); err != nil {
			return err
		}
		t.indexScopes[k] = scope
	}
	return rows.Err()
}

// indexScope returns the workspace number and the scope id of one index, and
// false when the workspace holds no entry of the index. It writes the logged
// calls first, so a statement the caller then sends with the ids sees them.
func (t *pgTx) indexScope(entity, index string) (wsn, scope int32, ok bool, err error) {
	if err := t.resolveEntries(); err != nil {
		return 0, 0, false, err
	}
	if err := t.loadIndexScopes(); err != nil {
		return 0, 0, false, err
	}
	if wsn, ok = t.indexScopes[indexKey{}]; !ok {
		return 0, 0, false, nil
	}
	scope, ok = t.indexScopes[indexKey{entity: entity, index: index}]
	return wsn, scope, ok, nil
}

// indexEntryRow is one entry a scan read: the entry, its rk column, and the
// row key once fillRowKeys has set it. i is the position of its range in the
// caller's list, for a read of several ranges.
type indexEntryRow struct {
	i      int
	entry  []byte
	rk     int64
	rowKey string
}

// fillRowKeys sets the row key of each of rows: from the entry itself where rk
// says the entry ends with it, and from index_rows, in one statement for all
// of rows, for the others.
func (t *pgTx) fillRowKeys(wsn int32, rows []indexEntryRow) error {
	var ids []int64
	for i := range rows {
		if rows[i].rk > 0 {
			ids = append(ids, rows[i].rk)
			continue
		}
		key, err := entryRowKey(rows[i].entry, rows[i].rk)
		if err != nil {
			return err
		}
		rows[i].rowKey = key
	}
	if len(ids) == 0 {
		return nil
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	restore := t.customPlanIfPartitioned()
	found, err := t.query(indexRowKeysSQL, wsn, ids)
	restore()
	if err != nil {
		return err
	}
	keys := make(map[int64]string, len(ids))
	for found.Next() {
		var (
			rid int64
			key string
		)
		if err := found.Scan(&rid, &key); err != nil {
			found.Close()
			return err
		}
		keys[rid] = key
	}
	found.Close()
	if err := found.Err(); err != nil {
		return err
	}
	for i := range rows {
		if rows[i].rk <= 0 {
			continue
		}
		key, ok := keys[rows[i].rk]
		if !ok {
			return fmt.Errorf("pgstore: index entry of workspace %q names row id %d, which index_rows does not hold", t.ws, rows[i].rk)
		}
		rows[i].rowKey = key
	}
	return nil
}

func (t *pgTx) ScanIndex(
	entity, index string,
	lo, hi []byte,
	limit int,
	order storage.ScanOrder,
	fn func(entry []byte, rowKey string) error,
) error {
	wsn, scope, ok, err := t.indexScope(entity, index)
	if err != nil || !ok {
		return err
	}
	// The bounds are optional on both ends, and a "compare only if bound"
	// predicate would defeat the index, so the WHERE clause is built to match
	// what was given. k is BYTEA, which Postgres compares bytewise — the order
	// the entry encoding is built for, and the reason this must never become a
	// text column.
	var where strings.Builder
	where.WriteString(`scope = $1 AND wsn = $2`)
	args := []any{scope, wsn}
	if lo != nil {
		args = append(args, lo)
		fmt.Fprintf(&where, ` AND k >= $%d`, len(args))
	}
	if hi != nil {
		args = append(args, hi)
		fmt.Fprintf(&where, ` AND k < $%d`, len(args))
	}
	args = append(args, limitArg(limit))
	// rk is part of the sort so that two entries sharing a key come out in a
	// fixed order; reversing the walk has to reverse it too, or the descending
	// scan would not be the exact mirror of the ascending one. Byte-equal
	// entries of different rows differ in rk: the ones that end with their
	// row key hold the lengths of keys of different lengths, and the others
	// hold their row ids. So they come out by key length, longest first, and
	// then by row id, which the contract leaves to the adapter.
	orderBy := `k, rk`
	if order == storage.ScanDesc {
		orderBy = `k DESC, rk DESC`
	}
	query := fmt.Sprintf(
		`SELECT k, rk FROM index_entries WHERE %s ORDER BY %s LIMIT $%d`, where.String(), orderBy, len(args))

	// The mode holds for the statement that reads the first batch and for the
	// one that declares a cursor for the rest, which both go out before the
	// first batch is yielded. fn may read anything, so the default mode is
	// back in the queue before fn runs.
	restore := t.customPlanIfPartitioned()
	defer restore()
	return scanBatches(t, query, args, limit,
		func(rows pgx.Rows) (indexEntryRow, error) {
			var r indexEntryRow
			if err := rows.Scan(&r.entry, &r.rk); err != nil {
				return indexEntryRow{}, err
			}
			return r, nil
		},
		func(batch []indexEntryRow) error {
			restore()
			if err := t.fillRowKeys(wsn, batch); err != nil {
				return err
			}
			for _, r := range batch {
				if err := fn(r.entry, r.rowKey); err != nil {
					return err
				}
			}
			return nil
		})
}

// ScanIndexRanges reads a run of bounded ranges a page at a time, every range
// of the run in one statement (see scanBoundedRanges), so a probe of many keys
// costs the round trips of its entries rather than one per key. A range with
// no upper bound reads through ScanIndex on its own, which costs a round trip
// more and is rare: a prefix range has a Hi unless its prefix is all 0xff.
func (t *pgTx) ScanIndexRanges(
	entity, index string,
	ranges []storage.IndexRange,
	fn func(i int, entry []byte, rowKey string) error,
) error {
	for start := 0; start < len(ranges); {
		if ranges[start].Hi == nil {
			i := start
			if err := t.ScanIndex(entity, index, ranges[i].Lo, nil, 0, storage.ScanAsc, func(entry []byte, rowKey string) error {
				return fn(i, entry, rowKey)
			}); err != nil {
				return err
			}
			start++
			continue
		}
		end := start + 1
		for end < len(ranges) && ranges[end].Hi != nil {
			end++
		}
		if err := t.scanBoundedRanges(entity, index, ranges[start:end], start, fn); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// scanBoundedRanges yields the entries of ranges, every one of which has a Hi,
// to fn as ScanIndexRanges does. first is the position of ranges[0] in the
// caller's list.
//
// It pages by key rather than through scanRows's cursor. Postgres cannot tell
// that the join below returns its rows in the order of the outer ORDER BY, so
// it sorts each range's entries, and an unlimited read sorts every entry of a
// range before it returns the first: 79 ms for the first 257 entries of a
// range of 200000, where a page of the same range takes 0.2 ms. A LIMIT inside
// the join bounds that sort to one page, and the next page starts after the
// last entry yielded.
func (t *pgTx) scanBoundedRanges(
	entity, index string,
	ranges []storage.IndexRange,
	first int,
	fn func(i int, entry []byte, rowKey string) error,
) error {
	if len(ranges) == 1 {
		// ScanIndex reads one range as fast, with a plan that has no join.
		return t.ScanIndex(entity, index, ranges[0].Lo, ranges[0].Hi, 0, storage.ScanAsc, func(entry []byte, rowKey string) error {
			return fn(first, entry, rowKey)
		})
	}
	wsn, scope, ok, err := t.indexScope(entity, index)
	if err != nil || !ok {
		return err
	}
	los := make([][]byte, len(ranges))
	his := make([][]byte, len(ranges))
	for i, r := range ranges {
		// The empty key sorts first, so it stands in for a nil Lo as a bound
		// the index can seek to.
		los[i] = notNilBytes(r.Lo)
		his[i] = r.Hi
	}
	// Each page reads one entry more than it yields, which tells whether the
	// range of the last entry yielded goes on.
	const pageRead = scanBatch + 1
	// Each range is a primary-key probe of its own through the LATERAL join,
	// which reads the range in index order and stops at the LIMIT. The ORDER
	// BY inside the join makes the LIMIT take the first entries of the range,
	// and it keeps the probe plan when the statement cache makes the statement
	// generic, where the planner does not know how many ranges there are: a
	// generic plan of the join without it reads every entry of the index
	// once and filters it against each range. (k, rk) is unique within one
	// range, as the primary key holds it, so the outer ORDER BY is a total
	// order.
	const across = `SELECT r.i, e.k, e.rk
		FROM unnest($3::bytea[], $4::bytea[]) WITH ORDINALITY AS r(lo, hi, i)
		CROSS JOIN LATERAL (
			SELECT k, rk FROM index_entries
			WHERE scope = $1 AND wsn = $2 AND k >= r.lo AND k < r.hi
			ORDER BY k, rk
			LIMIT $5
		) e
		ORDER BY r.i, e.k, e.rk
		LIMIT $5`
	// The rest of one range after an entry. The row comparison seeks in the
	// primary key, where a test of k alone would read every entry before it
	// again that shares its k, as all the rows below one parent do.
	const rest = `SELECT k, rk FROM index_entries
		WHERE scope = $1 AND wsn = $2 AND (k, rk) > ($3, $4) AND k < $5
		ORDER BY k, rk
		LIMIT $6`

	// from is the first range not yet read to its end, and after is the last
	// entry yielded from it, nil when it has yielded none.
	from := 0
	var after *indexEntryRow
	for from < len(ranges) {
		var (
			page []indexEntryRow
			err  error
		)
		restore := t.customPlanIfPartitioned()
		if after == nil {
			offset := from
			page, err = queryBatch(t, across,
				[]any{scope, wsn, los[from:], his[from:], int64(pageRead)}, pageRead,
				func(rows pgx.Rows) (indexEntryRow, error) {
					var (
						e   indexEntryRow
						ord int64
					)
					if err := rows.Scan(&ord, &e.entry, &e.rk); err != nil {
						return indexEntryRow{}, err
					}
					e.i = offset + int(ord-1) // WITH ORDINALITY counts from 1
					return e, nil
				})
		} else {
			i := from
			page, err = queryBatch(t, rest,
				[]any{scope, wsn, after.entry, after.rk, his[from], int64(pageRead)}, pageRead,
				func(rows pgx.Rows) (indexEntryRow, error) {
					e := indexEntryRow{i: i}
					if err := rows.Scan(&e.entry, &e.rk); err != nil {
						return indexEntryRow{}, err
					}
					return e, nil
				})
		}
		restore()
		if err != nil {
			return err
		}
		more := len(page) > scanBatch
		yielded := page[:min(len(page), scanBatch)]
		if err := t.fillRowKeys(wsn, yielded); err != nil {
			return err
		}
		for _, e := range yielded {
			if err := fn(first+e.i, e.entry, e.rowKey); err != nil {
				return err
			}
		}
		switch {
		case !more && after == nil:
			// No range from `from` on held more than this page.
			return nil
		case !more:
			from, after = from+1, nil
		case page[scanBatch].i == yielded[scanBatch-1].i:
			last := yielded[scanBatch-1]
			from, after = last.i, &last
		default:
			// The entry read past the page opens a range of its own, so the
			// ranges up to it are done and the next page starts at it.
			from, after = page[scanBatch].i, nil
		}
	}
	return nil
}
