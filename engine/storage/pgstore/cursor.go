package pgstore

import (
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
)

// scanBatch is how many rows one FETCH pulls back. It trades round trips
// against the memory a scan holds at once: every scan here is bounded by this
// rather than by the size of its result, which is the point — ScanEntity takes
// no limit, and bootstrap walks whole entities through it.
const scanBatch = 256

// scanCursor streams query's rows to yield through a server-side cursor.
//
// This is the one mechanism in this adapter that sqlitestore has no counterpart
// for, and the contract is what forces it: a scan callback may issue further
// reads on the same transaction (ScanIndex re-reads each candidate row,
// ScanChanges reads FieldDeltas), and pgx cannot run a second query on a
// connection whose result set is still open. So each FETCH is decoded into
// memory and its result set closed BEFORE any callback runs — while yield
// executes, the connection is idle and free for the re-entrant read.
//
// decode runs with the result set open and must only Scan; yield runs with it
// closed and may do anything a ReadTx can.
func scanCursor[T any](
	t *pgTx,
	query string,
	args []any,
	decode func(pgx.Rows) (T, error),
	yield func(T) error,
) error {
	name := t.nextCursorName(query)
	if _, err := t.tx.Exec(t.ctx, "DECLARE "+name+" NO SCROLL CURSOR FOR "+query, args...); err != nil {
		return err
	}
	defer func() {
		// Best effort. If the transaction is already aborted the CLOSE fails
		// too, and the error that aborted it is the one worth reporting; on a
		// healthy transaction this releases the cursor's resources rather than
		// holding them to commit.
		_, _ = t.tx.Exec(t.ctx, "CLOSE "+name)
	}()

	fetch := fmt.Sprintf("FETCH FORWARD %d FROM %s", scanBatch, name)
	for {
		batch, err := fetchBatch(t, fetch, decode)
		if err != nil {
			return err
		}
		for _, v := range batch {
			if err := yield(v); err != nil {
				return err
			}
		}
		// A short batch means the cursor is exhausted, which saves the empty
		// FETCH a full final batch would otherwise need.
		if len(batch) < scanBatch {
			return nil
		}
	}
}

// fetchBatch pulls one FETCH into memory. It exists as its own function so the
// result set is closed by the time the batch reaches a callback.
func fetchBatch[T any](t *pgTx, fetch string, decode func(pgx.Rows) (T, error)) ([]T, error) {
	rows, err := t.tx.Query(t.ctx, fetch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	batch := make([]T, 0, scanBatch)
	for rows.Next() {
		v, err := decode(rows)
		if err != nil {
			return nil, err
		}
		batch = append(batch, v)
	}
	return batch, rows.Err()
}

// nextCursorName names a cursor uniquely within the transaction, and by the
// shape of the query behind it across transactions. Names are never reused
// within a transaction, even after a CLOSE, so a scan re-entered from another
// scan's callback cannot collide with the cursor it is nested inside.
//
// The query hash is there for pgx's statement cache, which is keyed by SQL
// text and lives per connection: the FETCH for a cursor is prepared once and
// its result description reused for every later FETCH with the same text. Two
// transactions on one connection each open their first cursor, so a name made
// of the sequence alone would hand the second scan the first one's column
// description — and a change-log scan followed by an entity scan fails with
// "bind message has 8 result formats but query has 6 columns". Folding the
// query in makes same-named cursors same-shaped.
func (t *pgTx) nextCursorName(query string) string {
	t.cursorSeq++
	h := fnv.New32a()
	_, _ = h.Write([]byte(query))
	return fmt.Sprintf("syncplane_cur_%d_%08x", t.cursorSeq, h.Sum32())
}

// limitArg renders the contract's "limit <= 0 means unlimited" for a SQL LIMIT
// placeholder. Postgres reads LIMIT NULL as unlimited, so the rule travels as a
// value and the query text stays single-shaped — where SQLite spells the same
// thing as a negative limit.
func limitArg(limit int) *int64 {
	if limit <= 0 {
		return nil
	}
	n := int64(limit)
	return &n
}
