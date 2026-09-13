package pgstore

import (
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
)

// scanBatch is how many rows one FETCH pulls back. It trades round trips
// against the memory a scan holds at once: an unbounded scan is bounded by this
// rather than by the size of its result, which is the point — ScanEntity takes
// no limit, and bootstrap walks whole entities through it.
const scanBatch = 256

// maxDirectScan is the largest limit a scan may serve without a cursor. Past it
// the cursor's paging is what keeps a scan's memory off the size of its result.
//
// It sits above every bounded scan the engine issues at its default limits —
// the largest is a pull page, which asks for one row more than MaxPullChanges
// (4097 by default) so it can tell a full page from a final one — and a caller
// that raises those limits keeps the direct path until it asks for more rows
// than it would want buffered anyway.
const maxDirectScan = 8192

// scanRows streams query's rows to yield, buffering every row before any
// callback runs.
//
// The buffering is what the contract forces, and it is the reason this
// adapter has a mechanism sqlitestore has no counterpart for: a scan callback
// may issue further reads on the same transaction (ScanIndex re-reads each
// candidate row, ScanChanges reads FieldDeltas), and pgx cannot run a second
// query on a connection whose result set is still open. So rows are decoded
// into memory and their result set closed BEFORE any callback runs — while
// yield executes, the connection is idle and free for the re-entrant read.
//
// limit is the scan's own LIMIT, and decides how the rows are read: a bounded
// one small enough to hold at once takes a single query, and everything else —
// an unlimited scan, or a limit past maxDirectScan — pages through a
// server-side cursor at three round trips or more.
//
// decode runs with the result set open and must only Scan; yield runs with it
// closed and may do anything a ReadTx can.
func scanRows[T any](
	t *pgTx,
	query string,
	args []any,
	limit int,
	decode func(pgx.Rows) (T, error),
	yield func(T) error,
) error {
	if limit > 0 && limit <= maxDirectScan {
		batch, err := queryBatch(t, query, args, min(limit, scanBatch), decode)
		if err != nil {
			return err
		}
		return yieldAll(batch, yield)
	}
	return scanCursor(t, query, args, decode, yield)
}

// scanCursor streams query's rows to yield through a server-side cursor, one
// FETCH at a time. See scanRows for when a scan comes here.
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
		batch, err := queryBatch(t, fetch, nil, scanBatch, decode)
		if err != nil {
			return err
		}
		if err := yieldAll(batch, yield); err != nil {
			return err
		}
		// A short batch means the cursor is exhausted, which saves the empty
		// FETCH a full final batch would otherwise need.
		if len(batch) < scanBatch {
			return nil
		}
	}
}

// queryBatch runs one statement and decodes its whole result into memory. It
// exists as its own function so the result set is closed by the time the batch
// reaches a callback.
func queryBatch[T any](t *pgTx, query string, args []any, capacity int, decode func(pgx.Rows) (T, error)) ([]T, error) {
	rows, err := t.tx.Query(t.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	batch := make([]T, 0, capacity)
	for rows.Next() {
		v, err := decode(rows)
		if err != nil {
			return nil, err
		}
		batch = append(batch, v)
	}
	return batch, rows.Err()
}

func yieldAll[T any](batch []T, yield func(T) error) error {
	for _, v := range batch {
		if err := yield(v); err != nil {
			return err
		}
	}
	return nil
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
