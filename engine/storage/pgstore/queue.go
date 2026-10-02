package pgstore

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A write transaction does not send its writes one at a time. Every statement
// whose result it does not need — the row upsert, the index-entry replacement,
// the savepoint that brackets them — is appended to a queue instead, and the
// whole queue travels as one pgx batch at the next moment a round trip is
// unavoidable: a read, a statement whose command tag is inspected, or COMMIT.
//
// The statements that open a transaction — BEGIN, and a write transaction's
// workspace lock — wait in the same queue (see beginTx), so they travel with
// the transaction's first statement instead of costing a round trip each.
//
// The cost this removes is latency, not work. A push of N mutations issues
// three write-only statements per mutation and reads nothing between them, so
// under a per-statement Exec it pays N×3 round trips to a database that is
// idle for almost all of them; batched, the same statements are one. Postgres
// executes them identically and in the same order either way.
//
// Two rules keep the semantics unchanged:
//
//   - Read-your-writes. Every read travels in one batch with the queue,
//     behind it (see sendWith, and query, queryRow and exec, which are the only
//     ways this package reaches the connection for anything but a queued write
//     or the ROLLBACK in end), so a GetRow after a PutRow sees the row. The
//     memoized workspace scalars, index states and the in-memory sequence
//     answer without a statement and so need no send.
//   - Error attribution moves, failure does not. A statement that fails inside
//     a batch surfaces from whichever operation sent it rather than from the
//     one that wrote it, and aborts the transaction exactly as it would have
//     unbatched. The transaction is then dead: failed holds the error, every
//     later operation returns it, and WithWriteTx rolls back. No caller can
//     write past a storage error either way — a savepoint cannot rewind it,
//     because the batch may have carried statements from savepoints that have
//     since been released.
//
// A read transaction queues only its BEGIN, which its first read carries, and
// the two statements that open a scan cursor (see scanCursor), which it sends
// at once. Its queue is otherwise empty, so its reads travel alone.

// maxQueuedStatements caps how many statements one transaction holds unsent.
//
// pgx writes a whole batch before reading any of its results, so the cap bounds
// what both the client and the server buffer at once. It is far above the three
// statements a mutation writes and below the point where a very large push
// would hold its every row value in memory twice; a batch that reaches it is
// sent on the spot, which costs a round trip and changes nothing else.
const maxQueuedStatements = 1000

// queuedStmt is one statement waiting for the next send, with the arguments it
// was queued with. The caller must not retain the argument slice's backing
// values past the transaction, which no caller here does.
type queuedStmt struct {
	sql  string
	args []any
}

// queue defers sql to the transaction's next send, and reports only what has
// already gone wrong: a statement queued here is reported by the operation that
// sends it, not by this call.
func (t *pgTx) queue(sql string, args ...any) error {
	if t.failed != nil {
		return t.failed
	}
	t.queued = append(t.queued, queuedStmt{sql: sql, args: args})
	if len(t.queued) >= maxQueuedStatements {
		return t.send()
	}
	return nil
}

// send writes every queued statement as one batch and drains the results,
// returning the first error any of them produced. The queue is emptied whether
// or not the batch succeeded — after an error the transaction is aborted and
// nothing queued behind it could run anyway.
func (t *pgTx) send() error {
	if t.failed != nil {
		return t.failed
	}
	if len(t.queued) == 0 {
		return nil
	}
	_, err := t.sendWith()
	return err
}

// sendWith writes every queued statement and then reads as one batch, and
// drains the queued statements' results. When reads is empty, or a queued
// statement failed, it closes the batch and returns its error, which then
// fails the transaction. Otherwise it returns the batch positioned at the
// first of reads, and the caller must read each of them and close the batch
// before the transaction issues anything else.
//
// A read's own failure does not fail the transaction here: it is reported by
// the read, as it would be had it travelled alone.
func (t *pgTx) sendWith(reads ...queuedStmt) (pgx.BatchResults, error) {
	if t.failed != nil {
		return nil, t.failed
	}
	batch := &pgx.Batch{}
	for _, s := range t.queued {
		batch.Queue(s.sql, s.args...)
	}
	for _, s := range reads {
		batch.Queue(s.sql, s.args...)
	}
	n := len(t.queued)
	t.queued = t.queued[:0]
	if n > 0 {
		t.sends++
	}

	br := t.conn.SendBatch(t.ctx, batch)
	var first error
	// Every result is drained even after a failure: the connection carries one
	// per queued statement and cannot serve another query until they are read.
	for i := 0; i < n; i++ {
		if _, err := br.Exec(); err != nil && first == nil {
			first = err
		}
	}
	if first == nil && len(reads) > 0 {
		return br, nil
	}
	if err := br.Close(); err != nil && first == nil {
		first = err
	}
	if first != nil {
		t.failed = first
	}
	return nil, first
}

// exec runs a statement whose command tag the caller inspects, in one batch
// with the queue so it runs in the order it was written.
func (t *pgTx) exec(sql string, args ...any) (pgconn.CommandTag, error) {
	if t.failed != nil {
		return pgconn.CommandTag{}, t.failed
	}
	if len(t.queued) == 0 {
		return t.conn.Exec(t.ctx, sql, args...)
	}
	br, err := t.sendWith(queuedStmt{sql: sql, args: args})
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := br.Exec()
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	return tag, err
}

// query runs a read in one batch with the queue, so the read sees this
// transaction's own writes. The caller must close the rows before it issues
// another statement.
func (t *pgTx) query(sql string, args ...any) (pgx.Rows, error) {
	if t.failed != nil {
		return nil, t.failed
	}
	if len(t.queued) == 0 {
		return t.conn.Query(t.ctx, sql, args...)
	}
	br, err := t.sendWith(queuedStmt{sql: sql, args: args})
	if err != nil {
		return nil, err
	}
	rows, err := br.Query()
	if err != nil {
		_ = br.Close()
		return nil, err
	}
	return &batchRows{Rows: rows, br: br}, nil
}

// queryRow runs a single-row read in one batch with the queue. A failed send
// is reported from the returned row's Scan, which is where pgx reports a query
// failure too.
func (t *pgTx) queryRow(sql string, args ...any) pgx.Row {
	if t.failed != nil {
		return errRow{err: t.failed}
	}
	if len(t.queued) == 0 {
		return t.conn.QueryRow(t.ctx, sql, args...)
	}
	br, err := t.sendWith(queuedStmt{sql: sql, args: args})
	if err != nil {
		return errRow{err: err}
	}
	return batchRow{row: br.QueryRow(), br: br}
}

// errRow is a pgx.Row that only carries an error, so queryRow can report a
// failed send through the interface its callers already handle.
type errRow struct {
	err error
}

func (r errRow) Scan(...any) error { return r.err }

// batchRow is the row of a read that travelled at the end of a batch. Scan
// closes the batch, which frees the connection for the next statement.
type batchRow struct {
	row pgx.Row
	br  pgx.BatchResults
}

func (r batchRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if cerr := r.br.Close(); err == nil {
		err = cerr
	}
	return err
}

// batchRows is the result set of a read that travelled at the end of a batch.
// Exhausting or closing it closes the batch, which frees the connection for
// the next statement; Err reports a failure of either.
type batchRows struct {
	pgx.Rows
	br       pgx.BatchResults
	closeErr error
	closed   bool
}

func (r *batchRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.Close()
	return false
}

func (r *batchRows) Close() {
	if r.closed {
		return
	}
	r.closed = true
	r.Rows.Close()
	r.closeErr = r.br.Close()
}

func (r *batchRows) Err() error {
	if err := r.Rows.Err(); err != nil {
		return err
	}
	return r.closeErr
}
