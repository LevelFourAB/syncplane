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
// The cost this removes is latency, not work. A push of N mutations issues
// three write-only statements per mutation and reads nothing between them, so
// under a per-statement Exec it pays N×3 round trips to a database that is
// idle for almost all of them; batched, the same statements are one. Postgres
// executes them identically and in the same order either way.
//
// Two rules keep the semantics unchanged:
//
//   - Read-your-writes. Every read sends the queue first (see query, queryRow
//     and exec, which are the only ways this package reaches the transaction
//     for anything but a queued write), so a GetRow after a PutRow sees the
//     row. The memoized workspace scalars and the in-memory sequence answer
//     without a statement and so need no send.
//   - Error attribution moves, failure does not. A statement that fails inside
//     a batch surfaces from whichever operation sent it rather than from the
//     one that wrote it, and aborts the transaction exactly as it would have
//     unbatched. The transaction is then dead: failed holds the error, every
//     later operation returns it, and WithWriteTx rolls back. No caller can
//     write past a storage error either way — a savepoint cannot rewind it,
//     because the batch may have carried statements from savepoints that have
//     since been released.
//
// A read transaction queues only the two statements that open a scan cursor
// (see scanCursor), and sends them at once. Its queue is empty between
// operations, so every other send is a no-op.

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
	batch := &pgx.Batch{}
	for _, s := range t.queued {
		batch.Queue(s.sql, s.args...)
	}
	n := len(t.queued)
	t.queued = t.queued[:0]
	t.sends++

	br := t.tx.SendBatch(t.ctx, batch)
	var first error
	// Every result is drained even after a failure: the connection carries one
	// per queued statement and cannot serve another query until they are read.
	for i := 0; i < n; i++ {
		if _, err := br.Exec(); err != nil && first == nil {
			first = err
		}
	}
	if err := br.Close(); err != nil && first == nil {
		first = err
	}
	if first != nil {
		t.failed = first
	}
	return first
}

// exec runs a statement whose command tag the caller inspects, sending the
// queue first so it runs in the order it was written.
func (t *pgTx) exec(sql string, args ...any) (pgconn.CommandTag, error) {
	if err := t.send(); err != nil {
		return pgconn.CommandTag{}, err
	}
	return t.tx.Exec(t.ctx, sql, args...)
}

// query runs a read, sending the queue first so the read sees this
// transaction's own writes.
func (t *pgTx) query(sql string, args ...any) (pgx.Rows, error) {
	if err := t.send(); err != nil {
		return nil, err
	}
	return t.tx.Query(t.ctx, sql, args...)
}

// queryRow runs a single-row read, sending the queue first. A send that fails
// is reported from the returned row's Scan, which is where pgx reports a query
// failure too.
func (t *pgTx) queryRow(sql string, args ...any) pgx.Row {
	if err := t.send(); err != nil {
		return errRow{err: err}
	}
	return t.tx.QueryRow(t.ctx, sql, args...)
}

// errRow is a pgx.Row that only carries an error, so queryRow can report a
// failed send through the interface its callers already handle.
type errRow struct {
	err error
}

func (r errRow) Scan(...any) error { return r.err }
