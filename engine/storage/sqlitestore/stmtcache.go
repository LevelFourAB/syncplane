package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// stmtCache holds one prepared statement per SQL text for one pool.
//
// modernc.org/sqlite keeps no statement cache of its own, so a statement handed
// over as text is re-parsed on every call — tens of microseconds each, which a
// transaction applying thousands of statements pays thousands of times. A
// statement prepared on the pool is parsed once per connection instead:
// database/sql records the driver statement next to the connection it was
// prepared on, and sql.Tx.Stmt reuses that one when the transaction runs on the
// same connection. The writer pool is a single connection, so after warm-up
// every write statement is parsed once for the life of the process.
//
// Preparation is deferred rather than done on first use because sql.DB.Prepare
// takes a connection of its own: asking for one from inside a transaction that
// already holds the writer pool's only connection blocks until that transaction
// ends, which it cannot do while it is waiting. A miss therefore queues the text
// and lets the caller run it unprepared; warm prepares the queue from outside
// any transaction, so the second transaction to use a text finds it ready.
//
// The cache is keyed by the full text and never evicts, which is safe because
// the texts the transaction surface produces are a bounded set: the ones built
// at runtime vary only in the number of placeholders (readRowsByKey, capped by
// getRowsChunk), in which optional predicates are present (ScanIndex,
// PurgeEntityRows) or in savepoint depth.
//
// Safe for concurrent use.
type stmtCache struct {
	db *sql.DB

	mu      sync.RWMutex
	ready   map[string]*sql.Stmt
	pending map[string]struct{}
}

func newStmtCache(db *sql.DB) *stmtCache {
	return &stmtCache{
		db:      db,
		ready:   make(map[string]*sql.Stmt),
		pending: make(map[string]struct{}),
	}
}

// get returns the pool's prepared statement for query, or nil when there is
// none yet — in which case the text is queued for the next warm and the caller
// has to run it as text.
func (c *stmtCache) get(query string) *sql.Stmt {
	c.mu.RLock()
	stmt, ok := c.ready[query]
	c.mu.RUnlock()
	if ok {
		return stmt
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stmt, ok := c.ready[query]; ok {
		return stmt
	}
	c.pending[query] = struct{}{}
	return nil
}

// warm prepares every queued statement, and must be called with no transaction
// of this goroutine's open on the pool — preparing needs a connection.
//
// A text that fails to prepare is left queued, so it is retried on the next
// warm and runs as text until then; preparation is an optimization, and a
// statement the pool cannot prepare still executes.
func (c *stmtCache) warm(ctx context.Context) {
	c.mu.RLock()
	queued := len(c.pending)
	c.mu.RUnlock()
	if queued == 0 {
		return
	}

	c.mu.Lock()
	texts := make([]string, 0, len(c.pending))
	for q := range c.pending {
		texts = append(texts, q)
	}
	c.mu.Unlock()

	for _, q := range texts {
		stmt, err := c.db.PrepareContext(ctx, q)
		if err != nil {
			continue
		}
		c.mu.Lock()
		if _, dup := c.ready[q]; dup {
			// Another goroutine warmed the same text; keep its statement so
			// every caller shares one, and close the surplus.
			c.mu.Unlock()
			stmt.Close()
			continue
		}
		c.ready[q] = stmt
		delete(c.pending, q)
		c.mu.Unlock()
	}
}

// close releases every prepared statement. The pool must not be closed yet.
func (c *stmtCache) close() error {
	c.mu.Lock()
	stmts := c.ready
	c.ready = make(map[string]*sql.Stmt)
	c.pending = make(map[string]struct{})
	c.mu.Unlock()

	var err error
	for _, stmt := range stmts {
		err = errors.Join(err, stmt.Close())
	}
	return err
}

// prepared returns the pool's statement for query, or nil when the text has to
// run unprepared.
//
// A prepared statement is one SQLite virtual machine, reused rather than
// rebuilt, so two executions of the same text may not overlap on it. Only
// scanRows holds one open across caller code; it says so here, and a statement
// reached from inside such a scan falls back to the text form, which the driver
// compiles into a machine of its own.
//
// The transaction's handle on it is memoized because sql.Tx.Stmt allocates a
// wrapper and registers it with the transaction on every call, and a push of a
// thousand mutations makes thousands of those calls for a handful of texts. The
// transaction closes what it holds when it ends.
func (t *sqliteTx) prepared(query string) *sql.Stmt {
	if _, busy := t.scanning[query]; busy {
		return nil
	}
	if stmt, ok := t.txStmts[query]; ok {
		return stmt
	}
	pooled := t.stmts.get(query)
	if pooled == nil {
		return nil
	}
	stmt := t.tx.Stmt(pooled)
	if t.txStmts == nil {
		t.txStmts = make(map[string]*sql.Stmt)
	}
	t.txStmts[query] = stmt
	return stmt
}

// exec runs query on the transaction, through the pool's prepared statement
// when there is one.
func (t *sqliteTx) exec(query string, args ...any) (sql.Result, error) {
	stmt := t.prepared(query)
	if stmt == nil {
		return t.tx.Exec(query, args...)
	}
	return stmt.Exec(args...)
}

// query runs query on the transaction, through the pool's prepared statement
// when there is one. The rows are the caller's to close, and must be drained
// and closed before it runs anything else on the transaction; a scan that hands
// rows to caller code as it reads them belongs in scanRows.
func (t *sqliteTx) query(query string, args ...any) (*sql.Rows, error) {
	stmt := t.prepared(query)
	if stmt == nil {
		return t.tx.Query(query, args...)
	}
	return stmt.Query(args...)
}

// queryRow runs query on the transaction, through the pool's prepared statement
// when there is one. Scan the row before running anything else on the
// transaction (see query).
func (t *sqliteTx) queryRow(query string, args ...any) *sql.Row {
	stmt := t.prepared(query)
	if stmt == nil {
		return t.tx.QueryRow(query, args...)
	}
	return stmt.QueryRow(args...)
}

// scanRows runs query and hands fn one row at a time, closing the result set
// before it returns. fn may read and write through the same transaction,
// including scanning the same text again — the outer scan holds the prepared
// statement for its duration, so the nested one runs unprepared rather than
// stepping the machine the outer scan is in the middle of.
func (t *sqliteTx) scanRows(query string, args []any, fn func(*sql.Rows) error) error {
	var (
		rows *sql.Rows
		err  error
	)
	if stmt := t.prepared(query); stmt != nil {
		if t.scanning == nil {
			t.scanning = make(map[string]struct{})
		}
		t.scanning[query] = struct{}{}
		defer delete(t.scanning, query)
		rows, err = stmt.Query(args...)
	} else {
		rows, err = t.tx.Query(query, args...)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}
