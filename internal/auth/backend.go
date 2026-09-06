package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"modernc.org/sqlite"
	sqlitelib "modernc.org/sqlite/lib"
)

// The Store's SQL is written once, in the dialect both engines share:
// `?` placeholders, BIGINT for numbers and times, BOOLEAN, TEXT, and the
// INSERT ... ON CONFLICT and RETURNING forms both accept. A backend is the
// thin adapter that runs that SQL on one engine and maps its constraint errors
// to the two kinds the Store distinguishes. Everything an engine does
// differently — placeholder syntax, collation, the migration lock — lives
// here and nowhere else.

// constraint classifies a failed write by which constraint refused it.
type constraint int

const (
	constraintNone constraint = iota
	constraintUnique
	constraintForeignKey
)

// querier runs statements, either on a pool or inside a transaction.
type querier interface {
	exec(ctx context.Context, q string, args ...any) (int64, error)
	query(ctx context.Context, q string, args ...any) (rows, error)
}

// rows is the common subset of *sql.Rows and pgx.Rows.
type rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// backend is one database engine.
type backend interface {
	querier
	// tx runs fn inside a transaction, committing when it returns nil and
	// rolling back otherwise. The transaction takes the write lock at open on
	// SQLite, so two writers wait rather than fail.
	tx(ctx context.Context, fn func(querier) error) error
	// lockMigrations serializes concurrently booting nodes. It runs inside the
	// migration transaction, before any DDL, and holds until it commits.
	lockMigrations(ctx context.Context, q querier) error
	// ddl adapts a schema statement to the engine: the collation marker
	// {TEXT} becomes plain TEXT on SQLite and TEXT COLLATE "C" on Postgres.
	ddl(q string) string
	// classify maps an error from exec or query to the constraint that
	// refused the write, or constraintNone.
	classify(err error) constraint
	close() error
}

// sqliteBackend runs on modernc.org/sqlite through database/sql.
type sqliteBackend struct{ db *sql.DB }

// sqliteDSN builds the connection string the way sqlitestore does, escaping
// the path so `?`, `#` and `%` in it do not end the path early or drop the
// pragmas after it.
func sqliteDSN(path string) string {
	params := []string{
		// Write transactions take the lock at BEGIN, so contention waits
		// out busy_timeout instead of failing the upgrade with
		// SQLITE_BUSY_SNAPSHOT.
		"_txlock=immediate",
		"_pragma=busy_timeout(5000)",
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(NORMAL)",
		"_pragma=foreign_keys(1)",
	}
	return "file:" + url.PathEscape(path) + "?" + strings.Join(params, "&")
}

func openSQLite(path string) (*sqliteBackend, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("auth: open sqlite: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("auth: open sqlite: %w", err)
	}
	return &sqliteBackend{db: db}, nil
}

type sqlRows struct{ *sql.Rows }

func (r sqlRows) Close() { _ = r.Rows.Close() }

// sqlQuerier is the part of *sql.DB and *sql.Tx the backend uses.
type sqlQuerier interface {
	ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
}

type sqlAdapter struct{ q sqlQuerier }

func (a sqlAdapter) exec(ctx context.Context, q string, args ...any) (int64, error) {
	res, err := a.q.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (a sqlAdapter) query(ctx context.Context, q string, args ...any) (rows, error) {
	r, err := a.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return sqlRows{r}, nil
}

func (b *sqliteBackend) exec(ctx context.Context, q string, args ...any) (int64, error) {
	return sqlAdapter{b.db}.exec(ctx, q, args...)
}

func (b *sqliteBackend) query(ctx context.Context, q string, args ...any) (rows, error) {
	return sqlAdapter{b.db}.query(ctx, q, args...)
}

func (b *sqliteBackend) tx(ctx context.Context, fn func(querier) error) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(sqlAdapter{tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// lockMigrations is a no-op: the transaction opened with _txlock=immediate
// already holds the file's single write lock.
func (b *sqliteBackend) lockMigrations(context.Context, querier) error { return nil }

func (b *sqliteBackend) ddl(q string) string {
	return strings.ReplaceAll(q, "{TEXT}", "TEXT")
}

func (b *sqliteBackend) classify(err error) constraint {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return constraintNone
	}
	switch se.Code() {
	case sqlitelib.SQLITE_CONSTRAINT_UNIQUE, sqlitelib.SQLITE_CONSTRAINT_PRIMARYKEY:
		return constraintUnique
	case sqlitelib.SQLITE_CONSTRAINT_FOREIGNKEY:
		return constraintForeignKey
	}
	return constraintNone
}

func (b *sqliteBackend) close() error { return b.db.Close() }

// pgBackend runs on pgx.
type pgBackend struct{ pool *pgxpool.Pool }

func openPostgres(ctx context.Context, dsn string) (*pgBackend, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("auth: open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("auth: open postgres: %w", err)
	}
	return &pgBackend{pool: pool}, nil
}

// pgQuerier is the part of *pgxpool.Pool and pgx.Tx the backend uses.
type pgQuerier interface {
	Exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, q string, args ...any) (pgx.Rows, error)
}

type pgAdapter struct{ q pgQuerier }

// positional rewrites `?` placeholders to the `$n` form Postgres requires.
// The Store's SQL carries no `?` in any other position.
func positional(q string) string {
	var b strings.Builder
	n := 0
	for _, c := range q {
		if c == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

func (a pgAdapter) exec(ctx context.Context, q string, args ...any) (int64, error) {
	tag, err := a.q.Exec(ctx, positional(q), args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (a pgAdapter) query(ctx context.Context, q string, args ...any) (rows, error) {
	return a.q.Query(ctx, positional(q), args...)
}

func (b *pgBackend) exec(ctx context.Context, q string, args ...any) (int64, error) {
	return pgAdapter{b.pool}.exec(ctx, q, args...)
}

func (b *pgBackend) query(ctx context.Context, q string, args ...any) (rows, error) {
	return pgAdapter{b.pool}.query(ctx, q, args...)
}

func (b *pgBackend) tx(ctx context.Context, fn func(querier) error) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(pgAdapter{tx}); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// migrationLockKey is the advisory lock the auth migrations run under. It is
// distinct from pgstore's key ("syncplan" in ASCII) so the two stores'
// migrations do not serialize on each other; the value is "syncauth".
const migrationLockKey int64 = 0x73796E6361757468

// lockMigrations takes a transaction-scoped advisory lock, released at
// commit. CREATE TABLE IF NOT EXISTS is not concurrency-safe on its own: two
// sessions can both find the ledger missing and the loser fails on pg_type.
func (b *pgBackend) lockMigrations(ctx context.Context, q querier) error {
	_, err := q.exec(ctx, `SELECT pg_advisory_xact_lock(?)`, migrationLockKey)
	return err
}

// ddl applies COLLATE "C" to every text column. Equality lookups do not care,
// but every ORDER BY here pages by a text key, and a locale collation sorts
// differently from the bytewise comparison the caller sees in Go.
func (b *pgBackend) ddl(q string) string {
	return strings.ReplaceAll(q, "{TEXT}", `TEXT COLLATE "C"`)
}

func (b *pgBackend) classify(err error) constraint {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return constraintNone
	}
	switch pe.Code {
	case "23505":
		return constraintUnique
	case "23503":
		return constraintForeignKey
	}
	return constraintNone
}

func (b *pgBackend) close() error {
	b.pool.Close()
	return nil
}
