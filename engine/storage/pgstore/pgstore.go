// Package pgstore implements storage.Adapter on PostgreSQL via pgx v5.
//
// It is the adapter for deployments that outgrow a single process. Unlike
// sqlitestore, several server nodes may share one database, and writes to
// different workspaces commit concurrently instead of queueing behind one
// writer connection — the concurrency the storage contract permits and SQLite
// declines to take.
//
// # Connection model
//
// One pgxpool.Pool serves both reads and writes; a transaction holds one
// connection for its duration. There is no reader/writer split because there
// is nothing to split around: Postgres readers never block on writers, so the
// separate pool sqlitestore keeps to escape its single write lock buys nothing
// here.
//
// # Write mutual exclusion
//
// WithWriteTx takes pg_advisory_xact_lock over a hash of (namespace,
// workspace_id) as its FIRST statement, before invoking fn. The contract
// demands exclusion for the whole of fn including the reads it makes before
// its first write (the push path reads a client watermark, then writes it
// back), and row locks cannot give that: lazy registration means a workspace's
// first write has no workspaces row to lock, and SELECT ... FOR UPDATE on a
// row that does not exist locks nothing.
//
// The lock is keyed by a 64-bit hash, so two workspaces can collide and
// serialize against each other needlessly. That costs throughput, never
// correctness — and at 64 bits it takes on the order of 2^32 workspaces before
// a collision is likely at all.
//
// # Isolation
//
// Read transactions run REPEATABLE READ READ ONLY. READ COMMITTED would take a
// fresh snapshot per statement, and since every scan here interleaves reads
// (below), the single consistent snapshot WithReadTx promises would quietly not
// hold across a scan.
//
// Write transactions run READ COMMITTED: the advisory lock, not the isolation
// level, is what makes their pre-write reads stable. They still retry on
// serialization failure (40001) and deadlock (40P01), which cross-table writes
// inside one workspace can produce; the contract licenses re-running fn.
//
// # Scans and re-entrant reads
//
// Every scan method streams through a server-side cursor, decoding one FETCH
// into memory before handing rows to the callback (see scanCursor). This is
// the one mechanism here that is not a transliteration of sqlitestore, and it
// exists because the engine reads from INSIDE scan callbacks — ScanIndex
// re-reads each candidate row, ScanChanges reads FieldDeltas to ship a text
// delta. SQLite tolerates interleaved statements on one connection; pgx does
// not, so holding one result set open across the callback would fail on the
// engine's first indexed query or first pull of a text entity. Buffering the
// whole result instead would work but is unbounded: ScanEntity has no limit and
// compaction walks whole entities through it.
//
// # Collation and integer width
//
// Every text column is declared COLLATE "C". The engine compares keys with
// Go's <, which is bytewise, and shares that order with the adapter across page
// boundaries — under a locale collation an afterKey scan silently drops or
// repeats rows rather than failing. Index entry keys are bytea, which Postgres
// already compares bytewise.
//
// Postgres has no unsigned bigint, so sequences, versions and the store epoch
// round-trip through int64 by reinterpreting the bits. The epoch matters most:
// it is a genuinely random 64 bits (see ReadTx.StoreEpoch), not a counter that
// stays inside int63.
//
// # Re-entrancy
//
// Opening a write transaction from inside another write transaction on the same
// workspace deadlocks: the inner transaction takes a second pooled connection
// and waits for an advisory lock the outer one holds until commit. This is the
// same trap sqlitestore has for a different reason, and the server's re-entrancy
// guards (Mutate, Invoke, CollectGarbage) keep it from being reachable.
package pgstore

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/storage"
)

// migrations[i] takes a database at ledger version i to i+1. Index 0 is the
// baseline — the current shape stated once, as sqlitestore's schema is. Later
// structural changes append here, and the ledger is what makes them
// replayable.
//
// Every text column is COLLATE "C" (see the package comment). It is applied
// even to columns nothing orders by — the actor's subject and system label,
// consumer names — so the rule is "all of them" rather than a judgement call
// per column that a later index turns into a bug.
var migrations = []string{
	// 1: the baseline schema.
	`
	CREATE TABLE workspaces (
		namespace           TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id        TEXT COLLATE "C" NOT NULL,
		last_seq            BIGINT NOT NULL DEFAULT 0,
		tombstone_horizon   BIGINT NOT NULL DEFAULT 0,
		store_epoch         BIGINT NOT NULL DEFAULT 0,
		field_delta_horizon BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (namespace, workspace_id)
	);

	CREATE TABLE rows (
		namespace     TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id  TEXT COLLATE "C" NOT NULL,
		entity        TEXT COLLATE "C" NOT NULL,
		key           TEXT COLLATE "C" NOT NULL,
		value         BYTEA,
		version       BIGINT NOT NULL,
		deleted       BOOLEAN NOT NULL DEFAULT FALSE,
		updated_at    BIGINT NOT NULL DEFAULT 0,
		actor_subject TEXT COLLATE "C" NOT NULL DEFAULT '',
		actor_system  TEXT COLLATE "C" NOT NULL DEFAULT '',
		PRIMARY KEY (namespace, workspace_id, entity, key)
	);
	-- ScanChanges pages by the whole (version, entity, key) tuple, so the index
	-- carries all three: sqlitestore stops at version and sorts the tie-break,
	-- which a fresh schema has no reason to inherit.
	CREATE INDEX rows_by_version ON rows (namespace, workspace_id, version, entity, key);
	-- Partial, because deleted = true is the only collectable state, which keeps
	-- the GC index proportional to the dead rows rather than the live ones.
	CREATE INDEX rows_gc ON rows (namespace, workspace_id, updated_at) WHERE deleted;
	-- Backs ScanEntityByVersionDesc. The key order matches the recency window's
	-- tie-break (version descending, key ascending) so a page boundary lands
	-- where the in-memory top-K would have put it.
	CREATE INDEX rows_by_entity_version
		ON rows (namespace, workspace_id, entity, version DESC, key) WHERE NOT deleted;

	CREATE TABLE clients (
		namespace        TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id     TEXT COLLATE "C" NOT NULL,
		client_id        TEXT COLLATE "C" NOT NULL,
		last_mutation_id BIGINT NOT NULL DEFAULT 0,
		updated_at       BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (namespace, workspace_id, client_id)
	);
	CREATE INDEX clients_gc ON clients (namespace, workspace_id, updated_at);

	-- Server-global: app_id is the partition, because one engine hosts several
	-- apps and reloads every app's entities on boot before serving.
	CREATE TABLE schemas (
		app_id         TEXT COLLATE "C" PRIMARY KEY,
		descriptor_set BYTEA NOT NULL,
		revision       TEXT COLLATE "C" NOT NULL,
		updated_at     BIGINT NOT NULL DEFAULT 0
	);

	-- One row per (text field, write), holding the fragment that write added so
	-- a pull can ship a keystroke instead of a document. The primary key is
	-- ordered so the per-field span read (one row, one field, seq > cursor) is a
	-- range scan of its tail. frag is empty for a gap marker.
	CREATE TABLE field_deltas (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		entity       TEXT COLLATE "C" NOT NULL,
		key          TEXT COLLATE "C" NOT NULL,
		field        TEXT COLLATE "C" NOT NULL,
		seq          BIGINT NOT NULL,
		frag         BYTEA NOT NULL,
		updated_at   BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (namespace, workspace_id, entity, key, field, seq)
	);
	CREATE INDEX field_deltas_gc ON field_deltas (namespace, workspace_id, updated_at);

	CREATE TABLE text_floors (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		entity       TEXT COLLATE "C" NOT NULL,
		key          TEXT COLLATE "C" NOT NULL,
		field        TEXT COLLATE "C" NOT NULL,
		vv           BYTEA NOT NULL,
		PRIMARY KEY (namespace, workspace_id, entity, key, field)
	);

	CREATE TABLE idempotency (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		key          TEXT COLLATE "C" NOT NULL,
		seq          BIGINT NOT NULL,
		result       BYTEA NOT NULL,
		updated_at   BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (namespace, workspace_id, key)
	);
	CREATE INDEX idempotency_gc ON idempotency (namespace, workspace_id, updated_at);

	-- k is BYTEA, which Postgres compares bytewise — the order the engine's key
	-- encoder is built for, and the reason this must never become a text column
	-- whatever its collation. The key column sits in the primary key after k for
	-- uniqueness only: k already ends with the row key.
	CREATE TABLE index_entries (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		entity       TEXT COLLATE "C" NOT NULL,
		idx          TEXT COLLATE "C" NOT NULL,
		k            BYTEA NOT NULL,
		key          TEXT COLLATE "C" NOT NULL,
		PRIMARY KEY (namespace, workspace_id, entity, idx, k, key)
	);
	-- Entries are replaced and deleted per ROW, and neither caller knows which
	-- indexes the row occupies; this is what makes that affordable.
	CREATE INDEX index_entries_by_row
		ON index_entries (namespace, workspace_id, entity, key);

	CREATE TABLE index_state (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		entity       TEXT COLLATE "C" NOT NULL,
		idx          TEXT COLLATE "C" NOT NULL,
		spec_hash    TEXT COLLATE "C" NOT NULL,
		ready        BOOLEAN NOT NULL DEFAULT FALSE,
		PRIMARY KEY (namespace, workspace_id, entity, idx)
	);

	CREATE TABLE consumer_positions (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		consumer     TEXT COLLATE "C" NOT NULL,
		seq          BIGINT NOT NULL DEFAULT 0,
		entity       TEXT COLLATE "C" NOT NULL DEFAULT '',
		key          TEXT COLLATE "C" NOT NULL DEFAULT '',
		updated_at   BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (namespace, workspace_id, consumer)
	);
	CREATE INDEX consumer_positions_gc
		ON consumer_positions (namespace, workspace_id, updated_at);

	-- No GC index: shadows are deleted per row or wholesale with their
	-- consumer, and the primary key's (namespace, workspace_id, consumer)
	-- prefix is the range scan for that.
	CREATE TABLE consumer_shadows (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		consumer     TEXT COLLATE "C" NOT NULL,
		entity       TEXT COLLATE "C" NOT NULL,
		key          TEXT COLLATE "C" NOT NULL,
		snapshot     BYTEA NOT NULL,
		PRIMARY KEY (namespace, workspace_id, consumer, entity, key)
	);
	`,
	// 2: verdicts on refused mutations, so a retry that follows a lost push
	// response is answered with the rejection instead of a bare ack. NULL until
	// a client has had something refused, which is the common case.
	`ALTER TABLE clients ADD COLUMN rejections BYTEA;`,
	// Cursor-state entries: the membership sets of issued pull cursors, keyed
	// by the (id, gen) reference the cursor carries. See cursorstate.go for
	// the pruning that keeps the table bounded.
	`
	CREATE TABLE cursor_state (
		namespace    TEXT COLLATE "C" NOT NULL DEFAULT '',
		workspace_id TEXT COLLATE "C" NOT NULL,
		id           BIGINT NOT NULL,
		gen          BIGINT NOT NULL,
		state        BYTEA NOT NULL,
		written_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (namespace, workspace_id, id, gen)
	);
	`,
}

// migrationLockKey serializes schema migration across nodes. Several servers
// booting at once would otherwise race to apply the same migration, and CREATE
// TABLE is not idempotent. The value is "syncplan" in ASCII, chosen to be
// recognisable in pg_locks rather than for any property of the number.
const migrationLockKey int64 = 0x73796E63706C616E

// maxWriteAttempts bounds WithWriteTx's retry loop. Serialization failures and
// deadlocks are transient by definition, so a closure that cannot get through
// in this many tries is hitting something structural and should surface as an
// error rather than spin.
const maxWriteAttempts = 10

// Store implements storage.Adapter. Stores derived via Namespace share the
// pool; only the Store returned by Open owns (and closes) it.
type Store struct {
	pool  *pgxpool.Pool
	ns    string
	owner bool
}

// Option configures Open.
type Option func(*config)

type config struct {
	ns       string
	maxConns int32
}

// WithNamespace scopes the store to an app namespace. Namespaces partition
// workspaces, rows and client watermarks within one database; the default is
// "". Equivalent to Open(...).Namespace(ns) but spares the intermediate store.
func WithNamespace(ns string) Option {
	return func(c *config) { c.ns = ns }
}

// WithMaxConns caps the connection pool. A write transaction holds a connection
// while it waits for its workspace's advisory lock, so the pool bounds how many
// writers can be in flight — not how many can make progress, since the lock
// holder always has its own connection. Zero leaves pgx's default (or whatever
// the DSN's pool_max_conns says).
func WithMaxConns(n int32) Option {
	return func(c *config) { c.maxConns = n }
}

// Open connects to the Postgres database named by dsn (a libpq URI or key/value
// string) and applies any outstanding migrations.
//
// Several processes may Open the same database concurrently: migration is
// serialized by an advisory lock, which is the difference from sqlitestore's
// "never open the same file twice". Use Namespace to multiplex apps over one
// database.
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: parse dsn: %w", err)
	}
	if cfg.maxConns > 0 {
		poolCfg.MaxConns = cfg.maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, ns: cfg.ns, owner: true}, nil
}

// migrate brings the database up to len(migrations), under an advisory lock so
// that concurrently booting nodes apply each step once.
//
// The lock is taken FIRST, before even the ledger exists. CREATE TABLE IF NOT
// EXISTS looks idempotent but is not concurrency-safe: two sessions both find
// the table missing, both create it, and the loser fails on a unique violation
// in pg_type rather than shrugging. Nothing here may touch the schema outside
// the lock.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: acquire connection for migration: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("pgstore: take migration lock: %w", err)
	}
	defer func() {
		// Best effort: the lock is session-scoped, so a failure here is undone
		// by the connection returning to the pool and eventually closing.
		_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("pgstore: create migration ledger: %w", err)
	}

	var version int
	if err := conn.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return fmt.Errorf("pgstore: read schema version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf("pgstore: database schema version %d is newer than this binary (max %d)", version, len(migrations))
	}
	for ; version < len(migrations); version++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, migrations[version]); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: migration %d: %w", version+1, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, version+1); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: migration %d: record version: %w", version+1, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgstore: migration %d: commit: %w", version+1, err)
		}
	}
	return nil
}

// Namespace returns a view of the store scoped to the given app namespace,
// sharing the pool. Close on a derived store is a no-op — the Store returned by
// Open owns the pool and must outlive every derived store. The server-global
// schema catalog is shared across namespaces.
func (s *Store) Namespace(ns string) storage.Adapter {
	return &Store{pool: s.pool, ns: ns}
}

// Close closes the pool if this store owns it (came from Open); on a
// Namespace-derived store it is a no-op.
func (s *Store) Close() error {
	if !s.owner {
		return nil
	}
	s.pool.Close()
	return nil
}

// workspaceLockKey hashes (namespace, workspace_id) into the advisory-lock
// keyspace. The salt keeps syncplane's locks clear of any other application
// taking advisory locks on the same database; the separator keeps ("a", "bc")
// and ("ab", "c") apart.
func workspaceLockKey(ns, ws string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("syncplane:workspace\x00"))
	_, _ = h.Write([]byte(ns))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(ws))
	return int64(h.Sum64())
}

// readOnlyTx narrows a pgTx to the read surface. Embedding the INTERFACE rather
// than the struct is what does the work: only ReadTx's methods are promoted, so
// a closure that type-asserts its way to storage.WriteTx gets a failed
// assertion instead of writes WithReadTx would silently roll back.
type readOnlyTx struct {
	storage.ReadTx
}

func (s *Store) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		// REPEATABLE READ, not the default: a scan's callback issues further
		// reads on this transaction, and under READ COMMITTED each of them
		// would take its own snapshot.
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return fn(readOnlyTx{&pgTx{tx: tx, ctx: ctx, ns: s.ns, ws: workspaceID}})
}

func (s *Store) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	var lastErr error
	for attempt := 0; attempt < maxWriteAttempts; attempt++ {
		err := s.writeOnce(ctx, workspaceID, fn)
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err
		}
		lastErr = err
		// A short, growing pause: two transactions that just deadlocked will do
		// it again if both retry immediately.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Millisecond):
		}
	}
	return fmt.Errorf("pgstore: write transaction for workspace %q gave up after %d attempts: %w",
		workspaceID, maxWriteAttempts, lastErr)
}

// writeOnce is one attempt of WithWriteTx: a fresh transaction, the workspace
// lock, and one invocation of fn.
func (s *Store) writeOnce(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Before fn, unconditionally: the contract's exclusion covers the reads fn
	// makes before its first write, and an advisory lock — unlike a row lock —
	// does not need the workspace to exist yet. It releases on commit or
	// rollback.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`,
		workspaceLockKey(s.ns, workspaceID)); err != nil {
		return err
	}
	// The workspaces row is NOT created here: pgTx.register mints it on the
	// transaction's first actual write. A closure that only reads (a read-only
	// Mutate, a push whose every mutation was already applied) must leave the
	// workspace unregistered — see the WithWriteTx contract.
	if err := fn(&pgTx{tx: tx, ctx: ctx, ns: s.ns, ws: workspaceID}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// isRetryable reports whether err is a transient conflict WithWriteTx may
// resolve by running fn again: a serialization failure or a deadlock. Both can
// come out of fn rather than out of the commit, so the whole error chain is
// searched.
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "40001", // serialization_failure
		"40P01": // deadlock_detected
		return true
	}
	return false
}

func (s *Store) ListWorkspaceIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT workspace_id FROM workspaces WHERE namespace = $1 ORDER BY workspace_id`, s.ns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListPendingWorkspaceIDs answers "which workspaces owe this consumer work" as
// a single indexed query: the workspace scan is in primary-key order, and each
// row probes consumer_positions on its own primary key prefix (namespace,
// workspace_id, consumer). A workspace with no position row for the consumer is
// pending by the LEFT JOIN — a first run has acted on nothing.
//
// A non-empty entity or key is pending whatever the sequence says: that is a
// sub-position inside a same-sequence group, so the group has changes after it
// even when the sequence has reached the head. Only a clean boundary — the
// cursor a drained pass records — means caught up.
func (s *Store) ListPendingWorkspaceIDs(ctx context.Context, consumer, after string, limit int) ([]string, error) {
	if consumer == "" {
		return nil, fmt.Errorf("pgstore: consumer name is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("pgstore: limit must be positive, got %d", limit)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT w.workspace_id
		 FROM workspaces w
		 LEFT JOIN consumer_positions p
		   ON p.namespace = w.namespace
		   AND p.workspace_id = w.workspace_id
		   AND p.consumer = $1
		 WHERE w.namespace = $2 AND w.workspace_id > $3
		   AND (p.seq IS NULL OR p.seq < w.last_seq OR p.entity <> '' OR p.key <> '')
		 ORDER BY w.workspace_id
		 LIMIT $4`, consumer, s.ns, after, int64(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// PutAppSchema upserts an app's descriptor set into the server-global schema
// catalog (the namespace view is irrelevant here — app_id is the partition).
func (s *Store) PutAppSchema(ctx context.Context, appID string, set []byte, revision string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO schemas (app_id, descriptor_set, revision, updated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (app_id)
		 DO UPDATE SET descriptor_set = excluded.descriptor_set,
		               revision = excluded.revision, updated_at = excluded.updated_at`,
		appID, set, revision, time.Now().UnixMilli(),
	)
	return err
}

func (s *Store) ListAppSchemas(ctx context.Context) ([]storage.AppSchema, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT app_id, descriptor_set, revision, updated_at FROM schemas ORDER BY app_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.AppSchema
	for rows.Next() {
		var (
			a       storage.AppSchema
			updated int64
		)
		if err := rows.Scan(&a.AppID, &a.DescriptorSet, &a.Revision, &updated); err != nil {
			return nil, err
		}
		a.UpdatedAt = time.UnixMilli(updated)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAppSchema(ctx context.Context, appID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM schemas WHERE app_id = $1`, appID)
	return err
}

// newStoreEpoch mints the random identity a workspace's first write stamps on
// itself (see storage.ReadTx.StoreEpoch). It is generated here rather than in
// SQL because Postgres's random() is a float in [0,1) and cannot carry 64 bits;
// the epoch is a full-width random value, not a counter.
//
// Zero reads back as "no epoch", so it is rejected and redrawn.
func newStoreEpoch() (int64, error) {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("pgstore: mint store epoch: %w", err)
		}
		if v := binary.BigEndian.Uint64(b[:]); v != 0 {
			return int64(v), nil
		}
	}
}
