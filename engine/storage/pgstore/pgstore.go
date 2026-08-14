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
// By default one pgxpool.Pool serves both reads and writes; a transaction
// holds one connection for its duration. There is no reader/writer split
// within one database because there is nothing to split around: Postgres
// readers never block on writers, so the separate pool sqlitestore keeps to
// escape its single write lock buys nothing here.
//
// WithReadReplica adds a second pool, exposed through storage.ReplicaProvider,
// pointed at a hot standby (or at the primary itself). It carries only
// read-only snapshot transactions and the two workspace-list queries — no
// session state, no advisory locks, never migrations — and every floored read
// validates the replica's freshness against the workspace's last_seq inside
// the already-open transaction, falling back to the primary when the replica
// is behind. See replica.go.
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
// A replica read is a time-shifted read: the same REPEATABLE READ snapshot a
// primary read would have taken at some earlier moment. Convergence never
// depends on freshness — the engine's cursors and floors do — which is why
// the replica path bounds staleness per read by workspace sequence rather
// than by replication position.
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
// bootstrap walks whole entities through it.
//
// # Schema and physical layout
//
// Open applies any outstanding migrations at boot, serialized across nodes by
// an advisory lock. Migrate and Statements expose the same ledger to a
// deployment that would rather own its DDL as a deploy step, and
// WithoutMigrations turns the boot-time application into a check that the step
// has run. WithPartitions hash-partitions the workspace-scoped tables at
// creation time, which is the one schema decision this adapter cannot make for
// a database after it holds data. See schema.go.
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
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/storage"
)

// maxWriteAttempts bounds WithWriteTx's retry loop. Serialization failures and
// deadlocks are transient by definition, so a closure that cannot get through
// in this many tries is hitting something structural and should surface as an
// error rather than spin.
const maxWriteAttempts = 10

// Store implements storage.Adapter. Stores derived via Namespace share the
// pool; only the Store returned by Open owns (and closes) it.
type Store struct {
	pool *pgxpool.Pool
	// replica is the read-replica pool, nil unless Open was given
	// WithReadReplica. Only replica.go's reads and the workspace-list queries
	// touch it.
	replica *pgxpool.Pool
	ns      string
	owner   bool
	// leases backs the lease.Provider implementation (see leases.go), shared
	// by every derived namespace so they lease out of one space.
	leases *pgLeases
}

// Option configures Open, Migrate and Statements.
type Option func(*config)

type config struct {
	ns              string
	maxConns        int32
	replicaDSN      string
	replicaMaxConns int32
	partitions      int
	skipMigrations  bool
}

// newConfig applies opts and validates them, so Open, Migrate and Statements
// reject the same input.
func newConfig(opts []Option) (config, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	if p := cfg.partitions; p != 0 && (p < 0 || p > maxPartitions || p&(p-1) != 0) {
		return config{}, fmt.Errorf("pgstore: partitions must be zero or a power of two up to %d, got %d",
			maxPartitions, p)
	}
	return cfg, nil
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

// WithReadReplica adds a read-replica pool at dsn — a hot standby of the
// primary, or the primary itself — and makes the store implement
// storage.ReplicaProvider. The replica serves read-only snapshot transactions
// through that capability and the workspace-list queries; everything else,
// including migrations and leases, stays on the primary.
//
// Open fails when the schema at dsn is not at the primary's migration version,
// which is what catches a DSN that is not a standby of the primary's database.
func WithReadReplica(dsn string) Option {
	return func(c *config) { c.replicaDSN = dsn }
}

// WithReplicaMaxConns caps the replica pool. A replica connection is held for
// the duration of one read transaction, so this bounds concurrent replica
// reads; callers past the cap queue on the pool exactly as they do on the
// primary's. Zero leaves pgx's default (or whatever the replica DSN's
// pool_max_conns says).
func WithReplicaMaxConns(n int32) Option {
	return func(c *config) { c.replicaMaxConns = n }
}

// WithPartitions hash-partitions the workspace-scoped tables by (namespace,
// workspace_id) with the given modulus.
//
// The tables are rows, field_deltas, idempotency, index_entries and
// consumer_shadows. The modulus must be a power of two up to 1024; zero, the
// default, leaves each table whole, and Open, Migrate and Statements reject
// any other value.
//
// It is honoured only when the database has no migrations recorded yet; both
// Open and Migrate otherwise adopt the layout they find. A deploy step may
// therefore choose the layout once and every node boot without the option, and
// a boot naming a different modulus is ignored rather than obeyed. Nothing
// here converts a database that already holds data.
//
// docs/how-to/deploy-for-high-availability.md carries what the layout buys
// and what it costs.
func WithPartitions(n int) Option {
	return func(c *config) { c.partitions = n }
}

// WithoutMigrations makes Open verify the schema instead of applying it: it
// reads the migration ledger and fails unless the database is already at this
// binary's version, leaving Migrate or Statements to have brought it there.
//
// It is what makes a deploy step that owns the DDL authoritative rather than
// advisory, since otherwise the first node to boot with a newer binary
// migrates the database itself. A node opening this way issues no DDL and
// takes no advisory lock, so it may connect as a role with no privilege beyond
// the rows it serves.
//
// Migrate and Statements ignore it, as does WithPartitions alongside it —
// nothing is created for a modulus to apply to.
func WithoutMigrations() Option {
	return func(c *config) { c.skipMigrations = true }
}

// Open connects to the Postgres database named by dsn (a libpq URI or key/value
// string) and applies any outstanding migrations, or under WithoutMigrations
// verifies that something else already has.
//
// Several processes may Open the same database concurrently: migration is
// serialized by an advisory lock, which is the difference from sqlitestore's
// "never open the same file twice". Use Namespace to multiplex apps over one
// database.
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
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
	var schemaErr error
	if cfg.skipMigrations {
		schemaErr = verifySchema(ctx, pool)
	} else {
		schemaErr = migrate(ctx, pool, layout{partitions: cfg.partitions})
	}
	if schemaErr != nil {
		pool.Close()
		return nil, schemaErr
	}
	var replica *pgxpool.Pool
	if cfg.replicaDSN != "" {
		primaryVersion, err := schemaVersion(ctx, pool)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("pgstore: read schema version: %w", err)
		}
		replica, err = openReplica(ctx, cfg.replicaDSN, cfg.replicaMaxConns, primaryVersion)
		if err != nil {
			pool.Close()
			return nil, err
		}
	}
	// The lease session dials lazily, on the first TryAcquire, so a
	// deployment that never leases never pays the connection.
	return &Store{pool: pool, replica: replica, ns: cfg.ns, owner: true, leases: newPgLeases(poolCfg.ConnConfig.Copy())}, nil
}

// Namespace returns a view of the store scoped to the given app namespace,
// sharing the pool. Close on a derived store is a no-op — the Store returned by
// Open owns the pool and must outlive every derived store. The server-global
// schema catalog is shared across namespaces.
func (s *Store) Namespace(ns string) storage.Adapter {
	return &Store{pool: s.pool, replica: s.replica, ns: ns, leases: s.leases}
}

// Close closes the pool if this store owns it (came from Open); on a
// Namespace-derived store it is a no-op.
func (s *Store) Close() error {
	if !s.owner {
		return nil
	}
	s.leases.close()
	if s.replica != nil {
		s.replica.Close()
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
	// The replica may answer: every caller is a periodic re-poll, and the
	// contract licenses a bounded-staleness view. A replica failure retries
	// on the primary rather than surfacing — the list is how background
	// passes find work, and a lagging replica must not stall them.
	if s.replica != nil {
		if ids, err := s.listWorkspaceIDs(ctx, s.replica); err == nil {
			return ids, nil
		}
	}
	return s.listWorkspaceIDs(ctx, s.pool)
}

func (s *Store) listWorkspaceIDs(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx,
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

// ListCollectableWorkspaceIDs answers "which workspaces hold something this
// pass would reclaim" as a single query: the workspace scan is in primary-key
// order, and every candidate table is reached through its _gc index on
// (namespace, workspace_id, updated_at) — correlated per workspace or hashed
// once, whichever the planner costs lower. Either way a workspace with nothing
// collectable costs index reads and no transaction, where a pass without this
// took the workspace's advisory lock and ran six DELETEs to learn the same
// thing.
//
// The entity purges are the exception. Live rows of an aging entity are found
// on rows_by_entity_version's (namespace, workspace_id, entity) prefix with
// updated_at left as a filter, so a workspace whose entity has no expired row
// reads that entity's entries to say so — the same range PurgeEntityRows walks
// to delete them, read instead of written.
//
// On the primary rather than the replica, unlike the other two workspace-list
// queries: a lagging replica would keep returning workspaces a pass has just
// collected, and each of those costs the write transaction this query exists to
// avoid. Its cost is one query a minute, so there is nothing to offload.
func (s *Store) ListCollectableWorkspaceIDs(ctx context.Context, cutoffs storage.GCCutoffs, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("pgstore: limit must be positive, got %d", limit)
	}
	where, args := collectableWhere(s.ns, after, cutoffs)
	args = append(args, int64(limit))
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`SELECT w.workspace_id FROM workspaces w WHERE %s
		 ORDER BY w.workspace_id LIMIT $%d`, where, len(args)), args...)
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

// collectableWhere builds ListCollectableWorkspaceIDs' predicate, numbering
// placeholders from $1 (namespace) and $2 (after).
//
// A zero cutoff drops its probe rather than comparing against what UnixMilli()
// makes of it — the same distinction purgeWhere draws, and here it is the
// difference between "keep this kind forever" and "collect all of it".
func collectableWhere(ns, after string, cutoffs storage.GCCutoffs) (string, []any) {
	args := []any{ns, after}
	next := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	var probes []string
	exists := func(body string) { probes = append(probes, "EXISTS (SELECT 1 FROM "+body+")") }
	scoped := func(table string) string {
		return table + " t WHERE t.namespace = w.namespace AND t.workspace_id = w.workspace_id"
	}
	if !cutoffs.Tombstones.IsZero() {
		at := cutoffs.Tombstones.UnixMilli()
		exists(scoped("rows") + " AND t.deleted AND t.updated_at < " + next(at))
		exists(scoped("clients") + " AND t.updated_at < " + next(at))
		exists(scoped("consumer_positions") + " AND t.updated_at < " + next(at))
	}
	if !cutoffs.IdempotencyKeys.IsZero() {
		exists(scoped("idempotency") + " AND t.updated_at < " + next(cutoffs.IdempotencyKeys.UnixMilli()))
	}
	if !cutoffs.FieldDeltas.IsZero() {
		exists(scoped("field_deltas") + " AND t.updated_at < " + next(cutoffs.FieldDeltas.UnixMilli()))
	}
	for _, e := range cutoffs.Entities {
		if e.Before.IsZero() {
			continue
		}
		exists(scoped("rows") + " AND NOT t.deleted AND t.entity = " + next(e.Entity) +
			" AND t.updated_at < " + next(e.Before.UnixMilli()))
	}
	where := "w.namespace = $1 AND w.workspace_id > $2"
	if len(probes) == 0 {
		// A pass that would collect nothing has no workspaces to visit. Said
		// here rather than left to an empty OR, which reads as TRUE and would
		// return every workspace to a pass with nothing to do.
		return where + " AND FALSE", args
	}
	return where + " AND (" + strings.Join(probes, " OR ") + ")", args
}

// ListStaleIndexWorkspaceIDs scans the workspaces table and compares one
// column. There is no index for it — a btree cannot serve `<>` — and none is
// wanted: the scan is one row per workspace, which is what replaces the point
// read per (workspace × entity × index) the pass did to learn the same thing.
//
// On the primary, for ListCollectableWorkspaceIDs' reason: a lagging replica
// would re-select workspaces whose mark is already written, and each of those
// costs a lease and a write transaction.
func (s *Store) ListStaleIndexWorkspaceIDs(ctx context.Context, mark, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("pgstore: limit must be positive, got %d", limit)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT workspace_id FROM workspaces
		 WHERE namespace = $1 AND workspace_id > $2 AND index_build_mark <> $3
		 ORDER BY workspace_id LIMIT $4`, s.ns, after, mark, int64(limit))
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

// ListCompactableWorkspaceIDs compares two columns of the workspace row and
// reads nothing else. Whether a row is worth compacting is a question only
// decoding its text can answer, so the query does not ask it: it asks the
// cheaper question of whether the sweep has fallen behind the delta prune,
// which is the one the mark already records.
//
// Scanned rather than indexed, like ListStaleIndexWorkspaceIDs: the predicate
// compares two columns to each other, which no btree serves. On the primary,
// for the same reason that query is — a lagging replica re-selects workspaces
// whose mark is already written, and each costs a lease and a write
// transaction.
func (s *Store) ListCompactableWorkspaceIDs(ctx context.Context, minPurged int, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("pgstore: limit must be positive, got %d", limit)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT workspace_id FROM workspaces
		 WHERE namespace = $1 AND workspace_id > $2
		   AND (text_compact_seq < field_delta_horizon
		        OR (text_compact_seq = field_delta_horizon AND text_compact_entity <> '')
		        OR text_compact_min_purged > $3)
		 ORDER BY workspace_id LIMIT $4`, s.ns, after, int32(minPurged), int64(limit))
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
	// Replica-first with a primary retry, as in ListWorkspaceIDs and for the
	// same reason: a polling consumer tolerates staleness, not a stall.
	if s.replica != nil {
		if ids, err := s.listPendingWorkspaceIDs(ctx, s.replica, consumer, after, limit); err == nil {
			return ids, nil
		}
	}
	return s.listPendingWorkspaceIDs(ctx, s.pool, consumer, after, limit)
}

func (s *Store) listPendingWorkspaceIDs(ctx context.Context, pool *pgxpool.Pool, consumer, after string, limit int) ([]string, error) {
	rows, err := pool.Query(ctx,
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
