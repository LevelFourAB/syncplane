// Package sqlitestore implements storage.Adapter on SQLite via
// modernc.org/sqlite (pure Go, no CGO).
//
// Connection model: the store keeps two pools over the same database file in
// WAL mode — a single-connection writer and a multi-connection reader. WAL
// lets any number of readers run concurrently with the one writer, so reads
// (pull, query, the Subscribe streams' seq checks) do not queue behind a write
// burst. The
// single writer connection still serializes writes, which is exactly the
// per-workspace write mutual exclusion the storage contract requires, granted
// for free. An in-memory database (":memory:") is private to its connection
// and cannot be shared across pools, so it falls back to one shared
// connection; use a temp file to exercise the split.
//
// Locking: write transactions BEGIN IMMEDIATE (the writer DSN's
// _txlock=immediate), so they take SQLite's write lock up front instead of
// upgrading at their first write statement. A deferred upgrade that loses a
// race fails outright with SQLITE_BUSY_SNAPSHOT, which busy_timeout does not
// wait out and this adapter does not retry; taking the lock at BEGIN turns
// that failure back into a wait. Read transactions pass ReadOnly, which keeps
// them on a plain deferred BEGIN and off the write lock entirely.
//
// Re-entrancy: reads may be nested, but only up to the reader pool — a nested
// read holds its outer read's connection while it waits for one of its own, so
// depth times concurrency has to stay under WithReadConnections or the pool
// hands out its last connection to an outer read and nothing can finish. The
// engine's own read paths do not nest (each is one short transaction), and
// pgstore's pool is bounded the same way, so this bounds app code handed a
// read view rather than anything internal. Writes are stricter: they serialize
// on the one writer connection, so opening a write transaction from inside
// another write transaction on the same store deadlocks outright — the inner
// BeginTx waits forever for the connection the outer tx holds. This is
// intrinsic to SQLite's single-writer model, not the pool size. The server
// rejects re-entrant Mutate/Invoke/CollectGarbage with an error rather than
// letting them hang (see package engine/server); direct adapter callers must
// likewise never nest write transactions.
//
// Never open the same database file twice in one process: two writer pools
// each holding their own connection contend for the one write lock, so a write
// that outlives the other pool's busy_timeout fails with SQLITE_BUSY. To serve
// several apps from one file, open one Store and derive per-app views with
// Namespace.
//
// Tuning: the defaults suit a server rather than an embedded library, and each
// has an Option to move it — WithCacheSize (page cache, charged per
// connection), WithReadConnections (reader pool ceiling, and with it the
// store's memory ceiling), WithWALSizeLimit (how large the -wal sidecar is
// left after a checkpoint) and WithOptimizeInterval (how often query-planner
// statistics are refreshed; see maintain). The rest is fixed: synchronous is
// NORMAL, the WAL default, which trades the last commits before a power loss
// or kernel panic — not a process crash — for not paying an fsync per
// transaction.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/storage"
)

// applicationID stamps a file as a syncplane store, in the 32-bit slot SQLite
// keeps for exactly that (PRAGMA application_id). It is what makes user_version
// below readable: a database left by an earlier schema lineage, or one that was
// never a syncplane store at all, carries 0 here, so its version numbers are
// never mistaken for this lineage's. The value is "sync" in ASCII, chosen to be
// recognisable in a hex dump rather than for any property of the number.
const applicationID = 0x73796E63

// baselineVersion is the PRAGMA user_version a database created from schema
// carries, and the lowest this binary can open. The number counts within one
// schema lineage and says nothing across lineages, which is what applicationID
// settles first.
const baselineVersion = 1

// schema is the baseline every new database is created at — the current shape,
// stated once, applied only to an empty file. It must NEVER change once
// anything has been created from it: a database already in the field reaches a
// new shape by replaying migrations, so an edit here makes fresh and upgraded
// databases diverge. Structural evolution appends to migrations below.
const schema = `
CREATE TABLE workspaces (
	namespace           TEXT NOT NULL DEFAULT '',
	workspace_id        TEXT NOT NULL,
	last_seq            INTEGER NOT NULL DEFAULT 0,
	tombstone_horizon   INTEGER NOT NULL DEFAULT 0,
	-- A random identity pinning a cursor to the store lineage that issued it
	-- (see ReadTx.StoreEpoch); 0 reads back as "no epoch yet".
	store_epoch         INTEGER NOT NULL DEFAULT 0,
	-- The field-delta log's pruned-through watermark: a span reaching below it
	-- cannot be trusted and falls back to full state.
	field_delta_horizon INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (namespace, workspace_id)
);

CREATE TABLE rows (
	namespace     TEXT NOT NULL DEFAULT '',
	workspace_id  TEXT NOT NULL,
	entity        TEXT NOT NULL,
	key           TEXT NOT NULL,
	value         BLOB,
	version       INTEGER NOT NULL,
	deleted       INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER NOT NULL DEFAULT 0,
	-- Write attribution (see storage.Actor), two columns rather than one
	-- encoded blob so a query can filter on a subject or a system label
	-- without parsing, and so the empty string keeps its ordinary meaning.
	actor_subject TEXT NOT NULL DEFAULT '',
	actor_system  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (namespace, workspace_id, entity, key)
);
-- What ScanChanges pages the whole workspace by.
CREATE INDEX rows_by_version ON rows (namespace, workspace_id, version);
-- Partial, because deleted = 1 is the only collectable state, which keeps the
-- GC index proportional to the dead rows rather than the live ones. Without
-- it every pass range-scans the workspace: the primary key narrows to the
-- workspace but not to the collectable subset.
CREATE INDEX rows_gc ON rows (namespace, workspace_id, updated_at) WHERE deleted = 1;
-- Backs ScanEntityByVersionDesc. A recency window asks for ONE entity's newest
-- live rows, and rows_by_version orders by version across every entity, so
-- serving it from there means reading and discarding the other entities' rows.
-- It is a second index rather than a widening of the first, because putting
-- entity ahead of version there would cost the pull path its index. The key
-- order matches aheadByRecency's tie-break (version descending, key ascending)
-- so a page boundary lands where the in-memory top-K would have put it.
CREATE INDEX rows_by_entity_version
	ON rows (namespace, workspace_id, entity, version DESC, key) WHERE deleted = 0;

CREATE TABLE clients (
	namespace        TEXT NOT NULL DEFAULT '',
	workspace_id     TEXT NOT NULL,
	client_id        TEXT NOT NULL,
	last_mutation_id INTEGER NOT NULL DEFAULT 0,
	updated_at       INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (namespace, workspace_id, client_id)
);
CREATE INDEX clients_gc ON clients (namespace, workspace_id, updated_at);

-- The server-global runtime schema catalog. app_id is the partition (no
-- namespace or workspace columns): one engine hosts multiple apps, each
-- registering a self-contained FileDescriptorSet the engine reloads and
-- re-registers on boot.
CREATE TABLE schemas (
	app_id         TEXT PRIMARY KEY,
	descriptor_set BLOB NOT NULL,
	revision       TEXT NOT NULL,
	updated_at     INTEGER NOT NULL DEFAULT 0
);

-- The collaborative-text delta log — one row per (text field, write) holding
-- the fragment that write added, so a pull can ship a keystroke instead of a
-- document. The primary key is ordered so the per-field span read (one row,
-- one field, seq > cursor) is a range scan of its tail. frag is empty for a
-- gap marker (see WriteTx.PutFieldDelta).
CREATE TABLE field_deltas (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	field        TEXT NOT NULL,
	seq          INTEGER NOT NULL,
	frag         BLOB NOT NULL,
	updated_at   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (namespace, workspace_id, entity, key, field, seq)
);
CREATE INDEX field_deltas_gc ON field_deltas (namespace, workspace_id, updated_at);

-- Collaborative-text compaction floors — one row per text field whose
-- tombstones have been purged, holding the version vector the document held at
-- the purge. Read on every write to such a field, so it is keyed the way the
-- row is.
CREATE TABLE text_floors (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	field        TEXT NOT NULL,
	vv           BLOB NOT NULL,
	PRIMARY KEY (namespace, workspace_id, entity, key, field)
);

-- The idempotency ledger for trusted writes — one row per claimed key, holding
-- the sequence the original write took and the caller's recorded result.
CREATE TABLE idempotency (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	key          TEXT NOT NULL,
	seq          INTEGER NOT NULL,
	result       BLOB NOT NULL,
	updated_at   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (namespace, workspace_id, key)
);
-- Backs the retention sweep, which is a range scan of one workspace's expired
-- keys rather than of all of them.
CREATE INDEX idempotency_gc ON idempotency (namespace, workspace_id, updated_at);

-- Secondary index entries. WITHOUT ROWID with a covering primary key, so a
-- range scan of one index reads the b-tree it is already ordered by and never
-- touches rows. k is a BLOB, which SQLite compares with memcmp — the bytewise
-- order the engine's key encoder is built for. key sits in the PK after k for
-- uniqueness only: k already ends with the row key, so two live rows cannot
-- collide. entity comes ahead of idx because an index name is derived from a
-- FIELD name ("__field_state"), so two entities that mark a field of the same
-- name claim the same one, while an entry carries a bare row key that only
-- identifies a row within its own entity.
CREATE TABLE index_entries (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	idx          TEXT NOT NULL,
	k            BLOB NOT NULL,
	key          TEXT NOT NULL,
	PRIMARY KEY (namespace, workspace_id, entity, idx, k, key)
) WITHOUT ROWID;
-- What makes the write path affordable: entries are replaced per row and
-- deleted per row, and both need "every entry this row holds" without knowing
-- which indexes those are.
CREATE INDEX index_entries_by_row
	ON index_entries (namespace, workspace_id, entity, key);
-- Per-index build state: whether the entries cover the whole entity yet. Keyed
-- like the entries, so one readiness flag never spans two entities' sets.
CREATE TABLE index_state (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	idx          TEXT NOT NULL,
	spec_hash    TEXT NOT NULL,
	ready        INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (namespace, workspace_id, entity, idx)
);

-- Effect-consumer state — how far each consumer has acted on the change feed,
-- and the per-row snapshots it derives before-state from (see
-- storage.ConsumerPosition). Both are written by the SAME transaction, which is
-- what makes a crash redeliver a change instead of skipping it.
CREATE TABLE consumer_positions (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	consumer     TEXT NOT NULL,
	seq          INTEGER NOT NULL DEFAULT 0,
	entity       TEXT NOT NULL DEFAULT '',
	key          TEXT NOT NULL DEFAULT '',
	updated_at   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (namespace, workspace_id, consumer)
);
-- Backs the staleness sweep, which is a range scan of one workspace's expired
-- consumers rather than of every consumer.
CREATE INDEX consumer_positions_gc
	ON consumer_positions (namespace, workspace_id, updated_at);
-- No GC index of its own: shadows are only ever deleted per row, or wholesale
-- with their consumer, and the primary key's (namespace, workspace_id,
-- consumer) prefix is the range scan for that.
CREATE TABLE consumer_shadows (
	namespace    TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL,
	consumer     TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	snapshot     BLOB NOT NULL,
	PRIMARY KEY (namespace, workspace_id, consumer, entity, key)
);
`

// migrations[i] upgrades a database at user_version baselineVersion+i to
// baselineVersion+i+1. Each runs in its own transaction; user_version is
// bumped inside that transaction. A structural change appends here and leaves
// schema alone, so a database in the field reaches the new shape by the same
// steps a fresh one skips.
var migrations = []string{
	// Verdicts on refused mutations, so a retry that follows a lost push
	// response is answered with the rejection instead of a bare ack. NULL until
	// a client has had something refused, which is the common case.
	`ALTER TABLE clients ADD COLUMN rejections BLOB;`,
	// Cursor-state entries: the membership sets of issued pull cursors, keyed
	// by the (id, gen) reference the cursor carries. See cursorstate.go for
	// the pruning that keeps the table bounded.
	`CREATE TABLE cursor_state (
		namespace    TEXT NOT NULL DEFAULT '',
		workspace_id TEXT NOT NULL,
		id           INTEGER NOT NULL,
		gen          INTEGER NOT NULL,
		state        BLOB NOT NULL,
		written_at   INTEGER NOT NULL,
		PRIMARY KEY (namespace, workspace_id, id, gen)
	);`,
	// The index-build mark: the opaque fingerprint of the index declarations a
	// workspace's entries are fully built under (see
	// ListStaleIndexWorkspaceIDs). Empty is "never built", which is what the
	// backfill leaves behind and what makes every existing workspace stale to
	// the first pass after the upgrade.
	`ALTER TABLE workspaces ADD COLUMN index_build_mark TEXT NOT NULL DEFAULT '';`,
	// The text-compaction mark: how far the sweep has considered rows in
	// ScanChanges order, and the minPurged threshold it did so under (see
	// ListCompactableWorkspaceIDs). Zero is "never swept", so the first pass
	// after the upgrade starts every workspace from the beginning — which is
	// what compaction did on every pass before the sweep existed.
	`ALTER TABLE workspaces ADD COLUMN text_compact_seq INTEGER NOT NULL DEFAULT 0;
	 ALTER TABLE workspaces ADD COLUMN text_compact_entity TEXT NOT NULL DEFAULT '';
	 ALTER TABLE workspaces ADD COLUMN text_compact_key TEXT NOT NULL DEFAULT '';
	 ALTER TABLE workspaces ADD COLUMN text_compact_min_purged INTEGER NOT NULL DEFAULT 0;`,
	// The sequence that created each row (see storage.Row.CreatedVersion):
	// stamped on insert, kept on every overwrite. The 0 default is "unknown",
	// which pre-existing rows keep — the pull path treats unknown
	// conservatively, so a backfill is neither needed nor possible (the
	// creating sequence of an existing row is not recorded anywhere).
	`ALTER TABLE rows ADD COLUMN created_version INTEGER NOT NULL DEFAULT 0;`,
}

// Store implements storage.Adapter. Stores derived via Namespace share the
// underlying pools; only the Store returned by Open owns (and closes) them.
// read and write are the same handle for an in-memory database.
type Store struct {
	read  *sql.DB
	write *sql.DB
	ns    string
	owner bool
	// leases backs the lease.Provider implementation (see leases.go), shared
	// by every derived namespace so they lease out of one space.
	leases *lease.Local

	// The maintenance loop, owned by the Store Open returned: stop asks it to
	// return and done reports that it has. Both are nil on a derived store and
	// when WithOptimizeInterval disabled the loop.
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// Option configures Open.
type Option func(*config)

type config struct {
	ns               string
	cacheSize        int64
	readConns        int
	walSizeLimit     int64
	optimizeInterval time.Duration
}

const (
	// defaultCacheSize is the page cache SQLite keeps per CONNECTION, so the
	// store's ceiling is (1 + readConns) times this — the number to move if the
	// process has a memory budget to hit. SQLite's own default is 2 MiB, which
	// is sized for an embedded library rather than a server that keeps a hot
	// working set of rows and index entries.
	defaultCacheSize = 8 << 20
	// defaultWALSizeLimit caps the -wal sidecar. A checkpoint hands the WAL's
	// space back for reuse but does not shrink the file, so without a limit the
	// sidecar keeps whatever size the single busiest write burst left it at for
	// the life of the database. The limit is applied at checkpoint, so a
	// transaction larger than it still runs; it just gets truncated afterwards.
	defaultWALSizeLimit = 64 << 20
	// defaultOptimizeInterval is how often the writer connection refreshes
	// query-planner statistics; see maintain. SQLite suggests hourly or daily
	// for a long-lived connection, and hourly is the safer end for a store
	// whose indexes are created at runtime rather than at schema time.
	defaultOptimizeInterval = time.Hour
	// readIdleTimeout retires reader connections that have gone quiet, so an
	// idle process eventually gives back the page cache a read burst grew.
	readIdleTimeout = 5 * time.Minute
	// optimizeTimeout bounds one maintenance pass. analysis_limit already caps
	// the sampling; this caps the wait for the writer connection, which the
	// pass has to take from the write path.
	optimizeTimeout = 30 * time.Second
	// optimizePragma is what SQLite documents for a long-lived connection:
	// 0x00002 runs ANALYZE where the planner would benefit, and 0x10000 sizes
	// every table rather than only the recently used ones when picking
	// candidates.
	optimizePragma = `PRAGMA optimize=0x10002`
)

func defaultConfig() config {
	return config{
		cacheSize:        defaultCacheSize,
		readConns:        max(4, min(runtime.GOMAXPROCS(0), 8)),
		walSizeLimit:     defaultWALSizeLimit,
		optimizeInterval: defaultOptimizeInterval,
	}
}

// WithNamespace scopes the store to an app namespace. Namespaces partition
// workspaces, rows and client watermarks within one database file; the
// default is "". Equivalent to Open(path).Namespace(ns) but spares the
// intermediate store.
func WithNamespace(ns string) Option {
	return func(c *config) { c.ns = ns }
}

// WithCacheSize sets SQLite's page cache in bytes. It is charged per
// connection, so the store's worst case is this times one writer plus every
// reader connection — raise it together with, not independently of,
// WithReadConnections. Zero leaves SQLite's own default.
func WithCacheSize(bytes int64) Option {
	return func(c *config) { c.cacheSize = bytes }
}

// WithReadConnections caps the reader pool, which bounds both read concurrency
// and the page cache the store can hold (see WithCacheSize), and with it how
// deeply reads may nest (see the package comment). Idle readers are kept
// rather than churned, because a fresh connection starts with a cold cache and
// re-runs every pragma. Zero leaves the pool unbounded, which is
// database/sql's default; the writer pool is always exactly one connection.
func WithReadConnections(n int) Option {
	return func(c *config) { c.readConns = n }
}

// WithWALSizeLimit caps the -wal sidecar in bytes. Zero lets it grow to its
// high-water mark and stay there, which is SQLite's default.
func WithWALSizeLimit(bytes int64) Option {
	return func(c *config) { c.walSizeLimit = bytes }
}

// WithOptimizeInterval sets how often the store refreshes query-planner
// statistics (see maintain). Zero disables the background pass; Open and Close
// still run one, so a process that never reaches an interval is not left
// planning against no statistics at all.
func WithOptimizeInterval(d time.Duration) Option {
	return func(c *config) { c.optimizeInterval = d }
}

// Open opens (creating if necessary) a SQLite database at path. Use
// ":memory:" only for single-connection tests; prefer a temp file.
//
// Open a given file at most once per process: see the package comment. Use
// Namespace to multiplex apps over one Store.
//
// An empty path is created at the current schema; an existing database is
// brought up to it. A database that is not a syncplane store, or belongs to a
// schema lineage this binary cannot read, is refused with an error rather than
// opened — it has to be re-created, which costs every client of the workspaces
// it held a re-bootstrap.
func Open(path string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	// _txlock=immediate: write transactions take the write lock at BEGIN, so
	// contention waits out busy_timeout instead of failing the upgrade with
	// SQLITE_BUSY_SNAPSHOT (see the package comment).
	//
	// analysis_limit caps how many rows per index ANALYZE samples, which is
	// what keeps the maintenance pass bounded on a large database.
	writeParams := append(cfg.commonParams(),
		"_txlock=immediate",
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(NORMAL)",
		"_pragma=analysis_limit(400)")
	if cfg.walSizeLimit > 0 {
		writeParams = append(writeParams,
			fmt.Sprintf("_pragma=journal_size_limit(%d)", cfg.walSizeLimit))
	}
	write, err := sql.Open("sqlite", dsn(path, writeParams...))
	if err != nil {
		return nil, err
	}
	// SQLite allows one writer per file; a single connection serializes our
	// write transactions and grants the storage contract's write mutual
	// exclusion for free. It is also deliberately never retired — no max
	// lifetime, and an idle slot to hold it — so the pragmas above are paid for
	// once and the maintenance pass keeps meeting the same connection.
	write.SetMaxOpenConns(1)
	write.SetMaxIdleConns(1)
	if err := migrate(write); err != nil {
		write.Close()
		return nil, err
	}
	// Readers share the file via WAL and run concurrently with the writer and
	// each other; query_only keeps them off the write lock so they can never
	// hit SQLITE_BUSY_SNAPSHOT trying to upgrade. An in-memory database is
	// private to its connection, so reads fall back to the writer pool.
	read := write
	if !isMemory(path) {
		readParams := append(cfg.commonParams(), "_pragma=query_only(true)")
		read, err = sql.Open("sqlite", dsn(path, readParams...))
		if err != nil {
			write.Close()
			return nil, err
		}
		// Bounded, and idle-heavy: an unbounded pool would let a read burst
		// multiply the per-connection page cache by however many readers
		// arrived at once, while the database/sql default of two idle
		// connections would throw those caches away between bursts.
		if cfg.readConns > 0 {
			read.SetMaxOpenConns(cfg.readConns)
			read.SetMaxIdleConns(cfg.readConns)
		}
		read.SetConnMaxIdleTime(readIdleTimeout)
	}
	s := &Store{read: read, write: write, ns: cfg.ns, owner: true, leases: lease.NewLocal()}
	// Creating the schema creates indexes, which is one of the moments SQLite
	// names for re-analysis; it is also the only pass a short-lived process
	// gets before Close.
	s.optimize()
	if cfg.optimizeInterval > 0 {
		s.stop = make(chan struct{})
		s.done = make(chan struct{})
		go s.maintain(cfg.optimizeInterval)
	}
	return s, nil
}

// commonParams returns the DSN parameters both pools share. Every one of them
// is per-connection state, so the reader pool has to repeat what the writer
// sets rather than inherit it — journal_mode is the exception that makes the
// rest look inheritable, being a property of the file.
func (c config) commonParams() []string {
	params := []string{
		"_pragma=busy_timeout(5000)",
		"_pragma=foreign_keys(1)",
		// Sorters and other transient b-trees stay in memory instead of
		// spilling into files next to the database.
		"_pragma=temp_store(MEMORY)",
	}
	if c.cacheSize > 0 {
		params = append(params, cacheSizePragma(c.cacheSize))
	}
	return params
}

// cacheSizePragma renders a byte count as cache_size, which counts PAGES when
// positive and KiB when negative — a byte count passed through unsigned would
// ask for a cache thousands of times the size intended.
func cacheSizePragma(bytes int64) string {
	return fmt.Sprintf("_pragma=cache_size(-%d)", max(bytes/1024, 1))
}

// maintain refreshes query-planner statistics every interval until Close.
//
// SQLite does not keep statistics current on its own: without an ANALYZE the
// planner works from defaults, and this store creates its indexes at runtime —
// an entity's index exists only once something registers it — so the moment
// where a schema-time ANALYZE would have run never arrives. Stale statistics
// do not cost correctness, only plan quality, which is why the pass is
// advisory and its errors are dropped.
func (s *Store) maintain(interval time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.optimize()
		}
	}
}

// optimize runs one maintenance pass on the writer pool — ANALYZE writes
// sqlite_stat1, so it cannot go through the query_only readers, and the writer
// is the long-lived connection whose statistics SQLite is tracking anyway.
//
// The pass queues for the writer connection like any other write, so it waits
// out an in-flight transaction rather than interrupting one.
func (s *Store) optimize() {
	ctx, cancel := context.WithTimeout(context.Background(), optimizeTimeout)
	defer cancel()
	_, _ = s.write.ExecContext(ctx, optimizePragma)
}

// dsn builds the SQLite URI for path with the given parameters, each already
// in "key=value" form.
//
// The path is escaped rather than interpolated. The driver splits the DSN at
// the first '?' to find its parameters, and SQLite then parses what remains as
// a URI — cutting at '#' and decoding %XX — so a path containing any of those
// three characters would otherwise open a different database than the caller
// named, or silently lose every pragma below. ':' is left alone: it is
// meaningless in a URI path and escaping it would break the ":memory:"
// sentinel.
func dsn(path string, params ...string) string {
	return "file:" + uriPathEscaper.Replace(path) + "?" + strings.Join(params, "&")
}

// uriPathEscaper escapes the three characters SQLite's URI filename parser
// treats specially. '%' must come first in the sense that it is escaped too —
// otherwise a path with a literal %XX in it decodes to something else.
var uriPathEscaper = strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23")

// isMemory reports whether path names an in-memory database, which cannot be
// shared across separate connections and so disables the reader/writer split.
func isMemory(path string) bool {
	return path == ":memory:" || strings.Contains(path, ":memory:") || strings.Contains(path, "mode=memory")
}

// Namespace returns a view of the store scoped to the given app namespace,
// sharing the underlying database handle. Close on a derived store is a
// no-op — the Store returned by Open owns the handle and must outlive every
// derived store. The server-global schema catalog is shared across namespaces.
func (s *Store) Namespace(ns string) storage.Adapter {
	return &Store{read: s.read, write: s.write, ns: ns, leases: s.leases}
}

// migrate brings a database up to baselineVersion+len(migrations), creating the
// baseline first if it is empty. A fresh database runs the same migrations an
// existing one does, so the two end at the same shape.
//
// A database from another lineage — a different application_id, or a version
// outside the range this binary knows — is refused rather than opened: writing
// through a shape that is not the one on disk is the failure this check exists
// to prevent, and it is silent.
func migrate(db *sql.DB) error {
	var appID int
	if err := db.QueryRow(`PRAGMA application_id`).Scan(&appID); err != nil {
		return fmt.Errorf("sqlitestore: read application_id: %w", err)
	}
	if appID == 0 {
		// Either a file with nothing in it yet, or one this lineage never
		// stamped; applyBaseline tells those apart. It stamps the BASELINE, so
		// the loop below still runs: schema is frozen at the shape it was first
		// published with, and a fresh database reaches the current one by
		// replaying the same migrations a database in the field does.
		if err := applyBaseline(db); err != nil {
			return err
		}
	} else if appID != applicationID {
		return fmt.Errorf("sqlitestore: database application_id %#x is not a syncplane store (want %#x)", appID, applicationID)
	}

	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("sqlitestore: read user_version: %w", err)
	}
	latest := baselineVersion + len(migrations)
	if version < baselineVersion {
		return fmt.Errorf("sqlitestore: database user_version %d predates this binary's schema (baseline %d); it cannot be upgraded in place and must be re-created", version, baselineVersion)
	}
	if version > latest {
		return fmt.Errorf("sqlitestore: database user_version %d is newer than this binary (max %d)", version, latest)
	}
	for ; version < latest; version++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[version-baselineVersion]); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlitestore: migration %d: %w", version+1, err)
		}
		// PRAGMA does not support placeholders; version is internal.
		if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, version+1)); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlitestore: migration %d: bump user_version: %w", version+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlitestore: migration %d: commit: %w", version+1, err)
		}
	}
	return nil
}

// applyBaseline creates schema in an unstamped database and stamps it, in one
// transaction so a database is never left half-created.
//
// An unstamped file is either one nothing has written yet or one from a lineage
// that predates the stamp, and the two are told apart by whether any table is
// there. Laying the baseline over tables it did not create would leave the
// shape on disk and the shape in the code disagreeing.
func applyBaseline(db *sql.DB) error {
	var tables int
	if err := db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
	).Scan(&tables); err != nil {
		return fmt.Errorf("sqlitestore: inspect database: %w", err)
	}
	if tables > 0 {
		return fmt.Errorf("sqlitestore: database holds %d table(s) but carries no application_id; it is not a store this binary can read and must be re-created", tables)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(schema); err != nil {
		tx.Rollback()
		return fmt.Errorf("sqlitestore: apply schema: %w", err)
	}
	// PRAGMA takes no placeholders; both values are constants. The stamps go in
	// the same transaction as the schema, so a crash between them cannot leave
	// tables that claim a version they were not created at.
	if _, err := tx.Exec(fmt.Sprintf(
		"PRAGMA application_id = %d;\nPRAGMA user_version = %d;", applicationID, baselineVersion,
	)); err != nil {
		tx.Rollback()
		return fmt.Errorf("sqlitestore: stamp schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlitestore: apply schema: commit: %w", err)
	}
	return nil
}

// Close stops the maintenance loop and closes the database handle if this
// store owns it (came from Open); on a Namespace-derived store it is a no-op.
func (s *Store) Close() error {
	if !s.owner {
		return nil
	}
	if s.stop != nil {
		// Once: a second Close must not close a closed channel. It waits for
		// the loop to return before the pools go, so a pass in flight cannot
		// outlive the handle it is running on.
		s.stopOnce.Do(func() {
			close(s.stop)
			<-s.done
		})
	}
	// A parting pass, so a process that never lived long enough to tick still
	// leaves statistics behind for the next one that opens this file.
	s.optimize()
	err := s.write.Close()
	if s.read != s.write {
		err = errors.Join(err, s.read.Close())
	}
	return err
}

// readOnlyTx narrows a sqliteTx to the read surface. Embedding the INTERFACE
// rather than the struct is what does the work: only ReadTx's methods are
// promoted, so a closure that type-asserts its way to storage.WriteTx gets a
// failed assertion instead of writes WithReadTx would silently roll back.
// The reader pool is query_only as well, but an in-memory database has no
// reader pool to be query_only — this holds either way.
type readOnlyTx struct {
	storage.ReadTx
}

func (s *Store) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	// ReadOnly keeps modernc on a plain deferred BEGIN; without it the writer
	// DSN's _txlock=immediate would make every read take the write lock (and
	// an in-memory store, whose reads run on the writer pool, serialize).
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(readOnlyTx{&sqliteTx{tx: tx, ns: s.ns, ws: workspaceID}})
}

func (s *Store) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The workspaces row is NOT created here: sqliteTx.register mints it on the
	// transaction's first actual write. A closure that only reads (a read-only
	// Mutate, a push whose every mutation was already applied) must leave the
	// workspace unregistered — see the WithWriteTx contract.
	st := &sqliteTx{tx: tx, ns: s.ns, ws: workspaceID}
	if err := fn(st); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListWorkspaceIDs(ctx context.Context) ([]string, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT workspace_id FROM workspaces WHERE namespace = ? ORDER BY workspace_id`, s.ns)
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
// order, and each row probes the _gc indexes of the four collectable tables on
// their own (namespace, workspace_id, updated_at) prefix. Each probe stops at
// the first match, so a workspace with nothing collectable costs four index
// descents and no transaction at all.
//
// The entity purges are the exception. Live rows of an aging entity are found
// on rows_by_entity_version's (namespace, workspace_id, entity) prefix with
// updated_at left as a filter, so a workspace whose entity has no expired row
// reads that entity's entries to say so — the same range PurgeEntityRows walks
// to delete them, read instead of written.
func (s *Store) ListCollectableWorkspaceIDs(ctx context.Context, cutoffs storage.GCCutoffs, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	where, args := collectableWhere(s.ns, after, cutoffs)
	args = append(args, limit)
	rows, err := s.read.QueryContext(ctx,
		`SELECT w.workspace_id FROM workspaces w WHERE `+where+
			` ORDER BY w.workspace_id LIMIT ?`, args...)
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

// ListStaleIndexWorkspaceIDs scans the workspaces table and compares one
// column. There is no index for it — a btree cannot serve `<>` — and none is
// wanted: the scan is one row per workspace, which is what replaces the point
// read per (workspace × entity × index) the pass did to learn the same thing.
func (s *Store) ListStaleIndexWorkspaceIDs(ctx context.Context, mark, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	rows, err := s.read.QueryContext(ctx,
		`SELECT workspace_id FROM workspaces
		 WHERE namespace = ? AND workspace_id > ? AND index_build_mark <> ?
		 ORDER BY workspace_id LIMIT ?`, s.ns, after, mark, limit)
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
// Like ListStaleIndexWorkspaceIDs this scans the workspaces table rather than
// an index, for the same reason — the predicate compares two columns to each
// other, which no btree serves — and against the same alternative, which was
// scanning every text-bearing entity of every workspace to find out.
func (s *Store) ListCompactableWorkspaceIDs(ctx context.Context, minPurged int, after string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	rows, err := s.read.QueryContext(ctx,
		`SELECT workspace_id FROM workspaces
		 WHERE namespace = ? AND workspace_id > ?
		   AND (text_compact_seq < field_delta_horizon
		        OR (text_compact_seq = field_delta_horizon AND text_compact_entity <> '')
		        OR text_compact_min_purged > ?)
		 ORDER BY workspace_id LIMIT ?`, s.ns, after, minPurged, limit)
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

// collectableWhere builds ListCollectableWorkspaceIDs' predicate.
//
// The boolean columns are written `= 1` and `= 0` rather than as truth values
// because SQLite matches a partial index by a syntactic implication check: only
// the literal form the index was declared with (rows_gc's `WHERE deleted = 1`)
// makes the probe an index descent instead of a scan of the workspace's rows.
//
// A zero cutoff drops its probe rather than comparing against what UnixMilli()
// makes of it — the same distinction purgeWhere draws, and here it is the
// difference between "keep this kind forever" and "collect all of it".
func collectableWhere(ns, after string, cutoffs storage.GCCutoffs) (string, []any) {
	args := []any{ns, after}
	next := func(v any) string {
		args = append(args, v)
		return "?"
	}
	var probes []string
	exists := func(body string) { probes = append(probes, "EXISTS (SELECT 1 FROM "+body+")") }
	scoped := func(table string) string {
		return table + " t WHERE t.namespace = w.namespace AND t.workspace_id = w.workspace_id"
	}
	if !cutoffs.Tombstones.IsZero() {
		at := cutoffs.Tombstones.UnixMilli()
		exists(scoped("rows") + " AND t.deleted = 1 AND t.updated_at < " + next(at))
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
		exists(scoped("rows") + " AND t.deleted = 0 AND t.entity = " + next(e.Entity) +
			" AND t.updated_at < " + next(e.Before.UnixMilli()))
	}
	where := "w.namespace = ? AND w.workspace_id > ?"
	if len(probes) == 0 {
		// A pass that would collect nothing has no workspaces to visit. Said
		// here rather than left to an empty OR, which reads as TRUE and would
		// return every workspace to a pass with nothing to do.
		return where + " AND 1 = 0", args
	}
	return where + " AND (" + strings.Join(probes, " OR ") + ")", args
}

// ListPendingWorkspaceIDs answers "which workspaces owe this consumer work" as
// a single indexed query: the workspace scan is in primary-key order, and each
// row probes consumer_positions on its own primary key prefix (namespace,
// workspace_id, consumer). A workspace with no position row for the consumer is
// pending by the LEFT JOIN — a first run has acted on nothing.
//
// The cost is one probe per workspace, which is a scan the engine runs once per
// poll rather than the per-workspace read transaction it replaces. It is still
// linear in workspaces; an adapter with a cheaper answer (a dirty set
// maintained on write) may substitute one, as long as it stays complete.
//
// A non-empty entity or key is pending whatever the sequence says: that is a
// sub-position inside a same-sequence group, so the group has changes after it
// even when the sequence has reached the head. Only a clean boundary — the
// cursor a drained pass records — means caught up.
func (s *Store) ListPendingWorkspaceIDs(ctx context.Context, consumer, after string, limit int) ([]string, error) {
	if consumer == "" {
		return nil, fmt.Errorf("sqlitestore: consumer name is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	rows, err := s.read.QueryContext(ctx,
		`SELECT w.workspace_id
		 FROM workspaces w
		 LEFT JOIN consumer_positions p
		   ON p.namespace = w.namespace
		   AND p.workspace_id = w.workspace_id
		   AND p.consumer = ?
		 WHERE w.namespace = ? AND w.workspace_id > ?
		   AND (p.seq IS NULL OR p.seq < w.last_seq OR p.entity <> '' OR p.key <> '')
		 ORDER BY w.workspace_id
		 LIMIT ?`, consumer, s.ns, after, limit)
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
// catalog. It runs on the writer connection (the catalog is shared across
// namespaces, so the namespace view is irrelevant here).
func (s *Store) PutAppSchema(ctx context.Context, appID string, set []byte, revision string) error {
	_, err := s.write.ExecContext(ctx,
		`INSERT INTO schemas (app_id, descriptor_set, revision, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (app_id)
		 DO UPDATE SET descriptor_set = excluded.descriptor_set,
		               revision = excluded.revision, updated_at = excluded.updated_at`,
		appID, set, revision, time.Now().UnixMilli(),
	)
	return err
}

func (s *Store) ListAppSchemas(ctx context.Context) ([]storage.AppSchema, error) {
	rows, err := s.read.QueryContext(ctx,
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
	_, err := s.write.ExecContext(ctx, `DELETE FROM schemas WHERE app_id = ?`, appID)
	return err
}

type sqliteTx struct {
	tx *sql.Tx
	ns string
	ws string
	// savepoint nesting depth; names savepoints sp1, sp2, ...
	spDepth int
	// registered memoizes register's INSERT so a write burst does not repeat
	// it. Cleared on savepoint rollback, which may have undone that INSERT.
	registered bool
}

// register creates the workspaces row, and is called by every mutating method
// before it writes. Registration is deliberately lazy: workspace_id is
// unauthenticated wire input, so a transaction that turns out to write nothing
// must not leave behind a row that ListWorkspaceIDs — and with it every
// background pass — iterates forever.
//
// It mints the store epoch here and never touches it again (ON CONFLICT DO
// NOTHING): the epoch must stay stable for the workspace's lifetime so cursors
// keep matching across restarts. random() is SQLite's 64-bit PRNG.
func (t *sqliteTx) register() error {
	if t.registered {
		return nil
	}
	if _, err := t.tx.Exec(
		`INSERT INTO workspaces (namespace, workspace_id, store_epoch) VALUES (?, ?, random()) ON CONFLICT DO NOTHING`,
		t.ns, t.ws,
	); err != nil {
		return err
	}
	t.registered = true
	return nil
}

func (t *sqliteTx) CurrentSeq() (uint64, error) {
	var seq uint64
	err := t.tx.QueryRow(
		`SELECT last_seq FROM workspaces WHERE namespace = ? AND workspace_id = ?`, t.ns, t.ws,
	).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (t *sqliteTx) StoreEpoch() (uint64, error) {
	// Stored as a signed INTEGER (random() is signed); reinterpret the bits as
	// the unsigned epoch. The value is opaque — only equality matters.
	var epoch int64
	err := t.tx.QueryRow(
		`SELECT store_epoch FROM workspaces WHERE namespace = ? AND workspace_id = ?`, t.ns, t.ws,
	).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return uint64(epoch), err
}

func (t *sqliteTx) TombstoneHorizon() (uint64, error) {
	var h uint64
	err := t.tx.QueryRow(
		`SELECT tombstone_horizon FROM workspaces WHERE namespace = ? AND workspace_id = ?`, t.ns, t.ws,
	).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return h, err
}

func (t *sqliteTx) GetRow(entity, key string) (storage.Row, error) {
	row := storage.Row{Entity: entity, Key: key}
	var deleted int
	err := t.tx.QueryRow(
		`SELECT value, version, created_version, deleted, actor_subject, actor_system FROM rows
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key = ?`,
		t.ns, t.ws, entity, key,
	).Scan(&row.Value, &row.Version, &row.CreatedVersion, &deleted,
		&row.Actor.Subject, &row.Actor.System)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Row{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.Row{}, err
	}
	row.Deleted = deleted != 0
	return row, nil
}

// GetRows loops over GetRow rather than issuing one statement with an IN list.
// The batch read exists to save round trips, and there are none here — the
// database is in the process — so a variadic statement would buy nothing and
// cost a query plan that changes with the batch size. Looping also leaves no
// result set open across fn, which the contract lets fn read through.
func (t *sqliteTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	for _, key := range keys {
		row, err := t.GetRow(entity, key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	return t.ScanEntityAfter(entity, "", 0, fn)
}

func (t *sqliteTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	if limit <= 0 {
		limit = -1 // SQLite: negative LIMIT means unlimited
	}
	rows, err := t.tx.Query(
		`SELECT key, value, version, created_version, actor_subject, actor_system FROM rows
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND deleted = 0 AND key > ?
		 ORDER BY key LIMIT ?`,
		t.ns, t.ws, entity, afterKey, limit,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r := storage.Row{Entity: entity}
		if err := rows.Scan(&r.Key, &r.Value, &r.Version, &r.CreatedVersion,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (t *sqliteTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	if limit <= 0 {
		limit = -1 // SQLite: negative LIMIT means unlimited
	}
	// ORDER BY matches rows_by_entity_version exactly (version DESC, key ASC,
	// partial on deleted = 0), so this reads the index prefix and stops at
	// LIMIT instead of sorting the entity.
	rows, err := t.tx.Query(
		`SELECT key, value, version, created_version, actor_subject, actor_system FROM rows
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND deleted = 0
		 ORDER BY version DESC, key LIMIT ?`,
		t.ns, t.ws, entity, limit,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r := storage.Row{Entity: entity}
		if err := rows.Scan(&r.Key, &r.Value, &r.Version, &r.CreatedVersion,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (t *sqliteTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	if limit <= 0 {
		limit = -1
	}
	// Order by the full (version, entity, key) tuple so the first page of a
	// same-version group and every resume page agree on row order; the cursor
	// records the last (entity, key) within a split version to resume past it.
	var (
		rows *sql.Rows
		err  error
	)
	if afterEntity == "" {
		// Clean version boundary: everything strictly past afterSeq.
		rows, err = t.tx.Query(
			`SELECT entity, key, value, version, created_version, deleted,
			        actor_subject, actor_system FROM rows
			 WHERE namespace = ? AND workspace_id = ? AND version > ?
			 ORDER BY version, entity, key LIMIT ?`,
			t.ns, t.ws, afterSeq, limit,
		)
	} else {
		// Resume inside a same-version group: past afterSeq, plus the rest of
		// the split group whose (entity, key) sorts after (afterEntity, afterKey).
		rows, err = t.tx.Query(
			`SELECT entity, key, value, version, created_version, deleted,
			        actor_subject, actor_system FROM rows
			 WHERE namespace = ? AND workspace_id = ? AND (
			   version > ?
			   OR (version = ? AND (entity > ? OR (entity = ? AND key > ?)))
			 )
			 ORDER BY version, entity, key LIMIT ?`,
			t.ns, t.ws, afterSeq, afterSeq, afterEntity, afterEntity, afterKey, limit,
		)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r storage.Row
		var deleted int
		if err := rows.Scan(&r.Entity, &r.Key, &r.Value, &r.Version, &r.CreatedVersion, &deleted,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return err
		}
		r.Deleted = deleted != 0
		if r.Deleted {
			r.Value = nil
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (t *sqliteTx) ClientLastMutationID(clientID string) (uint64, error) {
	var id uint64
	err := t.tx.QueryRow(
		`SELECT last_mutation_id FROM clients WHERE namespace = ? AND workspace_id = ? AND client_id = ?`,
		t.ns, t.ws, clientID,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

func (t *sqliteTx) ClientRejections(clientID string) ([]storage.ClientRejection, error) {
	var blob []byte
	err := t.tx.QueryRow(
		`SELECT rejections FROM clients WHERE namespace = ? AND workspace_id = ? AND client_id = ?`,
		t.ns, t.ws, clientID,
	).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) || len(blob) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var recs []storage.ClientRejection
	if err := json.Unmarshal(blob, &recs); err != nil {
		return nil, fmt.Errorf("sqlitestore: decode rejections for client %q: %w", clientID, err)
	}
	return recs, nil
}

func (t *sqliteTx) IdempotencyRecord(key string) (storage.IdempotencyRecord, bool, error) {
	var rec storage.IdempotencyRecord
	err := t.tx.QueryRow(
		`SELECT seq, result FROM idempotency WHERE namespace = ? AND workspace_id = ? AND key = ?`,
		t.ns, t.ws, key,
	).Scan(&rec.Seq, &rec.Result)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return storage.IdempotencyRecord{}, false, err
	}
	// The column is NOT NULL, so "no result" round-trips as an empty blob;
	// normalize it back to nil so callers see what they recorded.
	if len(rec.Result) == 0 {
		rec.Result = nil
	}
	return rec, true, nil
}

func (t *sqliteTx) PutIdempotencyRecord(key string, rec storage.IdempotencyRecord) error {
	if err := t.register(); err != nil {
		return err
	}
	result := rec.Result
	if result == nil {
		result = []byte{}
	}
	_, err := t.tx.Exec(
		`INSERT INTO idempotency (namespace, workspace_id, key, seq, result, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, key)
		 DO UPDATE SET seq = excluded.seq, result = excluded.result, updated_at = excluded.updated_at`,
		t.ns, t.ws, key, rec.Seq, result, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) CollectIdempotencyKeys(cutoff time.Time) (int, error) {
	res, err := t.tx.Exec(
		`DELETE FROM idempotency WHERE namespace = ? AND workspace_id = ? AND updated_at < ?`,
		t.ns, t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (t *sqliteTx) ConsumerPosition(consumer string) (storage.ConsumerPosition, bool, error) {
	pos := storage.ConsumerPosition{Consumer: consumer}
	var updated int64
	err := t.tx.QueryRow(
		`SELECT seq, entity, key, updated_at FROM consumer_positions
		 WHERE namespace = ? AND workspace_id = ? AND consumer = ?`,
		t.ns, t.ws, consumer,
	).Scan(&pos.Seq, &pos.Entity, &pos.Key, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ConsumerPosition{}, false, nil
	}
	if err != nil {
		return storage.ConsumerPosition{}, false, err
	}
	pos.UpdatedAt = time.UnixMilli(updated)
	return pos, true, nil
}

func (t *sqliteTx) ListConsumerPositions() ([]storage.ConsumerPosition, error) {
	rows, err := t.tx.Query(
		`SELECT consumer, seq, entity, key, updated_at FROM consumer_positions
		 WHERE namespace = ? AND workspace_id = ? ORDER BY consumer`,
		t.ns, t.ws,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.ConsumerPosition
	for rows.Next() {
		var (
			pos     storage.ConsumerPosition
			updated int64
		)
		if err := rows.Scan(&pos.Consumer, &pos.Seq, &pos.Entity, &pos.Key, &updated); err != nil {
			return nil, err
		}
		pos.UpdatedAt = time.UnixMilli(updated)
		out = append(out, pos)
	}
	return out, rows.Err()
}

func (t *sqliteTx) SetConsumerPosition(consumer string, p storage.ConsumerPosition) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(
		`INSERT INTO consumer_positions (namespace, workspace_id, consumer, seq, entity, key, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, consumer)
		 DO UPDATE SET seq = excluded.seq, entity = excluded.entity,
		               key = excluded.key, updated_at = excluded.updated_at`,
		t.ns, t.ws, consumer, p.Seq, p.Entity, p.Key, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	var snapshot []byte
	err := t.tx.QueryRow(
		`SELECT snapshot FROM consumer_shadows
		 WHERE namespace = ? AND workspace_id = ? AND consumer = ? AND entity = ? AND key = ?`,
		t.ns, t.ws, consumer, entity, key,
	).Scan(&snapshot)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (t *sqliteTx) PutConsumerShadow(consumer, entity, key string, snapshot []byte) error {
	if err := t.register(); err != nil {
		return err
	}
	// The column is NOT NULL, so a nil snapshot stores as an empty blob and
	// reads back as one. A consumer that wants "no shadow" deletes it.
	if snapshot == nil {
		snapshot = []byte{}
	}
	_, err := t.tx.Exec(
		`INSERT INTO consumer_shadows (namespace, workspace_id, consumer, entity, key, snapshot)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, consumer, entity, key)
		 DO UPDATE SET snapshot = excluded.snapshot`,
		t.ns, t.ws, consumer, entity, key, snapshot,
	)
	return err
}

func (t *sqliteTx) DeleteConsumerShadow(consumer, entity, key string) error {
	_, err := t.tx.Exec(
		`DELETE FROM consumer_shadows
		 WHERE namespace = ? AND workspace_id = ? AND consumer = ? AND entity = ? AND key = ?`,
		t.ns, t.ws, consumer, entity, key,
	)
	return err
}

func (t *sqliteTx) DeleteConsumer(consumer string) error {
	if _, err := t.tx.Exec(
		`DELETE FROM consumer_shadows WHERE namespace = ? AND workspace_id = ? AND consumer = ?`,
		t.ns, t.ws, consumer,
	); err != nil {
		return err
	}
	_, err := t.tx.Exec(
		`DELETE FROM consumer_positions WHERE namespace = ? AND workspace_id = ? AND consumer = ?`,
		t.ns, t.ws, consumer,
	)
	return err
}

func (t *sqliteTx) CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (int, error) {
	// Shadows first, while the positions that name their consumers still exist:
	// the reverse order would leave every collected consumer's shadows behind
	// with nothing left to identify them by.
	if _, err := t.tx.Exec(
		`DELETE FROM consumer_shadows
		 WHERE namespace = ? AND workspace_id = ? AND consumer IN (
		     SELECT consumer FROM consumer_positions
		     WHERE namespace = ? AND workspace_id = ? AND updated_at < ? AND seq < ?
		 )`,
		t.ns, t.ws, t.ns, t.ws, cutoff.UnixMilli(), belowSeq,
	); err != nil {
		return 0, err
	}
	res, err := t.tx.Exec(
		`DELETE FROM consumer_positions
		 WHERE namespace = ? AND workspace_id = ? AND updated_at < ? AND seq < ?`,
		t.ns, t.ws, cutoff.UnixMilli(), belowSeq,
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (t *sqliteTx) FieldDeltaHorizon() (uint64, error) {
	var horizon uint64
	err := t.tx.QueryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE namespace = ? AND workspace_id = ?`, t.ns, t.ws,
	).Scan(&horizon)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return horizon, err
}

func (t *sqliteTx) FieldDeltas(entity, key, field string, afterSeq uint64) ([][]byte, bool, error) {
	horizon, err := t.FieldDeltaHorizon()
	if err != nil {
		return nil, false, err
	}
	// The span starts where the caller's knowledge ends; anything pruned at or
	// above that point is a hole the fold cannot see.
	if afterSeq < horizon {
		return nil, false, nil
	}
	rows, err := t.tx.Query(
		`SELECT frag FROM field_deltas
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key = ? AND field = ? AND seq > ?
		 ORDER BY seq`,
		t.ns, t.ws, entity, key, field, afterSeq,
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
			return nil, false, nil // gap marker: the span is not expressible as deltas
		}
		frags = append(frags, frag)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return frags, true, nil
}

func (t *sqliteTx) TextFloor(entity, key, field string) ([]byte, error) {
	var vv []byte
	err := t.tx.QueryRow(
		`SELECT vv FROM text_floors
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key = ? AND field = ?`,
		t.ns, t.ws, entity, key, field,
	).Scan(&vv)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return vv, err
}

func (t *sqliteTx) PutTextFloor(entity, key, field string, vv []byte) error {
	if err := t.register(); err != nil {
		return err
	}
	if len(vv) == 0 {
		_, err := t.tx.Exec(
			`DELETE FROM text_floors
			 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key = ? AND field = ?`,
			t.ns, t.ws, entity, key, field,
		)
		return err
	}
	_, err := t.tx.Exec(
		`INSERT INTO text_floors (namespace, workspace_id, entity, key, field, vv)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, entity, key, field)
		 DO UPDATE SET vv = excluded.vv`,
		t.ns, t.ws, entity, key, field, vv,
	)
	return err
}

func (t *sqliteTx) ScanIndex(
	entity, index string,
	lo, hi []byte,
	limit int,
	order storage.ScanOrder,
	fn func(entry []byte, rowKey string) error,
) error {
	if limit <= 0 {
		limit = -1 // SQLite: negative LIMIT means unlimited
	}
	// The bounds are optional on both ends, and SQLite has no way to express
	// "compare only if bound" in the WHERE clause without defeating the index,
	// so the predicate is built to match what was given. k is a BLOB, which
	// SQLite compares with memcmp — the bytewise order the entry encoding is
	// built for, and the reason this must never become a TEXT column.
	where := `namespace = ? AND workspace_id = ? AND entity = ? AND idx = ?`
	args := []any{t.ns, t.ws, entity, index}
	if lo != nil {
		where += ` AND k >= ?`
		args = append(args, lo)
	}
	if hi != nil {
		where += ` AND k < ?`
		args = append(args, hi)
	}
	args = append(args, limit)
	// The row key is part of the sort so that two entries sharing a key come
	// out in a fixed order; reversing the walk has to reverse it too, or the
	// descending scan would not be the exact mirror of the ascending one.
	orderBy := `k, key`
	if order == storage.ScanDesc {
		orderBy = `k DESC, key DESC`
	}
	rows, err := t.tx.Query(
		`SELECT k, key FROM index_entries WHERE `+where+` ORDER BY `+orderBy+` LIMIT ?`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			entry  []byte
			rowKey string
		)
		if err := rows.Scan(&entry, &rowKey); err != nil {
			return err
		}
		if err := fn(entry, rowKey); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (t *sqliteTx) IndexState(entity, index string) (string, bool, error) {
	var (
		specHash string
		ready    int
	)
	err := t.tx.QueryRow(
		`SELECT spec_hash, ready FROM index_state
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND idx = ?`,
		t.ns, t.ws, entity, index,
	).Scan(&specHash, &ready)
	if errors.Is(err, sql.ErrNoRows) {
		// An index nothing has built yet, which is the normal state right
		// after registration — not an error.
		return "", false, nil
	}
	return specHash, ready != 0, err
}

func (t *sqliteTx) SetIndexState(entity, index, specHash string, ready bool) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(
		`INSERT INTO index_state (namespace, workspace_id, entity, idx, spec_hash, ready)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, entity, idx)
		 DO UPDATE SET spec_hash = excluded.spec_hash, ready = excluded.ready`,
		t.ns, t.ws, entity, index, specHash, boolToInt(ready),
	)
	return err
}

// SetIndexBuildMark updates the workspace row and never calls register: the
// mark says "this workspace's indexes are built", which is vacuously true of a
// workspace that holds nothing, and registering one here would let a
// BuildIndexes call on an arbitrary id mint a workspace every later pass
// iterates.
func (t *sqliteTx) SetIndexBuildMark(mark string) error {
	_, err := t.tx.Exec(
		`UPDATE workspaces SET index_build_mark = ? WHERE namespace = ? AND workspace_id = ?`,
		mark, t.ns, t.ws,
	)
	return err
}

// SetTextCompactMark updates the workspace row and never calls register, for
// SetIndexBuildMark's reason: a workspace holding no rows has no text to
// compact, and a sweep must not mint one.
func (t *sqliteTx) SetTextCompactMark(mark storage.TextCompactMark) error {
	_, err := t.tx.Exec(
		`UPDATE workspaces
		    SET text_compact_seq = ?, text_compact_entity = ?, text_compact_key = ?,
		        text_compact_min_purged = ?
		  WHERE namespace = ? AND workspace_id = ?`,
		int64(mark.Seq), mark.Entity, mark.Key, mark.MinPurged, t.ns, t.ws,
	)
	return err
}

func (t *sqliteTx) TextCompactMark() (storage.TextCompactMark, error) {
	var m storage.TextCompactMark
	var seq int64
	err := t.tx.QueryRow(
		`SELECT text_compact_seq, text_compact_entity, text_compact_key, text_compact_min_purged
		   FROM workspaces WHERE namespace = ? AND workspace_id = ?`,
		t.ns, t.ws,
	).Scan(&seq, &m.Entity, &m.Key, &m.MinPurged)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.TextCompactMark{}, nil
	}
	if err != nil {
		return storage.TextCompactMark{}, err
	}
	m.Seq = uint64(seq)
	return m, nil
}

func (t *sqliteTx) PutIndexEntries(entity, key string, entries []storage.IndexEntry) error {
	if len(entries) == 0 {
		// Replace-with-nothing IS the delete, but a row that has never had
		// entries must not register the workspace on its account.
		return t.DeleteIndexEntries(entity, key)
	}
	if err := t.register(); err != nil {
		return err
	}
	// Replace-set by DIFFERENCE, not by clear-and-reinsert. A row carries an
	// entry per index and per element of every indexed list — a dozen indexes
	// is some tens of entries — while the PATCH that triggered the write
	// usually moves one or two of them. Every entry written dirties btree
	// pages that the savepoint each mutation runs in must copy into SQLite's
	// statement sub-journal, so the cost of a push tracks entries WRITTEN, not
	// entries held. Reading the current set first is one range read through
	// index_entries_by_row and turns the common case into no writes at all.
	have, err := t.rowIndexEntries(entity, key)
	if err != nil {
		return err
	}
	want := make(map[rowIndexEntry]struct{}, len(entries))
	for _, e := range entries {
		// A set, so a caller that passes the same entry twice stores one copy
		// instead of failing the whole write on a primary-key conflict: two
		// components of one index can only collide for one row that way, and a
		// harmless caller bug must not stick the write path.
		want[rowIndexEntry{idx: e.Index, k: string(e.Key)}] = struct{}{}
	}
	for e := range have {
		if _, keep := want[e]; keep {
			continue
		}
		if _, err := t.tx.Exec(
			`DELETE FROM index_entries
			 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND idx = ? AND k = ? AND key = ?`,
			t.ns, t.ws, entity, e.idx, []byte(e.k), key,
		); err != nil {
			return err
		}
	}
	for e := range want {
		if _, unchanged := have[e]; unchanged {
			continue
		}
		if _, err := t.tx.Exec(
			`INSERT INTO index_entries (namespace, workspace_id, entity, idx, k, key)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			t.ns, t.ws, entity, e.idx, []byte(e.k), key,
		); err != nil {
			return err
		}
	}
	return nil
}

// rowIndexEntry is one stored entry of one row, as a map key: the index it
// sits in and its sort key held as a string so the pair compares by value.
type rowIndexEntry struct {
	idx string
	k   string
}

// rowIndexEntries reads every index entry one row currently occupies, as the
// set PutIndexEntries diffs against.
func (t *sqliteTx) rowIndexEntries(entity, key string) (map[rowIndexEntry]struct{}, error) {
	rows, err := t.tx.Query(
		`SELECT idx, k FROM index_entries
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key = ?`,
		t.ns, t.ws, entity, key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	have := make(map[rowIndexEntry]struct{})
	for rows.Next() {
		var (
			idx string
			k   []byte
		)
		if err := rows.Scan(&idx, &k); err != nil {
			return nil, err
		}
		have[rowIndexEntry{idx: idx, k: string(k)}] = struct{}{}
	}
	return have, rows.Err()
}

func (t *sqliteTx) DeleteIndexEntries(entity, key string) error {
	// No register(): removing entries a row never had must leave no trace, the
	// same reason a write closure that writes nothing does not create a
	// workspace.
	_, err := t.tx.Exec(
		`DELETE FROM index_entries
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key = ?`,
		t.ns, t.ws, entity, key,
	)
	return err
}

func (t *sqliteTx) PutFieldDelta(entity, key, field string, seq uint64, frag []byte) error {
	if err := t.register(); err != nil {
		return err
	}
	if frag == nil {
		// A gap marker must round-trip as an empty (not NULL) blob: the column is
		// NOT NULL and the read path distinguishes empty from present.
		frag = []byte{}
	}
	_, err := t.tx.Exec(
		`INSERT INTO field_deltas (namespace, workspace_id, entity, key, field, seq, frag, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, entity, key, field, seq)
		 DO UPDATE SET frag = excluded.frag, updated_at = excluded.updated_at`,
		t.ns, t.ws, entity, key, field, seq, frag, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) PruneFieldDeltas(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxPruned uint64
	err := t.tx.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM field_deltas
		 WHERE namespace = ? AND workspace_id = ? AND updated_at < ?`,
		t.ns, t.ws, cutoffMillis,
	).Scan(&maxPruned)
	if err != nil {
		return 0, err
	}
	if maxPruned > 0 {
		if _, err := t.tx.Exec(
			`DELETE FROM field_deltas WHERE namespace = ? AND workspace_id = ? AND updated_at < ?`,
			t.ns, t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// MAX() keeps the horizon monotonic even if an older cutoff runs after a
		// newer one, exactly as CollectTombstones does.
		if _, err := t.tx.Exec(
			`UPDATE workspaces SET field_delta_horizon = MAX(field_delta_horizon, ?)
			 WHERE namespace = ? AND workspace_id = ?`,
			maxPruned, t.ns, t.ws,
		); err != nil {
			return 0, err
		}
	}
	var horizon uint64
	err = t.tx.QueryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE namespace = ? AND workspace_id = ?`, t.ns, t.ws,
	).Scan(&horizon)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return horizon, err
}

func (t *sqliteTx) NextSeq() (uint64, error) {
	if err := t.register(); err != nil {
		return 0, err
	}
	// UPDATE ... RETURNING ports directly to Postgres (with row locking).
	var seq uint64
	err := t.tx.QueryRow(
		`UPDATE workspaces SET last_seq = last_seq + 1
		 WHERE namespace = ? AND workspace_id = ? RETURNING last_seq`,
		t.ns, t.ws,
	).Scan(&seq)
	return seq, err
}

func (t *sqliteTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	// created_version stays out of the conflict SET: the stored value is the
	// key's original creation, which every overwrite keeps (see storage.Row).
	_, err := t.tx.Exec(
		`INSERT INTO rows (namespace, workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, entity, key)
		 DO UPDATE SET value = excluded.value, version = excluded.version, deleted = 0,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ns, t.ws, entity, key, value, version, version, time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
	return err
}

func (t *sqliteTx) DeleteRow(entity, key string, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(
		`INSERT INTO rows (namespace, workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES (?, ?, ?, ?, NULL, ?, ?, 1, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, entity, key)
		 DO UPDATE SET value = NULL, version = excluded.version, deleted = 1,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ns, t.ws, entity, key, version, version, time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
	return err
}

// boolToInt maps a Go bool onto the INTEGER column SQLite stores it in.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (t *sqliteTx) SetClientLastMutationID(clientID string, mutationID uint64) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.tx.Exec(
		`INSERT INTO clients (namespace, workspace_id, client_id, last_mutation_id, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, client_id)
		 DO UPDATE SET last_mutation_id = excluded.last_mutation_id, updated_at = excluded.updated_at`,
		t.ns, t.ws, clientID, mutationID, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) SetClientRejections(clientID string, recs []storage.ClientRejection) error {
	if err := t.register(); err != nil {
		return err
	}
	var blob []byte
	if len(recs) > 0 {
		var err error
		if blob, err = json.Marshal(recs); err != nil {
			return fmt.Errorf("sqlitestore: encode rejections for client %q: %w", clientID, err)
		}
	}
	_, err := t.tx.Exec(
		`INSERT INTO clients (namespace, workspace_id, client_id, rejections, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workspace_id, client_id)
		 DO UPDATE SET rejections = excluded.rejections, updated_at = excluded.updated_at`,
		t.ns, t.ws, clientID, blob, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) InSavepoint(fn func() error) error {
	t.spDepth++
	name := fmt.Sprintf("sp%d", t.spDepth)
	defer func() { t.spDepth-- }()
	if _, err := t.tx.Exec(`SAVEPOINT ` + name); err != nil {
		return err
	}
	if err := fn(); err != nil {
		// The rewind may undo register's INSERT (a rejected mutation that was
		// this transaction's only write), so re-run it on the next write. When
		// the INSERT predates the savepoint the extra statement is a harmless
		// ON CONFLICT DO NOTHING.
		t.registered = false
		// ROLLBACK TO rewinds the writes but keeps the savepoint open;
		// RELEASE discards it.
		if _, rbErr := t.tx.Exec(`ROLLBACK TO ` + name); rbErr != nil {
			return errors.Join(err, fmt.Errorf("sqlitestore: rollback savepoint: %w", rbErr))
		}
		if _, rbErr := t.tx.Exec(`RELEASE ` + name); rbErr != nil {
			return errors.Join(err, fmt.Errorf("sqlitestore: release savepoint: %w", rbErr))
		}
		return err
	}
	if _, err := t.tx.Exec(`RELEASE ` + name); err != nil {
		return fmt.Errorf("sqlitestore: release savepoint: %w", err)
	}
	return nil
}

func (t *sqliteTx) CollectTombstones(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxDeleted uint64
	err := t.tx.QueryRow(
		`SELECT COALESCE(MAX(version), 0) FROM rows
		 WHERE namespace = ? AND workspace_id = ? AND deleted = 1 AND updated_at < ?`,
		t.ns, t.ws, cutoffMillis,
	).Scan(&maxDeleted)
	if err != nil {
		return 0, err
	}
	if maxDeleted > 0 {
		if _, err := t.tx.Exec(
			`DELETE FROM rows WHERE namespace = ? AND workspace_id = ? AND deleted = 1 AND updated_at < ?`,
			t.ns, t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// MAX() keeps the horizon monotonic even if an older cutoff runs
		// after a newer one.
		if _, err := t.tx.Exec(
			`UPDATE workspaces SET tombstone_horizon = MAX(tombstone_horizon, ?)
			 WHERE namespace = ? AND workspace_id = ?`,
			maxDeleted, t.ns, t.ws,
		); err != nil {
			return 0, err
		}
	}
	return t.TombstoneHorizon()
}

// PurgeEntityRows hard-deletes the selected rows of one entity and the index
// entries they occupied. Entries go first so no statement can observe an entry
// whose row this call has already removed; both are in the caller's
// transaction, so a rollback keeps them together either way.
//
// The prefix becomes a half-open key RANGE rather than a LIKE: the column is
// BINARY-collated, so a range is the same bytewise comparison every other key
// predicate here makes, and it needs no escaping of LIKE's wildcards.
//
// A limit turns both statements into the same `key IN (SELECT … ORDER BY key
// LIMIT n)` subquery. The ORDER BY is load-bearing rather than cosmetic: the two
// subqueries run against the same rows (the entries delete does not touch them)
// and must select the SAME set, or entries outlive their row or die before it.
// A total order over a unique key is what makes them agree.
//
// more is "this call hit its limit", which the interface allows: the drain that
// exactly emptied the selection costs one more no-op call, which is cheaper than
// probing for a next row on every chunk.
func (t *sqliteTx) PurgeEntityRows(entity, keyPrefix string, before time.Time, limit int) (int, bool, error) {
	where, args := purgeWhere(t.ns, t.ws, entity, keyPrefix, before)
	selected, rowsWhere := where, where
	rowsArgs := args
	if limit > 0 {
		selected = where + " ORDER BY key LIMIT ?"
		args = append(args, limit)
		// The outer predicate repeats the (namespace, workspace, entity) keys the
		// subquery already applies; the subquery is what narrows the delete.
		rowsWhere = `namespace = ? AND workspace_id = ? AND entity = ? AND key IN (
		   SELECT key FROM rows WHERE ` + selected + `)`
		rowsArgs = append([]any{t.ns, t.ws, entity}, args...)
	}
	if _, err := t.tx.Exec(
		`DELETE FROM index_entries
		 WHERE namespace = ? AND workspace_id = ? AND entity = ? AND key IN (
		   SELECT key FROM rows WHERE `+selected+`
		 )`,
		append([]any{t.ns, t.ws, entity}, args...)...,
	); err != nil {
		return 0, false, err
	}
	res, err := t.tx.Exec(`DELETE FROM rows WHERE `+rowsWhere, rowsArgs...)
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	return int(n), limit > 0 && int(n) == limit, err
}

// purgeWhere builds PurgeEntityRows' row predicate and its arguments. Both
// narrowings are optional: an empty prefix bounds nothing, and a zero `before`
// means no age bound at all — which is NOT what its UnixMilli() would say, so
// the branch is load-bearing rather than an optimization.
func purgeWhere(ns, ws, entity, keyPrefix string, before time.Time) (string, []any) {
	where := "namespace = ? AND workspace_id = ? AND entity = ?"
	args := []any{ns, ws, entity}
	if keyPrefix != "" {
		hi, bounded := prefixUpperBound(keyPrefix)
		where += " AND key >= ?"
		args = append(args, keyPrefix)
		// Both halves are needed: ">= prefix" alone also matches every key that
		// merely sorts after it. The upper bound is missing only for a prefix of
		// nothing but 0xff bytes, which no UTF-8 key can contain — so in
		// practice this branch always runs.
		if bounded {
			where += " AND key < ?"
			args = append(args, hi)
		}
	}
	if !before.IsZero() {
		where += " AND updated_at < ?"
		args = append(args, before.UnixMilli())
	}
	return where, args
}

// prefixUpperBound returns the least string greater than every string starting
// with prefix, and whether one exists. Increment the last byte that can be
// incremented and drop everything after it; a prefix of nothing but 0xff bytes
// has no upper bound.
func prefixUpperBound(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

func (t *sqliteTx) CollectIdleClients(cutoff time.Time) (int, error) {
	res, err := t.tx.Exec(
		`DELETE FROM clients WHERE namespace = ? AND workspace_id = ? AND updated_at < ?`,
		t.ns, t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}
