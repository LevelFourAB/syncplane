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
// several apps from one file, open one Store and partition it with
// prefixstore.
//
// Tuning: the defaults suit a server rather than an embedded library, and each
// has an Option to move it — WithCacheSize (page cache, charged per
// connection), WithMmapSize (how much of the file is read through a shared
// memory mapping instead of a syscall), WithReadConnections (reader pool
// ceiling, and with it the store's memory ceiling), WithWALSizeLimit (how large
// the -wal sidecar is left after a checkpoint) and WithOptimizeInterval (how
// often query-planner statistics are refreshed; see maintain). The rest is
// fixed: synchronous is NORMAL, the WAL default, which trades the last commits
// before a power loss or kernel panic — not a process crash — for not paying an
// fsync per transaction. Each connection also holds a prepared statement per distinct SQL
// text the store has run on it — a bounded set, but one that counts against the
// same per-connection budget as the page cache.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sort"
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
	workspace_id        TEXT NOT NULL,
	last_seq            INTEGER NOT NULL DEFAULT 0,
	tombstone_horizon   INTEGER NOT NULL DEFAULT 0,
	-- A random identity pinning a cursor to the store lineage that issued it
	-- (see ReadTx.StoreEpoch); 0 reads back as "no epoch yet".
	store_epoch         INTEGER NOT NULL DEFAULT 0,
	-- The field-delta log's pruned-through watermark: a span reaching below it
	-- cannot be trusted and falls back to full state.
	field_delta_horizon INTEGER NOT NULL DEFAULT 0,
	-- The opaque fingerprint of the index declarations this workspace's entries
	-- are fully built under (see ListStaleIndexWorkspaceIDs). Empty is "never
	-- built", which is what makes a fresh workspace stale to the first pass.
	index_build_mark    TEXT NOT NULL DEFAULT '',
	-- How far the text-compaction sweep has considered rows in ScanChanges
	-- order, and the minPurged threshold it did so under (see
	-- ListCompactableWorkspaceIDs). Zero is "never swept".
	text_compact_seq        INTEGER NOT NULL DEFAULT 0,
	text_compact_entity     TEXT NOT NULL DEFAULT '',
	text_compact_key        TEXT NOT NULL DEFAULT '',
	text_compact_min_purged INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id)
);

CREATE TABLE rows (
	workspace_id  TEXT NOT NULL,
	entity        TEXT NOT NULL,
	key           TEXT NOT NULL,
	value         BLOB,
	version       INTEGER NOT NULL,
	-- The sequence that created the row (see storage.Row.CreatedVersion):
	-- stamped on insert, kept on every overwrite.
	created_version INTEGER NOT NULL DEFAULT 0,
	deleted       INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER NOT NULL DEFAULT 0,
	-- Write attribution (see storage.Actor), two columns rather than one
	-- encoded blob so a query can filter on a subject or a system label
	-- without parsing, and so the empty string keeps its ordinary meaning.
	actor_subject TEXT NOT NULL DEFAULT '',
	actor_system  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (workspace_id, entity, key)
);
-- What ScanChanges pages the whole workspace by.
CREATE INDEX rows_by_version ON rows (workspace_id, version);
-- Partial, because deleted = 1 is the only collectable state, which keeps the
-- GC index proportional to the dead rows rather than the live ones. Without
-- it every pass range-scans the workspace: the primary key narrows to the
-- workspace but not to the collectable subset.
CREATE INDEX rows_gc ON rows (workspace_id, updated_at) WHERE deleted = 1;
-- Backs ScanEntityByVersionDesc. A recency window asks for ONE entity's newest
-- live rows, and rows_by_version orders by version across every entity, so
-- serving it from there means reading and discarding the other entities' rows.
-- It is a second index rather than a widening of the first, because putting
-- entity ahead of version there would cost the pull path its index. The key
-- order matches aheadByRecency's tie-break (version descending, key ascending)
-- so a page boundary lands where the in-memory top-K would have put it.
CREATE INDEX rows_by_entity_version
	ON rows (workspace_id, entity, version DESC, key) WHERE deleted = 0;

CREATE TABLE clients (
	workspace_id     TEXT NOT NULL,
	client_id        TEXT NOT NULL,
	last_mutation_id INTEGER NOT NULL DEFAULT 0,
	updated_at       INTEGER NOT NULL DEFAULT 0,
	-- Verdicts on refused mutations, so a retry that follows a lost push
	-- response is answered with the rejection instead of a bare ack. NULL until
	-- a client has had something refused, which is the common case.
	rejections       BLOB,
	PRIMARY KEY (workspace_id, client_id)
);
CREATE INDEX clients_gc ON clients (workspace_id, updated_at);

-- The server-global catalog (see storage.Catalog): opaque payloads keyed by id
-- and belonging to no workspace, which is how a multi-app host persists each
-- application's registered FileDescriptorSet and reloads it on boot.
CREATE TABLE catalog (
	id         TEXT PRIMARY KEY,
	payload    BLOB NOT NULL,
	revision   TEXT NOT NULL,
	updated_at INTEGER NOT NULL DEFAULT 0
);

-- The collaborative-text delta log — one row per (text field, write) holding
-- the fragment that write added, so a pull can ship a keystroke instead of a
-- document. The primary key is ordered so the per-field span read (one row,
-- one field, seq > cursor) is a range scan of its tail. frag is empty for a
-- gap marker (see WriteTx.PutFieldDelta).
CREATE TABLE field_deltas (
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	field        TEXT NOT NULL,
	seq          INTEGER NOT NULL,
	frag         BLOB NOT NULL,
	updated_at   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, entity, key, field, seq)
);
CREATE INDEX field_deltas_gc ON field_deltas (workspace_id, updated_at);

-- Collaborative-text compaction floors — one row per text field whose
-- tombstones have been purged, holding the version vector the document held at
-- the purge. Read on every write to such a field, so it is keyed the way the
-- row is.
CREATE TABLE text_floors (
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	field        TEXT NOT NULL,
	vv           BLOB NOT NULL,
	PRIMARY KEY (workspace_id, entity, key, field)
);

-- The idempotency ledger for trusted writes — one row per claimed key, holding
-- the sequence the original write took and the caller's recorded result.
CREATE TABLE idempotency (
	workspace_id TEXT NOT NULL,
	key          TEXT NOT NULL,
	seq          INTEGER NOT NULL,
	result       BLOB NOT NULL,
	updated_at   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, key)
);
-- Backs the retention sweep, which is a range scan of one workspace's expired
-- keys rather than of all of them.
CREATE INDEX idempotency_gc ON idempotency (workspace_id, updated_at);

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
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	idx          TEXT NOT NULL,
	k            BLOB NOT NULL,
	key          TEXT NOT NULL,
	PRIMARY KEY (workspace_id, entity, idx, k, key)
) WITHOUT ROWID;
-- What makes the write path affordable: entries are replaced per row and
-- deleted per row, and both need "every entry this row holds" without knowing
-- which indexes those are.
CREATE INDEX index_entries_by_row
	ON index_entries (workspace_id, entity, key);
-- Per-index build state: whether the entries cover the whole entity yet. Keyed
-- like the entries, so one readiness flag never spans two entities' sets.
CREATE TABLE index_state (
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	idx          TEXT NOT NULL,
	spec_hash    TEXT NOT NULL,
	ready        INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, entity, idx)
);

-- Effect-consumer state — how far each consumer has acted on the change feed,
-- and the per-row snapshots it derives before-state from (see
-- storage.ConsumerPosition). Both are written by the SAME transaction, which is
-- what makes a crash redeliver a change instead of skipping it.
CREATE TABLE consumer_positions (
	workspace_id TEXT NOT NULL,
	consumer     TEXT NOT NULL,
	seq          INTEGER NOT NULL DEFAULT 0,
	entity       TEXT NOT NULL DEFAULT '',
	key          TEXT NOT NULL DEFAULT '',
	updated_at   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, consumer)
);
-- Backs the staleness sweep, which is a range scan of one workspace's expired
-- consumers rather than of every consumer.
CREATE INDEX consumer_positions_gc
	ON consumer_positions (workspace_id, updated_at);
-- No GC index of its own: shadows are only ever deleted per row, or wholesale
-- with their consumer, and the primary key's (workspace_id,
-- consumer) prefix is the range scan for that.
CREATE TABLE consumer_shadows (
	workspace_id TEXT NOT NULL,
	consumer     TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	snapshot     BLOB NOT NULL,
	PRIMARY KEY (workspace_id, consumer, entity, key)
);

-- Cursor-state entries: the membership sets of issued pull cursors, keyed by
-- the (id, gen) reference the cursor carries. See cursorstate.go for the
-- pruning that keeps the table bounded.
CREATE TABLE cursor_state (
	workspace_id TEXT NOT NULL,
	id           INTEGER NOT NULL,
	gen          INTEGER NOT NULL,
	state        BLOB NOT NULL,
	written_at   INTEGER NOT NULL,
	PRIMARY KEY (workspace_id, id, gen)
);
`

// migrations[i] upgrades a database at user_version baselineVersion+i to
// baselineVersion+i+1. Each runs in its own transaction; user_version is
// bumped inside that transaction. A structural change appends here and leaves
// schema alone, so a database in the field reaches the new shape by the same
// steps a fresh one skips.
var migrations = []string{
	// 2: sequence lineage — one row per committed write transaction that
	// allocated sequences, naming the first of them and the mark every one of
	// them carries (see ReadTx.SeqLineage). Lookups take the greatest
	// start_seq at or below a sequence, a descent of the primary key; the _gc
	// index serves PruneSeqLineage and the collectable probe.
	`CREATE TABLE seq_lineage (
	workspace_id TEXT NOT NULL,
	start_seq    INTEGER NOT NULL,
	mark         INTEGER NOT NULL,
	written_at   INTEGER NOT NULL,
	PRIMARY KEY (workspace_id, start_seq)
);
CREATE INDEX seq_lineage_gc ON seq_lineage (workspace_id, written_at);`,
	// 3: deletion marks — one row per workspace DeleteWorkspace removed and
	// RestoreWorkspace has not lifted since (see storage.Adapter). Read by
	// register on a workspace's first write and by WorkspaceDeleted.
	`CREATE TABLE deleted_workspaces (
	workspace_id TEXT NOT NULL,
	deleted_at   INTEGER NOT NULL,
	PRIMARY KEY (workspace_id)
);`,
	// 4: compact index entries. The entries are dropped rather than converted:
	// an index is derived, so clearing index_state and every build mark makes
	// the build pass rewrite them from the rows, and reads scan until it has.
	//
	// index_scopes gives each (workspace, entity, index) a short id, so an entry
	// stores an integer where it stored three names. AUTOINCREMENT keeps an id
	// from ever being handed out twice: an entry left under a reused id would
	// show up in the index of whichever workspace got it next.
	//
	// index_entries is WITHOUT ROWID with a covering primary key, so a range
	// scan reads the b-tree it is ordered by and never touches rows. k is a
	// BLOB, which SQLite compares with memcmp — the bytewise order the engine's
	// key encoder is built for. key sits in the key after k for uniqueness
	// only, and costs no bytes there: a WITHOUT ROWID table stores every column
	// in the cell either way.
	//
	// index_row_entries holds, per row, the packed list of entries it occupies
	// (see encodeRowEntries). Entries are replaced and deleted per row without
	// the caller naming the indexes, and one list per row finds them at a
	// fraction of the bytes a secondary index over index_entries costs, which
	// would repeat the workspace, entity, row key and entry per entry.
	`DROP INDEX index_entries_by_row;
DROP TABLE index_entries;
DELETE FROM index_state;
UPDATE workspaces SET index_build_mark = '';
CREATE TABLE index_scopes (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	idx          TEXT NOT NULL,
	UNIQUE (workspace_id, entity, idx)
);
CREATE TABLE index_entries (
	scope INTEGER NOT NULL,
	k     BLOB NOT NULL,
	key   TEXT NOT NULL,
	PRIMARY KEY (scope, k, key)
) WITHOUT ROWID;
CREATE TABLE index_row_entries (
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	entries      BLOB NOT NULL,
	PRIMARY KEY (workspace_id, entity, key)
) WITHOUT ROWID;`,
	// 5: index entries without a second copy of the row key. The entries and
	// the per-row lists are dropped for a rebuild, as in migration 4; the
	// scopes stay, because their ids are still valid.
	//
	// The engine ends most entries k with the row key (see rowKeySuffix), so key
	// holds the byte length of the row key when k ends with it, and the row
	// key text only when k does not. A small integer costs one byte where a
	// UUID key costs 36. key has no declared type, so
	// its affinity is BLOB and SQLite stores each value with the type it was
	// bound with: a TEXT or NUMERIC affinity would convert one form into the
	// other. The primary key stays unique: two rows with the same k and the
	// same length have the same row key, and an integer never equals a text.
	`DROP TABLE index_entries;
DELETE FROM index_row_entries;
DELETE FROM index_state;
UPDATE workspaces SET index_build_mark = '';
CREATE TABLE index_entries (
	scope INTEGER NOT NULL,
	k     BLOB NOT NULL,
	key   NOT NULL,
	PRIMARY KEY (scope, k, key)
) WITHOUT ROWID;`,
	// 6: a short id per row in place of the row key text. The entries and the
	// per-row lists are dropped for a rebuild, as in migration 5.
	//
	// index_row_keys gives each row with entries an integer id, and
	// index_row_entries records the id next to the row's entry list. The id is
	// an INTEGER PRIMARY KEY because VACUUM keeps such an id and can renumber a
	// plain rowid. AUTOINCREMENT gives each id one time only, so a committed
	// id names one row key for the life of the file, which is what lets the
	// Store cache it (see rowKeyCache). The map is a table of its own, not a
	// column of the lists, because a scan reads it once per entry that misses
	// the cache: its rows are short and its pages stay in the page cache,
	// while a list is kilobytes and fills a page by itself, which made each
	// lookup a page read.
	//
	// The key column of index_entries holds one of two integers (see
	// entryKeyValue):
	//   - zero or less: the byte length of the row key negated, when k ends
	//     with rowKeySuffix(row key).
	//   - more than zero: the id of the row, for any other entry. A search
	//     entry does not end with its row key, and the entries of all the rows
	//     that hold one term are byte-equal.
	// The primary key stays unique: one k with one length is one row key, one
	// id is one row, and no negated length equals an id. The id is the form
	// that is not negated because a row new to the index has the greatest id,
	// so its entry goes at the end of the entries that share its k. SQLite
	// fills b-tree pages best when keys arrive in ascending order; a negated
	// id makes them arrive in descending order, which splits more pages and
	// makes each put slower. An entry is deleted in the transaction that
	// deletes its row's id (see DeleteIndexEntries and DeleteWorkspace), so no
	// entry names an id that no row holds.
	`DROP TABLE index_entries;
DROP TABLE index_row_entries;
DELETE FROM index_state;
UPDATE workspaces SET index_build_mark = '';
CREATE TABLE index_row_keys (
	id  INTEGER PRIMARY KEY AUTOINCREMENT,
	key TEXT NOT NULL
);
CREATE TABLE index_row_entries (
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	id           INTEGER NOT NULL,
	entries      BLOB NOT NULL,
	PRIMARY KEY (workspace_id, entity, key)
) WITHOUT ROWID;
CREATE TABLE index_entries (
	scope INTEGER NOT NULL,
	k     BLOB NOT NULL,
	key   NOT NULL,
	PRIMARY KEY (scope, k, key)
) WITHOUT ROWID;`,
	// 7: entry lists in a smaller format, in a table keyed by row id. The
	// entries, the lists and the row ids are dropped for a rebuild, as in
	// migration 6: a list in the old format cannot be read, and an entry or
	// id left without its list cannot be found to be deleted.
	//
	// encodeRowEntries writes each scope once and stores an entry as the
	// bytes it does not share with the entry before it. index_row_ids maps a
	// row to its id, and index_row_entries holds the list of each id. The
	// lists are in a rowid table because SQLite keeps a cell of up to about a
	// page on a page of a rowid table, but only about a quarter page on a page
	// of a WITHOUT ROWID table. A list of a text-heavy row is a few kilobytes,
	// so in a WITHOUT ROWID table it puts all but its first bytes on an
	// overflow page of its own, and leaves most of that page empty.
	// index_row_keys stays a table of its own, for the reason migration 6
	// gives.
	`DROP TABLE index_entries;
DROP TABLE index_row_entries;
DELETE FROM index_row_keys;
DELETE FROM index_state;
UPDATE workspaces SET index_build_mark = '';
CREATE TABLE index_row_ids (
	workspace_id TEXT NOT NULL,
	entity       TEXT NOT NULL,
	key          TEXT NOT NULL,
	id           INTEGER NOT NULL,
	PRIMARY KEY (workspace_id, entity, key)
) WITHOUT ROWID;
CREATE TABLE index_row_entries (
	id      INTEGER PRIMARY KEY,
	entries BLOB NOT NULL
);
CREATE TABLE index_entries (
	scope INTEGER NOT NULL,
	k     BLOB NOT NULL,
	key   NOT NULL,
	PRIMARY KEY (scope, k, key)
) WITHOUT ROWID;`,
}

// workspaceTables names every table that holds state of one workspace, keyed
// by its workspace_id column. DeleteWorkspace empties each of them for the
// workspace it deletes, so a table added by a migration must be added here
// too; deleted_workspaces is the one table with that column left out, because
// it holds the mark DeleteWorkspace leaves. index_entries, index_row_keys and
// index_row_entries have no such column: their rows belong to a workspace
// through index_scopes and index_row_ids, and DeleteWorkspace deletes them
// through those before it empties them.
var workspaceTables = []string{
	"workspaces",
	"rows",
	"clients",
	"field_deltas",
	"text_floors",
	"idempotency",
	"index_scopes",
	"index_row_ids",
	"index_state",
	"consumer_positions",
	"consumer_shadows",
	"cursor_state",
	"seq_lineage",
}

// Store implements storage.Adapter. read and write are the same handle for an
// in-memory database.
type Store struct {
	read  *sql.DB
	write *sql.DB
	// Prepared statements per pool. readStmts and writeStmts are the same
	// cache exactly when read and write are the same handle.
	readStmts  *stmtCache
	writeStmts *stmtCache
	// leases backs the lease.Provider implementation (see leases.go).
	leases *lease.Local
	// rowKeys resolves the row ids that index entries hold (see ScanIndex).
	rowKeys *rowKeyCache

	// The maintenance loop: stop asks it to return and done reports that it
	// has. Both are nil when
	// WithOptimizeInterval disabled the loop.
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// Option configures Open.
type Option func(*config)

type config struct {
	cacheSize        int64
	mmapSize         int64
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
	// defaultMmapSize is how much of the database file SQLite may read through a
	// memory mapping rather than a read syscall (see WithMmapSize). A gibibyte
	// covers a database far larger than the page cache does, because it costs
	// address space rather than memory and is shared by every connection instead
	// of charged to each; SQLite maps whichever is smaller, this or the file.
	defaultMmapSize = 1 << 30
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
		mmapSize:         defaultMmapSize,
		readConns:        max(4, min(runtime.GOMAXPROCS(0), 8)),
		walSizeLimit:     defaultWALSizeLimit,
		optimizeInterval: defaultOptimizeInterval,
	}
}

// WithCacheSize sets SQLite's page cache in bytes. It is charged per
// connection, so the store's worst case is this times one writer plus every
// reader connection — raise it together with, not independently of,
// WithReadConnections. Zero leaves SQLite's own default.
func WithCacheSize(bytes int64) Option {
	return func(c *config) { c.cacheSize = bytes }
}

// WithMmapSize caps how much of the database file each connection may read
// through a memory mapping rather than a read syscall, in bytes. A mapped page
// is read straight out of the operating system's page cache, with no syscall
// and no copy into the connection's own cache, and the mapping is backed by
// pages the operating system already holds — so unlike WithCacheSize it costs
// address space rather than memory, and every connection shares one copy.
// Zero disables mapping and puts every page read back on a syscall.
//
// The caveat is the one SQLite documents for mmap: an I/O error on a mapped
// page reaches the process as a signal (SIGBUS) rather than an error code the
// call can return. A database on storage that can fail out from under a running
// process — a network mount, a removable disk — is a reason to pass zero.
func WithMmapSize(bytes int64) Option {
	return func(c *config) { c.mmapSize = bytes }
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
// prefixstore to multiplex several applications over one Store.
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
	writeStmts := newStmtCache(write)
	readStmts := writeStmts
	if read != write {
		readStmts = newStmtCache(read)
	}
	s := &Store{
		read: read, write: write,
		readStmts: readStmts, writeStmts: writeStmts,
		leases:  lease.NewLocal(),
		rowKeys: newRowKeyCache(rowKeyCacheCap),
	}
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
	if c.mmapSize > 0 {
		params = append(params, fmt.Sprintf("_pragma=mmap_size(%d)", c.mmapSize))
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

// Close stops the maintenance loop and closes the database handle.
func (s *Store) Close() error {
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
	// Statements go before the pools they were prepared on.
	err := s.writeStmts.close()
	if s.readStmts != s.writeStmts {
		err = errors.Join(err, s.readStmts.close())
	}
	err = errors.Join(err, s.write.Close())
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
	// Before BeginTx, because preparing needs a connection of its own and this
	// goroutine is about to hold one (the only one, for an in-memory store).
	s.readStmts.warm(ctx)
	// ReadOnly keeps modernc on a plain deferred BEGIN; without it the writer
	// DSN's _txlock=immediate would make every read take the write lock (and
	// an in-memory store, whose reads run on the writer pool, serialize).
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(readOnlyTx{&sqliteTx{
		tx: tx, stmts: s.readStmts, ws: workspaceID,
		rowKeys: s.rowKeys, learnRowKeys: true,
	}})
}

func (s *Store) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	// Before BeginTx: the writer pool has one connection, so preparing from
	// inside the transaction would wait on the connection the transaction holds.
	s.writeStmts.warm(ctx)
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The workspaces row is NOT created here: sqliteTx.register mints it on the
	// transaction's first actual write. A closure that only reads (a read-only
	// Mutate, a push whose every mutation was already applied) must leave the
	// workspace unregistered — see the WithWriteTx contract.
	st := &sqliteTx{tx: tx, stmts: s.writeStmts, ws: workspaceID, rowKeys: s.rowKeys}
	if err := fn(st); err != nil {
		return err
	}
	if err := st.commitLineage(); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteWorkspace empties every table in workspaceTables for the workspace
// and records its mark, in one write transaction.
func (s *Store) DeleteWorkspace(ctx context.Context, workspaceID string) error {
	return s.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		t := tx.(*sqliteTx)
		if _, err := t.exec(
			`DELETE FROM index_entries
			 WHERE scope IN (SELECT id FROM index_scopes WHERE workspace_id = ?)`,
			workspaceID,
		); err != nil {
			return fmt.Errorf("sqlitestore: delete workspace %q from index_entries: %w", workspaceID, err)
		}
		for _, table := range []string{"index_row_entries", "index_row_keys"} {
			if _, err := t.exec(
				`DELETE FROM `+table+`
				 WHERE id IN (SELECT id FROM index_row_ids WHERE workspace_id = ?)`,
				workspaceID,
			); err != nil {
				return fmt.Errorf("sqlitestore: delete workspace %q from %s: %w", workspaceID, table, err)
			}
		}
		for _, table := range workspaceTables {
			if _, err := t.exec(`DELETE FROM `+table+` WHERE workspace_id = ?`, workspaceID); err != nil {
				return fmt.Errorf("sqlitestore: delete workspace %q from %s: %w", workspaceID, table, err)
			}
		}
		_, err := t.exec(
			`INSERT INTO deleted_workspaces (workspace_id, deleted_at) VALUES (?, ?) ON CONFLICT DO NOTHING`,
			workspaceID, time.Now().UnixMilli(),
		)
		return err
	})
}

// RestoreWorkspace removes the workspace's deletion mark. It runs on the
// writer connection, like every write, so it cannot land halfway through a
// write transaction.
func (s *Store) RestoreWorkspace(ctx context.Context, workspaceID string) error {
	return s.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		_, err := tx.(*sqliteTx).exec(`DELETE FROM deleted_workspaces WHERE workspace_id = ?`, workspaceID)
		return err
	})
}

func (s *Store) WorkspaceDeleted(ctx context.Context, workspaceID string) (bool, error) {
	var deleted bool
	err := s.read.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM deleted_workspaces WHERE workspace_id = ?)`, workspaceID,
	).Scan(&deleted)
	return deleted, err
}

func (s *Store) ListWorkspaceIDs(ctx context.Context, rng storage.Range) ([]string, error) {
	where, args := rangeWhere("workspace_id", rng)
	rows, err := s.read.QueryContext(ctx,
		`SELECT workspace_id FROM workspaces WHERE `+where+` ORDER BY workspace_id`, args...)
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
// order, and each row probes the _gc index of each collectable table on its
// own (workspace_id, timestamp) prefix. Each probe stops at the first match, so
// a workspace with nothing collectable costs one index descent per table and no
// transaction at all.
//
// The entity purges are the exception. Live rows of an aging entity are found
// on rows_by_entity_version's (workspace_id, entity) prefix with
// updated_at left as a filter, so a workspace whose entity has no expired row
// reads that entity's entries to say so — the same range PurgeEntityRows walks
// to delete them, read instead of written.
func (s *Store) ListCollectableWorkspaceIDs(ctx context.Context, cutoffs storage.GCCutoffs, rng storage.Range, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	where, args := collectableWhere(rng, cutoffs)
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
func (s *Store) ListStaleIndexWorkspaceIDs(ctx context.Context, mark string, rng storage.Range, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	where, args := rangeWhere("workspace_id", rng)
	args = append(args, mark, limit)
	rows, err := s.read.QueryContext(ctx,
		`SELECT workspace_id FROM workspaces
		 WHERE `+where+` AND index_build_mark <> ?
		 ORDER BY workspace_id LIMIT ?`, args...)
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
func (s *Store) ListCompactableWorkspaceIDs(ctx context.Context, minPurged int, rng storage.Range, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	where, args := rangeWhere("workspace_id", rng)
	args = append(args, minPurged, limit)
	rows, err := s.read.QueryContext(ctx,
		`SELECT workspace_id FROM workspaces
		 WHERE `+where+`
		   AND (text_compact_seq < field_delta_horizon
		        OR (text_compact_seq = field_delta_horizon AND text_compact_entity <> '')
		        OR text_compact_min_purged > ?)
		 ORDER BY workspace_id LIMIT ?`, args...)
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
func collectableWhere(rng storage.Range, cutoffs storage.GCCutoffs) (string, []any) {
	where, args := rangeWhere("w.workspace_id", rng)
	next := func(v any) string {
		args = append(args, v)
		return "?"
	}
	var probes []string
	exists := func(body string) { probes = append(probes, "EXISTS (SELECT 1 FROM "+body+")") }
	scoped := func(table string) string {
		return table + " t WHERE t.workspace_id = w.workspace_id"
	}
	if !cutoffs.Tombstones.IsZero() {
		at := cutoffs.Tombstones.UnixMilli()
		exists(scoped("rows") + " AND t.deleted = 1 AND t.updated_at < " + next(at))
		exists(scoped("clients") + " AND t.updated_at < " + next(at) + " AND t.rejections IS NOT NULL")
		exists(scoped("consumer_positions") + " AND t.updated_at < " + next(at))
		exists(scoped("seq_lineage") + " AND t.written_at < " + next(at))
	}
	if !cutoffs.Clients.IsZero() {
		exists(scoped("clients") + " AND t.updated_at < " + next(cutoffs.Clients.UnixMilli()))
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
// row probes consumer_positions on its own primary key prefix (workspace_id,
// consumer). A workspace with no position row for the consumer is
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
func (s *Store) ListPendingWorkspaceIDs(ctx context.Context, consumer string, rng storage.Range, limit int) ([]string, error) {
	if consumer == "" {
		return nil, fmt.Errorf("sqlitestore: consumer name is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("sqlitestore: limit must be positive, got %d", limit)
	}
	where, args := rangeWhere("w.workspace_id", rng)
	args = append([]any{consumer}, append(args, limit)...)
	rows, err := s.read.QueryContext(ctx,
		`SELECT w.workspace_id
		 FROM workspaces w
		 LEFT JOIN consumer_positions p
		   ON p.workspace_id = w.workspace_id
		   AND p.consumer = ?
		 WHERE `+where+`
		   AND (p.seq IS NULL OR p.seq < w.last_seq OR p.entity <> '' OR p.key <> '')
		 ORDER BY w.workspace_id
		 LIMIT ?`, args...)
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

// PutCatalogEntry upserts an entry into the server-global catalog. It runs on
// the writer connection.
func (s *Store) PutCatalogEntry(ctx context.Context, id string, payload []byte, revision string) error {
	_, err := s.write.ExecContext(ctx,
		`INSERT INTO catalog (id, payload, revision, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (id)
		 DO UPDATE SET payload = excluded.payload,
		               revision = excluded.revision, updated_at = excluded.updated_at`,
		id, payload, revision, time.Now().UnixMilli(),
	)
	return err
}

// SwapCatalogEntry stores an entry of the server-global catalog when its stored
// revision is prev. It runs on the writer connection.
func (s *Store) SwapCatalogEntry(ctx context.Context, id string, payload []byte, revision, prev string) error {
	now := time.Now().UnixMilli()
	var (
		res sql.Result
		err error
	)
	if prev == "" {
		res, err = s.write.ExecContext(ctx,
			`INSERT INTO catalog (id, payload, revision, updated_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT (id) DO NOTHING`,
			id, payload, revision, now)
	} else {
		res, err = s.write.ExecContext(ctx,
			`UPDATE catalog SET payload = ?, revision = ?, updated_at = ?
			 WHERE id = ? AND revision = ?`,
			payload, revision, now, id, prev)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return storage.ErrCatalogConflict
	}
	return nil
}

func (s *Store) ListCatalogEntries(ctx context.Context) ([]storage.CatalogEntry, error) {
	rows, err := s.read.QueryContext(ctx,
		`SELECT id, payload, revision, updated_at FROM catalog ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.CatalogEntry
	for rows.Next() {
		var (
			e       storage.CatalogEntry
			updated int64
		)
		if err := rows.Scan(&e.ID, &e.Payload, &e.Revision, &updated); err != nil {
			return nil, err
		}
		e.UpdatedAt = time.UnixMilli(updated)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) DeleteCatalogEntry(ctx context.Context, id string) error {
	_, err := s.write.ExecContext(ctx, `DELETE FROM catalog WHERE id = ?`, id)
	return err
}

// rangeWhere renders rng as a predicate on col, with its arguments. The upper
// bound is left out when the range has none rather than compared against a
// sentinel, so a scan that owns the whole table plans as one that has no bound
// at all.
// newLineageMark mints a lineage mark (see storage.ReadTx.SeqLineage): a
// random 64-bit value, redrawn on zero because zero reads back as "no mark".
func newLineageMark() uint64 {
	for {
		if v := rand.Uint64(); v != 0 {
			return v
		}
	}
}

func rangeWhere(col string, rng storage.Range) (string, []any) {
	where := col + " > ?"
	args := []any{rng.After}
	if rng.Before != "" {
		where += " AND " + col + " < ?"
		args = append(args, rng.Before)
	}
	return where, args
}

type sqliteTx struct {
	tx *sql.Tx
	// stmts is the cache for the pool tx runs on; see exec, query and queryRow.
	stmts *stmtCache
	// txStmts memoizes this transaction's handles on the pool's statements, by
	// SQL text; the transaction closes them when it ends.
	txStmts map[string]*sql.Stmt
	// scanning holds the SQL texts whose result set a scanRows call still has
	// open, so a nested statement does not step a machine mid-scan.
	scanning map[string]struct{}
	ws       string
	// rowKeys is the Store's cache of row ids (see ScanIndex). learnRowKeys
	// lets this transaction add to it, which only a read-only transaction
	// may do: its snapshot holds committed data only, so an id it reads
	// cannot roll back and be given to a different row key. An in-memory store
	// runs reads on the writer's one connection, which a read transaction
	// holds, so no uncommitted write is open next to it. A write transaction
	// reads ids that a later rollback, or a rollback to a savepoint, can free.
	rowKeys      *rowKeyCache
	learnRowKeys bool
	// savepoint nesting depth; names savepoints sp1, sp2, ...
	spDepth int
	// registered memoizes register's INSERT so a write burst does not repeat
	// it. Cleared on savepoint rollback, which may have undone that INSERT.
	registered bool
	// seqStart is the first sequence this transaction allocated, 0 for none,
	// and mark the lineage mark its sequences carry once it commits (see
	// ReadTx.SeqLineage). commitLineage records the pair just before COMMIT,
	// which is the point where savepoint rewinds can no longer drop the
	// allocation it names.
	seqStart uint64
	mark     uint64
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
//
// It refuses a workspace DeleteWorkspace marked with storage.ErrWorkspaceDeleted.
func (t *sqliteTx) register() error {
	if t.registered {
		return nil
	}
	res, err := t.exec(
		`INSERT INTO workspaces (workspace_id, store_epoch) VALUES (?, random()) ON CONFLICT DO NOTHING`,
		t.ws,
	)
	if err != nil {
		return err
	}
	// Only a new row can belong to a marked workspace: DeleteWorkspace removes
	// the row in the transaction that writes the mark, and nothing registers
	// it while the mark stands. So a workspace that already has its row skips
	// the lookup, which keeps it off every write but a workspace's first.
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n > 0 {
		var deleted bool
		if err := t.queryRow(
			`SELECT EXISTS (SELECT 1 FROM deleted_workspaces WHERE workspace_id = ?)`, t.ws,
		).Scan(&deleted); err != nil {
			return err
		}
		if deleted {
			return fmt.Errorf("sqlitestore: workspace %q: %w", t.ws, storage.ErrWorkspaceDeleted)
		}
	}
	t.registered = true
	return nil
}

func (t *sqliteTx) CurrentSeq() (uint64, error) {
	var seq uint64
	err := t.queryRow(
		`SELECT last_seq FROM workspaces WHERE workspace_id = ?`, t.ws,
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
	err := t.queryRow(
		`SELECT store_epoch FROM workspaces WHERE workspace_id = ?`, t.ws,
	).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return uint64(epoch), err
}

func (t *sqliteTx) SeqLineage(seq uint64) (uint64, error) {
	if seq == 0 {
		return 0, nil
	}
	current, err := t.CurrentSeq()
	if err != nil || seq > current {
		return 0, err
	}
	// This transaction's own allocations have no row until commitLineage.
	if t.seqStart != 0 && seq >= t.seqStart {
		return t.mark, nil
	}
	// Stored signed; reinterpret the bits as the unsigned mark, as StoreEpoch
	// does.
	var mark int64
	err = t.queryRow(
		`SELECT mark FROM seq_lineage WHERE workspace_id = ? AND start_seq <= ?
		 ORDER BY start_seq DESC LIMIT 1`,
		t.ws, seq,
	).Scan(&mark)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return uint64(mark), err
}

// commitLineage records the lineage mark of the sequences this transaction
// allocated. WithWriteTx calls it after the closure succeeds and before
// COMMIT; a transaction that allocated nothing records nothing.
func (t *sqliteTx) commitLineage() error {
	if t.seqStart == 0 {
		return nil
	}
	_, err := t.exec(
		`INSERT INTO seq_lineage (workspace_id, start_seq, mark, written_at) VALUES (?, ?, ?, ?)`,
		t.ws, t.seqStart, int64(t.mark), time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) PruneSeqLineage(cutoff time.Time) (int, error) {
	if cutoff.IsZero() {
		return 0, nil
	}
	// By sequence rather than by age alone: a record that survived among
	// pruned older ones would answer for their sequences too, since a lookup
	// takes the greatest start_seq at or below the sequence it asks about.
	res, err := t.exec(
		`DELETE FROM seq_lineage WHERE workspace_id = ? AND start_seq <= (
			SELECT MAX(start_seq) FROM seq_lineage WHERE workspace_id = ? AND written_at < ?
		)`,
		t.ws, t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (t *sqliteTx) TombstoneHorizon() (uint64, error) {
	var h uint64
	err := t.queryRow(
		`SELECT tombstone_horizon FROM workspaces WHERE workspace_id = ?`, t.ws,
	).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return h, err
}

func (t *sqliteTx) GetRow(entity, key string) (storage.Row, error) {
	row := storage.Row{Entity: entity, Key: key}
	var deleted int
	err := t.queryRow(
		`SELECT value, version, created_version, deleted, actor_subject, actor_system FROM rows
		 WHERE workspace_id = ? AND entity = ? AND key = ?`,
		t.ws, entity, key,
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

// getRowsChunk is how many keys one batch statement carries. A statement per
// key costs a prepare, a driver round trip and a goroutine wake-up each, which
// dominates a batch of thousands even with the database in the process; a chunk
// pays that once. The number stays far below SQLITE_MAX_VARIABLE_NUMBER (999 by
// default) so the three bound values ahead of the key list always fit.
const getRowsChunk = 256

// GetRows resolves the keys with one statement per chunk of getRowsChunk.
//
// Rows are yielded in the order of keys, once per occurrence of a repeated key;
// a key with no row is skipped, and a tombstone is yielded with Deleted set,
// exactly as GetRow returns one. A chunk's rows are read and its result set
// closed before the first callback for that chunk, so fn may read through the
// same transaction.
func (t *sqliteTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	for start := 0; start < len(keys); start += getRowsChunk {
		chunk := keys[start:min(start+getRowsChunk, len(keys))]
		byKey, err := t.readRowsByKey(entity, chunk)
		if err != nil {
			return err
		}
		for _, key := range chunk {
			row, ok := byKey[key]
			if !ok {
				continue // no row: the batch form of ErrNotFound
			}
			if err := fn(row); err != nil {
				return err
			}
		}
	}
	return nil
}

// readRowsByKey reads one chunk of keys into a map, draining and closing the
// result set before returning. It selects the same columns GetRow does, so a
// batched read round-trips the actor and creation version the same way.
func (t *sqliteTx) readRowsByKey(entity string, keys []string) (map[string]storage.Row, error) {
	var sb strings.Builder
	sb.WriteString(
		`SELECT key, value, version, created_version, deleted, actor_subject, actor_system FROM rows
		 WHERE workspace_id = ? AND entity = ? AND key IN (`)
	args := make([]any, 0, len(keys)+3)
	args = append(args, t.ws, entity)
	for i, key := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
		args = append(args, key)
	}
	sb.WriteByte(')')

	rows, err := t.query(sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byKey := make(map[string]storage.Row, len(keys))
	for rows.Next() {
		r := storage.Row{Entity: entity}
		var deleted int
		if err := rows.Scan(&r.Key, &r.Value, &r.Version, &r.CreatedVersion, &deleted,
			&r.Actor.Subject, &r.Actor.System); err != nil {
			return nil, err
		}
		r.Deleted = deleted != 0
		byKey[r.Key] = r
	}
	return byKey, rows.Err()
}

func (t *sqliteTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	return t.ScanEntityAfter(entity, "", 0, fn)
}

func (t *sqliteTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	if limit <= 0 {
		limit = -1 // SQLite: negative LIMIT means unlimited
	}
	return t.scanRows(
		`SELECT key, value, version, created_version, actor_subject, actor_system FROM rows
		 WHERE workspace_id = ? AND entity = ? AND deleted = 0 AND key > ?
		 ORDER BY key LIMIT ?`,
		[]any{t.ws, entity, afterKey, limit},
		func(rows *sql.Rows) error {
			r := storage.Row{Entity: entity}
			if err := rows.Scan(&r.Key, &r.Value, &r.Version, &r.CreatedVersion,
				&r.Actor.Subject, &r.Actor.System); err != nil {
				return err
			}
			return fn(r)
		})
}

func (t *sqliteTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	if limit <= 0 {
		limit = -1 // SQLite: negative LIMIT means unlimited
	}
	// ORDER BY matches rows_by_entity_version exactly (version DESC, key ASC,
	// partial on deleted = 0), so this reads the index prefix and stops at
	// LIMIT instead of sorting the entity.
	return t.scanRows(
		`SELECT key, value, version, created_version, actor_subject, actor_system FROM rows
		 WHERE workspace_id = ? AND entity = ? AND deleted = 0
		 ORDER BY version DESC, key LIMIT ?`,
		[]any{t.ws, entity, limit},
		func(rows *sql.Rows) error {
			r := storage.Row{Entity: entity}
			if err := rows.Scan(&r.Key, &r.Value, &r.Version, &r.CreatedVersion,
				&r.Actor.Subject, &r.Actor.System); err != nil {
				return err
			}
			return fn(r)
		})
}

func (t *sqliteTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	if limit <= 0 {
		limit = -1
	}
	// Order by the full (version, entity, key) tuple so the first page of a
	// same-version group and every resume page agree on row order; the cursor
	// records the last (entity, key) within a split version to resume past it.
	var (
		text string
		args []any
	)
	if afterEntity == "" {
		// Clean version boundary: everything strictly past afterSeq.
		text = `SELECT entity, key, value, version, created_version, deleted,
			        actor_subject, actor_system FROM rows
			 WHERE workspace_id = ? AND version > ?
			 ORDER BY version, entity, key LIMIT ?`
		args = []any{t.ws, afterSeq, limit}
	} else {
		// Resume inside a same-version group: past afterSeq, plus the rest of
		// the split group whose (entity, key) sorts after (afterEntity, afterKey).
		text = `SELECT entity, key, value, version, created_version, deleted,
			        actor_subject, actor_system FROM rows
			 WHERE workspace_id = ? AND (
			   version > ?
			   OR (version = ? AND (entity > ? OR (entity = ? AND key > ?)))
			 )
			 ORDER BY version, entity, key LIMIT ?`
		args = []any{t.ws, afterSeq, afterSeq, afterEntity, afterEntity, afterKey, limit}
	}
	return t.scanRows(text, args, func(rows *sql.Rows) error {
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
		return fn(r)
	})
}

func (t *sqliteTx) ClientLastMutationID(clientID string) (uint64, error) {
	var id uint64
	err := t.queryRow(
		`SELECT last_mutation_id FROM clients WHERE workspace_id = ? AND client_id = ?`,
		t.ws, clientID,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

func (t *sqliteTx) ClientRejections(clientID string) ([]storage.ClientRejection, error) {
	var blob []byte
	err := t.queryRow(
		`SELECT rejections FROM clients WHERE workspace_id = ? AND client_id = ?`,
		t.ws, clientID,
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
	err := t.queryRow(
		`SELECT seq, result FROM idempotency WHERE workspace_id = ? AND key = ?`,
		t.ws, key,
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
	_, err := t.exec(
		`INSERT INTO idempotency (workspace_id, key, seq, result, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, key)
		 DO UPDATE SET seq = excluded.seq, result = excluded.result, updated_at = excluded.updated_at`,
		t.ws, key, rec.Seq, result, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) CollectIdempotencyKeys(cutoff time.Time) (int, error) {
	res, err := t.exec(
		`DELETE FROM idempotency WHERE workspace_id = ? AND updated_at < ?`,
		t.ws, cutoff.UnixMilli(),
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
	err := t.queryRow(
		`SELECT seq, entity, key, updated_at FROM consumer_positions
		 WHERE workspace_id = ? AND consumer = ?`,
		t.ws, consumer,
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
	rows, err := t.query(
		`SELECT consumer, seq, entity, key, updated_at FROM consumer_positions
		 WHERE workspace_id = ? ORDER BY consumer`,
		t.ws,
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
	_, err := t.exec(
		`INSERT INTO consumer_positions (workspace_id, consumer, seq, entity, key, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, consumer)
		 DO UPDATE SET seq = excluded.seq, entity = excluded.entity,
		               key = excluded.key, updated_at = excluded.updated_at`,
		t.ws, consumer, p.Seq, p.Entity, p.Key, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	var snapshot []byte
	err := t.queryRow(
		`SELECT snapshot FROM consumer_shadows
		 WHERE workspace_id = ? AND consumer = ? AND entity = ? AND key = ?`,
		t.ws, consumer, entity, key,
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
	_, err := t.exec(
		`INSERT INTO consumer_shadows (workspace_id, consumer, entity, key, snapshot)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, consumer, entity, key)
		 DO UPDATE SET snapshot = excluded.snapshot`,
		t.ws, consumer, entity, key, snapshot,
	)
	return err
}

func (t *sqliteTx) DeleteConsumerShadow(consumer, entity, key string) error {
	_, err := t.exec(
		`DELETE FROM consumer_shadows
		 WHERE workspace_id = ? AND consumer = ? AND entity = ? AND key = ?`,
		t.ws, consumer, entity, key,
	)
	return err
}

func (t *sqliteTx) DeleteConsumer(consumer string) error {
	if _, err := t.exec(
		`DELETE FROM consumer_shadows WHERE workspace_id = ? AND consumer = ?`,
		t.ws, consumer,
	); err != nil {
		return err
	}
	_, err := t.exec(
		`DELETE FROM consumer_positions WHERE workspace_id = ? AND consumer = ?`,
		t.ws, consumer,
	)
	return err
}

func (t *sqliteTx) CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (int, error) {
	// Shadows first, while the positions that name their consumers still exist:
	// the reverse order would leave every collected consumer's shadows behind
	// with nothing left to identify them by.
	if _, err := t.exec(
		`DELETE FROM consumer_shadows
		 WHERE workspace_id = ? AND consumer IN (
		     SELECT consumer FROM consumer_positions
		     WHERE workspace_id = ? AND updated_at < ? AND seq < ?
		 )`,
		t.ws, t.ws, cutoff.UnixMilli(), belowSeq,
	); err != nil {
		return 0, err
	}
	res, err := t.exec(
		`DELETE FROM consumer_positions
		 WHERE workspace_id = ? AND updated_at < ? AND seq < ?`,
		t.ws, cutoff.UnixMilli(), belowSeq,
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (t *sqliteTx) FieldDeltaHorizon() (uint64, error) {
	var horizon uint64
	err := t.queryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE workspace_id = ?`, t.ws,
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
	rows, err := t.query(
		`SELECT frag FROM field_deltas
		 WHERE workspace_id = ? AND entity = ? AND key = ? AND field = ? AND seq > ?
		 ORDER BY seq`,
		t.ws, entity, key, field, afterSeq,
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
	err := t.queryRow(
		`SELECT vv FROM text_floors
		 WHERE workspace_id = ? AND entity = ? AND key = ? AND field = ?`,
		t.ws, entity, key, field,
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
		_, err := t.exec(
			`DELETE FROM text_floors
			 WHERE workspace_id = ? AND entity = ? AND key = ? AND field = ?`,
			t.ws, entity, key, field,
		)
		return err
	}
	_, err := t.exec(
		`INSERT INTO text_floors (workspace_id, entity, key, field, vv)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, entity, key, field)
		 DO UPDATE SET vv = excluded.vv`,
		t.ws, entity, key, field, vv,
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
	args := []any{t.ws, entity, index}
	if lo != nil {
		args = append(args, lo)
	}
	if hi != nil {
		args = append(args, hi)
	}
	args = append(args, limit)
	return t.scanRows(
		scanIndexSQL(lo != nil, hi != nil, order),
		args,
		func(rows *sql.Rows) error {
			var (
				entry  []byte
				stored int64
			)
			if err := rows.Scan(&entry, &stored); err != nil {
				return err
			}
			rowKey, err := t.entryRowKey(entry, stored)
			if err != nil {
				return fmt.Errorf("sqlitestore: index entry of %s/%s: %w", entity, index, err)
			}
			return fn(entry, rowKey)
		})
}

// scanIndexSQL is the statement ScanIndex runs. Its arguments are the
// workspace id, the entity and the index name, then lo when hasLo, hi when
// hasHi, and the limit.
func scanIndexSQL(hasLo, hasHi bool, order storage.ScanOrder) string {
	// The bounds are optional on both ends, and SQLite has no way to express
	// "compare only if bound" in the WHERE clause without defeating the index,
	// so the predicate is built to match what was given. k is a BLOB, which
	// SQLite compares with memcmp — the bytewise order the entry encoding is
	// built for, and the reason this must never become a TEXT column.
	// The scope id is a scalar subquery, which SQLite evaluates once, so the
	// scan is still a range of the primary key. An index nothing has written
	// has no scope, the subquery is NULL and no entry matches it.
	where := `scope = (SELECT id FROM index_scopes WHERE workspace_id = ? AND entity = ? AND idx = ?)`
	if hasLo {
		where += ` AND k >= ?`
	}
	if hasHi {
		where += ` AND k < ?`
	}
	// The key column is part of the sort so that two entries sharing a key come
	// out in a fixed order; reversing the walk has to reverse it too, or the
	// descending scan would not be the exact mirror of the ascending one. It
	// holds an integer in every form (see entryKeyValue), so the order is
	// total. Entries of several rows that share a key come out by id, not by
	// row key, which the contract permits.
	orderBy := `k, key`
	if order == storage.ScanDesc {
		orderBy = `k DESC, key DESC`
	}
	// The statement does not join index_row_keys: entryRowKey resolves an id
	// from the Store's cache, which costs less than the join's lookup per
	// entry, and reads the table only on a miss.
	return `SELECT k, key FROM index_entries WHERE ` + where + ` ORDER BY ` + orderBy + ` LIMIT ?`
}

// ScanIndexRanges runs one ScanIndex per range. A statement costs no round
// trip in process, and each one probes the primary key for its own range,
// which a single statement over all of them would also have to do.
func (t *sqliteTx) ScanIndexRanges(
	entity, index string,
	ranges []storage.IndexRange,
	fn func(i int, entry []byte, rowKey string) error,
) error {
	for i, r := range ranges {
		if err := t.ScanIndex(entity, index, r.Lo, r.Hi, 0, storage.ScanAsc, func(entry []byte, rowKey string) error {
			return fn(i, entry, rowKey)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) IndexState(entity, index string) (string, bool, error) {
	var (
		specHash string
		ready    int
	)
	err := t.queryRow(
		`SELECT spec_hash, ready FROM index_state
		 WHERE workspace_id = ? AND entity = ? AND idx = ?`,
		t.ws, entity, index,
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
	_, err := t.exec(
		`INSERT INTO index_state (workspace_id, entity, idx, spec_hash, ready)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, entity, idx)
		 DO UPDATE SET spec_hash = excluded.spec_hash, ready = excluded.ready`,
		t.ws, entity, index, specHash, boolToInt(ready),
	)
	return err
}

// SetIndexBuildMark updates the workspace row and never calls register: the
// mark says "this workspace's indexes are built", which is vacuously true of a
// workspace that holds nothing, and registering one here would let a
// BuildIndexes call on an arbitrary id mint a workspace every later pass
// iterates.
func (t *sqliteTx) SetIndexBuildMark(mark string) error {
	_, err := t.exec(
		`UPDATE workspaces SET index_build_mark = ? WHERE workspace_id = ?`,
		mark, t.ws,
	)
	return err
}

// SetTextCompactMark updates the workspace row and never calls register, for
// SetIndexBuildMark's reason: a workspace holding no rows has no text to
// compact, and a sweep must not mint one.
func (t *sqliteTx) SetTextCompactMark(mark storage.TextCompactMark) error {
	_, err := t.exec(
		`UPDATE workspaces
		    SET text_compact_seq = ?, text_compact_entity = ?, text_compact_key = ?,
		        text_compact_min_purged = ?
		  WHERE workspace_id = ?`,
		int64(mark.Seq), mark.Entity, mark.Key, mark.MinPurged, t.ws,
	)
	return err
}

func (t *sqliteTx) TextCompactMark() (storage.TextCompactMark, error) {
	var m storage.TextCompactMark
	var seq int64
	err := t.queryRow(
		`SELECT text_compact_seq, text_compact_entity, text_compact_key, text_compact_min_purged
		   FROM workspaces WHERE workspace_id = ?`,
		t.ws,
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
	// entries held. Reading the current set first is one point read of the
	// row's id and entry list and turns the common case into no writes at all.
	listID, have, err := t.rowIndexEntries(entity, key)
	if err != nil {
		return err
	}
	scopes, err := t.indexScopes(entity)
	if err != nil {
		return err
	}
	want := make(map[rowIndexEntry]struct{}, len(entries))
	for _, e := range entries {
		id, ok := scopes[e.Index]
		if !ok {
			if id, err = t.createIndexScope(entity, e.Index); err != nil {
				return err
			}
			scopes[e.Index] = id
		}
		// A set, so a caller that passes the same entry twice stores one copy
		// instead of failing the whole write on a primary-key conflict: two
		// components of one index can only collide for one row that way, and a
		// harmless caller bug must not stick the write path.
		want[rowIndexEntry{scope: id, k: string(e.Key)}] = struct{}{}
	}
	changed := len(want) != len(have)
	if !changed {
		for e := range want {
			if _, unchanged := have[e]; !unchanged {
				changed = true
				break
			}
		}
	}
	if !changed {
		return nil
	}
	// The id goes first: an entry that does not end with the row key stores
	// it, and a row new to the index has none yet.
	if listID == 0 {
		res, err := t.exec(`INSERT INTO index_row_keys (key) VALUES (?)`, key)
		if err != nil {
			return err
		}
		if listID, err = res.LastInsertId(); err != nil {
			return err
		}
		if _, err := t.exec(
			`INSERT INTO index_row_ids (workspace_id, entity, key, id) VALUES (?, ?, ?, ?)`,
			t.ws, entity, key, listID,
		); err != nil {
			return err
		}
		if _, err := t.exec(
			`INSERT INTO index_row_entries (id, entries) VALUES (?, ?)`,
			listID, encodeRowEntries(key, want),
		); err != nil {
			return err
		}
	} else if _, err := t.exec(
		`UPDATE index_row_entries SET entries = ? WHERE id = ?`,
		encodeRowEntries(key, want), listID,
	); err != nil {
		return err
	}
	for e := range have {
		if _, keep := want[e]; keep {
			continue
		}
		if _, err := t.exec(
			`DELETE FROM index_entries WHERE scope = ? AND k = ? AND key = ?`,
			e.scope, []byte(e.k), entryKeyValue(e.k, key, listID),
		); err != nil {
			return err
		}
	}
	for e := range want {
		if _, unchanged := have[e]; unchanged {
			continue
		}
		if _, err := t.exec(
			`INSERT INTO index_entries (scope, k, key) VALUES (?, ?, ?)`,
			e.scope, []byte(e.k), entryKeyValue(e.k, key, listID),
		); err != nil {
			return err
		}
	}
	return nil
}

// rowIndexEntry is one stored entry of one row, as a map key: the scope id of
// the index it sits in and its sort key held as a string so the pair compares
// by value.
type rowIndexEntry struct {
	scope int64
	k     string
}

// indexScopes reads the scope id of every index of entity this workspace has
// stored entries for, keyed by index name.
func (t *sqliteTx) indexScopes(entity string) (map[string]int64, error) {
	rows, err := t.query(
		`SELECT idx, id FROM index_scopes WHERE workspace_id = ? AND entity = ?`,
		t.ws, entity,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var (
			idx string
			id  int64
		)
		if err := rows.Scan(&idx, &id); err != nil {
			return nil, err
		}
		out[idx] = id
	}
	return out, rows.Err()
}

// createIndexScope allocates the scope id of one index. The caller has read
// the scopes in the same write transaction and found none, so the insert
// cannot conflict.
func (t *sqliteTx) createIndexScope(entity, index string) (int64, error) {
	res, err := t.exec(
		`INSERT INTO index_scopes (workspace_id, entity, idx) VALUES (?, ?, ?)`,
		t.ws, entity, index,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// rowIndexEntries reads the id of one row and every index entry the row
// currently occupies, as the set PutIndexEntries diffs against. A row with no
// stored list has id 0 and no entries; a stored id is never 0.
func (t *sqliteTx) rowIndexEntries(entity, key string) (int64, map[rowIndexEntry]struct{}, error) {
	var (
		id   int64
		blob []byte
	)
	err := t.queryRow(
		`SELECT i.id, e.entries FROM index_row_ids i JOIN index_row_entries e ON e.id = i.id
		 WHERE i.workspace_id = ? AND i.entity = ? AND i.key = ?`,
		t.ws, entity, key,
	).Scan(&id, &blob)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, map[rowIndexEntry]struct{}{}, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if id <= 0 {
		// An id below 1 cannot be negated into the id form of the key column.
		// SQLite gives out ids from 1 up, so only a hand-edited file holds one.
		return 0, nil, fmt.Errorf("sqlitestore: index entry list of %s/%s has id %d", entity, key, id)
	}
	have, err := decodeRowEntries(key, blob)
	if err != nil {
		return 0, nil, fmt.Errorf("sqlitestore: index entry list of %s/%s: %w", entity, key, err)
	}
	return id, have, nil
}

// encodeRowEntries packs the entry set of the row with key rowKey, sorted so
// that the same set always packs to the same bytes. The entries of one scope
// make one group, and the groups are in ascending order of scope:
//
//	list  = group+
//	group = uvarint scope, uvarint head, part, entry*
//	entry = uvarint shared, uvarint head, part
//
// The first entry of a group shares no bytes with an entry before it, so it
// has no shared field. A head holds three values:
//
//   - head >> 3 is the length of part.
//   - Bits 1 and 2 name a tail that the list leaves out and decodeRowEntries
//     adds back. 0 is no tail. 1 is rowKeySuffix(rowKey): the engine ends most
//     entries with the row's key as an index string, which for a key with no
//     U+0000 and no astral character is its UTF-8 bytes and the terminator
//     "\x00\x00". 2 is "\x00\x00" alone, the end of an index string that is
//     not the row key, such as a search term.
//   - Bit 0 is set when another entry of the same group follows.
//
// An entry without its tail is the first shared bytes of the previous entry
// of the group without its tail, then part. The entries of a group are
// sorted, so neighbours often start with the same bytes. The checks are on
// the bytes alone, so the list round-trips whatever the entries hold; an entry
// the guesses do not fit only costs the bytes they would have saved.
func encodeRowEntries(rowKey string, set map[rowIndexEntry]struct{}) []byte {
	suffix := rowKeySuffix(rowKey)
	list := make([]rowIndexEntry, 0, len(set))
	size := 0
	for e := range set {
		list = append(list, e)
		size += 3*binary.MaxVarintLen64 + len(e.k)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].scope != list[j].scope {
			return list[i].scope < list[j].scope
		}
		return list[i].k < list[j].k
	})
	out := make([]byte, 0, size)
	prev := ""
	for i, e := range list {
		first := i == 0 || list[i-1].scope != e.scope
		k, tail := e.k, uint64(0)
		if strings.HasSuffix(k, suffix) {
			k, tail = k[:len(k)-len(suffix)], 1
		} else if strings.HasSuffix(k, indexStringEnd) {
			k, tail = k[:len(k)-len(indexStringEnd)], 2
		}
		shared := 0
		if first {
			out = binary.AppendUvarint(out, uint64(e.scope))
		} else {
			for shared < len(k) && shared < len(prev) && k[shared] == prev[shared] {
				shared++
			}
			out = binary.AppendUvarint(out, uint64(shared))
		}
		head := uint64(len(k)-shared)<<3 | tail<<1
		if i+1 < len(list) && list[i+1].scope == e.scope {
			head |= 1
		}
		out = binary.AppendUvarint(out, head)
		out = append(out, k[shared:]...)
		prev = k
	}
	return out
}

// indexStringEnd terminates an index string.
const indexStringEnd = "\x00\x00"

// rowKeySuffix is the tail encodeRowEntries leaves out of an entry of the row
// with key rowKey.
func rowKeySuffix(rowKey string) string { return rowKey + indexStringEnd }

// endsWithRowKey reports whether the entry k ends with rowKeySuffix(rowKey).
func endsWithRowKey(k, rowKey string) bool {
	n := len(k) - 2
	return n >= len(rowKey) && k[n:] == "\x00\x00" && k[n-len(rowKey):n] == rowKey
}

// entryKeyValue is the value the key column of index_entries holds for the
// entry k of the row with key rowKey and the given id: the byte length of
// rowKey negated when k ends with rowKeySuffix(rowKey), and id for any other
// entry. entryRowKey reads the row key back from either.
func entryKeyValue(k, rowKey string, id int64) int64 {
	if endsWithRowKey(k, rowKey) {
		return -int64(len(rowKey))
	}
	return id
}

// entryRowKey reverses entryKeyValue for the entry k, given the value the key
// column holds for it.
func (t *sqliteTx) entryRowKey(k []byte, stored int64) (string, error) {
	if stored > 0 {
		return t.rowKeyByID(stored)
	}
	end := len(k) - 2
	if int64(end) < -stored {
		return "", fmt.Errorf("row key length %d does not fit an entry of %d bytes", -stored, len(k))
	}
	return string(k[end+int(stored) : end]), nil
}

// rowKeyByID returns the row key with the given id, from the Store's cache
// when it holds the id. A hit is correct in any transaction: the cache holds
// committed ids only, and this transaction cannot give a committed id to a
// different row key (see rowKeyCache).
func (t *sqliteTx) rowKeyByID(id int64) (string, error) {
	if key, ok := t.rowKeys.get(id); ok {
		return key, nil
	}
	var key string
	err := t.queryRow(`SELECT key FROM index_row_keys WHERE id = ?`, id).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("entry names row id %d, which no row holds", id)
	}
	if err != nil {
		return "", err
	}
	if t.learnRowKeys {
		t.rowKeys.add(id, key)
	}
	return key, nil
}

// decodeRowEntries reverses encodeRowEntries for the row with key rowKey. It
// returns an error for a list that encodeRowEntries cannot have made, and for
// a list that ends inside a group.
func decodeRowEntries(rowKey string, b []byte) (map[rowIndexEntry]struct{}, error) {
	suffix := rowKeySuffix(rowKey)
	out := make(map[rowIndexEntry]struct{})
	// prev holds the previous entry of the group without its tail, and full
	// the entry being built, so that each entry costs one allocation: the
	// string it is stored as.
	var prev, full []byte
	var scope uint64
	more := false
	for len(b) > 0 {
		var shared uint64
		if more {
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, errors.New("truncated shared prefix")
			}
			if v > uint64(len(prev)) {
				return nil, fmt.Errorf("entry shares %d bytes with an entry of %d", v, len(prev))
			}
			shared, b = v, b[n:]
		} else {
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, errors.New("truncated scope id")
			}
			if v > math.MaxInt64 {
				return nil, fmt.Errorf("scope id %d is out of range", v)
			}
			if len(out) > 0 && v <= scope {
				return nil, fmt.Errorf("scope %d does not follow scope %d", v, scope)
			}
			scope, b = v, b[n:]
		}
		head, n := binary.Uvarint(b)
		size := head >> 3
		if n <= 0 || size > uint64(len(b)-n) {
			return nil, errors.New("truncated entry")
		}
		b = b[n:]
		prev = append(prev[:shared], b[:size]...)
		b = b[size:]
		full = append(full[:0], prev...)
		switch head >> 1 & 3 {
		case 0:
		case 1:
			full = append(full, suffix...)
		case 2:
			full = append(full, indexStringEnd...)
		default:
			return nil, errors.New("unknown entry tail")
		}
		out[rowIndexEntry{scope: int64(scope), k: string(full)}] = struct{}{}
		more = head&1 == 1
	}
	if more {
		return nil, errors.New("truncated group")
	}
	return out, nil
}

func (t *sqliteTx) DeleteIndexEntries(entity, key string) error {
	// No register(): removing entries a row never had must leave no trace, the
	// same reason a write closure that writes nothing does not create a
	// workspace.
	listID, have, err := t.rowIndexEntries(entity, key)
	if err != nil {
		return err
	}
	if listID == 0 {
		return nil
	}
	// Every entry goes with the id: an entry left behind would hold an id that
	// SQLite may give to the next row that gains entries.
	for e := range have {
		if _, err := t.exec(
			`DELETE FROM index_entries WHERE scope = ? AND k = ? AND key = ?`,
			e.scope, []byte(e.k), entryKeyValue(e.k, key, listID),
		); err != nil {
			return err
		}
	}
	if _, err := t.exec(
		`DELETE FROM index_row_ids
		 WHERE workspace_id = ? AND entity = ? AND key = ?`,
		t.ws, entity, key,
	); err != nil {
		return err
	}
	if _, err := t.exec(`DELETE FROM index_row_entries WHERE id = ?`, listID); err != nil {
		return err
	}
	_, err = t.exec(`DELETE FROM index_row_keys WHERE id = ?`, listID)
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
	_, err := t.exec(
		`INSERT INTO field_deltas (workspace_id, entity, key, field, seq, frag, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (workspace_id, entity, key, field, seq)
		 DO UPDATE SET frag = excluded.frag, updated_at = excluded.updated_at`,
		t.ws, entity, key, field, seq, frag, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) PruneFieldDeltas(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxPruned uint64
	err := t.queryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM field_deltas
		 WHERE workspace_id = ? AND updated_at < ?`,
		t.ws, cutoffMillis,
	).Scan(&maxPruned)
	if err != nil {
		return 0, err
	}
	if maxPruned > 0 {
		if _, err := t.exec(
			`DELETE FROM field_deltas WHERE workspace_id = ? AND updated_at < ?`,
			t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// MAX() keeps the horizon monotonic even if an older cutoff runs after a
		// newer one, exactly as CollectTombstones does.
		if _, err := t.exec(
			`UPDATE workspaces SET field_delta_horizon = MAX(field_delta_horizon, ?)
			 WHERE workspace_id = ?`,
			maxPruned, t.ws,
		); err != nil {
			return 0, err
		}
	}
	var horizon uint64
	err = t.queryRow(
		`SELECT field_delta_horizon FROM workspaces WHERE workspace_id = ?`, t.ws,
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
	if err := t.queryRow(
		`UPDATE workspaces SET last_seq = last_seq + 1
		 WHERE workspace_id = ? RETURNING last_seq`,
		t.ws,
	).Scan(&seq); err != nil {
		return 0, err
	}
	if t.seqStart == 0 {
		t.seqStart = seq
		if t.mark == 0 {
			t.mark = newLineageMark()
		}
	}
	return seq, nil
}

func (t *sqliteTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	// created_version stays out of the conflict SET: the stored value is the
	// key's original creation, which every overwrite keeps (see storage.Row).
	_, err := t.exec(
		`INSERT INTO rows (workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
		 ON CONFLICT (workspace_id, entity, key)
		 DO UPDATE SET value = excluded.value, version = excluded.version, deleted = 0,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ws, entity, key, value, version, version, time.Now().UnixMilli(),
		actor.Subject, actor.System,
	)
	return err
}

func (t *sqliteTx) DeleteRow(entity, key string, version uint64, actor storage.Actor) error {
	if err := t.register(); err != nil {
		return err
	}
	_, err := t.exec(
		`INSERT INTO rows (workspace_id, entity, key, value, version, created_version, deleted, updated_at,
		                   actor_subject, actor_system)
		 VALUES (?, ?, ?, NULL, ?, ?, 1, ?, ?, ?)
		 ON CONFLICT (workspace_id, entity, key)
		 DO UPDATE SET value = NULL, version = excluded.version, deleted = 1,
		               updated_at = excluded.updated_at,
		               actor_subject = excluded.actor_subject,
		               actor_system = excluded.actor_system`,
		t.ws, entity, key, version, version, time.Now().UnixMilli(),
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
	_, err := t.exec(
		`INSERT INTO clients (workspace_id, client_id, last_mutation_id, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (workspace_id, client_id)
		 DO UPDATE SET last_mutation_id = excluded.last_mutation_id, updated_at = excluded.updated_at`,
		t.ws, clientID, mutationID, time.Now().UnixMilli(),
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
	_, err := t.exec(
		`INSERT INTO clients (workspace_id, client_id, rejections, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (workspace_id, client_id)
		 DO UPDATE SET rejections = excluded.rejections, updated_at = excluded.updated_at`,
		t.ws, clientID, blob, time.Now().UnixMilli(),
	)
	return err
}

func (t *sqliteTx) InSavepoint(fn func() error) error {
	t.spDepth++
	name := fmt.Sprintf("sp%d", t.spDepth)
	defer func() { t.spDepth-- }()
	if _, err := t.exec(`SAVEPOINT ` + name); err != nil {
		return err
	}
	// The rewind undoes an allocation fn made, so it undoes fn's claim to be
	// the transaction's first one too.
	seqStart := t.seqStart
	if err := fn(); err != nil {
		t.seqStart = seqStart
		// The rewind may undo register's INSERT (a rejected mutation that was
		// this transaction's only write), so re-run it on the next write. When
		// the INSERT predates the savepoint the extra statement is a harmless
		// ON CONFLICT DO NOTHING.
		t.registered = false
		// ROLLBACK TO rewinds the writes but keeps the savepoint open;
		// RELEASE discards it.
		if _, rbErr := t.exec(`ROLLBACK TO ` + name); rbErr != nil {
			return errors.Join(err, fmt.Errorf("sqlitestore: rollback savepoint: %w", rbErr))
		}
		if _, rbErr := t.exec(`RELEASE ` + name); rbErr != nil {
			return errors.Join(err, fmt.Errorf("sqlitestore: release savepoint: %w", rbErr))
		}
		return err
	}
	if _, err := t.exec(`RELEASE ` + name); err != nil {
		return fmt.Errorf("sqlitestore: release savepoint: %w", err)
	}
	return nil
}

func (t *sqliteTx) CollectTombstones(cutoff time.Time) (uint64, error) {
	cutoffMillis := cutoff.UnixMilli()
	var maxDeleted uint64
	err := t.queryRow(
		`SELECT COALESCE(MAX(version), 0) FROM rows
		 WHERE workspace_id = ? AND deleted = 1 AND updated_at < ?`,
		t.ws, cutoffMillis,
	).Scan(&maxDeleted)
	if err != nil {
		return 0, err
	}
	if maxDeleted > 0 {
		if _, err := t.exec(
			`DELETE FROM rows WHERE workspace_id = ? AND deleted = 1 AND updated_at < ?`,
			t.ws, cutoffMillis,
		); err != nil {
			return 0, err
		}
		// MAX() keeps the horizon monotonic even if an older cutoff runs
		// after a newer one.
		if _, err := t.exec(
			`UPDATE workspaces SET tombstone_horizon = MAX(tombstone_horizon, ?)
			 WHERE workspace_id = ?`,
			maxDeleted, t.ws,
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
// A limit turns both the key selection and the row delete into the same
// `… ORDER BY key LIMIT n` selection. The ORDER BY is load-bearing rather than
// cosmetic: the two run against the same rows (the entries delete does not
// touch them) and must select the SAME set, or entries outlive their row or
// die before it. A total order over a unique key is what makes them agree.
//
// more is "this call hit its limit", which the interface allows: the drain that
// exactly emptied the selection costs one more no-op call, which is cheaper than
// probing for a next row on every chunk.
func (t *sqliteTx) PurgeEntityRows(entity, keyPrefix string, before time.Time, limit int) (int, bool, error) {
	where, args := purgeWhere(t.ws, entity, keyPrefix, before)
	selected, rowsWhere := where, where
	rowsArgs := args
	if limit > 0 {
		selected = where + " ORDER BY key LIMIT ?"
		args = append(args, limit)
		// The outer predicate repeats the (workspace, entity) keys the
		// subquery already applies; the subquery is what narrows the delete.
		rowsWhere = `workspace_id = ? AND entity = ? AND key IN (
		   SELECT key FROM rows WHERE ` + selected + `)`
		rowsArgs = append([]any{t.ws, entity}, args...)
	}
	// Entries are found through each row's entry list, which no statement over
	// index_entries alone can read, so the selected keys come out first. The
	// selection is bounded by the limit when there is one.
	var keys []string
	if err := t.scanRows(`SELECT key FROM rows WHERE `+selected, args, func(rows *sql.Rows) error {
		var k string
		if err := rows.Scan(&k); err != nil {
			return err
		}
		keys = append(keys, k)
		return nil
	}); err != nil {
		return 0, false, err
	}
	for _, k := range keys {
		if err := t.DeleteIndexEntries(entity, k); err != nil {
			return 0, false, err
		}
	}
	res, err := t.exec(`DELETE FROM rows WHERE `+rowsWhere, rowsArgs...)
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
func purgeWhere(ws, entity, keyPrefix string, before time.Time) (string, []any) {
	where := "workspace_id = ? AND entity = ?"
	args := []any{ws, entity}
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

func (t *sqliteTx) ExpireClientRejections(cutoff time.Time) (int, error) {
	// updated_at is deliberately left alone: it is what CollectIdleClients
	// measures idleness by, and expiring verdicts is not client activity.
	res, err := t.exec(
		`UPDATE clients SET rejections = NULL
		 WHERE workspace_id = ? AND updated_at < ? AND rejections IS NOT NULL`,
		t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (t *sqliteTx) CollectIdleClients(cutoff time.Time) (int, error) {
	res, err := t.exec(
		`DELETE FROM clients WHERE workspace_id = ? AND updated_at < ?`,
		t.ws, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}
