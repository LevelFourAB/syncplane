package pgstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrations[i] takes a database at ledger version i to i+1. Index 0 is the
// baseline — the current shape stated once, as sqlitestore's schema is. Later
// structural changes append here, and the ledger is what makes them
// replayable.
//
// The baseline states a lineage that keys every table by the workspace id
// alone. The lineage before it keyed them by (namespace, workspace_id), which
// no step can rewrite in place — a hash-partitioned table cannot change its
// partition key — so such a database is refused at Open instead (see
// checkLineage).
//
// Every step takes the layout, because a partitioned database and an
// unpartitioned one are not the same schema. Only the baseline branches on it
// so far; a step that adds an index will have to, since CREATE INDEX on a
// partitioned parent cannot be CONCURRENTLY and a large deployment has to
// build it per partition instead.
//
// Every text column is COLLATE "C" (see the package comment). It is applied
// even to columns nothing orders by — the actor's subject and system label,
// consumer names — so the rule is "all of them" rather than a judgement call
// per column that a later index turns into a bug.
var migrations = []migration{
	{additive: true, sql: baselineSchema},
	{additive: true, sql: fixed(seqLineageDDL)},
	{additive: true, sql: fixed(deletedWorkspacesDDL)},
}

// deletedWorkspacesDDL is migration 3: one row per workspace DeleteWorkspace
// removed and RestoreWorkspace has not lifted since (see storage.Adapter).
// Read by WorkspaceDeleted, and by the guard every registering write
// transaction sends with its COMMIT (see pgTx.flush).
//
// Unpartitioned whatever the layout, for the reason seq_lineage is, and
// because it holds one small row per deleted workspace.
const deletedWorkspacesDDL = `
CREATE TABLE deleted_workspaces (
	workspace_id TEXT COLLATE "C" NOT NULL,
	deleted_at   BIGINT NOT NULL,
	CONSTRAINT deleted_workspaces_pkey PRIMARY KEY (workspace_id)
);
`

// workspaceTables names every table that holds state of one workspace, keyed
// by its workspace_id column. DeleteWorkspace empties each of them for the
// workspace it deletes, so a table added by a migration must be added here
// too; deleted_workspaces is the one table with that column left out, because
// it holds the mark DeleteWorkspace leaves.
var workspaceTables = []string{
	"workspaces",
	"rows",
	"clients",
	"field_deltas",
	"text_floors",
	"idempotency",
	"index_entries",
	"index_state",
	"consumer_positions",
	"consumer_shadows",
	"cursor_state",
	"seq_lineage",
}

// seqLineageDDL is migration 2: one row per committed write transaction that
// allocated sequences, naming the first of them and the mark every one of them
// carries (see storage.ReadTx.SeqLineage). Lookups take the greatest start_seq
// at or below a sequence, a descent of the primary key; the _gc index serves
// PruneSeqLineage and the collectable probe.
//
// Unpartitioned whatever the layout: GC keeps it to the write transactions of
// one tombstone retention, and a step can only branch on whether the layout is
// partitioned, not on the modulus a new partitioned table would need.
const seqLineageDDL = `
CREATE TABLE seq_lineage (
	workspace_id TEXT COLLATE "C" NOT NULL,
	start_seq    BIGINT NOT NULL,
	mark         BIGINT NOT NULL,
	written_at   BIGINT NOT NULL,
	PRIMARY KEY (workspace_id, start_seq)
);
CREATE INDEX seq_lineage_gc ON seq_lineage (workspace_id, written_at);
`

// migration is one step of the ledger.
type migration struct {
	// sql is the step under a given layout.
	sql func(layout) string
	// additive reports that a binary which does not know this step can still
	// serve the schema it leaves behind — true of anything that only adds
	// tables, columns or indexes, since an older binary serves those by never
	// naming them.
	//
	// It is stated rather than defaulted because the zero value has to be the
	// safe one: a step that drops or narrows something and forgets to say so
	// would otherwise let an older binary boot and fail later, deep in a read,
	// which is the failure this whole mechanism exists to convert into a
	// refusal at Open.
	additive bool
}

// fixed adapts a step whose SQL does not depend on the layout.
func fixed(sql string) func(layout) string {
	return func(layout) string { return sql }
}

// ledgerDDL creates the migration ledger. IF NOT EXISTS because two paths run
// it: migrate, before it can read a version at all, and the first script
// Statements returns — which is what spares an externally applied schema a
// separate bootstrap.
//
// additive is here as well as in migration 4 because both orders have to work:
// a database created by this DDL needs the column before step 1 records
// itself, and one created before the column existed gets it from the step.
const ledgerDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version    INTEGER PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	additive   BOOLEAN NOT NULL DEFAULT TRUE
);
`

// baselineDDL is migration 1. The %[1]s after five of the tables is where the
// partitioning clause lands; see partitionedTables for which five and why.
const baselineDDL = `
CREATE TABLE workspaces (
	workspace_id        TEXT COLLATE "C" NOT NULL,
	last_seq            BIGINT NOT NULL DEFAULT 0,
	tombstone_horizon   BIGINT NOT NULL DEFAULT 0,
	store_epoch         BIGINT NOT NULL DEFAULT 0,
	field_delta_horizon BIGINT NOT NULL DEFAULT 0,
	-- The opaque fingerprint of the index declarations this workspace's
	-- entries are fully built under (see ListStaleIndexWorkspaceIDs). Empty is
	-- "never built", which makes a fresh workspace stale to the first pass.
	index_build_mark    TEXT COLLATE "C" NOT NULL DEFAULT '',
	-- How far the text-compaction sweep has considered rows in ScanChanges
	-- order, and the minPurged threshold it did so under (see
	-- ListCompactableWorkspaceIDs). Zero is "never swept".
	text_compact_seq        BIGINT NOT NULL DEFAULT 0,
	text_compact_entity     TEXT COLLATE "C" NOT NULL DEFAULT '',
	text_compact_key        TEXT COLLATE "C" NOT NULL DEFAULT '',
	text_compact_min_purged INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id)
);

CREATE TABLE rows (
	workspace_id  TEXT COLLATE "C" NOT NULL,
	entity        TEXT COLLATE "C" NOT NULL,
	key           TEXT COLLATE "C" NOT NULL,
	value         BYTEA,
	version       BIGINT NOT NULL,
	-- The sequence that created the row (see storage.Row.CreatedVersion):
	-- stamped on insert, kept on every overwrite. 0 is "unknown", which the
	-- pull path treats conservatively.
	created_version BIGINT NOT NULL DEFAULT 0,
	deleted       BOOLEAN NOT NULL DEFAULT FALSE,
	updated_at    BIGINT NOT NULL DEFAULT 0,
	actor_subject TEXT COLLATE "C" NOT NULL DEFAULT '',
	actor_system  TEXT COLLATE "C" NOT NULL DEFAULT '',
	PRIMARY KEY (workspace_id, entity, key)
)%[1]s;
-- ScanChanges pages by the whole (version, entity, key) tuple, so the index
-- carries all three: sqlitestore stops at version and sorts the tie-break,
-- which a fresh schema has no reason to inherit.
CREATE INDEX rows_by_version ON rows (workspace_id, version, entity, key);
-- Partial, because deleted = true is the only collectable state, which keeps
-- the GC index proportional to the dead rows rather than the live ones.
CREATE INDEX rows_gc ON rows (workspace_id, updated_at) WHERE deleted;
-- Backs ScanEntityByVersionDesc. The key order matches the recency window's
-- tie-break (version descending, key ascending) so a page boundary lands
-- where the in-memory top-K would have put it.
CREATE INDEX rows_by_entity_version
	ON rows (workspace_id, entity, version DESC, key) WHERE NOT deleted;

CREATE TABLE clients (
	workspace_id     TEXT COLLATE "C" NOT NULL,
	client_id        TEXT COLLATE "C" NOT NULL,
	last_mutation_id BIGINT NOT NULL DEFAULT 0,
	updated_at       BIGINT NOT NULL DEFAULT 0,
	-- Verdicts on refused mutations, so a retry that follows a lost push
	-- response is answered with the rejection instead of a bare ack. NULL until
	-- a client has had something refused, which is the common case.
	rejections       BYTEA,
	PRIMARY KEY (workspace_id, client_id)
);
CREATE INDEX clients_gc ON clients (workspace_id, updated_at);

-- The server-global catalog (see storage.Catalog): opaque payloads keyed by id
-- and belonging to no workspace, which is how a multi-app host persists each
-- application's registered FileDescriptorSet and reloads it on boot.
CREATE TABLE catalog (
	id         TEXT COLLATE "C" PRIMARY KEY,
	payload    BYTEA NOT NULL,
	revision   TEXT COLLATE "C" NOT NULL,
	updated_at BIGINT NOT NULL DEFAULT 0
);

-- One row per (text field, write), holding the fragment that write added so
-- a pull can ship a keystroke instead of a document. The primary key is
-- ordered so the per-field span read (one row, one field, seq > cursor) is a
-- range scan of its tail. frag is empty for a gap marker.
CREATE TABLE field_deltas (
	workspace_id TEXT COLLATE "C" NOT NULL,
	entity       TEXT COLLATE "C" NOT NULL,
	key          TEXT COLLATE "C" NOT NULL,
	field        TEXT COLLATE "C" NOT NULL,
	seq          BIGINT NOT NULL,
	frag         BYTEA NOT NULL,
	updated_at   BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, entity, key, field, seq)
)%[1]s;
CREATE INDEX field_deltas_gc ON field_deltas (workspace_id, updated_at);

CREATE TABLE text_floors (
	workspace_id TEXT COLLATE "C" NOT NULL,
	entity       TEXT COLLATE "C" NOT NULL,
	key          TEXT COLLATE "C" NOT NULL,
	field        TEXT COLLATE "C" NOT NULL,
	vv           BYTEA NOT NULL,
	PRIMARY KEY (workspace_id, entity, key, field)
);

CREATE TABLE idempotency (
	workspace_id TEXT COLLATE "C" NOT NULL,
	key          TEXT COLLATE "C" NOT NULL,
	seq          BIGINT NOT NULL,
	result       BYTEA NOT NULL,
	updated_at   BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, key)
)%[1]s;
CREATE INDEX idempotency_gc ON idempotency (workspace_id, updated_at);

-- k is BYTEA, which Postgres compares bytewise — the order the engine's key
-- encoder is built for, and the reason this must never become a text column
-- whatever its collation. The key column sits in the primary key after k
-- because entries of different rows can be byte-equal: a search entry does not
-- end with its row key.
CREATE TABLE index_entries (
	workspace_id TEXT COLLATE "C" NOT NULL,
	entity       TEXT COLLATE "C" NOT NULL,
	idx          TEXT COLLATE "C" NOT NULL,
	k            BYTEA NOT NULL,
	key          TEXT COLLATE "C" NOT NULL,
	PRIMARY KEY (workspace_id, entity, idx, k, key)
)%[1]s;
-- Entries are replaced and deleted per ROW, and neither caller knows which
-- indexes the row occupies; this is what makes that affordable.
CREATE INDEX index_entries_by_row
	ON index_entries (workspace_id, entity, key);

CREATE TABLE index_state (
	workspace_id TEXT COLLATE "C" NOT NULL,
	entity       TEXT COLLATE "C" NOT NULL,
	idx          TEXT COLLATE "C" NOT NULL,
	spec_hash    TEXT COLLATE "C" NOT NULL,
	ready        BOOLEAN NOT NULL DEFAULT FALSE,
	PRIMARY KEY (workspace_id, entity, idx)
);

CREATE TABLE consumer_positions (
	workspace_id TEXT COLLATE "C" NOT NULL,
	consumer     TEXT COLLATE "C" NOT NULL,
	seq          BIGINT NOT NULL DEFAULT 0,
	entity       TEXT COLLATE "C" NOT NULL DEFAULT '',
	key          TEXT COLLATE "C" NOT NULL DEFAULT '',
	updated_at   BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (workspace_id, consumer)
);
CREATE INDEX consumer_positions_gc
	ON consumer_positions (workspace_id, updated_at);

-- No GC index: shadows are deleted per row or wholesale with their
-- consumer, and the primary key's (workspace_id, consumer)
-- prefix is the range scan for that.
CREATE TABLE consumer_shadows (
	workspace_id TEXT COLLATE "C" NOT NULL,
	consumer     TEXT COLLATE "C" NOT NULL,
	entity       TEXT COLLATE "C" NOT NULL,
	key          TEXT COLLATE "C" NOT NULL,
	snapshot     BYTEA NOT NULL,
	PRIMARY KEY (workspace_id, consumer, entity, key)
)%[1]s;

-- Cursor-state entries: the membership sets of issued pull cursors, keyed by
-- the (id, gen) reference the cursor carries. See cursorstate.go for the
-- pruning that keeps the table bounded.
--
-- Unpartitioned whatever the layout asks for: the store behind it is pluggable
-- (server.WithCursorStateStore), entries are immutable and any of them may be
-- evicted at any time, so a deployment large enough for its churn to matter
-- points it at something else entirely.
CREATE TABLE cursor_state (
	workspace_id TEXT COLLATE "C" NOT NULL,
	id           BIGINT NOT NULL,
	gen          BIGINT NOT NULL,
	state        BYTEA NOT NULL,
	written_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (workspace_id, id, gen)
);
`

// partitionedTables names the tables a partitioned layout hash-partitions, in
// the order the baseline creates them. Each one leads its primary key with
// workspace_id — which is what Postgres demands of a unique constraint on a
// partitioned table — and every statement against them binds that column to
// equality, so pruning reaches a single partition and no update ever moves a
// row across one.
//
// The rest are left whole on purpose. workspaces is scanned ACROSS workspaces
// by ListWorkspaceIDs and ListPendingWorkspaceIDs, with no equality on the key
// to prune by, and holds one row per workspace. clients, text_floors,
// index_state, consumer_positions, cursor_state and catalog are small enough
// that the partition count would cost more than the maintenance granularity
// buys.
var partitionedTables = []string{
	"rows",
	"field_deltas",
	"idempotency",
	"index_entries",
	"consumer_shadows",
}

// maxPartitions caps WithPartitions. The modulus multiplies by
// len(partitionedTables), and again by the indexes each of them carries, so
// the ceiling already means tens of thousands of relations — past the point
// where planning time and the file count cost more than the maintenance
// granularity is worth.
const maxPartitions = 1024

// layout is the physical shape of the workspace-scoped tables. It belongs to
// the database rather than to a Store: every workspace in one schema sees the
// same layout.
type layout struct {
	// partitions is the hash modulus the baseline creates the tables in
	// partitionedTables with; zero leaves each of them a single table.
	//
	// On a database that is already migrated this is the number of partitions
	// found attached, which distinguishes partitioned from not and nothing
	// finer — splitting one partition into two leaves a set of mixed moduli.
	// A migration may therefore branch on `partitions > 0` and on nothing else.
	partitions int
}

// partitionBy is the clause the baseline appends to each table in
// partitionedTables, empty when the layout is unpartitioned. It carries its
// own leading space, so an unpartitioned schema reads as `);`.
func (l layout) partitionBy() string {
	if l.partitions == 0 {
		return ""
	}
	return " PARTITION BY HASH (workspace_id)"
}

// baselineSchema is migration 1 under the given layout: the tables, then the
// partitions of the five that have them.
//
// Partitions come after the indexes rather than before, which is the same
// schema either way — an index on a partitioned parent is created on every
// partition attached later — and keeps the tables readable as one block.
func baselineSchema(l layout) string {
	var b strings.Builder
	fmt.Fprintf(&b, baselineDDL, l.partitionBy())
	if l.partitions == 0 {
		return b.String()
	}
	// Zero-padded to the width of the largest remainder, so the partitions of
	// a table sort in the order they are numbered wherever they are listed.
	width := len(strconv.Itoa(l.partitions - 1))
	for _, table := range partitionedTables {
		b.WriteString("\n")
		for i := range l.partitions {
			fmt.Fprintf(&b, "CREATE TABLE %s_p%0*d PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d);\n",
				table, width, i, table, l.partitions, i)
		}
	}
	return b.String()
}

// SchemaVersion returns the ledger version this binary migrates a database to,
// counting from 1. A database Open accepts without complaint is at this version
// or, when every step between is additive, above it.
func SchemaVersion() int {
	return len(migrations)
}

// CanServe reports whether a binary that knows binaryVersion migrations can
// serve a database migrated to schemaVersion — true when every step between
// them only added to the schema.
//
// It answers from THIS binary's migration list, so it is for a process that
// knows schemaVersion: a deploy step asking, before it migrates, whether the
// fleet it is about to run ahead of will survive until the roll reaches it. A
// schemaVersion this binary does not know is unanswerable and reports false.
//
// The running node's own version of this question is decided at Open, out of
// the ledger, since a binary cannot judge steps it does not carry.
func CanServe(binaryVersion, schemaVersion int) bool {
	if schemaVersion > len(migrations) {
		return false
	}
	if binaryVersion < 0 {
		binaryVersion = 0
	}
	for v := binaryVersion + 1; v <= schemaVersion; v++ {
		if !migrations[v-1].additive {
			return false
		}
	}
	return true
}

// Migration is one step of pgstore's schema ledger.
type Migration struct {
	// Version is the ledger version the step brings the database to, counting
	// from 1.
	Version int
	// SQL is the whole step — its DDL and the row recording it in
	// schema_migrations — as a script to run in a single transaction.
	SQL string
}

// Statements returns every migration this binary knows, in order, as SQL for a
// database that has none of them yet.
//
// Applying them all, each in its own transaction, leaves a database Open
// accepts as fully migrated: each script records itself in the ledger and the
// first creates the ledger, so nothing needs stamping afterwards. It is the
// offline form of Migrate, for DDL that a migration tool owns or that is read
// before it runs.
//
// Only WithPartitions changes the output; every other option is ignored, and an
// invalid one is the only error returned.
//
// Open applies whatever the ledger is missing, so a node that boots before
// these have run migrates the database itself; WithoutMigrations makes it fail
// instead. A node whose binary does not know the ledger's highest version
// fails Open either way, which is what a step applied ahead of a roll costs.
func Statements(opts ...Option) ([]Migration, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	l := layout{partitions: cfg.partitions}
	out := make([]Migration, 0, len(migrations))
	for version := 1; version <= len(migrations); version++ {
		out = append(out, Migration{Version: version, SQL: migrationSQL(version, l)})
	}
	return out, nil
}

// Migrate applies any outstanding migrations to the database at dsn and closes
// its connection before returning.
//
// Open does the same at boot. Running it from a deploy step instead lets the
// fleet boot against a database already at the head — at the cost Statements
// describes, since a node older than the ledger cannot open the database at
// all. Give those nodes WithoutMigrations so the step is the only thing that
// applies DDL.
//
// Several processes may run it concurrently: each step is applied once,
// serialized by an advisory lock. That lock is session-scoped and held across
// the transactions applying the steps, so dsn must not name a
// transaction-pooling proxy.
//
// WithPartitions applies only to a database with no migrations recorded.
func Migrate(ctx context.Context, dsn string, opts ...Option) error {
	cfg, err := newConfig(opts)
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("pgstore: connect: %w", err)
	}
	defer pool.Close()
	return migrate(ctx, pool, layout{partitions: cfg.partitions})
}

// querier is the QueryRow that a pool, a single acquired connection and a
// transaction all offer, so the ledger is read through one statement wherever
// it is read from.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// schemaVersion reads the ledger's highest applied version, or zero when the
// ledger exists but is empty. A database with no ledger at all errors.
func schemaVersion(ctx context.Context, q querier) (int, error) {
	var version int
	err := q.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version)
	return version, err
}

// checkLineage refuses a database from the lineage that partitioned every
// table by an app namespace, which the workspaces table carries a namespace
// column of.
//
// It is checked before the ledger version, because that database's version
// numbers count within the OTHER lineage: it sits above this binary's baseline
// and would otherwise be read as a schema from the future rather than one from
// the past. Nothing upgrades it — the namespace is half of every primary key,
// and of the partition key of five of the tables — so it has to be re-created,
// at the cost of a re-bootstrap for every client of the workspaces it held.
func checkLineage(ctx context.Context, q querier) error {
	var found bool
	err := q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'workspaces' AND column_name = 'namespace')`).Scan(&found)
	if err != nil {
		return fmt.Errorf("pgstore: read schema lineage: %w", err)
	}
	if found {
		return fmt.Errorf("pgstore: database belongs to the app-namespace schema lineage; it cannot be upgraded in place and must be re-created")
	}
	return nil
}

// serveFloor is the lowest ledger version whose binary can still serve this
// database: the highest step recorded as non-additive, or zero when every step
// was additive. A binary at or above it may open a database ahead of itself.
//
// The floor is a suffix rather than the exact set of binaries a step broke,
// which is a deliberate over-approximation. Dropping at step 12 a column added
// at step 10 really only breaks binaries 10 and 11 — binary 9 never named it —
// but a rolling deploy moves one version at a time, so the interval that would
// model it exactly buys nothing and the cost of being conservative is a
// refused boot rather than a bad read.
//
// The additive column arrives in migration 4 and is read only when the ledger
// is AHEAD of this binary, which puts the database past 4 whenever this code
// asks. No older shape of the ledger reaches here.
func serveFloor(ctx context.Context, q querier) (int, error) {
	var floor int
	err := q.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE NOT additive`).Scan(&floor)
	if err != nil {
		return 0, fmt.Errorf("pgstore: read schema compatibility: %w", err)
	}
	return floor, nil
}

// verifySchema checks that this binary can serve the database, which is what
// Open does in place of migrate under WithoutMigrations. A ledger short of this
// binary is the deploy step not having run; a ledger ahead of it is judged by
// serveFloor.
//
// It reads the ledger and nothing else — no DDL, no advisory lock — so a node
// running this way needs no schema privileges beyond the rows it serves. That
// is not a property migrate can be given: Postgres checks CREATE on the schema
// before it checks whether the table is already there, so even the ledger's
// CREATE TABLE IF NOT EXISTS is refused to a role without it.
func verifySchema(ctx context.Context, pool *pgxpool.Pool) error {
	if err := checkLineage(ctx, pool); err != nil {
		return err
	}
	version, err := schemaVersion(ctx, pool)
	if err != nil {
		return fmt.Errorf("pgstore: read schema version (has this database been migrated?): %w", err)
	}
	if version < len(migrations) {
		return fmt.Errorf("pgstore: database schema at version %d, want %d: apply the outstanding migrations before opening without them",
			version, len(migrations))
	}
	return checkAhead(ctx, pool, version)
}

// checkAhead decides whether a ledger past this binary's own version is one it
// may still serve. Nothing to do when the database is not ahead.
func checkAhead(ctx context.Context, q querier, version int) error {
	if version <= len(migrations) {
		return nil
	}
	floor, err := serveFloor(ctx, q)
	if err != nil {
		return err
	}
	if len(migrations) < floor {
		return errSchemaNewer(version, floor)
	}
	return nil
}

// errSchemaNewer is the refusal both boot paths share, and it is now specific
// about the way out: the database carries a step this binary cannot serve
// through, so the answer is a newer binary rather than anything done here.
func errSchemaNewer(version, floor int) error {
	return fmt.Errorf("pgstore: database schema version %d needs a binary at version %d or later (this one knows %d): migration %d removed or narrowed something older binaries read",
		version, floor, len(migrations), floor)
}

// migrationSQL is one ledger step as a script: its DDL followed by the row
// recording it. Version 1 also creates the ledger, so the scripts bootstrap an
// empty database on their own.
func migrationSQL(version int, l layout) string {
	step := migrations[version-1]
	var b strings.Builder
	if version == 1 {
		b.WriteString(ledgerDDL)
	}
	b.WriteString(step.sql(l))
	fmt.Fprintf(&b, "\nINSERT INTO schema_migrations (version, additive) VALUES (%d, %t);\n",
		version, step.additive)
	return b.String()
}

// migrationLockKey serializes schema migration across nodes. Several servers
// booting at once would otherwise race to apply the same migration, and CREATE
// TABLE is not idempotent. The value is "syncplan" in ASCII, chosen to be
// recognisable in pg_locks rather than for any property of the number.
const migrationLockKey int64 = 0x73796E63706C616E

// migrate brings the database up to len(migrations), under an advisory lock so
// that concurrently booting nodes apply each step once.
//
// want is the layout to CREATE the schema with, and is read only when the
// ledger is empty; an already-migrated database keeps the layout it has, which
// is what lets a deploy step choose one and every later boot omit it.
//
// The lock is taken FIRST, before even the ledger exists. CREATE TABLE IF NOT
// EXISTS looks idempotent but is not concurrency-safe: two sessions both find
// the table missing, both create it, and the loser fails on a unique violation
// in pg_type rather than shrugging. Nothing here may touch the schema outside
// the lock.
func migrate(ctx context.Context, pool *pgxpool.Pool, want layout) error {
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

	if _, err := conn.Exec(ctx, ledgerDDL); err != nil {
		return fmt.Errorf("pgstore: create migration ledger: %w", err)
	}

	if err := checkLineage(ctx, conn); err != nil {
		return err
	}

	version, err := schemaVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("pgstore: read schema version: %w", err)
	}
	if version > len(migrations) {
		// Ahead of this binary: there is nothing to apply, and whether it may
		// serve at all is the ledger's to say.
		return checkAhead(ctx, conn, version)
	}
	l := want
	if version > 0 {
		if l, err = detectLayout(ctx, conn); err != nil {
			return err
		}
	}
	for ; version < len(migrations); version++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, migrationSQL(version+1, l)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgstore: migration %d: %w", version+1, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgstore: migration %d: commit: %w", version+1, err)
		}
	}
	return nil
}

// detectLayout reads back the layout of an already-migrated database, so a
// migration written for both layouts is generated for the one it is about to
// run against. Reading the catalog rather than a recorded number is what keeps
// it true after a partition is split.
func detectLayout(ctx context.Context, conn *pgxpool.Conn) (layout, error) {
	var partitions int
	// to_regclass resolves through search_path and yields NULL — hence a count
	// of zero — for a table that is not there at all.
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = to_regclass($1)`,
		partitionedTables[0]).Scan(&partitions); err != nil {
		return layout{}, fmt.Errorf("pgstore: read table layout: %w", err)
	}
	return layout{partitions: partitions}, nil
}
