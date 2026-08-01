package sqlitestore_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "modernc.org/sqlite"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/storage/storagetest"
)

func TestSqlitestore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQLite Store Suite")
}

var _ = storagetest.Conformance("sqlite", func() storage.Adapter {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return store
})

// A namespaced store must satisfy the full adapter contract too.
var _ = storagetest.Conformance("sqlite (namespaced)", func() storage.Adapter {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"),
		sqlitestore.WithNamespace("app1"))
	Expect(err).NotTo(HaveOccurred())
	return store
})

// One root store, two derived namespaces sharing its *sql.DB. cleanup owns
// the real Close — derived stores' Close is a no-op.
var _ = storagetest.ConformanceNamespaced("sqlite", func() (storage.Adapter, storage.Adapter, func()) {
	root, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return root.Namespace("a"), root.Namespace("b"), func() {
		Expect(root.Close()).To(Succeed())
	}
})

var _ = Describe("SQLite migrations", func() {
	It("stamps user_version and keeps data intact across reopen", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "migrate.db")

		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())

		store, err = sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("v1")))
			return nil
		})).To(Succeed())

		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
		var version int
		Expect(db.QueryRow(`PRAGMA user_version`).Scan(&version)).To(Succeed())
		Expect(version).To(Equal(12), "all migrations applied exactly once")
	})

	It("migrates a pre-migration database in place", func() {
		path := filepath.Join(GinkgoT().TempDir(), "old.db")
		// Build a database the way the pre-migration binary did: baseline
		// schema only, user_version 0, no updated_at column.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`
			CREATE TABLE workspaces (
				workspace_id      TEXT PRIMARY KEY,
				last_seq          INTEGER NOT NULL DEFAULT 0,
				tombstone_horizon INTEGER NOT NULL DEFAULT 0
			);
			CREATE TABLE rows (
				workspace_id TEXT NOT NULL,
				entity       TEXT NOT NULL,
				key          TEXT NOT NULL,
				value        BLOB,
				version      INTEGER NOT NULL,
				deleted      INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (workspace_id, entity, key)
			);
			CREATE TABLE clients (
				workspace_id     TEXT NOT NULL,
				client_id        TEXT NOT NULL,
				last_mutation_id INTEGER NOT NULL DEFAULT 0,
				updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
				PRIMARY KEY (workspace_id, client_id)
			);
			INSERT INTO rows (workspace_id, entity, key, value, version, deleted)
			VALUES ('ws1', 'widget', 'k1', x'aa', 1, 0);
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithReadTx(context.Background(), "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte{0xaa}))
			return nil
		})).To(Succeed())
	})

	It("migrates a user_version-1 database into the default namespace", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "v1.db")
		// A database as the user_version-1 binary left it: rows already have
		// the INTEGER updated_at from migration 1, clients still carry the
		// TEXT datetime('now') timestamps, nothing is namespaced.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`
			CREATE TABLE workspaces (
				workspace_id      TEXT PRIMARY KEY,
				last_seq          INTEGER NOT NULL DEFAULT 0,
				tombstone_horizon INTEGER NOT NULL DEFAULT 0
			);
			CREATE TABLE rows (
				workspace_id TEXT NOT NULL,
				entity       TEXT NOT NULL,
				key          TEXT NOT NULL,
				value        BLOB,
				version      INTEGER NOT NULL,
				deleted      INTEGER NOT NULL DEFAULT 0,
				updated_at   INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (workspace_id, entity, key)
			);
			CREATE INDEX rows_by_version ON rows (workspace_id, version);
			CREATE TABLE clients (
				workspace_id     TEXT NOT NULL,
				client_id        TEXT NOT NULL,
				last_mutation_id INTEGER NOT NULL DEFAULT 0,
				updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
				PRIMARY KEY (workspace_id, client_id)
			);
			INSERT INTO workspaces (workspace_id, last_seq, tombstone_horizon) VALUES ('ws1', 5, 2);
			INSERT INTO rows (workspace_id, entity, key, value, version, deleted, updated_at)
			VALUES ('ws1', 'widget', 'k1', x'aa', 5, 0, 1700000000000);
			INSERT INTO clients (workspace_id, client_id, last_mutation_id, updated_at)
			VALUES ('ws1', 'c1', 3, '2024-01-02 03:04:05');
			PRAGMA user_version = 1;
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		// All pre-migration data is visible through the default namespace ''.
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte{0xaa}))
			Expect(row.Version).To(BeEquivalentTo(5))
			seq, err := tx.CurrentSeq()
			Expect(err).NotTo(HaveOccurred())
			Expect(seq).To(BeEquivalentTo(5))
			horizon, err := tx.TombstoneHorizon()
			Expect(err).NotTo(HaveOccurred())
			Expect(horizon).To(BeEquivalentTo(2))
			id, err := tx.ClientLastMutationID("c1")
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(BeEquivalentTo(3))
			// Migration 3->4 backfills a store epoch for the pre-existing
			// workspace, so cursors already in the field are pinned to this
			// lineage on the next pull.
			epoch, err := tx.StoreEpoch()
			Expect(err).NotTo(HaveOccurred())
			Expect(epoch).NotTo(BeZero(), "pre-existing workspace backfilled with an epoch")
			// Migration 8->9 adds attribution columns. A row written before it
			// reads back unattributed, which is the truth about it: nothing
			// recorded who wrote it, and nothing can reconstruct it.
			Expect(row.Actor).To(Equal(storage.Actor{}), "pre-migration row is unattributed")
			return nil
		})).To(Succeed())

		// ...and invisible from any other namespace.
		Expect(store.Namespace("other").WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			_, err := tx.GetRow("widget", "k1")
			Expect(err).To(MatchError(storage.ErrNotFound))
			return nil
		})).To(Succeed())

		// The clients.updated_at TEXT timestamp became unix millis.
		raw, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(raw.Close()).To(Succeed()) })
		var ns string
		var updatedAt int64
		Expect(raw.QueryRow(
			`SELECT namespace, updated_at FROM clients WHERE workspace_id = 'ws1' AND client_id = 'c1'`,
		).Scan(&ns, &updatedAt)).To(Succeed())
		Expect(ns).To(Equal(""))
		want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixMilli()
		Expect(updatedAt).To(Equal(want), "TEXT datetime converted to unix millis")
	})

	It("gives a user_version-9 database index storage without touching its rows", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "v9.db")
		// A database as the user_version-9 binary left it: namespaced,
		// attributed, and with no index tables at all.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`
			CREATE TABLE workspaces (
				namespace           TEXT NOT NULL DEFAULT '',
				workspace_id        TEXT NOT NULL,
				last_seq            INTEGER NOT NULL DEFAULT 0,
				tombstone_horizon   INTEGER NOT NULL DEFAULT 0,
				store_epoch         INTEGER NOT NULL DEFAULT 0,
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
				actor_issuer  TEXT NOT NULL DEFAULT '',
				actor_subject TEXT NOT NULL DEFAULT '',
				actor_client  TEXT NOT NULL DEFAULT '',
				actor_system  INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (namespace, workspace_id, entity, key)
			);
			CREATE INDEX rows_by_version ON rows (namespace, workspace_id, version);
			INSERT INTO workspaces (workspace_id, last_seq, store_epoch) VALUES ('ws1', 7, 42);
			INSERT INTO rows (workspace_id, entity, key, value, version)
			VALUES ('ws1', 'widget', 'k1', x'aa', 5), ('ws1', 'widget', 'k2', x'bb', 7);
			PRAGMA user_version = 9;
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			// Pre-existing rows survive untouched...
			row, err := tx.GetRow("widget", "k1")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte{0xaa}))
			// ...and the new recency scan reads them newest-first.
			var keys []string
			Expect(tx.ScanEntityByVersionDesc("widget", 0, func(r storage.Row) error {
				keys = append(keys, r.Key)
				return nil
			})).To(Succeed())
			Expect(keys).To(Equal([]string{"k2", "k1"}))
			// An index nobody has built reads as absent, not as an error.
			hash, ready, err := tx.IndexState("issue", "__ref_project")
			Expect(err).NotTo(HaveOccurred())
			Expect(hash).To(BeEmpty())
			Expect(ready).To(BeFalse())
			return nil
		})).To(Succeed())
	})
})

var _ = Describe("SQLite transaction and DSN invariants", func() {
	ctx := context.Background()

	It("opens a path containing URI metacharacters, pragmas intact", func() {
		// '?' ends the DSN's path for the driver, '#' for SQLite's URI parser,
		// and '%' introduces an escape SQLite decodes — an unescaped path with
		// any of them opens a different database, or none, and drops every
		// pragma after it.
		dir := filepath.Join(GinkgoT().TempDir(), "odd ?#%2f dir")
		Expect(os.Mkdir(dir, 0o755)).To(Succeed())
		path := filepath.Join(dir, "test.db")

		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
		})).To(Succeed())

		// The bytes landed in the file the caller named, not in one SQLite
		// invented by decoding the path.
		Expect(path).To(BeAnExistingFile())
		Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("v")))
			return nil
		})).To(Succeed())

		// The pragmas that follow the path in the DSN reached the connection
		// rather than being eaten by the path: journal_mode(WAL) is the one the
		// whole reader/writer split rests on, and WAL leaves a sidecar log.
		Expect(path + "-wal").To(BeAnExistingFile())
	})

	It("hands read closures a transaction that cannot be widened to a write", func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "ro.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
			// WithReadTx never commits, so a write reached this way would be
			// silently rolled back; the assertion must fail instead.
			_, ok := tx.(storage.WriteTx)
			Expect(ok).To(BeFalse(), "a read tx must not satisfy storage.WriteTx")
			return nil
		})).To(Succeed())
	})

	It("takes the write lock at BEGIN, before the closure's first write", func() {
		path := filepath.Join(GinkgoT().TempDir(), "immediate.db")
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		// A separate connection standing in for another process; the short
		// busy_timeout keeps the contention check fast.
		other, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(50)")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(other.Close()).To(Succeed()) })
		conn, err := other.Conn(ctx)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(conn.Close()).To(Succeed()) })

		reading := make(chan struct{})
		release := make(chan struct{})
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				// Read only: with a deferred BEGIN nothing would be locked yet.
				if _, err := tx.CurrentSeq(); err != nil {
					return err
				}
				close(reading)
				<-release
				return nil
			})
		}()

		<-reading
		_, err = conn.ExecContext(ctx, `BEGIN IMMEDIATE`)
		Expect(err).To(HaveOccurred(), "the store's write tx already holds the write lock")

		close(release)
		Eventually(writeDone, time.Second).Should(Receive(BeNil()))
	})
})

var _ = Describe("SQLite maintenance", func() {
	ctx := context.Background()

	// analyzed reports how many query-planner statistics the file holds, read
	// through a connection of the test's own so the answer never depends on
	// the store's pools. A database nothing has ANALYZEd has no sqlite_stat1
	// table at all, which reads as zero rather than as an error.
	analyzed := func(path string) int {
		db, err := sql.Open("sqlite", "file:"+path+"?_pragma=query_only(true)")
		Expect(err).NotTo(HaveOccurred())
		defer db.Close()
		var present int
		Expect(db.QueryRow(
			`SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_stat1'`,
		).Scan(&present)).To(Succeed())
		if present == 0 {
			return 0
		}
		var rows int
		Expect(db.QueryRow(`SELECT count(*) FROM sqlite_stat1`).Scan(&rows)).To(Succeed())
		return rows
	}

	// seed writes enough rows for ANALYZE to have something to say about them.
	seed := func(store storage.Adapter) {
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			for i := range 200 {
				if err := tx.PutRow("widget", fmt.Sprintf("k%d", i), []byte("v"), uint64(i+1), storage.Actor{}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}

	It("refreshes query-planner statistics on the configured interval", func() {
		path := filepath.Join(GinkgoT().TempDir(), "optimize.db")
		store, err := sqlitestore.Open(path, sqlitestore.WithOptimizeInterval(20*time.Millisecond))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		seed(store)
		// Statistics land while the store is still open: this is the pass that
		// keeps a long-running process planning against the data it has now,
		// not the data it had at boot.
		Eventually(func() int { return analyzed(path) }, 2*time.Second, 20*time.Millisecond).
			Should(BeNumerically(">", 0))
	})

	It("leaves statistics behind when the interval never elapses", func() {
		path := filepath.Join(GinkgoT().TempDir(), "closed.db")
		store, err := sqlitestore.Open(path, sqlitestore.WithOptimizeInterval(0))
		Expect(err).NotTo(HaveOccurred())
		seed(store)
		Expect(store.Close()).To(Succeed())

		// No background pass ever ran, and the process still has to hand the
		// next one usable statistics.
		Expect(analyzed(path)).To(BeNumerically(">", 0))
	})

	It("closes cleanly more than once", func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "twice.db"))
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Close()).To(Succeed())
		// The second Close must not close the maintenance loop's channel twice.
		Expect(store.Close()).To(Succeed())
	})
})

var _ = Describe("SQLite reader/writer split", func() {
	ctx := context.Background()

	It("serves reads concurrently with an open write transaction", func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "split.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		// Seed a committed row the concurrent reader can find.
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k", []byte("v"), 1, storage.Actor{})
		})).To(Succeed())

		writing := make(chan struct{})
		release := make(chan struct{})
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				if err := tx.PutRow("widget", "k2", []byte("v2"), 2, storage.Actor{}); err != nil {
					return err
				}
				close(writing) // the write tx now holds the single writer connection
				<-release      // hold it open until the reader has finished
				return nil
			})
		}()

		<-writing
		// Under the old single-connection store this read would block on the
		// writer connection until the tx above committed; with the reader pool
		// it returns immediately against the WAL snapshot.
		read := make(chan error, 1)
		go func() {
			read <- store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				row, err := tx.GetRow("widget", "k")
				Expect(row.Value).To(Equal([]byte("v")))
				return err
			})
		}()
		Eventually(read, time.Second).Should(Receive(BeNil()))

		close(release)
		Eventually(writeDone, time.Second).Should(Receive(BeNil()))
	})
})
