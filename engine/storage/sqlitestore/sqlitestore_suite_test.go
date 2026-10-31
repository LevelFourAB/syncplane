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

var _ = storagetest.CursorState("sqlite", func() storage.Adapter {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return store
})

// The same store handed back twice: one process is the whole sharing a
// single-process adapter's model permits, so it is also the whole lease space.
var _ = storagetest.Leases("sqlite", func() (storage.Adapter, storage.Adapter, func()) {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return store, store, func() {
		Expect(store.Close()).To(Succeed())
	}
})

var _ = storagetest.Catalog("sqlite", func() storage.Adapter {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	return store
})

var _ = Describe("SQLite schema versioning", func() {
	// The stamps a database created by this binary carries, spelled out rather
	// than read from the package: changing either is a compatibility break for
	// every database in the field, and this is the assertion that makes it
	// deliberate.
	const applicationID = 0x73796E63
	const baselineVersion = 1
	// The version a database this binary creates or upgrades ends at: the
	// baseline plus every migration since. Bump it with each migration added.
	const latestVersion = 5

	// oldIndexTables puts the index entry tables back in the shape every
	// version before 4 has, so a test can rewind a database past migration 4.
	const oldIndexTables = `
		DROP TABLE index_entries;
		DROP TABLE index_row_entries;
		DROP TABLE index_scopes;
		CREATE TABLE index_entries (
			workspace_id TEXT NOT NULL,
			entity       TEXT NOT NULL,
			idx          TEXT NOT NULL,
			k            BLOB NOT NULL,
			key          TEXT NOT NULL,
			PRIMARY KEY (workspace_id, entity, idx, k, key)
		) WITHOUT ROWID;
		CREATE INDEX index_entries_by_row ON index_entries (workspace_id, entity, key);`

	// foreign creates a database holding one table under the given stamps,
	// standing in for whatever a binary other than this one left behind.
	foreign := func(name string, appID, version int) string {
		path := filepath.Join(GinkgoT().TempDir(), name)
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(fmt.Sprintf(`
			CREATE TABLE workspaces (workspace_id TEXT PRIMARY KEY);
			PRAGMA application_id = %d;
			PRAGMA user_version = %d;
		`, appID, version))
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())
		return path
	}

	It("stamps a new database and keeps its data across reopen", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "baseline.db")

		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())

		// The second Open finds the schema already there and must leave it — and
		// the rows in it — alone.
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
		var appID, version int
		Expect(db.QueryRow(`PRAGMA application_id`).Scan(&appID)).To(Succeed())
		Expect(appID).To(Equal(applicationID))
		Expect(db.QueryRow(`PRAGMA user_version`).Scan(&version)).To(Succeed())
		// A fresh database ends where an upgraded one does, so the two never
		// differ in shape.
		Expect(version).To(Equal(latestVersion))
	})

	It("upgrades a baseline database to record sequence lineage", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "upgrade.db")
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			_, err := tx.NextSeq()
			return err
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())

		// Back to the baseline shape, holding a sequence allocated before the
		// lineage table existed.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(oldIndexTables + fmt.Sprintf("\nDROP TABLE seq_lineage;\nDROP TABLE deleted_workspaces;\nPRAGMA user_version = %d;", baselineVersion))
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err = sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			_, err := tx.NextSeq()
			return err
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			Expect(tx.SeqLineage(1)).To(BeZero(), "a sequence from before the upgrade has no mark")
			Expect(tx.SeqLineage(2)).NotTo(BeZero())
			return nil
		})).To(Succeed())
	})

	It("upgrades a database holding index entries by dropping them for a rebuild", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "upgrade-index.db")
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{})
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())

		// Back to the version-3 shape: entries keyed by name, a ready index and
		// a workspace whose build mark says it is up to date.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(oldIndexTables + `
			INSERT INTO index_entries VALUES ('ws1', 'widget', 'by_label', x'01', 'k1');
			INSERT INTO index_state (workspace_id, entity, idx, spec_hash, ready)
				VALUES ('ws1', 'widget', 'by_label', 'h1', 1);
			UPDATE workspaces SET index_build_mark = 'm1' WHERE workspace_id = 'ws1';
			PRAGMA user_version = 3;`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err = sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			var keys []string
			Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, storage.ScanAsc, func(_ []byte, k string) error {
				keys = append(keys, k)
				return nil
			})).To(Succeed())
			Expect(keys).To(BeEmpty(), "the old entries are not carried over")
			hash, ready, err := tx.IndexState("widget", "by_label")
			Expect(err).NotTo(HaveOccurred())
			Expect(hash).To(BeEmpty())
			Expect(ready).To(BeFalse(), "an index without its entries must not read as ready")
			return nil
		})).To(Succeed())
		stale, err := store.ListStaleIndexWorkspaceIDs(ctx, "m1", storage.Range{}, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(ContainElement("ws1"), "the build pass must visit the workspace again")

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{{Index: "by_label", Key: []byte{0x02}}})
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			var keys []string
			Expect(tx.ScanIndex("widget", "by_label", nil, nil, 0, storage.ScanAsc, func(_ []byte, k string) error {
				keys = append(keys, k)
				return nil
			})).To(Succeed())
			Expect(keys).To(Equal([]string{"k1"}))
			return nil
		})).To(Succeed())
	})

	It("upgrades version-4 index entries by dropping them for a rebuild", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "upgrade-index-4.db")
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := tx.PutRow("widget", "k1", []byte("v1"), 1, storage.Actor{}); err != nil {
				return err
			}
			if err := tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{{Index: "by_label", Key: []byte("a\x00k1\x00\x00")}}); err != nil {
				return err
			}
			return tx.SetIndexState("widget", "by_label", "h1", true)
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())

		// Back to the version-4 shape, where key holds the row key as text, with
		// a ready index and a workspace whose build mark says it is up to date.
		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`
			DROP TABLE index_entries;
			CREATE TABLE index_entries (
				scope INTEGER NOT NULL,
				k     BLOB NOT NULL,
				key   TEXT NOT NULL,
				PRIMARY KEY (scope, k, key)
			) WITHOUT ROWID;
			INSERT INTO index_entries SELECT id, x'61006b310000', 'k1' FROM index_scopes;
			UPDATE workspaces SET index_build_mark = 'm1' WHERE workspace_id = 'ws1';
			PRAGMA user_version = 4;`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		store, err = sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		scan := func() []string {
			GinkgoHelper()
			var keys []string
			Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				return tx.ScanIndex("widget", "by_label", nil, nil, 0, storage.ScanAsc, func(_ []byte, k string) error {
					keys = append(keys, k)
					return nil
				})
			})).To(Succeed())
			return keys
		}
		Expect(scan()).To(BeEmpty(), "the old entries are not carried over")
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			_, ready, err := tx.IndexState("widget", "by_label")
			Expect(err).NotTo(HaveOccurred())
			Expect(ready).To(BeFalse(), "an index without its entries must not read as ready")
			return nil
		})).To(Succeed())
		stale, err := store.ListStaleIndexWorkspaceIDs(ctx, "m1", storage.Range{}, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(ContainElement("ws1"), "the build pass must visit the workspace again")

		// The per-row list went with the entries, so the rebuild writes the same
		// entry again instead of reading it as already stored.
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{{Index: "by_label", Key: []byte("a\x00k1\x00\x00")}})
		})).To(Succeed())
		Expect(scan()).To(Equal([]string{"k1"}))
	})

	It("refuses a database from a schema lineage that predates the stamp", func() {
		// The version numbering restarts per lineage, so an unstamped database is
		// refused whatever it says its version is — including one this binary
		// would otherwise read as its own baseline.
		for _, version := range []int{0, baselineVersion, 12} {
			_, err := sqlitestore.Open(foreign(fmt.Sprintf("old-%d.db", version), 0, version))
			Expect(err).To(MatchError(ContainSubstring("carries no application_id")))
		}
	})

	It("refuses a database that was never a syncplane store", func() {
		_, err := sqlitestore.Open(foreign("other.db", 0x11223344, baselineVersion))
		Expect(err).To(MatchError(ContainSubstring("is not a syncplane store")))
	})

	It("refuses a database newer than the binary", func() {
		_, err := sqlitestore.Open(foreign("future.db", applicationID, latestVersion+1))
		Expect(err).To(MatchError(ContainSubstring("newer than this binary")))
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
