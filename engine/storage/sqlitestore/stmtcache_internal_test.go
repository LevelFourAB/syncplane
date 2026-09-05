package sqlitestore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// countingDriver wraps the SQLite driver and counts how often a connection is
// handed one SQL text to compile. It deliberately exposes nothing but
// driver.Conn, so database/sql cannot route a bare Exec through
// driver.ExecerContext: every execution then goes through Prepare, and the
// counter reads as "times this text was parsed".
type countingDriver struct {
	inner driver.Driver

	mu       sync.Mutex
	prepares map[string]int
}

func (d *countingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: conn, d: d}, nil
}

func (d *countingDriver) count(query string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.prepares[query]
}

type countingConn struct {
	driver.Conn
	d *countingDriver
}

func (c countingConn) Prepare(query string) (driver.Stmt, error) {
	c.d.mu.Lock()
	c.d.prepares[query]++
	c.d.mu.Unlock()
	return c.Conn.Prepare(query)
}

// counting is registered once for the whole suite; the SQLite driver it wraps
// is taken from a throwaway pool because the driver type itself is not exported.
var counting = func() *countingDriver {
	probe, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	defer probe.Close()
	d := &countingDriver{inner: probe.Driver(), prepares: map[string]int{}}
	sql.Register("sqlite-counting", d)
	return d
}()

var _ = Describe("Prepared statement cache", func() {
	const upsert = `INSERT INTO t (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v`

	// A single-connection pool over the counting driver, shaped like the store's
	// writer pool, with the one table the statements below touch.
	newPool := func() *sql.DB {
		path := filepath.Join(GinkgoT().TempDir(), "counted.db")
		db, err := sql.Open("sqlite-counting", dsn(path, "_txlock=immediate", "_pragma=journal_mode(WAL)"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		_, err = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)`)
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	// runTx does one transaction of writes through the cache, warming first the
	// way WithWriteTx does.
	runTx := func(db *sql.DB, cache *stmtCache, writes int) {
		ctx := context.Background()
		cache.warm(ctx)
		tx, err := db.BeginTx(ctx, nil)
		Expect(err).NotTo(HaveOccurred())
		t := &sqliteTx{tx: tx, stmts: cache}
		for i := range writes {
			_, err := t.exec(upsert, fmt.Sprintf("k%d", i), i)
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(tx.Commit()).To(Succeed())
	}

	It("compiles one statement text once, however many times it runs", func() {
		db := newPool()
		cache := newStmtCache(db)

		// The first transaction misses and runs the text as text; the warm at the
		// top of the second prepares it.
		runTx(db, cache, 4)
		runTx(db, cache, 4)
		warmed := counting.count(upsert)

		// Everything after that reuses the one prepared statement, across
		// transaction boundaries as well as within one.
		for range 10 {
			runTx(db, cache, 4)
		}
		Expect(counting.count(upsert)).To(Equal(warmed),
			"a prepared statement must be reused, not recompiled per transaction")
		Expect(cache.ready).To(HaveLen(1))
		Expect(cache.pending).To(BeEmpty())
	})

	It("keeps one entry per distinct text and closes them with the store", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "cache.db"))
		Expect(err).NotTo(HaveOccurred())

		ctx := context.Background()
		write := func(seq uint64) {
			Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
				if err := tx.PutRow("e", "k", []byte("v"), seq, storage.Actor{}); err != nil {
					return err
				}
				_, err := tx.NextSeq()
				return err
			})).To(Succeed())
		}
		for seq := range uint64(5) {
			write(seq + 1)
		}

		// PutRow, NextSeq and the register INSERT are three texts, whatever the
		// number of transactions that ran them.
		before := len(store.writeStmts.ready)
		Expect(before).To(BeNumerically(">=", 3))
		texts := make([]string, 0, before)
		for q := range store.writeStmts.ready {
			texts = append(texts, q)
		}
		for seq := range uint64(5) {
			write(seq + 6)
		}
		Expect(store.writeStmts.ready).To(HaveLen(before))

		// The same statement object serves every transaction that runs the text.
		for _, q := range texts {
			stmt := store.writeStmts.get(q)
			Expect(stmt).NotTo(BeNil())
			Expect(store.writeStmts.get(q)).To(BeIdenticalTo(stmt))
		}

		Expect(store.Close()).To(Succeed())
		Expect(store.writeStmts.ready).To(BeEmpty())
	})

	It("keeps a scan whole when its callback runs the same scan again", func() {
		// A prepared statement is one virtual machine, so a nested scan of the
		// same text has to get a machine of its own or both walks lose rows.
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "nested.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		ctx := context.Background()
		const rows = 5
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			for i := range rows {
				if err := tx.PutRow("e", fmt.Sprintf("k%d", i), []byte("v"), uint64(i+1), storage.Actor{}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())

		scan := func() (outer int, inner []int) {
			Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				return tx.ScanEntity("e", func(storage.Row) error {
					outer++
					nested := 0
					if err := tx.ScanEntity("e", func(storage.Row) error {
						nested++
						return nil
					}); err != nil {
						return err
					}
					inner = append(inner, nested)
					return nil
				})
			})).To(Succeed())
			return outer, inner
		}

		// The first pass leaves the text queued; the second runs it prepared,
		// which is the pass the nesting has to survive.
		scan()
		outer, inner := scan()
		Expect(outer).To(Equal(rows))
		Expect(inner).To(Equal([]int{rows, rows, rows, rows, rows}))
	})

	It("serves reads after the pool has retired the connection it prepared on", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "idle.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		ctx := context.Background()
		Expect(store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			return tx.PutRow("e", "k", []byte("v"), 1, storage.Actor{})
		})).To(Succeed())

		read := func() []byte {
			var value []byte
			Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				row, err := tx.GetRow("e", "k")
				value = row.Value
				return err
			})).To(Succeed())
			return value
		}
		Expect(read()).To(Equal([]byte("v")))
		Expect(read()).To(Equal([]byte("v")), "the second read runs prepared")

		// database/sql's cleaner runs on a one-second floor, so this waits for a
		// tick rather than for the timeout itself.
		store.read.SetConnMaxIdleTime(time.Millisecond)
		Eventually(func() int { return store.read.Stats().Idle }, 10*time.Second).To(BeZero())

		Expect(read()).To(Equal([]byte("v")),
			"a statement whose connection was retired must be re-prepared, not fail")
	})
})
