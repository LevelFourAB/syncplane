package sqlitestore

import (
	"database/sql"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// pragma reads one pragma back from a pool, which is the only way to tell that
// a DSN parameter reached the connection rather than being dropped.
func pragma(db *sql.DB, name string) string {
	var v string
	ExpectWithOffset(1, db.QueryRow("PRAGMA "+name).Scan(&v)).To(Succeed())
	return v
}

var _ = Describe("SQLite tuning", func() {
	It("renders a byte cache size in SQLite's negative-KiB form", func() {
		// cache_size counts pages when positive, so a byte count handed over
		// as-is would ask for a cache thousands of times the size intended.
		Expect(cacheSizePragma(8 << 20)).To(Equal("_pragma=cache_size(-8192)"))
		// ...and a sub-KiB request still has to name a cache, not disable one.
		Expect(cacheSizePragma(512)).To(Equal("_pragma=cache_size(-1)"))
	})

	It("applies the tuning pragmas to both pools", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "tuned.db"),
			WithCacheSize(4<<20), WithWALSizeLimit(1<<20))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(pragma(store.write, "cache_size")).To(Equal("-4096"))
		Expect(pragma(store.write, "temp_store")).To(Equal("2"), "MEMORY")
		Expect(pragma(store.write, "journal_size_limit")).To(Equal("1048576"))
		Expect(pragma(store.write, "analysis_limit")).To(Equal("400"))

		// The reader pool is a separate connection, and every pragma above is
		// per-connection state: nothing carries over from the writer.
		Expect(store.read).NotTo(BeIdenticalTo(store.write))
		Expect(pragma(store.read, "cache_size")).To(Equal("-4096"))
		Expect(pragma(store.read, "temp_store")).To(Equal("2"), "MEMORY")
		Expect(pragma(store.read, "query_only")).To(Equal("1"))
	})

	It("leaves SQLite's own defaults in place when a tunable is zero", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "plain.db"),
			WithCacheSize(0), WithWALSizeLimit(0))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(pragma(store.write, "cache_size")).To(Equal("-2000"))
		Expect(pragma(store.write, "journal_size_limit")).To(Equal("-1"), "no limit")
	})

	It("bounds the reader pool and holds its connections idle", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "pool.db"),
			WithReadConnections(3))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.read.Stats().MaxOpenConnections).To(Equal(3))
		Expect(store.write.Stats().MaxOpenConnections).To(Equal(1),
			"the writer is what serializes writes; nothing may widen it")
	})

	It("never widens the writer pool for an in-memory database", func() {
		// An in-memory database is private to its connection, so reads run on
		// the writer pool. A reader cap applied to that shared handle would let
		// two write transactions run at once and lose write mutual exclusion.
		store, err := Open(":memory:", WithReadConnections(8))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.read).To(BeIdenticalTo(store.write))
		Expect(store.write.Stats().MaxOpenConnections).To(Equal(1))
	})
})
