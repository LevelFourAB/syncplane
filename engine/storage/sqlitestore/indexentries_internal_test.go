package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("index entry row keys", func() {
	var store *Store
	ctx := context.Background()

	// Each entry is the one the row with that key holds in by_label. The first
	// ends with the row key the way the engine writes it for a plain key. The
	// others do not: the engine escapes U+0000 to "\x00\xff" and writes an
	// astral character as a CESU-8 surrogate pair, so the entry ends with bytes
	// other than the key's own, and one entry does not end with its key at all.
	entries := map[string][]byte{
		"plain":            []byte("a\x00\x00plain\x00\x00"),
		"nul\u0000key":     []byte("b\x00\x00nul\x00\xffkey\x00\x00"),
		"astral\U0001f600": []byte("c\x00\x00astral\xed\xa0\xbd\xed\xb8\x80\x00\x00"),
		"elsewhere":        []byte("d\x00\x00"),
	}
	order := []string{"plain", "nul\u0000key", "astral\U0001f600", "elsewhere"}

	scan := func(o storage.ScanOrder) []string {
		GinkgoHelper()
		var keys []string
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			return tx.ScanIndex("widget", "by_label", nil, nil, 0, o, func(entry []byte, rowKey string) error {
				Expect(entry).To(Equal(entries[rowKey]))
				keys = append(keys, rowKey)
				return nil
			})
		})).To(Succeed())
		return keys
	}

	BeforeEach(func() {
		var err error
		store, err = Open(filepath.Join(GinkgoT().TempDir(), "rowkeys.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for key, k := range entries {
				if err := tx.PutRow("widget", key, []byte("v"), 1, storage.Actor{}); err != nil {
					return err
				}
				if err := tx.PutIndexEntries("widget", key, []storage.IndexEntry{{Index: "by_label", Key: k}}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	})

	It("stores the row key as a length only when the entry ends with it, and as the row's id otherwise", func() {
		rows, err := store.read.Query(`
			SELECT e.k, typeof(e.key), e.key <= 0, r.key
			  FROM index_entries e
			  LEFT JOIN index_row_keys r ON r.id = e.key`)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		forms := map[string]string{}
		for rows.Next() {
			var (
				k      []byte
				typ    string
				length bool
				byID   sql.NullString
			)
			Expect(rows.Scan(&k, &typ, &length, &byID)).To(Succeed())
			Expect(typ).To(Equal("integer"))
			if length {
				forms[string(k)] = "length"
			} else {
				Expect(byID.Valid).To(BeTrue(), "an id names a stored row key")
				forms[string(k)] = "id:" + byID.String
			}
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(forms).To(Equal(map[string]string{
			string(entries["plain"]):            "length",
			string(entries["nul\u0000key"]):     "id:nul\u0000key",
			string(entries["astral\U0001f600"]): "id:astral\U0001f600",
			string(entries["elsewhere"]):        "id:elsewhere",
		}))
	})

	It("scans an index as a range of its primary key with no sort, in both directions", func() {
		for _, o := range []storage.ScanOrder{storage.ScanAsc, storage.ScanDesc} {
			for _, bounds := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
				args := []any{"ws1", "widget", "by_label"}
				if bounds[0] {
					args = append(args, []byte("b"))
				}
				if bounds[1] {
					args = append(args, []byte("d"))
				}
				args = append(args, -1)
				rows, err := store.read.Query(`EXPLAIN QUERY PLAN `+scanIndexSQL(bounds[0], bounds[1], o), args...)
				Expect(err).NotTo(HaveOccurred())
				var plan []string
				for rows.Next() {
					var (
						id, parent, unused int
						detail             string
					)
					Expect(rows.Scan(&id, &parent, &unused, &detail)).To(Succeed())
					plan = append(plan, detail)
				}
				Expect(rows.Err()).NotTo(HaveOccurred())
				Expect(rows.Close()).To(Succeed())
				desc := fmt.Sprintf("order %v, bounds %v: %v", o, bounds, plan)
				Expect(plan).To(ContainElement(HavePrefix("SEARCH index_entries USING PRIMARY KEY (scope=?")), desc)
				Expect(plan).NotTo(ContainElement(ContainSubstring("TEMP B-TREE")), desc)
			}
		}
	})

	It("scans back the row key of every entry in both directions", func() {
		Expect(scan(storage.ScanAsc)).To(Equal(order))
		Expect(scan(storage.ScanDesc)).To(Equal([]string{order[3], order[2], order[1], order[0]}))
	})

	It("deletes every entry with its row", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for key := range entries {
				if err := tx.DeleteIndexEntries("widget", key); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		Expect(scan(storage.ScanAsc)).To(BeEmpty())
		var n int
		Expect(store.read.QueryRow(`SELECT COUNT(*) FROM index_entries`).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero())
	})

	It("replaces an entry that moves between the two forms", func() {
		// The old entry must be found under the form it was stored in, or the
		// replace leaves it behind next to the new one.
		moved := []byte("e\x00\x00plain\x00\x00")
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := tx.PutIndexEntries("widget", "plain", []storage.IndexEntry{{Index: "by_label", Key: []byte("z")}}); err != nil {
				return err
			}
			return tx.PutIndexEntries("widget", "plain", []storage.IndexEntry{{Index: "by_label", Key: moved}})
		})).To(Succeed())
		entries["plain"] = moved
		DeferCleanup(func() { entries["plain"] = []byte("a\x00\x00plain\x00\x00") })
		Expect(scan(storage.ScanAsc)).To(Equal([]string{"nul\u0000key", "astral\U0001f600", "elsewhere", "plain"}))
	})
})

var _ = Describe("byte-equal index entries of different rows", func() {
	var store *Store
	ctx := context.Background()

	// The entry a search term posting has: it does not end with the row key,
	// so every row that holds the term holds these same bytes.
	term := []byte("\x01term\x00\x00")

	put := func(keys ...string) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for _, key := range keys {
				if err := tx.PutIndexEntries("widget", key, []storage.IndexEntry{{Index: "__search", Key: term}}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}
	del := func(keys ...string) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for _, key := range keys {
				if err := tx.DeleteIndexEntries("widget", key); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}
	scan := func(o storage.ScanOrder) []string {
		GinkgoHelper()
		var keys []string
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			return tx.ScanIndex("widget", "__search", nil, nil, 0, o, func(entry []byte, rowKey string) error {
				Expect(entry).To(Equal(term))
				keys = append(keys, rowKey)
				return nil
			})
		})).To(Succeed())
		return keys
	}
	listID := func(key string) int64 {
		GinkgoHelper()
		var id int64
		Expect(store.read.QueryRow(
			`SELECT id FROM index_row_entries WHERE workspace_id = 'ws1' AND entity = 'widget' AND key = ?`, key,
		).Scan(&id)).To(Succeed())
		return id
	}

	BeforeEach(func() {
		var err error
		store, err = Open(filepath.Join(GinkgoT().TempDir(), "equal.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		put("row-a", "row-b")
	})

	It("scans back each row's own entry, the descending walk the mirror of the ascending one", func() {
		asc := scan(storage.ScanAsc)
		Expect(asc).To(ConsistOf("row-a", "row-b"))
		Expect(scan(storage.ScanDesc)).To(Equal([]string{asc[1], asc[0]}))
	})

	It("limits a scan to whole entries in the order of the walk", func() {
		asc := scan(storage.ScanAsc)
		for _, o := range []storage.ScanOrder{storage.ScanAsc, storage.ScanDesc} {
			var keys []string
			Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
				return tx.ScanIndex("widget", "__search", nil, nil, 1, o, func(_ []byte, rowKey string) error {
					keys = append(keys, rowKey)
					return nil
				})
			})).To(Succeed())
			if o == storage.ScanAsc {
				Expect(keys).To(Equal(asc[:1]))
			} else {
				Expect(keys).To(Equal(asc[1:]))
			}
		}
	})

	It("removes each entry with its own row", func() {
		del("row-a")
		Expect(scan(storage.ScanAsc)).To(Equal([]string{"row-b"}))
		del("row-b")
		Expect(scan(storage.ScanAsc)).To(BeEmpty())
		var n int
		Expect(store.read.QueryRow(`SELECT COUNT(*) FROM index_entries`).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero())
	})

	It("does not give a new row the id of a deleted row", func() {
		// The scan puts the id of row-b in the Store's cache, which must not
		// make a later row with a different key read back as row-b.
		Expect(scan(storage.ScanAsc)).To(ConsistOf("row-a", "row-b"))
		freed := listID("row-b")
		Expect(freed).To(BeNumerically(">", listID("row-a")), "row-b holds the greatest id")
		del("row-b")
		put("row-c")
		Expect(listID("row-c")).To(BeNumerically(">", freed))
		Expect(scan(storage.ScanAsc)).To(ConsistOf("row-a", "row-c"))
		Expect(scan(storage.ScanDesc)).To(ConsistOf("row-a", "row-c"))
	})
})

var _ = Describe("row key cache", func() {
	var store *Store
	ctx := context.Background()

	term := []byte("\x01term\x00\x00")

	putTerm := func(tx storage.WriteTx, keys ...string) error {
		for _, key := range keys {
			if err := tx.PutIndexEntries("widget", key, []storage.IndexEntry{{Index: "__search", Key: term}}); err != nil {
				return err
			}
		}
		return nil
	}
	scanTx := func(tx storage.ReadTx) ([]string, error) {
		var keys []string
		err := tx.ScanIndex("widget", "__search", nil, nil, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
			keys = append(keys, rowKey)
			return nil
		})
		return keys, err
	}
	scan := func() []string {
		GinkgoHelper()
		var keys []string
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			var err error
			keys, err = scanTx(tx)
			return err
		})).To(Succeed())
		return keys
	}
	listID := func(key string) int64 {
		GinkgoHelper()
		var id int64
		Expect(store.read.QueryRow(
			`SELECT id FROM index_row_entries WHERE workspace_id = 'ws1' AND entity = 'widget' AND key = ?`, key,
		).Scan(&id)).To(Succeed())
		return id
	}
	cached := func() int {
		n := 0
		for i := range store.rowKeys.shards {
			sh := &store.rowKeys.shards[i]
			sh.mu.RLock()
			n += len(sh.m)
			sh.mu.RUnlock()
		}
		return n
	}

	BeforeEach(func() {
		var err error
		store, err = Open(filepath.Join(GinkgoT().TempDir(), "rowkeycache.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	})

	It("learns the ids a read transaction resolves", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return putTerm(tx, "row-a", "row-b")
		})).To(Succeed())
		Expect(cached()).To(BeZero())
		Expect(scan()).To(Equal([]string{"row-a", "row-b"}))
		key, ok := store.rowKeys.get(listID("row-b"))
		Expect(ok).To(BeTrue())
		Expect(key).To(Equal("row-b"))
	})

	It("does not learn an id that a rolled-back transaction gave out", func() {
		rollback := errors.New("roll back")
		var lost int64
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := putTerm(tx, "row-lost"); err != nil {
				return err
			}
			keys, err := scanTx(tx)
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"row-lost"}))
			Expect(tx.(*sqliteTx).queryRow(
				`SELECT id FROM index_row_entries WHERE key = 'row-lost'`,
			).Scan(&lost)).To(Succeed())
			return rollback
		})).To(MatchError(rollback))
		_, ok := store.rowKeys.get(lost)
		Expect(ok).To(BeFalse(), "a write transaction must not teach the cache")

		// The rollback also rolled back the sequence, so the next row gets
		// the same id under a different key.
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return putTerm(tx, "row-kept")
		})).To(Succeed())
		Expect(listID("row-kept")).To(Equal(lost))
		Expect(scan()).To(Equal([]string{"row-kept"}))
	})

	It("resolves an id from the cache in a write transaction", func() {
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return putTerm(tx, "row-a")
		})).To(Succeed())
		Expect(scan()).To(Equal([]string{"row-a"}))
		// A key no read can return shows which source answered.
		_, err := store.write.Exec(`UPDATE index_row_keys SET key = 'changed'`)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := putTerm(tx, "row-b"); err != nil {
				return err
			}
			keys, err := scanTx(tx)
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"row-a", "row-b"}), "a hit for row-a, a read of this transaction's row-b")
			return nil
		})).To(Succeed())
		_, ok := store.rowKeys.get(listID("row-b"))
		Expect(ok).To(BeFalse())
	})

	It("scans back every row while a small cache clears its shards", func() {
		store.rowKeys = newRowKeyCache(1)
		keys := make([]string, 100)
		for i := range keys {
			keys[i] = fmt.Sprintf("row-%03d", i)
		}
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return putTerm(tx, keys...)
		})).To(Succeed())
		for range 3 {
			Expect(scan()).To(Equal(keys))
			Expect(cached()).To(BeNumerically("<=", rowKeyCacheShards))
		}
	})
})
