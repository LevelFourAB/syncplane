package sqlitestore

import (
	"context"
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

	It("stores the row key as a length only when the entry ends with it", func() {
		rows, err := store.read.Query(`SELECT k, typeof(key) FROM index_entries ORDER BY k`)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		types := map[string]string{}
		for rows.Next() {
			var (
				k   []byte
				typ string
			)
			Expect(rows.Scan(&k, &typ)).To(Succeed())
			types[string(k)] = typ
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(types).To(Equal(map[string]string{
			string(entries["plain"]):            "integer",
			string(entries["nul\u0000key"]):     "text",
			string(entries["astral\U0001f600"]): "text",
			string(entries["elsewhere"]):        "text",
		}))
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
