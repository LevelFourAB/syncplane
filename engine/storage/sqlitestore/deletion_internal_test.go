package sqlitestore

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("workspace deletion coverage", func() {
	It("empties every table that has a workspace_id column", func() {
		// A table a migration adds is invisible to DeleteWorkspace until it is
		// named in workspaceTables, and the conformance suite only checks the
		// state it knows to write. Reading the schema is what catches the
		// table nobody remembered.
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "tables.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		rows, err := store.read.Query(`
			SELECT m.name FROM sqlite_master m, pragma_table_info(m.name) c
			WHERE m.type = 'table' AND c.name = 'workspace_id'`)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		var tables []string
		for rows.Next() {
			var name string
			Expect(rows.Scan(&name)).To(Succeed())
			tables = append(tables, name)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())

		Expect(tables).To(ConsistOf(append([]string{"deleted_workspaces"}, workspaceTables...)))
	})

	Describe("index entries, which are keyed by scope rather than by workspace", func() {
		var store *Store
		ctx := context.Background()

		count := func(table string) int {
			GinkgoHelper()
			var n int
			Expect(store.read.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n)).To(Succeed())
			return n
		}
		// dangling counts the entries that name a row id no row holds any
		// more.
		dangling := func() int {
			GinkgoHelper()
			var n int
			Expect(store.read.QueryRow(`
				SELECT COUNT(*) FROM index_entries e
				 WHERE e.key > 0 AND NOT EXISTS (SELECT 1 FROM index_row_keys r WHERE r.id = e.key)`,
			).Scan(&n)).To(Succeed())
			return n
		}

		BeforeEach(func() {
			var err error
			store, err = Open(filepath.Join(GinkgoT().TempDir(), "entries.db"))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				for _, k := range []string{"k1", "k2"} {
					if err := tx.PutRow("widget", k, []byte("v"), 1, storage.Actor{}); err != nil {
						return err
					}
					if err := tx.PutIndexEntries("widget", k, []storage.IndexEntry{
						{Index: "by_label", Key: []byte(k)},
						{Index: "by_owner", Key: []byte(k)},
					}); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
			Expect(count("index_entries")).To(Equal(4))
			Expect(count("index_row_ids")).To(Equal(2))
			Expect(count("index_row_entries")).To(Equal(2))
			Expect(count("index_row_keys")).To(Equal(2))
		})

		It("leaves none behind when the workspace is deleted", func() {
			Expect(store.DeleteWorkspace(ctx, "ws1")).To(Succeed())
			Expect(count("index_entries")).To(BeZero())
			Expect(count("index_row_ids")).To(BeZero())
			Expect(count("index_row_entries")).To(BeZero())
			Expect(count("index_row_keys")).To(BeZero())
			Expect(count("index_scopes")).To(BeZero())
		})

		It("keeps the lists and ids of another workspace", func() {
			Expect(store.WithWriteTx(ctx, "ws2", func(tx storage.WriteTx) error {
				return tx.PutIndexEntries("widget", "k1", []storage.IndexEntry{{Index: "by_label", Key: []byte("k1")}})
			})).To(Succeed())
			Expect(store.DeleteWorkspace(ctx, "ws1")).To(Succeed())
			Expect(count("index_entries")).To(Equal(1))
			Expect(count("index_row_ids")).To(Equal(1))
			Expect(count("index_row_entries")).To(Equal(1))
			Expect(count("index_row_keys")).To(Equal(1))
			Expect(store.WithWriteTx(ctx, "ws2", func(tx storage.WriteTx) error {
				return tx.DeleteIndexEntries("widget", "k1")
			})).To(Succeed())
			Expect(count("index_entries")).To(BeZero())
			Expect(count("index_row_entries")).To(BeZero())
		})

		It("leaves none behind when the rows are purged", func() {
			Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				_, _, err := tx.PurgeEntityRows("widget", "", time.Now().Add(time.Hour), 1)
				return err
			})).To(Succeed())
			Expect(count("index_entries")).To(Equal(2), "a limited purge drops the entries of the rows it purged")
			Expect(count("index_row_ids")).To(Equal(1))
			Expect(count("index_row_entries")).To(Equal(1))
			Expect(count("index_row_keys")).To(Equal(1))
			Expect(dangling()).To(BeZero())
		})

		It("leaves none behind when a row's entries are deleted", func() {
			Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
				return tx.DeleteIndexEntries("widget", "k1")
			})).To(Succeed())
			Expect(count("index_entries")).To(Equal(2))
			Expect(count("index_row_ids")).To(Equal(1))
			Expect(count("index_row_entries")).To(Equal(1))
			Expect(count("index_row_keys")).To(Equal(1))
			Expect(dangling()).To(BeZero())
		})
	})
})
