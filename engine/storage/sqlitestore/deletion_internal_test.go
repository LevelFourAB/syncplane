package sqlitestore

import (
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
})
