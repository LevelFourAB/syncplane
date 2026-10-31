package pgstore

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
)

var _ = Describe("workspace deletion coverage", func() {
	It("empties every table that has a workspace_id column", func() {
		// A table a migration adds is invisible to DeleteWorkspace until it is
		// named in workspaceTables, and the conformance suite only checks the
		// state it knows to write. Reading the schema is what catches the
		// table nobody remembered.
		ctx := context.Background()
		store, err := Open(ctx, pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		rows, err := store.pool.Query(ctx, `
			SELECT c.table_name FROM information_schema.columns c
			JOIN information_schema.tables t USING (table_schema, table_name)
			WHERE c.table_schema = current_schema() AND c.column_name = 'workspace_id'
			  AND t.table_type = 'BASE TABLE'`)
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
