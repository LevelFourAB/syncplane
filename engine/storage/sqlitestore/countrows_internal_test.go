package sqlitestore

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("CountRows", func() {
	It("counts from a covering index and does not read the rows table", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "count.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(context.Background(), "ws1", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "a", []byte("1"), 1, storage.Actor{})
		})).To(Succeed())

		rows, err := store.read.Query(`EXPLAIN QUERY PLAN `+countRowsSQL, "ws1", "widget")
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
		Expect(plan).To(ContainElement(ContainSubstring("USING COVERING INDEX")), "%v", plan)
	})
})
