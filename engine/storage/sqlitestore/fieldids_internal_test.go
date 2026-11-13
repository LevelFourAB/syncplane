package sqlitestore

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("field ids of the delta log", func() {
	ctx := context.Background()

	fieldIDs := func(store *Store) []int64 {
		GinkgoHelper()
		var ids []int64
		rows, err := store.read.Query(`SELECT id FROM field_ids ORDER BY id`)
		Expect(err).NotTo(HaveOccurred())
		for rows.Next() {
			var id int64
			Expect(rows.Scan(&id)).To(Succeed())
			ids = append(ids, id)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(rows.Close()).To(Succeed())
		return ids
	}

	It("deletes the ids that a prune leaves without entries, and never gives an id two times", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "fieldids.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := tx.PutFieldDelta("widget", "k1", "body", 1, []byte{1}); err != nil {
				return err
			}
			return tx.PutFieldDelta("widget", "k2", "body", 2, []byte{2})
		})).To(Succeed())
		Expect(fieldIDs(store)).To(Equal([]int64{1, 2}))

		// Only k1's entry is old enough to prune.
		Expect(store.write.Exec(`UPDATE field_deltas SET updated_at = 1 WHERE field_id = 1`)).Error().NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			_, err := tx.PruneFieldDeltas(time.UnixMilli(1000))
			return err
		})).To(Succeed())
		Expect(fieldIDs(store)).To(Equal([]int64{2}), "k1's id holds no entry")

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			return tx.PutFieldDelta("widget", "k1", "body", 3, []byte{3})
		})).To(Succeed())
		Expect(fieldIDs(store)).To(Equal([]int64{2, 3}), "k1 gets a new id, not its old one")
		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			frags, complete, err := tx.FieldDeltas("widget", "k1", "body", 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(frags).To(Equal([][]byte{{3}}))
			frags, complete, err = tx.FieldDeltas("widget", "k2", "body", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(frags).To(Equal([][]byte{{2}}))
			return nil
		})).To(Succeed())
	})

	It("keeps the id of a field that still holds an entry after a prune", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "fieldids.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := tx.PutFieldDelta("widget", "k1", "body", 1, []byte{1}); err != nil {
				return err
			}
			if err := tx.PutFieldDelta("widget", "k1", "body", 2, []byte{2}); err != nil {
				return err
			}
			return tx.PutFieldDelta("widget", "k2", "body", 3, []byte{3})
		})).To(Succeed())

		// k1 loses its first entry and keeps its second; k2 loses its only one.
		Expect(store.write.Exec(`UPDATE field_deltas SET updated_at = 1 WHERE seq IN (1, 3)`)).Error().NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			horizon, err := tx.PruneFieldDeltas(time.UnixMilli(1000))
			Expect(horizon).To(Equal(uint64(3)))
			return err
		})).To(Succeed())
		Expect(fieldIDs(store)).To(Equal([]int64{1}), "k1 holds an entry, k2 holds none")
	})

	It("deletes the older entries of a field when a gap marker is written", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "fieldids.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for seq, field := range []string{"body", "body", "title", "body"} {
				if err := tx.PutFieldDelta("widget", "k1", field, uint64(seq+1), []byte{byte(seq + 1)}); err != nil {
					return err
				}
			}
			return tx.PutFieldDelta("widget", "k1", "body", 5, nil)
		})).To(Succeed())

		// The marker hides seq 1, 2 and 4 of body from every reader, so they
		// are deleted; title's entry belongs to another field and stays.
		rows, err := store.read.Query(`SELECT seq, length(frag) FROM field_deltas ORDER BY seq`)
		Expect(err).NotTo(HaveOccurred())
		var entries [][2]int64
		for rows.Next() {
			var seq, n int64
			Expect(rows.Scan(&seq, &n)).To(Succeed())
			entries = append(entries, [2]int64{seq, n})
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(rows.Close()).To(Succeed())
		Expect(entries).To(Equal([][2]int64{{3, 1}, {5, 0}}))
	})
})
