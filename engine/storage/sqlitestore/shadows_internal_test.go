package sqlitestore

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("consumer shadows", func() {
	ctx := context.Background()

	It("writes a shadow again with the same or a new snapshot, and gives each consumer one scope", func() {
		store, err := Open(filepath.Join(GinkgoT().TempDir(), "shadows.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			for _, put := range []struct {
				consumer, key string
				snapshot      []byte
			}{
				{"a", "k1", []byte{1}},
				{"a", "k1", []byte{1}},
				{"a", "k1", nil},
				{"a", "k2", []byte{2}},
				{"b", "k1", []byte{3}},
			} {
				if err := tx.PutConsumerShadow(put.consumer, "widget", put.key, put.snapshot); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		Expect(store.WithWriteTx(ctx, "ws1", func(tx storage.WriteTx) error {
			if err := tx.DeleteConsumer("a"); err != nil {
				return err
			}
			return tx.PutConsumerShadow("a", "widget", "k1", []byte{4})
		})).To(Succeed())

		Expect(store.WithReadTx(ctx, "ws1", func(tx storage.ReadTx) error {
			for _, want := range []struct {
				consumer, key string
				snapshot      []byte
			}{
				{"a", "k1", []byte{4}},
				{"a", "k2", nil},
				{"b", "k1", []byte{3}},
			} {
				got, err := tx.ConsumerShadow(want.consumer, "widget", want.key)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(want.snapshot), "%s/%s", want.consumer, want.key)
			}
			return nil
		})).To(Succeed())
		var scopes int
		Expect(store.read.QueryRow(`SELECT COUNT(*) FROM consumer_scopes`).Scan(&scopes)).To(Succeed())
		Expect(scopes).To(Equal(2))
	})
})
