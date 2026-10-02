package pgstore_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// The workspace's position scalars and index states are read once per
// transaction and kept, so every way the transaction can move them under
// itself has to drop what it kept. The conformance suite covers the forward
// moves; a savepoint rewind is the one that moves them BACKWARD, and only this
// adapter has it.
var _ = Describe("Postgres workspace position", func() {
	ctx := context.Background()
	const ws = "ws"

	It("forgets a store epoch a savepoint rolled back", func() {
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		boom := errors.New("boom")
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(tx.StoreEpoch()).To(BeZero())
			Expect(tx.InSavepoint(func() error {
				if err := tx.SetClientLastMutationID("c1", 1); err != nil {
					return err
				}
				Expect(tx.StoreEpoch()).NotTo(BeZero(), "the write registered the workspace")
				return boom
			})).To(MatchError(boom))
			// The rewind took the workspaces row with it, and with the row the
			// epoch that was minted on it.
			Expect(tx.StoreEpoch()).To(BeZero())
			return nil
		})).To(Succeed())

		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			Expect(tx.StoreEpoch()).To(BeZero())
			return nil
		})).To(Succeed())
	})

	It("forgets an index state a savepoint rolled back", func() {
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.SetIndexState("widget", "by_label", "old", true)
		})).To(Succeed())

		boom := errors.New("boom")
		state := func(tx storage.ReadTx) []any {
			GinkgoHelper()
			hash, ready, err := tx.IndexState("widget", "by_label")
			Expect(err).NotTo(HaveOccurred())
			return []any{hash, ready}
		}
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(state(tx)).To(Equal([]any{"old", true}))
			Expect(tx.InSavepoint(func() error {
				if err := tx.SetIndexState("widget", "by_label", "new", false); err != nil {
					return err
				}
				Expect(state(tx)).To(Equal([]any{"new", false}), "a write must see its own index state")
				return boom
			})).To(MatchError(boom))
			Expect(state(tx)).To(Equal([]any{"old", true}), "the rewind took the new state with it")
			return nil
		})).To(Succeed())
	})
})
