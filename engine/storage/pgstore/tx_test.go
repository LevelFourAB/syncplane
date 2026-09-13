package pgstore_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// The workspace's position scalars are read once per transaction and kept, so
// every way the transaction can move them under itself has to drop what it
// kept. The conformance suite covers the forward moves; a savepoint rewind is
// the one that moves them BACKWARD, and only this adapter has it.
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
})
