package pgstore_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// unwritableKey is a key Postgres refuses: a text column cannot hold a NUL, and
// pgx encodes the string without noticing, so the statement fails at execution
// rather than in front of it. That is what puts the failure INSIDE the batch,
// which is the case two of the specs below exist for.
var unwritableKey = "bad" + string(rune(0)) + "key"

// A write transaction holds its write-only statements back and sends them as
// one batch (see queue.go). The conformance suite pins what a caller may
// observe; these specs pin the two things only this adapter can get wrong — a
// savepoint rewound in the queue rather than on the server, and a statement
// that fails long after the call that wrote it.
//
// Every store here caps the pool at one connection. A batch whose results were
// not drained leaves the connection unusable, and with one connection the next
// transaction blocks on it instead of quietly taking a fresh one.
var _ = Describe("Postgres write batching", func() {
	ctx := context.Background()
	const ws = "ws"

	oneConn := func() *pgstore.Store {
		GinkgoHelper()
		store := open(pgstore.WithMaxConns(1))
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		return store
	}

	keys := func(store *pgstore.Store, entity string) []string {
		GinkgoHelper()
		out := []string{}
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			return tx.ScanEntity(entity, func(r storage.Row) error {
				out = append(out, r.Key)
				return nil
			})
		})).To(Succeed())
		return out
	}

	It("discards exactly a rolled-back savepoint's writes and keeps the rest", func() {
		// The rewind that costs nothing: nothing has been sent, so the undo is
		// the queue's own truncation. What surrounds the savepoint has to
		// survive it on both sides.
		store := oneConn()
		boom := errors.New("boom")
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(tx.PutRow("widget", "before", []byte("v"), 1, storage.Actor{})).To(Succeed())
			Expect(tx.InSavepoint(func() error {
				Expect(tx.PutRow("widget", "doomed-a", []byte("v"), 2, storage.Actor{})).To(Succeed())
				Expect(tx.PutRow("widget", "doomed-b", []byte("v"), 3, storage.Actor{})).To(Succeed())
				Expect(tx.SetClientLastMutationID("c1", 5)).To(Succeed())
				return boom
			})).To(MatchError(boom))
			Expect(tx.PutRow("widget", "after", []byte("v"), 4, storage.Actor{})).To(Succeed())
			return nil
		})).To(Succeed())

		Expect(keys(store, "widget")).To(Equal([]string{"after", "before"}))
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			Expect(tx.ClientLastMutationID("c1")).To(BeZero())
			return nil
		})).To(Succeed())
	})

	It("rolls a savepoint back on the server once a read has sent it", func() {
		// The other half of the same rewind. The read inside fn sends the
		// savepoint and the write before it, so the undo has to be a ROLLBACK TO
		// — and must reach the same place the truncation would have.
		store := oneConn()
		boom := errors.New("boom")
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(tx.PutRow("widget", "before", []byte("v"), 1, storage.Actor{})).To(Succeed())
			Expect(tx.InSavepoint(func() error {
				if err := tx.PutRow("widget", "doomed", []byte("v"), 2, storage.Actor{}); err != nil {
					return err
				}
				// Forces the send: a read has to see this transaction's writes.
				row, err := tx.GetRow("widget", "doomed")
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Value).To(Equal([]byte("v")))
				return boom
			})).To(MatchError(boom))
			// The rewind is visible immediately, which takes another send.
			_, err := tx.GetRow("widget", "doomed")
			Expect(err).To(MatchError(storage.ErrNotFound))
			Expect(tx.PutRow("widget", "after", []byte("v"), 3, storage.Actor{})).To(Succeed())
			return nil
		})).To(Succeed())

		Expect(keys(store, "widget")).To(Equal([]string{"after", "before"}))
	})

	It("keeps an outer savepoint's writes when the inner one rolls back", func() {
		store := oneConn()
		boom := errors.New("inner")
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.InSavepoint(func() error {
				if err := tx.PutRow("widget", "outer", []byte("v"), 1, storage.Actor{}); err != nil {
					return err
				}
				Expect(tx.InSavepoint(func() error {
					Expect(tx.PutRow("widget", "inner", []byte("v"), 2, storage.Actor{})).To(Succeed())
					return boom
				})).To(MatchError(boom))
				// A second savepoint at the same depth: the first one's rewind
				// must leave that depth usable.
				return tx.InSavepoint(func() error {
					return tx.PutRow("widget", "inner-2", []byte("v"), 3, storage.Actor{})
				})
			})
		})).To(Succeed())

		Expect(keys(store, "widget")).To(Equal([]string{"inner-2", "outer"}))
	})

	It("discards an inner savepoint's writes when the outer one rolls back", func() {
		// The reverse nesting: the inner savepoint succeeded and is still
		// deferred when the outer one rewinds over it.
		store := oneConn()
		boom := errors.New("outer")
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(tx.InSavepoint(func() error {
				Expect(tx.InSavepoint(func() error {
					return tx.PutRow("widget", "inner", []byte("v"), 1, storage.Actor{})
				})).To(Succeed())
				Expect(tx.PutRow("widget", "outer", []byte("v"), 2, storage.Actor{})).To(Succeed())
				return boom
			})).To(MatchError(boom))
			return tx.PutRow("widget", "kept", []byte("v"), 3, storage.Actor{})
		})).To(Succeed())

		Expect(keys(store, "widget")).To(Equal([]string{"kept"}))
	})

	It("commits a savepoint holding more statements than one batch carries", func() {
		// Past the queue's cap the batch goes out mid-savepoint, which is the
		// path where a later rewind can no longer be the queue's own. Nothing
		// about the committed result may depend on where the cap fell.
		store := oneConn()
		const rows = 2500
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.InSavepoint(func() error {
				for i := 0; i < rows; i++ {
					seq, err := tx.NextSeq()
					if err != nil {
						return err
					}
					if err := tx.PutRow("widget", fmt.Sprintf("k%04d", i), []byte("v"), seq, storage.Actor{}); err != nil {
						return err
					}
				}
				return nil
			})
		})).To(Succeed())

		Expect(keys(store, "widget")).To(HaveLen(rows))
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			Expect(tx.CurrentSeq()).To(BeEquivalentTo(rows))
			return nil
		})).To(Succeed())
	})

	It("fails the transaction when a batched statement fails", func() {
		// The failure reaches the caller through whatever sent the batch, here
		// the commit — later than the call that wrote it, and to the same end.
		store := oneConn()
		err := store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			if err := tx.PutRow("widget", "good", []byte("v"), 1, storage.Actor{}); err != nil {
				return err
			}
			return tx.PutRow("widget", unwritableKey, []byte("v"), 2, storage.Actor{})
		})
		Expect(err).To(HaveOccurred())

		Expect(keys(store, "widget")).To(BeEmpty(), "the whole transaction rolled back")

		// And the store is still usable: the failed batch's results were
		// drained, so its connection went back to the pool intact.
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "later", []byte("v"), 1, storage.Actor{})
		})).To(Succeed())
		Expect(keys(store, "widget")).To(Equal([]string{"later"}))
	})

	It("reports a batched statement's failure from the read that sends it", func() {
		// The same failure, observed earlier: a read after the bad write sends
		// the batch, so the error surfaces from the read rather than from the
		// commit. Either way nothing of the transaction persists.
		store := oneConn()
		var readErr error
		err := store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			if err := tx.PutRow("widget", unwritableKey, []byte("v"), 1, storage.Actor{}); err != nil {
				return err
			}
			_, readErr = tx.GetRow("widget", "anything")
			return readErr
		})
		Expect(readErr).To(HaveOccurred())
		Expect(readErr).NotTo(MatchError(storage.ErrNotFound))
		Expect(err).To(MatchError(readErr))

		Expect(keys(store, "widget")).To(BeEmpty())
	})
})
