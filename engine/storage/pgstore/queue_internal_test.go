package pgstore

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// queue_test.go pins what the write-behind queue must produce; these two specs
// reach inside for the thing that produces it, because both rewinds commit the
// same rows and nothing a caller can see tells them apart. A savepoint that
// stopped truncating and started sending a ROLLBACK TO would still pass every
// spec out there — and would have given back the round trips this exists for.
var _ = Describe("Postgres write queue", func() {
	ctx := context.Background()
	const ws = "ws"

	// inWriteTx runs fn with the concrete transaction, which is what the store
	// hands the closure anyway.
	inWriteTx := func(fn func(t *pgTx) error) {
		GinkgoHelper()
		store, err := Open(ctx, pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return fn(tx.(*pgTx))
		})).To(Succeed())
	}

	It("sends one batch for a run of savepointed writes", func() {
		// The shape of a push: a savepoint per mutation, a row and its index
		// entries inside each, and nothing read in between. Only NextSeq's first
		// allocation has to reach the server before the commit.
		inWriteTx(func(t *pgTx) error {
			for i := 0; i < 20; i++ {
				if err := t.InSavepoint(func() error {
					seq, err := t.NextSeq()
					if err != nil {
						return err
					}
					key := fmt.Sprintf("k%02d", i)
					if err := t.PutRow("widget", key, []byte("v"), seq, storage.Actor{}); err != nil {
						return err
					}
					return t.PutIndexEntries("widget", key,
						[]storage.IndexEntry{{Index: "by_v", Key: []byte("v")}})
				}); err != nil {
					return err
				}
			}
			Expect(t.sends).To(Equal(1), "the sequence allocation is the only read")
			return nil
		})
	})

	It("rewinds a savepoint without a round trip when nothing has been sent", func() {
		boom := errors.New("boom")
		inWriteTx(func(t *pgTx) error {
			// Get the one unavoidable read out of the way, so what follows is
			// measured against a queue that is purely write-behind.
			if _, err := t.NextSeq(); err != nil {
				return err
			}
			if err := t.PutRow("widget", "before", []byte("v"), 1, storage.Actor{}); err != nil {
				return err
			}
			queued, sends := len(t.queued), t.sends

			Expect(t.InSavepoint(func() error {
				Expect(t.PutRow("widget", "doomed", []byte("v"), 2, storage.Actor{})).To(Succeed())
				Expect(t.DeleteIndexEntries("widget", "doomed")).To(Succeed())
				return boom
			})).To(MatchError(boom))

			Expect(t.sends).To(Equal(sends), "the rewind must not talk to the server")
			Expect(t.queued).To(HaveLen(queued),
				"and must drop its own entries exactly, keeping what preceded it")
			return nil
		})
	})
})
