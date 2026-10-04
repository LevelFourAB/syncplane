package pgstore

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// cursor_test.go pins the rows a scan returns; these specs reach inside for how
// it reads them, because a scan that declared a cursor for every unbounded read
// returns the same rows and only costs two more round trips each time.
var _ = Describe("Postgres scan reads", func() {
	ctx := context.Background()
	const ws = "ws"

	openSeeded := func(n int) *Store {
		GinkgoHelper()
		store, err := Open(ctx, pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := 0; i < n; i++ {
				seq, err := tx.NextSeq()
				if err != nil {
					return err
				}
				if err := tx.PutRow("issue", fmt.Sprintf("i%04d", i), []byte("v"), seq,
					storage.Actor{}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		return store
	}

	DescribeTable("an unbounded scan declares a cursor only when its result passes one batch",
		func(n, cursors int) {
			store := openSeeded(n)
			Expect(store.WithReadTx(ctx, ws, func(rtx storage.ReadTx) error {
				t := rtx.(readOnlyTx).ReadTx.(*pgTx)
				var seen int
				if err := t.ScanEntity("issue", func(storage.Row) error {
					seen++
					return nil
				}); err != nil {
					return err
				}
				Expect(seen).To(Equal(n))
				Expect(t.cursorSeq).To(Equal(cursors))

				// The unnamed portal of this count is in the view too.
				var open int
				Expect(t.conn.QueryRow(ctx, `SELECT count(*) FROM pg_cursors WHERE name <> ''`).
					Scan(&open)).To(Succeed())
				Expect(open).To(BeZero(), "a scan must close the cursor it declared")
				return nil
			})).To(Succeed())
		},
		Entry("one batch", scanBatch, 0),
		Entry("one row more", scanBatch+1, 1),
	)

	It("reports a cursor that fails to open before any row reaches the callback", func() {
		store := openSeeded(0)
		// The direct read accepts a data-modifying WITH and DECLARE refuses it,
		// so the first read returns more than a batch and the DECLARE fails.
		const query = `WITH d AS (DELETE FROM rows WHERE false RETURNING 1)
		               SELECT g FROM generate_series(1, 300) AS g ORDER BY g LIMIT $1`
		var scanErr error
		err := store.WithWriteTx(ctx, ws, func(wtx storage.WriteTx) error {
			t := wtx.(*pgTx)
			var yielded int
			scanErr = scanRows(t, query, []any{limitArg(0)}, 0,
				func(rows pgx.Rows) (int, error) {
					var g int
					return g, rows.Scan(&g)
				},
				func(int) error {
					yielded++
					return nil
				})
			Expect(yielded).To(BeZero())
			Expect(t.failed).To(MatchError(scanErr))
			return scanErr
		})
		var pgErr *pgconn.PgError
		Expect(errors.As(scanErr, &pgErr)).To(BeTrue(), "got %v", scanErr)
		Expect(pgErr.Code).To(Equal("0A000")) // feature_not_supported
		Expect(err).To(MatchError(scanErr))
	})
})
