package pgstore_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

var _ = Describe("Postgres cursor-state retention", func() {
	It("keeps an entry that a Get reads while it ages, and drops one that no Get reads", func() {
		// Given a store with two entries, read and unread
		ctx := context.Background()
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		conn, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
		age := func(days int) {
			GinkgoHelper()
			_, err := conn.Exec(ctx, `UPDATE cursor_state SET written_at = written_at - make_interval(days => $1)`, days)
			Expect(err).NotTo(HaveOccurred())
		}
		cs := store.CursorStateStore()
		read, unread := cursorstate.Ref{ID: 1, Gen: 1}, cursorstate.Ref{ID: 2, Gen: 1}
		Expect(cs.Put(ctx, "ws", read, []byte("read"))).To(Succeed())
		Expect(cs.Put(ctx, "ws", unread, []byte("unread"))).To(Succeed())

		// When both age by 29 days, one is read, and both age by 2 more days
		// before another entry is written
		age(29)
		_, err = cs.Get(ctx, "ws", read)
		Expect(err).NotTo(HaveOccurred())
		age(2)
		Expect(cs.Put(ctx, "ws", cursorstate.Ref{ID: 3, Gen: 1}, []byte("new"))).To(Succeed())

		// Then the store keeps the entry it read, and has dropped the other
		got, err := cs.Get(ctx, "ws", read)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]byte("read")))
		_, err = cs.Get(ctx, "ws", unread)
		Expect(err).To(MatchError(cursorstate.ErrNotFound))
	})
})
