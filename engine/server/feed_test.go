package server_test

import (
	"context"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Server.Changes", func() {
	var (
		ctx context.Context
		srv *server.Server
	)
	const ws = "ws1"

	push := func(m *syncplanev1.Mutation) *syncplanev1.PushResponse {
		GinkgoHelper()
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(
			ws,
			[]*syncplanev1.Mutation{m})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
		return resp.Msg
	}

	drain := func(after server.Cursor, limit int) ([]server.Change, server.Cursor, bool) {
		GinkgoHelper()
		var got []server.Change
		next, more, err := srv.Changes(ctx, ws, after, limit, func(ch server.Change) error {
			got = append(got, ch)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		return got, next, more
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("draining", func() {
		BeforeEach(func() {
			srv = newTestServer()
		})

		It("yields committed changes in order with decoded values and tombstones", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
			push(deleteMutation("c1", 3, "todo", "t1"))

			got, next, more := drain(server.Cursor{}, 0)
			Expect(more).To(BeFalse())
			// Only the latest state of each row is observable: t1's put at
			// seq 1 was overwritten by its tombstone at seq 3.
			Expect(got).To(HaveLen(2))

			Expect(got[0].Entity).To(Equal("todo"))
			Expect(got[0].Key).To(Equal("t2"))
			Expect(got[0].Seq).To(BeEquivalentTo(2))
			Expect(got[0].Deleted).To(BeFalse())
			todo, ok := got[0].Message.(*todov1.Todo)
			Expect(ok).To(BeTrue())
			Expect(todo.GetTitle()).To(Equal("two"))

			Expect(got[1].Key).To(Equal("t1"))
			Expect(got[1].Seq).To(BeEquivalentTo(3))
			Expect(got[1].Deleted).To(BeTrue())
			Expect(got[1].Message).To(BeNil())

			Expect(next).To(Equal(server.Cursor{Seq: 3}))
		})

		It("resumes from a returned cursor and sees only newer changes", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			_, next, _ := drain(server.Cursor{}, 0)

			got, _, _ := drain(next, 0)
			Expect(got).To(BeEmpty())

			push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
			got, next, more := drain(next, 0)
			Expect(more).To(BeFalse())
			Expect(got).To(HaveLen(1))
			Expect(got[0].Key).To(Equal("t2"))
			Expect(next.Seq).To(BeEquivalentTo(2))
		})

		It("paginates across a same-sequence batch group without dropping rows", func() {
			push(batchMutation("c1", 1,
				crudPut("todo", "a", &todov1.Todo{Id: "a", Title: "a"}),
				crudPut("todo", "b", &todov1.Todo{Id: "b", Title: "b"}),
				crudPut("todo", "c", &todov1.Todo{Id: "c", Title: "c"}),
			))

			page1, cur, more := drain(server.Cursor{}, 2)
			Expect(more).To(BeTrue())
			Expect(page1).To(HaveLen(2))
			// The cursor splits the seq-1 group: it must carry the
			// sub-position of the last delivered row.
			Expect(cur).To(Equal(server.Cursor{Seq: 1, Entity: "todo", Key: "b"}))

			page2, next, more := drain(cur, 2)
			Expect(more).To(BeFalse())
			Expect(page2).To(HaveLen(1))
			Expect(next).To(Equal(server.Cursor{Seq: 1}))

			var keys []string
			for _, ch := range append(page1, page2...) {
				Expect(ch.Seq).To(BeEquivalentTo(1))
				keys = append(keys, ch.Key)
			}
			Expect(keys).To(ConsistOf("a", "b", "c"))
		})

		It("observes Mutate writes", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: "m1", Title: "via mutate"})
			})).To(Succeed())

			got, _, _ := drain(server.Cursor{}, 0)
			Expect(got).To(HaveLen(1))
			Expect(got[0].Key).To(Equal("m1"))
		})

		It("propagates fn errors", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			boom := func(server.Change) error { return context.Canceled }
			_, _, err := srv.Changes(ctx, ws, server.Cursor{}, 0, boom)
			Expect(err).To(MatchError(context.Canceled))
		})
	})

	Describe("tombstone horizon", func() {
		It("returns ErrCursorTooOld with a resume cursor after GC", func() {
			srv = newTestServer(server.WithGC(-time.Second, 0))
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			push(deleteMutation("c1", 2, "todo", "t1"))
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			var calls int
			next, more, err := srv.Changes(ctx, ws, server.Cursor{}, 0, func(server.Change) error {
				calls++
				return nil
			})
			Expect(err).To(MatchError(server.ErrCursorTooOld))
			Expect(calls).To(BeZero(), "no changes delivered alongside the error")
			Expect(more).To(BeFalse())
			Expect(next.Seq).To(BeEquivalentTo(2), "resume cursor sits at the current seq")

			// After a full resync the consumer resumes from the returned
			// cursor and sees only post-resync changes.
			push(putMutation("c1", 3, "todo", "t2", &todov1.Todo{Id: "t2"}))
			got, _, _ := drain(next, 0)
			Expect(got).To(HaveLen(1))
			Expect(got[0].Key).To(Equal("t2"))
		})

		It("returns ErrCursorTooOld for a cursor ahead of the store (recreated/restored store)", func() {
			// A cursor from a previous, larger incarnation of the store at this
			// position: seq sits above the current sequence, which a
			// per-workspace-monotonic store could never have issued.
			srv = newTestServer()
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			var calls int
			next, more, err := srv.Changes(ctx, ws, server.Cursor{Seq: 86}, 0, func(server.Change) error {
				calls++
				return nil
			})
			Expect(err).To(MatchError(server.ErrCursorTooOld))
			Expect(calls).To(BeZero(), "no changes spliced from a foreign position")
			Expect(more).To(BeFalse())
			Expect(next.Seq).To(BeEquivalentTo(1), "resume cursor sits at the current seq")
		})
	})

	Describe("unregistered entities", func() {
		It("emits the envelope with a nil Message", func() {
			store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "feed.db"))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			srv = mustNewServer(store, server.WithEntities(&todov1.Todo{}))

			Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				seq, err := tx.NextSeq()
				if err != nil {
					return err
				}
				return tx.PutRow("mystery", "k1", []byte{0xde, 0xad}, seq, storage.Actor{})
			})).To(Succeed())

			got, _, _ := drain(server.Cursor{}, 0)
			Expect(got).To(HaveLen(1))
			Expect(got[0].Entity).To(Equal("mystery"))
			Expect(got[0].Message).To(BeNil())
			Expect(got[0].Deleted).To(BeFalse())
		})
	})
})

var _ = Describe("Server.SubscribeChanges", func() {
	const ws = "ws-sub"

	It("signals after Push and Mutate commits, not after read-only calls", func() {
		ctx := context.Background()
		// Coalescing off: every commit signals immediately, so the spec can
		// reason about individual receives.
		srv := newTestServer(server.WithNotifyCoalescing(0))

		ch, cancel := srv.SubscribeChanges(ws)
		defer cancel()

		_, err := srv.Push(ctx, connect.NewRequest(pushOf(
			ws,
			[]*syncplanev1.Mutation{
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
			})))
		Expect(err).NotTo(HaveOccurred())
		Eventually(ch).Should(Receive(), "push commit signals")

		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&todov1.Todo{Id: "t2"})
		})).To(Succeed())
		Eventually(ch).Should(Receive(), "mutate commit signals")

		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			var todo todov1.Todo
			_, err := tx.Get(&todo, "t1")
			return err
		})).To(Succeed())
		Consistently(ch, 100*time.Millisecond).ShouldNot(Receive(), "read-only mutate stays silent")
	})

	It("closes the channel after cancel", func() {
		srv := newTestServer()
		ch, cancel := srv.SubscribeChanges(ws)
		cancel()
		Eventually(ch).Should(BeClosed())
	})
})
