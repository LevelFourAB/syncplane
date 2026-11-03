package upstream_test

import (
	"context"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/client"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

var _ = Describe("Sync", func() {
	var (
		o   *origin
		n   *local
		ctx context.Context
	)
	BeforeEach(func() {
		ctx = context.Background()
		o = newOrigin()
		n = newLocal(o, nil)
		n.add()
		n.flush()
	})

	It("queues writes while offline and pushes them after reconnecting", func() {
		n.part.Cut()
		Eventually(func() upstream.State { return n.status().State }).Should(Equal(upstream.StateOffline))
		n.put(&todov1.Todo{Id: "t1", Title: "offline one"})
		n.put(&todov1.Todo{Id: "t2", Title: "offline two"})
		Expect(title(n.todo("t1"))).To(Equal("offline one"))
		Eventually(func() int { return n.status().Pending }).Should(Equal(2))
		Expect(o.todo("t1")).To(BeNil())

		n.part.Heal()
		n.flush()
		Expect(title(o.todo("t1"))).To(Equal("offline one"))
		Expect(title(o.todo("t2"))).To(Equal("offline two"))
	})

	It("applies a write once when the response to its push was lost", func() {
		n.part.losePushes.Store(1)
		rc := n.put(&todov1.Todo{Id: "t1", Title: "once"})
		Expect(rc.Wait(ctx)).To(Succeed())
		n.flush()
		Expect(title(o.todo("t1"))).To(Equal("once"))
		Expect(n.rejected()).To(BeEmpty())
		Expect(n.part.losePushes.Load()).To(BeNumerically("<", 0))
	})

	Describe("concurrent edits of one field", func() {
		BeforeEach(func() {
			o.put(&todov1.Todo{Id: "t1", Title: "base"})
			Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("base"))
		})

		It("keeps the write that reached the origin last: the local one", func() {
			n.part.Cut()
			o.patch(&todov1.Todo{Id: "t1", Title: "origin"}, "title")
			n.patch(&todov1.Todo{Id: "t1", Title: "local"}, "title")
			n.part.Heal()
			n.flush()
			Expect(title(o.todo("t1"))).To(Equal("local"))
			Expect(title(n.todo("t1"))).To(Equal("local"))
		})

		It("keeps the write that reached the origin last: the origin's", func() {
			rc := n.patch(&todov1.Todo{Id: "t1", Title: "local"}, "title")
			Expect(rc.Wait(ctx)).To(Succeed())
			o.patch(&todov1.Todo{Id: "t1", Title: "origin"}, "title")
			Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("origin"))
			n.flush()
			Expect(title(o.todo("t1"))).To(Equal("origin"))
		})
	})

	It("merges a local PATCH with a concurrent origin edit of another field", func() {
		o.put(&todov1.Todo{Id: "t1", Title: "base"})
		Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("base"))
		n.part.Cut()
		n.patch(&todov1.Todo{Id: "t1", Title: "renamed"}, "title")
		o.patch(&todov1.Todo{Id: "t1", Completed: true}, "completed")
		n.part.Heal()
		n.flush()
		for _, t := range []*todov1.Todo{n.todo("t1"), o.todo("t1")} {
			Expect(t.GetTitle()).To(Equal("renamed"))
			Expect(t.GetCompleted()).To(BeTrue())
		}
	})

	It("shows a pending local write on top of origin changes until the origin confirms it", func() {
		o.put(&todov1.Todo{Id: "t1", Title: "base"})
		Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("base"))
		n.part.Cut()
		n.patch(&todov1.Todo{Id: "t1", Title: "mine"}, "title")
		// The origin's change arrives while the local write is still pending:
		// the pending title stays on top of it.
		o.patch(&todov1.Todo{Id: "t1", Completed: true}, "completed")
		n.part.Heal()
		Eventually(func() bool { return n.todo("t1").GetCompleted() }).Should(BeTrue())
		Expect(title(n.todo("t1"))).To(Equal("mine"))
	})

	It("never forwards a write to a local-only entity", func() {
		rc, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w1", Label: "local"})
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Forwarded).To(BeFalse())
		Expect(rc.Wait(ctx)).To(Succeed())
		pushes := n.part.pushes.Load()
		n.put(&todov1.Todo{Id: "t1", Title: "synced"})
		n.flush()
		Expect(n.part.pushes.Load()).To(Equal(pushes + 1))
		Expect(n.status().Pending).To(BeZero())
		w := getRow[*testdatav1.Widget](n.srv, "w1")
		Expect(w.GetLabel()).To(Equal("local"))
	})

	It("forwards the bound part of a mutation that also writes a local-only entity", func() {
		_, err := n.ws().Apply(ctx, func(tx *server.ApplyTx) error {
			if err := tx.Put(&testdatav1.Widget{Id: "w1"}); err != nil {
				return err
			}
			return tx.Put(&todov1.Todo{Id: "t1", Title: "mixed"})
		})
		Expect(err).NotTo(HaveOccurred())
		n.flush()
		Expect(title(o.todo("t1"))).To(Equal("mixed"))
	})

	It("refuses a local write to a pull-only entity", func() {
		_, err := n.ws().Put(ctx, &todov1.Annotation{Id: "a1", Body: "nope"})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "%v", err)
		Expect(rej.Code).To(Equal(server.CodePermissionDenied))

		o.put(&todov1.Annotation{Id: "a2", Body: "from origin"})
		Eventually(func() string { return getRow[*todov1.Annotation](n.srv, "a2").GetBody() }).Should(Equal("from origin"))
	})

	It("refuses local writes to a workspace that is not linked", func() {
		_, err := n.link.Workspace("other").Put(ctx, &todov1.Todo{Id: "t1"})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "%v", err)
		Expect(rej.Code).To(Equal(server.CodePermissionDenied))
	})

	It("narrows and widens what it holds when the subscriptions change", func() {
		o.put(&todov1.Todo{Id: "t1", Title: "open"}, &todov1.Todo{Id: "t2", Title: "done", Completed: true})
		Eventually(func() *todov1.Todo { return n.todo("t1") }).ShouldNot(BeNil())
		Eventually(func() *todov1.Todo { return n.todo("t2") }).ShouldNot(BeNil())

		Expect(n.link.SetSubscriptions(ctx, ws, &syncplanev1.Subscription{
			Entity: "todo", Predicate: query.Eq("completed", true),
		})).To(Succeed())
		Eventually(func() *todov1.Todo { return n.todo("t1") }).Should(BeNil())
		Expect(n.todo("t2")).NotTo(BeNil())

		Expect(n.link.SetSubscriptions(ctx, ws)).To(Succeed())
		Eventually(func() *todov1.Todo { return n.todo("t1") }).ShouldNot(BeNil())
	})

	It("lets a status callback call back into the link while the subscriptions change", func() {
		var (
			armed     atomic.Bool
			m         *local
			reentered = make(chan error, 1)
		)
		m = newLocal(o, func(c *upstream.Config) {
			c.OnStatus = func(s upstream.Status) {
				if s.State != upstream.StateLive && armed.CompareAndSwap(true, false) {
					reentered <- m.link.SetSubscriptions(ctx, ws)
				}
			}
		})
		m.add()
		m.flush()
		Eventually(func() upstream.State { return m.status().State }).Should(Equal(upstream.StateLive))

		armed.Store(true)
		done := make(chan error, 1)
		go func() {
			done <- m.link.SetSubscriptions(ctx, ws, &syncplanev1.Subscription{
				Entity: "todo", Predicate: query.Eq("completed", true),
			})
		}()
		Eventually(done).Should(Receive(Succeed()))
		Eventually(reentered).Should(Receive(Succeed()))
	})

	It("refuses a subscription to an entity that is not bound", func() {
		err := n.link.SetSubscriptions(ctx, ws, &syncplanev1.Subscription{Entity: "widget"})
		Expect(err).To(HaveOccurred())
	})

	It("serves a downstream client the converged rows", func() {
		dc, err := client.New(n.ts.URL)
		Expect(err).NotTo(HaveOccurred())
		dw := dc.Workspace(ws)
		_, err = dw.Put(ctx, &todov1.Todo{Id: "t1", Title: "from browser"})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return title(o.todo("t1")) }).Should(Equal("from browser"))

		o.put(&todov1.Todo{Id: "t2", Title: "from origin"})
		Eventually(func() string {
			row, _, err := client.Get[*todov1.Todo](ctx, dw, "t2")
			Expect(err).NotTo(HaveOccurred())
			return row.Value.GetTitle()
		}).Should(Equal("from origin"))
	})

	It("resumes after a restart with writes still pending", func() {
		n.part.Cut()
		n.put(&todov1.Todo{Id: "t1", Title: "before restart"})
		n.restart()
		n.part.Cut()
		Expect(n.link.Workspaces()).To(Equal([]string{ws}))
		Eventually(func() int { return n.status().Pending }).Should(Equal(1))
		n.put(&todov1.Todo{Id: "t2", Title: "after restart"})
		n.part.Heal()
		n.flush()
		Expect(title(o.todo("t1"))).To(Equal("before restart"))
		Expect(title(o.todo("t2"))).To(Equal("after restart"))
		Expect(n.rejected()).To(BeEmpty())
	})

	It("does not reuse a mutation id the origin applied for a local transaction that did not commit", func() {
		n.put(&todov1.Todo{Id: "t1", Title: "first"})
		n.flush()
		clientID := n.link.ClientID()
		n.stop()
		// A write-through push that reached the origin, for a local
		// transaction the process did not live to commit.
		value, err := marshal(&todov1.Todo{Id: "ghost", Title: "applied upstream only"})
		Expect(err).NotTo(HaveOccurred())
		pushDirect(o, clientID, 2, &syncplanev1.CrudMutation{Entity: "todo", Key: "ghost", Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value})

		n.start()
		n.put(&todov1.Todo{Id: "t2", Title: "after the crash"})
		n.flush()
		Expect(title(o.todo("t2"))).To(Equal("after the crash"))
		Expect(title(n.todo("ghost"))).To(Equal("applied upstream only"))
	})

	It("reports the status of a workspace through the callback", func() {
		states := make(chan upstream.State, 64)
		m := newLocal(o, func(c *upstream.Config) {
			c.OnStatus = func(s upstream.Status) {
				select {
				case states <- s.State:
				default:
				}
			}
		})
		m.add()
		Eventually(states).Should(Receive(Equal(upstream.StateLive)))
	})

})
