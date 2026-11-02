package upstream_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

var _ = Describe("Write-through", func() {
	var (
		o   *origin
		n   *local
		ctx context.Context
	)
	BeforeEach(func() {
		ctx = context.Background()
		o = newOrigin(server.WithPermissions(denyForbidden{}))
		n = newLocal(o, func(c *upstream.Config) { c.WriteMode = upstream.ModeWriteThrough })
		n.add()
		n.flush()
	})

	It("commits a local write once the origin applied it", func() {
		rc, err := n.ws().Put(ctx, &todov1.Todo{Id: "t1", Title: "through"})
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t1"))).To(Equal("through"))
		Expect(title(n.todo("t1"))).To(Equal("through"))
		Expect(rc.Wait(ctx)).To(Succeed())
		n.flush()
	})

	It("fails a local write the origin rejects and commits nothing", func() {
		_, err := n.ws().Put(ctx, &todov1.Todo{Id: "t1", Title: "forbidden"})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "%v", err)
		Expect(rej.Code).To(Equal(server.CodePermissionDenied))
		Expect(n.todo("t1")).To(BeNil())

		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t2", Title: "fine"})
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t2"))).To(Equal("fine"))
		n.flush()
		Expect(n.rejected()).To(BeEmpty())
	})

	It("fails a local write while the origin is unreachable", func() {
		n.part.Cut()
		_, err := n.ws().Put(ctx, &todov1.Todo{Id: "t1", Title: "offline"})
		Expect(errors.Is(err, upstream.ErrOriginUnavailable)).To(BeTrue(), "%v", err)
		Expect(n.todo("t1")).To(BeNil())

		n.part.Heal()
		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t2", Title: "online"})
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t2"))).To(Equal("online"))
		n.flush()
		Expect(o.todo("t1")).To(BeNil())
	})

	It("pushes the writes queued before it first", func() {
		n.part.Cut()
		_, err := n.ws().Put(upstream.WriteBehind(ctx), &todov1.Todo{Id: "t1", Title: "behind"})
		Expect(err).NotTo(HaveOccurred())
		n.part.Heal()
		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t2", Title: "through"})
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t1"))).To(Equal("behind"))
		Expect(title(o.todo("t2"))).To(Equal("through"))
		n.flush()
	})

	It("applies to one call through the context marker", func() {
		m := newLocal(o, nil)
		Expect(m.link.AddWorkspace(ctx, "ws2")).To(Succeed())
		_, err := m.link.Workspace("ws2").Put(upstream.WriteThrough(ctx), &todov1.Todo{Id: "t1", Title: "marked"})
		Expect(err).NotTo(HaveOccurred())
		var got *todov1.Todo
		Expect(o.srv.View(ctx, "ws2", func(tx *server.ViewTx) error {
			got = &todov1.Todo{}
			_, _, err := tx.Get(got, "t1")
			return err
		})).To(Succeed())
		Expect(got.GetTitle()).To(Equal("marked"))
	})

	It("does not wait for the worker to learn the watermark after a restart", func() {
		_, err := n.ws().Put(ctx, &todov1.Todo{Id: "t1", Title: "one"})
		Expect(err).NotTo(HaveOccurred())
		n.restart()
		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t2", Title: "two"})
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t2"))).To(Equal("two"))
		n.flush()
	})

	It("checks a compare-and-swap against a write the origin applied and a page already confirmed", func() {
		// A worker that failed waits out a long backoff, so only the
		// write-through below resends the base write.
		m := newLocal(o, func(c *upstream.Config) {
			c.WriteMode = upstream.ModeWriteThrough
			c.RetryMin, c.RetryMax = time.Hour, time.Hour
		})
		m.add()
		m.flush()
		before := m.status().OriginSeq
		m.part.losePushes.Store(1)
		_, err := m.ws().Put(upstream.WriteBehind(ctx), &todov1.Todo{Id: "t1", Title: "a"})
		Expect(err).NotTo(HaveOccurred())
		// The origin applied the base and its answer was lost. A page then
		// brings the base and a watermark past it, while the base is still
		// unanswered in the queue.
		Eventually(func() uint64 { return m.status().OriginSeq }).Should(BeNumerically(">", before))
		Expect(m.status().Pending).To(Equal(1))

		row, ok, err := upstream.Get[*todov1.Todo](ctx, m.ws(), "t1")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		v := row.Version
		_, err = m.ws().Patch(ctx, &todov1.Todo{Id: "t1", Title: "b"}, []string{"title"}, &v)
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t1"))).To(Equal("b"))
	})
})
