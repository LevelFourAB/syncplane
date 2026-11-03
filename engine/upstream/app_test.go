package upstream_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

var _ = Describe("Workspace", func() {
	var (
		o   *origin
		n   *local
		w   *upstream.Workspace
		ctx context.Context
	)
	BeforeEach(func() {
		ctx = context.Background()
		o = newOrigin(server.WithPermissions(denyForbidden{}))
		n = newLocal(o, nil)
		n.add()
		n.flush()
		w = n.ws()
	})

	It("reads and queries the local rows, pending writes included", func() {
		o.put(&todov1.Todo{Id: "t1", Title: "b", Completed: true})
		Eventually(func() *todov1.Todo { return n.todo("t1") }).ShouldNot(BeNil())
		n.part.Cut()
		_, err := w.Put(ctx, &todov1.Todo{Id: "t2", Title: "a", Completed: true})
		Expect(err).NotTo(HaveOccurred())
		_, err = w.Put(ctx, &todov1.Todo{Id: "t3", Title: "c"})
		Expect(err).NotTo(HaveOccurred())

		row, ok, err := upstream.Get[*todov1.Todo](ctx, w, "t2")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(row.Value.GetTitle()).To(Equal("a"))
		_, ok, err = upstream.Get[*todov1.Todo](ctx, w, "missing")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())

		rows, err := upstream.Query[*todov1.Todo](ctx, w, query.Spec{
			Where:   query.Eq("completed", true),
			OrderBy: []*syncplanev1.OrderKey{query.Asc("title")},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(2))
		Expect(rows[0].Key).To(Equal("t2"))
		Expect(rows[1].Key).To(Equal("t1"))
	})

	It("awaits the origin's verdict on a write by its local sequence", func() {
		n.part.Cut()
		ok, err := w.Put(ctx, &todov1.Todo{Id: "t1", Title: "fine"})
		Expect(err).NotTo(HaveOccurred())
		bad, err := w.Put(ctx, &todov1.Todo{Id: "t2", Title: "forbidden"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.link.Await(ctx, ws, 999999)).To(MatchError(upstream.ErrUnknownWrite))

		short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		Expect(n.link.Await(short, ws, ok.Seq)).To(MatchError(context.DeadlineExceeded))

		n.part.Heal()
		Expect(n.link.Await(ctx, ws, ok.Seq)).To(Succeed())
		var rej *upstream.RejectedError
		Expect(n.link.Await(ctx, ws, bad.Seq)).To(BeAssignableToTypeOf(rej))
		n.flush()
		Expect(n.link.Await(ctx, ws, ok.Seq)).To(Succeed(), "a verdict is remembered after its write is confirmed")
	})

	It("watches local and origin changes as they commit", func() {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		changes := make(chan string, 16)
		head, err := n.srv.CurrentSeq(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			for page, err := range w.Watch(wctx, &server.Cursor{Seq: head}) {
				if err != nil {
					return
				}
				for _, c := range page.Changes {
					changes <- c.Key
				}
			}
		}()
		_, err = w.Put(ctx, &todov1.Todo{Id: "t1", Title: "local"})
		Expect(err).NotTo(HaveOccurred())
		Eventually(changes).Should(Receive(Equal("t1")))
		o.put(&todov1.Todo{Id: "t2", Title: "origin"})
		Eventually(changes).Should(Receive(Equal("t2")))
	})

	It("resumes a watch from the cursor of a page it yielded", func() {
		_, err := w.Put(ctx, &todov1.Todo{Id: "t1", Title: "one"})
		Expect(err).NotTo(HaveOccurred())
		var resume server.Cursor
		for page, err := range w.Watch(ctx, &server.Cursor{}) {
			Expect(err).NotTo(HaveOccurred())
			resume = page.Cursor
			break
		}
		Expect(resume.Epoch).NotTo(BeZero(), "the cursor carries the store lineage")
		_, err = w.Put(ctx, &todov1.Todo{Id: "t2", Title: "two"})
		Expect(err).NotTo(HaveOccurred())
		for page, err := range w.Watch(ctx, &resume) {
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Changes).To(HaveLen(1))
			Expect(page.Changes[0].Key).To(Equal("t2"))
			break
		}
	})
})
