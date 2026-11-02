package upstream_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// denyForbidden rejects every write of a todo titled "forbidden".
type denyForbidden struct{ server.AllowAll }

func (denyForbidden) CanMutate(_ context.Context, _ server.Principal, _ string, w server.Write) error {
	if c := w.Crud; c != nil && c.GetEntity() == "todo" && c.GetOp() != syncplanev1.CrudOp_CRUD_OP_DELETE {
		var t todov1.Todo
		if err := proto.Unmarshal(c.GetValue(), &t); err == nil && t.GetTitle() == "forbidden" {
			return server.Reject(server.CodePermissionDenied, "forbidden title")
		}
	}
	return nil
}

var _ = Describe("Verdicts", func() {
	var (
		o   *origin
		n   *local
		ctx context.Context
	)
	BeforeEach(func() {
		ctx = context.Background()
		o = newOrigin(server.WithPermissions(denyForbidden{}))
		o.put(&todov1.Todo{Id: "t1", Title: "ok"})
		n = newLocal(o, nil)
		n.add()
		n.flush()
	})

	Describe("a write the origin rejects", func() {
		It("is reverted locally and reported once", func() {
			rc := n.patch(&todov1.Todo{Id: "t1", Title: "forbidden"}, "title")
			err := rc.Wait(ctx)
			var rej *upstream.RejectedError
			Expect(errors.As(err, &rej)).To(BeTrue(), "%v", err)
			Expect(rej.Code).To(Equal(server.CodePermissionDenied))

			Eventually(n.rejected).Should(HaveLen(1))
			r := n.rejected()[0]
			Expect(r.Workspace).To(Equal(ws))
			Expect(r.LocalSeq).To(Equal(rc.Seq))
			Expect(r.Code).To(Equal(server.CodePermissionDenied))
			Expect(r.Rows).To(Equal([]upstream.RowRef{{Entity: "todo", Key: "t1"}}))
			Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("ok"))
			Expect(n.link.Await(ctx, ws, rc.Seq)).To(MatchError(rej))

			n.restart()
			n.flush()
			Consistently(n.rejected, "200ms").Should(HaveLen(1))
		})

		It("is reported once when it was pending across a restart", func() {
			n.part.Cut()
			n.put(&todov1.Todo{Id: "t2", Title: "forbidden"})
			Expect(n.todo("t2")).NotTo(BeNil())
			n.restart()
			n.part.Heal()
			n.flush()
			Expect(n.rejected()).To(HaveLen(1))
			Expect(n.todo("t2")).To(BeNil())
			n.restart()
			n.flush()
			Consistently(n.rejected, "200ms").Should(HaveLen(1))
		})

		It("is reported once when the push response carrying it was lost", func() {
			n.part.losePushes.Store(1)
			n.put(&todov1.Todo{Id: "t2", Title: "forbidden"})
			n.put(&todov1.Todo{Id: "t3", Title: "fine"})
			n.flush()
			Expect(n.rejected()).To(HaveLen(1))
			Expect(n.rejected()[0].Rows).To(Equal([]upstream.RowRef{{Entity: "todo", Key: "t2"}}))
			Expect(n.todo("t2")).To(BeNil())
			Expect(title(o.todo("t3"))).To(Equal("fine"))
		})

		It("leaves the writes queued after it to apply", func() {
			n.part.Cut()
			n.put(&todov1.Todo{Id: "t2", Title: "forbidden"})
			n.put(&todov1.Todo{Id: "t3", Title: "fine"})
			n.part.Heal()
			n.flush()
			Expect(n.rejected()).To(HaveLen(1))
			Expect(title(o.todo("t3"))).To(Equal("fine"))
			Expect(n.todo("t2")).To(BeNil())
		})
	})

	Describe("a compare-and-swap", func() {
		version := func(key string) uint64 {
			GinkgoHelper()
			row, ok, err := upstream.Get[*todov1.Todo](ctx, n.ws(), key)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			return row.Version
		}

		It("checks the origin version of the row it was made against", func() {
			v := version("t1")
			rc, err := n.ws().Patch(ctx, &todov1.Todo{Id: "t1", Title: "swapped"}, []string{"title"}, &v)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.Wait(ctx)).To(Succeed())
			Expect(title(o.todo("t1"))).To(Equal("swapped"))
		})

		It("is rejected with conflict when the origin row changed meanwhile", func() {
			n.part.Cut()
			o.patch(&todov1.Todo{Id: "t1", Title: "theirs"}, "title")
			v := version("t1")
			_, err := n.ws().Patch(ctx, &todov1.Todo{Id: "t1", Title: "mine"}, []string{"title"}, &v)
			Expect(err).NotTo(HaveOccurred())
			n.part.Heal()
			n.flush()
			Expect(n.rejected()).To(HaveLen(1))
			Expect(n.rejected()[0].Code).To(Equal(server.CodeConflict))
			Expect(title(n.todo("t1"))).To(Equal("theirs"))
		})

		It("made against a pending write checks the version that write takes at the origin", func() {
			n.part.Cut()
			n.put(&todov1.Todo{Id: "t5", Title: "a"})
			v := version("t5")
			_, err := n.ws().Patch(ctx, &todov1.Todo{Id: "t5", Title: "b"}, []string{"title"}, &v)
			Expect(err).NotTo(HaveOccurred())
			n.part.Heal()
			n.flush()
			Expect(n.rejected()).To(BeEmpty())
			Expect(title(o.todo("t5"))).To(Equal("b"))
		})

		It("made against a pending write whose push response was lost applies once the write is pulled", func() {
			n.part.blockPush.Store(true)
			n.put(&todov1.Todo{Id: "t7", Title: "a"})
			v := version("t7")
			_, err := n.ws().Patch(ctx, &todov1.Todo{Id: "t7", Title: "b"}, []string{"title"}, &v)
			Expect(err).NotTo(HaveOccurred())
			// Close the stream and keep it closed, so no pull retires the base
			// write before its resend is answered ALREADY_APPLIED.
			n.part.blockReads.Store(true)
			n.part.Cut()
			n.part.Heal()
			n.part.losePushes.Store(1)
			n.part.blockPush.Store(false)
			Eventually(n.part.pushes.Load).Should(BeNumerically(">=", 2))
			n.part.blockReads.Store(false)
			n.flush()
			Expect(n.rejected()).To(BeEmpty())
			Expect(title(o.todo("t7"))).To(Equal("b"))
		})

		It("made against a pending write the origin rejects is rejected with conflict", func() {
			n.part.Cut()
			n.put(&todov1.Todo{Id: "t6", Title: "forbidden"})
			v := version("t6")
			_, err := n.ws().Patch(ctx, &todov1.Todo{Id: "t6", Title: "fine"}, []string{"title"}, &v)
			Expect(err).NotTo(HaveOccurred())
			n.part.Heal()
			n.flush()
			Expect(n.rejected()).To(HaveLen(2))
			Expect(n.rejected()[1].Code).To(Equal(server.CodeConflict))
			Expect(n.todo("t6")).To(BeNil())
			Expect(o.todo("t6")).To(BeNil())
		})
	})
})

var _ = Describe("Custom mutations", func() {
	var (
		o   *origin
		ctx context.Context
	)
	BeforeEach(func() {
		ctx = context.Background()
		o = newOrigin()
		o.put(&todov1.Todo{Id: "t1", Title: "one"}, &todov1.Todo{Id: "t2", Title: "two"})
	})

	It("runs a locally declared mutator as a prediction the origin's result replaces", func() {
		mirror := func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
			var todos []*todov1.Todo
			if err := tx.Scan(&todov1.Todo{}, func(msg proto.Message) error {
				todos = append(todos, msg.(*todov1.Todo))
				return nil
			}); err != nil {
				return err
			}
			for _, t := range todos {
				t.Completed = true
				t.Title += " (predicted)"
				if err := tx.Put(t); err != nil {
					return err
				}
			}
			return nil
		}
		n := newLocal(o, nil, server.WithMutator("complete_all", &todov1.CompleteAllArgs{}, mirror))
		n.add()
		n.flush()
		n.part.Cut()
		rc, err := n.ws().Invoke(ctx, "complete_all", &todov1.CompleteAllArgs{})
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Forwarded).To(BeTrue())
		Expect(title(n.todo("t1"))).To(Equal("one (predicted)"))
		n.part.Heal()
		Expect(rc.Wait(ctx)).To(Succeed())
		n.flush()
		for _, key := range []string{"t1", "t2"} {
			Expect(n.todo(key).GetCompleted()).To(BeTrue())
			Expect(n.todo(key).GetTitle()).NotTo(ContainSubstring("predicted"))
		}
	})

	It("forwards a mutation the local engine declares no mutator for", func() {
		n := newLocal(o, nil)
		n.add()
		n.flush()
		rc, err := n.ws().Invoke(ctx, "complete_all", &todov1.CompleteAllArgs{})
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Forwarded).To(BeTrue())
		Expect(rc.Seq).To(BeZero())
		Expect(rc.Wait(ctx)).To(Succeed())
		n.flush()
		Expect(n.todo("t1").GetCompleted()).To(BeTrue())
		Expect(o.todo("t2").GetCompleted()).To(BeTrue())
	})

	It("forwards a mutation a downstream client pushes", func() {
		n := newLocal(o, nil)
		n.add()
		n.flush()
		args, err := anyOf(&todov1.CompleteAllArgs{})
		Expect(err).NotTo(HaveOccurred())
		resp, err := n.srv.Push(ctx, connectRequest(&syncplanev1.PushRequest{
			Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: []*syncplanev1.Mutation{{
				ClientId: "browser", MutationId: 1,
				Payload: &syncplanev1.Mutation_Custom{Custom: &syncplanev1.CustomMutation{Name: "complete_all", Args: args}},
			}}}},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetResults()[0].GetResults()[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		Eventually(func() bool { return o.todo("t1").GetCompleted() }).Should(BeTrue())
		Eventually(func() bool { return n.todo("t1").GetCompleted() }).Should(BeTrue())
	})

	It("drops a mutation the origin keeps failing, and pushes the writes after it", func() {
		o := newOrigin(server.WithMutator("explode", nil, func(context.Context, *server.EntityTx, proto.Message) error {
			return errors.New("boom")
		}))
		n := newLocal(o, func(c *upstream.Config) { c.PoisonThreshold = 2 })
		n.add()
		n.flush()
		n.part.Cut()
		rc, err := n.ws().Invoke(ctx, "explode", nil)
		Expect(err).NotTo(HaveOccurred())
		n.put(&todov1.Todo{Id: "t1", Title: "after"})
		n.part.Heal()
		n.flush()
		Expect(n.rejected()).To(HaveLen(1))
		Expect(n.rejected()[0].Mutator).To(Equal("explode"))
		Expect(n.rejected()[0].Code).To(Equal(server.CodeMutationFailed))
		var rej *upstream.RejectedError
		Expect(errors.As(rc.Wait(ctx), &rej)).To(BeTrue())
		Expect(title(o.todo("t1"))).To(Equal("after"))
	})
})
