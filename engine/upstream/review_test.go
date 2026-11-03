package upstream_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// pushBatch pushes crud mutations to srv as one batch of one downstream
// client and returns the verdicts.
func pushBatch(srv *server.Server, clientID string, cruds ...*syncplanev1.CrudMutation) []*syncplanev1.MutationResult {
	GinkgoHelper()
	var muts []*syncplanev1.Mutation
	for i, c := range cruds {
		muts = append(muts, &syncplanev1.Mutation{ClientId: clientID, MutationId: uint64(i + 1), Payload: &syncplanev1.Mutation_Crud{Crud: c}})
	}
	resp, err := srv.Push(context.Background(), connect.NewRequest(&syncplanev1.PushRequest{
		Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: muts}},
	}))
	Expect(err).NotTo(HaveOccurred())
	wr := resp.Msg.GetResults()[0]
	Expect(wr.GetErrorCode()).To(BeEmpty(), wr.GetErrorMessage())
	return wr.GetResults()
}

func putTodo(t *todov1.Todo) *syncplanev1.CrudMutation {
	GinkgoHelper()
	value, err := marshal(t)
	Expect(err).NotTo(HaveOccurred())
	return &syncplanev1.CrudMutation{Entity: "todo", Key: t.GetId(), Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value}
}

func originSeq(o *origin) uint64 {
	GinkgoHelper()
	seq, err := o.srv.CurrentSeq(context.Background(), ws)
	Expect(err).NotTo(HaveOccurred())
	return seq
}

var _ = Describe("Review fixes", func() {
	var ctx context.Context
	BeforeEach(func() { ctx = context.Background() })

	It("applies each mutation of a write-through downstream batch once when a later one is rejected", func() {
		o := newOrigin(server.WithPermissions(denyForbidden{}))
		n := newLocal(o, func(c *upstream.Config) { c.WriteMode = upstream.ModeWriteThrough })
		n.add()
		n.flush()
		_, err := n.ws().Put(ctx, &todov1.Todo{Id: "t0", Title: "warm"})
		Expect(err).NotTo(HaveOccurred())

		before := originSeq(o)
		results := pushBatch(n.srv, "browser",
			putTodo(&todov1.Todo{Id: "t1", Title: "fine"}),
			putTodo(&todov1.Todo{Id: "t2", Title: "forbidden"}))
		Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		Expect(results[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(results[1].GetErrorCode()).To(Equal(server.CodePermissionDenied))
		Expect(originSeq(o)-before).To(Equal(uint64(1)), "the origin applied t1 once")

		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t3", Title: "after"})
		Expect(err).NotTo(HaveOccurred())
		n.flush()
		Expect(title(n.todo("t1"))).To(Equal("fine"))
		Expect(n.todo("t2")).To(BeNil())
		Expect(title(o.todo("t3"))).To(Equal("after"))
		Expect(n.rejected()).To(BeEmpty())
	})

	It("does not hang a Flush called from OnRejected", func() {
		o := newOrigin(server.WithPermissions(denyForbidden{}))
		inner := make(chan error, 1)
		var n *local
		n = newLocal(o, func(c *upstream.Config) {
			c.OnRejected = func(upstream.Rejection) {
				fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				inner <- n.link.Flush(fctx, ws)
			}
		})
		n.add()
		n.flush()
		n.put(&todov1.Todo{Id: "t1", Title: "forbidden"})
		n.flush()
		Eventually(inner).Should(Receive(Succeed()))
	})

	It("leaves the rows of a workspace that is not linked when asked to remove it", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		n.flush()
		n.put(&todov1.Todo{Id: "t1", Title: "kept"})
		n.flush()
		Expect(n.link.RemoveWorkspace(ctx, ws, upstream.RemoveOptions{})).To(Succeed())
		Expect(n.link.RemoveWorkspace(ctx, ws, upstream.RemoveOptions{DeleteRows: true})).To(Succeed())
		Expect(title(n.todo("t1"))).To(Equal("kept"))
	})

	Describe("rows the local engine held before the workspace was linked", func() {
		var (
			o *origin
			n *local
		)
		BeforeEach(func() {
			// Small pages, so each pass over the rows takes several.
			DeferCleanup(upstream.SetScanPage(2))
			o = newOrigin()
			o.put(&todov1.Todo{Id: "t1", Title: "origin"})
			n = newLocal(o, nil)
			n.stop()
			// The app used the engine without the link before.
			store, err := sqlitestore.Open(n.path)
			Expect(err).NotTo(HaveOccurred())
			plain, err := server.New(store, server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(plain.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				for i := 5; i <= 9; i++ {
					if err := tx.Put(&todov1.Todo{Id: fmt.Sprintf("t%d", i), Title: "local before linking"}); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())
			Expect(plain.Close()).To(Succeed())
			Expect(store.Close()).To(Succeed())
			n.start()
		})

		It("are discarded by default: the origin's snapshot decides", func() {
			n.add()
			n.flush()
			for i := 5; i <= 9; i++ {
				Expect(n.todo(fmt.Sprintf("t%d", i))).To(BeNil())
			}
			Expect(title(n.todo("t1"))).To(Equal("origin"))
		})

		It("are pushed to the origin when the workspace is added with AdoptLocalRows", func() {
			Expect(n.link.AddWorkspaceWith(ctx, ws, upstream.AddOptions{AdoptLocalRows: true})).To(Succeed())
			n.flush()
			for i := 5; i <= 9; i++ {
				Expect(title(n.todo(fmt.Sprintf("t%d", i)))).To(Equal("local before linking"))
				Expect(title(o.todo(fmt.Sprintf("t%d", i)))).To(Equal("local before linking"))
			}
			Expect(title(n.todo("t1"))).To(Equal("origin"))
		})

		It("are deleted in pages when the workspace is removed with its rows", func() {
			Expect(n.link.AddWorkspaceWith(ctx, ws, upstream.AddOptions{AdoptLocalRows: true})).To(Succeed())
			n.flush()
			Expect(n.link.RemoveWorkspace(ctx, ws, upstream.RemoveOptions{DeleteRows: true})).To(Succeed())
			rows, err := n.srv.CurrentSeq(ctx, ws)
			Expect(err).NotTo(HaveOccurred())
			Expect(rows).NotTo(BeZero())
			for _, key := range []string{"t1", "t5", "t6", "t7", "t8", "t9"} {
				Expect(n.todo(key)).To(BeNil(), key)
			}
		})
	})

	It("realigns and retries a write-through write after a failed one burned an id", func() {
		o := newOrigin()
		n := newLocal(o, func(c *upstream.Config) { c.WriteMode = upstream.ModeWriteThrough })
		n.add()
		n.flush()
		_, err := n.ws().Put(ctx, &todov1.Todo{Id: "t0", Title: "first"})
		Expect(err).NotTo(HaveOccurred())
		n.part.Cut()
		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t1", Title: "lost"})
		Expect(errors.Is(err, upstream.ErrOriginUnavailable)).To(BeTrue(), "%v", err)
		n.part.Heal()
		_, err = n.ws().Put(ctx, &todov1.Todo{Id: "t2", Title: "next"})
		Expect(err).NotTo(HaveOccurred())
		Expect(title(o.todo("t2"))).To(Equal("next"))
		n.flush()
	})

	It("settles the receipt of a write-through write made after the queue was renumbered", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		n.flush()
		n.put(&todov1.Todo{Id: "t0", Title: "first"})
		n.flush()
		// A write-through push lost in transit burns an id, so the next
		// write-behind push meets a gap and the queue is renumbered.
		n.part.Cut()
		_, err := n.ws().Put(upstream.WriteThrough(ctx), &todov1.Todo{Id: "lost", Title: "lost"})
		Expect(errors.Is(err, upstream.ErrOriginUnavailable)).To(BeTrue(), "%v", err)
		n.part.Heal()
		n.put(&todov1.Todo{Id: "t1", Title: "behind"})
		n.flush()
		// No pull may confirm the next write before the receipt is read.
		n.part.blockReads.Store(true)
		n.part.Cut()
		n.part.Heal()
		rc, err := n.ws().Put(upstream.WriteThrough(ctx), &todov1.Todo{Id: "t2", Title: "through"})
		Expect(err).NotTo(HaveOccurred())
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		Expect(rc.Wait(wctx)).To(Succeed())
	})

	It("stops a workspace whose link state was deleted with the workspace", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		n.flush()
		Expect(n.srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		o.put(&todov1.Todo{Id: "t1", Title: "arrives after the deletion"})
		Eventually(func() upstream.State { return n.status().State }).Should(Equal(upstream.StateFailed))
		Expect(n.status().LastError).To(MatchError(upstream.ErrNotLinked))
		Expect(n.link.Flush(ctx, ws)).To(MatchError(upstream.ErrNotLinked))
	})

	It("closes after the engine and its store were closed first", func() {
		o := newOrigin()
		store, err := sqlitestore.Open(GinkgoT().TempDir() + "/order.db")
		Expect(err).NotTo(HaveOccurred())
		link, err := upstream.New(upstream.Config{
			OriginURL: o.ts.URL, Bindings: []upstream.Binding{{Prototype: &todov1.Todo{}}},
			RetryMin: 10 * time.Millisecond, RetryMax: 50 * time.Millisecond,
		})
		Expect(err).NotTo(HaveOccurred())
		srv, err := server.New(store, append([]server.Option{server.WithEntities(&todov1.Todo{})}, link.ServerOptions()...)...)
		Expect(err).NotTo(HaveOccurred())
		Expect(link.Start(ctx, srv)).To(Succeed())
		Expect(link.AddWorkspace(ctx, ws)).To(Succeed())
		Expect(link.Flush(ctx, ws)).To(Succeed())
		Expect(srv.Close()).To(Succeed())
		Expect(store.Close()).To(Succeed())
		o.put(&todov1.Todo{Id: "t1", Title: "arrives after the store closed"})
		done := make(chan error, 1)
		go func() { done <- link.Close() }()
		Eventually(done).WithTimeout(5 * time.Second).Should(Receive(Succeed()))
	})

	It("can be started again after a failed Start", func() {
		o := newOrigin()
		store, err := sqlitestore.Open(GinkgoT().TempDir() + "/retry.db")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)
		link, err := upstream.New(upstream.Config{OriginURL: o.ts.URL, Bindings: []upstream.Binding{{Prototype: &todov1.Todo{}}}})
		Expect(err).NotTo(HaveOccurred())
		bad, err := server.New(store, append([]server.Option{server.WithEntities(&todov1.Note{})}, link.ServerOptions()...)...)
		Expect(err).NotTo(HaveOccurred())
		Expect(link.Start(ctx, bad)).NotTo(Succeed())
		Expect(bad.Close()).To(Succeed())
		good, err := server.New(store, append([]server.Option{server.WithEntities(&todov1.Todo{})}, link.ServerOptions()...)...)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(good.Close)
		Expect(link.Start(ctx, good)).To(Succeed())
		Expect(link.Close()).To(Succeed())
	})

	It("keeps syncing the other workspaces when one's link state does not load", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		Expect(n.link.AddWorkspace(ctx, "ws2")).To(Succeed())
		n.flush()
		n.stop()
		store, err := sqlitestore.Open(n.path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, "ws2", func(tx storage.WriteTx) error {
			return tx.PutConsumerShadow(server.ReservedConsumerPrefix+"upstream", "#m", "queue", []byte{0xff})
		})).To(Succeed())
		Expect(store.Close()).To(Succeed())
		n.start()
		st, ok := n.link.Status("ws2")
		Expect(ok).To(BeTrue())
		Expect(st.State).To(Equal(upstream.StateFailed))
		Expect(st.LastError).To(HaveOccurred())
		o.put(&todov1.Todo{Id: "t1", Title: "still syncing"})
		Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("still syncing"))
	})

	It("drains a large offline queue in time linear in its size", Label("scale"), func() {
		drain := func(entries int) time.Duration {
			GinkgoHelper()
			o := newOrigin()
			n := newLocal(o, nil)
			n.add()
			n.flush()
			n.part.Cut()
			for i := range entries {
				_, err := n.ws().Put(ctx, &todov1.Todo{Id: fmt.Sprintf("t%06d", i), Title: "offline"})
				Expect(err).NotTo(HaveOccurred())
			}
			Eventually(func() int { return n.status().Pending }).Should(Equal(entries))
			start := time.Now()
			n.part.Heal()
			fctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			defer cancel()
			Expect(n.link.Flush(fctx, ws)).To(Succeed())
			return time.Since(start)
		}
		small, large := scaleSizes()
		ts, tl := drain(small), drain(large)
		ratio := float64(tl) / float64(ts) / (float64(large) / float64(small))
		GinkgoWriter.Printf("drain %d: %v, %d: %v, per-entry ratio %.2f\n", small, ts, large, tl, ratio)
		Expect(ratio).To(BeNumerically("<", 3), "per-entry drain time grows with the queue")
	})
})

// scaleSizes returns the queue sizes the scale spec compares:
// UPSTREAM_SCALE_SMALL and UPSTREAM_SCALE_LARGE, or 500 and 2000.
func scaleSizes() (small, large int) {
	small, large = 500, 2000
	if v, err := strconv.Atoi(os.Getenv("UPSTREAM_SCALE_SMALL")); err == nil && v > 0 {
		small = v
	}
	if v, err := strconv.Atoi(os.Getenv("UPSTREAM_SCALE_LARGE")); err == nil && v > 0 {
		large = v
	}
	return small, large
}
