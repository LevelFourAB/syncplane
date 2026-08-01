package connect_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

// newIncFixture builds a fixture over a source with a native change cursor.
func newIncFixture(opts ...func(*engineconnect.Config)) (*fixture, *incTodoSource) {
	GinkgoHelper()
	src := newIncTodoSource()
	return newFixtureOver(nil, src, opts...), src
}

var _ = Describe("Incremental ingress", func() {
	const ws = "ws-inc"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("checkpoints then lists on the first pass, and rides the cursor after", func() {
		f, src := newIncFixture()
		src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})

		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		lists, deltas, points := src.counts()
		Expect(lists).To(Equal(1), "no cursor yet: the first pass enumerates")
		Expect(points).To(Equal(1), "and checkpoints BEFORE that enumeration")
		Expect(deltas).To(BeZero())
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil())

		sync, err := f.st.SourceSync(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(sync.Cursor).To(Equal("1"))
		Expect(sync.LastFullSync).NotTo(BeZero())

		// From here the store is never enumerated again.
		src.set(ws, &todov1.Todo{Id: "t2", Title: "two"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		lists, deltas, _ = src.counts()
		Expect(lists).To(Equal(1))
		Expect(deltas).To(Equal(1))
		Expect(f.engineTodo(ctx, ws, "t2")).NotTo(BeNil())

		sync, err = f.st.SourceSync(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(sync.Cursor).To(Equal("2"))
	})

	It("drains every page in one call while the source says More", func() {
		f, src := newIncFixture()
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed()) // establishes the cursor
		src.pageSize = 1

		src.set(ws, &todov1.Todo{Id: "t1"})
		src.set(ws, &todov1.Todo{Id: "t2"})
		src.set(ws, &todov1.Todo{Id: "t3"})

		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		_, deltas, _ := src.counts()
		Expect(deltas).To(Equal(3), "a page at a time, without waiting for the next tick")
		for _, id := range []string{"t1", "t2", "t3"} {
			Expect(f.engineTodo(ctx, ws, id)).NotTo(BeNil(), id)
		}
	})

	It("tombstones rows the delta reports deleted", func() {
		f, src := newIncFixture()
		src.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil())

		src.remove(ws, "t1")
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).To(BeNil())

		lists, _, _ := src.counts()
		Expect(lists).To(Equal(1), "a reported deletion needs no enumeration")
	})

	It("falls back to a checkpoint and a full list when the cursor is rejected", func() {
		f, src := newIncFixture()
		src.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())

		// The account was rebuilt behind the connector's back: the cursor is
		// meaningless and the edit it would have carried is only visible to a
		// full enumeration.
		src.set(ws, &todov1.Todo{Id: "t2", Title: "two"})
		src.invalidateCursor()

		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		lists, _, points := src.counts()
		Expect(lists).To(Equal(2), "recovery happens inside the same pass")
		Expect(points).To(Equal(2))
		Expect(f.engineTodo(ctx, ws, "t2")).NotTo(BeNil())

		// And the fresh cursor is live again.
		src.set(ws, &todov1.Todo{Id: "t3"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		lists, _, _ = src.counts()
		Expect(lists).To(Equal(2))
		Expect(f.engineTodo(ctx, ws, "t3")).NotTo(BeNil())
	})

	// The cursor is persisted only after its page has been applied, so a crash
	// in between replays the page. Both halves have to be no-ops the second
	// time: an unchanged item by fingerprint, and a deletion by reading the
	// row before writing a tombstone over a row that is already gone.
	It("replays a page idempotently when the crash lost the cursor", func() {
		f, src := newIncFixture()
		src.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		sync, err := f.st.SourceSync(ctx, ws)
		Expect(err).NotTo(HaveOccurred())

		src.set(ws, &todov1.Todo{Id: "t2", Title: "two"})
		src.remove(ws, "t1")
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).To(BeNil())
		seq := f.currentSeq(ctx, ws)

		// Rewind to before the page, as a crash between applying it and
		// persisting its cursor would have left things.
		Expect(f.st.SetSourceSync(ctx, ws, sync)).To(Succeed())
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())

		Expect(f.currentSeq(ctx, ws)).To(Equal(seq), "the replay must write nothing at all")
		Expect(f.engineTodo(ctx, ws, "t1")).To(BeNil())
		Expect(f.engineTodo(ctx, ws, "t2").GetTitle()).To(Equal("two"))
	})

	It("re-enumerates once FullReconcileInterval has elapsed", func() {
		f, src := newIncFixture(func(cfg *engineconnect.Config) {
			cfg.FullReconcileInterval = time.Nanosecond
		})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		lists, _, _ := src.counts()
		Expect(lists).To(Equal(2), "the safety net is due on every pass at this interval")
	})

	It("never re-enumerates when the safety net is disabled", func() {
		f, src := newIncFixture(func(cfg *engineconnect.Config) {
			cfg.FullReconcileInterval = -1
		})
		for i := 0; i < 3; i++ {
			Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		}
		lists, _, _ := src.counts()
		Expect(lists).To(Equal(1), "only the cursorless first pass may enumerate")
	})

	// A row missing from a delta means "unchanged", not "deleted" — only an
	// enumeration can tell the difference, so only the full pass sweeps.
	It("does not sweep source deletions on an incremental pass", func() {
		f, src := newIncFixture()
		src.set(ws, &todov1.Todo{Id: "t1"})
		src.set(ws, &todov1.Todo{Id: "t2"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())

		// t1 vanishes without the feed ever reporting it; t2 changes so the
		// incremental pass has work to do.
		src.todoSource.remove(ws, "t1")
		src.set(ws, &todov1.Todo{Id: "t2", Title: "changed"})

		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t2").GetTitle()).To(Equal("changed"))
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil(), "an unreported row is not a deleted row")

		// The safety net is what notices.
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).To(BeNil())
	})

	It("is exactly Reconcile for a source without a cursor", func() {
		f := newFixture()
		f.src.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil())

		// Nothing is recorded: there is no cursor to keep.
		sync, err := f.st.SourceSync(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(sync).To(Equal(engineconnect.SourceSync{}))
	})
})
