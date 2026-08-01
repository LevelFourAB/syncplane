package connect_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

var _ = Describe("Connector.Reconcile", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("imports a populated source into an empty engine (initial sync)", func() {
		f := newFixture()
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})
		f.src.set(ws, &todov1.Todo{Id: "t2", Title: "two", Completed: true})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(f.engineTodo(ctx, ws, "t1").GetTitle()).To(Equal("one"))
		Expect(f.engineTodo(ctx, ws, "t2").GetCompleted()).To(BeTrue())

		// Quiescence: a second pass ingests nothing and writes nothing.
		seq := f.currentSeq(ctx, ws)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq))
		Expect(f.src.writeCount()).To(BeZero())
	})

	It("exports a populated engine into an empty source (initial sync, other direction)", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("one"))
		Expect(f.src.get(ws, "t2").GetTitle()).To(Equal("two"))
		Expect(f.src.writeCount()).To(Equal(2))
	})

	It("ingests source drift field-precisely, preserving concurrent client edits", func() {
		f := newFixture()
		// Converged starting point.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "old", Completed: false}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Concurrently: client renames; source completes.
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t1", &todov1.Todo{Id: "t1", Title: "renamed", Completed: false}))
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "old", Completed: true})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Both edits survive on both sides.
		merged := f.engineTodo(ctx, ws, "t1")
		Expect(merged.GetTitle()).To(Equal("renamed"))
		Expect(merged.GetCompleted()).To(BeTrue())
		inSource := f.src.get(ws, "t1")
		Expect(inSource.GetTitle()).To(Equal("renamed"))
		Expect(inSource.GetCompleted()).To(BeTrue())

		// And the system is quiescent afterwards.
		writes := f.src.writeCount()
		seq := f.currentSeq(ctx, ws)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq))
	})

	It("tombstones rows deleted at the source without resurrecting them", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		f.src.remove(ws, "t1")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).To(BeNil(), "source delete tombstones the engine row")

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).To(BeNil())

		// No resurrect loop: further passes change nothing.
		seq := f.currentSeq(ctx, ws)
		writes := f.src.writeCount()
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq))
		Expect(f.src.writeCount()).To(Equal(writes))
	})

	It("terminates the echo cycle after a client edit", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1))

		// The egressed write is now visible in the source; reconcile must
		// recognize it as our own echo, not a foreign edit.
		seq := f.currentSeq(ctx, ws)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1), "no echo write")
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq), "no echo mutation")
	})

	It("repairs a fingerprint-only change without touching the engine", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Content untouched, fingerprint moved (mtime bump).
		f.src.touch(ws, "t1", "different-but-same-content")

		seq := f.currentSeq(ctx, ws)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq), "no engine write for a touch")

		sh, err := f.st.Shadow(ctx, ws, "todo", "t1")
		Expect(err).NotTo(HaveOccurred())
		Expect(sh.SourceFP).To(Equal("different-but-same-content"), "fingerprint repaired")

		// Subsequent passes are quiescent.
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1))
	})

	It("applies source-only edits when no client raced (plain drift)", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "edited in source"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1").GetTitle()).To(Equal("edited in source"))

		// The ingress must not echo back out.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1), "only the original egress ever wrote")
	})
})
