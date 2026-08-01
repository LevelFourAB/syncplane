package connect_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/text"
)

// ingressOnly rebinds the fixture's entity so nothing is written back.
func ingressOnly(prototype proto.Message) func(*engineconnect.Config) {
	return func(cfg *engineconnect.Config) {
		cfg.Entities = nil
		cfg.Bindings = []engineconnect.Binding{{Prototype: prototype, Direction: engineconnect.IngressOnly}}
	}
}

var _ = Describe("Ingress-only entities", func() {
	const ws = "ws-ingress-only"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("never writes engine changes back to the source", func() {
		f := newFixture(ingressOnly(&todov1.Todo{}))
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "from client"}))

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).To(BeNil())
		Expect(f.src.writeCount()).To(BeZero())

		// Deletes are egress too.
		f.push(ctx, ws, deleteMutation("c1", 2, "todo", "t1"))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.deleteCount()).To(BeZero())
	})

	It("still ingests source edits and source deletions", func() {
		f := newFixture(ingressOnly(&todov1.Todo{}))
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "from source"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1").GetTitle()).To(Equal("from source"))

		f.src.remove(ws, "t1")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).To(BeNil(), "the deletion sweep is ingress, and still runs")
	})

	// The shadow base for an ingress-only row is the SOURCE's view, not the
	// merged engine row. Store the merge instead and the next source edit
	// would diff against fields the source has never seen and clear them.
	It("keeps a client-only field across an unrelated source edit", func() {
		f := newFixture(ingressOnly(&todov1.Todo{}))
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "a"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		// A client edits a field the source does not carry.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "a", Completed: true}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Two rounds: the second proves the base did not drift on the first.
		for _, title := range []string{"b", "c"} {
			f.src.set(ws, &todov1.Todo{Id: "t1", Title: title})
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			got := f.engineTodo(ctx, ws, "t1")
			Expect(got.GetTitle()).To(Equal(title), "the source wins the field it changed")
			Expect(got.GetCompleted()).To(BeTrue(), "and touches nothing else")
		}
	})

	It("accepts a source that cannot write when nothing is bidirectional", func() {
		f := newFixtureOver(nil, &readOnlyTodoSource{inner: newTodoSource()}, ingressOnly(&todov1.Todo{}))
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "read only"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
	})

	It("rejects a source that cannot write when something is bidirectional", func() {
		f := newFixture()
		_, err := engineconnect.New(engineconnect.Config{
			Server:   f.srv,
			Source:   &readOnlyTodoSource{inner: newTodoSource()},
			State:    f.st,
			Entities: []proto.Message{&todov1.Todo{}},
		})
		Expect(err).To(MatchError(ContainSubstring("Writer")))
	})
})

var _ = Describe("Ingress-only collaborative text", func() {
	const ws = "ws-ingress-only-text"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// Without write-back there is no shared ancestor, so the rule is narrower
	// than the bidirectional three-way: leave the field alone while the
	// source's string is unchanged, and let the source win the characters it
	// touches when it does change.
	It("leaves the field alone while the source's string is unchanged", func() {
		f := newNoteFixtureOpts(ingressOnly(&todov1.Note{}))
		f.src.edit(ws, "n1", "hello")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.engineEdit(ctx, ws, "n1", "client-1", func(d *text.Doc) {
			Expect(d.Insert(5, " world")).To(Succeed())
		})
		// The body did not change, only its stamp — the pass must still not
		// read the engine's extra text as something to overwrite.
		f.src.touch(ws, "n1", "restamped")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("hello world"))

		// And nothing goes back out.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.at(ws, "n1")).To(Equal("hello"))
	})

	It("lets the source win when it does change the string", func() {
		f := newNoteFixtureOpts(ingressOnly(&todov1.Note{}))
		f.src.edit(ws, "n1", "hello")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.engineEdit(ctx, ws, "n1", "client-1", func(d *text.Doc) {
			Expect(d.Insert(5, " world")).To(Succeed())
		})

		f.src.edit(ws, "n1", "goodbye")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("goodbye"))

		// Applied as a minimal diff onto the live document, under the
		// connector's one identity — not a wholesale replacement.
		Expect(f.engineDoc(ctx, ws, "n1").VersionVector()).To(HaveLen(2))
	})
})
