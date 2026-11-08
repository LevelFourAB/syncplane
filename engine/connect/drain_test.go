package connect_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// failingState wraps a State and fails the first PutShadow, simulating a
// crash between the source write and the bookkeeping update.
type failingState struct {
	engineconnect.State
	failed bool
}

var errInjected = errors.New("injected state failure")

func (f *failingState) PutShadow(ctx context.Context, ws, entity, key string, s engineconnect.Shadow) error {
	if !f.failed {
		f.failed = true
		return errInjected
	}
	return f.State.PutShadow(ctx, ws, entity, key, s)
}

var _ = Describe("Connector.Drain", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("egresses client pushes to the source exactly once at quiescence", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hello"}))

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("hello"))
		Expect(f.src.writeCount()).To(Equal(1))

		// The feed is dry; another drain writes nothing.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1))
	})

	It("pages through the feed with a small batch size", func() {
		f := newFixture(func(cfg *engineconnect.Config) { cfg.BatchSize = 2 })
		for i := 1; i <= 5; i++ {
			id := fmt.Sprintf("t%d", i)
			f.push(ctx, ws, putMutation("c1", uint64(i), "todo", id, &todov1.Todo{Id: id, Title: id}))
		}

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(5))
		for i := 1; i <= 5; i++ {
			Expect(f.src.get(ws, fmt.Sprintf("t%d", i))).NotTo(BeNil())
		}
	})

	It("suppresses changes the connector itself ingressed", func() {
		f := newFixture()
		// Simulate an ingress write the way Reconcile records it: mutate,
		// then stamp the shadow with the allocated seq.
		todo := &todov1.Todo{Id: "t1", Title: "from source"}
		var seq uint64
		Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			if err := tx.Put(todo); err != nil {
				return err
			}
			seq = tx.Seq()
			return nil
		})).To(Succeed())
		Expect(f.st.PutShadow(ctx, ws, "todo", "t1", engineconnect.Shadow{
			SourceFP:      "fp-source",
			EngineVersion: seq,
			Base:          mustMarshal(todo),
		})).To(Succeed())

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(BeZero(), "own ingress write must not echo back")
	})

	It("redelivers idempotently when the cursor update was lost", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hello"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1))

		// Crash simulation: cursor regressed to zero, shadow intact.
		Expect(f.st.SetCursor(ctx, ws, server.Cursor{})).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1), "shadow suppresses the redelivered change")
	})

	It("persists a cursor epoch and lineage mark across the whole 64-bit range", func() {
		// Both are random, so half of all of them set the high bit — which the
		// state store must carry through a signed column intact, or every drain
		// against such a store reads its own cursor as foreign.
		f := newFixture()
		cur := server.Cursor{Seq: 7, Entity: "todo", Key: "t1", Epoch: 1<<63 | 0x1234, Lineage: 1<<63 | 0x5678}
		Expect(f.st.SetCursor(ctx, ws, cur)).To(Succeed())
		Expect(f.st.Cursor(ctx, ws)).To(Equal(cur))
	})

	It("recovers when the shadow update was lost after the source write", func() {
		f := newFixtureWithState(func(st engineconnect.State) engineconnect.State {
			return &failingState{State: st}
		})
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hello"}))

		// First drain: source write lands, bookkeeping fails — the crash
		// window between Source.Write and PutShadow.
		Expect(f.conn.Drain(ctx, ws)).To(MatchError(errInjected))
		Expect(f.src.writeCount()).To(Equal(1))

		// Recovery drain: the change redelivers (cursor never advanced) and
		// the source upsert is idempotent.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(2), "redelivered as an idempotent upsert")
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("hello"))

		// And now it is quiescent.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(2))
	})

	It("keeps a client edit that was not egressed when the State is lost", func() {
		// Given a connector with a stable writer id, a row reconciled from the
		// source, and a client edit the connector has not drained yet
		f := newFixture(func(c *engineconnect.Config) { c.WriterID = "w1" })
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "source v1"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "client edit"}))

		// When the State loses the shadow and the egress cursor
		Expect(f.st.DeleteShadow(ctx, ws, "todo", "t1")).To(Succeed())
		Expect(f.st.SetCursor(ctx, ws, server.Cursor{})).To(Succeed())
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Then the edit survives in the engine and reaches the source
		Expect(f.engineTodo(ctx, ws, "t1").GetTitle()).To(Equal("client edit"),
			"a lost State costs a reconcile, never a committed edit")
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("client edit"))
	})

	It("takes a source edit made after the last egress when the State is lost", func() {
		// Given a connector with a stable writer id, and a client edit it
		// drained to the source
		f := newFixture(func(c *engineconnect.Config) { c.WriterID = "w1" })
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "source v1"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "client edit"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("client edit"))

		// When the source changes the row, and the State is lost
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "source v2"})
		Expect(f.st.DeleteShadow(ctx, ws, "todo", "t1")).To(Succeed())
		Expect(f.st.SetCursor(ctx, ws, server.Cursor{})).To(Succeed())
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Then the source's newer value wins on both sides
		Expect(f.engineTodo(ctx, ws, "t1").GetTitle()).To(Equal("source v2"))
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("source v2"))
	})

	It("rewrites nothing when the State is lost with both sides in step", func() {
		// Given a row identical on both sides and no pending client edit
		f := newFixture()
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "same"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		seq, writes := f.currentSeq(ctx, ws), f.src.writeCount()

		// When the State loses the shadow and the egress cursor
		Expect(f.st.DeleteShadow(ctx, ws, "todo", "t1")).To(Succeed())
		Expect(f.st.SetCursor(ctx, ws, server.Cursor{})).To(Succeed())
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		// Then neither side is written
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq), "no engine write")
		Expect(f.src.writeCount()).To(Equal(writes), "no source write")
		Expect(f.engineTodo(ctx, ws, "t1").GetTitle()).To(Equal("same"))
	})

	It("egresses deletes and clears the shadow", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hello"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).NotTo(BeNil())

		f.push(ctx, ws, deleteMutation("c1", 2, "todo", "t1"))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).To(BeNil())

		sh, err := f.st.Shadow(ctx, ws, "todo", "t1")
		Expect(err).NotTo(HaveOccurred())
		Expect(sh).To(BeNil())
	})

	It("skips entities outside the bound set", func() {
		f := newFixture()
		f.push(ctx, ws, putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t1", &todov1.Todo{Id: "t1"}))

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1), "only the bound todo egresses")

		// The cursor still advanced past the widget; nothing replays.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(1))
	})
})

// newFixtureWithState builds a fixture, wrapping the state store before the
// connector is constructed.
func newFixtureWithState(wrap func(engineconnect.State) engineconnect.State) *fixture {
	GinkgoHelper()
	return newFixture(func(cfg *engineconnect.Config) {
		cfg.State = wrap(cfg.State)
	})
}
