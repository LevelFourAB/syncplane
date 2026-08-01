package connect_test

import (
	"context"
	"fmt"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

// Convergence property: whatever interleaving of client writes, source
// edits and partial sync passes occurs, a settled system has identical
// state on both sides and goes fully quiescent — no echo loops, no lost
// writes, no resurrections.
var _ = Describe("Convergence under random interleavings", func() {
	const ws = "ws-fuzz"

	It("reaches identical, quiescent state on both sides", func() {
		ctx := context.Background()
		f := newFixture()
		rng := rand.New(rand.NewSource(GinkgoRandomSeed()))

		ids := []string{"a", "b", "c", "d", "e"}
		var mutationID uint64
		for step := 0; step < 80; step++ {
			id := ids[rng.Intn(len(ids))]
			switch rng.Intn(6) {
			case 0, 1: // client upsert
				mutationID++
				f.push(ctx, ws, putMutation("c1", mutationID, "todo", id,
					&todov1.Todo{Id: id, Title: fmt.Sprintf("client-%d", step), Completed: rng.Intn(2) == 0}))
			case 2: // client delete
				mutationID++
				f.push(ctx, ws, deleteMutation("c1", mutationID, "todo", id))
			case 3: // source upsert (behind the engine's back)
				f.src.set(ws, &todov1.Todo{Id: id, Title: fmt.Sprintf("source-%d", step), Completed: rng.Intn(2) == 0})
			case 4: // source delete
				f.src.remove(ws, id)
			case 5: // a sync pass runs mid-stream
				Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
				Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			}
		}

		// Settle: passes until quiescent (bounded — divergence fails fast).
		settled := false
		for i := 0; i < 5 && !settled; i++ {
			seq := f.currentSeq(ctx, ws)
			writes, deletes := f.src.writeCount(), f.src.deleteCount()
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			settled = f.currentSeq(ctx, ws) == seq &&
				f.src.writeCount() == writes && f.src.deleteCount() == deletes
		}
		Expect(settled).To(BeTrue(), "system keeps churning after 5 settle passes (echo loop?) seed=%d", GinkgoRandomSeed())

		// Both sides hold identical state.
		for _, id := range ids {
			inEngine := f.engineTodo(ctx, ws, id)
			inSource := f.src.get(ws, id)
			if inEngine == nil {
				Expect(inSource).To(BeNil(), "id %s: engine deleted but source kept it (seed=%d)", id, GinkgoRandomSeed())
				continue
			}
			Expect(inSource).NotTo(BeNil(), "id %s: source deleted but engine kept it (seed=%d)", id, GinkgoRandomSeed())
			Expect(proto.Equal(inEngine, inSource)).To(BeTrue(),
				"id %s diverged (seed=%d): engine=%v source=%v", id, GinkgoRandomSeed(), inEngine, inSource)
		}
	})
})
