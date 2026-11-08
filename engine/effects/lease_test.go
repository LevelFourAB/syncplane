package effects_test

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/effects"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/server"
)

// droppingLocker is a Locker whose backend can lose every lease at once without
// telling the holders, as a Postgres lease session does when its connection
// drops: a holder keeps running its pass while another process acquires the
// same lease.
type droppingLocker struct {
	mu    sync.Mutex
	local *lease.Local
}

func newDroppingLocker() *droppingLocker { return &droppingLocker{local: lease.NewLocal()} }

func (d *droppingLocker) TryAcquire(ctx context.Context, role, workspaceID string) (func(), bool, error) {
	d.mu.Lock()
	l := d.local
	d.mu.Unlock()
	return l.TryAcquire(ctx, role, workspaceID)
}

// drop frees every lease held so far. Their release funcs stay callable and
// affect nothing.
func (d *droppingLocker) drop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.local = lease.NewLocal()
}

var _ = Describe("Effects leases", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("skips a pass whose lease another process holds, leaving the position alone", func() {
		locker := lease.NewLocal()
		f := newFixtureFull([]server.Option{server.WithLeases(locker)})
		rec := newRecorder()
		Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

		f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})

		// Stand in for another node mid-drain: hold this consumer's lease.
		// The role string is wire-format-adjacent — a renamed role would make
		// two versions of the engine drain the same consumer concurrently
		// during a rolling deploy — so it is spelled out rather than shared
		// with the implementation.
		release, ok, err := locker.TryAcquire(ctx, "effects/widgets", ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		Expect(f.rt.Drain(ctx, "widgets", ws)).To(MatchError(effects.ErrLeaseHeld))
		Expect(rec.keys()).To(BeEmpty(), "a skipped pass must not deliver")
		positions, err := f.srv.ConsumerPositions(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(positions).To(BeEmpty(), "a skipped pass must not advance the position")

		release()
		Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())
		Expect(rec.keys()).To(Equal([]string{"w1"}), "the freed lease hands the work back")
	})

	It("does not move the consumer back when a pass commits after losing its lease", func() {
		// Given two processes draining one consumer, and a pass on the first
		// that is parked in its handler with w1 "v2"
		locker := newDroppingLocker()
		f := newFixtureFull([]server.Option{server.WithLeases(locker)})
		other, err := effects.New(effects.Config{Server: f.srv})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(other.Close()).To(Succeed()) })

		gate := newBlocker(4)
		stale := newRecorder()
		Expect(effects.On(f.rt, "widgets", func(ctx context.Context, ev effects.Event[*testdatav1.Widget]) error {
			if err := gate.handle(ctx, ev); err != nil {
				return err
			}
			return stale.handle(ctx, ev)
		}, effects.Watch("label"))).To(Succeed())
		fresh := newRecorder()
		Expect(effects.On(other, "widgets", fresh.handle, effects.Watch("label"))).To(Succeed())

		f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "v1"})
		Expect(other.Drain(ctx, "widgets", ws)).To(Succeed())
		f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "v2"})

		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			_ = f.rt.Drain(ctx, "widgets", ws)
		}()
		Eventually(gate.entered).Should(Receive())

		// When the lease is lost, the other process delivers w1 "v3" and
		// commits past it, and only then the parked pass finishes
		locker.drop()
		f.put(ctx, ws, &testdatav1.Widget{Id: "w1", Label: "v3"})
		Expect(other.Drain(ctx, "widgets", ws)).To(Succeed())
		Expect(fresh.events()).To(HaveLen(2))
		Expect(fresh.events()[1].After.GetLabel()).To(Equal("v3"))
		head, _, err := f.srv.ConsumerPosition(ctx, ws, "widgets")
		Expect(err).NotTo(HaveOccurred())

		gate.releaseAll()
		Eventually(done).Should(BeClosed())

		// Then the position and shadows stay where the other process left them,
		// so a later pass does not deliver "v3" again against the older "v2"
		pos, _, err := f.srv.ConsumerPosition(ctx, ws, "widgets")
		Expect(err).NotTo(HaveOccurred())
		Expect(pos.Seq).To(Equal(head.Seq), "the stale pass must not move the position back")
		Expect(other.Drain(ctx, "widgets", ws)).To(Succeed())
		Expect(fresh.events()).To(HaveLen(2), "v3 was already delivered")
		Expect(stale.events()).To(HaveLen(1))
	})

	It("releases its lease when the pass finishes", func() {
		locker := lease.NewLocal()
		f := newFixtureFull([]server.Option{server.WithLeases(locker)})
		rec := newRecorder()
		Expect(effects.On(f.rt, "widgets", rec.handle)).To(Succeed())

		f.put(ctx, ws, &testdatav1.Widget{Id: "w1"})
		Expect(f.rt.Drain(ctx, "widgets", ws)).To(Succeed())

		release, ok, err := locker.TryAcquire(ctx, "effects/widgets", ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue(), "a finished pass must not keep holding its lease")
		release()
	})
})
