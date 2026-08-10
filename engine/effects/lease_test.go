package effects_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/effects"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/lease"
	"github.com/levelfourab/syncplane/engine/server"
)

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
