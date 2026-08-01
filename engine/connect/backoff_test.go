package connect_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

// The specs here drive real timers with millisecond durations, matching the
// rest of the suite: there is no clock seam in this package, and a runner's
// pacing is only observable through the passes it does or does not make.
var _ = Describe("Runner backoff", func() {
	const ws = "ws-backoff"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("coalesces every trigger that arrives inside the backoff window", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = -1 // triggers only, no ticks
			cfg.RetryMin = 400 * time.Millisecond
			cfg.RetryMax = 400 * time.Millisecond
		})
		src.failFor(ws, errors.New("the API is down"))

		Expect(f.conn.Start(ctx)).To(Succeed())
		f.conn.Kick(ws)
		Eventually(src.listCount).Should(Equal(1))

		// A burst of hints against a source that just failed must not turn
		// into a burst of calls.
		for i := 0; i < 5; i++ {
			f.conn.Kick(ws)
		}
		Consistently(src.listCount, 150*time.Millisecond, 20*time.Millisecond).Should(Equal(1))

		// The pending trigger fires once the window closes.
		Eventually(src.listCount, 2*time.Second).Should(BeNumerically(">=", 2))
	})

	It("waits exactly as long as the source asked", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = -1
			// Far shorter than the delay the source dictates below, so any
			// retry inside the window would have to have ignored it.
			cfg.RetryMin = 10 * time.Millisecond
			cfg.RetryMax = 10 * time.Millisecond
		})
		src.failFor(ws, engineconnect.RetryAfter(errors.New("429 slow down"), 5*time.Second))

		Expect(f.conn.Start(ctx)).To(Succeed())
		f.conn.Kick(ws)
		Eventually(src.listCount).Should(Equal(1))

		for i := 0; i < 5; i++ {
			f.conn.Kick(ws)
		}
		Consistently(src.listCount, 300*time.Millisecond, 25*time.Millisecond).Should(Equal(1))
	})

	It("attempts every trigger when backoff is disabled", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = -1
			cfg.RetryMin = -1
		})
		src.failFor(ws, errors.New("still down"))

		Expect(f.conn.Start(ctx)).To(Succeed())
		f.conn.Kick(ws)
		Eventually(src.listCount).Should(BeNumerically(">=", 1))
		Eventually(func() time.Time {
			h, _ := f.conn.Health(ws)
			return h.NextAttempt
		}).Should(BeZero(), "no window to wait out")

		for i := 0; i < 3; i++ {
			before := src.listCount()
			f.conn.Kick(ws)
			Eventually(src.listCount).Should(BeNumerically(">", before))
		}
	})

	It("leaves a healthy workspace alone while another backs off", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = -1
			cfg.RetryMin = time.Minute // ws-a is parked for the whole spec
		})
		src.failFor("ws-a", errors.New("only a is broken"))

		Expect(f.conn.Start(ctx)).To(Succeed())
		f.conn.Kick("ws-a")
		Eventually(func() int {
			h, _ := f.conn.Health("ws-a")
			return h.ConsecutiveFailures
		}).Should(BeNumerically(">=", 1))

		f.push(ctx, "ws-b", putMutation("c1", 1, "todo", "b1", &todov1.Todo{Id: "b1", Title: "healthy"}))
		f.conn.Kick("ws-b")
		Eventually(func() *todov1.Todo { return f.src.get("ws-b", "b1") }).ShouldNot(BeNil())

		h, _ := f.conn.Health("ws-b")
		Expect(h.Kind).To(Equal(engineconnect.FailureNone))
	})

	It("paces bulk ingress, and charges only for real writes", func() {
		const pause = 20 * time.Millisecond
		f := newFixture(func(cfg *engineconnect.Config) {
			cfg.IngestChunk = 1
			cfg.IngestPause = pause
		})
		for _, id := range []string{"t1", "t2", "t3"} {
			f.src.set(ws, &todov1.Todo{Id: id})
		}

		start := time.Now()
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(time.Since(start)).To(BeNumerically(">=", 3*pause))

		// Everything is unchanged now, so the second pass writes nothing and
		// pays nothing.
		start = time.Now()
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(time.Since(start)).To(BeNumerically("<", pause))
	})

	It("attempts a direct call even inside a backoff window", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = -1
			cfg.RetryMin = time.Minute
		})
		src.failFor(ws, errors.New("down"))
		Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
		Expect(src.listCount()).To(Equal(1))

		// The runner would be parked for a minute; the caller asked, so it runs.
		src.failFor(ws, nil)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(src.listCount()).To(Equal(2))
	})
})
