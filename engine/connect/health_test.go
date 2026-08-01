package connect_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

var _ = Describe("Connector health", func() {
	const ws = "ws-health"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("reports nothing for a workspace no pass has touched", func() {
		f := newFixture()
		_, ok := f.conn.Health("ws-never-seen")
		Expect(ok).To(BeFalse())
		Expect(f.conn.HealthAll()).To(BeEmpty())
	})

	It("classifies a transient failure and counts it", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src)
		boom := errors.New("connection reset")
		src.failFor(ws, boom)

		Expect(f.conn.Ingress(ctx, ws)).To(MatchError(boom))
		h, ok := f.conn.Health(ws)
		Expect(ok).To(BeTrue())
		Expect(h.WorkspaceID).To(Equal(ws))
		Expect(h.Kind).To(Equal(engineconnect.FailureTransient))
		Expect(h.LastError).To(MatchError(boom))
		Expect(h.ConsecutiveFailures).To(Equal(1))
		Expect(h.LastAttempt).NotTo(BeZero())
		Expect(h.LastSuccess).To(BeZero())
		Expect(h.NextAttempt).To(BeTemporally(">", h.LastAttempt))

		Expect(f.conn.Ingress(ctx, ws)).To(MatchError(boom))
		h, _ = f.conn.Health(ws)
		Expect(h.ConsecutiveFailures).To(Equal(2))
	})

	It("classifies an expired credential as needing user action", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src)
		src.failFor(ws, fmt.Errorf("refresh token rejected: %w", engineconnect.ErrNeedsUserAction))

		Expect(f.conn.Ingress(ctx, ws)).NotTo(Succeed())
		h, _ := f.conn.Health(ws)
		Expect(h.Kind).To(Equal(engineconnect.FailureUserAction))
		Expect(h.LastError).To(MatchError(engineconnect.ErrNeedsUserAction))
	})

	It("clears the failure once a pass gets through", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src)
		src.failFor(ws, errors.New("timeout"))
		Expect(f.conn.Ingress(ctx, ws)).NotTo(Succeed())

		src.failFor(ws, nil)
		src.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Ingress(ctx, ws)).To(Succeed())

		h, _ := f.conn.Health(ws)
		Expect(h.LastError).To(BeNil())
		Expect(h.Kind).To(Equal(engineconnect.FailureNone))
		Expect(h.ConsecutiveFailures).To(BeZero())
		Expect(h.NextAttempt).To(BeZero())
		Expect(h.LastSuccess).NotTo(BeZero())
	})

	It("fires OnHealth after every pass, successful or not", func() {
		var (
			mu   sync.Mutex
			seen []engineconnect.Health
		)
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.OnHealth = func(h engineconnect.Health) {
				mu.Lock()
				defer mu.Unlock()
				seen = append(seen, h)
			}
		})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		src.failFor(ws, errors.New("nope"))
		Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(seen).To(HaveLen(2))
		Expect(seen[0].Kind).To(Equal(engineconnect.FailureNone))
		Expect(seen[1].Kind).To(Equal(engineconnect.FailureTransient))
	})

	It("keeps one entry per workspace, ordered", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src)
		src.failFor("ws-b", errors.New("only b is broken"))

		Expect(f.conn.Reconcile(ctx, "ws-b")).NotTo(Succeed())
		Expect(f.conn.Reconcile(ctx, "ws-a")).To(Succeed())

		all := f.conn.HealthAll()
		Expect(all).To(HaveLen(2))
		Expect(all[0].WorkspaceID).To(Equal("ws-a"))
		Expect(all[0].Kind).To(Equal(engineconnect.FailureNone))
		Expect(all[1].WorkspaceID).To(Equal("ws-b"))
		Expect(all[1].Kind).To(Equal(engineconnect.FailureTransient))
	})

	It("honours a Retry-After the source dictated, verbatim", func() {
		src := newFlakyTodoSource()
		f := newFixtureOver(nil, src)
		src.failFor(ws, engineconnect.RetryAfter(errors.New("429 slow down"), 30*time.Second))

		Expect(f.conn.Ingress(ctx, ws)).NotTo(Succeed())
		h, _ := f.conn.Health(ws)
		// No jitter, no doubling: the source knows when its quota resets.
		Expect(h.NextAttempt.Sub(h.LastAttempt)).To(BeNumerically("~", 30*time.Second, time.Second))
	})
})
