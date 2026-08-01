package connect_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Connector lifecycle", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("converges both directions while running", func() {
		const ws = "ws-live"
		f := newFixture(func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = 20 * time.Millisecond
		})
		Expect(f.conn.Start(ctx)).To(Succeed())

		// Engine → source, driven by the commit signal.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "from client"}))
		Eventually(func() *todov1.Todo {
			return f.src.get(ws, "t1")
		}).Should(Not(BeNil()))
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("from client"))

		// Source → engine, driven by Kick.
		f.src.set(ws, &todov1.Todo{Id: "t2", Title: "from source"})
		f.conn.Kick(ws)
		Eventually(func() *todov1.Todo {
			return f.engineTodo(ctx, ws, "t2")
		}).Should(Not(BeNil()))
	})

	It("exports rows that existed before Start (engine-first initial sync)", func() {
		const ws = "ws-preexisting"
		f := newFixture()
		// Rows committed long before the connector ever runs — the
		// "layer sync over my existing database" case.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "old data"}))
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "older data"}))

		Expect(f.conn.Start(ctx)).To(Succeed())
		Eventually(func() *todov1.Todo {
			return f.src.get(ws, "t1")
		}).Should(Not(BeNil()), "initial discovery must drain pre-existing rows")
		Eventually(func() *todov1.Todo {
			return f.src.get(ws, "t2")
		}).Should(Not(BeNil()))
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("old data"))
	})

	It("starts a runner for a source-first workspace via Kick", func() {
		const ws = "ws-source-first"
		f := newFixture(func(cfg *engineconnect.Config) {
			// Periodic reconcile off: only the Kick may trigger the import.
			cfg.ReconcileInterval = -1
		})
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "pre-existing"})

		Expect(f.conn.Start(ctx)).To(Succeed())
		f.conn.Kick(ws)
		Eventually(func() *todov1.Todo {
			return f.engineTodo(ctx, ws, "t1")
		}).Should(Not(BeNil()))
	})

	It("rejects a second Start and stops cleanly on Close", func() {
		f := newFixture()
		Expect(f.conn.Start(ctx)).To(Succeed())
		Expect(f.conn.Start(ctx)).To(MatchError(ContainSubstring("twice")))
		// Close is exercised by the fixture's DeferCleanup; Kick after Close
		// must not panic.
		Expect(f.conn.Close()).To(Succeed())
		f.conn.Kick("ws-any")
	})
})

// useOpener rewires a fixture to mint one source per workspace.
func useOpener(o *sourceOpener) func(*engineconnect.Config) {
	return func(cfg *engineconnect.Config) {
		cfg.Source = nil
		cfg.OpenSource = o.open
	}
}

var _ = Describe("Per-workspace sources", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("opens once per workspace and closes on Close", func() {
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))

		Expect(f.conn.Reconcile(ctx, "ws-a")).To(Succeed())
		Expect(f.conn.Ingress(ctx, "ws-a")).To(Succeed())
		Expect(f.conn.Drain(ctx, "ws-a")).To(Succeed())
		opens, closes := opener.counts("ws-a")
		Expect(opens).To(Equal(1), "a source is built once and reused across passes")
		Expect(closes).To(BeZero())

		Expect(f.conn.Reconcile(ctx, "ws-b")).To(Succeed())
		opens, _ = opener.counts("ws-b")
		Expect(opens).To(Equal(1), "and every workspace gets its own")

		Expect(f.conn.Close()).To(Succeed())
		_, closes = opener.counts("ws-a")
		Expect(closes).To(Equal(1))
		_, closes = opener.counts("ws-b")
		Expect(closes).To(Equal(1))
	})

	It("closes the workspace's source on Forget and reopens it on demand", func() {
		const ws = "ws-forget"
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.conn.Forget(ws)
		opens, closes := opener.counts(ws)
		Expect(opens).To(Equal(1))
		Expect(closes).To(Equal(1))
		_, ok := f.conn.Health(ws)
		Expect(ok).To(BeFalse(), "a forgotten workspace leaves no stale status behind")

		// Shadows survive, so coming back resumes rather than re-imports.
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		opens, _ = opener.counts(ws)
		Expect(opens).To(Equal(2))
	})

	// The re-consent seam: a credential that will not heal by retrying drops
	// the cached source, so whatever a reconnect flow stored is picked up on
	// the next pass without restarting the process.
	It("rebuilds the source after a pass needs user action", func() {
		const ws = "ws-reauth"
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		opener.failList(errors.New("connection reset"))
		Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
		opens, closes := opener.counts(ws)
		Expect(opens).To(Equal(1), "a transient failure keeps the source")
		Expect(closes).To(BeZero())

		opener.failList(fmt.Errorf("refresh token revoked: %w", engineconnect.ErrNeedsUserAction))
		Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
		opens, closes = opener.counts(ws)
		Expect(opens).To(Equal(1))
		Expect(closes).To(Equal(1), "a credential failure drops it")

		opener.failList(nil)
		opener.store.set(ws, &todov1.Todo{Id: "t1"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		opens, _ = opener.counts(ws)
		Expect(opens).To(Equal(2))
		Expect(f.engineTodo(ctx, ws, "t1")).NotTo(BeNil())
	})

	It("reports a source that cannot be opened at all", func() {
		const ws = "ws-noauth"
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener))
		opener.failWith(fmt.Errorf("no stored credential: %w", engineconnect.ErrNeedsUserAction))

		Expect(f.conn.Ingress(ctx, ws)).To(MatchError(engineconnect.ErrNeedsUserAction))
		h, ok := f.conn.Health(ws)
		Expect(ok).To(BeTrue())
		Expect(h.Kind).To(Equal(engineconnect.FailureUserAction))
		opens, _ := opener.counts(ws)
		Expect(opens).To(BeZero())
	})
})

var _ = Describe("Cursor-too-old recovery", func() {
	const ws = "ws-gc"
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("heals engine-side deletes and creates the feed lost to GC", func() {
		f := newFixtureFull([]server.Option{server.WithGC(-time.Second, 0)})

		// Converged baseline: t1 exists on both sides, cursor past seq 1.
		f.push(ctx, ws, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).NotTo(BeNil())

		// While the connector is "offline": t2 is created, t1 is deleted,
		// and GC collects the tombstone — the feed below the horizon is gone.
		f.push(ctx, ws, putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
		f.push(ctx, ws, deleteMutation("c1", 3, "todo", "t1"))
		Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())

		// The next drain hits the horizon and falls back to a full repair.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.get(ws, "t1")).To(BeNil(), "the GC-collected delete reached the source")
		Expect(f.src.get(ws, "t2")).NotTo(BeNil(), "the skipped create reached the source")

		// Converged and quiescent afterwards.
		writes := f.src.writeCount()
		seq := f.currentSeq(ctx, ws)
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))
		Expect(f.currentSeq(ctx, ws)).To(Equal(seq))
	})
})

var _ = Describe("Multiple workspaces", func() {
	It("drain one workspace without touching another", func() {
		ctx := context.Background()
		f := newFixture()
		f.push(ctx, "ws-a", putMutation("c1", 1, "todo", "a1", &todov1.Todo{Id: "a1"}))
		f.push(ctx, "ws-b", putMutation("c1", 1, "todo", "b1", &todov1.Todo{Id: "b1"}))

		Expect(f.conn.Drain(ctx, "ws-a")).To(Succeed())
		Expect(f.src.get("ws-a", "a1")).NotTo(BeNil())
		Expect(f.src.get("ws-b", "b1")).To(BeNil(), "ws-b has not drained")

		Expect(f.conn.Drain(ctx, "ws-b")).To(Succeed())
		Expect(f.src.get("ws-b", "b1")).NotTo(BeNil())
	})
})
