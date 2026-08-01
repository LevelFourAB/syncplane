package connect_test

import (
	"context"
	"slices"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
)

// gatedTodoSource holds every call inside the source for a beat and records
// how many were in there at once — in total, and for any single workspace.
// The two peaks are the evidence for the connector's two concurrency claims:
// Config.Workers bounds the fleet, and one workspace never runs two passes at
// a time however it is triggered.
type gatedTodoSource struct {
	*todoSource
	delay time.Duration

	mu     sync.Mutex
	live   int
	peak   int
	liveWS map[string]int
	peakWS int
	calls  int
}

func newGatedTodoSource(delay time.Duration) *gatedTodoSource {
	return &gatedTodoSource{
		todoSource: newTodoSource(),
		delay:      delay,
		liveWS:     make(map[string]int),
	}
}

func (s *gatedTodoSource) enter(ws string) {
	s.mu.Lock()
	s.calls++
	s.live++
	s.liveWS[ws]++
	if s.live > s.peak {
		s.peak = s.live
	}
	if s.liveWS[ws] > s.peakWS {
		s.peakWS = s.liveWS[ws]
	}
	s.mu.Unlock()
	time.Sleep(s.delay)
}

func (s *gatedTodoSource) leave(ws string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live--
	s.liveWS[ws]--
}

func (s *gatedTodoSource) stats() (peak, peakWS, calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak, s.peakWS, s.calls
}

func (s *gatedTodoSource) List(ctx context.Context, ws string, fn func(engineconnect.Item) error) error {
	s.enter(ws)
	defer s.leave(ws)
	return s.todoSource.List(ctx, ws, fn)
}

func (s *gatedTodoSource) Write(ctx context.Context, ws string, msg proto.Message) (string, error) {
	s.enter(ws)
	defer s.leave(ws)
	return s.todoSource.Write(ctx, ws, msg)
}

// staticWorkspaces returns a Workspaces func over a list a spec can rewrite
// mid-run, which is how a workspace stops being managed.
func staticWorkspaces(ids *[]string, mu *sync.Mutex) func(context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(*ids), nil
	}
}

var _ = Describe("Pass scheduling", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("runs no more passes at once than Config.Workers", func() {
		ids := make([]string, 24)
		for i := range ids {
			ids[i] = "ws-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		}
		var mu sync.Mutex
		src := newGatedTodoSource(5 * time.Millisecond)
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.Workers = 2
			cfg.ReconcileInterval = 10 * time.Millisecond
			cfg.Workspaces = staticWorkspaces(&ids, &mu)
		})
		Expect(f.conn.Start(ctx)).To(Succeed())

		// Every workspace has been through a pass, several times over.
		Eventually(func() int {
			_, _, calls := src.stats()
			return calls
		}).Should(BeNumerically(">=", 2*len(ids)))

		peak, _, _ := src.stats()
		Expect(peak).To(BeNumerically("<=", 2),
			"the worker pool is the only thing between a fleet of workspaces and the source")
	})

	It("never runs two passes for one workspace at once", func() {
		const ws = "ws-serial"
		ids := []string{ws}
		var mu sync.Mutex
		src := newGatedTodoSource(5 * time.Millisecond)
		f := newFixtureOver(nil, src, func(cfg *engineconnect.Config) {
			cfg.Workers = 8
			cfg.ReconcileInterval = time.Millisecond
			cfg.Workspaces = staticWorkspaces(&ids, &mu)
		})
		Expect(f.conn.Start(ctx)).To(Succeed())

		// Hand-driven passes race the background ones for the same workspace:
		// both go through converge, so both take its turn.
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			}()
		}
		wg.Wait()

		_, peakWS, calls := src.stats()
		Expect(calls).To(BeNumerically(">=", 8))
		Expect(peakWS).To(Equal(1))
	})
})

var _ = Describe("The managed set", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("drops a workspace Config.Workspaces stops naming", func() {
		ids := []string{"ws-a", "ws-b"}
		var mu sync.Mutex
		opener := newSourceOpener()
		f := newFixtureOver(nil, opener.store, useOpener(opener), func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = 10 * time.Millisecond
			cfg.Workspaces = staticWorkspaces(&ids, &mu)
		})
		Expect(f.conn.Start(ctx)).To(Succeed())
		Eventually(f.conn.Tracked).Should(Equal(2))
		Eventually(func() int { opens, _ := opener.counts("ws-b"); return opens }).Should(Equal(1))

		// The account is disconnected, the workspace deleted — whatever made
		// the list stop naming it.
		mu.Lock()
		ids = []string{"ws-a"}
		mu.Unlock()

		Eventually(f.conn.Tracked).Should(Equal(1), "a sweep drops what the list no longer names")
		Eventually(func() int { _, closes := opener.counts("ws-b"); return closes }).
			Should(Equal(1), "and releases its source with it")
		Eventually(func() bool { _, ok := f.conn.Health("ws-b"); return ok }).
			Should(BeFalse(), "leaving no stale status behind")

		// The one still listed keeps converging.
		opener.store.set("ws-a", &todov1.Todo{Id: "t1", Title: "still here"})
		Eventually(func() *todov1.Todo { return f.engineTodo(ctx, "ws-a", "t1") }).Should(Not(BeNil()))
	})

	It("keeps a Kicked workspace the list never names", func() {
		const ws = "ws-announced"
		var ids []string
		var mu sync.Mutex
		f := newFixture(func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = 10 * time.Millisecond
			cfg.Workspaces = staticWorkspaces(&ids, &mu)
		})
		Expect(f.conn.Start(ctx)).To(Succeed())

		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "source only"})
		f.conn.Kick(ws)
		Eventually(func() *todov1.Todo { return f.engineTodo(ctx, ws, "t1") }).Should(Not(BeNil()))

		// Sweeps keep running against a list that has never named it, and it
		// is still managed and still picking up source edits.
		Consistently(f.conn.Tracked, 100*time.Millisecond).Should(Equal(1))
		f.src.set(ws, &todov1.Todo{Id: "t2", Title: "later"})
		Eventually(func() *todov1.Todo { return f.engineTodo(ctx, ws, "t2") }).Should(Not(BeNil()))
	})

	It("ignores commits in a workspace it does not manage", func() {
		ids := []string{"ws-in"}
		var mu sync.Mutex
		f := newFixture(func(cfg *engineconnect.Config) {
			cfg.ReconcileInterval = 10 * time.Millisecond
			cfg.Workspaces = staticWorkspaces(&ids, &mu)
		})
		Expect(f.conn.Start(ctx)).To(Succeed())

		// Both commit, and the activity stream carries both — it reports every
		// workspace in the store, not just the ones this connector manages.
		f.push(ctx, "ws-in", putMutation("c1", 1, "todo", "in1", &todov1.Todo{Id: "in1"}))
		f.push(ctx, "ws-out", putMutation("c1", 1, "todo", "out1", &todov1.Todo{Id: "out1"}))

		Eventually(func() *todov1.Todo { return f.src.get("ws-in", "in1") }).Should(Not(BeNil()))
		Consistently(func() *todov1.Todo { return f.src.get("ws-out", "out1") }, 100*time.Millisecond).
			Should(BeNil(), "Config.Workspaces is what decides, not the stream")
		Expect(f.conn.Tracked()).To(Equal(1))
	})
})
