package pubsub_test

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
)

// recordingNotifier counts Notify calls per workspace.
type recordingNotifier struct {
	mu    sync.Mutex
	count map[string]int
}

func newRecordingNotifier() *recordingNotifier {
	return &recordingNotifier{count: map[string]int{}}
}

func (n *recordingNotifier) Notify(workspaceID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.count[workspaceID]++
}

func (n *recordingNotifier) Subscribe(string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() { close(ch) }
}

func (n *recordingNotifier) SubscribeSet(workspaceIDs []string) pubsub.Set {
	return pubsub.NewLocal().SubscribeSet(workspaceIDs)
}

func (n *recordingNotifier) notifies(workspaceID string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.count[workspaceID]
}

var _ = Describe("Coalescing", func() {
	const window = 50 * time.Millisecond

	var (
		inner *recordingNotifier
		c     pubsub.Notifier
	)

	BeforeEach(func() {
		inner = newRecordingNotifier()
		c = pubsub.NewCoalescing(inner, window)
	})

	It("passes the first notify through synchronously", func() {
		c.Notify("ws")
		Expect(inner.notifies("ws")).To(Equal(1), "leading edge: no added latency on a quiet workspace")
	})

	It("collapses a burst into a leading and one trailing notify", func() {
		for i := 0; i < 10; i++ {
			c.Notify("ws")
		}
		Expect(inner.notifies("ws")).To(Equal(1), "burst within the window must not fan out")
		Eventually(func() int { return inner.notifies("ws") }).Should(Equal(2), "the burst's tail is owed one trailing notify")
		Consistently(func() int { return inner.notifies("ws") }, 3*window).Should(Equal(2), "no further notifies once the burst is flushed")
	})

	It("returns to immediate delivery once the cooldown passes clean", func() {
		c.Notify("ws")
		Eventually(func() int {
			c.Notify("ws") // keeps probing until a cooldown has fully expired
			return inner.notifies("ws")
		}, time.Second, 2*window).Should(BeNumerically(">=", 2))
	})

	It("keeps workspaces independent", func() {
		c.Notify("ws-a")
		c.Notify("ws-a")
		c.Notify("ws-b")
		Expect(inner.notifies("ws-a")).To(Equal(1))
		Expect(inner.notifies("ws-b")).To(Equal(1), "ws-a's cooldown must not delay ws-b")
	})

	It("caps a sustained write stream at one notify per window", func() {
		deadline := time.Now().Add(4 * window)
		for time.Now().Before(deadline) {
			c.Notify("ws")
			time.Sleep(window / 10)
		}
		Eventually(func() int { return inner.notifies("ws") }).Should(BeNumerically(">=", 2))
		// ~40 notifies went in over 4 windows; at most one per window plus
		// the leading and trailing edges may come out.
		Expect(inner.notifies("ws")).To(BeNumerically("<=", 7))
	})

	It("is a passthrough when the window is zero", func() {
		Expect(pubsub.NewCoalescing(inner, 0)).To(BeIdenticalTo(pubsub.Notifier(inner)))
	})

	Describe("Close", func() {
		It("drops the trailing notify owed at close time", func() {
			c.Notify("ws")
			c.Notify("ws") // owes a trailing notify one window from now
			Expect(c.(*pubsub.Coalescing).Close()).To(Succeed())
			Consistently(func() int { return inner.notifies("ws") }, 4*window).Should(Equal(1),
				"a shared notifier must not see a publish from a shut-down process")
		})

		It("passes a notify racing shutdown straight through", func() {
			Expect(c.(*pubsub.Coalescing).Close()).To(Succeed())
			c.Notify("ws")
			c.Notify("ws")
			Expect(inner.notifies("ws")).To(Equal(2), "no timers left to coalesce on; losing the signal is worse than uncapped")
		})

		It("is idempotent", func() {
			cc := c.(*pubsub.Coalescing)
			c.Notify("ws")
			Expect(cc.Close()).To(Succeed())
			Expect(cc.Close()).To(Succeed())
		})
	})

	It("delegates Subscribe to the wrapped notifier", func() {
		local := pubsub.NewLocal()
		wrapped := pubsub.NewCoalescing(local, window)
		signals, cancel := wrapped.Subscribe("ws")
		defer cancel()
		wrapped.Notify("ws")
		Eventually(signals).Should(Receive())
	})
})
