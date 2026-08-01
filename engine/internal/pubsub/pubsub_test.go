package pubsub_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/internal/pubsub/pubsubtest"
)

var _ = pubsubtest.Conformance("local", func() pubsub.Notifier {
	return pubsub.NewLocal()
})

// The wrapper is what every server actually subscribes through, so it has to
// satisfy the same seam as the notifier it wraps. The window is short enough
// that the conformance suite's Eventually budgets cover a trailing notify.
var _ = pubsubtest.Conformance("coalescing over local", func() pubsub.Notifier {
	n := pubsub.NewCoalescing(pubsub.NewLocal(), 5*time.Millisecond)
	DeferCleanup(n.(*pubsub.Coalescing).Close)
	return n
})

var _ = Describe("Local workspace activity", func() {
	It("queues every workspace notified while nothing is reading", func() {
		l := pubsub.NewLocal()
		ids, cancel, ok := pubsub.SubscribeActivity(l)
		Expect(ok).To(BeTrue())
		DeferCleanup(cancel)

		// Far more workspaces at once than any buffer would hold, with nothing
		// reading. Coalescing folds repeats of ONE workspace; it must not drop
		// distinct ones, because a dropped id is a workspace whose reaction
		// waits for the next sweep — and the sweep can be turned off.
		const total = 500
		for i := 0; i < total; i++ {
			l.Notify(fmt.Sprintf("ws-%d", i))
		}
		seen := map[string]struct{}{}
		for i := 0; i < total; i++ {
			var id string
			Eventually(ids).Should(Receive(&id))
			seen[id] = struct{}{}
		}
		Expect(seen).To(HaveLen(total))
	})

	It("delivers ids in the order they were notified", func() {
		l := pubsub.NewLocal()
		ids, cancel, ok := pubsub.SubscribeActivity(l)
		Expect(ok).To(BeTrue())
		DeferCleanup(cancel)

		// FIFO is fairness: a consumer draining this stream must not leave the
		// workspace that has been waiting longest behind a busier one.
		for i := 0; i < 5; i++ {
			l.Notify(fmt.Sprintf("ws-%d", i))
		}
		for i := 0; i < 5; i++ {
			Eventually(ids).Should(Receive(Equal(fmt.Sprintf("ws-%d", i))))
		}
	})
})
