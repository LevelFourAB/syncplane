package pubsub_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/internal/pubsub/pubsubtest"
)

var _ = pubsubtest.Conformance("prefixed over local", func() pubsub.Notifier {
	return pubsub.Prefixed(pubsub.NewLocal(), "app/")
})

// manualActivity is a notifier whose activity stream a spec drives by hand.
// Sending on an unbuffered channel makes the timing exact: the send returns
// only once the view's goroutine has taken the id and is trying to pass it on.
type manualActivity struct {
	pubsub.Notifier
	ids chan string
}

func (n *manualActivity) SubscribeActivity() (<-chan string, func()) {
	return n.ids, func() { close(n.ids) }
}

// noActivity hides the Activity extension of the notifier it embeds.
type noActivity struct{ pubsub.Notifier }

var _ = Describe("Prefixed", func() {
	It("keeps two partitions of one notifier apart", func() {
		shared := pubsub.NewLocal()
		a := pubsub.Prefixed(shared, "a/")
		b := pubsub.Prefixed(shared, "b/")

		inA, cancelA := a.Subscribe("team")
		DeferCleanup(cancelA)
		inB, cancelB := b.Subscribe("team")
		DeferCleanup(cancelB)

		b.Notify("team")
		Eventually(inB).Should(Receive())
		Consistently(inA, "50ms").ShouldNot(Receive())
	})

	It("reports a Set's members under the ids the caller named", func() {
		shared := pubsub.NewLocal()
		set := pubsub.Prefixed(shared, "a/").SubscribeSet([]string{"team", "other"})
		DeferCleanup(set.Close)

		shared.Notify("a/team")
		Eventually(set.Wake()).Should(Receive())
		Expect(set.Changed()).To(ConsistOf("team"))
	})

	It("unscopes the activity stream and drops other partitions", func() {
		n := &manualActivity{Notifier: pubsub.NewLocal(), ids: make(chan string)}
		ids, cancel, ok := pubsub.SubscribeActivity(pubsub.Prefixed(n, "app/"))
		Expect(ok).To(BeTrue())
		DeferCleanup(cancel)

		n.ids <- "other/ws"
		n.ids <- "app/"
		n.ids <- "app/mine"
		Eventually(ids).Should(Receive(Equal("mine")))
	})

	It("releases an activity stream nobody reads when cancelled", func() {
		n := &manualActivity{Notifier: pubsub.NewLocal(), ids: make(chan string)}
		ids, cancel, _ := pubsub.SubscribeActivity(pubsub.Prefixed(n, "app/"))

		// After this send the goroutine holds the id and is blocked passing it
		// on, because nothing reads ids.
		n.ids <- "app/ws"

		cancel()
		Eventually(ids).Should(BeClosed(), "cancel must not deliver the id it was holding")
	})

	It("reports no activity when the notifier below has none", func() {
		_, _, ok := pubsub.SubscribeActivity(pubsub.Prefixed(noActivity{pubsub.NewLocal()}, "app/"))
		Expect(ok).To(BeFalse())
	})

	It("finds activity through a wrapper below it", func() {
		inner := pubsub.NewCoalescing(pubsub.NewLocal(), 5*time.Millisecond)
		DeferCleanup(inner.(*pubsub.Coalescing).Close)
		_, cancel, ok := pubsub.SubscribeActivity(pubsub.Prefixed(inner, "app/"))
		Expect(ok).To(BeTrue())
		cancel()
	})

	It("returns the notifier unchanged for an empty prefix", func() {
		n := pubsub.NewLocal()
		Expect(pubsub.Prefixed(n, "")).To(BeIdenticalTo(n))
	})
})
