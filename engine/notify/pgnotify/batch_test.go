package pgnotify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/pglisten"
)

// newBatchNotifier builds just enough of a Notifier to drive nextBatch and
// deliver without a transport.
func newBatchNotifier() *Notifier {
	c := &Notifier{
		logger: slog.Default(),
		local:  pubsub.NewLocal(),
		subs:   make(map[string]int),
	}
	c.q.init()
	return c
}

var _ = Describe("publish batching", func() {
	ctx := context.Background()

	It("carries a quiet queue's single key without the more field", func() {
		c := newBatchNotifier()
		c.q.push("app/a")
		body, ok := c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		Expect(body).To(MatchJSON(`{"ws":"app/a"}`))
	})

	It("drains queued keys into one payload in queue order", func() {
		c := newBatchNotifier()
		c.q.push("app/a")
		c.q.push("app/b")
		c.q.push("other/c")
		body, ok := c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		var p payload
		Expect(json.Unmarshal([]byte(body), &p)).To(Succeed())
		Expect(p).To(Equal(payload{WS: "app/a", More: []payload{
			{WS: "app/b"},
			{WS: "other/c"},
		}}))
	})

	It("splits an overfull queue into payloads under the NOTIFY limit", func() {
		c := newBatchNotifier()
		const total = 300
		// Ids with quotes and multibyte runes, so the sizing must account for
		// JSON escaping rather than trusting string length.
		want := make(map[string]struct{}, total)
		for i := 0; i < total; i++ {
			ws := fmt.Sprintf(`ws-%03d-"π"-%s`, i, strings.Repeat("x", 40))
			want[ws] = struct{}{}
			c.q.push(ws)
		}
		batches := 0
		for len(want) > 0 {
			body, ok := c.nextBatch(ctx)
			Expect(ok).To(BeTrue())
			Expect(len(body)).To(BeNumerically("<=", pglisten.MaxPayload))
			batches++
			var p payload
			Expect(json.Unmarshal([]byte(body), &p)).To(Succeed())
			for _, e := range append([]payload{{WS: p.WS}}, p.More...) {
				Expect(want).To(HaveKey(e.WS), "a key must not be published twice")
				delete(want, e.WS)
			}
		}
		Expect(batches).To(BeNumerically(">", 1), "the spec must actually overflow one payload")
	})

	It("drops a key whose id alone overflows a NOTIFY and keeps going", func() {
		c := newBatchNotifier()
		c.q.push(strings.Repeat("x", pglisten.MaxPayload))
		c.q.push("ok")
		body, ok := c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		Expect(body).To(BeEmpty())
		body, ok = c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		Expect(body).To(MatchJSON(`{"ws":"ok"}`))
	})

	It("returns ok=false once the context is done", func() {
		c := newBatchNotifier()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, ok := c.nextBatch(cancelled)
		Expect(ok).To(BeFalse())
	})

	It("fans a batched payload out to every entry's workspace", func() {
		c := newBatchNotifier()
		a, cancelA := c.local.Subscribe("app/a")
		DeferCleanup(cancelA)
		b, cancelB := c.local.Subscribe("app/b")
		DeferCleanup(cancelB)
		other, cancelOther := c.local.Subscribe("other/c")
		DeferCleanup(cancelOther)

		c.deliver(`{"ws":"app/a","more":[{"ws":"app/b"},{"ws":"other/c"}]}`)
		Expect(a).To(Receive())
		Expect(b).To(Receive())
		Expect(other).To(Receive())
	})
})
