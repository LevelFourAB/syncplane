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

// newBatchCore builds just enough of a core to drive nextBatch and deliver
// without a transport.
func newBatchCore() *core {
	c := &core{
		logger: slog.Default(),
		locals: make(map[string]*pubsub.Local),
		subs:   make(map[nsWorkspace]int),
	}
	c.q.init()
	return c
}

var _ = Describe("publish batching", func() {
	ctx := context.Background()

	It("carries a quiet queue's single key without the more field", func() {
		c := newBatchCore()
		c.q.push(nsWorkspace{ns: "app", ws: "a"})
		body, ok := c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		Expect(body).To(MatchJSON(`{"ns":"app","ws":"a"}`))
	})

	It("drains queued keys into one payload in queue order", func() {
		c := newBatchCore()
		c.q.push(nsWorkspace{ns: "app", ws: "a"})
		c.q.push(nsWorkspace{ns: "app", ws: "b"})
		c.q.push(nsWorkspace{ns: "other", ws: "c"})
		body, ok := c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		var p payload
		Expect(json.Unmarshal([]byte(body), &p)).To(Succeed())
		Expect(p).To(Equal(payload{NS: "app", WS: "a", More: []payload{
			{NS: "app", WS: "b"},
			{NS: "other", WS: "c"},
		}}))
	})

	It("splits an overfull queue into payloads under the NOTIFY limit", func() {
		c := newBatchCore()
		const total = 300
		// Ids with quotes and multibyte runes, so the sizing must account for
		// JSON escaping rather than trusting string length.
		want := make(map[string]struct{}, total)
		for i := 0; i < total; i++ {
			ws := fmt.Sprintf(`ws-%03d-"π"-%s`, i, strings.Repeat("x", 40))
			want[ws] = struct{}{}
			c.q.push(nsWorkspace{ns: "app", ws: ws})
		}
		batches := 0
		for len(want) > 0 {
			body, ok := c.nextBatch(ctx)
			Expect(ok).To(BeTrue())
			Expect(len(body)).To(BeNumerically("<=", pglisten.MaxPayload))
			batches++
			var p payload
			Expect(json.Unmarshal([]byte(body), &p)).To(Succeed())
			for _, e := range append([]payload{{NS: p.NS, WS: p.WS}}, p.More...) {
				Expect(want).To(HaveKey(e.WS), "a key must not be published twice")
				delete(want, e.WS)
			}
		}
		Expect(batches).To(BeNumerically(">", 1), "the spec must actually overflow one payload")
	})

	It("drops a key whose id alone overflows a NOTIFY and keeps going", func() {
		c := newBatchCore()
		c.q.push(nsWorkspace{ns: "app", ws: strings.Repeat("x", pglisten.MaxPayload)})
		c.q.push(nsWorkspace{ns: "app", ws: "ok"})
		body, ok := c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		Expect(body).To(BeEmpty())
		body, ok = c.nextBatch(ctx)
		Expect(ok).To(BeTrue())
		Expect(body).To(MatchJSON(`{"ns":"app","ws":"ok"}`))
	})

	It("returns ok=false once the context is done", func() {
		c := newBatchCore()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, ok := c.nextBatch(cancelled)
		Expect(ok).To(BeFalse())
	})

	It("fans a batched payload out to every entry's namespace", func() {
		c := newBatchCore()
		a, cancelA := c.localFor("app").Subscribe("a")
		DeferCleanup(cancelA)
		b, cancelB := c.localFor("app").Subscribe("b")
		DeferCleanup(cancelB)
		other, cancelOther := c.localFor("other").Subscribe("c")
		DeferCleanup(cancelOther)

		c.deliver(`{"ns":"app","ws":"a","more":[{"ns":"app","ws":"b"},{"ns":"other","ws":"c"}]}`)
		Expect(a).To(Receive())
		Expect(b).To(Receive())
		Expect(other).To(Receive())
	})
})
