package upstream_test

import (
	"context"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// Status.OriginHeadSeq and Status.Behind: how far the link trails the origin,
// learned from the head every page reports.
var _ = Describe("Changes behind the origin", func() {
	It("reports the distance while pages drain, and 0 once caught up", func() {
		o := newOrigin()
		var (
			mu   sync.Mutex
			seen []upstream.Status
		)
		n := newLocal(o, func(c *upstream.Config) {
			c.PageLimit = 2
			c.OnStatus = func(s upstream.Status) {
				mu.Lock()
				seen = append(seen, s)
				mu.Unlock()
			}
		})
		n.add()
		n.flush()

		n.part.Cut()
		for i := range 10 {
			// One write each, so the origin's head moves ten sequences.
			o.put(&todov1.Todo{Id: fmt.Sprintf("t%d", i)})
		}
		n.part.Heal()
		n.flush()

		var head uint64
		Expect(o.store.WithReadTx(context.Background(), ws, func(tx storage.ReadTx) error {
			var err error
			head, err = tx.CurrentSeq()
			return err
		})).To(Succeed())

		Eventually(func() upstream.Status { return n.status() }).Should(And(
			HaveField("OriginHeadSeq", head),
			HaveField("OriginSeq", head),
		))
		Expect(n.status().Behind()).To(BeZero())

		mu.Lock()
		defer mu.Unlock()
		var maxBehind uint64
		for _, s := range seen {
			maxBehind = max(maxBehind, s.Behind())
		}
		Expect(maxBehind).To(BeNumerically(">", 0), "a page of two changes leaves the rest behind")
	})
})
