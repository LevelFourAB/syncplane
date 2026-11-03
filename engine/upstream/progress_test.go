package upstream_test

import (
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// Bootstrap progress in Status: what an app reads to show how much of the
// first download is left.
var _ = Describe("Bootstrap progress", func() {
	var (
		mu      sync.Mutex
		reports []*upstream.BootstrapProgress
	)
	// record keeps the Bootstrap field of every status OnStatus receives, nil
	// ones included, so a spec can see where an attempt ended.
	record := func(c *upstream.Config) {
		c.OnStatus = func(s upstream.Status) {
			if s.Workspace != ws {
				return
			}
			mu.Lock()
			reports = append(reports, s.Bootstrap)
			mu.Unlock()
		}
	}
	seen := func() []*upstream.BootstrapProgress {
		mu.Lock()
		defer mu.Unlock()
		return append([]*upstream.BootstrapProgress(nil), reports...)
	}
	progressOf := func(all []*upstream.BootstrapProgress) []upstream.BootstrapProgress {
		var out []upstream.BootstrapProgress
		for _, p := range all {
			if p != nil {
				out = append(out, *p)
			}
		}
		return out
	}
	seed := func(o *origin, n int) {
		GinkgoHelper()
		for i := range n {
			o.put(&todov1.Todo{Id: fmt.Sprintf("t%02d", i), Title: strings.Repeat(fmt.Sprintf("%x", i*7919), 40)})
		}
	}
	// expectMonotonic checks that rows applied and the fraction done never go
	// down, and that the fraction never passes 1.
	expectMonotonic := func(ps []upstream.BootstrapProgress) {
		GinkgoHelper()
		for i := 1; i < len(ps); i++ {
			prev, cur := ps[i-1], ps[i]
			Expect(cur.RowsApplied).To(BeNumerically(">=", prev.RowsApplied), "%v", ps)
			Expect(cur.EntitiesDone).To(BeNumerically(">=", prev.EntitiesDone), "%v", ps)
			if prev.RowsEstimated > 0 && cur.RowsEstimated > 0 {
				Expect(float64(cur.RowsApplied)/float64(cur.RowsEstimated)).To(
					BeNumerically(">=", float64(prev.RowsApplied)/float64(prev.RowsEstimated)), "%v", ps)
			}
		}
		for _, p := range ps {
			if p.RowsEstimated > 0 {
				Expect(p.RowsApplied).To(BeNumerically("<=", p.RowsEstimated), "%v", ps)
			}
		}
	}

	BeforeEach(func() {
		mu.Lock()
		reports = nil
		mu.Unlock()
	})

	It("reports rows and entities as chunks land, and nil once the snapshot is in", func() {
		DeferCleanup(upstream.SetBootProgressInterval(0))
		o := newOrigin(server.WithChunkSize(2))
		seed(o, 20)
		n := newLocal(o, record)
		n.add()
		n.flush()

		all := seen()
		ps := progressOf(all)
		expectMonotonic(ps)
		Expect(len(ps)).To(BeNumerically(">=", 10), "one report per chunk when nothing throttles")
		last := ps[len(ps)-1]
		Expect(last.RowsApplied).To(BeEquivalentTo(20))
		Expect(last.RowsEstimated).To(BeEquivalentTo(20))
		Expect(last.EntitiesTotal).To(BeNumerically(">", 0))
		Expect(last.EntitiesDone).To(Equal(last.EntitiesTotal))
		Expect(all[len(all)-1]).To(BeNil(), "the last report has no bootstrap running")
		Expect(n.status().Bootstrap).To(BeNil())
	})

	It("throttles reports of progress alone, but always reports the start and the end", func() {
		DeferCleanup(upstream.SetBootProgressInterval(time.Hour))
		o := newOrigin(server.WithChunkSize(2))
		seed(o, 20)
		n := newLocal(o, record)
		n.add()
		n.flush()

		ps := progressOf(seen())
		expectMonotonic(ps)
		Expect(ps).NotTo(BeEmpty())
		Expect(ps[0].RowsApplied).To(BeZero(), "the start is reported")
		Expect(ps[len(ps)-1].RowsApplied).To(BeEquivalentTo(20), "the end is reported")
		var between int
		for _, p := range ps {
			if p.RowsApplied > 0 && p.RowsApplied < 20 {
				between++
			}
		}
		Expect(between).To(BeNumerically("<=", 1), "ten chunks, and at most one report between start and end: %v", ps)
	})

	It("continues from what an interrupted attempt applied", func() {
		DeferCleanup(upstream.SetBootProgressInterval(0))
		o := newOrigin(server.WithChunkSize(2))
		seed(o, 20)
		n := newLocal(o, record)
		n.part.breakBootstrap.Store(400)
		n.add()
		n.flush()
		Expect(n.part.bootstraps.Load()).To(BeNumerically(">=", 2))

		// Split the reports into attempts at the nil that ends each one.
		var attempts [][]upstream.BootstrapProgress
		var cur []upstream.BootstrapProgress
		for _, p := range seen() {
			if p == nil {
				if len(cur) > 0 {
					attempts = append(attempts, cur)
				}
				cur = nil
				continue
			}
			cur = append(cur, *p)
		}
		Expect(len(attempts)).To(BeNumerically(">=", 2), "%v", attempts)
		first, second := attempts[0], attempts[len(attempts)-1]
		reached := first[len(first)-1].RowsApplied
		Expect(reached).To(BeNumerically(">", 0), "the first attempt applied rows before it broke: %v", attempts)
		Expect(reached).To(BeNumerically("<", 20))
		Expect(second[0].RowsApplied).To(Equal(reached), "the next attempt starts where the last one stopped")

		var joined []upstream.BootstrapProgress
		for _, a := range attempts {
			joined = append(joined, a...)
		}
		expectMonotonic(joined)
		end := second[len(second)-1]
		Expect(end.RowsApplied).To(BeEquivalentTo(20), "no row is counted twice")
		Expect(end.EntitiesDone).To(Equal(end.EntitiesTotal))
	})
})
