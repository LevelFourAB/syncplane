package upstream

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bootstrap progress throttle", func() {
	var (
		w       *workspace
		mu      sync.Mutex
		reports []*BootstrapProgress
	)
	seen := func() []*BootstrapProgress {
		mu.Lock()
		defer mu.Unlock()
		return append([]*BootstrapProgress(nil), reports...)
	}

	BeforeEach(func() {
		old := bootProgressInterval
		bootProgressInterval = 50 * time.Millisecond
		DeferCleanup(func() { bootProgressInterval = old })
		mu.Lock()
		reports = nil
		mu.Unlock()
		w = &workspace{
			l: &Link{cfg: Config{OnStatus: func(s Status) {
				mu.Lock()
				reports = append(reports, s.Bootstrap)
				mu.Unlock()
			}}},
			id:      "ws",
			pend:    map[uint64]uint64{},
			changed: make(chan struct{}),
		}
	})

	It("reports held-back progress when the interval has passed, with no further chunk", func() {
		w.progress(BootstrapProgress{RowsApplied: 1}, true)
		w.progress(BootstrapProgress{RowsApplied: 2}, false)
		w.progress(BootstrapProgress{RowsApplied: 3}, false)
		Expect(seen()).To(HaveLen(1), "the throttle holds the two chunks back")

		Eventually(func() []*BootstrapProgress { return seen() }).Should(HaveLen(2))
		Expect(*seen()[1]).To(Equal(BootstrapProgress{RowsApplied: 3}), "the newest progress, once")
		Consistently(func() []*BootstrapProgress { return seen() }, 120*time.Millisecond).Should(HaveLen(2))
	})

	It("drops held-back progress when the attempt ends", func() {
		w.progress(BootstrapProgress{RowsApplied: 1}, true)
		w.progress(BootstrapProgress{RowsApplied: 2}, false)
		w.endProgress()
		Consistently(func() []*BootstrapProgress { return seen() }, 120*time.Millisecond).Should(HaveLen(2))
		Expect(seen()[1]).To(BeNil(), "the end clears the progress, and nothing reports after it")
		Expect(w.statusLocked().Bootstrap).To(BeNil())
	})
})
