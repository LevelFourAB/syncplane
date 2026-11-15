package connect

import (
	"container/heap"
	"time"
)

// wakeHeap orders the workspaces that asked for a pass at a later time, the
// earliest first. One timer for the whole connector fires at the earliest
// wake, so a workspace that waits costs a heap entry, not a timer or a
// goroutine.
type wakeHeap []*workspace

func (h wakeHeap) Len() int           { return len(h) }
func (h wakeHeap) Less(i, j int) bool { return h[i].wakeAt.Before(h[j].wakeAt) }
func (h wakeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].wakeIdx = i
	h[j].wakeIdx = j
}

func (h *wakeHeap) Push(x any) {
	w := x.(*workspace)
	w.wakeIdx = len(*h)
	*h = append(*h, w)
}

func (h *wakeHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	w.wakeIdx = -1
	return w
}

// setWakeLocked asks for a drain-level pass of w at at, replacing the wake it
// had; a zero at removes it. It does nothing while the connector is not
// started, since nothing would run the pass. Caller holds mu.
func (c *Connector) setWakeLocked(w *workspace, at time.Time) {
	if at.IsZero() || !c.started || c.stopped || c.tracked[w.id] != w {
		c.unwakeLocked(w)
		return
	}
	w.wakeAt = at
	if w.wakeIdx >= 0 {
		heap.Fix(&c.wakes, w.wakeIdx)
	} else {
		heap.Push(&c.wakes, w)
	}
	c.armWakeLocked()
}

// unwakeLocked removes w's wake. Caller holds mu.
func (c *Connector) unwakeLocked(w *workspace) {
	if w.wakeIdx >= 0 {
		heap.Remove(&c.wakes, w.wakeIdx)
		c.armWakeLocked()
	}
	w.wakeAt = time.Time{}
}

// armWakeLocked sets the timer to the earliest wake. Caller holds mu.
func (c *Connector) armWakeLocked() {
	if len(c.wakes) == 0 || c.stopped {
		if c.wakeTimer != nil {
			c.wakeTimer.Stop()
		}
		return
	}
	d := max(c.wakes[0].wakeAt.Sub(c.now()), 0)
	if c.wakeTimer == nil {
		c.wakeTimer = time.AfterFunc(d, c.fireWakes)
		return
	}
	c.wakeTimer.Reset(d)
}

// fireWakes schedules every workspace whose wake is due.
func (c *Connector) fireWakes() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	now := c.now()
	for len(c.wakes) > 0 && !c.wakes[0].wakeAt.After(now) {
		w := heap.Pop(&c.wakes).(*workspace)
		w.wakeAt = time.Time{}
		c.scheduleLocked(w, triggerDrain)
	}
	c.armWakeLocked()
}
