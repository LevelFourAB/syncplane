package pubsub

import (
	"sync"
	"time"
)

// Coalescing wraps a Notifier and caps the notify rate per workspace. The
// first change in a quiet workspace passes through immediately, then a
// cooldown of one window starts: changes landing during a cooldown collapse
// into a single trailing notify at its end (which starts the next
// cooldown). Steady-state, a write-heavy workspace emits at most one notify
// per window, while a quiet one keeps zero added latency.
//
// The wrap sits on the send side, so a shared Notifier (e.g. NATS-backed)
// also carries less traffic on the bus.
//
// A Coalescing owns timers, so it must be Closed when the component that
// installed it shuts down.
type Coalescing struct {
	inner  Notifier
	window time.Duration

	mu     sync.Mutex
	closed bool
	// Workspaces in cooldown, with the timer that ends it.
	cooling map[string]*cooldown
}

type cooldown struct {
	// owed records whether a notify arrived during this cooldown, i.e.
	// whether a trailing notify is due when it ends.
	owed  bool
	timer *time.Timer
}

// NewCoalescing wraps inner with per-workspace coalescing. A window of zero
// or less returns inner unchanged — the caller gets no wrapper, and so has
// nothing to Close.
func NewCoalescing(inner Notifier, window time.Duration) Notifier {
	if window <= 0 {
		return inner
	}
	return &Coalescing{inner: inner, window: window, cooling: make(map[string]*cooldown)}
}

func (c *Coalescing) Notify(workspaceID string) {
	c.mu.Lock()
	if c.closed {
		// No timer to hang a trailing notify on; pass straight through so a
		// write racing shutdown still wakes subscribers, just uncapped.
		c.mu.Unlock()
		c.inner.Notify(workspaceID)
		return
	}
	if cd, ok := c.cooling[workspaceID]; ok {
		cd.owed = true
		c.mu.Unlock()
		return
	}
	c.cooling[workspaceID] = &cooldown{
		timer: time.AfterFunc(c.window, func() { c.endCooldown(workspaceID) }),
	}
	c.mu.Unlock()

	c.inner.Notify(workspaceID)
}

// endCooldown delivers the owed trailing notify (starting the next
// cooldown), or returns a clean workspace to idle.
func (c *Coalescing) endCooldown(workspaceID string) {
	c.mu.Lock()
	cd, ok := c.cooling[workspaceID]
	if !ok || c.closed {
		// Close raced this timer: it had already fired and was waiting on the
		// mutex when Close stopped it. Dropping the notify here is what makes
		// Close a hard stop.
		c.mu.Unlock()
		return
	}
	if !cd.owed {
		delete(c.cooling, workspaceID)
		c.mu.Unlock()
		return
	}
	cd.owed = false
	cd.timer = time.AfterFunc(c.window, func() { c.endCooldown(workspaceID) })
	c.mu.Unlock()

	c.inner.Notify(workspaceID)
}

// Close stops the pending cooldown timers. A trailing notify owed at Close
// time is dropped rather than delivered afterwards: past Close the wrapper
// never touches the inner notifier on its own again. That matters for a
// shared (NATS/Redis) notifier, which would otherwise see publishes from a
// process that has already shut down.
//
// It does not close the inner notifier — the caller that supplied it owns
// its lifecycle. Notify still works after Close (passed through uncapped),
// so a write racing shutdown loses no signal. Close is idempotent.
func (c *Coalescing) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for id, cd := range c.cooling {
		cd.timer.Stop()
		delete(c.cooling, id)
	}
	return nil
}

func (c *Coalescing) Subscribe(workspaceID string) (<-chan struct{}, func()) {
	return c.inner.Subscribe(workspaceID)
}

func (c *Coalescing) SubscribeSet(workspaceIDs []string) Set {
	return c.inner.SubscribeSet(workspaceIDs)
}

// Unwrap exposes the wrapped notifier so optional extensions (Activity) are
// found on it. Coalescing deliberately does not forward those itself: a
// forwarding method would satisfy the extension interface even when the inner
// notifier cannot, turning "not supported" into a subscription that never
// fires.
func (c *Coalescing) Unwrap() Notifier { return c.inner }
