package connect

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// trigger says how much work one wake-up needs. The values are ordered
// because a sync is a superset of a drain: coalescing several triggers into
// one pass keeps the largest.
type trigger int

const (
	triggerNone trigger = iota
	triggerDrain
	triggerSync
)

// wsState is where a workspace sits in the connector's cycle.
//
// The state machine is what enforces one pass at a time per workspace without
// a goroutine per workspace: a workspace is in the ready queue at most once
// and under a worker at most once, so two passes cannot overlap however many
// triggers arrive.
type wsState uint8

const (
	// wsIdle is resident but unscheduled: nothing to do, or a failure with
	// backoff disabled waiting for the next trigger.
	wsIdle wsState = iota
	wsQueued
	wsRunning
	// wsParked failed and is waiting out its backoff window. Triggers
	// arriving now fold into dirty and fire when the window closes, so a
	// rate-limited source sees one retry per backoff step rather than one per
	// commit.
	wsParked
)

// workspace is one workspace's place in the connector's schedule: the unit the
// old lifecycle gave a goroutine, a ticker and a change subscription each.
// Here it is a struct in a map, so what the connector holds is a small record
// per managed workspace rather than a running loop per managed workspace, and
// dropping one is a map delete rather than a shutdown protocol.
type workspace struct {
	id string

	// Guarded by Connector.mu.
	state wsState
	// level is what the queued pass will run, or what the running pass ran.
	level trigger
	// dirty is the largest trigger that arrived while a pass was running or
	// while the workspace was parked, folded into the pass that follows. A
	// pass already in flight may have read past the change that triggered it,
	// so it is followed by another rather than trusted to have seen it.
	dirty trigger
	// sticky marks a workspace that announced itself through Kick rather than
	// through discovery. Config.Workspaces does not name it, so a sweep must
	// not read its absence from that list as "gone".
	sticky bool
	// refs counts callers holding this workspace outside the state machine —
	// the public Ingress, Reconcile and Drain, which run on their caller's
	// goroutine. Releasing waits for them, or a workspace dropped and
	// re-admitted would let a second pass run alongside the one still in
	// flight.
	refs int
	// retry re-queues a parked workspace when its window closes. Only a
	// FAILING workspace holds a timer, so timers cost what failures cost.
	retry *time.Timer
	// cancel stops the pass a worker is currently running, so Forget can drop
	// one workspace mid-flight without touching the rest. nil when no worker
	// pass is in flight; hand-driven passes belong to their caller's context
	// and are never cancelled from here.
	cancel context.CancelFunc

	// run serializes passes for this workspace. The state machine already
	// keeps two workers off one workspace; this is what also excludes a
	// hand-driven Ingress, Reconcile or Drain from a pass a worker is running,
	// so two passes cannot interleave their shadow and cursor writes.
	run sync.Mutex
}

// trackedLocked returns the workspace's record, creating an unscheduled one if
// the connector is not tracking it. Caller holds mu.
func (c *Connector) trackedLocked(workspaceID string) *workspace {
	if w, ok := c.tracked[workspaceID]; ok {
		return w
	}
	w := &workspace{id: workspaceID}
	c.tracked[workspaceID] = w
	return w
}

// admitLocked brings a workspace into the managed set, scheduling the initial
// sync that a workspace the connector has not seen before needs — the engine
// may hold rows that never reached the source, and the source rows that never
// reached the engine. fresh reports whether it was newly admitted. Caller
// holds mu.
func (c *Connector) admitLocked(workspaceID string) (w *workspace, fresh bool) {
	if w, ok := c.tracked[workspaceID]; ok {
		return w, false
	}
	w = c.trackedLocked(workspaceID)
	c.scheduleLocked(w, triggerSync)
	return w, true
}

// scheduleLocked puts a workspace in line for a pass at level t, folding into
// whatever it is already doing. Caller holds mu.
func (c *Connector) scheduleLocked(w *workspace, t trigger) {
	if t == triggerNone || !c.started || c.stopped {
		return
	}
	// A workspace that has been forgotten (or replaced by a re-admitted
	// record) is no longer the connector's to run: a retry timer or an
	// in-flight pass may still hold a pointer to it.
	if c.tracked[w.id] != w {
		return
	}
	switch w.state {
	case wsQueued:
		if t > w.level {
			w.level = t
		}
		return
	case wsRunning, wsParked:
		if t > w.dirty {
			w.dirty = t
		}
		return
	}
	w.level = t
	w.state = wsQueued
	c.queue = append(c.queue, w)
	c.ready.Signal()
}

// worker drains the ready queue until the connector stops. Workers are the
// only bound on how much of this runs at once: a sweep queues every managed
// workspace, and what keeps a fleet of them from arriving at one rate-limited
// source together is the size of this pool, not the size of the queue.
func (c *Connector) worker(ctx context.Context) {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		for len(c.queue) == 0 && !c.stopped {
			c.ready.Wait()
		}
		if c.stopped {
			c.mu.Unlock()
			return
		}
		w := c.queue[0]
		if len(c.queue) == 1 {
			c.queue = nil // drop the backing array rather than re-slice into it
		} else {
			c.queue = c.queue[1:]
		}
		w.state = wsRunning
		level := w.level
		passCtx, cancel := context.WithCancel(ctx)
		w.cancel = cancel
		c.mu.Unlock()

		c.runPass(passCtx, w, level)
		cancel()
	}
}

// runPass takes the workspace's turn and folds the outcome back into its
// state.
func (c *Connector) runPass(ctx context.Context, w *workspace, level trigger) {
	err := c.attempt(ctx, w.id, level)

	// Health is read before mu is taken: recordPass computed NextAttempt
	// inside the pass, and mu is never held while reaching for hmu.
	var next time.Time
	if err != nil {
		if h, ok := c.Health(w.id); ok {
			next = h.NextAttempt
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	w.cancel = nil
	w.state = wsIdle
	// A cancelled context is Close or Forget stopping the pass, not the source
	// failing: leave the workspace idle rather than parking a shutdown as an
	// outage.
	if c.stopped || ctx.Err() != nil {
		return
	}
	if err != nil && !next.IsZero() {
		if delay := time.Until(next); delay > 0 {
			w.state = wsParked
			// The failed pass still has to happen, so the retry runs at least
			// the level that failed.
			if level > w.dirty {
				w.dirty = level
			}
			w.retry = time.AfterFunc(delay, func() { c.retryWorkspace(w) })
			c.logger.LogAttrs(ctx, slog.LevelDebug, "backing off after failed pass",
				slog.String("workspace", w.id), slog.Duration("delay", delay))
			return
		}
	}
	// Either the pass succeeded, or backoff is disabled and every trigger
	// attempts. Anything that arrived while it ran runs now.
	t := w.dirty
	w.dirty = triggerNone
	c.scheduleLocked(w, t)
}

// retryWorkspace re-queues a parked workspace when its backoff window closes.
func (c *Connector) retryWorkspace(w *workspace) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || w.state != wsParked {
		return
	}
	w.retry = nil
	w.state = wsIdle
	t := w.dirty
	w.dirty = triggerNone
	c.scheduleLocked(w, t)
}

// attempt runs one pass at the requested level: the source→engine half only
// when the trigger asked for a full sync, then the engine→source half.
//
// Draining is skipped when ingress failed: draining against a source that just
// failed writes into the same wall, and its shadows are the ones ingress would
// have repaired.
func (c *Connector) attempt(ctx context.Context, workspaceID string, t trigger) error {
	var err error
	if t >= triggerSync {
		if err = c.Ingress(ctx, workspaceID); err != nil {
			c.logPassFailure(ctx, workspaceID, "ingress", err)
		}
	}
	if err == nil {
		if err = c.Drain(ctx, workspaceID); err != nil {
			c.logPassFailure(ctx, workspaceID, "drain", err)
		}
	}
	return err
}

// logPassFailure reports a background pass failure. Convergence work must
// never take the process down, so this is where the error stops and Health is
// where it stays visible.
func (c *Connector) logPassFailure(ctx context.Context, workspaceID, op string, err error) {
	if err == nil || ctx.Err() != nil {
		return
	}
	c.logger.LogAttrs(ctx, slog.LevelError, "convergence pass failed",
		slog.String("workspace", workspaceID), slog.String("op", op), slog.Any("err", err))
}

// hold takes a reference to a workspace on behalf of a caller-driven pass and
// returns it. It never schedules: a hand-driven connector has no workers, and
// calling Drain for a workspace is not a request to manage it.
func (c *Connector) hold(workspaceID string) *workspace {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.trackedLocked(workspaceID)
	w.refs++
	return w
}

// release drops a caller's reference. With no schedule running — never
// started, or stopped — the record is dropped with it, since nothing will ever
// come back for it and remembering it would make a hand-driven connector's
// memory grow with the workspaces it has been asked about.
func (c *Connector) release(w *workspace) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w.refs--
	if !c.started || c.stopped {
		c.evictLocked(w)
	}
}

// evictLocked drops a workspace from the managed set when nothing is
// outstanding for it, reporting whether it did. Caller holds mu.
//
// Everything durable about a workspace — its shadows, its cursors — lives in
// State, so a record costs nothing to rebuild and the connector keeps one only
// while it has a reason: work queued, a pass running, a failure being retried,
// or a caller holding it.
func (c *Connector) evictLocked(w *workspace) bool {
	if w.state != wsIdle || w.dirty != triggerNone || w.refs > 0 {
		return false
	}
	if c.tracked[w.id] == w {
		delete(c.tracked, w.id)
	}
	return true
}

// dropLocked removes a workspace whether or not it is busy: the caller has
// decided the connector is done with it. A pass in flight is cancelled, and
// its record is orphaned rather than deleted twice — scheduleLocked refuses a
// record the map no longer holds, so neither the pass nor a pending retry can
// put it back. Caller holds mu.
func (c *Connector) dropLocked(workspaceID string) {
	w, ok := c.tracked[workspaceID]
	if !ok {
		return
	}
	delete(c.tracked, workspaceID)
	if w.retry != nil {
		w.retry.Stop()
		w.retry = nil
	}
	if w.cancel != nil {
		w.cancel()
	}
}

// Tracked reports how many workspaces the connector is holding state for. It
// is the managed set — what discovery found plus what Kick announced — and is
// what the resident cost of a running connector is proportional to.
func (c *Connector) Tracked() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tracked)
}
