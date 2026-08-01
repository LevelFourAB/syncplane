package connect

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Start launches the connector's background loops and returns. Close (or
// cancelling ctx) stops everything.
//
// Work reaches a workspace three ways: the activity stream drains it as soon
// as the engine commits to it, a sweep discovers workspaces and runs the
// periodic ingress+drain pass (Config.ReconcileInterval), and Kick is the
// manual nudge. All three feed one queue drained by a fixed pool of workers
// (Config.Workers), which is what keeps a fleet of workspaces from arriving at
// one rate-limited source together.
//
// Errors inside the workers are logged and retried on the next trigger —
// convergence work must never take the process down. A workspace whose pass
// failed backs off (Config.RetryMin/RetryMax), coalescing everything that
// arrives inside the window into one deferred pass, and reports why through
// Health.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return fmt.Errorf("connect: Start called twice")
	}
	c.started = true
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	// Subscribe BEFORE the first sweep: a workspace written between the two is
	// reported on the stream and waits there, where the reverse order would
	// see it in neither and leave it to the next sweep.
	ids, cancelIDs, watching := c.srv.SubscribeActivity()
	if err := c.sweep(ctx, true); err != nil {
		if watching {
			cancelIDs()
		}
		c.stop()
		return err
	}

	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx)
	}

	if watching {
		c.watch(ctx, ids, cancelIDs)
	} else {
		// Without an activity stream a commit is invisible until the next
		// sweep, so egress latency becomes the sweep interval rather than the
		// round trip to the source.
		c.logger.LogAttrs(ctx, slog.LevelInfo,
			"notifier cannot report workspace activity; commits are found by sweeping only",
			slog.Duration("interval", c.sweepInterval()))
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.sweepInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.sweep(ctx, false); err != nil && ctx.Err() == nil {
					c.logger.LogAttrs(ctx, slog.LevelError, "workspace sweep failed", slog.Any("err", err))
				}
			}
		}
	}()
	return nil
}

// sweepInterval is how often the sweep runs. With periodic passes disabled it
// still runs, at a sane cadence, because discovery and pruning ride on it.
func (c *Connector) sweepInterval() time.Duration {
	if c.interval > 0 {
		return c.interval
	}
	return time.Minute
}

// Kick schedules a near-immediate ingress+drain for the workspace,
// coalescing with one already pending. Wire external change hints here —
// fsnotify watchers, database triggers, webhooks. Kicking a workspace the
// engine has never seen brings it into the managed set, so source-first
// workspaces can announce themselves; a sweep will not drop one again, since
// Config.Workspaces does not name it.
func (c *Connector) Kick(workspaceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.stopped {
		return
	}
	w, fresh := c.admitLocked(workspaceID)
	w.sticky = true
	if !fresh {
		c.scheduleLocked(w, triggerSync)
	}
}

// Forget drops the workspace from the managed set and releases its source —
// the teardown half of Config.OpenSource, for when a user disconnects an
// account. A pass in flight for it is cancelled. Shadows and cursors are left
// in place, so reconnecting resumes rather than re-imports; drop them
// separately if the data should not survive.
//
// A workspace Config.Workspaces still lists comes back on the next sweep with
// a freshly opened source, which is what makes this the right call after
// revoking a credential too. Exclude it from Config.Workspaces to keep it
// gone — a sweep drops what that list no longer names, so with the default
// list a workspace that disappears from the engine needs no Forget at all.
func (c *Connector) Forget(workspaceID string) {
	c.mu.Lock()
	c.dropLocked(workspaceID)
	c.mu.Unlock()
	// In-flight passes keep the source alive until they release it.
	c.srcs.invalidate(workspaceID)
	c.forgetHealth(workspaceID)
}

// watch turns activity signals into queued drains, so a workspace that is
// written egresses now rather than at the next sweep.
//
// The signal is only a wake-up: the pass it schedules resumes from the durable
// cursor, so the write that produced it is egressed even though it committed
// before anything was listening. Unlike Kick it does not admit a workspace —
// the stream carries every workspace in the store, and Config.Workspaces is
// what decides which of them this connector converges.
func (c *Connector) watch(ctx context.Context, ids <-chan string, cancel func()) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case ws, open := <-ids:
				if !open {
					return
				}
				c.mu.Lock()
				if w, ok := c.tracked[ws]; ok {
					c.scheduleLocked(w, triggerDrain)
				}
				c.mu.Unlock()
			}
		}
	}()
}

// sweep reconciles the managed set with Config.Workspaces and schedules the
// periodic pass.
//
// A workspace the list no longer names is dropped: its record, its health and
// its opened source go with it, so a deleted or disconnected account stops
// costing anything without waiting for a restart. One that is busy — queued,
// running, backing off, or held by a direct call — is left for the next sweep
// rather than pulled out from under its own pass.
//
// initial marks Start's sweep, which schedules every workspace it finds even
// when periodic passes are off: the engine may hold rows that never reached
// the source, and until one pass has run nobody knows.
func (c *Connector) sweep(ctx context.Context, initial bool) error {
	// Outside mu: listing is adopter code that may block on I/O, and nothing
	// else may wait on the connector's lock behind it.
	ids, err := c.listWS(ctx)
	if err != nil {
		return err
	}

	periodic := initial || c.interval > 0
	listed := make(map[string]struct{}, len(ids))
	var dropped []string

	c.mu.Lock()
	for _, id := range ids {
		listed[id] = struct{}{}
		// A workspace admitted here is already queued for its first sync, and
		// the periodic schedule below folds into that rather than doubling it.
		c.admitLocked(id)
	}
	// One pass over the managed set: what the list dropped goes, what stayed —
	// including the Kicked workspaces the list never named — gets its turn.
	for id, w := range c.tracked {
		if _, ok := listed[id]; !ok && !w.sticky {
			if c.evictLocked(w) {
				dropped = append(dropped, id)
			}
			continue
		}
		if periodic {
			c.scheduleLocked(w, triggerSync)
		}
	}
	c.mu.Unlock()

	for _, id := range dropped {
		c.srcs.invalidate(id)
		c.forgetHealth(id)
		c.logger.LogAttrs(ctx, slog.LevelDebug, "workspace no longer managed; dropped",
			slog.String("workspace", id))
	}
	return nil
}

// stop cancels the run context and releases everything the connector is
// holding: the queue, the workers waiting on it, and the retry timers of
// workspaces that were backing off.
func (c *Connector) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	for _, w := range c.tracked {
		if w.retry != nil {
			w.retry.Stop()
			w.retry = nil
		}
	}
	c.queue = nil
	c.ready.Broadcast()
}
