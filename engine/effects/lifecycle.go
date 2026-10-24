package effects

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Start launches the background loops and returns. Close (or cancelling ctx)
// stops everything.
//
// Work reaches a consumer three ways, in descending order of how much they
// carry: the activity stream wakes the workspaces that are being written, a
// sweep finds work the stream did not carry (a signal lost, a node restarted,
// a consumer registered against a store with history), and Kick is the manual
// nudge. All three feed one queue drained by a fixed pool of workers.
//
// Failures inside the workers are logged and retried with backoff — a reaction
// that fails must not take the process down — so Health is where a consumer
// that cannot make progress becomes visible.
func (rt *Runtime) Start(ctx context.Context) error {
	rt.mu.Lock()
	if rt.started {
		rt.mu.Unlock()
		return fmt.Errorf("effects: Start called twice")
	}
	if len(rt.consumers) == 0 {
		rt.mu.Unlock()
		return fmt.Errorf("effects: Start with no consumers registered")
	}
	rt.started = true
	ctx, rt.cancel = context.WithCancel(ctx)
	rt.runCtx = ctx
	rt.mu.Unlock()

	// Subscribe BEFORE the first sweep: a workspace written between the two is
	// reported on the stream and waits there, where the reverse order would
	// see it in neither and leave it to the next sweep.
	ids, cancelIDs, watching := rt.srv.SubscribeActivity()
	if err := rt.sweep(ctx); err != nil {
		if watching {
			cancelIDs()
		}
		rt.stop()
		return err
	}

	rt.mu.Lock()
	for i := 0; i < rt.workers; i++ {
		rt.wg.Add(1)
		go rt.worker(ctx)
	}
	rt.mu.Unlock()

	if watching {
		rt.watch(ctx, ids, cancelIDs)
	} else {
		rt.logger.LogAttrs(ctx, slog.LevelInfo,
			"notifier cannot report workspace activity; work is found by sweeping only",
			slog.Duration("interval", rt.interval))
	}
	if rt.interval <= 0 {
		return nil
	}
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		ticker := time.NewTicker(rt.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := rt.sweep(ctx); err != nil && ctx.Err() == nil {
					rt.logger.LogAttrs(ctx, slog.LevelError, "sweep for pending work failed", slog.Any("err", err))
				}
			}
		}
	}()
	return nil
}

// Kick schedules a near-immediate pass for every consumer on the workspace,
// coalescing with one already pending. Wire it to whatever knows a workspace
// changed before the engine does; a workspace the runtime holds no state for
// starts here rather than waiting for the next sweep.
func (rt *Runtime) Kick(workspaceID string) {
	rt.wake(workspaceID)
}

// Close stops the workers and waits for in-flight passes to finish. It does not
// close the sinks — the caller owns those. Idempotent.
func (rt *Runtime) Close() error {
	rt.stop()
	rt.wg.Wait()
	// After the workers are done, so a sampling callback cannot run against a
	// half-torn-down runtime. Unregistering releases the meter provider's
	// reference to this Runtime; without it a closed runtime would be sampled
	// forever, reporting the lag it was stuck at when it stopped.
	rt.mx.close()
	return nil
}

// stop cancels the run context and releases everything the runtime is holding:
// the queue, the workers waiting on it, and the retry timers of pairs that were
// backing off.
func (rt *Runtime) stop() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stopped {
		return
	}
	rt.stopped = true
	if rt.cancel != nil {
		rt.cancel()
		rt.cancel = nil
	}
	for _, p := range rt.pairs {
		if p.retry != nil {
			p.retry.Stop()
			p.retry = nil
		}
	}
	rt.queue = nil
	rt.ready.Broadcast()
}

// watch turns activity signals into queued work, so a workspace that is written
// gets its reaction now rather than at the next sweep.
//
// The signal is only a wake-up: the pass it schedules drains from the
// consumer's durable position, so the write that produced the signal is
// delivered even though it committed before anything was listening for it.
func (rt *Runtime) watch(ctx context.Context, ids <-chan string, cancel func()) {
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case ws, open := <-ids:
				if !open {
					return
				}
				rt.wake(ws)
			}
		}
	}()
}

// sweep queues the work the activity stream did not carry.
//
// It asks the engine which workspaces each consumer is behind on, rather than
// draining every workspace to find out. That is the difference between one
// query per consumer and a read transaction per (consumer, workspace) every
// interval, and it is why the cost of an idle runtime tracks the work waiting
// for it instead of the size of the store.
func (rt *Runtime) sweep(ctx context.Context) error {
	if rt.listWS != nil {
		// An adopter-supplied list replaces the query: it names workspaces the
		// engine cannot be asked about, so every one is queued and the pass
		// itself decides there is nothing to do.
		ids, err := rt.listWS(ctx)
		if err != nil {
			return err
		}
		for _, ws := range ids {
			rt.wake(ws)
		}
		return nil
	}
	for _, c := range rt.consumers {
		if err := rt.sweepConsumer(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

// sweepConsumer queues each of one consumer's pending workspaces.
func (rt *Runtime) sweepConsumer(ctx context.Context, c *consumer) error {
	for ws, err := range rt.srv.PendingWorkspaces(ctx, c.name) {
		if err != nil {
			return err
		}
		rt.wakeConsumer(c, ws)
	}
	return nil
}
