package effects

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// pairState is where a (consumer, workspace) pair sits in the runtime's cycle.
//
// The state machine is what enforces the per-workspace ordering contract
// without a goroutine per pair: a pair is in the ready queue at most once and
// under a worker at most once, so two passes for one consumer on one workspace
// cannot overlap however many triggers arrive.
type pairState uint8

const (
	// pairIdle is resident but unscheduled: nothing to do, or a failure with
	// backoff disabled waiting for the next trigger. An idle pair with no
	// reason to be remembered is evicted.
	pairIdle pairState = iota
	pairQueued
	pairRunning
	// pairParked failed and is waiting out its backoff window. Triggers
	// arriving now are dropped: the retry drains everything committed since,
	// because a pass always runs the feed to exhaustion.
	pairParked
)

// pair is one consumer's work on one workspace: the unit the old runtime gave a
// goroutine, a ticker and a notifier subscription each. Here it is a struct in
// a map, created when there is a reason to track it and dropped when there is
// not, so what the runtime holds is proportional to the work outstanding rather
// than to the workspaces that have ever existed.
type pair struct {
	key      runnerKey
	consumer *consumer

	// Guarded by Runtime.mu.
	state pairState
	// dirty records a trigger that arrived while a pass was running. That pass
	// may already have read past the change, so it is followed by another
	// rather than trusted to have seen it.
	dirty bool
	// refs counts callers holding this pair outside the state machine — Drain,
	// which runs on its caller's goroutine. Eviction waits for them, or a
	// re-created pair would let a second pass run the same workspace
	// concurrently with the one still in flight.
	refs int
	// retry re-queues a parked pair when its window closes. Only a FAILING
	// pair holds a timer, so timers cost what failures cost rather than what
	// workspaces cost — the property that made a timer per pair untenable.
	retry *time.Timer

	// run serializes passes for this pair. The state machine already keeps two
	// workers off one pair; this is what also excludes a hand-driven Drain
	// from a pass a worker is running.
	run sync.Mutex
}

// pairLocked returns the pair for a consumer and workspace, creating it if the
// runtime is not tracking one. Caller holds mu.
func (rt *Runtime) pairLocked(c *consumer, workspaceID string) *pair {
	key := runnerKey{consumer: c.name, workspace: workspaceID}
	if p, ok := rt.pairs[key]; ok {
		return p
	}
	p := &pair{key: key, consumer: c}
	rt.pairs[key] = p
	// It is resident again, so its health is live rather than a souvenir of a
	// pair that finished; take it back out of the retention window.
	rt.unretire(key)
	return p
}

// scheduleLocked puts a pair in line for a pass, folding into whatever it is
// already doing. Caller holds mu.
func (rt *Runtime) scheduleLocked(p *pair) {
	switch p.state {
	case pairQueued:
		return // already in line
	case pairRunning:
		p.dirty = true
		return
	case pairParked:
		return // backing off; the retry covers everything committed since
	}
	p.state = pairQueued
	rt.queue = append(rt.queue, p)
	rt.ready.Signal()
}

// wake schedules a pass for every consumer on one workspace. It is what an
// activity signal, a Kick and a sweep hit all reach.
func (rt *Runtime) wake(workspaceID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.started || rt.stopped {
		return
	}
	for _, c := range rt.consumers {
		rt.scheduleLocked(rt.pairLocked(c, workspaceID))
	}
}

// wakeConsumer schedules a pass for one consumer on one workspace — what a
// sweep produces, since it asks per consumer.
func (rt *Runtime) wakeConsumer(c *consumer, workspaceID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.started || rt.stopped {
		return
	}
	rt.scheduleLocked(rt.pairLocked(c, workspaceID))
}

// worker drains the ready queue until the runtime stops. Workers are the only
// bound on how much of this runs at once: a sweep may queue every workspace in
// the store, and what protects the storage layer from that is the size of this
// pool, not the size of the queue.
func (rt *Runtime) worker(ctx context.Context) {
	defer rt.wg.Done()
	for {
		rt.mu.Lock()
		for len(rt.queue) == 0 && !rt.stopped {
			rt.ready.Wait()
		}
		if rt.stopped {
			rt.mu.Unlock()
			return
		}
		p := rt.queue[0]
		if len(rt.queue) == 1 {
			rt.queue = nil // drop the backing array rather than re-slice into it
		} else {
			rt.queue = rt.queue[1:]
		}
		p.state = pairRunning
		p.dirty = false
		rt.mu.Unlock()

		rt.runPass(ctx, p)
	}
}

// runPass takes the pair's turn and folds the outcome back into its state.
func (rt *Runtime) runPass(ctx context.Context, p *pair) {
	p.run.Lock()
	h, err := rt.pass(ctx, p.consumer, p.key.workspace)
	p.run.Unlock()

	logger := rt.logger.With("consumer", p.key.consumer, "workspace", p.key.workspace)
	if err != nil && ctx.Err() == nil {
		logger.LogAttrs(ctx, slog.LevelError, "effect pass failed", slog.Any("err", err))
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	// A cancelled context is Close stopping the pass, not the consumer failing:
	// leave the pair idle rather than parking a shutdown as an outage.
	if rt.stopped || ctx.Err() != nil {
		p.state = pairIdle
		return
	}
	p.state = pairIdle
	if err != nil {
		if delay := time.Until(h.NextAttempt); !h.NextAttempt.IsZero() && delay > 0 {
			p.state = pairParked
			p.retry = time.AfterFunc(delay, func() { rt.retryPair(p) })
			logger.LogAttrs(ctx, slog.LevelDebug, "backing off after failed pass",
				slog.Duration("delay", delay), slog.Int("failures", h.ConsecutiveFailures),
				slog.Uint64("position", h.Position), slog.Uint64("lag", h.Lag()))
			return
		}
		// Backoff is disabled, so every trigger attempts. Stay resident: a
		// failing pair is the one thing an operator has to be able to see.
		if p.dirty {
			rt.scheduleLocked(p)
		}
		return
	}
	if p.dirty {
		rt.scheduleLocked(p)
		return
	}
	// Success means the feed was drained to exhaustion — a pass never stops
	// early — so there is nothing left to remember about this pair.
	rt.evictLocked(p)
}

// retryPair re-queues a parked pair when its backoff window closes.
func (rt *Runtime) retryPair(p *pair) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stopped || p.state != pairParked {
		return
	}
	p.retry = nil
	p.state = pairIdle
	rt.scheduleLocked(p)
}

// evictLocked drops a pair the runtime has no reason to track. Caller holds mu.
//
// Everything durable about a consumer — its position, its shadows — lives in
// the workspace's store, so a pair costs nothing to rebuild and the runtime
// keeps one only while it has a reason: work queued, a pass running, a failure
// to stay visible, or a caller holding it. This is what makes the resident set
// O(workspaces with work) rather than O(workspaces ever seen).
func (rt *Runtime) evictLocked(p *pair) {
	if p.state != pairIdle || p.dirty || p.refs > 0 {
		return
	}
	delete(rt.pairs, p.key)
	rt.retire(p.key)
}
