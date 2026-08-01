package effects

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/server"
)

// Drain runs one pass for one consumer on one workspace, on the calling
// goroutine, and returns whatever stopped it. It is the manual entry point:
// Start drives this same pass from activity signals and the sweep, and both
// record Health, so a runtime driven by hand is exactly as observable as a
// started one.
//
// Backoff is ignored — the caller asked — and, like a worker, it drains the feed
// to exhaustion before returning. It does honour the per-workspace ordering
// contract: if a worker is mid-pass on this same pair, Drain waits for it rather
// than running a second pass alongside it.
func (rt *Runtime) Drain(ctx context.Context, consumer, workspaceID string) error {
	rt.mu.Lock()
	c, ok := rt.consumers[consumer]
	if !ok {
		rt.mu.Unlock()
		return fmt.Errorf("effects: no consumer named %q", consumer)
	}
	// Hold a reference across the pass: without one, the pair could be evicted
	// and re-created while this pass runs, and the new pair would let a worker
	// start a second one on the same workspace.
	p := rt.pairLocked(c, workspaceID)
	p.refs++
	rt.mu.Unlock()

	p.run.Lock()
	_, err := rt.pass(ctx, c, workspaceID)
	p.run.Unlock()

	rt.mu.Lock()
	p.refs--
	rt.evictLocked(p)
	rt.mu.Unlock()
	return err
}

// pass is one drain plus the health it produces. A cancelled context is a
// shutdown rather than a failure, and is not recorded as one.
func (rt *Runtime) pass(ctx context.Context, c *consumer, workspaceID string) (Health, error) {
	// A pass is the natural span: it is one consumer's unit of work on one
	// workspace, it is where sink I/O happens, and it is what backoff schedules.
	// It has no parent — the write that triggered it committed in another
	// request, and possibly on another node — so this is a trace root.
	ctx, span := rt.mx.tracer.Start(ctx, "syncplane.effects.pass",
		trace.WithAttributes(
			telemetry.AttrConsumer.String(c.name),
			telemetry.AttrWorkspace.String(workspaceID),
		))
	start := time.Now()

	position, err := rt.drain(ctx, c, workspaceID)

	attrs := metric.WithAttributes(
		telemetry.AttrConsumer.String(c.name),
		telemetry.AttrOutcome.String(telemetry.Classify(err)),
	)
	rt.mx.passes.Add(ctx, 1, attrs)
	rt.mx.passDuration.Record(ctx, time.Since(start).Seconds(), attrs)
	telemetry.EndSpan(span, err)

	if ctx.Err() != nil {
		return Health{}, err
	}
	// A pass that drained the feed dry has arrived at the head; only a failing
	// one needs it read separately to report how far behind it is stuck.
	head := position
	if err != nil {
		if h, headErr := rt.srv.CurrentSeq(ctx, workspaceID); headErr == nil {
			head = h
		}
	}
	return rt.recordPass(runnerKey{consumer: c.name, workspace: workspaceID}, position, head, err), err
}

// drain runs one pass: read the consumer's position, deliver every change after
// it, and commit how far it got. It returns the position the consumer now holds,
// which is the head of the feed when the pass drained it dry.
//
// The order inside a page is the whole design. Handlers run first, against a
// snapshot the read transaction has already released — a sink's I/O must never
// pin an engine read tx, and a handler that writes back must not be inside the
// transaction it writes through. Only then does the bookkeeping commit, so every
// crash point redelivers rather than skips.
func (rt *Runtime) drain(ctx context.Context, c *consumer, workspaceID string) (uint64, error) {
	logger := rt.logger.With("consumer", c.name, "workspace", workspaceID)
	for {
		cur, _, err := rt.srv.ConsumerPosition(ctx, workspaceID, c.name)
		if err != nil {
			return 0, err
		}
		// Collect the page inside the read snapshot, act on it outside.
		var page []server.Change
		next, more, err := rt.srv.Changes(ctx, workspaceID, cur, rt.batch, func(ch server.Change) error {
			if c.wants(ch.Entity) {
				page = append(page, ch)
			}
			return nil
		})
		if errors.Is(err, server.ErrCursorTooOld) {
			// Changes between this position and the horizon are gone,
			// tombstones included. Unlike a mirror, an effect has no way to
			// repair that: current state cannot tell you which notifications
			// were never sent. So this is a reported loss, not a recovery —
			// drop the shadows (they describe a past nothing arriving now lines
			// up against) and resume at the head.
			logger.LogAttrs(ctx, slog.LevelWarn,
				"consumer fell below the tombstone horizon; changes were missed and cannot be recovered",
				slog.Uint64("position", cur.Seq), slog.Uint64("resume_seq", next.Seq))
			if err := rt.srv.ResetConsumer(ctx, workspaceID, c.name); err != nil {
				return cur.Seq, err
			}
			if err := rt.srv.AdvanceConsumer(ctx, workspaceID, c.name, server.ConsumerUpdate{Cursor: next}); err != nil {
				return cur.Seq, err
			}
			continue
		}
		if err != nil {
			return cur.Seq, err
		}

		up := server.ConsumerUpdate{Cursor: next}
		delivered, deliverErr := rt.deliverPage(ctx, c, workspaceID, page, &up)
		// Counted even when the page then failed to commit: the sink was
		// handed these, and at-least-once means it may be handed them again.
		// Delivery attempts are what the sink's own load is made of.
		rt.mx.changes.Add(ctx, int64(delivered), metric.WithAttributes(
			telemetry.AttrConsumer.String(c.name)))
		if deliverErr != nil {
			if delivered == 0 {
				// Nothing got through: leave the position untouched so the page
				// is redelivered whole on the next attempt.
				return cur.Seq, deliverErr
			}
			// Commit the prefix that did get through. Resuming just past the
			// last delivered change keeps a wedge from re-running the handlers
			// ahead of it on every retry — the sub-position is exactly what the
			// feed's (seq, entity, key) order exists for. Changes the consumer
			// filtered out sort before it and are skipped again anyway.
			last := page[delivered-1]
			up.Cursor = server.Cursor{Seq: last.Seq, Entity: last.Entity, Key: last.Key}
			if err := rt.srv.AdvanceConsumer(ctx, workspaceID, c.name, up); err != nil {
				return cur.Seq, errors.Join(deliverErr, err)
			}
			return up.Cursor.Seq, deliverErr
		}
		// A pass over a quiet workspace has nothing to record: the position is
		// already where Changes says the feed ends and no shadow moved. Skipping
		// the write keeps an idle runtime from taking every workspace's write
		// lock on every tick, which at N consumers over M workspaces is a
		// steady drip of transactions that contend with real writes and change
		// nothing.
		if next != cur || len(up.Shadows) > 0 {
			if err := rt.srv.AdvanceConsumer(ctx, workspaceID, c.name, up); err != nil {
				return cur.Seq, err
			}
		}
		if len(page) > 0 {
			logger.LogAttrs(ctx, slog.LevelDebug, "delivered page", slog.Int("changes", len(page)))
		}
		if !more {
			return next.Seq, nil
		}
	}
}

// deliverPage hands each change to the sink in feed order, accumulating the
// shadow writes to commit with the position. It stops at the first failure and
// reports how many changes got through, so the caller can commit that prefix.
//
// A shadow is appended only AFTER its change was delivered, which is what keeps
// the two in step: a change that failed leaves both its shadow and the position
// as they were, so the retry sees the same before-state it saw the first time.
func (rt *Runtime) deliverPage(ctx context.Context, c *consumer, workspaceID string, page []server.Change, up *server.ConsumerUpdate) (int, error) {
	delivered := 0
	for _, ch := range page {
		out, shadow, err := rt.buildChange(ctx, c, workspaceID, ch)
		if err != nil {
			return delivered, err
		}
		if err := c.sink.Deliver(ctx, out); err != nil {
			return delivered, fmt.Errorf("effects: consumer %q handling %s/%s at seq %d: %w",
				c.name, ch.Entity, ch.Key, ch.Seq, err)
		}
		if shadow != nil {
			up.Shadows = append(up.Shadows, *shadow)
		}
		delivered++
	}
	return delivered, nil
}

// buildChange turns a feed change into what the sink sees, and returns the
// shadow write to land with it (nil when the consumer watches nothing on this
// entity).
func (rt *Runtime) buildChange(ctx context.Context, c *consumer, workspaceID string, ch server.Change) (Change, *server.ConsumerShadowWrite, error) {
	out := Change{
		Workspace: workspaceID,
		Entity:    ch.Entity,
		Key:       ch.Key,
		Seq:       ch.Seq,
		Deleted:   ch.Deleted,
		Actor:     ch.Actor,
		After:     ch.Message,
	}
	prototype, paths := c.watches(ch.Entity)
	if paths == nil {
		return out, nil, nil
	}
	raw, err := rt.srv.ConsumerShadow(ctx, workspaceID, c.name, ch.Entity, ch.Key)
	if err != nil {
		return Change{}, nil, err
	}
	// A deleted row has no message, so its watched fields project to unset —
	// which is the truth about them and makes the diff report what was lost.
	after, err := project(prototype, ch.Message, paths)
	if err != nil {
		return Change{}, nil, err
	}
	if raw == nil {
		out.FirstSeen = true
	} else {
		before := prototype.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(raw, before); err != nil {
			return Change{}, nil, fmt.Errorf("effects: consumer %q decoding shadow for %s/%s: %w",
				c.name, ch.Entity, ch.Key, err)
		}
		out.Before = before
		if out.Changed, err = changedFields(before, after, paths); err != nil {
			return Change{}, nil, err
		}
	}
	if ch.Deleted {
		// Drop the shadow with the row. A key that comes back later is a first
		// sighting again, rather than being diffed against what the deleted row
		// happened to hold.
		return out, &server.ConsumerShadowWrite{Entity: ch.Entity, Key: ch.Key}, nil
	}
	snapshot, err := proto.Marshal(after)
	if err != nil {
		return Change{}, nil, fmt.Errorf("effects: consumer %q encoding shadow for %s/%s: %w",
			c.name, ch.Entity, ch.Key, err)
	}
	if snapshot == nil {
		// A projection with nothing set marshals to no bytes, and a nil snapshot
		// means "delete this shadow" to the storage seam. An empty slice records
		// what it actually is: a row this consumer has seen, whose watched
		// fields are all unset.
		snapshot = []byte{}
	}
	return out, &server.ConsumerShadowWrite{Entity: ch.Entity, Key: ch.Key, Snapshot: snapshot}, nil
}
