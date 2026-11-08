package upstream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

const (
	// streamStable is how long a stream must stay open before its end counts
	// as a disconnect rather than a failure to connect, which resets the
	// backoff.
	streamStable = 10 * time.Second
	// A stream may stay silent for three of the origin's heartbeat
	// intervals, at least streamIdleMin and at most streamIdle. Until the
	// link has seen two heartbeats it allows streamIdleProbe, doubled each
	// time a stream is ended for silence: an origin whose interval is longer
	// costs a few reconnects, not a stream that stays dead for minutes.
	streamIdle      = 90 * time.Second
	streamIdleMin   = 2 * time.Second
	streamIdleProbe = 10 * time.Second
)

// idleLimit is how long a stream may stay silent, given the longest gap seen
// between two heartbeats.
func (l *Link) idleLimit() time.Duration {
	if l.cfg.StreamIdleTimeout > 0 {
		return l.cfg.StreamIdleTimeout
	}
	hb := time.Duration(l.heartbeat.Load())
	if hb <= 0 {
		if p := time.Duration(l.idleProbe.Load()); p > 0 {
			return p
		}
		return streamIdleProbe
	}
	return min(max(3*hb, streamIdleMin), streamIdle)
}

// runStreams keeps one Subscribe stream open per client id over every ready
// workspace, reopening it when the set changes. While the stream cannot be
// opened, it pulls the ready workspaces between attempts.
func (l *Link) runStreams(ctx context.Context) {
	bo := newBackoff(l.cfg.RetryMin, l.cfg.RetryMax)
	for {
		groups := l.streamGroups()
		if len(groups) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-l.streamWake:
				continue
			}
		}
		sctx, cancel := context.WithCancel(ctx)
		errs := make(chan error, len(groups))
		for cid, wss := range groups {
			go func() { errs <- l.stream(sctx, cid, wss) }()
		}
		started := time.Now()
		left := len(groups)
		var err error
		select {
		case <-ctx.Done():
		case <-l.streamWake:
		case err = <-errs:
			left--
		}
		cancel()
		for ; left > 0; left-- {
			<-errs
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			continue
		}
		if time.Since(started) > streamStable {
			bo.reset()
		}
		l.log.LogAttrs(ctx, slog.LevelDebug, "stream ended; retrying", slog.Any("error", err))
		for _, wss := range groups {
			for _, w := range wss {
				w.update(func(s *status) { s.offline, s.live, s.lastErr = true, false, err })
			}
		}
		l.pullFallback(ctx, groups)
		t := time.NewTimer(bo.next())
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		case <-l.streamWake:
			t.Stop()
		}
	}
}

// streamGroups returns the ready workspaces by client id.
func (l *Link) streamGroups() map[string][]*workspace {
	l.mu.Lock()
	all := make([]*workspace, 0, len(l.ws))
	for _, w := range l.ws {
		all = append(all, w)
	}
	l.mu.Unlock()
	groups := map[string][]*workspace{}
	for _, w := range all {
		if w.isReady() {
			groups[w.clientID] = append(groups[w.clientID], w)
		}
	}
	return groups
}

// pullFallback pulls every ready workspace once, so they keep up while the
// stream is down. A workspace whose pull fails goes back to its worker.
func (l *Link) pullFallback(ctx context.Context, groups map[string][]*workspace) {
	for _, wss := range groups {
		for _, w := range wss {
			if !w.isReady() || !w.pullMu.TryLock() {
				continue
			}
			err := w.pullDrainLocked(ctx)
			w.pullMu.Unlock()
			if err != nil && !errors.Is(err, errReset) && ctx.Err() == nil && !isTransient(err) {
				w.unready(false, false)
			}
		}
	}
}

// isTransient reports a failure in transit, which the next attempt may not
// meet.
func isTransient(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeCanceled,
		connect.CodeUnknown, connect.CodeAborted, connect.CodeInternal:
		return true
	}
	return false
}

// stream runs one Subscribe stream over wss. It returns nil when every
// workspace left the stream or the stream must reopen with other
// no_delta_entities, and the stream's error otherwise.
func (l *Link) stream(ctx context.Context, clientID string, wss []*workspace) error {
	noDelta := l.noDeltaEntities()
	req := &syncplanev1.SubscribeRequest{ClientId: clientID, Limit: l.cfg.PageLimit, NoDeltaEntities: noDelta}
	byID := map[string]*workspace{}
	gens := map[string]uint64{}
	for _, w := range wss {
		w.mu.Lock()
		meta, ready, gen := w.meta, w.ready && !w.needBoot, w.subGen
		w.mu.Unlock()
		gens[w.id] = gen
		if !ready {
			continue
		}
		req.Scopes = append(req.Scopes, &syncplanev1.WorkspaceScope{
			WorkspaceId:   w.id,
			Cursor:        meta.cursor,
			Subscriptions: l.effectiveSubs(meta.subs),
			MinSeq:        meta.ackedSeq,
		})
		byID[w.id] = w
	}
	if len(byID) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var idled atomic.Bool
	watchdog := time.AfterFunc(l.idleLimit(), func() {
		idled.Store(true)
		cancel()
	})
	defer watchdog.Stop()
	defer func() {
		if idled.Load() && l.heartbeat.Load() == 0 {
			l.idleProbe.Store(int64(min(2*l.idleLimit(), streamIdle)))
		}
	}()
	var lastBeat time.Time
	opened := time.Now()
	stream, err := l.rpc.Subscribe(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	defer stream.Close()
	for stream.Receive() {
		msg := stream.Msg()
		if msg.GetHeartbeat() {
			now := time.Now()
			if !lastBeat.IsZero() {
				if gap := now.Sub(lastBeat); gap > time.Duration(l.heartbeat.Load()) {
					l.heartbeat.Store(int64(gap))
				}
			} else if l.heartbeat.Load() == 0 {
				// The first heartbeat bounds the interval from below: allow
				// three such gaps before the next one.
				if p := min(3*now.Sub(opened), streamIdle); p > time.Duration(l.idleProbe.Load()) {
					l.idleProbe.Store(int64(p))
				}
			}
			lastBeat = now
		}
		watchdog.Reset(l.idleLimit())
		w := byID[msg.GetWorkspaceId()]
		if w == nil {
			continue
		}
		w.observeRevision(msg.GetSchemaRevision())
		switch {
		case msg.GetPage() != nil:
			w.pullMu.Lock()
			// Checked under pullMu: a page that arrives after the workspace
			// left the stream must not land on top of what the worker pulls.
			var err error
			if w.isReady() && !w.behind(msg.GetPage()) {
				err = w.applyPage(ctx, msg.GetPage(), noDelta)
			}
			w.pullMu.Unlock()
			switch {
			case err == nil, errors.Is(err, errReset), errors.Is(err, ErrNotLinked):
			case errors.Is(err, errFullState):
				// Reopened at once with the entity in no_delta_entities, from
				// the cursors the page did not advance.
				return nil
			case ctx.Err() != nil:
				return ctx.Err()
			default:
				return err
			}
		case msg.GetCaughtUp():
			w.caughtUp(gens[w.id])
		case msg.GetDropped() != nil:
			delete(byID, w.id)
			switch msg.GetDropped().GetReason() {
			case syncplanev1.WorkspaceDropped_REASON_RESET:
				w.unready(true, true)
			case syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED:
				w.mu.Lock()
				w.revokeNow = true
				w.mu.Unlock()
				w.unready(false, false)
			default:
				w.unready(false, false)
			}
			if len(byID) == 0 {
				return nil
			}
		}
	}
	if err := stream.Err(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return io.ErrUnexpectedEOF
}

// behind reports a page whose position is older than the stored cursor's:
// applying it would move the cursor back.
func (w *workspace) behind(page *syncplanev1.PullResponse) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return page.GetSeq() != 0 && page.GetSeq() < w.meta.originSeq
}
