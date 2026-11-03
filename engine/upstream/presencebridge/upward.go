package presencebridge

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"connectrpc.com/connect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/presence"
)

// maxUpQueued bounds the events waiting for the origin. State folds per
// session, so only broadcasts and departures grow it.
const maxUpQueued = 4096

// publishTimeout bounds one Publish to the origin, so a stalled origin delays
// the queue rather than holding it forever.
const publishTimeout = 10 * time.Second

// upQueue holds local events on their way to the origin. It never blocks a
// publisher: a session's queued state is replaced in place by a newer one, and
// an event that finds the queue full is dropped.
type upQueue struct {
	mu      sync.Mutex
	items   []*presence.Event
	stateAt map[sessionKey]*presence.Event
	wake    chan struct{}
}

func (q *upQueue) init() {
	q.stateAt = make(map[sessionKey]*presence.Event)
	q.wake = make(chan struct{}, 1)
}

func (q *upQueue) push(ev presence.Event) {
	k := sessionKey{topic: ev.Topic, session: ev.Participant.SessionID}
	q.mu.Lock()
	switch ev.Kind {
	case presence.KindState:
		if waiting, ok := q.stateAt[k]; ok {
			if ev.Participant.Revision >= waiting.Participant.Revision {
				*waiting = ev
			}
			q.mu.Unlock()
			return
		}
	case presence.KindLeave:
		// A state pushed after this leave must queue behind it rather than
		// fold into a slot in front of it, or it would land first and the
		// leave would withdraw it.
		delete(q.stateAt, k)
	}
	if len(q.items) >= maxUpQueued {
		q.mu.Unlock()
		return
	}
	queued := &ev
	q.items = append(q.items, queued)
	if ev.Kind == presence.KindState {
		q.stateAt[k] = queued
	}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// take empties the queue and returns what it held, in queue order.
func (q *upQueue) take() []*presence.Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.items
	q.items = nil
	clear(q.stateAt)
	return items
}

// sendLoop publishes queued events to the origin until the bridge closes.
func (b *Bridge) sendLoop() {
	bo := backoff{min: b.cfg.minBackoff, max: b.cfg.maxBackoff}
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.up.wake:
		}
		failed := false
		for _, req := range b.requests(b.up.take()) {
			if err := b.publishUp(req); err != nil {
				// The rest of the batch is dropped with this one: presence heals
				// on the next local re-announcement, and an unreachable origin
				// would refuse it the same way.
				failed = true
				break
			}
		}
		if !failed {
			bo.reset()
			continue
		}
		if !b.pause(bo.next(), nil) {
			return
		}
	}
}

// publishUp sends one request. It returns an error only when the origin could
// not be reached; a refusal is logged and the request is dropped, since the
// next one is refused the same way.
func (b *Bridge) publishUp(req *syncplanev1.PublishRequest) error {
	ctx, cancel := context.WithTimeout(b.ctx, publishTimeout)
	defer cancel()
	resp, err := b.origin.Publish(ctx, connect.NewRequest(req))
	if err == nil {
		b.noteOriginAnnounce(resp.Msg.GetAnnounceInterval().AsDuration())
		return nil
	}
	if b.ctx.Err() != nil {
		return err
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeUnknown, connect.CodeDeadlineExceeded,
		connect.CodeInternal, connect.CodeAborted, connect.CodeCanceled:
		b.cfg.logger.LogAttrs(b.ctx, slog.LevelWarn, "presencebridge: origin publish failed",
			slog.String("session", req.GetSessionId()), slog.String("error", err.Error()))
		return err
	default:
		b.cfg.logger.LogAttrs(b.ctx, slog.LevelDebug, "presencebridge: origin refused a publish",
			slog.String("session", req.GetSessionId()), slog.String("error", err.Error()))
		return nil
	}
}

// noteOriginAnnounce records the origin's announce interval and warns once
// when local records would lapse on the origin between two forwards.
func (b *Bridge) noteOriginAnnounce(d time.Duration) {
	if d <= 0 {
		return
	}
	if prev := b.originAnnounce.Swap(int64(d)); prev != int64(d) && d < b.cfg.announce {
		b.cfg.logger.LogAttrs(b.ctx, slog.LevelWarn,
			"presencebridge: the origin announces more often than the bridge; local participants can lapse on the origin",
			slog.Duration("origin", d), slog.Duration("bridge", b.cfg.announce))
	}
}

// requests groups events into Publish requests: one session per request, in
// the order the session's events were queued, at most maxPublishItems items
// each.
func (b *Bridge) requests(events []*presence.Event) []*syncplanev1.PublishRequest {
	if len(events) == 0 {
		return nil
	}
	var order []string
	bySession := make(map[string][]*syncplanev1.PublishItem)
	for _, ev := range events {
		sid := ev.Participant.SessionID
		if _, ok := bySession[sid]; !ok {
			order = append(order, sid)
		}
		bySession[sid] = append(bySession[sid], publishItem(ev))
	}
	var reqs []*syncplanev1.PublishRequest
	for _, sid := range order {
		items := bySession[sid]
		for len(items) > 0 {
			n := min(len(items), b.cfg.maxPublishItems)
			reqs = append(reqs, &syncplanev1.PublishRequest{SessionId: sid, Items: items[:n]})
			items = items[n:]
		}
	}
	return reqs
}

// publishItem renders a local event as the origin takes it. The sequence is
// cleared: it counts the local engine's writes, which mean nothing on the
// origin.
func publishItem(ev *presence.Event) *syncplanev1.PublishItem {
	item := &syncplanev1.PublishItem{Topic: topicWire(ev.Topic)}
	switch ev.Kind {
	case presence.KindState:
		item.Body = &syncplanev1.PublishItem_Presence{Presence: &syncplanev1.PresenceUpdate{
			State:    ev.Participant.State,
			Revision: ev.Participant.Revision,
		}}
	case presence.KindMessage:
		item.Body = &syncplanev1.PublishItem_Message{Message: ev.Payload}
	default:
		item.Body = &syncplanev1.PublishItem_Leave{Leave: true}
	}
	return item
}

func topicWire(t presence.Topic) *syncplanev1.Topic {
	return &syncplanev1.Topic{WorkspaceId: t.WorkspaceID, Name: t.Name, Entity: t.Entity, Key: t.Key}
}

func topicOf(w *syncplanev1.Topic) presence.Topic {
	return presence.Topic{
		WorkspaceID: w.GetWorkspaceId(),
		Name:        w.GetName(),
		Entity:      w.GetEntity(),
		Key:         w.GetKey(),
	}
}

// backoff is an exponential retry delay, jittered over the upper half of each
// delay.
type backoff struct {
	min, max time.Duration
	cur      time.Duration
}

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	} else {
		b.cur = min(b.cur*2, b.max)
	}
	half := b.cur / 2
	return half + rand.N(half+1)
}

func (b *backoff) reset() { b.cur = 0 }
