package presencebridge

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/presence"
)

// originRoom is one bridged topic that has local subscribers: the origin's
// roster for it as the bridge relays it, and the shard whose stream watches
// it. Guarded by Bridge.mu.
type originRoom struct {
	topic presence.Topic
	refs  int
	shard *shard
	// participants is the origin's roster without the local sessions, as the
	// last snapshot and the updates after it described it.
	participants map[string]presence.Participant
	// live is true while the stream that delivered the roster is up. Only a
	// live roster is re-announced locally, so a lost stream lets its
	// participants lapse.
	live bool
	// parkedUntil holds a topic the origin dropped out of its shard's streams
	// until a retry is due; drops counts the consecutive drops that set it.
	parkedUntil time.Time
	drops       int
}

// shard is one origin Watch stream and the topics it carries. Its goroutine
// reopens the stream when the topic set changes or the stream fails. Guarded
// by Bridge.mu, except wake.
type shard struct {
	topics map[presence.Topic]*originRoom
	// restart asks the goroutine to reopen the stream with the current topic
	// set rather than back off.
	restart bool
	cancel  context.CancelFunc
	wake    chan struct{}
}

func (s *shard) requestRestartLocked() {
	s.restart = true
	if s.cancel != nil {
		s.cancel()
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// join counts one local subscription to a bridged topic, watching it on the
// origin with the first, and returns the release of that subscription.
func (b *Bridge) join(topic presence.Topic) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return func() {}
	}
	rm, ok := b.rooms[topic]
	if ok {
		rm.refs++
		return func() { b.release(rm) }
	}
	rm = &originRoom{topic: topic, refs: 1, participants: make(map[string]presence.Participant)}
	b.rooms[topic] = rm
	b.assignLocked(rm)
	return func() { b.release(rm) }
}

// release drops one local subscription, and stops watching the topic on the
// origin with the last.
func (b *Bridge) release(rm *originRoom) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rm.refs--
	if rm.refs > 0 {
		return
	}
	if b.rooms[rm.topic] == rm {
		delete(b.rooms, rm.topic)
	}
	if s := rm.shard; s != nil {
		delete(s.topics, rm.topic)
		s.requestRestartLocked()
	}
	// Nothing on this node subscribes to the topic any more, so the roster
	// has nobody to withdraw from.
	rm.participants = nil
	rm.live = false
}

// assignLocked puts a topic on a shard with room for it, starting a new shard
// when every shard is full.
func (b *Bridge) assignLocked(rm *originRoom) {
	var target *shard
	for s := range b.shards {
		if len(s.topics) < b.cfg.maxTopics {
			target = s
			break
		}
	}
	if target == nil {
		target = &shard{topics: make(map[presence.Topic]*originRoom), wake: make(chan struct{}, 1)}
		b.shards[target] = struct{}{}
		b.wg.Add(1)
		go func() { defer b.wg.Done(); b.runShard(target) }()
	}
	target.topics[rm.topic] = rm
	rm.shard = target
	target.requestRestartLocked()
}

// runShard keeps one origin stream open over the shard's topics until the
// shard has no topics or the bridge closes.
func (b *Bridge) runShard(s *shard) {
	bo := backoff{min: b.cfg.minBackoff, max: b.cfg.maxBackoff}
	// A local stream joins its rooms one at a time, so the first open waits
	// for the burst to settle like every later topic change does.
	ok := b.pause(-1, s.wake)
	for {
		b.mu.Lock()
		if !ok || b.closed.Load() || len(s.topics) == 0 {
			delete(b.shards, s)
			b.mu.Unlock()
			return
		}
		s.restart = false
		// The topic set is read below under the same lock, so a change
		// signalled before this point is already in it; a later one signals
		// again.
		select {
		case <-s.wake:
		default:
		}
		now := time.Now()
		var topics []presence.Topic
		var retry time.Time
		for t, rm := range s.topics {
			if rm.parkedUntil.After(now) {
				if retry.IsZero() || rm.parkedUntil.Before(retry) {
					retry = rm.parkedUntil
				}
				continue
			}
			topics = append(topics, t)
		}
		ctx, cancel := context.WithCancel(b.ctx)
		s.cancel = cancel
		b.mu.Unlock()

		wait := time.Duration(-1)
		switch {
		case len(topics) > 0:
			healthy, err := b.stream(ctx, s, topics)
			b.mu.Lock()
			for _, rm := range s.topics {
				rm.live = false
			}
			restart := s.restart
			b.mu.Unlock()
			if healthy {
				bo.reset()
			}
			if !restart {
				if b.ctx.Err() == nil {
					b.cfg.logger.LogAttrs(b.ctx, slog.LevelWarn, "presencebridge: origin watch ended",
						slog.Int("topics", len(topics)), slog.Any("error", err))
				}
				wait = bo.next()
			}
		case !retry.IsZero():
			wait = time.Until(retry)
		}
		cancel()
		ok = b.pause(wait, s.wake)
	}
}

// stream runs one origin Watch over topics until it ends, relaying its
// frames. It reports whether the origin delivered any frame.
func (b *Bridge) stream(ctx context.Context, s *shard, topics []presence.Topic) (bool, error) {
	wire := make([]*syncplanev1.Topic, len(topics))
	for i, t := range topics {
		wire[i] = topicWire(t)
	}
	st, err := b.origin.Watch(ctx, connect.NewRequest(&syncplanev1.WatchRequest{
		SessionId: b.sessionID,
		Topics:    wire,
	}))
	if err != nil {
		return false, err
	}
	defer func() { _ = st.Close() }()
	healthy := false
	for st.Receive() {
		msg := st.Msg()
		if msg.GetHeartbeat() {
			continue
		}
		healthy = true
		b.relay(s, msg)
	}
	err = st.Err()
	if err == nil {
		err = errors.New("stream closed by the origin")
	}
	return healthy, err
}

// relay applies one origin frame to the topic's roster and publishes what
// changed to the local subscribers.
func (b *Bridge) relay(s *shard, msg *syncplanev1.WatchResponse) {
	topic := topicOf(msg.GetTopic())
	b.mu.Lock()
	defer b.mu.Unlock()
	rm, ok := b.rooms[topic]
	if !ok || rm.shard != s {
		return
	}
	switch body := msg.GetBody().(type) {
	case *syncplanev1.WatchResponse_Snapshot:
		roster := make(map[string]presence.Participant, len(body.Snapshot.GetParticipants()))
		for _, p := range body.Snapshot.GetParticipants() {
			if b.locals.has(topic, p.GetSessionId()) {
				continue
			}
			roster[p.GetSessionId()] = participantOf(p)
		}
		for sid, held := range rm.participants {
			if _, still := roster[sid]; !still {
				b.publishDown(leaveEvent(topic, held))
			}
		}
		// Every participant is published, changed or not: a record that lapsed
		// locally while the stream was down is admitted again, and one that did
		// not is refreshed.
		for _, p := range roster {
			b.publishDown(presence.Event{Topic: topic, Kind: presence.KindState, Participant: p})
		}
		rm.participants = roster
		rm.live = true
		rm.drops = 0
	case *syncplanev1.WatchResponse_Updated:
		p := body.Updated
		if b.locals.has(topic, p.GetSessionId()) {
			return
		}
		next := participantOf(p)
		if held, ok := rm.participants[next.SessionID]; ok && next.Revision < held.Revision {
			return
		}
		rm.participants[next.SessionID] = next
		b.publishDown(presence.Event{Topic: topic, Kind: presence.KindState, Participant: next})
	case *syncplanev1.WatchResponse_Left:
		held, ok := rm.participants[body.Left]
		if !ok {
			return
		}
		delete(rm.participants, body.Left)
		b.publishDown(leaveEvent(topic, held))
	case *syncplanev1.WatchResponse_Message:
		m := body.Message
		if b.locals.has(topic, m.GetSenderSessionId()) {
			return
		}
		b.publishDown(presence.Event{
			Topic: topic,
			Kind:  presence.KindMessage,
			Participant: presence.Participant{
				SessionID: m.GetSenderSessionId(),
				Issuer:    OriginIssuer,
				Subject:   m.GetSenderSubject(),
			},
			Payload: m.GetPayload(),
		})
	case *syncplanev1.WatchResponse_Dropped:
		// Every reason is retried. A refusal the origin calls permanent can
		// still lift for a bridge: an anchor row written locally reaches the
		// origin only when the link pushes it.
		rm.withdrawLocked(b)
		rm.drops++
		bo := backoff{min: b.cfg.minBackoff, max: b.cfg.maxBackoff}
		var delay time.Duration
		for range rm.drops {
			delay = bo.next()
		}
		rm.parkedUntil = time.Now().Add(delay)
		b.cfg.logger.LogAttrs(b.ctx, slog.LevelInfo, "presencebridge: origin dropped a room",
			slog.String("topic", topic.String()),
			slog.String("reason", body.Dropped.GetReason().String()),
			slog.String("message", body.Dropped.GetMessage()),
			slog.Duration("retry", delay))
		time.AfterFunc(delay, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if !b.closed.Load() && rm.shard == s && s.topics[topic] == rm {
				s.requestRestartLocked()
			}
		})
	}
}

// withdrawLocked removes the origin's participants from the local rooms at
// once and stops re-announcing them.
func (rm *originRoom) withdrawLocked(b *Bridge) {
	for _, held := range rm.participants {
		b.publishDown(leaveEvent(rm.topic, held))
	}
	clear(rm.participants)
	rm.live = false
}

// reannounce refreshes every participant of a live origin roster locally, as
// the origin's own node does for the participants whose streams it holds.
func (b *Bridge) reannounce() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, rm := range b.rooms {
		if !rm.live {
			continue
		}
		for _, p := range rm.participants {
			b.publishDown(presence.Event{Topic: rm.topic, Kind: presence.KindState, Participant: p})
		}
	}
}

// publishDown hands an origin event to the local subscribers. presence.Local
// never blocks, so it is safe under Bridge.mu.
func (b *Bridge) publishDown(ev presence.Event) {
	_ = b.down.Publish(b.ctx, ev)
}

// participantOf renders an origin participant as a local record. The sequence
// is cleared: it counts the origin's writes, which mean nothing locally.
func participantOf(p *syncplanev1.Participant) presence.Participant {
	return presence.Participant{
		SessionID: p.GetSessionId(),
		Issuer:    OriginIssuer,
		Subject:   p.GetSubject(),
		State:     p.GetPresence().GetState(),
		Revision:  p.GetPresence().GetRevision(),
	}
}

func leaveEvent(topic presence.Topic, p presence.Participant) presence.Event {
	return presence.Event{
		Topic: topic,
		Kind:  presence.KindLeave,
		Participant: presence.Participant{
			SessionID: p.SessionID,
			Issuer:    p.Issuer,
			Subject:   p.Subject,
		},
	}
}
