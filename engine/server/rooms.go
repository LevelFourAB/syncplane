package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/metric"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/presence"
)

// roomTopic is the live state of one topic on a Watch stream: what the gate
// resolved for it, and whether it is still on the stream.
type roomTopic struct {
	topic  presence.Topic
	access roomAccess
	// live is false once the topic has been dropped. A dropped topic is
	// skipped by the tail loop; the client has been told and will reopen when
	// it has a usable set again.
	live bool
}

// Watch is the ephemeral plane's read side: a stream of one session's rooms.
//
// For each topic it runs the join gate, sends the roster it knows, and then
// forwards participant updates, departures and broadcasts as they arrive —
// whichever node published them. The topic set is fixed for the life of the
// stream, which is what lets every gate resolve once at open instead of per
// frame; a client that navigates somewhere else closes the stream and reopens
// it, and pays a snapshot rather than a catch-up for doing so.
//
// Failure is scoped to a topic. A room whose gate refuses, whose anchor row
// went away, or whose app hook failed is DROPPED with a typed reason while
// every other topic keeps streaming; an error is returned only when the STREAM
// cannot continue. That is the same split Subscribe makes, for the same reason.
//
// Access is re-checked while the stream runs. Every heartbeat re-runs the gate
// for each live topic — including re-reading an anchored row, memoized across
// this node's streams so a document everyone has open costs one read per
// interval rather than one per watcher — so losing access to a document drops
// its room within one heartbeat instead of whenever the client reconnects.
// Authentication is NOT re-run; a credential that expires mid-stream rides to
// the end of the stream, exactly as on Subscribe.
//
// Nothing here is durable. A frame the server could not hand over is gone, and
// the plane heals by re-announcement rather than by replay — see
// engine/presence for the contract.
func (s *Server) Watch(
	ctx context.Context,
	req *connect.Request[syncplanev1.WatchRequest],
	stream *connect.ServerStream[syncplanev1.WatchResponse],
) error {
	msg := req.Msg
	topics, err := s.validateWatch(msg)
	if err != nil {
		return err
	}

	watch := s.rooms.Open(msg.GetSessionId(), msg.GetFrontedSessionIds()...)
	defer watch.Close()

	// Every session this stream speaks for is bound to the principal that
	// opened it, so a Publish naming one has to come from the same caller.
	// Bound before any room is entered, released when the stream ends.
	subject := principalOf(ctx).Subject
	bound := make([]string, 0, 1+len(msg.GetFrontedSessionIds()))
	defer func() {
		for _, id := range bound {
			s.roomSessions.release(id, watch)
		}
	}()
	for _, id := range append([]string{msg.GetSessionId()}, msg.GetFrontedSessionIds()...) {
		if !s.roomSessions.bind(id, subject, watch) {
			return connect.NewError(connect.CodePermissionDenied,
				errors.New("session id is already held by a different principal"))
		}
		bound = append(bound, id)
	}

	s.tel().roomStreams.Add(ctx, 1)
	defer s.tel().roomStreams.Add(ctx, -1)

	limit := s.limits.MaxParticipantsPerRoom
	if want := int(msg.GetLimit()); want > 0 && want < limit {
		limit = want
	}

	logger := s.reqLog(ctx, "watch", "", msg.GetSessionId())
	open := make([]*roomTopic, 0, len(topics))
	for _, t := range topics {
		rt, err := s.joinRoom(ctx, stream, watch, t, limit)
		if err != nil {
			return err
		}
		if rt != nil {
			open = append(open, rt)
		}
	}
	if len(open) == 0 {
		// Every topic was refused. The client holds the frames explaining why;
		// an inert stream would just occupy a connection.
		return nil
	}
	s.tel().roomsJoined.Add(ctx, int64(len(open)))
	defer s.tel().roomsJoined.Add(ctx, -int64(len(open)))

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()

	events := watch.Events()
	// The outbox counts what it discarded rather than blocking on a stream
	// that is not draining; reporting the delta per heartbeat is how a
	// deployment sees the at-most-once contract actually biting.
	var reported uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				// The registry shut down under the stream.
				return nil
			}
			if err := s.sendRoomEvent(stream, open, ev); err != nil {
				return err
			}
		case <-heartbeat.C:
			if dropped := watch.Dropped(); dropped > reported {
				s.tel().roomEventsDropped.Add(ctx, int64(dropped-reported), metric.WithAttributes(
					telemetry.AttrDropReason.String("slow_stream")))
				reported = dropped
			}
			anyLive, err := s.regateRooms(ctx, stream, watch, open, logger)
			if err != nil {
				return err
			}
			if !anyLive {
				return nil
			}
			if err := stream.Send(&syncplanev1.WatchResponse{
				Body: &syncplanev1.WatchResponse_Heartbeat{Heartbeat: true},
			}); err != nil {
				return err
			}
		}
	}
}

// validateWatch enforces the request's shape before any room is touched. These
// are properties of the request, so they are RPC errors rather than dropped
// topics.
func (s *Server) validateWatch(msg *syncplanev1.WatchRequest) ([]presence.Topic, error) {
	if msg.GetSessionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("watch needs a session id"))
	}
	if err := naming.ValidateSessionID(msg.GetSessionId()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if n := len(msg.GetFrontedSessionIds()); n > s.limits.MaxRoomsPerStream {
		// Bounded by the same knob the room set is: both scale with how many
		// tabs one browser has open, and both cost per re-gate tick.
		return nil, roomLimitError(
			fmt.Sprintf("watch fronts %d sessions", n),
			"MaxRoomsPerStream", s.limits.MaxRoomsPerStream)
	}
	for _, id := range msg.GetFrontedSessionIds() {
		if err := naming.ValidateSessionID(id); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("fronted session: %w", err))
		}
	}
	wire := msg.GetTopics()
	if len(wire) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("watch needs at least one topic"))
	}
	if len(wire) > s.limits.MaxRoomsPerStream {
		return nil, roomLimitError(
			fmt.Sprintf("watch carries %d rooms", len(wire)),
			"MaxRoomsPerStream", s.limits.MaxRoomsPerStream)
	}
	topics := make([]presence.Topic, 0, len(wire))
	seen := make(map[presence.Topic]struct{}, len(wire))
	for _, w := range wire {
		t := topicOf(w)
		if err := t.Validate(); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic: %w", err))
		}
		// Two entries for one topic would deliver every frame twice and leave
		// the client unable to tell which roster it was reconciling.
		if _, dup := seen[t]; dup {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("topic %s appears more than once", t))
		}
		seen[t] = struct{}{}
		topics = append(topics, t)
	}
	return topics, nil
}

// joinRoom gates one topic and sends its snapshot. A refused topic is reported
// as a dropped frame and returns a nil roomTopic; an error means the stream
// itself could not continue.
func (s *Server) joinRoom(
	ctx context.Context,
	stream *connect.ServerStream[syncplanev1.WatchResponse],
	watch *presence.Watch,
	t presence.Topic,
	limit int,
) (*roomTopic, error) {
	access, reason, err := s.openRoom(ctx, t)
	if err != nil {
		return nil, s.sendTopicDropped(stream, t, reason, err)
	}
	// The bus subscription is taken here, after every gate has passed, so a
	// refused caller leaves nothing behind in a room they were denied.
	roster, truncated, err := watch.Join(t)
	if err != nil {
		return nil, s.sendTopicDropped(stream, t, syncplanev1.TopicDropped_REASON_ERROR, err)
	}

	visible := filterParticipants(roster, access.visible)
	if len(visible) > limit {
		// The roster is ordered by session id, so every recipient of an
		// over-full room is shown the same prefix.
		visible = visible[:limit]
		truncated = true
	}
	s.tel().roomParticipants.Record(ctx, int64(len(visible)))

	wire := make([]*syncplanev1.Participant, len(visible))
	for i, p := range visible {
		wire[i] = participantWire(p)
	}
	if err := stream.Send(&syncplanev1.WatchResponse{
		Topic: topicWire(t),
		Body: &syncplanev1.WatchResponse_Snapshot{Snapshot: &syncplanev1.RoomSnapshot{
			Participants: wire,
			Truncated:    truncated,
		}},
	}); err != nil {
		return nil, err
	}
	return &roomTopic{topic: t, access: access, live: true}, nil
}

// sendRoomEvent forwards one registry event to the stream, if the stream still
// holds its topic and the caller may see who it is about.
func (s *Server) sendRoomEvent(
	stream *connect.ServerStream[syncplanev1.WatchResponse],
	open []*roomTopic,
	ev presence.Event,
) error {
	rt := findRoomTopic(open, ev.Topic)
	if rt == nil || !rt.live {
		return nil
	}
	if rt.access.visible != nil && !rt.access.visible(ev.Participant.Subject) {
		return nil
	}
	frame := &syncplanev1.WatchResponse{Topic: topicWire(ev.Topic)}
	switch ev.Kind {
	case presence.KindState:
		frame.Body = &syncplanev1.WatchResponse_Updated{Updated: participantWire(ev.Participant)}
	case presence.KindLeave:
		frame.Body = &syncplanev1.WatchResponse_Left{Left: ev.Participant.SessionID}
	case presence.KindMessage:
		frame.Body = &syncplanev1.WatchResponse_Message{Message: &syncplanev1.RoomMessage{
			SenderSessionId: ev.Participant.SessionID,
			SenderSubject:   ev.Participant.Subject,
			Payload:         ev.Payload,
		}}
	default:
		return nil
	}
	return stream.Send(frame)
}

// regateRooms re-runs the join gate for every live topic and drops the ones it
// no longer admits. It reports whether any topic is still live.
//
// A stream outlives the decision that admitted it. Gated only at open, a client
// that loses access to a document keeps watching its room for as long as the
// stream stays healthy — which, for an open tab, can be indefinitely. Doing
// this on the heartbeat bounds that window to one heartbeat interval, on a
// timer the client cannot influence, and the anchor cache keeps the cost at one
// row read per document per interval however many people have it open.
//
// A settled denial drops the topic as ACCESS_REVOKED, the same permanent
// verdict the open-time gate sends. A gate that cannot DECIDE drops it as
// ERROR: an outage must never read as revocation, but continuing to serve a
// room whose access can no longer be affirmed is the wrong side to fail on.
func (s *Server) regateRooms(
	ctx context.Context,
	stream *connect.ServerStream[syncplanev1.WatchResponse],
	watch *presence.Watch,
	open []*roomTopic,
	logger *slog.Logger,
) (bool, error) {
	anyLive := false
	for _, rt := range open {
		if !rt.live {
			continue
		}
		access, reason, err := s.openRoom(ctx, rt.topic)
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "watch topic dropped by access re-check",
				slog.String("topic", rt.topic.String()),
				slog.String("reason", reason.String()),
				slog.String("cause", err.Error()))
			rt.live = false
			watch.Leave(rt.topic)
			if sendErr := s.sendTopicDropped(stream, rt.topic, reason, err); sendErr != nil {
				return false, sendErr
			}
			continue
		}
		// The redaction hook can change its mind too, so the filter is replaced
		// rather than kept from open.
		rt.access = access
		anyLive = true
	}
	return anyLive, nil
}

// sendTopicDropped emits a TopicDropped frame. The message is for logs only —
// clients branch on reason — so it carries the cause verbatim.
func (s *Server) sendTopicDropped(
	stream *connect.ServerStream[syncplanev1.WatchResponse],
	t presence.Topic,
	reason syncplanev1.TopicDropped_Reason,
	cause error,
) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return stream.Send(&syncplanev1.WatchResponse{
		Topic: topicWire(t),
		Body: &syncplanev1.WatchResponse_Dropped{Dropped: &syncplanev1.TopicDropped{
			Reason:  reason,
			Message: msg,
		}},
	})
}

// findRoomTopic locates a topic on the stream. The set is fixed and small, so a
// scan beats a map the stream would have to keep in step with it.
func findRoomTopic(open []*roomTopic, t presence.Topic) *roomTopic {
	for _, rt := range open {
		if rt.topic == t {
			return rt
		}
	}
	return nil
}

// filterParticipants applies the caller's redaction hook to a roster.
func filterParticipants(roster []presence.Participant, visible ParticipantFilter) []presence.Participant {
	if visible == nil {
		return roster
	}
	out := roster[:0:0]
	for _, p := range roster {
		if visible(p.Subject) {
			out = append(out, p)
		}
	}
	return out
}

// topicOf decodes a wire topic. It does not validate; Topic.Validate does.
func topicOf(w *syncplanev1.Topic) presence.Topic {
	return presence.Topic{
		WorkspaceID: w.GetWorkspaceId(),
		Name:        w.GetName(),
		Entity:      w.GetEntity(),
		Key:         w.GetKey(),
	}
}

func topicWire(t presence.Topic) *syncplanev1.Topic {
	return &syncplanev1.Topic{
		WorkspaceId: t.WorkspaceID,
		Name:        t.Name,
		Entity:      t.Entity,
		Key:         t.Key,
	}
}

func participantWire(p presence.Participant) *syncplanev1.Participant {
	return &syncplanev1.Participant{
		SessionId: p.SessionID,
		Subject:   p.Subject,
		Presence: &syncplanev1.PresenceUpdate{
			State:    p.State,
			Revision: p.Revision,
			Seq:      p.Seq,
		},
	}
}

// roomKindAttr labels a publish for telemetry. The set is closed by the oneof,
// so it is safe to key a metric by.
func roomKindAttr(k presence.Kind) metric.MeasurementOption {
	return metric.WithAttributes(telemetry.AttrRoomEventKind.String(k.String()))
}
