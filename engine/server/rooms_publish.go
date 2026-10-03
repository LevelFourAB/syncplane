package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/presence"
)

// Publish is the ephemeral plane's write side: state, broadcasts and
// departures, in one unary round trip.
//
// It is unary because only RECEIVING a room needs a stream — and because the
// browser ConnectRPC subset has no client-streaming to make it anything else.
// A client that watches nothing may still publish, and a client whose stream is
// held by another tab publishes directly rather than through it.
//
// # Identity
//
// Every item is stamped with the caller's own principal. Nothing in the payload
// names the publisher, so presence cannot be used to impersonate. The
// session_id is self-asserted like a client id. When this node holds Watch
// streams for the session, the caller must have opened one of them.
//
// # Authorization
//
// Each item's topic passes the same gate a join does, so a room cannot be
// written to by someone who could not watch it. A topic this session is already
// watching on this node skips it: the gate ran when the topic was joined and
// the stream's heartbeat keeps re-running it, so charging a storage read to
// every cursor movement would buy nothing.
//
// Items are independent. There is no atomicity across them and none is implied;
// a failure is reported for the request as a whole, and what was already handed
// to the bus stays handed over.
//
// The response carries the announce interval, which is what a publisher holding
// no Watch stream needs: nothing re-announces its records, so they lapse three
// intervals after its last publish unless it re-publishes them itself.
func (s *Server) Publish(
	ctx context.Context,
	req *connect.Request[syncplanev1.PublishRequest],
) (*connect.Response[syncplanev1.PublishResponse], error) {
	msg := req.Msg
	if err := s.validatePublish(msg); err != nil {
		return nil, err
	}

	principal := principalOf(ctx)
	if !s.roomSessions.check(msg.GetSessionId(), principal.Subject) {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("session id is held on this node by a different principal only"))
	}

	for _, item := range msg.GetItems() {
		ev, err := s.publishEvent(ctx, msg.GetSessionId(), principal, item)
		if err != nil {
			return nil, err
		}
		if err := s.rooms.Publish(ctx, ev); err != nil {
			if errors.Is(err, presence.ErrClosed) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("server is shutting down"))
			}
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("presence bus: %w", err))
		}
		s.tel().roomPublishes.Add(ctx, 1, roomKindAttr(ev.Kind))
	}
	return connect.NewResponse(&syncplanev1.PublishResponse{
		AnnounceInterval: durationpb.New(s.rooms.Announce()),
	}), nil
}

// validatePublish enforces the request's shape and its size limits before any
// room is touched.
func (s *Server) validatePublish(msg *syncplanev1.PublishRequest) error {
	if err := naming.ValidateSessionID(msg.GetSessionId()); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	items := msg.GetItems()
	if len(items) == 0 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("publish needs at least one item"))
	}
	if len(items) > s.limits.MaxPublishItems {
		return roomLimitError(
			fmt.Sprintf("publish carries %d items", len(items)),
			"MaxPublishItems", s.limits.MaxPublishItems)
	}
	for _, item := range items {
		if err := topicOf(item.GetTopic()).Validate(); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("topic: %w", err))
		}
		switch body := item.GetBody().(type) {
		case *syncplanev1.PublishItem_Presence:
			if n := len(body.Presence.GetState()); n > s.limits.MaxPresenceStateBytes {
				return roomLimitError(
					fmt.Sprintf("presence state is %d bytes", n),
					"MaxPresenceStateBytes", s.limits.MaxPresenceStateBytes)
			}
		case *syncplanev1.PublishItem_Message:
			if n := len(body.Message); n > s.limits.MaxBroadcastBytes {
				return roomLimitError(
					fmt.Sprintf("broadcast is %d bytes", n),
					"MaxBroadcastBytes", s.limits.MaxBroadcastBytes)
			}
		case *syncplanev1.PublishItem_Leave:
			// A leave carries nothing to bound.
		default:
			return connect.NewError(connect.CodeInvalidArgument,
				errors.New("publish item sets no body"))
		}
	}
	return nil
}

// publishEvent gates one item's topic and builds the event for it, with the
// subject taken from the principal rather than from anything the client sent.
func (s *Server) publishEvent(
	ctx context.Context,
	sessionID string,
	principal Principal,
	item *syncplanev1.PublishItem,
) (presence.Event, error) {
	t := topicOf(item.GetTopic())
	if !s.roomSessions.joined(sessionID, principal.Subject, t) {
		if _, _, err := s.openRoom(ctx, t); err != nil {
			if connect.CodeOf(err) != connect.CodeUnknown {
				return presence.Event{}, err
			}
			return presence.Event{}, connect.NewError(connect.CodePermissionDenied, err)
		}
	}

	who := presence.Participant{SessionID: sessionID, Subject: principal.Subject}
	switch body := item.GetBody().(type) {
	case *syncplanev1.PublishItem_Presence:
		who.State = body.Presence.GetState()
		who.Revision = body.Presence.GetRevision()
		who.Seq = body.Presence.GetSeq()
		return presence.Event{Topic: t, Kind: presence.KindState, Participant: who}, nil
	case *syncplanev1.PublishItem_Message:
		return presence.Event{Topic: t, Kind: presence.KindMessage, Participant: who, Payload: body.Message}, nil
	default:
		return presence.Event{Topic: t, Kind: presence.KindLeave, Participant: who}, nil
	}
}
