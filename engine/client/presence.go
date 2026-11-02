package client

import (
	"context"
	"iter"
	"sync/atomic"

	"connectrpc.com/connect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// Room is one presence session in one room: what this Client publishes there
// and watches there. Presence state and broadcasts are ephemeral; the server
// stores none of them. A Room is safe for concurrent use.
type Room struct {
	c       *Client
	topic   *syncplanev1.Topic
	session string
	rev     atomic.Uint64
}

// Room returns a session in the room named name of the workspace. The room is
// unanchored: workspace access admits a caller. Use AnchoredRoom for a room
// that follows the read access of one row. The session id is random per Room.
func (w *Workspace) Room(name string) *Room {
	return w.room(&syncplanev1.Topic{WorkspaceId: w.id, Name: name})
}

// AnchoredRoom returns a session in the room named name that is anchored to
// the row of entity and key: only a caller that can read the row is admitted.
func (w *Workspace) AnchoredRoom(name, entity, key string) *Room {
	return w.room(&syncplanev1.Topic{WorkspaceId: w.id, Name: name, Entity: entity, Key: key})
}

func (w *Workspace) room(t *syncplanev1.Topic) *Room {
	return &Room{c: w.c, topic: t, session: randomID()}
}

// SessionID returns the session id the Room publishes and watches as.
func (r *Room) SessionID() string { return r.session }

// SetPresence replaces this session's presence state in the room with state,
// at the next revision. seq is the workspace sequence the state was made at,
// or 0 when it does not matter. While no Watch of this Room is open, the
// state expires unless SetPresence is called again at the interval the
// returned response names.
func (r *Room) SetPresence(ctx context.Context, state []byte, seq uint64) (*syncplanev1.PublishResponse, error) {
	return r.publish(ctx, &syncplanev1.PublishItem{Topic: r.topic, Body: &syncplanev1.PublishItem_Presence{
		Presence: &syncplanev1.PresenceUpdate{State: state, Revision: r.rev.Add(1), Seq: seq},
	}})
}

// Broadcast sends payload to every session watching the room now, this one
// included. Nobody receives it later.
func (r *Room) Broadcast(ctx context.Context, payload []byte) error {
	_, err := r.publish(ctx, &syncplanev1.PublishItem{Topic: r.topic, Body: &syncplanev1.PublishItem_Message{Message: payload}})
	return err
}

// Leave withdraws this session from the room; watchers see it leave at once.
func (r *Room) Leave(ctx context.Context) error {
	_, err := r.publish(ctx, &syncplanev1.PublishItem{Topic: r.topic, Body: &syncplanev1.PublishItem_Leave{Leave: true}})
	return err
}

func (r *Room) publish(ctx context.Context, item *syncplanev1.PublishItem) (*syncplanev1.PublishResponse, error) {
	resp, err := r.c.presence.Publish(ctx, connect.NewRequest(&syncplanev1.PublishRequest{
		SessionId: r.session,
		Items:     []*syncplanev1.PublishItem{item},
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Watch opens a stream on the room and yields its frames: the roster first,
// then updates, departures, broadcasts and at most one TopicDropped.
// Heartbeats are not yielded. While the stream is open the server keeps this
// session's presence alive.
//
// The stream does not reconnect. Iteration ends when ctx is done (without an
// error), after a dropped frame, or when the stream fails, which it yields as
// an error.
func (r *Room) Watch(ctx context.Context) iter.Seq2[*syncplanev1.WatchResponse, error] {
	return func(yield func(*syncplanev1.WatchResponse, error) bool) {
		stream, err := r.c.presence.Watch(ctx, connect.NewRequest(&syncplanev1.WatchRequest{
			SessionId: r.session,
			Topics:    []*syncplanev1.Topic{r.topic},
		}))
		if err != nil {
			if ctx.Err() == nil {
				yield(nil, err)
			}
			return
		}
		defer stream.Close()
		for stream.Receive() {
			msg := stream.Msg()
			if msg.GetHeartbeat() {
				continue
			}
			if !yield(msg, nil) || msg.GetDropped() != nil {
				return
			}
		}
		if err := stream.Err(); err != nil && ctx.Err() == nil {
			yield(nil, err)
		}
	}
}
