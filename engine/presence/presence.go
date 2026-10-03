// Package presence carries the sync engine's ephemeral plane: the state that
// only matters while someone is connected. Cursors, selections, "who is viewing
// this", typing indicators. None of it is stored, none of it is replayed, and
// none of it touches the mutation log.
//
// Bus is the seam a multi-node deployment plugs a shared implementation into,
// installed with server.WithPresenceBus; presencetest.Conformance is the
// behavior suite such an implementation must pass, and pgpresence is one over
// Postgres LISTEN/NOTIFY. Local is strictly single-node: an event published on
// one process never reaches watchers on another, so a two-node deployment left
// on the default shows each node only the participants it is itself serving.
//
// # At most once
//
// Delivery carries no guarantee. An event the bus could not hand over — a
// watcher that is not reading, a broker that dropped it, a payload that did not
// fit — is gone, and nothing replays it.
//
// Presence survives that because it is soft state rather than a change log:
// Registry re-announces every record it is responsible for on a fixed interval,
// and drops a record nobody has re-announced within the expiry window. A lost
// update is therefore corrected within one announce interval, and a session
// whose process died is forgotten without anything having to notice it die.
// That is what lets the plane run with no TTL table, no GC horizon and no
// storage of any kind.
//
// A broadcast message has no such backstop. It reaches whoever is watching at
// the moment it arrives and nobody else, which is the whole contract; an app
// that cannot lose one writes a row instead.
package presence

import (
	"context"
	"fmt"

	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// Topic addresses one room. It is comparable, so it is usable as a map key —
// which is how a Registry and a Bus index their subscriptions.
//
// A topic is either ANCHORED to a row (Entity and Key both set) or not (both
// empty). The distinction is an authorization one and belongs to the server:
// an anchored topic admits only callers who could read the row it names. The
// bus treats the four fields as an opaque address.
type Topic struct {
	WorkspaceID string
	Name        string
	Entity      string
	Key         string
}

// Anchored reports whether the topic names a row.
func (t Topic) Anchored() bool { return t.Entity != "" || t.Key != "" }

// Validate checks the topic's shape: a valid workspace id, a valid room name,
// and an anchor that is either complete or absent. It does NOT check that the
// entity is registered or that the row exists — those are the server's
// questions, asked at the join gate.
func (t Topic) Validate() error {
	if err := naming.ValidateWorkspaceID(t.WorkspaceID); err != nil {
		return err
	}
	if err := naming.ValidateRoomName(t.Name); err != nil {
		return err
	}
	if (t.Entity == "") != (t.Key == "") {
		return fmt.Errorf("room anchor needs both an entity and a key, got entity %q and key %q", t.Entity, t.Key)
	}
	if t.Entity != "" {
		if err := naming.ValidateEntityName(t.Entity); err != nil {
			return err
		}
		if err := naming.ValidateRowKey(t.Key); err != nil {
			return err
		}
	}
	return nil
}

// String renders a topic for logs. Anchored topics read as
// `ws/name@entity/key`.
func (t Topic) String() string {
	if t.Anchored() {
		return t.WorkspaceID + "/" + t.Name + "@" + t.Entity + "/" + t.Key
	}
	return t.WorkspaceID + "/" + t.Name
}

// Kind discriminates what an Event says.
type Kind uint8

const (
	// KindState writes or refreshes one session's record in the topic. A
	// re-announcement is one of these, carrying the revision the state was
	// first published with.
	KindState Kind = iota
	// KindLeave withdraws one session from the topic. It carries no state.
	KindLeave
	// KindMessage is a broadcast: a payload for whoever is watching now.
	KindMessage
)

func (k Kind) String() string {
	switch k {
	case KindState:
		return "state"
	case KindLeave:
		return "leave"
	case KindMessage:
		return "message"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// Participant is one session's record in one room.
type Participant struct {
	// SessionID identifies the publishing session. It is the identity every
	// other field hangs off: records are keyed by it, a departure names it, and
	// an expiry is measured per session.
	SessionID string
	// Subject is who is publishing, stamped by the server from the request
	// principal. Empty when the publisher was unauthenticated. A bus must carry
	// it verbatim and must never let a payload set it.
	Subject string
	// State is the app's own bytes, opaque to the engine and to the bus.
	State []byte
	// Revision orders two deliveries of the same session's state in the same
	// topic; see Event for the rule.
	Revision uint64
	// Seq is the workspace sequence the state was authored against, as the
	// publisher last saw it. 0 means the publisher did not say.
	Seq uint64
}

// Event is one thing that happened in one topic.
//
// # Ordering
//
// There is no ordering across topics, and none between the presence plane and
// the mutation log. Within one topic, two KindState events for the same session
// are ordered by Participant.Revision, which the publisher mints and increments
// per (session, topic): a receiver keeps the highest revision it has seen and
// discards anything lower. That is what lets a bus reorder, re-deliver or
// coalesce state without the roster flickering.
//
// A KindLeave carries no revision, so it is not ordered against the state it
// supersedes. A leave that overtakes a later state of the same session removes
// a record that is still live — and the next announce puts it back, which is
// the window the design accepts rather than the guarantee it makes.
type Event struct {
	Topic Topic
	Kind  Kind
	// Participant is the session the event concerns. KindState carries the
	// whole record; KindLeave and KindMessage set only SessionID and Subject,
	// naming the departing session and the sender respectively.
	Participant Participant
	// Payload is the broadcast body, set only for KindMessage.
	Payload []byte
}

// Bus fans presence events out to every node serving a topic.
//
// # What an implementation must guarantee
//
// Publish must not block on a watcher that is not reading; a bus that cannot
// deliver drops instead, and drops are expected (see the package doc). Publish
// returns an error only when the event could not be ACCEPTED at all — an
// oversize payload, a broker that is down — so a caller can surface the failure
// to the publisher rather than have it disappear silently.
//
// Subscribe returns a channel of events for exactly one topic and a cancel
// func. The channel is closed after cancel, and cancel is idempotent. A
// subscriber sees events published on any node, including its own, and
// including its own publishes.
//
// An implementation MAY coalesce KindState events for one (topic, session,
// subject) down to the one with the highest revision, keeping the later of two
// equal revisions, because a receiver keeps only the highest anyway. It must
// NOT keep a lower revision in place of a higher one — events do not always
// arrive in revision order — and must NOT fold together events of two
// subjects, because a receiver keeps a record for the subject that wrote it.
// It must NOT coalesce KindMessage events, which are each distinct — it may
// only drop them.
//
// Every method is safe for concurrent use.
type Bus interface {
	Publish(ctx context.Context, ev Event) error
	Subscribe(topic Topic) (<-chan Event, func())
}
