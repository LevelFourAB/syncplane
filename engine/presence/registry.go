package presence

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// DefaultAnnounce is the interval a Registry re-announces its own records on,
// and the unit its expiry window is a multiple of.
//
// It is the knob the whole plane's feel hangs off. Shorter makes a departure
// that was never announced — a killed tab, a lost connection — disappear
// sooner, and costs one bus event per local record per interval. Longer costs
// less and leaves ghosts on screen for longer. It must also stay comfortably
// under the window a client's own reconnect takes, or a watcher that reconnects
// mid-interval sees the room empty until the next sweep.
const DefaultAnnounce = 5 * time.Second

// expiryFactor turns the announce interval into the expiry window. A record is
// dropped once it has missed this many announcements, so the window absorbs one
// lost event and one late sweep before a live session is declared gone.
const expiryFactor = 3

// DefaultMaxParticipants caps the records a Registry keeps per room.
const DefaultMaxParticipants = 128

// ErrClosed is returned by a Registry whose Close has run.
var ErrClosed = errors.New("presence: registry is closed")

// Options configures a Registry. The zero value is valid and takes every
// default.
type Options struct {
	// Announce is the re-announce interval; see DefaultAnnounce. Zero or
	// negative takes the default.
	Announce time.Duration
	// MaxParticipants caps the records kept per room. Zero or negative takes
	// DefaultMaxParticipants. Beyond it a room keeps the lowest session ids and
	// reports itself truncated — lowest rather than newest so that two nodes
	// holding the same over-full room keep the same participants, and every
	// watcher is shown the same prefix.
	MaxParticipants int
	// Now reads the current time, and is the only clock a Registry consults.
	// Nil takes time.Now.
	Now func() time.Time
}

func (o Options) withDefaults() Options {
	if o.Announce <= 0 {
		o.Announce = DefaultAnnounce
	}
	if o.MaxParticipants <= 0 {
		o.MaxParticipants = DefaultMaxParticipants
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Registry is one node's view of the rooms it is serving, and the state machine
// that keeps that view honest without storing anything.
//
// It holds a record per (topic, session) it has heard about, subscribes to the
// bus once per topic however many local streams watch it, and fans what arrives
// out to those streams. Sweep is the whole liveness mechanism: it re-announces
// the records of sessions whose Watch stream this node holds, and drops records
// nobody has re-announced within the expiry window, reporting them as
// departures. Nothing else expires anything, and nothing is persisted.
//
// A Registry is safe for concurrent use. It reads the clock only through
// Options.Now, so expiry and re-announcement advance on the caller's clock
// rather than on wall time.
type Registry struct {
	bus  Bus
	opts Options

	mu     sync.Mutex
	rooms  map[Topic]*room
	closed bool
}

// NewRegistry returns a Registry fanning out over bus.
func NewRegistry(bus Bus, opts Options) *Registry {
	return &Registry{
		bus:   bus,
		opts:  opts.withDefaults(),
		rooms: make(map[Topic]*room),
	}
}

// Announce reports the re-announce interval in force, which is also the
// interval Sweep is meant to be called on.
func (r *Registry) Announce() time.Duration { return r.opts.Announce }

// MaxParticipants reports the per-room record cap in force.
func (r *Registry) MaxParticipants() int { return r.opts.MaxParticipants }

// room is one topic's node-local state, held for as long as some local stream
// watches it.
type room struct {
	topic        Topic
	participants map[string]*record
	watchers     map[*Watch]struct{}
	// cancel releases the single bus subscription every local watcher of this
	// topic shares; stopped closes behind the forwarder goroutine.
	cancel func()
	// truncated records that the room has turned a participant away for the
	// cap. It is sticky for the room's life: a watcher told its roster is
	// partial must not be told otherwise by a later snapshot that happens to
	// fit.
	truncated bool
}

// record is one session's presence in one room, plus the liveness the Registry
// tracks alongside it.
type record struct {
	p Participant
	// seenAt is when the record was last written or re-announced. Expiry is
	// measured from it and from nothing else.
	seenAt time.Time
}

// Watch is one Watch stream's registration with a Registry: the topics it has
// joined and the single channel every frame for them arrives on.
//
// Events is the channel the stream's tail loop selects on. It is closed when
// Close runs or the Registry shuts down, which is how a stream learns to end.
type Watch struct {
	reg       *Registry
	sessionID string
	// fronted is every session this stream keeps alive, sessionID included. It
	// is fixed for the stream's life and read without a lock.
	fronted map[string]struct{}
	out     *outbox

	mu     sync.Mutex
	topics map[Topic]struct{}
	closed bool
}

// Open registers a stream for sessionID, plus any further sessions it is
// responsible for. Those ids are what records are keyed on, and holding them
// here is what makes this node re-announce them.
//
// A browser shares one stream across an account's tabs and the tabs that are
// not holding it still publish, so fronting several sessions is the normal
// case rather than an exception; a session named by nobody's stream is
// forgotten within the expiry window.
//
// The returned Watch must be closed when the stream ends; leaving it open
// leaves this node re-announcing records for sessions that are gone.
func (r *Registry) Open(sessionID string, fronted ...string) *Watch {
	all := make(map[string]struct{}, len(fronted)+1)
	all[sessionID] = struct{}{}
	for _, id := range fronted {
		all[id] = struct{}{}
	}
	return &Watch{
		reg:       r,
		sessionID: sessionID,
		fronted:   all,
		out:       newOutbox(),
		topics:    make(map[Topic]struct{}),
	}
}

// SessionID reports the session that opened this stream. Its records, and those
// of every session it fronts, are re-announced for as long as it is open.
func (w *Watch) SessionID() string { return w.sessionID }

// Events carries every frame for every topic this stream has joined. It is
// closed on Close.
func (w *Watch) Events() <-chan Event { return w.out.out }

// Dropped reports how many events this stream's outbox has discarded because
// the stream was not reading fast enough.
func (w *Watch) Dropped() uint64 { return w.out.dropCount() }

// Join adds a topic to this stream and returns the node's current roster for
// it, ordered by session id, together with whether the room has turned
// participants away for the cap.
//
// The roster is what this node knows, which is the sessions whose streams it
// holds plus whatever the bus has told it. A node that has just created the
// room sees an empty one until the other nodes' next announce.
//
// Joining a topic twice is a no-op that still returns the roster.
func (w *Watch) Join(topic Topic) (participants []Participant, truncated bool, err error) {
	// w.mu is held across the Registry's lock, never the other way round: the
	// Registry's own fan-out reaches a stream through its outbox rather than
	// through this lock, so the order only ever runs in this direction.
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, false, ErrClosed
	}

	r := w.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, ErrClosed
	}
	w.topics[topic] = struct{}{}
	rm := r.roomLocked(topic)
	rm.watchers[w] = struct{}{}
	return rm.snapshotLocked(), rm.truncated, nil
}

// Leave removes a topic from this stream. The stream stops receiving its
// frames; the session's RECORD in that room is untouched, and withdrawing it is
// a Publish of a leave event.
func (w *Watch) Leave(topic Topic) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.topics, topic)

	r := w.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropWatcherLocked(topic, w)
}

// Close releases every topic this stream joined and closes Events. It is
// idempotent.
func (w *Watch) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	topics := make([]Topic, 0, len(w.topics))
	for t := range w.topics {
		topics = append(topics, t)
	}
	w.topics = nil
	w.mu.Unlock()

	r := w.reg
	r.mu.Lock()
	for _, t := range topics {
		r.dropWatcherLocked(t, w)
	}
	r.mu.Unlock()
	w.out.close()
}

// Joined reports whether this stream currently holds the topic. It is the
// cheap answer to "has this session already been admitted here", which a
// publisher on the same node can rely on instead of re-running the gate.
func (w *Watch) Joined(topic Topic) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.topics[topic]
	return ok
}

// Publish sends an event to every node serving its topic, this one included.
// The event comes back through the bus subscription rather than being applied
// here, so a publisher's own node updates on exactly the path every other node
// does.
//
// It returns the bus's error when the event could not be accepted at all; a
// successful return means the event was handed over, not that anybody received
// it.
func (r *Registry) Publish(ctx context.Context, ev Event) error {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return r.bus.Publish(ctx, ev)
}

// Sweep advances the liveness machinery to now. It is meant to be called every
// Announce interval, and does two things in one pass:
//
//   - Re-announces every record whose session has a Watch stream on this node,
//     so the other nodes keep it alive. The record's own revision is carried
//     unchanged, so an announcement that overtakes a newer state is discarded
//     by revision rather than winning.
//   - Drops every record that has gone an expiry window without being written
//     or re-announced, reporting it to local watchers as a departure.
//
// A record this node owns is also refreshed locally, so a bus that dropped the
// announcement costs the other nodes their view of it and not this one's.
//
// Publishing happens with r's lock released, so a slow bus delays the next
// sweep rather than blocking every join and publish behind it.
func (r *Registry) Sweep(ctx context.Context) {
	expiry := time.Duration(expiryFactor) * r.opts.Announce
	now := r.opts.Now()

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	var announce []Event
	for _, rm := range r.rooms {
		for sid, rec := range rm.participants {
			if rm.ownsLocked(sid) {
				rec.seenAt = now
				announce = append(announce, Event{
					Topic:       rm.topic,
					Kind:        KindState,
					Participant: rec.p,
				})
				continue
			}
			if now.Sub(rec.seenAt) > expiry {
				rm.removeLocked(sid)
			}
		}
	}
	r.mu.Unlock()

	// One record's announcement failing says nothing about the next, and the
	// record survives locally either way — the loss shows up on the OTHER nodes
	// as a roster one participant short, until the bus recovers and the next
	// sweep fills it back in.
	for _, ev := range announce {
		_ = r.bus.Publish(ctx, ev)
	}
}

// Close releases every bus subscription and closes every open Watch. It is
// idempotent, and a Registry does not come back.
func (r *Registry) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	rooms := r.rooms
	r.rooms = make(map[Topic]*room)
	watchers := make(map[*Watch]struct{})
	for _, rm := range rooms {
		rm.cancel()
		for w := range rm.watchers {
			watchers[w] = struct{}{}
		}
	}
	r.mu.Unlock()

	for w := range watchers {
		w.Close()
	}
}

// roomLocked returns the topic's room, creating it — and its single bus
// subscription — on first use. Called with r.mu held.
func (r *Registry) roomLocked(topic Topic) *room {
	if rm, ok := r.rooms[topic]; ok {
		return rm
	}
	events, cancel := r.bus.Subscribe(topic)
	rm := &room{
		topic:        topic,
		participants: make(map[string]*record),
		watchers:     make(map[*Watch]struct{}),
		cancel:       cancel,
	}
	r.rooms[topic] = rm
	go r.forward(events)
	return rm
}

// dropWatcherLocked removes one watcher from a topic, tearing the room down
// with its last one. Called with r.mu held.
func (r *Registry) dropWatcherLocked(topic Topic, w *Watch) {
	rm, ok := r.rooms[topic]
	if !ok {
		return
	}
	delete(rm.watchers, w)
	if len(rm.watchers) > 0 {
		return
	}
	// Nothing local cares about this topic any more. Dropping the subscription
	// with the last watcher is what keeps a node's bus footprint proportional
	// to the rooms it is serving rather than to every room it has ever served.
	delete(r.rooms, topic)
	rm.cancel()
}

// forward drains one room's bus subscription into the Registry until the
// subscription is cancelled.
func (r *Registry) forward(events <-chan Event) {
	for ev := range events {
		r.apply(ev)
	}
}

// apply folds one event into the node's state and hands it to the local
// watchers that should see it. It is the single path a frame reaches a stream
// by, whichever node published it.
func (r *Registry) apply(ev Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rm, ok := r.rooms[ev.Topic]
	if !ok {
		return // the last watcher left between the publish and the delivery
	}
	switch ev.Kind {
	case KindMessage:
		// Broadcasts hold no state, so they pass straight through.
		rm.fanOutLocked(ev)
	case KindLeave:
		if _, held := rm.participants[ev.Participant.SessionID]; !held {
			return
		}
		rm.removeLocked(ev.Participant.SessionID)
	case KindState:
		rm.upsertLocked(ev, r.opts.MaxParticipants, r.opts.Now())
	}
}

// upsertLocked applies a state event under the last-writer-wins rule and fans
// it out when it changed something. Called with the Registry's lock held.
func (rm *room) upsertLocked(ev Event, maxParticipants int, now time.Time) {
	sid := ev.Participant.SessionID
	held, exists := rm.participants[sid]
	if exists {
		if ev.Participant.Revision < held.p.Revision {
			return // an older delivery of state we already have
		}
		// Liveness is refreshed even by a re-announcement that changes nothing,
		// which is exactly what a re-announcement is for.
		held.seenAt = now
		unchanged := ev.Participant.Revision == held.p.Revision
		held.p = ev.Participant
		if unchanged {
			return
		}
		rm.fanOutLocked(ev)
		return
	}
	if !rm.admitLocked(sid, maxParticipants) {
		return
	}
	rm.participants[sid] = &record{p: ev.Participant, seenAt: now}
	rm.fanOutLocked(ev)
}

// admitLocked decides whether a new session fits the room, evicting the highest
// session id when it does not and the newcomer sorts below it. Called with the
// Registry's lock held.
func (rm *room) admitLocked(sid string, maxParticipants int) bool {
	if len(rm.participants) < maxParticipants {
		return true
	}
	rm.truncated = true
	highest := ""
	for other := range rm.participants {
		if other > highest {
			highest = other
		}
	}
	if sid >= highest {
		return false
	}
	rm.removeLocked(highest)
	return true
}

// removeLocked drops a record and tells the local watchers it is gone. Called
// with the Registry's lock held.
func (rm *room) removeLocked(sid string) {
	rec, ok := rm.participants[sid]
	if !ok {
		return
	}
	delete(rm.participants, sid)
	rm.fanOutLocked(Event{
		Topic: rm.topic,
		Kind:  KindLeave,
		Participant: Participant{
			SessionID: sid,
			Subject:   rec.p.Subject,
		},
	})
}

// fanOutLocked queues an event on every local stream watching this topic.
// push never blocks, so holding the Registry's lock across the fan-out cannot
// stall a publisher behind a slow reader.
func (rm *room) fanOutLocked(ev Event) {
	for w := range rm.watchers {
		w.out.push(ev)
	}
}

// ownsLocked reports whether a local stream for this session is watching this
// topic, which is what makes this node responsible for re-announcing the
// session's record here. Membership in watchers IS the answer: a stream is
// added on Join and removed on Leave and Close.
func (rm *room) ownsLocked(sid string) bool {
	for w := range rm.watchers {
		if _, fronted := w.fronted[sid]; fronted {
			return true
		}
	}
	return false
}

// snapshotLocked renders the roster, ordered by session id so every watcher of
// the same room is handed the same order.
func (rm *room) snapshotLocked() []Participant {
	out := make([]Participant, 0, len(rm.participants))
	for _, rec := range rm.participants {
		out = append(out, rec.p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out
}
