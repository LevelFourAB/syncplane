// Package pgpresence implements the ephemeral plane's fan-out over PostgreSQL
// LISTEN/NOTIFY, so rooms reach clients connected to any node rather than only
// the one holding a given session's stream.
//
// Install it with server.WithPresenceBus. It is independent of which storage
// adapter is in use, and a deployment reaching for a second node usually
// already has a Postgres — which is what makes this cheaper than standing up
// NATS or Redis for the same job.
//
// # What it does and does not guarantee
//
// Delivery is at most once, which is the seam's contract rather than a shortcut
// taken here — see engine/presence. Three places lean on that licence: a full
// publish queue drops rather than blocks, an event published while the listener
// is reconnecting is gone (Postgres buffers nothing across a dropped LISTEN),
// and a subscriber that is not reading loses what overflows its queue.
//
// Presence heals from all three within one announce interval, because a record
// is re-announced by the node holding its session's stream rather than replayed
// from anywhere. Broadcast messages do not heal, and an app that cannot lose one
// writes a row instead.
//
// The one failure that is NOT silent is an oversize payload. Postgres caps a
// NOTIFY payload at 8000 bytes and the state travels base64-encoded inside it,
// so an event that will not fit is refused at Publish — the publisher learns its
// state is too large instead of watching it vanish on every node but its own.
// The engine caps state and message size at the RPC boundary
// (Limits.MaxPresenceStateBytes, Limits.MaxBroadcastBytes) well under this, so
// reaching it means those limits were raised past what the transport can carry.
//
// # Shape
//
// Publish does two things: it fans out LOCALLY first, then queues a pg_notify.
// The local half keeps same-node latency off the database round trip; the
// remote half reaches the other nodes. Every payload carries the publishing
// node's id and a node discards its own events on the way back in, because —
// unlike a content-free poke — delivering a broadcast twice would show it twice.
//
// The transport underneath is engine/pglisten: one connection parked on the
// channel, one issuing pg_notify, and the reconnect loop over both. What
// arrives feeds a presence.Local.
//
// Open dials its own transport. A deployment that also runs pgnotify passes
// both a shared one instead, halving the connections either would open alone:
//
//	link, err := pglisten.Open(ctx, dsn)
//	...
//	defer link.Close()
//	bus, err := pgpresence.New(ctx, link)
//
// # Hosting several applications
//
// One bus serves every app a server.Manager hosts. Each app's room registry
// prefixes the workspace id of every topic it puts on the bus with its own
// share of the id space, so two apps that both have a workspace "team" do not
// serve each other's rooms and this package needs no notion of an app:
//
//	b, err := pgpresence.Open(ctx, dsn)
//	...
//	server.NewManager(ctx, root, server.WithServerOptions(server.WithPresenceBus(b)))
package pgpresence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/levelfourab/syncplane/engine/pglisten"
	"github.com/levelfourab/syncplane/engine/presence"
)

// DefaultChannel is the LISTEN/NOTIFY channel used unless WithChannel says
// otherwise. One channel carries every topic of every app on it; the payload's
// routing prefix separates them.
//
// It differs from pgnotify's channel so the two can share one transport: a
// pglisten.Conn delivers a payload only to the listener registered for its
// channel, so neither package ever sees the other's traffic.
const DefaultChannel = "syncplane_presence"

// publishQueue bounds what is waiting to reach the database. State folds per
// (topic, session, subject), so it is sized by active PARTICIPANTS rather than
// by publish rate; only broadcasts accumulate.
const publishQueue = 4096

// fieldSep separates the routing prefix's fields. Every field it separates —
// the workspace id, the room name, the anchor entity and key — is validated to
// exclude ASCII control characters, so it cannot appear inside one, and an app
// prefix on the workspace id is built from the same alphabet. See
// engine/internal/naming.
const fieldSep = "\x1f"

// Bus is a presence.Bus backed by LISTEN/NOTIFY. It holds the transport, the
// publisher goroutine and the node-local fan-out.
type Bus struct {
	link    *pglisten.Conn
	channel string
	logger  *slog.Logger
	// ownsLink records that Open dialled the transport, so Close takes it down
	// with the bus. A transport handed to New belongs to the caller.
	ownsLink bool
	// nodeID identifies this process on the channel, so its own events can be
	// discarded on the way back in rather than delivered a second time.
	nodeID string

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu    sync.Mutex
	local *presence.Local
	// subs counts live subscriptions per topic, so the listener can discard an
	// event for a room this node serves nobody in before decoding its body.
	// Reference-counted because several streams of one room share a topic and
	// the last cancel is what retires it.
	subs   map[presence.Topic]int
	closed bool

	q queue
}

// Option configures Open and New.
type Option func(*config)

type config struct {
	channel string
	logger  *slog.Logger
}

// WithChannel sets the LISTEN/NOTIFY channel. Give separate deployments that
// share one database separate channels, or each will serve the other's rooms.
// Must match [a-z_][a-z0-9_]*.
func WithChannel(name string) Option {
	return func(c *config) { c.channel = name }
}

// WithLogger installs the logger used for this package's own trouble —
// undecodable events, failed publishes. Defaults to slog.Default. Open passes
// it on to the transport it dials; a transport handed to New keeps whatever
// logger it was opened with.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

func settings(opts []Option) config {
	cfg := config{channel: DefaultChannel, logger: slog.Default()}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Open connects to the Postgres named by dsn and starts listening, on a
// transport of its own that Close takes down with it.
//
// The listener connection is established before returning, so a bad DSN or an
// unreachable database fails here rather than degrading silently into a bus
// that never delivers.
func Open(ctx context.Context, dsn string, opts ...Option) (*Bus, error) {
	cfg := settings(opts)
	// Checked before dialling, so a misconfigured channel costs no connection.
	if err := pglisten.ValidateChannel(cfg.channel); err != nil {
		return nil, fmt.Errorf("pgpresence: %w", err)
	}
	link, err := pglisten.Open(ctx, dsn, pglisten.WithLogger(cfg.logger))
	if err != nil {
		return nil, fmt.Errorf("pgpresence: %w", err)
	}
	b, err := start(ctx, link, cfg, true)
	if err != nil {
		_ = link.Close()
		return nil, err
	}
	return b, nil
}

// New builds a bus on an existing transport, so a process also running pgnotify
// opens one pair of connections rather than two.
//
// New blocks until the channel is subscribed, which spans a reconnect if the
// database is down; ctx bounds that wait. link must outlive the bus — Close
// stops publishing and delivery but leaves the transport running.
func New(ctx context.Context, link *pglisten.Conn, opts ...Option) (*Bus, error) {
	cfg := settings(opts)
	if err := pglisten.ValidateChannel(cfg.channel); err != nil {
		return nil, fmt.Errorf("pgpresence: %w", err)
	}
	return start(ctx, link, cfg, false)
}

// start mints the node id, registers the channel and brings the publisher
// goroutine up.
func start(ctx context.Context, link *pglisten.Conn, cfg config, ownsLink bool) (*Bus, error) {
	var node [8]byte
	if _, err := rand.Read(node[:]); err != nil {
		return nil, fmt.Errorf("pgpresence: node id: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	b := &Bus{
		link:     link,
		channel:  cfg.channel,
		logger:   cfg.logger,
		ownsLink: ownsLink,
		nodeID:   hex.EncodeToString(node[:]),
		cancel:   cancel,
		local:    presence.NewLocal(),
		subs:     make(map[presence.Topic]int),
	}
	b.q.init()

	if err := link.Listen(ctx, cfg.channel, listener{b: b}); err != nil {
		cancel()
		return nil, fmt.Errorf("pgpresence: %w", err)
	}

	b.wg.Add(1)
	go func() { defer b.wg.Done(); b.publishLoop(runCtx) }()

	return b, nil
}

// listener routes the transport's callbacks into the bus.
type listener struct{ b *Bus }

func (l listener) Notification(payload string) { l.b.deliver(payload) }

// Reconnected does nothing, and there is nothing it could do: a poke is
// content-free and can be re-sent from nothing, while a presence event WAS the
// state. What fills the gap instead is the announce loop each node runs for the
// sessions whose streams it holds — so a reconnect costs the other nodes a
// stale roster for at most one announce interval, and costs any broadcast sent
// during the gap entirely.
func (l listener) Reconnected() {}

// Close stops publishing and delivery. It is idempotent. A bus from Open
// closes its transport; one from New leaves the caller's alone.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	b.cancel()
	b.q.wake()
	b.wg.Wait()
	if b.ownsLink {
		return b.link.Close()
	}
	return nil
}

// Publish fans the event out to this node's subscribers and queues it for the
// others.
//
// It returns an error only when the event cannot be carried at all — a payload
// past what a NOTIFY can hold — and then it is delivered nowhere, so a
// publisher never has to reason about a partial send. Everything else is
// best-effort by contract.
func (b *Bus) Publish(_ context.Context, ev presence.Event) error {
	// Sized before anything is delivered, so an event that cannot reach the
	// other nodes does not reach this one's subscribers either.
	body, err := encodeBody(ev)
	if err != nil {
		return err
	}
	line := b.nodeID + fieldSep + routingPrefix(ev.Topic) + fieldSep + body
	if len(line) > pglisten.MaxPayload {
		return fmt.Errorf("pgpresence: event is %d bytes, over the %d the NOTIFY payload can carry",
			len(line), pglisten.MaxPayload)
	}

	// Local first, and synchronously: same-node watchers must not wait on a
	// database round trip to see a cursor move on their own node.
	b.local.Publish(context.Background(), ev)
	b.q.push(ev, line)
	return nil
}

// Subscribe returns this topic's event channel and its teardown.
func (b *Bus) Subscribe(topic presence.Topic) (<-chan presence.Event, func()) {
	ch, cancel := b.local.Subscribe(topic)
	b.track(topic, 1)
	var once sync.Once
	return ch, func() {
		cancel()
		// The contract requires cancel to survive being called twice, and the
		// reference count must not go negative when it is.
		once.Do(func() { b.track(topic, -1) })
	}
}

// fanout returns the node-local fan-out, or nil once the bus is closed.
// Incoming events go through it so that a bus closed while its transport keeps
// running stops delivering.
func (b *Bus) fanout() *presence.Local {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	return b.local
}

func (b *Bus) track(key presence.Topic, delta int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := b.subs[key] + delta
	if count <= 0 {
		delete(b.subs, key)
		return
	}
	b.subs[key] = count
}

// serves reports whether anything on this node subscribes to the topic. The
// listener asks before decoding a body, which is what keeps a node's cost
// proportional to the rooms it serves rather than to every room in the
// deployment.
func (b *Bus) serves(topic presence.Topic) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subs[topic] > 0
}

// body is the wire form of an event, less the routing prefix that carries its
// address. Field names are short because every one of them is repeated in every
// cursor movement, and the payload budget is shared with the app's own state.
// []byte marshals to base64, which is what the NOTIFY text payload requires.
//
// Issuer is written on every event, the empty issuer included, so its absence
// marks an event from a node that predates it (see decode).
type body struct {
	Kind     uint8   `json:"k"`
	Session  string  `json:"sid"`
	Issuer   *string `json:"iss"`
	Subject  string  `json:"sub,omitempty"`
	Revision uint64  `json:"rev,omitempty"`
	Seq      uint64  `json:"seq,omitempty"`
	State    []byte  `json:"st,omitempty"`
	Payload  []byte  `json:"pl,omitempty"`
}

func encodeBody(ev presence.Event) (string, error) {
	issuer := ev.Participant.Issuer
	raw, err := json.Marshal(body{
		Kind:     uint8(ev.Kind),
		Session:  ev.Participant.SessionID,
		Issuer:   &issuer,
		Subject:  ev.Participant.Subject,
		Revision: ev.Participant.Revision,
		Seq:      ev.Participant.Seq,
		State:    ev.Participant.State,
		Payload:  ev.Payload,
	})
	if err != nil {
		return "", fmt.Errorf("pgpresence: encode event: %w", err)
	}
	return string(raw), nil
}

// routingPrefix renders a topic as the four separator-delimited fields the
// listener rebuilds it from.
func routingPrefix(t presence.Topic) string {
	return t.WorkspaceID + fieldSep + t.Name + fieldSep + t.Entity + fieldSep + t.Key
}

// decode splits one notification back into the node that sent it, its topic
// and its event. ok is false for an event to drop: a state or a leave from a
// node that sends no issuer.
//
// Such a node predates the issuer, so its event names a record of an unknown
// issuer. Read as the empty issuer, it could change or withdraw the record of
// a principal with no issuer and the same subject. Dropping it costs a rolling
// deploy nothing durable: the old node's sessions are missing from the rosters
// of the new nodes until the deploy ends and their streams reconnect. A
// broadcast names no record, so it is delivered.
func decode(payload string) (node string, ev presence.Event, ok bool, err error) {
	parts := strings.SplitN(payload, fieldSep, 6)
	if len(parts) != 6 {
		return "", presence.Event{}, false, fmt.Errorf("payload has %d fields, want 6", len(parts))
	}
	var b body
	if err := json.Unmarshal([]byte(parts[5]), &b); err != nil {
		return "", presence.Event{}, false, err
	}
	kind := presence.Kind(b.Kind)
	if b.Issuer == nil && kind != presence.KindMessage {
		return parts[0], presence.Event{}, false, nil
	}
	var issuer string
	if b.Issuer != nil {
		issuer = *b.Issuer
	}
	return parts[0], presence.Event{
		Topic: presence.Topic{
			WorkspaceID: parts[1],
			Name:        parts[2],
			Entity:      parts[3],
			Key:         parts[4],
		},
		Kind: kind,
		Participant: presence.Participant{
			SessionID: b.Session,
			Issuer:    issuer,
			Subject:   b.Subject,
			State:     b.State,
			Revision:  b.Revision,
			Seq:       b.Seq,
		},
		Payload: b.Payload,
	}, true, nil
}

// publishLoop drains the queue onto the database. Publishing happens here
// rather than in Publish so that Publish never blocks on the network — the
// local fan-out has already happened by the time an entry lands here.
func (b *Bus) publishLoop(ctx context.Context) {
	for {
		item, ok := b.q.pop(ctx)
		if !ok {
			return
		}
		if err := b.link.Publish(ctx, b.channel, item.line); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Best-effort: this node's own subscribers were served before the
			// entry queued, and a lost presence record is re-announced within
			// one announce interval.
			b.logger.Warn("pgpresence: publish failed", "topic", item.topic.String(), "error", err)
		}
	}
}

// deliver routes one received notification, dropping it as early as the payload
// allows.
func (b *Bus) deliver(payload string) {
	// The node id is the first field, so this node's own events are discarded
	// before anything is decoded. Every event passes through here twice on a
	// single-node deployment, which is the common case.
	if node, _, found := strings.Cut(payload, fieldSep); found && node == b.nodeID {
		return
	}
	node, ev, ok, err := decode(payload)
	if err != nil {
		b.logger.Warn("pgpresence: undecodable event", "error", err)
		return
	}
	if !ok || node == b.nodeID {
		return
	}
	if !b.serves(ev.Topic) {
		return
	}
	if local := b.fanout(); local != nil {
		_ = local.Publish(context.Background(), ev)
	}
}

// queued is one entry waiting for the database: what it takes to coalesce and
// log it, plus the line already rendered so the publish loop only does I/O.
type queued struct {
	topic    presence.Topic
	session  string
	issuer   string
	subject  string
	revision uint64
	kind     presence.Kind
	line     string
}

func (q *queued) key() pendingKey {
	return pendingKey{topic: q.topic, session: q.session, issuer: q.issuer, subject: q.subject}
}

// pendingKey identifies the queued state of one session in one room, as one
// owner, an issuer and a subject, wrote it. A receiver keeps a record for the
// owner that wrote it, so state from another owner is not a newer version of
// it and must not fold in.
type pendingKey struct {
	topic   presence.Topic
	session string
	issuer  string
	subject string
}

// queue is the publish backlog: a FIFO in which STATE folds per (topic,
// session, issuer, subject) and nothing else does. A fold keeps the higher revision.
//
// Folding is what bounds it. State is last-writer-wins by revision, so a second
// update from a session whose first has not been published yet buys nothing —
// which means
// the backlog is sized by how many participants are active, not by how fast
// they move their cursors, and a hundred users at 10 Hz cost a hundred
// pg_notify round trips per drain rather than a thousand. Broadcasts are each
// distinct and must never fold; they may only be dropped.
type queue struct {
	mu      sync.Mutex
	items   []*queued
	pending map[pendingKey]*queued
	dropped uint64
	// signal carries "the queue is no longer empty", coalesced into one slot:
	// the queue itself is the state, this is only the poke.
	signal chan struct{}
}

func (q *queue) init() {
	q.pending = make(map[pendingKey]*queued)
	q.signal = make(chan struct{}, 1)
}

func (q *queue) push(ev presence.Event, line string) {
	item := &queued{
		topic:    ev.Topic,
		session:  ev.Participant.SessionID,
		issuer:   ev.Participant.Issuer,
		subject:  ev.Participant.Subject,
		revision: ev.Participant.Revision,
		kind:     ev.Kind,
		line:     line,
	}
	key := item.key()

	q.mu.Lock()
	switch ev.Kind {
	case presence.KindState:
		if waiting, ok := q.pending[key]; ok {
			// Arrival order is not revision order (a Registry re-announces
			// after releasing its lock), and every receiver drops a lower
			// revision, so a lower one in the slot would lose the higher one.
			if item.revision >= waiting.revision {
				*waiting = *item
			}
			q.mu.Unlock()
			return
		}
	case presence.KindLeave:
		// A state published after this leave must not fold into a slot that
		// sits in front of it, or the record would be resurrected.
		delete(q.pending, key)
	}
	if len(q.items) >= publishQueue {
		// The database is not keeping up. Dropping is the licensed failure:
		// local subscribers were already served, and a lost record is
		// re-announced within one announce interval.
		q.dropped++
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, item)
	if ev.Kind == presence.KindState {
		q.pending[key] = item
	}
	q.mu.Unlock()
	q.wake()
}

func (q *queue) wake() {
	select {
	case q.signal <- struct{}{}:
	default: // a poke is already outstanding; the queue carries the rest
	}
}

// pop blocks until an entry is available, returning ok=false once ctx is done.
func (q *queue) pop(ctx context.Context) (queued, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			item := q.items[0]
			if len(q.items) == 1 {
				// Drop the backing array with the last entry rather than keep
				// re-slicing into it, so a burst does not pin its payloads.
				q.items = nil
			} else {
				q.items = q.items[1:]
			}
			if item.kind == presence.KindState && q.pending[item.key()] == item {
				delete(q.pending, item.key())
			}
			out := *item
			q.mu.Unlock()
			return out, true
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return queued{}, false
		case <-q.signal:
		}
	}
}

// Compile-time proof that a view satisfies the seam the server plugs into, and
// that the transport's callback contract is met.
var (
	_ presence.Bus      = (*Bus)(nil)
	_ pglisten.Listener = listener{}
)
