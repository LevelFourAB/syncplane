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
// Receiving is one DEDICATED connection (never a pooled one — it is held for
// the process's lifetime) parked in WaitForNotification, feeding what it
// receives into a presence.Local per namespace.
//
// # Namespaces
//
// Every payload carries the app namespace alongside the topic, and each
// namespace gets its own presence.Local. server.Manager shares one bus across
// every app it hosts, and workspace ids are namespaced per app — so without
// this, two apps that both have a workspace "team" would serve each other's
// rooms. Derive a per-app view with Namespace and hand it to that app's server:
//
//	b, err := pgpresence.Open(ctx, dsn)
//	...
//	server.NewManager(ctx, root,
//		server.WithAppOptions(func(appID string) []server.Option {
//			return []server.Option{server.WithPresenceBus(b.Namespace(appID))}
//		}))
package pgpresence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/presence"
)

// DefaultChannel is the LISTEN/NOTIFY channel used unless WithChannel says
// otherwise. One channel carries every namespace and every topic; the payload's
// routing prefix separates them.
//
// It is deliberately not pgnotify's channel. A node running only the notifier
// would otherwise receive presence payloads it cannot decode and log every one
// of them as a broken notification.
const DefaultChannel = "syncplane_presence"

// maxPayload is Postgres's NOTIFY payload limit, less room to be sure the
// routing prefix fits alongside the body.
const maxPayload = 7900

// publishQueue bounds what is waiting to reach the database. State folds per
// (namespace, topic, session), so it is sized by active PARTICIPANTS rather
// than by publish rate; only broadcasts accumulate.
const publishQueue = 4096

// fieldSep separates the routing prefix's fields. Every field it separates —
// the namespace, the workspace id, the room name, the anchor entity and key —
// is validated to exclude ASCII control characters, so it cannot appear inside
// one. See engine/internal/naming.
const fieldSep = "\x1f"

// channelName is what LISTEN will accept unquoted. The channel is interpolated
// into the statement (LISTEN takes no parameters), so it is validated rather
// than trusted.
var channelName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Bus is a presence.Bus backed by LISTEN/NOTIFY. The value returned by Open
// owns the connections; values from Namespace are views sharing them.
type Bus struct {
	core  *core
	ns    string
	owner bool
}

// core is the state every namespace view shares: the connections, the listener
// and publisher goroutines, and one presence.Local per namespace.
type core struct {
	pool    *pgxpool.Pool
	dsn     string
	channel string
	logger  *slog.Logger
	// nodeID identifies this process on the channel, so its own events can be
	// discarded on the way back in rather than delivered a second time.
	nodeID string

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	locals map[string]*presence.Local
	// subs counts live subscriptions per (namespace, topic), so the listener
	// can discard an event for a room this node serves nobody in before
	// decoding its body. Reference-counted because several streams of one room
	// share a key and the last cancel is what retires it.
	subs   map[nsTopic]int
	closed bool

	q queue
}

type nsTopic struct {
	ns    string
	topic presence.Topic
}

// Option configures Open.
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

// WithLogger installs the logger used for the transport's own trouble —
// reconnects, dropped events. Defaults to slog.Default.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// Open connects to the Postgres named by dsn and starts listening.
//
// The listener connection is established before returning, so a bad DSN or an
// unreachable database fails here rather than degrading silently into a bus
// that never delivers.
func Open(ctx context.Context, dsn string, opts ...Option) (*Bus, error) {
	cfg := config{channel: DefaultChannel, logger: slog.Default()}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !channelName.MatchString(cfg.channel) {
		return nil, fmt.Errorf("pgpresence: channel %q must match %s", cfg.channel, channelName)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgpresence: connect: %w", err)
	}
	// Fail fast on an unusable database rather than discovering it on the first
	// publish, when there is no caller left to report it to.
	listener, err := listen(ctx, dsn, cfg.channel)
	if err != nil {
		pool.Close()
		return nil, err
	}

	var node [8]byte
	if _, err := rand.Read(node[:]); err != nil {
		_ = listener.Close(ctx)
		pool.Close()
		return nil, fmt.Errorf("pgpresence: node id: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &core{
		pool:    pool,
		dsn:     dsn,
		channel: cfg.channel,
		logger:  cfg.logger,
		nodeID:  hex.EncodeToString(node[:]),
		cancel:  cancel,
		locals:  make(map[string]*presence.Local),
		subs:    make(map[nsTopic]int),
	}
	c.q.init()

	c.wg.Add(2)
	go func() { defer c.wg.Done(); c.listenLoop(runCtx, listener) }()
	go func() { defer c.wg.Done(); c.publishLoop(runCtx) }()

	return &Bus{core: c, owner: true}, nil
}

// listen opens a dedicated connection and subscribes it to the channel. It is
// deliberately NOT a pooled connection: the subscription lives as long as the
// process, and a pooled one would either be held out of the pool forever or
// returned and silently unsubscribed.
func listen(ctx context.Context, dsn, channel string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgpresence: connect listener: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("pgpresence: listen on %q: %w", channel, err)
	}
	return conn, nil
}

// Namespace returns a view of the bus scoped to one app namespace, sharing the
// connections. Close on a derived view is a no-op — the Bus returned by Open
// owns the transport and must outlive every view.
//
// The namespace travels in the routing prefix, so it must not contain
// U+001F; one that does is refused at Publish rather than here.
func (b *Bus) Namespace(ns string) *Bus {
	return &Bus{core: b.core, ns: ns}
}

// Close stops the listener and publisher and releases the connections. It is
// idempotent, and a no-op on a Namespace-derived view.
func (b *Bus) Close() error {
	if !b.owner {
		return nil
	}
	b.core.mu.Lock()
	if b.core.closed {
		b.core.mu.Unlock()
		return nil
	}
	b.core.closed = true
	b.core.mu.Unlock()

	b.core.cancel()
	b.core.q.wake()
	b.core.wg.Wait()
	b.core.pool.Close()
	return nil
}

// Publish fans the event out to this node's subscribers and queues it for the
// others.
//
// It returns an error only when the event cannot be carried at all — a
// namespace that would corrupt the routing prefix, or a payload past what a
// NOTIFY can hold — and then it is delivered nowhere, so a publisher never has
// to reason about a partial send. Everything else is best-effort by contract.
func (b *Bus) Publish(_ context.Context, ev presence.Event) error {
	if strings.Contains(b.ns, fieldSep) {
		return fmt.Errorf("pgpresence: namespace %q contains the field separator \\u001f", b.ns)
	}
	// Sized before anything is delivered, so an event that cannot reach the
	// other nodes does not reach this one's subscribers either.
	body, err := encodeBody(ev)
	if err != nil {
		return err
	}
	line := b.core.nodeID + fieldSep + b.ns + fieldSep + routingPrefix(ev.Topic) + fieldSep + body
	if len(line) > maxPayload {
		return fmt.Errorf("pgpresence: event is %d bytes, over the %d the NOTIFY payload can carry",
			len(line), maxPayload)
	}

	// Local first, and synchronously: same-node watchers must not wait on a
	// database round trip to see a cursor move on their own node.
	b.core.localFor(b.ns).Publish(context.Background(), ev)
	b.core.q.push(b.ns, ev, line)
	return nil
}

// Subscribe returns this topic's event channel and its teardown.
func (b *Bus) Subscribe(topic presence.Topic) (<-chan presence.Event, func()) {
	ch, cancel := b.core.localFor(b.ns).Subscribe(topic)
	key := nsTopic{ns: b.ns, topic: topic}
	b.core.track(key, 1)
	var once sync.Once
	return ch, func() {
		cancel()
		// The contract requires cancel to survive being called twice, and the
		// reference count must not go negative when it is.
		once.Do(func() { b.core.track(key, -1) })
	}
}

// localFor returns the namespace's fan-out, creating it on first use.
func (c *core) localFor(ns string) *presence.Local {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locals[ns]
	if !ok {
		l = presence.NewLocal()
		c.locals[ns] = l
	}
	return l
}

// lookup returns the namespace's fan-out without creating one. Incoming events
// go through this so another node's namespaces — apps this process does not
// host — cannot grow the map.
func (c *core) lookup(ns string) *presence.Local {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.locals[ns]
}

func (c *core) track(key nsTopic, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.subs[key] + delta
	if n <= 0 {
		delete(c.subs, key)
		return
	}
	c.subs[key] = n
}

// serves reports whether anything on this node subscribes to the topic. The
// listener asks before decoding a body, which is what keeps a node's cost
// proportional to the rooms it serves rather than to every room in the
// deployment.
func (c *core) serves(key nsTopic) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs[key] > 0
}

// body is the wire form of an event, less the routing prefix that carries its
// address. Field names are short because every one of them is repeated in every
// cursor movement, and the payload budget is shared with the app's own state.
// []byte marshals to base64, which is what the NOTIFY text payload requires.
type body struct {
	Kind     uint8  `json:"k"`
	Session  string `json:"sid"`
	Subject  string `json:"sub,omitempty"`
	Revision uint64 `json:"rev,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
	State    []byte `json:"st,omitempty"`
	Payload  []byte `json:"pl,omitempty"`
}

func encodeBody(ev presence.Event) (string, error) {
	raw, err := json.Marshal(body{
		Kind:     uint8(ev.Kind),
		Session:  ev.Participant.SessionID,
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

// decode splits one notification back into the node that sent it, its
// namespace, its topic and its event.
func decode(payload string) (node, ns string, ev presence.Event, err error) {
	parts := strings.SplitN(payload, fieldSep, 7)
	if len(parts) != 7 {
		return "", "", presence.Event{}, fmt.Errorf("payload has %d fields, want 7", len(parts))
	}
	var b body
	if err := json.Unmarshal([]byte(parts[6]), &b); err != nil {
		return "", "", presence.Event{}, err
	}
	return parts[0], parts[1], presence.Event{
		Topic: presence.Topic{
			WorkspaceID: parts[2],
			Name:        parts[3],
			Entity:      parts[4],
			Key:         parts[5],
		},
		Kind: presence.Kind(b.Kind),
		Participant: presence.Participant{
			SessionID: b.Session,
			Subject:   b.Subject,
			State:     b.State,
			Revision:  b.Revision,
			Seq:       b.Seq,
		},
		Payload: b.Payload,
	}, nil
}

// publishLoop drains the queue onto the database. Publishing happens here
// rather than in Publish so that Publish never blocks on the network — the
// local fan-out has already happened by the time an entry lands here.
func (c *core) publishLoop(ctx context.Context) {
	for {
		item, ok := c.q.pop(ctx)
		if !ok {
			return
		}
		if _, err := c.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, c.channel, item.line); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Best-effort: this node's own subscribers were served before the
			// entry queued, and a lost presence record is re-announced within
			// one announce interval.
			c.logger.Warn("pgpresence: publish failed", "topic", item.topic.String(), "error", err)
		}
	}
}

// listenLoop delivers events until the bus is closed, reconnecting whenever the
// listener connection fails.
//
// There is no re-poke on reconnect, and there cannot be one: a poke is
// content-free and can be re-sent from nothing, while a presence event WAS the
// state. What fills the gap instead is the announce loop each node runs for the
// sessions whose streams it holds — so a reconnect costs the other nodes a
// stale roster for at most one announce interval, and costs any broadcast sent
// during the gap entirely.
func (c *core) listenLoop(ctx context.Context, conn *pgx.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
		}
	}()
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		if conn == nil {
			var err error
			conn, err = listen(ctx, c.dsn, c.channel)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Warn("pgpresence: listener reconnect failed", "error", err, "retry_in", backoff)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff *= 2; backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			backoff = 100 * time.Millisecond
		}

		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("pgpresence: listener connection lost", "error", err)
			continue
		}
		c.deliver(notification.Payload)
	}
}

// deliver routes one received notification, dropping it as early as the payload
// allows.
func (c *core) deliver(payload string) {
	// The node id is the first field, so this node's own events are discarded
	// before anything is decoded. Every event passes through here twice on a
	// single-node deployment, which is the common case.
	if node, _, found := strings.Cut(payload, fieldSep); found && node == c.nodeID {
		return
	}
	node, ns, ev, err := decode(payload)
	if err != nil {
		c.logger.Warn("pgpresence: undecodable event", "error", err)
		return
	}
	if node == c.nodeID {
		return
	}
	if !c.serves(nsTopic{ns: ns, topic: ev.Topic}) {
		return
	}
	if local := c.lookup(ns); local != nil {
		_ = local.Publish(context.Background(), ev)
	}
}

// queued is one entry waiting for the database: what it takes to coalesce and
// log it, plus the line already rendered so the publish loop only does I/O.
type queued struct {
	ns      string
	topic   presence.Topic
	session string
	kind    presence.Kind
	line    string
}

// pendingKey identifies the queued state of one session in one room.
type pendingKey struct {
	ns      string
	topic   presence.Topic
	session string
}

// queue is the publish backlog: a FIFO in which STATE folds per (namespace,
// topic, session) and nothing else does.
//
// Folding is what bounds it. State is last-writer-wins, so a second update from
// a session whose first has not been published yet buys nothing — which means
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

func (q *queue) push(ns string, ev presence.Event, line string) {
	key := pendingKey{ns: ns, topic: ev.Topic, session: ev.Participant.SessionID}
	item := &queued{
		ns:      ns,
		topic:   ev.Topic,
		session: ev.Participant.SessionID,
		kind:    ev.Kind,
		line:    line,
	}

	q.mu.Lock()
	switch ev.Kind {
	case presence.KindState:
		if waiting, ok := q.pending[key]; ok {
			*waiting = *item
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
			key := pendingKey{ns: item.ns, topic: item.topic, session: item.session}
			if item.kind == presence.KindState && q.pending[key] == item {
				delete(q.pending, key)
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

// Compile-time proof that a view satisfies the seam the server plugs into.
var _ presence.Bus = (*Bus)(nil)
