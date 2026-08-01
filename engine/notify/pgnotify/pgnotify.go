// Package pgnotify implements the engine's change-notification fan-out over
// PostgreSQL LISTEN/NOTIFY, so pokes reach clients connected to any node rather
// than only the one that took the write.
//
// Install it with server.WithNotifier. It is independent of which storage
// adapter is in use — a Postgres notifier in front of a SQLite store is a
// perfectly good single-database-plus-many-readers deployment — and a
// deployment reaching for a second node usually already has a Postgres, which
// is what makes this cheaper than standing up NATS or Redis for the same job.
//
// # What it does and does not guarantee
//
// Delivery is BEST-EFFORT, and that is a property of the seam rather than a
// shortcut taken here: pokes cut latency, they do not carry correctness.
// Clients converge by polling regardless, so a dropped notification costs one
// poll interval. Three places lean on that licence — a full publish queue drops
// rather than blocks, a notification published while the listener is
// reconnecting is gone (Postgres buffers nothing across a dropped LISTEN), and
// a payload over the 8000-byte NOTIFY limit is skipped.
//
// # Shape
//
// Notify does two things: it fans out LOCALLY first, then queues a pg_notify.
// The local half is what keeps same-node latency at zero and off the database
// round trip; the remote half is what reaches the other nodes. The publishing
// node also receives its own notification back, which costs one redundant
// wakeup and nothing else — signals are content-free and coalesce, so no
// node-id de-duplication is needed.
//
// Receiving is one DEDICATED connection (never a pooled one — it is held for
// the process's lifetime) parked in WaitForNotification, feeding what it
// receives into a pubsub.Local per namespace. Local already implements the
// per-workspace fan-out, the coalescing send buffer and the Activity extension,
// so this package adds a transport and inherits the semantics.
//
// # Namespaces
//
// Every payload carries the app namespace alongside the workspace id, and each
// namespace gets its own pubsub.Local. server.Manager shares one notifier
// across every app it hosts, and workspace ids are namespaced per app — so
// without this, two apps that both have a workspace "team" would wake each
// other's clients. Derive a per-app view with Namespace and hand it to that
// app's server:
//
//	n, err := pgnotify.Open(ctx, dsn)
//	...
//	server.NewManager(ctx, root,
//		server.WithAppOptions(func(appID string) []server.Option {
//			return []server.Option{server.WithNotifier(n.Namespace(appID))}
//		}))
//
// # Composition
//
// pubsub.NewCoalescing wraps the SEND side, so the server's notify window caps
// pg_notify traffic exactly as it caps in-process traffic — the bus carries
// less, not just the goroutines.
package pgnotify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
)

// DefaultChannel is the LISTEN/NOTIFY channel used unless WithChannel says
// otherwise. One channel carries every namespace; the payload separates them.
const DefaultChannel = "syncplane_poke"

// maxPayload is Postgres's NOTIFY payload limit, less room for the JSON
// envelope. A workspace id long enough to hit this is pathological, and the
// alternative to skipping it is failing a write path over a poke.
const maxPayload = 7900

// publishQueue bounds what is waiting to reach the database. It holds at most
// one entry per (namespace, workspace) — see queue — so it is sized by active
// workspaces rather than by write rate.
const publishQueue = 4096

// channelName is what LISTEN will accept unquoted. The channel is interpolated
// into the statement (LISTEN takes no parameters), so it is validated rather
// than trusted.
var channelName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Notifier is a pubsub.Notifier backed by LISTEN/NOTIFY. The value returned by
// Open owns the connections; values from Namespace are views sharing them.
type Notifier struct {
	core  *core
	ns    string
	owner bool
}

// core is the state every namespace view shares: the connections, the listener
// and publisher goroutines, and one pubsub.Local per namespace.
type core struct {
	pool    *pgxpool.Pool
	dsn     string
	channel string
	logger  *slog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	locals map[string]*pubsub.Local
	// subs counts live per-workspace subscriptions, so a reconnect knows which
	// workspaces to re-poke. Reference-counted because several streams of one
	// workspace share a key and the last cancel is what retires it.
	subs   map[nsWorkspace]int
	closed bool

	q queue
}

type nsWorkspace struct{ ns, ws string }

// Option configures Open.
type Option func(*config)

type config struct {
	channel string
	logger  *slog.Logger
}

// WithChannel sets the LISTEN/NOTIFY channel. Give separate deployments that
// share one database separate channels, or each will wake the other's clients
// for workspaces they do not have. Must match [a-z_][a-z0-9_]*.
func WithChannel(name string) Option {
	return func(c *config) { c.channel = name }
}

// WithLogger installs the logger used for the transport's own trouble —
// reconnects, dropped notifications. Defaults to slog.Default.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// Open connects to the Postgres named by dsn and starts listening.
//
// The listener connection is established before returning, so a bad DSN or an
// unreachable database fails here rather than degrading silently into a
// notifier that never delivers.
func Open(ctx context.Context, dsn string, opts ...Option) (*Notifier, error) {
	cfg := config{channel: DefaultChannel, logger: slog.Default()}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !channelName.MatchString(cfg.channel) {
		return nil, fmt.Errorf("pgnotify: channel %q must match %s", cfg.channel, channelName)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgnotify: connect: %w", err)
	}
	// Fail fast on an unusable database rather than discovering it on the first
	// write, when there is no caller left to report it to.
	listener, err := listen(ctx, dsn, cfg.channel)
	if err != nil {
		pool.Close()
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &core{
		pool:    pool,
		dsn:     dsn,
		channel: cfg.channel,
		logger:  cfg.logger,
		cancel:  cancel,
		locals:  make(map[string]*pubsub.Local),
		subs:    make(map[nsWorkspace]int),
	}
	c.q.init()

	c.wg.Add(2)
	go func() { defer c.wg.Done(); c.listenLoop(runCtx, listener) }()
	go func() { defer c.wg.Done(); c.publishLoop(runCtx) }()

	return &Notifier{core: c, owner: true}, nil
}

// listen opens a dedicated connection and subscribes it to the channel. It is
// deliberately NOT a pooled connection: the subscription lives as long as the
// process, and a pooled one would either be held out of the pool forever or
// returned and silently unsubscribed.
func listen(ctx context.Context, dsn, channel string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgnotify: connect listener: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("pgnotify: listen on %q: %w", channel, err)
	}
	return conn, nil
}

// Namespace returns a view of the notifier scoped to one app namespace, sharing
// the connections. Close on a derived view is a no-op — the Notifier returned
// by Open owns the transport and must outlive every view.
func (n *Notifier) Namespace(ns string) *Notifier {
	return &Notifier{core: n.core, ns: ns}
}

// Close stops the listener and publisher and releases the connections. It is
// idempotent, and a no-op on a Namespace-derived view.
func (n *Notifier) Close() error {
	if !n.owner {
		return nil
	}
	n.core.mu.Lock()
	if n.core.closed {
		n.core.mu.Unlock()
		return nil
	}
	n.core.closed = true
	n.core.mu.Unlock()

	n.core.cancel()
	n.core.q.wake()
	n.core.wg.Wait()
	n.core.pool.Close()
	return nil
}

func (n *Notifier) Notify(workspaceID string) {
	// Local first, and synchronously: same-node subscribers must not wait on a
	// database round trip to learn about a write their own node just took.
	n.core.localFor(n.ns).Notify(workspaceID)
	n.core.q.push(nsWorkspace{ns: n.ns, ws: workspaceID})
}

func (n *Notifier) Subscribe(workspaceID string) (<-chan struct{}, func()) {
	ch, cancel := n.core.localFor(n.ns).Subscribe(workspaceID)
	key := nsWorkspace{ns: n.ns, ws: workspaceID}
	n.core.track(key, 1)
	var once sync.Once
	return ch, func() {
		cancel()
		// The contract requires cancel to survive being called twice, and the
		// reference count must not go negative when it is.
		once.Do(func() { n.core.track(key, -1) })
	}
}

// SubscribeActivity reports every workspace of this namespace that sees a
// notify, local or remote. It is the same stream pubsub.Local serves, which is
// what makes workspace discovery work across nodes: a workspace first written
// on another node arrives here as a notification rather than waiting for a
// consumer's next sweep.
//
// Reconnects are the gap. A workspace written while the listener was down is
// not announced — nothing re-derives what was missed — so a consumer keeps its
// periodic sweep as the backstop the contract already asks for.
func (n *Notifier) SubscribeActivity() (<-chan string, func()) {
	return n.core.localFor(n.ns).SubscribeActivity()
}

// localFor returns the namespace's fan-out, creating it on first use.
func (c *core) localFor(ns string) *pubsub.Local {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locals[ns]
	if !ok {
		l = pubsub.NewLocal()
		c.locals[ns] = l
	}
	return l
}

// lookup returns the namespace's fan-out without creating one. Incoming
// notifications go through this so another node's namespaces — apps this
// process does not host — cannot grow the map.
func (c *core) lookup(ns string) *pubsub.Local {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.locals[ns]
}

func (c *core) track(key nsWorkspace, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.subs[key] + delta
	if n <= 0 {
		delete(c.subs, key)
		return
	}
	c.subs[key] = n
}

// subscribed snapshots the live subscriptions for a reconnect re-poke.
func (c *core) subscribed() []nsWorkspace {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]nsWorkspace, 0, len(c.subs))
	for k := range c.subs {
		out = append(out, k)
	}
	return out
}

// payload is the wire form of a notification. The namespace travels with the
// workspace id because one notifier serves every app in the process.
type payload struct {
	NS string `json:"ns"`
	WS string `json:"ws"`
}

// publishLoop drains the queue onto the database. Publishing happens here
// rather than in Notify so that Notify never blocks on the network — it is
// called from write paths, and the local fan-out has already happened by the
// time an entry lands here.
func (c *core) publishLoop(ctx context.Context) {
	for {
		key, ok := c.q.pop(ctx)
		if !ok {
			return
		}
		body, err := json.Marshal(payload{NS: key.ns, WS: key.ws})
		if err != nil {
			c.logger.Error("pgnotify: encode notification", "error", err)
			continue
		}
		if len(body) > maxPayload {
			c.logger.Warn("pgnotify: notification exceeds the NOTIFY payload limit; dropping",
				"workspace", key.ws, "bytes", len(body))
			continue
		}
		if _, err := c.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, c.channel, string(body)); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Best-effort: subscribers on other nodes fall back to polling, and
			// this node's own subscribers were served before the entry queued.
			c.logger.Warn("pgnotify: publish failed", "workspace", key.ws, "error", err)
		}
	}
}

// listenLoop delivers notifications until the notifier is closed, reconnecting
// whenever the listener connection fails.
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
				c.logger.Warn("pgnotify: listener reconnect failed", "error", err, "retry_in", backoff)
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
			// Postgres buffers nothing across a dropped LISTEN, so anything
			// published while this connection was down is simply gone. Poke
			// every workspace someone is watching rather than leave those
			// clients sitting until their next poll — the signals are
			// content-free, so a spurious one costs a wasted pull at worst.
			c.repoke()
		}

		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("pgnotify: listener connection lost", "error", err)
			continue
		}

		var p payload
		if err := json.Unmarshal([]byte(notification.Payload), &p); err != nil {
			c.logger.Warn("pgnotify: undecodable notification", "error", err)
			continue
		}
		if local := c.lookup(p.NS); local != nil {
			local.Notify(p.WS)
		}
	}
}

// repoke signals every workspace with a live subscription, after a reconnect
// has left a hole in the stream.
func (c *core) repoke() {
	for _, key := range c.subscribed() {
		if local := c.lookup(key.ns); local != nil {
			local.Notify(key.ws)
		}
	}
}

// queue is the publish backlog: a FIFO holding at most ONE entry per
// (namespace, workspace).
//
// The de-duplication is what bounds it. A notification says "look at this
// workspace" rather than carrying a change, so the second entry for a workspace
// that has not been published yet buys nothing — which means the backlog is
// sized by how many workspaces are active, not by how fast they are written.
// Mirrors the same reasoning in pubsub.Local's activity queue.
type queue struct {
	mu      sync.Mutex
	items   []nsWorkspace
	pending map[nsWorkspace]struct{}
	// signal carries "the queue is no longer empty", coalesced into one slot:
	// the queue itself is the state, this is only the poke.
	signal chan struct{}
}

func (q *queue) init() {
	q.pending = make(map[nsWorkspace]struct{})
	q.signal = make(chan struct{}, 1)
}

func (q *queue) push(key nsWorkspace) {
	q.mu.Lock()
	if _, waiting := q.pending[key]; waiting {
		q.mu.Unlock()
		return
	}
	if len(q.items) >= publishQueue {
		// The database is not keeping up. Dropping is the licensed failure:
		// local subscribers were already served, and remote ones poll.
		q.mu.Unlock()
		return
	}
	q.pending[key] = struct{}{}
	q.items = append(q.items, key)
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
func (q *queue) pop(ctx context.Context) (nsWorkspace, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			key := q.items[0]
			if len(q.items) == 1 {
				// Drop the backing array with the last entry rather than keep
				// re-slicing into it, so a burst does not pin its strings.
				q.items = nil
			} else {
				q.items = q.items[1:]
			}
			delete(q.pending, key)
			q.mu.Unlock()
			return key, true
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nsWorkspace{}, false
		case <-q.signal:
		}
	}
}

// Compile-time proof that a view satisfies both the notifier seam and the
// optional extension effects reaches through pubsub.SubscribeActivity.
var (
	_ pubsub.Notifier = (*Notifier)(nil)
	_ pubsub.Activity = (*Notifier)(nil)
)
