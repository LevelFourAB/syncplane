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
// a payload over the NOTIFY size limit is skipped.
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
// The transport underneath is engine/pglisten: one connection parked on the
// channel, one issuing pg_notify, and the reconnect loop over both. What
// arrives feeds a pubsub.Local per namespace, which already implements the
// per-workspace fan-out, the coalescing send buffer and the Activity extension
// — so this package adds a wire format and inherits the semantics.
//
// Open dials its own transport. A deployment that also runs pgpresence passes
// both a shared one instead, halving the connections either would open alone:
//
//	link, err := pglisten.Open(ctx, dsn)
//	...
//	defer link.Close()
//	notifier, err := pgnotify.New(ctx, link)
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
	"sync"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/pglisten"
)

// DefaultChannel is the LISTEN/NOTIFY channel used unless WithChannel says
// otherwise. One channel carries every namespace; the payload separates them.
const DefaultChannel = "syncplane_poke"

// publishQueue bounds what is waiting to reach the database. It holds at most
// one entry per (namespace, workspace) — see queue — so it is sized by active
// workspaces rather than by write rate.
const publishQueue = 4096

// Notifier is a pubsub.Notifier backed by LISTEN/NOTIFY. The value returned by
// Open or New owns the publisher goroutine; values from Namespace are views
// sharing it.
type Notifier struct {
	core  *core
	ns    string
	owner bool
}

// core is the state every namespace view shares: the transport, the publisher
// goroutine, and one pubsub.Local per namespace.
type core struct {
	link    *pglisten.Conn
	channel string
	logger  *slog.Logger
	// ownsLink records that Open dialled the transport, so Close takes it down
	// with the notifier. A transport handed to New belongs to the caller.
	ownsLink bool

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

// Option configures Open and New.
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

// WithLogger installs the logger used for this package's own trouble —
// undecodable notifications, dropped ones, failed publishes. Defaults to
// slog.Default. Open passes it on to the transport it dials; a transport handed
// to New keeps whatever logger it was opened with.
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
// unreachable database fails here rather than degrading silently into a
// notifier that never delivers.
func Open(ctx context.Context, dsn string, opts ...Option) (*Notifier, error) {
	cfg := settings(opts)
	// Checked before dialling, so a misconfigured channel costs no connection.
	if err := pglisten.ValidateChannel(cfg.channel); err != nil {
		return nil, fmt.Errorf("pgnotify: %w", err)
	}
	link, err := pglisten.Open(ctx, dsn, pglisten.WithLogger(cfg.logger))
	if err != nil {
		return nil, fmt.Errorf("pgnotify: %w", err)
	}
	n, err := start(ctx, link, cfg, true)
	if err != nil {
		_ = link.Close()
		return nil, err
	}
	return n, nil
}

// New builds a notifier on an existing transport, so a process also running
// pgpresence opens one pair of connections rather than two.
//
// New blocks until the channel is subscribed, which spans a reconnect if the
// database is down; ctx bounds that wait. link must outlive the notifier —
// Close stops publishing and delivery but leaves the transport running.
func New(ctx context.Context, link *pglisten.Conn, opts ...Option) (*Notifier, error) {
	cfg := settings(opts)
	if err := pglisten.ValidateChannel(cfg.channel); err != nil {
		return nil, fmt.Errorf("pgnotify: %w", err)
	}
	return start(ctx, link, cfg, false)
}

// start registers the channel and brings the publisher goroutine up.
func start(ctx context.Context, link *pglisten.Conn, cfg config, ownsLink bool) (*Notifier, error) {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &core{
		link:     link,
		channel:  cfg.channel,
		logger:   cfg.logger,
		ownsLink: ownsLink,
		cancel:   cancel,
		locals:   make(map[string]*pubsub.Local),
		subs:     make(map[nsWorkspace]int),
	}
	c.q.init()

	if err := link.Listen(ctx, cfg.channel, listener{core: c}); err != nil {
		cancel()
		return nil, fmt.Errorf("pgnotify: %w", err)
	}

	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.publishLoop(runCtx) }()

	return &Notifier{core: c, owner: true}, nil
}

// listener routes the transport's callbacks into the notifier.
type listener struct{ core *core }

func (l listener) Notification(payload string) { l.core.deliver(payload) }

// Reconnected re-pokes every workspace someone is watching. Nothing replays
// what the gap swallowed, and a poke is content-free — so re-sending one costs
// a wasted pull at worst, where leaving it out costs those clients a poll
// interval.
func (l listener) Reconnected() { l.core.repoke() }

// Namespace returns a view of the notifier scoped to one app namespace, sharing
// the transport. Close on a derived view is a no-op — the value returned by
// Open or New owns the fan-out and must outlive every view.
func (n *Notifier) Namespace(ns string) *Notifier {
	return &Notifier{core: n.core, ns: ns}
}

// Close stops publishing and delivery. It is idempotent, and a no-op on a
// Namespace-derived view. A notifier from Open closes its transport; one from
// New leaves the caller's alone.
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
	if n.core.ownsLink {
		return n.core.link.Close()
	}
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

func (n *Notifier) SubscribeSet(workspaceIDs []string) pubsub.Set {
	set := n.core.localFor(n.ns).SubscribeSet(workspaceIDs)
	keys := make([]nsWorkspace, len(workspaceIDs))
	for i, ws := range workspaceIDs {
		keys[i] = nsWorkspace{ns: n.ns, ws: ws}
		n.core.track(keys[i], 1)
	}
	return &trackedSet{Set: set, core: n.core, keys: keys}
}

// trackedSet holds each member's reconnect re-poke registration for the life
// of the subscription. Close must survive being called twice, and the
// reference counts must not go negative when it is.
type trackedSet struct {
	pubsub.Set
	core *core
	keys []nsWorkspace
	once sync.Once
}

func (s *trackedSet) Close() {
	s.Set.Close()
	s.once.Do(func() {
		for _, key := range s.keys {
			s.core.track(key, -1)
		}
	})
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
// process does not host — cannot grow the map, and so a notifier closed while
// its transport keeps running stops delivering.
func (c *core) lookup(ns string) *pubsub.Local {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
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
	// More carries the rest of a batched publish — see nextBatch. A decoder
	// from before this field existed ignores it and still delivers the head
	// entry, so a mixed-version deployment degrades to slower pokes, not
	// lost ones.
	More []payload `json:"more,omitempty"`
}

// deliver routes one received notification to its namespace's fan-out.
func (c *core) deliver(raw string) {
	var p payload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		c.logger.Warn("pgnotify: undecodable notification", "error", err)
		return
	}
	c.notifyLocal(p.NS, p.WS)
	for _, m := range p.More {
		c.notifyLocal(m.NS, m.WS)
	}
}

func (c *core) notifyLocal(ns, ws string) {
	if local := c.lookup(ns); local != nil {
		local.Notify(ws)
	}
}

// publishLoop drains the queue onto the database. Publishing happens here
// rather than in Notify so that Notify never blocks on the network — it is
// called from write paths, and the local fan-out has already happened by the
// time an entry lands here.
func (c *core) publishLoop(ctx context.Context) {
	for {
		body, ok := c.nextBatch(ctx)
		if !ok {
			return
		}
		if body == "" {
			continue
		}
		if err := c.link.Publish(ctx, c.channel, body); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Best-effort: subscribers on other nodes fall back to polling, and
			// this node's own subscribers were served before the entry queued.
			c.logger.Warn("pgnotify: publish failed", "error", err)
		}
	}
}

// nextBatch blocks for the next queued key, then drains as many more as one
// NOTIFY can carry, encoded as the head payload with the rest in More.
//
// Batching is what lifts the publish ceiling. One entry per publish costs one
// database round trip per key, capping a node at one notification per RTT with
// the queue absorbing — and eventually dropping — the overshoot. Filling each
// round trip up to MaxPayload lifts that ceiling by the batch factor, and
// divides by the same factor the async-queue entries every listening backend
// in the deployment must scan. A quiet deployment batches one: the loop takes
// only what is already queued, never waits for more.
//
// It returns ok=false once ctx is done. An empty body with ok=true is a key
// whose id alone overflows a NOTIFY, dropped as the licensed failure.
func (c *core) nextBatch(ctx context.Context) (string, bool) {
	key, ok := c.q.pop(ctx)
	if !ok {
		return "", false
	}
	head := payload{NS: key.ns, WS: key.ws}
	body, err := json.Marshal(head)
	if err != nil {
		c.logger.Error("pgnotify: encode notification", "error", err)
		return "", true
	}
	if len(body) > pglisten.MaxPayload {
		// A workspace id long enough to hit this is pathological, and the
		// alternative to skipping it is failing a write path over a poke.
		c.logger.Warn("pgnotify: notification exceeds the NOTIFY payload limit; dropping",
			"workspace", key.ws, "bytes", len(body))
		return "", true
	}
	// size tracks the encoded batch exactly without re-marshaling it per
	// entry: appending entry n costs its own bytes plus a separating comma,
	// and the first appended entry also opens the `,"more":[` … `]` framing
	// (9 bytes, with its closing bracket paid for by the comma the entry
	// does not need).
	size := len(body)
	for {
		var entry []byte
		next, ok := c.q.popIf(func(k nsWorkspace) bool {
			var err error
			entry, err = json.Marshal(payload{NS: k.ns, WS: k.ws})
			if err != nil {
				return false
			}
			delta := len(entry) + 1
			if len(head.More) == 0 {
				delta += 9
			}
			return size+delta <= pglisten.MaxPayload
		})
		if !ok {
			break
		}
		size += len(entry) + 1
		if len(head.More) == 0 {
			size += 9
		}
		head.More = append(head.More, payload{NS: next.ns, WS: next.ws})
	}
	if len(head.More) == 0 {
		return string(body), true
	}
	batched, err := json.Marshal(head)
	if err != nil {
		c.logger.Error("pgnotify: encode notification", "error", err)
		return "", true
	}
	return string(batched), true
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

// popIf pops the head entry if accept approves it, without blocking. It
// returns ok=false when the queue is empty or the head was refused; a refused
// head stays queued and leads the next batch.
func (q *queue) popIf(accept func(nsWorkspace) bool) (nsWorkspace, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nsWorkspace{}, false
	}
	key := q.items[0]
	if !accept(key) {
		return nsWorkspace{}, false
	}
	if len(q.items) == 1 {
		q.items = nil
	} else {
		q.items = q.items[1:]
	}
	delete(q.pending, key)
	return key, true
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

// Compile-time proof that a view satisfies the notifier seam, the optional
// extension effects reaches through pubsub.SubscribeActivity, and the
// transport's callback contract.
var (
	_ pubsub.Notifier   = (*Notifier)(nil)
	_ pubsub.Activity   = (*Notifier)(nil)
	_ pglisten.Listener = listener{}
)
