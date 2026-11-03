package presencebridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/presence"
)

// OriginIssuer is the issuer a Bridge puts on every participant and broadcast
// sender it relays from the origin. Local presence records are owned by issuer
// and subject together, so a local principal with this issuer could write
// into the records of origin participants; an authenticator must not issue
// it.
const OriginIssuer = "syncplane.upstream"

// expiryFactor mirrors presence.Registry's expiry window: a record lapses after
// this many announce intervals without a re-announcement.
const expiryFactor = 3

// settle is how long a shard waits after a change to its topic set before it
// reopens its origin stream, so a burst of room changes costs one reopen.
const settle = 20 * time.Millisecond

// Default values of the options.
const (
	DefaultMaxTopicsPerStream = 64
	DefaultMaxPublishItems    = 32
	DefaultMinBackoff         = 250 * time.Millisecond
	DefaultMaxBackoff         = 30 * time.Second
)

// Option configures New.
type Option func(*config)

type config struct {
	bridged         func(workspaceID string) bool
	announce        time.Duration
	logger          *slog.Logger
	maxTopics       int
	maxPublishItems int
	minBackoff      time.Duration
	maxBackoff      time.Duration
}

// WithWorkspaces bridges the named workspaces and no others. It replaces an
// earlier WithWorkspaces or WithWorkspaceFilter. Without either, every
// workspace is bridged.
func WithWorkspaces(ids ...string) Option {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return WithWorkspaceFilter(func(id string) bool {
		_, ok := set[id]
		return ok
	})
}

// WithWorkspaceFilter bridges the workspaces for which fn returns true. It
// replaces an earlier WithWorkspaces or WithWorkspaceFilter.
//
// fn is called on every publish and every room subscription, from many
// goroutines, and must be fast and safe for concurrent use. A room that is
// already open on the local engine keeps the answer fn gave when the room
// opened, for its downward half; publishes follow the current answer.
func WithWorkspaceFilter(fn func(workspaceID string) bool) Option {
	return func(c *config) { c.bridged = fn }
}

// WithAnnounce sets how often the bridge re-announces origin participants
// locally. It must be at most the local engine's room announce interval
// (server.WithRoomAnnounce), or origin participants lapse locally while the
// origin stream is up. Zero or negative takes presence.DefaultAnnounce, which
// is also the engine's default.
func WithAnnounce(d time.Duration) Option {
	return func(c *config) { c.announce = d }
}

// WithLogger sets the logger for failed origin calls and lost origin streams.
// The default is slog.Default.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithOriginLimits sets the origin's room limits the bridge must stay within:
// the rooms one Watch stream may carry (the origin's MaxRoomsPerStream) and
// the items one Publish may carry (its MaxPublishItems). Zero or negative
// takes DefaultMaxTopicsPerStream and DefaultMaxPublishItems, which match the
// engine's defaults. The bridge opens one origin stream per
// maxTopicsPerStream watched rooms.
func WithOriginLimits(maxTopicsPerStream, maxPublishItems int) Option {
	return func(c *config) {
		c.maxTopics = maxTopicsPerStream
		c.maxPublishItems = maxPublishItems
	}
}

// WithReconnectBackoff bounds the wait before the bridge retries a failed
// origin call: it starts at minimum, doubles per consecutive failure up to
// maximum, and has jitter. Zero or negative values take DefaultMinBackoff and
// DefaultMaxBackoff.
func WithReconnectBackoff(minimum, maximum time.Duration) Option {
	return func(c *config) {
		c.minBackoff = minimum
		c.maxBackoff = maximum
	}
}

func settings(opts []Option) config {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.bridged == nil {
		cfg.bridged = func(string) bool { return true }
	}
	if cfg.announce <= 0 {
		cfg.announce = presence.DefaultAnnounce
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.maxTopics <= 0 {
		cfg.maxTopics = DefaultMaxTopicsPerStream
	}
	if cfg.maxPublishItems <= 0 {
		cfg.maxPublishItems = DefaultMaxPublishItems
	}
	if cfg.minBackoff <= 0 {
		cfg.minBackoff = DefaultMinBackoff
	}
	if cfg.maxBackoff <= 0 {
		cfg.maxBackoff = DefaultMaxBackoff
	}
	if cfg.maxBackoff < cfg.minBackoff {
		cfg.maxBackoff = cfg.minBackoff
	}
	return cfg
}

// Bridge is a presence.Bus that joins the rooms of bridged workspaces to the
// origin's rooms. See the package documentation for what crosses the hop.
//
// Publish and Subscribe never wait for the origin. A Bridge is safe for
// concurrent use.
type Bridge struct {
	inner  presence.Bus
	down   *presence.Local
	origin syncplanev1connect.PresenceServiceClient
	cfg    config
	// sessionID is the session the bridge's own origin streams watch as. It
	// never publishes, so it never appears in an origin roster.
	sessionID string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// closed mirrors the closed state for the paths that must not take mu.
	closed atomic.Bool
	// originAnnounce is the origin's announce interval in nanoseconds, as its
	// last Publish response reported it; 0 until one arrives.
	originAnnounce atomic.Int64

	mu     sync.Mutex
	rooms  map[presence.Topic]*originRoom
	shards map[*shard]struct{}

	locals localSessions
	up     upQueue
}

// New returns a Bridge over inner, the bus the local engine would use without
// it, that reaches the origin through origin. A nil inner takes
// presence.NewLocal(). origin carries the bridge's identity on the origin, so
// it must authenticate as a principal that may watch and publish the bridged
// rooms; client.Client.PresenceService returns one.
//
// New starts the bridge's goroutines and does not contact the origin. Close
// stops them. New panics if origin is nil.
func New(inner presence.Bus, origin syncplanev1connect.PresenceServiceClient, opts ...Option) *Bridge {
	if origin == nil {
		panic("presencebridge: New needs an origin PresenceService client")
	}
	if inner == nil {
		inner = presence.NewLocal()
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bridge{
		inner:     inner,
		down:      presence.NewLocal(),
		origin:    origin,
		cfg:       settings(opts),
		sessionID: "presencebridge-" + randomHex(12),
		ctx:       ctx,
		cancel:    cancel,
		rooms:     make(map[presence.Topic]*originRoom),
		shards:    make(map[*shard]struct{}),
	}
	b.locals.init()
	b.up.init()

	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.sendLoop() }()
	go func() { defer b.wg.Done(); b.announceLoop() }()
	return b
}

// Publish hands ev to the inner bus and, when ev's workspace is bridged and
// the inner bus accepted it, queues it for the origin. It returns the inner
// bus's error. It never blocks on the origin: when the origin queue is full,
// the event is not forwarded.
func (b *Bridge) Publish(ctx context.Context, ev presence.Event) error {
	if err := b.inner.Publish(ctx, ev); err != nil {
		return err
	}
	if b.closed.Load() || ev.Participant.Issuer == OriginIssuer || !b.cfg.bridged(ev.Topic.WorkspaceID) {
		return nil
	}
	// The origin knows every forwarded session as the bridge's own principal,
	// so it cannot tell two local owners apart. Holding each session to its
	// first local owner here is what stops one local principal from writing
	// into another's record on the origin.
	if !b.locals.claim(ev, b.localTTL()) {
		return nil
	}
	b.up.push(ev)
	return nil
}

// Subscribe returns the topic's events from the inner bus and, for a bridged
// workspace, the origin's participants and broadcasts. The first subscription
// to a bridged topic starts watching it on the origin; the cancel of the last
// one stops. Events from the origin reach only the subscribers of this Bridge,
// never the inner bus.
func (b *Bridge) Subscribe(topic presence.Topic) (<-chan presence.Event, func()) {
	if !b.cfg.bridged(topic.WorkspaceID) {
		return b.inner.Subscribe(topic)
	}
	in, cancelIn := b.inner.Subscribe(topic)
	down, cancelDown := b.down.Subscribe(topic)
	release := b.join(topic)

	out := make(chan presence.Event)
	stop := make(chan struct{})
	go b.merge(in, down, out, stop)

	var once sync.Once
	return out, func() {
		once.Do(func() {
			close(stop)
			cancelIn()
			cancelDown()
			release()
		})
	}
}

// Close stops watching and publishing to the origin, and withdraws every
// origin participant from the local rooms. It does not close the inner bus,
// and subscriptions keep delivering its events. After Close the Bridge
// forwards nothing and watches nothing. Close is idempotent and blocks until
// the bridge's goroutines have stopped; records already forwarded lapse on
// the origin after its expiry window.
func (b *Bridge) Close() error {
	b.mu.Lock()
	if b.closed.Load() {
		b.mu.Unlock()
		return nil
	}
	b.closed.Store(true)
	b.cancel()
	b.mu.Unlock()

	b.wg.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, rm := range b.rooms {
		rm.withdrawLocked(b)
	}
	return nil
}

// merge feeds one subscription from the inner bus and from the origin until
// the subscription is cancelled.
func (b *Bridge) merge(in, down <-chan presence.Event, out chan<- presence.Event, stop <-chan struct{}) {
	defer close(out)
	for in != nil || down != nil {
		var ev presence.Event
		var ok bool
		select {
		case <-stop:
			return
		case ev, ok = <-in:
			if !ok {
				in = nil
				continue
			}
			// A session seen on the inner bus is local, wherever on the local
			// deployment it was published, so the origin's echo of it must not
			// be relayed back.
			b.locals.note(ev)
		case ev, ok = <-down:
			if !ok {
				down = nil
				continue
			}
		}
		// Checked before the send as well as during it: a select with both
		// cases ready picks at random, and a cancelled subscription must not
		// deliver.
		select {
		case <-stop:
			return
		default:
		}
		select {
		case out <- ev:
		case <-stop:
			return
		}
	}
}

// localTTL is how long a local session stays known after its last event. It
// must outlast the origin's memory of the session's forwarded record, or an
// origin roster read in between would relay that record back as an origin
// participant.
func (b *Bridge) localTTL() time.Duration {
	interval := b.cfg.announce
	if o := time.Duration(b.originAnnounce.Load()); o > interval {
		interval = o
	}
	return (expiryFactor + 1) * interval
}

// announceLoop re-announces origin participants locally and ages out local
// sessions, once per announce interval.
func (b *Bridge) announceLoop() {
	t := time.NewTicker(b.cfg.announce)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case now := <-t.C:
			b.reannounce()
			b.locals.expire(now, b.localTTL())
		}
	}
}

// pause waits d, or until wake fires and the settle delay passes. A negative d
// waits for wake alone. It reports false when the bridge closed meanwhile.
func (b *Bridge) pause(d time.Duration, wake <-chan struct{}) bool {
	var timer <-chan time.Time
	if d >= 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-b.ctx.Done():
		return false
	case <-timer:
		return true
	case <-wake:
		t := time.NewTimer(settle)
		defer t.Stop()
		select {
		case <-b.ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
}

func randomHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
