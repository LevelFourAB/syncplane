// Package pglisten is the PostgreSQL LISTEN/NOTIFY transport the engine's
// cross-node fan-outs are built on: one connection parked on the channels a
// process listens to, one issuing pg_notify, and the reconnect loop that keeps
// both alive.
//
// It carries strings and knows nothing about what they mean. What belongs in a
// payload, how a backlog is coalesced and what a lost notification costs are
// questions for the package above — engine/notify/pgnotify for pokes,
// engine/presence/pgpresence for the ephemeral plane.
//
// # Sharing one Conn
//
// Both of those open their own transport when handed a DSN. A deployment
// running both opens one Conn instead and hands it to each, which is the
// difference between two connections per node and four:
//
//	link, err := pglisten.Open(ctx, dsn)
//	if err != nil {
//		return err
//	}
//	defer link.Close()
//
//	notifier, err := pgnotify.New(ctx, link)
//	...
//	bus, err := pgpresence.New(ctx, link)
//
// Each registers its own channel and receives only that channel's traffic, so
// sharing the connection does not mix the two streams. Closing the Conn stops
// both; closing either of them leaves the Conn and the other one running.
//
// # Connections
//
// Two, and neither is pooled: a listener held for as long as the Conn is open,
// and a publisher dialled on the first Publish and redialled whenever it is
// found closed. A node's fan-out costs those two however many channels it
// registers.
//
// # Delivery
//
// Best-effort. Postgres buffers nothing across a dropped LISTEN, so everything
// published while the listener is reconnecting is lost, and
// Listener.Reconnected is where a caller learns a gap happened.
package pglisten

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// MaxPayload is the largest payload Publish carries, a margin under Postgres's
// 8000-byte NOTIFY limit. Publish refuses anything longer, so a caller that has
// to report the failure to whoever produced the payload can size it against
// this first.
const MaxPayload = 7900

// ErrClosed is returned by Listen and Publish on a Conn that has been closed.
var ErrClosed = errors.New("pglisten: connection is closed")

// channelName is what LISTEN will accept unquoted. A channel is interpolated
// into the statement — LISTEN takes no parameters — so it is validated rather
// than trusted.
var channelName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// ValidateChannel returns a non-nil error if name is not a channel LISTEN
// accepts unquoted, meaning it does not match [a-z_][a-z0-9_]*.
//
// Listen applies the same check, so this is only needed to reject a configured
// channel before opening a Conn costs a connection.
func ValidateChannel(name string) error {
	if !channelName.MatchString(name) {
		return fmt.Errorf("channel %q must match %s", name, channelName)
	}
	return nil
}

// Listener receives one channel's traffic.
//
// Calls are serialized: a Conn invokes at most one Listener method at a time,
// across every channel and every Listener it carries. Neither method may block
// — anything slow here stalls delivery on all of them.
type Listener interface {
	// Notification handles one payload, byte for byte as Publish was given it.
	Notification(payload string)
	// Reconnected reports that the listener connection was replaced, leaving a
	// gap in the stream that nothing replays. A caller whose notifications can
	// be resynthesized re-sends them here; one carrying state that cannot has
	// nothing to do.
	Reconnected()
}

// Conn is a shared LISTEN/NOTIFY transport. It must come from Open — the zero
// value is not usable — and every method is safe for concurrent use.
type Conn struct {
	dsn    string
	logger *slog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
	// done is closed by Close, so a Listen waiting on a registration is
	// released rather than blocking until its own context expires.
	done chan struct{}

	mu        sync.Mutex
	listeners map[string]Listener
	// ready holds one channel per registration that has not been LISTENed yet,
	// closed by the listener goroutine once it has. Listen blocks on it, so a
	// caller that returns from Listen knows the subscription is live and not
	// merely promised.
	ready map[string]chan struct{}
	// interrupt aborts the current WaitForNotification so a new registration is
	// picked up. nil while the listener goroutine is not waiting.
	interrupt context.CancelFunc
	closed    bool

	// pubMu serializes publishing and guards pub, which is dialled lazily.
	pubMu sync.Mutex
	pub   *pgx.Conn
}

// Option configures Open.
type Option func(*config)

type config struct {
	logger *slog.Logger
}

// WithLogger installs the logger used for the transport's own trouble —
// reconnects, failed publishes. Defaults to slog.Default.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// Open connects to the Postgres named by dsn (a libpq URI or key/value string)
// and starts listening.
//
// The listener connection is established before returning, so a bad DSN or an
// unreachable database fails here rather than degrading silently into a
// transport that never delivers. ctx bounds the dial only: the Conn outlives it
// and holds its connections until the caller Closes it.
func Open(ctx context.Context, dsn string, opts ...Option) (*Conn, error) {
	cfg := config{logger: slog.Default()}
	for _, opt := range opts {
		opt(&cfg)
	}

	listener, err := dial(ctx, dsn)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Conn{
		dsn:       dsn,
		logger:    cfg.logger,
		cancel:    cancel,
		done:      make(chan struct{}),
		listeners: make(map[string]Listener),
		ready:     make(map[string]chan struct{}),
	}

	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.listenLoop(runCtx, listener) }()

	return c, nil
}

// dial opens a connection outside any pool. Both of the transport's connections
// are long-lived and single-purpose, which is what a pool is the wrong shape
// for: the listener holds a session-scoped subscription and the publisher is
// one serialized writer.
func dial(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pglisten: connect: %w", err)
	}
	return conn, nil
}

// Listen subscribes to channel and routes its notifications to l. It returns
// once the subscription is live on the database.
//
// The channel must match [a-z_][a-z0-9_]* and must not already be registered.
// ctx bounds the wait for the subscription to take effect — which spans a
// reconnect if the database is down — and abandoning it unregisters the
// channel. Returns ErrClosed on a closed Conn.
//
// Registering a channel while others are already carrying traffic costs those
// others whatever is published over the next round trip — the same gap a
// reconnect leaves. Registering every channel before any of them is published
// to avoids it.
func (c *Conn) Listen(ctx context.Context, channel string, l Listener) error {
	if err := ValidateChannel(channel); err != nil {
		return fmt.Errorf("pglisten: %w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if _, dup := c.listeners[channel]; dup {
		c.mu.Unlock()
		return fmt.Errorf("pglisten: channel %q is already registered", channel)
	}
	c.listeners[channel] = l
	ready := make(chan struct{})
	c.ready[channel] = ready
	interrupt := c.interrupt
	c.mu.Unlock()

	// The listener goroutine is parked in WaitForNotification and cannot issue
	// a statement until it is out; cancelling its context is what gets it out.
	if interrupt != nil {
		interrupt()
	}

	select {
	case <-ready:
		return nil
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		c.unregister(channel)
		return fmt.Errorf("pglisten: listen on %q: %w", channel, ctx.Err())
	}
}

// unregister drops a channel whose Listen gave up waiting.
func (c *Conn) unregister(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.listeners, channel)
	delete(c.ready, channel)
}

// Publish sends payload on channel, blocking until Postgres has accepted it or
// ctx is done.
//
// Publishes on one Conn are serialized, so a caller that must not wait on
// another's round trip queues and publishes from a goroutine of its own.
// Payloads over MaxPayload bytes are refused without being sent. Returns
// ErrClosed on a closed Conn.
func (c *Conn) Publish(ctx context.Context, channel, payload string) error {
	if len(payload) > MaxPayload {
		return fmt.Errorf("pglisten: payload is %d bytes, over the %d a NOTIFY can carry",
			len(payload), MaxPayload)
	}

	c.pubMu.Lock()
	defer c.pubMu.Unlock()

	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}

	for attempt := 0; ; attempt++ {
		conn, err := c.publisher(ctx)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload); err != nil {
			// A statement that failed on a connection still up is a real
			// failure. A connection that died taking the statement with it is
			// the idle publisher having been reaped or failed over, and is
			// worth one retry on a freshly dialled one.
			if conn.IsClosed() {
				c.pub = nil
				if attempt == 0 && ctx.Err() == nil {
					continue
				}
			}
			return fmt.Errorf("pglisten: publish on %q: %w", channel, err)
		}
		return nil
	}
}

// publisher returns the publishing connection, dialling one if there is none or
// the last is closed. Called with pubMu held.
func (c *Conn) publisher(ctx context.Context) (*pgx.Conn, error) {
	if c.pub != nil && !c.pub.IsClosed() {
		return c.pub, nil
	}
	conn, err := dial(ctx, c.dsn)
	if err != nil {
		return nil, err
	}
	c.pub = conn
	return conn, nil
}

// Close stops the listener and releases both connections. It is idempotent, and
// unblocks any Listen still waiting for its subscription.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()

	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	if c.pub != nil {
		_ = c.pub.Close(context.Background())
		c.pub = nil
	}
	return nil
}

// listenLoop owns the listener connection: it subscribes what has been
// registered, hands notifications to their listeners, and redials whenever the
// connection goes — whether it failed or was interrupted to pick up a new
// channel.
func (c *Conn) listenLoop(ctx context.Context, conn *pgx.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
		}
	}()

	const minBackoff = 100 * time.Millisecond
	const maxBackoff = 5 * time.Second
	backoff := minBackoff
	// wait sleeps out the current backoff and lengthens it, reporting false once
	// the Conn is closed. Dialling and subscribing share it, so a connection
	// that keeps coming up and then refusing to LISTEN backs off like one that
	// will not come up at all rather than spinning on redials.
	wait := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
		return true
	}
	// subscribed tracks what this connection has LISTENed, so a redial
	// resubscribes everything and a registration only costs its own statement.
	subscribed := make(map[string]bool)
	// reconnected is false for the connection Open established: there is no gap
	// before the first one, and nothing is subscribed to announce it to.
	reconnected := false

	for {
		if conn == nil {
			var err error
			conn, err = dial(ctx, c.dsn)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Warn("pglisten: listener reconnect failed", "error", err, "retry_in", backoff)
				if !wait() {
					return
				}
				continue
			}
			clear(subscribed)
			reconnected = true
		}

		if err := c.subscribe(ctx, conn, subscribed); err != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("pglisten: subscribing failed", "error", err, "retry_in", backoff)
			if !wait() {
				return
			}
			continue
		}
		// Both halves came up, so the next failure starts from a short retry.
		backoff = minBackoff

		if reconnected {
			reconnected = false
			c.announceReconnect()
		}

		waitCtx, stop := context.WithCancel(ctx)
		if !c.armInterrupt(stop, subscribed) {
			// A registration landed between subscribing above and arming here.
			// Waiting now would park until the next notification — which is a
			// wakeup nothing is obliged to send — so go round and subscribe it.
			stop()
			continue
		}
		notification, err := conn.WaitForNotification(waitCtx)
		c.setInterrupt(nil)
		stop()

		if err != nil {
			// Cancelling the context to pick up a registration breaks the
			// connection just as a real fault does — pgx aborts the read by
			// deadline and the session does not survive it — so both go down
			// the same redial path, and only the fault is worth a log line.
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
			if ctx.Err() != nil {
				return
			}
			if waitCtx.Err() == nil {
				c.logger.Warn("pglisten: listener connection lost", "error", err)
			}
			continue
		}

		c.dispatch(notification.Channel, notification.Payload)
	}
}

// subscribe issues LISTEN for every registered channel this connection has not
// subscribed yet, releasing each channel's waiting Listen as it lands.
func (c *Conn) subscribe(ctx context.Context, conn *pgx.Conn, subscribed map[string]bool) error {
	for _, channel := range c.unsubscribed(subscribed) {
		if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
			return fmt.Errorf("listen on %q: %w", channel, err)
		}
		subscribed[channel] = true
		c.markReady(channel)
	}
	return nil
}

// unsubscribed lists the registered channels missing from subscribed, sorted so
// the statements do not come out in map order.
func (c *Conn) unsubscribed(subscribed map[string]bool) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var todo []string
	for channel := range c.listeners {
		if !subscribed[channel] {
			todo = append(todo, channel)
		}
	}
	slices.Sort(todo)
	return todo
}

// markReady releases the Listen waiting on this channel's first subscription.
func (c *Conn) markReady(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ready, ok := c.ready[channel]; ok {
		close(ready)
		delete(c.ready, channel)
	}
}

// armInterrupt publishes the cancel func Listen uses to get the listener
// goroutine out of its wait, and reports whether waiting is safe.
//
// Deciding both under one lock is what closes the window between subscribing
// and waiting: a Listen that registered in it read a nil interrupt and sent no
// wakeup, so the wait it was meant to break would never end.
func (c *Conn) armInterrupt(cancel context.CancelFunc, subscribed map[string]bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for channel := range c.listeners {
		if !subscribed[channel] {
			return false
		}
	}
	c.interrupt = cancel
	return true
}

// setInterrupt clears the cancel func once the wait is over.
func (c *Conn) setInterrupt(cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.interrupt = cancel
}

// dispatch hands one notification to its channel's listener. A payload for a
// channel nobody registered is dropped: another process on the same database
// may listen to channels this one does not.
func (c *Conn) dispatch(channel, payload string) {
	c.mu.Lock()
	l := c.listeners[channel]
	c.mu.Unlock()
	if l != nil {
		l.Notification(payload)
	}
}

// announceReconnect tells every listener the stream had a gap.
func (c *Conn) announceReconnect() {
	c.mu.Lock()
	ls := make([]Listener, 0, len(c.listeners))
	for _, l := range c.listeners {
		ls = append(ls, l)
	}
	c.mu.Unlock()
	for _, l := range ls {
		l.Reconnected()
	}
}
