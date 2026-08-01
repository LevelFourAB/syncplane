package connect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// pass is the scope of one convergence run: the source handle it borrowed
// and the pacing counter it accumulates. Every internal converge step takes
// one, so the source is acquired exactly once per pass no matter how many
// steps it fans into (a Drain that hits the feed horizon runs a reconcile and
// a resync inside the same pass, on the same handle).
type pass struct {
	src Source
	// w is the source's Writer half, nil when it does not implement one —
	// which New only permits when every binding is IngressOnly.
	w Writer

	cache *sourceCache
	entry *sourceEntry

	// wrote counts engine writes for IngestChunk pacing.
	wrote int
}

// writer returns the source's Writer half. Egress paths are gated on
// direction, and New rejects a bidirectional binding over a source without a
// Writer, so a nil here is a bug rather than a configuration error — report
// it as such instead of panicking inside a background loop.
func (p *pass) writer() (Writer, error) {
	if p.w == nil {
		return nil, errors.New("connect: source does not implement Writer")
	}
	return p.w, nil
}

// release returns the handle to the cache.
func (p *pass) release() {
	if p.cache != nil {
		p.cache.release(p.entry)
	}
}

// sourceEntry is one workspace's cached source and its in-flight ref count.
type sourceEntry struct {
	src  Source
	refs int
	// dead marks an entry that has been evicted: it is no longer handed out,
	// and closes as soon as the last in-flight pass releases it.
	dead bool
}

// sourceCache owns the mapping from workspace to Source.
//
// With Config.Source there is one instance for every workspace and nothing to
// cache — the connector never closes what it did not open. With
// Config.OpenSource each workspace gets its own, built lazily on first use so
// credentials, connections and rate-limit state have somewhere to live, and
// reused across passes so a per-minute tick does not re-run an OAuth
// handshake. An entry is evicted — closed, when the Source implements
// io.Closer, and rebuilt on the next pass — by Connector.Close, by Forget,
// and by any pass that fails with ErrNeedsUserAction, which is the seam a
// re-consent flow hangs off: fix the credential, and the next pass picks it
// up without restarting the process.
type sourceCache struct {
	static Source
	open   func(ctx context.Context, workspaceID string) (Source, error)
	// check validates a freshly opened source against the bindings (a
	// bidirectional binding needs a Writer). Run once per open, not per pass.
	check  func(Source) error
	logger *slog.Logger

	mu      sync.Mutex
	closed  bool
	entries map[string]*sourceEntry
}

func newSourceCache(static Source, open func(context.Context, string) (Source, error), check func(Source) error, logger *slog.Logger) *sourceCache {
	return &sourceCache{
		static:  static,
		open:    open,
		check:   check,
		logger:  logger,
		entries: make(map[string]*sourceEntry),
	}
}

func (sc *sourceCache) acquire(ctx context.Context, workspaceID string) (*pass, error) {
	if sc.static != nil {
		w, _ := sc.static.(Writer)
		return &pass{src: sc.static, w: w}, nil
	}
	sc.mu.Lock()
	if sc.closed {
		sc.mu.Unlock()
		return nil, errors.New("connect: connector is closed")
	}
	if e, ok := sc.entries[workspaceID]; ok {
		e.refs++
		sc.mu.Unlock()
		return sc.handle(workspaceID, e), nil
	}
	sc.mu.Unlock()

	// Opening is adopter code that may block on network I/O (a token
	// refresh): never under the cache lock. Two passes racing the first open
	// of one workspace can therefore both build a source; the loser's is
	// closed below rather than leaked.
	src, err := sc.open(ctx, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("connect: open source for workspace %q: %w", workspaceID, err)
	}
	if src == nil {
		return nil, fmt.Errorf("connect: OpenSource returned a nil Source for workspace %q", workspaceID)
	}
	if err := sc.check(src); err != nil {
		sc.closeSource(src)
		return nil, err
	}

	sc.mu.Lock()
	if sc.closed {
		sc.mu.Unlock()
		sc.closeSource(src)
		return nil, errors.New("connect: connector is closed")
	}
	if e, ok := sc.entries[workspaceID]; ok {
		e.refs++
		sc.mu.Unlock()
		sc.closeSource(src)
		return sc.handle(workspaceID, e), nil
	}
	e := &sourceEntry{src: src, refs: 1}
	sc.entries[workspaceID] = e
	sc.mu.Unlock()
	return sc.handle(workspaceID, e), nil
}

func (sc *sourceCache) handle(workspaceID string, e *sourceEntry) *pass {
	w, _ := e.src.(Writer)
	return &pass{src: e.src, w: w, cache: sc, entry: e}
}

func (sc *sourceCache) release(e *sourceEntry) {
	if e == nil {
		return
	}
	sc.mu.Lock()
	e.refs--
	last := e.refs == 0 && e.dead
	sc.mu.Unlock()
	if last {
		sc.closeSource(e.src)
	}
}

// invalidate evicts a workspace's source. Passes already holding it keep
// running against it — they are mid-flight and their handle is still valid —
// and the source closes once the last of them releases.
func (sc *sourceCache) invalidate(workspaceID string) {
	sc.mu.Lock()
	e, ok := sc.entries[workspaceID]
	if !ok {
		sc.mu.Unlock()
		return
	}
	delete(sc.entries, workspaceID)
	e.dead = true
	last := e.refs == 0
	sc.mu.Unlock()
	if last {
		sc.closeSource(e.src)
	}
}

// closeAll evicts every workspace and closes what is not in flight. Called
// once, from Connector.Close, after the runners have stopped.
func (sc *sourceCache) closeAll() error {
	sc.mu.Lock()
	sc.closed = true
	idle := make([]Source, 0, len(sc.entries))
	for id, e := range sc.entries {
		delete(sc.entries, id)
		e.dead = true
		if e.refs == 0 {
			idle = append(idle, e.src)
		}
	}
	sc.mu.Unlock()
	var errs []error
	for _, src := range idle {
		if closer, ok := src.(io.Closer); ok {
			errs = append(errs, closer.Close())
		}
	}
	return errors.Join(errs...)
}

// closeSource closes a source that implements io.Closer. Failures are logged,
// not propagated: every caller is on a path (release, eviction) whose own
// result is about something else, and a source that will not close is not a
// reason to fail a convergence pass.
func (sc *sourceCache) closeSource(src Source) {
	closer, ok := src.(io.Closer)
	if !ok {
		return
	}
	if err := closer.Close(); err != nil && sc.logger != nil {
		sc.logger.LogAttrs(context.Background(), slog.LevelWarn, "closing source failed", slog.Any("err", err))
	}
}
