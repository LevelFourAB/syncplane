package blobbridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// ErrRunning is returned by Run when another Run of the same Bridge has not
// returned yet.
var ErrRunning = errors.New("blobbridge: already running")

// Default values of the options.
const (
	DefaultConcurrency   = 4
	DefaultMinBackoff    = time.Second
	DefaultMaxBackoff    = time.Minute
	DefaultSweepInterval = time.Minute
	DefaultFetchPatience = 10 * time.Minute
	DefaultConsumerName  = "blobbridge"
)

// StageWait is how long a read of a blob that the local engine does not hold
// waits, at most, for the fetch from the origin to stage the blob locally.
const StageWait = 10 * time.Second

// Bridge is a blob.Store that wraps the local engine's store and moves blobs
// between that engine and an origin server. Build one with New, pass it to
// server.WithBlobStore, and call Run with the server.
//
// Without a Run in progress, the Bridge is the wrapped store and nothing
// more. A Bridge is safe for concurrent use.
type Bridge struct {
	inner  blob.Store
	origin syncplanev1connect.BlobServiceClient
	cfg    config
	// active is the state of the Run in progress, nil between runs.
	active atomic.Pointer[runState]
}

// runState is what one Run shares with the store methods.
type runState struct {
	srv   *server.Server
	sched *scheduler

	// rescans are the workspaces whose local blobs must all be checked
	// again; rescanNow wakes the goroutine that does it.
	mu        sync.Mutex
	rescans   map[string]bool
	rescanNow chan struct{}
}

// requestRescan queues a check of every local blob of the workspace.
func (st *runState) requestRescan(ws string) {
	st.mu.Lock()
	st.rescans[ws] = true
	st.mu.Unlock()
	select {
	case st.rescanNow <- struct{}{}:
	default:
	}
}

// takeRescan removes one requested workspace from the queue.
func (st *runState) takeRescan() (string, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for ws := range st.rescans {
		delete(st.rescans, ws)
		return ws, true
	}
	return "", false
}

type config struct {
	entities    map[string]bool
	concurrency int
	prefetch    bool
	readWait    time.Duration
	patience    time.Duration
	minBackoff  time.Duration
	maxBackoff  time.Duration
	sweep       time.Duration
	consumer    string
	log         *slog.Logger
}

// Option configures New.
type Option func(*config)

// WithEntities limits the bridge to blobs that rows of the named entities
// name. A blob that only rows of other entities name is not uploaded and not
// fetched. Without this option every entity counts. Call it with the entities
// that sync with the origin.
func WithEntities(names ...string) Option {
	return func(c *config) {
		if c.entities == nil {
			c.entities = map[string]bool{}
		}
		for _, n := range names {
			c.entities[n] = true
		}
	}
}

// WithConcurrency sets how many transfers and checks run at the same time.
// Values below 1 mean 1. The default is DefaultConcurrency.
func WithConcurrency(n int) Option {
	return func(c *config) { c.concurrency = max(n, 1) }
}

// WithPrefetch makes the bridge fetch a blob from the origin as soon as a row
// that names it is committed in the local engine, without waiting for a read.
// It applies to rows committed while Run runs. Prefetch is off by default.
func WithPrefetch() Option {
	return func(c *config) { c.prefetch = true }
}

// WithReadWait sets how long a read of a blob that the local engine does not
// hold waits for the fetch from the origin to commit it (see Bridge.Stat).
// A read that the fetch does not finish in time sees the blob as being
// uploaded once the fetch has staged it, and the local engine notifies the
// workspace when the blob commits. The default, 0, waits only for the fetch
// to stage the blob.
func WithReadWait(d time.Duration) Option {
	return func(c *config) { c.readWait = max(d, 0) }
}

// WithFetchPatience sets how long the bridge keeps trying to fetch a blob
// after the last read or prefetch that asked for it, while the origin does
// not have it or cannot be reached. Non-positive values keep the default,
// DefaultFetchPatience.
func WithFetchPatience(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.patience = d
		}
	}
}

// WithBackoff sets the wait before the first retry of a failed transfer or
// check, and the largest wait; the wait doubles after each failure, with
// jitter. Non-positive values keep the defaults, DefaultMinBackoff and
// DefaultMaxBackoff.
func WithBackoff(minWait, maxWait time.Duration) Option {
	return func(c *config) {
		if minWait > 0 {
			c.minBackoff = minWait
		}
		if maxWait > 0 {
			c.maxBackoff = maxWait
		}
	}
}

// WithSweepInterval sets how often Run lists the local workspaces whose
// change feed has rows it has not read, to find writes that no activity
// notification reported. Non-positive values keep the default,
// DefaultSweepInterval.
func WithSweepInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.sweep = d
		}
	}
}

// WithConsumerName sets the name under which Run keeps its position in the
// change feed of each local workspace (see server.AdvanceConsumer). The name
// must not be used by any other consumer of the local engine. The default is
// DefaultConsumerName.
func WithConsumerName(name string) Option {
	return func(c *config) { c.consumer = name }
}

// WithLogger sets the logger for transfer failures. The default is
// slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.log = l }
}

// New returns a Bridge over inner, the local engine's blob store, that
// transfers blobs with origin. The Bridge owns inner: Close closes it.
//
// The local server must use the Bridge as its only blob store, and must not
// be hosted by a server.Manager: the Bridge reads the names in inner as
// workspace ids.
func New(inner blob.Store, origin syncplanev1connect.BlobServiceClient, opts ...Option) *Bridge {
	cfg := config{
		concurrency: DefaultConcurrency,
		patience:    DefaultFetchPatience,
		minBackoff:  DefaultMinBackoff,
		maxBackoff:  DefaultMaxBackoff,
		sweep:       DefaultSweepInterval,
		consumer:    DefaultConsumerName,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.log == nil {
		cfg.log = slog.Default()
	}
	cfg.maxBackoff = max(cfg.maxBackoff, cfg.minBackoff)
	return &Bridge{inner: inner, origin: origin, cfg: cfg}
}

// Run moves blobs between srv and the origin until ctx ends, then waits for
// the transfers in progress to stop and returns nil. srv must be the server
// that uses the Bridge as its blob store.
//
// Uploads: Run uploads a blob when it is committed locally, and when a row
// that names it is committed. It follows the change feed of each local
// workspace from a position it keeps in the local engine (see
// WithConsumerName), so its work follows the workspaces being written. A blob
// that a row names when Run reads the row from the feed is uploaded also when
// the row leaves the local engine before the upload. At start it also checks
// every blob committed in the local store, a bounded number at a time, and
// uploads those that a live row names and the origin does not report
// complete. An interrupted upload resumes from the chunks the origin holds. Failed transfers are retried with backoff; a refusal by the
// origin (invalid_argument, resource_exhausted, permission_denied) is logged
// and retried at the next Run.
//
// Reads: while Run runs, a read of a blob that the local store does not hold
// starts a fetch from the origin when a live row of the local workspace names
// the blob (see Stat). Concurrent reads of one blob share one fetch. Each
// chunk is checked against its hash before it is stored.
//
// Run returns ErrRunning when another Run of the Bridge is in progress, and
// the server's error when the consumer name is not valid.
func (b *Bridge) Run(ctx context.Context, srv *server.Server) error {
	if srv == nil {
		return errors.New("blobbridge: Run needs a server")
	}
	if _, err := srv.ConsumerPendingWorkspaces(ctx, b.cfg.consumer, "", 1); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	st := &runState{
		srv:       srv,
		sched:     newScheduler(b.cfg.minBackoff, b.cfg.maxBackoff),
		rescans:   map[string]bool{},
		rescanNow: make(chan struct{}, 1),
	}
	if !b.active.CompareAndSwap(nil, st) {
		return ErrRunning
	}
	defer b.active.Store(nil)

	f := newFeed(b, st)
	f.start()
	defer f.stop()

	var wg sync.WaitGroup
	for range b.cfg.concurrency {
		wg.Go(func() { b.work(ctx, st) })
	}
	wg.Go(func() { b.rescanLoop(ctx, st) })
	f.loop(ctx)
	wg.Wait()
	return nil
}

// running returns the state of the Run in progress, or nil, for a store call
// made with ctx. Calls the bridge makes itself into the local engine see nil,
// so they never trigger more work.
func (b *Bridge) running(ctx context.Context) *runState {
	if isBridgeCall(ctx) {
		return nil
	}
	return b.active.Load()
}

// counts reports whether rows of entity count for the bridge.
func (b *Bridge) counts(entity string) bool {
	return b.cfg.entities == nil || b.cfg.entities[entity]
}

// entityFilter is counts in the form server.ReferencedBlobs takes.
func (b *Bridge) entityFilter() func(string) bool {
	if b.cfg.entities == nil {
		return nil
	}
	return b.counts
}

// rescanLoad is the most checks and uploads a rescan keeps queued: it reads
// more digests from the store only below it.
const rescanLoad = 4 * checkBatch

// rescanLoop checks every blob committed in the local store, then every
// blob of each workspace a rescan is requested for, until ctx ends.
func (b *Bridge) rescanLoop(ctx context.Context, st *runState) {
	for ws, err := range b.inner.ListWorkspaces(ctx) {
		if err != nil {
			b.logFailure(ctx, "list workspaces of the local blob store", "", "", err)
			break
		}
		b.rescanWorkspace(ctx, st, ws)
	}
	for {
		for ws, ok := st.takeRescan(); ok; ws, ok = st.takeRescan() {
			b.rescanWorkspace(ctx, st, ws)
		}
		select {
		case <-ctx.Done():
			return
		case <-st.rescanNow:
		}
	}
}

// rescanWorkspace queues a check of every blob committed in the workspace,
// one batch at a time, each once the queued work is below rescanLoad.
func (b *Bridge) rescanWorkspace(ctx context.Context, st *runState, ws string) {
	batch := make([]blob.Digest, 0, checkBatch)
	flush := func() {
		st.sched.waitLoad(ctx, rescanLoad)
		st.sched.check(ws, batch, false)
		batch = batch[:0]
	}
	for d, err := range b.inner.List(ctx, ws) {
		if err != nil {
			b.logFailure(ctx, "list local blobs", ws, "", err)
			break
		}
		if ctx.Err() != nil {
			return
		}
		batch = append(batch, d)
		if len(batch) == checkBatch {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
}

type bridgeCallKey struct{}

// bridgeCall marks ctx as a call the bridge makes into the local engine.
func bridgeCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, bridgeCallKey{}, true)
}

func isBridgeCall(ctx context.Context) bool {
	v, _ := ctx.Value(bridgeCallKey{}).(bool)
	return v
}
