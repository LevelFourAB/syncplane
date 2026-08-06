package connect

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/id"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/server"
)

// Config wires a Connector to a server and a source.
type Config struct {
	// Server is the engine the connector ingresses into (server.Mutate) and
	// egresses out of (server.Changes).
	Server *server.Server
	// Source is the adopter's port over the external store, shared by every
	// workspace. Exactly one of Source and OpenSource is required.
	Source Source
	// OpenSource builds a Source for one workspace, so a per-user credentialed
	// store has somewhere to keep its token, connection and rate-limit state.
	// It is called lazily on the workspace's first pass and the result is
	// reused; when the returned Source implements io.Closer it is closed once
	// the connector is done with it (Close, Forget, or a pass failing with
	// ErrNeedsUserAction). Exactly one of Source and OpenSource is required.
	OpenSource func(ctx context.Context, workspaceID string) (Source, error)
	// State is the connector's durable bookkeeping (e.g. sqlitestate.Open).
	// The Connector owns it after New succeeds; Close closes it.
	State State
	// Entities are prototypes of the entities this connector binds
	// bidirectionally — the shorthand for one Bindings entry per prototype
	// with the default Direction. Each must be registered on Server. Changes
	// to entities outside this set (and outside Bindings) never reach the
	// Source.
	Entities []proto.Message
	// Bindings declares entities whose direction is not the default. Merged
	// with Entities; binding the same entity through both is an error.
	Bindings []Binding
	// BatchSize bounds rows per change-feed page during egress (default 256).
	BatchSize int
	// Workers bounds how many convergence passes run at once, across every
	// workspace (default 16). It is the connector's only concurrency bound: a
	// sweep queues every managed workspace, and what keeps them from arriving
	// at the source together is the size of this pool, not the size of the
	// queue.
	//
	// It is the setting that decides how hard a connector leans on the far
	// side. Lower it for an API with a shared per-account rate limit; raise it
	// when passes are slow and independent, since a fleet of workspaces takes
	// this number of passes per round trip to get through. Passes for one
	// workspace never overlap whatever this is set to.
	Workers int
	// ReconcileInterval is how often Start runs a periodic pass per workspace
	// (and how often workspaces are discovered and dropped). 0 means the
	// default (1 minute); negative disables periodic passes, leaving commit
	// signals, Kick and manual calls as the only triggers — discovery still
	// runs, at a fixed one-minute cadence. For an Incremental source this is
	// how often Changes is polled, not how often the store is enumerated.
	ReconcileInterval time.Duration
	// FullReconcileInterval bounds how long an Incremental source may run on
	// its cursor alone before a full List runs as a safety net — catching
	// whatever a change feed silently omits. 0 means the default (1 hour);
	// negative runs a full pass only when there is no cursor or the source
	// rejects the one it has. Ignored for a source that is not Incremental,
	// where every pass is already a full List.
	FullReconcileInterval time.Duration
	// RetryMin and RetryMax bound the per-workspace backoff after a failed
	// pass: the delay doubles from RetryMin (default 5s) up to RetryMax
	// (default 5m), jittered. A negative RetryMin disables backoff, so every
	// trigger attempts. An error carrying RetryAfter overrides both.
	RetryMin time.Duration
	RetryMax time.Duration
	// IngestChunk and IngestPause pace bulk ingress: after every IngestChunk
	// engine writes, an ingress pass sleeps IngestPause before continuing.
	// Both default to 0, which is off — set both to keep a cold-start import
	// of a large account from starving interactive writes behind the
	// workspace's write lock. Unchanged items are free; only writes count.
	IngestChunk int
	IngestPause time.Duration
	// OnHealth is called with the workspace's status after every pass,
	// successful or not. Persist it, render it, alert on it. It runs on the
	// calling goroutine — a worker's, or that of a direct Ingress / Reconcile
	// / Drain call — so it must not block for long.
	OnHealth func(Health)
	// Workspaces enumerates the workspaces Start manages, and is re-read on
	// every sweep: a workspace it stops naming is dropped, releasing its
	// source and its health. Default: the engine's workspace list —
	// sufficient once a workspace has been written, but a source-first
	// workspace (data exists only in the external store) must be supplied here
	// or nudged with Kick, which manages it regardless of this list.
	Workspaces func(ctx context.Context) ([]string, error)
	// WriterID names this connector as a collaborative-text writer, and must be
	// unique among connectors that can run at the same time — two live writers
	// sharing one mint colliding item ids (see applyTextFields). Reusing the
	// same value across restarts of a single connector is the point: the peer
	// is spelled out in every document it writes to and nothing reclaims it, so
	// a per-run identity leaves one dead entry per restart in every document
	// the connector has touched. Empty means a fresh identity per instance,
	// which is always safe but pays that cost.
	//
	// It also labels the connector's writes on the change feed, as
	// "connect:<WriterID>" — the name an effect or another connector suppresses
	// its own echo by. Empty labels them "connect", which is enough to tell a
	// connector's write from a GC pass but not one connector from another.
	WriterID string
	// Logger is the structured logger the background loops write to. Loop
	// failures are logged (never returned — convergence work must not take the
	// process down) with "component" and "workspace" attributes. nil means
	// slog.Default(); a discard-handler logger silences the connector.
	Logger *slog.Logger
	// TracerProvider and MeterProvider are the OpenTelemetry providers the
	// connector records on. nil means the global provider, which is no-op until
	// the process installs an SDK — the same fallback Logger makes to
	// slog.Default().
	//
	// Per-workspace health is deliberately NOT a metric (workspace is an
	// unbounded partition key); use OnHealth or HealthAll for that. What these
	// carry is the aggregate: pass rate, pass duration, and the failure-kind
	// split. See docs/reference/telemetry.md.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// binding is one bound entity: what to instantiate, and which way it flows.
type binding struct {
	prototype proto.Message
	direction Direction
}

// Connector keeps the engine and one external source converged. Build one
// with New, then drive it manually (Ingress, Reconcile, Drain) or via Start.
//
// A started connector holds a fixed pool of workers, one activity
// subscription, one sweep timer, and a small record per managed workspace —
// not a loop per workspace. What runs at once is Config.Workers; what is
// remembered is the workspaces Config.Workspaces names plus the ones Kick
// announced, since everything durable about a workspace's progress lives in
// State. Tracked reports that set.
type Connector struct {
	srv   *server.Server
	srcs  *sourceCache
	state State
	// bound maps wire entity names to their binding.
	bound map[string]binding
	// needsWriter records that some binding is Bidirectional, so every source
	// this connector uses must implement Writer.
	needsWriter  bool
	batch        int
	workers      int
	interval     time.Duration
	fullInterval time.Duration
	retryMin     time.Duration
	retryMax     time.Duration
	ingestChunk  int
	ingestPause  time.Duration
	onHealth     func(Health)
	listWS       func(ctx context.Context) ([]string, error)
	logger       *slog.Logger
	mx           *metrics
	// peer is this connector's collaborative-text writer identity, fixed for the
	// instance's lifetime (Config.WriterID, or one minted in New): see
	// applyTextFields.
	peer string
	// label names this connector on every row it writes (server.TrustedAs), so
	// a feed consumer can recognize a connector's writes and skip them. It is
	// derived from Config.WriterID rather than from the minted peer: a label
	// that changes per run tells a consumer nothing.
	label string

	mu      sync.Mutex
	ready   *sync.Cond
	started bool
	stopped bool
	cancel  context.CancelFunc
	// tracked is the managed set: every workspace the connector holds a
	// schedule record for, which is what discovery listed plus what Kick
	// announced, minus what Forget and a sweep have dropped.
	tracked map[string]*workspace
	// queue is the ready line the workers drain, in the order workspaces were
	// scheduled. A workspace appears in it at most once — wsState is what
	// guarantees that — so its length is bounded by the managed set.
	queue []*workspace
	wg    sync.WaitGroup

	hmu    sync.Mutex
	health map[string]Health
}

// New validates cfg and builds a Connector.
func New(cfg Config) (*Connector, error) {
	if cfg.Server == nil || cfg.State == nil {
		return nil, fmt.Errorf("connect: Server and State are required")
	}
	if (cfg.Source == nil) == (cfg.OpenSource == nil) {
		return nil, fmt.Errorf("connect: exactly one of Source and OpenSource is required")
	}
	if len(cfg.Entities) == 0 && len(cfg.Bindings) == 0 {
		return nil, fmt.Errorf("connect: at least one entity must be bound")
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = 256
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = 16
	}
	interval := cfg.ReconcileInterval
	if interval == 0 {
		interval = time.Minute
	}
	fullInterval := cfg.FullReconcileInterval
	if fullInterval == 0 {
		fullInterval = time.Hour
	}
	retryMin := cfg.RetryMin
	if retryMin == 0 {
		retryMin = 5 * time.Second
	}
	retryMax := cfg.RetryMax
	if retryMax <= 0 {
		retryMax = 5 * time.Minute
	}
	if retryMax < retryMin {
		retryMax = retryMin
	}
	listWS := cfg.Workspaces
	if listWS == nil {
		listWS = cfg.Server.WorkspaceIDs
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "connect")
	writerID := cfg.WriterID
	label := "connect"
	if writerID == "" {
		writerID = id.New()
	} else {
		label += ":" + writerID
	}
	providers := telemetry.Providers{
		TracerProvider: cfg.TracerProvider,
		MeterProvider:  cfg.MeterProvider,
	}
	mx, err := newMetrics(providers)
	if err != nil {
		return nil, fmt.Errorf("connect: telemetry: %w", err)
	}
	c := &Connector{
		srv:          cfg.Server,
		state:        cfg.State,
		bound:        make(map[string]binding, len(cfg.Entities)+len(cfg.Bindings)),
		batch:        batch,
		workers:      workers,
		interval:     interval,
		fullInterval: fullInterval,
		retryMin:     retryMin,
		retryMax:     retryMax,
		ingestChunk:  cfg.IngestChunk,
		ingestPause:  cfg.IngestPause,
		onHealth:     cfg.OnHealth,
		listWS:       listWS,
		logger:       logger,
		mx:           mx,
		tracked:      make(map[string]*workspace),
		health:       make(map[string]Health),
		peer:         "connector:" + writerID,
		label:        label,
	}
	c.ready = sync.NewCond(&c.mu)
	bind := func(prototype proto.Message, dir Direction) error {
		entity, _, err := cfg.Server.EntityKey(prototype)
		if err != nil {
			return fmt.Errorf("connect: bind %T: %w", prototype, err)
		}
		if _, dup := c.bound[entity]; dup {
			return fmt.Errorf("connect: entity %q bound twice", entity)
		}
		c.bound[entity] = binding{prototype: prototype, direction: dir}
		if dir == Bidirectional {
			c.needsWriter = true
		}
		return nil
	}
	for _, prototype := range cfg.Entities {
		if err := bind(prototype, Bidirectional); err != nil {
			return nil, err
		}
	}
	for _, b := range cfg.Bindings {
		if b.Prototype == nil {
			return nil, fmt.Errorf("connect: binding with no Prototype")
		}
		if err := bind(b.Prototype, b.Direction); err != nil {
			return nil, err
		}
	}
	// A bidirectional binding needs somewhere to write. Config.Source can be
	// checked right here; an OpenSource result only when it is first built,
	// which is why the check travels with the source cache.
	check := func(src Source) error {
		if _, ok := src.(Writer); ok || !c.needsWriter {
			return nil
		}
		return fmt.Errorf("connect: source %T does not implement Writer, required by a Bidirectional binding", src)
	}
	if cfg.Source != nil {
		if err := check(cfg.Source); err != nil {
			return nil, err
		}
	}
	c.srcs = newSourceCache(cfg.Source, cfg.OpenSource, check, logger)
	// Registered after the Connector exists, because the callback samples it.
	// Close unregisters, so a connector that is built and closed leaves
	// nothing behind in the meter provider.
	if err := mx.observe(providers, c); err != nil {
		return nil, fmt.Errorf("connect: telemetry: %w", err)
	}
	return c, nil
}

// converge runs one pass: take the workspace's turn, acquire its source, do
// the work, fold the outcome into Health. Every public entry point goes
// through here, so a connector driven by hand is as observable — and as
// careful with source lifetime — as one driven by Start.
//
// Taking the turn is what keeps a hand-driven Ingress, Reconcile or Drain from
// running alongside a worker's pass on the same workspace, where the two would
// interleave their shadow and cursor writes. It waits for the pass in flight;
// it does not wait out a backoff window, which direct calls still ignore
// because the caller asked.
func (c *Connector) converge(ctx context.Context, workspaceID string, fn func(*pass) error) (err error) {
	w := c.hold(workspaceID)
	defer c.release(w)
	w.run.Lock()
	defer w.run.Unlock()

	ctx, span := c.mx.tracer.Start(ctx, "syncplane.connect.pass",
		trace.WithAttributes(telemetry.AttrWorkspace.String(workspaceID)))
	start := time.Now()
	// kind is filled in below once health has classified the failure; a pass
	// that returns before that (cancellation) leaves it FailureNone, which is
	// right — nothing about the source failed.
	kind := FailureNone
	defer func() {
		attrs := metric.WithAttributes(
			telemetry.AttrOutcome.String(telemetry.Classify(err)),
			telemetry.AttrFailureKind.String(kind.String()),
		)
		c.mx.passes.Add(ctx, 1, attrs)
		c.mx.passDuration.Record(ctx, time.Since(start).Seconds(), attrs)
		telemetry.EndSpan(span, err)
	}()

	p, err := c.srcs.acquire(ctx, workspaceID)
	if err == nil {
		err = fn(p)
		p.release()
	}
	// A cancelled context is the caller (or Close) stopping the pass, not the
	// source failing: leave health as it stands rather than reporting a
	// shutdown as an outage.
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return err
	}
	h := c.recordPass(workspaceID, err)
	kind = h.Kind
	if h.Kind == FailureUserAction {
		// The credential is stale: drop the cached source so the next pass
		// rebuilds it, picking up whatever a re-consent flow has stored.
		c.srcs.invalidate(workspaceID)
	}
	return err
}

// Close stops the workers and the loops Start launched, waiting for in-flight
// passes to finish, closes any per-workspace sources it opened, and releases
// the connector's state store. Idempotent.
func (c *Connector) Close() error {
	c.stop()
	c.wg.Wait()
	// After the workers are done, so a sampling callback cannot run against a
	// half-torn-down connector.
	c.mx.close()
	return errors.Join(c.srcs.closeAll(), c.state.Close())
}

// decodeBase unmarshals a shadow's base bytes into a fresh message of the
// bound prototype's type.
func (c *Connector) decodeBase(prototype proto.Message, base []byte) (proto.Message, error) {
	msg := prototype.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(base, msg); err != nil {
		return nil, fmt.Errorf("connect: decode shadow base: %w", err)
	}
	return msg, nil
}
