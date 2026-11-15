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
	// Records switches the connector to record mode, where one external
	// record syncs with a root row and its member rows; see Records. It
	// cannot be used with Source, OpenSource, Entities, Bindings or State,
	// and it requires WriterID: all its bookkeeping is in the engine's store.
	Records *Records
	// Source is the adopter's port over the external store, shared by every
	// workspace. Exactly one of Source and OpenSource is required, except in
	// record mode.
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
	// with the default Direction, where the source owns every field. Each must
	// be registered on Server. Changes to entities outside this set (and
	// outside Bindings) never reach the Source.
	Entities []proto.Message
	// Bindings declares entities whose direction is not the default, or whose
	// source owns only some fields. Merged with Entities; binding the same
	// entity through both is an error.
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
	// WriterID names this connector. It labels the connector's writes on the
	// change feed as "connect:<WriterID>", the name an effect or another
	// connector suppresses its echo by. Give each instance of one connector the
	// same WriterID, also an origin and its replicas, so that one constant
	// skips the writes of all of them. Empty labels the writes "connect", which
	// tells a connector's write from a GC pass but not one connector from
	// another.
	//
	// With a WriterID, the connector also records how far it has egressed in
	// the engine's store, as the consumer position of "connect:<WriterID>".
	// That copy outlives a lost State: a row the engine changed after it is a
	// client edit the source has not received, and the next pass sends it to
	// the source instead of taking the source's older value. Without a
	// WriterID, a lost State loses such edits (see State).
	//
	// When TextPeer is empty, WriterID also sets the collaborative-text peer,
	// as "connector:<WriterID>"; see TextPeer.
	WriterID string
	// TextPeer is the collaborative-text writer identity the connector mints
	// text items under, used as given with no prefix. Two connectors that can
	// write the same rows at the same time, such as an origin and its replicas,
	// must have different peers, because two live writers that share a peer
	// mint colliding item ids. Keep the same value across restarts of one
	// instance: a document keeps an entry for each peer that wrote to it and
	// nothing removes it, so a peer per run adds one dead entry per restart to
	// each document the connector wrote.
	//
	// Empty means "connector:<WriterID>", or a new peer for each instance when
	// WriterID is empty too, which is safe but pays the cost of a peer per run.
	TextPeer string
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

// binding is one bound entity: what to instantiate, which way it flows, and
// which of its fields the source owns.
type binding struct {
	prototype proto.Message
	direction Direction
	// owned holds the units (top-level fields and oneof groups) the source
	// owns, key fields included; nil means every field. See ownedFields.
	owned map[string]bool
	// ownedUnits lists the owned units that are not text fields: the ones an
	// item carries on Item.Msg rather than in Item.TextFields.
	ownedUnits []string
}

// Connector keeps the engine and one external source converged. Build one
// with New, then drive it manually (Ingress, Reconcile, Drain, Flush) or via
// Start. Its methods are safe for concurrent use; passes for one workspace
// never overlap.
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
	// instance's lifetime (Config.TextPeer, Config.WriterID, or one minted in
	// New): see applyTextFields.
	peer string
	// label names this connector on every row it writes (server.TrustedAs), so
	// a feed consumer can recognize a connector's writes and skip them. It is
	// derived from Config.WriterID rather than from the minted peer: a label
	// that changes per run tells a consumer nothing.
	label string
	// consumer is the engine consumer name the egress position is mirrored
	// under, "" without Config.WriterID. See egressMirror. Record mode keeps
	// all its bookkeeping under it.
	consumer string
	// rec is the record-mode configuration, nil in row mode.
	rec *records
	// now is the clock record mode reads for quiet periods and delete grace.
	now func() time.Time

	mu      sync.Mutex
	ready   *sync.Cond
	started bool
	stopped bool
	cancel  context.CancelFunc
	// runs holds the run lock of each workspace id with a pass running or
	// waiting; see runLock.
	runs map[string]*runLock
	// tracked is the managed set: every workspace the connector holds a
	// schedule record for, which is what discovery listed plus what Kick
	// announced, minus what Forget and a sweep have dropped.
	tracked map[string]*workspace
	// queue is the ready line the workers drain, in the order workspaces were
	// scheduled. A workspace appears in it at most once — wsState is what
	// guarantees that — so its length is bounded by the managed set.
	queue []*workspace
	wg    sync.WaitGroup
	// wakes and wakeTimer schedule the later passes record mode asks for; see
	// setWakeLocked.
	wakes     wakeHeap
	wakeTimer *time.Timer
	// unflushed names the workspaces whose last record pass left records
	// waiting for egress, which Close flushes.
	unflushed map[string]struct{}

	hmu    sync.Mutex
	health map[string]Health
	// failures holds the records each workspace's Health.Refused lists.
	failures map[string]map[failureID]RecordFailure
}

// New validates cfg and builds a Connector.
func New(cfg Config) (*Connector, error) {
	if cfg.Server == nil {
		return nil, fmt.Errorf("connect: Server is required")
	}
	var rec *records
	if cfg.Records != nil {
		var err error
		if rec, err = newRecords(cfg); err != nil {
			return nil, err
		}
	} else {
		if cfg.State == nil {
			return nil, fmt.Errorf("connect: Server and State are required")
		}
		if (cfg.Source == nil) == (cfg.OpenSource == nil) {
			return nil, fmt.Errorf("connect: exactly one of Source and OpenSource is required")
		}
		if len(cfg.Entities) == 0 && len(cfg.Bindings) == 0 {
			return nil, fmt.Errorf("connect: at least one entity must be bound")
		}
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
	peer := cfg.TextPeer
	if peer == "" {
		peer = "connector:" + writerID
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
		runs:         make(map[string]*runLock),
		health:       make(map[string]Health),
		failures:     make(map[string]map[failureID]RecordFailure),
		peer:         peer,
		label:        label,
		rec:          rec,
		now:          time.Now,
		unflushed:    make(map[string]struct{}),
	}
	if cfg.WriterID != "" {
		c.consumer = label
	}
	c.ready = sync.NewCond(&c.mu)
	bind := func(prototype proto.Message, dir Direction, fields []string) error {
		entity, _, err := cfg.Server.EntityKey(prototype)
		if err != nil {
			return fmt.Errorf("connect: bind %T: %w", prototype, err)
		}
		switch dir {
		case Bidirectional, IngressOnly:
		case EgressOnly:
			return fmt.Errorf("connect: binding %q: EgressOnly is for record types only", entity)
		default:
			return fmt.Errorf("connect: binding %q: unknown Direction %d", entity, dir)
		}
		if _, dup := c.bound[entity]; dup {
			return fmt.Errorf("connect: entity %q bound twice", entity)
		}
		meta, _ := cfg.Server.Entity(entity)
		owned, units, err := ownedFields(meta, fields)
		if err != nil {
			return err
		}
		c.bound[entity] = binding{prototype: prototype, direction: dir, owned: owned, ownedUnits: units}
		if dir == Bidirectional {
			c.needsWriter = true
		}
		return nil
	}
	for _, prototype := range cfg.Entities {
		if err := bind(prototype, Bidirectional, nil); err != nil {
			return nil, err
		}
	}
	for _, b := range cfg.Bindings {
		if b.Prototype == nil {
			return nil, fmt.Errorf("connect: binding with no Prototype")
		}
		if err := bind(b.Prototype, b.Direction, b.Fields); err != nil {
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
	defer c.lockRun(workspaceID)()

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
		if c.rec != nil {
			c.afterRecordPass(ctx, w, p, err)
		}
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
//
// In record mode Close first flushes each workspace whose records wait for
// egress, or whose last pass failed (see Flush), for at most one minute each,
// and returns the errors of those flushes with the rest.
func (c *Connector) Close() error {
	c.stop()
	c.wg.Wait()
	var errs []error
	if c.rec != nil {
		c.mu.Lock()
		pending := make([]string, 0, len(c.unflushed))
		for ws := range c.unflushed {
			pending = append(pending, ws)
		}
		c.mu.Unlock()
		for _, ws := range pending {
			if err := c.flushOnExit(ws); err != nil {
				errs = append(errs, fmt.Errorf("connect: flush workspace %q: %w", ws, err))
			}
		}
	}
	// After the workers are done, so a sampling callback cannot run against a
	// half-torn-down connector.
	c.mx.close()
	errs = append(errs, c.srcs.closeAll())
	if c.state != nil {
		errs = append(errs, c.state.Close())
	}
	return errors.Join(errs...)
}

// exitFlushTimeout bounds the flush of one workspace that Close and Forget
// run in record mode.
const exitFlushTimeout = time.Minute

// flushOnExit flushes one workspace for Close or Forget, bounded by
// exitFlushTimeout.
func (c *Connector) flushOnExit(ws string) error {
	ctx, cancel := context.WithTimeout(context.Background(), exitFlushTimeout)
	defer cancel()
	return c.Flush(ctx, ws)
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
