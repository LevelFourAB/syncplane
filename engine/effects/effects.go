// Package effects runs server-side reactions to committed changes: webhooks,
// notifications, search indexing, agents — anything that should happen after a
// row is written and is not itself part of the write.
//
// It is a consumer of the engine's change feed (server.Changes), packaged so
// that every adopter does not rebuild the same drain loop. What it adds over
// calling Changes directly is the bookkeeping that is easy to get subtly wrong:
// a durable position kept in the engine's own store, scheduling that wakes a
// workspace when it is written, backoff and health for a handler that keeps
// failing, and before-state for handlers that react to a transition rather than
// to a state.
//
// # The contract
//
// Handlers run POST-COMMIT, on a background goroutine, never inside the write
// transaction they react to. Side effects must stay out of retried write
// closures, and Mutate is non-reentrant, so a handler is free to write back
// through the server API — it is not running inside anything.
//
// Delivery is AT-LEAST-ONCE. A crash between a handler acting and its position
// committing redelivers the change, by design: it is the only recovery an
// outbound effect has, since there is no reconcile pass that can un-send an
// email or notice a webhook was never posted. Handlers must be idempotent. One
// that writes back should do so through server.MutateIdempotent keyed on the
// change (workspace, entity, key and seq identify it exactly).
//
// Ordering is PER WORKSPACE, matching the feed. One consumer's handler runs
// serially for a workspace; different workspaces, and different consumers,
// proceed independently.
//
// A handler that keeps failing WEDGES its own consumer for that workspace: the
// position does not advance, the change is retried with backoff, and everything
// behind it waits. This is deliberate — the alternative is skipping a change
// with no record and no repair path — and it is bounded to one consumer, since
// each has its own position. Watch Health to see it happen; nothing else will
// tell you.
//
// # What the feed can and cannot tell you
//
// The feed reports STATE, not transitions, and coalesces by row: a handler sees
// the latest state of a row, never every intermediate write. Declaring Watch
// fields recovers the part of a transition that matters — what those fields held
// when this consumer last acted on the row — but it cannot recover writes that
// coalesced away in between.
//
// It is also not an event log. Changes older than the workspace's tombstone
// horizon are collected, and a consumer that falls that far behind resets and
// resumes at the current sequence, having permanently missed what it never saw.
// An effect that must not miss anything needs the event persisted as a row
// through Mutate, where it syncs and is retained under normal row rules.
//
// # What it costs across many workspaces
//
// A runtime is expected to carry a consumer across every workspace in a store,
// so nothing here is allowed to scale with how many of those there are.
//
// Work is scheduled, not polled. Writing a workspace wakes it through the
// notifier's activity stream — one subscription for the whole store, not one
// per workspace — and a periodic sweep asks the engine which workspaces each
// consumer is behind on rather than opening a transaction per workspace to find
// out. What runs is bounded by a fixed pool of workers, and what is REMEMBERED
// is bounded by the work outstanding: a (consumer, workspace) pair costs memory
// while it has work queued, a pass running or a failure being retried, and is
// forgotten once it has drained, because everything durable about its progress
// is in the workspace's store. Tracked reports that resident set.
//
// The two things that do scale with the store are a sweep's query, once per
// interval per consumer, and the storage the positions themselves occupy. Both
// are proportional to workspaces that have been written, neither is on the path
// a reaction takes, and GC collects the positions of consumers that have gone.
//
// # Single instance
//
// The cursor is engine-owned, but nothing yet leases a workspace to one drainer.
// Two processes running the same consumer each drain the feed and each fire the
// handler, so run exactly one instance of a given consumer until leasing lands.
// A rolling deploy counts: the outgoing process drains while the incoming one
// starts, and Close waiting on in-flight passes narrows that window without
// closing it.
package effects

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Config wires a Runtime to a server.
type Config struct {
	// Server is the engine whose change feed the runtime drains. Required.
	Server *server.Server
	// BatchSize bounds rows per change-feed page (default 256). A page is
	// delivered handler by handler and its bookkeeping commits once, so this is
	// also how much work one crash can redeliver.
	BatchSize int
	// Workers bounds how many drain passes run at once, across every consumer
	// and workspace (default 16). It is the runtime's only concurrency bound: a
	// sweep can queue as many pairs as there are workspaces with work, and what
	// keeps that from arriving at the storage layer all at once is the size of
	// this pool.
	//
	// Raise it for sinks that are slow and I/O-bound — a webhook consumer with
	// thousands of workspaces is limited by this number times the per-pass
	// latency, not by the engine. Lower it to leave more of the store's read
	// capacity to serving clients. Passes for one (consumer, workspace) never
	// overlap whatever this is set to.
	Workers int
	// Interval is how often the runtime sweeps for work it was not told about.
	// 0 means the default (15 seconds); negative disables the periodic sweep,
	// leaving activity signals and Kick as the only triggers. Start always
	// sweeps once, so a runtime finds what is already waiting for it however
	// this is set.
	//
	// The sweep is a safety net rather than the main path. A workspace that is
	// written wakes its consumers through the notifier, so reaction latency is
	// not this interval — but notifier signals carry no guarantee, and a
	// notifier that cannot report activity at all (see
	// server.SubscribeActivity) leaves the sweep as the only way to find work.
	// Disabling it is therefore a choice to rely on the notifier.
	Interval time.Duration
	// RetryMin and RetryMax bound per-consumer, per-workspace backoff after a
	// failed pass: the delay doubles from RetryMin (default 5s) up to RetryMax
	// (default 5m), jittered so consumers failing against the same destination
	// do not retry in lockstep. A negative RetryMin disables backoff, so every
	// trigger attempts.
	RetryMin time.Duration
	RetryMax time.Duration
	// OnHealth is called with a consumer's status after every pass, successful
	// or not. It is the surface that makes a wedged consumer visible; without it
	// (or without polling Health) a handler failing forever looks like a handler
	// with nothing to do.
	//
	// It runs on the worker's goroutine, inside the pass it describes, so it
	// must not block for long and must not call Drain for that same consumer and
	// workspace — the pass it would wait for is the one calling it. Health and
	// HealthAll are safe to call from it.
	OnHealth func(Health)
	// TracerProvider and MeterProvider are the OpenTelemetry providers the
	// runtime records on. nil means the global provider, which is no-op until
	// the process installs an SDK — the same fallback Logger makes to
	// slog.Default().
	//
	// Per-(consumer, workspace) health is deliberately NOT a metric: workspace
	// is an unbounded partition key, and a runtime is expected to carry a
	// consumer across every workspace in the store. Lag is exported as a
	// maximum per consumer; use OnHealth or HealthAll to find WHICH workspace
	// is behind. See docs/reference/telemetry.md.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	// Workspaces enumerates the workspaces a sweep considers. Setting it
	// REPLACES the engine's pending-work query with a plain list, which is the
	// escape hatch for a runtime that has to cover workspaces the engine cannot
	// be asked about — and it costs a queued pass per workspace per sweep,
	// whether or not there is anything to do. Leave it nil unless that trade is
	// the one you want.
	//
	// It bounds the sweep only. A workspace that announces itself through the
	// notifier, or arrives via Kick, is drained either way.
	Workspaces func(ctx context.Context) ([]string, error)
	// Logger is the structured logger the background loops write to. Failures
	// are logged and never returned — a reaction failing must not take the
	// process down — with "component", "consumer" and "workspace" attributes.
	// nil means slog.Default().
	Logger *slog.Logger
	// HealthRetention bounds how many pairs that have drained and gone quiet
	// keep a Health entry (default 1024; negative keeps none). See Health for
	// what the runtime does and does not remember.
	HealthRetention int
}

// Change is one committed change as an effect consumer sees it: the feed's
// change, plus whatever before-state the consumer declared an interest in. It
// is what a Sink receives; the typed Event handlers get is a view of it.
type Change struct {
	Workspace string
	Entity    string
	Key       string
	// Seq is the workspace sequence of the mutation that wrote this state.
	Seq uint64
	// Deleted reports a tombstone: the row is gone and After is nil.
	Deleted bool
	// Actor is who made the write, as the feed reports it — the row's LAST
	// writer, not the writer of every change that coalesced into this one. Use
	// it to suppress a consumer's own echo, not as an audit record; see
	// server.Change.Actor for the blind spot that comes with it.
	Actor storage.Actor
	// After is the row's new state, nil when Deleted and nil when the entity is
	// not registered on this server.
	After proto.Message
	// Before holds the watched fields as they stood when this consumer last
	// acted on the row, and nil when it never has (or declared no Watch
	// fields). Only watched fields are populated — it is a shadow of the row,
	// not the row.
	Before proto.Message
	// Changed names the watched fields whose value differs between Before and
	// After. Empty when nothing watched changed, and empty when there is no
	// Before to compare against.
	Changed []string
	// FirstSeen reports that this consumer held no shadow of the row, so there
	// was no before-state to diff and Changed is empty. It is the honest answer
	// to "did status become Done" when the consumer has never looked at the row:
	// not "no", but "unknown".
	//
	// It is true for a row created just now, for a row that existed before the
	// consumer was registered, and for the first sighting after a horizon reset
	// — the shadow is the consumer's memory, not the row's history, so the three
	// are indistinguishable. Handlers choose: skip them to react only to
	// observed transitions, or treat them as changes to react to what is
	// already there. Always false for a consumer that declared no Watch fields,
	// which keeps no memory to be first against.
	FirstSeen bool
}

// Sink receives the changes one consumer is interested in. It is the seam
// between the runtime's bookkeeping and what actually reacts: the in-process
// handlers registered with On are one implementation, and publishing to a
// broker — where retry schedules, dead-lettering and fan-out become the broker's
// job rather than this package's — is the other one it is shaped for.
//
// Deliver is called serially per workspace, in feed order, from the runtime's
// runner goroutine. Returning an error wedges the consumer on that workspace
// until the change goes through, so return nil for anything the sink means to
// skip. The runtime does not own the sink and never closes it.
type Sink interface {
	Deliver(ctx context.Context, ch Change) error
}

// SinkFunc adapts a plain function to Sink.
type SinkFunc func(ctx context.Context, ch Change) error

func (f SinkFunc) Deliver(ctx context.Context, ch Change) error { return f(ctx, ch) }

// consumer is one registered reaction: a durable name, the entities it cares
// about, and where its changes go.
type consumer struct {
	name string
	// entities bounds which changes reach the sink, mapping wire entity name to
	// the prototype it decodes as. Empty means every entity in the workspace,
	// which is what a broker sink wants and an entity handler never does — and
	// which is why a consumer with no entities cannot watch fields: there is no
	// prototype to resolve a field path against.
	entities map[string]proto.Message
	sink     Sink
	// watch maps a wire entity name to the field paths whose before-state this
	// consumer keeps a shadow of. Absent means no shadow, which costs nothing.
	watch map[string][]string
}

// wants reports whether a change to this entity reaches the sink.
func (c *consumer) wants(entity string) bool {
	if len(c.entities) == 0 {
		return true
	}
	_, ok := c.entities[entity]
	return ok
}

// watches returns the field paths this consumer keeps a shadow of for an
// entity, and the prototype to build those shadows from. A nil paths result
// means no shadow work for this entity at all.
func (c *consumer) watches(entity string) (prototype proto.Message, paths []string) {
	paths = c.watch[entity]
	if len(paths) == 0 {
		return nil, nil
	}
	return c.entities[entity], paths
}

// Runtime drains the change feed into registered consumers. Build one with New,
// register consumers, then Start it.
//
// What it holds while running is proportional to the work outstanding, not to
// the store: a fixed pool of workers, one activity subscription, one sweep
// timer, and a pair of bookkeeping structs per (consumer, workspace) that has
// something to do. A workspace that is quiet costs nothing, and one that has
// drained is forgotten until it is written again.
type Runtime struct {
	srv       *server.Server
	batch     int
	workers   int
	interval  time.Duration
	retryMin  time.Duration
	retryMax  time.Duration
	onHealth  func(Health)
	listWS    func(ctx context.Context) ([]string, error)
	logger    *slog.Logger
	consumers map[string]*consumer

	mu      sync.Mutex
	ready   *sync.Cond
	started bool
	stopped bool
	runCtx  context.Context
	cancel  context.CancelFunc
	// pairs is the resident set: every (consumer, workspace) the runtime is
	// tracking, which is those with work queued, a pass running, a failure to
	// keep visible, or a caller holding them.
	pairs map[runnerKey]*pair
	// queue is the ready line the workers drain, in the order pairs were
	// scheduled. A pair appears in it at most once — pairState is what
	// guarantees that — so its length is bounded by the resident set.
	queue []*pair
	wg    sync.WaitGroup

	mx *metrics

	hmu       sync.Mutex
	health    map[runnerKey]Health
	retention int
	retired   []retirement
	retiredAt map[runnerKey]uint64
	retireSeq uint64
}

// New validates cfg and builds a Runtime with no consumers registered.
func New(cfg Config) (*Runtime, error) {
	if cfg.Server == nil {
		return nil, fmt.Errorf("effects: Server is required")
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = 256
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = 16
	}
	retention := cfg.HealthRetention
	if retention == 0 {
		retention = 1024
	}
	if retention < 0 {
		retention = 0
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = 15 * time.Second
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
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	providers := telemetry.Providers{
		TracerProvider: cfg.TracerProvider,
		MeterProvider:  cfg.MeterProvider,
	}
	mx, err := newMetrics(providers)
	if err != nil {
		return nil, fmt.Errorf("effects: telemetry: %w", err)
	}
	rt := &Runtime{
		srv:      cfg.Server,
		batch:    batch,
		workers:  workers,
		interval: interval,
		retryMin: retryMin,
		retryMax: retryMax,
		onHealth: cfg.OnHealth,
		// nil is meaningful: it selects the engine's pending-work query over a
		// listing, so only an adopter-supplied enumerator lands here.
		listWS:    cfg.Workspaces,
		logger:    logger.With("component", "effects"),
		consumers: make(map[string]*consumer),
		pairs:     make(map[runnerKey]*pair),
		health:    make(map[runnerKey]Health),
		retention: retention,
		retiredAt: make(map[runnerKey]uint64),
		mx:        mx,
	}
	rt.ready = sync.NewCond(&rt.mu)
	// Registered after the Runtime exists, because the callback samples it.
	// Close unregisters, so a runtime that is built and closed leaves nothing
	// behind in the meter provider.
	if err := mx.observe(providers, rt); err != nil {
		return nil, fmt.Errorf("effects: telemetry: %w", err)
	}
	return rt, nil
}

// Register wires a Sink to the changes of the given entities, under a durable
// consumer name. Passing no entities subscribes to every entity in the
// workspace, including ones registered later and ones this server cannot decode
// (whose changes arrive with a nil After).
//
// The name is the identity of the consumer's durable position, so it must be
// stable across restarts: renaming a consumer does not carry its progress over,
// it starts a first run — which, against a workspace that has collected
// tombstones, means resuming at the current sequence rather than replaying
// history. Register before Start; registering afterwards is an error, because
// runners are built per consumer when they start.
func (rt *Runtime) Register(name string, sink Sink, entities ...proto.Message) error {
	return rt.register(name, sink, entities, nil)
}

func (rt *Runtime) register(name string, sink Sink, entities []proto.Message, watch map[string][]string) error {
	if sink == nil {
		return fmt.Errorf("effects: consumer %q needs a sink", name)
	}
	if err := validateName(name); err != nil {
		return err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.started {
		return fmt.Errorf("effects: consumer %q registered after Start", name)
	}
	if _, dup := rt.consumers[name]; dup {
		return fmt.Errorf("effects: consumer %q registered twice", name)
	}
	c := &consumer{name: name, sink: sink, entities: make(map[string]proto.Message, len(entities)), watch: watch}
	for _, prototype := range entities {
		if prototype == nil {
			return fmt.Errorf("effects: consumer %q registered with a nil entity", name)
		}
		entity, _, err := rt.srv.EntityKey(prototype)
		if err != nil {
			return fmt.Errorf("effects: consumer %q: %w", name, err)
		}
		c.entities[entity] = prototype
	}
	rt.consumers[name] = c
	return nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("effects: consumer name is required")
	}
	if len(name) > server.MaxConsumerNameBytes {
		return fmt.Errorf("effects: consumer name is %d bytes, limit is %d", len(name), server.MaxConsumerNameBytes)
	}
	return nil
}
