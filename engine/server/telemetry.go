package server

import (
	"context"
	"log/slog"

	"connectrpc.com/otelconnect"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
)

// WithTracerProvider sets the OpenTelemetry tracer provider the engine records
// spans on. The default is the global provider, which is no-op until the
// process installs an SDK — the same fallback shape as WithLogger and
// slog.Default().
//
// Pass one explicitly rather than relying on the global when a test needs to
// assert against its own recorder, or when a host runs several engines that
// should report separately.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(s *Server) error {
		if tp != nil {
			s.providers.TracerProvider = tp
		}
		return nil
	}
}

// WithMeterProvider sets the OpenTelemetry meter provider the engine records
// metrics on. The default is the global provider, no-op until an SDK is
// installed.
//
// The instruments are listed in docs/reference/telemetry.md. Their names are a
// compatibility surface once dashboards are built on them, so they change like
// any other public API.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(s *Server) error {
		if mp != nil {
			s.providers.MeterProvider = mp
		}
		return nil
	}
}

// serverTelemetry is everything the engine records through: the instruments
// and the tracer, resolved together because they come from the same pair of
// providers.
type serverTelemetry struct {
	*metrics
	tracer trace.Tracer
}

// tel returns the server's telemetry, falling back to a no-op set built on
// demand. New always installs a real one; the fallback is for a Server
// constructed directly — internal tests do this to reach unexported state —
// which would otherwise nil-panic inside a background loop rather than
// anywhere near the code that built it. Same reasoning as s.log() falling back
// to slog.Default().
func (s *Server) tel() *serverTelemetry {
	s.telOnce.Do(func() {
		if s.telemetry != nil {
			return
		}
		// A no-op provider cannot fail to build an instrument, so the error is
		// genuinely dead here — unlike in New, where the provider is the
		// caller's and a bad one must be reported.
		m, _ := newMetrics(telemetry.Providers{
			MeterProvider:  metricnoop.NewMeterProvider(),
			TracerProvider: tracenoop.NewTracerProvider(),
		})
		s.telemetry = &serverTelemetry{metrics: m, tracer: tracenoop.NewTracerProvider().Tracer(telemetry.ScopeName)}
	})
	return s.telemetry
}

// metrics is the server's instrument set, built once in New. Instruments are
// safe for concurrent use and cheap to record on, so every request path holds
// the same pointers rather than resolving anything per call.
//
// Read the cardinality rule in engine/internal/telemetry before adding an
// attribute here: nothing in this struct may be keyed by workspace or client.
type metrics struct {
	// pushMutations counts mutations the server accepted and applied, by
	// entity and kind. Paired with pushRejections it gives the accept ratio
	// that a client stuck in a rebase loop destroys.
	pushMutations metric.Int64Counter
	// pushRejections counts mutations the server refused, by entity and
	// rejection code. This is the signal a 200 OK hides: a rejected mutation
	// is still an ack, so the RPC layer reports every push as a success no
	// matter how much of it was thrown away.
	pushRejections metric.Int64Counter
	// pushBatch times one workspace's batch — the unit that holds a write
	// transaction, and so the unit that contends.
	pushBatch metric.Float64Histogram

	// pullChanges distributes how many changes a pull returned. A pull that
	// consistently returns a full page means clients are not keeping up.
	pullChanges metric.Int64Histogram
	// pullResets counts clients told to re-bootstrap, by reason. A
	// cursor_below_horizon reset means a client fell behind GC and must
	// re-download the workspace, so this is the metric that says whether
	// retention is set correctly for how long clients actually stay away.
	pullResets metric.Int64Counter

	// streams tracks Subscribe streams currently open. An up-down counter
	// rather than a counter because what matters is the level, not the rate.
	streams metric.Int64UpDownCounter

	// notifyRequested counts change notifications the engine raised;
	// notifyPublished counts the ones that reached the notifier underneath the
	// coalescing wrapper. The gap between them is what WithNotifyCoalescing
	// absorbed, which is the only direct read on whether the window is set
	// sensibly: near-zero collapse means the window is doing nothing but
	// adding latency, near-total collapse means it is worth widening.
	notifyRequested metric.Int64Counter
	notifyPublished metric.Int64Counter

	// gcDuration times a whole pass over every workspace, by outcome.
	gcDuration metric.Float64Histogram
	// gcWorkspaces counts per-workspace pass results. A pass collects
	// per-workspace failures and continues, so a partial failure is invisible
	// in gcDuration alone — this is where one persistently broken workspace
	// shows up.
	gcWorkspaces metric.Int64Counter
	// gcCollected counts what a pass reclaimed, by kind. The
	// stranded_consumers kind is the one worth alerting on: an effect consumer
	// collected below the horizon is one that stopped reacting to writes long
	// enough to be given up on.
	gcCollected metric.Int64Counter

	// indexBuildDuration times one backfill batch, by outcome.
	indexBuildDuration metric.Float64Histogram
	// indexBuildRows counts rows backfilled into a secondary index. Until an
	// index is built, reads that would use it fall back to scanning, so this
	// is how far along that is.
	indexBuildRows metric.Int64Counter
}

// Instrument names. Declared as constants in one block because they are the
// contract a dashboard is written against — a rename here is a breaking change
// for whoever is graphing it, and it should be visible as one.
const (
	metricPushMutations      = "syncplane.push.mutations"
	metricPushRejections     = "syncplane.push.rejections"
	metricPushBatchDuration  = "syncplane.push.batch.duration"
	metricPullChanges        = "syncplane.pull.changes"
	metricPullResets         = "syncplane.pull.resets"
	metricSubscribeStreams   = "syncplane.subscribe.streams"
	metricNotifyRequested    = "syncplane.notify.requested"
	metricNotifyPublished    = "syncplane.notify.published"
	metricGCDuration         = "syncplane.gc.duration"
	metricGCWorkspaces       = "syncplane.gc.workspaces"
	metricGCCollected        = "syncplane.gc.collected"
	metricIndexBuildDuration = "syncplane.index.build.duration"
	metricIndexBuildRows     = "syncplane.index.build.rows"
)

// newMetrics declares the server's instruments. A failure here is a
// misconfigured meter, not a runtime condition, so it surfaces from New
// alongside a bad schema rather than being swallowed.
func newMetrics(p telemetry.Providers) (*metrics, error) {
	b := telemetry.NewBuilder(p)
	m := &metrics{
		pushMutations: b.Int64Counter(metricPushMutations,
			metric.WithDescription("Mutations accepted and applied."),
			metric.WithUnit("{mutation}")),
		pushRejections: b.Int64Counter(metricPushRejections,
			metric.WithDescription("Mutations refused, by rejection code."),
			metric.WithUnit("{mutation}")),
		pushBatch: b.Float64Histogram(metricPushBatchDuration,
			metric.WithDescription("Duration of one workspace's push batch."),
			metric.WithUnit("s")),
		pullChanges: b.Int64Histogram(metricPullChanges,
			metric.WithDescription("Changes returned by one pull."),
			metric.WithUnit("{change}")),
		pullResets: b.Int64Counter(metricPullResets,
			metric.WithDescription("Clients told to re-bootstrap, by reason."),
			metric.WithUnit("{reset}")),
		streams: b.Int64UpDownCounter(metricSubscribeStreams,
			metric.WithDescription("Subscribe streams currently open."),
			metric.WithUnit("{stream}")),
		notifyRequested: b.Int64Counter(metricNotifyRequested,
			metric.WithDescription("Change notifications raised by the engine, before coalescing."),
			metric.WithUnit("{notification}")),
		notifyPublished: b.Int64Counter(metricNotifyPublished,
			metric.WithDescription("Change notifications that reached the notifier, after coalescing."),
			metric.WithUnit("{notification}")),
		gcDuration: b.Float64Histogram(metricGCDuration,
			metric.WithDescription("Duration of a garbage-collection pass over every workspace."),
			metric.WithUnit("s")),
		gcWorkspaces: b.Int64Counter(metricGCWorkspaces,
			metric.WithDescription("Per-workspace garbage-collection results."),
			metric.WithUnit("{workspace}")),
		gcCollected: b.Int64Counter(metricGCCollected,
			metric.WithDescription("Rows reclaimed by garbage collection, by kind."),
			metric.WithUnit("{row}")),
		indexBuildDuration: b.Float64Histogram(metricIndexBuildDuration,
			metric.WithDescription("Duration of one secondary-index backfill batch."),
			metric.WithUnit("s")),
		indexBuildRows: b.Int64Counter(metricIndexBuildRows,
			metric.WithDescription("Rows backfilled into a secondary index."),
			metric.WithUnit("{row}")),
	}
	return m, b.Err()
}

// countingNotifier counts the notifies passing one point in the notifier
// chain and forwards everything else. Two of them, on either side of the
// coalescing wrapper, is what makes coalescing measurable without pubsub
// having to know telemetry exists: what went in, what came out, difference
// absorbed.
//
// Unwrap keeps the chain traversable — SubscribeActivity walks it looking for
// the optional Activity extension, and a wrapper that did not implement this
// would silently hide a shared notifier's workspace discovery.
type countingNotifier struct {
	inner pubsub.Notifier
	count metric.Int64Counter
}

// Notify records and forwards. Notifier has no context — a notify is raised
// from wherever a write committed, including background work with no request
// to attribute it to — so this records against the background context. That
// costs the exemplar linking a metric back to a trace, which is an acceptable
// trade for not threading a context through a fan-out seam that has never
// needed one.
func (n *countingNotifier) Notify(workspaceID string) {
	n.count.Add(context.Background(), 1)
	n.inner.Notify(workspaceID)
}

func (n *countingNotifier) Subscribe(workspaceID string) (<-chan struct{}, func()) {
	return n.inner.Subscribe(workspaceID)
}

// Unwrap exposes the wrapped notifier, per the pubsub chain convention.
func (n *countingNotifier) Unwrap() pubsub.Notifier { return n.inner }

// rpcInterceptor builds the otelconnect interceptor: RPC spans, RED metrics
// and W3C trace-context extraction for every procedure, from the semantic
// conventions rather than from anything hand-rolled here. Extraction is what
// makes a client-side trace and this server's spans one trace.
func rpcInterceptor(p telemetry.Providers) (*otelconnect.Interceptor, error) {
	opts := []otelconnect.Option{
		// Without this, every span and metric carries the peer address, which
		// on a public sync endpoint is one attribute value per client — the
		// cardinality rule applied to what otelconnect emits, not just to what
		// this repo emits.
		otelconnect.WithoutServerPeerAttributes(),
	}
	if p.TracerProvider != nil {
		opts = append(opts, otelconnect.WithTracerProvider(p.TracerProvider))
	}
	if p.MeterProvider != nil {
		opts = append(opts, otelconnect.WithMeterProvider(p.MeterProvider))
	}
	return otelconnect.NewInterceptor(opts...)
}

// traceIDAttrs returns the trace correlation attributes for ctx, empty when
// nothing is recording. Logs carry them so a log line found by grep leads back
// to the trace it happened in, and so a trace leads to the semantic logging
// the spans do not duplicate.
//
// Empty when no span is recording, which keeps an uninstrumented deployment's
// log lines exactly as they were.
func traceIDAttrs(ctx context.Context) []slog.Attr {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []slog.Attr{
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	}
}

// traceIDArgs is traceIDAttrs for the variadic ...any logging calls
// (Logger.With and friends) that cannot take a []slog.Attr.
func traceIDArgs(ctx context.Context) []any {
	attrs := traceIDAttrs(ctx)
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	return args
}
