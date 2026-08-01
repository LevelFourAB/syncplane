// Package telemetry holds what every instrumented engine package shares: the
// instrumentation scope, the attribute keys, and the provider resolution that
// makes telemetry free when nobody has configured it.
//
// # API only
//
// Nothing here imports the OpenTelemetry SDK. The engine ships as a library,
// and an adopter who wants no telemetry should not link an SDK, a batcher or
// an exporter to get that. The API's own no-op implementation is what runs
// until someone installs a real provider, so an uninstrumented deployment pays
// an interface call per instrument and nothing else. The SDK belongs in main
// packages (see cmd/syncplaned) and in tests.
//
// # The cardinality rule
//
// Metric attributes must be bounded by the SCHEMA. Span attributes may be
// bounded by the TRAFFIC.
//
// A metric stream exists per distinct attribute set, for as long as the
// process lives, in the collector's memory and in the time-series database
// behind it. An entity name, a rejection code, a reset reason and a consumer
// name are all drawn from finite sets an app declares once, so they are safe
// to key a metric by. workspace_id and client_id are not: workspace_id is the
// engine's hard partition key and grows with the deployment, and client_id is
// an unauthenticated per-installation handle that grows without bound and is
// never reused. Keying a metric by either is how a monitoring bill becomes an
// outage.
//
// Both are genuinely useful when debugging one request, though, which is what
// spans are for — a span is sampled, has a bounded lifetime, and is indexed
// rather than aggregated. So AttrWorkspace and AttrClient are documented as
// span-only, and no metric in this repo may take them.
//
// The same rule rejects free-form strings on metrics: an error message, a row
// key, a cursor. Classify into a bounded enum (see Outcome) and record that.
//
// "Bounded by the schema" means bounded by the schema THIS SERVER REGISTERED,
// not by the name a request happened to carry. An entity name arrives from the
// wire, and a request naming an entity that does not exist is rejected — but
// recording the name it asked for would let any client mint an unbounded
// number of metric streams by pushing garbage. Names taken from a request are
// therefore resolved against the catalog first and replaced with Unregistered
// when they miss, which turns an attack on the metrics pipeline into a single
// extra series.
package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName identifies the instrumentation scope every engine instrument and
// span is created under. It names the instrumenting library rather than the
// instrumented one, which for an engine that instruments itself is the same
// string — but backends key dashboards off it, so it is declared once here
// rather than spelled out per package.
const ScopeName = "github.com/levelfourab/syncplane/engine"

// Attribute keys. The syncplane.* namespace keeps engine attributes clear of
// the semantic conventions otelconnect and otelhttp emit (rpc.*, http.*,
// server.*), so a backend can select on either without collision.
//
// Read the cardinality rule in the package doc before adding one.
const (
	// AttrEntity is a registered entity's wire name. Bounded by the schema.
	AttrEntity = attribute.Key("syncplane.entity")
	// AttrOutcome is a bounded classification of how an operation ended. Use
	// the Outcome constants rather than an error string.
	AttrOutcome = attribute.Key("syncplane.outcome")
	// AttrRejectionCode is the RejectionCode a mutation was refused with.
	AttrRejectionCode = attribute.Key("syncplane.rejection_code")
	// AttrResetReason is why a pull forced the client to re-bootstrap; the
	// resetReason enum in engine/server.
	AttrResetReason = attribute.Key("syncplane.reset_reason")
	// AttrMutationKind distinguishes crud from custom mutations.
	AttrMutationKind = attribute.Key("syncplane.mutation_kind")
	// AttrMutator is a custom mutation's registered mutator name. Bounded:
	// mutators are funcs, declared with WithMutator at construction, and can
	// never be provisioned at runtime.
	AttrMutator = attribute.Key("syncplane.mutator")
	// AttrTxKind distinguishes read from write transactions.
	AttrTxKind = attribute.Key("syncplane.tx_kind")
	// AttrConsumer is an effect consumer's registered name. Bounded: consumers
	// are declared in code.
	AttrConsumer = attribute.Key("syncplane.consumer")
	// AttrSource is a connector source's declared name.
	AttrSource = attribute.Key("syncplane.source")
	// AttrIndex is a secondary index's name, which is derived from field names
	// and so bounded by the schema.
	AttrIndex = attribute.Key("syncplane.index")
	// AttrGCKind names what a garbage-collection pass reclaimed. A fixed set
	// decided by what the pass collects, not by any data it saw.
	AttrGCKind = attribute.Key("syncplane.gc_kind")
	// AttrFailureKind buckets why a connector pass failed — the
	// connect.FailureKind enum, which separates "wait, it heals" from "ask the
	// user to reconnect".
	AttrFailureKind = attribute.Key("syncplane.failure_kind")

	// AttrWorkspace is the workspace partition key. SPAN ONLY — unbounded.
	AttrWorkspace = attribute.Key("syncplane.workspace_id")
	// AttrClient is the client idempotency handle. SPAN ONLY — unbounded, and
	// never reused across installations.
	AttrClient = attribute.Key("syncplane.client_id")
)

// Outcome values for AttrOutcome. Anything that can fail classifies into one
// of these rather than reporting an error string, so the attribute stays
// bounded however the underlying errors evolve.
const (
	// OutcomeOK is a clean success.
	OutcomeOK = "ok"
	// OutcomeError is a fault: the operation did not do what it was asked to.
	OutcomeError = "error"
	// OutcomeCanceled is the caller walking away — a canceled context or a
	// deadline. Distinguished from error because it is not a server fault and
	// should not page anyone.
	OutcomeCanceled = "canceled"
	// OutcomeSkipped is work that was correctly declined: nothing to do, or a
	// precondition that says this is not ours to run.
	OutcomeSkipped = "skipped"
)

// Unregistered stands in for a schema-scoped name a request asked for but the
// catalog does not have. Recording the requested name would make the attribute
// client-controlled and therefore unbounded; collapsing every miss onto one
// value keeps the "did clients ask for something we do not serve" signal
// without the cardinality. Which name it was is in the logs and on the span.
const Unregistered = "_unregistered"

// Providers is the pair of OpenTelemetry providers a component was configured
// with. The zero value is valid and resolves to the globals, which are no-op
// unless the process installed real ones — so a component that is handed
// nothing still works, exactly as a nil *slog.Logger falls back to
// slog.Default().
//
// Taking providers as values rather than reaching for the globals directly is
// what lets a test assert against its own reader without racing every other
// test in the package for global state.
type Providers struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// Tracer returns the engine-scoped tracer.
func (p Providers) Tracer() trace.Tracer {
	tp := p.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer(ScopeName)
}

// Meter returns the engine-scoped meter.
func (p Providers) Meter() metric.Meter {
	mp := p.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	return mp.Meter(ScopeName)
}

// Builder creates instruments off one Meter and remembers the first failure,
// so declaring a block of them costs one error check at the end rather than
// one per instrument.
//
// A failed constructor still yields a usable no-op instrument. That matters
// because construction errors are reported to the caller of New, which may
// choose to carry on: nothing should then nil-panic on the first request.
type Builder struct {
	meter metric.Meter
	err   error
}

// NewBuilder starts an instrument block on p's meter.
func NewBuilder(p Providers) *Builder { return &Builder{meter: p.Meter()} }

// Err returns the first instrument that failed to construct, if any.
func (b *Builder) Err() error { return b.err }

// Int64Counter declares a monotonic count.
func (b *Builder) Int64Counter(name string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	inst, err := b.meter.Int64Counter(name, opts...)
	if err != nil {
		b.record(err)
		inst, _ = metricnoop.Meter{}.Int64Counter(name)
	}
	return inst
}

// Int64UpDownCounter declares a value that rises and falls, such as a count of
// things currently open.
func (b *Builder) Int64UpDownCounter(name string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter {
	inst, err := b.meter.Int64UpDownCounter(name, opts...)
	if err != nil {
		b.record(err)
		inst, _ = metricnoop.Meter{}.Int64UpDownCounter(name)
	}
	return inst
}

// Float64Histogram declares a distribution. Durations are recorded in seconds,
// per OpenTelemetry convention, not in the time.Duration nanoseconds the code
// measures in.
func (b *Builder) Float64Histogram(name string, opts ...metric.Float64HistogramOption) metric.Float64Histogram {
	inst, err := b.meter.Float64Histogram(name, opts...)
	if err != nil {
		b.record(err)
		inst, _ = metricnoop.Meter{}.Float64Histogram(name)
	}
	return inst
}

// Int64Histogram declares a distribution of counts, such as rows per pass.
func (b *Builder) Int64Histogram(name string, opts ...metric.Int64HistogramOption) metric.Int64Histogram {
	inst, err := b.meter.Int64Histogram(name, opts...)
	if err != nil {
		b.record(err)
		inst, _ = metricnoop.Meter{}.Int64Histogram(name)
	}
	return inst
}

// Int64ObservableGauge declares a value sampled at collection time, for state
// a component holds but never "events" on — a consumer's lag, say, which is a
// fact about the world rather than something that happens.
//
// No callback is attached here. Callbacks are registered separately, with
// Meter.RegisterCallback, because that is the form that can be UNREGISTERED:
// a gauge sampling a component that has been closed would otherwise keep the
// component alive through the meter provider and keep reporting its last known
// state forever. Anything with a Close owes its gauges an Unregister.
func (b *Builder) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) metric.Int64ObservableGauge {
	inst, err := b.meter.Int64ObservableGauge(name, opts...)
	if err != nil {
		b.record(err)
		inst, _ = metricnoop.Meter{}.Int64ObservableGauge(name)
	}
	return inst
}

func (b *Builder) record(err error) {
	if b.err == nil {
		b.err = err
	}
}
