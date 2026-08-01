// Package otelstore instruments any storage.Adapter with OpenTelemetry.
//
// It wraps the transaction seam rather than any one implementation, so a
// third-party adapter gets the same timings as sqlitestore without writing
// them itself, and so the numbers mean the same thing across adapters:
// everything the caller waited for, including whatever the adapter did
// internally to serve the request.
//
// It lives in its own package so that storage, which every adapter
// implementation imports, stays free of the OpenTelemetry dependency.
//
//	store, err := sqlitestore.Open(path)
//	…
//	instrumented, err := otelstore.Wrap(store)
//	…
//	srv, err := server.New(instrumented, …)
//
// Wrap once, at the root. A Manager namespaces the root adapter per app and
// the wrapper carries through Namespace, so every app is instrumented by the
// single call — wrapping again per app would double-count.
package otelstore

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Instrument names, declared together because they are the contract a
// dashboard is written against.
const metricTxDuration = "syncplane.storage.tx.duration"

// Option configures Wrap.
type Option func(*config)

type config struct {
	providers telemetry.Providers
}

// WithTracerProvider sets the tracer provider (default: the global one, no-op
// until an SDK is installed).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) { c.providers.TracerProvider = tp }
}

// WithMeterProvider sets the meter provider (default: the global one, no-op
// until an SDK is installed).
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) { c.providers.MeterProvider = mp }
}

// Wrap returns inner instrumented with transaction spans and timings.
//
// Wrap does not take ownership: Close forwards to inner, exactly as every
// other method does, so the usual "the caller closes the store" rule is
// unchanged.
func Wrap(inner storage.Adapter, opts ...Option) (storage.Adapter, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	b := telemetry.NewBuilder(cfg.providers)
	mx := &metrics{
		tracer: cfg.providers.Tracer(),
		txDuration: b.Float64Histogram(metricTxDuration,
			metric.WithDescription("Duration of a storage transaction, from the caller's point of view."),
			metric.WithUnit("s")),
	}
	if err := b.Err(); err != nil {
		return nil, err
	}
	return &store{Adapter: inner, mx: mx}, nil
}

type metrics struct {
	tracer     trace.Tracer
	txDuration metric.Float64Histogram
}

// store instruments the transaction seam and forwards everything else.
//
// The embedded interface is deliberate. storage.Adapter carries schema
// registration, workspace listing and consumer paging alongside the two
// transaction methods, and it grows as the engine does; embedding means a
// method added to the interface keeps compiling and keeps working here
// untouched, instead of turning every extension into an edit in this package.
// The trade is that a new method is uninstrumented until someone overrides it,
// which is the right default — a method nobody has thought about the
// semantics of is better forwarded verbatim than timed wrongly.
type store struct {
	storage.Adapter
	mx *metrics
}

// Namespace keeps the wrapper on the namespaced child, so a Manager hosting
// many apps over one root adapter instruments all of them from the single
// Wrap at the root.
func (s *store) Namespace(name string) storage.Adapter {
	return &store{Adapter: s.Adapter.Namespace(name), mx: s.mx}
}

// WithReadTx times the read transaction as a whole.
func (s *store) WithReadTx(ctx context.Context, workspaceID string, fn func(storage.ReadTx) error) error {
	return s.observe(ctx, workspaceID, "read", func(ctx context.Context) error {
		return s.Adapter.WithReadTx(ctx, workspaceID, fn)
	})
}

// WithWriteTx times the write transaction as a whole — which is what the
// caller waited for, retries of the closure included. The engine's throughput
// ceiling is how long a workspace's write lock is held, so this is the
// contention signal: a write duration that climbs while push volume does not
// is queueing, not work.
func (s *store) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	return s.observe(ctx, workspaceID, "write", func(ctx context.Context) error {
		return s.Adapter.WithWriteTx(ctx, workspaceID, fn)
	})
}

func (s *store) observe(ctx context.Context, workspaceID, kind string, run func(context.Context) error) error {
	ctx, span := s.mx.tracer.Start(ctx, "syncplane.storage.tx."+kind,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			telemetry.AttrTxKind.String(kind),
			telemetry.AttrWorkspace.String(workspaceID),
		))
	start := time.Now()
	err := run(ctx)
	// Workspace is on the span but never on the metric: it is the engine's
	// partition key and grows with the deployment.
	s.mx.txDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
		telemetry.AttrTxKind.String(kind),
		telemetry.AttrOutcome.String(telemetry.Classify(err)),
	))
	telemetry.EndSpan(span, err)
	return err
}

// compile-time guard: the wrapper must stay a complete Adapter even as the
// interface grows.
var _ storage.Adapter = (*store)(nil)
