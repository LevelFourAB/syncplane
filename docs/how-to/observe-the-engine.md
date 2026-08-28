# Observing the engine

This guide shows you how to connect OpenTelemetry to the engine so pushes, pulls, storage transactions, and background work appear in your traces and metrics.

For descriptions of each instrument, see [Telemetry](../reference/telemetry.md).

## Prerequisites

Before you begin, install the OpenTelemetry Go SDK and exporter packages:

```sh
go get go.opentelemetry.io/otel/sdk \
       go.opentelemetry.io/otel/sdk/metric \
       go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp \
       go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp
```

The engine depends only on the OpenTelemetry API. If you do not configure an SDK, the engine runs the no-op implementation.

## Setting up telemetry

Follow these steps to configure OpenTelemetry in your application:

1. Initialize the OpenTelemetry providers and register them as globals:

   ```go
   import (
   	"go.opentelemetry.io/otel"
   	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
   	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
   	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
   	"go.opentelemetry.io/otel/sdk/resource"
   	sdktrace "go.opentelemetry.io/otel/sdk/trace"
   	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
   )

   func setupTelemetry(ctx context.Context) (func(context.Context) error, error) {
   	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
   		semconv.SchemaURL, semconv.ServiceName("my-sync-server")))
   	if err != nil {
   		return nil, err
   	}

   	traceExp, err := otlptracehttp.New(ctx)   // honours OTEL_EXPORTER_OTLP_ENDPOINT
   	if err != nil {
   		return nil, err
   	}
   	metricExp, err := otlpmetrichttp.New(ctx)
   	if err != nil {
   		return nil, err
   	}

   	tp := sdktrace.NewTracerProvider(
   		sdktrace.WithResource(res),
   		sdktrace.WithBatcher(traceExp),
   	)
   	mp := sdkmetric.NewMeterProvider(
   		sdkmetric.WithResource(res),
   		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
   	)

   	// Setting the globals means anything that was not handed a provider
   	// explicitly still reports. The engine falls back to these.
   	otel.SetTracerProvider(tp)
   	otel.SetMeterProvider(mp)

   	return func(ctx context.Context) error {
   		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
   	}, nil
   }
   ```

2. Wrap the storage adapter and initialize the server:

   Wrap your storage adapter once at the root with `otelstore.Wrap` before passing it to `server.New`. A manager namespaces the root adapter per application and carries the wrapper through, so wrapping per application causes double-counting.

   ```go
   // Storage is instrumented by wrapping the adapter — so any adapter gets it,
   // not just sqlitestore. Wrap ONCE, at the root: a Manager namespaces the root
   // adapter per app and the wrapper carries through, so wrapping per app would
   // double-count.
   store, err := sqlitestore.Open(path)
   if err != nil { ... }
   defer store.Close()

   instrumented, err := otelstore.Wrap(store)
   if err != nil { ... }

   srv, err := server.New(instrumented,
   	server.WithEntities(&todov1.Todo{}),
   	// Optional: pass providers explicitly instead of relying on the globals.
   	server.WithTracerProvider(tp),
   	server.WithMeterProvider(mp),
   )
   ```

3. Configure background workers:

   If you use effects or connectors, pass the providers in their configuration:

   ```go
   rt, err := effects.New(effects.Config{
   	Server:         srv,
   	TracerProvider: tp,
   	MeterProvider:  mp,
   })
   ```

   Both components fall back to the global providers if you omit these fields.

4. Shut down the providers on exit:

   Call the `shutdown` function before the process exits. If you omit shutdown, the final batch of telemetry is not sent.

## Confirming the result

To verify your instrumentation in automated tests, pass a manual reader explicitly instead of using global providers. This isolates parallel tests from each other:

```go
reader := sdkmetric.NewManualReader()
srv, err := server.New(store,
	server.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))

// ... exercise the server ...

var rm metricdata.ResourceMetrics
err = reader.Collect(ctx, &rm)
```

**Note:** Register a `ManualReader` with only one `MeterProvider`. A second registration is rejected with a log line rather than an error, leaving a meter that records nothing. Always build a fresh reader per provider.
