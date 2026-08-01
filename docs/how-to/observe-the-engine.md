# Observe the engine

Wire OpenTelemetry to the engine so pushes, pulls, storage transactions and
background work show up in your traces and metrics.

For what each instrument means, see [Telemetry](../reference/telemetry.md).

## What you need

The engine depends only on the OpenTelemetry **API**. To get anything out of
it, your `main` supplies the **SDK** and an exporter:

```sh
go get go.opentelemetry.io/otel/sdk \
       go.opentelemetry.io/otel/sdk/metric \
       go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp \
       go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp
```

Nothing else changes if you skip this: the API's no-op implementation is what
runs, and the engine costs an interface call per instrument.

## Set up the providers

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

Call `shutdown` before the process exits, or the last batch never leaves.

## Wire the engine

Setting the globals is enough for the server. Two things need more:

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

Effects and connectors take providers on their config:

```go
rt, err := effects.New(effects.Config{
	Server:         srv,
	TracerProvider: tp,
	MeterProvider:  mp,
})
```

Both default to the globals, so with `otel.Set*Provider` called you can leave
them out.

## Trace context from clients

`otelconnect` extracts W3C trace context from incoming requests, so a client
that sends `traceparent` produces one trace spanning both sides. Nothing needs
enabling — if the header arrives, the server's spans are children of the
client's.

The TypeScript client does not generate trace context (it takes no
OpenTelemetry dependency; see [Telemetry](../reference/telemetry.md#the-typescript-client)).
Attach the header in your own Connect interceptor if you want end-to-end
traces.

## What to alert on

Four signals cover most of what goes wrong and are invisible without this:

- **`syncplane.push.rejections`** — a rejected mutation is still a `200 OK`, so
  a client wedged in a rebase loop is invisible in every RPC-level metric.
- **`syncplane.pull.resets`** with `reset_reason=cursor_below_horizon` — clients
  are falling behind GC and re-downloading whole workspaces. Raise `WithGC`
  retention.
- **`syncplane.effects.lag`** — a consumer that stopped keeping up. Pair it with
  `syncplane.effects.passes{outcome=error}` to tell "wedged" from "busy".
- **`syncplane.gc.collected{gc_kind=stranded_consumers}`** — an effect consumer
  was collected below the horizon. What it missed cannot be recovered.

For tuning rather than alerting, compare `syncplane.notify.requested` against
`syncplane.notify.published`: the gap is what `WithNotifyCoalescing` absorbed.

## Testing your instrumentation

Pass a provider explicitly and collect on demand, rather than reaching for the
globals — that way parallel tests cannot see each other's measurements:

```go
reader := sdkmetric.NewManualReader()
srv, err := server.New(store,
	server.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))

// ... exercise the server ...

var rm metricdata.ResourceMetrics
err = reader.Collect(ctx, &rm)
```

A `ManualReader` can be registered with exactly **one** `MeterProvider`. A
second registration is refused with a log line rather than an error, leaving a
meter that silently records nothing — so build a fresh reader per provider.
