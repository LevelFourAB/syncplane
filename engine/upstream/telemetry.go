package upstream

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
)

// Instrument names, declared together because they are the contract a
// dashboard is written against.
const (
	metricPushes     = "syncplane.upstream.pushes"
	metricPages      = "syncplane.upstream.pages"
	metricRejections = "syncplane.upstream.rejections"
	metricPending    = "syncplane.upstream.pending"
	metricBootstraps = "syncplane.upstream.bootstraps"
)

// metrics is the link's instrument set. Per-workspace state is not a metric
// (workspace ids are unbounded); Status and Config.OnStatus carry it.
type metrics struct {
	providers  telemetry.Providers
	tracer     trace.Tracer
	pushes     metric.Int64Counter
	pages      metric.Int64Counter
	rejections metric.Int64Counter
	bootstraps metric.Int64Counter
	pending    metric.Int64ObservableGauge
	reg        metric.Registration
}

func newMetrics(p telemetry.Providers) (*metrics, error) {
	b := telemetry.NewBuilder(p)
	m := &metrics{
		providers: p,
		tracer:    p.Tracer(),
		pushes: b.Int64Counter(metricPushes,
			metric.WithDescription("Pushes of local writes to the origin, by outcome."),
			metric.WithUnit("{push}")),
		pages: b.Int64Counter(metricPages,
			metric.WithDescription("Origin change pages applied to the local engine, by outcome."),
			metric.WithUnit("{page}")),
		rejections: b.Int64Counter(metricRejections,
			metric.WithDescription("Local writes the origin rejected, by rejection code."),
			metric.WithUnit("{mutation}")),
		bootstraps: b.Int64Counter(metricBootstraps,
			metric.WithDescription("Snapshots of the origin applied to the local engine, by outcome."),
			metric.WithUnit("{bootstrap}")),
		pending: b.Int64ObservableGauge(metricPending,
			metric.WithDescription("Local writes the origin has not confirmed, over every linked workspace."),
			metric.WithUnit("{mutation}")),
	}
	return m, b.Err()
}

// observe samples the pending count at collection time.
func (m *metrics) observe(l *Link) {
	reg, err := m.providers.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var n int64
		for _, id := range l.Workspaces() {
			if st, ok := l.Status(id); ok {
				n += int64(st.Pending)
			}
		}
		o.ObserveInt64(m.pending, n)
		return nil
	}, m.pending)
	if err == nil {
		m.reg = reg
	}
}

func (m *metrics) close() {
	if m.reg != nil {
		_ = m.reg.Unregister()
		m.reg = nil
	}
}

func (m *metrics) count(ctx context.Context, c metric.Int64Counter, err error) {
	c.Add(ctx, 1, metric.WithAttributes(telemetry.AttrOutcome.String(telemetry.Classify(err))))
}

func (m *metrics) rejected(ctx context.Context, code string) {
	m.rejections.Add(ctx, 1, metric.WithAttributes(telemetry.AttrRejectionCode.String(code)))
}

func (m *metrics) span(ctx context.Context, name, ws string) (context.Context, trace.Span) {
	return m.tracer.Start(ctx, name, trace.WithAttributes(attribute.String(string(telemetry.AttrWorkspace), ws)))
}
