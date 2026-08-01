package effects

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
)

// Instrument names, declared together because they are the contract a
// dashboard is written against.
const (
	metricPasses       = "syncplane.effects.passes"
	metricPassDuration = "syncplane.effects.pass.duration"
	metricChanges      = "syncplane.effects.changes"
	metricLag          = "syncplane.effects.lag"
	metricTracked      = "syncplane.effects.tracked"
)

// metrics is the runtime's instrument set, built once in New.
//
// The consumer name is the only identity on any of these. It is declared in
// code by Register, so it is bounded; the workspace is not, and a runtime is
// expected to carry a consumer across every workspace in the store. That is
// why lag is reported as a MAXIMUM over workspaces rather than per workspace:
// the operational question is "is any workspace falling behind", which a max
// answers exactly, and which a per-workspace series would answer at the cost
// of one time series per workspace per consumer. Which workspace it is comes
// from HealthAll.
type metrics struct {
	tracer trace.Tracer
	// passes counts drain passes by consumer and outcome. A consumer whose
	// error rate is 100% and whose lag is climbing is wedged; without this it
	// looks exactly like one with nothing to do, because both deliver nothing.
	passes metric.Int64Counter
	// passDuration times one drain pass, sink I/O included.
	passDuration metric.Float64Histogram
	// changes counts changes actually handed to a sink.
	changes metric.Int64Counter
	// lag is the largest Head-Position across a consumer's workspaces: how
	// many sequences behind the furthest-behind workspace is. It is the number
	// that keeps growing while a consumer is stuck.
	lag metric.Int64ObservableGauge
	// tracked is the resident-set size — pairs with work queued, a pass
	// running, or a failure being retried. Flat as the store grows, climbing
	// when work arrives faster than the workers drain it.
	tracked metric.Int64ObservableGauge
	// reg is the observable-gauge callback registration, unregistered by Close
	// so a closed runtime stops being sampled and can be collected. Without
	// this the meter provider would hold the Runtime alive forever and keep
	// reporting a dead runtime's last known lag.
	reg metric.Registration
}

func newMetrics(p telemetry.Providers) (*metrics, error) {
	b := telemetry.NewBuilder(p)
	m := &metrics{
		tracer: p.Tracer(),
		passes: b.Int64Counter(metricPasses,
			metric.WithDescription("Effect drain passes, by consumer and outcome."),
			metric.WithUnit("{pass}")),
		passDuration: b.Float64Histogram(metricPassDuration,
			metric.WithDescription("Duration of one effect drain pass, sink I/O included."),
			metric.WithUnit("s")),
		changes: b.Int64Counter(metricChanges,
			metric.WithDescription("Changes delivered to effect sinks."),
			metric.WithUnit("{change}")),
		lag: b.Int64ObservableGauge(metricLag,
			metric.WithDescription("Largest feed lag across a consumer's workspaces, in sequences."),
			metric.WithUnit("{sequence}")),
		tracked: b.Int64ObservableGauge(metricTracked,
			metric.WithDescription("Consumer/workspace pairs the runtime is holding state for."),
			metric.WithUnit("{pair}")),
	}
	return m, b.Err()
}

// observe registers the callback that samples rt's state at collection time.
// Lag and resident-set size are facts about the world rather than events, so
// they are read when someone asks rather than pushed on every pass — which
// also means a consumer that has stopped running entirely still reports the
// lag it is stuck at.
func (m *metrics) observe(p telemetry.Providers, rt *Runtime) error {
	reg, err := p.Meter().RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			// Max lag per consumer, so the series count is bounded by the
			// number of registered consumers rather than by the workspace
			// count.
			worst := map[string]uint64{}
			for _, h := range rt.HealthAll() {
				if lag := h.Lag(); lag > worst[h.Consumer] {
					worst[h.Consumer] = lag
				}
			}
			for consumer, lag := range worst {
				o.ObserveInt64(m.lag, int64(lag), metric.WithAttributes(
					telemetry.AttrConsumer.String(consumer)))
			}
			o.ObserveInt64(m.tracked, int64(rt.Tracked()))
			return nil
		},
		m.lag, m.tracked,
	)
	if err != nil {
		return err
	}
	m.reg = reg
	return nil
}

// close stops the sampling callback. Safe on a runtime whose registration
// never happened.
func (m *metrics) close() {
	if m.reg != nil {
		_ = m.reg.Unregister()
		m.reg = nil
	}
}
