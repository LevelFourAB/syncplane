package connect

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
)

// Instrument names, declared together because they are the contract a
// dashboard is written against.
const (
	metricPasses       = "syncplane.connect.passes"
	metricPassDuration = "syncplane.connect.pass.duration"
	metricTracked      = "syncplane.connect.tracked"
)

// metrics is the connector's instrument set, built once in New.
//
// There is deliberately no per-workspace health metric here. A connector's
// interesting state — which account's token expired — is per workspace, and
// workspace is the engine's unbounded partition key, so exporting it as a
// metric attribute would mint one time series per connected account. That
// state is what Config.OnHealth and Connector.HealthAll exist for: they are
// indexed by workspace and cost nothing to hold. What belongs here is the
// aggregate the enum bounds — how many passes ran, and how they ended.
type metrics struct {
	tracer trace.Tracer
	// passes counts convergence passes by outcome and failure kind. A rising
	// user_action share is the signal a product surfaces as "reconnect your
	// account" rather than retries silently forever.
	passes metric.Int64Counter
	// passDuration times one pass end to end, source acquisition included.
	passDuration metric.Float64Histogram
	// tracked is how many workspaces the connector holds a schedule record
	// for. It is the number of accounts a process is converging, and the
	// denominator for whether Config.Workers is enough of them at once.
	tracked metric.Int64ObservableGauge

	reg metric.Registration
}

func newMetrics(p telemetry.Providers) (*metrics, error) {
	b := telemetry.NewBuilder(p)
	m := &metrics{
		tracer: p.Tracer(),
		passes: b.Int64Counter(metricPasses,
			metric.WithDescription("Connector convergence passes, by outcome and failure kind."),
			metric.WithUnit("{pass}")),
		passDuration: b.Float64Histogram(metricPassDuration,
			metric.WithDescription("Duration of one connector convergence pass."),
			metric.WithUnit("s")),
		tracked: b.Int64ObservableGauge(metricTracked,
			metric.WithDescription("Workspaces the connector is holding schedule state for."),
			metric.WithUnit("{workspace}")),
	}
	return m, b.Err()
}

// observe registers the callback that samples c's state at collection time.
// The size of the managed set is a fact about the world rather than an event,
// so it is read when someone asks rather than pushed on every pass.
func (m *metrics) observe(p telemetry.Providers, c *Connector) error {
	reg, err := p.Meter().RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(m.tracked, int64(c.Tracked()))
			return nil
		},
		m.tracked,
	)
	if err != nil {
		return err
	}
	m.reg = reg
	return nil
}

// close stops the sampling callback, releasing the meter provider's reference
// to the connector — without it a closed connector would be sampled forever.
// Safe on a connector whose registration never happened.
func (m *metrics) close() {
	if m.reg != nil {
		_ = m.reg.Unregister()
		m.reg = nil
	}
}
