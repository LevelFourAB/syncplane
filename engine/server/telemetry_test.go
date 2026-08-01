package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// Instrument names and attribute keys are a compatibility surface: dashboards
// and alerts are written against them, so a rename breaks a deployment the
// same way an API change does. These specs pin the ones that carry the signals
// nothing else in the system reports — a rejected mutation (which the RPC layer
// sees as a success) and a forced re-bootstrap.
var _ = Describe("Telemetry", func() {
	var (
		reader *sdkmetric.ManualReader
		srv    *server.Server
	)

	// meterOption installs a FRESH manual reader and returns the option wiring a
	// provider to it, rebinding `reader` so collect() below reads the right one.
	//
	// Fresh every time: a ManualReader can be registered with exactly one
	// MeterProvider, and a second registration is refused with a log line rather
	// than an error — leaving a meter that silently records nothing. A spec that
	// needs its own server options must therefore take a new reader with it, not
	// reuse the one BeforeEach made.
	meterOption := func() server.Option {
		reader = sdkmetric.NewManualReader()
		return server.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	}

	BeforeEach(func() {
		// A manual reader collects on demand, so specs assert on exactly the
		// operations they performed with no background collection interval to
		// race against. Provided explicitly rather than through the global
		// provider so parallel specs cannot see each other's measurements.
		srv = newTestServer(meterOption())
	})

	// collect returns the recorded data points of one instrument, by name.
	collect := func(name string) []metricdata.DataPoint[int64] {
		GinkgoHelper()
		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(context.Background(), &rm)).To(Succeed())
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != name {
					continue
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				Expect(ok).To(BeTrue(), "%s is not an int64 sum", name)
				return sum.DataPoints
			}
		}
		return nil
	}

	// attrsOf finds the single data point carrying every wanted attribute.
	pointWith := func(points []metricdata.DataPoint[int64], want ...attribute.KeyValue) *metricdata.DataPoint[int64] {
		for i, p := range points {
			all := true
			for _, kv := range want {
				if v, ok := p.Attributes.Value(kv.Key); !ok || v != kv.Value {
					all = false
					break
				}
			}
			if all {
				return &points[i]
			}
		}
		return nil
	}

	It("counts applied mutations by entity", func() {
		_, err := srv.Push(context.Background(), connect.NewRequest(pushOf("ws1", []*syncplanev1.Mutation{
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
		})))
		Expect(err).NotTo(HaveOccurred())

		points := collect("syncplane.push.mutations")
		p := pointWith(points,
			attribute.String("syncplane.entity", "widget"),
			attribute.String("syncplane.outcome", "ok"))
		Expect(p).NotTo(BeNil(), "no applied-widget point in %v", points)
		Expect(p.Value).To(Equal(int64(1)))
	})

	It("counts a rejected mutation by rejection code, not as a success", func() {
		// A rejection is an ack: the RPC returns 200 and the watermark advances,
		// so this counter is the only place the refusal is visible.
		srv := newTestServer(meterOption(), server.WithPermissions(denyWrites{}))
		resp, err := srv.Push(context.Background(), connect.NewRequest(pushOf("ws1", []*syncplanev1.Mutation{
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
		})))
		Expect(err).NotTo(HaveOccurred(), "a rejection must not surface as an RPC error")
		Expect(pushResults(resp.Msg)[0].GetStatus()).
			To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))

		p := pointWith(collect("syncplane.push.rejections"),
			attribute.String("syncplane.entity", "widget"),
			attribute.String("syncplane.rejection_code", server.CodePermissionDenied))
		Expect(p).NotTo(BeNil())
		Expect(p.Value).To(Equal(int64(1)))
		// The same mutation must not also read as applied.
		Expect(pointWith(collect("syncplane.push.mutations"),
			attribute.String("syncplane.outcome", "ok"))).To(BeNil())
	})

	It("does not let an unregistered entity name become a metric attribute", func() {
		// Entity names come off the wire. Echoing one back would let any client
		// mint unbounded metric streams by pushing garbage — the requests are
		// all rejected, and the metrics pipeline falls over anyway.
		_, err := srv.Push(context.Background(), connect.NewRequest(pushOf("ws1", []*syncplanev1.Mutation{
			putMutation("c1", 1, "NoSuchEntity", "k1", &testdatav1.Widget{Id: "w1"}),
		})))
		Expect(err).NotTo(HaveOccurred())

		points := collect("syncplane.push.rejections")
		Expect(pointWith(points, attribute.String("syncplane.entity", "NoSuchEntity"))).
			To(BeNil(), "the requested entity name leaked into a metric attribute")
		Expect(pointWith(points, attribute.String("syncplane.entity", "_unregistered"))).
			NotTo(BeNil(), "the rejection was not counted at all")
	})

	It("keeps the unbounded workspace and client keys off every metric", func() {
		// The cardinality rule, asserted rather than merely documented: these
		// two grow with the deployment and belong on spans only.
		_, err := srv.Push(context.Background(), connect.NewRequest(pushOf("ws1", []*syncplanev1.Mutation{
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
		})))
		Expect(err).NotTo(HaveOccurred())

		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(context.Background(), &rm)).To(Succeed())
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				for _, set := range attributeSets(m) {
					for _, banned := range []attribute.Key{"syncplane.workspace_id", "syncplane.client_id"} {
						_, found := set.Value(banned)
						Expect(found).To(BeFalse(), "%s carries %s", m.Name, banned)
					}
				}
			}
		}
	})
})

// attributeSets pulls the attribute set off every data point of a metric,
// whatever aggregation it uses.
func attributeSets(m metricdata.Metrics) []attribute.Set {
	var out []attribute.Set
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, p := range d.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, p := range d.DataPoints {
			out = append(out, p.Attributes)
		}
	}
	return out
}

// denyWrites refuses every mutation, so a push comes back as a rejection ack.
type denyWrites struct{ server.AllowAll }

func (denyWrites) CanMutate(context.Context, server.Principal, string, server.Write) error {
	return server.Reject(server.CodePermissionDenied, "no writes in this spec")
}
