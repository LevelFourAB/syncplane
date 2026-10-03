package server_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// An integer literal outside an int64 or uint64 column's range is equal to no
// stored value, so no index entry can bound it. Each read runs against the same
// rows twice, once with the indexes built and once with them disabled, and both
// must return the scan's rows. A bound wrapped into the column's range drops
// rows from the indexed run only.
var _ = Describe("Indexed reads with a literal outside a 64-bit column's range", func() {
	counterSpec := server.DynamicEntity{
		Name:      "counter",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "big", Number: 2, Kind: protoreflect.Int64Kind},
			{Name: "ubig", Number: 3, Kind: protoreflect.Uint64Kind},
		},
		Indexes: []server.DynamicIndex{
			{Name: "by_big", Key: []server.DynamicIndexKey{{Path: "big"}}},
			{Name: "by_ubig", Key: []server.DynamicIndexKey{{Path: "ubig"}}},
		},
	}

	// serve starts a server that holds rows at both ends of each column's
	// range and returns a client for it.
	serve := func(built bool) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		ctx := context.Background()
		var opts []server.Option
		if !built {
			opts = append(opts, server.WithLimits(server.Limits{DisableIndexes: true}))
		}
		srv := newTestServer(opts...)
		Expect(srv.RegisterDynamicEntity(counterSpec)).To(Succeed())
		mt, err := server.BuildDynamicMessageType(counterSpec)
		Expect(err).NotTo(HaveOccurred())

		var muts []*syncplanev1.Mutation
		for i, r := range []struct {
			id   string
			big  int64
			ubig uint64
		}{
			{"r1", math.MinInt64, 0},
			{"r2", -1, 1},
			{"r3", 5, 1 << 63},
			{"r4", math.MaxInt64, math.MaxUint64},
		} {
			row := buildNote(mt, map[string]any{"id": r.id, "big": r.big, "ubig": r.ubig})
			muts = append(muts, putMutation("c1", uint64(i+1), "counter", r.id, row))
		}
		_, err = srv.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		if built {
			Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		}

		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	queryKeys := func(c syncplanev1connect.SyncServiceClient, cond *syncplanev1.Condition) []string {
		GinkgoHelper()
		resp, err := c.Query(context.Background(), connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "counter",
			Predicate:   cond,
		}))
		Expect(err).NotTo(HaveOccurred())
		keys := []string{}
		for _, r := range resp.Msg.GetRows() {
			keys = append(keys, r.GetKey())
		}
		sort.Strings(keys)
		return keys
	}

	// bootstrapKeys is the partial sync a client starts with a subscription
	// that carries cond.
	bootstrapKeys := func(c syncplanev1connect.SyncServiceClient, cond *syncplanev1.Condition) []string {
		GinkgoHelper()
		stream, err := c.Bootstrap(context.Background(), connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId:   ws,
			ClientId:      "c2",
			Subscriptions: []*syncplanev1.Subscription{{Entity: "counter", Predicate: cond}},
		}))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(stream.Close()).To(Succeed()) }()
		keys := []string{}
		for stream.Receive() {
			for _, rc := range stream.Msg().GetChunk().GetRows() {
				if rc.GetEntity() == "counter" && !rc.GetTombstone() {
					keys = append(keys, rc.GetKey())
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		sort.Strings(keys)
		return keys
	}

	cmp := func(path string, op syncplanev1.CmpOp, v *syncplanev1.QueryValue) *syncplanev1.Condition {
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
			Path: []string{path}, Op: op, Value: v,
		}}}
	}
	intValue := func(v int64) *syncplanev1.QueryValue {
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: v}}
	}
	uintValue := func(v uint64) *syncplanev1.QueryValue {
		return &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_UintValue{UintValue: v}}
	}

	DescribeTable("returns the scan's rows from the index",
		func(cond *syncplanev1.Condition, want []string) {
			scanning, indexed := serve(false), serve(true)
			Expect(queryKeys(scanning, cond)).To(Equal(want), "the scan is the definition")
			Expect(queryKeys(indexed, cond)).To(Equal(want), "Query")
			Expect(bootstrapKeys(scanning, cond)).To(Equal(want), "the scanned bootstrap")
			Expect(bootstrapKeys(indexed, cond)).To(Equal(want), "the narrowed bootstrap")
		},
		Entry("a negative literal against uint64",
			cmp("ubig", syncplanev1.CmpOp_CMP_OP_GT, intValue(-1)),
			[]string{"r1", "r2", "r3", "r4"}),
		Entry("an equality below the uint64 range",
			cmp("ubig", syncplanev1.CmpOp_CMP_OP_EQ, intValue(-1)),
			[]string{}),
		Entry("a literal above the int64 range",
			cmp("big", syncplanev1.CmpOp_CMP_OP_LT, uintValue(1<<63)),
			[]string{"r1", "r2", "r3", "r4"}),
		Entry("a literal at the edge of the uint64 range",
			cmp("ubig", syncplanev1.CmpOp_CMP_OP_GTE, uintValue(1<<63)),
			[]string{"r3", "r4"}),
	)
})
