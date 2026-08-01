package server_test

import (
	"context"
	"errors"
	"path/filepath"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("ViewTx.Query", func() {
	const ws = "ws-view-query"
	var (
		ctx context.Context
		srv *server.Server
	)

	put := func(widgets ...*testdatav1.Widget) {
		GinkgoHelper()
		for _, w := range widgets {
			w := w
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(w)
			})).To(Succeed())
		}
	}

	// run collects the ids the spec selects, in the order they arrive.
	run := func(spec query.Spec) []string {
		GinkgoHelper()
		var ids []string
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			return tx.Query(&testdatav1.Widget{}, spec, func(msg proto.Message, _ uint64) error {
				ids = append(ids, msg.(*testdatav1.Widget).GetId())
				return nil
			})
		})).To(Succeed())
		return ids
	}

	BeforeEach(func() {
		ctx = context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "view-query.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		srv = mustNewServer(store, server.WithEntities(&testdatav1.Widget{}))

		put(
			&testdatav1.Widget{Id: "a", Label: "alpha", Count: 3, Tags: []string{"x", "y"}},
			&testdatav1.Widget{Id: "b", Label: "beta", Count: 1, Tags: []string{"y"}},
			&testdatav1.Widget{Id: "c", Label: "gamma", Count: 2, Flag: true},
		)
	})

	It("returns every live row when the spec is empty", func() {
		Expect(run(query.Spec{})).To(Equal([]string{"a", "b", "c"}))
	})

	It("filters on a compiled predicate", func() {
		Expect(run(query.Spec{Where: query.Gt("count", 1)})).To(Equal([]string{"a", "c"}))
	})

	It("composes boolean conditions", func() {
		spec := query.Spec{Where: query.And(
			query.Gte("count", 2),
			query.Or(query.Contains("label", "amm"), query.Eq("flag", true)),
		)}
		Expect(run(spec)).To(Equal([]string{"c"}))
	})

	It("matches repeated fields by membership", func() {
		Expect(run(query.Spec{Where: query.Has("tags", "y")})).To(Equal([]string{"a", "b"}))
		Expect(run(query.Spec{Where: query.IsEmpty("tags")})).To(Equal([]string{"c"}))
	})

	It("orders by a field, descending", func() {
		spec := query.Spec{OrderBy: []*syncplanev1.OrderKey{query.Desc("count")}}
		Expect(run(spec)).To(Equal([]string{"a", "c", "b"}))
	})

	It("pages an ordering with limit and offset", func() {
		order := []*syncplanev1.OrderKey{query.Asc("count")}
		Expect(run(query.Spec{OrderBy: order, Limit: 2})).To(Equal([]string{"b", "c"}))
		Expect(run(query.Spec{OrderBy: order, Limit: 2, Offset: 1})).To(Equal([]string{"c", "a"}))
		Expect(run(query.Spec{OrderBy: order, Limit: 2, Offset: 2})).To(Equal([]string{"a"}))
		Expect(run(query.Spec{OrderBy: order, Offset: 5})).To(BeEmpty())
	})

	It("hands back the version a result was written at, for a CAS write", func() {
		var got *testdatav1.Widget
		var version uint64
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			return tx.Query(&testdatav1.Widget{}, query.Spec{Where: query.Eq("label", "beta")},
				func(msg proto.Message, v uint64) error {
					got, version = msg.(*testdatav1.Widget), v
					return nil
				})
		})).To(Succeed())
		Expect(got.GetId()).To(Equal("b"))

		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "b", Label: "beta-2"}, []string{"label"}, &version)
		})).To(Succeed())

		// The same version is now stale, so a second CAS on it must fail.
		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "b", Label: "beta-3"}, []string{"label"}, &version)
		})
		Expect(err).To(HaveOccurred())
	})

	It("skips deleted rows", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Delete(&testdatav1.Widget{}, "b")
		})).To(Succeed())
		Expect(run(query.Spec{})).To(Equal([]string{"a", "c"}))
	})

	It("surfaces a compile error rather than matching nothing", func() {
		err := srv.View(ctx, ws, func(tx *server.ViewTx) error {
			return tx.Query(&testdatav1.Widget{}, query.Spec{Where: query.Eq("nope", 1)},
				func(proto.Message, uint64) error { return nil })
		})
		// engine/query writes for the wire; on the Go path the error is wrapped
		// so the developer sees which layer and which entity it came from.
		Expect(err).To(MatchError(ContainSubstring(`server: query widget: `)))
		Expect(err).To(MatchError(ContainSubstring(`no field "nope"`)))
	})

	It("rejects an unregistered entity", func() {
		err := srv.View(ctx, ws, func(tx *server.ViewTx) error {
			return tx.Query(&testdatav1.Project{}, query.Spec{},
				func(proto.Message, uint64) error { return nil })
		})
		Expect(err).To(MatchError(ContainSubstring(
			"is not a registered entity; register it first with server.WithEntities(&Project{})")))
	})

	It("stops and surfaces an error from the callback", func() {
		boom := errors.New("boom")
		var seen int
		err := srv.View(ctx, ws, func(tx *server.ViewTx) error {
			return tx.Query(&testdatav1.Widget{}, query.Spec{}, func(proto.Message, uint64) error {
				seen++
				return boom
			})
		})
		Expect(err).To(MatchError(boom))
		Expect(seen).To(Equal(1))
	})

	It("finds rows to update from inside a mutation", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			var stale []*testdatav1.Widget
			if err := tx.Query(&testdatav1.Widget{}, query.Spec{Where: query.Lt("count", 3)},
				func(msg proto.Message) error {
					stale = append(stale, msg.(*testdatav1.Widget))
					return nil
				}); err != nil {
				return err
			}
			for _, w := range stale {
				w.Flag = true
				if err := tx.Put(w); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())

		Expect(run(query.Spec{Where: query.Eq("flag", true)})).To(Equal([]string{"b", "c"}))
	})

	It("evaluates a predicate exactly as the Query RPC does", func() {
		// The point of one shared compiler: the same condition must select the
		// same rows whether it arrives over the wire or is built in-process.
		predicate := query.And(query.Gte("count", 2), query.Contains("label", "a"))
		order := []*syncplanev1.OrderKey{query.Desc("count")}

		resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "widget",
			Predicate:   predicate,
			OrderBy:     order,
		}))
		Expect(err).NotTo(HaveOccurred())
		viaRPC := make([]string, 0, len(resp.Msg.GetRows()))
		for _, r := range resp.Msg.GetRows() {
			viaRPC = append(viaRPC, r.GetKey())
		}

		Expect(run(query.Spec{Where: predicate, OrderBy: order})).To(Equal(viaRPC))
	})
})
