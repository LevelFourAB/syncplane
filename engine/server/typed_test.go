package server_test

import (
	"context"
	"fmt"
	"iter"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Typed reads and writes", func() {
	var (
		ctx context.Context
		srv *server.Server
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv = newTestServer()
	})

	put := func(widgets ...*testdatav1.Widget) {
		GinkgoHelper()
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			for _, w := range widgets {
				if err := tx.Put(w); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
	}

	// manyWidgets returns n widgets keyed w0000…, enough to span several
	// pages of a scan.
	manyWidgets := func(n int) ([]*testdatav1.Widget, []string) {
		widgets := make([]*testdatav1.Widget, n)
		ids := make([]string, n)
		for i := range n {
			ids[i] = fmt.Sprintf("w%04d", i)
			widgets[i] = &testdatav1.Widget{Id: ids[i], Count: int32(i)}
		}
		return widgets, ids
	}

	// collect ranges seq to the end and returns the ids it yielded, failing
	// the spec on the first error.
	collect := func(seq func(yield func(*testdatav1.Widget, error) bool)) []string {
		GinkgoHelper()
		var ids []string
		for w, err := range seq {
			Expect(err).NotTo(HaveOccurred())
			ids = append(ids, w.GetId())
		}
		return ids
	}

	Describe("Scan", func() {
		It("yields every live row as the type argument, in key order, across pages", func() {
			// Given more widgets than one page holds, one of them deleted
			widgets, ids := manyWidgets(600)
			put(widgets...)
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return server.Delete[*testdatav1.Widget](tx, "w0300")
			})).To(Succeed())

			// When a View scans the entity
			var got []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				got = collect(server.Scan[*testdatav1.Widget](tx))
				return nil
			})).To(Succeed())

			// Then it yields every other widget, once each, in key order
			Expect(got).To(Equal(slices.Delete(slices.Clone(ids), 300, 301)))
		})

		It("stops when the loop breaks", func() {
			widgets, _ := manyWidgets(600)
			put(widgets...)

			var got []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				for w, err := range server.Scan[*testdatav1.Widget](tx) {
					if err != nil {
						return err
					}
					got = append(got, w.GetId())
					if len(got) == 3 {
						break
					}
				}
				return nil
			})).To(Succeed())

			Expect(got).To(Equal([]string{"w0000", "w0001", "w0002"}))
		})

		It("lets the loop body write each row it yields through an EntityTx", func() {
			// Given more widgets than one page holds
			widgets, ids := manyWidgets(600)
			put(widgets...)

			// When a Mutate bumps every widget from inside the loop
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				for w, err := range server.Scan[*testdatav1.Widget](tx) {
					if err != nil {
						return err
					}
					w.Label = "bumped"
					if err := tx.Put(w); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())

			// Then every widget carries the write
			var labels []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				for w, err := range server.Scan[*testdatav1.Widget](tx) {
					if err != nil {
						return err
					}
					labels = append(labels, w.GetLabel())
				}
				return nil
			})).To(Succeed())
			Expect(labels).To(HaveLen(len(ids)))
			Expect(labels).To(HaveEach("bumped"))
		})

		It("records a Delete for each row an ApplyTx yields", func() {
			put(&testdatav1.Widget{Id: "a"}, &testdatav1.Widget{Id: "b"}, &testdatav1.Widget{Id: "c"})

			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				for w, err := range server.Scan[*testdatav1.Widget](tx) {
					if err != nil {
						return err
					}
					if w.GetId() != "b" {
						if err := server.Delete[*testdatav1.Widget](tx, w.GetId()); err != nil {
							return err
						}
					}
				}
				return nil
			})).To(Succeed())

			var got []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				got = collect(server.Scan[*testdatav1.Widget](tx))
				return nil
			})).To(Succeed())
			Expect(got).To(Equal([]string{"b"}))
		})

		It("skips the rows the caller's read filter hides", func() {
			// Given a read filter that hides flagged widgets
			srv = newTestServer(server.WithPermissions(hideFlagged{}))
			put(&testdatav1.Widget{Id: "a"}, &testdatav1.Widget{Id: "b", Flag: true}, &testdatav1.Widget{Id: "c"})

			var got []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				got = collect(server.Scan[*testdatav1.Widget](tx))
				return nil
			})).To(Succeed())
			Expect(got).To(Equal([]string{"a", "c"}))
		})

		It("yields an error when ranged after its transaction ended", func() {
			put(&testdatav1.Widget{Id: "a"})

			var seq iter.Seq2[*testdatav1.Widget, error]
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				seq = server.Scan[*testdatav1.Widget](tx)
				return nil
			})).To(Succeed())

			var errs []error
			for _, err := range seq {
				errs = append(errs, err)
			}
			Expect(errs).To(HaveLen(1))
			Expect(errs[0]).To(HaveOccurred())
		})

		It("yields an error for a type argument that is not a registered entity", func() {
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				for _, err := range server.Scan[*testdatav1.Settings](tx) {
					return err
				}
				return nil
			})).To(MatchError(ContainSubstring("not a registered entity")))
		})

		It("yields an error for an interface type argument", func() {
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				for _, err := range server.Scan[proto.Message](tx) {
					return err
				}
				return nil
			})).To(MatchError(ContainSubstring("interface")))
		})
	})

	Describe("ScanRows", func() {
		It("yields each row with the version and actor Get reports", func() {
			put(&testdatav1.Widget{Id: "a"})
			put(&testdatav1.Widget{Id: "b"})

			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				for row, err := range server.ScanRows[*testdatav1.Widget](tx) {
					Expect(err).NotTo(HaveOccurred())
					found, meta, err := tx.GetMeta(&testdatav1.Widget{}, row.Msg.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(row.RowMeta).To(Equal(meta))
					Expect(row.Version).NotTo(BeZero())
				}
				return nil
			})).To(Succeed())
		})
	})

	Describe("Query", func() {
		BeforeEach(func() {
			put(
				&testdatav1.Widget{Id: "a", Count: 3},
				&testdatav1.Widget{Id: "b", Count: 1},
				&testdatav1.Widget{Id: "c", Count: 2},
			)
		})

		spec := query.Spec{Where: query.Gt("count", 1)}

		It("yields the rows that match, as the type argument", func() {
			var got []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				got = collect(server.Query[*testdatav1.Widget](tx, spec))
				return nil
			})).To(Succeed())
			Expect(got).To(Equal([]string{"a", "c"}))
		})

		It("lets the loop body write each row it yields through an EntityTx", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				for w, err := range server.Query[*testdatav1.Widget](tx, spec) {
					if err != nil {
						return err
					}
					w.Count = 0
					if err := tx.Put(w); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())

			var got []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				got = collect(server.Query[*testdatav1.Widget](tx, spec))
				return nil
			})).To(Succeed())
			Expect(got).To(BeEmpty())
		})

		It("yields each match with its version through QueryRows", func() {
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				for row, err := range server.QueryRows[*testdatav1.Widget](tx, spec) {
					Expect(err).NotTo(HaveOccurred())
					_, version, err := tx.Get(&testdatav1.Widget{}, row.Msg.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(row.Version).To(Equal(version))
				}
				return nil
			})).To(Succeed())
		})

		It("yields the error of a spec that does not compile", func() {
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				for _, err := range server.Query[*testdatav1.Widget](tx, query.Spec{Where: query.Eq("nope", 1)}) {
					return err
				}
				return nil
			})).To(MatchError(ContainSubstring("nope")))
		})
	})

	Describe("Get", func() {
		It("reads a row as the type argument", func() {
			put(&testdatav1.Widget{Id: "a", Label: "alpha"})

			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				w, found, err := server.Get[*testdatav1.Widget](tx, "a")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(w.GetLabel()).To(Equal("alpha"))
				return nil
			})).To(Succeed())
		})

		It("returns nil and false for a missing row", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				w, found, err := server.Get[*testdatav1.Widget](tx, "missing")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeFalse())
				Expect(w).To(BeNil())
				return nil
			})).To(Succeed())
		})
	})
})

var _ = Describe("Mutator", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = server.Trusted(context.Background()) })

	It("passes the arguments as the type argument", func() {
		srv := newTestServer(server.Mutator("add_todo",
			func(_ context.Context, tx *server.EntityTx, todo *todov1.Todo) error {
				return tx.Put(todo)
			}))

		Expect(srv.Invoke(ctx, ws, "add_todo", &todov1.Todo{Id: "t1", Title: "milk"})).To(Succeed())

		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			todo, found, err := server.Get[*todov1.Todo](tx, "t1")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(todo.GetTitle()).To(Equal("milk"))
			return nil
		})).To(Succeed())
	})

	It("passes a zero message when the invocation carries no arguments", func() {
		var got *todov1.Todo
		srv := newTestServer(server.Mutator("peek",
			func(_ context.Context, _ *server.EntityTx, todo *todov1.Todo) error {
				got = todo
				return nil
			}))

		Expect(srv.Invoke(ctx, ws, "peek", nil)).To(Succeed())
		Expect(got).NotTo(BeNil())
		Expect(proto.Equal(got, &todov1.Todo{})).To(BeTrue())
	})

	It("refuses a nil func", func() {
		Expect(newTestServerErr(server.Mutator[*todov1.Todo]("nothing", nil))).
			To(MatchError(ContainSubstring("needs a name and a func")))
	})

	It("refuses an interface type argument", func() {
		Expect(newTestServerErr(server.Mutator("any",
			func(context.Context, *server.EntityTx, proto.Message) error { return nil }))).
			To(MatchError(ContainSubstring("interface")))
	})
})

var _ = Describe("FilterRows", func() {
	It("passes each row to the func as the type argument", func() {
		filter := server.FilterRows(func(key string, w *testdatav1.Widget) bool {
			return key == "a" && w.GetLabel() == "alpha"
		})

		Expect(filter("a", &testdatav1.Widget{Id: "a", Label: "alpha"})).To(BeTrue())
		Expect(filter("a", &testdatav1.Widget{Id: "a", Label: "beta"})).To(BeFalse())
	})

	It("refuses a row of another message type", func() {
		filter := server.FilterRows(func(string, *testdatav1.Widget) bool { return true })

		Expect(filter("t1", &todov1.Todo{Id: "t1"})).To(BeFalse())
	})
})

// hideFlagged is a policy whose read filter hides every flagged widget.
type hideFlagged struct{ server.AllowAll }

func (hideFlagged) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if entity != "widget" {
		return nil, nil
	}
	return server.FilterRows(func(_ string, w *testdatav1.Widget) bool { return !w.GetFlag() }), nil
}
