package rebase_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

var _ = Describe("Requeue", func() {
	var model *rebase.Model

	BeforeEach(func() {
		model = mustModel(&testdatav1.Widget{})
	})

	put := func(w *testdatav1.Widget) rebase.Op {
		return opOf(crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", w.GetId(), w))
	}
	patch := func(w *testdatav1.Widget, mask ...string) rebase.Op {
		return opOf(crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", w.GetId(), w, mask...))
	}
	del := rebase.Op{Kind: syncplanev1.CrudOp_CRUD_OP_DELETE}
	base := rebase.Row{Present: true, Value: []byte{0x0a, 0x02, 'w', '1', 0x18, 0x07}}
	fields := func(r rebase.Row) *testdatav1.Widget {
		GinkgoHelper()
		if !r.Present {
			return nil
		}
		var w testdatav1.Widget
		Expect(proto.Unmarshal(r.Value, &w)).To(Succeed())
		w.Body = nil
		return &w
	}

	DescribeTable("leaves the row as the original order does when sent behind the later writes",
		func(op rebase.Op, later []rebase.Op) {
			want, err := model.Replay(base, append([]rebase.Op{op}, later...))
			Expect(err).NotTo(HaveOccurred())
			next, keep, err := model.Requeue(op, later)
			Expect(err).NotTo(HaveOccurred())
			ops := later
			if keep {
				ops = append(append([]rebase.Op{}, later...), next)
			}
			got, err := model.Replay(base, ops)
			Expect(err).NotTo(HaveOccurred())
			Expect(fields(got)).To(Equal(fields(want)))
		},
		Entry("a PUT before a PATCH of one of its fields",
			put(&testdatav1.Widget{Id: "w1", Label: "first", Count: 1}),
			[]rebase.Op{patch(&testdatav1.Widget{Id: "w1", Label: "second"}, "label")}),
		Entry("a PATCH before a PATCH of the same field and another",
			patch(&testdatav1.Widget{Id: "w1", Label: "first", Count: 1}, "label", "count"),
			[]rebase.Op{patch(&testdatav1.Widget{Id: "w1", Label: "second", Big: 3}, "label", "big")}),
		Entry("a PUT before a PUT",
			put(&testdatav1.Widget{Id: "w1", Label: "first"}),
			[]rebase.Op{put(&testdatav1.Widget{Id: "w1", Label: "second"})}),
		Entry("a PATCH before a DELETE",
			patch(&testdatav1.Widget{Id: "w1", Label: "first"}, "label"),
			[]rebase.Op{del}),
		Entry("a DELETE before a PUT",
			del,
			[]rebase.Op{put(&testdatav1.Widget{Id: "w1", Label: "again"})}),
		Entry("a DELETE before a PATCH",
			del,
			[]rebase.Op{patch(&testdatav1.Widget{Id: "w1", Label: "second"}, "label")}),
	)

	It("keeps the text of a write a later PUT supersedes, as a PATCH of its documents", func() {
		op := put(&testdatav1.Widget{Id: "w1", Label: "first", Body: doc("a", "bye")})
		op.Text = map[string][]byte{"body": doc("a", "bye")}
		next, keep, err := model.Requeue(op, []rebase.Op{put(&testdatav1.Widget{Id: "w1", Label: "second"})})
		Expect(err).NotTo(HaveOccurred())
		Expect(keep).To(BeTrue())
		Expect(next.Kind).To(Equal(syncplanev1.CrudOp_CRUD_OP_PATCH))
		Expect(next.Mask).To(Equal([]string{"body"}))
		Expect(next.Text).To(Equal(op.Text))
	})

	It("returns op unchanged without later writes", func() {
		op := put(&testdatav1.Widget{Id: "w1", Label: "first"})
		next, keep, err := model.Requeue(op, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(keep).To(BeTrue())
		Expect(next).To(Equal(op))
	})
})
