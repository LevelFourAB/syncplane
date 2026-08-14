package query_test

import (
	"math"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

var _ = Describe("CompileGroup", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	group := func(path ...string) *query.Group {
		GinkgoHelper()
		g, err := query.CompileGroup(widget, path)
		Expect(err).NotTo(HaveOccurred())
		return g
	}
	values := func(g *query.Group, w *testdatav1.Widget) []string {
		out := make([]string, 0, 2)
		for _, gv := range g.Values(w.ProtoReflect()) {
			out = append(out, gv.Key())
		}
		return out
	}

	It("accepts a comparable leaf", func() {
		Expect(group("label").Multi()).To(BeFalse())
		Expect(group("count").Multi()).To(BeFalse())
		Expect(group("flag").Multi()).To(BeFalse())
		Expect(group("custom", "priority", "number").Multi()).To(BeFalse())
	})

	It("accepts a repeated scalar and groups per element", func() {
		g := group("tags")
		Expect(g.Multi()).To(BeTrue())
		Expect(values(g, &testdatav1.Widget{Tags: []string{"a", "b"}})).To(HaveLen(2))
	})

	It("rejects a leaf no comparison reaches", func() {
		_, err := query.CompileGroup(widget, []string{"settings"})
		Expect(err).To(MatchError(ContainSubstring("not comparable")))
		_, err = query.CompileGroup(widget, []string{"members"})
		Expect(err).To(MatchError(ContainSubstring("cannot be grouped by")))
	})

	It("puts a row with no value at the path in a group of its own", func() {
		g := group("maybe")
		Expect(values(g, &testdatav1.Widget{})).To(Equal([]string{"\x00missing"}))
		set := "x"
		Expect(values(g, &testdatav1.Widget{Maybe: &set})).To(Equal([]string{"vsx"}))
	})

	It("puts a row holding an EMPTY list in the no-value group", func() {
		// Otherwise such a row would belong to no group at all, and a facet's
		// counts would silently omit it.
		Expect(values(group("tags"), &testdatav1.Widget{})).To(Equal([]string{"\x00missing"}))
	})

	It("collapses an integral double and a bigint of the same magnitude", func() {
		Expect(values(group("count"), &testdatav1.Widget{Count: 5})).To(
			Equal(values(group("big"), &testdatav1.Widget{Big: 5})))
	})

	It("orders real values before NaN before the no-value group", func() {
		only := func(g *query.Group, w *testdatav1.Widget) query.GroupValue {
			return g.Values(w.ProtoReflect())[0]
		}
		plain := only(group("ratio"), &testdatav1.Widget{Ratio: 1})
		nan := only(group("ratio"), &testdatav1.Widget{Ratio: math.NaN()})
		missing := only(group("maybe"), &testdatav1.Widget{})
		Expect(query.CompareGroupValues(plain, nan)).To(BeNumerically("<", 0))
		Expect(query.CompareGroupValues(nan, missing)).To(BeNumerically("<", 0))
		Expect(query.CompareGroupValues(missing, plain)).To(BeNumerically(">", 0))
	})
})

var _ = Describe("PlanGroup", func() {
	widget := (&testdatav1.Widget{}).ProtoReflect().Descriptor()

	index := func(name string, paths ...string) query.Index {
		GinkgoHelper()
		comps := make([]query.IndexComponent, 0, len(paths))
		for _, p := range paths {
			desc := false
			if p[0] == '-' {
				desc, p = true, p[1:]
			}
			c, err := query.CompileIndexComponent(widget, strings.Split(p, "."), desc)
			Expect(err).NotTo(HaveOccurred())
			comps = append(comps, c)
		}
		enc, err := query.NewIndexEncoder(comps)
		Expect(err).NotTo(HaveOccurred())
		return query.Index{Name: name, Encoder: enc}
	}
	compile := func(cond *syncplanev1.Condition) *query.Compiled {
		GinkgoHelper()
		c, err := query.Compile(widget, cond)
		Expect(err).NotTo(HaveOccurred())
		return c
	}
	group := func(path ...string) *query.Group {
		GinkgoHelper()
		g, err := query.CompileGroup(widget, path)
		Expect(err).NotTo(HaveOccurred())
		return g
	}

	byLabel := index("by_label", "label")
	byLabelCount := index("by_label_count", "label", "count")
	byCountLabel := index("by_count_label", "count", "label")
	byLabelDesc := index("by_label_desc", "-label")
	byTags := index("by_tags", "tags")

	It("partitions on a leading component with no predicate", func() {
		p := query.PlanGroup([]query.Index{byLabel}, nil, group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.Index).To(Equal("by_label"))
		Expect(p.Residual).To(BeNil())
		Expect(p.EntriesAreRows).To(BeTrue())
		Expect(p.Desc).To(BeFalse())
	})

	It("partitions on the component after the equality prefix", func() {
		p := query.PlanGroup([]query.Index{byCountLabel},
			compile(query.Eq("count", 3)), group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.Index).To(Equal("by_count_label"))
		Expect(p.Residual).To(BeNil(), "the prefix answers the predicate")
	})

	It("declines an index whose partition component the predicate pins", func() {
		// Every entry in range shares one label, so the index says nothing the
		// predicate did not.
		Expect(query.PlanGroup([]query.Index{byLabel},
			compile(query.Eq("label", "a")), group("label"))).To(BeNil())
	})

	It("declines an index that does not lead with the grouping path", func() {
		Expect(query.PlanGroup([]query.Index{byLabelCount}, nil, group("count"))).To(BeNil())
	})

	It("keeps an unanswered term as the residual", func() {
		p := query.PlanGroup([]query.Index{byLabel},
			compile(query.Gt("count", 2)), group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.Residual).NotTo(BeNil(), "count is not a component of this index")
	})

	It("bounds which groups the walk visits from an inequality on the path", func() {
		p := query.PlanGroup([]query.Index{byLabel},
			compile(query.Gte("label", "m")), group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.Residual).To(BeNil())
		Expect(p.Range.Lo).NotTo(BeEmpty(), "the range starts at the first group at or above m")
	})

	It("reports a descending partition component so the walk can reverse", func() {
		p := query.PlanGroup([]query.Index{byLabelDesc}, nil, group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.Desc).To(BeTrue())
	})

	It("partitions a repeated path over its fan-out component", func() {
		p := query.PlanGroup([]query.Index{byTags}, nil, group("tags"))
		Expect(p).NotTo(BeNil())
		Expect(p.EntriesAreRows).To(BeTrue(),
			"within one tag's range a row holds exactly one entry")
	})

	It("declines when the component fans out and the path does not", func() {
		Expect(query.PlanGroup([]query.Index{byTags}, nil, group("label"))).To(BeNil())
	})

	It("declines an `in` that would read one row from two ranges", func() {
		// A row matching both values would be counted twice, and a count has no
		// row in hand to dedupe by.
		Expect(query.PlanGroup([]query.Index{byCountLabel},
			compile(query.In("count", 1, 2)), group("label"))).To(BeNil())
	})

	It("reports that entries are not rows when a component past the partition fans out", func() {
		p := query.PlanGroup([]query.Index{index("by_label_tags", "label", "tags")},
			nil, group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.EntriesAreRows).To(BeFalse())
	})

	It("prefers the index that pins more of the predicate", func() {
		p := query.PlanGroup([]query.Index{byLabel, byCountLabel},
			compile(query.Eq("count", 3)), group("label"))
		Expect(p).NotTo(BeNil())
		Expect(p.Index).To(Equal("by_count_label"))
	})
})
