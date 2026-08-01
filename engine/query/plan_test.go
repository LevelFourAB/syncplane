package query_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// The planner's job is to decline safely and to serve exactly. The
// differential tests in engine/server prove the "exactly" half against real
// rows; these pin the decisions themselves, so that a planner which silently
// stopped choosing any index would fail here rather than pass everything by
// falling back.
var _ = Describe("PlanQuery", func() {
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
	order := func(keys ...*syncplanev1.OrderKey) *query.Order {
		GinkgoHelper()
		o, err := query.CompileOrder(widget, keys)
		Expect(err).NotTo(HaveOccurred())
		return o
	}

	byLabel := index("by_label", "label")
	byLabelCount := index("by_label_count", "label", "-count")

	It("serves an equality on the leading component", func() {
		p := query.PlanQuery([]query.Index{byLabel}, compile(query.Eq("label", "a")), nil)
		Expect(p).NotTo(BeNil())
		Expect(p.Index).To(Equal("by_label"))
		Expect(p.Ranges).To(HaveLen(1))
		Expect(p.Residual).To(BeNil(), "the range is exact, so nothing is re-checked")
	})

	It("prefers the composite index when it also supplies the ordering", func() {
		p := query.PlanQuery(
			[]query.Index{byLabel, byLabelCount},
			compile(query.Eq("label", "a")),
			order(query.Desc("count")))
		Expect(p).NotTo(BeNil())
		Expect(p.Index).To(Equal("by_label_count"))
		Expect(p.Ordered).To(BeTrue(), "reading the range in order IS the requested order")
	})

	It("does not claim an ordering the index sorts the other way", func() {
		p := query.PlanQuery(
			[]query.Index{byLabelCount},
			compile(query.Eq("label", "a")),
			order(query.Asc("count")))
		Expect(p).NotTo(BeNil())
		Expect(p.Ordered).To(BeFalse(), "the component is stored descending")
	})

	It("leaves a term the range does not cover as a residual", func() {
		p := query.PlanQuery([]query.Index{byLabel},
			compile(query.And(query.Eq("label", "a"), query.Eq("flag", true))), nil)
		Expect(p).NotTo(BeNil())
		Expect(p.Residual).NotTo(BeNil(), "flag is not in this index and must be re-checked")
	})

	It("turns an `in` into one range per value", func() {
		p := query.PlanQuery([]query.Index{byLabel},
			compile(query.In("label", "a", "b", "c")), nil)
		Expect(p).NotTo(BeNil())
		Expect(p.Ranges).To(HaveLen(3))
	})

	It("reports an `in` as unordered, since several ranges need a merge", func() {
		p := query.PlanQuery([]query.Index{byLabel},
			compile(query.In("label", "a", "b")), order(query.Asc("label")))
		Expect(p).NotTo(BeNil())
		Expect(p.Ordered).To(BeFalse())
	})

	It("bounds a range from an inequality", func() {
		p := query.PlanQuery([]query.Index{byLabel}, compile(query.Gt("label", "m")), nil)
		Expect(p).NotTo(BeNil())
		Expect(p.Ranges).To(HaveLen(1))
		Expect(p.Ranges[0].Lo).NotTo(BeEmpty())
		Expect(p.Residual).To(BeNil())
	})

	It("bounds a range from startsWith, which is a prefix and so contiguous", func() {
		p := query.PlanQuery([]query.Index{byLabel}, compile(query.StartsWith("label", "ab")), nil)
		Expect(p).NotTo(BeNil())
		Expect(p.Residual).To(BeNil())
	})

	It("serves an ordering with no predicate at all", func() {
		p := query.PlanQuery([]query.Index{byLabel}, nil, order(query.Asc("label")))
		Expect(p).NotTo(BeNil())
		Expect(p.Ordered).To(BeTrue())
	})

	DescribeTable("declines, leaving the caller to scan",
		func(pred *syncplanev1.Condition, keys ...*syncplanev1.OrderKey) {
			var o *query.Order
			if len(keys) > 0 {
				o = order(keys...)
			}
			Expect(query.PlanQuery([]query.Index{byLabel, byLabelCount}, compile(pred), o)).To(BeNil())
		},
		Entry("contains, which no range bounds", query.Contains("label", "x")),
		Entry("endsWith, likewise", query.EndsWith("label", "x")),
		Entry("or, which needs a merge of several ranges", query.Or(query.Eq("label", "a"), query.Eq("count", 1))),
		Entry("a predicate on a field no index covers", query.Eq("flag", true)),
		Entry("an ordering no index covers", nil, query.Asc("big")),
	)

	It("declines when there are no candidates", func() {
		Expect(query.PlanQuery(nil, compile(query.Eq("label", "a")), nil)).To(BeNil())
	})

	It("declines a bare full scan, which the row keyspace already serves", func() {
		Expect(query.PlanQuery([]query.Index{byLabel}, nil, nil)).To(BeNil())
	})

	It("declines a literal of a kind the column cannot hold", func() {
		// eq("count", "text") is a legitimate no-match, not an error — so it
		// must stay a residual the evaluator answers false to rather than
		// becoming a bound that means something else.
		byCount := index("by_count", "count")
		p := query.PlanQuery([]query.Index{byCount}, compile(query.Eq("count", "text")), nil)
		Expect(p).To(BeNil())
	})

	Describe("over a fan-out component", func() {
		byTags := index("by_tags", "tags")
		byTagsLabel := index("by_tags_label", "tags", "label")
		byMember := index("by_member", "members.uid")

		It("serves membership over a repeated scalar", func() {
			p := query.PlanQuery([]query.Index{byTags}, compile(query.Has("tags", "a")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("by_tags"))
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil(), "the range is exact")
		})

		It("turns hasAny into one range per value", func() {
			p := query.PlanQuery([]query.Index{byTags}, compile(query.HasAny("tags", "a", "b")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(2))
			Expect(p.Residual).To(BeNil())
		})

		It("pins hasAll on one value and re-checks the rest", func() {
			// The rows holding every value are a subset of those holding the
			// first, so one narrow range plus a residual beats a union.
			p := query.PlanQuery([]query.Index{byTags}, compile(query.HasAll("tags", "a", "b")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).NotTo(BeNil())
		})

		It("serves an ordering off the component AFTER a pinned fan-out", func() {
			// Each matching row holds exactly one entry under the pinned tag —
			// duplicates collapse — so walking the range is a row ordering.
			p := query.PlanQuery(
				[]query.Index{byTagsLabel},
				compile(query.Has("tags", "a")),
				order(query.Asc("label")))
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("by_tags_label"))
			Expect(p.Ordered).To(BeTrue())
		})

		It("does not claim an ordering when the fan-out fans into several ranges", func() {
			p := query.PlanQuery(
				[]query.Index{byTagsLabel},
				compile(query.HasAny("tags", "a", "b")),
				order(query.Asc("label")))
			Expect(p).NotTo(BeNil())
			Expect(p.Ordered).To(BeFalse(), "several ranges need a merge the caller does not do")
		})

		It("serves any() over a repeated message field", func() {
			p := query.PlanQuery(
				[]query.Index{byMember},
				compile(query.Any("members", query.Eq("uid", "u1"))),
				nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("by_member"))
			Expect(p.Residual).To(BeNil())
		})

		It("keeps a multi-condition any() residual, since the range cannot say which element it matched", func() {
			p := query.PlanQuery(
				[]query.Index{byMember},
				compile(query.Any("members", query.And(query.Eq("uid", "u1"), query.Gt("role", 1)))),
				nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).NotTo(BeNil())
		})

		It("declines an any() whose condition names no indexed field", func() {
			p := query.PlanQuery(
				[]query.Index{byMember},
				compile(query.Any("members", query.Eq("name", "x"))),
				nil)
			Expect(p).To(BeNil())
		})

		It("declines membership the index is not keyed by", func() {
			p := query.PlanQuery([]query.Index{byTags}, compile(query.Has("atomic_tags", "a")), nil)
			Expect(p).To(BeNil())
		})
	})
})
