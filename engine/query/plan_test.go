package query_test

import (
	"fmt"
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
		Entry("an or with a branch no index serves", query.Or(query.Eq("label", "a"), query.Eq("count", 1))),
		Entry("a predicate on a field no index covers", query.Eq("flag", true)),
		Entry("an ordering no index covers", nil, query.Asc("big")),
	)

	Describe("over a top-level or", func() {
		byCount := index("by_count", "count")

		It("unions one probe set per branch, each on its own index", func() {
			p := query.PlanQuery([]query.Index{byLabel, byCount},
				compile(query.Or(query.Eq("label", "a"), query.Eq("count", 1))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Union).To(HaveLen(2))
			Expect(p.Union[0].Index).To(Equal("by_label"))
			Expect(p.Union[1].Index).To(Equal("by_count"))
			Expect(p.Residual).To(BeNil(), "every branch's range is exact")
			Expect(p.Ordered).To(BeFalse())
			Expect(p.Prefix).To(BeNil())
		})

		It("widens a branch's residual to the whole or", func() {
			// A row a branch's range yields may fail that branch and still match
			// another, so only the full predicate is a correct per-row re-check.
			pred := compile(query.Or(
				query.And(query.Eq("label", "a"), query.Eq("flag", true)),
				query.Eq("count", 1)))
			p := query.PlanQuery([]query.Index{byLabel, byCount}, pred, nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).To(Equal(pred))
		})

		It("plans a one-branch or as the branch itself, ordering intact", func() {
			p := query.PlanQuery([]query.Index{byLabelCount},
				compile(query.Or(query.Eq("label", "a"))),
				order(query.Desc("count")))
			Expect(p).NotTo(BeNil())
			Expect(p.Union).To(BeEmpty())
			Expect(p.Ordered).To(BeTrue())
		})

		It("declines a union whose probes outgrow the fan-out guard", func() {
			labels := make([]string, 40)
			for i := range labels {
				labels[i] = fmt.Sprintf("v%02d", i)
			}
			counts := make([]int, 30)
			for i := range counts {
				counts[i] = i
			}
			p := query.PlanQuery([]query.Index{byLabel, byCount},
				compile(query.Or(query.In("label", labels...), query.In("count", counts...))), nil)
			Expect(p).To(BeNil())
		})
	})

	Describe("over two inequalities on one component", func() {
		byCount := index("by_count", "count")

		It("intersects them into one bounded range", func() {
			p := query.PlanQuery([]query.Index{byCount},
				compile(query.And(query.Gte("count", 1), query.Lt("count", 5))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil(), "both bounds are the range, so nothing is re-checked")
		})

		It("keeps a third bound residual", func() {
			p := query.PlanQuery([]query.Index{byCount},
				compile(query.And(query.Gte("count", 1), query.Lt("count", 5), query.Lt("count", 3))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).NotTo(BeNil())
		})
	})

	Describe("over presence", func() {
		byMaybe := index("by_maybe", "maybe")
		byMaybeLabel := index("by_maybe_label", "maybe", "label")
		byTags := index("by_tags", "tags")

		It("pins isNotSet to the missing rank", func() {
			p := query.PlanQuery([]query.Index{byMaybe}, compile(query.IsNotSet("maybe")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("by_maybe"))
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil(), "the missing rank IS the term")
		})

		It("keeps the components after a pinned isNotSet usable", func() {
			p := query.PlanQuery([]query.Index{byMaybeLabel},
				compile(query.IsNotSet("maybe")), order(query.Asc("label")))
			Expect(p).NotTo(BeNil())
			Expect(p.Ordered).To(BeTrue())
		})

		It("bounds isSet to the set ranks", func() {
			p := query.PlanQuery([]query.Index{byMaybe}, compile(query.IsSet("maybe")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil())
		})

		It("declines presence over a fan-out component", func() {
			// An empty list encodes as missing while the list itself always
			// resolves as found, so the ranks and the evaluator disagree.
			Expect(query.PlanQuery([]query.Index{byTags}, compile(query.IsSet("tags")), nil)).To(BeNil())
			Expect(query.PlanQuery([]query.Index{byTags}, compile(query.IsNotSet("tags")), nil)).To(BeNil())
		})
	})

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

	Describe("map fan-out", func() {
		// Serving one custom field means pinning the entry key and then ranging
		// over the value inside it. Nothing else about the index is different,
		// which is the point: an EAV index is an ordinary composite whose first
		// column happens to be the entry key.
		//
		// One declared path is one component, so these go through the same
		// `index` helper as everything above — the layout is stated, not derived.
		cfNumber := index("cf_number", "custom.*.key", "custom.*.value.number")
		cfNumRank := index("cf_num_rank", "custom.*.key", "custom.*.value.number", "-count")
		cfLabels := index("cf_labels", "custom.*.key", "custom.*.value.labels.values")
		// The mirror layout, and the one with no key component at all — the
		// exactness guard's subject.
		cfByNumber := index("cf_by_number", "custom.*.value.number", "custom.*.key")
		cfNumberOnly := index("cf_number_only", "custom.*.value.number")

		It("pins the entry key and the leaf from one predicate", func() {
			p := query.PlanQuery([]query.Index{cfNumber}, compile(query.Eq("custom.priority.number", 5)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_number"))
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil(), "key plus leaf is the whole predicate")
		})

		It("takes a range on the leaf inside the pinned key", func() {
			p := query.PlanQuery([]query.Index{cfNumber}, compile(query.Gt("custom.priority.number", 3)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil())
		})

		It("serves an ordering off the leaf once the key is pinned", func() {
			// The relaxation a map needs and a list cannot have: a map holds at
			// most ONE value per key, so a row pinned to a key occupies exactly
			// one position and the leaf is a row ordering.
			p := query.PlanQuery(
				[]query.Index{cfNumber},
				compile(query.Gt("custom.priority.number", 0)),
				order(query.Asc("custom.priority.number")))
			Expect(p).NotTo(BeNil())
			Expect(p.Ordered).To(BeTrue())
		})

		It("serves a second sort key after the pinned custom field", func() {
			p := query.PlanQuery(
				[]query.Index{cfNumRank},
				compile(query.Eq("custom.priority.number", 5)),
				order(query.Desc("count")))
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_num_rank"))
			Expect(p.Ordered).To(BeTrue())
		})

		It("declines an ordering by a custom field no predicate pins", func() {
			// Rows lacking the key sit outside every key range and must sort last,
			// and the executor has no second pass to append them — so the honest
			// answer is to scan.
			p := query.PlanQuery(
				[]query.Index{cfNumber}, compile(nil), order(query.Asc("custom.priority.number")))
			Expect(p).To(BeNil())
		})

		It("does not read one custom field's range as another's", func() {
			// The key component and the leaf come from ONE declared path and must
			// describe the same entry: pinning `team` and then ranging on
			// `priority`'s number would answer a question nobody asked.
			p := query.PlanQuery([]query.Index{cfNumber}, compile(query.And(
				query.Eq("custom.team.number", 1),
				query.Eq("custom.priority.number", 5),
			)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).NotTo(BeNil(), "one of the two terms is re-checked per row")
		})

		It("declines isNotSet, which a key-pinned range would answer by dropping rows", func() {
			// Every row with no such entry matches, and those rows are not in the
			// key's range at all.
			p := query.PlanQuery(
				[]query.Index{cfNumber}, compile(query.IsNotSet("custom.priority.number")), nil)
			Expect(p).To(BeNil())
		})

		It("pins the key from isSet, which does imply the entry is there", func() {
			p := query.PlanQuery(
				[]query.Index{cfNumber}, compile(query.IsSet("custom.priority.number")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).NotTo(BeNil(), "the range covers the key, the term re-checks the variant")
		})

		It("serves membership over a list nested inside one entry", func() {
			p := query.PlanQuery(
				[]query.Index{cfLabels}, compile(query.Has("custom.tags.labels.values", "ui")), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_labels"))
			Expect(p.Residual).To(BeNil())
		})

		It("pins the value first and the key second under the mirror layout", func() {
			// [value, key] answers the same named-field equality as [key, value]
			// does, from the other end: pin the value, then the key beside it.
			p := query.PlanQuery(
				[]query.Index{cfByNumber}, compile(query.Eq("custom.priority.number", 5)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_by_number"))
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil(), "value plus key is the whole predicate")
		})

		It("keeps a named-field term residual against a value-only index", func() {
			// THE exactness guard. The range covers `number == 5` under EVERY
			// entry key, so it contains rows where a different custom field holds
			// 5. Marking the term answered would return them.
			p := query.PlanQuery(
				[]query.Index{cfNumberOnly}, compile(query.Eq("custom.priority.number", 5)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_number_only"))
			Expect(p.Residual).NotTo(BeNil(),
				"the range drops the key constraint, so the term is re-checked per row")
		})

		It("keeps a named-field inequality residual against a value-only index", func() {
			// Same guard on the range side: a bound on the value alone says
			// nothing about which field carries it.
			p := query.PlanQuery(
				[]query.Index{cfNumberOnly}, compile(query.Gt("custom.priority.number", 3)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).NotTo(BeNil())
		})

		It("does not read one custom field's range as another's under the mirror layout", func() {
			p := query.PlanQuery([]query.Index{cfByNumber}, compile(query.And(
				query.Eq("custom.team.number", 1),
				query.Eq("custom.priority.number", 5),
			)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).NotTo(BeNil(), "one of the two terms is re-checked per row")
		})

		It("serves a cross-key any() off the mirror layout", func() {
			// The point of [value, key]: "which custom field holds 5" pins the
			// value and stops, and the range answers the existential outright —
			// there is no key constraint to drop.
			p := query.PlanQuery([]query.Index{cfByNumber},
				compile(query.Any("custom", query.Eq("value.number", 5))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_by_number"))
			Expect(p.Ranges).To(HaveLen(1))
			Expect(p.Residual).To(BeNil())
		})

		It("serves a cross-key any() off a value-only index too", func() {
			// No key component to pin, and none needed: the term names no key.
			p := query.PlanQuery([]query.Index{cfNumberOnly},
				compile(query.Any("custom", query.Eq("value.number", 5))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).To(BeNil())
		})

		It("pins both sides of one entry from an any() conjoining them", func() {
			// key and value come from ONE entry position, so the two components
			// together answer the whole existential rather than half of it.
			both := query.Any("custom", query.And(
				query.Eq("key", "priority"), query.Eq("value.number", 5)))
			for _, idx := range []query.Index{cfNumber, cfByNumber} {
				p := query.PlanQuery([]query.Index{idx}, compile(both), nil)
				Expect(p).NotTo(BeNil(), idx.Name)
				Expect(p.Ranges).To(HaveLen(1), idx.Name)
				Expect(p.Residual).To(BeNil(), idx.Name)
			}
		})

		It("keeps a conjunction residual when only one conjunct reduces", func() {
			// The rest has to be re-checked against the SAME entry, and the range
			// cannot say which entry it matched.
			p := query.PlanQuery([]query.Index{cfNumberOnly}, compile(query.Any("custom", query.And(
				query.Eq("key", "priority"), query.Eq("value.number", 5)))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).NotTo(BeNil())
		})

		It("serves an any() over the keys alone", func() {
			p := query.PlanQuery([]query.Index{cfNumber},
				compile(query.Any("custom", query.Eq("key", "priority"))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Index).To(Equal("cf_number"))
			Expect(p.Residual).To(BeNil(), "the key range IS the question")
		})

		It("fans an in() over entry values into one probe per value", func() {
			p := query.PlanQuery([]query.Index{cfByNumber},
				compile(query.Any("custom", query.In("value.number", 1, 5))), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Ranges).To(HaveLen(2))
			Expect(p.Residual).To(BeNil())
		})

		It("declines an any() the index does not fan over", func() {
			// cf_labels fans over the map AND a list inside one entry, so its
			// value component reads label elements, not the entry's number.
			p := query.PlanQuery([]query.Index{cfLabels},
				compile(query.Any("custom", query.Eq("value.number", 5))), nil)
			Expect(p).To(BeNil())
		})

		It("does not read a nested list's index as another custom field's", func() {
			// cf_labels fans over the map AND over a list inside one entry, so a
			// row occupies several positions even with the key pinned. Nothing can
			// order by it — an order key may never name a repeated path, so
			// CompileOrder rejects the spelling outright — and the index must not
			// answer a predicate on a different field either.
			p := query.PlanQuery(
				[]query.Index{cfLabels}, compile(query.Eq("custom.tags.number", 1)), nil)
			Expect(p).NotTo(BeNil())
			Expect(p.Residual).NotTo(BeNil(), "only the entry key is pinned; the leaf is re-checked")
		})
	})
})
