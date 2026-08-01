package query_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

var _ = Describe("Builder", func() {
	// matches compiles cond against Widget and evaluates it, so each case
	// asserts on behaviour rather than on the proto tree that produced it.
	matches := func(cond *syncplanev1.Condition, w *testdatav1.Widget) bool {
		GinkgoHelper()
		compiled, err := query.Compile(widgetDesc, cond)
		Expect(err).NotTo(HaveOccurred())
		return compiled.Eval(w.ProtoReflect())
	}

	It("compares strings", func() {
		w := &testdatav1.Widget{Label: "hello"}
		Expect(matches(query.Eq("label", "hello"), w)).To(BeTrue())
		Expect(matches(query.Neq("label", "hello"), w)).To(BeFalse())
		Expect(matches(query.StartsWith("label", "he"), w)).To(BeTrue())
		Expect(matches(query.EndsWith("label", "lo"), w)).To(BeTrue())
		Expect(matches(query.Contains("label", "ell"), w)).To(BeTrue())
		Expect(matches(query.In("label", "a", "hello"), w)).To(BeTrue())
		Expect(matches(query.In[string]("label"), w)).To(BeFalse(), "an empty list matches nothing")
	})

	It("compares numbers across widths", func() {
		w := &testdatav1.Widget{Count: 5, Big: 1 << 40, Ratio: 1.5}
		Expect(matches(query.Gt("count", 4), w)).To(BeTrue())
		Expect(matches(query.Gte("count", int8(5)), w)).To(BeTrue())
		Expect(matches(query.Lt("count", int64(6)), w)).To(BeTrue())
		Expect(matches(query.Lte("count", uint32(5)), w)).To(BeTrue())
		Expect(matches(query.Eq("big", int64(1<<40)), w)).To(BeTrue())
		Expect(matches(query.Gt("ratio", 1.0), w)).To(BeTrue())
		Expect(matches(query.Eq("ratio", float32(1.5)), w)).To(BeTrue())
	})

	It("compares booleans", func() {
		Expect(matches(query.Eq("flag", true), &testdatav1.Widget{Flag: true})).To(BeTrue())
		Expect(matches(query.Eq("flag", false), &testdatav1.Widget{})).To(BeTrue())
	})

	It("converts a named integer type like its underlying kind", func() {
		// Generated enums are named int32s; without kind-based conversion they
		// would not reach the wire at all.
		type priority int32
		w := &testdatav1.Widget{Count: 2}
		Expect(matches(query.Eq("count", priority(2)), w)).To(BeTrue())
	})

	It("tests repeated fields", func() {
		w := &testdatav1.Widget{Tags: []string{"x", "y"}}
		Expect(matches(query.Has("tags", "x"), w)).To(BeTrue())
		Expect(matches(query.HasAny("tags", "z", "y"), w)).To(BeTrue())
		Expect(matches(query.HasAll("tags", "x", "y"), w)).To(BeTrue())
		Expect(matches(query.HasAll("tags", "x", "z"), w)).To(BeFalse())
		Expect(matches(query.IsEmpty("tags"), w)).To(BeFalse())
		Expect(matches(query.IsEmpty("tags"), &testdatav1.Widget{})).To(BeTrue())
	})

	It("tests presence", func() {
		set := &testdatav1.Widget{Maybe: proto.String("")}
		Expect(matches(query.IsSet("maybe"), set)).To(BeTrue())
		Expect(matches(query.IsNotSet("maybe"), &testdatav1.Widget{})).To(BeTrue())
		// Implicit-presence scalars are constitutionally set, zero or not.
		Expect(matches(query.IsSet("count"), &testdatav1.Widget{})).To(BeTrue())
	})

	It("composes boolean structure", func() {
		w := &testdatav1.Widget{Label: "hello", Count: 5}
		Expect(matches(query.And(query.Eq("label", "hello"), query.Gt("count", 4)), w)).To(BeTrue())
		Expect(matches(query.And(query.Eq("label", "nope"), query.Gt("count", 4)), w)).To(BeFalse())
		Expect(matches(query.Or(query.Eq("label", "nope"), query.Gt("count", 4)), w)).To(BeTrue())
		Expect(matches(query.Not(query.Eq("label", "nope")), w)).To(BeTrue())
		Expect(matches(query.And(), w)).To(BeTrue(), "no conditions matches everything")
		Expect(matches(query.Or(), w)).To(BeFalse(), "no alternatives matches nothing")
	})

	It("addresses nested fields with a dotted path", func() {
		w := &testdatav1.Widget{Settings: &testdatav1.Settings{Color: "red"}}
		Expect(matches(query.Eq("settings.color", "red"), w)).To(BeTrue())
		Expect(query.Path("settings.color")).To(Equal([]string{"settings", "color"}))
	})

	It("rejects a path that names no field, so a typo is not a silent no-match", func() {
		_, err := query.Compile(widgetDesc, query.Eq("labl", "hello"))
		Expect(err).To(MatchError(ContainSubstring(`no field "labl"`)))
	})

	It("builds order keys in both directions", func() {
		asc := query.Asc("count")
		Expect(asc.GetPath()).To(Equal([]string{"count"}))
		Expect(asc.GetDir()).To(Equal(syncplanev1.Direction_DIRECTION_ASC))
		Expect(query.Desc("settings.size").GetDir()).To(Equal(syncplanev1.Direction_DIRECTION_DESC))

		order, err := query.CompileOrder(widgetDesc, []*syncplanev1.OrderKey{query.Desc("count")})
		Expect(err).NotTo(HaveOccurred())
		a := &testdatav1.Widget{Count: 2}
		b := &testdatav1.Widget{Count: 1}
		Expect(order.Compare(a.ProtoReflect(), b.ProtoReflect(), "a", "b")).To(BeNumerically("<", 0))
	})
})
