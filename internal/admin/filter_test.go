package admin

import (
	"fmt"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/internal/admin/ui"
)

var _ = Describe("filter conditions", func() {
	var md protoreflect.MessageDescriptor

	BeforeEach(func() { md = thingDescriptor() })

	// filterQuery builds the query string one condition row submits.
	filterQuery := func(path, op, value string) url.Values {
		return url.Values{"f[0].path": {path}, "f[0].op": {op}, "f[0].v": {value}}
	}

	// sub is one sub-condition of a group: path, operator, operand.
	type sub [3]string

	// groupQuery builds the query string a condition group submits: the group's
	// own row, plus one indexed sub-row per element condition.
	groupQuery := func(path string, subs ...sub) url.Values {
		q := url.Values{"f[0].path": {path}, "f[0].op": {string(opAny)}}
		for i, s := range subs {
			base := fmt.Sprintf("f[0].s[%d]", i)
			q.Set(base+".path", s[0])
			q.Set(base+".op", s[1])
			q.Set(base+".v", s[2])
		}
		return q
	}

	// viewOf renders the bar for a parsed filter. The workspace context only
	// decides whether a reference operand gets a picker URL.
	viewOf := func(fs filterState) ui.FilterView {
		return fs.view("/e", "/e/filter", refContext{appID: "demo", ws: "w1"})
	}

	// pathsIn lists the paths a field select offers.
	pathsIn := func(opts []ui.FilterFieldOption) []string {
		out := make([]string, 0, len(opts))
		for _, o := range opts {
			out = append(out, o.Path)
		}
		return out
	}

	// matches compiles what the filter bar built and evaluates it, so each case
	// asserts on which rows the table would show rather than on the condition
	// tree that produced it.
	matches := func(q url.Values, fill func(protoreflect.Message)) bool {
		GinkgoHelper()
		fs := newFilterState(md, q)
		Expect(fs.err()).To(BeEmpty())
		compiled, err := query.Compile(md, fs.condition())
		Expect(err).NotTo(HaveOccurred())
		row := dynamicpb.NewMessage(md)
		fill(row)
		return compiled.Eval(row)
	}

	// set assigns a top-level field of a row by name.
	set := func(m protoreflect.Message, name string, v protoreflect.Value) {
		m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(name)), v)
	}

	pathsOf := func(fields []filterField) []string {
		out := make([]string, 0, len(fields))
		for _, f := range fields {
			out = append(out, f.path)
		}
		return out
	}

	opsOf := func(path string) []filterOp {
		GinkgoHelper()
		for _, f := range filterFields(md) {
			if f.path == path {
				return f.ops()
			}
		}
		Fail("no filterable field " + path)
		return nil
	}

	It("offers every filterable path, descending one level", func() {
		// Maps and opaque fields are left out: no operator says anything about
		// them. A repeated field appears as itself, never as a prefix.
		Expect(pathsOf(filterFields(md))).To(Equal([]string{
			"id", "title", "active",
			"settings", "settings.color", "settings.size",
			"owner", "owner.id",
			"watchers", "tags", "steps",
		}))
	})

	It("offers the operators a field's shape admits", func() {
		Expect(opsOf("title")).To(Equal([]filterOp{
			opContains, opNotContains, opEq, opNeq, opStartsWith, opEndsWith, opIn,
		}))
		Expect(opsOf("settings.size")).To(Equal([]filterOp{
			opEq, opNeq, opGt, opGte, opLt, opLte, opIn,
		}))
		Expect(opsOf("active")).To(Equal([]filterOp{opEq}))
		Expect(opsOf("tags")).To(Equal([]filterOp{opHas, opHasAny, opHasAll, opIsEmpty, opIsNotEmpty}))
		// A reference compares by the target key it stores, so it takes a
		// chosen row as well as a presence check.
		Expect(opsOf("owner")).To(Equal([]filterOp{opEq, opIn, opIsSet, opIsNotSet}))
		Expect(opsOf("watchers")).To(Equal([]filterOp{opHas, opHasAny, opHasAll, opIsEmpty, opIsNotEmpty}))
		// A keyed message list has no comparable element, so its contents are
		// reachable only through an element-scoped group.
		Expect(opsOf("steps")).To(Equal([]filterOp{opAny, opIsEmpty, opIsNotEmpty}))
		// An implicit-presence scalar is always set, so presence is not offered.
		Expect(opsOf("title")).NotTo(ContainElement(opIsSet))
	})

	It("offers the reference itself rather than the key it stores", func() {
		offered := pathsIn(viewOf(newFilterState(md, nil)).Fields)
		Expect(offered).To(ContainElement("owner"))
		Expect(offered).NotTo(ContainElement("owner.id"))
		// The key path stays addressable all the same, so a filter bookmarked
		// when it was offered still reads rather than erroring.
		Expect(newFilterState(md, filterQuery("owner.id", "eq", "p1")).err()).To(BeEmpty())
	})

	It("filters strings by substring, prefix and equality", func() {
		title := func(s string) func(protoreflect.Message) {
			return func(m protoreflect.Message) { set(m, "title", protoreflect.ValueOfString(s)) }
		}
		Expect(matches(filterQuery("title", "contains", "milk"), title("buy milk"))).To(BeTrue())
		Expect(matches(filterQuery("title", "contains", "milk"), title("buy bread"))).To(BeFalse())
		Expect(matches(filterQuery("title", "not_contains", "milk"), title("buy bread"))).To(BeTrue())
		Expect(matches(filterQuery("title", "starts_with", "buy"), title("buy milk"))).To(BeTrue())
		Expect(matches(filterQuery("title", "eq", "buy milk"), title("buy milk"))).To(BeTrue())
		Expect(matches(filterQuery("title", "in", "buy milk, buy bread"), title("buy bread"))).To(BeTrue())
		Expect(matches(filterQuery("title", "in", "buy milk, buy bread"), title("buy eggs"))).To(BeFalse())
	})

	It("filters numbers, booleans and repeated scalars", func() {
		big := func(m protoreflect.Message) {
			sub := m.Mutable(md.Fields().ByName("settings")).Message()
			sub.Set(sub.Descriptor().Fields().ByName("size"), protoreflect.ValueOfInt32(9))
		}
		Expect(matches(filterQuery("settings.size", "gt", "5"), big)).To(BeTrue())
		Expect(matches(filterQuery("settings.size", "lte", "5"), big)).To(BeFalse())

		on := func(m protoreflect.Message) { set(m, "active", protoreflect.ValueOfBool(true)) }
		Expect(matches(filterQuery("active", "eq", "true"), on)).To(BeTrue())
		Expect(matches(filterQuery("active", "eq", "false"), on)).To(BeFalse())

		tagged := func(m protoreflect.Message) {
			list := m.Mutable(md.Fields().ByName("tags")).List()
			list.Append(protoreflect.ValueOfString("red"))
			list.Append(protoreflect.ValueOfString("blue"))
		}
		Expect(matches(filterQuery("tags", "has", "red"), tagged)).To(BeTrue())
		Expect(matches(filterQuery("tags", "has_all", "red, blue"), tagged)).To(BeTrue())
		Expect(matches(filterQuery("tags", "has_all", "red, green"), tagged)).To(BeFalse())
		Expect(matches(filterQuery("tags", "is_empty", ""), tagged)).To(BeFalse())
		Expect(matches(filterQuery("tags", "is_not_empty", ""), tagged)).To(BeTrue())
	})

	It("filters through a reference by the target key it stores", func() {
		owned := func(m protoreflect.Message) {
			ref := m.Mutable(md.Fields().ByName("owner")).Message()
			ref.Set(ref.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("p1"))
		}
		// The condition names the reference; it compares owner.id underneath.
		Expect(matches(filterQuery("owner", "eq", "p1"), owned)).To(BeTrue())
		Expect(matches(filterQuery("owner", "eq", "p2"), owned)).To(BeFalse())
		Expect(matches(filterQuery("owner.id", "eq", "p1"), owned)).To(BeTrue())
		Expect(matches(filterQuery("owner", "is_set", ""), owned)).To(BeTrue())
		Expect(matches(filterQuery("owner", "is_not_set", ""), func(protoreflect.Message) {})).To(BeTrue())

		// Several chosen targets arrive as one f[i].v per chip.
		anyOf := url.Values{"f[0].path": {"owner"}, "f[0].op": {"in"}, "f[0].v": {"p2", "p1"}}
		Expect(matches(anyOf, owned)).To(BeTrue())
		anyOther := url.Values{"f[0].path": {"owner"}, "f[0].op": {"in"}, "f[0].v": {"p2", "p3"}}
		Expect(matches(anyOther, owned)).To(BeFalse())
	})

	It("filters a repeated reference by membership", func() {
		watched := func(m protoreflect.Message) {
			list := m.Mutable(md.Fields().ByName("watchers")).List()
			for _, id := range []string{"p1", "p2"} {
				elem := list.NewElement()
				elem.Message().Set(elem.Message().Descriptor().Fields().ByName("id"), protoreflect.ValueOfString(id))
				list.Append(elem)
			}
		}
		Expect(matches(filterQuery("watchers", "has", "p1"), watched)).To(BeTrue())
		Expect(matches(filterQuery("watchers", "has", "p3"), watched)).To(BeFalse())

		all := url.Values{"f[0].path": {"watchers"}, "f[0].op": {"has_all"}, "f[0].v": {"p1", "p2"}}
		Expect(matches(all, watched)).To(BeTrue())
		missing := url.Values{"f[0].path": {"watchers"}, "f[0].op": {"has_all"}, "f[0].v": {"p1", "p3"}}
		Expect(matches(missing, watched)).To(BeFalse())
		Expect(matches(filterQuery("watchers", "is_empty", ""), watched)).To(BeFalse())
	})

	It("asks a group's sub-conditions of the same element", func() {
		steps := func(m protoreflect.Message) {
			list := m.Mutable(md.Fields().ByName("steps")).List()
			add := func(id, text string, done bool) {
				elem := list.NewElement()
				e := elem.Message()
				fields := e.Descriptor().Fields()
				e.Set(fields.ByName("id"), protoreflect.ValueOfString(id))
				e.Set(fields.ByName("text"), protoreflect.ValueOfString(text))
				e.Set(fields.ByName("done"), protoreflect.ValueOfBool(done))
				list.Append(elem)
			}
			add("s1", "deploy the thing", false)
			add("s2", "review the thing", true)
		}

		Expect(matches(groupQuery("steps", sub{"text", "contains", "deploy"}), steps)).To(BeTrue())
		Expect(matches(groupQuery("steps", sub{"text", "contains", "ship"}), steps)).To(BeFalse())

		// Both sub-conditions hold of the row, but of DIFFERENT elements — the
		// scoping is the whole point of a group, so this must not match.
		Expect(matches(groupQuery("steps",
			sub{"text", "contains", "deploy"},
			sub{"done", "eq", "true"},
		), steps)).To(BeFalse())
		// The element that satisfies both does match.
		Expect(matches(groupQuery("steps",
			sub{"text", "contains", "review"},
			sub{"done", "eq", "true"},
		), steps)).To(BeTrue())

		// Emptiness still asks about the list itself.
		Expect(matches(filterQuery("steps", "is_not_empty", ""), steps)).To(BeTrue())
	})

	It("leaves a group out of the predicate until a sub-condition is complete", func() {
		half := url.Values{"f[0].path": {"steps"}, "f[0].op": {"any"}}
		Expect(newFilterState(md, half).condition()).To(BeNil())
		Expect(newFilterState(md, groupQuery("steps", sub{"text", "contains", ""})).condition()).To(BeNil())
	})

	It("reports a sub-condition it cannot read, and applies none of the group", func() {
		fs := newFilterState(md, groupQuery("steps", sub{"nope", "eq", "x"}))
		Expect(fs.err()).To(ContainSubstring("cannot be filtered"))
		Expect(fs.condition()).To(BeNil())
		Expect(viewOf(fs).Conds[0].Subs[0].Error).To(ContainSubstring("cannot be filtered"))

		// A readable sub-condition beside a broken one is not applied either:
		// the table reports the problem rather than narrowing by half a group.
		mixed := newFilterState(md, groupQuery("steps",
			sub{"text", "contains", "deploy"},
			sub{"nope", "eq", "x"},
		))
		Expect(mixed.condition()).To(BeNil())
	})

	It("renders a group as sub-rows against the element's fields", func() {
		c := viewOf(newFilterState(md, groupQuery("steps", sub{"text", "contains", "deploy"}))).Conds[0]
		Expect(c.Group).To(BeTrue())
		Expect(c.SubName()).To(Equal("f[0].s"))
		// The sub-row's field select offers the ELEMENT's fields, not the row's.
		Expect(pathsIn(c.SubFields)).To(Equal([]string{"id", "text", "done"}))
		Expect(c.Subs).To(HaveLen(1))
		Expect(c.Subs[0].Name).To(Equal("f[0].s[0]"))
		Expect(c.Subs[0].Value.Value).To(Equal("deploy"))
		Expect(c.SubTemplate.Name).To(Equal("f[0].s[__i__]"))

		// A group just opened carries a blank sub-row, so it is usable without
		// a round trip to add one.
		blank := viewOf(newFilterState(md, url.Values{"f[0].path": {"steps"}, "f[0].op": {"any"}})).Conds[0]
		Expect(blank.Subs).To(HaveLen(1))
		Expect(blank.Subs[0].Path).To(BeEmpty())
	})

	It("renders a reference operand as a picker chip carrying the target key", func() {
		c := viewOf(newFilterState(md, filterQuery("owner", "eq", "p1"))).Conds[0]
		Expect(c.Value.Kind).To(Equal(ui.ValueRef))
		ref := c.Value.Ref
		Expect(ref.Entity).To(Equal("person"))
		Expect(ref.Name).To(Equal("f[0].v"), "the chips submit as the condition's operand")
		Expect(ref.PickerURL).To(Equal("/apps/demo/ws/w1/e/person/picker"))
		Expect(ref.Inline).To(BeTrue())
		Expect(ref.Multi).To(BeFalse(), `"is" holds one target`)
		Expect(ref.Items).To(HaveLen(1))
		Expect(ref.Key(ref.Items[0])).To(Equal("p1"))
		// Unresolved, a chip reads as the key it carries; fillRefLabels
		// replaces that with the target row's display label.
		Expect(ref.Items[0].Label).To(Equal("p1"))

		multi := url.Values{"f[0].path": {"watchers"}, "f[0].op": {"has_any"}, "f[0].v": {"p1", "p2"}}
		mref := viewOf(newFilterState(md, multi)).Conds[0].Value.Ref
		Expect(mref.Multi).To(BeTrue())
		Expect(mref.Items).To(HaveLen(2))
	})

	It("combines conditions with match all / match any", func() {
		q := url.Values{
			"f[0].path": {"title"}, "f[0].op": {"contains"}, "f[0].v": {"milk"},
			"f[1].path": {"active"}, "f[1].op": {"eq"}, "f[1].v": {"true"},
		}
		milk := func(m protoreflect.Message) { set(m, "title", protoreflect.ValueOfString("buy milk")) }

		all := url.Values{"match": {ui.MatchAll}}
		any := url.Values{"match": {ui.MatchAny}}
		for k, v := range q {
			all[k], any[k] = v, v
		}
		Expect(matches(all, milk)).To(BeFalse(), "the row is not active")
		Expect(matches(any, milk)).To(BeTrue(), "one condition is enough")
	})

	It("leaves a half-built condition out of the predicate", func() {
		// A row with a field but no operand yet must not narrow the table.
		Expect(newFilterState(md, filterQuery("title", "contains", "")).condition()).To(BeNil())
		Expect(newFilterState(md, url.Values{"f[0].path": {""}}).condition()).To(BeNil())
		// A presence operator needs no operand, so it applies immediately.
		Expect(newFilterState(md, filterQuery("owner", "is_set", "")).condition()).NotTo(BeNil())
	})

	It("reports an operand it cannot read instead of dropping the condition", func() {
		fs := newFilterState(md, filterQuery("settings.size", "gt", "soon"))
		Expect(fs.err()).To(ContainSubstring("whole number"))
		Expect(fs.condition()).To(BeNil())
		Expect(fs.view("/e", "/e/filter", refContext{}).Conds[0].Error).To(ContainSubstring("whole number"))

		unknown := newFilterState(md, filterQuery("nope", "eq", "x"))
		Expect(unknown.err()).To(ContainSubstring("cannot be filtered"))
	})

	It("falls back to the field's first operator when the submitted one does not apply", func() {
		// Changing the field of a row built against another one leaves a usable
		// condition rather than an error.
		fs := newFilterState(md, filterQuery("tags", "contains", "red"))
		Expect(fs.err()).To(BeEmpty())
		Expect(fs.conds[0].op).To(Equal(opHas))
		Expect(fs.view("/e", "/e/filter", refContext{}).Conds[0].Ops[0].Selected).To(BeTrue())
	})

	It("renders the operand control the operator needs", func() {
		control := func(path, op string) ui.ValueControl {
			return newFilterState(md, filterQuery(path, op, "")).view("/e", "/e/filter", refContext{}).Conds[0].Value
		}
		Expect(control("title", "contains").Kind).To(Equal(ui.ValueText))
		Expect(control("settings.size", "gt").Kind).To(Equal(ui.ValueNumber))
		Expect(control("active", "eq").Kind).To(Equal(ui.ValueBool))
		Expect(control("owner", "is_set").Kind).To(Equal(ui.ValueNone))
		Expect(control("tags", "has_any").Kind).To(Equal(ui.ValueList))
	})

	// Enums cannot be synthesized by the dynamic entity builder, so this one
	// borrows a generated message that has one. Only the descriptor matters.
	It("compares an enum by its number and labels its values", func() {
		md := (&syncplanev1.OrderKey{}).ProtoReflect().Descriptor()
		q := url.Values{"f[0].path": {"dir"}, "f[0].op": {"eq"}, "f[0].v": {"2"}}
		fs := newFilterState(md, q)
		Expect(fs.err()).To(BeEmpty())
		compiled, err := query.Compile(md, fs.condition())
		Expect(err).NotTo(HaveOccurred())
		Expect(compiled.Eval((&syncplanev1.OrderKey{
			Dir: syncplanev1.Direction_DIRECTION_DESC,
		}).ProtoReflect())).To(BeTrue())
		Expect(compiled.Eval((&syncplanev1.OrderKey{
			Dir: syncplanev1.Direction_DIRECTION_ASC,
		}).ProtoReflect())).To(BeFalse())

		ctl := fs.view("/e", "/e/filter", refContext{}).Conds[0].Value
		Expect(ctl.Kind).To(Equal(ui.ValueEnum))
		Expect(ctl.Options).To(ContainElement(ui.EnumOption{
			Value: "2", Label: "direction desc", Selected: true,
		}))
	})
})

var _ = Describe("paging URLs", func() {
	It("carries the filter into the next page's cursor", func() {
		q := url.Values{
			"f[0].path": {"title"}, "f[0].op": {"contains"}, "f[0].v": {"milk"},
			"match":  {"any"},
			"offset": {"50"},
		}
		next, err := url.Parse(pageURL("/apps/demo/ws/w1/e/todo", q, "offset", "100"))
		Expect(err).NotTo(HaveOccurred())
		Expect(next.Path).To(Equal("/apps/demo/ws/w1/e/todo"))
		Expect(next.Query().Get("f[0].v")).To(Equal("milk"))
		Expect(next.Query().Get("match")).To(Equal("any"))
		Expect(next.Query().Get("offset")).To(Equal("100"), "the stale cursor is replaced")
	})
})
