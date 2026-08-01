package admin

import (
	"bytes"
	"context"
	"net/url"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/admin/ui"
)

// thingDescriptor builds a rich dynamic entity covering every field shape the
// form renderer classifies, plus a "person" target so references resolve.
func thingDescriptor() protoreflect.MessageDescriptor {
	GinkgoHelper()
	person := server.DynamicEntity{
		Name: "person", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	thing := server.DynamicEntity{
		Name: "thing", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "active", Number: 3, Kind: protoreflect.BoolKind},
			{Name: "doc", Number: 4, Kind: protoreflect.BytesKind, Opaque: true},
			{Name: "settings", Number: 5, Message: []server.DynamicField{
				{Name: "color", Number: 1, Kind: protoreflect.StringKind},
				{Name: "size", Number: 2, Kind: protoreflect.Int32Kind},
			}},
			{Name: "owner", Number: 6, Ref: "person"},
			{Name: "watchers", Number: 7, Ref: "person", Repeated: true},
			{Name: "tags", Number: 8, Kind: protoreflect.StringKind, Repeated: true},
			{Name: "attrs", Number: 9, MapKey: protoreflect.StringKind, MapValue: protoreflect.StringKind},
			{Name: "steps", Number: 10, Repeated: true, KeyField: []string{"id"}, Message: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "text", Number: 2, Kind: protoreflect.StringKind},
				// A second, differently typed element field: what a condition
				// group is for is asking about both at once, of one element.
				{Name: "done", Number: 3, Kind: protoreflect.BoolKind},
			}},
		},
	}
	fd, err := server.BuildDynamicFile(person, thing)
	Expect(err).NotTo(HaveOccurred())
	md := fd.Messages().ByName("Thing")
	Expect(md).NotTo(BeNil())
	return md
}

func keyFieldsOf(md protoreflect.MessageDescriptor) []protoreflect.FieldDescriptor {
	return []protoreflect.FieldDescriptor{md.Fields().ByName("id")}
}

var _ = Describe("form rendering", func() {
	var md protoreflect.MessageDescriptor

	BeforeEach(func() { md = thingDescriptor() })

	kindOf := func(views []ui.FieldView, name string) ui.FieldKind {
		for _, v := range views {
			if v.Label == name {
				return v.Kind
			}
		}
		Fail("no field " + name)
		return 0
	}

	It("classifies every field shape", func() {
		empty := dynamicpb.NewMessage(md)
		views := buildForm(md, empty.ProtoReflect(), keyFieldsOf(md), false)
		Expect(kindOf(views, "title")).To(Equal(ui.KindScalar))
		Expect(kindOf(views, "active")).To(Equal(ui.KindBool))
		Expect(kindOf(views, "doc")).To(Equal(ui.KindOpaque))
		Expect(kindOf(views, "settings")).To(Equal(ui.KindMessage))
		Expect(kindOf(views, "owner")).To(Equal(ui.KindRef))
		Expect(kindOf(views, "watchers")).To(Equal(ui.KindRef))
		Expect(kindOf(views, "tags")).To(Equal(ui.KindRepeatedScalar))
		Expect(kindOf(views, "attrs")).To(Equal(ui.KindMap))
		Expect(kindOf(views, "steps")).To(Equal(ui.KindKeyedList))
	})

	// viewByLabel finds a built field view by its label.
	viewByLabel := func(views []ui.FieldView, label string) ui.FieldView {
		for _, v := range views {
			if v.Label == label {
				return v
			}
		}
		Fail("no field " + label)
		return ui.FieldView{}
	}

	// renderField renders one field view to HTML via the templ component.
	renderField := func(fv ui.FieldView) string {
		var buf bytes.Buffer
		Expect(ui.Field(fv).Render(context.Background(), &buf)).To(Succeed())
		return buf.String()
	}

	It("renders a remove control on each repeated item and its add template", func() {
		// A thing carrying one keyed-list item, one repeated scalar and one map
		// entry, so each repeated kind has an existing element to remove.
		msg := dynamicpb.NewMessage(md)
		steps := msg.Mutable(field(md, "steps")).List()
		step := steps.NewElement()
		step.Message().Set(field(step.Message().Descriptor(), "id"), protoreflect.ValueOfString("s1"))
		steps.Append(step)
		msg.Mutable(field(md, "tags")).List().Append(protoreflect.ValueOfString("a"))
		msg.Mutable(field(md, "attrs")).Map().Set(
			protoreflect.ValueOfString("k").MapKey(), protoreflect.ValueOfString("v"))

		views := buildForm(md, msg, keyFieldsOf(md), true)
		// Each repeated field renders one remove control for the existing item
		// plus one in the clone template the add button stamps out.
		for _, label := range []string{"steps", "tags", "attrs"} {
			html := renderField(viewByLabel(views, label))
			Expect(strings.Count(html, "data-repeat-remove")).To(Equal(2), "field %q", label)
		}
	})

	It("omits remove controls on a read-only repeated field", func() {
		msg := dynamicpb.NewMessage(md)
		steps := msg.Mutable(field(md, "steps")).List()
		step := steps.NewElement()
		step.Message().Set(field(step.Message().Descriptor(), "id"), protoreflect.ValueOfString("s1"))
		steps.Append(step)

		fv := formBuilder{}.field(field(md, "steps"), "steps", msg, true /* readOnly */)
		Expect(renderField(fv)).NotTo(ContainSubstring("data-repeat-remove"))
	})

	It("renders key fields read-only on edit only", func() {
		empty := dynamicpb.NewMessage(md)
		create := buildForm(md, empty.ProtoReflect(), keyFieldsOf(md), false)
		edit := buildForm(md, empty.ProtoReflect(), keyFieldsOf(md), true)
		Expect(idField(create).ReadOnly).To(BeFalse())
		Expect(idField(edit).ReadOnly).To(BeTrue())
	})

	It("classifies a compiled enum field", func() {
		crud := (&syncplanev1.CrudMutation{}).ProtoReflect().Descriptor()
		fv := formBuilder{}.field(crud.Fields().ByName("op"), "op", dynamicpb.NewMessage(crud), false)
		Expect(fv.Kind).To(Equal(ui.KindEnum))
		var labels []string
		for _, o := range fv.EnumOptions {
			labels = append(labels, o.Label)
		}
		Expect(labels).To(ContainElement("CRUD_OP_PUT"))
	})
})

var _ = Describe("form binding", func() {
	var md protoreflect.MessageDescriptor

	BeforeEach(func() { md = thingDescriptor() })

	fullForm := func() url.Values {
		return url.Values{
			"id":             {"t1"},
			"title":          {"Hello"},
			"active":         {"true"},
			"settings.color": {"red"},
			"settings.size":  {"4"},
			"owner.id":       {"p1"},
			"watchers[0].id": {"p2"},
			"watchers[1].id": {"p3"},
			"tags[0]":        {"a"},
			"tags[1]":        {"b"},
			"attrs[0].key":   {"k"},
			"attrs[0].value": {"v"},
		}
	}

	It("binds every field shape from dotted/indexed names", func() {
		msg, err := bind(md, fullForm())
		Expect(err).NotTo(HaveOccurred())
		m := msg.ProtoReflect()

		Expect(m.Get(field(md, "title")).String()).To(Equal("Hello"))
		Expect(m.Get(field(md, "active")).Bool()).To(BeTrue())

		settings := m.Get(field(md, "settings")).Message()
		Expect(settings.Get(field(settings.Descriptor(), "color")).String()).To(Equal("red"))
		Expect(settings.Get(field(settings.Descriptor(), "size")).Int()).To(BeEquivalentTo(4))

		owner := m.Get(field(md, "owner")).Message()
		Expect(owner.Get(field(owner.Descriptor(), "id")).String()).To(Equal("p1"))

		watchers := m.Get(field(md, "watchers")).List()
		Expect(watchers.Len()).To(Equal(2))
		Expect(watchers.Get(1).Message().Get(field(md.Fields().ByName("watchers").Message(), "id")).String()).To(Equal("p3"))

		tags := m.Get(field(md, "tags")).List()
		Expect(tags.Len()).To(Equal(2))
		Expect(tags.Get(0).String()).To(Equal("a"))

		attrs := m.Get(field(md, "attrs")).Map()
		Expect(attrs.Get(protoreflect.ValueOfString("k").MapKey()).String()).To(Equal("v"))
	})

	It("computes a PATCH mask of only the changed fields, excluding keys", func() {
		loaded, err := bind(md, fullForm())
		Expect(err).NotTo(HaveOccurred())

		edited := fullForm()
		edited.Set("title", "World")         // changed
		edited.Set("settings.color", "blue") // changed (nested leaf)
		edited.Set("id", "t1")               // key unchanged
		bound, err := bind(md, edited)
		Expect(err).NotTo(HaveOccurred())

		mask := changedMask(loaded, bound, keyFieldsOf(md))
		Expect(mask).To(ConsistOf("title", "settings.color"))
	})
})

func idField(views []ui.FieldView) ui.FieldView {
	for _, v := range views {
		if v.Label == "id" {
			return v
		}
	}
	Fail("no id field")
	return ui.FieldView{}
}

func field(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	return md.Fields().ByName(protoreflect.Name(name))
}
