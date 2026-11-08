package lww_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// A oneof is one field: masks name the GROUP, and the group is replaced whole.
// These pin the rule from both ends — what the group form does, and that the
// member form is refused rather than quietly doing something else.
var _ = Describe("Merge of oneofs", func() {
	var base *testdatav1.Widget

	BeforeEach(func() {
		base = &testdatav1.Widget{
			Id:     "w1",
			Label:  "original",
			Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "hello"},
		}
	})

	It("replaces the active case with the source's", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceNumber{ChoiceNumber: 7}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoiceNumber()).To(BeEquivalentTo(7))
		Expect(base.GetChoiceText()).To(BeEmpty(), "the case that lost must be gone")
	})

	It("sets a case on a base that had none", func() {
		base.Choice = nil
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "set"}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoiceText()).To(Equal("set"))
	})

	// The whole reason the group is the unit: "the writer left the oneof empty"
	// is a statement the group can carry and a single member cannot.
	It("clears the group when the source has no case", func() {
		src := &testdatav1.Widget{}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoice()).To(BeNil())
	})

	It("leaves the group alone when the mask does not name it", func() {
		src := &testdatav1.Widget{Label: "patched"}
		Expect(lww.Merge(base, src, []string{"label"})).To(Succeed())
		Expect(base.GetChoiceText()).To(Equal("hello"))
	})

	It("replaces a message case whole", func() {
		base.Choice = &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "red", Size: 4},
		}
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "blue"},
		}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		Expect(base.GetChoiceSettings().GetColor()).To(Equal("blue"))
		Expect(base.GetChoiceSettings().GetSize()).To(BeZero(), "the group is atomic, not merged per field")
	})

	It("does not alias a source message case into the destination", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "blue"},
		}}
		Expect(lww.Merge(base, src, []string{"choice"})).To(Succeed())
		src.GetChoiceSettings().Color = "mutated-after-merge"
		Expect(base.GetChoiceSettings().GetColor()).To(Equal("blue"))
	})

	It("rejects a mask that names a member", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceText{ChoiceText: "x"}}
		err := lww.Merge(base, src, []string{"choice_text"})
		Expect(err).To(MatchError(ContainSubstring(`member of oneof "choice"`)))
		Expect(base.GetChoiceText()).To(Equal("hello"), "a rejected path must not half-apply")
	})

	It("rejects a sub-path into a message case", func() {
		src := &testdatav1.Widget{Choice: &testdatav1.Widget_ChoiceSettings{
			ChoiceSettings: &testdatav1.Settings{Color: "blue"},
		}}
		Expect(lww.Merge(base, src, []string{"choice.choice_settings.color"})).NotTo(Succeed())
	})

	// proto3 `optional` compiles to a synthetic oneof. Treating that as a real
	// group would make every optional field unmaskable, which is why every check
	// goes through lww.OneofOf.
	It("still masks an optional field by its own name", func() {
		base.Maybe = nil
		src := &testdatav1.Widget{Maybe: ptr("here")}
		Expect(lww.Merge(base, src, []string{"maybe"})).To(Succeed())
		Expect(base.GetMaybe()).To(Equal("here"))

		cleared := &testdatav1.Widget{}
		Expect(lww.Merge(base, cleared, []string{"maybe"})).To(Succeed())
		Expect(base.Maybe).To(BeNil(), "explicit presence must still be clearable")
	})

	// Adding a member to a oneof is additive, so a writer and a server can
	// disagree on the members for a while. The member only one side declares
	// is unknown bytes to the other, and the group must still change as one
	// unit, whichever side writes it.
	Describe("a member a newer schema added", func() {
		// row describes `oneof choice { a, b }`, plus `c` in the newer schema.
		row := func(newer bool) protoreflect.MessageDescriptor {
			GinkgoHelper()
			optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
			str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
			member := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
				return &descriptorpb.FieldDescriptorProto{
					Name: proto.String(name), Number: proto.Int32(number), Type: str, Label: optional,
					JsonName: proto.String(name), OneofIndex: proto.Int32(0),
				}
			}
			fields := []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: str, Label: optional, JsonName: proto.String("id")},
				member("a", 2),
				member("b", 3),
			}
			file := "lww/oneof_v1.proto"
			if newer {
				fields = append(fields, member("c", 4))
				file = "lww/oneof_v2.proto"
			}
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name: proto.String(file), Package: proto.String("lww.oneof"), Syntax: proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{{
					Name:      proto.String("Row"),
					Field:     fields,
					OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("choice")}},
				}},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			return fd.Messages().ByName("Row")
		}
		var older, newer protoreflect.MessageDescriptor
		BeforeEach(func() {
			older, newer = row(false), row(true)
		})

		// build returns a message of md with each named string field set.
		build := func(md protoreflect.MessageDescriptor, fields map[string]string) *dynamicpb.Message {
			m := dynamicpb.NewMessage(md)
			for name, v := range fields {
				m.Set(md.Fields().ByName(protoreflect.Name(name)), protoreflect.ValueOfString(v))
			}
			return m
		}
		// readAs decodes m's wire bytes under md, as a reader on that schema.
		readAs := func(md protoreflect.MessageDescriptor, m proto.Message) *dynamicpb.Message {
			GinkgoHelper()
			raw, err := proto.Marshal(m)
			Expect(err).NotTo(HaveOccurred())
			out := dynamicpb.NewMessage(md)
			Expect(proto.Unmarshal(raw, out)).To(Succeed())
			return out
		}
		// active names the member the oneof holds, with its value.
		active := func(m *dynamicpb.Message) string {
			f := m.WhichOneof(m.Descriptor().Oneofs().ByName("choice"))
			if f == nil {
				return "<none>"
			}
			return string(f.Name()) + "=" + m.Get(f).String()
		}

		It("refuses a write of a member the server does not declare, and keeps the stored one", func() {
			// Given a row stored by the older schema, holding a
			stored := build(older, map[string]string{"id": "r1", "a": "old"})
			// When a newer writer sets c, as the older server decodes it
			src := readAs(older, build(newer, map[string]string{"c": "new"}))
			err := lww.Merge(stored, src, []string{"choice"})

			// Then the merge refuses, and the row still holds a
			Expect(err).To(MatchError(ContainSubstring(`oneof "choice"`)))
			Expect(active(stored)).To(Equal("a=old"))
		})

		It("still clears the group for a value that carries nothing unknown", func() {
			stored := build(older, map[string]string{"id": "r1", "a": "old"})
			Expect(lww.Merge(stored, build(older, nil), []string{"choice"})).To(Succeed())
			Expect(active(stored)).To(Equal("<none>"))
		})

		It("takes a known member from a value that also carries unknown fields", func() {
			stored := build(older, map[string]string{"id": "r1", "a": "old"})
			src := build(older, map[string]string{"b": "mine"})
			src.SetUnknown(protowire.AppendString(protowire.AppendTag(nil, 9, protowire.BytesType), "unrelated"))
			Expect(lww.Merge(stored, src, []string{"choice"})).To(Succeed())
			Expect(active(stored)).To(Equal("b=mine"))
		})
	})
})

func ptr[T any](v T) *T { return &v }
