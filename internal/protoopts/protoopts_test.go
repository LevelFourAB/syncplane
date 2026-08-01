package protoopts_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// uninterpretedEntity encodes (syncplane.v1.entity) exactly as it appears on the
// wire, then hands back the bytes as options this process cannot interpret.
// That is byte-for-byte what an app's descriptor carries when its vendored
// syncplane/v1/options.proto generated its own extension type: same field number,
// same encoding, a Go type the engine never sees.
func uninterpretedEntity(eo *syncplanev1.EntityOptions) *descriptorpb.MessageOptions {
	encoded := &descriptorpb.MessageOptions{}
	proto.SetExtension(encoded, syncplanev1.E_Entity, eo)
	raw, err := proto.Marshal(encoded)
	Expect(err).NotTo(HaveOccurred())

	foreign := &descriptorpb.MessageOptions{}
	foreign.ProtoReflect().SetUnknown(raw)
	return foreign
}

func uninterpretedBehavior(fb *syncplanev1.FieldBehavior) *descriptorpb.FieldOptions {
	encoded := &descriptorpb.FieldOptions{}
	proto.SetExtension(encoded, syncplanev1.E_FieldBehavior, fb)
	raw, err := proto.Marshal(encoded)
	Expect(err).NotTo(HaveOccurred())

	foreign := &descriptorpb.FieldOptions{}
	foreign.ProtoReflect().SetUnknown(raw)
	return foreign
}

// buildMessage assembles a one-message file so the options travel through
// protodesc the way a real descriptor set does.
func buildMessage(mopts *descriptorpb.MessageOptions, fopts *descriptorpb.FieldOptions) protoreflect.MessageDescriptor {
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("foreign/v1/thing.proto"),
		Package: proto.String("foreign.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:    proto.String("Thing"),
			Options: mopts,
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("id"),
				Number:   proto.Int32(1),
				JsonName: proto.String("id"),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Options:  fopts,
			}},
		}},
	}, nil)
	Expect(err).NotTo(HaveOccurred())
	return fd.Messages().Get(0)
}

var _ = Describe("Protoopts", func() {
	Describe("descriptors generated against this module", func() {
		It("reads entity options", func() {
			eo := protoopts.Entity((&todov1.Todo{}).ProtoReflect().Descriptor())
			Expect(eo.GetName()).To(Equal("todo"))
			Expect(eo.GetKeyFields()).To(Equal([]string{"id"}))
		})

		It("returns nil for a message that is not an entity", func() {
			md := (&syncplanev1.EntityOptions{}).ProtoReflect().Descriptor()
			Expect(protoopts.Entity(md)).To(BeNil())
			Expect(protoopts.HasUninterpretedOptions(md)).To(BeFalse())
		})

		It("returns nil for a field with no behavior", func() {
			fd := (&todov1.Todo{}).ProtoReflect().Descriptor().Fields().ByName("title")
			Expect(protoopts.FieldBehavior(fd)).To(BeNil())
		})
	})

	Describe("descriptors carrying a divergent options.proto", func() {
		It("recovers entity options the naive read misses", func() {
			md := buildMessage(uninterpretedEntity(&syncplanev1.EntityOptions{
				Name:      "thing",
				KeyFields: []string{"id"},
			}), nil)

			// Guard the premise: without the fallback this message reads as
			// unannotated, which is the bug being fixed.
			mopts, ok := md.Options().(*descriptorpb.MessageOptions)
			Expect(ok).To(BeTrue())
			Expect(proto.HasExtension(mopts, syncplanev1.E_Entity)).To(BeFalse())

			eo := protoopts.Entity(md)
			Expect(eo.GetName()).To(Equal("thing"))
			Expect(eo.GetKeyFields()).To(Equal([]string{"id"}))
		})

		It("recovers field behavior the naive read misses", func() {
			md := buildMessage(nil, uninterpretedBehavior(&syncplanev1.FieldBehavior{Text: true}))
			fd := md.Fields().ByName("id")

			fopts, ok := fd.Options().(*descriptorpb.FieldOptions)
			Expect(ok).To(BeTrue())
			Expect(proto.HasExtension(fopts, syncplanev1.E_FieldBehavior)).To(BeFalse())

			Expect(protoopts.FieldBehavior(fd).GetText()).To(BeTrue())
		})

		It("reports uninterpreted options so the caller can name the cause", func() {
			divergent := buildMessage(uninterpretedEntity(&syncplanev1.EntityOptions{Name: "thing"}), nil)
			Expect(protoopts.HasUninterpretedOptions(divergent)).To(BeTrue())

			unannotated := buildMessage(nil, nil)
			Expect(protoopts.Entity(unannotated)).To(BeNil())
			Expect(protoopts.HasUninterpretedOptions(unannotated)).To(BeFalse())
		})
	})

	Describe("OptionsGoPackage", func() {
		It("is the import path a vendored options.proto must keep", func() {
			Expect(protoopts.OptionsGoPackage).To(Equal("github.com/levelfourab/syncplane/engine/gen/syncplane/v1"))
			Expect(protoopts.OptionsGoPackage).NotTo(ContainSubstring(";"))
		})
	})
})
