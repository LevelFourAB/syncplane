package admin

import (
	"log/slog"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

func init() { slog.SetDefault(slog.New(slog.DiscardHandler)) }

func TestAdmin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Admin Suite")
}

// descriptorSet assembles the self-contained FileDescriptorSet an app registers
// (descriptor.proto + syncplane/v1/options.proto + one synthesized file holding
// every entity), mirroring the server test helper of the same name. The
// synthesized file is last, so a spec that needs an option DynamicEntity cannot
// express can patch it before marshalling.
func descriptorSet(specs ...server.DynamicEntity) *descriptorpb.FileDescriptorSet {
	GinkgoHelper()
	descFile := (&descriptorpb.FileDescriptorProto{}).ProtoReflect().Descriptor().ParentFile()
	set := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descFile),
			protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_options_proto),
		},
	}
	fd, err := server.BuildDynamicFile(specs...)
	Expect(err).NotTo(HaveOccurred())
	set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	return set
}

// descriptorSetBytes is descriptorSet marshalled for Manager.Register.
func descriptorSetBytes(specs ...server.DynamicEntity) []byte {
	GinkgoHelper()
	return marshalSet(descriptorSet(specs...))
}

func marshalSet(set *descriptorpb.FileDescriptorSet) []byte {
	GinkgoHelper()
	raw, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

// optIntoHistory marks the named message in the set's synthesized file as
// recording field history for one field — the (syncplane.v1.entity).history and
// (field_behavior).history pair DynamicEntity has no spelling for, and what
// makes registration pull the engine's own history entity into the catalog.
//
// The options are re-parsed rather than read with proto.GetExtension: a
// descriptor built by BuildDynamicFile and round-tripped through protodesc
// carries them, but nothing guarantees they come back as resolved extensions
// rather than unknown fields.
func optIntoHistory(set *descriptorpb.FileDescriptorSet, message, field string) {
	GinkgoHelper()
	last := set.GetFile()[len(set.GetFile())-1]
	for _, msg := range last.GetMessageType() {
		if msg.GetName() != message {
			continue
		}
		mopts := reparse(msg.GetOptions(), &descriptorpb.MessageOptions{})
		entity, _ := proto.GetExtension(mopts, syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
		Expect(entity.GetName()).NotTo(BeEmpty(), "message %s carries no entity options", message)
		entity.History = &syncplanev1.HistoryOptions{}
		proto.SetExtension(mopts, syncplanev1.E_Entity, entity)
		msg.Options = mopts

		for _, f := range msg.GetField() {
			if f.GetName() != field {
				continue
			}
			fopts := reparse(f.GetOptions(), &descriptorpb.FieldOptions{})
			behavior, _ := proto.GetExtension(fopts, syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior)
			if behavior == nil {
				behavior = &syncplanev1.FieldBehavior{}
			}
			behavior.History = true
			proto.SetExtension(fopts, syncplanev1.E_FieldBehavior, behavior)
			f.Options = fopts
			return
		}
		Fail("no field " + field + " on message " + message)
	}
	Fail("no message " + message + " in the synthesized file")
}

// reparse re-decodes an options message into dst, resolving extensions against
// the global registry. An absent src yields an empty dst.
func reparse[T proto.Message](src proto.Message, dst T) T {
	GinkgoHelper()
	if src == nil || !src.ProtoReflect().IsValid() {
		return dst
	}
	raw, err := proto.Marshal(src)
	Expect(err).NotTo(HaveOccurred())
	Expect(proto.Unmarshal(raw, dst)).To(Succeed())
	return dst
}
