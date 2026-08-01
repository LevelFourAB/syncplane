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

// descriptorSetBytes assembles the self-contained FileDescriptorSet an app
// registers (descriptor.proto + syncplane/v1/options.proto + one synthesized file
// holding every entity), mirroring the server test helper of the same name.
func descriptorSetBytes(specs ...server.DynamicEntity) []byte {
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
	raw, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return raw
}
