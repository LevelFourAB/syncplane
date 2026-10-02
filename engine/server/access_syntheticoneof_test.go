package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A proto3 optional field sits in a synthetic oneof named after it with a
// leading underscore. The model treats the field itself as the unit a guard
// restricts, so a name of the synthetic oneof must not pass for a oneof the
// guard restricts, which would leave the field open.
var _ = Describe("Access model fields named through a synthetic oneof", func() {
	// optionalStatus returns the descriptor set of the org schema in which
	// task carries `optional string status = 3`, and guard restricts fields.
	optionalStatus := func(fields []string) []byte {
		GinkgoHelper()
		specs := orgSpecs()
		task := specNamed(specs, "task")
		task.Fields = append(task.Fields, server.DynamicField{Name: "status", Number: 3, Kind: protoreflect.StringKind})
		task.Access.Guard = []*syncplanev1.FieldGuard{{
			Fields: fields,
			Allow:  []*syncplanev1.AccessRule{permRule("edit_tasks", "read", "write")},
		}}
		set := &descriptorpb.FileDescriptorSet{}
		Expect(proto.Unmarshal(descriptorSetBytes(specs...), set)).To(Succeed())
		found := false
		for _, file := range set.GetFile() {
			for _, m := range file.GetMessageType() {
				if m.GetName() != "Task" {
					continue
				}
				for _, f := range m.GetField() {
					if f.GetName() == "status" {
						f.Proto3Optional = proto.Bool(true)
						f.OneofIndex = proto.Int32(int32(len(m.GetOneofDecl())))
						m.OneofDecl = append(m.OneofDecl, &descriptorpb.OneofDescriptorProto{Name: proto.String("_status")})
						found = true
					}
				}
			}
		}
		Expect(found).To(BeTrue())
		raw, err := proto.Marshal(set)
		Expect(err).NotTo(HaveOccurred())
		return raw
	}

	It("refuses a guard that names the synthetic oneof of an optional field", func() {
		// Given a guard on task that names _status, the synthetic oneof of
		// the optional status field
		srv := newBareServer()

		// When the schema registers
		_, err := srv.RegisterDescriptorSet(optionalStatus([]string{"_status"}))

		// Then registration refuses the name, since status is the unit that
		// writes and reads change
		Expect(err).To(MatchError(ContainSubstring(`"_status" is not a field or a oneof`)))
	})

	It("accepts a guard that names the optional field itself", func() {
		srv := newBareServer()
		_, err := srv.RegisterDescriptorSet(optionalStatus([]string{"status"}))
		Expect(err).NotTo(HaveOccurred())
	})
})
