package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
)

// The dynamic tier reaches full parity with the compiled proto tier: embedded
// sub-messages, typed references (to-one, to-many, self), keyed/movable message
// lists, and scalar maps — all synthesized from runtime descriptors with no
// generated Go. These specs mirror syncplane/testdata/v1's Issue/Project/User
// reference fixtures, built through the DynamicEntity struct path.
var _ = Describe("Dynamic entity parity", func() {
	project := server.DynamicEntity{
		Name: "project", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	user := server.DynamicEntity{
		Name: "user", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	issue := server.DynamicEntity{
		Name: "issue", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "settings", Number: 3, Message: []server.DynamicField{
				{Name: "color", Number: 1, Kind: protoreflect.StringKind},
				{Name: "size", Number: 2, Kind: protoreflect.Int32Kind},
			}},
			{Name: "project", Number: 4, Ref: "project"},
			{Name: "assignee", Number: 5, Ref: "user"},
			{Name: "reporter", Number: 6, Ref: "user"},
			{Name: "blocked_by", Number: 7, Ref: "issue", Repeated: true},
			{Name: "members", Number: 8, Repeated: true, KeyField: []string{"uid"}, Message: []server.DynamicField{
				{Name: "uid", Number: 1, Kind: protoreflect.StringKind},
				{Name: "name", Number: 2, Kind: protoreflect.StringKind},
			}},
			{Name: "items", Number: 9, Repeated: true, KeyField: []string{"id"}, OrderField: "pos", Message: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "pos", Number: 2, Kind: protoreflect.StringKind},
				{Name: "label", Number: 3, Kind: protoreflect.StringKind},
			}},
			{Name: "attrs", Number: 10, MapKey: protoreflect.StringKind, MapValue: protoreflect.StringKind},
		},
	}
	refSpecs := []server.DynamicEntity{project, user, issue}

	Describe("BuildDynamicFile", func() {
		var fd protoreflect.FileDescriptor
		var issueMD protoreflect.MessageDescriptor

		BeforeEach(func() {
			var err error
			fd, err = server.BuildDynamicFile(refSpecs...)
			Expect(err).NotTo(HaveOccurred())
			issueMD = fd.Messages().ByName("Issue")
			Expect(issueMD).NotTo(BeNil())
		})

		It("synthesizes one file with every entity as a top-level message", func() {
			names := []string{}
			for i := 0; i < fd.Messages().Len(); i++ {
				names = append(names, string(fd.Messages().Get(i).Name()))
			}
			Expect(names).To(ConsistOf("Project", "User", "Issue"))
		})

		It("emits an embedded sub-message as a nested type, not a reference", func() {
			fdr := issueMD.Fields().ByName("settings")
			Expect(fdr.Kind()).To(Equal(protoreflect.MessageKind))
			Expect(lww.IsRef(fdr)).To(BeFalse(), "Settings is not an entity")
			Expect(string(fdr.Message().Name())).To(Equal("Settings"))
		})

		It("detects to-one, to-many and self references annotation-free", func() {
			pr := issueMD.Fields().ByName("project")
			Expect(lww.IsRef(pr)).To(BeTrue())
			kf := lww.RefKeyFields(pr)
			Expect(kf).To(HaveLen(1))
			Expect(string(kf[0].Name())).To(Equal("id"))

			self := issueMD.Fields().ByName("blocked_by")
			Expect(lww.IsRef(self)).To(BeTrue())
			Expect(self.IsList()).To(BeTrue())
		})

		It("carries key_field / order_field behavior on keyed and movable lists", func() {
			members := issueMD.Fields().ByName("members")
			mb := fieldBehaviorOf(members)
			Expect(mb.GetKeyField()).To(Equal([]string{"uid"}))
			Expect(mb.GetOrderField()).To(BeEmpty())

			items := issueMD.Fields().ByName("items")
			ib := fieldBehaviorOf(items)
			Expect(ib.GetKeyField()).To(Equal([]string{"id"}))
			Expect(ib.GetOrderField()).To(Equal("pos"))
		})

		It("synthesizes a scalar map the runtime recognizes as a map", func() {
			attrs := issueMD.Fields().ByName("attrs")
			Expect(attrs.IsMap()).To(BeTrue())
			Expect(attrs.MapKey().Kind()).To(Equal(protoreflect.StringKind))
			Expect(attrs.MapValue().Kind()).To(Equal(protoreflect.StringKind))
		})

		DescribeTable("rejects invalid reference/key shapes",
			func(mutate func(specs []server.DynamicEntity), msg string) {
				specs := cloneSpecs(refSpecs)
				mutate(specs)
				_, err := server.BuildDynamicFile(specs...)
				Expect(err).To(MatchError(ContainSubstring(msg)))
			},
			Entry("reference to an unknown entity", func(s []server.DynamicEntity) {
				s[2].Fields[3].Ref = "ghost"
			}, "references unknown entity"),
			Entry("message-typed key field", func(s []server.DynamicEntity) {
				s[2].KeyFields = []string{"settings"}
			}, "must be a singular scalar"),
			Entry("order_field without a keyed list", func(s []server.DynamicEntity) {
				s[2].Fields[2].OrderField = "color" // settings is a singular embedded message
			}, "order_field requires a keyed message list"),
		)
	})

	Describe("RegisterDescriptorSet with references", func() {
		var (
			ctx context.Context
			srv *server.Server
		)
		BeforeEach(func() {
			ctx = context.Background()
			srv = newBareServer()
		})
		push := func(muts ...*syncplanev1.Mutation) {
			GinkgoHelper()
			_, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
			Expect(err).NotTo(HaveOccurred())
		}
		pullRows := func() map[string]*syncplanev1.RowChange {
			GinkgoHelper()
			resp, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred())
			out := map[string]*syncplanev1.RowChange{}
			for _, c := range resp.Msg.GetPatch().GetChanges() {
				out[c.GetKey()] = c
			}
			return out
		}

		It("ingests the referencing schema and projects refs to key fields", func() {
			names, err := srv.RegisterDescriptorSet(descriptorSetBytes(refSpecs...))
			Expect(err).NotTo(HaveOccurred())
			Expect(names).To(Equal([]string{"issue", "project", "user"}))

			issueMT := dynamicMessageType(refSpecs, "Issue")
			row := issueMT.New()
			fds := issueMT.Descriptor().Fields()
			row.Set(fds.ByName("id"), protoreflect.ValueOfString("i1"))
			row.Set(fds.ByName("title"), protoreflect.ValueOfString("a bug"))
			// A fat ref stub: both id and name set. The server must store only id.
			proj := row.Mutable(fds.ByName("project")).Message()
			proj.Set(proj.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("p1"))
			proj.Set(proj.Descriptor().Fields().ByName("name"), protoreflect.ValueOfString("should be stripped"))
			push(putMutation("c1", 1, "issue", "i1", row.Interface()))

			stored := issueMT.New().Interface()
			Expect(proto.Unmarshal(pullRows()["i1"].GetValue(), stored)).To(Succeed())
			gotProj := stored.ProtoReflect().Get(fds.ByName("project")).Message()
			Expect(gotProj.Get(gotProj.Descriptor().Fields().ByName("id")).String()).To(Equal("p1"))
			Expect(gotProj.Get(gotProj.Descriptor().Fields().ByName("name")).String()).
				To(BeEmpty(), "ref projection strips the non-key field")
		})

		It("accepts an additive evolution that appends a message field", func() {
			_, err := srv.RegisterDescriptorSet(descriptorSetBytes(refSpecs...))
			Expect(err).NotTo(HaveOccurred())
			before := srvRevision(srv, ctx)

			evolved := cloneSpecs(refSpecs)
			evolved[2].Fields = append(evolved[2].Fields, server.DynamicField{
				Name: "meta", Number: 11, Message: []server.DynamicField{
					{Name: "note", Number: 1, Kind: protoreflect.StringKind},
				},
			})
			_, err = srv.RegisterDescriptorSet(descriptorSetBytes(evolved...))
			Expect(err).NotTo(HaveOccurred())
			Expect(srvRevision(srv, ctx)).NotTo(Equal(before))
		})

		It("rejects retargeting a reference to a different entity", func() {
			_, err := srv.RegisterDescriptorSet(descriptorSetBytes(refSpecs...))
			Expect(err).NotTo(HaveOccurred())
			rev := srvRevision(srv, ctx)

			retargeted := cloneSpecs(refSpecs)
			retargeted[2].Fields[3].Ref = "user" // project -> user, same field number 4
			_, err = srv.RegisterDescriptorSet(descriptorSetBytes(retargeted...))
			Expect(err).To(MatchError(ContainSubstring("message type changed")))
			Expect(srvRevision(srv, ctx)).To(Equal(rev), "a rejected change does not partially apply")
		})

		It("yields a stable revision on idempotent re-registration", func() {
			_, err := srv.RegisterDescriptorSet(descriptorSetBytes(refSpecs...))
			Expect(err).NotTo(HaveOccurred())
			rev := srvRevision(srv, ctx)
			_, err = srv.RegisterDescriptorSet(descriptorSetBytes(refSpecs...))
			Expect(err).NotTo(HaveOccurred())
			Expect(srvRevision(srv, ctx)).To(Equal(rev))
		})
	})

	It("rejects a descriptor set whose key field is message-typed (buildEntityInfo)", func() {
		srv := newBareServer()
		_, err := srv.RegisterDescriptorSet(messageKeyDescriptorSet())
		Expect(err).To(MatchError(ContainSubstring("must be a string, integer, bool or enum")))
	})
})

// fieldBehaviorOf reads the (syncplane.v1.field_behavior) extension off a field
// descriptor, or a zero value when absent.
func fieldBehaviorOf(fd protoreflect.FieldDescriptor) *syncplanev1.FieldBehavior {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	if !ok || opts == nil {
		return &syncplanev1.FieldBehavior{}
	}
	if b, ok := proto.GetExtension(opts, syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior); ok && b != nil {
		return b
	}
	return &syncplanev1.FieldBehavior{}
}

// dynamicMessageType builds the named message of a multi-entity registration.
func dynamicMessageType(specs []server.DynamicEntity, name string) protoreflect.MessageType {
	GinkgoHelper()
	fd, err := server.BuildDynamicFile(specs...)
	Expect(err).NotTo(HaveOccurred())
	md := fd.Messages().ByName(protoreflect.Name(name))
	Expect(md).NotTo(BeNil())
	return dynamicpb.NewMessageType(md)
}

// cloneSpecs deep-copies the entity specs (and their field slices) so a table
// entry can mutate one without disturbing the shared fixture.
func cloneSpecs(specs []server.DynamicEntity) []server.DynamicEntity {
	out := make([]server.DynamicEntity, len(specs))
	for i, s := range specs {
		s.KeyFields = append([]string(nil), s.KeyFields...)
		s.Fields = append([]server.DynamicField(nil), s.Fields...)
		out[i] = s
	}
	return out
}

// srvRevision reads the live schema revision off a Pull.
func srvRevision(srv *server.Server, ctx context.Context) string {
	GinkgoHelper()
	resp, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
	Expect(err).NotTo(HaveOccurred())
	return resp.Msg.GetSchemaRevision()
}

// messageKeyDescriptorSet hand-rolls a descriptor set whose entity names a
// message-typed field in key_fields — a shape the struct path's own guard
// would reject earlier, so we build it raw to exercise buildEntityInfo.
func messageKeyDescriptorSet() []byte {
	GinkgoHelper()
	descFile := (&descriptorpb.FileDescriptorProto{}).ProtoReflect().Descriptor().ParentFile()
	mopts := &descriptorpb.MessageOptions{}
	proto.SetExtension(mopts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{
		Name: "bad", KeyFields: []string{"nested"},
	})
	entity := &descriptorpb.DescriptorProto{
		Name:    proto.String("Bad"),
		Options: mopts,
		Field: []*descriptorpb.FieldDescriptorProto{{
			Name:     proto.String("nested"),
			Number:   proto.Int32(1),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".sync.dynamic.Bad.Nested"),
		}},
		NestedType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Nested"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("v"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}},
		}},
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("sync/dynamic/schema.proto"),
		Package:     proto.String("sync.dynamic"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"syncplane/v1/options.proto"},
		MessageType: []*descriptorpb.DescriptorProto{entity},
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{
		protodesc.ToFileDescriptorProto(descFile),
		protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_query_proto),
		protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_options_proto),
		file,
	}}
	raw, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return raw
}
