package lww_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// A polymorphic reference is a oneof of references: on the holder
// (OneofRefEntity.target, Attachment.parent) or inside a reference message
// (OwnerRef.owner, held by Memo). Each member is a case with its own Ref, and
// the accessors that read a stored value already report an inactive case as
// holding nothing.
var _ = Describe("Polymorphic references", func() {
	oneofRef := (&testdatav1.OneofRefEntity{}).ProtoReflect().Descriptor()
	memo := (&testdatav1.Memo{}).ProtoReflect().Descriptor()
	attachment := (&testdatav1.Attachment{}).ProtoReflect().Descriptor()
	field := func(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
		GinkgoHelper()
		fd := md.Fields().ByName(protoreflect.Name(name))
		Expect(fd).NotTo(BeNil())
		return fd
	}
	targets := func(refs []*lww.Ref) []string {
		out := make([]string, len(refs))
		for i, r := range refs {
			out[i] = string(r.Target.Name())
		}
		return out
	}

	Describe("classification", func() {
		It("classifies each member of a oneof on the holder as its own case", func() {
			project := lww.RefsOf(field(oneofRef, "project"))
			Expect(project).To(HaveLen(1))
			Expect(project[0].Group.Name()).To(BeEquivalentTo("target"))
			Expect(project[0].Polymorphic()).To(BeTrue())
			Expect(project[0].InnerCase()).To(BeFalse())
			Expect(project[0].CaseName()).To(Equal("project"))
			Expect(project[0].CaseJSONName()).To(Equal("project"))
			// A member is a reference to one entity on its own.
			Expect(lww.RefOf(field(oneofRef, "project"))).NotTo(BeNil())
			Expect(lww.RefOf(field(oneofRef, "user")).Target.Name()).To(BeEquivalentTo("User"))
		})

		It("classifies a reference message whose target is a oneof as one case per member", func() {
			owner := lww.RefsOf(field(memo, "owner"))
			Expect(targets(owner)).To(Equal([]string{"Project", "User"}))
			for _, r := range owner {
				Expect(r.Field.Name()).To(BeEquivalentTo("owner"))
				Expect(r.Workspace.Name()).To(BeEquivalentTo("workspace_id"))
				Expect(r.Qualified()).To(BeTrue())
				Expect(r.Polymorphic()).To(BeTrue())
				Expect(r.InnerCase()).To(BeTrue())
				Expect(r.Group).To(BeNil())
			}
			Expect(owner[0].CaseName()).To(Equal("project"))
			Expect(owner[0].KeyPathNames(owner[0].KeyFields[0])).To(Equal([]string{"project", "id"}))
			Expect(owner[1].KeyPathNames(owner[1].KeyFields[0])).To(Equal([]string{"user", "id"}))
			// No single target, so the one-entity view declines while the
			// field is still a reference for every consumer that asks.
			Expect(lww.RefOf(field(memo, "owner"))).To(BeNil())
			Expect(lww.RefKeyFields(field(memo, "owner"))).To(BeNil())
			Expect(lww.IsRef(field(memo, "owner"))).To(BeTrue())
			Expect(lww.IsRefMessageField(field(memo, "owner"))).To(BeTrue())
			Expect(lww.IsRefMessageField(field(memo, "mentions"))).To(BeTrue())
		})

		It("keeps a plain member of a mixed oneof out of the cases", func() {
			Expect(lww.RefsOf(field(attachment, "label"))).To(BeEmpty())
			project := lww.RefOf(field(attachment, "project"))
			user := lww.RefOf(field(attachment, "user"))
			Expect(project.Group.Name()).To(BeEquivalentTo("parent"))
			Expect(project.Qualified()).To(BeFalse())
			Expect(user.Group.Name()).To(BeEquivalentTo("parent"))
			Expect(user.Qualified()).To(BeTrue())
			Expect(user.Value.Name()).To(BeEquivalentTo("user"))
		})

		It("reads only the case a stored value holds", func() {
			owner := lww.RefsOf(field(memo, "owner"))
			value := (&testdatav1.OwnerRef{
				WorkspaceId: "org",
				Owner:       &testdatav1.OwnerRef_User{User: &testdatav1.User{Id: "u1"}},
			}).ProtoReflect()
			_, ok := owner[0].Stub(value)
			Expect(ok).To(BeFalse(), "the project case is not the active one")
			stub, ok := owner[1].Stub(value)
			Expect(ok).To(BeTrue())
			Expect(stub.Get(owner[1].KeyFields[0]).String()).To(Equal("u1"))
			Expect(owner[1].WorkspaceOf(value)).To(Equal("org"))
		})
	})

	Describe("element identity", func() {
		mentions := func() protoreflect.FieldDescriptor { return field(memo, "mentions") }

		It("puts the case between the workspace and the key", func() {
			Expect(lww.RefItemKey(mentions(), (&testdatav1.OwnerRef{
				WorkspaceId: "org",
				Owner:       &testdatav1.OwnerRef_Project{Project: &testdatav1.Project{Id: "p/1"}},
			}).ProtoReflect())).To(Equal(`org/project/p\/1`))
			Expect(lww.RefItemKey(mentions(), (&testdatav1.OwnerRef{
				Owner: &testdatav1.OwnerRef_User{User: &testdatav1.User{Id: "p/1"}},
			}).ProtoReflect())).To(Equal(`/user/p\/1`))
		})

		It("renders a value holding no case with an empty case and no key", func() {
			Expect(lww.RefItemKey(mentions(), (&testdatav1.OwnerRef{WorkspaceId: "org"}).ProtoReflect())).To(Equal("org//"))
			Expect(lww.RefItemKey(mentions(), (&testdatav1.OwnerRef{}).ProtoReflect())).To(Equal("//"))
		})

		It("renders a reference message to one entity exactly as before", func() {
			watchers := field((&testdatav1.Task{}).ProtoReflect().Descriptor(), "watchers")
			Expect(lww.RefItemKey(watchers, (&testdatav1.UserRef{
				WorkspaceId: "org", User: &testdatav1.User{Id: "u1"},
			}).ProtoReflect())).To(Equal("org/u1"))
		})
	})

	Describe("merge", func() {
		project := func(ws, id string) *testdatav1.OwnerRef {
			return &testdatav1.OwnerRef{WorkspaceId: ws, Owner: &testdatav1.OwnerRef_Project{Project: &testdatav1.Project{Id: id}}}
		}
		user := func(ws, id string) *testdatav1.OwnerRef {
			return &testdatav1.OwnerRef{WorkspaceId: ws, Owner: &testdatav1.OwnerRef_User{User: &testdatav1.User{Id: id}}}
		}

		It("replaces a oneof of references on the holder as a group", func() {
			dst := &testdatav1.OneofRefEntity{Id: "r1", Target: &testdatav1.OneofRefEntity_Project{Project: &testdatav1.Project{Id: "p1"}}}
			src := &testdatav1.OneofRefEntity{Id: "r1", Target: &testdatav1.OneofRefEntity_User{User: &testdatav1.User{Id: "u1"}}}
			Expect(lww.Merge(dst, src, []string{"target"})).To(Succeed())
			Expect(dst.GetUser().GetId()).To(Equal("u1"))
			Expect(dst.GetProject()).To(BeNil())
			// A member is not a mask unit, the group is.
			Expect(lww.Merge(dst, src, []string{"project"})).To(MatchError(ContainSubstring(`mask the oneof instead`)))
		})

		It("replaces a polymorphic reference message whole", func() {
			dst := &testdatav1.Memo{Id: "n1", Owner: project("org", "p1")}
			src := &testdatav1.Memo{Id: "n1", Owner: user("", "u1")}
			Expect(lww.Merge(dst, src, []string{"owner.workspace_id"})).To(Succeed())
			Expect(dst.GetOwner().GetWorkspaceId()).To(BeEmpty())
			Expect(dst.GetOwner().GetUser().GetId()).To(Equal("u1"))
			Expect(dst.GetOwner().GetProject()).To(BeNil())
		})

		It("merges a repeated polymorphic reference message as a set keyed by case and key", func() {
			dst := &testdatav1.Memo{Id: "n1", Mentions: []*testdatav1.OwnerRef{project("org", "x"), user("org", "x")}}
			// Remove the project case of x, keep the user case, add a user u2.
			src := &testdatav1.Memo{Id: "n1", Mentions: []*testdatav1.OwnerRef{user("org", "x"), user("", "u2")}}
			Expect(lww.Merge(dst, src, []string{"mentions.org/project/x", "mentions./user/u2"})).To(Succeed())
			Expect(dst.GetMentions()).To(HaveLen(2))
			Expect(proto.Equal(dst.GetMentions()[0], user("org", "x"))).To(BeTrue())
			Expect(proto.Equal(dst.GetMentions()[1], user("", "u2"))).To(BeTrue())
		})
	})

	Describe("reference message rules", func() {
		refOpts := func() *descriptorpb.MessageOptions {
			o := &descriptorpb.MessageOptions{}
			proto.SetExtension(o, syncplanev1.E_Reference, &syncplanev1.ReferenceOptions{})
			return o
		}
		entityOpts := func(name string) *descriptorpb.MessageOptions {
			o := &descriptorpb.MessageOptions{}
			proto.SetExtension(o, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: name, KeyFields: []string{"id"}})
			return o
		}
		workspaceOpts := func() *descriptorpb.FieldOptions {
			o := &descriptorpb.FieldOptions{}
			proto.SetExtension(o, syncplanev1.E_FieldBehavior, &syncplanev1.FieldBehavior{Workspace: true})
			return o
		}
		str := func(name string, number int32, opts *descriptorpb.FieldOptions, oneof *int32) *descriptorpb.FieldDescriptorProto {
			return &descriptorpb.FieldDescriptorProto{
				Name: proto.String(name), Number: proto.Int32(number),
				Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Options:    opts,
				OneofIndex: oneof,
			}
		}
		msg := func(name string, number int32, typeName string, oneof *int32) *descriptorpb.FieldDescriptorProto {
			return &descriptorpb.FieldDescriptorProto{
				Name: proto.String(name), Number: proto.Int32(number),
				Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName:   proto.String(typeName),
				OneofIndex: oneof,
			}
		}
		// build compiles a reference message with the given fields and oneofs
		// beside two entities to point at, and checks it.
		build := func(fields []*descriptorpb.FieldDescriptorProto, oneofs ...string) error {
			GinkgoHelper()
			decls := make([]*descriptorpb.OneofDescriptorProto, len(oneofs))
			for i, name := range oneofs {
				decls[i] = &descriptorpb.OneofDescriptorProto{Name: proto.String(name)}
			}
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name:    proto.String("poly/refs.proto"),
				Package: proto.String("poly"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("A"), Options: entityOpts("a"), Field: []*descriptorpb.FieldDescriptorProto{str("id", 1, nil, nil)}},
					{Name: proto.String("B"), Options: entityOpts("b"), Field: []*descriptorpb.FieldDescriptorProto{str("id", 1, nil, nil)}},
					{Name: proto.String("Ref"), Options: refOpts(), Field: fields, OneofDecl: decls},
				},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			return lww.CheckReferenceMessage(fd.Messages().ByName("Ref"))
		}

		It("accepts a oneof of entity-typed fields beside a workspace", func() {
			Expect(build([]*descriptorpb.FieldDescriptorProto{
				str("workspace_id", 1, workspaceOpts(), nil),
				msg("a", 2, ".poly.A", proto.Int32(0)),
				msg("b", 3, ".poly.B", proto.Int32(0)),
			}, "target")).To(Succeed())
		})

		It("refuses a workspace field inside the oneof", func() {
			Expect(build([]*descriptorpb.FieldDescriptorProto{
				str("workspace_id", 1, workspaceOpts(), proto.Int32(0)),
				msg("a", 2, ".poly.A", proto.Int32(0)),
			}, "target")).To(MatchError(ContainSubstring(`workspace field "workspace_id" is a member of oneof "target"`)))
		})

		It("refuses entity-typed fields that are not the members of one oneof", func() {
			Expect(build([]*descriptorpb.FieldDescriptorProto{
				msg("a", 1, ".poly.A", nil),
				msg("b", 2, ".poly.B", proto.Int32(0)),
			}, "target")).To(MatchError(ContainSubstring(`fields "a" and "b" are both entity-typed; a reference names one target, or one of several as the members of a single oneof`)))
			Expect(build([]*descriptorpb.FieldDescriptorProto{
				msg("a", 1, ".poly.A", proto.Int32(0)),
				msg("b", 2, ".poly.B", proto.Int32(1)),
			}, "first", "second")).To(MatchError(ContainSubstring(`fields "a" and "b" are both entity-typed`)))
		})

		It("refuses a plain member inside the oneof", func() {
			Expect(build([]*descriptorpb.FieldDescriptorProto{
				msg("a", 1, ".poly.A", proto.Int32(0)),
				str("label", 2, nil, proto.Int32(0)),
			}, "target")).To(MatchError(ContainSubstring(`field "label" is neither the entity field nor a workspace field`)))
		})
	})
})
