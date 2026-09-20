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

// A reference message (UserRef) wraps a User stub together with the workspace
// it lives in. Ref describes both stored shapes through one model, so every
// consumer addresses the key and the workspace the same way whatever the
// field's type is.
var _ = Describe("Workspace-qualified references", func() {
	task := (&testdatav1.Task{}).ProtoReflect().Descriptor()
	issue := (&testdatav1.Issue{}).ProtoReflect().Descriptor()
	field := func(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
		GinkgoHelper()
		fd := md.Fields().ByName(protoreflect.Name(name))
		Expect(fd).NotTo(BeNil())
		return fd
	}

	It("describes a reference message field by its target, key path and workspace", func() {
		ref := lww.RefOf(field(task, "assignee"))
		Expect(ref).NotTo(BeNil())
		Expect(ref.Target.Name()).To(BeEquivalentTo("User"))
		Expect(ref.Value.Name()).To(BeEquivalentTo("user"))
		Expect(ref.Workspace.Name()).To(BeEquivalentTo("workspace_id"))
		Expect(ref.Qualified()).To(BeTrue())
		Expect(ref.KeyPathNames(ref.KeyFields[0])).To(Equal([]string{"user", "id"}))
		Expect(lww.IsRef(field(task, "assignee"))).To(BeTrue())
		Expect(lww.IsRefMessageField(field(task, "assignee"))).To(BeTrue())
		Expect(lww.IsRefMessageField(field(task, "watchers"))).To(BeTrue())
		Expect(lww.RefKeyFields(field(task, "assignee"))[0].Name()).To(BeEquivalentTo("id"))
	})

	It("keeps an entity-typed reference's key on the stored value itself", func() {
		ref := lww.RefOf(field(issue, "project"))
		Expect(ref).NotTo(BeNil())
		Expect(ref.Value).To(BeNil())
		Expect(ref.Workspace).To(BeNil())
		Expect(ref.Qualified()).To(BeFalse())
		Expect(ref.KeyPathNames(ref.KeyFields[0])).To(Equal([]string{"id"}))
		Expect(lww.IsRefMessageField(field(issue, "project"))).To(BeFalse())
	})

	It("reads the stub and the workspace out of a stored value", func() {
		ref := lww.RefOf(field(task, "assignee"))
		id := ref.KeyFields[0]

		value := (&testdatav1.UserRef{WorkspaceId: "org", User: &testdatav1.User{Id: "u1"}}).ProtoReflect()
		stub, ok := ref.Stub(value)
		Expect(ok).To(BeTrue())
		Expect(stub.Get(id).String()).To(Equal("u1"))
		Expect(ref.WorkspaceOf(value)).To(Equal("org"))
		Expect(ref.ResolvesIn(value, "org")).To(BeTrue())
		Expect(ref.ResolvesIn(value, "proj")).To(BeFalse())

		bare := (&testdatav1.UserRef{User: &testdatav1.User{Id: "u1"}}).ProtoReflect()
		Expect(ref.WorkspaceOf(bare)).To(BeEmpty())
		Expect(ref.ResolvesIn(bare, "proj")).To(BeTrue(), "no workspace means the holder's own")

		empty := (&testdatav1.UserRef{WorkspaceId: "org"}).ProtoReflect()
		_, ok = ref.Stub(empty)
		Expect(ok).To(BeFalse(), "a reference message without its stub points at nothing")
	})

	It("admits only the entity and workspace fields below a reference message", func() {
		ref := lww.RefOf(field(task, "assignee"))
		Expect(ref.Addressable("user")).To(BeTrue())
		Expect(ref.Addressable("workspace_id")).To(BeTrue())
		Expect(ref.Addressable("id")).To(BeFalse(), "the key sits under the entity field")
		Expect(ref.AddressableNames()).To(Equal([]string{"user", "workspace_id"}))

		plain := lww.RefOf(field(issue, "project"))
		Expect(plain.Addressable("id")).To(BeTrue())
		Expect(plain.Addressable("name")).To(BeFalse())
	})

	It("replaces a reference message whole when a mask reaches into it", func() {
		base := &testdatav1.Task{
			Id:       "t1",
			Assignee: &testdatav1.UserRef{WorkspaceId: "org", User: &testdatav1.User{Id: "u1"}},
		}
		src := &testdatav1.Task{Assignee: &testdatav1.UserRef{User: &testdatav1.User{Id: "u2"}}}
		Expect(lww.Merge(base, src, []string{"assignee.workspace_id"})).To(Succeed())
		// The workspace and the key move together: the source's empty workspace
		// lands beside its key, never the old workspace beside a new key.
		Expect(base.GetAssignee().GetWorkspaceId()).To(BeEmpty())
		Expect(base.GetAssignee().GetUser().GetId()).To(Equal("u2"))
	})

	// A repeated reference message merges as a SET keyed by the stored value —
	// the workspace beside the target key — so two writers that add and remove
	// different elements both land, as they do on a repeated entity-typed
	// reference. Nothing else in an element could carry a change: the stored
	// value is projected down to exactly those fields.
	Describe("a repeated reference message", func() {
		uref := func(ws, id string) *testdatav1.UserRef {
			return &testdatav1.UserRef{WorkspaceId: ws, User: &testdatav1.User{Id: id}}
		}
		names := func(t *testdatav1.Task) []string {
			out := make([]string, 0, len(t.GetWatchers()))
			for _, w := range t.GetWatchers() {
				out = append(out, w.GetWorkspaceId()+"/"+w.GetUser().GetId())
			}
			return out
		}
		watchers := field(task, "watchers")

		It("renders an element's identity as its workspace beside its key", func() {
			r := lww.RefOf(watchers)
			Expect(r.ItemKey(uref("org", "u1").ProtoReflect())).To(Equal("org/u1"))
			// An empty workspace is a part of its own, so it never collides with
			// a key that happens to contain the separator.
			Expect(r.ItemKey(uref("", "u1").ProtoReflect())).To(Equal("/u1"))
			Expect(r.ItemKey(uref("a/b", "c.d").ProtoReflect())).To(Equal(`a\/b/c\.d`))
			Expect(lww.KeyFieldsFor(watchers)).To(BeEmpty(), "the identity is not a key-field list")
		})

		It("merges per element, so an add and a remove from different writers both land", func() {
			base := &testdatav1.Task{Id: "t1", Watchers: []*testdatav1.UserRef{uref("org", "u1"), uref("org", "u2")}}
			fromA := &testdatav1.Task{Watchers: []*testdatav1.UserRef{uref("org", "u2")}}
			Expect(lww.Merge(base, fromA, []string{"watchers.org/u1"})).To(Succeed())
			fromB := &testdatav1.Task{Watchers: []*testdatav1.UserRef{uref("org", "u1"), uref("org", "u2"), uref("org", "u3")}}
			Expect(lww.Merge(base, fromB, []string{"watchers.org/u3"})).To(Succeed())
			Expect(names(base)).To(Equal([]string{"org/u2", "org/u3"}))
		})

		It("keeps the same key in two workspaces as two elements", func() {
			base := &testdatav1.Task{Id: "t1", Watchers: []*testdatav1.UserRef{uref("org", "u1")}}
			src := &testdatav1.Task{Watchers: []*testdatav1.UserRef{uref("", "u1")}}
			Expect(lww.Merge(base, src, []string{"watchers./u1"})).To(Succeed())
			Expect(names(base)).To(Equal([]string{"org/u1", "/u1"}))
			Expect(lww.Merge(base, &testdatav1.Task{}, []string{"watchers.org/u1"})).To(Succeed())
			Expect(names(base)).To(Equal([]string{"/u1"}))
		})

		It("holds each element once, whatever either side repeats", func() {
			base := &testdatav1.Task{Id: "t1", Watchers: []*testdatav1.UserRef{uref("org", "u1"), uref("org", "u1")}}
			src := &testdatav1.Task{Watchers: []*testdatav1.UserRef{uref("org", "u1"), uref("org", "u1")}}
			Expect(lww.Merge(base, src, []string{"watchers.org/u1"})).To(Succeed())
			Expect(names(base)).To(Equal([]string{"org/u1"}))
			Expect(lww.Merge(base, &testdatav1.Task{}, []string{"watchers.org/u1"})).To(Succeed())
			Expect(names(base)).To(BeEmpty())
		})

		It("addresses the whole element when a mask reaches below it", func() {
			base := &testdatav1.Task{Id: "t1"}
			src := &testdatav1.Task{Watchers: []*testdatav1.UserRef{uref("org", "u1")}}
			Expect(lww.Merge(base, src, []string{"watchers.org/u1.user.id"})).To(Succeed())
			Expect(names(base)).To(Equal([]string{"org/u1"}))
		})

		It("replaces the list whole when the mask names the field bare", func() {
			base := &testdatav1.Task{Id: "t1", Watchers: []*testdatav1.UserRef{uref("org", "u1")}}
			src := &testdatav1.Task{Watchers: []*testdatav1.UserRef{uref("org", "u2")}}
			Expect(lww.Merge(base, src, []string{"watchers"})).To(Succeed())
			Expect(names(base)).To(Equal([]string{"org/u2"}))
		})
	})

	Describe("a malformed reference message", func() {
		stringField := func(name string, number int32, workspace bool) *descriptorpb.FieldDescriptorProto {
			f := &descriptorpb.FieldDescriptorProto{
				Name:   proto.String(name),
				Number: proto.Int32(number),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}
			if workspace {
				opts := &descriptorpb.FieldOptions{}
				proto.SetExtension(opts, syncplanev1.E_FieldBehavior, &syncplanev1.FieldBehavior{Workspace: true})
				f.Options = opts
			}
			return f
		}
		userField := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
			return &descriptorpb.FieldDescriptorProto{
				Name:     proto.String(name),
				Number:   proto.Int32(number),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".t.User"),
			}
		}
		// build synthesizes a file holding a User entity, a Ref message declaring
		// (syncplane.v1.reference) with the given fields, and a Holder whose
		// `ref` field is of that type. It returns the Ref descriptor and the
		// holder field.
		build := func(refFields []*descriptorpb.FieldDescriptorProto, alsoEntity bool) (protoreflect.MessageDescriptor, protoreflect.FieldDescriptor) {
			GinkgoHelper()
			userOpts := &descriptorpb.MessageOptions{}
			proto.SetExtension(userOpts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: "user", KeyFields: []string{"id"}})
			refOpts := &descriptorpb.MessageOptions{}
			proto.SetExtension(refOpts, syncplanev1.E_Reference, &syncplanev1.ReferenceOptions{})
			if alsoEntity {
				proto.SetExtension(refOpts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{Name: "ref", KeyFields: []string{"workspace_id"}})
			}
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name:    proto.String("t/refs.proto"),
				Package: proto.String("t"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("User"), Options: userOpts, Field: []*descriptorpb.FieldDescriptorProto{stringField("id", 1, false)}},
					{Name: proto.String("Ref"), Options: refOpts, Field: refFields},
					{Name: proto.String("Holder"), Field: []*descriptorpb.FieldDescriptorProto{{
						Name:     proto.String("ref"),
						Number:   proto.Int32(1),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String(".t.Ref"),
					}}},
				},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			return fd.Messages().ByName("Ref"), fd.Messages().ByName("Holder").Fields().ByName("ref")
		}

		It("is well-formed with an entity field alone, or beside one workspace field", func() {
			ref, holder := build([]*descriptorpb.FieldDescriptorProto{userField("user", 1)}, false)
			Expect(lww.CheckReferenceMessage(ref)).To(Succeed())
			Expect(lww.RefOf(holder).Qualified()).To(BeFalse())

			ref, holder = build([]*descriptorpb.FieldDescriptorProto{stringField("workspace_id", 1, true), userField("user", 2)}, false)
			Expect(lww.CheckReferenceMessage(ref)).To(Succeed())
			Expect(lww.RefOf(holder).Qualified()).To(BeTrue())
		})

		DescribeTable("is reported, and its holder field is not a reference",
			func(fields []*descriptorpb.FieldDescriptorProto, alsoEntity bool, want string) {
				ref, holder := build(fields, alsoEntity)
				Expect(lww.CheckReferenceMessage(ref)).To(MatchError(ContainSubstring(want)))
				Expect(lww.RefOf(holder)).To(BeNil())
			},
			Entry("an extra field", []*descriptorpb.FieldDescriptorProto{userField("user", 1), stringField("label", 2, false)}, false,
				`field "label" is neither the entity field nor a workspace field`),
			Entry("two workspace fields", []*descriptorpb.FieldDescriptorProto{stringField("a", 1, true), stringField("b", 2, true), userField("user", 3)}, false,
				`both marked workspace`),
			Entry("two entity fields", []*descriptorpb.FieldDescriptorProto{userField("a", 1), userField("b", 2)}, false,
				`both entity-typed`),
			Entry("no entity field", []*descriptorpb.FieldDescriptorProto{stringField("workspace_id", 1, true)}, false,
				`has no entity-typed field`),
			Entry("a reference message that is also an entity", []*descriptorpb.FieldDescriptorProto{stringField("workspace_id", 1, true), userField("user", 2)}, true,
				`is also an entity`),
		)

		It("rejects a workspace field outside a reference message", func() {
			// Stray declares no reference option, so a workspace flag on one of
			// its fields has nothing to attach to.
			fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name:    proto.String("t/stray.proto"),
				Package: proto.String("t"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("Stray"), Field: []*descriptorpb.FieldDescriptorProto{stringField("home", 1, true)}},
				},
			}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(lww.CheckReferenceMessage(fd.Messages().ByName("Stray"))).To(MatchError(And(
				ContainSubstring(`field "home" is marked workspace`),
				ContainSubstring(`is not a reference message`),
			)))
		})

		It("rejects a workspace field that is not a singular string", func() {
			ws := &descriptorpb.FieldDescriptorProto{
				Name:   proto.String("workspace_id"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
			}
			opts := &descriptorpb.FieldOptions{}
			proto.SetExtension(opts, syncplanev1.E_FieldBehavior, &syncplanev1.FieldBehavior{Workspace: true})
			ws.Options = opts
			ref, _ := build([]*descriptorpb.FieldDescriptorProto{ws, userField("user", 2)}, false)
			Expect(lww.CheckReferenceMessage(ref)).To(MatchError(ContainSubstring("must be a singular string")))
		})
	})
})
