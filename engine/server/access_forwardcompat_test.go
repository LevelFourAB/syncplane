package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A schema written against a newer options.proto can reach a server built
// against an older one. Whatever the older server cannot read in an access
// declaration could narrow access, so it must refuse the schema rather than
// read it as something wider.
var _ = Describe("Access model forward compatibility", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(specs []server.DynamicEntity, opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(specs...), opts...)
		return err
	}

	// archivedTask is the org schema with a bool archived field on task.
	archivedTask := func() []server.DynamicEntity {
		specs := orgSpecs()
		task := specNamed(specs, "task")
		task.Fields = append(task.Fields, server.DynamicField{Name: "archived", Number: 3, Kind: protoreflect.BoolKind})
		return specs
	}
	// not wraps cond in a not condition.
	not := func(cond *syncplanev1.Condition) *syncplanev1.Condition {
		return &syncplanev1.Condition{Kind: &syncplanev1.Condition_Not{Not: cond}}
	}
	// A value no build of query.proto declares, as a newer one would write it.
	const futureOp = 99

	DescribeTable("refuses a when condition with an operator this server does not know",
		func(cond *syncplanev1.Condition) {
			// Given the org schema, where task reads by view_tasks only on rows
			// that do not match cond
			specs := archivedTask()
			read := permRule("view_tasks", "read")
			read.When = not(cond)
			specNamed(specs, "task").Access.Allow[0] = read

			// When it registers
			err := register(specs)

			// Then registration fails and names the rule, since this server
			// would otherwise evaluate the unknown operator as false, and the
			// not around it as true on every row
			Expect(err).To(MatchError(ContainSubstring("access allow[0] when")))
		},
		Entry("a cmp operator", &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
			Path: []string{"archived"}, Op: syncplanev1.CmpOp(futureOp),
			Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_BoolValue{BoolValue: true}},
		}}}),
		Entry("a str operator", &syncplanev1.Condition{Kind: &syncplanev1.Condition_Str{Str: &syncplanev1.StrCond{
			Path: []string{"id"}, Op: syncplanev1.StrOp(futureOp), Value: "x",
		}}}),
		Entry("a present operator", &syncplanev1.Condition{Kind: &syncplanev1.Condition_Present{Present: &syncplanev1.PresentCond{
			Path: []string{"archived"}, Op: syncplanev1.PresentOp(futureOp),
		}}}),
	)

	It("refuses a within condition with an operator this server does not know", func() {
		// Given the org schema, where task reads by view_tasks within a project
		// that does not match a cmp condition with an unknown operator
		specs := archivedTask()
		project := specNamed(specs, "project")
		project.Fields = append(project.Fields, server.DynamicField{Name: "private", Number: 2, Kind: protoreflect.BoolKind})
		read := permRule("view_tasks", "read")
		read.Within = []*syncplanev1.AccessWithin{{
			Entity: "project",
			When: not(&syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path: []string{"private"}, Op: syncplanev1.CmpOp(futureOp),
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_BoolValue{BoolValue: true}},
			}}}),
		}}
		specNamed(specs, "task").Access.Allow[0] = read

		// When it registers
		err := register(specs)

		// Then registration fails and names the entry
		Expect(err).To(MatchError(ContainSubstring("within[0]")))
	})
})

// The creator mark decides who writes a field: a create that is not trusted
// stores the caller's subject in it, and no later write that is not trusted
// changes it. A rule with caller_is on that field relies on it, so a
// registration that drops the mark widens who the rule admits.
var _ = Describe("Access model evolution of a creator field", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(specs []server.DynamicEntity, opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(specs...), opts...)
		return err
	}

	// withNotes is the org schema with a note below a project, which a caller
	// with edit_tasks creates, and only its author updates.
	withNotes := func(creator bool) []server.DynamicEntity {
		str := protoreflect.StringKind
		return append(orgSpecs(), server.DynamicEntity{
			Name: "note", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "project", Number: 2, Ref: "project"},
				{Name: "author_id", Number: 3, Kind: str, Creator: creator},
				{Name: "body", Number: 4, Kind: str},
			},
			Access: &syncplanev1.AccessOptions{
				Parents: []string{"project"},
				Allow: []*syncplanev1.AccessRule{
					permRule("view_tasks", "read"),
					permRule("edit_tasks", "create"),
					func() *syncplanev1.AccessRule {
						r := permRule("edit_tasks", "update")
						r.CallerIs = "author_id"
						return r
					}(),
				},
			},
		})
	}

	It("refuses a registration that drops the creator mark from a field a rule checks", func() {
		// Given a served schema whose note.author_id is a creator field that
		// the update rule checks with caller_is
		Expect(register(withNotes(true))).To(Succeed())

		// When a registration drops the creator mark, so that a create can name
		// any author and the update rule then admits that author
		err := register(withNotes(false))

		// Then it is refused as a change to the access model
		Expect(err).To(MatchError(ContainSubstring(`entity "note" changes its access model`)))
		Expect(register(withNotes(false), server.AllowAccessChange())).To(Succeed())
	})
})

// A descriptor set built against an older options.proto can still carry the
// entity and field write policies that the access model replaced. Their field
// numbers are reserved, so this server reads them as unknown fields. A policy
// that made an entity or a field trusted-only must not turn into an entity or
// a field that every admitted caller writes.
var _ = Describe("Access model options from an older options.proto", func() {
	// withUnknown returns the descriptor set of specs, with raw appended to
	// the unknown fields of the entity options of the message named entity.
	withUnknown := func(specs []server.DynamicEntity, entity string, raw []byte) []byte {
		GinkgoHelper()
		set := &descriptorpb.FileDescriptorSet{}
		Expect(proto.Unmarshal(descriptorSetBytes(specs...), set)).To(Succeed())
		for _, file := range set.GetFile() {
			for _, m := range file.GetMessageType() {
				if !proto.HasExtension(m.GetOptions(), syncplanev1.E_Entity) {
					continue
				}
				eo := proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
				if eo.GetName() != entity {
					continue
				}
				r := eo.ProtoReflect()
				r.SetUnknown(append(r.GetUnknown(), raw...))
			}
		}
		out, err := proto.Marshal(set)
		Expect(err).NotTo(HaveOccurred())
		return out
	}

	It("refuses an entity that still sets the removed delete_policy", func() {
		// Given a widget entity outside the model whose options still carry
		// delete_policy = DELETE_POLICY_TRUSTED_ONLY (field 4, value 2)
		srv := newBareServer()
		stale := protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), 2)
		set := withUnknown([]server.DynamicEntity{plainSpec("widget")}, "widget", stale)

		// When it registers
		_, err := srv.RegisterDescriptorSet(set)

		// Then registration is refused and names the entity, rather than
		// serving widget as an entity every admitted caller deletes from
		Expect(err).To(MatchError(ContainSubstring(`"widget"`)))
	})

	It("refuses a field that still sets the removed write_policy", func() {
		// Given a widget entity whose id field behavior still carries
		// write_policy = TRUSTED_ONLY (field 8, value 2)
		srv := newBareServer()
		stale := protowire.AppendVarint(protowire.AppendTag(nil, 8, protowire.VarintType), 2)
		set := withUnknownBehavior(plainSpec("widget"), "id", stale)

		// When it registers
		_, err := srv.RegisterDescriptorSet(set)

		// Then registration is refused, names the field and says what
		// replaces the policy
		Expect(err).To(MatchError(And(
			ContainSubstring(`"widget"`), ContainSubstring("write_policy"), ContainSubstring("guard"))))
	})
})

// A newer options.proto can add a field to EntityOptions or FieldBehavior
// that narrows access or changes how a field merges. This server cannot read
// it, so it refuses the schema rather than serve it without the field.
var _ = Describe("Entity options from a newer options.proto", func() {
	// future is a field number that no build of options.proto declares in
	// EntityOptions, FieldBehavior or IndexSpec.
	future := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)

	It("refuses an entity whose options carry a field this server does not know", func() {
		// Given a widget entity whose options carry field 99
		srv := newBareServer()
		set := withUnknownEntityOptions(plainSpec("widget"), func(eo *syncplanev1.EntityOptions) protoreflect.Message {
			return eo.ProtoReflect()
		}, future)

		// When it registers
		_, err := srv.RegisterDescriptorSet(set)

		// Then registration is refused and names the entity and the field
		Expect(err).To(MatchError(And(ContainSubstring(`"widget"`), ContainSubstring("field 99"))))
	})

	It("refuses an entity whose nested options carry a field this server does not know", func() {
		// Given a widget entity with an index, whose index options carry
		// field 99
		srv := newBareServer()
		spec := plainSpec("widget")
		spec.Fields = append(spec.Fields, server.DynamicField{Name: "title", Number: 2, Kind: protoreflect.StringKind})
		spec.Indexes = []server.DynamicIndex{{Name: "by_title", Key: []server.DynamicIndexKey{{Path: "title"}}}}
		set := withUnknownEntityOptions(spec, func(eo *syncplanev1.EntityOptions) protoreflect.Message {
			return eo.GetIndex()[0].ProtoReflect()
		}, future)

		// When it registers
		_, err := srv.RegisterDescriptorSet(set)

		// Then registration is refused and names the option
		Expect(err).To(MatchError(And(ContainSubstring(`"widget"`), ContainSubstring("options.index[0]"))))
	})

	It("refuses a field whose behavior carries a field this server does not know", func() {
		// Given a widget entity whose id field behavior carries field 99
		srv := newBareServer()
		set := withUnknownBehavior(plainSpec("widget"), "id", future)

		// When it registers
		_, err := srv.RegisterDescriptorSet(set)

		// Then registration is refused and names the field
		Expect(err).To(MatchError(And(ContainSubstring(`"widget"`), ContainSubstring("id"), ContainSubstring("field 99"))))
	})
})

// withUnknownEntityOptions returns the descriptor set of spec, with raw
// appended to the unknown fields of the message that pick returns from the
// entity options.
func withUnknownEntityOptions(spec server.DynamicEntity, pick func(*syncplanev1.EntityOptions) protoreflect.Message, raw []byte) []byte {
	GinkgoHelper()
	set := &descriptorpb.FileDescriptorSet{}
	Expect(proto.Unmarshal(descriptorSetBytes(spec), set)).To(Succeed())
	for _, file := range set.GetFile() {
		for _, m := range file.GetMessageType() {
			if !proto.HasExtension(m.GetOptions(), syncplanev1.E_Entity) {
				continue
			}
			r := pick(proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions))
			r.SetUnknown(append(r.GetUnknown(), raw...))
		}
	}
	out, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return out
}

// withUnknownBehavior returns the descriptor set of spec, with raw appended
// to the unknown fields of the field behavior of the field named field.
func withUnknownBehavior(spec server.DynamicEntity, field string, raw []byte) []byte {
	GinkgoHelper()
	set := &descriptorpb.FileDescriptorSet{}
	Expect(proto.Unmarshal(descriptorSetBytes(spec), set)).To(Succeed())
	for _, file := range set.GetFile() {
		for _, m := range file.GetMessageType() {
			if !proto.HasExtension(m.GetOptions(), syncplanev1.E_Entity) {
				continue
			}
			for _, f := range m.GetField() {
				if f.GetName() != field {
					continue
				}
				if f.Options == nil {
					f.Options = &descriptorpb.FieldOptions{}
				}
				b, _ := proto.GetExtension(f.GetOptions(), syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior)
				if b == nil {
					b = &syncplanev1.FieldBehavior{}
				}
				r := b.ProtoReflect()
				r.SetUnknown(append(r.GetUnknown(), raw...))
				proto.SetExtension(f.GetOptions(), syncplanev1.E_FieldBehavior, b)
			}
		}
	}
	out, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return out
}
