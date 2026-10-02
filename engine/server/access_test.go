package server_test

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// orgSpecs is a small schema that uses the access model: org members hold
// roles on the whole workspace, and tasks sit below projects. Each call builds
// fresh specs, so a spec may change them freely.
func orgSpecs() []server.DynamicEntity {
	str := protoreflect.StringKind
	return []server.DynamicEntity{
		{
			Name:      "org_member",
			KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "user_id", Number: 2, Kind: str},
				{Name: "role", Number: 3, Kind: str},
			},
			Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{{
				Name:   "org_admin",
				Grants: []string{"view_members", "manage_members", "view_projects", "edit_projects", "view_tasks", "edit_tasks"},
			}}},
			Access: &syncplanev1.AccessOptions{Read: "view_members", Write: "manage_members"},
		},
		{
			Name:      "project",
			KeyFields: []string{"id"},
			Fields:    []server.DynamicField{{Name: "id", Number: 1, Kind: str}},
			Access:    &syncplanev1.AccessOptions{Read: "view_projects", Write: "edit_projects"},
		},
		{
			Name:      "task",
			KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "project", Number: 2, Ref: "project", AccessParent: true},
			},
			Access: &syncplanev1.AccessOptions{Read: "view_tasks", Write: "edit_tasks"},
		},
	}
}

// specNamed returns the spec of the named entity, for a spec to change in
// place.
func specNamed(specs []server.DynamicEntity, name string) *server.DynamicEntity {
	GinkgoHelper()
	for i := range specs {
		if specs[i].Name == name {
			return &specs[i]
		}
	}
	Fail("no spec named " + name)
	return nil
}

// unknownField returns the bytes of a field number no build of the options
// knows, as a newer options.proto would write them.
func unknownField() []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, 999, protowire.VarintType), 1)
}

// accessFileSet returns a descriptor set holding the compiled access test
// schema, after change has edited its file.
func accessFileSet(change func(*descriptorpb.FileDescriptorProto)) []byte {
	GinkgoHelper()
	descFile := (&descriptorpb.FileDescriptorProto{}).ProtoReflect().Descriptor().ParentFile()
	file := protodesc.ToFileDescriptorProto(accessv1.File_syncplane_testdata_access_v1_access_proto)
	if change != nil {
		change(file)
	}
	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{
		protodesc.ToFileDescriptorProto(descFile),
		protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_options_proto),
		file,
	}})
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func messageNamed(file *descriptorpb.FileDescriptorProto, name string) *descriptorpb.DescriptorProto {
	GinkgoHelper()
	for _, m := range file.GetMessageType() {
		if m.GetName() == name {
			return m
		}
	}
	Fail("no message named " + name)
	return nil
}

var _ = Describe("Access model registration", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(specs []server.DynamicEntity, opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(specs...), opts...)
		return err
	}

	Context("a valid schema", func() {
		It("registers the compiled org schema", func() {
			_, err := server.New(newTestStore(), server.WithEntities(
				&accessv1.OrgMember{}, &accessv1.Project{}, &accessv1.ProjectMember{},
				&accessv1.Task{}, &accessv1.Label{}, &accessv1.Note{},
			))
			Expect(err).NotTo(HaveOccurred())
		})

		It("registers a grant entity whose role field is an enum", func() {
			_, err := server.New(newTestStore(), server.WithEntities(&accessv1.Team{}, &accessv1.TeamMember{}))
			Expect(err).NotTo(HaveOccurred())
		})

		It("registers a descriptor set that uses the model", func() {
			Expect(register(orgSpecs())).To(Succeed())
		})

		// view_members governs OrgMember at the workspace root and
		// ProjectMember below a project, so an org role that grants it reaches
		// the roster of every project.
		It("warns when one permission governs entities at different anchor depths", func() {
			logs := &recordingHandler{}
			_, err := server.New(newTestStore(), server.WithLogger(slog.New(logs)), server.WithEntities(
				&accessv1.OrgMember{}, &accessv1.Project{}, &accessv1.ProjectMember{},
				&accessv1.Task{}, &accessv1.Label{}, &accessv1.Note{},
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(logs.attrs("access model warning", "finding")).To(ConsistOf(
				ContainSubstring(`permission "manage_members"`),
				ContainSubstring(`permission "view_members"`),
			))
		})

		It("does not warn when every permission governs one depth", func() {
			logs := &recordingHandler{}
			srv = newBareServer(server.WithLogger(slog.New(logs)))
			Expect(register(orgSpecs())).To(Succeed())
			Expect(logs.count("access model warning")).To(BeZero())
		})
	})

	DescribeTable("refuses a schema that breaks a rule of one entity",
		func(change func([]server.DynamicEntity), want string) {
			specs := orgSpecs()
			change(specs)
			Expect(register(specs)).To(MatchError(ContainSubstring(want)))
		},
		Entry("access without a write permission", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.Write = ""
		}, "must name both a read and a write permission"),
		Entry("unrestricted together with a permission", func(s []server.DynamicEntity) {
			specNamed(s, "project").Access.Unrestricted = true
		}, "sets unrestricted together with read or write"),
		Entry("a permission that is not an identifier", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.Read = "view-tasks"
		}, `permission name "view-tasks" is not an identifier`),
		Entry("an access parent on an entity without access", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access = nil
		}, `field "project" is marked access_parent, but the entity declares no access option`),
		Entry("an access parent on an unrestricted entity", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access = &syncplanev1.AccessOptions{Unrestricted: true}
		}, "but access is unrestricted"),
		Entry("two access parents", func(s []server.DynamicEntity) {
			task := specNamed(s, "task")
			task.Fields = append(task.Fields, server.DynamicField{Name: "other", Number: 3, Ref: "project", AccessParent: true})
		}, `fields "project" and "other" are both marked access_parent`),
		Entry("a repeated access parent", func(s []server.DynamicEntity) {
			specNamed(s, "task").Fields[1].Repeated = true
		}, `access_parent field "project" is repeated`),
		Entry("a workspace-qualified access parent", func(s []server.DynamicEntity) {
			specNamed(s, "task").Fields[1].RefWorkspace = true
		}, `access_parent field "project" must be a field whose type is an entity message`),
		Entry("unknown fields inside access", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.ProtoReflect().SetUnknown(unknownField())
		}, "access carries option fields this server does not know"),
		Entry("a grant entity outside the model", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Access = &syncplanev1.AccessOptions{Unrestricted: true}
		}, "a grant entity must declare access with a read and a write permission"),
		Entry("a grant with no roles", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Roles = nil
		}, "grant declares no roles"),
		Entry("a grant subject field that does not exist", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Subject = "who"
		}, `grant subject field "who" not found`),
		Entry("a grant subject field that is not a string", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Fields[1].Kind = protoreflect.Int64Kind
		}, `grant subject field "user_id" is int64; it must be a string`),
		Entry("a repeated grant role field", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Fields[2].Repeated = true
		}, `grant role field "role" is repeated`),
		Entry("one field as both subject and role", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Role = "user_id"
		}, `grant names field "user_id" as both subject and role`),
		Entry("a role declared twice by one entity", func(s []server.DynamicEntity) {
			g := specNamed(s, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_admin", Grants: []string{"view_tasks"}})
		}, `grant declares role "org_admin" twice`),
		Entry("a role that grants nothing", func(s []server.DynamicEntity) {
			g := specNamed(s, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_guest"})
		}, `grant role "org_guest" grants no permissions`),
		Entry("a role that grants a permission twice", func(s []server.DynamicEntity) {
			g := specNamed(s, "org_member").Grant
			g.Roles[0].Grants = append(g.Roles[0].Grants, "view_tasks")
		}, `grant role "org_admin" grants "view_tasks" twice`),
		Entry("a role name that is not an identifier", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Roles[0].Name = "org admin"
		}, `role name "org admin" is not an identifier`),
		Entry("unknown fields inside grant", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.ProtoReflect().SetUnknown(unknownField())
		}, "grant carries option fields this server does not know"),
		Entry("unknown fields inside a role", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Roles[0].ProtoReflect().SetUnknown(unknownField())
		}, "grant role carries option fields this server does not know"),
	)

	DescribeTable("refuses a schema that breaks a rule across entities",
		func(change func([]server.DynamicEntity) []server.DynamicEntity, want string) {
			Expect(register(change(orgSpecs()))).To(MatchError(ContainSubstring(want)))
		},
		Entry("an entity without access", func(s []server.DynamicEntity) []server.DynamicEntity {
			return append(s, server.DynamicEntity{
				Name: "comment", KeyFields: []string{"id"},
				Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
			})
		}, `entity "comment" declares no access option`),
		Entry("an access parent outside the model", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "project").Access = &syncplanev1.AccessOptions{Unrestricted: true}
			return s
		}, `points at entity "project", which is not in the access model`),
		Entry("an access parent with an access parent of its own", func(s []server.DynamicEntity) []server.DynamicEntity {
			return append(s, server.DynamicEntity{
				Name: "comment", KeyFields: []string{"id"},
				Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "task", Number: 2, Ref: "task", AccessParent: true},
				},
				Access: &syncplanev1.AccessOptions{Read: "view_tasks", Write: "edit_tasks"},
			})
		}, `points at entity "task", which has an access_parent of its own`),
		Entry("an entity that is its own access parent", func(s []server.DynamicEntity) []server.DynamicEntity {
			return append(s, server.DynamicEntity{
				Name: "folder", KeyFields: []string{"id"},
				Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "parent", Number: 2, Ref: "folder", AccessParent: true},
				},
				Access: &syncplanev1.AccessOptions{Read: "view_tasks", Write: "edit_tasks"},
			})
		}, `points at entity "folder", which has an access_parent of its own`),
		Entry("a role declared by two entities", func(s []server.DynamicEntity) []server.DynamicEntity {
			guest := specNamed(s, "org_member")
			return append(s, server.DynamicEntity{
				Name: "guest", KeyFields: []string{"id"}, Fields: guest.Fields,
				Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{
					{Name: "org_admin", Grants: []string{"view_tasks"}},
				}},
				Access: &syncplanev1.AccessOptions{Read: "view_members", Write: "manage_members"},
			})
		}, `role "org_admin" is declared by both entity "guest" and entity "org_member"`),
		Entry("a permission that no role grants", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Write = "edit_everything"
			return s
		}, `permission "edit_everything" of entity "task" is granted by no role`),
		Entry("a role that grants a permission no entity uses", func(s []server.DynamicEntity) []server.DynamicEntity {
			g := specNamed(s, "org_member").Grant
			g.Roles[0].Grants = append(g.Roles[0].Grants, "delete_org")
			return s
		}, `role "org_admin" of entity "org_member" grants permission "delete_org", which no entity or mutator uses`),
	)

	Context("compiled entities", func() {
		It("refuses New when an access parent is not registered", func() {
			_, err := server.New(newTestStore(), server.WithEntities(&accessv1.OrgMember{}, &accessv1.Task{}))
			Expect(err).To(MatchError(ContainSubstring(
				"access_parent field \"project\" points at syncplane.testdata.access.v1.Project, which is not a registered entity")))
		})

		It("refuses an enum role that is not a value of the enum", func() {
			set := accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				member := messageNamed(file, "TeamMember")
				eo := proto.GetExtension(member.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
				eo.GetGrant().GetRoles()[0].Name = "TEAM_ROLE_OWNER"
			})
			_, err := srv.RegisterDescriptorSet(set)
			Expect(err).To(MatchError(ContainSubstring(
				`grant role "TEAM_ROLE_OWNER" is not a value of enum syncplane.testdata.access.v1.TeamRole`)))
		})

		// RegisterEntity adds one entity at a time, so a schema can only be
		// judged once it is complete. Until it is, nothing is served.
		It("fails closed while RegisterEntity leaves the model incomplete", func() {
			Expect(srv.RegisterEntity(&accessv1.Task{})).To(Succeed())

			ctx := context.Background()
			_, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeFailedPrecondition))
			Expect(err).To(MatchError(ContainSubstring("which is not a registered entity")))
			Expect(srv.View(ctx, ws, func(*server.ViewTx) error { return nil })).
				To(MatchError(ContainSubstring("which is not a registered entity")))

			for _, msg := range []proto.Message{&accessv1.Project{}, &accessv1.OrgMember{}, &accessv1.Label{}} {
				Expect(srv.RegisterEntity(msg)).To(Succeed())
			}
			// Once complete, the model judges the caller, who holds no grant.
			_, err = srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})
	})

	Context("evolution", func() {
		BeforeEach(func() {
			Expect(register(orgSpecs())).To(Succeed())
		})

		addRole := func() []server.DynamicEntity {
			specs := orgSpecs()
			g := specNamed(specs, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_viewer", Grants: []string{"view_tasks"}})
			return specs
		}

		It("refuses a change to an entity's access model", func() {
			Expect(register(addRole())).To(MatchError(ContainSubstring(
				`entity "org_member" changes its access model`)))
		})

		It("applies the change when the registration allows it", func() {
			Expect(register(addRole(), server.AllowAccessChange())).To(Succeed())
		})

		It("accepts an additive field without the flag", func() {
			specs := orgSpecs()
			task := specNamed(specs, "task")
			task.Fields = append(task.Fields, server.DynamicField{Name: "title", Number: 3, Kind: protoreflect.StringKind})
			Expect(register(specs)).To(Succeed())
		})

		It("refuses a schema adopting the model without the flag", func() {
			srv = newBareServer()
			plain := orgSpecs()[1:]
			for i := range plain {
				plain[i].Access = nil
				for j := range plain[i].Fields {
					plain[i].Fields[j].AccessParent = false
				}
			}
			Expect(register(plain)).To(Succeed())
			Expect(register(orgSpecs())).To(MatchError(ContainSubstring("changes its access model")))
			Expect(register(orgSpecs(), server.AllowAccessChange())).To(Succeed())
		})
	})

	Context("evolution of an enum role field", func() {
		BeforeEach(func() {
			_, err := srv.RegisterDescriptorSet(accessFileSet(nil))
			Expect(err).NotTo(HaveOccurred())
		})

		// A stored role is a number, and its value name picks the role, so a
		// renumbering moves every row that holds the number to another role.
		It("refuses renumbered role values", func() {
			set := accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				for _, e := range file.GetEnumType() {
					if e.GetName() != "TeamRole" {
						continue
					}
					v := e.GetValue()
					v[1].Number, v[2].Number = v[2].Number, v[1].Number
				}
			})
			_, err := srv.RegisterDescriptorSet(set)
			Expect(err).To(MatchError(ContainSubstring(`entity "team_member" changes its access model`)))
		})

		It("refuses a role field moved to another enum type", func() {
			set := accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				member := messageNamed(file, "TeamMember")
				for _, e := range file.GetEnumType() {
					if e.GetName() == "TeamRole" {
						// Nested, so its values do not clash with TeamRole's.
						other := proto.Clone(e).(*descriptorpb.EnumDescriptorProto)
						other.Name = proto.String("Role")
						member.EnumType = append(member.EnumType, other)
					}
				}
				for _, f := range member.GetField() {
					if f.GetName() == "role" {
						f.TypeName = proto.String(".syncplane.testdata.access.v1.TeamMember.Role")
					}
				}
			})
			_, err := srv.RegisterDescriptorSet(set, server.AllowAccessChange())
			Expect(err).To(MatchError(ContainSubstring(`field "role" enum type changed`)))
		})
	})
})

// attrs returns the value of key on every record logged with msg.
func (h *recordingHandler) attrs(msg, key string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		if r.Message != msg {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				out = append(out, a.Value.String())
				return false
			}
			return true
		})
	}
	return out
}
