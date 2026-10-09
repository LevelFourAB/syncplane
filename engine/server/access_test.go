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
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
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
			Access: readWrite("view_members", "manage_members"),
		},
		{
			Name:      "project",
			KeyFields: []string{"id"},
			Fields:    []server.DynamicField{{Name: "id", Number: 1, Kind: str}},
			Access:    readWrite("view_projects", "edit_projects"),
		},
		{
			Name:      "task",
			KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "project", Number: 2, Ref: "project", AccessParent: true},
			},
			Access: readWrite("view_tasks", "edit_tasks"),
		},
	}
}

// readWrite returns access options with one rule that admits reads by read
// and one that admits every write by write.
func readWrite(read, write string) *syncplanev1.AccessOptions {
	return &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{permRule(read, "read"), permRule(write, "write")}}
}

// permRule returns a rule that admits ops, each one of "read", "create",
// "update", "delete" and "write", to a caller that holds permission.
func permRule(permission string, ops ...string) *syncplanev1.AccessRule {
	r := opsRule(ops...)
	r.Requires = []string{permission}
	return r
}

// admittedRule returns a rule that admits ops to every admitted caller.
func admittedRule(ops ...string) *syncplanev1.AccessRule {
	r := opsRule(ops...)
	r.Requires, r.Admitted = nil, &syncplanev1.AdmittedCaller{}
	return r
}

func opsRule(ops ...string) *syncplanev1.AccessRule {
	GinkgoHelper()
	r := &syncplanev1.AccessRule{}
	for _, op := range ops {
		switch op {
		case "read":
			r.Read = true
		case "create":
			r.Create = true
		case "update":
			r.Update = true
		case "delete":
			r.Delete = true
		case "write":
			r.Write = true
		default:
			Fail("unknown operation " + op)
		}
	}
	return r
}

// commentSpec is a comment below a task, two hops below the project, for
// orgSpecs.
func commentSpec() server.DynamicEntity {
	return server.DynamicEntity{
		Name: "comment", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "task", Number: 2, Ref: "task", AccessParent: true},
		},
		Access: readWrite("view_tasks", "edit_tasks"),
	}
}

// withFolders adds to org specs a folder tree whose folders sit below folders
// and whose files sit below a folder, read and written by permissions of
// their own that org admins hold.
func withFolders(specs []server.DynamicEntity) []server.DynamicEntity {
	GinkgoHelper()
	admin := specNamed(specs, "org_member").Grant.Roles[0]
	admin.Grants = append(admin.Grants, "view_files", "edit_files")
	str := protoreflect.StringKind
	return append(specs,
		server.DynamicEntity{
			Name: "folder", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "parent", Number: 2, Ref: "folder", AccessParent: true},
			},
			Access: readWrite("view_files", "edit_files"),
		},
		server.DynamicEntity{
			Name: "file", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "folder", Number: 2, Ref: "folder", AccessParent: true},
			},
			Access: readWrite("view_files", "edit_files"),
		},
	)
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

// plainSpec is an entity with a key and nothing else: no access, no grant,
// no access parent.
func plainSpec(name string) server.DynamicEntity {
	return server.DynamicEntity{
		Name: name, KeyFields: []string{"id"},
		Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
	}
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
		protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_query_proto),
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

		It("registers a role that includes another role and grants nothing of its own", func() {
			specs := orgSpecs()
			g := specNamed(specs, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_owner", Includes: []string{"org_admin"}})
			Expect(register(specs)).To(Succeed())
		})

		It("registers a role that also grants a permission of a role it includes", func() {
			specs := orgSpecs()
			g := specNamed(specs, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_owner", Includes: []string{"org_admin"}, Grants: []string{"view_tasks"}})
			Expect(register(specs)).To(Succeed())
		})

		It("registers an entity that declares no access beside entities that do", func() {
			Expect(register(append(orgSpecs(), plainSpec("comment")))).To(Succeed())
		})

		// Every caller that enters a workspace of a schema with a role holds a
		// permission, and an entity outside the model is open to each of them.
		It("warns about an entity outside the model when the schema declares a role", func() {
			logs := &recordingHandler{}
			srv = newBareServer(server.WithLogger(slog.New(logs)))
			Expect(register(append(orgSpecs(), plainSpec("comment"), plainSpec("audit")))).To(Succeed())
			Expect(logs.attrs("access model warning", "finding")).To(ConsistOf(
				ContainSubstring(`entity "audit" declares no access option`),
				ContainSubstring(`entity "comment" declares no access option`),
			))
			Expect(logs.levels("access model warning")).To(HaveEach(slog.LevelWarn))
		})

		// Without a role, admitted means what the workspace hook admits, and an
		// entity outside the model behaves as it does in a schema without one.
		It("does not warn about an entity outside the model when the schema declares no role", func() {
			logs := &recordingHandler{}
			_, err := server.New(newTestStore(), server.WithLogger(slog.New(logs)),
				server.WithEntities(&accessv1.Note{}, &accessv1.Setting{}, &testdatav1.Widget{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(logs.count("access model warning")).To(BeZero())
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

		It("does not warn when one permission governs a row and the rows below it", func() {
			logs := &recordingHandler{}
			srv = newBareServer(server.WithLogger(slog.New(logs)))
			Expect(register(append(orgSpecs(), commentSpec()))).To(Succeed())
			Expect(logs.count("access model warning")).To(BeZero())
		})

		It("registers rules that admit every admitted caller, on an access parent, below one and on a grant entity", func() {
			specs := orgSpecs()
			specNamed(specs, "project").Access = &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{
				admittedRule("read"), permRule("edit_projects", "write"), permRule("view_projects", "read"),
			}}
			specNamed(specs, "task").Access = &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{
				admittedRule("read", "write"), permRule("view_tasks", "read"), permRule("edit_tasks", "write"),
			}}
			specNamed(specs, "org_member").Access.Allow[0] = admittedRule("read")
			specNamed(specs, "org_member").Access.Allow = append(specNamed(specs, "org_member").Access.Allow,
				permRule("view_members", "read"))
			Expect(register(specs)).To(Succeed())
		})

		It("registers rules that split the write operations across permissions", func() {
			specs := orgSpecs()
			specNamed(specs, "task").Access = &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{
				permRule("view_tasks", "read"), permRule("view_tasks", "create"),
				permRule("edit_tasks", "update", "delete"), permRule("edit_projects", "read"),
			}}
			Expect(register(specs)).To(Succeed())
		})

		It("registers an access parent with an access parent of its own", func() {
			Expect(register(append(orgSpecs(), commentSpec()))).To(Succeed())
		})

		It("registers an entity that is its own access parent, and rows below it", func() {
			Expect(register(withFolders(orgSpecs()))).To(Succeed())
		})

		It("refuses a chain of parent entities longer than MaxAccessDepth", func() {
			srv = newBareServer(server.WithLimits(server.Limits{MaxAccessDepth: 1}))
			Expect(register(append(orgSpecs(), commentSpec()))).To(MatchError(ContainSubstring(
				`entity "comment" can sit 2 access_parent hops below an entity without a parent, more than MaxAccessDepth (1) allows`)))
		})

		It("accepts a recursive chain whatever MaxAccessDepth is", func() {
			srv = newBareServer(server.WithLimits(server.Limits{MaxAccessDepth: 1}))
			Expect(register(withFolders(orgSpecs()))).To(Succeed())
		})

		It("warns when one permission governs a recursive entity and one at a fixed depth", func() {
			logs := &recordingHandler{}
			srv = newBareServer(server.WithLogger(slog.New(logs)))
			specs := withFolders(orgSpecs())
			specNamed(specs, "file").Access = readWrite("view_tasks", "edit_files")
			Expect(register(specs)).To(Succeed())
			Expect(logs.attrs("access model warning", "finding")).To(ConsistOf(
				ContainSubstring(`permission "view_tasks"`),
			))
		})
	})

	DescribeTable("refuses a schema that breaks a rule of one entity",
		func(change func([]server.DynamicEntity), want string) {
			specs := orgSpecs()
			change(specs)
			Expect(register(specs)).To(MatchError(ContainSubstring(want)))
		},
		Entry("access without a rule", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access = &syncplanev1.AccessOptions{}
		}, "access declares no rule"),
		Entry("a rule without an operation", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.Allow[1] = permRule("edit_tasks")
		}, "access allow[1] names no operation"),
		Entry("a rule without a source", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.Allow[0].Requires = nil
		}, "access allow[0] names no source"),
		Entry("a rule with an empty permission", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.Allow[0] = permRule("", "read")
		}, "access allow[0] requires: permission name"),
		Entry("two rules that are the same", func(s []server.DynamicEntity) {
			a := specNamed(s, "task").Access
			a.Allow = append(a.Allow, permRule("view_tasks", "read"))
		}, "access allow[0] and allow[2] are the same rule"),
		Entry("a permission that is not an identifier", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access = readWrite("view-tasks", "edit_tasks")
		}, `permission name "view-tasks" is not an identifier`),
		Entry("an access parent on an entity without access", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access = nil
		}, `field "project" is marked access_parent, but the entity declares no access option`),
		Entry("two access parents", func(s []server.DynamicEntity) {
			task := specNamed(s, "task")
			task.Fields = append(task.Fields, server.DynamicField{Name: "other", Number: 3, Ref: "project", AccessParent: true})
		}, `fields "project" and "other" are both marked access_parent`),
		Entry("a repeated access parent", func(s []server.DynamicEntity) {
			specNamed(s, "task").Fields[1].Repeated = true
		}, `access_parent field "project" is repeated`),
		Entry("a workspace-qualified access parent", func(s []server.DynamicEntity) {
			specNamed(s, "task").Fields[1].RefWorkspace = true
		}, `access_parent field "project" is a reference message with a workspace field`),
		Entry("unknown fields inside access", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.ProtoReflect().SetUnknown(unknownField())
		}, "access carries option fields this server does not know"),
		Entry("unknown fields inside a rule", func(s []server.DynamicEntity) {
			specNamed(s, "task").Access.Allow[1].ProtoReflect().SetUnknown(unknownField())
		}, "access allow[1] carries option fields this server does not know"),
		Entry("unknown fields inside an admitted source", func(s []server.DynamicEntity) {
			admitted := &syncplanev1.AdmittedCaller{}
			admitted.ProtoReflect().SetUnknown(unknownField())
			rule := specNamed(s, "task").Access.Allow[0]
			rule.Requires, rule.Admitted = nil, admitted
		}, "access allow[0] admitted carries option fields this server does not know"),
		Entry("a grant entity without access", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Access = nil
		}, "a grant entity must declare access"),
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
		Entry("a role that includes a role the grant does not declare", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Roles[0].Includes = []string{"org_viewer"}
		}, `grant role "org_admin" includes "org_viewer", which is not a role of this grant`),
		Entry("a role that includes a role twice", func(s []server.DynamicEntity) {
			g := specNamed(s, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_owner", Includes: []string{"org_admin", "org_admin"}})
		}, `grant role "org_owner" includes "org_admin" twice`),
		Entry("a role that includes itself", func(s []server.DynamicEntity) {
			specNamed(s, "org_member").Grant.Roles[0].Includes = []string{"org_admin"}
		}, `grant role "org_admin" includes itself through org_admin → org_admin`),
		Entry("roles that include each other", func(s []server.DynamicEntity) {
			g := specNamed(s, "org_member").Grant
			g.Roles[0].Includes = []string{"org_owner"}
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_owner", Includes: []string{"org_admin"}})
		}, `grant role "org_admin" includes itself through org_admin → org_owner → org_admin`),
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

	Context("under WithStrictAccess", func() {
		BeforeEach(func() {
			srv = newBareServer(server.WithStrictAccess())
		})

		It("registers a schema where every entity declares access", func() {
			Expect(register(orgSpecs())).To(Succeed())
		})

		It("refuses an entity that declares no access", func() {
			Expect(register(append(orgSpecs(), plainSpec("comment")))).To(MatchError(ContainSubstring(
				`entity "comment" declares no access option`)))
		})

		It("refuses an entity that declares no access in a schema without a role", func() {
			_, err := server.New(newTestStore(), server.WithStrictAccess(),
				server.WithEntities(&accessv1.Note{}, &accessv1.Setting{}, &testdatav1.Widget{}))
			Expect(err).To(MatchError(ContainSubstring(`entity "widget" declares no access option`)))
		})

		It("registers a schema that does not use the model", func() {
			_, err := server.New(newTestStore(), server.WithStrictAccess(), server.WithEntities(&testdatav1.Widget{}))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	DescribeTable("refuses a schema that breaks a rule across entities",
		func(change func([]server.DynamicEntity) []server.DynamicEntity, want string) {
			Expect(register(change(orgSpecs()))).To(MatchError(ContainSubstring(want)))
		},
		Entry("an access parent outside the model", func(s []server.DynamicEntity) []server.DynamicEntity {
			task := specNamed(s, "task")
			task.Access = nil
			task.Fields[1].AccessParent = false
			return append(s, commentSpec())
		}, `points at entity "task", which is not in the access model`),
		Entry("a role declared by two entities", func(s []server.DynamicEntity) []server.DynamicEntity {
			guest := specNamed(s, "org_member")
			return append(s, server.DynamicEntity{
				Name: "guest", KeyFields: []string{"id"}, Fields: guest.Fields,
				Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{
					{Name: "org_admin", Grants: []string{"view_tasks"}},
				}},
				Access: readWrite("view_members", "manage_members"),
			})
		}, `role "org_admin" is declared by both entity "guest" and entity "org_member"`),
		Entry("a role that includes a role of another entity", func(s []server.DynamicEntity) []server.DynamicEntity {
			guest := specNamed(s, "org_member")
			return append(s, server.DynamicEntity{
				Name: "guest", KeyFields: []string{"id"}, Fields: guest.Fields,
				Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{
					{Name: "org_guest", Includes: []string{"org_admin"}},
				}},
				Access: readWrite("view_members", "manage_members"),
			})
		}, `grant role "org_guest" includes "org_admin", which is not a role of this grant`),
		Entry("a permission that no role grants", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access = readWrite("view_tasks", "edit_everything")
			return s
		}, `permission "edit_everything" of entity "task" is granted by no role`),
		Entry("a role that grants a permission no rule uses", func(s []server.DynamicEntity) []server.DynamicEntity {
			g := specNamed(s, "org_member").Grant
			g.Roles[0].Grants = append(g.Roles[0].Grants, "delete_org")
			return s
		}, `role "org_admin" of entity "org_member" grants permission "delete_org", which no access rule or mutator uses`),
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

		// Includes are expanded at registration, so what each role grants is
		// all the evolution check compares.
		It("accepts roles rewritten with includes to the same permissions without the flag", func() {
			srv = newBareServer()
			flat := orgSpecs()
			g := specNamed(flat, "org_member").Grant
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_viewer", Grants: []string{"view_members", "view_projects", "view_tasks"}})
			Expect(register(flat)).To(Succeed())

			nested := orgSpecs()
			g = specNamed(nested, "org_member").Grant
			g.Roles[0].Includes = []string{"org_viewer"}
			g.Roles[0].Grants = []string{"manage_members", "edit_projects", "edit_tasks"}
			g.Roles = append(g.Roles, &syncplanev1.Role{Name: "org_viewer", Grants: []string{"view_members", "view_projects", "view_tasks"}})
			Expect(register(nested)).To(Succeed())
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

		// The first role makes the gate refuse every caller without a grant,
		// although no entity that was registered before changes.
		It("refuses a schema declaring its first role without the flag", func() {
			srv = newBareServer()
			note := server.DynamicEntity{
				Name: "note", KeyFields: []string{"id"},
				Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
				Access: &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{admittedRule("read", "write")}},
			}
			Expect(register([]server.DynamicEntity{note})).To(Succeed())

			withMembers := []server.DynamicEntity{note, orgSpecs()[0]}
			specNamed(withMembers, "org_member").Grant.Roles[0].Grants = []string{"view_members", "manage_members"}
			Expect(register(withMembers)).To(MatchError(ContainSubstring("the schema declares its first role")))
			Expect(register(withMembers, server.AllowAccessChange())).To(Succeed())
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
// levels returns the level of every record with the message.
func (h *recordingHandler) levels(msg string) []slog.Level {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Level
	for _, r := range h.records {
		if r.Message == msg {
			out = append(out, r.Level)
		}
	}
	return out
}

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
