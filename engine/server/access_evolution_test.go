package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// evolutionSpecs is a schema that uses every part of the access model a
// registration can change: two roles, one with an include, a parent chain, a
// guard, a creator field, a team with members and an admitted rule.
func evolutionSpecs() []server.DynamicEntity {
	str := protoreflect.StringKind
	specs := orgSpecs()
	admin := specNamed(specs, "org_member")
	admin.Grant.Roles[0].Grants = append(admin.Grant.Roles[0].Grants, "view_teams", "manage_teams", "view_labels")
	admin.Grant.Roles = append(admin.Grant.Roles,
		&syncplanev1.Role{Name: "org_viewer", Grants: []string{"view_tasks"}},
		&syncplanev1.Role{Name: "org_lead", Includes: []string{"org_viewer"}, Grants: []string{"view_projects"}},
	)
	project := specNamed(specs, "project")
	project.Fields = append(project.Fields,
		server.DynamicField{Name: "owner_id", Number: 2, Kind: str},
		server.DynamicField{Name: "state", Number: 3, Kind: str},
	)
	task := specNamed(specs, "task")
	task.Fields = append(task.Fields,
		server.DynamicField{Name: "title", Number: 3, Kind: str},
		server.DynamicField{Name: "status", Number: 4, Kind: str},
		server.DynamicField{Name: "author", Number: 5, Kind: str, Creator: true},
	)
	task.Access.Guard = []*syncplanev1.FieldGuard{{
		Fields: []string{"status"},
		Allow:  []*syncplanev1.AccessRule{permRule("edit_tasks", "update")},
	}}
	return append(specs,
		server.DynamicEntity{
			Name: "team", KeyFields: []string{"id"},
			Fields:  []server.DynamicField{{Name: "id", Number: 1, Kind: str}},
			Members: &syncplanev1.MembersOptions{Entity: "team_member"},
			Access:  readWrite("view_teams", "manage_teams"),
		},
		server.DynamicEntity{
			Name: "team_member", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "team", Number: 2, Ref: "team"},
				{Name: "user_id", Number: 3, Kind: str},
				{Name: "role", Number: 4, Kind: str},
				{Name: "alt_user", Number: 5, Kind: str},
				{Name: "alt_role", Number: 6, Kind: str},
			},
			Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{
				{Name: "team_lead", Grants: []string{"view_teams", "manage_teams"}},
			}},
			Access: withParents(readWrite("view_teams", "manage_teams"), "team"),
		},
		server.DynamicEntity{
			Name: "label", KeyFields: []string{"id"},
			Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: str}},
			Access: &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{
				admittedRule("read"), permRule("view_labels", "write"),
			}},
		},
	)
}

// AllowAccessChange guards every change that widens or narrows what a caller
// may do with rows already stored. Each entry is one such change, applied to a
// served schema by a runtime registration.
var _ = Describe("Access model changes that need AllowAccessChange", func() {
	var srv *server.Server

	register := func(specs []server.DynamicEntity, opts ...server.RegisterOption) error {
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(specs...), opts...)
		return err
	}

	BeforeEach(func() {
		// Given a server that serves the evolution schema
		srv = newBareServer()
		Expect(register(evolutionSpecs())).To(Succeed())
	})

	DescribeTable("given a change to the access model, when a registration applies it",
		func(change func([]server.DynamicEntity) []server.DynamicEntity) {
			// When the registration does not allow access changes
			err := register(change(evolutionSpecs()))

			// Then it is refused, and allowed with the flag
			Expect(err).To(HaveOccurred(), "an access change was registered without AllowAccessChange")
			Expect(err.Error()).To(Or(ContainSubstring("changes its access model"), ContainSubstring("role")))
			Expect(register(change(evolutionSpecs()), server.AllowAccessChange())).To(Succeed())
		},
		Entry("a role grants one more permission", func(s []server.DynamicEntity) []server.DynamicEntity {
			r := specNamed(s, "org_member").Grant.Roles[1]
			r.Grants = append(r.Grants, "view_projects")
			return s
		}),
		Entry("a role grants one permission less", func(s []server.DynamicEntity) []server.DynamicEntity {
			// org_viewer still grants view_tasks.
			r := specNamed(s, "org_member").Grant.Roles[0]
			r.Grants = []string{"view_members", "manage_members", "view_projects", "edit_projects", "edit_tasks",
				"view_teams", "manage_teams", "view_labels"}
			return s
		}),
		Entry("an include changes what a role grants", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "org_member").Grant.Roles[2].Includes = nil
			return s
		}),
		Entry("a role is removed", func(s []server.DynamicEntity) []server.DynamicEntity {
			g := specNamed(s, "org_member").Grant
			g.Roles = g.Roles[:2]
			return s
		}),
		Entry("a new rule", func(s []server.DynamicEntity) []server.DynamicEntity {
			t := specNamed(s, "task")
			t.Access.Allow = append(t.Access.Allow, permRule("view_projects", "read"))
			return s
		}),
		Entry("a rule names other operations", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[1] = permRule("edit_tasks", "create", "update")
			return s
		}),
		Entry("a rule needs another permission", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[0].Requires = []string{"view_projects"}
			return s
		}),
		Entry("a rule needs one more permission", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[0].Requires = []string{"view_tasks", "view_projects"}
			return s
		}),
		Entry("a rule gets a caller_is check", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[1].CallerIs = "author"
			return s
		}),
		Entry("a rule gets a fields check", func(s []server.DynamicEntity) []server.DynamicEntity {
			t := specNamed(s, "task")
			t.Access.Allow = append(t.Access.Allow, &syncplanev1.AccessRule{Update: true, Requires: []string{"view_tasks"}, Fields: []string{"title"}})
			return s
		}),
		Entry("a rule gets a when check", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[1].When = stringIs("title", "open")
			return s
		}),
		Entry("a rule gets a within entry", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[0].Within = []*syncplanev1.AccessWithin{{Entity: "project", CallerIs: "owner_id"}}
			return s
		}),
		Entry("a within entry changes its check", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Allow[0].Within = []*syncplanev1.AccessWithin{{Entity: "project", When: stringIs("state", "open")}}
			return s
		}),
		Entry("a guard is added", func(s []server.DynamicEntity) []server.DynamicEntity {
			t := specNamed(s, "task")
			t.Access.Guard = append(t.Access.Guard, &syncplanev1.FieldGuard{
				Fields: []string{"title"}, Allow: []*syncplanev1.AccessRule{permRule("edit_projects", "update")},
			})
			return s
		}),
		Entry("a guard is removed", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Guard = nil
			return s
		}),
		Entry("a guard rule needs another permission", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Guard[0].Allow[0].Requires = []string{"edit_projects"}
			return s
		}),
		Entry("a guard gets a read rule that hides its field", func(s []server.DynamicEntity) []server.DynamicEntity {
			g := specNamed(s, "task").Access.Guard[0]
			g.Allow = append(g.Allow, permRule("edit_tasks", "read"))
			return s
		}),
		Entry("a guard restricts one more field", func(s []server.DynamicEntity) []server.DynamicEntity {
			g := specNamed(s, "task").Access.Guard[0]
			g.Fields = append(g.Fields, "title")
			return s
		}),
		Entry("an entity loses its access parent", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Access.Parents = nil
			return s
		}),
		Entry("an entity leaves the model", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "label").Access = nil
			return s
		}),
		Entry("a team loses its members option", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "team").Members = nil
			return s
		}),
		Entry("a creator mark is removed", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Fields[4].Creator = false
			return s
		}),
		Entry("a creator mark is added", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Fields[2].Creator = true
			return s
		}),
		Entry("an admitted rule needs a permission", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "label").Access.Allow[0] = permRule("view_labels", "read")
			return s
		}),
		Entry("a permission rule becomes admitted", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "label").Access.Allow[1] = admittedRule("write")
			return s
		}),
		Entry("a grant names another subject field", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "team_member").Grant.Subject = "alt_user"
			return s
		}),
		Entry("a grant names another role field", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "team_member").Grant.Role = "alt_role"
			return s
		}),
		Entry("an entity drops its grant option", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "team_member").Grant = nil
			specNamed(s, "team").Members = nil
			return s
		}),
	)

	DescribeTable("given a change that leaves access as it is, when a registration applies it",
		func(change func([]server.DynamicEntity) []server.DynamicEntity) {
			// When the registration does not allow access changes
			err := register(change(evolutionSpecs()))

			// Then it is accepted
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("rules in another order", func(s []server.DynamicEntity) []server.DynamicEntity {
			a := specNamed(s, "task").Access
			a.Allow[0], a.Allow[1] = a.Allow[1], a.Allow[0]
			return s
		}),
		Entry("roles in another order", func(s []server.DynamicEntity) []server.DynamicEntity {
			g := specNamed(s, "org_member").Grant
			g.Roles[0], g.Roles[1] = g.Roles[1], g.Roles[0]
			return s
		}),
		Entry("a requires list in another order", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "label").Access.Allow[1].Requires = []string{"view_labels"}
			return s
		}),
		Entry("a new field without access marks", func(s []server.DynamicEntity) []server.DynamicEntity {
			t := specNamed(s, "task")
			t.Fields = append(t.Fields, server.DynamicField{Name: "notes", Number: 9, Kind: protoreflect.StringKind})
			return s
		}),
		Entry("an indexed mark on a field", func(s []server.DynamicEntity) []server.DynamicEntity {
			specNamed(s, "task").Fields[2].Indexed = true
			return s
		}),
		Entry("a new entity in the model", func(s []server.DynamicEntity) []server.DynamicEntity {
			return append(s, server.DynamicEntity{
				Name: "milestone", KeyFields: []string{"id"},
				Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
				Access: readWrite("view_projects", "edit_projects"),
			})
		}),
	)
})
