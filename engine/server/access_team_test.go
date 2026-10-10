package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// teamEntities registers the team schema of the access test proto: squads
// whose squad_member rows make members, repos whose repo_member rows grant to
// a user or to a squad, repo_squad rows that grant to a squad alone, and
// issues below the repos.
func teamEntities() server.Option {
	return server.WithEntities(
		&accessv1.Squad{}, &accessv1.SquadMember{}, &accessv1.Repo{},
		&accessv1.RepoMember{}, &accessv1.RepoSquad{}, &accessv1.Issue{},
	)
}

func squad(id string) *accessv1.Squad { return &accessv1.Squad{Id: id} }

func repo(id string) *accessv1.Repo { return &accessv1.Repo{Id: id} }

func squadMember(id, squadID, user, role string) *accessv1.SquadMember {
	return &accessv1.SquadMember{Id: id, Squad: squad(squadID), UserId: user, Role: role}
}

func repoUser(id, repoID, user, role string) *accessv1.RepoMember {
	return &accessv1.RepoMember{Id: id, Repo: repo(repoID), Member: &accessv1.RepoMember_UserId{UserId: user}, Role: role}
}

func repoForSquad(id, repoID, squadID, role string) *accessv1.RepoMember {
	return &accessv1.RepoMember{Id: id, Repo: repo(repoID), Member: &accessv1.RepoMember_Squad{Squad: squad(squadID)}, Role: role}
}

func issue(id, repoID string) *accessv1.Issue {
	return &accessv1.Issue{Id: id, Repo: repo(repoID), Title: id}
}

// seedSquads writes the rows every team spec starts from:
//
//   - squad design has dana as lead and dave as member, squad ops has olga
//     and rita, and squad lonely has no members;
//   - design writes in repo r1 and reads r3 through a repo_squad row, ops
//     reads r2, and lonely reads r2;
//   - rita reads r1 herself, and admin administers r1 and r2;
//   - ghosts writes in r1, and gus is a member of ghosts, but no squad ghosts
//     exists;
//   - issues i1, i2 and i3 are in r1, r2 and r3.
func (f *accessFixture) seedSquads() {
	GinkgoHelper()
	f.seed(
		squad("design"), squad("ops"), squad("lonely"),
		squadMember("sm-dana", "design", "dana", "squad_lead"),
		squadMember("sm-dave", "design", "dave", "squad_member"),
		squadMember("sm-olga", "ops", "olga", "squad_member"),
		squadMember("sm-rita", "ops", "rita", "squad_member"),
		squadMember("sm-gus", "ghosts", "gus", "squad_member"),
		repo("r1"), repo("r2"), repo("r3"),
		repoForSquad("rm-design", "r1", "design", "repo_writer"),
		&accessv1.RepoSquad{Id: "rs-design", Repo: repo("r3"), Squad: squad("design"), Role: "repo_squad_reader"},
		repoForSquad("rm-ops", "r2", "ops", "repo_reader"),
		repoForSquad("rm-lonely", "r2", "lonely", "repo_reader"),
		repoUser("rm-rita", "r1", "rita", "repo_reader"),
		repoUser("rm-admin1", "r1", "admin", "repo_admin"),
		repoUser("rm-admin2", "r2", "admin", "repo_admin"),
		repoForSquad("rm-ghosts", "r1", "ghosts", "repo_writer"),
		issue("i1", "r1"), issue("i2", "r2"), issue("i3", "r3"),
	)
}

var _ = Describe("Access model registration of teams", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(change func(*descriptorpb.FileDescriptorProto), opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(accessFileSet(change), opts...)
		return err
	}
	optionsOf := func(file *descriptorpb.FileDescriptorProto, message string) *syncplanev1.EntityOptions {
		GinkgoHelper()
		return proto.GetExtension(messageNamed(file, message).GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
	}
	fieldNamed := func(m *descriptorpb.DescriptorProto, name string) *descriptorpb.FieldDescriptorProto {
		GinkgoHelper()
		for _, fd := range m.GetField() {
			if fd.GetName() == name {
				return fd
			}
		}
		Fail("no field named " + name)
		return nil
	}

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), teamEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	It("registers the descriptor set", func() {
		Expect(register(nil)).To(Succeed())
	})

	DescribeTable("refuses a schema that breaks a rule of teams",
		func(change func(*descriptorpb.FileDescriptorProto), want string) {
			Expect(register(change)).To(MatchError(ContainSubstring(want)))
		},
		Entry("members without an entity", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "Squad").Members.Entity = ""
		}, `entity "squad": members names no entity`),
		Entry("unknown fields inside members", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "Squad").Members.ProtoReflect().SetUnknown(unknownField())
		}, "members carries option fields this server does not know"),
		Entry("members that names an entity which is not registered", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "Squad").Members.Entity = "nobody"
		}, `entity "squad" members names entity "nobody", which is not registered`),
		Entry("members that names an entity which grants nothing", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "Squad").Members.Entity = "repo"
		}, `entity "squad" members names entity "repo", which is not a grant entity`),
		Entry("members that names a grant entity anchored at another entity", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "Squad").Members.Entity = "repo_member"
		}, `entity "squad" members names entity "repo_member", whose access_parent does not point at "squad" alone`),
		Entry("members whose subject is a team", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "SquadMember").Grant.Subject = "squad"
		}, `entity "squad" members names entity "squad_member", whose grant subject is not one string field; a team cannot be a member of a team`),
		Entry("a grant subject that points at an entity without members", func(file *descriptorpb.FileDescriptorProto) {
			optionsOf(file, "RepoSquad").Grant.Subject = "repo"
		}, `entity "repo_squad" grant subject field "repo" points at entity "repo", which declares no members`),
		Entry("a grant subject that is a repeated reference", func(file *descriptorpb.FileDescriptorProto) {
			fieldNamed(messageNamed(file, "RepoSquad"), "squad").Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}, `grant subject field "squad" is repeated`),
		Entry("a grant subject oneof with a member that is neither a string nor a reference", func(file *descriptorpb.FileDescriptorProto) {
			// A oneof's members are declared together, so it goes after the
			// last one.
			m := messageNamed(file, "RepoMember")
			at := slices.Index(m.Field, fieldNamed(m, "squad")) + 1
			m.Field = slices.Insert(m.Field, at, &descriptorpb.FieldDescriptorProto{
				Name:       proto.String("rank"),
				JsonName:   proto.String("rank"),
				Number:     proto.Int32(9),
				Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				OneofIndex: proto.Int32(0),
			})
		}, `grant subject field "rank" is int64; it must be a string or a reference`),
	)

	// teamSpecs adds to org specs a team whose team_member rows make members,
	// and, with grants, a project_team entity that grants to a team through a
	// plain reference.
	teamSpecs := func(grants bool) []server.DynamicEntity {
		str := protoreflect.StringKind
		specs := append(orgSpecs(),
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
					{Name: "team", Number: 2, Ref: "team", AccessParent: true},
					{Name: "user_id", Number: 3, Kind: str},
					{Name: "role", Number: 4, Kind: str},
				},
				Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{
					{Name: "team_lead", Grants: []string{"view_teams", "manage_teams"}},
				}},
				Access: readWrite("view_teams", "manage_teams"),
			},
		)
		if !grants {
			return specs
		}
		return append(specs, server.DynamicEntity{
			Name: "project_team", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: str},
				{Name: "project", Number: 2, Ref: "project", AccessParent: true},
				{Name: "team", Number: 3, Ref: "team"},
				{Name: "role", Number: 4, Kind: str},
			},
			Grant: &syncplanev1.GrantOptions{Subject: "team", Role: "role", Roles: []*syncplanev1.Role{
				{Name: "project_team_writer", Grants: []string{"view_projects", "view_tasks", "edit_tasks"}},
			}},
			Access: readWrite("view_projects", "edit_projects"),
		})
	}

	It("registers dynamic entities that grant to a team through a plain reference", func() {
		Expect(srv.RegisterDynamicEntities(teamSpecs(true)...)).To(Succeed())
	})

	It("changes the access spec when an entity declares members, so the change needs the flag", func() {
		registerSpecs := func(specs []server.DynamicEntity, opts ...server.RegisterOption) error {
			_, err := srv.RegisterDescriptorSet(descriptorSetBytes(specs...), opts...)
			return err
		}
		without := teamSpecs(false)
		specNamed(without, "team").Members = nil
		Expect(registerSpecs(without)).To(Succeed())

		Expect(registerSpecs(teamSpecs(false))).To(MatchError(ContainSubstring(`entity "team" changes its access model`)))
		Expect(registerSpecs(teamSpecs(false), server.AllowAccessChange())).To(Succeed())
	})
})

var _ = Describe("Access model teams", func() {
	var (
		f       *accessFixture
		clients int
	)

	push := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(user, entity, key string, value proto.Message) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return putMutation(c, 1, entity, key, value) })
	}
	del := func(user, entity, key string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return deleteMutation(c, 1, entity, key) })
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	rejected := func(r *syncplanev1.MutationResult, code string) string {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(code), r.GetErrorMessage())
		return r.GetErrorMessage()
	}
	trustedDelete := func(prototype proto.Message, key string) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(prototype, key)
		})).To(Succeed())
	}
	// staff holds squad_lead at the root of every workspace: the squads'
	// permissions without a squad_member row.
	staff := server.WithGlobalRoles(func(_ context.Context, p server.Principal) ([]string, error) {
		if p.Subject == "staff" {
			return []string{"squad_lead"}, nil
		}
		return nil, nil
	})

	// The specs read through the indexes the resolution probes, which a
	// workspace has only once they are built. One spec reads without them.
	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), teamEntities(), staff)
		f.seedSquads()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	Describe("reads", func() {
		It("lets every member of a squad read what a grant to the squad gives", func() {
			Expect(f.pull("dave")).To(SatisfyAll(
				ContainElements("repo/r1", "issue/i1", "repo/r3", "issue/i3", "squad/design"),
				lackAll("issue/i2", "repo/r2"),
			))
			Expect(f.pull("dana")).To(ContainElements("issue/i1", "issue/i3"))
		})

		It("adds the grants of the caller's squads to the caller's own", func() {
			Expect(f.pull("rita")).To(ContainElements("issue/i1", "issue/i2"))
		})

		It("gives nothing to a caller in no squad the grant names", func() {
			Expect(f.pull("olga")).To(SatisfyAll(ContainElement("issue/i2"), lackAll("issue/i1", "issue/i3")))
		})

		It("does not make a member of a caller who holds the squad's permissions at the root", func() {
			Expect(f.pull("staff")).To(SatisfyAll(
				ContainElements("squad/design", "squad_member/sm-dave"),
				lackAll("issue/i1", "issue/i2", "issue/i3"),
			))
		})

		It("grants nothing through a squad that does not exist", func() {
			_, err := f.pullErr("gus")
			Expect(err).To(HaveOccurred())
		})

		It("resolves a squad's grants without the indexes", func() {
			f = newSchemaFixtureOn(newTestStore(), teamEntities(), staff, server.WithLimits(server.Limits{DisableIndexes: true}))
			f.seedSquads()
			Expect(f.pull("dave")).To(SatisfyAll(ContainElements("issue/i1", "issue/i3"), lackAll("issue/i2")))
		})

		It("sends the anchors a squad's grant gives in the CallerAccess summary", func() {
			r := newAccessReplica(f, "dave")
			resp := r.pullOnce(0)
			held := heldAt(resp.GetCallerAccess())
			Expect(held).To(HaveKeyWithValue("edit_issues", []string{"repo/r1"}))
			Expect(held).To(HaveKeyWithValue("view_issues", ConsistOf("repo/r1", "repo/r3")))
		})
	})

	Describe("writes", func() {
		It("lets a member write where the squad's role allows it", func() {
			applied(put("dave", "issue", "i-new", issue("i-new", "r1")))
			rejected(put("olga", "issue", "i-new2", issue("i-new2", "r2")), server.CodeAccessDenied)
		})

		It("lets a repo admin grant a role to a squad only where the admin holds it", func() {
			applied(put("admin", "repo_member", "rm-ops1", repoForSquad("rm-ops1", "r1", "ops", "repo_writer")))
			rejected(put("admin", "repo_member", "rm-ops3", repoForSquad("rm-ops3", "r3", "ops", "repo_reader")), server.CodeAccessDenied)
			Expect(f.pull("olga")).To(ContainElement("issue/i1"))
		})

		It("refuses a grant row that names no subject", func() {
			rejected(put("admin", "repo_member", "rm-none", &accessv1.RepoMember{Id: "rm-none", Repo: repo("r1"), Role: "repo_reader"}),
				server.CodeInvalidArgument)
			rejected(put("admin", "repo_member", "rm-blank", repoForSquad("rm-blank", "r1", "", "repo_reader")),
				server.CodeInvalidArgument)
		})

		It("lets a squad lead add a member, who then reads what the squad's grants give", func() {
			applied(put("dana", "squad_member", "sm-nina", squadMember("sm-nina", "design", "nina", "squad_member")))
			rejected(put("dave", "squad_member", "sm-nils", squadMember("sm-nils", "design", "nils", "squad_member")),
				server.CodeAccessDenied)

			Expect(f.pull("nina")).To(ContainElements("issue/i1", "issue/i3"))
		})

		It("refuses deleting a squad that a live grant names", func() {
			msg := rejected(del("staff", "squad", "lonely"), server.CodeConflict)
			Expect(msg).To(ContainSubstring(`row "lonely" is the subject of a live repo_member row`))

			applied(del("admin", "repo_member", "rm-lonely"))
			applied(del("staff", "squad", "lonely"))
		})

		It("lets a user leave a repo by deleting their own row, and not a squad's row", func() {
			rejected(del("dave", "repo_member", "rm-design"), server.CodeAccessDenied)
			applied(del("rita", "repo_member", "rm-rita"))
		})
	})

	Describe("sync", func() {
		It("sends what a squad's grants give when the caller joins, and removes it when the caller leaves", func() {
			r := newAccessReplica(f, "rita")
			r.sync()
			Expect(r.view()).NotTo(ContainElement("issue/i3"))

			f.seed(squadMember("sm-rita2", "design", "rita", "squad_member"))
			r.sync()
			Expect(r.view()).To(ContainElement("issue/i3"))

			trustedDelete(&accessv1.SquadMember{}, "sm-rita2")
			r.sync()
			Expect(r.view()).NotTo(ContainElement("issue/i3"))
			Expect(r.resets).To(BeZero())
		})

		It("removes what a grant to a squad gave from every member when the grant is deleted", func() {
			dave, dana := newAccessReplica(f, "dave"), newAccessReplica(f, "dana")
			dave.sync()
			dana.sync()

			trustedDelete(&accessv1.RepoMember{}, "rm-design")
			dave.sync()
			dana.sync()
			Expect(dave.view()).NotTo(ContainElement("issue/i1"))
			Expect(dana.view()).NotTo(ContainElement("issue/i1"))
			Expect(dave.view()).To(ContainElement("issue/i3"))
		})
	})
})
