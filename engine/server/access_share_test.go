package server_test

import (
	"fmt"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// shareEntities registers the share-control schema of the access test proto:
// a commons row that every commons_member grant anchors at, stewards at the
// root, initiatives that are shared below the commons or restricted at the
// root, their grants to users and squads, and milestones below them.
func shareEntities() server.Option {
	return server.WithEntities(
		&accessv1.Commons{}, &accessv1.CommonsMember{}, &accessv1.Steward{},
		&accessv1.Squad{}, &accessv1.SquadMember{},
		&accessv1.Initiative{}, &accessv1.InitiativeMember{}, &accessv1.Milestone{},
	)
}

func commons() *accessv1.Commons { return &accessv1.Commons{Id: "main"} }

func initiative(id string) *accessv1.Initiative { return &accessv1.Initiative{Id: id} }

func initiativeUser(id, initiativeID, user, role string) *accessv1.InitiativeMember {
	return &accessv1.InitiativeMember{
		Id: id, Initiative: initiative(initiativeID),
		Member: &accessv1.InitiativeMember_UserId{UserId: user}, Role: role,
	}
}

// seedShares writes the rows every share-control spec starts from:
//
//   - stella is a steward at the root;
//   - amy and pat are commons members, and pat administers both initiatives;
//   - gil reads launch, and squad design, with dana in it, reads launch too;
//     neither is a commons member;
//   - roadmap is shared and launch is restricted, with milestones m-roadmap
//     and m-launch below them.
func (f *accessFixture) seedShares() {
	GinkgoHelper()
	f.seed(
		commons(),
		&accessv1.Steward{Id: "st-stella", UserId: "stella", Role: "steward"},
		&accessv1.CommonsMember{Id: "cm-amy", Commons: commons(), UserId: "amy", Role: "commons_member"},
		&accessv1.CommonsMember{Id: "cm-pat", Commons: commons(), UserId: "pat", Role: "commons_member"},
		squad("design"),
		squadMember("sm-dana", "design", "dana", "squad_member"),
		&accessv1.Initiative{Id: "roadmap", Commons: commons(), Name: "Roadmap"},
		&accessv1.Initiative{Id: "launch", Name: "Launch"},
		&accessv1.Milestone{Id: "m-roadmap", Initiative: initiative("roadmap"), Title: "Q1"},
		&accessv1.Milestone{Id: "m-launch", Initiative: initiative("launch"), Title: "Beta"},
		initiativeUser("im-pat-roadmap", "roadmap", "pat", "initiative_admin"),
		initiativeUser("im-pat-launch", "launch", "pat", "initiative_admin"),
		initiativeUser("im-gil", "launch", "gil", "initiative_reader"),
		&accessv1.InitiativeMember{
			Id: "im-design", Initiative: initiative("launch"),
			Member: &accessv1.InitiativeMember_Squad{Squad: squad("design")}, Role: "initiative_reader",
		},
	)
}

var _ = Describe("Access model share control through a commons row", func() {
	var (
		f       *accessFixture
		clients int
	)

	one := func(user string, m *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		results := f.push(user, m.GetClientId(), m)
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	client := func() string {
		clients++
		return fmt.Sprintf("c%d", clients)
	}
	share := func(user, id string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return one(user, patchMutation(client(), 1, "initiative", id,
			&accessv1.Initiative{Id: id, Commons: commons()}, "commons"))
	}
	restrict := func(user, id string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return one(user, patchMutation(client(), 1, "initiative", id, initiative(id), "commons"))
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	refused := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied), r.GetErrorMessage())
	}
	converged := func(r *accessReplica) {
		GinkgoHelper()
		Expect(r.view()).To(Equal(f.pull(r.user)))
		Expect(r.resets).To(BeZero())
	}

	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), shareEntities())
		f.seedShares()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), shareEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	DescribeTable("shows a shared initiative to every commons member, and a restricted one to its own grants",
		func(user string, want []string) {
			rows := f.pull(user)
			Expect(append(rowsOf("initiative", rows), rowsOf("milestone", rows)...)).To(Equal(want))
		},
		Entry("a commons member", "amy",
			[]string{"initiative/roadmap", "milestone/m-roadmap"}),
		Entry("a commons member with a grant on the restricted one", "pat",
			[]string{"initiative/launch", "initiative/roadmap", "milestone/m-launch", "milestone/m-roadmap"}),
		Entry("a guest with a grant of their own", "gil",
			[]string{"initiative/launch", "milestone/m-launch"}),
		Entry("a guest through a squad", "dana",
			[]string{"initiative/launch", "milestone/m-launch"}),
		Entry("a steward at the root", "stella",
			[]string{"initiative/launch", "initiative/roadmap", "milestone/m-launch", "milestone/m-roadmap"}),
	)

	It("shares a restricted initiative with the workspace in one update by its admin", func() {
		amy := newAccessReplica(f, "amy")
		gil := newAccessReplica(f, "gil")
		amy.sync()
		gil.sync()
		Expect(amy.view()).NotTo(ContainElements("initiative/launch", "milestone/m-launch"))

		applied(share("pat", "launch"))
		amy.sync()
		gil.sync()
		Expect(amy.view()).To(ContainElements("initiative/launch", "milestone/m-launch"))
		Expect(gil.view()).To(ContainElements("initiative/launch", "milestone/m-launch"))
		converged(amy)
		converged(gil)
	})

	It("restricts a shared initiative, and keeps it for callers with grants of their own", func() {
		amy := newAccessReplica(f, "amy")
		pat := newAccessReplica(f, "pat")
		amy.sync()
		pat.sync()
		Expect(amy.view()).To(ContainElements("initiative/roadmap", "milestone/m-roadmap"))

		applied(restrict("pat", "roadmap"))
		amy.sync()
		pat.sync()
		Expect(amy.view()).NotTo(ContainElement("initiative/roadmap"))
		Expect(amy.view()).NotTo(ContainElement("milestone/m-roadmap"))
		Expect(pat.view()).To(ContainElements("initiative/roadmap", "milestone/m-roadmap"))
		converged(amy)
		converged(pat)
	})

	It("lets a commons member edit a shared initiative but not restrict it", func() {
		applied(one("amy", patchMutation(client(), 1, "initiative", "roadmap",
			&accessv1.Initiative{Id: "roadmap", Name: "Renamed"}, "name")))
		refused(restrict("amy", "roadmap"))
	})

	It("refuses to share a restricted initiative for a caller without the write permission on it", func() {
		refused(share("gil", "launch"))
		refused(share("amy", "launch"))
	})

	It("lets a commons member create a shared initiative but not a restricted one", func() {
		applied(one("amy", putMutation(client(), 1, "initiative", "open",
			&accessv1.Initiative{Id: "open", Commons: commons(), Name: "Open"})))
		refused(one("amy", putMutation(client(), 1, "initiative", "closed",
			&accessv1.Initiative{Id: "closed", Name: "Closed"})))
	})
})
