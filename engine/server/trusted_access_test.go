package server_test

import (
	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// trustedPullFrom pulls every page after cursor as user, and returns what the
// pages carried as "entity/key" (live rows) and "-entity/key" (tombstones),
// and the cursor after the last page.
func trustedPullFrom(f *accessFixture, user string, cursor []byte) ([]string, []byte) {
	GinkgoHelper()
	var out []string
	for more := true; more; {
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-" + user, Cursor: cursor})
		as(user, req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetReset_()).To(BeFalse(), "the pull reset the client")
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			name := rc.GetEntity() + "/" + rc.GetKey()
			if rc.GetTombstone() {
				name = "-" + name
			}
			out = append(out, name)
		}
		cursor, more = resp.Msg.GetNextCursor(), resp.Msg.GetHasMore()
	}
	return out, cursor
}

// Trusted writes skip the access rules, but the engine's own bookkeeping of
// the model (the move log, the cycle and depth checks) must still run for
// them, also while a run of RegisterEntity calls leaves the model incomplete.
var _ = Describe("Trusted writes while a registration leaves the access model incomplete", func() {
	It("tombstones the comments below a task that a trusted write moved out of the reader's project", func() {
		// Given guest reads project p1, and holds t1 and its comment c1
		f := newAccessFixture(server.WithEntities(&accessv1.Comment{}))
		f.seedOrg()
		f.seed(&accessv1.Comment{Id: "c1", Task: &accessv1.Task{Id: "t1"}, Body: "on one"})
		rows, cursor := trustedPullFrom(f, "guest", nil)
		Expect(rows).To(ContainElements("task/t1", "comment/c1"))

		// And a runtime registration leaves the model incomplete: team_member
		// names the access parent "team", which is not registered yet
		Expect(f.srv.RegisterEntity(&accessv1.TeamMember{})).To(Succeed())

		// When a connector moves t1 from p1 to p2, which guest does not read
		Expect(f.srv.Mutate(server.TrustedAs(f.ctx, "connector"), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Task{Id: "t1", Project: project("p2")}, []string{"project"}, nil)
		})).To(Succeed())

		// And the registration completes the model
		Expect(f.srv.RegisterEntity(&accessv1.Team{})).To(Succeed())

		// Then guest's next pull takes t1 and the comment below it away
		rows, _ = trustedPullFrom(f, "guest", cursor)
		Expect(rows).To(ContainElement("-task/t1"))
		Expect(rows).To(ContainElement("-comment/c1"), "guest still holds a comment of a task in p2")

		// And a fresh pull agrees that guest reads neither
		Expect(f.pull("guest")).NotTo(ContainElements("task/t1", "comment/c1"))
	})

	It("refuses a trusted write that would put a folder below itself", func() {
		// Given a folder tree where mid sits below top
		f := newFolderFixtureOn(newTestStore())
		f.seed(folder("top"), &accessv1.Folder{Id: "mid", Parent: folder("top")})

		// And a runtime registration leaves the model incomplete
		Expect(f.srv.RegisterEntity(&accessv1.TeamMember{})).To(Succeed())

		// When a connector moves top below mid
		err := f.srv.Mutate(server.TrustedAs(f.ctx, "connector"), ws, func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.Folder{Id: "top", Parent: folder("mid")})
		})

		// Then the write is refused, as it is while the model is complete
		Expect(err).To(HaveOccurred(), "a trusted write stored a cycle of access parents")
	})
})
