package server_test

import (
	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A rule with caller_is admits a caller on the rows whose field names it, so
// what the rule keeps from a caller depends on a field that other writers may
// change.

// pulledRowOf returns the value of the row of entity with key that a full pull
// shows user, and the fields the pull names as hidden; found is false when the
// pull does not show the row.
func pulledRowOf(f *accessFixture, user, entity, key string, into proto.Message) (hidden []string, found bool) {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-rowof-" + user})
	as(user, req.Header())
	resp, err := f.sync.Pull(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	for _, rc := range resp.Msg.GetPatch().GetChanges() {
		if rc.GetEntity() == entity && rc.GetKey() == key && !rc.GetTombstone() {
			Expect(proto.Unmarshal(rc.GetValue(), into)).To(Succeed())
			return rc.GetHidden(), true
		}
	}
	return nil, false
}

var _ = Describe("Access model fields that a caller_is check reads", func() {
	var f *accessFixture

	It("keeps a review hidden from a caller that points the record at itself", func() {
		// Given: staff s1 in p1 is about guest, and only guest reads its
		// review. padmin writes the staff of p1 and does not read the review.
		f = newAccessFixture(server.WithEntities(&accessv1.Staff{}))
		f.seedOrg()
		f.seed(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Review: "needs coaching", PersonId: "guest"})
		var before accessv1.Staff
		hidden, found := pulledRowOf(f, "padmin", "staff", "s1", &before)
		Expect(found).To(BeTrue())
		Expect(hidden).To(ContainElement("review"))

		// When: padmin sets person_id to itself.
		results := f.push("padmin", "c-padmin", patchMutation("c-padmin", 1, "staff", "s1",
			&accessv1.Staff{Id: "s1", PersonId: "padmin"}, "person_id"))

		// Then: the write is refused, and padmin still does not read the
		// review written about guest.
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeAccessDenied), results[0].GetStatus().String())
		var after accessv1.Staff
		_, found = pulledRowOf(f, "padmin", "staff", "s1", &after)
		Expect(found).To(BeTrue())
		Expect(after.GetReview()).To(BeEmpty(), "padmin read the review written about guest")
	})

	It("refuses a move by a caller who reads only its own rows of an entity below the moved row", func() {
		// Given: page pg1 on t1 in p1 belongs to alice. blind edits tasks in p1
		// and p2 and reads only the pages it owns; writer reads the tasks and
		// pages of p2.
		f = newAccessFixture(server.WithEntities(&accessv1.Page{}))
		f.seedOrg()
		f.seed(
			&accessv1.ProjectMember{Id: "pm-blind1", Project: project("p1"), UserId: "blind", Role: "project_editor"},
			&accessv1.ProjectMember{Id: "pm-blind2", Project: project("p2"), UserId: "blind", Role: "project_editor"},
			&accessv1.Page{Id: "pg1", Task: &accessv1.Task{Id: "t1"}, OwnerId: "alice"},
		)
		Expect(f.pull("blind")).NotTo(ContainElement("page/pg1"))
		Expect(f.pull("writer")).NotTo(ContainElement("page/pg1"))

		// When: blind moves t1 to p2.
		results := f.push("blind", "c-blind", patchMutation("c-blind", 1, "task", "t1",
			&accessv1.Task{Id: "t1", Project: project("p2")}, "project"))

		// Then: the move is refused, as for a caller who reads no page, and
		// writer does not read alice's page.
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeAccessDenied), results[0].GetStatus().String())
		Expect(f.pull("writer")).NotTo(ContainElement("page/pg1"))
	})
})
