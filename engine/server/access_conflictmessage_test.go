package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A refusal names only what the caller sent. The fixture is seedOrg with
// seedComments: comment c1 sits below task t1 in p1, and blind edits the tasks
// of p1 without reading them or their comments.
var _ = Describe("Access model conflicts over rows below a row", func() {
	var f *accessFixture

	BeforeEach(func() {
		f = newDeepFixtureOn(newTestStore())
		f.seedOrg()
		f.seedComments()
		f.seed(&accessv1.ProjectMember{Id: "pm-blind1", Project: project("p1"), UserId: "blind", Role: "project_editor"})
		Expect(f.pull("blind")).NotTo(ContainElement("comment/c1"))
	})

	It("does not name the entity of a row below that the caller cannot read when it refuses a delete", func() {
		// When: blind deletes t1.
		results := f.push("blind", "c-blind", deleteMutation("c-blind", 1, "task", "t1"))

		// Then: it is refused as a conflict, and the message does not tell
		// that a comment sits below t1.
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeConflict), results[0].GetStatus().String())
		Expect(results[0].GetErrorMessage()).NotTo(ContainSubstring("comment"))
	})

	It("does not name the entity of a row below that the caller cannot read when it refuses a create", func() {
		// Given: a trusted write deletes t1, and c1 stays.
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&accessv1.Task{}, "t1")
		})).To(Succeed())

		// When: blind creates t1 in p1 again.
		results := f.push("blind", "c-blind",
			putMutation("c-blind", 1, "task", "t1", &accessv1.Task{Id: "t1", Project: project("p1"), Title: "again"}))

		// Then: it is refused as a conflict, and the message does not tell
		// that a comment names t1.
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeConflict), results[0].GetStatus().String())
		Expect(results[0].GetErrorMessage()).NotTo(ContainSubstring("comment"))
	})
})
