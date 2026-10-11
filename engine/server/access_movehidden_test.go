package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A move needs, where the row is before it, the right to read the rows below
// it and the fields of those rows that a guard can hide. The fixture is
// "staff", whose salary only a caller with edit_projects reads.
var _ = Describe("Access model moves of a row with fields the mover cannot read", func() {
	It("does not let a caller read a hidden field by moving the row to where it reads that field", func() {
		// Given: s1 in p1 has a salary. padmin writes staff in p1 without
		// edit_projects, so it does not read the salary there, and owns p2,
		// where it reads salaries.
		f := newAccessFixture(server.WithEntities(&accessv1.Staff{}))
		f.seedOrg()
		f.seed(
			&accessv1.ProjectMember{Id: "pm-padmin-own2", Project: project("p2"), UserId: "padmin", Role: "project_owner"},
			&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000, PersonId: "guest"},
		)
		var before accessv1.Staff
		hidden, found := pulledRowOf(f, "padmin", "staff", "s1", &before)
		Expect(found).To(BeTrue())
		Expect(hidden).To(ContainElement("salary"))

		// When: padmin moves s1 to p2.
		f.push("padmin", "c-padmin", patchMutation("c-padmin", 1, "staff", "s1",
			&accessv1.Staff{Id: "s1", Project: project("p2")}, "project"))

		// Then: padmin still does not read the salary it could not read in p1.
		var after accessv1.Staff
		_, found = pulledRowOf(f, "padmin", "staff", "s1", &after)
		Expect(found).To(BeTrue())
		Expect(after.GetSalary()).To(BeZero(), "padmin read the salary of a record it moved out of p1")
	})

	It("does not let a caller read a row by moving it to where it reads that entity", func() {
		// Given: blind edits the tasks of p1 without reading them, and writes
		// and reads the tasks of p2. t1 is in p1.
		f := newAccessFixture()
		f.seedOrg()
		f.seed(
			&accessv1.ProjectMember{Id: "pm-blind1", Project: project("p1"), UserId: "blind", Role: "project_editor"},
			&accessv1.ProjectMember{Id: "pm-blind2", Project: project("p2"), UserId: "blind", Role: "project_writer"},
		)
		Expect(f.pull("blind")).NotTo(ContainElement("task/t1"))

		// When: blind moves t1 to p2.
		f.push("blind", "c-blind", patchMutation("c-blind", 1, "task", "t1",
			&accessv1.Task{Id: "t1", Project: project("p2")}, "project"))

		// Then: blind still does not read t1, which it could not read in p1.
		Expect(f.pull("blind")).NotTo(ContainElement("task/t1"))
	})

	It("refuses a move by a caller who cannot read a hidden field of the row where it is", func() {
		// Given: as above.
		f := newAccessFixture(server.WithEntities(&accessv1.Staff{}))
		f.seedOrg()
		f.seed(
			&accessv1.ProjectMember{Id: "pm-padmin-own2", Project: project("p2"), UserId: "padmin", Role: "project_owner"},
			&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000, PersonId: "guest"},
		)

		// When: padmin moves s1 to p2.
		results := f.push("padmin", "c-padmin", patchMutation("c-padmin", 1, "staff", "s1",
			&accessv1.Staff{Id: "s1", Project: project("p2")}, "project"))

		// Then: the move is refused, as a move of a row with rows below it
		// that the caller cannot read is.
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(results[0].GetErrorCode()).To(Equal(server.CodeAccessDenied))
	})
})
