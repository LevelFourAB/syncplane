package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

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

// staffGuardsSet is the access test schema with the guards of Staff that
// change returns. It receives them in declaration order: salary, review,
// bonus, pin and bio.
func staffGuardsSet(change func(guards []*syncplanev1.FieldGuard) []*syncplanev1.FieldGuard) []byte {
	GinkgoHelper()
	return accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
		a := entityOptionsOf(file, "Staff").GetAccess()
		a.Guard = change(a.Guard)
	})
}

// withoutSalaryAndBio drops the guards of salary and bio, which callers with
// edit_projects read where the record sits, so that the guards left are the
// ones a spec is about.
func withoutSalaryAndBio(guards []*syncplanev1.FieldGuard) []*syncplanev1.FieldGuard {
	return []*syncplanev1.FieldGuard{guards[1], guards[2], guards[3]}
}

// A move shows the row to whoever reads where it goes. A field that no client
// reads, or that only the caller its row names reads, has the same readers
// wherever the row sits, so a move shows it to nobody new and does not need
// the mover to read it. The fixture is "staff": bonus and pin no client
// reads, and review only the caller that person_id names reads.
var _ = Describe("Access model moves of a row with fields a move shows to nobody new", func() {
	// schemaFixture runs the schema set with admin, which holds org_admin at
	// the root, p1 and p2, padmin, which writes staff in p1 without
	// edit_projects and owns p2, and rows. The server decodes rows with the
	// descriptors it registered, so every row goes in as a dynamic message of
	// that file.
	schemaFixture := func(set []byte, rows ...proto.Message) *accessFixture {
		GinkgoHelper()
		f := newSchemaFixtureOn(newTestStore(), server.WithEntities())
		_, err := f.srv.RegisterDescriptorSet(set)
		Expect(err).NotTo(HaveOccurred())
		meta, ok := f.srv.Entity("staff")
		Expect(ok).To(BeTrue())
		file := meta.Descriptor.ParentFile()
		rows = append([]proto.Message{
			&accessv1.OrgMember{Id: "om-admin", UserId: "admin", Role: "org_admin"},
			&accessv1.Project{Id: "p1", Name: "one"},
			&accessv1.Project{Id: "p2", Name: "two"},
			&accessv1.ProjectMember{Id: "pm-padmin", Project: project("p1"), UserId: "padmin", Role: "project_admin"},
			&accessv1.ProjectMember{Id: "pm-padmin-own2", Project: project("p2"), UserId: "padmin", Role: "project_owner"},
		}, rows...)
		for i, row := range rows {
			out := dynamicpb.NewMessage(file.Messages().ByName(row.ProtoReflect().Descriptor().Name()))
			Expect(proto.Unmarshal(mustMarshal(row), out)).To(Succeed())
			rows[i] = out
		}
		f.seed(rows...)
		return f
	}

	// staffFixture is schemaFixture with s1 in p1 about guest.
	staffFixture := func(set []byte) *accessFixture {
		GinkgoHelper()
		return schemaFixture(set,
			&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000, PersonId: "guest", Bonus: 7, Review: "good", Pin: "1234"})
	}

	moveS1 := func(f *accessFixture, user string) *syncplanev1.MutationResult {
		GinkgoHelper()
		rs := f.push(user, "c-"+user, patchMutation("c-"+user, 1, "staff", "s1",
			&accessv1.Staff{Id: "s1", Project: project("p2")}, "project"))
		Expect(rs).To(HaveLen(1))
		return rs[0]
	}

	It("lets admin, which reads salary and bio at the root, move a record whose bonus, pin and review it does not read", func() {
		// Given the fixture schema
		f := staffFixture(accessFileSet(nil))

		// When admin moves s1 to p2
		r := moveS1(f, "admin")

		// Then the move applies
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), "%s %s", r.GetErrorCode(), r.GetErrorMessage())
	})

	It("lets padmin move a record whose review only the person it is about reads", func() {
		// Given the schema without the salary and bio guards
		f := staffFixture(staffGuardsSet(withoutSalaryAndBio))

		// When padmin moves s1 to p2
		r := moveS1(f, "padmin")

		// Then the move applies
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), "%s %s", r.GetErrorCode(), r.GetErrorMessage())
	})

	It("refuses padmin the move when callers with edit_projects also read review where the record sits", func() {
		// Given the schema without the salary and bio guards, where review is
		// also read by a caller with edit_projects, which padmin holds in p2
		// and not in p1
		f := staffFixture(staffGuardsSet(func(guards []*syncplanev1.FieldGuard) []*syncplanev1.FieldGuard {
			guards = withoutSalaryAndBio(guards)
			guards[0].Allow = append(guards[0].Allow, &syncplanev1.AccessRule{Read: true, Requires: []string{"edit_projects"}})
			return guards
		}))

		// When padmin moves s1 to p2
		r := moveS1(f, "padmin")

		// Then the move is refused, since it would show padmin the review
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied))
	})

	It("refuses padmin the move when review shows to every admitted caller on a record in p2", func() {
		// Given the schema without the salary and bio guards, where review is
		// read by every admitted caller when the record sits in p2
		f := staffFixture(staffGuardsSet(func(guards []*syncplanev1.FieldGuard) []*syncplanev1.FieldGuard {
			guards = withoutSalaryAndBio(guards)
			guards[0].Allow[0] = &syncplanev1.AccessRule{Read: true, Admitted: &syncplanev1.AdmittedCaller{},
				When: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path: []string{"project", "id"}, Op: syncplanev1.CmpOp_CMP_OP_EQ,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "p2"}},
				}}}}
			return guards
		}))

		// When padmin moves s1 to p2
		r := moveS1(f, "padmin")

		// Then the move is refused, since it would show everyone the review
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied))
	})

	Context("with staff records below tasks", func() {
		// staffBelowTaskSet is the access test schema with Staff moved below
		// Task, two hops below its project, with the guards that change
		// returns.
		staffBelowTaskSet := func(change func(guards []*syncplanev1.FieldGuard) []*syncplanev1.FieldGuard) []byte {
			GinkgoHelper()
			return accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				staff := messageNamed(file, "Staff")
				staff.Field = append(staff.Field, &descriptorpb.FieldDescriptorProto{
					Name: proto.String("task"), JsonName: proto.String("task"), Number: proto.Int32(10),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					TypeName: proto.String(".syncplane.testdata.access.v1.Task"),
				})
				a := entityOptionsOf(file, "Staff").GetAccess()
				a.Parents = []string{"task"}
				a.Guard = change(a.Guard)
			})
		}
		keep := func(guards []*syncplanev1.FieldGuard) []*syncplanev1.FieldGuard { return guards }

		// moveTask moves task to p2 as user, with s1 below t1 and nothing
		// below t-empty.
		moveTask := func(set []byte, user, task string) *syncplanev1.MutationResult {
			GinkgoHelper()
			f := schemaFixture(set,
				&accessv1.Task{Id: "t1", Project: project("p1")},
				&accessv1.Task{Id: "t-empty", Project: project("p1")},
				staffInTask(&accessv1.Staff{Id: "s1", Name: "ann", Salary: 5000, PersonId: "guest", Bonus: 7}, "t1"))
			rs := f.push(user, "c-"+user, patchMutation("c-"+user, 1, "task", task,
				&accessv1.Task{Id: task, Project: project("p2")}, "project"))
			Expect(rs).To(HaveLen(1))
			return rs[0]
		}

		It("lets admin move a task with a staff record below it, and one with nothing below it", func() {
			for _, task := range []string{"t1", "t-empty"} {
				r := moveTask(staffBelowTaskSet(keep), "admin", task)
				Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
					"%s: %s %s", task, r.GetErrorCode(), r.GetErrorMessage())
			}
		})

		It("lets padmin move a task when no field of the staff records below shows to other callers where they sit", func() {
			r := moveTask(staffBelowTaskSet(withoutSalaryAndBio), "padmin", "t1")
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), "%s %s", r.GetErrorCode(), r.GetErrorMessage())
		})

		It("refuses padmin the move of a task when callers with edit_projects read the salary of the staff records below", func() {
			r := moveTask(staffBelowTaskSet(keep), "padmin", "t1")
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied))
		})
	})
})

// staffInTask is s as a row of a schema whose Staff names its task in field
// 10, which the compiled message lacks: the field rides in its unknown fields.
func staffInTask(s *accessv1.Staff, taskID string) *accessv1.Staff {
	GinkgoHelper()
	b := protowire.AppendTag(nil, 10, protowire.BytesType)
	s.ProtoReflect().SetUnknown(protowire.AppendBytes(b, mustMarshal(&accessv1.Task{Id: taskID})))
	return s
}
