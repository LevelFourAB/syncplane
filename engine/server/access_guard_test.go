package server_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// The fixture is "expense" in proto/private/syncplane/testdata/access/v1, below
// a project. Its guards keep `total` from every client, `status` for callers
// with edit_projects, `notes` for the reporter, and `estimate` changeable only
// while the expense is open; `title` follows the entity rules alone.
//
// In p1, padmin holds edit_tasks without edit_projects, and owner holds both.

var _ = Describe("Access model registration of field guards", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	guardsOf := func(file *descriptorpb.FileDescriptorProto) *syncplanev1.AccessOptions {
		GinkgoHelper()
		m := messageNamed(file, "Expense")
		return proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	}
	readRule := func() *syncplanev1.AccessRule {
		return &syncplanev1.AccessRule{Read: true, Admitted: &syncplanev1.AdmittedCaller{}}
	}
	guard := func(fields []string, rules ...*syncplanev1.AccessRule) *syncplanev1.FieldGuard {
		return &syncplanev1.FieldGuard{Fields: fields, Allow: append([]*syncplanev1.AccessRule{readRule()}, rules...)}
	}

	It("registers the descriptor set", func() {
		_, err := srv.RegisterDescriptorSet(accessFileSet(nil))
		Expect(err).NotTo(HaveOccurred())
	})

	DescribeTable("refuses a guard that cannot take effect as written",
		func(change func(a *syncplanev1.AccessOptions), want string) {
			_, err := srv.RegisterDescriptorSet(accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
				change(guardsOf(file))
			}))
			Expect(err).To(MatchError(ContainSubstring(want)))
		},
		Entry("a guard without fields", func(a *syncplanev1.AccessOptions) {
			a.Guard = append(a.Guard, guard(nil))
		}, `access guard[4] names no field`),
		Entry("a key field", func(a *syncplanev1.AccessOptions) {
			a.Guard = append(a.Guard, guard([]string{"id"}))
		}, `access guard[4] fields: "id" is a key field`),
		Entry("an access parent", func(a *syncplanev1.AccessOptions) {
			a.Guard = append(a.Guard, guard([]string{"project"}))
		}, `access guard[4] names "project", which is an access parent`),
		Entry("a creator field", func(a *syncplanev1.AccessOptions) {
			a.Guard = append(a.Guard, guard([]string{"reporter_id"}))
		}, `access guard[4] names "reporter_id", which is a creator field`),
		Entry("a field two guards name", func(a *syncplanev1.AccessOptions) {
			a.Guard = append(a.Guard, guard([]string{"title", "total"}))
		}, `access guard[4] and access guard[0] both name "total"`),
		Entry("a rule that names delete", func(a *syncplanev1.AccessOptions) {
			a.Guard[0].Allow = append(a.Guard[0].Allow, &syncplanev1.AccessRule{Delete: true, Requires: []string{"edit_tasks"}})
		}, `access guard[0] allow[1] names delete`),
		Entry("a rule with a fields list", func(a *syncplanev1.AccessOptions) {
			a.Guard[0].Allow = append(a.Guard[0].Allow,
				&syncplanev1.AccessRule{Update: true, Requires: []string{"edit_tasks"}, Fields: []string{"total"}})
		}, `access guard[0] allow[1] names fields`),
		Entry("the same rule twice", func(a *syncplanev1.AccessOptions) {
			a.Guard[0].Allow = append(a.Guard[0].Allow,
				&syncplanev1.AccessRule{Update: true, Requires: []string{"edit_tasks"}},
				&syncplanev1.AccessRule{Update: true, Requires: []string{"edit_tasks"}})
		}, `access guard[0] allow[1] and allow[2] are the same rule`),
		Entry("a caller_is check on a guarded field", func(a *syncplanev1.AccessOptions) {
			a.Guard[0].Allow = append(a.Guard[0].Allow,
				&syncplanev1.AccessRule{Update: true, Requires: []string{"edit_tasks"}, CallerIs: "status"})
		}, `access guard[0] allow[1] reads "status", which a guard restricts`),
		Entry("a when check on a guarded field", func(a *syncplanev1.AccessOptions) {
			a.Guard[1].Allow = append(a.Guard[1].Allow, &syncplanev1.AccessRule{
				Update: true, Requires: []string{"edit_tasks"},
				When: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
					Path: []string{"total"}, Op: syncplanev1.CmpOp_CMP_OP_EQ,
					Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: 0}},
				}}},
			})
		}, `access guard[1] allow[2] reads "total", which a guard restricts`),
		Entry("a permission no role grants", func(a *syncplanev1.AccessOptions) {
			a.Guard[0].Allow = append(a.Guard[0].Allow, &syncplanev1.AccessRule{Update: true, Requires: []string{"set_totals"}})
		}, `permission "set_totals" of entity "expense" is granted by no role`),
		Entry("a guard with fields this server does not know", func(a *syncplanev1.AccessOptions) {
			a.Guard[0].ProtoReflect().SetUnknown(unknownField())
		}, `access guard[0] carries option fields this server does not know`),
	)

	It("refuses a change of a guard unless the registration allows it", func() {
		_, err := srv.RegisterDescriptorSet(accessFileSet(nil))
		Expect(err).NotTo(HaveOccurred())
		change := func(file *descriptorpb.FileDescriptorProto) {
			a := guardsOf(file)
			a.Guard[0].Allow = append(a.Guard[0].Allow, &syncplanev1.AccessRule{Update: true, Requires: []string{"edit_projects"}})
		}

		_, err = srv.RegisterDescriptorSet(accessFileSet(change))
		Expect(err).To(MatchError(ContainSubstring("AllowAccessChange")))
		_, err = srv.RegisterDescriptorSet(accessFileSet(change), server.AllowAccessChange())
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Access model field guards", func() {
	var (
		f       *accessFixture
		clients int
	)

	BeforeEach(func() {
		clients = 0
		f = newAccessFixture(server.WithEntities(&accessv1.Expense{}))
		f.seedOrg()
		f.seed(
			&accessv1.Expense{Id: "e1", Project: project("p1"), Title: "lunch", Status: "open", Total: 100,
				ReporterId: "padmin", Notes: "receipt", Estimate: 3},
			&accessv1.Expense{Id: "e2", Project: project("p1"), Title: "taxi", Status: "open", Total: 40,
				ReporterId: "padmin", Estimate: 5, Closed: true},
		)
	})

	push := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(user string, value *accessv1.Expense) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return putMutation(c, 1, "expense", value.GetId(), value) })
	}
	patch := func(user, key string, value *accessv1.Expense, paths ...string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return patchMutation(c, 1, "expense", key, value, paths...) })
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	denied := func(r *syncplanev1.MutationResult) string {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied), r.GetErrorMessage())
		return r.GetErrorMessage()
	}
	expense := func(key string) *accessv1.Expense {
		GinkgoHelper()
		var (
			out   accessv1.Expense
			found bool
		)
		Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&out, key)
			return err
		})).To(Succeed())
		Expect(found).To(BeTrue(), "expense %q", key)
		return &out
	}

	Describe("a PATCH", func() {
		It("is refused before the row is read when the guard admits no update", func() {
			policy := &recordingPolicy{}
			f = newAccessFixture(server.WithEntities(&accessv1.Expense{}), server.WithPermissions(policy))
			f.seedOrg()

			Expect(denied(patch("padmin", "e1", &accessv1.Expense{Total: 1}, "total"))).
				To(ContainSubstring(`the guard of field "total" admits no update`))
			Expect(policy.writes).To(BeEmpty())
		})

		It("changes a guarded field only for a caller a guard rule admits", func() {
			Expect(denied(patch("padmin", "e1", &accessv1.Expense{Status: "approved"}, "status"))).
				To(ContainSubstring(`field "status" of row "e1" may not be changed by this caller`))
			Expect(expense("e1").GetStatus()).To(Equal("open"))

			applied(patch("owner", "e1", &accessv1.Expense{Status: "approved"}, "status"))
			Expect(expense("e1").GetStatus()).To(Equal("approved"))
		})

		It("refuses the whole mask when one path is guarded from the caller", func() {
			denied(patch("padmin", "e1", &accessv1.Expense{Title: "dinner", Status: "approved"}, "title", "status"))

			Expect(expense("e1").GetTitle()).To(Equal("lunch"))
		})

		It("checks a guard rule's caller_is on the row", func() {
			applied(patch("padmin", "e1", &accessv1.Expense{Notes: "two receipts"}, "notes"))
			denied(patch("owner", "e1", &accessv1.Expense{Notes: "not mine"}, "notes"))

			Expect(expense("e1").GetNotes()).To(Equal("two receipts"))
		})

		It("needs a guard rule's when check on the stored row and on the row as it will be stored", func() {
			applied(patch("padmin", "e1", &accessv1.Expense{Estimate: 4}, "estimate"))
			denied(patch("padmin", "e2", &accessv1.Expense{Estimate: 6}, "estimate"))
			denied(patch("padmin", "e1", &accessv1.Expense{Estimate: 9, Closed: true}, "estimate", "closed"))
			applied(patch("padmin", "e1", &accessv1.Expense{Closed: true}, "closed"))

			Expect(expense("e1").GetEstimate()).To(Equal(int32(4)))
			Expect(expense("e2").GetEstimate()).To(Equal(int32(5)))
		})

		It("leaves a field without a guard to the entity rules", func() {
			applied(patch("padmin", "e1", &accessv1.Expense{Title: "dinner"}, "title"))
			Expect(expense("e1").GetTitle()).To(Equal("dinner"))
		})
	})

	Describe("a PUT that updates a row", func() {
		It("keeps the stored value of each guarded field the caller may not write", func() {
			row := expense("e1")
			row.Title, row.Status, row.Total, row.Notes = "dinner", "approved", 1, "two receipts"
			applied(put("padmin", row))

			got := expense("e1")
			Expect(got.GetTitle()).To(Equal("dinner"))
			Expect(got.GetStatus()).To(Equal("open"))
			Expect(got.GetTotal()).To(Equal(int64(100)))
			Expect(got.GetNotes()).To(Equal("two receipts"))
		})

		It("lands each guarded field a guard rule admits for the caller", func() {
			row := expense("e1")
			row.Status, row.Notes = "approved", "not mine"
			applied(put("owner", row))

			got := expense("e1")
			Expect(got.GetStatus()).To(Equal("approved"))
			Expect(got.GetNotes()).To(Equal("receipt"))
		})
	})

	Describe("a PUT that creates a row", func() {
		It("stores the zero value in each guarded field the caller may not write", func() {
			applied(put("padmin", &accessv1.Expense{Id: "e3", Project: project("p1"), Title: "hotel",
				Status: "approved", Total: 900, Notes: "mine", Estimate: 2}))

			got := expense("e3")
			Expect(got.GetTitle()).To(Equal("hotel"))
			Expect(got.GetStatus()).To(BeEmpty())
			Expect(got.GetTotal()).To(BeZero())
			Expect(got.GetReporterId()).To(Equal("padmin"))
			Expect(got.GetNotes()).To(Equal("mine"), "the creator stamp makes padmin the reporter")
			Expect(got.GetEstimate()).To(Equal(int32(2)))
		})

		It("stores each guarded field a guard rule admits for the caller", func() {
			applied(put("owner", &accessv1.Expense{Id: "e3", Project: project("p1"), Title: "hotel", Status: "approved"}))

			Expect(expense("e3").GetStatus()).To(Equal("approved"))
		})
	})

	It("admits a trusted write to every guarded field", func() {
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Expense{Id: "e1", Total: 120}, []string{"total"}, nil)
		})).To(Succeed())

		Expect(expense("e1").GetTotal()).To(Equal(int64(120)))
	})

	Describe("the in-process API", func() {
		It("holds an Apply to the guards", func() {
			padmin := server.WithPrincipal(f.ctx, server.Principal{Subject: "padmin"})

			Expect(f.srv.Apply(padmin, ws, func(tx *server.ApplyTx) error {
				row := expense("e1")
				row.Title, row.Total = "dinner", 1
				return tx.Put(row)
			})).To(Succeed())
			Expect(expense("e1").GetTitle()).To(Equal("dinner"))
			Expect(expense("e1").GetTotal()).To(Equal(int64(100)))

			err := f.srv.Apply(padmin, ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&accessv1.Expense{Id: "e1", Status: "approved"}, []string{"status"}, nil)
			})
			r, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeAccessDenied))
		})
	})
})
