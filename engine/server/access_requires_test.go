package server_test

import (
	"context"
	"fmt"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// billingEntities registers the billing schema of the access test proto:
// accounts with members, root billing roles, and invoices and charges below
// the accounts, whose rules need two permissions together.
func billingEntities() server.Option {
	return server.WithEntities(
		&accessv1.Account{}, &accessv1.AccountMember{}, &accessv1.Biller{},
		&accessv1.Invoice{}, &accessv1.Charge{},
	)
}

func account(id string) *accessv1.Account { return &accessv1.Account{Id: id} }

func accountMember(id, accountID, user, role string) *accessv1.AccountMember {
	return &accessv1.AccountMember{Id: id, Account: account(accountID), UserId: user, Role: role}
}

func invoiceIn(id, accountID string) *accessv1.Invoice {
	return &accessv1.Invoice{Id: id, Account: account(accountID), Title: id}
}

func chargeOn(id, invoiceID string) *accessv1.Charge {
	return &accessv1.Charge{Id: id, Invoice: &accessv1.Invoice{Id: invoiceID}, Memo: id}
}

// seedBilling writes the rows every billing spec starts from:
//
//   - viewer views a1 and holds no billing role;
//   - reader views a1 and reads billing at the root;
//   - editor edits in a1 and reads billing at the root, but cannot use it;
//   - clerk edits in a1 and a2 and uses billing at the root;
//   - local edits in a1 and a2 and uses billing at a2 only;
//   - invoice i1 is in a1 and i2 in a2, and charge c1 is on i1 and c2 on i2.
func (f *accessFixture) seedBilling() {
	GinkgoHelper()
	f.seed(
		account("a1"), account("a2"),
		accountMember("am-viewer", "a1", "viewer", "account_viewer"),
		accountMember("am-reader", "a1", "reader", "account_viewer"),
		accountMember("am-editor", "a1", "editor", "account_editor"),
		accountMember("am-clerk-1", "a1", "clerk", "account_editor"),
		accountMember("am-clerk-2", "a2", "clerk", "account_editor"),
		accountMember("am-local-1", "a1", "local", "account_editor"),
		accountMember("am-local-2", "a2", "local", "account_editor"),
		accountMember("am-local-b", "a2", "local", "account_biller"),
		&accessv1.Biller{Id: "b-reader", UserId: "reader", Role: "billing_reader"},
		&accessv1.Biller{Id: "b-editor", UserId: "editor", Role: "billing_reader"},
		&accessv1.Biller{Id: "b-clerk", UserId: "clerk", Role: "billing_clerk"},
		invoiceIn("i1", "a1"), invoiceIn("i2", "a2"),
		chargeOn("c1", "i1"), chargeOn("c2", "i2"),
	)
}

var _ = Describe("Access model registration of rules that need several permissions", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(change func(*descriptorpb.FileDescriptorProto), opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(accessFileSet(change), opts...)
		return err
	}
	rulesOf := func(file *descriptorpb.FileDescriptorProto, message string) *syncplanev1.AccessOptions {
		GinkgoHelper()
		m := messageNamed(file, message)
		return proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	}
	rolesOf := func(file *descriptorpb.FileDescriptorProto, message string) []*syncplanev1.Role {
		GinkgoHelper()
		m := messageNamed(file, message)
		return proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetGrant().GetRoles()
	}
	warnings := func(change func(*descriptorpb.FileDescriptorProto)) []string {
		GinkgoHelper()
		logs := &recordingHandler{}
		srv = newBareServer(server.WithLogger(slog.New(logs)))
		Expect(register(change)).To(Succeed())
		return logs.attrs("access model warning", "finding")
	}

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), billingEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	DescribeTable("refuses a rule whose source cannot be checked",
		func(change func(*syncplanev1.AccessRule), want string) {
			Expect(register(func(file *descriptorpb.FileDescriptorProto) {
				change(rulesOf(file, "Invoice").Allow[0])
			})).To(MatchError(ContainSubstring(want)))
		},
		Entry("with both requires and admitted",
			func(r *syncplanev1.AccessRule) { r.Admitted = &syncplanev1.AdmittedCaller{} },
			`access allow[0] sets both requires and admitted; set exactly one`),
		Entry("with neither requires nor admitted",
			func(r *syncplanev1.AccessRule) { r.Requires = nil },
			`access allow[0] names no source; set requires or admitted`),
		Entry("that requires a permission twice",
			func(r *syncplanev1.AccessRule) {
				r.Requires = []string{"view_invoices", "read_billing", "view_invoices"}
			},
			`access allow[0] requires "view_invoices" twice`),
		Entry("that requires a permission that breaks the naming rule",
			func(r *syncplanev1.AccessRule) { r.Requires = []string{"view_invoices", "read-billing"} },
			`access allow[0] requires: permission name "read-billing"`),
		Entry("that requires a permission no role grants",
			func(r *syncplanev1.AccessRule) { r.Requires = []string{"view_invoices", "audit_billing"} },
			`permission "audit_billing" of entity "invoice" is granted by no role`),
	)

	It("refuses two rules that require the same permissions in another order as the same rule", func() {
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Invoice")
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Read: true, Requires: []string{"read_billing", "view_invoices"},
			})
		})).To(MatchError(ContainSubstring(`access allow[0] and allow[2] are the same rule`)))
	})

	It("warns about a rule whose permissions imply one another", func() {
		findings := warnings(func(file *descriptorpb.FileDescriptorProto) {
			roles := rolesOf(file, "AccountMember")
			// Every role that grants view_invoices then grants read_billing.
			roles[0].Grants = append(roles[0].Grants, "read_billing")
		})
		Expect(findings).To(ContainElement(`entity "invoice": access allow[0] needs "view_invoices" and "read_billing" together, ` +
			`but every role that grants "view_invoices" grants "read_billing" as well, ` +
			`so the rule admits the same callers as one that needs "view_invoices" alone`))
	})

	It("warns that a rule needing more permissions never decides beside one needing fewer", func() {
		findings := warnings(func(file *descriptorpb.FileDescriptorProto) {
			rules := rulesOf(file, "Invoice")
			rules.Allow = append(rules.Allow, &syncplanev1.AccessRule{
				Read: true, Requires: []string{"view_invoices"},
			})
		})
		Expect(findings).To(ContainElement(
			`entity "invoice": access allow[0] never decides read: allow[2] admits every caller and row it admits there`))
		Expect(findings).NotTo(ContainElement(ContainSubstring(`access allow[2] never decides`)))
	})

	It("changes the access spec with the permissions a rule requires, so the change needs the flag", func() {
		Expect(register(nil)).To(Succeed())
		change := func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Invoice").Allow[1].Requires = []string{"edit_invoices"}
		}
		Expect(register(change)).To(MatchError(ContainSubstring(`entity "invoice" changes its access model`)))
		Expect(register(change, server.AllowAccessChange())).To(Succeed())
	})
})

var _ = Describe("Rules that need several permissions", func() {
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
	patch := func(user, entity, key string, value proto.Message, paths ...string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return patchMutation(c, 1, entity, key, value, paths...) })
	}
	del := func(user, entity, key string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return deleteMutation(c, 1, entity, key) })
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	denied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied), r.GetErrorMessage())
	}
	converged := func(r *accessReplica) {
		GinkgoHelper()
		Expect(r.view()).To(Equal(f.pull(r.user)))
	}

	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), billingEntities())
		f.seedBilling()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	Describe("on a read rule", func() {
		It("reads a row only where the caller holds both permissions", func() {
			Expect(f.pull("viewer")).To(lackAll("invoice/i1", "invoice/i2", "charge/c1", "charge/c2"))
			Expect(f.pull("reader")).To(ContainElements("invoice/i1", "charge/c1"))
			Expect(f.pull("reader")).To(lackAll("invoice/i2", "charge/c2"))
			Expect(f.pull("clerk")).To(ContainElements("invoice/i1", "invoice/i2", "charge/c1", "charge/c2"))
		})

		It("needs both permissions on the row's own chain", func() {
			// local views a1 and a2, and reads billing at a2 alone.
			Expect(f.pull("local")).To(ContainElements("invoice/i2", "charge/c2"))
			Expect(f.pull("local")).To(lackAll("invoice/i1", "charge/c1"))
		})

		It("narrows a bootstrap and a query to the rows below both permissions", func() {
			Expect(f.bootstrapRows("reader")).To(ContainElements("invoice/i1", "charge/c1"))
			Expect(f.bootstrapRows("reader")).To(lackAll("invoice/i2", "charge/c2"))
			Expect(f.queryKeys("reader", "invoice")).To(Equal([]string{"i1"}))
			Expect(f.queryKeys("local", "charge")).To(Equal([]string{"c2"}))
			Expect(f.queryKeys("viewer", "charge")).To(BeEmpty())
		})

		It("reads every row for a caller who holds both permissions at the root", func() {
			f = newSchemaFixtureOn(newTestStore(), billingEntities(), server.WithGlobalRoles(
				func(_ context.Context, p server.Principal) ([]string, error) {
					if p.Subject == "staff" {
						return []string{"account_viewer", "billing_reader"}, nil
					}
					return nil, nil
				}))
			f.seedBilling()

			Expect(f.pull("staff")).To(ContainElements("invoice/i1", "invoice/i2", "charge/c1", "charge/c2"))
			Expect(f.queryKeys("staff", "charge")).To(Equal([]string{"c1", "c2"}))
		})
	})

	Describe("on a write rule", func() {
		It("lets a caller who holds both permissions write", func() {
			applied(patch("clerk", "invoice", "i1", &accessv1.Invoice{Title: "renamed"}, "title"))
			applied(put("clerk", "charge", "c3", chargeOn("c3", "i2")))
			applied(del("clerk", "charge", "c1"))
		})

		It("refuses a caller who holds one of the two", func() {
			// editor holds edit_invoices at a1, and read_billing but not
			// use_billing.
			denied(patch("editor", "invoice", "i1", &accessv1.Invoice{Title: "renamed"}, "title"))
			denied(put("editor", "charge", "c3", chargeOn("c3", "i1")))
			denied(del("editor", "charge", "c1"))
		})

		It("checks both permissions on the row's own chain", func() {
			applied(patch("local", "invoice", "i2", &accessv1.Invoice{Title: "renamed"}, "title"))
			denied(patch("local", "invoice", "i1", &accessv1.Invoice{Title: "renamed"}, "title"))
		})

		It("admits a move only where both permissions hold at both ends", func() {
			denied(patch("local", "invoice", "i2", &accessv1.Invoice{Account: account("a1")}, "account"))
			applied(patch("clerk", "invoice", "i1", &accessv1.Invoice{Account: account("a2")}, "account"))
		})
	})

	Describe("sync", func() {
		It("removes the rows when the second permission is revoked, and sends them back when it is granted again", func() {
			r := newAccessReplica(f, "reader")
			r.sync()
			Expect(r.view()).To(ContainElements("invoice/i1", "charge/c1"))

			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&accessv1.Biller{}, "b-reader")
			})).To(Succeed())
			r.sync()
			Expect(r.view()).To(lackAll("invoice/i1", "charge/c1"))
			converged(r)

			f.seed(&accessv1.Biller{Id: "b-reader", UserId: "reader", Role: "billing_reader"})
			r.sync()
			Expect(r.view()).To(ContainElements("invoice/i1", "charge/c1"))
			converged(r)
			Expect(r.resets).To(BeZero())
		})

		It("sends the rows below a row that moves to where the caller holds both permissions", func() {
			r := newAccessReplica(f, "local")
			r.sync()
			Expect(r.view()).To(lackAll("invoice/i1", "charge/c1"))

			// local holds view_invoices at a1 and at a2, so the move changes
			// only where read_billing holds.
			f.seed(invoiceIn("i1", "a2"))
			r.sync()
			Expect(r.view()).To(ContainElements("invoice/i1", "charge/c1"))
			converged(r)

			f.seed(invoiceIn("i1", "a1"))
			r.sync()
			Expect(r.view()).To(lackAll("invoice/i1", "charge/c1"))
			converged(r)
			Expect(r.resets).To(BeZero())
		})
	})
})
