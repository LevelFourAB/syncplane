package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// A mutator body is server code, so its own reads see every row. Its reads
// through AsCaller read as the invoking caller's client would.
var _ = Describe("Reads in a mutator body in a schema that uses the access model", func() {
	var (
		f    *accessFixture
		body func(tx *server.EntityTx) error
	)

	BeforeEach(func() {
		body = nil
		// Given staff s1 in p1, which guest reads without its salary, and s2
		// in p2, which guest does not read
		f = newAccessFixture(server.WithEntities(&accessv1.Staff{}),
			server.WithMutator("peek", nil, func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
				return body(tx)
			}, server.MutatorAccess{Admitted: true}))
		f.seedOrg()
		f.seed(
			&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000, PersonId: "guest"},
			&accessv1.Staff{Id: "s2", Project: project("p2"), Name: "bob", Salary: 6000, PersonId: "writer"},
		)
	})

	invokeAs := func(user string) error {
		return f.srv.Invoke(server.WithPrincipal(f.ctx, server.Principal{Subject: user}), ws, "peek", nil)
	}

	// keysOf collects the ids a read passes to its callback.
	keysOf := func(keys *[]string) func(msg proto.Message) error {
		return func(msg proto.Message) error {
			*keys = append(*keys, msg.(*accessv1.Staff).GetId())
			return nil
		}
	}

	It("shows the body's own reads every row with every field", func() {
		var s1, s2 accessv1.Staff
		var found2 bool
		var keys []string
		body = func(tx *server.EntityTx) error {
			if _, err := tx.Get(&s1, "s1"); err != nil {
				return err
			}
			var err error
			if found2, err = tx.Get(&s2, "s2"); err != nil {
				return err
			}
			return tx.Query(&accessv1.Staff{}, query.Spec{Where: query.Gt("salary", int64(0))}, keysOf(&keys))
		}

		// When guest invokes the mutator
		Expect(invokeAs("guest")).To(Succeed())

		// Then the body reads both rows, the salary of s1 and a query by salary
		Expect(found2).To(BeTrue())
		Expect(s1.GetSalary()).To(Equal(int64(5000)))
		Expect(keys).To(ConsistOf("s1", "s2"))
	})

	It("reads through AsCaller only the rows and fields the invoking caller reads", func() {
		var s1, s2 accessv1.Staff
		var found1, found2, metaFound2 bool
		var scanned, paged []string
		body = func(tx *server.EntityTx) error {
			view := tx.AsCaller()
			var err error
			if found1, err = view.Get(&s1, "s1"); err != nil {
				return err
			}
			if found2, err = view.Get(&s2, "s2"); err != nil {
				return err
			}
			if metaFound2, _, err = view.GetMeta(&accessv1.Staff{}, "s2"); err != nil {
				return err
			}
			if err := view.Scan(&accessv1.Staff{}, keysOf(&scanned)); err != nil {
				return err
			}
			_, _, err = view.ScanPage(&accessv1.Staff{}, "", 10, keysOf(&paged))
			return err
		}

		// When guest invokes the mutator
		Expect(invokeAs("guest")).To(Succeed())

		// Then s2 reads as absent, and s1 reads without its salary
		Expect(found1).To(BeTrue())
		Expect(s1.GetName()).To(Equal("ann"))
		Expect(s1.GetSalary()).To(BeZero(), "the body read through AsCaller a salary guest does not read")
		Expect(found2).To(BeFalse(), "the body read through AsCaller a row guest does not read")
		Expect(metaFound2).To(BeFalse())
		Expect(scanned).To(ConsistOf("s1"))
		Expect(paged).To(ConsistOf("s1"))
	})

	It("refuses through AsCaller a query that names a field the invoking caller does not read on every row", func() {
		var keys []string
		body = func(tx *server.EntityTx) error {
			return tx.AsCaller().Query(&accessv1.Staff{}, query.Spec{
				OrderBy: []*syncplanev1.OrderKey{query.Desc("salary")},
			}, keysOf(&keys))
		}

		// When guest invokes the mutator
		err := invokeAs("guest")

		// Then the query is refused, and nothing is answered in salary order
		Expect(err).To(HaveOccurred(), "guest saw %v in salary order", keys)
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
	})

	It("reads in a Server.Mutate for a caller only the rows and fields the caller reads", func() {
		var s1 accessv1.Staff
		var found2 bool
		var scanned []string
		read := func(tx *server.EntityTx) error {
			if _, err := tx.Get(&s1, "s1"); err != nil {
				return err
			}
			var err error
			if found2, err = tx.Get(&accessv1.Staff{}, "s2"); err != nil {
				return err
			}
			scanned = nil
			return tx.Scan(&accessv1.Staff{}, keysOf(&scanned))
		}

		// When guest reads in a Mutate
		guest := server.WithPrincipal(f.ctx, server.Principal{Subject: "guest"})
		Expect(f.srv.Mutate(guest, ws, read)).To(Succeed())

		// Then s2 reads as absent, and s1 reads without its salary
		Expect(found2).To(BeFalse(), "a Mutate for guest read a row guest does not read")
		Expect(s1.GetSalary()).To(BeZero(), "a Mutate for guest read a salary guest does not read")
		Expect(scanned).To(ConsistOf("s1"))

		// And a query by salary is refused
		var keys []string
		err := f.srv.Mutate(guest, ws, func(tx *server.EntityTx) error {
			return tx.Query(&accessv1.Staff{}, query.Spec{Where: query.Gt("salary", int64(0))}, keysOf(&keys))
		})
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied), "guest saw %v", keys)

		// And a trusted Mutate reads every row with every field
		s1.Reset()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, read)).To(Succeed())
		Expect(found2).To(BeTrue())
		Expect(s1.GetSalary()).To(Equal(int64(5000)))
		Expect(scanned).To(ConsistOf("s1", "s2"))
	})

	It("reads through AsCaller a row the body wrote earlier in the invocation", func() {
		var found bool
		body = func(tx *server.EntityTx) error {
			view := tx.AsCaller()
			// The first read binds what guest reads before the write.
			if _, err := view.Get(&accessv1.Staff{}, "s1"); err != nil {
				return err
			}
			if err := tx.Put(&accessv1.ProjectMember{Id: "pm-guest-p2", Project: project("p2"), UserId: "guest", Role: "project_reader"}); err != nil {
				return err
			}
			var err error
			found, err = view.Get(&accessv1.Staff{}, "s2")
			return err
		}

		// When guest invokes a mutator that grants guest p2, then reads s2
		Expect(invokeAs("guest")).To(Succeed())

		// Then the read sees the grant the body wrote
		Expect(found).To(BeTrue())
	})
})
