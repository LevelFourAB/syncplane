package server_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A schema registered from a descriptor set has its own descriptors, so a
// compiled message with the same full name is a different Go type. The
// in-process API serves it all the same: a value is read through its wire
// bytes, and a read fills it through them.
var _ = Describe("A compiled message on a schema from a descriptor set", func() {
	var f *accessFixture

	BeforeEach(func() {
		f = newAccessFixture(server.WithEntities(&accessv1.Staff{}))
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(nil))
		Expect(err).NotTo(HaveOccurred())
		f.seedOrg()
	})

	trusted := func() context.Context { return server.Trusted(f.ctx) }
	caller := func(user string) context.Context {
		return server.WithPrincipal(f.ctx, server.Principal{Subject: user})
	}
	mutate := func(ctx context.Context, fn func(tx *server.EntityTx) error) error {
		GinkgoHelper()
		var err error
		Expect(func() { err = f.srv.Mutate(ctx, ws, fn) }).NotTo(Panic())
		return err
	}
	staff := func(ctx context.Context, key string) (*accessv1.Staff, bool) {
		GinkgoHelper()
		var (
			out   accessv1.Staff
			found bool
		)
		Expect(mutate(ctx, func(tx *server.EntityTx) error {
			var err error
			found, err = tx.Get(&out, key)
			return err
		})).To(Succeed())
		return &out, found
	}

	It("writes a Put", func() {
		Expect(mutate(trusted(), func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000})
		})).To(Succeed())

		got, found := staff(trusted(), "s1")
		Expect(found).To(BeTrue())
		Expect(got.GetName()).To(Equal("ann"))
		Expect(got.GetSalary()).To(BeEquivalentTo(5000))
	})

	It("writes a Patch", func() {
		f.seed(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann"})

		Expect(mutate(trusted(), func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Staff{Id: "s1", Name: "anna"}, []string{"name"}, nil)
		})).To(Succeed())

		got, _ := staff(trusted(), "s1")
		Expect(got.GetName()).To(Equal("anna"))
	})

	It("deletes a row", func() {
		f.seed(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann"})

		Expect(mutate(trusted(), func(tx *server.EntityTx) error {
			return tx.Delete(&accessv1.Staff{}, "s1")
		})).To(Succeed())

		_, found := staff(trusted(), "s1")
		Expect(found).To(BeFalse())
	})

	It("reads a row through ViewTx.Get", func() {
		f.seed(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann"})

		var got accessv1.Staff
		Expect(f.srv.View(trusted(), ws, func(tx *server.ViewTx) error {
			_, _, err := tx.Get(&got, "s1")
			return err
		})).To(Succeed())
		Expect(got.GetName()).To(Equal("ann"))
	})

	It("clears the fields a guard hides from the caller of a read", func() {
		f.seed(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000})

		got, found := staff(caller("guest"), "s1")
		Expect(found).To(BeTrue())
		Expect(got.GetName()).To(Equal("ann"))
		Expect(got.GetSalary()).To(BeZero())
	})

	It("derives the row key through EntityKey", func() {
		entity, key, err := f.srv.EntityKey(&accessv1.Staff{Id: "s1"})
		Expect(err).NotTo(HaveOccurred())
		Expect(entity).To(Equal("staff"))
		Expect(key).To(Equal("s1"))
	})

	It("refuses a value that does not encode, and writes nothing", func() {
		err := mutate(trusted(), func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "\xff"})
		})
		Expect(err).To(MatchError(ContainSubstring("staff")))

		_, found := staff(trusted(), "s1")
		Expect(found).To(BeFalse())
	})

	It("leaves the caller's message as it was", func() {
		msg := &accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann"}
		before := proto.Clone(msg)

		Expect(mutate(trusted(), func(tx *server.EntityTx) error { return tx.Put(msg) })).To(Succeed())
		Expect(proto.Equal(msg, before)).To(BeTrue())
	})
})
