package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A write to a row the caller cannot read must not tell the caller that the
// row exists. The fixture is seedOrg: t1 is in p1, which writer and guest
// cannot write; task t-none does not exist.
var _ = Describe("Access model writes to a row that does not exist", func() {
	var (
		f       *accessFixture
		opts    []server.Option
		clients int
	)

	BeforeEach(func() {
		clients = 0
		opts = nil
	})

	JustBeforeEach(func() {
		f = newAccessFixture(opts...)
		f.seedOrg()
	})

	one := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := "c-missing-" + string(rune('a'+clients))
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	del := func(user, key string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return one(user, func(c string) *syncplanev1.Mutation { return deleteMutation(c, 1, "task", key) })
	}
	patch := func(user, key string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return one(user, func(c string) *syncplanev1.Mutation {
			return patchMutation(c, 1, "task", key, &accessv1.Task{Id: key, Title: "x"}, "title")
		})
	}
	verdict := func(r *syncplanev1.MutationResult) string {
		return r.GetStatus().String() + " " + r.GetErrorCode()
	}

	It("answers a delete of a missing row exactly as a delete of a hidden row", func() {
		// When: writer deletes t1, which it cannot read, and t-none, which
		// does not exist.
		hidden := del("writer", "t1")
		missing := del("writer", "t-none")

		// Then: the two answers are the same.
		Expect(verdict(missing)).To(Equal(verdict(hidden)))
	})

	It("answers a patch of a missing row exactly as a patch of a hidden row", func() {
		// When: writer patches t1, which it cannot read, and t-none, which
		// does not exist.
		hidden := patch("writer", "t1")
		missing := patch("writer", "t-none")

		// Then: the two answers are the same.
		Expect(verdict(missing)).To(Equal(verdict(hidden)))
	})

	It("does not let a caller that may delete no task write a tombstone", func() {
		// When: guest, who reads p1 and writes nothing, deletes a task key that
		// does not exist.
		r := del("guest", "t-none")

		// Then: it is refused.
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED), verdict(r))
	})

	// The application's ReadFilter narrows the model, and a write to a row it
	// hides is also a write to a row the caller may not change.
	Context("when the application's ReadFilter hides a row the model also refuses", func() {
		// Given: the ReadFilter hides t1, which writer and guest may not
		// change in the model either.
		BeforeEach(func() {
			opts = []server.Option{server.WithPermissions(hideTask{key: "t1"})}
		})

		It("answers a patch of a missing row exactly as a patch of the hidden row", func() {
			Expect(verdict(patch("writer", "t-none"))).To(Equal(verdict(patch("writer", "t1"))))
		})

		It("answers a delete of a missing row exactly as a delete of the hidden row", func() {
			Expect(verdict(del("guest", "t-none"))).To(Equal(verdict(del("guest", "t1"))))
		})
	})

	Context("when the application's ReadFilter hides a row the model lets the caller change", func() {
		// Given: the ReadFilter hides t2, which writer may change in the model.
		BeforeEach(func() {
			opts = []server.Option{server.WithPermissions(hideTask{key: "t2"})}
		})

		It("answers a patch of a missing row exactly as a patch of the hidden row", func() {
			Expect(verdict(patch("writer", "t-none"))).To(Equal(verdict(patch("writer", "t2"))))
		})

		It("answers a delete of a missing row exactly as a delete of the hidden row", func() {
			Expect(verdict(del("writer", "t-none"))).To(Equal(verdict(del("writer", "t2"))))
		})
	})
})
