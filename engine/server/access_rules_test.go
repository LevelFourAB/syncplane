package server_test

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// The org schema's Label has a read rule only, so it is server-owned, and
// Entry has a read and a create rule, so it is append-only. Both are read
// and created with permissions the org members hold at the root.
var _ = Describe("Access rules", func() {
	var (
		f       *accessFixture
		policy  *recordingPolicy
		clients int
	)

	// push sends one mutation as user from a fresh client, so that no two
	// calls share a mutation id.
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
	putAt := func(user, entity, key string, value proto.Message, version uint64) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation {
			return withExpectedVersion(putMutation(c, 1, entity, key, value), version)
		})
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
	denied := func(r *syncplanev1.MutationResult) string {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied), r.GetErrorMessage())
		return r.GetErrorMessage()
	}
	get := func(dst proto.Message, key string) bool {
		GinkgoHelper()
		var found bool
		Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(dst, key)
			return err
		})).To(Succeed())
		return found
	}
	labelName := func(key string) string {
		GinkgoHelper()
		var l accessv1.Label
		Expect(get(&l, key)).To(BeTrue())
		return l.GetName()
	}
	entryBody := func(key string) (string, bool) {
		GinkgoHelper()
		var e accessv1.Entry
		found := get(&e, key)
		return e.GetBody(), found
	}
	member := func(id, projectID, user, role string) *accessv1.ProjectMember {
		return &accessv1.ProjectMember{Id: id, Project: project(projectID), UserId: user, Role: role}
	}
	// withTaskRules registers the access test file again with the task
	// entity's rules replaced by rules.
	withTaskRules := func(rules ...*syncplanev1.AccessRule) {
		GinkgoHelper()
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
			task := messageNamed(file, "Task")
			eo := proto.GetExtension(task.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			eo.Access = &syncplanev1.AccessOptions{Allow: rules}
		}), server.AllowAccessChange())
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		policy = &recordingPolicy{}
		f = newAccessFixture(server.WithEntities(&accessv1.Entry{}), server.WithPermissions(policy),
			// A mutator's rows are covered by the verdict on its invocation, so
			// its body writes rows no rule lets the caller write.
			server.WithMutator("renameLabel", &accessv1.Label{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Patch(args, []string{"name"}, nil)
				},
				server.MutatorAccess{Permission: "view_labels"}),
		)
		f.seedOrg()
		f.seed(&accessv1.Entry{Id: "e1", Body: "seeded"})
	})

	Describe("an operation no rule admits", func() {
		It("refuses every client write of a server-owned entity", func() {
			Expect(denied(put("admin", "label", "l2", &accessv1.Label{Id: "l2"}))).
				To(ContainSubstring(`entity "label" has no access rule that admits create or update`))
			Expect(denied(putAt("admin", "label", "l2", &accessv1.Label{Id: "l2"}, 0))).
				To(ContainSubstring(`admits create, `))
			Expect(denied(patch("admin", "label", "l1", &accessv1.Label{Name: "x"}, "name"))).
				To(ContainSubstring(`admits update, `))
			Expect(denied(del("admin", "label", "l1"))).
				To(ContainSubstring(`admits delete, `))

			Expect(get(&accessv1.Label{}, "l2")).To(BeFalse())
			Expect(labelName("l1")).To(Equal("urgent"))
		})

		It("refuses before the app's policy is consulted", func() {
			del("admin", "label", "l1")

			Expect(policy.writes).To(BeEmpty())
		})

		It("rolls back the whole batch", func() {
			denied(push("admin", func(c string) *syncplanev1.Mutation {
				return batchMutation(c, 1,
					crudPut("note", "n2", &accessv1.Note{Id: "n2", Body: "lands only with the batch"}),
					crudDelete("label", "l1"),
				)
			}))

			Expect(get(&accessv1.Note{}, "n2")).To(BeFalse())
			Expect(labelName("l1")).To(Equal("urgent"))
		})

		It("holds the in-process API to the same rules", func() {
			ctx := server.WithPrincipal(f.ctx, server.Principal{Subject: "admin"})
			for _, err := range []error{
				f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error { return tx.Delete(&accessv1.Label{}, "l1") }),
				f.srv.Apply(ctx, ws, func(tx *server.ApplyTx) error { return tx.Delete(&accessv1.Label{}, "l1") }),
			} {
				rej, ok := server.AsRejection(err)
				Expect(ok).To(BeTrue(), "got %v", err)
				Expect(rej.Code).To(Equal(server.CodeAccessDenied))
			}
			Expect(labelName("l1")).To(Equal("urgent"))
		})

		It("admits a trusted write", func() {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&accessv1.Label{Id: "l1", Name: "renamed"})
			})).To(Succeed())

			Expect(labelName("l1")).To(Equal("renamed"))
		})

		It("admits a mutator body, whose rows the invocation covers", func() {
			applied(push("member", func(c string) *syncplanev1.Mutation {
				return customMutation(c, 1, "renameLabel", &accessv1.Label{Id: "l1", Name: "renamed"})
			}))

			Expect(labelName("l1")).To(Equal("renamed"))
		})
	})

	Describe("an append-only entity", func() {
		It("creates a row through a guarded create", func() {
			applied(putAt("member", "entry", "e2", &accessv1.Entry{Id: "e2", Body: "posted"}, 0))

			body, found := entryBody("e2")
			Expect(found).To(BeTrue())
			Expect(body).To(Equal("posted"))
		})

		// An unguarded PUT is a create or an update depending on the stored
		// row, and only the write transaction can tell which.
		It("creates a row through an unguarded PUT of a new key", func() {
			applied(put("member", "entry", "e2", &accessv1.Entry{Id: "e2", Body: "posted"}))

			_, found := entryBody("e2")
			Expect(found).To(BeTrue())
		})

		It("refuses an unguarded PUT over a row that exists", func() {
			denied(put("member", "entry", "e1", &accessv1.Entry{Id: "e1", Body: "edited"}))

			body, _ := entryBody("e1")
			Expect(body).To(Equal("seeded"))
		})

		It("refuses a guarded create of a key that exists, without a conflict that would tell", func() {
			denied(putAt("member", "entry", "e1", &accessv1.Entry{Id: "e1", Body: "edited"}, 0))

			body, _ := entryBody("e1")
			Expect(body).To(Equal("seeded"))
		})

		It("refuses a PUT that provably updates, a PATCH and a DELETE", func() {
			denied(putAt("member", "entry", "e1", &accessv1.Entry{Id: "e1", Body: "edited"}, 1))
			denied(patch("member", "entry", "e1", &accessv1.Entry{Body: "edited"}, "body"))
			denied(del("member", "entry", "e1"))

			body, found := entryBody("e1")
			Expect(found).To(BeTrue())
			Expect(body).To(Equal("seeded"))
		})

		It("refuses a create where the caller does not hold the rule's permission", func() {
			// guest reads tasks in p1 only, and an entry sits at the root.
			denied(putAt("guest", "entry", "e2", &accessv1.Entry{Id: "e2", Body: "posted"}, 0))

			_, found := entryBody("e2")
			Expect(found).To(BeFalse())
		})
	})

	Describe("several rules for one operation", func() {
		It("lets a caller read by any of the read rules", func() {
			f.seed(member("pm-editor", "p2", "editor", "project_editor"))
			Expect(f.pull("editor")).NotTo(ContainElement("task/t2"))

			// project_editor grants edit_tasks and not view_tasks.
			withTaskRules(permRule("view_tasks", "read"), permRule("edit_tasks", "read"), permRule("edit_tasks", "write"))

			Expect(f.pull("editor")).To(ContainElement("task/t2"))
			Expect(f.pull("editor")).NotTo(ContainElement("task/t1"))
			Expect(f.pull("guest")).To(ContainElement("task/t1"))
			Expect(f.pull("guest")).NotTo(ContainElement("task/t2"))
		})

		It("admits each write operation by the rules that name it", func() {
			withTaskRules(permRule("view_tasks", "read", "create"), permRule("edit_tasks", "update"),
				permRule("edit_projects", "delete"))

			// guest reads p1, writer writes in p2, owner owns p1.
			applied(put("guest", "task", "t9", &accessv1.Task{Id: "t9", Project: project("p1")}))
			denied(patch("guest", "task", "t1", &accessv1.Task{Title: "x"}, "title"))
			applied(patch("writer", "task", "t2", &accessv1.Task{Title: "x"}, "title"))
			denied(del("writer", "task", "t2"))
			applied(del("owner", "task", "t1"))
		})

		It("needs one rule to admit both the stored row and the moved one", func() {
			// split holds view_members in p1 and edit_tasks in p2, and neither
			// permission in the other project.
			f.seed(member("pm-split-1", "p1", "split", "project_reader"), member("pm-split-2", "p2", "split", "project_editor"))
			withTaskRules(permRule("view_tasks", "read"), permRule("view_members", "create", "update"),
				permRule("edit_tasks", "update"))

			applied(patch("split", "task", "t1", &accessv1.Task{Title: "in one"}, "title"))
			applied(patch("split", "task", "t2", &accessv1.Task{Title: "in two"}, "title"))
			denied(patch("split", "task", "t1", &accessv1.Task{Project: project("p2")}, "project"))

			// admin holds both at the root.
			applied(patch("admin", "task", "t1", &accessv1.Task{Project: project("p2")}, "project"))
		})

		It("lets every admitted caller through an admitted rule, and no one else", func() {
			withTaskRules(admittedRule("read"), permRule("view_tasks", "read"), permRule("edit_tasks", "write"))

			Expect(f.pull("guest")).To(ContainElements("task/t1", "task/t2", "task/t3"))
			_, err := f.pullErr("stranger")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})

		It("resyncs the rows a grant shows through a second read rule", func() {
			// Seeded before the schema is registered again: a compiled message
			// does not match the types the registration brings.
			f.seed(member("pm-editor", "p1", "editor", "project_editor"))
			withTaskRules(permRule("view_tasks", "read"), permRule("edit_tasks", "read"), permRule("edit_tasks", "write"))
			r := newAccessReplica(f, "editor")
			r.sync()
			Expect(r.view()).To(ContainElement("task/t1"))
			Expect(r.view()).NotTo(ContainElement("task/t2"))

			applied(put("admin", "project_member", "pm-editor-2", member("pm-editor-2", "p2", "editor", "project_editor")))
			r.sync()

			Expect(r.view()).To(ContainElement("task/t2"))
			Expect(r.view()).To(Equal(f.pull("editor")))
		})
	})
})

// A push authorizes its writes before it takes the workspace write lock, and
// registration does not wait for that lock, so the write path checks the
// rules of an entity registered in between again.
var _ = Describe("Access rules for an entity registered while a push waits", func() {
	var (
		f     *accessFixture
		store *registerOnWrite
	)

	BeforeEach(func() {
		store = &registerOnWrite{Adapter: newTestStore()}
		f = newAccessFixtureOn(store)
		f.seedOrg()
		store.register = func() {
			defer GinkgoRecover()
			Expect(f.srv.RegisterEntity(&accessv1.Entry{})).To(Succeed())
		}
		store.armed.Store(true)
	})

	It("refuses an operation no rule of the entity admits", func() {
		result := f.push("member", "c1", patchMutation("c1", 1, "entry", "e1", &accessv1.Entry{Body: "x"}, "body"))[0]

		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(result.GetErrorCode()).To(Equal(server.CodeAccessDenied))
		Expect(result.GetErrorMessage()).To(ContainSubstring(`entity "entry" has no access rule that admits update`))
	})

	It("applies a write a rule of the entity admits", func() {
		result := f.push("member", "c1", withExpectedVersion(
			putMutation("c1", 1, "entry", "e1", &accessv1.Entry{Id: "e1", Body: "posted"}), 0))[0]

		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), result.GetErrorMessage())
	})
})
