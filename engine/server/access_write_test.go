package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Access model writes", func() {
	var (
		f       *accessFixture
		clients int
	)

	// write pushes one mutation as user from a fresh client and returns its
	// result.
	write := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(user, entity, key string, value proto.Message) *syncplanev1.MutationResult {
		GinkgoHelper()
		return write(user, func(c string) *syncplanev1.Mutation { return putMutation(c, 1, entity, key, value) })
	}
	patch := func(user, entity, key string, value proto.Message, paths ...string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return write(user, func(c string) *syncplanev1.Mutation { return patchMutation(c, 1, entity, key, value, paths...) })
	}
	del := func(user, entity, key string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return write(user, func(c string) *syncplanev1.Mutation { return deleteMutation(c, 1, entity, key) })
	}

	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	refused := func(r *syncplanev1.MutationResult, code string) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(code), r.GetErrorMessage())
	}
	denied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		refused(r, server.CodeAccessDenied)
	}

	task := func(id, projectID string) *accessv1.Task {
		t := &accessv1.Task{Id: id, Title: "t " + id}
		if projectID != "" {
			t.Project = project(projectID)
		}
		return t
	}
	member := func(id, projectID, user, role string) *accessv1.ProjectMember {
		m := &accessv1.ProjectMember{Id: id, UserId: user, Role: role}
		if projectID != "" {
			m.Project = project(projectID)
		}
		return m
	}

	Context("rows below an access parent", func() {
		BeforeEach(func() {
			f = newAccessFixture()
			f.seedOrg()
		})

		It("creates a row where the caller holds the write permission", func() {
			applied(put("writer", "task", "n2", task("n2", "p2")))
			applied(put("admin", "task", "n0", task("n0", "")))
		})

		It("refuses a create where the caller does not", func() {
			denied(put("writer", "task", "n1", task("n1", "p1")))
			denied(put("writer", "task", "n0", task("n0", "")))
			denied(put("guest", "task", "n1", task("n1", "p1")))
			denied(put("member", "task", "n1", task("n1", "p1")))
		})

		It("refuses a parent that is missing exactly as one that is hidden", func() {
			missing := put("writer", "task", "n9", task("n9", "p9"))
			hidden := put("writer", "task", "n1", task("n1", "p1"))
			denied(missing)
			denied(hidden)
			Expect(missing.GetErrorMessage()).To(Equal(
				`entity "task": row "n9" may not be changed by this caller`))
			Expect(hidden.GetErrorMessage()).To(Equal(
				`entity "task": row "n1" may not be changed by this caller`))
		})

		It("updates and deletes where the stored row sits", func() {
			applied(patch("writer", "task", "t2", &accessv1.Task{Id: "t2", Title: "edited"}, "title"))
			denied(patch("writer", "task", "t1", &accessv1.Task{Id: "t1", Title: "edited"}, "title"))
			denied(del("writer", "task", "t1"))
			applied(del("writer", "task", "t2"))
		})

		It("refuses before the version check, so the version stays hidden", func() {
			r := write("writer", func(c string) *syncplanev1.Mutation {
				return withExpectedVersion(patchMutation(c, 1, "task", "t1", &accessv1.Task{Id: "t1", Title: "x"}, "title"), 999)
			})
			denied(r)
			Expect(r.GetErrorMessage()).NotTo(ContainSubstring("version"))
		})

		It("checks both ends of a move", func() {
			// Into p1, which the writer cannot write.
			denied(patch("writer", "task", "t2", task("t2", "p1"), "project"))
			denied(put("writer", "task", "t2", task("t2", "p1")))
			// Out of p1, which the writer cannot write either.
			denied(patch("writer", "task", "t1", task("t1", "p2"), "project"))
			applied(patch("admin", "task", "t2", task("t2", "p1"), "project"))
		})

		It("logs at debug level why no rule admits a refused write", func() {
			logs := &recordingHandler{}
			f = newAccessFixture(server.WithLogger(slog.New(logs)))
			f.seedOrg()

			denied(patch("writer", "task", "t1", &accessv1.Task{Id: "t1", Title: "edited"}, "title"))
			refusal := refusalLog(logs)
			Expect(refusal).To(HaveKeyWithValue("entity", "task"))
			Expect(refusal).To(HaveKeyWithValue("key", "t1"))
			Expect(refusal).To(HaveKeyWithValue("op", "update"))
			Expect(refusal).To(HaveKeyWithValue("rules", ContainSubstring(
				"allow[1] write:requires=edit_tasks: edit_tasks is not held at the root, the row or a row above it")))
			Expect(refusal["rules"]).NotTo(ContainSubstring("allow[0]"), "a rule that does not name the operation")
		})

		It("logs a create that no rule admits", func() {
			logs := &recordingHandler{}
			f = newAccessFixture(server.WithLogger(slog.New(logs)))
			f.seedOrg()

			denied(put("guest", "task", "n1", task("n1", "p1")))
			refusal := refusalLog(logs)
			Expect(refusal).To(HaveKeyWithValue("op", "create"))
			Expect(refusal).To(HaveKeyWithValue("rules", ContainSubstring(
				"allow[1] write:requires=edit_tasks: edit_tasks is not held at the root, the row or a row above it")))
		})

		It("writes an entity outside the model for every admitted caller", func() {
			applied(put("guest", "note", "n2", &accessv1.Note{Id: "n2", Body: "hi"}))
		})

		It("holds in-process writes to the same rules", func() {
			writer := server.WithPrincipal(f.ctx, server.Principal{Subject: "writer"})
			err := f.srv.Mutate(writer, ws, func(tx *server.EntityTx) error {
				return tx.Put(task("n1", "p1"))
			})
			r, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeAccessDenied))

			Expect(f.srv.Apply(writer, ws, func(tx *server.ApplyTx) error {
				return tx.Put(task("n2", "p2"))
			})).To(Succeed())
			err = f.srv.Apply(writer, ws, func(tx *server.ApplyTx) error {
				return tx.Put(task("n3", "p1"))
			})
			r, ok = server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeAccessDenied))
		})
	})

	Context("grant rows", func() {
		BeforeEach(func() {
			f = newAccessFixture()
			f.seedOrg()
		})

		It("lets a caller grant roles it holds itself", func() {
			applied(put("padmin", "project_member", "pm-x", member("pm-x", "p1", "x", "project_reader")))
			applied(put("padmin", "project_member", "pm-y", member("pm-y", "p1", "y", "project_admin")))
		})

		It("refuses to grant a role stronger than the caller's", func() {
			denied(put("padmin", "project_member", "pm-x", member("pm-x", "p1", "x", "project_owner")))
			denied(patch("padmin", "project_member", "pm-guest", member("pm-guest", "p1", "guest", "project_owner"), "role"))
		})

		It("refuses to change or remove a grant stronger than the caller's", func() {
			denied(patch("padmin", "project_member", "pm-owner", member("pm-owner", "p1", "owner", "project_reader"), "role"))
			denied(patch("padmin", "project_member", "pm-owner", member("pm-owner", "p1", "padmin", "project_owner"), "user_id"))
			denied(del("padmin", "project_member", "pm-owner"))
			applied(del("owner", "project_member", "pm-padmin"))
		})

		It("refuses to move a grant to an anchor the caller does not manage", func() {
			denied(patch("padmin", "project_member", "pm-guest", member("pm-guest", "p2", "guest", "project_reader"), "project"))
			denied(put("padmin", "project_member", "pm-x", member("pm-x", "p2", "x", "project_reader")))
		})

		It("refuses a grant that could never grant, on every write path", func() {
			refused(put("admin", "project_member", "pm-x", member("pm-x", "p1", "x", "superuser")), server.CodeInvalidArgument)
			refused(put("admin", "project_member", "pm-x", member("pm-x", "p1", "", "project_reader")), server.CodeInvalidArgument)
			refused(put("admin", "project_member", "pm-x", member("pm-x", "", "x", "project_reader")), server.CodeInvalidArgument)

			err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(member("pm-x", "p1", "x", "superuser"))
			})
			r, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeInvalidArgument))
		})

		// The grant written first in the batch is what lets the task land.
		It("lets later writes of a batch see earlier ones", func() {
			r := write("admin", func(c string) *syncplanev1.Mutation {
				return batchMutation(c, 1,
					crudPut("project", "p3", &accessv1.Project{Id: "p3"}),
					crudPut("project_member", "pm-w3", member("pm-w3", "p3", "writer", "project_writer")))
			})
			applied(r)
			applied(put("writer", "task", "n3", task("n3", "p3")))
		})

		It("refuses a write in a batch after the caller revoked its own grant", func() {
			r := write("padmin", func(c string) *syncplanev1.Mutation {
				return batchMutation(c, 1,
					crudDelete("project_member", "pm-padmin"),
					crudPut("task", "n1", task("n1", "p1")))
			})
			denied(r)
			Expect(f.pull("padmin")).To(ContainElement("project_member/pm-padmin"), "the batch rolled back")
		})
	})

	Context("mutators", func() {
		var (
			canEdit bool
			seqs    [2]uint64
		)

		BeforeEach(func() {
			rename := func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
				id := args.(*accessv1.TaskArgs).GetTaskId()
				return tx.Patch(&accessv1.Task{Id: id, Title: "renamed"}, []string{"title"}, nil)
			}
			probe := func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
				var err error
				canEdit, err = tx.Can("edit_tasks", &accessv1.Task{}, args.(*accessv1.TaskArgs).GetTaskId())
				return err
			}
			retitle := func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
				id := args.(*accessv1.TaskArgs).GetTaskId()
				caller := tx.AsCaller()
				if err := caller.Patch(&accessv1.Task{Id: id, Title: "by caller"}, []string{"title"}, nil); err != nil {
					return err
				}
				seqs = [2]uint64{tx.Seq(), caller.Seq()}
				return nil
			}
			noop := func(context.Context, *server.EntityTx, proto.Message) error { return nil }
			f = newAccessFixture(
				server.WithMutator("rename_task", &accessv1.TaskArgs{}, rename,
					server.MutatorAccess{Permission: "edit_tasks", Entity: "task", KeyField: "task_id"}),
				server.WithMutator("retitle_as_caller", &accessv1.TaskArgs{}, retitle,
					server.MutatorAccess{Permission: "view_tasks", Entity: "task", KeyField: "task_id"}),
				server.WithMutator("probe", &accessv1.TaskArgs{}, probe,
					server.MutatorAccess{Permission: "view_tasks", Entity: "task", KeyField: "task_id"}),
				server.WithMutator("org_wide", nil, noop, server.MutatorAccess{Permission: "manage_members"}),
				server.WithMutator("unguarded", nil, noop),
			)
			f.seedOrg()
		})

		invoke := func(user, name string, args proto.Message) *syncplanev1.MutationResult {
			GinkgoHelper()
			return write(user, func(c string) *syncplanev1.Mutation {
				if args != nil {
					return customMutation(c, 1, name, args)
				}
				return &syncplanev1.Mutation{ClientId: c, MutationId: 1, Payload: &syncplanev1.Mutation_Custom{
					Custom: &syncplanev1.CustomMutation{Name: name},
				}}
			})
		}

		It("checks an invocation at the row its args name", func() {
			applied(invoke("writer", "rename_task", &accessv1.TaskArgs{TaskId: "t2"}))
			denied(invoke("writer", "rename_task", &accessv1.TaskArgs{TaskId: "t1"}))
			denied(invoke("writer", "rename_task", &accessv1.TaskArgs{TaskId: "missing"}))
		})

		It("checks an invocation without a row at the workspace root", func() {
			applied(invoke("admin", "org_wide", nil))
			denied(invoke("padmin", "org_wide", nil))
		})

		It("refuses a mutator without MutatorAccess to every caller but a trusted one", func() {
			denied(invoke("admin", "unguarded", nil))
			Expect(f.srv.Invoke(server.Trusted(f.ctx), ws, "unguarded", nil)).To(Succeed())
		})

		It("holds Invoke to the same check", func() {
			writer := server.WithPrincipal(f.ctx, server.Principal{Subject: "writer"})
			err := f.srv.Invoke(writer, ws, "rename_task", &accessv1.TaskArgs{TaskId: "t1"})
			r, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeAccessDenied))
			Expect(f.srv.Invoke(writer, ws, "rename_task", &accessv1.TaskArgs{TaskId: "t2"})).To(Succeed())
		})

		title := func(id string) string {
			GinkgoHelper()
			var t accessv1.Task
			Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
				_, _, err := tx.Get(&t, id)
				return err
			})).To(Succeed())
			return t.GetTitle()
		}

		It("checks a body's write through AsCaller as the caller's own write", func() {
			denied(invoke("guest", "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"}))
			Expect(title("t1")).To(Equal("in one"))
			applied(invoke("padmin", "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"}))
			Expect(title("t1")).To(Equal("by caller"))
		})

		It("checks a write through AsCaller under Invoke too, and not under a Trusted Invoke", func() {
			guest := server.WithPrincipal(f.ctx, server.Principal{Subject: "guest"})
			err := f.srv.Invoke(guest, ws, "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"})
			r, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeAccessDenied))
			_, err = f.srv.InvokeIdempotent(guest, ws, "k1", "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"})
			r, ok = server.AsRejection(err)
			Expect(ok).To(BeTrue(), "%v", err)
			Expect(r.Code).To(Equal(server.CodeAccessDenied))
			Expect(title("t1")).To(Equal("in one"))

			Expect(f.srv.Invoke(server.Trusted(f.ctx), ws, "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"})).
				To(Succeed())
			Expect(title("t1")).To(Equal("by caller"))
		})

		It("stamps a write through AsCaller with the transaction's sequence", func() {
			padmin := server.WithPrincipal(f.ctx, server.Principal{Subject: "padmin"})
			Expect(f.srv.Invoke(padmin, ws, "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"})).To(Succeed())
			Expect(seqs[0]).NotTo(BeZero())
			Expect(seqs[1]).To(Equal(seqs[0]))

			seqs = [2]uint64{}
			applied(invoke("padmin", "retitle_as_caller", &accessv1.TaskArgs{TaskId: "t1"}))
			Expect(seqs[0]).NotTo(BeZero())
			Expect(seqs[1]).To(Equal(seqs[0]))
		})

		It("lets a body ask what the invoking caller holds", func() {
			applied(invoke("guest", "probe", &accessv1.TaskArgs{TaskId: "t1"}))
			Expect(canEdit).To(BeFalse())
			applied(invoke("padmin", "probe", &accessv1.TaskArgs{TaskId: "t1"}))
			Expect(canEdit).To(BeTrue())
		})
	})

	DescribeTable("refuses a MutatorAccess the model cannot check",
		func(ma server.MutatorAccess, args proto.Message, want string) {
			noop := func(context.Context, *server.EntityTx, proto.Message) error { return nil }
			_, err := server.New(newTestStore(), accessEntities(), server.WithMutator("m", args, noop, ma))
			Expect(err).To(MatchError(ContainSubstring(want)))
		},
		Entry("a permission no role grants", server.MutatorAccess{Permission: "launch"}, nil,
			`permission "launch" of mutator m is granted by no role`),
		Entry("a key field the args do not have", server.MutatorAccess{Permission: "edit_tasks", Entity: "task", KeyField: "id"},
			&accessv1.TaskArgs{}, `key field "id" must be a singular string field`),
		Entry("an entity without a key field", server.MutatorAccess{Permission: "edit_tasks", Entity: "task"},
			&accessv1.TaskArgs{}, "names an Entity without a KeyField"),
	)
})

var _ = Describe("EntityTx.Can for a permission that only code checks", func() {
	var srv *server.Server

	// The access test proto with project_owner also granting export_projects,
	// which no rule and no MutatorAccess uses. The rows are planted below the
	// engine, since a compiled message does not write to an entity registered
	// from a descriptor set.
	BeforeEach(func() {
		f := newSchemaFixtureOn(newTestStore(), server.WithEntities())
		srv = f.srv
		_, err := srv.RegisterDescriptorSet(accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
			m := messageNamed(file, "ProjectMember")
			roles := proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetGrant().GetRoles()
			for _, r := range roles {
				if r.GetName() == "project_owner" {
					r.Grants = append(r.Grants, "export_projects")
				}
			}
		}))
		Expect(err).NotTo(HaveOccurred())
		f.plant("project", "p1", project("p1"))
		f.plant("project_member", "pm-owner",
			&accessv1.ProjectMember{Id: "pm-owner", Project: project("p1"), UserId: "owner", Role: "project_owner"})
		f.plant("project_member", "pm-padmin",
			&accessv1.ProjectMember{Id: "pm-padmin", Project: project("p1"), UserId: "padmin", Role: "project_admin"})
	})

	can := func(ctx context.Context, permission string) (bool, error) {
		var held bool
		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			var err error
			held, err = tx.Can(permission, &accessv1.Project{}, "p1")
			return err
		})
		return held, err
	}
	caller := func(user string) context.Context {
		return server.WithPrincipal(context.Background(), server.Principal{Subject: user})
	}

	It("holds for a caller whose role grants it, and not for one whose role does not", func() {
		Expect(can(caller("owner"), "export_projects")).To(BeTrue())
		Expect(can(caller("padmin"), "export_projects")).To(BeFalse())
	})

	It("refuses a permission no role grants", func() {
		_, err := can(caller("owner"), "export_project")
		Expect(err).To(MatchError(server.ErrUnknownPermission))
		Expect(err).To(MatchError(ContainSubstring(`"export_project"`)))
	})

	It("refuses a permission no role grants in a Trusted transaction too", func() {
		_, err := can(server.Trusted(context.Background()), "export_project")
		Expect(err).To(MatchError(server.ErrUnknownPermission))
		Expect(can(server.Trusted(context.Background()), "export_projects")).To(BeTrue())
	})
})

// refusalLog returns the attributes of the one "access refused" record logs
// holds, each rendered as text.
func refusalLog(logs *recordingHandler) map[string]string {
	GinkgoHelper()
	logs.mu.Lock()
	defer logs.mu.Unlock()
	var found []map[string]string
	for _, r := range logs.records {
		if r.Message != "access refused" {
			continue
		}
		Expect(r.Level).To(Equal(slog.LevelDebug))
		attrs := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			if v, ok := a.Value.Any().([]string); ok {
				attrs[a.Key] = strings.Join(v, "\n")
			} else {
				attrs[a.Key] = a.Value.String()
			}
			return true
		})
		found = append(found, attrs)
	}
	Expect(found).To(HaveLen(1))
	return found[0]
}
