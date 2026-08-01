package server_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// recordingPolicy captures every hook call so a spec can assert on what the
// engine handed the policy, not merely on whether the write landed.
type recordingPolicy struct {
	server.AllowAll
	writes     []server.Write
	workspaces []string
	// denyWrite, when set, rejects any Write it returns true for.
	denyWrite func(server.Write) bool
	// denyWorkspace, when set, denies the workspace outright.
	denyWorkspace bool
	// hideTitle hides todo rows whose title equals it, via ReadFilter.
	hideTitle string
}

func (p *recordingPolicy) CanAccessWorkspace(_ context.Context, _ server.Principal, ws string) error {
	p.workspaces = append(p.workspaces, ws)
	if p.denyWorkspace {
		return server.DenyWorkspace("not a member")
	}
	return nil
}

func (p *recordingPolicy) CanMutate(_ context.Context, _ server.Principal, _ string, w server.Write) error {
	p.writes = append(p.writes, w)
	if p.denyWrite != nil && p.denyWrite(w) {
		return server.Reject(server.CodePermissionDenied, "nope")
	}
	return nil
}

func (p *recordingPolicy) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if p.hideTitle == "" {
		return nil, nil
	}
	return func(_ string, row proto.Message) bool {
		todo, ok := row.(*todov1.Todo)
		return !ok || todo.GetTitle() != p.hideTitle
	}, nil
}

var _ = Describe("In-process API permissions", func() {
	var (
		ctx    context.Context
		policy *recordingPolicy
		srv    *server.Server
	)

	BeforeEach(func() {
		ctx = context.Background()
		policy = &recordingPolicy{}
		srv = newTestServer(server.WithPermissions(policy))
	})

	// seed plants a row without going through the path under test.
	seed := func(todo *todov1.Todo) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(todo)
		})).To(Succeed())
	}

	get := func(key string) (*todov1.Todo, uint64, bool) {
		GinkgoHelper()
		var (
			out     todov1.Todo
			found   bool
			version uint64
		)
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, version, err = tx.Get(&out, key)
			return err
		})).To(Succeed())
		return &out, version, found
	}

	Describe("Apply", func() {
		It("authorizes each recorded write with the CrudMutation a client would have sent", func() {
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "buy milk"})
			})).To(Succeed())

			Expect(policy.workspaces).To(Equal([]string{ws}))
			Expect(policy.writes).To(HaveLen(1))
			crud := policy.writes[0].Crud
			Expect(crud).NotTo(BeNil())
			Expect(policy.writes[0].Custom).To(BeNil())
			Expect(crud.GetEntity()).To(Equal("todo"))
			Expect(crud.GetKey()).To(Equal("t1"))
			Expect(crud.GetOp()).To(Equal(syncplanev1.CrudOp_CRUD_OP_PUT))

			// The value is the submitted message, byte-for-byte — the same thing
			// a policy would decode off the wire.
			var decoded todov1.Todo
			Expect(proto.Unmarshal(crud.GetValue(), &decoded)).To(Succeed())
			Expect(decoded.GetTitle()).To(Equal("buy milk"))

			_, _, found := get("t1")
			Expect(found).To(BeTrue())
		})

		It("carries the effective mask and expected version of a PATCH", func() {
			seed(&todov1.Todo{Id: "t1", Title: "old"})
			_, version, _ := get("t1")

			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&todov1.Todo{Id: "t1", Title: "new"}, []string{"title"}, &version)
			})).To(Succeed())

			crud := policy.writes[0].Crud
			Expect(crud.GetOp()).To(Equal(syncplanev1.CrudOp_CRUD_OP_PATCH))
			Expect(crud.GetFieldMask().GetPaths()).To(Equal([]string{"title"}))
			Expect(crud.GetExpectedVersion()).To(Equal(version))
		})

		It("authorizes a DELETE with no value, whatever the prototype carries", func() {
			seed(&todov1.Todo{Id: "t1", Title: "doomed"})

			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				// A populated prototype is the natural way to write this; the
				// wire never carries a value on a DELETE, so neither may this.
				return tx.Delete(&todov1.Todo{Id: "t1", Title: "doomed"}, "t1")
			})).To(Succeed())

			crud := policy.writes[0].Crud
			Expect(crud.GetOp()).To(Equal(syncplanev1.CrudOp_CRUD_OP_DELETE))
			Expect(crud.GetKey()).To(Equal("t1"))
			Expect(crud.GetValue()).To(BeEmpty())

			_, _, found := get("t1")
			Expect(found).To(BeFalse())
		})

		It("applies nothing when any recorded write is rejected", func() {
			policy.denyWrite = func(w server.Write) bool {
				return w.Crud.GetKey() == "t2"
			}

			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				if err := tx.Put(&todov1.Todo{Id: "t1", Title: "allowed"}); err != nil {
					return err
				}
				return tx.Put(&todov1.Todo{Id: "t2", Title: "denied"})
			})
			rejection, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue())
			Expect(rejection.Code).To(Equal(server.CodePermissionDenied))

			// The allowed write is rolled back with the denied one: the writes
			// are authorized before the transaction opens, so neither lands.
			_, _, found := get("t1")
			Expect(found).To(BeFalse())
		})

		It("refuses the whole call when the workspace gate denies", func() {
			policy.denyWorkspace = true
			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "nope"})
			})
			denial, ok := server.AsWorkspaceDenial(err)
			Expect(ok).To(BeTrue())
			Expect(denial.Message).To(Equal("not a member"))
			// Denied before the closure ran at all.
			Expect(policy.writes).To(BeEmpty())
		})

		It("runs the closure exactly once", func() {
			calls := 0
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				calls++
				return tx.Put(&todov1.Todo{Id: "t1", Title: "once"})
			})).To(Succeed())
			Expect(calls).To(Equal(1))
		})

		It("refuses to read a key it has already written rather than serving a stale row", func() {
			seed(&todov1.Todo{Id: "t1", Title: "before"})

			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				if err := tx.Put(&todov1.Todo{Id: "t1", Title: "after"}); err != nil {
					return err
				}
				var out todov1.Todo
				_, _, err := tx.Get(&out, "t1")
				return err
			})
			Expect(err).To(MatchError(ContainSubstring("after writing it")))
		})

		It("writes nothing and consumes no sequence when the closure records no writes", func() {
			Expect(srv.Apply(ctx, "never-written", func(tx *server.ApplyTx) error {
				var out todov1.Todo
				_, _, err := tx.Get(&out, "absent")
				return err
			})).To(Succeed())

			ids, err := srv.WorkspaceIDs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).NotTo(ContainElement("never-written"))
		})

		It("applies recorded writes in order, so a later one observes an earlier one", func() {
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				if err := tx.Put(&todov1.Todo{Id: "t1", Title: "first"}); err != nil {
					return err
				}
				return tx.Patch(&todov1.Todo{Id: "t1", Title: "second"}, []string{"title"}, nil)
			})).To(Succeed())

			todo, _, found := get("t1")
			Expect(found).To(BeTrue())
			Expect(todo.GetTitle()).To(Equal("second"))
		})

		It("hides read-filtered rows from the closure", func() {
			seed(&todov1.Todo{Id: "t1", Title: "secret"})
			seed(&todov1.Todo{Id: "t2", Title: "public"})
			policy.hideTitle = "secret"

			var seen []string
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				var hidden todov1.Todo
				found, _, err := tx.Get(&hidden, "t1")
				if err != nil {
					return err
				}
				Expect(found).To(BeFalse(), "a filtered row must read as absent")

				_, _, err = tx.ScanPage(&todov1.Todo{}, "", 10, func(msg proto.Message, _ uint64) error {
					seen = append(seen, msg.(*todov1.Todo).GetTitle())
					return nil
				})
				return err
			})).To(Succeed())
			Expect(seen).To(Equal([]string{"public"}))
		})

		It("skips both hooks when the ctx is Trusted", func() {
			policy.denyWorkspace = true
			Expect(srv.Apply(server.Trusted(ctx), ws, func(tx *server.ApplyTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "system"})
			})).To(Succeed())
			Expect(policy.workspaces).To(BeEmpty())
			Expect(policy.writes).To(BeEmpty())
		})
	})

	Describe("ApplyIdempotent", func() {
		It("replays the first outcome without re-running the closure", func() {
			calls := 0
			write := func() server.WriteOutcome {
				GinkgoHelper()
				out, err := srv.ApplyIdempotent(ctx, ws, "key-1", func(tx *server.ApplyTx) ([]byte, error) {
					calls++
					if err := tx.Put(&todov1.Todo{Id: "t1", Title: fmt.Sprintf("attempt %d", calls)}); err != nil {
						return nil, err
					}
					return []byte("first response"), nil
				})
				Expect(err).NotTo(HaveOccurred())
				return out
			}

			first := write()
			Expect(first.Applied).To(BeTrue())
			Expect(first.Result).To(Equal([]byte("first response")))

			second := write()
			Expect(second.Applied).To(BeFalse())
			Expect(second.Seq).To(Equal(first.Seq))
			Expect(second.Result).To(Equal([]byte("first response")))

			Expect(calls).To(Equal(1), "the closure must not run for a claimed key")
			todo, _, _ := get("t1")
			Expect(todo.GetTitle()).To(Equal("attempt 1"))
		})

		It("leaves the key unclaimed when the write is rejected", func() {
			policy.denyWrite = func(server.Write) bool { return true }
			_, err := srv.ApplyIdempotent(ctx, ws, "key-1", func(tx *server.ApplyTx) ([]byte, error) {
				return nil, tx.Put(&todov1.Todo{Id: "t1", Title: "denied"})
			})
			Expect(err).To(HaveOccurred())

			// Retryable: a rejection is not a recorded outcome.
			policy.denyWrite = nil
			out, err := srv.ApplyIdempotent(ctx, ws, "key-1", func(tx *server.ApplyTx) ([]byte, error) {
				return nil, tx.Put(&todov1.Todo{Id: "t1", Title: "allowed"})
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Applied).To(BeTrue())
		})
	})

	Describe("Mutate", func() {
		It("authorizes each write against the ctx principal", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "buy milk"})
			})).To(Succeed())

			Expect(policy.workspaces).To(Equal([]string{ws}))
			Expect(policy.writes).To(HaveLen(1))
			Expect(policy.writes[0].Crud.GetKey()).To(Equal("t1"))
			Expect(policy.writes[0].Crud.GetOp()).To(Equal(syncplanev1.CrudOp_CRUD_OP_PUT))
		})

		It("rolls the transaction back when a write is rejected", func() {
			policy.denyWrite = func(w server.Write) bool { return w.Crud.GetKey() == "t2" }

			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				if err := tx.Put(&todov1.Todo{Id: "t1", Title: "allowed"}); err != nil {
					return err
				}
				return tx.Put(&todov1.Todo{Id: "t2", Title: "denied"})
			})
			_, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue())

			_, _, found := get("t1")
			Expect(found).To(BeFalse())
		})

		It("authorizes a DELETE with no value", func() {
			seed(&todov1.Todo{Id: "t1", Title: "doomed"})
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Delete(&todov1.Todo{}, "t1")
			})).To(Succeed())

			crud := policy.writes[0].Crud
			Expect(crud.GetOp()).To(Equal(syncplanev1.CrudOp_CRUD_OP_DELETE))
			Expect(crud.GetKey()).To(Equal("t1"))
			Expect(crud.GetValue()).To(BeEmpty())
		})

		It("skips both hooks when the ctx is Trusted", func() {
			policy.denyWorkspace = true
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "system"})
			})).To(Succeed())
			Expect(policy.writes).To(BeEmpty())
		})

		It("denies before opening the transaction when the workspace gate refuses", func() {
			policy.denyWorkspace = true
			ran := false
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				ran = true
				return nil
			})
			_, ok := server.AsWorkspaceDenial(err)
			Expect(ok).To(BeTrue())
			Expect(ran).To(BeFalse())
		})
	})

	Describe("View", func() {
		It("applies the read filter to Get, ScanPage and Query", func() {
			seed(&todov1.Todo{Id: "t1", Title: "secret"})
			seed(&todov1.Todo{Id: "t2", Title: "public"})
			policy.hideTitle = "secret"

			var scanned, queried []string
			Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
				var hidden todov1.Todo
				found, _, err := tx.Get(&hidden, "t1")
				if err != nil {
					return err
				}
				Expect(found).To(BeFalse())

				if _, _, err := tx.ScanPage(&todov1.Todo{}, "", 10, func(msg proto.Message, _ uint64) error {
					scanned = append(scanned, msg.(*todov1.Todo).GetTitle())
					return nil
				}); err != nil {
					return err
				}
				return tx.Query(&todov1.Todo{}, query.Spec{}, func(msg proto.Message, _ uint64) error {
					queried = append(queried, msg.(*todov1.Todo).GetTitle())
					return nil
				})
			})).To(Succeed())

			Expect(scanned).To(Equal([]string{"public"}))
			Expect(queried).To(Equal([]string{"public"}))
		})

		It("returns every live row for a Trusted read", func() {
			seed(&todov1.Todo{Id: "t1", Title: "secret"})
			policy.hideTitle = "secret"

			var out todov1.Todo
			var found bool
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				var err error
				found, _, err = tx.Get(&out, "t1")
				return err
			})).To(Succeed())
			Expect(found).To(BeTrue())
		})

		It("refuses the read when the workspace gate denies", func() {
			policy.denyWorkspace = true
			err := srv.View(ctx, ws, func(tx *server.ViewTx) error { return nil })
			_, ok := server.AsWorkspaceDenial(err)
			Expect(ok).To(BeTrue())
		})
	})

	Describe("Invoke", func() {
		var invoked int

		BeforeEach(func() {
			invoked = 0
			policy = &recordingPolicy{}
			srv = newTestServer(
				server.WithPermissions(policy),
				server.WithMutator("addTodo", &todov1.Todo{}, func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					invoked++
					// Two rows, so a per-row policy would see two Writes.
					if err := tx.Put(&todov1.Todo{Id: "m1", Title: args.(*todov1.Todo).GetTitle()}); err != nil {
						return err
					}
					return tx.Put(&todov1.Todo{Id: "m2", Title: "side effect"})
				}),
			)
		})

		It("authorizes the verb once, not the rows the mutator writes", func() {
			Expect(srv.Invoke(ctx, ws, "addTodo", &todov1.Todo{Title: "from mutator"})).To(Succeed())

			Expect(invoked).To(Equal(1))
			Expect(policy.workspaces).To(Equal([]string{ws}))
			Expect(policy.writes).To(HaveLen(1), "one Write{Custom} for the invocation, matching push")

			custom := policy.writes[0].Custom
			Expect(custom).NotTo(BeNil())
			Expect(policy.writes[0].Crud).To(BeNil())
			Expect(custom.GetName()).To(Equal("addTodo"))

			var args todov1.Todo
			Expect(custom.GetArgs().UnmarshalTo(&args)).To(Succeed())
			Expect(args.GetTitle()).To(Equal("from mutator"))
		})

		It("does not run the mutator when the verb is rejected", func() {
			policy.denyWrite = func(server.Write) bool { return true }
			err := srv.Invoke(ctx, ws, "addTodo", &todov1.Todo{Title: "denied"})
			_, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue())
			Expect(invoked).To(BeZero())
		})

		It("skips the hooks when the ctx is Trusted", func() {
			policy.denyWorkspace = true
			Expect(srv.Invoke(server.Trusted(ctx), ws, "addTodo", &todov1.Todo{Title: "system"})).To(Succeed())
			Expect(policy.writes).To(BeEmpty())
			Expect(invoked).To(Equal(1))
		})
	})

	Describe("the anonymous principal", func() {
		It("is what an unauthenticated ctx authorizes as, so a deny-by-default policy refuses", func() {
			srv = newTestServer(server.WithPermissions(denyAnonymous{}))

			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "anonymous"})
			})
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, errAnonymous)).To(BeTrue())

			// The same call with an identity is admitted.
			Expect(srv.Apply(server.WithPrincipal(ctx, server.Principal{Subject: "u1"}), ws,
				func(tx *server.ApplyTx) error {
					return tx.Put(&todov1.Todo{Id: "t1", Title: "authenticated"})
				})).To(Succeed())
		})
	})
})

var errAnonymous = errors.New("anonymous callers may not write")

// denyAnonymous is the shape the docs recommend: lead with IsAnonymous.
type denyAnonymous struct{ server.AllowAll }

func (denyAnonymous) CanAccessWorkspace(_ context.Context, p server.Principal, _ string) error {
	if p.IsAnonymous() {
		return errAnonymous
	}
	return nil
}
