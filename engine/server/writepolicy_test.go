package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// Three fixtures, all in proto/private/syncplane/testdata/v1/testdata.proto:
// "ledger" restricts deletes only, "activity" restricts all three (a
// server-owned entity), and "comment" restricts updates and deletes but not
// creates (an append-only one). "todo" declares nothing and is the control.
var _ = Describe("Entity write policy", func() {
	var (
		ctx    context.Context
		policy *recordingPolicy
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		policy = &recordingPolicy{}
		srv = newTestServer(
			server.WithPermissions(policy),
			// The escape hatch: a mutator's rows are covered by the verdict on
			// its invocation, so these write protected rows on behalf of an
			// untrusted caller.
			server.WithMutator("purgeLedger", &testdatav1.Ledger{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Delete(&testdatav1.Ledger{}, args.(*testdatav1.Ledger).GetId())
				}),
			server.WithMutator("editComment", &testdatav1.Comment{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					c := args.(*testdatav1.Comment)
					return tx.Patch(c, []string{"body"}, nil)
				}),
		)

		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	// seed plants rows through the trusted path, which no policy applies to.
	seedLedger := func(id string) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Ledger{Id: id, Label: "seeded"})
		})).To(Succeed())
	}

	seedComment := func(id string) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Comment{Id: id, Body: "seeded"})
		})).To(Succeed())
	}

	ledgerExists := func(id string) bool {
		GinkgoHelper()
		var (
			out   testdatav1.Ledger
			found bool
		)
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&out, id)
			return err
		})).To(Succeed())
		return found
	}

	commentBody := func(id string) (string, bool) {
		GinkgoHelper()
		var (
			out   testdatav1.Comment
			found bool
		)
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&out, id)
			return err
		})).To(Succeed())
		return out.GetBody(), found
	}

	activityExists := func(id string) bool {
		GinkgoHelper()
		var (
			out   testdatav1.Activity
			found bool
		)
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&out, id)
			return err
		})).To(Succeed())
		return found
	}

	denied := func(result *syncplanev1.MutationResult) string {
		GinkgoHelper()
		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))
		return result.GetErrorMessage()
	}

	Describe("over the wire", func() {
		BeforeEach(func() {
			seedLedger("l1")
			seedComment("c1")
		})

		It("rejects a DELETE of a delete-restricted entity and leaves the row", func() {
			Expect(denied(push(deleteMutation("c1", 1, "ledger", "l1"))[0])).To(ContainSubstring("ledger"))
			Expect(ledgerExists("l1")).To(BeTrue())
		})

		It("refuses before the app's policy is consulted", func() {
			push(deleteMutation("c1", 1, "ledger", "l1"))

			// The engine's own rule is not a fallback for a policy that had no
			// opinion: an allow-everything policy never saw this write at all.
			Expect(policy.writes).To(BeEmpty())
		})

		It("still accepts PUT and PATCH of a delete-only restriction", func() {
			results := push(
				putMutation("c1", 1, "ledger", "l2", &testdatav1.Ledger{Id: "l2", Label: "made"}),
				patchMutation("c1", 2, "ledger", "l2", &testdatav1.Ledger{Label: "edited"}, "label"),
			)
			for _, r := range results {
				Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			}
			Expect(ledgerExists("l2")).To(BeTrue())
		})

		It("still accepts DELETE of an entity that declares no policy", func() {
			Expect(push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "control"}))[0].GetStatus()).
				To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			result := push(deleteMutation("c1", 2, "todo", "t1"))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})

		It("rolls back a whole batch that carries one protected DELETE", func() {
			result := push(batchMutation("c1", 1,
				crudPut("todo", "t1", &todov1.Todo{Id: "t1", Title: "control"}),
				crudDelete("ledger", "l1"),
			))[0]

			Expect(denied(result)).To(ContainSubstring("ledger"))
			Expect(ledgerExists("l1")).To(BeTrue())
		})

		It("rolls back a whole batch that carries one protected PATCH", func() {
			result := push(batchMutation("c1", 1,
				crudPut("todo", "t1", &todov1.Todo{Id: "t1", Title: "control"}),
				crudPatch("comment", "c1", &testdatav1.Comment{Body: "edited"}, "body"),
			))[0]

			Expect(denied(result)).To(ContainSubstring("comment"))
			body, _ := commentBody("c1")
			Expect(body).To(Equal("seeded"))
		})

		Describe("a server-owned entity", func() {
			It("refuses an unguarded PUT", func() {
				Expect(denied(push(putMutation("c1", 1, "activity", "a1",
					&testdatav1.Activity{Id: "a1", What: "made"}))[0])).To(ContainSubstring("activity"))
				Expect(activityExists("a1")).To(BeFalse())
			})

			It("refuses a PUT that provably creates", func() {
				Expect(denied(push(withExpectedVersion(
					putMutation("c1", 1, "activity", "a1", &testdatav1.Activity{Id: "a1", What: "made"}), 0,
				))[0])).To(ContainSubstring("create_policy"))
				Expect(activityExists("a1")).To(BeFalse())
			})

			It("refuses a PATCH", func() {
				Expect(denied(push(patchMutation("c1", 1, "activity", "a1",
					&testdatav1.Activity{What: "edited"}, "what"))[0])).To(ContainSubstring("update_policy"))
			})

			It("refuses a DELETE", func() {
				Expect(denied(push(deleteMutation("c1", 1, "activity", "a1"))[0])).
					To(ContainSubstring("delete_policy"))
			})
		})

		Describe("an append-only entity", func() {
			It("accepts a PUT that provably creates", func() {
				result := push(withExpectedVersion(
					putMutation("c1", 1, "comment", "c2", &testdatav1.Comment{Id: "c2", Body: "posted"}), 0,
				))[0]

				Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
				body, found := commentBody("c2")
				Expect(found).To(BeTrue())
				Expect(body).To(Equal("posted"))
			})

			It("refuses an unguarded PUT, and says how to assert a create", func() {
				msg := denied(push(putMutation("c1", 1, "comment", "c2",
					&testdatav1.Comment{Id: "c2", Body: "posted"}))[0])

				Expect(msg).To(ContainSubstring("comment"))
				Expect(msg).To(ContainSubstring("update_policy"))
				Expect(msg).To(ContainSubstring("expected_version 0"))
				_, found := commentBody("c2")
				Expect(found).To(BeFalse())
			})

			It("refuses a PUT that provably updates", func() {
				Expect(denied(push(withExpectedVersion(
					putMutation("c1", 1, "comment", "c1", &testdatav1.Comment{Id: "c1", Body: "edited"}), 1,
				))[0])).To(ContainSubstring("update_policy"))

				body, _ := commentBody("c1")
				Expect(body).To(Equal("seeded"))
			})

			It("refuses a PATCH", func() {
				Expect(denied(push(patchMutation("c1", 1, "comment", "c1",
					&testdatav1.Comment{Body: "edited"}, "body"))[0])).To(ContainSubstring("update_policy"))

				body, _ := commentBody("c1")
				Expect(body).To(Equal("seeded"))
			})

			It("refuses a create before the app's policy is consulted", func() {
				push(putMutation("c1", 1, "comment", "c2", &testdatav1.Comment{Id: "c2", Body: "posted"}))

				Expect(policy.writes).To(BeEmpty())
			})
		})
	})

	Describe("in-process API", func() {
		BeforeEach(func() {
			seedLedger("l1")
			seedComment("c1")
		})

		It("refuses Apply, which is held to the same policy as a client", func() {
			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Delete(&testdatav1.Ledger{}, "l1")
			})

			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rej.Code).To(Equal(server.CodePermissionDenied))
			Expect(ledgerExists("l1")).To(BeTrue())
		})

		It("refuses Mutate", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Ledger{}, "l1")
			})

			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rej.Code).To(Equal(server.CodePermissionDenied))
			Expect(ledgerExists("l1")).To(BeTrue())
		})

		It("admits a Trusted write, which is what the policy is defined against", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.Ledger{}, "l1")
			})).To(Succeed())

			Expect(ledgerExists("l1")).To(BeFalse())
		})

		It("refuses an unguarded Put of an update-restricted entity", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Comment{Id: "c2", Body: "posted"})
			})

			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rej.Code).To(Equal(server.CodePermissionDenied))
		})

		It("lets WithExpectedVersion(0) state a create, which lands", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Comment{Id: "c2", Body: "posted"}, server.WithExpectedVersion(0))
			})).To(Succeed())

			body, found := commentBody("c2")
			Expect(found).To(BeTrue())
			Expect(body).To(Equal("posted"))
		})

		It("lets WithExpectedVersion(0) state a create through Apply too", func() {
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Put(&testdatav1.Comment{Id: "c3", Body: "posted"}, server.WithExpectedVersion(0))
			})).To(Succeed())

			_, found := commentBody("c3")
			Expect(found).To(BeTrue())
		})

		It("conflicts when WithExpectedVersion(0) meets a row that already exists", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Comment{Id: "c1", Body: "clobber"}, server.WithExpectedVersion(0))
			})

			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rej.Code).To(Equal(server.CodeConflict))
			body, _ := commentBody("c1")
			Expect(body).To(Equal("seeded"))
		})

		It("admits a Trusted unguarded Put of a server-owned entity", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Activity{Id: "a1", What: "recorded"})
			})).To(Succeed())

			Expect(activityExists("a1")).To(BeTrue())
		})
	})

	Describe("the custom-mutator escape hatch", func() {
		BeforeEach(func() {
			seedLedger("l1")
			seedComment("c1")
		})

		It("lets an untrusted caller delete through a mutator, whose rows are not authorized one by one", func() {
			result := push(customMutation("c1", 1, "purgeLedger", &testdatav1.Ledger{Id: "l1"}))[0]

			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(ledgerExists("l1")).To(BeFalse())
		})

		It("lets an untrusted caller edit an append-only row through a mutator", func() {
			result := push(customMutation("c1", 1, "editComment",
				&testdatav1.Comment{Id: "c1", Body: "edited"}))[0]

			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			body, _ := commentBody("c1")
			Expect(body).To(Equal("edited"))
		})
	})
})
