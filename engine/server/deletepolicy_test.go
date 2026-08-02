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

// The "ledger" entity declares delete_policy TRUSTED_ONLY (see
// proto/private/syncplane/testdata/v1/testdata.proto); "todo" declares nothing
// and is the control.
var _ = Describe("Entity delete policy", func() {
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
			// its invocation, so this deletes a protected row on behalf of an
			// untrusted caller.
			server.WithMutator("purgeLedger", &testdatav1.Ledger{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Delete(&testdatav1.Ledger{}, args.(*testdatav1.Ledger).GetId())
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

	// seed plants a ledger row through the trusted path, which the policy does
	// not apply to.
	seedLedger := func(id string) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Ledger{Id: id, Label: "seeded"})
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

	Describe("over the wire", func() {
		BeforeEach(func() { seedLedger("l1") })

		It("rejects a DELETE of a TRUSTED_ONLY entity and leaves the row", func() {
			result := push(deleteMutation("c1", 1, "ledger", "l1"))[0]

			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))
			Expect(result.GetErrorMessage()).To(ContainSubstring("ledger"))
			Expect(ledgerExists("l1")).To(BeTrue())
		})

		It("refuses before the app's policy is consulted", func() {
			push(deleteMutation("c1", 1, "ledger", "l1"))

			// The engine's own rule is not a fallback for a policy that had no
			// opinion: an allow-everything policy never saw this write at all.
			Expect(policy.writes).To(BeEmpty())
		})

		It("still accepts PUT and PATCH of the same entity", func() {
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

			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))
			Expect(ledgerExists("l1")).To(BeTrue())
		})
	})

	Describe("in-process API", func() {
		BeforeEach(func() { seedLedger("l1") })

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
	})

	Describe("the custom-mutator escape hatch", func() {
		BeforeEach(func() { seedLedger("l1") })

		It("lets an untrusted caller delete through a mutator, whose rows are not authorized one by one", func() {
			result := push(customMutation("c1", 1, "purgeLedger", &testdatav1.Ledger{Id: "l1"}))[0]

			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(ledgerExists("l1")).To(BeFalse())
		})
	})
})
