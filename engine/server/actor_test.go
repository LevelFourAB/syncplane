package server_test

import (
	"context"
	"strings"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("Write attribution", func() {
	var (
		ctx context.Context
		srv *server.Server
	)
	const ws = "ws1"

	principal := server.Principal{Issuer: "https://issuer.example", Subject: "user-7"}

	// actorOf drains the feed and returns the actor recorded for one row. The
	// feed is where attribution has to be observable: it is the only place a
	// consumer running on its own ctx can recover who wrote something.
	actorOf := func(entity, key string) storage.Actor {
		GinkgoHelper()
		var got *server.Change
		_, _, err := srv.Changes(ctx, ws, server.Cursor{}, 0, func(ch server.Change) error {
			if ch.Entity == entity && ch.Key == key {
				c := ch
				got = &c
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil(), "no change for %s/%s", entity, key)
		return got.Actor
	}

	push := func(pctx context.Context, m *syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := srv.Push(pctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{m})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
				"setup push rejected: %s", r.GetErrorMessage())
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		srv = newTestServer()
	})

	Describe("the sync path", func() {
		It("records the authenticated caller", func() {
			push(server.WithPrincipal(ctx, principal),
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Subject: principal.Subject}))
		})

		It("records nothing for a push no authenticator ran on", func() {
			push(ctx, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))

			// Attribution is what authentication buys: the client_id a push
			// came under is an idempotency handle a client chooses for itself,
			// so it is not recorded as identity.
			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{}))
		})

		It("records the subject alone, never the issuer that vouched for it", func() {
			push(server.WithPrincipal(ctx, server.Principal{
				Issuer:  "https://issuer.example",
				Subject: "user-7",
			}), putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Subject: "user-7"}),
				"an authenticator serving several issuers has to namespace its subjects")
		})

		It("attributes a tombstone to whoever deleted the row", func() {
			push(ctx, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			push(server.WithPrincipal(ctx, principal), deleteMutation("c2", 1, "todo", "t1"))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Subject: principal.Subject}),
				"the tombstone records the deleter, not the row's last writer")
		})

		It("keeps the last writer only, since the feed coalesces by row", func() {
			push(server.WithPrincipal(ctx, principal),
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			push(server.WithPrincipal(ctx, server.Principal{Subject: "user-9"}),
				putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "two"}))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Subject: "user-9"}),
				"the earlier writer is not recoverable — this is attribution, not an audit log")
		})

		It("attributes rows a mutator writes to the caller who invoked it", func() {
			srv = newTestServer(server.WithMutator("rename", &testdatav1.Widget{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Put(&testdatav1.Widget{Id: "w1", Label: args.(*testdatav1.Widget).GetLabel()})
				}))

			push(server.WithPrincipal(ctx, principal),
				customMutation("c1", 1, "rename", &testdatav1.Widget{Label: "renamed"}))

			Expect(actorOf("widget", "w1")).To(Equal(storage.Actor{Subject: principal.Subject}),
				"a mutator body writes as its caller, never as itself")
		})
	})

	Describe("in-process writes", func() {
		put := func(wctx context.Context, key string) {
			GinkgoHelper()
			Expect(srv.Mutate(wctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: key, Title: "t"})
			})).To(Succeed())
		}

		It("records the ctx principal", func() {
			put(server.WithPrincipal(ctx, principal), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Subject: principal.Subject}))
		})

		It("marks a trusted write as system, named by TrustedAs", func() {
			put(server.TrustedAs(ctx, "connect:markdown"), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{System: "connect:markdown"}))
		})

		It("marks a bare trusted write as unnamed system work", func() {
			put(server.Trusted(ctx), "t1")

			// Distinguishable from an unattributed write, which is what a
			// consumer deciding whether to react has to tell apart.
			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{System: "system"}))
		})

		It("records the caller a trusted write ran under alongside its label", func() {
			// Trusted composes with a principal so a request ctx can opt one
			// call out of enforcement. Both fields answer different questions:
			// the label says a bypass ran, the subject says whose request it
			// ran under.
			put(server.TrustedAs(server.WithPrincipal(ctx, principal), "migration"), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{
				Subject: principal.Subject,
				System:  "migration",
			}))
		})

		It("truncates a system label rather than failing the write", func() {
			long := strings.Repeat("x", naming.MaxActorSystemBytes+10)
			put(server.TrustedAs(ctx, long), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{
				System: strings.Repeat("x", naming.MaxActorSystemBytes),
			}), "a typo in a connector's name must not stall the connector")
		})

		It("falls back to the bare label when a system label is unusable", func() {
			put(server.TrustedAs(ctx, "connect\u001fmarkdown"), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{System: "system"}))
		})

		It("rejects a write whose principal cannot be recorded", func() {
			bad := server.WithPrincipal(ctx, server.Principal{
				Subject: strings.Repeat("u", naming.MaxActorSubjectBytes+1),
			})
			err := srv.Mutate(bad, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "t"})
			})
			// A truncated identity is a different identity and possibly a
			// colliding one, so the subject is the half that fails.
			Expect(err).To(MatchError(ContainSubstring("actor subject")))
		})

		It("records an unauthenticated in-process write as unattributed", func() {
			put(ctx, "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{}))
		})
	})
})
