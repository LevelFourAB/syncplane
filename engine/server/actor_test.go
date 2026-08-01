package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
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
		It("records the authenticated caller and the pushing client", func() {
			push(server.WithPrincipal(ctx, principal),
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{
				Issuer:  principal.Issuer,
				Subject: principal.Subject,
				Client:  "c1",
			}))
		})

		It("records the client alone when no authenticator ran", func() {
			push(ctx, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))

			// An anonymous caller is not "no attribution": client_id still says
			// which replica wrote, which is what echo suppression keys on.
			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Client: "c1"}))
		})

		It("attributes a tombstone to whoever deleted the row", func() {
			push(ctx, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			push(server.WithPrincipal(ctx, principal), deleteMutation("c2", 1, "todo", "t1"))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{
				Issuer:  principal.Issuer,
				Subject: principal.Subject,
				Client:  "c2",
			}), "the tombstone records the deleter, not the row's last writer")
		})

		It("keeps the last writer only, since the feed coalesces by row", func() {
			push(server.WithPrincipal(ctx, principal),
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			push(ctx, putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "two"}))

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{Client: "c2"}),
				"the earlier writer is not recoverable — this is attribution, not an audit log")
		})

		It("attributes rows a mutator writes to the caller who invoked it", func() {
			srv = newTestServer(server.WithMutator("rename", &testdatav1.Widget{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Put(&testdatav1.Widget{Id: "w1", Label: args.(*testdatav1.Widget).GetLabel()})
				}))

			push(server.WithPrincipal(ctx, principal),
				customMutation("c1", 1, "rename", &testdatav1.Widget{Label: "renamed"}))

			Expect(actorOf("widget", "w1")).To(Equal(storage.Actor{
				Issuer:  principal.Issuer,
				Subject: principal.Subject,
				Client:  "c1",
			}), "a mutator body writes as its caller, never as itself")
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

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{
				Issuer:  principal.Issuer,
				Subject: principal.Subject,
			}), "no client_id off the sync path")
		})

		It("marks a trusted write as system, named by TrustedAs", func() {
			put(server.TrustedAs(ctx, "connect:markdown"), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{
				System: true, Subject: "connect:markdown",
			}))
		})

		It("marks a bare trusted write as unnamed system work", func() {
			put(server.Trusted(ctx), "t1")

			// Distinguishable from an unattributed write, which is what a
			// consumer deciding whether to react has to tell apart.
			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{System: true}))
			Expect(actorOf("todo", "t1")).NotTo(Equal(storage.Actor{}))
		})

		It("prefers the trusted marker over a principal left on the ctx", func() {
			// Trusted composes with a principal so a request ctx can opt one
			// call out of enforcement. Recording the principal would attribute a
			// write to a caller who was never authorized for it.
			put(server.TrustedAs(server.WithPrincipal(ctx, principal), "migration"), "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{System: true, Subject: "migration"}))
		})

		It("records an unauthenticated in-process write as unattributed", func() {
			put(ctx, "t1")

			Expect(actorOf("todo", "t1")).To(Equal(storage.Actor{}))
		})
	})
})
