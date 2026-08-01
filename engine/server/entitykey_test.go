package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Server.EntityKey", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newTestServer()
	})

	It("derives single-field keys verbatim", func() {
		entity, key, err := srv.EntityKey(&todov1.Todo{Id: "t1", Title: "ignored"})
		Expect(err).NotTo(HaveOccurred())
		Expect(entity).To(Equal("todo"))
		Expect(key).To(Equal("t1"))
	})

	It("escapes composite keys exactly like the push path", func() {
		pair := &testdatav1.Pair{Ns: "a/b", Id: "c", Label: "x"}
		entity, key, err := srv.EntityKey(pair)
		Expect(err).NotTo(HaveOccurred())
		Expect(entity).To(Equal("pair"))
		Expect(key).To(Equal(`a\/b/c`))

		// Push validates the client-supplied key against its own derivation;
		// an APPLIED result proves EntityKey and push agree.
		resp, err := srv.Push(context.Background(), connect.NewRequest(pushOf(
			"ws-key",
			[]*syncplanev1.Mutation{putMutation("c1", 1, entity, key, pair)})))
		Expect(err).NotTo(HaveOccurred())
		Expect(pushResults(resp.Msg)[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
	})

	It("rejects unregistered messages", func() {
		_, _, err := srv.EntityKey(&syncplanev1.PushRequest{})
		// Naming the fix, not just the fact: reaching the engine before wiring
		// the schema is the common first mistake (issues/072).
		Expect(err).To(MatchError(ContainSubstring(
			"is not a registered entity; register it first with server.WithEntities(&PushRequest{}) or Server.RegisterEntity")))
	})
})
