package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A JavaScript client holds a map as a plain object, where the key
// "__proto__" sets the prototype instead of adding an entry. A row with such
// an entry would lose it on the first PUT from a TS client, so no write path
// stores one.
var _ = Describe("Map key __proto__", func() {
	var (
		ctx context.Context
		srv *server.Server
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv = newTestServer()
	})

	push := func(m *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{m})))
		Expect(err).NotTo(HaveOccurred())
		results := pushResults(resp.Msg)
		Expect(results).To(HaveLen(1))
		return results[0]
	}

	expectRefused := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		Expect(r.GetErrorMessage()).To(ContainSubstring("__proto__"))
	}

	It("refuses a PUT that holds the key", func() {
		expectRefused(push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
			Id: "w1", Attrs: map[string]string{"__proto__": "x", "k": "v"},
		})))
	})

	It("refuses a PATCH that adds the key", func() {
		Expect(push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"})).GetStatus()).
			To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		expectRefused(push(patchMutation("c1", 2, "widget", "w1",
			&testdatav1.Widget{Attrs: map[string]string{"__proto__": "x"}}, "attrs.__proto__")))
	})

	It("refuses the key in a map of messages", func() {
		expectRefused(push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
			Id: "w1", Custom: map[string]*testdatav1.FieldValue{"__proto__": {}},
		})))
	})

	It("refuses a trusted Put that holds the key", func() {
		err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Attrs: map[string]string{"__proto__": "x"}})
		})
		Expect(err).To(MatchError(ContainSubstring("__proto__")))
	})

	It("stores other keys, the empty key and a key that only starts the same", func() {
		attrs := map[string]string{"": "a", "__proto": "b", "__proto__x": "c", "proto": "d"}
		Expect(push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Attrs: attrs})).GetStatus()).
			To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
	})
})
