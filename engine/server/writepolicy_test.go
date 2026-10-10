package server_test

import (
	"context"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A push authorizes its writes before it takes the workspace write lock, and
// registration does not wait for that lock. An entity registered in between is
// unknown when the push authorizes and known when it applies.
var _ = Describe("Field write policy for an entity registered while a push waits", func() {
	var (
		ctx   context.Context
		srv   *server.Server
		store *registerOnWrite
	)

	BeforeEach(func() {
		ctx = server.WithPrincipal(context.Background(), server.Principal{Subject: "alice"})
		store = &registerOnWrite{Adapter: newTestStore()}
		srv = mustNewServer(store,
			server.WithEntities(&todov1.Todo{}),
			// Protects one more field of "guarded" for every caller, which is
			// the half of field protection the schema cannot state.
			server.WithPermissions(&fieldPolicy{protect: map[string][]string{"guarded": {"label"}}}),
		)
		store.register = func() {
			defer GinkgoRecover()
			Expect(srv.RegisterEntity(&testdatav1.Guarded{})).To(Succeed())
		}
		store.armed.Store(true)
	})

	push := func(m *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{m})))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)[0]
	}

	It("refuses a client PATCH whose mask names a field no guard rule admits writing", func() {
		result := push(batchMutation("c1", 1,
			crudPut("guarded", "g1", &testdatav1.Guarded{Id: "g1", Label: "x"}),
			crudPatch("guarded", "g1", &testdatav1.Guarded{Owner: "alice"}, "owner"),
		))

		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(result.GetErrorCode()).To(Equal(server.CodeAccessDenied))
		var g testdatav1.Guarded
		var found bool
		Expect(srv.View(server.Trusted(context.Background()), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&g, "g1")
			return err
		})).To(Succeed())
		Expect(found).To(BeFalse(), "the batch is atomic, so its PUT rolls back with the refused PATCH")
	})

	It("refuses a client PATCH whose mask names a field FieldPermissions protects", func() {
		result := push(batchMutation("c1", 1,
			crudPut("guarded", "g1", &testdatav1.Guarded{Id: "g1"}),
			crudPatch("guarded", "g1", &testdatav1.Guarded{Label: "renamed"}, "label"),
		))

		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))
		Expect(result.GetErrorMessage()).To(ContainSubstring("label"))
	})

	It("applies a write the entity's rules allow", func() {
		result := push(putMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{Id: "g1"}))

		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
	})
})

// registerOnWrite runs register the first time a write transaction opens after
// armed is set. That is the moment between a push's authorization and its
// apply, where a runtime registration (RegisterEntity, RegisterDescriptorSet,
// AdminService) can land while the push waits for the write lock.
type registerOnWrite struct {
	storage.Adapter
	armed    atomic.Bool
	once     sync.Once
	register func()
}

func (a *registerOnWrite) WithWriteTx(ctx context.Context, workspaceID string, fn func(storage.WriteTx) error) error {
	if a.armed.Load() {
		a.once.Do(a.register)
	}
	return a.Adapter.WithWriteTx(ctx, workspaceID, fn)
}
