package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// countingPerms records how many times CanMutate fires and optionally injects a
// verdict, so a spec can assert the hook's invocation count and timing (issue
// 017: the hooks now run OUTSIDE transaction scope).
type countingPerms struct {
	server.AllowAll
	mu      sync.Mutex
	mutate  int
	verdict func(w server.Write) error
}

func (p *countingPerms) CanMutate(_ context.Context, _ server.Principal, _ string, w server.Write) error {
	p.mu.Lock()
	p.mutate++
	p.mu.Unlock()
	if p.verdict != nil {
		return p.verdict(w)
	}
	return nil
}

func (p *countingPerms) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mutate
}

var _ = Describe("Permissions hook scheduling (issue 017)", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() { ctx = context.Background() })

	// startWith builds a server over a custom store + permissions and wires an
	// httptest client to it.
	startWith := func(store storage.Adapter, perms server.Permissions) {
		srv := mustNewServer(store,
			server.WithEntities(&todov1.Todo{}),
			server.WithPermissions(perms))
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	openStore := func() storage.Adapter {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		return store
	}

	push := func(mutations ...*syncplanev1.Mutation) ([]*syncplanev1.MutationResult, error) {
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		if err != nil {
			return nil, err
		}
		return pushResults(resp.Msg), nil
	}

	It("invokes CanMutate exactly once per mutation even when the write closure retries", func() {
		perms := &countingPerms{}
		startWith(&retryingAdapter{Adapter: openStore()}, perms)

		resp, err := push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}),
		)
		Expect(err).NotTo(HaveOccurred())
		for _, r := range resp {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
		// The write closure ran twice (rolled-back + real); CanMutate, hoisted
		// out of the closure, ran once per mutation — not twice.
		Expect(perms.calls()).To(Equal(2), "CanMutate must not re-run on a write-closure retry")
	})

	It("still acks a CanMutate rejection as REJECTED and bumps the watermark", func() {
		perms := &countingPerms{verdict: func(w server.Write) error {
			if w.MutationId == 1 {
				return server.Reject(server.CodePermissionDenied, "nope")
			}
			return nil
		}}
		startWith(openStore(), perms)

		resp, err := push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "denied"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(resp[0].GetErrorCode()).To(Equal(server.CodePermissionDenied))
		Expect(resp[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		// The rejection is an ack: the watermark advanced past both ids.
		pull, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c1"}))
		Expect(err).NotTo(HaveOccurred())
		Expect(pull.Msg.GetLastAckedMutationId()).To(BeEquivalentTo(2))
	})

	It("fails the whole workspace when CanMutate returns a non-rejection error", func() {
		// A hook that could not decide is transient, so the batch fails as
		// "internal" — retryable — rather than acking mutations the policy
		// never approved. It is scoped to the workspace, not the request: a
		// push spanning several workspaces must still apply the others.
		boom := errors.New("introspection backend down")
		perms := &countingPerms{verdict: func(w server.Write) error {
			if w.MutationId == 2 {
				return boom
			}
			return nil
		}}
		startWith(openStore(), perms)

		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetResults()).To(HaveLen(1))
		Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeInternal))
		Expect(resp.Msg.GetResults()[0].GetResults()).To(BeEmpty())

		// Nothing committed: authorization runs before any write transaction.
		pull, perr := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c1"}))
		Expect(perr).NotTo(HaveOccurred())
		Expect(pull.Msg.GetPatch().GetChanges()).To(BeEmpty())
		Expect(pull.Msg.GetLastAckedMutationId()).To(BeEquivalentTo(0))
	})
})
