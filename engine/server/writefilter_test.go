package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

// ownerPolicy hides todos titled "secret …" and widgets labelled "secret", and
// decides todo writes with a WriteFilter of its own: a todo titled
// "<subject>: …" may be written only by that subject, and every other todo by
// anyone — including the ones the read filter hides. Widgets get no write
// filter, so their writes fall back to the read filter.
type ownerPolicy struct {
	server.AllowAll
	mu       sync.Mutex
	resolved int
	seen     []server.Write
	hookErr  error
}

func (p *ownerPolicy) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	switch entity {
	case "todo":
		return func(_ string, row proto.Message) bool {
			return !strings.HasPrefix(row.(*todov1.Todo).GetTitle(), "secret")
		}, nil
	case "widget":
		return func(_ string, row proto.Message) bool {
			return row.(*testdatav1.Widget).GetLabel() != "secret"
		}, nil
	}
	return nil, nil
}

func (p *ownerPolicy) WriteFilter(
	_ context.Context, who server.Principal, _, entity string,
) (server.RowWriteFilter, error) {
	p.mu.Lock()
	p.resolved++
	p.mu.Unlock()
	if p.hookErr != nil {
		return nil, p.hookErr
	}
	if entity != "todo" {
		return nil, nil
	}
	return func(w server.Write, stored proto.Message) error {
		p.mu.Lock()
		p.seen = append(p.seen, w)
		p.mu.Unlock()
		owner, _, owned := strings.Cut(stored.(*todov1.Todo).GetTitle(), ": ")
		if owned && owner != who.Subject {
			return server.Reject(server.CodePermissionDenied, "only the owner may change this todo")
		}
		return nil
	}, nil
}

func (p *ownerPolicy) resolvedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resolved
}

func (p *ownerPolicy) writesSeen() []server.Write {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.seen)
}

func (p *ownerPolicy) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resolved, p.seen = 0, nil
}

var _ = Describe("Writes to rows the caller may not read", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	start := func(s *server.Server) {
		srv = s
		path, handler := s.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	pullTodos := func(clientID string) map[string]*todov1.Todo {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: clientID}))
		Expect(err).NotTo(HaveOccurred())
		out := map[string]*todov1.Todo{}
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetTombstone() || rc.GetEntity() != "todo" {
				continue
			}
			var t todov1.Todo
			Expect(proto.Unmarshal(rc.GetValue(), &t)).To(Succeed())
			out[rc.GetKey()] = &t
		}
		return out
	}

	// stored reads a todo as the server holds it, past every filter.
	stored := func(key string) (*todov1.Todo, bool) {
		GinkgoHelper()
		var t todov1.Todo
		var found bool
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&t, key)
			return err
		})).To(Succeed())
		return &t, found
	}

	expectDenied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodePermissionDenied))
	}

	expectRejection := func(err error, code string) {
		GinkgoHelper()
		rejection, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "want a rejection, got %v", err)
		Expect(rejection.Code).To(Equal(code))
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("with only a ReadFilter", func() {
		BeforeEach(func() {
			start(newTestServer(server.WithPermissions(exampleapp.NewPermissions())))
			// Creating a row the creator cannot read is allowed: there is no
			// stored row to check, and CanMutate saw the value.
			Expect(push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "secret plan", SortKey: "confidential"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "public"}),
			)).To(HaveEach(HaveField("Status", syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED)))
		})

		It("refuses a PATCH and leaves the row as it was", func() {
			expectDenied(push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed"))[0])

			t, found := stored("t1")
			Expect(found).To(BeTrue())
			Expect(t.GetCompleted()).To(BeFalse())
		})

		It("refuses a PATCH that would make the row readable, so its other fields never reach the caller", func() {
			expectDenied(push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Title: "public now"}, "title"))[0])

			Expect(pullTodos("c2")).NotTo(HaveKey("t1"))
		})

		It("refuses a PUT over the row", func() {
			expectDenied(push(putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "mine now"}))[0])

			t, _ := stored("t1")
			Expect(t.GetTitle()).To(Equal("secret plan"))
			Expect(t.GetSortKey()).To(Equal("confidential"))
		})

		It("refuses a DELETE", func() {
			expectDenied(push(deleteMutation("c2", 1, "todo", "t1"))[0])

			_, found := stored("t1")
			Expect(found).To(BeTrue())
		})

		It("refuses a guarded write without disclosing the row's version", func() {
			m := patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed")
			wrong := uint64(999)
			m.GetCrud().ExpectedVersion = &wrong

			r := push(m)[0]
			expectDenied(r)
			Expect(r.GetErrorMessage()).NotTo(ContainSubstring("version"))
		})

		It("rolls back a whole batch when one of its writes targets a hidden row", func() {
			expectDenied(push(batchMutation("c2", 1,
				crudPatch("todo", "t2", &todov1.Todo{Completed: true}, "completed"),
				crudPatch("todo", "t1", &todov1.Todo{Completed: true}, "completed"),
			))[0])

			t, _ := stored("t2")
			Expect(t.GetCompleted()).To(BeFalse(), "the allowed write rolls back with the refused one")
		})

		It("applies writes to rows the caller may read", func() {
			r := push(
				patchMutation("c2", 1, "todo", "t2", &todov1.Todo{Completed: true}, "completed"),
				putMutation("c2", 2, "todo", "t3", &todov1.Todo{Id: "t3", Title: "new"}),
			)
			Expect(r).To(HaveEach(HaveField("Status", syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED)))
		})

		It("applies a write to a key whose row is deleted", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&todov1.Todo{}, "t1")
			})).To(Succeed())

			r := push(putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "fresh"}))[0]
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})

		It("lets a Trusted write change the row", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&todov1.Todo{Id: "t1", Completed: true}, []string{"completed"}, nil)
			})).To(Succeed())

			t, _ := stored("t1")
			Expect(t.GetCompleted()).To(BeTrue())
		})

		It("refuses the write through Apply", func() {
			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&todov1.Todo{Id: "t1", Completed: true}, []string{"completed"}, nil)
			})
			expectRejection(err, server.CodePermissionDenied)

			t, _ := stored("t1")
			Expect(t.GetCompleted()).To(BeFalse())
		})

		It("refuses the write through Mutate", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Delete(&todov1.Todo{}, "t1")
			})
			expectRejection(err, server.CodePermissionDenied)

			_, found := stored("t1")
			Expect(found).To(BeTrue())
		})

		It("refuses a Put through Mutate", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&todov1.Todo{Id: "t1", Title: "mine now"})
			})
			expectRejection(err, server.CodePermissionDenied)

			t, _ := stored("t1")
			Expect(t.GetTitle()).To(Equal("secret plan"))
		})
	})

	Describe("with a WriteFilter", func() {
		var policy *ownerPolicy

		BeforeEach(func() {
			policy = &ownerPolicy{}
			start(newTestServer(append(exampleapp.MutatorOptions(nil), server.WithPermissions(policy))...))
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				for _, t := range []*todov1.Todo{
					{Id: "t1", Title: "secret plan"},
					{Id: "t2", Title: "alice: groceries"},
				} {
					if err := tx.Put(t); err != nil {
						return err
					}
				}
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "secret"})
			})).To(Succeed())
			policy.reset()
		})

		It("decides in place of the ReadFilter", func() {
			r := push(patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed"))[0]
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
				"the write filter admits a row the read filter hides")
		})

		It("refuses what the filter refuses, with the filter's verdict", func() {
			r := push(deleteMutation("c1", 1, "todo", "t2"))[0]
			expectDenied(r)
			Expect(r.GetErrorMessage()).To(Equal("only the owner may change this todo"))

			bob := server.WithPrincipal(ctx, server.Principal{Subject: "bob"})
			expectRejection(srv.Apply(bob, ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&todov1.Todo{Id: "t2", Completed: true}, []string{"completed"}, nil)
			}), server.CodePermissionDenied)

			alice := server.WithPrincipal(ctx, server.Principal{Subject: "alice"})
			Expect(srv.Apply(alice, ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&todov1.Todo{Id: "t2", Completed: true}, []string{"completed"}, nil)
			})).To(Succeed())
			t, _ := stored("t2")
			Expect(t.GetCompleted()).To(BeTrue())
		})

		It("falls back to the ReadFilter for an entity it returns nil for", func() {
			expectDenied(push(patchMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Count: 1}, "count"))[0])
		})

		It("hands the filter the write as the client sent it", func() {
			push(patchMutation("c7", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed"))

			seen := policy.writesSeen()
			Expect(seen).To(HaveLen(1))
			Expect(seen[0].ClientId).To(Equal("c7"))
			Expect(seen[0].MutationId).To(BeEquivalentTo(1))
			Expect(seen[0].Custom).To(BeNil())
			Expect(seen[0].Crud.GetOp()).To(Equal(syncplanev1.CrudOp_CRUD_OP_PATCH))
			Expect(seen[0].Crud.GetKey()).To(Equal("t1"))
			Expect(seen[0].Crud.GetFieldMask().GetPaths()).To(Equal([]string{"completed"}))
		})

		It("is not consulted for a create", func() {
			r := push(putMutation("c1", 1, "todo", "t3", &todov1.Todo{Id: "t3", Title: "alice: new"}))[0]
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(policy.writesSeen()).To(BeEmpty())
		})

		It("is not consulted for the rows a mutator body writes", func() {
			r := push(customMutation("c1", 1, "complete_all", &todov1.CompleteAllArgs{}))[0]
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			t, _ := stored("t2")
			Expect(t.GetCompleted()).To(BeTrue(), "the mutator wrote alice's todo for an anonymous caller")
			Expect(policy.writesSeen()).To(BeEmpty())
		})

		It("aborts the batch when the hook cannot decide", func() {
			policy.hookErr = errors.New("membership store down")

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed"),
			})))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeInternal))

			t, _ := stored("t1")
			Expect(t.GetCompleted()).To(BeFalse())
		})
	})

	It("resolves the WriteFilter once per entity per push, even when the write closure retries", func() {
		policy := &ownerPolicy{}
		start(mustNewServer(&retryingAdapter{Adapter: newTestStore()}, testEntities(), server.WithPermissions(policy)))
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			if err := tx.Put(&todov1.Todo{Id: "t1", Title: "one"}); err != nil {
				return err
			}
			return tx.Put(&todov1.Todo{Id: "t2", Title: "two"})
		})).To(Succeed())

		r := push(
			patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed"),
			patchMutation("c1", 2, "todo", "t2", &todov1.Todo{Completed: true}, "completed"),
		)
		Expect(r).To(HaveEach(HaveField("Status", syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED)))
		Expect(policy.resolvedCount()).To(Equal(1))
	})
})
