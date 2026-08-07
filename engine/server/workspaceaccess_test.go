package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// gatePerms answers the workspace gate from a per-workspace verdict function
// and records whether any LATER hook ran, so a spec can prove the gate fired
// first rather than merely fired.
type gatePerms struct {
	server.AllowAll
	verdict   func(p server.Principal, ws string) error
	laterHook atomic.Bool
}

func (g *gatePerms) CanAccessWorkspace(_ context.Context, p server.Principal, ws string) error {
	if g.verdict == nil {
		return nil
	}
	return g.verdict(p, ws)
}

func (g *gatePerms) CanMutate(context.Context, server.Principal, string, server.Write) error {
	g.laterHook.Store(true)
	return nil
}

func (g *gatePerms) ReadFilter(context.Context, server.Principal, string, string) (server.RowFilter, error) {
	g.laterHook.Store(true)
	return nil, nil
}

func (g *gatePerms) FilterEpoch(context.Context, server.Principal, string) (uint64, error) {
	g.laterHook.Store(true)
	return 0, nil
}

// midStreamGate is a workspace gate whose verdict a spec can swap while a
// stream is running — the membership change a live re-check has to notice. The
// atomic is what makes flipping it from the spec goroutine safe while the
// server's stream goroutine reads it.
type midStreamGate struct {
	server.AllowAll
	verdict atomic.Value // func(ws string) error
}

func (g *midStreamGate) set(f func(ws string) error) { g.verdict.Store(f) }

// deny settles a permanent "no" for one workspace; fail makes the same
// workspace undecidable; denyAll revokes every workspace at once.
func (g *midStreamGate) deny(ws, message string) {
	g.set(func(w string) error {
		if w == ws {
			return server.DenyWorkspace(message)
		}
		return nil
	})
}

func (g *midStreamGate) fail(ws string, cause error) {
	g.set(func(w string) error {
		if w == ws {
			return cause
		}
		return nil
	})
}

func (g *midStreamGate) denyAll(message string) {
	g.set(func(string) error { return server.DenyWorkspace(message) })
}

func (g *midStreamGate) CanAccessWorkspace(_ context.Context, _ server.Principal, ws string) error {
	if f, ok := g.verdict.Load().(func(string) error); ok {
		return f(ws)
	}
	return nil
}

var _ = Describe("Workspace-access authorization", func() {
	const (
		mine    = "w-mine"
		theirs  = "w-theirs"
		myToken = "alice-token"
	)

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
		perms  *gatePerms
		srv    *server.Server
	)

	start := func(s *server.Server) {
		path, handler := s.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		h := http.Header{}
		h.Set("Authorization", "Bearer "+myToken)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL,
			connect.WithInterceptors(headerInterceptor(h)))
	}

	// Every workspace-scoped RPC, as a name plus a call that returns the
	// request's error. Streams are opened and drained so the handler's error
	// surfaces rather than the stream-open result.
	//
	// streaming marks the RPCs whose happy path never completes (Subscribe
	// stays open until the ctx ends), so specs that assert success know to
	// skip them.
	//
	// perWorkspace marks the RPCs that carry a SET of workspaces — Push and
	// Subscribe. There a refusal is data about one workspace (a code on
	// WorkspaceResults, a WorkspaceDropped frame), not a failure of the
	// request: reporting it at the transport would deny the client every other
	// workspace in the same call. `outcome` normalizes both shapes to the
	// machine-readable code, so the specs below assert the same thing of every
	// RPC regardless of where the verdict rode.
	type rpc struct {
		name         string
		streaming    bool
		perWorkspace bool
		call         func(ws string) error
		outcome      func(ws string) string
	}

	// codeOfErr extracts the machine-readable code an RPC error carries.
	codeOfErr := func(err error) string {
		if err == nil {
			return ""
		}
		var ce *connect.Error
		if errors.As(err, &ce) {
			if code := ce.Meta().Get(server.ErrorCodeMetadataKey); code != "" {
				return code
			}
			// An untyped failure. Report the transport code so a spec can tell
			// "internal" from "denied" without a second accessor.
			if ce.Code() == connect.CodeInternal {
				return server.CodeInternal
			}
			return ce.Code().String()
		}
		return err.Error()
	}

	// streamCtx bounds a drained stream. A gated stream errors immediately;
	// without the gate the drain would otherwise block forever, so a regression
	// has to surface as a fast DeadlineExceeded rather than a hung suite.
	streamCtx := func() context.Context {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		DeferCleanup(cancel)
		return c
	}

	// pushOnce sends a one-mutation push and returns that workspace's results.
	pushOnce := func(ws string) (*syncplanev1.WorkspaceResults, error) {
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			[]*syncplanev1.Mutation{
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hi"}),
			})))
		if err != nil {
			return nil, err
		}
		if len(resp.Msg.GetResults()) != 1 {
			return nil, errors.New("expected exactly one workspace result")
		}
		return resp.Msg.GetResults()[0], nil
	}

	// subscribeOnce opens a one-scope stream and returns the first dropped
	// frame, or nil if the scope was served.
	subscribeOnce := func(ws string) (*syncplanev1.WorkspaceDropped, error) {
		stream, err := client.Subscribe(streamCtx(), connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
		}))
		if err != nil {
			return nil, err
		}
		defer func() { _ = stream.Close() }()
		for stream.Receive() {
			if d := stream.Msg().GetDropped(); d != nil {
				return d, nil
			}
			if stream.Msg().GetCaughtUp() {
				return nil, nil // served
			}
		}
		return nil, stream.Err()
	}

	rpcs := func() []rpc {
		return []rpc{
			{
				name:         "Push",
				perWorkspace: true,
				call: func(ws string) error {
					_, err := pushOnce(ws)
					return err
				},
				outcome: func(ws string) string {
					res, err := pushOnce(ws)
					if err != nil {
						return codeOfErr(err)
					}
					return res.GetErrorCode()
				},
			},
			{
				name: "Pull",
				call: func(ws string) error {
					_, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
						WorkspaceId: ws, ClientId: "c1",
					}))
					return err
				},
			},
			{
				name: "Query",
				call: func(ws string) error {
					_, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
						WorkspaceId: ws, Entity: "todo",
					}))
					return err
				},
			},
			{
				name: "Bootstrap",
				call: func(ws string) error {
					stream, err := client.Bootstrap(streamCtx(), connect.NewRequest(&syncplanev1.BootstrapRequest{
						WorkspaceId: ws, ClientId: "c1",
					}))
					if err != nil {
						return err
					}
					defer func() { _ = stream.Close() }()
					for stream.Receive() {
					}
					return stream.Err()
				},
			},
			{
				name:         "Subscribe",
				streaming:    true,
				perWorkspace: true,
				call: func(ws string) error {
					_, err := subscribeOnce(ws)
					return err
				},
				outcome: func(ws string) string {
					dropped, err := subscribeOnce(ws)
					if err != nil {
						return codeOfErr(err)
					}
					switch dropped.GetReason() {
					case syncplanev1.WorkspaceDropped_REASON_ACCESS_REVOKED:
						return server.CodeWorkspaceAccessRevoked
					case syncplanev1.WorkspaceDropped_REASON_ERROR:
						return server.CodeInternal
					case syncplanev1.WorkspaceDropped_REASON_RESET:
						return "reset"
					default:
						return ""
					}
				},
			},
		}
	}

	// outcomeOf normalizes: RPCs that deny at the transport report through
	// their error, the workspace-set RPCs through their body.
	outcomeOf := func(r rpc, ws string) string {
		if r.outcome != nil {
			return r.outcome(ws)
		}
		return codeOfErr(r.call(ws))
	}

	BeforeEach(func() {
		ctx = context.Background()
		perms = &gatePerms{}
	})

	Describe("a workspace the caller is not a member of", func() {
		BeforeEach(func() {
			perms.verdict = func(_ server.Principal, ws string) error {
				if ws != mine {
					return server.DenyWorkspace("not a member of this workspace")
				}
				return nil
			}
			srv = newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(server.StaticTokenAuthenticator(
					map[string]server.Principal{myToken: {Subject: "alice"}})),
			)
			start(srv)
		})

		It("refuses every workspace-scoped RPC, tagged as permanent", func() {
			// One assertion for both shapes: whether the verdict rides on the
			// transport or in the body, the client must be able to read
			// "permanently revoked" and stop retrying.
			for _, r := range rpcs() {
				Expect(outcomeOf(r, theirs)).To(Equal(server.CodeWorkspaceAccessRevoked),
					"%s denial code", r.name)
			}
		})

		It("denies the single-workspace RPCs at the transport", func() {
			for _, r := range rpcs() {
				if r.perWorkspace {
					continue
				}
				err := r.call(theirs)
				Expect(err).To(HaveOccurred(), "%s must be gated", r.name)
				Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied),
					"%s denial code", r.name)
			}
		})

		It("keeps a workspace-set RPC's request succeeding, denying only that workspace", func() {
			// The reason Push and Subscribe report per workspace: a client
			// holding a revoked workspace alongside good ones must still be
			// served the good ones. Here the request itself succeeds while the
			// refused workspace carries the denial.
			resp, err := client.Push(ctx, connect.NewRequest(&syncplanev1.PushRequest{
				Batches: []*syncplanev1.WorkspaceBatch{
					{WorkspaceId: theirs, Mutations: []*syncplanev1.Mutation{
						putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "denied"}),
					}},
					{WorkspaceId: mine, Mutations: []*syncplanev1.Mutation{
						putMutation("c1", 1, "todo", "t2", &todov1.Todo{Id: "t2", Title: "allowed"}),
					}},
				},
			}))
			Expect(err).NotTo(HaveOccurred(), "one revoked workspace must not fail the request")
			Expect(resp.Msg.GetResults()).To(HaveLen(2))
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeWorkspaceAccessRevoked))
			Expect(resp.Msg.GetResults()[0].GetResults()).To(BeEmpty())
			Expect(resp.Msg.GetResults()[1].GetErrorCode()).To(BeEmpty())
			Expect(resp.Msg.GetResults()[1].GetResults()).To(HaveLen(1))
			Expect(resp.Msg.GetResults()[1].GetResults()[0].GetStatus()).
				To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})

		It("still serves the caller's own workspace", func() {
			for _, r := range rpcs() {
				if r.streaming {
					continue // no terminating happy path to assert on
				}
				Expect(r.call(mine)).To(Succeed(), "%s on an allowed workspace", r.name)
			}
		})

		It("runs before any other permission hook", func() {
			for _, r := range rpcs() {
				perms.laterHook.Store(false)
				_ = r.call(theirs)
				Expect(perms.laterHook.Load()).To(BeFalse(),
					"%s: CanMutate/ReadFilter/FilterEpoch must not run for a denied workspace", r.name)
			}
		})

		It("leaves no trace in the workspace it refused", func() {
			// The leak this guards is not row data but existence: a watermark
			// row or a lazily-created workspace would confirm the workspace to
			// a caller who was refused, and would survive the request.
			for _, r := range rpcs() {
				_ = r.call(theirs)
			}
			ids, err := srv.WorkspaceIDs(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(ids).NotTo(ContainElement(theirs))
		})

		It("writes nothing on a denied push", func() {
			res, err := pushOnce(theirs)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetErrorCode()).To(Equal(server.CodeWorkspaceAccessRevoked))

			// Read the denied workspace back through the trusted server-side
			// path, which the gate does not cover, so this sees what is really
			// stored rather than what the gate would let a client see.
			perms.verdict = nil
			resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: theirs, ClientId: "auditor",
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetPatch().GetChanges()).To(BeEmpty())
		})
	})

	Describe("a gate that cannot reach its membership store", func() {
		BeforeEach(func() {
			perms.verdict = func(server.Principal, string) error {
				return errors.New("membership service unreachable")
			}
			start(newTestServer(server.WithPermissions(perms)))
		})

		It("fails as internal, never as a denial", func() {
			// A client must not read an outage as revocation: the permanent
			// denial is a cue to discard local data, and an outage is not.
			for _, r := range rpcs() {
				Expect(outcomeOf(r, theirs)).To(Equal(server.CodeInternal),
					"%s on a failed lookup", r.name)
			}
			// And at the transport, for the RPCs that deny there.
			for _, r := range rpcs() {
				if r.perWorkspace {
					continue
				}
				err := r.call(theirs)
				Expect(err).To(HaveOccurred())
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeInternal),
					"%s on a failed lookup", r.name)

				var ce *connect.Error
				Expect(errors.As(err, &ce)).To(BeTrue())
				Expect(ce.Meta().Get(server.ErrorCodeMetadataKey)).To(BeEmpty(),
					"%s must not carry the revocation code", r.name)
			}
		})

		It("keeps the gate's own wording off the wire", func() {
			// A gate words its errors for an operator and may name the host it
			// could not reach. The caller gets the code, which is all it can act
			// on; the wording goes to the log.
			res, err := pushOnce(theirs)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetErrorCode()).To(Equal(server.CodeInternal))
			Expect(res.GetErrorMessage()).NotTo(ContainSubstring("membership service unreachable"))
		})
	})

	Describe("a gate that picks its own transport code", func() {
		BeforeEach(func() {
			perms.verdict = func(server.Principal, string) error {
				return connect.NewError(connect.CodeUnauthenticated,
					errors.New("session expired"))
			}
			start(newTestServer(server.WithPermissions(perms)))
		})

		It("passes the chosen code through", func() {
			err := rpcs()[1].call(theirs)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnauthenticated))
		})
	})

	Describe("the default policy", func() {
		BeforeEach(func() {
			start(newTestServer())
		})

		It("admits every workspace, since AllowAll inherits an open gate", func() {
			// Pinning the documented default: single-tenant deployments keep
			// working untouched, and a multi-tenant one MUST override the hook.
			for _, r := range rpcs() {
				if r.streaming {
					continue // no terminating happy path to assert on
				}
				Expect(r.call(theirs)).To(Succeed(), "%s under AllowAll", r.name)
			}
		})
	})

	Describe("the principal reaching the gate", func() {
		It("is the authenticated caller", func() {
			var seen []server.Principal
			perms.verdict = func(p server.Principal, _ string) error {
				seen = append(seen, p)
				return nil
			}
			start(newTestServer(
				server.WithPermissions(perms),
				server.WithAuthenticator(server.StaticTokenAuthenticator(
					map[string]server.Principal{myToken: {Subject: "alice", Issuer: "test"}})),
			))

			Expect(rpcs()[1].call(mine)).To(Succeed())
			Expect(seen).To(HaveLen(1))
			Expect(seen[0].Subject).To(Equal("alice"))
			Expect(seen[0].Issuer).To(Equal("test"))
		})

		It("is anonymous when no authenticator is installed", func() {
			var seen []server.Principal
			perms.verdict = func(p server.Principal, _ string) error {
				seen = append(seen, p)
				return nil
			}
			start(newTestServer(server.WithPermissions(perms)))

			Expect(rpcs()[1].call(mine)).To(Succeed())
			Expect(seen).To(HaveLen(1))
			Expect(seen[0].IsAnonymous()).To(BeTrue())
		})
	})

	Describe("DenyWorkspace", func() {
		It("is recognisable through a wrapped error chain", func() {
			wrapped := errors.Join(errors.New("context"), server.DenyWorkspace("gone"))
			d, ok := server.AsWorkspaceDenial(wrapped)
			Expect(ok).To(BeTrue())
			Expect(d.Message).To(Equal("gone"))
		})

		It("is not confused with a mutation Rejection", func() {
			_, isRejection := server.AsRejection(server.DenyWorkspace("gone"))
			Expect(isRejection).To(BeFalse())

			_, isDenial := server.AsWorkspaceDenial(
				server.Reject(server.CodePermissionDenied, "nope"))
			Expect(isDenial).To(BeFalse())
		})
	})
})
