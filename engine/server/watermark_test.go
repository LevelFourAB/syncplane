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
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Client watermarks", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	// The caller is whoever the test headers name; no header is anonymous.
	authn := server.AuthenticatorFunc(func(_ context.Context, h http.Header) (server.Principal, error) {
		return server.Principal{Subject: h.Get("X-Test-Subject"), Issuer: h.Get("X-Test-Issuer")}, nil
	})

	alice := server.Principal{Subject: "alice"}
	mallory := server.Principal{Subject: "mallory"}
	anonymous := server.Principal{}

	BeforeEach(func() {
		ctx = context.Background()
		srv = newTestServer(server.WithAuthenticator(authn))
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	as := func(h http.Header, p server.Principal) {
		if p.Subject != "" {
			h.Set("X-Test-Subject", p.Subject)
		}
		if p.Issuer != "" {
			h.Set("X-Test-Issuer", p.Issuer)
		}
	}

	pushAs := func(p server.Principal, muts ...*syncplanev1.Mutation) *syncplanev1.WorkspaceResults {
		GinkgoHelper()
		req := connect.NewRequest(pushOf(ws, muts))
		as(req.Header(), p)
		resp, err := client.Push(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetResults()).To(HaveLen(1))
		return resp.Msg.GetResults()[0]
	}

	statuses := func(p server.Principal, muts ...*syncplanev1.Mutation) []syncplanev1.MutationStatus {
		GinkgoHelper()
		res := pushAs(p, muts...)
		Expect(res.GetErrorCode()).To(BeEmpty(), "batch failed: %s", res.GetErrorMessage())
		out := make([]syncplanev1.MutationStatus, len(res.GetResults()))
		for i, r := range res.GetResults() {
			out[i] = r.GetStatus()
		}
		return out
	}

	pullAs := func(p server.Principal, clientID string) (*syncplanev1.PullResponse, error) {
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: clientID})
		as(req.Header(), p)
		resp, err := client.Pull(ctx, req)
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}

	title := func(key string) string {
		GinkgoHelper()
		resp, err := pullAs(anonymous, "")
		Expect(err).NotTo(HaveOccurred())
		var found string
		for _, rc := range resp.GetPatch().GetChanges() {
			if rc.GetEntity() == "todo" && rc.GetKey() == key && !rc.GetTombstone() {
				var t todov1.Todo
				Expect(proto.Unmarshal(rc.GetValue(), &t)).To(Succeed())
				found = t.GetTitle()
			}
		}
		return found
	}

	const (
		applied  = syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED
		rejected = syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED
		already  = syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED
	)

	Describe("across principals", func() {
		It("applies a client's next mutation after another principal pushed under its client id", func() {
			Expect(statuses(alice, putMutation("c-alice", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))
			Expect(statuses(mallory, putMutation("c-alice", 2, "todo", "m1", &todov1.Todo{Id: "m1", Title: "mallory"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))

			Expect(statuses(alice, patchMutation("c-alice", 2, "todo", "t1", &todov1.Todo{Title: "v2"}, "title"))).
				To(Equal([]syncplanev1.MutationStatus{applied}))
			Expect(title("t1")).To(Equal("v2"))
		})

		It("applies a client's first mutations after another principal opened its client id at a high id", func() {
			Expect(statuses(mallory, putMutation("c-alice", 1<<40, "todo", "m1", &todov1.Todo{Id: "m1", Title: "mallory"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))

			Expect(statuses(alice,
				putMutation("c-alice", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}),
				patchMutation("c-alice", 2, "todo", "t1", &todov1.Todo{Title: "v2"}, "title"),
			)).To(Equal([]syncplanev1.MutationStatus{applied, applied}))
			Expect(title("t1")).To(Equal("v2"))
		})

		It("still answers a principal's own resend as ALREADY_APPLIED", func() {
			put := putMutation("c-alice", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"})
			Expect(statuses(alice, put)).To(Equal([]syncplanev1.MutationStatus{applied}))
			Expect(statuses(mallory, putMutation("c-alice", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "mallory"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))

			Expect(statuses(alice, put)).To(Equal([]syncplanev1.MutationStatus{already}))
			Expect(title("t1")).To(Equal("mallory"), "the resend must not apply again")
		})

		It("keeps anonymous callers on one shared watermark per client id", func() {
			Expect(statuses(anonymous, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))
			Expect(statuses(anonymous, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))).
				To(Equal([]syncplanev1.MutationStatus{already}))
		})

		It("treats one subject from two issuers as two callers", func() {
			fromA := server.Principal{Subject: "alice", Issuer: "https://a.example"}
			fromB := server.Principal{Subject: "alice", Issuer: "https://b.example"}
			Expect(statuses(fromA, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "a"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))

			Expect(statuses(fromB, putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "b"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))
		})

		It("reports each principal the watermark of its own pushes on pull", func() {
			Expect(statuses(alice,
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "v1"}),
			)).To(HaveLen(2))
			Expect(statuses(mallory, putMutation("c1", 1, "todo", "m1", &todov1.Todo{Id: "m1", Title: "m"}))).To(HaveLen(1))

			for p, want := range map[string]uint64{"alice": 2, "mallory": 1, "": 0} {
				resp, err := pullAs(server.Principal{Subject: p}, "c1")
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetLastAckedMutationId()).To(Equal(want), "watermark reported to %q", p)
			}
		})

		It("reports each principal the watermark of its own pushes on a Subscribe stream", func() {
			Expect(statuses(alice,
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "v1"}),
			)).To(HaveLen(2))
			Expect(statuses(mallory, putMutation("c1", 1, "todo", "m1", &todov1.Todo{Id: "m1", Title: "m"}))).To(HaveLen(1))

			req := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c1",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
			})
			as(req.Header(), mallory)
			streamCtx, cancel := context.WithCancel(ctx)
			DeferCleanup(cancel)
			stream, err := client.Subscribe(streamCtx, req)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = stream.Close() })
			Expect(stream.Receive()).To(BeTrue(), "stream error: %v", stream.Err())
			page := stream.Msg().GetPage()
			Expect(page).NotTo(BeNil(), "the first message must be the catch-up page")
			Expect(page.GetLastAckedMutationId()).To(Equal(uint64(1)))
		})

		It("reports each principal the watermark of its own pushes on Bootstrap", func() {
			Expect(statuses(alice,
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "v1"}),
			)).To(HaveLen(2))

			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"})
			as(req.Header(), mallory)
			stream, err := client.Bootstrap(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = stream.Close() })
			Expect(stream.Receive()).To(BeTrue(), "stream error: %v", stream.Err())
			Expect(stream.Msg().GetBegin()).NotTo(BeNil())
			Expect(stream.Msg().GetBegin().GetLastAckedMutationId()).To(BeZero())
		})

		It("replays a recorded rejection only to the principal it was given to", func() {
			// A PATCH of a missing row is refused as a conflict, and the verdict
			// is recorded against the client.
			Expect(statuses(alice, patchMutation("c1", 1, "todo", "missing", &todov1.Todo{Title: "x"}, "title"))).
				To(Equal([]syncplanev1.MutationStatus{rejected}))

			Expect(statuses(mallory, putMutation("c1", 1, "todo", "m1", &todov1.Todo{Id: "m1", Title: "m"}))).
				To(Equal([]syncplanev1.MutationStatus{applied}))
		})
	})

	Describe("client_id validation", func() {
		It("refuses a push whose client_id holds a control character", func() {
			res := pushAs(anonymous, putMutation("c\u001f1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			Expect(res.GetErrorCode()).To(Equal("invalid_argument"))
			Expect(res.GetResults()).To(BeEmpty())
		})

		It("refuses a pull whose client_id holds a control character", func() {
			_, err := pullAs(anonymous, "c\u00001")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses a Bootstrap whose client_id holds a control character", func() {
			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws, ClientId: "c\n1",
			}))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = stream.Close() })
			Expect(stream.Receive()).To(BeFalse())
			Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses a Subscribe stream whose client_id holds a control character", func() {
			stream, err := client.Subscribe(ctx, connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c\t1",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
			}))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = stream.Close() })
			Expect(stream.Receive()).To(BeFalse())
			Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
		})
	})
})
