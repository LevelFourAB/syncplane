package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

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

		It("does not disclose a client's watermark in a gap reply", func() {
			for id := uint64(1); id <= 3; id++ {
				Expect(statuses(anonymous, putMutation("victim", id, "todo", fmt.Sprintf("v%d", id), &todov1.Todo{Id: fmt.Sprintf("v%d", id)}))).
					To(Equal([]syncplanev1.MutationStatus{applied}))
			}

			res := pushAs(anonymous, putMutation("victim", 1_000_000, "todo", "junk", &todov1.Todo{Id: "junk"}))
			Expect(res.GetErrorCode()).To(Equal(server.CodeMutationIDGap))
			Expect(res.GetErrorMessage()).NotTo(ContainSubstring("want 4"),
				"the expected id tells any caller which ids to use up next")
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

	Describe("a batch that says what the client sent", func() {
		// pushSent pushes muts as anonymous with sent_through set.
		pushSent := func(sent uint64, muts ...*syncplanev1.Mutation) *syncplanev1.WorkspaceResults {
			GinkgoHelper()
			req := connect.NewRequest(&syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{
				WorkspaceId: ws, Mutations: muts, SentThrough: proto.Uint64(sent),
			}}})
			resp, err := client.Push(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()).To(HaveLen(1))
			return resp.Msg.GetResults()[0]
		}
		todo := func(client string, id uint64, key string) *syncplanev1.Mutation {
			return putMutation(client, id, "todo", key, &todov1.Todo{Id: key, Title: key})
		}

		It("applies a first mutation", func() {
			res := pushSent(0, todo("c1", 1, "t1"))
			Expect(res.GetErrorCode()).To(BeEmpty(), res.GetErrorMessage())
			Expect(title("t1")).To(Equal("t1"))
		})

		It("answers a resend of an applied mutation as ALREADY_APPLIED", func() {
			Expect(pushSent(0, todo("c1", 1, "t1")).GetErrorCode()).To(BeEmpty())
			res := pushSent(1, todo("c1", 1, "t1"), todo("c1", 2, "t2"))
			Expect(res.GetErrorCode()).To(BeEmpty(), res.GetErrorMessage())
			Expect(res.GetResults()[0].GetStatus()).To(Equal(already))
			Expect(res.GetResults()[1].GetStatus()).To(Equal(applied))
		})

		It("refuses the batch when another writer used the client's next ids", func() {
			for id := uint64(1); id <= 3; id++ {
				Expect(pushSent(id-1, todo("victim", id, fmt.Sprintf("v%d", id))).GetErrorCode()).To(BeEmpty())
			}
			// Another anonymous caller pushes the victim's next ten ids.
			var burn []*syncplanev1.Mutation
			for id := uint64(4); id <= 13; id++ {
				burn = append(burn, todo("victim", id, fmt.Sprintf("junk%d", id)))
			}
			Expect(statuses(anonymous, burn...)).To(HaveEach(applied))

			res := pushSent(3, todo("victim", 4, "important"))
			Expect(res.GetErrorCode()).To(Equal(server.CodeClientIDInUse))
			Expect(res.GetResults()).To(BeEmpty())
			Expect(res.GetErrorMessage()).NotTo(ContainSubstring("13"),
				"the reply must not tell the caller the watermark")
			Expect(title("important")).To(BeEmpty())
		})

		It("refuses the batch rather than replay a verdict another writer's mutation got", func() {
			Expect(pushSent(0, todo("c1", 1, "t1")).GetErrorCode()).To(BeEmpty())
			// The other writer's id 2 is rejected: a PATCH of a missing row.
			other := patchMutation("c1", 2, "todo", "missing", &todov1.Todo{Title: "x"}, "title")
			Expect(statuses(anonymous, other)).To(Equal([]syncplanev1.MutationStatus{rejected}))

			res := pushSent(1, todo("c1", 2, "mine"))
			Expect(res.GetErrorCode()).To(Equal(server.CodeClientIDInUse))
			Expect(title("mine")).To(BeEmpty())
		})

		It("refuses a batch that mixes client ids", func() {
			res := pushSent(0, todo("c1", 1, "t1"), todo("c2", 1, "t2"))
			Expect(res.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			Expect(title("t1")).To(BeEmpty())
		})

		It("keeps judging a batch that does not say by the watermark alone", func() {
			Expect(pushSent(0, todo("c1", 1, "t1")).GetErrorCode()).To(BeEmpty())
			Expect(statuses(anonymous, todo("c1", 2, "t2"))).To(Equal([]syncplanev1.MutationStatus{applied}))
			Expect(statuses(anonymous, todo("c1", 2, "t2"))).To(Equal([]syncplanev1.MutationStatus{already}))
		})

		It("judges each principal by its own watermark", func() {
			Expect(statuses(mallory, todo("c-alice", 1, "m1"))).To(Equal([]syncplanev1.MutationStatus{applied}))
			req := connect.NewRequest(&syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{
				WorkspaceId: ws, Mutations: []*syncplanev1.Mutation{todo("c-alice", 1, "a1")}, SentThrough: proto.Uint64(0),
			}}})
			as(req.Header(), alice)
			resp, err := client.Push(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(BeEmpty())
			Expect(title("a1")).To(Equal("a1"))
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

		// The client_id is a storage key on every store: Postgres fails a key
		// longer than its b-tree row limit as an internal error the client
		// would retry.
		Describe("length", func() {
			longest := strings.Repeat("c", 128)
			tooLong := strings.Repeat("c", 129)

			It("refuses a push whose client_id is longer than 128 bytes", func() {
				res := pushAs(anonymous, putMutation(tooLong, 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
				Expect(res.GetErrorCode()).To(Equal("invalid_argument"))
				Expect(res.GetErrorMessage()).To(ContainSubstring("129 bytes"))
				Expect(res.GetResults()).To(BeEmpty())
			})

			It("applies a push whose client_id is 128 bytes", func() {
				res := pushAs(anonymous, putMutation(longest, 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
				Expect(res.GetErrorCode()).To(BeEmpty(), "message: %s", res.GetErrorMessage())
				Expect(res.GetResults()[0].GetStatus()).To(Equal(applied))
			})

			It("refuses a pull whose client_id is longer than 128 bytes", func() {
				_, err := pullAs(anonymous, tooLong)
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			})

			It("refuses a Bootstrap whose client_id is longer than 128 bytes", func() {
				stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
					WorkspaceId: ws, ClientId: tooLong,
				}))
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { _ = stream.Close() })
				Expect(stream.Receive()).To(BeFalse())
				Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
			})

			It("refuses a Subscribe stream whose client_id is longer than 128 bytes", func() {
				stream, err := client.Subscribe(ctx, connect.NewRequest(&syncplanev1.SubscribeRequest{
					ClientId: tooLong,
					Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
				}))
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() { _ = stream.Close() })
				Expect(stream.Receive()).To(BeFalse())
				Expect(connect.CodeOf(stream.Err())).To(Equal(connect.CodeInvalidArgument))
			})
		})
	})
})
