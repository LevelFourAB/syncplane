package server_test

import (
	"context"
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
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

var _ = Describe("Limits", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	start := func(srv *server.Server) {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	pull := func(cursor []byte, limit uint32) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Cursor:      cursor,
			Limit:       limit,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("savepoints around mutations", func() {
		BeforeEach(func() {
			start(newTestServer(exampleapp.MutatorOptions(nil)...))
		})

		It("rolls back rows a mutator wrote before rejecting, but still acks", func() {
			resp := push(customMutation("c1", 1, "write_then_reject", &todov1.Todo{Id: "w1", Title: "ghost"}))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeConflict))

			pulled := pull(nil, 0)
			Expect(pulled.GetPatch().GetChanges()).To(BeEmpty(), "rejected mutator writes must roll back")
			Expect(pulled.GetLastAckedMutationId()).To(BeEquivalentTo(1), "rejection still advances the watermark")

			// The watermark covers the id, so the mutator does not run again —
			// the retry is answered from the recorded verdict.
			retry := push(customMutation("c1", 1, "write_then_reject", &todov1.Todo{Id: "w1", Title: "ghost"}))
			Expect(retry[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(retry[0].GetErrorCode()).To(Equal(server.CodeConflict))
			Expect(pull(nil, 0).GetPatch().GetChanges()).To(BeEmpty())
		})

		It("does not let a rejected mutation consume a sequence", func() {
			srv := newTestServer(server.WithPermissions(exampleapp.NewPermissions()))
			start(srv)

			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "forbidden one"}))
			resp := push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(resp[0].GetSeq()).To(BeEquivalentTo(1), "the rejected mutation's seq rolled back")
		})

		It("applies the rest of a mixed batch around a rejection", func() {
			srv := newTestServer(server.WithPermissions(exampleapp.NewPermissions()))
			start(srv)

			resp := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "first"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "forbidden middle"}),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "third"}),
			)
			Expect(resp).To(HaveLen(3))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(resp[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(resp[2].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(resp[0].GetSeq()).To(BeEquivalentTo(1))
			Expect(resp[2].GetSeq()).To(BeEquivalentTo(2))

			pulled := pull(nil, 0)
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(2))
			Expect(pulled.GetLastAckedMutationId()).To(BeEquivalentTo(3))
		})
	})

	Describe("pull pagination", func() {
		It("pages through changes with has_more until drained", func() {
			start(newTestServer())
			for i, key := range []string{"t1", "t2", "t3"} {
				push(putMutation("c1", uint64(i+1), "todo", key, &todov1.Todo{Id: key}))
			}

			first := pull(nil, 2)
			Expect(first.GetHasMore()).To(BeTrue())
			Expect(first.GetPatch().GetChanges()).To(HaveLen(2))

			second := pull(first.GetNextCursor(), 2)
			Expect(second.GetHasMore()).To(BeFalse())
			Expect(second.GetPatch().GetChanges()).To(HaveLen(1))
			Expect(second.GetPatch().GetChanges()[0].GetKey()).To(Equal("t3"))

			third := pull(second.GetNextCursor(), 2)
			Expect(third.GetPatch().GetChanges()).To(BeEmpty())
		})

		It("advances the cursor past read-filtered rows (limit 1)", func() {
			start(newTestServer(server.WithPermissions(exampleapp.NewPermissions())))
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "visible one"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret stuff"}),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "visible two"}),
			)

			var titles []string
			var tombstoned []string
			cursor := []byte(nil)
			for i := 0; i < 10; i++ {
				resp := pull(cursor, 1)
				for _, rc := range resp.GetPatch().GetChanges() {
					if rc.GetTombstone() {
						tombstoned = append(tombstoned, rc.GetKey())
						continue
					}
					var todo todov1.Todo
					Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
					titles = append(titles, todo.GetTitle())
				}
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					break
				}
			}
			Expect(titles).To(Equal([]string{"visible one", "visible two"}),
				"the loop terminates and sees every visible row exactly once")
			Expect(tombstoned).To(Equal([]string{"t2"}),
				"read-filtered rows surface as tombstones so stale copies get dropped")
		})

		It("clamps the requested limit to MaxPullChanges", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxPullChanges: 2})))
			for i, key := range []string{"t1", "t2", "t3"} {
				push(putMutation("c1", uint64(i+1), "todo", key, &todov1.Todo{Id: key}))
			}
			resp := pull(nil, 1000)
			Expect(resp.GetPatch().GetChanges()).To(HaveLen(2))
			Expect(resp.GetHasMore()).To(BeTrue())
		})
	})

	Describe("request size cap", func() {
		// MaxRequestBytes is the HTTP-level wall in front of every finer limit:
		// an over-sized body is rejected as resource_exhausted before it is
		// buffered or unmarshaled, so unbounded inputs (here, a huge value, but
		// equally a giant cursor or subscription set) never reach validation.
		It("rejects an over-sized request body before parsing it", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxRequestBytes: 1024})))
			_, err := client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: strings.Repeat("x", 4096)}),
				})))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
		})

		It("admits a body within the cap", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxRequestBytes: 1024})))
			resp := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "small"}))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})
	})

	Describe("misconfigured limits", func() {
		// A negative limit is not a relaxed bound, it is an inverted one: the
		// storage contract reads a non-positive scan limit as unlimited, so a
		// negative MaxPullChanges used to make Pull scan the whole workspace,
		// emit nothing, and hand back a cursor regressed to sequence 0 with
		// has_more set — a loop the client cannot escape.
		It("falls back to the default rather than inverting a bound", func() {
			start(newTestServer(server.WithLimits(server.Limits{
				MaxPullChanges:      -1,
				MaxMutationsPerPush: -1,
			})))
			for i, key := range []string{"t1", "t2", "t3"} {
				push(putMutation("c1", uint64(i+1), "todo", key, &todov1.Todo{Id: key}))
			}

			pulled := pull(nil, 0)
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(3))
			Expect(pulled.GetHasMore()).To(BeFalse())
			Expect(pulled.GetNextCursor()).NotTo(BeEmpty())
		})
	})

	Describe("push limits", func() {
		It("fails oversized batches with an RPC error (the client must chunk)", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxMutationsPerPush: 2})))
			_, err := client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
					putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2"}),
					putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3"}),
				})))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			Expect(err).To(MatchError(ContainSubstring(
				"push of 3 mutations, over MaxMutationsPerPush (2); raise it with server.WithLimits")))
		})

		// Per-mutation limit violations are deterministic, so they REJECT (an
		// ack) rather than erroring — a single oversized mutation must not be
		// able to wedge the queue forever.
		It("rejects oversized values without aborting the batch", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxValueBytes: 32})))
			resp := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: strings.Repeat("x", 100)}))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))

			// The rejection is an ack: the watermark advanced, so a resend is
			// answered from the record rather than applied, and the queue drains.
			resend := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}))
			Expect(resend[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(resend[0].GetSeq()).To(BeZero())
		})

		It("rejects oversized field masks without aborting the batch", func() {
			start(newTestServer(server.WithLimits(server.Limits{MaxFieldMaskPaths: 1})))
			resp := push(patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}, "id", "title"))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})

		It("rejects oversized custom-mutator args without aborting the batch", func() {
			start(newTestServer(append(exampleapp.MutatorOptions(nil),
				server.WithLimits(server.Limits{MaxValueBytes: 16}))...))
			resp := push(customMutation("c1", 1, "write_then_reject", &todov1.Todo{Id: "w1", Title: strings.Repeat("x", 100)}))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})
	})

	// Deterministic validation failures must not abort the batch with an RPC
	// error — that wedges the client queue on the poison head forever. They
	// reject (an ack) so the queue self-heals.
	Describe("poison mutations reject instead of erroring", func() {
		BeforeEach(func() {
			start(newTestServer(exampleapp.MutatorOptions(nil)...))
		})

		rejectionOf := func(m *syncplanev1.Mutation) *syncplanev1.MutationResult {
			GinkgoHelper()
			result := push(m)[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			return result
		}

		It("rejects an unknown entity", func() {
			rejectionOf(putMutation("c1", 1, "nonesuch", "t1", &todov1.Todo{Id: "t1"}))
		})

		It("rejects an unknown mutator", func() {
			rejectionOf(customMutation("c1", 1, "no_such_mutator", &todov1.Todo{Id: "t1"}))
		})

		It("rejects a field-mask path the schema does not know (version skew)", func() {
			rejectionOf(patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}, "ghost_field"))
		})

		It("keeps the rest of a batch around a poison mutation and advances the watermark", func() {
			resp := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "first"}),
				putMutation("c1", 2, "nonesuch", "x", &todov1.Todo{Id: "x"}),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "third"}),
			)
			Expect(resp).To(HaveLen(3))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(resp[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(resp[2].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			pulled := pull(nil, 0)
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(2), "the poison mutation wrote nothing")
			Expect(pulled.GetLastAckedMutationId()).To(BeEquivalentTo(3), "the poison mutation was still acked")
		})
	})

	Describe("bootstrap chunking", func() {
		It("splits the snapshot into chunks of the configured size", func() {
			start(newTestServer(server.WithChunkSize(2)))
			for i, key := range []string{"t1", "t2", "t3", "t4", "t5"} {
				push(putMutation("c1", uint64(i+1), "todo", key, &todov1.Todo{Id: key}))
			}

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
				ClientId:    "c1",
			}))
			Expect(err).NotTo(HaveOccurred())

			var chunkSizes []int
			total := 0
			for stream.Receive() {
				if chunk := stream.Msg().GetChunk(); chunk != nil {
					chunkSizes = append(chunkSizes, len(chunk.GetRows()))
					total += len(chunk.GetRows())
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(total).To(Equal(5))
			Expect(len(chunkSizes)).To(BeNumerically(">=", 3))
			for _, size := range chunkSizes {
				Expect(size).To(BeNumerically("<=", 2))
			}
		})
	})
})
