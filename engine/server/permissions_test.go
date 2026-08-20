package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

var _ = Describe("Permissions and custom mutations", func() {
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

	pull := func() *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	titles := func(changes []*syncplanev1.RowChange) []string {
		var out []string
		for _, rc := range changes {
			if rc.GetTombstone() {
				continue
			}
			var todo todov1.Todo
			Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
			out = append(out, todo.GetTitle())
		}
		return out
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("CanMutate write hook", func() {
		BeforeEach(func() {
			srv := newTestServer(server.WithPermissions(exampleapp.NewPermissions()))
			start(srv)
		})

		It("turns business rejections into REJECTED results, not RPC errors", func() {
			resp := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))
			Expect(result.GetErrorMessage()).NotTo(BeEmpty())

			Expect(pull().GetPatch().GetChanges()).To(BeEmpty(), "rejected mutation must not write")
		})

		It("acks rejections: the watermark advances and the queue continues", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "forbidden fruit"}))

			// Retry of the rejected id replays the verdict, and the next id applies.
			resp := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "forbidden fruit"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}),
			)
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(resp[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			Expect(pull().GetLastAckedMutationId()).To(BeEquivalentTo(2))
			Expect(titles(pull().GetPatch().GetChanges())).To(Equal([]string{"fine"}))
		})

		It("replays the verdict when the push response was lost", func() {
			// The rejection commits and bumps the watermark, then the response
			// never reaches the client. Answering the retry with a bare
			// ALREADY_APPLIED would drop the edit with no explanation, so the
			// recorded verdict answers instead — code and message intact.
			first := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}))
			Expect(first[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))

			retry := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}))
			Expect(retry[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(retry[0].GetErrorCode()).To(Equal(first[0].GetErrorCode()))
			Expect(retry[0].GetErrorMessage()).To(Equal(first[0].GetErrorMessage()))
			Expect(retry[0].GetSeq()).To(BeZero(), "a replayed rejection consumes no sequence")

			Expect(pull().GetPatch().GetChanges()).To(BeEmpty(), "the replay must not write either")
		})

		It("keeps replaying a verdict while later ids ride along unacked", func() {
			// Every response is lost, so the queue head stays put while new
			// mutations pile up behind it. The verdict must survive the batches
			// that follow it, not just the one that recorded it.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}))
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}),
			)
			resp := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "also fine"}),
			)
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(resp[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED))
			Expect(resp[2].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})

		It("keeps each client's verdicts to itself", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "very forbidden"}))
			push(putMutation("c2", 1, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}))

			resp := push(putMutation("c2", 1, "todo", "t2", &todov1.Todo{Id: "t2", Title: "fine"}))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED),
				"c2 applied its id 1; c1's rejection of the same id is none of its business")
		})

		It("enforces the policy on a write buried inside a batch and rolls the batch back", func() {
			resp := push(batchMutation("c1", 1,
				crudPut("todo", "t1", &todov1.Todo{Id: "t1", Title: "fine"}),
				crudPut("todo", "t2", &todov1.Todo{Id: "t2", Title: "very forbidden"}),
			))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))

			Expect(pull().GetPatch().GetChanges()).To(BeEmpty(),
				"rejecting any sub-mutation rolls back the whole batch, including the allowed write")
		})
	})

	Describe("ReadFilter hook", func() {
		BeforeEach(func() {
			srv := newTestServer(server.WithPermissions(exampleapp.NewPermissions()))
			start(srv)
		})

		It("filters rows out of pull responses", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "top secret plans"}),
			)
			Expect(titles(pull().GetPatch().GetChanges())).To(Equal([]string{"public"}))
		})

		Describe("hidden rows are not disclosed as tombstones", func() {
			pullFrom := func(cursor []byte) *syncplanev1.PullResponse {
				GinkgoHelper()
				resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
					WorkspaceId: ws,
					ClientId:    "c1",
					Cursor:      cursor,
				}))
				Expect(err).NotTo(HaveOccurred())
				return resp.Msg
			}
			tombstoneKeys := func(resp *syncplanev1.PullResponse) []string {
				var out []string
				for _, rc := range resp.GetPatch().GetChanges() {
					if rc.GetTombstone() {
						out = append(out, rc.GetKey())
					}
				}
				return out
			}

			It("sends nothing for a filtered row created past the client's cursor", func() {
				push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}))
				first := pullFrom(nil)
				Expect(titles(first.GetPatch().GetChanges())).To(Equal([]string{"public"}))

				push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret invite code"}))
				resp := pullFrom(first.GetNextCursor())
				Expect(resp.GetPatch().GetChanges()).To(BeEmpty(),
					"a tombstone here would hand the client the key and change time of a row it may not read")
			})

			It("sends no tombstones on a first pull, which delivers to a client that holds nothing", func() {
				push(
					putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}),
					putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret stash"}),
				)
				resp := pullFrom(nil)
				Expect(titles(resp.GetPatch().GetChanges())).To(Equal([]string{"public"}))
				Expect(tombstoneKeys(resp)).To(BeEmpty())
			})

			It("still revokes with a tombstone when an edit hides a row the client holds", func() {
				push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}))
				first := pullFrom(nil)
				Expect(titles(first.GetPatch().GetChanges())).To(Equal([]string{"public"}))

				push(putMutation("c1", 2, "todo", "t1", &todov1.Todo{Id: "t1", Title: "now secret"}))
				resp := pullFrom(first.GetNextCursor())
				Expect(tombstoneKeys(resp)).To(Equal([]string{"t1"}),
					"the client holds t1 and must drop it")
			})

			It("stays conservative for hidden rows the cursor cannot rule out", func() {
				// t2 is born hidden after the client's first pull (no tombstone),
				// but once the cursor has advanced past its creation the server
				// cannot prove the client never held it — it keeps no history of
				// what t2 said back then — so a later edit tombstones it. The
				// disclosure this leaves open is documented on
				// Permissions.ReadFilter.
				push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}))
				first := pullFrom(nil)

				push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret v1"}))
				second := pullFrom(first.GetNextCursor())
				Expect(second.GetPatch().GetChanges()).To(BeEmpty())

				push(putMutation("c1", 3, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret v2"}))
				third := pullFrom(second.GetNextCursor())
				Expect(tombstoneKeys(third)).To(Equal([]string{"t2"}))
			})
		})

		It("filters rows out of bootstrap streams", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "public"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "secret stash"}),
			)

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
				ClientId:    "c1",
			}))
			Expect(err).NotTo(HaveOccurred())
			var rows []*syncplanev1.RowChange
			for stream.Receive() {
				if chunk := stream.Msg().GetChunk(); chunk != nil {
					rows = append(rows, chunk.GetRows()...)
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(titles(rows)).To(Equal([]string{"public"}))
		})
	})

	Describe("custom mutators", func() {
		BeforeEach(func() {
			start(newTestServer(exampleapp.MutatorOptions(nil)...))
		})

		It("applies a registered mutator transactionally", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two", Completed: true}),
				customMutation("c1", 3, "complete_all", &todov1.CompleteAllArgs{}),
			)

			changes := pull().GetPatch().GetChanges()
			Expect(changes).To(HaveLen(2))
			for _, rc := range changes {
				var todo todov1.Todo
				Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
				Expect(todo.GetCompleted()).To(BeTrue())
			}
		})

		It("rejects unknown mutators as invalid argument", func() {
			// Deterministic: an unknown mutator (e.g. an older server that
			// lacks one a newer client uses) acks via rejection so it cannot
			// wedge the queue.
			result := push(customMutation("c1", 1, "no_such_mutator", &todov1.CompleteAllArgs{}))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})

		It("lets mutators reject with business errors", func() {
			start(newTestServer(server.WithMutator("always_no", &todov1.CompleteAllArgs{},
				func(context.Context, *server.EntityTx, proto.Message) error {
					return server.Reject("nope", "always rejected")
				})))

			resp := push(customMutation("c1", 1, "always_no", &todov1.CompleteAllArgs{}))
			result := resp[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal("nope"))
		})
	})
})

func customMutation(clientID string, mutationID uint64, name string, args proto.Message) *syncplanev1.Mutation {
	anyArgs, err := anypb.New(args)
	Expect(err).NotTo(HaveOccurred())
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload: &syncplanev1.Mutation_Custom{Custom: &syncplanev1.CustomMutation{
			Name: name,
			Args: anyArgs,
		}},
	}
}
