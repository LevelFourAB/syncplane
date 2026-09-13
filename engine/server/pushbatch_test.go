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
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// What a batch of mutations owes each of its members. One push batch is one
// write transaction, and the engine spends as little of it on the database as
// the outcomes allow — one watermark write per client, sequences handed out
// without a statement each, base rows read for the whole batch at once. These
// specs pin the outcomes those economies must not change: where the watermark
// lands, which sequence a rejected mutation leaves behind, and what a mutation
// sees of the ones before it.
var _ = Describe("Push batches", func() {
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
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	pullAs := func(clientID string) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    clientID,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// todoAt reads one todo out of a full pull, or nil when the row is gone.
	todoAt := func(key string) *todov1.Todo {
		GinkgoHelper()
		for _, change := range pullAs("reader").GetPatch().GetChanges() {
			if change.GetEntity() != "todo" || change.GetKey() != key || change.GetTombstone() {
				continue
			}
			var todo todov1.Todo
			Expect(proto.Unmarshal(change.GetValue(), &todo)).To(Succeed())
			return &todo
		}
		return nil
	}

	statuses := func(results []*syncplanev1.MutationResult) []syncplanev1.MutationStatus {
		out := make([]syncplanev1.MutationStatus, 0, len(results))
		for _, r := range results {
			out = append(out, r.GetStatus())
		}
		return out
	}

	const (
		applied = syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED
		already = syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED
		refused = syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED
	)

	// A PATCH of a row that does not exist is the cheapest rejection to build:
	// delete-wins refuses it as a conflict, deterministically.
	patchOfNothing := func(clientID string, mutationID uint64, key string) *syncplanev1.Mutation {
		return patchMutation(clientID, mutationID, "todo", key,
			&todov1.Todo{Title: "ghost"}, "title")
	}

	BeforeEach(func() {
		ctx = context.Background()
		start(newTestServer())
	})

	Describe("the client watermark", func() {
		It("advances past a mutation rejected in the middle of the batch", func() {
			results := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				patchOfNothing("c1", 2, "gone"),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "three"}),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, refused, applied}))
			Expect(pullAs("c1").GetLastAckedMutationId()).To(BeEquivalentTo(3))
		})

		It("advances past a mutation rejected as the LAST of the batch", func() {
			// The bump has to land even when nothing follows it in the batch —
			// otherwise the client resends id 2 forever and never reaches 3.
			results := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				patchOfNothing("c1", 2, "gone"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, refused}))
			Expect(pullAs("c1").GetLastAckedMutationId()).To(BeEquivalentTo(2))

			Expect(statuses(push(
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "three"}),
			))).To(Equal([]syncplanev1.MutationStatus{applied}), "no gap after the rejection")
		})

		It("replays the verdict when a rejected id is resent", func() {
			// The recorded verdict is written alongside the watermark, so it has
			// to describe the same mutation the watermark covers.
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				patchOfNothing("c1", 2, "gone"),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "three"}),
			)
			replay := push(patchOfNothing("c1", 2, "gone"))
			Expect(replay[0].GetStatus()).To(Equal(refused))
			Expect(replay[0].GetErrorCode()).To(Equal(server.CodeConflict))
		})

		It("keeps one watermark per client across a batch that interleaves them", func() {
			results := push(
				putMutation("c1", 1, "todo", "a", &todov1.Todo{Id: "a"}),
				putMutation("c2", 1, "todo", "b", &todov1.Todo{Id: "b"}),
				putMutation("c1", 2, "todo", "c", &todov1.Todo{Id: "c"}),
				putMutation("c2", 2, "todo", "d", &todov1.Todo{Id: "d"}),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, applied, applied, applied}))
			Expect(pullAs("c1").GetLastAckedMutationId()).To(BeEquivalentTo(2))
			Expect(pullAs("c2").GetLastAckedMutationId()).To(BeEquivalentTo(2))

			Expect(statuses(push(
				putMutation("c1", 2, "todo", "a", &todov1.Todo{Id: "a", Title: "resent"}),
				putMutation("c2", 3, "todo", "e", &todov1.Todo{Id: "e"}),
			))).To(Equal([]syncplanev1.MutationStatus{already, applied}))
		})
	})

	Describe("sequence allocation", func() {
		It("leaves no gap where a mutation was rejected", func() {
			// A rejected mutation rolls its sequence back with its writes, so
			// the next applied one takes it instead. A gap would strand every
			// client's cursor on a sequence no change ever carries.
			results := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				patchOfNothing("c1", 2, "gone"),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "three"}),
			)
			Expect(results[0].GetSeq()).To(BeEquivalentTo(1))
			Expect(results[1].GetSeq()).To(BeZero(), "a rejected mutation reports no sequence")
			Expect(results[2].GetSeq()).To(BeEquivalentTo(2))

			pulled := pullAs("reader")
			Expect(pulled.GetSeq()).To(BeEquivalentTo(2))
			versions := map[string]uint64{}
			for _, change := range pulled.GetPatch().GetChanges() {
				versions[change.GetKey()] = change.GetVersion()
			}
			Expect(versions).To(Equal(map[string]uint64{"t1": 1, "t3": 2}))
		})

		It("carries the allocations of one batch into the next", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2"}),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3"}),
			)
			results := push(putMutation("c1", 4, "todo", "t4", &todov1.Todo{Id: "t4"}))
			Expect(results[0].GetSeq()).To(BeEquivalentTo(4),
				"a committed batch persists every sequence it took")
		})
	})

	Describe("a mutation's view of the ones before it", func() {
		It("PATCHes the row a PUT earlier in the same batch created", func() {
			// The PATCH's base is the PUT's row, not the absent row the batch
			// started with — which a stale base would refuse outright.
			results := push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one", Completed: true}),
				patchMutation("c1", 2, "todo", "t1", &todov1.Todo{Title: "two"}, "title"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, applied}))
			Expect(todoAt("t1").GetTitle()).To(Equal("two"))
			Expect(todoAt("t1").GetCompleted()).To(BeTrue(), "the PUT's untouched field survives the merge")
		})

		It("PATCHes the row a PUT created even once the entity has been read", func() {
			// The sharp case for any shared read: an earlier mutation of the
			// SAME entity has already pulled the batch's rows into memory, and
			// among them the row this PUT is about to create. Serving that
			// snapshot to the PATCH would refuse it as a conflict.
			push(putMutation("c1", 1, "todo", "t0", &todov1.Todo{Id: "t0", Title: "zero"}))
			results := push(
				patchMutation("c1", 2, "todo", "t0", &todov1.Todo{Title: "read me"}, "title"),
				putMutation("c1", 3, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one", Completed: true}),
				patchMutation("c1", 4, "todo", "t1", &todov1.Todo{Title: "two"}, "title"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, applied, applied}))
			Expect(todoAt("t1").GetTitle()).To(Equal("two"))
			Expect(todoAt("t1").GetCompleted()).To(BeTrue())
		})

		It("refuses a PATCH of a row a DELETE earlier in the same batch removed", func() {
			// Led by a PATCH of another row of the same entity, so the batch's
			// rows are already in memory when the DELETE lands: the PATCH after
			// it must see the tombstone, not the snapshot.
			push(
				putMutation("c1", 1, "todo", "t0", &todov1.Todo{Id: "t0", Title: "zero"}),
				putMutation("c1", 2, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
			)
			results := push(
				patchMutation("c1", 3, "todo", "t0", &todov1.Todo{Title: "read me"}, "title"),
				deleteMutation("c1", 4, "todo", "t1"),
				patchMutation("c1", 5, "todo", "t1", &todov1.Todo{Title: "two"}, "title"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, applied, refused}))
			Expect(results[2].GetErrorCode()).To(Equal(server.CodeConflict))
			Expect(todoAt("t1")).To(BeNil())
		})

		It("PATCHes the row a batch mutation earlier in the same push created", func() {
			results := push(
				batchMutation("c1", 1, crudPut("widget", "w1", &testdatav1.Widget{Id: "w1", Label: "A"})),
				patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Count: 7}, "count"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, applied}))

			var widget testdatav1.Widget
			for _, change := range pullAs("reader").GetPatch().GetChanges() {
				if change.GetKey() == "w1" {
					Expect(proto.Unmarshal(change.GetValue(), &widget)).To(Succeed())
				}
			}
			Expect(widget.GetLabel()).To(Equal("A"))
			Expect(widget.GetCount()).To(BeEquivalentTo(7))
		})

		It("PATCHes the row a custom mutation earlier in the same push created", func() {
			// A mutator's writes cannot be read off the wire, so nothing after
			// one may be served from a base read taken before the batch.
			start(newTestServer(server.WithMutator("create", &testdatav1.Widget{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Put(&testdatav1.Widget{Id: "w1", Label: args.(*testdatav1.Widget).GetLabel()})
				})))

			results := push(
				customMutation("c1", 1, "create", &testdatav1.Widget{Label: "A"}),
				patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Count: 7}, "count"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{applied, applied}))

			var widget testdatav1.Widget
			for _, change := range pullAs("reader").GetPatch().GetChanges() {
				if change.GetKey() == "w1" {
					Expect(proto.Unmarshal(change.GetValue(), &widget)).To(Succeed())
				}
			}
			Expect(widget.GetLabel()).To(Equal("A"))
			Expect(widget.GetCount()).To(BeEquivalentTo(7))
		})

		It("PATCHes the row a rejected mutation did NOT change", func() {
			// The rejected PATCH rolls back; the one after it must merge onto
			// what was stored before, and must not inherit anything the rewind
			// undid.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			results := push(
				withExpectedVersion(
					patchMutation("c1", 2, "todo", "t1", &todov1.Todo{Title: "stale"}, "title"), 99),
				patchMutation("c1", 3, "todo", "t1", &todov1.Todo{Title: "three"}, "title"),
			)
			Expect(statuses(results)).To(Equal([]syncplanev1.MutationStatus{refused, applied}))
			Expect(todoAt("t1").GetTitle()).To(Equal("three"))
		})
	})
})
