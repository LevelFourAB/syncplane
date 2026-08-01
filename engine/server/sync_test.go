package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
)

const ws = "ws-test"

var _ = Describe("SyncService", func() {
	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	startServer := func(srv interface {
		Handler() (string, http.Handler)
	}) syncplanev1connect.SyncServiceClient {
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	pull := func(cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Cursor:      cursor,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	decodeTodo := func(rc *syncplanev1.RowChange) *todov1.Todo {
		GinkgoHelper()
		var todo todov1.Todo
		Expect(proto.Unmarshal(rc.GetValue(), &todo)).To(Succeed())
		return &todo
	}

	BeforeEach(func() {
		ctx = context.Background()
		client = startServer(newTestServer())
	})

	Describe("Push + Pull", func() {
		It("applies a PUT and serves it on the next pull", func() {
			resp := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "hello"}))
			Expect(resp).To(HaveLen(1))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(resp[0].GetSeq()).To(BeEquivalentTo(1))

			pulled := pull(nil)
			Expect(pulled.GetReset_()).To(BeFalse())
			Expect(pulled.GetLastAckedMutationId()).To(BeEquivalentTo(1))
			Expect(pulled.GetSeq()).To(BeEquivalentTo(1), "response reports the seq it is current through")
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(1))
			change := pulled.GetPatch().GetChanges()[0]
			Expect(change.GetEntity()).To(Equal("todo"))
			Expect(change.GetKey()).To(Equal("t1"))
			Expect(change.GetVersion()).To(BeEquivalentTo(1))
			Expect(decodeTodo(change).GetTitle()).To(Equal("hello"))
		})

		It("advances the cursor so already-pulled changes do not repeat", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}))
			first := pull(nil)
			Expect(first.GetPatch().GetChanges()).To(HaveLen(1))

			second := pull(first.GetNextCursor())
			Expect(second.GetPatch().GetChanges()).To(BeEmpty())

			push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}))
			third := pull(second.GetNextCursor())
			Expect(third.GetPatch().GetChanges()).To(HaveLen(1))
			Expect(third.GetPatch().GetChanges()[0].GetKey()).To(Equal("t2"))
		})

		It("treats resent mutation ids as ALREADY_APPLIED without reapplying", func() {
			m := putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"})
			push(m)
			resp := push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "OVERWRITE"}))
			Expect(resp[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED))

			pulled := pull(nil)
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(1))
			Expect(decodeTodo(pulled.GetPatch().GetChanges()[0]).GetTitle()).To(Equal("v1"))
		})

		It("fails the whole batch on a mutation id gap", func() {
			// The gap is reported on the WORKSPACE, not as an RPC error: a
			// client pushing several workspaces at once must not lose the
			// others because one queue is out of order. Nothing in the batch
			// applies, so the client resends from its watermark.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3"}),
				})))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()).To(HaveLen(1))
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeMutationIDGap))
			Expect(resp.Msg.GetResults()[0].GetResults()).To(BeEmpty())

			// And the gapped mutation left no trace.
			pulled := pull(nil)
			for _, rc := range pulled.GetPatch().GetChanges() {
				Expect(rc.GetKey()).NotTo(Equal("t3"))
			}
		})

		It("rejects PUTs whose key does not match the key fields", func() {
			// A deterministic validation failure: it acks (rejects) rather
			// than erroring so it cannot wedge the queue on retry.
			result := push(putMutation("c1", 1, "todo", "WRONG", &todov1.Todo{Id: "t1"}))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})

		It("keeps composite-key rows distinct when a field value contains the separator", func() {
			// ("a/b", "c") and ("a", "b/c") both naively join to "a/b/c".
			// Escaping the separator in each field keeps them distinct rows.
			push(
				putMutation("c1", 1, "pair", `a\/b/c`, &testdatav1.Pair{Ns: "a/b", Id: "c", Label: "first"}),
				putMutation("c1", 2, "pair", `a/b\/c`, &testdatav1.Pair{Ns: "a", Id: "b/c", Label: "second"}),
			)

			pulled := pull(nil)
			rows := map[string]string{}
			for _, c := range pulled.GetPatch().GetChanges() {
				var p testdatav1.Pair
				Expect(proto.Unmarshal(c.GetValue(), &p)).To(Succeed())
				rows[c.GetKey()] = p.GetLabel()
			}
			Expect(rows).To(HaveLen(2), "distinct field tuples must not collide")
			Expect(rows).To(HaveKeyWithValue(`a\/b/c`, "first"))
			Expect(rows).To(HaveKeyWithValue(`a/b\/c`, "second"))
		})

		It("rejects a composite-key PUT whose key omits the required escaping", func() {
			// The naive join "a/b/c" must not be accepted for ("a/b", "c").
			result := push(putMutation("c1", 1, "pair", "a/b/c",
				&testdatav1.Pair{Ns: "a/b", Id: "c"}))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})

		It("serves tombstones for deletes on pull", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "doomed"}),
				deleteMutation("c1", 2, "todo", "t1"),
			)
			pulled := pull(nil)
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(1))
			change := pulled.GetPatch().GetChanges()[0]
			Expect(change.GetTombstone()).To(BeTrue())
			Expect(change.GetVersion()).To(BeEquivalentTo(2))
		})
	})

	Describe("PATCH / LWW", func() {
		It("merges interleaved patches from two clients per field, last writer wins", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "original", Completed: false}))
			// c2 sets completed, then c1 renames: both must survive.
			push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Completed: true}, "completed"))
			push(patchMutation("c1", 2, "todo", "t1", &todov1.Todo{Title: "renamed"}, "title"))
			// c2 renames again: last writer wins on title.
			push(patchMutation("c2", 2, "todo", "t1", &todov1.Todo{Title: "final"}, "title"))

			pulled := pull(nil)
			todo := decodeTodo(pulled.GetPatch().GetChanges()[0])
			Expect(todo.GetTitle()).To(Equal("final"))
			Expect(todo.GetCompleted()).To(BeTrue())
		})

		It("honors opaque fields end-to-end", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{
				Id:  "w1",
				Doc: &testdatav1.Blob{Data: []byte{1, 2, 3}, Note: "v1"},
			}))
			push(patchMutation("c2", 1, "widget", "w1",
				&testdatav1.Widget{Doc: &testdatav1.Blob{Note: "v2"}}, "doc.note"))

			pulled := pull(nil)
			var widget testdatav1.Widget
			Expect(proto.Unmarshal(pulled.GetPatch().GetChanges()[0].GetValue(), &widget)).To(Succeed())
			Expect(widget.GetDoc().GetNote()).To(Equal("v2"))
			Expect(widget.GetDoc().GetData()).To(BeEmpty(), "opaque field replaced whole")
		})

		// Delete-wins: a PATCH is not an upsert. Patching a row that is gone
		// would write a half-formed ghost (key fields live in crud.key, not
		// the value), so the server rejects it as a conflict instead.
		It("rejects a PATCH of a missing row as a conflict", func() {
			result := push(patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "born"}, "id", "title"))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeConflict))
			Expect(pull(nil).GetPatch().GetChanges()).To(BeEmpty(), "no ghost row written")
		})

		It("rejects a PATCH after DELETE as a conflict (no resurrection)", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "original"}))
			push(deleteMutation("c1", 2, "todo", "t1"))
			// Offline "rename" arriving after the delete must not resurrect t1.
			result := push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Title: "renamed"}, "title"))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeConflict))

			// The row stays deleted: a fresh bootstrap sees no live t1.
			pulled := pull(nil)
			for _, ch := range pulled.GetPatch().GetChanges() {
				if ch.GetKey() == "t1" {
					Expect(ch.GetTombstone()).To(BeTrue(), "t1 remains a tombstone")
				}
			}
		})

		// A poison/version-skew mask must reject as invalid_argument even when
		// the target row is gone — payload validity is checked before the
		// delete-wins conflict.
		It("rejects an unknown mask path before the missing-row conflict", func() {
			result := push(patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}, "ghost_field"))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})

		It("requires a field mask on PATCH", func() {
			result := push(patchMutation("c1", 1, "todo", "t1", &todov1.Todo{Title: "x"}))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))
		})

		// A row's storage key and its embedded key fields must agree. A mask
		// naming a key field would drift the value-embedded identity away from
		// crud.key, so the server rejects it (mirrors the PUT derived-key check)
		// and leaves the row untouched.
		It("rejects a PATCH whose mask rewrites a key field", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "original"}))
			result := push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t2", Title: "hijacked"}, "id", "title"))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeInvalidArgument))

			// Nothing drifted: t1 keeps its id and original title.
			todo := decodeTodo(pull(nil).GetPatch().GetChanges()[0])
			Expect(todo.GetId()).To(Equal("t1"))
			Expect(todo.GetTitle()).To(Equal("original"), "rejected write rolled back")
		})

		// Naming the key field with its existing value is a no-op on identity,
		// so the derived key still matches and the rest of the mask applies.
		It("allows a PATCH mask that names a key field without changing it", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "original"}))
			result := push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "renamed"}, "id", "title"))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(decodeTodo(pull(nil).GetPatch().GetChanges()[0]).GetTitle()).To(Equal("renamed"))
		})
	})

	Describe("expected_version (CAS)", func() {
		It("applies a PATCH whose expected_version matches the row", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"})) // seq 1
			result := push(withExpectedVersion(
				patchMutation("c1", 2, "todo", "t1", &todov1.Todo{Title: "v2"}, "title"), 1))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			Expect(decodeTodo(pull(nil).GetPatch().GetChanges()[0]).GetTitle()).To(Equal("v2"))
		})

		It("rejects a PATCH whose expected_version is stale and writes nothing", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "v1"}))     // seq 1
			push(patchMutation("c2", 1, "todo", "t1", &todov1.Todo{Title: "moved"}, "title")) // seq 2, row now at 2

			// c1 still believes the row is at version 1.
			result := push(withExpectedVersion(
				patchMutation("c1", 2, "todo", "t1", &todov1.Todo{Title: "stale"}, "title"), 1))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeConflict))
			Expect(decodeTodo(pull(nil).GetPatch().GetChanges()[0]).GetTitle()).To(Equal("moved"), "rejected write rolled back")
		})

		It("creates a row with a PUT guarded by expected_version 0 (apply-if-absent)", func() {
			result := push(withExpectedVersion(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "fresh"}), 0))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(decodeTodo(pull(nil).GetPatch().GetChanges()[0]).GetTitle()).To(Equal("fresh"))
		})

		It("rejects a PUT guarded by expected_version 0 when the row already exists", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "existing"})) // seq 1
			result := push(withExpectedVersion(
				putMutation("c1", 2, "todo", "t1", &todov1.Todo{Id: "t1", Title: "clobber"}), 0))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeConflict))
			Expect(decodeTodo(pull(nil).GetPatch().GetChanges()[0]).GetTitle()).To(Equal("existing"))
		})

		It("guards DELETE: applies on a version match, rejects on a mismatch", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"})) // seq 1
			push(putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2"})) // seq 2

			stale := push(withExpectedVersion(deleteMutation("c1", 3, "todo", "t1"), 99))[0]
			Expect(stale.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(stale.GetErrorCode()).To(Equal(server.CodeConflict))

			ok := push(withExpectedVersion(deleteMutation("c1", 4, "todo", "t1"), 1))[0]
			Expect(ok.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})

		It("treats a deleted row as version 0 for the CAS check", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"})) // seq 1
			push(deleteMutation("c1", 2, "todo", "t1"))                      // tombstone at seq 2

			// A guard expecting the pre-delete version sees current 0 → conflict.
			result := push(withExpectedVersion(
				putMutation("c2", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "revive"}), 1))[0]
			Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(result.GetErrorCode()).To(Equal(server.CodeConflict))
		})
	})

	Describe("Bootstrap", func() {
		It("streams Begin, Chunks and End with a usable cursor", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}),
				deleteMutation("c1", 3, "todo", "t2"),
				putMutation("c1", 4, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			)

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
				ClientId:    "c1",
			}))
			Expect(err).NotTo(HaveOccurred())

			var (
				begin *syncplanev1.BootstrapResponse_Begin
				rows  []*syncplanev1.RowChange
				ended bool
			)
			for stream.Receive() {
				switch body := stream.Msg().GetBody().(type) {
				case *syncplanev1.BootstrapResponse_Begin_:
					begin = body.Begin
				case *syncplanev1.BootstrapResponse_Chunk_:
					rows = append(rows, body.Chunk.GetRows()...)
				case *syncplanev1.BootstrapResponse_End_:
					ended = true
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(begin).NotTo(BeNil())
			Expect(begin.GetLastAckedMutationId()).To(BeEquivalentTo(4))
			Expect(begin.GetSeq()).To(BeEquivalentTo(4), "Begin reports the snapshot seq")
			Expect(ended).To(BeTrue())

			// Tombstoned t2 is absent; live rows from both entities present.
			keys := map[string]string{}
			for _, rc := range rows {
				keys[rc.GetEntity()+"/"+rc.GetKey()] = rc.GetEntity()
				Expect(rc.GetTombstone()).To(BeFalse())
			}
			Expect(keys).To(HaveLen(2))
			Expect(keys).To(HaveKey("todo/t1"))
			Expect(keys).To(HaveKey("widget/w1"))

			// The Begin cursor resumes cleanly: nothing new to pull.
			pulled := pull(begin.GetCursor())
			Expect(pulled.GetPatch().GetChanges()).To(BeEmpty())
		})

		It("pages large entities across multiple read transactions", func() {
			client = startServer(newTestServer(server.WithChunkSize(2)))

			var mutations []*syncplanev1.Mutation
			want := map[string]bool{}
			for i := 1; i <= 5; i++ {
				id := fmt.Sprintf("t%d", i)
				mutations = append(mutations, putMutation("c1", uint64(i), "todo", id, &todov1.Todo{Id: id}))
				want[id] = true
			}
			push(mutations...)

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
			}))
			Expect(err).NotTo(HaveOccurred())
			got := map[string]bool{}
			chunks := 0
			for stream.Receive() {
				if chunk := stream.Msg().GetChunk(); chunk != nil {
					chunks++
					Expect(len(chunk.GetRows())).To(BeNumerically("<=", 2))
					for _, rc := range chunk.GetRows() {
						got[rc.GetKey()] = true
					}
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(got).To(Equal(want), "every row arrives exactly once across pages")
			Expect(chunks).To(BeNumerically(">=", 3), "5 rows at chunk size 2 need several chunks")
		})

		It("heals writes that race the snapshot via a pull from the Begin cursor", func() {
			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "old"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2"}),
			)

			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId: ws,
			}))
			Expect(err).NotTo(HaveOccurred())
			var cursor []byte
			for stream.Receive() {
				if begin := stream.Msg().GetBegin(); begin != nil {
					cursor = begin.GetCursor()
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())

			// Writes after the Begin cursor (standing in for writes landing
			// between bootstrap pages): the follow-up pull re-delivers the
			// edit and tombstones the delete, whatever the pages saw.
			push(
				patchMutation("c1", 3, "todo", "t1", &todov1.Todo{Title: "new"}, "title"),
				deleteMutation("c1", 4, "todo", "t2"),
			)

			pulled := pull(cursor)
			Expect(pulled.GetReset_()).To(BeFalse())
			changes := pulled.GetPatch().GetChanges()
			Expect(changes).To(HaveLen(2))
			byKey := map[string]*syncplanev1.RowChange{}
			for _, rc := range changes {
				byKey[rc.GetKey()] = rc
			}
			Expect(decodeTodo(byKey["t1"]).GetTitle()).To(Equal("new"))
			Expect(byKey["t2"].GetTombstone()).To(BeTrue())
		})
	})

	Describe("randomized convergence", func() {
		It("matches a sequential replay of the same patches in seq order", func() {
			rng := rand.New(rand.NewSource(GinkgoRandomSeed()))
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))

			type op struct {
				seq   uint64
				value *todov1.Todo
				path  string
			}
			var ops []op
			nextID := map[string]uint64{"c1": 1, "c2": 0}
			titles := []string{"alpha", "beta", "gamma", "delta"}

			for i := 0; i < 60; i++ {
				clientID := []string{"c1", "c2"}[rng.Intn(2)]
				nextID[clientID]++
				var value *todov1.Todo
				var path string
				if rng.Intn(2) == 0 {
					value = &todov1.Todo{Title: titles[rng.Intn(len(titles))]}
					path = "title"
				} else {
					value = &todov1.Todo{Completed: rng.Intn(2) == 0}
					path = "completed"
				}
				resp := push(patchMutation(clientID, nextID[clientID], "todo", "t1", value, path))
				result := resp[0]
				Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
				ops = append(ops, op{seq: result.GetSeq(), value: value, path: path})
			}

			// Replay locally in seq order — the server assigned seqs are the
			// linearization, so the local model must equal the server row.
			expected := &todov1.Todo{Id: "t1"}
			for _, o := range ops {
				Expect(lww.Merge(expected, o.value, []string{o.path})).To(Succeed())
			}

			pulled := pull(nil)
			Expect(pulled.GetPatch().GetChanges()).To(HaveLen(1))
			Expect(proto.Equal(decodeTodo(pulled.GetPatch().GetChanges()[0]), expected)).To(BeTrue())
		})
	})

	Describe("two clients converging", func() {
		It("both clients see identical state after pulling", func() {
			// Two independent connect clients (same server) write and pull.
			clientA := client
			clientB := client

			_, err := clientA.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("ca", 1, "todo", "a1", &todov1.Todo{Id: "a1", Title: "from A"}),
				})))
			Expect(err).NotTo(HaveOccurred())
			_, err = clientB.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("cb", 1, "todo", "b1", &todov1.Todo{Id: "b1", Title: "from B"}),
					patchMutation("cb", 2, "todo", "a1", &todov1.Todo{Completed: true}, "completed"),
				})))
			Expect(err).NotTo(HaveOccurred())

			stateOf := func(c syncplanev1connect.SyncServiceClient, clientID string) map[string]string {
				resp, err := c.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
					WorkspaceId: ws,
					ClientId:    clientID,
				}))
				Expect(err).NotTo(HaveOccurred())
				state := map[string]string{}
				for _, rc := range resp.Msg.GetPatch().GetChanges() {
					todo := decodeTodo(rc)
					state[rc.GetKey()] = todo.GetTitle()
				}
				return state
			}

			stateA := stateOf(clientA, "ca")
			stateB := stateOf(clientB, "cb")
			Expect(stateA).To(Equal(stateB))
			Expect(stateA).To(HaveLen(2))
		})
	})

	// Provenance: the store behind a stable URL is swapped, recreated empty,
	// or restored from backup, so the client presents a cursor this store
	// never issued. Pull must re-bootstrap rather than scan from a foreign
	// position (which silently regresses the cursor and wedges the client) or
	// splice a different lineage's rows onto the client's. See issue 058.
	Describe("Cursor provenance", func() {
		decodeCursor := func(raw []byte) *syncplanecursorv1.Cursor {
			GinkgoHelper()
			var c syncplanecursorv1.Cursor
			Expect(proto.Unmarshal(raw, &c)).To(Succeed())
			return &c
		}

		It("stamps a store epoch into every issued cursor and does not reset-loop on it", func() {
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}))
			first := pull(nil)
			Expect(first.GetReset_()).To(BeFalse())
			epoch := decodeCursor(first.GetNextCursor()).GetStoreEpoch()
			Expect(epoch).NotTo(BeZero(), "an issued cursor carries the store epoch")

			// Re-presenting the freshly issued cursor matches the store and
			// must not reset — the bootstrap/pull cursor is self-consistent.
			again := pull(first.GetNextCursor())
			Expect(again.GetReset_()).To(BeFalse())
			Expect(decodeCursor(again.GetNextCursor()).GetStoreEpoch()).To(Equal(epoch))
		})

		It("resets a cursor whose seq is ahead of the store (recreated/restored store)", func() {
			// The store holds one row (seq 1); the client arrives with a cursor
			// from a previous, larger incarnation of the store at the same URL.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}))
			ahead := mustMarshal(&syncplanecursorv1.Cursor{Seq: 86})
			resp := pull(ahead)
			Expect(resp.GetReset_()).To(BeTrue())
		})

		It("resets a cursor stamped with a different store epoch (lineage swap)", func() {
			// A foreign store whose seq sits within range — the seq guard alone
			// cannot catch it; only the epoch can.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}))
			cur := decodeCursor(pull(nil).GetNextCursor())
			cur.StoreEpoch = cur.GetStoreEpoch() + 1 // a different, still non-zero lineage
			resp := pull(mustMarshal(cur))
			Expect(resp.GetReset_()).To(BeTrue())
		})

		It("accepts a legacy epoch-less cursor and stamps it going forward", func() {
			// Pre-upgrade cursors carry no store epoch (0). They are honored
			// once — the seq guard still covers their empty/restore cases — and
			// gain epoch protection from the cursor they receive back.
			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "x"}))
			legacy := mustMarshal(&syncplanecursorv1.Cursor{Seq: 1})
			resp := pull(legacy)
			Expect(resp.GetReset_()).To(BeFalse())
			Expect(decodeCursor(resp.GetNextCursor()).GetStoreEpoch()).NotTo(BeZero())
		})
	})
})
