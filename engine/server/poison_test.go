package server_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// filteringPerms puts every read on the decoding path: the filter admits every
// row, but being NON-NIL is what matters — a nil filter takes the zero-decode
// fast path, where an undecodable row is never noticed.
type filteringPerms struct{ server.AllowAll }

func (filteringPerms) ReadFilter(context.Context, server.Principal, string, string) (server.RowFilter, error) {
	return func(string, proto.Message) bool { return true }, nil
}

// recordingHandler collects log records so a spec can assert on what the engine
// reported (and how often).
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Message == msg {
			n++
		}
	}
	return n
}

// A row whose bytes do not decode used to fail whichever request had to look
// inside it, which — the failure being deterministic — took Pull, Bootstrap and
// Query down for the whole workspace until somebody rewrote the row. These
// specs plant such a row (only the storage adapter can: every write path
// refuses to produce one) and pin the quarantine that replaced that: the row
// goes invisible, everything else keeps syncing, and a write can repair it.
var _ = Describe("Undecodable rows", func() {
	const badKey = "w-bad"

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
		store  storage.Adapter
		engine *server.Server
	)

	startWith := func(opts ...server.Option) {
		srv, adapter := newTestServerStore(opts...)
		engine, store = srv, adapter
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

	// plantPoison writes bytes no message type can decode straight to storage,
	// standing in for the ways one really gets there: an older binary, a trusted
	// writer with a bug, a corrupted store.
	plantPoison := func(entity, key string) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			seq, err := tx.NextSeq()
			if err != nil {
				return err
			}
			return tx.PutRow(entity, key, []byte{0xff, 0xff, 0xff}, seq, storage.Actor{})
		})).To(Succeed())
	}

	pull := func(subs ...*syncplanev1.Subscription) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Subscriptions: subs,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// changeKinds maps each change's key to whether it arrived as a tombstone.
	changeKinds := func(changes []*syncplanev1.RowChange) map[string]bool {
		out := map[string]bool{}
		for _, rc := range changes {
			out[rc.GetKey()] = rc.GetTombstone()
		}
		return out
	}

	seedWidgets := func() {
		GinkgoHelper()
		push(
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Count: 30}),
			putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2", Count: 20}),
		)
		plantPoison("widget", badKey)
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("with a subscription predicate", func() {
		BeforeEach(func() {
			startWith()
			seedWidgets()
		})

		It("pulls every other row and tombstones the poison one", func() {
			got := changeKinds(pull(widgetSub(countGte(0))).GetPatch().GetChanges())
			Expect(got).To(HaveKeyWithValue("w1", false))
			Expect(got).To(HaveKeyWithValue("w2", false))
			// Hidden, exactly like a row a read filter denies: a client that
			// synced it before it went bad is told to drop it.
			Expect(got).To(HaveKeyWithValue(badKey, true))
		})

		It("bootstraps the rest of the workspace", func() {
			stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
				WorkspaceId:   ws,
				ClientId:      "c1",
				Subscriptions: []*syncplanev1.Subscription{widgetSub(countGte(0))},
			}))
			Expect(err).NotTo(HaveOccurred())
			var keys []string
			for stream.Receive() {
				if chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_); ok {
					for _, rc := range chunk.Chunk.GetRows() {
						keys = append(keys, rc.GetKey())
					}
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(keys).To(ConsistOf("w1", "w2"))
		})

		It("keeps the row out of a window it would otherwise rank into", func() {
			got := changeKinds(pull(windowSub(3, countGte(0))).GetPatch().GetChanges())
			Expect(got).To(HaveKeyWithValue("w1", false))
			Expect(got).To(HaveKeyWithValue("w2", false))
			// Not a member, so hidden — which pull spells as a tombstone.
			Expect(got).To(HaveKeyWithValue(badKey, true))
		})
	})

	Describe("with a read filter", func() {
		BeforeEach(func() {
			startWith(server.WithPermissions(filteringPerms{}))
			seedWidgets()
		})

		It("pulls every other row", func() {
			got := changeKinds(pull().GetPatch().GetChanges())
			Expect(got).To(HaveKeyWithValue("w1", false))
			Expect(got).To(HaveKeyWithValue("w2", false))
			Expect(got).To(HaveKeyWithValue(badKey, true))
		})

		It("keeps it out of a by-recency window", func() {
			// The poison row is the most recent write, so it would otherwise
			// take the single window slot; w2 gets it instead.
			got := changeKinds(pull(recencySub(1)).GetPatch().GetChanges())
			Expect(got).To(HaveKeyWithValue(badKey, true))
			Expect(got).To(HaveKeyWithValue("w2", false))
		})

		It("answers queries over the rest", func() {
			resp, err := client.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
				WorkspaceId: ws,
				Entity:      "widget",
			}))
			Expect(err).NotTo(HaveOccurred())
			var keys []string
			for _, rc := range resp.Msg.GetRows() {
				keys = append(keys, rc.GetKey())
			}
			Expect(keys).To(ConsistOf("w1", "w2"))
		})
	})

	Describe("with a follow spec", func() {
		followProject := &syncplanev1.Subscription{
			Entity: "issue",
			Follow: &syncplanev1.FollowSpec{
				Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				Depth: 1,
			},
		}

		BeforeEach(func() {
			startWith()
		})

		It("drops an undecodable follow target like an orphan", func() {
			push(
				putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", &testdatav1.Issue{
					Id: "i1", Title: "first", Project: &testdatav1.Project{Id: "p1"},
				}),
			)
			plantPoison("project", "p1")

			got := changeKinds(pull(followProject).GetPatch().GetChanges())
			Expect(got).To(HaveKeyWithValue("i1", false))
			Expect(got).To(HaveKeyWithValue("p1", true))
		})

		It("skips an undecodable seed row and follows from the rest", func() {
			push(
				putMutation("c1", 1, "project", "p1", &testdatav1.Project{Id: "p1", Name: "alpha"}),
				putMutation("c1", 2, "issue", "i1", &testdatav1.Issue{
					Id: "i1", Title: "first", Project: &testdatav1.Project{Id: "p1"},
				}),
			)
			plantPoison("issue", "i-bad")

			// i-bad rides the zero-decode fast path out (no filter, no
			// predicate); what matters is that the closure still resolved p1
			// from the seed the scan could read.
			got := changeKinds(pull(followProject).GetPatch().GetChanges())
			Expect(got).To(HaveKeyWithValue("i1", false))
			Expect(got).To(HaveKeyWithValue("p1", false))
		})
	})

	Describe("reporting", func() {
		It("logs the row once per process, not once per request", func() {
			logs := &recordingHandler{}
			startWith(server.WithLogger(slog.New(logs)))
			seedWidgets()

			pull(widgetSub(countGte(0)))
			pull(widgetSub(countGte(0)))
			Expect(logs.count("undecodable row quarantined")).To(Equal(1))
		})
	})

	Describe("repair", func() {
		BeforeEach(func() {
			startWith()
			seedWidgets()
		})

		It("takes a PUT over the poison row and syncs it again", func() {
			// The PUT path reads the stored row to union its collaborative-text
			// field; an undecodable base is no base, so the write lands rather
			// than leaving the row unfixable.
			results := push(putMutation("c1", 3, "widget", badKey,
				&testdatav1.Widget{Id: badKey, Count: 5, Label: "repaired"}))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			for _, rc := range pull(widgetSub(countGte(0))).GetPatch().GetChanges() {
				if rc.GetKey() != badKey {
					continue
				}
				Expect(rc.GetTombstone()).To(BeFalse())
				var w testdatav1.Widget
				Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
				Expect(w.GetLabel()).To(Equal("repaired"))
				return
			}
			Fail("repaired row was not delivered")
		})

		It("rejects a PATCH of it without wedging the client's queue", func() {
			results := push(
				patchMutation("c1", 3, "widget", badKey, &testdatav1.Widget{Id: badKey, Label: "x"}, "label"),
				putMutation("c1", 4, "widget", "w3", &testdatav1.Widget{Id: "w3", Count: 1}),
			)
			Expect(results).To(HaveLen(2))
			// A rejection is an ack: the client drops the mutation and the rest
			// of the batch still applies. An RPC error here would have the client
			// resend the same doomed mutation forever.
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(results[0].GetErrorCode()).To(Equal(server.CodeConflict))
			Expect(results[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})

		It("takes a DELETE of it", func() {
			results := push(deleteMutation("c1", 3, "widget", badKey))
			Expect(results[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		})
	})

	Describe("trusted writes", func() {
		BeforeEach(func() {
			startWith()
			seedWidgets()
		})

		It("take a Put over the poison row", func() {
			Expect(engine.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: badKey, Label: "repaired"})
			})).To(Succeed())

			var w testdatav1.Widget
			Expect(engine.View(ctx, ws, func(tx *server.ViewTx) error {
				found, _, err := tx.Get(&w, badKey)
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
			Expect(w.GetLabel()).To(Equal("repaired"))
		})

		It("reject a Patch of it, so a custom mutation acks instead of wedging", func() {
			err := engine.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Widget{Id: badKey, Label: "x"}, []string{"label"}, nil)
			})
			rejection, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rejection.Code).To(Equal(server.CodeConflict))
		})
	})

	Describe("the zero-decode fast path", func() {
		BeforeEach(func() {
			startWith()
			seedWidgets()
		})

		It("still ships the row verbatim", func() {
			// Deliberate asymmetry: a full sync with no filter and no predicate
			// never decodes a row, and paying for a decode of every row to catch
			// a corrupt one would tax every healthy workspace. The bytes go out
			// as stored and the fault surfaces at the reader instead.
			for _, rc := range pull().GetPatch().GetChanges() {
				if rc.GetKey() == badKey {
					Expect(rc.GetTombstone()).To(BeFalse())
					Expect(rc.GetValue()).To(Equal([]byte{0xff, 0xff, 0xff}))
					return
				}
			}
			Fail("the poison row was not delivered")
		})
	})
})
