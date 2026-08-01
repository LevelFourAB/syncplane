package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Garbage collection", func() {
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

	push := func(mutations ...*syncplanev1.Mutation) {
		GinkgoHelper()
		_, err := client.Push(ctx, connect.NewRequest(pushOf(
			ws,
			mutations)))
		Expect(err).NotTo(HaveOccurred())
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

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("forces a reset for cursors that predate collected tombstones", func() {
		// Negative retention puts the cutoff in the future: everything
		// already written is immediately collectable (millisecond timestamp
		// granularity makes retention 0 racy).
		srv := newTestServer(server.WithGC(-time.Second, 0))
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
		start(srv)

		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "keep"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "doomed"}),
		)
		preDelete := pull(nil)
		Expect(preDelete.GetPatch().GetChanges()).To(HaveLen(2))

		push(deleteMutation("c1", 3, "todo", "t2"))
		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

		// The pre-delete cursor can no longer see the collected tombstone.
		resp := pull(preDelete.GetNextCursor())
		Expect(resp.GetReset_()).To(BeTrue())

		// A fresh bootstrap works and only contains live rows.
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		var keys []string
		var cursor []byte
		for stream.Receive() {
			if begin := stream.Msg().GetBegin(); begin != nil {
				cursor = begin.GetCursor()
			}
			if chunk := stream.Msg().GetChunk(); chunk != nil {
				for _, rc := range chunk.GetRows() {
					keys = append(keys, rc.GetKey())
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(keys).To(Equal([]string{"t1"}))

		// The bootstrap cursor is at the current seq: no reset, no changes.
		after := pull(cursor)
		Expect(after.GetReset_()).To(BeFalse())
		Expect(after.GetPatch().GetChanges()).To(BeEmpty())
	})

	It("resets an empty cursor once the horizon has advanced", func() {
		// The empty cursor decodes to seq 0, so it is below every nonzero
		// horizon: replaying a workspace from the beginning through Pull works
		// only until the first tombstone is collected. A fresh client's
		// entry point is Bootstrap, not pull(nil) — pinned here because both
		// the proto and the pull comments promise the replay, and the promise
		// is bounded by GC.
		srv := newTestServer(server.WithGC(-time.Second, 0))
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
		start(srv)

		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "keep"}),
			putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "doomed"}),
		)
		Expect(pull(nil).GetReset_()).To(BeFalse(), "no GC yet: replay from seq 0 serves the whole workspace")

		push(deleteMutation("c1", 3, "todo", "t2"))
		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

		resp := pull(nil)
		Expect(resp.GetReset_()).To(BeTrue(), "horizon advanced: even a stateless client must bootstrap")
		Expect(resp.GetPatch().GetChanges()).To(BeEmpty())
	})

	It("keeps tombstones younger than the retention", func() {
		srv := newTestServer(server.WithGC(time.Hour, 0))
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
		start(srv)

		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
			deleteMutation("c1", 2, "todo", "t1"),
		)
		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

		resp := pull(nil)
		Expect(resp.GetReset_()).To(BeFalse(), "young tombstones survive")
		Expect(resp.GetPatch().GetChanges()).To(HaveLen(1))
		Expect(resp.GetPatch().GetChanges()[0].GetTombstone()).To(BeTrue())
	})

	It("requires WithGC for manual collection", func() {
		srv := newTestServer()
		DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
		Expect(srv.CollectGarbage(ctx, ws)).To(MatchError(ContainSubstring("WithGC")))
	})

	It("runs the background loop and stops cleanly on Close", func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "gc.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		srv := mustNewServer(store,
			server.WithEntities(&todov1.Todo{}),
			server.WithGC(0, time.Millisecond))
		start(srv)

		push(
			putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}),
			deleteMutation("c1", 2, "todo", "t1"),
		)

		// The ticker collects the tombstone without manual intervention.
		Eventually(func() bool {
			return pull(nil).GetReset_()
		}).Should(BeTrue(), "background pass collected the tombstone and bumped the horizon")

		// The same pass prunes the idle client watermark.
		Eventually(func() uint64 {
			return pull(nil).GetLastAckedMutationId()
		}).Should(BeZero(), "background pass pruned the idle client")

		Expect(srv.Close()).To(Succeed())
		Expect(srv.Close()).To(Succeed(), "Close is idempotent")
	})

	Describe("idle clients and watermark adoption", func() {
		It("prunes idle clients and re-admits a returning client at its next mutation id", func() {
			srv := newTestServer(server.WithGC(-time.Second, 0))
			DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
			start(srv)

			push(
				putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1", Title: "one"}),
				putMutation("c1", 2, "todo", "t2", &todov1.Todo{Id: "t2", Title: "two"}),
				putMutation("c1", 3, "todo", "t3", &todov1.Todo{Id: "t3", Title: "three"}),
			)
			Expect(pull(nil).GetLastAckedMutationId()).To(BeEquivalentTo(3))

			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(pull(nil).GetLastAckedMutationId()).To(BeZero(), "pruned client reads as unknown")

			// The returning client pushes mutation 4: watermark 0 plus
			// mutation_id > 1 adopts instead of failing the gap check.
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 4, "todo", "t4", &todov1.Todo{Id: "t4", Title: "four"}),
				})))
			Expect(err).NotTo(HaveOccurred())
			Expect(pushResults(resp.Msg)[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			// Idempotency works again from the adopted watermark.
			resp, err = client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 4, "todo", "t4", &todov1.Todo{Id: "t4", Title: "RESENT"}),
				})))
			Expect(err).NotTo(HaveOccurred())
			Expect(pushResults(resp.Msg)[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED))

			// ...including the gap check: 6 after 4 is still refused.
			resp, err = client.Push(ctx, connect.NewRequest(pushOf(
				ws,
				[]*syncplanev1.Mutation{
					putMutation("c1", 6, "todo", "t6", &todov1.Todo{Id: "t6"}),
				})))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeMutationIDGap))
		})

		It("keeps clients younger than the retention", func() {
			srv := newTestServer(server.WithGC(time.Hour, 0))
			DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
			start(srv)

			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(pull(nil).GetLastAckedMutationId()).To(BeEquivalentTo(1), "young client survives")
		})
	})

	Describe("effect consumers", func() {
		It("collects a consumer stranded below the horizon, with its shadows", func() {
			srv := newTestServer(server.WithGC(-time.Second, 0))
			DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
			start(srv)

			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			// A consumer that got as far as seq 1 and then stopped.
			Expect(srv.AdvanceConsumer(ctx, ws, "indexer", server.ConsumerUpdate{
				Cursor:  server.Cursor{Seq: 1},
				Shadows: []server.ConsumerShadowWrite{{Entity: "todo", Key: "t1", Snapshot: []byte("shadow")}},
			})).To(Succeed())

			// Deleting and collecting drives the horizon above it.
			push(deleteMutation("c1", 2, "todo", "t1"))
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			_, found, err := srv.ConsumerPosition(ctx, ws, "indexer")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse(), "a stranded consumer is collected")
			snapshot, err := srv.ConsumerShadow(ctx, ws, "indexer", "todo", "t1")
			Expect(err).NotTo(HaveOccurred())
			Expect(snapshot).To(BeNil(), "its shadows go with it")
		})

		It("spares a consumer that has kept up", func() {
			srv := newTestServer(server.WithGC(-time.Second, 0))
			DeferCleanup(func() { Expect(srv.Close()).To(Succeed()) })
			start(srv)

			push(putMutation("c1", 1, "todo", "t1", &todov1.Todo{Id: "t1"}))
			push(deleteMutation("c1", 2, "todo", "t1"))
			// Caught up to the head, which is where the horizon lands.
			Expect(srv.AdvanceConsumer(ctx, ws, "indexer", server.ConsumerUpdate{
				Cursor: server.Cursor{Seq: 2},
			})).To(Succeed())

			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())

			pos, found, err := srv.ConsumerPosition(ctx, ws, "indexer")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue(), "an idle but current consumer is not garbage")
			Expect(pos.Seq).To(BeEquivalentTo(2))
		})
	})
})
