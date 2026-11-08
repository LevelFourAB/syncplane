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

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

var _ = Describe("Late rejection", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
		// origins holds the origin of every captured client write, by
		// mutation id.
		origins map[uint64]server.MutationOrigin
	)

	BeforeEach(func() {
		ctx = context.Background()
		origins = map[uint64]server.MutationOrigin{}
		capture := func(_ context.Context, w *server.CapturedWrite) error {
			if w.Origin != nil {
				origins[w.Origin.MutationID] = *w.Origin
			}
			return nil
		}
		srv, _ = newTestServerStore(server.WithWriteCapture(capture))
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	push := func(muts ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}
	pull := func(clientID string, cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: clientID, Cursor: cursor,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}
	// rejectLate reverts the widget a mutation put and records its late
	// rejection, as a write-behind link does.
	rejectLate := func(id uint64, key string) {
		GinkgoHelper()
		Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			if err := tx.Delete("widget", key); err != nil {
				return err
			}
			return tx.RejectLate(origins[id], server.CodePermissionDenied, "refused upstream")
		})).To(Succeed())
	}
	// lateIDs pulls from cursor to the end and returns the mutation ids of
	// every late rejection the pages carried.
	lateIDs := func(clientID string, cursor []byte) map[uint64]bool {
		GinkgoHelper()
		ids := map[uint64]bool{}
		for range 1000 {
			page := pull(clientID, cursor)
			for _, l := range page.GetLateRejections() {
				ids[l.GetMutationId()] = true
			}
			cursor = page.GetNextCursor()
			if !page.GetHasMore() {
				return ids
			}
		}
		Fail("the pulls did not settle")
		return nil
	}

	It("gives each client write its origin", func() {
		push(
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			batchMutation("c1", 2,
				crudPut("widget", "w2", &testdatav1.Widget{Id: "w2"}),
				crudPut("widget", "w3", &testdatav1.Widget{Id: "w3"})),
		)
		Expect(origins[1].Client).NotTo(BeEmpty())
		Expect(origins[1].Targets).To(Equal([]storage.RejectionTarget{{Entity: "widget", Key: "w1"}}))
		Expect(origins[2].Client).To(Equal(origins[1].Client))
		Expect(origins[2].Targets).To(Equal([]storage.RejectionTarget{
			{Entity: "widget", Key: "w2"}, {Entity: "widget", Key: "w3"},
		}))

		Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w4"})
		})).To(Succeed())
		Expect(origins).To(HaveLen(2), "a write that no client pushed has no origin")
	})

	It("reports a rejection once, on the page that moves the client past it", func() {
		res := push(
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2"}),
		)
		Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		before := pull("c1", nil)
		Expect(before.GetLateRejections()).To(BeEmpty())

		rejectLate(1, "w1")

		page := pull("c1", before.GetNextCursor())
		Expect(page.GetLateRejections()).To(HaveLen(1))
		late := page.GetLateRejections()[0]
		Expect(late.GetMutationId()).To(BeEquivalentTo(1))
		Expect(late.GetErrorCode()).To(Equal(server.CodePermissionDenied))
		Expect(late.GetErrorMessage()).To(Equal("refused upstream"))
		Expect(late.GetTargets()).To(HaveLen(1))
		Expect(late.GetTargets()[0].GetEntity()).To(Equal("widget"))
		Expect(late.GetTargets()[0].GetKey()).To(Equal("w1"))
		Expect(page.GetPatch().GetChanges()).To(ContainElement(Satisfy(func(rc *syncplanev1.RowChange) bool {
			return rc.GetKey() == "w1" && rc.GetTombstone()
		})), "the revert is on the same page")

		Expect(pull("c1", page.GetNextCursor()).GetLateRejections()).To(BeEmpty())
		Expect(pull("c1", before.GetNextCursor()).GetLateRejections()).To(HaveLen(1),
			"a pull from the earlier cursor receives it again")
		Expect(pull("c2", before.GetNextCursor()).GetLateRejections()).To(BeEmpty(),
			"another client does not receive it")
	})

	It("answers a resend of the mutation with its original ack", func() {
		push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		rejectLate(1, "w1")

		res := push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED))
	})

	It("takes a sequence when it reverts no row", func() {
		push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		before := pull("c1", nil)

		Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			return tx.RejectLate(origins[1], server.CodeConflict, "")
		})).To(Succeed())

		page := pull("c1", before.GetNextCursor())
		Expect(page.GetSeq()).To(BeNumerically(">", before.GetSeq()))
		Expect(page.GetLateRejections()).To(HaveLen(1))
		Expect(page.GetLateRejections()[0].GetErrorCode()).To(Equal(server.CodeConflict))
	})

	It("keeps a client's rejections ordered by mutation id", func() {
		push(
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2"}),
		)
		before := pull("c1", nil)
		rejectLate(2, "w2")
		rejectLate(1, "w1")

		var ids []uint64
		for _, l := range pull("c1", before.GetNextCursor()).GetLateRejections() {
			ids = append(ids, l.GetMutationId())
		}
		Expect(ids).To(Equal([]uint64{1, 2}))
	})

	It("refuses an origin without a client or a mutation id, and an empty code", func() {
		push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		for _, call := range []func(tx *server.ReplicaTx) error{
			func(tx *server.ReplicaTx) error {
				return tx.RejectLate(server.MutationOrigin{MutationID: 1}, "conflict", "")
			},
			func(tx *server.ReplicaTx) error {
				return tx.RejectLate(server.MutationOrigin{Client: origins[1].Client}, "conflict", "")
			},
			func(tx *server.ReplicaTx) error { return tx.RejectLate(origins[1], "", "") },
		} {
			Expect(srv.Replicate(ctx, ws, call)).To(HaveOccurred())
		}
	})

	It("sends the rejection on a stream frame with its cursor", func() {
		push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		before := pull("c1", nil)

		sctx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		stream, err := client.Subscribe(sctx, connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: before.GetNextCursor()}},
		}))
		Expect(err).NotTo(HaveOccurred())
		for {
			Expect(stream.Receive()).To(BeTrue(), "stream closed early: %v", stream.Err())
			if stream.Msg().GetCaughtUp() {
				break
			}
		}

		rejectLate(1, "w1")
		for {
			Expect(stream.Receive()).To(BeTrue(), "stream closed early: %v", stream.Err())
			page := stream.Msg().GetPage()
			if page == nil || len(page.GetLateRejections()) == 0 {
				continue
			}
			Expect(page.GetLateRejections()[0].GetMutationId()).To(BeEquivalentTo(1))
			Expect(page.GetNextCursor()).NotTo(BeEmpty())
			break
		}
	})

	// A late rejection waits in the client's rejection record until a pull
	// reaches it, so the record's size bound must not drop it first.
	Describe("before the client pulls", func() {
		It("delivers every late rejection one pass records", func() {
			// Given 700 applied writes, as a long offline queue would leave
			const n = 700
			for chunk := range n / 100 {
				var muts []*syncplanev1.Mutation
				for i := range 100 {
					id := uint64(chunk*100 + i + 1)
					key := fmt.Sprintf("w%04d", id)
					muts = append(muts, putMutation("c1", id, "widget", key, &testdatav1.Widget{Id: key}))
				}
				for _, r := range push(muts...) {
					Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
				}
			}
			before := pull("c1", nil)
			for before.GetHasMore() {
				before = pull("c1", before.GetNextCursor())
			}

			// When the origin refuses every one of them in one pass
			msg := "refused upstream: " + strings.Repeat("x", 82)
			Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
				for id := uint64(1); id <= n; id++ {
					if err := tx.Delete("widget", fmt.Sprintf("w%04d", id)); err != nil {
						return err
					}
					if err := tx.RejectLate(origins[id], server.CodePermissionDenied, msg); err != nil {
						return err
					}
				}
				return nil
			})).To(Succeed())

			// Then the client hears of each one
			got := lateIDs("c1", before.GetNextCursor())
			var missing []uint64
			for id := uint64(1); id <= n; id++ {
				if !got[id] {
					missing = append(missing, id)
				}
			}
			Expect(missing).To(BeEmpty(), "every reverted mutation is reported to its client")
		})

		It("keeps a late rejection across a push the server rejects", func() {
			// Given a late rejection the client has not pulled
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
			before := pull("c1", nil)
			rejectLate(1, "w1")

			// When the client pushes 1000 writes that are each refused
			push(putMutation("c2", 1, "widget", "hot", &testdatav1.Widget{Id: "hot"}))
			id := uint64(2)
			for range 10 {
				var muts []*syncplanev1.Mutation
				for range 100 {
					muts = append(muts, withExpectedVersion(
						putMutation("c1", id, "widget", "hot", &testdatav1.Widget{Id: "hot"}), 999999))
					id++
				}
				for _, r := range push(muts...) {
					Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
				}
			}

			// Then its next pull still carries the late rejection
			Expect(lateIDs("c1", before.GetNextCursor())).To(HaveKey(uint64(1)))
		})

		It("delivers a late rejection whose message is larger than the record's bound", func() {
			push(putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
			before := pull("c1", nil)

			Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
				if err := tx.Delete("widget", "w1"); err != nil {
					return err
				}
				return tx.RejectLate(origins[1], server.CodePermissionDenied, strings.Repeat("x", 70<<10))
			})).To(Succeed())

			Expect(lateIDs("c1", before.GetNextCursor())).To(HaveKey(uint64(1)),
				"the message may be cut short, but the rejection must arrive")
		})
	})
})
