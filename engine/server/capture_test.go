package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// captureConsumer is the consumer name the specs' captures keep shadows under.
const captureConsumer = "syncplane.capture-test"

var _ = Describe("Write capture", func() {
	var (
		ctx    context.Context
		srv    *server.Server
		store  storage.Adapter
		client syncplanev1connect.SyncServiceClient
		// captured holds the writes of the attempt that committed: a capture
		// that runs again on a retried transaction replaces what an abandoned
		// attempt recorded for the same sequence.
		captured []server.CapturedWrite
		// reject, when it returns a non-nil error, is what the capture returns
		// for a write.
		reject func(w *server.CapturedWrite) error
	)

	// capture records each write, and keeps a shadow per write keyed by its
	// first op (or its mutator) so that a spec can see what committed.
	capture := func(_ context.Context, w *server.CapturedWrite) error {
		if err := reject(w); err != nil {
			return err
		}
		c := *w
		c.Ops = append([]server.CapturedOp(nil), w.Ops...)
		c.Tx = nil
		if c.Seq != 0 {
			captured = slices.DeleteFunc(captured, func(o server.CapturedWrite) bool { return o.Seq == c.Seq })
		}
		captured = append(captured, c)
		key := "custom"
		if len(w.Ops) > 0 {
			key = w.Ops[0].Key
		}
		return w.Tx.PutConsumerShadow(captureConsumer, "#captured", key, []byte("x"))
	}

	newServerWith := func(opts ...server.Option) {
		srv, store = newTestServerStore(append([]server.Option{server.WithWriteCapture(capture)}, opts...)...)
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	BeforeEach(func() {
		ctx = context.Background()
		captured = nil
		reject = func(*server.CapturedWrite) error { return nil }
		newServerWith()
	})

	push := func(muts ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	shadowed := func(key string) bool {
		GinkgoHelper()
		var found bool
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			b, err := tx.ConsumerShadow(captureConsumer, "#captured", key)
			found = b != nil
			return err
		})).To(Succeed())
		return found
	}

	stored := func(entity, key string) storage.Row {
		GinkgoHelper()
		var row storage.Row
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			row, err = tx.GetRow(entity, key)
			return err
		})).To(Succeed())
		return row
	}

	Describe("a client push", func() {
		It("captures each mutation once with what it wrote", func() {
			res := push(
				putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1", Label: "a"}),
				withExpectedVersion(patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Label: "b"}, "label"), 1),
				deleteMutation("c1", 3, "widget", "w1"),
			)
			Expect(res).To(HaveLen(3))
			Expect(captured).To(HaveLen(3))

			put, patch, del := captured[0], captured[1], captured[2]
			Expect(put.Workspace).To(Equal(ws))
			Expect(put.Seq).To(Equal(res[0].GetSeq()))
			Expect(put.Custom).To(BeNil())
			Expect(put.Ops).To(HaveLen(1))
			Expect(put.Ops[0].Op).To(Equal(syncplanev1.CrudOp_CRUD_OP_PUT))
			Expect(put.Ops[0].Entity).To(Equal("widget"))
			Expect(put.Ops[0].Key).To(Equal("w1"))
			Expect(put.Ops[0].PrevVersion).To(BeZero())
			Expect(proto.Equal(decodeWidget(put.Ops[0].Value), &testdatav1.Widget{Id: "w1", Label: "a"})).To(BeTrue())
			Expect(proto.Equal(decodeWidget(put.Ops[0].After), &testdatav1.Widget{Id: "w1", Label: "a"})).To(BeTrue())

			Expect(patch.Ops).To(HaveLen(1))
			Expect(patch.Ops[0].Op).To(Equal(syncplanev1.CrudOp_CRUD_OP_PATCH))
			Expect(patch.Ops[0].Mask).To(Equal([]string{"label"}))
			Expect(patch.Ops[0].ExpectedVersion).To(HaveValue(BeEquivalentTo(1)))
			Expect(patch.Ops[0].PrevVersion).To(Equal(res[0].GetSeq()))
			Expect(proto.Equal(decodeWidget(patch.Ops[0].After), &testdatav1.Widget{Id: "w1", Label: "b"})).To(BeTrue())

			Expect(del.Ops).To(HaveLen(1))
			Expect(del.Ops[0].Op).To(Equal(syncplanev1.CrudOp_CRUD_OP_DELETE))
			Expect(del.Ops[0].PrevVersion).To(Equal(res[1].GetSeq()))
			Expect(del.Ops[0].Value).To(BeNil())
			Expect(del.Ops[0].After).To(BeNil())
			Expect(shadowed("w1")).To(BeTrue(), "what the capture writes commits with the mutation")
		})

		It("captures a batch mutation as one write of every op", func() {
			push(&syncplanev1.Mutation{
				ClientId: "c1", MutationId: 1,
				Payload: &syncplanev1.Mutation_Batch{Batch: &syncplanev1.BatchMutation{Mutations: []*syncplanev1.CrudMutation{
					crudPut("widget", "w1", &testdatav1.Widget{Id: "w1"}),
					crudPut("widget", "w2", &testdatav1.Widget{Id: "w2"}),
				}}},
			})
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Ops).To(HaveLen(2))
			Expect(captured[0].Ops[1].Key).To(Equal("w2"))
		})

		It("rolls a mutation back with the capture's rejection and keeps its neighbours", func() {
			reject = func(w *server.CapturedWrite) error {
				if w.Ops[0].Key == "w2" {
					// The capture's own write must not survive its rejection.
					Expect(w.Tx.PutConsumerShadow(captureConsumer, "#captured", "w2", []byte("x"))).To(Succeed())
					return server.Reject(server.CodePermissionDenied, "not forwarded")
				}
				return nil
			}
			res := push(
				putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
				putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2"}),
				putMutation("c1", 3, "widget", "w3", &testdatav1.Widget{Id: "w3"}),
			)
			Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(res[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(res[1].GetErrorCode()).To(Equal(server.CodePermissionDenied))
			Expect(res[2].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

			Expect(shadowed("w1")).To(BeTrue())
			Expect(shadowed("w2")).To(BeFalse(), "the rejected mutation's savepoint took the capture's write")
			Expect(shadowed("w3")).To(BeTrue())
			var rowErr error
			Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				_, rowErr = tx.GetRow("widget", "w2")
				return nil
			})).To(Succeed())
			Expect(rowErr).To(MatchError(storage.ErrNotFound))

			res = push(putMutation("c1", 4, "widget", "w4", &testdatav1.Widget{Id: "w4"}))
			Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
				"a rejection is an ack, so the watermark moved past it")
		})

		It("leaves nothing of a mutation the engine rejects", func() {
			res := push(
				putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
				withExpectedVersion(putMutation("c1", 2, "widget", "w2", &testdatav1.Widget{Id: "w2"}), 7),
			)
			Expect(res[1].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(shadowed("w2")).To(BeFalse())
			Expect(captured).To(HaveLen(1), "the guarded run replaced the unguarded one")
		})

		It("fails the batch on a capture error that is not a rejection", func() {
			reject = func(*server.CapturedWrite) error { return errors.New("link state unreadable") }
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			})))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeInternal))
			Expect(shadowed("w1")).To(BeFalse())
		})

		It("captures a custom mutation with the rows its mutator wrote", func() {
			newServerWith(server.WithMutator("relabel", &testdatav1.Widget{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Put(args)
				}))
			res := push(customMutation("c1", 1, "relabel", &testdatav1.Widget{Id: "w1", Label: "z"}))
			Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Custom.GetName()).To(Equal("relabel"))
			Expect(captured[0].Ops).To(HaveLen(1))
			Expect(captured[0].Ops[0].Key).To(Equal("w1"))
			Expect(captured[0].Seq).To(Equal(res[0].GetSeq()))
		})
	})

	Describe("in-process writes", func() {
		It("captures Apply", func() {
			Expect(srv.Apply(server.Trusted(ctx), ws, func(tx *server.ApplyTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"})
			})).To(Succeed())
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Ops[0].Key).To(Equal("w1"))
			Expect(captured[0].Seq).To(Equal(stored("widget", "w1").Version))
			Expect(captured[0].Actor.System).To(Equal("system"))
		})

		It("captures ApplyIdempotent once and not its replay", func() {
			for range 2 {
				_, err := srv.ApplyIdempotent(server.Trusted(ctx), ws, "k", func(tx *server.ApplyTx) ([]byte, error) {
					return nil, tx.Put(&testdatav1.Widget{Id: "w1"})
				})
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(captured).To(HaveLen(1))
		})

		It("captures Mutate, with the writes that landed", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				Expect(tx.Put(&testdatav1.Widget{Id: "w1"})).To(Succeed())
				// A refused write is the body's to take as a "no".
				_, refused := server.AsRejection(tx.Patch(&testdatav1.Widget{Id: "gone", Label: "x"}, []string{"label"}, nil))
				Expect(refused).To(BeTrue())
				return tx.Delete(&testdatav1.Widget{}, "w1")
			})).To(Succeed())
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Ops).To(HaveLen(2))
			Expect(captured[0].Ops[0].Op).To(Equal(syncplanev1.CrudOp_CRUD_OP_PUT))
			Expect(captured[0].Ops[1].Op).To(Equal(syncplanev1.CrudOp_CRUD_OP_DELETE))
			Expect(captured[0].Ops[1].PrevVersion).To(Equal(captured[0].Seq),
				"the delete read the row the put wrote at the same sequence")
		})

		It("captures MutateIdempotent once", func() {
			for range 2 {
				_, err := srv.MutateIdempotent(server.Trusted(ctx), ws, "k", func(tx *server.EntityTx) ([]byte, error) {
					return nil, tx.Put(&testdatav1.Widget{Id: "w1"})
				})
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(captured).To(HaveLen(1))
		})

		It("does not capture a call that writes nothing", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(*server.EntityTx) error { return nil })).To(Succeed())
			Expect(captured).To(BeEmpty())
		})

		It("returns the capture's rejection and writes nothing", func() {
			reject = func(*server.CapturedWrite) error { return server.Reject(server.CodePermissionDenied, "pull only") }
			err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1"})
			})
			r, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue())
			Expect(r.Code).To(Equal(server.CodePermissionDenied))
			seq, err := srv.CurrentSeq(ctx, ws)
			Expect(err).NotTo(HaveOccurred())
			Expect(seq).To(BeZero())
		})

		It("reports a plain-string text value as the document it resolved to", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: "w1"}, server.WithTextString("body", "hello"), server.WithTextPeer("p1"))
			})).To(Succeed())
			Expect(captured).To(HaveLen(1))
			s, err := text.PlainText(decodeWidget(captured[0].Ops[0].Value).GetBody())
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal("hello"))
		})

		It("captures Invoke with the custom mutation it ran", func() {
			newServerWith(server.WithMutator("relabel", &testdatav1.Widget{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					return tx.Put(args)
				}))
			Expect(srv.Invoke(server.Trusted(ctx), ws, "relabel", &testdatav1.Widget{Id: "w1"})).To(Succeed())
			_, err := srv.InvokeIdempotent(server.Trusted(ctx), ws, "k", "relabel", &testdatav1.Widget{Id: "w2"})
			Expect(err).NotTo(HaveOccurred())
			Expect(captured).To(HaveLen(2))
			for i, key := range []string{"w1", "w2"} {
				Expect(captured[i].Custom.GetName()).To(Equal("relabel"))
				var args testdatav1.Widget
				Expect(captured[i].Custom.GetArgs().UnmarshalTo(&args)).To(Succeed())
				Expect(args.GetId()).To(Equal(key))
				Expect(captured[i].Ops).To(HaveLen(1))
			}
		})

		It("does not capture Replicate", func() {
			Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
				return tx.Put("widget", "w1", mustMarshal(&testdatav1.Widget{Id: "w1"}))
			})).To(Succeed())
			Expect(captured).To(BeEmpty())
		})
	})

	Describe("unknown mutators", func() {
		It("are refused without forwarding", func() {
			res := push(customMutation("c1", 1, "elsewhere", &testdatav1.Widget{Id: "w1"}))
			Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
			Expect(res[0].GetErrorCode()).To(Equal(server.CodeInvalidArgument))
			Expect(captured).To(BeEmpty())
			Expect(srv.Invoke(server.Trusted(ctx), ws, "elsewhere", nil)).To(MatchError(ContainSubstring("unknown mutator")))
		})

		It("apply as writing nothing and are captured when forwarded", func() {
			newServerWith(server.WithUnknownMutatorForwarding())
			res := push(customMutation("c1", 1, "elsewhere", &testdatav1.Widget{Id: "w1"}))
			Expect(res[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(res[0].GetSeq()).NotTo(BeZero())
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Custom.GetName()).To(Equal("elsewhere"))
			Expect(captured[0].Ops).To(BeEmpty())
			Expect(captured[0].Seq).To(Equal(res[0].GetSeq()))
			Expect(shadowed("custom")).To(BeTrue())

			again := push(customMutation("c1", 1, "elsewhere", &testdatav1.Widget{Id: "w1"}))
			Expect(again[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED))
			Expect(captured).To(HaveLen(1))

			// A pull past the empty sequence still lands on a cursor the next
			// write extends.
			first, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred())
			Expect(first.Msg.GetPatch().GetChanges()).To(BeEmpty())
			push(putMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
			next, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
				WorkspaceId: ws, Cursor: first.Msg.GetNextCursor(),
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(next.Msg.GetPatch().GetChanges()).To(HaveLen(1))
		})

		It("are accepted by Invoke when forwarded", func() {
			newServerWith(server.WithUnknownMutatorForwarding())
			Expect(srv.Invoke(server.Trusted(ctx), ws, "elsewhere", &testdatav1.Widget{Id: "w1"})).To(Succeed())
			Expect(captured).To(HaveLen(1))
			Expect(captured[0].Seq).To(BeZero(), "a call that writes nothing takes no sequence")
			var args testdatav1.Widget
			Expect(anypb.UnmarshalTo(captured[0].Custom.GetArgs(), &args, proto.UnmarshalOptions{})).To(Succeed())
			Expect(args.GetId()).To(Equal("w1"))
		})
	})
})

var _ = Describe("Consumer names", func() {
	It("refuses the prefix the engine reserves", func() {
		srv := newTestServer()
		ctx := context.Background()
		_, _, err := srv.ConsumerPosition(ctx, ws, server.ReservedConsumerPrefix+"upstream")
		Expect(err).To(MatchError(ContainSubstring("reserves")))
		Expect(srv.AdvanceConsumer(ctx, ws, "syncplane.x", server.ConsumerUpdate{})).NotTo(Succeed())
		Expect(srv.ResetConsumer(ctx, ws, "syncplane.x")).NotTo(Succeed())
		_, _, err = srv.ConsumerPosition(ctx, ws, "syncplane-not-reserved")
		Expect(err).NotTo(HaveOccurred())
	})
})

func decodeWidget(b []byte) *testdatav1.Widget {
	GinkgoHelper()
	var w testdatav1.Widget
	Expect(proto.Unmarshal(b, &w)).To(Succeed())
	return &w
}
