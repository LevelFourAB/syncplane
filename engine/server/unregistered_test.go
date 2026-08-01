package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A store outlives the catalog that filled it: a build that stops declaring an
// entity leaves its rows behind, and nothing removes them. Such a row cannot be
// decoded, filtered or explained, so no sync path emits it — these specs pin
// that the forward pull scan agrees with bootstrap and resync, which never see
// those rows at all because they iterate the registered entities.
var _ = Describe("Rows for unregistered entities", func() {
	const mystery = "mystery"

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
		store  storage.Adapter
	)

	// plantMystery writes rows for an entity the catalog does not hold, straight
	// to storage — the state a dropped entity declaration leaves behind.
	plantMystery := func() {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			seq, err := tx.NextSeq()
			if err != nil {
				return err
			}
			if err := tx.PutRow(mystery, "m1", []byte{0xde, 0xad}, seq, storage.Actor{}); err != nil {
				return err
			}
			seq, err = tx.NextSeq()
			if err != nil {
				return err
			}
			return tx.DeleteRow(mystery, "m2", seq, storage.Actor{})
		})).To(Succeed())
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

	entitiesOf := func(changes []*syncplanev1.RowChange) []string {
		out := make([]string, 0, len(changes))
		for _, rc := range changes {
			out = append(out, rc.GetEntity())
		}
		return out
	}

	BeforeEach(func() {
		ctx = context.Background()
		srv, adapter := newTestServerStore()
		store = adapter
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", 1, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		pushResults(resp.Msg)
		plantMystery()
	})

	// The full-sync client under the default AllowAll permissions is the case
	// that used to leak: no read filter and no predicate means classify took its
	// zero-decode fast path, which answered "visible" before ever asking whether
	// the entity was registered.
	It("are not pulled by a full-sync client", func() {
		changes := pull(nil).GetPatch().GetChanges()
		Expect(entitiesOf(changes)).To(ConsistOf("widget"))
	})

	It("are not streamed by a bootstrap", func() {
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
		}))
		Expect(err).NotTo(HaveOccurred())
		var entities []string
		for stream.Receive() {
			if chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_); ok {
				entities = append(entities, entitiesOf(chunk.Chunk.GetRows())...)
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(entities).To(ConsistOf("widget"))
	})

	// Skipped rows must still advance the cursor past themselves, or a client
	// whose page ends on one would ask for the same span forever. The planted
	// rows sit at the highest sequences, so the resumed pull proves the cursor
	// cleared them.
	It("still advance the pull cursor", func() {
		first := pull(nil)
		Expect(pull(first.GetNextCursor()).GetPatch().GetChanges()).To(BeEmpty())
	})

	// Nothing here is destructive: a client that synced the entity while it was
	// still declared keeps its rows, so re-declaring it does not have to recover
	// data a tombstone would have deleted below every client's cursor.
	It("are not tombstoned", func() {
		for _, rc := range pull(nil).GetPatch().GetChanges() {
			Expect(rc.GetEntity()).NotTo(Equal(mystery))
		}
	})

	It("stay invisible with a read filter installed", func() {
		srv, adapter := newTestServerStore(server.WithPermissions(filteringPerms{}))
		store = adapter
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		plantMystery()

		Expect(pull(nil).GetPatch().GetChanges()).To(BeEmpty())
	})
})
