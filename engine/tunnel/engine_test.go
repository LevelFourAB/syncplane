package tunnel_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("the sync engine behind a tunnel", func() {
	var (
		mux      *http.ServeMux
		tunneled syncplanev1connect.SyncServiceClient
		direct   syncplanev1connect.SyncServiceClient
		s        *served
	)

	BeforeEach(func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "sync.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)
		srv, err := server.New(store, server.WithEntities(&todov1.Todo{}), server.WithChunkSize(2))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(srv.Close)

		mux = http.NewServeMux()
		for path, h := range srv.Handlers() {
			mux.Handle(path, h)
		}
		s = serve(mux)
		tunneled = syncplanev1connect.NewSyncServiceClient(s.http, "http://tunnel")

		hs := httptest.NewServer(mux)
		DeferCleanup(hs.Close)
		direct = syncplanev1connect.NewSyncServiceClient(hs.Client(), hs.URL)
	})

	push := func(client syncplanev1connect.SyncServiceClient, id string, mutation uint64) {
		value, err := proto.Marshal(&todov1.Todo{Id: id, Title: "title " + id})
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Push(context.Background(), connect.NewRequest(&syncplanev1.PushRequest{
			Batches: []*syncplanev1.WorkspaceBatch{{
				WorkspaceId: "ws",
				Mutations: []*syncplanev1.Mutation{{
					ClientId: "c1", MutationId: mutation,
					Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
						Entity: "todo", Key: id, Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value,
					}},
				}},
			}},
		}))
		Expect(err).NotTo(HaveOccurred())
	}

	It("serves unary calls: a push through the tunnel reads back over HTTP and the reverse", func() {
		push(tunneled, "a", 1)
		push(direct, "b", 2)

		for _, client := range []syncplanev1connect.SyncServiceClient{tunneled, direct} {
			resp, err := client.Pull(context.Background(),
				connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: "ws"}))
			Expect(err).NotTo(HaveOccurred())
			var keys []string
			for _, ch := range resp.Msg.GetPatch().GetChanges() {
				keys = append(keys, ch.GetKey())
			}
			Expect(keys).To(ConsistOf("a", "b"))
		}
	})

	It("serves server-streaming calls in several messages", func() {
		for i, id := range []string{"a", "b", "c", "d", "e"} {
			push(tunneled, id, uint64(i+1))
		}
		stream, err := tunneled.Bootstrap(context.Background(),
			connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: "ws"}))
		Expect(err).NotTo(HaveOccurred())
		defer stream.Close()
		messages, rows := 0, 0
		for stream.Receive() {
			messages++
			rows += len(stream.Msg().GetChunk().GetRows())
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(rows).To(Equal(5))
		// A chunk size of 2 makes the snapshot span several stream messages.
		Expect(messages).To(BeNumerically(">", 1))
	})

	It("passes the engine's error codes through unchanged", func() {
		req := func() *connect.Request[syncplanev1.PullRequest] {
			return connect.NewRequest(&syncplanev1.PullRequest{})
		}
		_, directErr := direct.Pull(context.Background(), req())
		Expect(directErr).To(HaveOccurred())
		_, tunnelErr := tunneled.Pull(context.Background(), req())
		Expect(connect.CodeOf(tunnelErr)).To(Equal(connect.CodeOf(directErr)))
	})

	It("reports a closed tunnel as unavailable", func() {
		Expect(s.client.Close()).To(Succeed())
		_, err := tunneled.Pull(context.Background(),
			connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: "ws"}))
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnavailable))
	})
})
