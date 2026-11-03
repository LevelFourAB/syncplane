package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"google.golang.org/protobuf/proto"
)

// countlessStore hands fn a ReadTx that does not implement
// storage.RowCounter, which is the shape of a third-party adapter that cannot
// count.
type countlessStore struct{ storage.Adapter }

func (s countlessStore) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	return s.Adapter.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		return fn(struct{ storage.ReadTx }{tx})
	})
}

// hideOneTodo hides one todo from every caller.
type hideOneTodo struct {
	server.AllowAll
	key string
}

func (h hideOneTodo) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if entity != "todo" {
		return nil, nil
	}
	return func(key string, _ proto.Message) bool { return key != h.key }, nil
}

// The row estimates on Begin: the live rows of each announced entity, which a
// client divides the rows it applied by to show progress, sent only when the
// snapshot ships every one of them.
var _ = Describe("Bootstrap row estimates", func() {
	var ctx context.Context

	connectTo := func(srv *server.Server) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	// seed writes five todos and two widgets, then deletes one todo, so the
	// workspace holds four live todos, a tombstone and two widgets.
	seed := func(client syncplanev1connect.SyncServiceClient) {
		GinkgoHelper()
		var muts []*syncplanev1.Mutation
		for i := 1; i <= 5; i++ {
			id := fmt.Sprintf("t%d", i)
			muts = append(muts, putMutation("c1", uint64(i), "todo", id, &todov1.Todo{Id: id, Completed: i%2 == 0}))
		}
		muts = append(muts,
			putMutation("c1", 6, "widget", "w1", &testdatav1.Widget{Id: "w1"}),
			putMutation("c1", 7, "widget", "w2", &testdatav1.Widget{Id: "w2"}),
			deleteMutation("c1", 8, "todo", "t5"),
		)
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		pushResults(resp.Msg)
	}

	// run reads one Bootstrap: its Begin, the rows it shipped and the last
	// resume token, stopping after `chunks` chunks when that is above 0.
	run := func(client syncplanev1connect.SyncServiceClient, req *syncplanev1.BootstrapRequest, chunks int) (*syncplanev1.BootstrapResponse_Begin, int, []byte) {
		GinkgoHelper()
		stream, err := client.Bootstrap(ctx, connect.NewRequest(req))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(stream.Close()).To(Succeed()) }()
		var (
			begin *syncplanev1.BootstrapResponse_Begin
			rows  int
			token []byte
			seen  int
		)
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				begin = body.Begin
			case *syncplanev1.BootstrapResponse_Chunk_:
				rows += len(body.Chunk.GetRows())
				token = body.Chunk.GetResumeToken()
				seen++
				if chunks > 0 && seen == chunks {
					return begin, rows, token
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		return begin, rows, token
	}

	// estimates maps each announced entity to its estimate.
	estimates := func(begin *syncplanev1.BootstrapResponse_Begin) map[string]uint64 {
		GinkgoHelper()
		Expect(begin.GetRowEstimates()).To(HaveLen(len(begin.GetEntities())))
		out := map[string]uint64{}
		for i, e := range begin.GetEntities() {
			out[e] = begin.GetRowEstimates()[i]
		}
		return out
	}

	BeforeEach(func() { ctx = context.Background() })

	It("counts the live rows of every announced entity, in the announced order", func() {
		client := connectTo(newTestServer())
		seed(client)

		begin, rows, _ := run(client, &syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 0)
		got := estimates(begin)
		Expect(got).To(HaveKeyWithValue("todo", uint64(4)), "the tombstone is not counted")
		Expect(got).To(HaveKeyWithValue("widget", uint64(2)))
		Expect(got).To(HaveKeyWithValue("project", uint64(0)), "an empty entity is expected to ship nothing")
		var total uint64
		for _, n := range begin.GetRowEstimates() {
			total += n
		}
		Expect(total).To(BeEquivalentTo(rows), "with nothing written during the snapshot, the estimate is exact")
	})

	It("gives no estimate when a subscription narrows an entity", func() {
		client := connectTo(newTestServer())
		seed(client)

		begin, rows, _ := run(client, &syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Subscriptions: []*syncplanev1.Subscription{{
				Entity:    "todo",
				Predicate: query.Eq("completed", true),
			}},
		}, 0)
		Expect(begin.GetEntities()).To(Equal([]string{"todo"}))
		Expect(begin.GetRowEstimates()).To(BeEmpty(), "a count of the whole entity costs more than the narrowed snapshot")
		Expect(rows).To(Equal(2), "t2 and t4 match")
	})

	It("counts a subscription that covers whole entities", func() {
		client := connectTo(newTestServer())
		seed(client)

		begin, rows, _ := run(client, &syncplanev1.BootstrapRequest{
			WorkspaceId:   ws,
			ClientId:      "c1",
			Subscriptions: []*syncplanev1.Subscription{{Entity: "todo"}, {Entity: "widget"}},
		}, 0)
		Expect(estimates(begin)).To(Equal(map[string]uint64{"todo": 4, "widget": 2}))
		Expect(rows).To(Equal(6))
	})

	It("gives no estimate when a read filter hides rows from the caller", func() {
		client := connectTo(newTestServer(server.WithPermissions(hideOneTodo{key: "t1"})))
		seed(client)

		begin, rows, _ := run(client, &syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 0)
		Expect(begin.GetRowEstimates()).To(BeEmpty(), "a count would tell the caller that a hidden row exists")
		Expect(rows).To(Equal(5))
	})

	It("gives no estimate to a caller the access model limits, and counts for one it does not", func() {
		f := newAccessFixture()
		f.seedOrg()
		bootstrapAs := func(user string) *syncplanev1.BootstrapResponse_Begin {
			GinkgoHelper()
			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-" + user})
			as(user, req.Header())
			stream, err := f.sync.Bootstrap(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(stream.Close()).To(Succeed()) }()
			var begin *syncplanev1.BootstrapResponse_Begin
			for stream.Receive() {
				if b := stream.Msg().GetBegin(); b != nil {
					begin = b
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			return begin
		}

		Expect(bootstrapAs("guest").GetRowEstimates()).To(BeEmpty(), "guest reads only project p1")
		admin := bootstrapAs("admin")
		Expect(admin.GetRowEstimates()).To(HaveLen(len(admin.GetEntities())), "admin reads every row")
	})

	It("counts the whole snapshot again on a resumed attempt", func() {
		client := connectTo(newTestServer(server.WithChunkSize(2)))
		seed(client)
		req := &syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}
		first, firstRows, token := run(client, req, 1)
		Expect(firstRows).To(Equal(2))

		// A row written between the attempts is counted, and not shipped: it
		// was created after the snapshot's Begin.
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", 9, "widget", "w3", &testdatav1.Widget{Id: "w3"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		pushResults(resp.Msg)

		second, rest, _ := run(client, &syncplanev1.BootstrapRequest{
			WorkspaceId: ws,
			ClientId:    "c1",
			Resume:      &syncplanev1.BootstrapResume{Cursor: first.GetCursor(), Token: token},
		}, 0)
		Expect(second.GetResumed()).To(BeTrue())
		Expect(second.GetEntities()).To(Equal(first.GetEntities()))
		Expect(estimates(first)).To(HaveKeyWithValue("widget", uint64(2)))
		Expect(estimates(second)).To(HaveKeyWithValue("widget", uint64(3)))
		Expect(estimates(second)).To(HaveKeyWithValue("todo", uint64(4)),
			"the rows the first attempt shipped are still counted")
		Expect(firstRows+rest).To(Equal(6), "both attempts together ship the snapshot once")
	})

	It("gives no estimate when the store's ReadTx cannot count", func() {
		client := connectTo(mustNewServer(countlessStore{newTestStore()}, testEntities()))
		seed(client)

		begin, rows, _ := run(client, &syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}, 0)
		Expect(begin.GetEntities()).NotTo(BeEmpty())
		Expect(begin.GetRowEstimates()).To(BeEmpty())
		Expect(rows).To(Equal(6), "the snapshot is unchanged")
	})
})
