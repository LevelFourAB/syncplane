package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// fakeStateStore is a map-backed cursorstate.Store the specs steer: failPuts
// simulates an outage, wipe simulates eviction, puts counts the entries the
// server actually wrote.
type fakeStateStore struct {
	mu       sync.Mutex
	entries  map[string][]byte
	puts     int
	failPuts bool
}

func newFakeStateStore() *fakeStateStore {
	return &fakeStateStore{entries: map[string][]byte{}}
}

func (f *fakeStateStore) key(ws string, ref cursorstate.Ref) string {
	return fmt.Sprintf("%s/%x/%x", ws, ref.ID, ref.Gen)
}

func (f *fakeStateStore) Get(_ context.Context, ws string, ref cursorstate.Ref) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.entries[f.key(ws, ref)]
	if !ok {
		return nil, cursorstate.ErrNotFound
	}
	return state, nil
}

func (f *fakeStateStore) Put(_ context.Context, ws string, ref cursorstate.Ref, state []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPuts {
		return fmt.Errorf("store down")
	}
	f.puts++
	f.entries[f.key(ws, ref)] = state
	return nil
}

func (f *fakeStateStore) setFailPuts(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPuts = fail
}

func (f *fakeStateStore) wipe() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = map[string][]byte{}
}

func (f *fakeStateStore) writes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

var _ = Describe("Cursor state store", func() {
	var (
		ctx     context.Context
		state   *fakeStateStore
		client  syncplanev1connect.SyncServiceClient
		nextMut uint64
	)

	// Keys are padded so a cursor inlining the window is unmistakably larger
	// than one referencing it.
	widgetKey := func(i int) string {
		return fmt.Sprintf("widget-%06d-%s", i, strings.Repeat("x", 24))
	}

	const windowLimit = 40

	windowed := func() *syncplanev1.Subscription {
		return &syncplanev1.Subscription{
			Entity: "widget",
			Window: &syncplanev1.Window{OrderBy: countDesc(), Limit: windowLimit},
		}
	}

	serve := func(srv *server.Server) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	BeforeEach(func() {
		ctx = context.Background()
		state = newFakeStateStore()
		nextMut = 0
		client = serve(newTestServer(server.WithCursorStateStore(state)))
	})

	putWidget := func(c syncplanev1connect.SyncServiceClient, i int, count int32) {
		GinkgoHelper()
		id := widgetKey(i)
		nextMut++
		resp, err := c.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", nextMut, "widget", id, &testdatav1.Widget{Id: id, Count: count}),
		})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
	}

	bootstrapCursor := func(c syncplanev1connect.SyncServiceClient) []byte {
		GinkgoHelper()
		stream, err := c.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
			WorkspaceId: ws, ClientId: "c1", Subscriptions: []*syncplanev1.Subscription{windowed()},
		}))
		Expect(err).NotTo(HaveOccurred())
		var cursor []byte
		for stream.Receive() {
			if begin := stream.Msg().GetBegin(); begin != nil {
				cursor = begin.GetCursor()
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(cursor).NotTo(BeEmpty())
		return cursor
	}

	pull := func(c syncplanev1connect.SyncServiceClient, cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := c.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c1", Cursor: cursor,
			Subscriptions: []*syncplanev1.Subscription{windowed()},
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	decode := func(raw []byte) *syncplanecursorv1.Cursor {
		GinkgoHelper()
		var c syncplanecursorv1.Cursor
		Expect(proto.Unmarshal(raw, &c)).To(Succeed())
		return &c
	}

	fill := func(c syncplanev1connect.SyncServiceClient) {
		GinkgoHelper()
		for i := range windowLimit {
			putWidget(c, i, int32(i))
		}
	}

	It("issues cursors that reference their sets instead of restating them", func() {
		fill(client)
		cursor := bootstrapCursor(client)

		c := decode(cursor)
		Expect(c.GetSetsRef().GetId()).NotTo(BeZero())
		Expect(c.GetSubscriptions()).To(BeEmpty())
		Expect(c.GetWindows()).To(BeEmpty())
		Expect(c.GetClosure()).To(BeNil())
		// The point of the exercise: the window alone is ~40 keys of ~38 bytes.
		Expect(len(cursor)).To(BeNumerically("<", 100))
		Expect(state.writes()).To(BeNumerically(">", 0))
	})

	It("reuses the entry while membership stands still and writes one when it moves", func() {
		fill(client)
		cursor := bootstrapCursor(client)
		before := state.writes()

		idle := pull(client, cursor)
		Expect(idle.GetReset_()).To(BeFalse())
		Expect(decode(idle.GetNextCursor()).GetSetsRef()).To(Equal(decode(cursor).GetSetsRef()),
			"an idle pull must reissue the ref, not a new entry")
		Expect(state.writes()).To(Equal(before), "an idle pull must not write state")

		// A row outranking every member enters the window and evicts the tail:
		// membership moved, so the next cursor names a new entry.
		putWidget(client, windowLimit+1, int32(windowLimit+100))
		after := pull(client, idle.GetNextCursor())
		Expect(after.GetReset_()).To(BeFalse())
		Expect(state.writes()).To(Equal(before+1))
		ref := decode(after.GetNextCursor()).GetSetsRef()
		Expect(ref.GetId()).To(Equal(decode(cursor).GetSetsRef().GetId()),
			"one client's cursors share a lineage")
		Expect(ref.GetGen()).NotTo(Equal(decode(cursor).GetSetsRef().GetGen()))
	})

	It("resets a client whose entry was evicted", func() {
		fill(client)
		cursor := bootstrapCursor(client)
		state.wipe()
		resp := pull(client, cursor)
		Expect(resp.GetReset_()).To(BeTrue())
	})

	It("inlines the sets while the store cannot be written, and recovers", func() {
		fill(client)
		cursor := bootstrapCursor(client)

		state.setFailPuts(true)
		putWidget(client, windowLimit+1, int32(windowLimit+100))
		degraded := pull(client, cursor)
		Expect(degraded.GetReset_()).To(BeFalse())
		fat := decode(degraded.GetNextCursor())
		Expect(fat.GetSetsRef()).To(BeNil())
		Expect(fat.GetWindows()).NotTo(BeEmpty(), "the sets must ride inline when the store is down")
		Expect(len(degraded.GetNextCursor())).To(BeNumerically(">", 10*len(cursor)))

		// The fat cursor keeps working against the still-degraded store, and
		// the first pull after recovery moves its sets back into it.
		Expect(pull(client, degraded.GetNextCursor()).GetReset_()).To(BeFalse())
		state.setFailPuts(false)
		recovered := pull(client, degraded.GetNextCursor())
		Expect(recovered.GetReset_()).To(BeFalse())
		Expect(decode(recovered.GetNextCursor()).GetSetsRef().GetId()).NotTo(BeZero())
	})

	It("adopts a cursor issued by a server without a store", func() {
		store := newTestStore()
		legacy := serve(mustNewServer(store, testEntities(), server.WithCursorStateStore(nil)))
		modern := serve(mustNewServer(store, testEntities(), server.WithCursorStateStore(state)))

		fill(legacy)
		cursor := bootstrapCursor(legacy)
		Expect(decode(cursor).GetSetsRef()).To(BeNil())

		resp := pull(modern, cursor)
		Expect(resp.GetReset_()).To(BeFalse())
		Expect(resp.GetPatch().GetChanges()).To(BeEmpty(),
			"adopting a cursor is a representation change, never a data change")
		Expect(decode(resp.GetNextCursor()).GetSetsRef().GetId()).NotTo(BeZero())
	})

	It("takes the adapter-provided store when none is configured", func() {
		// newTestServer builds plainly on sqlitestore, which is a
		// cursorstate.Provider — so slim cursors need no configuration at all.
		plain := serve(newTestServer())
		fill(plain)
		Expect(decode(bootstrapCursor(plain)).GetSetsRef().GetId()).NotTo(BeZero())
	})
})
