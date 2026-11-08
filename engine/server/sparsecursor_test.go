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
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
)

// A Subscribe scope keeps its own position, so next_cursor on a page frame is
// the client's resume point and nothing else. For a windowed subscription that
// resume point carries the whole window membership and dwarfs the row that
// prompted the frame, which is what makes restating it on every frame worth
// avoiding — the steady-state cost of a live subscription should be the row
// that changed, not the row plus the client's entire membership.
var _ = Describe("Sparse subscribe cursors", func() {
	var (
		ctx     context.Context
		cancel  context.CancelFunc
		client  syncplanev1connect.SyncServiceClient
		nextMut uint64
	)

	// Keys are padded so the window's key-set outweighs the page it rides on by
	// enough that the policy's decision is unambiguous rather than marginal.
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

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		nextMut = 0
		// Cursor state disabled, so cursors inline their membership sets:
		// withholding only ever fires on a cursor that outweighs its page, and
		// with a store every cursor is a slim reference the policy correctly
		// never skips. This suite pins the degraded mode, which is also every
		// server without a store.
		srv := newTestServer(server.WithCursorStateStore(nil))
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(func() {
			cancel()
			ts.Close()
		})
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	putWidget := func(i int, count int32) string {
		GinkgoHelper()
		id := widgetKey(i)
		nextMut++
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("c1", nextMut, "widget", id, &testdatav1.Widget{Id: id, Count: count}),
		})))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
		return id
	}

	// bootstrapCursor fills the window and returns the cursor a client would
	// hold coming out of Bootstrap — the position every stream below opens at.
	bootstrapCursor := func() []byte {
		GinkgoHelper()
		for i := range windowLimit {
			putWidget(i, int32(i))
		}
		stream, err := client.Bootstrap(ctx, connect.NewRequest(&syncplanev1.BootstrapRequest{
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

	open := func(cursor []byte, sparse bool) *connect.ServerStreamForClient[syncplanev1.SubscribeResponse] {
		GinkgoHelper()
		stream, err := client.Subscribe(ctx, connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes: []*syncplanev1.WorkspaceScope{{
				WorkspaceId:   ws,
				Cursor:        cursor,
				Subscriptions: []*syncplanev1.Subscription{windowed()},
			}},
			SparseCursor: sparse,
		}))
		Expect(err).NotTo(HaveOccurred())
		return stream
	}

	// awaitCaughtUp reads past the catch-up backlog so the specs observe only
	// the live frames each push produces.
	awaitCaughtUp := func(stream *connect.ServerStreamForClient[syncplanev1.SubscribeResponse]) {
		GinkgoHelper()
		for {
			Expect(stream.Receive()).To(BeTrue(), "stream closed early: %v", stream.Err())
			if stream.Msg().GetCaughtUp() {
				return
			}
		}
	}

	// nextPage reads frames until one carries a page, skipping heartbeats.
	nextPage := func(stream *connect.ServerStreamForClient[syncplanev1.SubscribeResponse]) *syncplanev1.PullResponse {
		GinkgoHelper()
		for {
			Expect(stream.Receive()).To(BeTrue(), "stream closed early: %v", stream.Err())
			if page := stream.Msg().GetPage(); page != nil {
				return page
			}
		}
	}

	It("restates the cursor on every frame for a client that did not opt in", func() {
		stream := open(bootstrapCursor(), false)
		awaitCaughtUp(stream)
		for i := range 5 {
			putWidget(windowLimit+i, int32(windowLimit+i))
			Expect(nextPage(stream).GetNextCursor()).NotTo(BeEmpty(),
				"a client that cannot keep its cursor must be given one on every frame")
		}
	})

	It("withholds the cursor between checkpoints and sends far fewer bytes", func() {
		stream := open(bootstrapCursor(), true)
		awaitCaughtUp(stream)

		// Edits to rows the window holds: a frame that moves the membership
		// carries its cursor.
		const frames = 2 * windowLimit
		var withheld, checkpoints, cursorBytes, largest int
		for i := range frames {
			putWidget(i%windowLimit, int32(windowLimit+i))
			cursor := nextPage(stream).GetNextCursor()
			if len(cursor) == 0 {
				withheld++
				continue
			}
			checkpoints++
			cursorBytes += len(cursor)
			largest = max(largest, len(cursor))
		}

		Expect(withheld).To(BeNumerically(">", 0), "no frame skipped its cursor")
		Expect(checkpoints).To(BeNumerically(">", 0), "the cursor was never checkpointed")
		// The point of the exercise: restating it every frame is what this
		// replaces, so the total has to come in well under that.
		Expect(cursorBytes).To(BeNumerically("<", frames*largest/2))
	})

	It("re-delivers what followed the last checkpoint when the stream is reopened", func() {
		stream := open(bootstrapCursor(), true)
		awaitCaughtUp(stream)

		// Drive frames until one is withheld, so the client's durable position
		// is knowingly behind the rows it has already been sent.
		var checkpoint []byte
		var afterCheckpoint []string
		for i := range 24 {
			id := putWidget(i, int32(windowLimit+i))
			if cursor := nextPage(stream).GetNextCursor(); len(cursor) > 0 {
				checkpoint = cursor
				afterCheckpoint = nil
			} else {
				afterCheckpoint = append(afterCheckpoint, id)
			}
			// Stop on a withheld frame that follows a checkpoint: that is the
			// state where the client holds a cursor older than what it has seen.
			if len(checkpoint) > 0 && len(afterCheckpoint) > 0 {
				break
			}
		}
		Expect(checkpoint).NotTo(BeEmpty())
		Expect(afterCheckpoint).NotTo(BeEmpty(), "no frame was withheld, so nothing was at risk")

		// A client that died here resumes from the checkpoint, not from the
		// last frame it saw — everything after it has to arrive again.
		resumed := open(checkpoint, true)
		seen := map[string]bool{}
		for {
			Expect(resumed.Receive()).To(BeTrue(), "stream closed early: %v", resumed.Err())
			if page := resumed.Msg().GetPage(); page != nil {
				for _, rc := range page.GetPatch().GetChanges() {
					seen[rc.GetKey()] = true
				}
			}
			if resumed.Msg().GetCaughtUp() {
				break
			}
		}
		for _, id := range afterCheckpoint {
			Expect(seen).To(HaveKey(id), "row written after the checkpoint was not re-delivered")
		}
	})

	// Re-delivery after a reopen covers the rows still in scope. A row a
	// withheld frame delivered, or a member it removed, can also change scope
	// while the stream is closed, and the client must still end where a fresh
	// client does.
	Describe("a row that changes scope while the stream is closed", func() {
		// held is a client's rows, by key, with their count.
		type held map[string]int32

		put := func(id string, count int32) {
			GinkgoHelper()
			nextMut++
			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("writer", nextMut, "widget", id, &testdatav1.Widget{Id: id, Count: count}),
			})))
			Expect(err).NotTo(HaveOccurred())
			for _, r := range pushResults(resp.Msg) {
				Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			}
		}
		apply := func(h held, page *syncplanev1.PullResponse) {
			GinkgoHelper()
			for _, rc := range page.GetPatch().GetChanges() {
				if rc.GetTombstone() {
					delete(h, rc.GetKey())
					continue
				}
				var w testdatav1.Widget
				Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
				h[rc.GetKey()] = w.GetCount()
			}
		}
		pullAll := func(h held, cursor []byte, sub *syncplanev1.Subscription) []byte {
			GinkgoHelper()
			for range 50 {
				resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
					WorkspaceId: ws, ClientId: "c1", Cursor: cursor, Subscriptions: []*syncplanev1.Subscription{sub},
				}))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Msg.GetReset_()).To(BeFalse())
				apply(h, resp.Msg)
				cursor = resp.Msg.GetNextCursor()
				if !resp.Msg.GetHasMore() {
					return cursor
				}
			}
			Fail("the pulls did not settle")
			return nil
		}
		stream := func(streamCtx context.Context, cursor []byte, sub *syncplanev1.Subscription) *connect.ServerStreamForClient[syncplanev1.SubscribeResponse] {
			GinkgoHelper()
			s, err := client.Subscribe(streamCtx, connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId:     "c1",
				Scopes:       []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: cursor, Subscriptions: []*syncplanev1.Subscription{sub}}},
				SparseCursor: true,
			}))
			Expect(err).NotTo(HaveOccurred())
			return s
		}
		// catchUp applies frames to h until the stream reports caught up, and
		// returns the last cursor a frame carried, or cursor if none did.
		catchUp := func(h held, s *connect.ServerStreamForClient[syncplanev1.SubscribeResponse], cursor []byte) []byte {
			GinkgoHelper()
			for {
				Expect(s.Receive()).To(BeTrue(), "stream closed early: %v", s.Err())
				if page := s.Msg().GetPage(); page != nil {
					apply(h, page)
					if len(page.GetNextCursor()) > 0 {
						cursor = page.GetNextCursor()
					}
				}
				if s.Msg().GetCaughtUp() {
					return cursor
				}
			}
		}
		useStore := func() {
			srv := newTestServer()
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(func() {
				cancel()
				ts.Close()
			})
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		}

		// reopened returns what a client holds, and what a fresh client holds,
		// after it streamed rows from newRow until a frame came without a
		// cursor (or for a few rows, as a frame that changes what the client
		// holds carries its cursor), the stream dropped, change ran on each of
		// those rows, and the client reopened from its last cursor.
		reopened := func(sub *syncplanev1.Subscription, newRow func(int) (string, int32), change func(id string)) (got, fresh held) {
			GinkgoHelper()
			got = held{}
			checkpoint := pullAll(got, nil, sub)
			streamCtx, drop := context.WithCancel(ctx)
			live := stream(streamCtx, checkpoint, sub)
			checkpoint = catchUp(got, live, checkpoint)

			// Given rows streamed until a frame went out without a cursor
			var streamed []string
			withheld := false
			for i := 0; !withheld && i < 5; i++ {
				id, count := newRow(i)
				put(id, count)
				streamed = append(streamed, id)
				var page *syncplanev1.PullResponse
				for page == nil {
					Expect(live.Receive()).To(BeTrue(), "stream closed early: %v", live.Err())
					page = live.Msg().GetPage()
				}
				apply(got, page)
				if len(page.GetNextCursor()) > 0 {
					checkpoint = page.GetNextCursor()
				} else {
					withheld = true
				}
			}

			// When the stream drops, those rows change, and the client reopens
			// from its last cursor
			drop()
			for live.Receive() {
			}
			for _, id := range streamed {
				change(id)
			}
			catchUp(got, stream(ctx, checkpoint, sub), checkpoint)

			fresh = held{}
			pullAll(fresh, nil, sub)
			return got, fresh
		}

		It("removes a row that left a predicate scope", func() {
			useStore()
			put("base", 50)
			got, fresh := reopened(widgetSub(countGte(10)),
				func(i int) (string, int32) { return fmt.Sprintf("r%02d", i), 50 },
				func(id string) { put(id, 1) })

			Expect(got).To(Equal(fresh))
		})

		It("re-delivers a row that stayed in a predicate scope", func() {
			useStore()
			put("base", 50)
			got, fresh := reopened(widgetSub(countGte(10)),
				func(i int) (string, int32) { return fmt.Sprintf("r%02d", i), 50 },
				func(id string) { put(id, 60) })

			Expect(got).To(Equal(fresh))
		})

		It("holds the window a fresh client holds, with inline cursors", func() {
			for i := range windowLimit {
				put(widgetKey(i), int32(100+i))
			}
			got, fresh := reopened(windowed(),
				func(i int) (string, int32) { return widgetKey(1000 + i), int32(1000 + i) },
				// Below every seeded row: it leaves the window.
				func(id string) { put(id, 1) })

			Expect(got).To(Equal(fresh))
		})

		It("holds the window a fresh client holds, with a cursor-state store", func() {
			useStore()
			for i := range 3 {
				put(fmt.Sprintf("s%d", i), int32(100+i))
			}
			got, fresh := reopened(windowSub(3, nil),
				func(i int) (string, int32) { return fmt.Sprintf("n%d", i), int32(1000 + i) },
				func(id string) { put(id, 1) })

			Expect(got).To(Equal(fresh))
		})
	})
})
