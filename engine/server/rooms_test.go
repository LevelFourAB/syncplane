package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/server"
)

// roomAnnounce is short enough that a spec can watch an unannounced record
// expire without the suite noticing the wait.
const roomAnnounce = 50 * time.Millisecond

// roomTopic builds an unanchored topic in the shared test workspace.
func roomTopic(name string) *syncplanev1.Topic {
	return &syncplanev1.Topic{WorkspaceId: ws, Name: name}
}

// anchoredTopic builds a topic anchored to a todo row.
func anchoredTopic(name, key string) *syncplanev1.Topic {
	return &syncplanev1.Topic{WorkspaceId: ws, Name: name, Entity: "todo", Key: key}
}

var _ = Describe("Rooms", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		// opts is what a Context adds to the server it runs against; set it in
		// a BeforeEach, since JustBeforeEach builds the server.
		opts     []server.Option
		presence syncplanev1connect.PresenceServiceClient
		sync     syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		opts = nil
	})

	// Every service is mounted, which is what the presence client needs and
	// what a deployment gets from HTTPHandler.
	JustBeforeEach(func() {
		srv := newTestServer(append([]server.Option{server.WithRoomAnnounce(roomAnnounce)}, opts...)...)
		mux := http.NewServeMux()
		for path, h := range srv.Handlers() {
			mux.Handle(path, h)
		}
		ts := httptest.NewServer(mux)
		// Cancel the context (closing any open Watch stream) BEFORE closing the
		// httptest server, or ts.Close blocks waiting for the live stream.
		// Closing the server last stops its announce sweep, which these specs
		// run at 50ms and would otherwise leave ticking for the whole suite.
		DeferCleanup(func() {
			cancel()
			ts.Close()
			Expect(srv.Close()).To(Succeed())
		})
		presence = syncplanev1connect.NewPresenceServiceClient(ts.Client(), ts.URL)
		sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	watchReq := func(req *syncplanev1.WatchRequest, header http.Header) (*connect.ServerStreamForClient[syncplanev1.WatchResponse], error) {
		r := connect.NewRequest(req)
		for k, vs := range header {
			for _, v := range vs {
				r.Header().Add(k, v)
			}
		}
		return presence.Watch(ctx, r)
	}

	watch := func(session string, topics ...*syncplanev1.Topic) *connect.ServerStreamForClient[syncplanev1.WatchResponse] {
		GinkgoHelper()
		stream, err := watchReq(&syncplanev1.WatchRequest{SessionId: session, Topics: topics}, nil)
		Expect(err).NotTo(HaveOccurred())
		return stream
	}

	recv := func(stream *connect.ServerStreamForClient[syncplanev1.WatchResponse]) *syncplanev1.WatchResponse {
		GinkgoHelper()
		Expect(stream.Receive()).To(BeTrue(), "stream closed early: %v", stream.Err())
		return stream.Msg()
	}

	// recvBody skips heartbeats, which are stream-level noise to every spec
	// that is not asserting on them.
	recvBody := func(stream *connect.ServerStreamForClient[syncplanev1.WatchResponse]) *syncplanev1.WatchResponse {
		GinkgoHelper()
		for {
			if msg := recv(stream); !msg.GetHeartbeat() {
				return msg
			}
		}
	}

	// frames drains a stream in the background, so a spec can assert that
	// something does NOT arrive. Receive blocks with no way to select on it,
	// which makes a plain read useless for a negative assertion. Do not mix it
	// with recv on the same stream.
	frames := func(stream *connect.ServerStreamForClient[syncplanev1.WatchResponse]) <-chan *syncplanev1.WatchResponse {
		out := make(chan *syncplanev1.WatchResponse, 64)
		go func() {
			defer GinkgoRecover()
			defer close(out)
			for stream.Receive() {
				if msg := stream.Msg(); !msg.GetHeartbeat() {
					out <- msg
				}
			}
		}()
		return out
	}

	publishReq := func(req *syncplanev1.PublishRequest, header http.Header) error {
		r := connect.NewRequest(req)
		for k, vs := range header {
			for _, v := range vs {
				r.Header().Add(k, v)
			}
		}
		_, err := presence.Publish(ctx, r)
		return err
	}

	publish := func(session string, items ...*syncplanev1.PublishItem) error {
		return publishReq(&syncplanev1.PublishRequest{SessionId: session, Items: items}, nil)
	}

	publishOK := func(session string, items ...*syncplanev1.PublishItem) {
		GinkgoHelper()
		Expect(publish(session, items...)).To(Succeed())
	}

	stateItem := func(t *syncplanev1.Topic, rev uint64, body string) *syncplanev1.PublishItem {
		return &syncplanev1.PublishItem{
			Topic: t,
			Body: &syncplanev1.PublishItem_Presence{Presence: &syncplanev1.PresenceUpdate{
				State:    []byte(body),
				Revision: rev,
			}},
		}
	}

	Describe("Watch", func() {
		It("sends an empty snapshot for a room nobody is in", func() {
			msg := recvBody(watch("s1", roomTopic("cursors")))
			Expect(msg.GetTopic().GetName()).To(Equal("cursors"))
			Expect(msg.GetSnapshot()).NotTo(BeNil())
			Expect(msg.GetSnapshot().GetParticipants()).To(BeEmpty())
			Expect(msg.GetSnapshot().GetTruncated()).To(BeFalse())
		})

		It("sends one snapshot per topic", func() {
			stream := watch("s1", roomTopic("a"), roomTopic("b"))
			seen := []string{}
			for range 2 {
				msg := recvBody(stream)
				Expect(msg.GetSnapshot()).NotTo(BeNil())
				seen = append(seen, msg.GetTopic().GetName())
			}
			Expect(seen).To(ConsistOf("a", "b"))
		})

		It("delivers a session's own state back to it", func() {
			t := roomTopic("cursors")
			stream := watch("s1", t)
			recvBody(stream) // snapshot

			publishOK("s1", stateItem(t, 1, "hello"))
			msg := recvBody(stream)
			Expect(msg.GetUpdated().GetSessionId()).To(Equal("s1"))
			Expect(string(msg.GetUpdated().GetPresence().GetState())).To(Equal("hello"))
			Expect(msg.GetUpdated().GetPresence().GetRevision()).To(Equal(uint64(1)))
		})

		It("delivers another session's state", func() {
			t := roomTopic("cursors")
			stream := watch("s1", t)
			recvBody(stream)

			publishOK("s2", stateItem(t, 3, "there"))
			msg := recvBody(stream)
			Expect(msg.GetUpdated().GetSessionId()).To(Equal("s2"))
			Expect(msg.GetUpdated().GetPresence().GetRevision()).To(Equal(uint64(3)))
		})

		It("carries the authoring sequence through", func() {
			t := roomTopic("cursors")
			stream := watch("s1", t)
			recvBody(stream)

			publishOK("s2", &syncplanev1.PublishItem{
				Topic: t,
				Body: &syncplanev1.PublishItem_Presence{Presence: &syncplanev1.PresenceUpdate{
					Revision: 1, Seq: 4242,
				}},
			})
			Expect(recvBody(stream).GetUpdated().GetPresence().GetSeq()).To(Equal(uint64(4242)))
		})

		It("shows a later joiner the roster it missed", func() {
			t := roomTopic("cursors")
			first := watch("s1", t)
			recvBody(first)
			publishOK("s1", stateItem(t, 1, "hello"))
			recvBody(first) // the record has landed

			snap := recvBody(watch("s2", t)).GetSnapshot()
			Expect(snap.GetParticipants()).To(HaveLen(1))
			Expect(snap.GetParticipants()[0].GetSessionId()).To(Equal("s1"))
			Expect(string(snap.GetParticipants()[0].GetPresence().GetState())).To(Equal("hello"))
		})

		It("reports an explicit leave", func() {
			t := roomTopic("cursors")
			observer := watch("s2", t)
			recvBody(observer)
			publishOK("s1", stateItem(t, 1, ""))
			recvBody(observer)

			publishOK("s1", &syncplanev1.PublishItem{
				Topic: t,
				Body:  &syncplanev1.PublishItem_Leave{Leave: true},
			})
			Expect(recvBody(observer).GetLeft()).To(Equal("s1"))
		})

		// The whole liveness mechanism: a session that vanishes without saying
		// so is forgotten because nothing re-announced it.
		It("reports a departure for a session that stops being announced", func() {
			t := roomTopic("cursors")
			observer := watch("s2", t)
			recvBody(observer)
			// s1 has no Watch stream, so no node is responsible for its record.
			publishOK("s1", stateItem(t, 1, ""))
			Expect(recvBody(observer).GetUpdated().GetSessionId()).To(Equal("s1"))
			Expect(recvBody(observer).GetLeft()).To(Equal("s1"))
		})

		It("keeps a watched session alive past the expiry window", func() {
			t := roomTopic("cursors")
			held := watch("s1", t)
			recvBody(held)
			publishOK("s1", stateItem(t, 1, ""))
			recvBody(held)

			observer := frames(watch("s2", t))
			var snapshot *syncplanev1.WatchResponse
			Eventually(observer).Should(Receive(&snapshot))
			Expect(snapshot.GetSnapshot().GetParticipants()).To(HaveLen(1))

			// The node holds s1's stream, so it re-announces the record — and a
			// re-announcement of state a watcher already has is not a frame.
			Consistently(observer, 6*roomAnnounce).ShouldNot(Receive(),
				"a session whose stream this node holds must never expire")
		})

		It("delivers a broadcast to every watcher, sender included", func() {
			t := roomTopic("chat")
			a := watch("s1", t)
			b := watch("s2", t)
			recvBody(a)
			recvBody(b)

			publishOK("s1", &syncplanev1.PublishItem{
				Topic: t,
				Body:  &syncplanev1.PublishItem_Message{Message: []byte("ping")},
			})
			for _, stream := range []*connect.ServerStreamForClient[syncplanev1.WatchResponse]{a, b} {
				msg := recvBody(stream)
				Expect(string(msg.GetMessage().GetPayload())).To(Equal("ping"))
				Expect(msg.GetMessage().GetSenderSessionId()).To(Equal("s1"))
			}
		})

		It("does not put a broadcast's sender in the room", func() {
			t := roomTopic("chat")
			stream := watch("s1", t)
			recvBody(stream)
			publishOK("s2", &syncplanev1.PublishItem{
				Topic: t,
				Body:  &syncplanev1.PublishItem_Message{Message: []byte("ping")},
			})
			Expect(recvBody(stream).GetMessage()).NotTo(BeNil())
			Expect(recvBody(watch("s3", t)).GetSnapshot().GetParticipants()).To(BeEmpty())
		})

		It("keeps rooms isolated", func() {
			here := roomTopic("here")
			stream := watch("s1", here)
			recvBody(stream)

			publishOK("s2", stateItem(roomTopic("elsewhere"), 1, ""))
			publishOK("s3", stateItem(here, 1, ""))
			Expect(recvBody(stream).GetUpdated().GetSessionId()).To(Equal("s3"))
		})

		Context("with a short heartbeat", func() {
			BeforeEach(func() { opts = []server.Option{server.WithHeartbeat(50 * time.Millisecond)} })

			It("sends heartbeats", func() {
				stream := watch("s1", roomTopic("cursors"))
				recv(stream) // snapshot
				Expect(recv(stream).GetHeartbeat()).To(BeTrue())
			})
		})

		// A heartbeat that is not positive keeps the default, as it does for
		// Subscribe.
		for _, hb := range []time.Duration{0, -time.Second} {
			Context("with WithHeartbeat("+hb.String()+")", func() {
				BeforeEach(func() { opts = []server.Option{server.WithHeartbeat(hb)} })

				It("keeps the stream open after the snapshot", func() {
					t := roomTopic("cursors")
					stream := watch("s1", t)
					Expect(recvBody(stream).GetSnapshot()).NotTo(BeNil())

					publishOK("s2", stateItem(t, 1, ""))
					Expect(recvBody(stream).GetUpdated().GetSessionId()).To(Equal("s2"))
				})
			})
		}
	})

	Describe("Watch validation", func() {
		// A request-shape failure ends the stream rather than dropping a topic:
		// there is no topic to report it against.
		expectStreamError := func(req *syncplanev1.WatchRequest) error {
			GinkgoHelper()
			stream, err := watchReq(req, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(stream.Receive()).To(BeFalse())
			return stream.Err()
		}

		It("refuses a stream with no session id", func() {
			err := expectStreamError(&syncplanev1.WatchRequest{Topics: []*syncplanev1.Topic{roomTopic("a")}})
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses a stream with no topics", func() {
			err := expectStreamError(&syncplanev1.WatchRequest{SessionId: "s1"})
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses a duplicate topic", func() {
			err := expectStreamError(&syncplanev1.WatchRequest{
				SessionId: "s1",
				Topics:    []*syncplanev1.Topic{roomTopic("a"), roomTopic("a")},
			})
			Expect(err).To(MatchError(ContainSubstring("more than once")))
		})

		It("refuses a malformed room name", func() {
			err := expectStreamError(&syncplanev1.WatchRequest{
				SessionId: "s1",
				Topics:    []*syncplanev1.Topic{{WorkspaceId: ws, Name: ""}},
			})
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses a half-specified anchor", func() {
			err := expectStreamError(&syncplanev1.WatchRequest{
				SessionId: "s1",
				Topics:    []*syncplanev1.Topic{{WorkspaceId: ws, Name: "a", Entity: "todo"}},
			})
			Expect(err).To(MatchError(ContainSubstring("both an entity and a key")))
		})

		Context("with a two-room cap", func() {
			BeforeEach(func() {
				opts = []server.Option{server.WithLimits(server.Limits{MaxRoomsPerStream: 2})}
			})

			It("refuses more rooms than the limit allows", func() {
				err := expectStreamError(&syncplanev1.WatchRequest{
					SessionId: "s1",
					Topics:    []*syncplanev1.Topic{roomTopic("a"), roomTopic("b"), roomTopic("c")},
				})
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
				Expect(err).To(MatchError(ContainSubstring("MaxRoomsPerStream")))
			})
		})
	})

	Describe("Publish", func() {
		It("refuses a request with no items", func() {
			Expect(connect.CodeOf(publish("s1"))).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses a request with no session id", func() {
			err := publish("", stateItem(roomTopic("cursors"), 1, ""))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("refuses an item with no body", func() {
			err := publish("s1", &syncplanev1.PublishItem{Topic: roomTopic("cursors")})
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("accepts a publish from a session watching nothing", func() {
			Expect(publish("s1", stateItem(roomTopic("cursors"), 1, "x"))).To(Succeed())
		})

		// A publisher with no Watch stream has nothing re-announcing its
		// records, so the response tells it how often to refresh them itself.
		It("reports the announce interval", func() {
			res, err := presence.Publish(ctx, connect.NewRequest(&syncplanev1.PublishRequest{
				SessionId: "s1",
				Items:     []*syncplanev1.PublishItem{stateItem(roomTopic("cursors"), 1, "x")},
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Msg.GetAnnounceInterval().AsDuration()).To(Equal(roomAnnounce))
		})

		Context("with tight limits", func() {
			BeforeEach(func() {
				opts = []server.Option{server.WithLimits(server.Limits{
					MaxPublishItems:       2,
					MaxPresenceStateBytes: 8,
					MaxBroadcastBytes:     8,
				})}
			})

			It("refuses more items than the limit allows", func() {
				t := roomTopic("cursors")
				err := publish("s1", stateItem(t, 1, ""), stateItem(t, 2, ""), stateItem(t, 3, ""))
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
				Expect(err).To(MatchError(ContainSubstring("MaxPublishItems")))
			})

			It("refuses oversize presence state", func() {
				err := publish("s1", stateItem(roomTopic("cursors"), 1, strings.Repeat("x", 9)))
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
				Expect(err).To(MatchError(ContainSubstring("MaxPresenceStateBytes")))
			})

			It("refuses an oversize broadcast", func() {
				err := publish("s1", &syncplanev1.PublishItem{
					Topic: roomTopic("chat"),
					Body:  &syncplanev1.PublishItem_Message{Message: []byte(strings.Repeat("x", 9))},
				})
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
				Expect(err).To(MatchError(ContainSubstring("MaxBroadcastBytes")))
			})
		})
	})

	Describe("anchored rooms", func() {
		putTodo := func(key string) {
			GinkgoHelper()
			resp, err := sync.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c-"+key, 1, "todo", key, &todov1.Todo{Id: key, Title: "t"}),
			})))
			Expect(err).NotTo(HaveOccurred())
			for _, r := range pushResults(resp.Msg) {
				Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			}
		}

		It("admits a caller who can read the anchor row", func() {
			putTodo("t1")
			msg := recvBody(watch("s1", anchoredTopic("cursors", "t1")))
			Expect(msg.GetSnapshot()).NotTo(BeNil())
			Expect(msg.GetTopic().GetEntity()).To(Equal("todo"))
			Expect(msg.GetTopic().GetKey()).To(Equal("t1"))
		})

		It("drops a topic whose anchor row does not exist", func() {
			msg := recvBody(watch("s1", anchoredTopic("cursors", "missing")))
			Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
		})

		It("drops a topic whose anchor names an unknown entity", func() {
			msg := recvBody(watch("s1", &syncplanev1.Topic{
				WorkspaceId: ws, Name: "cursors", Entity: "nope", Key: "k",
			}))
			Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
			Expect(msg.GetDropped().GetMessage()).To(ContainSubstring("unknown entity"))
		})

		// The row emitter's readable() answers only the read-filter question,
		// so a server-only entity has to be refused by name — nothing
		// downstream would notice the anchor was meaningless.
		It("drops a topic anchored to a server-only entity", func() {
			msg := recvBody(watch("s1", &syncplanev1.Topic{
				WorkspaceId: ws, Name: "cursors", Entity: "syncplane_field_history", Key: "k",
			}))
			Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
			Expect(msg.GetDropped().GetMessage()).To(ContainSubstring("server-only"))
		})

		It("keeps two anchors of the same room name apart", func() {
			putTodo("t1")
			putTodo("t2")
			stream := watch("s1", anchoredTopic("cursors", "t1"))
			recvBody(stream)

			publishOK("s2", stateItem(anchoredTopic("cursors", "t2"), 1, ""))
			publishOK("s3", stateItem(anchoredTopic("cursors", "t1"), 1, ""))
			Expect(recvBody(stream).GetUpdated().GetSessionId()).To(Equal("s3"))
		})

		Context("behind a read filter", func() {
			BeforeEach(func() {
				opts = []server.Option{server.WithPermissions(hideTodo{key: "secret"})}
			})

			It("admits a readable anchor and drops a hidden one", func() {
				putTodo("secret")
				putTodo("public")
				Expect(recvBody(watch("s1", anchoredTopic("cursors", "public"))).GetSnapshot()).NotTo(BeNil())
				msg := recvBody(watch("s2", anchoredTopic("cursors", "secret")))
				Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
			})

			It("refuses a publish into a room whose anchor the caller cannot read", func() {
				putTodo("secret")
				err := publish("s1", stateItem(anchoredTopic("cursors", "secret"), 1, ""))
				Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
			})
		})
	})

	Describe("the workspace gate", func() {
		Context("with one workspace denied", func() {
			BeforeEach(func() {
				opts = []server.Option{server.WithPermissions(denyWorkspace{ws: "forbidden"})}
			})

			It("drops a topic in a workspace the caller may not enter", func() {
				msg := recvBody(watch("s1", &syncplanev1.Topic{WorkspaceId: "forbidden", Name: "a"}))
				Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
			})

			It("ends the stream when every topic was refused", func() {
				stream := watch("s1", &syncplanev1.Topic{WorkspaceId: "forbidden", Name: "a"})
				Expect(recv(stream).GetDropped()).NotTo(BeNil())
				Expect(stream.Receive()).To(BeFalse())
				Expect(stream.Err()).NotTo(HaveOccurred())
			})

			It("keeps the other topics when one is refused", func() {
				stream := watch("s1",
					&syncplanev1.Topic{WorkspaceId: "forbidden", Name: "a"},
					roomTopic("b"))
				got := map[string]bool{}
				for range 2 {
					msg := recvBody(stream)
					got[msg.GetTopic().GetName()] = msg.GetSnapshot() != nil
				}
				Expect(got).To(Equal(map[string]bool{"a": false, "b": true}))
			})
		})

		// A stream outlives the decision that admitted it, so the gate runs
		// again on the heartbeat.
		Context("with access revoked mid-stream", func() {
			var gate *revocableGate

			BeforeEach(func() {
				gate = &revocableGate{}
				opts = []server.Option{
					server.WithHeartbeat(50 * time.Millisecond),
					server.WithPermissions(gate),
				}
			})

			It("drops the topic within a heartbeat", func() {
				stream := watch("s1", roomTopic("cursors"))
				Expect(recvBody(stream).GetSnapshot()).NotTo(BeNil())

				gate.revoke()
				Eventually(func() syncplanev1.TopicDropped_Reason {
					return recvBody(stream).GetDropped().GetReason()
				}, "5s").Should(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
			})
		})
	})

	Describe("the app's room hooks", func() {
		Context("with a refused room", func() {
			BeforeEach(func() { opts = []server.Option{server.WithPermissions(roomPolicy{deny: "private"})} })

			It("admits the rooms it allows and drops the one it refuses", func() {
				Expect(recvBody(watch("s1", roomTopic("public"))).GetSnapshot()).NotTo(BeNil())
				msg := recvBody(watch("s2", roomTopic("private")))
				Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
			})

			It("refuses a publish into the room it refuses", func() {
				err := publish("s1", stateItem(roomTopic("private"), 1, ""))
				Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
			})
		})

		Context("with a hook that cannot decide", func() {
			BeforeEach(func() { opts = []server.Option{server.WithPermissions(roomPolicy{fail: "flaky"})} })

			// An outage must never read as a revocation: the client backs off
			// and reopens rather than discarding the room.
			It("drops the topic as retryable", func() {
				msg := recvBody(watch("s1", roomTopic("flaky")))
				Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ERROR))
			})
		})

		// Being able to read a row is not the same as being allowed to know who
		// else is looking at it.
		Context("with a redaction hook", func() {
			BeforeEach(func() {
				opts = []server.Option{
					server.WithAuthenticator(subjectAuthenticator{}),
					server.WithPermissions(roomPolicy{hideSubject: "hidden"}),
				}
			})

			It("hides a redacted participant from the roster and from later frames", func() {
				t := roomTopic("cursors")
				stream, err := watchReq(
					&syncplanev1.WatchRequest{SessionId: "s1", Topics: []*syncplanev1.Topic{t}},
					subjectHeader("watcher"))
				Expect(err).NotTo(HaveOccurred())
				Expect(recvBody(stream).GetSnapshot()).NotTo(BeNil())

				Expect(publishReq(&syncplanev1.PublishRequest{
					SessionId: "s-hidden",
					Items:     []*syncplanev1.PublishItem{stateItem(t, 1, "")},
				}, subjectHeader("hidden"))).To(Succeed())
				Expect(publishReq(&syncplanev1.PublishRequest{
					SessionId: "s-seen",
					Items:     []*syncplanev1.PublishItem{stateItem(t, 1, "")},
				}, subjectHeader("seen"))).To(Succeed())

				Expect(recvBody(stream).GetUpdated().GetSessionId()).To(Equal("s-seen"))

				later, err := watchReq(
					&syncplanev1.WatchRequest{SessionId: "s2", Topics: []*syncplanev1.Topic{t}},
					subjectHeader("watcher"))
				Expect(err).NotTo(HaveOccurred())
				snap := recvBody(later).GetSnapshot()
				Expect(snap.GetParticipants()).To(HaveLen(1))
				Expect(snap.GetParticipants()[0].GetSubject()).To(Equal("seen"))
			})
		})
	})

	Describe("identity", func() {
		BeforeEach(func() { opts = []server.Option{server.WithAuthenticator(subjectAuthenticator{})} })

		watchAs := func(subject, session string, t *syncplanev1.Topic) *connect.ServerStreamForClient[syncplanev1.WatchResponse] {
			GinkgoHelper()
			stream, err := watchReq(
				&syncplanev1.WatchRequest{SessionId: session, Topics: []*syncplanev1.Topic{t}},
				subjectHeader(subject))
			Expect(err).NotTo(HaveOccurred())
			return stream
		}

		publishAs := func(subject, session string, t *syncplanev1.Topic) error {
			return publishReq(&syncplanev1.PublishRequest{
				SessionId: session,
				Items:     []*syncplanev1.PublishItem{stateItem(t, 1, "")},
			}, subjectHeader(subject))
		}

		It("stamps the subject from the principal, not the payload", func() {
			t := roomTopic("cursors")
			stream := watchAs("u1", "s1", t)
			recvBody(stream)

			Expect(publishAs("u2", "s2", t)).To(Succeed())
			Expect(recvBody(stream).GetUpdated().GetSubject()).To(Equal("u2"))
		})

		It("refuses a publish naming a session another principal holds", func() {
			t := roomTopic("cursors")
			recvBody(watchAs("u1", "s1", t))
			Expect(connect.CodeOf(publishAs("u2", "s1", t))).To(Equal(connect.CodePermissionDenied))
		})

		It("admits a publish naming the session's own principal", func() {
			t := roomTopic("cursors")
			recvBody(watchAs("u1", "s1", t))
			Expect(publishAs("u1", "s1", t)).To(Succeed())
		})

		// A session id is shown to every watcher of a room, so any member can
		// name it. Naming it must give a stranger no hold on the session.
		Describe("a session id another principal names", func() {
			// openAs opens a stream on its own context and reads its snapshot.
			// The returned func ends the stream without a leave, which is what
			// a killed tab or a dropped connection looks like.
			openAs := func(subject, session string, t *syncplanev1.Topic, fronted ...string) (*connect.ServerStreamForClient[syncplanev1.WatchResponse], context.CancelFunc) {
				GinkgoHelper()
				sctx, scancel := context.WithCancel(ctx)
				r := connect.NewRequest(&syncplanev1.WatchRequest{
					SessionId:         session,
					FrontedSessionIds: fronted,
					Topics:            []*syncplanev1.Topic{t},
				})
				r.Header().Set("X-Test-Subject", subject)
				stream, err := presence.Watch(sctx, r)
				Expect(err).NotTo(HaveOccurred())
				Expect(recvBody(stream).GetSnapshot()).NotTo(BeNil(), "stream ended: %v", stream.Err())
				return stream, scancel
			}

			leftOf := func(m *syncplanev1.WatchResponse) string { return m.GetLeft() }

			It("forgets a departed session while a stranger's stream fronts it", func() {
				t := roomTopic("cursors")
				_, closeOwner := openAs("u1", "s1", t)
				observer, _ := openAs("u3", "s3", t)
				seen := frames(observer)
				Expect(publishAs("u1", "s1", t)).To(Succeed())
				Eventually(seen).Should(Receive(WithTransform(
					func(m *syncplanev1.WatchResponse) string { return m.GetUpdated().GetSessionId() },
					Equal("s1"))))

				openAs("u2", "s2", t, "s1")
				closeOwner()

				Eventually(seen, "2s").Should(Receive(WithTransform(leftOf, Equal("s1"))),
					"a stream of another principal must not keep the record alive")
			})

			It("forgets a departed session while a stranger's stream claims it as its own", func() {
				t := roomTopic("cursors")
				_, closeOwner := openAs("u1", "s1", t)
				observer, _ := openAs("u3", "s3", t)
				seen := frames(observer)
				Expect(publishAs("u1", "s1", t)).To(Succeed())
				Eventually(seen).Should(Receive(WithTransform(
					func(m *syncplanev1.WatchResponse) string { return m.GetUpdated().GetSessionId() },
					Equal("s1"))))

				openAs("u2", "s1", t)
				closeOwner()

				Eventually(seen, "2s").Should(Receive(WithTransform(leftOf, Equal("s1"))))
			})

			It("lets the owner reopen its session after a stranger named it", func() {
				t := roomTopic("cursors")
				_, closeOwner := openAs("u1", "s1", t)
				closeOwner()
				openAs("u2", "s2", t, "s1")
				openAs("u2", "s1", t)

				// The owner is back: its stream opens and it may publish as s1
				// again, although the stranger's streams still name s1.
				Eventually(func() error {
					sctx, scancel := context.WithCancel(ctx)
					defer scancel()
					r := connect.NewRequest(&syncplanev1.WatchRequest{
						SessionId: "s1",
						Topics:    []*syncplanev1.Topic{t},
					})
					r.Header().Set("X-Test-Subject", "u1")
					stream, err := presence.Watch(sctx, r)
					if err != nil {
						return err
					}
					if !stream.Receive() {
						return stream.Err()
					}
					return publishAs("u1", "s1", t)
				}, "2s").Should(Succeed())
			})

			It("does not let a stranger's stream change the owner's record", func() {
				t := roomTopic("cursors")
				openAs("u1", "s1", t)
				observer, _ := openAs("u3", "s3", t)
				seen := frames(observer)
				Expect(publishAs("u1", "s1", t)).To(Succeed())
				Eventually(seen).Should(Receive(WithTransform(
					func(m *syncplanev1.WatchResponse) string { return m.GetUpdated().GetSubject() },
					Equal("u1"))))

				openAs("u2", "s1", t)
				// The stranger now holds a stream for s1, so its publish is
				// accepted; the record still belongs to u1 and is not changed.
				Expect(publishReq(&syncplanev1.PublishRequest{
					SessionId: "s1",
					Items:     []*syncplanev1.PublishItem{stateItem(t, 99, "spoofed")},
				}, subjectHeader("u2"))).To(Succeed())

				Consistently(seen, "200ms").ShouldNot(Receive(WithTransform(
					func(m *syncplanev1.WatchResponse) string { return m.GetUpdated().GetSubject() },
					Equal("u2"))))
			})
		})
	})

	Describe("the participant cap", func() {
		BeforeEach(func() {
			opts = []server.Option{server.WithLimits(server.Limits{MaxParticipantsPerRoom: 2})}
		})

		It("keeps the lowest session ids and flags the snapshot truncated", func() {
			t := roomTopic("cursors")
			// One watcher keeps the room alive on this node while the records
			// arrive; the cap is applied as they land.
			holder := watch("s0", t)
			recvBody(holder)
			for _, sid := range []string{"s1", "s2", "s3"} {
				publishOK(sid, stateItem(t, 1, ""))
			}

			Eventually(func() []string {
				snap := recvBody(watch("probe", t)).GetSnapshot()
				if !snap.GetTruncated() {
					return nil
				}
				ids := make([]string, len(snap.GetParticipants()))
				for i, p := range snap.GetParticipants() {
					ids[i] = p.GetSessionId()
				}
				return ids
			}, "5s").Should(Equal([]string{"s1", "s2"}))
		})
	})
})

// Two nodes on one bus each hold their own session bindings, so a stranger on
// one node can name a session whose owner is connected to the other. Only the
// registry can keep that from mattering.
var _ = Describe("Rooms on two nodes", func() {
	var (
		ctx  context.Context
		bus  *presence.Local
		t    *syncplanev1.Topic
		node func() syncplanev1connect.PresenceServiceClient
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)
		bus = presence.NewLocal()
		t = roomTopic("cursors")
		node = func() syncplanev1connect.PresenceServiceClient {
			srv := newTestServer(
				server.WithRoomAnnounce(roomAnnounce),
				server.WithAuthenticator(subjectAuthenticator{}),
				server.WithPresenceBus(bus),
			)
			mux := http.NewServeMux()
			for path, h := range srv.Handlers() {
				mux.Handle(path, h)
			}
			ts := httptest.NewServer(mux)
			DeferCleanup(func() {
				cancel()
				ts.CloseClientConnections()
				ts.Close()
				Expect(srv.Close()).To(Succeed())
			})
			return syncplanev1connect.NewPresenceServiceClient(ts.Client(), ts.URL)
		}
	})

	// openOn opens a stream on its own context, reads its snapshot and drains
	// the rest in the background. The returned func ends the stream without a
	// leave.
	openOn := func(
		c syncplanev1connect.PresenceServiceClient, subject, session string, fronted ...string,
	) (<-chan *syncplanev1.WatchResponse, context.CancelFunc) {
		GinkgoHelper()
		sctx, scancel := context.WithCancel(ctx)
		r := connect.NewRequest(&syncplanev1.WatchRequest{
			SessionId:         session,
			FrontedSessionIds: fronted,
			Topics:            []*syncplanev1.Topic{t},
		})
		r.Header().Set("X-Test-Subject", subject)
		stream, err := c.Watch(sctx, r)
		Expect(err).NotTo(HaveOccurred())
		out := make(chan *syncplanev1.WatchResponse, 64)
		go func() {
			defer GinkgoRecover()
			defer close(out)
			for stream.Receive() {
				if msg := stream.Msg(); !msg.GetHeartbeat() {
					out <- msg
				}
			}
		}()
		Eventually(out).Should(Receive(WithTransform(
			func(m *syncplanev1.WatchResponse) bool { return m.GetSnapshot() != nil }, BeTrue())))
		return out, scancel
	}

	It("forgets a departed session that a stranger on another node fronts", func() {
		first, other := node(), node()
		_, closeOwner := openOn(first, "u1", "s1")
		seen, _ := openOn(first, "u3", "s3")
		elsewhere, _ := openOn(other, "u2", "s2", "s1")

		r := connect.NewRequest(&syncplanev1.PublishRequest{
			SessionId: "s1",
			Items: []*syncplanev1.PublishItem{{
				Topic: t,
				Body: &syncplanev1.PublishItem_Presence{Presence: &syncplanev1.PresenceUpdate{
					State: []byte("cursor"), Revision: 1,
				}},
			}},
		})
		r.Header().Set("X-Test-Subject", "u1")
		_, err := first.Publish(ctx, r)
		Expect(err).NotTo(HaveOccurred())
		updatedS1 := Receive(WithTransform(
			func(m *syncplanev1.WatchResponse) string { return m.GetUpdated().GetSessionId() }, Equal("s1")))
		Eventually(seen).Should(updatedS1)
		Eventually(elsewhere).Should(updatedS1, "the other node holds the record before the owner leaves")

		closeOwner()

		leftS1 := Receive(WithTransform(
			func(m *syncplanev1.WatchResponse) string { return m.GetLeft() }, Equal("s1")))
		Eventually(seen, "2s").Should(leftS1,
			"the other node must not re-announce a record its stream does not own")
		Eventually(elsewhere, "2s").Should(leftS1)
	})
})

// subjectHeader builds the header subjectAuthenticator reads an identity from.
func subjectHeader(subject string) http.Header {
	h := http.Header{}
	h.Set("X-Test-Subject", subject)
	return h
}

// subjectAuthenticator trusts a header, which is what lets a spec drive several
// identities through one server.
type subjectAuthenticator struct{}

func (subjectAuthenticator) Authenticate(_ context.Context, h http.Header) (server.Principal, error) {
	return server.Principal{Subject: h.Get("X-Test-Subject")}, nil
}

// hideTodo denies reads of one todo key and allows everything else.
type hideTodo struct {
	server.AllowAll
	key string
}

func (h hideTodo) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if entity != "todo" {
		return nil, nil
	}
	return func(key string, _ proto.Message) bool { return key != h.key }, nil
}

// denyWorkspace refuses one workspace permanently.
type denyWorkspace struct {
	server.AllowAll
	ws string
}

func (d denyWorkspace) CanAccessWorkspace(_ context.Context, _ server.Principal, workspace string) error {
	if workspace == d.ws {
		return server.DenyWorkspace("not a member")
	}
	return nil
}

// revocableGate admits every workspace until revoke is called, so a spec can
// flip access under a live stream.
type revocableGate struct {
	server.AllowAll
	revoked atomic.Bool
}

func (g *revocableGate) revoke() { g.revoked.Store(true) }

func (g *revocableGate) CanAccessWorkspace(context.Context, server.Principal, string) error {
	if g.revoked.Load() {
		return server.DenyWorkspace("removed from project")
	}
	return nil
}

// roomPolicy exercises the optional RoomPermissions extension.
type roomPolicy struct {
	server.AllowAll
	// deny is a room name refused permanently; fail is one whose hook errors.
	deny string
	fail string
	// hideSubject is a participant subject the roster must not show.
	hideSubject string
}

func (p roomPolicy) CanJoinRoom(_ context.Context, _ server.Principal, _ string, t presence.Topic) error {
	switch t.Name {
	case p.deny:
		return server.DenyRoom("not invited")
	case p.fail:
		return errors.New("membership service is down")
	}
	return nil
}

func (p roomPolicy) VisibleParticipants(
	_ context.Context, _ server.Principal, _ string, _ presence.Topic,
) (server.ParticipantFilter, error) {
	if p.hideSubject == "" {
		return nil, nil
	}
	return func(subject string) bool { return subject != p.hideSubject }, nil
}
