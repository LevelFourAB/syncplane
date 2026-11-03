package presencebridge_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/client"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream/presencebridge"
)

var _ = Describe("Bridge", func() {
	var (
		ctx      context.Context
		opts     []presencebridge.Option
		originTS *httptest.Server
		localTS  *httptest.Server
		link     *cutter
		bridge   *presencebridge.Bridge
	)

	BeforeEach(func() {
		opts = nil
	})

	// JustBeforeEach builds the engines, so a Context can add bridge options
	// in a BeforeEach.
	JustBeforeEach(func() {
		_, originTS = startServer("origin")
		link = newCutter(originTS.Client().Transport)
		origin := syncplanev1connect.NewPresenceServiceClient(&http.Client{Transport: link}, originTS.URL,
			connect.WithInterceptors(asUser("bridge")))
		bridge = presencebridge.New(presence.NewLocal(), origin, append([]presencebridge.Option{
			presencebridge.WithWorkspaces("acme"),
			presencebridge.WithAnnounce(announce),
			presencebridge.WithReconnectBackoff(20*time.Millisecond, 100*time.Millisecond),
			presencebridge.WithLogger(discard),
		}, opts...)...)
		DeferCleanup(bridge.Close)
		_, localTS = startServer("local", server.WithPresenceBus(bridge))

		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		// Registered last so it runs first: the Watch streams end before the
		// servers they run on close.
		DeferCleanup(cancel)
	})

	localRoom := func(user, workspace, name string) *client.Room {
		return userClient(localTS, user).Workspace(workspace).Room(name)
	}
	originRoom := func(user, workspace, name string) *client.Room {
		return userClient(originTS, user).Workspace(workspace).Room(name)
	}
	present := func(room *client.Room, state string) {
		GinkgoHelper()
		_, err := room.SetPresence(ctx, []byte(state), 0)
		Expect(err).NotTo(HaveOccurred())
	}

	// meet puts alice on the local engine and bob on the origin in one room,
	// and waits until each sees the other.
	meet := func() (alice, bob *client.Room, aliceView, bobView *roster) {
		GinkgoHelper()
		alice = localRoom("alice", "acme", "doc")
		bob = originRoom("bob", "acme", "doc")
		aliceView = follow(ctx, alice)
		bobView = follow(ctx, bob)
		present(alice, "a")
		present(bob, "b")
		Eventually(aliceView.Members).Should(Equal(map[string]string{
			alice.SessionID(): "alice/a",
			bob.SessionID():   "bob/b",
		}))
		Eventually(bobView.Members).Should(Equal(map[string]string{
			alice.SessionID(): "bridge/a",
			bob.SessionID():   "bob/b",
		}))
		return alice, bob, aliceView, bobView
	}

	It("shows local and origin participants to each other", func() {
		alice, bob, aliceView, bobView := meet()

		present(alice, "a2")
		present(bob, "b2")
		Eventually(aliceView.Members).Should(HaveKeyWithValue(bob.SessionID(), "bob/b2"))
		Eventually(bobView.Members).Should(HaveKeyWithValue(alice.SessionID(), "bridge/a2"))
	})

	It("does not show a local participant a second time through the origin", func() {
		alice, bob, aliceView, _ := meet()

		Consistently(aliceView.Members, 6*announce).Should(Equal(map[string]string{
			alice.SessionID(): "alice/a",
			bob.SessionID():   "bob/b",
		}))
	})

	It("carries broadcasts both ways, once each", func() {
		alice, bob, aliceView, bobView := meet()

		Expect(alice.Broadcast(ctx, []byte("from-alice"))).To(Succeed())
		Expect(bob.Broadcast(ctx, []byte("from-bob"))).To(Succeed())

		fromAlice := alice.SessionID() + "/alice/from-alice"
		fromBob := bob.SessionID() + "/bob/from-bob"
		Eventually(aliceView.Messages).Should(ConsistOf(fromAlice, fromBob))
		Eventually(bobView.Messages).Should(ConsistOf(alice.SessionID()+"/bridge/from-alice", fromBob))
		Consistently(aliceView.Messages, 4*announce).Should(HaveLen(2))
		Consistently(bobView.Messages, 4*announce).Should(HaveLen(2))
	})

	It("withdraws a local participant from the origin when it leaves", func() {
		alice, _, _, bobView := meet()

		Expect(alice.Leave(ctx)).To(Succeed())
		Eventually(bobView.Members).ShouldNot(HaveKey(alice.SessionID()))
	})

	It("lets a local participant whose stream ended lapse on the origin", func() {
		carol := localRoom("carol", "acme", "doc")
		bob := originRoom("bob", "acme", "doc")
		carolCtx, stopCarol := context.WithCancel(ctx)
		follow(carolCtx, carol)
		bobView := follow(ctx, bob)
		present(carol, "c")
		Eventually(bobView.Members).Should(HaveKey(carol.SessionID()))

		stopCarol()
		Eventually(bobView.Members, 20*announce).ShouldNot(HaveKey(carol.SessionID()))
	})

	It("lets origin participants lapse while the origin is unreachable, and restores them", func() {
		alice, bob, aliceView, bobView := meet()

		link.cut()
		Eventually(aliceView.Members, 20*announce).Should(Equal(map[string]string{
			alice.SessionID(): "alice/a",
		}))
		Eventually(bobView.Members, 20*announce).Should(Equal(map[string]string{
			bob.SessionID(): "bob/b",
		}))

		// Local presence keeps working without the origin.
		dave := localRoom("dave", "acme", "doc")
		follow(ctx, dave)
		present(dave, "d")
		Eventually(aliceView.Members).Should(HaveKeyWithValue(dave.SessionID(), "dave/d"))

		link.restore()
		Eventually(aliceView.Members, 40*announce).Should(Equal(map[string]string{
			alice.SessionID(): "alice/a",
			bob.SessionID():   "bob/b",
			dave.SessionID():  "dave/d",
		}))
		Eventually(bobView.Members, 40*announce).Should(Equal(map[string]string{
			alice.SessionID(): "bridge/a",
			bob.SessionID():   "bob/b",
			dave.SessionID():  "bridge/d",
		}))
	})

	It("keeps a workspace that is not bridged local", func() {
		alice := localRoom("alice", "other", "doc")
		bob := originRoom("bob", "other", "doc")
		aliceView := follow(ctx, alice)
		bobView := follow(ctx, bob)
		present(alice, "a")
		present(bob, "b")
		Expect(alice.Broadcast(ctx, []byte("hi"))).To(Succeed())
		Expect(bob.Broadcast(ctx, []byte("hi"))).To(Succeed())

		Eventually(aliceView.Members).Should(HaveKey(alice.SessionID()))
		Consistently(aliceView.Members, 6*announce).Should(Equal(map[string]string{alice.SessionID(): "alice/a"}))
		Consistently(bobView.Members, 2*announce).Should(Equal(map[string]string{bob.SessionID(): "bob/b"}))
		Expect(aliceView.Messages()).To(HaveLen(1))
		Expect(bobView.Messages()).To(HaveLen(1))
	})

	It("does not forward a session's publishes from a second local principal", func() {
		bob := originRoom("bob", "acme", "doc")
		bobView := follow(ctx, bob)
		// Something must watch the room locally for its records to be held.
		follow(ctx, localRoom("dave", "acme", "doc"))

		// Alice publishes without a stream, so the local engine binds her
		// session to nobody and Mallory may name it.
		alice := localRoom("alice", "acme", "doc")
		present(alice, "a")
		Eventually(bobView.Members).Should(HaveKeyWithValue(alice.SessionID(), "bridge/a"))

		mallory := userClient(localTS, "mallory").PresenceService()
		_, err := mallory.Publish(ctx, connect.NewRequest(&syncplanev1.PublishRequest{
			SessionId: alice.SessionID(),
			Items: []*syncplanev1.PublishItem{{
				Topic: &syncplanev1.Topic{WorkspaceId: "acme", Name: "doc"},
				Body: &syncplanev1.PublishItem_Presence{Presence: &syncplanev1.PresenceUpdate{
					State: []byte("evil"), Revision: 99,
				}},
			}},
		}))
		Expect(err).NotTo(HaveOccurred())
		// Alice holds no stream, so her record lapses on its own; what must
		// never appear is Mallory's state under her session.
		Consistently(bobView.Members, 6*announce).ShouldNot(HaveKeyWithValue(alice.SessionID(), "bridge/evil"))
	})

	It("withdraws origin participants on Close and keeps local presence", func() {
		alice, bob, aliceView, _ := meet()

		Expect(bridge.Close()).To(Succeed())
		Eventually(aliceView.Members).Should(Equal(map[string]string{alice.SessionID(): "alice/a"}))
		Consistently(aliceView.Members, 6*announce).ShouldNot(HaveKey(bob.SessionID()))
		Expect(bridge.Close()).To(Succeed())
	})

	It("rejoins a room the local engine opens again", func() {
		bob := originRoom("bob", "acme", "doc")
		follow(ctx, bob)
		present(bob, "b")

		alice := localRoom("alice", "acme", "doc")
		firstCtx, closeFirst := context.WithCancel(ctx)
		first := follow(firstCtx, alice)
		Eventually(first.Members).Should(HaveKey(bob.SessionID()))
		closeFirst()

		again := follow(ctx, localRoom("alice", "acme", "doc"))
		Eventually(again.Members).Should(HaveKeyWithValue(bob.SessionID(), "bob/b"))
	})

	Context("with one room per origin stream", func() {
		BeforeEach(func() {
			opts = append(opts, presencebridge.WithOriginLimits(1, 1))
		})

		It("watches every room on a stream of its own", func() {
			var views []*roster
			var bobs []*client.Room
			for _, name := range []string{"one", "two", "three"} {
				bob := originRoom("bob", "acme", name)
				follow(ctx, bob)
				present(bob, "b-"+name)
				bobs = append(bobs, bob)
				views = append(views, follow(ctx, localRoom("alice", "acme", name)))
			}
			for i, name := range []string{"one", "two", "three"} {
				Eventually(views[i].Members).Should(HaveKeyWithValue(bobs[i].SessionID(), "bob/b-"+name))
			}
		})
	})

	It("retries a room the origin refused until the origin admits it", func() {
		todo := &todov1.Todo{Id: "t1", Title: "shared"}
		_, err := userClient(localTS, "alice").Workspace("acme").Put(ctx, todo)
		Expect(err).NotTo(HaveOccurred())

		// The origin does not hold the anchor row yet, so it refuses the room.
		alice := userClient(localTS, "alice").Workspace("acme").AnchoredRoom("doc", "todo", "t1")
		aliceView := follow(ctx, alice)
		present(alice, "a")
		Eventually(aliceView.Members).Should(Equal(map[string]string{alice.SessionID(): "alice/a"}))

		_, err = userClient(originTS, "bob").Workspace("acme").Put(ctx, todo)
		Expect(err).NotTo(HaveOccurred())
		bob := userClient(originTS, "bob").Workspace("acme").AnchoredRoom("doc", "todo", "t1")
		bobView := follow(ctx, bob)
		present(bob, "b")

		Eventually(aliceView.Members, 40*announce).Should(HaveKeyWithValue(bob.SessionID(), "bob/b"))
		Eventually(bobView.Members, 40*announce).Should(HaveKeyWithValue(alice.SessionID(), "bridge/a"))
	})
})

var _ = Describe("Bridge as a bus", func() {
	var (
		ctx    context.Context
		bridge *presencebridge.Bridge
		bob    *client.Room
		topic  = presence.Topic{WorkspaceID: "acme", Name: "doc"}
	)

	BeforeEach(func() {
		_, originTS := startServer("origin")
		bridge = presencebridge.New(nil,
			syncplanev1connect.NewPresenceServiceClient(originTS.Client(), originTS.URL,
				connect.WithInterceptors(asUser("bridge"))),
			presencebridge.WithAnnounce(announce),
			presencebridge.WithLogger(discard))
		DeferCleanup(bridge.Close)
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)
		bob = userClient(originTS, "bob").Workspace("acme").Room("doc")
	})

	// collect drains a subscription into a slice the spec can poll.
	collect := func(ch <-chan presence.Event) func() []presence.Event {
		var mu sync.Mutex
		var got []presence.Event
		go func() {
			for ev := range ch {
				mu.Lock()
				got = append(got, ev)
				mu.Unlock()
			}
		}()
		return func() []presence.Event {
			mu.Lock()
			defer mu.Unlock()
			return append([]presence.Event(nil), got...)
		}
	}

	It("relays origin participants with the origin issuer and never relays a local session back", func() {
		ch, cancelSub := bridge.Subscribe(topic)
		DeferCleanup(cancelSub)
		events := collect(ch)
		follow(ctx, bob)
		_, err := bob.SetPresence(ctx, []byte("b"), 0)
		Expect(err).NotTo(HaveOccurred())

		local := presence.Participant{SessionID: "local-1", Subject: "alice", State: []byte("a"), Revision: 1, Seq: 7}
		Expect(bridge.Publish(ctx, presence.Event{Topic: topic, Kind: presence.KindState, Participant: local})).To(Succeed())
		Expect(bridge.Publish(ctx, presence.Event{Topic: topic, Kind: presence.KindMessage, Participant: local, Payload: []byte("hi")})).To(Succeed())

		Eventually(events).Should(ContainElement(SatisfyAll(
			HaveField("Kind", presence.KindState),
			HaveField("Participant.SessionID", bob.SessionID()),
			HaveField("Participant.Issuer", presencebridge.OriginIssuer),
			HaveField("Participant.Subject", "bob"),
			HaveField("Participant.Seq", uint64(0)),
		)))
		echo := SatisfyAll(
			HaveField("Participant.SessionID", "local-1"),
			HaveField("Participant.Issuer", presencebridge.OriginIssuer),
		)
		Consistently(events, 6*announce).ShouldNot(ContainElement(echo))
	})
})
