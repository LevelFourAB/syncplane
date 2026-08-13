package presence_test

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/presence"
)

// clock is the injected time source. Every liveness rule the Registry has is
// expressed in elapsed time, so a fake clock is what lets the specs assert
// expiry and re-announcement without waiting for either.
type clock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *clock {
	return &clock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

var _ = Describe("Registry", func() {
	const announce = time.Second

	var (
		ctx   context.Context
		bus   *presence.Local
		clk   *clock
		reg   *presence.Registry
		topic presence.Topic
	)

	BeforeEach(func() {
		ctx = context.Background()
		bus = presence.NewLocal()
		clk = newClock()
		reg = presence.NewRegistry(bus, presence.Options{
			Announce:        announce,
			MaxParticipants: 3,
			Now:             clk.now,
		})
		DeferCleanup(reg.Close)
		topic = presence.Topic{WorkspaceID: "ws", Name: "cursors"}
	})

	open := func(session string) *presence.Watch {
		GinkgoHelper()
		w := reg.Open(session)
		DeferCleanup(w.Close)
		return w
	}

	join := func(w *presence.Watch, t presence.Topic) []presence.Participant {
		GinkgoHelper()
		got, _, err := w.Join(t)
		Expect(err).NotTo(HaveOccurred())
		return got
	}

	publishState := func(t presence.Topic, session string, rev uint64, body string) {
		GinkgoHelper()
		Expect(reg.Publish(ctx, presence.Event{
			Topic: t,
			Kind:  presence.KindState,
			Participant: presence.Participant{
				SessionID: session,
				Subject:   "u:" + session,
				State:     []byte(body),
				Revision:  rev,
			},
		})).To(Succeed())
	}

	// receive drains until an event arrives, so a spec never depends on the
	// bus's hop being synchronous.
	receive := func(w *presence.Watch) presence.Event {
		GinkgoHelper()
		var ev presence.Event
		Eventually(w.Events()).Should(Receive(&ev))
		return ev
	}

	It("reports an empty room to the first joiner", func() {
		Expect(join(open("s1"), topic)).To(BeEmpty())
	})

	It("delivers a session's own state back to it", func() {
		w := open("s1")
		join(w, topic)
		publishState(topic, "s1", 1, "hello")

		ev := receive(w)
		Expect(ev.Kind).To(Equal(presence.KindState))
		Expect(ev.Participant.SessionID).To(Equal("s1"))
		Expect(string(ev.Participant.State)).To(Equal("hello"))
	})

	It("shows a later joiner the roster it missed", func() {
		first := open("s1")
		join(first, topic)
		publishState(topic, "s1", 1, "hello")
		receive(first) // the record has landed on this node

		got := join(open("s2"), topic)
		Expect(got).To(HaveLen(1))
		Expect(got[0].SessionID).To(Equal("s1"))
		Expect(string(got[0].State)).To(Equal("hello"))
	})

	It("orders a snapshot by session id", func() {
		w := open("s2")
		join(w, topic)
		for _, sid := range []string{"s3", "s1"} {
			publishState(topic, sid, 1, "")
			receive(w)
		}
		publishState(topic, "s2", 1, "")
		receive(w)

		got := join(open("late"), topic)
		Expect(got).To(HaveLen(3))
		Expect([]string{got[0].SessionID, got[1].SessionID, got[2].SessionID}).
			To(Equal([]string{"s1", "s2", "s3"}))
	})

	It("keeps the highest revision and discards an older delivery", func() {
		w := open("s1")
		join(w, topic)
		publishState(topic, "s1", 5, "new")
		Expect(string(receive(w).Participant.State)).To(Equal("new"))

		publishState(topic, "s1", 4, "old")
		publishState(topic, "s1", 6, "newer")
		Expect(string(receive(w).Participant.State)).To(Equal("newer"),
			"the stale revision must never reach a watcher")

		got := join(open("late"), topic)
		Expect(string(got[0].State)).To(Equal("newer"))
	})

	// A re-announcement carries the revision the state was first published
	// with, so it must refresh liveness without waking every watcher.
	It("does not fan out a re-announcement of state a watcher already has", func() {
		w := open("s1")
		join(w, topic)
		publishState(topic, "s1", 1, "hello")
		receive(w)

		publishState(topic, "s1", 1, "hello")
		Consistently(w.Events(), "100ms").ShouldNot(Receive())
	})

	It("removes a participant on an explicit leave", func() {
		w := open("s1")
		join(w, topic)
		publishState(topic, "s1", 1, "")
		receive(w)

		Expect(reg.Publish(ctx, presence.Event{
			Topic:       topic,
			Kind:        presence.KindLeave,
			Participant: presence.Participant{SessionID: "s1"},
		})).To(Succeed())

		ev := receive(w)
		Expect(ev.Kind).To(Equal(presence.KindLeave))
		Expect(ev.Participant.SessionID).To(Equal("s1"))
		Expect(join(open("late"), topic)).To(BeEmpty())
	})

	It("passes a broadcast through without recording a participant", func() {
		w := open("s1")
		join(w, topic)
		Expect(reg.Publish(ctx, presence.Event{
			Topic:       topic,
			Kind:        presence.KindMessage,
			Participant: presence.Participant{SessionID: "s1", Subject: "u1"},
			Payload:     []byte("ping"),
		})).To(Succeed())

		ev := receive(w)
		Expect(ev.Kind).To(Equal(presence.KindMessage))
		Expect(string(ev.Payload)).To(Equal("ping"))
		Expect(join(open("late"), topic)).To(BeEmpty(),
			"a broadcast must not put its sender in the room")
	})

	Describe("liveness", func() {
		// The node holding a session's stream is the one that keeps its record
		// alive; nothing else does.
		It("keeps a locally-held record alive across the expiry window", func() {
			w := open("s1")
			join(w, topic)
			publishState(topic, "s1", 1, "")
			receive(w)

			for i := 0; i < 6; i++ {
				clk.advance(announce)
				reg.Sweep(ctx)
			}
			Expect(join(open("late"), topic)).To(HaveLen(1))
		})

		It("drops a record nobody re-announces, and reports it as a departure", func() {
			// s2 watches the room; s1's record arrives over the bus with no
			// stream of its own on this node, which is what a session on
			// another node — or a dead one — looks like from here.
			w := open("s2")
			join(w, topic)
			publishState(topic, "s1", 1, "")
			receive(w)

			clk.advance(4 * announce)
			reg.Sweep(ctx)

			ev := receive(w)
			Expect(ev.Kind).To(Equal(presence.KindLeave))
			Expect(ev.Participant.SessionID).To(Equal("s1"))
			Expect(join(open("late"), topic)).To(BeEmpty())
		})

		It("does not drop a record inside the expiry window", func() {
			w := open("s2")
			join(w, topic)
			publishState(topic, "s1", 1, "")
			receive(w)

			clk.advance(2 * announce)
			reg.Sweep(ctx)
			Consistently(w.Events(), "100ms").ShouldNot(Receive())
			Expect(join(open("late"), topic)).To(HaveLen(1))
		})

		// The announcement is what a second node hears; here the second node is
		// a bare bus subscription.
		It("re-announces a locally-held record on the bus at its own revision", func() {
			w := open("s1")
			join(w, topic)
			publishState(topic, "s1", 7, "hello")
			receive(w)

			elsewhere, cancel := bus.Subscribe(topic)
			DeferCleanup(cancel)

			clk.advance(announce)
			reg.Sweep(ctx)

			var ev presence.Event
			Eventually(elsewhere).Should(Receive(&ev))
			Expect(ev.Kind).To(Equal(presence.KindState))
			Expect(ev.Participant.Revision).To(Equal(uint64(7)))
			Expect(string(ev.Participant.State)).To(Equal("hello"))
		})

		It("stops re-announcing once the session's stream closes", func() {
			w := open("s1")
			join(w, topic)
			publishState(topic, "s1", 1, "")
			receive(w)

			// A second stream keeps the room alive on this node after s1 goes.
			observer := open("s2")
			join(observer, topic)
			w.Close()

			clk.advance(4 * announce)
			reg.Sweep(ctx)
			Expect(receive(observer).Kind).To(Equal(presence.KindLeave))
		})
	})

	Describe("the participant cap", func() {
		It("keeps the lowest session ids and flags the room truncated", func() {
			w := open("s0")
			join(w, topic)
			for _, sid := range []string{"s1", "s2", "s3"} {
				publishState(topic, sid, 1, "")
			}
			Eventually(func() int {
				got, _, err := open("probe").Join(topic)
				Expect(err).NotTo(HaveOccurred())
				return len(got)
			}).Should(Equal(3))

			// s9 sorts above every stored id, so it does not displace one.
			publishState(topic, "s9", 1, "")
			// s0 sorts below them all, so it evicts s3.
			publishState(topic, "s0", 1, "")

			var got []presence.Participant
			var truncated bool
			Eventually(func() []string {
				var err error
				got, truncated, err = open("probe2").Join(topic)
				Expect(err).NotTo(HaveOccurred())
				ids := make([]string, len(got))
				for i, p := range got {
					ids[i] = p.SessionID
				}
				return ids
			}).Should(Equal([]string{"s0", "s1", "s2"}))
			Expect(truncated).To(BeTrue())
		})
	})

	Describe("teardown", func() {
		It("closes a stream's channel and stops delivering to it", func() {
			w := open("s1")
			join(w, topic)
			w.Close()
			Eventually(w.Events()).Should(BeClosed())
			Expect(w.Close).NotTo(Panic(), "Close is a deferred teardown; it must be idempotent")
		})

		It("refuses a join after the stream closed", func() {
			w := open("s1")
			w.Close()
			_, _, err := w.Join(topic)
			Expect(err).To(MatchError(presence.ErrClosed))
		})

		It("stops delivering a topic the stream left", func() {
			w := open("s1")
			other := open("s2")
			join(w, topic)
			join(other, topic)

			w.Leave(topic)
			publishState(topic, "s2", 1, "")
			Eventually(other.Events()).Should(Receive())
			Consistently(w.Events(), "100ms").ShouldNot(Receive())
		})

		It("refuses a publish and a join once closed", func() {
			reg.Close()
			Expect(reg.Publish(ctx, presence.Event{Topic: topic})).To(MatchError(presence.ErrClosed))
			_, _, err := reg.Open("s1").Join(topic)
			Expect(err).To(MatchError(presence.ErrClosed))
		})

		It("closes every open stream on shutdown", func() {
			w := open("s1")
			join(w, topic)
			reg.Close()
			Eventually(w.Events()).Should(BeClosed())
		})

		It("survives a double close", func() {
			reg.Close()
			Expect(reg.Close).NotTo(Panic())
		})
	})
})
