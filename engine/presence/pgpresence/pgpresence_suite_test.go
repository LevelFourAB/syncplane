package pgpresence_test

import (
	"context"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/presence/pgpresence"
	"github.com/levelfourab/syncplane/engine/presence/presencetest"
)

func TestPgpresence(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Presence Suite")
}

var _ = AfterSuite(pgtest.Shutdown)

// The conformance suite is written for an asynchronous transport, so it applies
// verbatim. Each spec gets its own channel: the specs share one database, and a
// channel shared between them would let one spec's publish satisfy another's
// assertion.
var _ = presencetest.Conformance("postgres", func() presence.Bus {
	return newBus()
})

// newBus builds a bus on its own channel and registers its teardown.
func newBus(opts ...pgpresence.Option) *pgpresence.Bus {
	GinkgoHelper()
	opts = append([]pgpresence.Option{pgpresence.WithChannel(pgtest.UniqueName("sp_pres"))}, opts...)
	b, err := pgpresence.Open(context.Background(), pgtest.ServerDSN(), opts...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(b.Close()).To(Succeed()) })
	return b
}

var _ = Describe("pgpresence", func() {
	ctx := context.Background()

	topic := func(name string) presence.Topic {
		return presence.Topic{WorkspaceID: "ws", Name: name}
	}

	state := func(t presence.Topic, session string, rev uint64, body string) presence.Event {
		return presence.Event{
			Topic: t,
			Kind:  presence.KindState,
			Participant: presence.Participant{
				SessionID: session,
				Subject:   "u:" + session,
				State:     []byte(body),
				Revision:  rev,
			},
		}
	}

	// twoNodes builds two independent buses sharing one channel — the whole
	// point of the package, and something no in-process bus can be asked to do.
	twoNodes := func() (*pgpresence.Bus, *pgpresence.Bus) {
		GinkgoHelper()
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_pres")
		open := func() *pgpresence.Bus {
			b, err := pgpresence.Open(ctx, dsn, pgpresence.WithChannel(channel))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(b.Close()).To(Succeed()) })
			return b
		}
		return open(), open()
	}

	It("delivers presence state published on another node", func() {
		nodeA, nodeB := twoNodes()
		t := topic("cursors")
		ch, _ := nodeB.Subscribe(t)
		Expect(nodeA.Publish(ctx, state(t, "s1", 3, "hello"))).To(Succeed())

		var got presence.Event
		Eventually(ch, 5*time.Second).Should(Receive(&got),
			"a cursor moved on one node must reach a watcher connected to another")
		Expect(got.Participant.SessionID).To(Equal("s1"))
		Expect(got.Participant.Subject).To(Equal("u:s1"))
		Expect(got.Participant.Revision).To(Equal(uint64(3)))
		Expect(string(got.Participant.State)).To(Equal("hello"))
	})

	It("delivers a broadcast published on another node", func() {
		nodeA, nodeB := twoNodes()
		t := topic("chat")
		ch, _ := nodeB.Subscribe(t)
		Expect(nodeA.Publish(ctx, presence.Event{
			Topic:       t,
			Kind:        presence.KindMessage,
			Participant: presence.Participant{SessionID: "s1"},
			Payload:     []byte("ping"),
		})).To(Succeed())

		var got presence.Event
		Eventually(ch, 5*time.Second).Should(Receive(&got))
		Expect(got.Kind).To(Equal(presence.KindMessage))
		Expect(string(got.Payload)).To(Equal("ping"))
	})

	It("delivers a leave published on another node", func() {
		nodeA, nodeB := twoNodes()
		t := topic("cursors")
		ch, _ := nodeB.Subscribe(t)
		Expect(nodeA.Publish(ctx, presence.Event{
			Topic:       t,
			Kind:        presence.KindLeave,
			Participant: presence.Participant{SessionID: "s1"},
		})).To(Succeed())

		var got presence.Event
		Eventually(ch, 5*time.Second).Should(Receive(&got))
		Expect(got.Kind).To(Equal(presence.KindLeave))
		Expect(got.Participant.SessionID).To(Equal("s1"))
	})

	// The publishing node fans out locally and also receives its own event back
	// off the channel. Delivering a broadcast twice would show it twice, which
	// is why the payload carries a node id.
	It("does not deliver a publisher its own event twice", func() {
		bus := newBus()
		t := topic("echo")
		ch, _ := bus.Subscribe(t)
		Expect(bus.Publish(ctx, presence.Event{
			Topic:       t,
			Kind:        presence.KindMessage,
			Participant: presence.Participant{SessionID: "s1"},
			Payload:     []byte("once"),
		})).To(Succeed())

		Eventually(ch).Should(Receive())
		Consistently(ch, "1s").ShouldNot(Receive(),
			"the publisher's own event must not come back off the channel")
	})

	It("keeps namespaces apart on one channel", func() {
		nodeA, nodeB := twoNodes()
		t := topic("cursors")
		hit, _ := nodeB.Namespace("app-a").Subscribe(t)
		miss, _ := nodeB.Namespace("app-b").Subscribe(t)

		Expect(nodeA.Namespace("app-a").Publish(ctx, state(t, "s1", 1, ""))).To(Succeed())
		Eventually(hit, 5*time.Second).Should(Receive())
		Consistently(miss, "500ms").ShouldNot(Receive(),
			"workspace ids are namespaced per app, so two apps sharing a workspace name must not share a room")
	})

	It("keeps anchored topics apart across nodes", func() {
		nodeA, nodeB := twoNodes()
		bare := topic("anchored")
		anchored := bare
		anchored.Entity, anchored.Key = "todo", "t1"

		hit, _ := nodeB.Subscribe(anchored)
		miss, _ := nodeB.Subscribe(bare)
		Expect(nodeA.Publish(ctx, state(anchored, "s1", 1, ""))).To(Succeed())
		Eventually(hit, 5*time.Second).Should(Receive())
		Consistently(miss, "500ms").ShouldNot(Receive())
	})

	// Dropping it in the transport would leave the publisher believing its state
	// went out while every node but its own showed nothing.
	It("refuses an event too large for a NOTIFY payload", func() {
		bus := newBus()
		t := topic("oversize")
		ch, _ := bus.Subscribe(t)

		err := bus.Publish(ctx, state(t, "s1", 1, strings.Repeat("x", 8000)))
		Expect(err).To(MatchError(ContainSubstring("NOTIFY payload can carry")))
		Consistently(ch, "500ms").ShouldNot(Receive(),
			"a refused event must not reach local subscribers either")
	})

	It("carries the largest state the engine's default limit allows", func() {
		nodeA, nodeB := twoNodes()
		t := topic("atlimit")
		ch, _ := nodeB.Subscribe(t)
		// 4 KiB is Limits.MaxPresenceStateBytes; base64 inflates it to ~5.5 KiB
		// and the envelope has to fit alongside.
		Expect(nodeA.Publish(ctx, state(t, "s1", 1, strings.Repeat("x", 4<<10)))).To(Succeed())

		var got presence.Event
		Eventually(ch, 5*time.Second).Should(Receive(&got))
		Expect(got.Participant.State).To(HaveLen(4 << 10))
	})

	It("refuses a namespace that would corrupt the routing prefix", func() {
		bus := newBus()
		err := bus.Namespace("a\x1fb").Publish(ctx, state(topic("x"), "s1", 1, ""))
		Expect(err).To(MatchError(ContainSubstring("field separator")))
	})

	It("rejects a channel name LISTEN could not take unquoted", func() {
		_, err := pgpresence.Open(ctx, pgtest.ServerDSN(), pgpresence.WithChannel("Bad Channel"))
		Expect(err).To(MatchError(ContainSubstring("must match")))
	})

	It("fails to open against an unreachable database", func() {
		_, err := pgpresence.Open(ctx, "postgres://nobody@127.0.0.1:1/none?sslmode=disable")
		Expect(err).To(HaveOccurred())
	})

	It("survives a double close", func() {
		b := newBus()
		Expect(b.Close()).To(Succeed())
		Expect(b.Close()).To(Succeed())
	})

	It("makes Close on a namespace view a no-op", func() {
		b := newBus()
		view := b.Namespace("app")
		Expect(view.Close()).To(Succeed())

		t := topic("still-alive")
		ch, _ := view.Subscribe(t)
		Expect(view.Publish(ctx, state(t, "s1", 1, ""))).To(Succeed())
		Eventually(ch).Should(Receive(), "the owner outlives its views; closing one must not stop the transport")
	})
})
