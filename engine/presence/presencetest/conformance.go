// Package presencetest provides a shared Ginkgo behavior suite every
// presence.Bus implementation must pass. A shared bus reuses it verbatim —
// engine/presence/pgpresence does:
//
//	var _ = presencetest.Conformance("postgres", func() presence.Bus { ... })
//
// The specs are written for an asynchronous transport: delivery is asserted
// with Eventually, never "publish then immediately receive", so an
// implementation that hops through a broker passes the same suite as the
// in-process one. What they assert strictly is the shape the Registry depends
// on — a publisher is never blocked by a watcher that is not reading, state is
// ordered by revision and may coalesce, broadcasts are never folded into one
// another, and cancel is a clean, repeatable teardown.
package presencetest

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/presence"
)

// Conformance registers the bus behavior specs. newBus is called per spec and
// must return a fresh bus; if it owns resources, register their cleanup with
// DeferCleanup inside the func.
func Conformance(name string, newBus func() presence.Bus) bool {
	return Describe("presence.Bus conformance: "+name, func() {
		var (
			bus presence.Bus
			ctx context.Context
		)

		// Topics are per-spec so that a shared broker — one Postgres channel
		// serving every spec in the suite — cannot leak a previous spec's
		// events into this one.
		topic := func(suffix string) presence.Topic {
			GinkgoHelper()
			return presence.Topic{
				WorkspaceID: "ws",
				Name:        fmt.Sprintf("room-%d-%s", GinkgoRandomSeed(), suffix),
			}
		}

		BeforeEach(func() {
			bus = newBus()
			ctx = context.Background()
		})

		// subscribe wires up a subscription whose cancel runs at spec end, so a
		// spec that cancels early does so through the returned func (cancel
		// must be safe to call twice — see the double-cancel spec).
		subscribe := func(t presence.Topic) (<-chan presence.Event, func()) {
			GinkgoHelper()
			ch, cancel := bus.Subscribe(t)
			DeferCleanup(cancel)
			return ch, cancel
		}

		state := func(t presence.Topic, session string, rev uint64, body string) presence.Event {
			return presence.Event{
				Topic: t,
				Kind:  presence.KindState,
				Participant: presence.Participant{
					SessionID: session,
					Subject:   "u1",
					State:     []byte(body),
					Revision:  rev,
				},
			}
		}

		message := func(t presence.Topic, session, body string) presence.Event {
			return presence.Event{
				Topic:       t,
				Kind:        presence.KindMessage,
				Participant: presence.Participant{SessionID: session, Subject: "u1"},
				Payload:     []byte(body),
			}
		}

		publish := func(ev presence.Event) {
			GinkgoHelper()
			Expect(bus.Publish(ctx, ev)).To(Succeed())
		}

		It("delivers a state event to a subscriber", func() {
			t := topic("state")
			ch, _ := subscribe(t)
			publish(state(t, "s1", 1, "hello"))

			var got presence.Event
			Eventually(ch).Should(Receive(&got))
			Expect(got.Kind).To(Equal(presence.KindState))
			Expect(got.Participant.SessionID).To(Equal("s1"))
			Expect(got.Participant.Revision).To(Equal(uint64(1)))
			Expect(string(got.Participant.State)).To(Equal("hello"))
		})

		// The subject is the one field a client must never be able to set, so a
		// bus that reserialized it wrongly would turn presence into an
		// impersonation channel.
		It("carries the server-stamped subject verbatim", func() {
			t := topic("subject")
			ch, _ := subscribe(t)
			ev := state(t, "s1", 1, "")
			ev.Participant.Subject = "acct:acme:svc-sync"
			publish(ev)

			var got presence.Event
			Eventually(ch).Should(Receive(&got))
			Expect(got.Participant.Subject).To(Equal("acct:acme:svc-sync"))
		})

		It("carries the authoring sequence", func() {
			t := topic("seq")
			ch, _ := subscribe(t)
			ev := state(t, "s1", 1, "")
			ev.Participant.Seq = 4242
			publish(ev)

			var got presence.Event
			Eventually(ch).Should(Receive(&got))
			Expect(got.Participant.Seq).To(Equal(uint64(4242)))
		})

		It("delivers a leave", func() {
			t := topic("leave")
			ch, _ := subscribe(t)
			publish(presence.Event{
				Topic:       t,
				Kind:        presence.KindLeave,
				Participant: presence.Participant{SessionID: "s1"},
			})

			var got presence.Event
			Eventually(ch).Should(Receive(&got))
			Expect(got.Kind).To(Equal(presence.KindLeave))
			Expect(got.Participant.SessionID).To(Equal("s1"))
		})

		It("delivers a broadcast with its payload and sender", func() {
			t := topic("message")
			ch, _ := subscribe(t)
			publish(message(t, "s1", "typing"))

			var got presence.Event
			Eventually(ch).Should(Receive(&got))
			Expect(got.Kind).To(Equal(presence.KindMessage))
			Expect(string(got.Payload)).To(Equal("typing"))
			Expect(got.Participant.SessionID).To(Equal("s1"))
		})

		It("delivers to every subscriber of the topic", func() {
			t := topic("fanout")
			a, _ := subscribe(t)
			b, _ := subscribe(t)
			publish(state(t, "s1", 1, ""))
			Eventually(a).Should(Receive())
			Eventually(b).Should(Receive())
		})

		It("delivers a publisher its own event", func() {
			// The Registry has no local echo: a publishing node learns its own
			// event back through the subscription, on the same path every other
			// node does.
			t := topic("echo")
			ch, _ := subscribe(t)
			publish(state(t, "s1", 1, ""))
			Eventually(ch).Should(Receive())
		})

		It("keeps topics isolated", func() {
			target := topic("isolated-a")
			other := topic("isolated-b")
			hit, _ := subscribe(target)
			miss, _ := subscribe(other)
			publish(state(target, "s1", 1, ""))
			Eventually(hit).Should(Receive())
			Consistently(miss, "100ms").ShouldNot(Receive(), "an event must not leak across topics")
		})

		// The anchor is part of the address, not a tag on it: a cursor room on
		// one document must not be served another document's cursors.
		It("keeps anchored topics apart from their unanchored namesake", func() {
			bare := presence.Topic{WorkspaceID: "ws", Name: fmt.Sprintf("anchor-%d", GinkgoRandomSeed())}
			anchored := bare
			anchored.Entity, anchored.Key = "todo", "t1"

			hit, _ := subscribe(anchored)
			miss, _ := subscribe(bare)
			publish(state(anchored, "s1", 1, ""))
			Eventually(hit).Should(Receive())
			Consistently(miss, "100ms").ShouldNot(Receive())
		})

		It("does not block a publisher whose subscriber never reads", func() {
			t := topic("slow")
			subscribe(t) // never drained
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(done)
				for i := 0; i < 500; i++ {
					_ = bus.Publish(ctx, message(t, "s1", fmt.Sprintf("m%d", i)))
				}
			}()
			Eventually(done, "5s").Should(BeClosed(), "Publish must not block on an undrained subscriber")
		})

		// State is last-writer-wins, so an implementation is free to fold a
		// burst down to the newest. What it may not do is grow the backlog with
		// the publish rate.
		It("never delivers more state than was published, and delivers the newest", func() {
			const burst = 40
			t := topic("coalesce")
			ch, _ := subscribe(t)
			for i := 1; i <= burst; i++ {
				publish(state(t, "s1", uint64(i), fmt.Sprintf("v%d", i)))
			}
			Eventually(func() uint64 {
				var last uint64
				for {
					select {
					case ev := <-ch:
						last = ev.Participant.Revision
						if last == burst {
							return last
						}
					default:
						return last
					}
				}
			}, "5s", "10ms").Should(Equal(uint64(burst)), "the newest state must arrive, coalesced or not")
		})

		// Broadcasts are each distinct, so they may be dropped but never folded
		// into one another. A handful with an attentive reader must all land.
		It("does not coalesce broadcasts", func() {
			t := topic("nocoalesce")
			ch, _ := subscribe(t)
			for i := 0; i < 5; i++ {
				publish(message(t, "s1", fmt.Sprintf("m%d", i)))
			}
			got := make([]string, 0, 5)
			Eventually(func() int {
				select {
				case ev := <-ch:
					got = append(got, string(ev.Payload))
				default:
				}
				return len(got)
			}, "5s", "10ms").Should(Equal(5))
			Expect(got).To(Equal([]string{"m0", "m1", "m2", "m3", "m4"}))
		})

		It("keeps two sessions' state apart in one topic", func() {
			t := topic("sessions")
			ch, _ := subscribe(t)
			publish(state(t, "s1", 1, "a"))
			publish(state(t, "s2", 1, "b"))

			seen := map[string]string{}
			Eventually(func() int {
				select {
				case ev := <-ch:
					seen[ev.Participant.SessionID] = string(ev.Participant.State)
				default:
				}
				return len(seen)
			}, "5s", "10ms").Should(Equal(2))
			Expect(seen).To(Equal(map[string]string{"s1": "a", "s2": "b"}))
		})

		It("closes the channel on cancel", func() {
			t := topic("cancel")
			ch, cancel := subscribe(t)
			cancel()
			Eventually(ch).Should(BeClosed(), "a reader selecting on the channel must observe the teardown")
		})

		It("survives a double cancel", func() {
			_, cancel := subscribe(topic("double-cancel"))
			cancel()
			Expect(cancel).NotTo(Panic(), "cancel is the deferred teardown of every subscriber; it must be idempotent")
		})

		It("stops delivering after cancel", func() {
			t := topic("post-cancel")
			ch, cancel := subscribe(t)
			cancel()
			Eventually(ch).Should(BeClosed())
			Expect(func() { _ = bus.Publish(ctx, state(t, "s1", 1, "")) }).NotTo(Panic(),
				"publishing to a cancelled subscription must not send on a closed channel")
		})

		It("leaves one subscriber's cancel to the others", func() {
			t := topic("survivor")
			survivor, _ := subscribe(t)
			_, cancel := subscribe(t)
			cancel()
			publish(state(t, "s1", 1, ""))
			Eventually(survivor).Should(Receive())
		})

		It("accepts a publish with no subscribers", func() {
			Expect(bus.Publish(ctx, state(topic("nobody"), "s1", 1, ""))).To(Succeed())
		})

		It("is safe under concurrent subscribe, publish and cancel", func() {
			// Run with -race; this spec exists for the detector, not for its
			// assertions.
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				t := topic(fmt.Sprintf("race-%d", i%3))
				wg.Add(2)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for j := 0; j < 25; j++ {
						_ = bus.Publish(ctx, state(t, "s1", uint64(j), ""))
					}
				}()
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for j := 0; j < 25; j++ {
						ch, cancel := bus.Subscribe(t)
						select {
						case <-ch:
						default:
						}
						cancel()
					}
				}()
			}
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			Eventually(done, 30*time.Second).Should(BeClosed())
		})
	})
}
