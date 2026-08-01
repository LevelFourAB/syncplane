// Package pubsubtest provides a shared Ginkgo behavior suite every
// pubsub.Notifier implementation must pass. A shared notifier reuses it
// verbatim — engine/notify/pgnotify does:
//
//	var _ = pubsubtest.Conformance("postgres", func() pubsub.Notifier { ... })
//
// The specs are written for an asynchronous transport: delivery is asserted
// with Eventually, never "notify then immediately receive", so an
// implementation that hops through a broker passes the same suite as the
// in-process one. What they do assert strictly is the shape the server
// depends on — signals are content-free and coalesce, Notify never blocks on
// a subscriber that is not reading, and cancel is a clean, repeatable
// teardown.
//
// The pubsub.Activity extension is covered too, and skipped for an
// implementation that does not offer it.
package pubsubtest

import (
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
)

// Conformance registers the notifier behavior specs. newNotifier is called
// per spec and must return a fresh notifier; if it owns resources, register
// their cleanup with DeferCleanup inside the func.
func Conformance(name string, newNotifier func() pubsub.Notifier) bool {
	return Describe("pubsub.Notifier conformance: "+name, func() {
		var n pubsub.Notifier

		BeforeEach(func() {
			n = newNotifier()
		})

		// subscribe wires up a subscription whose cancel runs at spec end, so
		// a spec that cancels early does so through the returned func (cancel
		// must be safe to call twice — see the double-cancel spec).
		subscribe := func(ws string) (<-chan struct{}, func()) {
			GinkgoHelper()
			ch, cancel := n.Subscribe(ws)
			DeferCleanup(cancel)
			return ch, cancel
		}

		It("delivers a signal to a subscriber", func() {
			ch, _ := subscribe("ws")
			n.Notify("ws")
			Eventually(ch).Should(Receive())
		})

		It("delivers to every subscriber of the workspace", func() {
			a, _ := subscribe("ws")
			b, _ := subscribe("ws")
			n.Notify("ws")
			Eventually(a).Should(Receive())
			Eventually(b).Should(Receive())
		})

		It("keeps workspaces isolated", func() {
			target, _ := subscribe("ws-a")
			other, _ := subscribe("ws-b")
			n.Notify("ws-a")
			Eventually(target).Should(Receive())
			Consistently(other, "100ms").ShouldNot(Receive(), "a signal must not leak across workspaces")
		})

		It("signals a subscriber that never reads without blocking the publisher", func() {
			subscribe("ws") // never drained
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(done)
				for i := 0; i < 100; i++ {
					n.Notify("ws")
				}
			}()
			Eventually(done, "2s").Should(BeClosed(), "Notify must not block on an undrained subscriber")
		})

		It("coalesces a burst for a subscriber that is not draining", func() {
			const burst = 10
			ch, _ := subscribe("ws")
			for i := 0; i < burst; i++ {
				n.Notify("ws")
			}
			// The contract is "at least one signal after any notify", never
			// one per notify: the subscriber must learn there is work, and
			// what is queued behind it must not grow with the write rate.
			Eventually(ch).Should(Receive())
			received := 1
			Eventually(func() bool { // drain until the channel stays quiet
				select {
				case <-ch:
					received++
					return false
				default:
					return true
				}
			}, "500ms", "10ms").Should(BeTrue())
			Expect(received).To(BeNumerically("<", burst), "a slow subscriber must not see one signal per notify")
		})

		It("signals again after the subscriber drains", func() {
			ch, _ := subscribe("ws")
			n.Notify("ws")
			Eventually(ch).Should(Receive())
			n.Notify("ws")
			Eventually(ch).Should(Receive(), "a drained subscriber must be signalled by the next notify")
		})

		It("closes the channel on cancel", func() {
			ch, cancel := subscribe("ws")
			cancel()
			Eventually(ch).Should(BeClosed(), "a reader selecting on the channel must observe the teardown")
		})

		It("survives a double cancel", func() {
			_, cancel := subscribe("ws")
			cancel()
			Expect(cancel).NotTo(Panic(), "cancel is the deferred teardown of every subscriber; it must be idempotent")
		})

		It("stops delivering after cancel", func() {
			ch, cancel := subscribe("ws")
			cancel()
			Eventually(ch).Should(BeClosed())
			Expect(func() { n.Notify("ws") }).NotTo(Panic(), "notifying a cancelled subscription must not send on a closed channel")
		})

		It("leaves one subscriber's cancel to the others", func() {
			survivor, _ := subscribe("ws")
			_, cancel := subscribe("ws")
			cancel()
			n.Notify("ws")
			Eventually(survivor).Should(Receive())
		})

		It("accepts a notify with no subscribers", func() {
			Expect(func() { n.Notify("ws") }).NotTo(Panic())
		})

		// The Activity extension is optional, so these specs skip for an
		// implementation that does not offer it — but an implementation that
		// claims it is held to the whole shape, because effects drives all of
		// its per-workspace work off this one stream: a workspace it never
		// hears about waits for the next sweep, and one it hears about only
		// once stops reacting after the first change.
		Context("workspace activity", func() {
			var ids <-chan string

			// subscribeActivity skips the spec when the notifier cannot report
			// activity at all.
			subscribeActivity := func() (<-chan string, func()) {
				GinkgoHelper()
				ch, cancel, ok := pubsub.SubscribeActivity(n)
				if !ok {
					Skip("notifier does not implement pubsub.Activity")
				}
				DeferCleanup(cancel)
				return ch, cancel
			}

			BeforeEach(func() { ids, _ = subscribeActivity() })

			It("reports a workspace nobody subscribed to", func() {
				n.Notify("ws")
				Eventually(ids).Should(Receive(Equal("ws")))
			})

			It("reports a workspace again on its next notify", func() {
				n.Notify("ws")
				Eventually(ids).Should(Receive(Equal("ws")))
				n.Notify("ws")
				Eventually(ids).Should(Receive(Equal("ws")),
					"this is a change signal; a consumer that saw a workspace once still needs its later writes")
			})

			It("reports every workspace that sees a notify", func() {
				n.Notify("ws-a")
				n.Notify("ws-b")
				Eventually(ids).Should(Receive(Equal("ws-a")))
				Eventually(ids).Should(Receive(Equal("ws-b")))
			})

			It("reports to a subscriber that attached after the fact", func() {
				n.Notify("ws")
				Eventually(ids).Should(Receive(Equal("ws")))

				late, _ := subscribeActivity()
				n.Notify("ws")
				Eventually(late).Should(Receive(Equal("ws")))
			})

			It("reports to every subscriber", func() {
				other, _ := subscribeActivity()
				n.Notify("ws")
				Eventually(ids).Should(Receive(Equal("ws")))
				Eventually(other).Should(Receive(Equal("ws")))
			})

			It("coalesces a burst on one workspace for a subscriber that is not draining", func() {
				const burst = 50
				stalled, _ := subscribeActivity() // not read until the burst is over
				for i := 0; i < burst; i++ {
					n.Notify("ws")
				}
				// What is queued must track the workspaces needing a look, not
				// the writes: a consumer drains a workspace to exhaustion, so
				// the second id of the same workspace buys nothing and the
				// hundredth costs memory a write-heavy workspace can grow
				// without bound.
				Eventually(stalled).Should(Receive(Equal("ws")))
				received := 1
				Eventually(func() bool {
					select {
					case <-stalled:
						received++
						return false
					default:
						return true
					}
				}, "500ms", "10ms").Should(BeTrue())
				Expect(received).To(BeNumerically("<", burst),
					"a stalled subscriber must not see one id per notify")
			})

			It("does not block a publisher whose subscriber never reads", func() {
				subscribeActivity() // never drained
				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					defer close(done)
					for i := 0; i < 500; i++ {
						n.Notify(fmt.Sprintf("ws-%d", i))
					}
				}()
				Eventually(done, "2s").Should(BeClosed())
			})

			It("closes the channel on cancel and survives a double cancel", func() {
				ch, cancel := subscribeActivity()
				cancel()
				Eventually(ch).Should(BeClosed())
				Expect(cancel).NotTo(Panic())
				Expect(func() { n.Notify("ws") }).NotTo(Panic(),
					"notifying a cancelled subscription must not send on a closed channel")
			})

			It("leaves one subscriber's cancel to the others", func() {
				_, cancel := subscribeActivity()
				cancel()
				n.Notify("ws")
				Eventually(ids).Should(Receive(Equal("ws")))
			})
		})

		It("is safe under concurrent subscribe, notify and cancel", func() {
			// Run with -race; this spec exists for the detector, not for its
			// assertions.
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				ws := fmt.Sprintf("ws-%d", i%3)
				wg.Add(2)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for j := 0; j < 50; j++ {
						n.Notify(ws)
					}
				}()
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for j := 0; j < 50; j++ {
						ch, cancel := n.Subscribe(ws)
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
			Eventually(done, 10*time.Second).Should(BeClosed())
		})
	})
}
