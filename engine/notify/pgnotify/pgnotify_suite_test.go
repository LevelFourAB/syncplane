package pgnotify_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/internal/pubsub/pubsubtest"
	"github.com/levelfourab/syncplane/engine/notify/pgnotify"
	"github.com/levelfourab/syncplane/engine/pglisten"
	"github.com/levelfourab/syncplane/internal/pgtest"
)

func TestPgnotify(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Notifier Suite")
}

var _ = AfterSuite(pgtest.Shutdown)

// The conformance suite is written for an asynchronous transport, so it applies
// verbatim — including the Activity half, which this implementation offers.
// Each spec gets its own channel: the specs share one database, and a channel
// shared between them would let one spec's notify satisfy another's assertion.
var _ = pubsubtest.Conformance("postgres", func() pubsub.Notifier {
	return newNotifier()
})

// newNotifier builds a notifier on its own channel and registers its teardown.
func newNotifier(opts ...pgnotify.Option) *pgnotify.Notifier {
	GinkgoHelper()
	dsn := pgtest.ServerDSN()
	opts = append([]pgnotify.Option{pgnotify.WithChannel(pgtest.UniqueName("sp_chan"))}, opts...)
	n, err := pgnotify.Open(context.Background(), dsn, opts...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(n.Close()).To(Succeed()) })
	return n
}

var _ = Describe("pgnotify", func() {
	ctx := context.Background()

	// twoNodes builds two independent notifiers sharing one channel — the whole
	// point of the package, and something no in-process notifier can be asked
	// to do.
	twoNodes := func() (*pgnotify.Notifier, *pgnotify.Notifier) {
		GinkgoHelper()
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_chan")
		open := func() *pgnotify.Notifier {
			n, err := pgnotify.Open(ctx, dsn, pgnotify.WithChannel(channel))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(n.Close()).To(Succeed()) })
			return n
		}
		return open(), open()
	}

	It("delivers a poke to a subscriber on another node", func() {
		nodeA, nodeB := twoNodes()
		ch, _ := nodeB.Subscribe("ws")
		nodeA.Notify("ws")
		Eventually(ch, 5*time.Second).Should(Receive(),
			"a write on one node must wake a client connected to another")
	})

	It("delivers a burst of workspaces batched over one channel", func() {
		// A burst outruns the publisher's round trips, so most of these keys
		// cross the wire inside a batched payload — this is the end-to-end
		// check that the other node's decoder unpacks them all.
		nodeA, nodeB := twoNodes()
		const workspaces = 300
		ids := make([]string, workspaces)
		for i := range ids {
			ids[i] = fmt.Sprintf("ws-%03d", i)
		}
		set := nodeB.SubscribeSet(ids)
		DeferCleanup(set.Close)
		for _, ws := range ids {
			nodeA.Notify(ws)
		}
		seen := make(map[string]struct{}, workspaces)
		Eventually(func() int {
			for _, ws := range set.Changed() {
				seen[ws] = struct{}{}
			}
			return len(seen)
		}, 10*time.Second).Should(Equal(workspaces),
			"every workspace of a burst must arrive, however it was batched")
	})

	It("reports another node's workspace on the activity stream", func() {
		// This is what turns workspace discovery cross-node: effects subscribes
		// before listing, and a workspace first written elsewhere would
		// otherwise wait for the next sweep.
		nodeA, nodeB := twoNodes()
		ids, cancel, ok := pubsub.SubscribeActivity(nodeB)
		Expect(ok).To(BeTrue())
		DeferCleanup(cancel)

		nodeA.Notify("ws-created-elsewhere")
		Eventually(ids, 5*time.Second).Should(Receive(Equal("ws-created-elsewhere")))
	})

	It("keeps namespaces apart across nodes", func() {
		// One notifier serves every app a Manager hosts, and workspace ids are
		// namespaced per app — so "team" in app1 and "team" in app2 are
		// unrelated and must not wake each other.
		nodeA, nodeB := twoNodes()
		target, _ := nodeB.Namespace("app1").Subscribe("team")
		other, _ := nodeB.Namespace("app2").Subscribe("team")

		nodeA.Namespace("app1").Notify("team")
		Eventually(target, 5*time.Second).Should(Receive())
		Consistently(other, "300ms").ShouldNot(Receive(),
			"a poke must not cross the app namespace boundary")
	})

	It("delivers to a subscriber on the publishing node without a round trip", func() {
		// Local fan-out happens in Notify itself, so a same-node subscriber does
		// not wait on the database. The self-delivered copy that arrives later
		// is a redundant wakeup and nothing else.
		node, _ := twoNodes()
		ch, _ := node.Subscribe("ws")
		node.Notify("ws")
		Expect(ch).To(Receive(), "the local fan-out is synchronous with Notify")
	})

	It("composes with the coalescing wrapper", func() {
		// NewCoalescing wraps the send side, so it caps what reaches the bus.
		// Unwrap must still find Activity underneath.
		nodeA, nodeB := twoNodes()
		capped := pubsub.NewCoalescing(nodeA, 50*time.Millisecond)
		DeferCleanup(func() {
			if c, ok := capped.(*pubsub.Coalescing); ok {
				Expect(c.Close()).To(Succeed())
			}
		})

		ch, _ := nodeB.Subscribe("ws")
		for range 20 {
			capped.Notify("ws")
		}
		Eventually(ch, 5*time.Second).Should(Receive())

		_, cancel, ok := pubsub.SubscribeActivity(capped)
		Expect(ok).To(BeTrue(), "the wrapper must not hide the notifier's Activity support")
		cancel()
	})

	It("delivers on a transport it did not open", func() {
		// What New is for: a process also running pgpresence hands both the
		// same transport and opens one pair of connections rather than two.
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_chan")

		link, err := pglisten.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(link.Close()).To(Succeed()) })

		receiver, err := pgnotify.New(ctx, link, pgnotify.WithChannel(channel))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(receiver.Close()).To(Succeed()) })

		sender := newNotifier(pgnotify.WithChannel(channel))
		ch, _ := receiver.Subscribe("ws")
		sender.Notify("ws")
		Eventually(ch, 5*time.Second).Should(Receive())
	})

	It("leaves a transport it did not open running after Close", func() {
		// The caller may still be using it, which is the whole point of handing
		// one in. Two notifiers on one transport stand in for the notifier and
		// the presence bus a real deployment shares it between.
		dsn := pgtest.ServerDSN()
		link, err := pglisten.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(link.Close()).To(Succeed()) })

		first, err := pgnotify.New(ctx, link, pgnotify.WithChannel(pgtest.UniqueName("sp_chan")))
		Expect(err).NotTo(HaveOccurred())

		channel := pgtest.UniqueName("sp_chan")
		second, err := pgnotify.New(ctx, link, pgnotify.WithChannel(channel))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(second.Close()).To(Succeed()) })

		Expect(first.Close()).To(Succeed())

		sender := newNotifier(pgnotify.WithChannel(channel))
		ch, _ := second.Subscribe("ws")
		sender.Notify("ws")
		Eventually(ch, 5*time.Second).Should(Receive(),
			"closing one consumer must not take the shared transport down")
	})

	It("rejects a channel name it would have to interpolate unsafely", func() {
		_, err := pgnotify.Open(ctx, pgtest.ServerDSN(), pgnotify.WithChannel("bad name; DROP"))
		Expect(err).To(HaveOccurred())
	})

	It("resumes delivery after its listener connection is killed", func() {
		// Postgres buffers nothing across a dropped LISTEN, so a reconnect is
		// both a hole in the stream and the one moment this transport can
		// silently stop working. Terminating the backend is what a failover, a
		// restart or an idle-connection reaper does to it in production.
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_chan")
		nodeA, err := pgnotify.Open(ctx, dsn, pgnotify.WithChannel(channel))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(nodeA.Close()).To(Succeed()) })
		nodeB, err := pgnotify.Open(ctx, dsn, pgnotify.WithChannel(channel))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(nodeB.Close()).To(Succeed()) })

		ch, _ := nodeB.Subscribe("ws")
		// A set member must be covered by the same re-poke as a plain
		// subscription; "ws-set" sees no notify before the kill, so a wake can
		// only come from the reconnect.
		set := nodeB.SubscribeSet([]string{"ws-set"})
		DeferCleanup(set.Close)
		// Prove the pair works before breaking it, so a failure below is the
		// reconnect and not the setup.
		nodeA.Notify("ws")
		Eventually(ch, 5*time.Second).Should(Receive())

		admin, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(admin.Close(ctx)).To(Succeed()) })
		// The listener connection's last statement is its LISTEN, which is what
		// makes it identifiable among the pool's connections.
		tag, err := admin.Exec(ctx,
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = $1`,
			"LISTEN "+channel)
		Expect(err).NotTo(HaveOccurred())
		Expect(tag.RowsAffected()).To(BeNumerically(">", 0), "no listener backend was found to kill")

		// A workspace someone is watching is re-poked once the listener is
		// back, because anything published during the gap is unrecoverable.
		Eventually(ch, 10*time.Second).Should(Receive(),
			"a reconnect must re-poke live subscriptions rather than leave them waiting for a poll")
		Eventually(set.Wake(), 10*time.Second).Should(Receive(),
			"the re-poke must cover set members as well as plain subscriptions")
		Expect(set.Changed()).To(ContainElement("ws-set"))

		// And the re-LISTEN took: fresh pokes flow again.
		Eventually(func() bool {
			nodeA.Notify("ws")
			select {
			case <-ch:
				return true
			case <-time.After(200 * time.Millisecond):
				return false
			}
		}, 10*time.Second).Should(BeTrue(), "delivery must resume after the reconnect")
	})
})
