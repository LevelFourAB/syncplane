package upstream_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// Minimal reproductions of failures the convergence harness found. Each one
// fails while the bug it documents is open.

var _ = Describe("Convergence repros", Label("convergence"), func() {
	ctx := context.Background()

	It("reports a rejected write once when the process stops right after Flush returned", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w1", Label: "Xforbidden"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Expect(n.rejected()).To(HaveLen(1))
		n.restart()
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Consistently(n.rejected, "300ms").Should(HaveLen(1))
	})

	It("remembers the verdict on a decided write after a write-through write failed in transit", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		rc, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w1", Label: "one"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())
		Expect(n.link.Await(ctx, convWS, rc.Seq)).To(Succeed())

		n.part.Cut()
		_, err = n.ws().Put(upstream.WriteThrough(ctx), &testdatav1.Widget{Id: "w2", Label: "through"})
		Expect(err).To(MatchError(upstream.ErrOriginUnavailable))
		n.part.Heal()
		_, err = n.ws().Put(ctx, &testdatav1.Widget{Id: "w3", Label: "three"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())
		Expect(n.link.Await(ctx, convWS, rc.Seq)).To(Succeed(), "Await of a write the origin applied")
	})

	It("holds the rows of a widened scope when Flush returns", func() {
		o := newCOrigin()
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			for _, k := range []string{"w1", "w2"} {
				if err := tx.Put(&testdatav1.Widget{Id: k, Label: k, Flag: k == "w1"}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		n := newCNode(o, "n0", nil)
		addConv(n)
		Expect(n.link.SetSubscriptions(ctx, convWS, &syncplanev1.Subscription{Entity: "widget", Predicate: query.Eq("flag", true)})).To(Succeed())
		Eventually(func() *testdatav1.Widget { return widgetAt(n.srv, "w2") }).Should(BeNil())
		Expect(n.link.SetSubscriptions(ctx, convWS)).To(Succeed())
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Expect(widgetAt(n.srv, "w2")).NotTo(BeNil(), "Flush returned before the widened scope arrived: status %+v", n.status())
		expectConverged(o, n)
	})

	It("holds the rows of a scope widened right after a restart when Flush returns", func() {
		o := newCOrigin()
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			for _, k := range []string{"w1", "w2"} {
				if err := tx.Put(&testdatav1.Widget{Id: k, Label: k, Flag: k == "w1"}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		n := newCNode(o, "n0", nil)
		addConv(n)
		Expect(n.link.SetSubscriptions(ctx, convWS, &syncplanev1.Subscription{Entity: "widget", Predicate: query.Eq("flag", true)})).To(Succeed())
		Eventually(func() *testdatav1.Widget { return widgetAt(n.srv, "w2") }).Should(BeNil())
		n.restart()
		Expect(n.link.SetSubscriptions(ctx, convWS)).To(Succeed())
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Expect(widgetAt(n.srv, "w2")).NotTo(BeNil(), "Flush returned before the widened scope arrived: status %+v", n.status())
		expectConverged(o, n)
	})

	It("forwards a text write to a row outside the subscribed scope without reusing the peer's text ids", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w1"}, server.WithTextString("body", "hello"), server.WithTextPeer("app"))
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())

		// The row leaves the scope, so the local engine no longer holds the
		// text ids the peer minted in it.
		Expect(n.link.SetSubscriptions(ctx, convWS, &syncplanev1.Subscription{Entity: "widget", Predicate: query.Eq("flag", true)})).To(Succeed())
		Eventually(func() *testdatav1.Widget { return widgetAt(n.srv, "w1") }).Should(BeNil())
		_, err = n.ws().Put(ctx, &testdatav1.Widget{Id: "w1"}, server.WithTextString("body", "bye"), server.WithTextPeer("app"))
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())
		Expect(n.rejected()).To(BeEmpty(), "the origin refused the write")
		Expect(bodyText(widgetAt(o.srv, "w1"))).To(ContainSubstring("bye"))
	})

	It("settles a write-through receipt while the live stream is silent", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		n.chaos.stallStream.Store(1)
		n.part.Cut()
		n.part.Heal()
		Eventually(n.chaos.stalled.Load).Should(BeNumerically(">=", 1))
		rc, err := n.ws().Put(upstream.WriteThrough(ctx), &testdatav1.Widget{Id: "w1", Label: "through"})
		Expect(err).NotTo(HaveOccurred())
		Expect(widgetAt(o.srv, "w1").GetLabel()).To(Equal("through"))
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		Expect(rc.Wait(wctx)).To(Succeed(), "the origin applied the write before Put returned")
	})
})
