package connect_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A connector's writes reach the same change feed everything else reads, so
// they have to be recognizable: a consumer reacting to changes must be able to
// tell a connector's ingress from a user's edit and skip its own echo.
var _ = Describe("Connector write attribution", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// actorOf returns the actor recorded for one row, read from the feed.
	actorOf := func(f *fixture, key string) storage.Actor {
		GinkgoHelper()
		var got *server.Change
		_, _, err := f.srv.Changes(ctx, ws, server.Cursor{}, 0, func(ch server.Change) error {
			if ch.Key == key {
				c := ch
				got = &c
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil(), "no change for %s", key)
		return got.Actor
	}

	It("labels an ingress write with the connector's writer id", func() {
		f := newFixture(func(cfg *engineconnect.Config) { cfg.WriterID = "markdown" })
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(actorOf(f, "t1")).To(Equal(storage.Actor{System: "connect:markdown"}))
	})

	It("falls back to a bare label when no writer id is configured", func() {
		// Still enough to tell a connector's write from a GC pass, which is
		// what an unnamed bypass could not say.
		f := newFixture()
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(actorOf(f, "t1")).To(Equal(storage.Actor{System: "connect"}))
	})
})
