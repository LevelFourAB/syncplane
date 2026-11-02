package upstream_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/client"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// Specs for origin answers the convergence harness cannot reach at random.

func addConv(n *cnode) {
	GinkgoHelper()
	Expect(n.link.AddWorkspace(context.Background(), convWS)).To(Succeed())
	Expect(n.flush(10 * time.Second)).To(Succeed())
}

func expectConverged(o *corigin, nodes ...*cnode) {
	GinkgoHelper()
	origin := dumpRows(o.srv, convWS)
	for _, n := range nodes {
		Expect(diffRows("origin", origin, n.name, dumpRows(n.srv, convWS))).To(BeEmpty())
	}
}

func widgetAt(srv *server.Server, key string) *testdatav1.Widget {
	GinkgoHelper()
	w := &testdatav1.Widget{}
	var found bool
	Expect(srv.View(server.Trusted(context.Background()), convWS, func(tx *server.ViewTx) error {
		var err error
		found, _, err = tx.Get(w, key)
		return err
	})).To(Succeed())
	if !found {
		return nil
	}
	return w
}

func bodyText(w *testdatav1.Widget) string {
	s, _ := textOf(w.GetBody())
	return s
}

var _ = Describe("Origin answers", Label("convergence"), func() {
	ctx := context.Background()

	It("queues writes while the origin grants no access yet, and pushes them once it does", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w1", Label: "before"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())

		o.perm.noAccess.Store(true)
		Eventually(func() upstream.State { return n.status().State }).Should(Equal(upstream.StateNoAccess))
		_, err = n.ws().Put(ctx, &testdatav1.Widget{Id: "w2", Label: "while denied"})
		Expect(err).NotTo(HaveOccurred())
		Expect(widgetAt(n.srv, "w1")).NotTo(BeNil(), "the local copy stays")
		Eventually(func() int { return n.status().Pending }).Should(Equal(1))
		Consistently(func() upstream.State { return n.status().State }, "300ms").Should(Equal(upstream.StateNoAccess))

		o.perm.noAccess.Store(false)
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Expect(widgetAt(o.srv, "w2").GetLabel()).To(Equal("while denied"))
		Expect(n.status().State).To(Equal(upstream.StateLive))
		n.mu.Lock()
		Expect(n.revoked).To(BeZero())
		n.mu.Unlock()
		Expect(n.rejected()).To(BeEmpty())
		expectConverged(o, n)
	})

	It("keeps writing after the origin collected its client record", func() {
		o := newCOrigin(server.WithGC(time.Millisecond, 0), server.WithClientRetention(time.Millisecond))
		n := newCNode(o, "n0", nil)
		addConv(n)
		for _, k := range []string{"w1", "w2"} {
			_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: k, Label: k})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(n.flush(5 * time.Second)).To(Succeed())
		time.Sleep(5 * time.Millisecond)
		Expect(o.srv.CollectGarbage(ctx, convWS)).To(Succeed())

		_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w3", Label: "after collection"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Expect(widgetAt(o.srv, "w3").GetLabel()).To(Equal("after collection"))
		Expect(n.rejected()).To(BeEmpty())
		expectConverged(o, n)
	})

	It("recovers when the origin lost the link's watermark (mutation_id_gap)", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: "w1", Label: "one"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())

		// Take a backup of the origin while the link holds watermark 1.
		backup := filepath.Join(GinkgoT().TempDir(), "backup")
		Expect(os.MkdirAll(backup, 0o755)).To(Succeed())
		o.close()
		copyDB(o.path, backup)
		o.open()

		for _, k := range []string{"w2", "w3"} {
			_, err := n.ws().Put(ctx, &testdatav1.Widget{Id: k, Label: k})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(n.flush(10 * time.Second)).To(Succeed())

		// Restore the backup: the origin forgets mutations 2 and 3, so the
		// link's next mutation id is not contiguous with its watermark.
		n.part.Cut()
		o.close()
		restoreDB(backup, o.path)
		o.open()
		_, err = n.ws().Put(ctx, &testdatav1.Widget{Id: "w4", Label: "after restore"})
		Expect(err).NotTo(HaveOccurred())
		n.part.Heal()

		Expect(n.flush(15*time.Second)).To(Succeed(), "status %+v", n.status())
		Expect(widgetAt(o.srv, "w4").GetLabel()).To(Equal("after restore"), "the write made after the restore reaches the origin")
		expectConverged(o, n)
	})

	It("reports the origin's schema revision and keeps the fields its newer schema adds", func() {
		newer := server.DynamicEntity{
			Name: "gadget", Package: "conv.skew", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "name", Number: 2, Kind: protoreflect.StringKind},
				{Name: "extra", Number: 3, Kind: protoreflect.Int64Kind},
			},
		}
		older := newer
		older.Fields = newer.Fields[:2]
		newerType, err := server.BuildDynamicMessageType(newer)
		Expect(err).NotTo(HaveOccurred())
		olderType, err := server.BuildDynamicMessageType(older)
		Expect(err).NotTo(HaveOccurred())
		gadget := func(mt protoreflect.MessageType, id, name string, extra int64) proto.Message {
			m := dynamicpb.NewMessage(mt.Descriptor())
			fs := mt.Descriptor().Fields()
			m.Set(fs.ByName("id"), protoreflect.ValueOfString(id))
			if name != "" {
				m.Set(fs.ByName("name"), protoreflect.ValueOfString(name))
			}
			if extra != 0 {
				m.Set(fs.ByName("extra"), protoreflect.ValueOfInt64(extra))
			}
			return m
		}

		o := newCOrigin(server.WithSchemaRevision("r2"))
		Expect(o.srv.RegisterDynamicEntity(newer)).To(Succeed())
		n := newCNodeWith(o, "n0", func(c *upstream.Config) {
			c.Bindings = append(c.Bindings, upstream.Binding{Entity: "gadget"})
		}, func(srv *server.Server) {
			Expect(srv.RegisterDynamicEntity(older)).To(Succeed())
		}, server.WithSchemaRevision("r1"))
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Put(gadget(newerType, "g1", "a", 7))
		})).To(Succeed())
		addConv(n)
		Expect(n.status().SchemaRevision).To(Equal("r2"))

		// A local PATCH of the field both schemas know, then a concurrent
		// origin edit of the field only the origin knows.
		_, err = n.ws().Patch(ctx, gadget(olderType, "g1", "b", 0), []string{"name"}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())
		n.part.Cut()
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Patch(gadget(newerType, "g1", "", 8), []string{"extra"}, nil)
		})).To(Succeed())
		_, err = n.ws().Patch(ctx, gadget(olderType, "g1", "c", 0), []string{"name"}, nil)
		Expect(err).NotTo(HaveOccurred())
		n.part.Heal()
		Expect(n.flush(10 * time.Second)).To(Succeed())

		read := func(srv *server.Server, url string) proto.Message {
			GinkgoHelper()
			c, err := client.New(url)
			Expect(err).NotTo(HaveOccurred())
			snap, err := c.Workspace(convWS).Bootstrap(ctx)
			Expect(err).NotTo(HaveOccurred())
			var got proto.Message
			for rc, err := range snap.Rows() {
				Expect(err).NotTo(HaveOccurred())
				if rc.GetEntity() == "gadget" && rc.GetKey() == "g1" {
					got = dynamicpb.NewMessage(newerType.Descriptor())
					Expect(proto.Unmarshal(rc.GetValue(), got)).To(Succeed())
				}
			}
			return got
		}
		want := gadget(newerType, "g1", "c", 8)
		Expect(proto.Equal(read(o.srv, o.ts.URL), want)).To(BeTrue(), "origin row %v", read(o.srv, o.ts.URL))
		local := read(n.srv, n.ts.URL)
		Expect(proto.Equal(local, want)).To(BeTrue(), "local row %v, want %v", local, want)
	})

	It("reconnects a live stream that went silent before the link saw a heartbeat", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		before := n.chaos.subscribes.Load()
		n.chaos.stallStream.Store(1)
		// End the open stream, so the next one is the one that goes silent.
		n.part.Cut()
		n.part.Heal()
		Eventually(n.chaos.stalled.Load).Should(BeNumerically(">=", 1))
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "after the stall"})
		})).To(Succeed())
		// The origin sends a heartbeat every 300ms; 15s is fifty missed
		// heartbeats.
		Eventually(func() string { return widgetAt(n.srv, "w1").GetLabel() }).WithTimeout(15*time.Second).
			Should(Equal("after the stall"), "the link kept a silent stream for 15s (subscribes %d -> %d, status %+v)", before, n.chaos.subscribes.Load(), n.status())
	})

	It("reconnects a live stream that went silent after the link saw the origin's heartbeats", func() {
		o := newCOrigin()
		n := newCNode(o, "n0", nil)
		addConv(n)
		// Let the link see a few of the origin's 300ms heartbeats first.
		time.Sleep(1500 * time.Millisecond)
		before := n.chaos.subscribes.Load()
		n.chaos.stallStream.Store(1)
		// End the open stream, so the next one is the one that goes silent.
		n.part.Cut()
		n.part.Heal()
		Eventually(n.chaos.stalled.Load).Should(BeNumerically(">=", 1))
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "after the stall"})
		})).To(Succeed())
		// The origin sends a heartbeat every 300ms; 15s is fifty missed
		// heartbeats.
		Eventually(func() string { return widgetAt(n.srv, "w1").GetLabel() }).WithTimeout(15*time.Second).
			Should(Equal("after the stall"), "the link kept a silent stream for 15s (subscribes %d -> %d, status %+v)", before, n.chaos.subscribes.Load(), n.status())
	})

	It("keeps or reports a local text edit the origin refuses as text_state_stale", func() {
		o := newCOrigin(server.WithGC(time.Millisecond, 0), server.WithTextDeltaRetention(time.Millisecond), server.WithTextCompaction(1))
		n := newCNode(o, "n0", nil)
		addConv(n)
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "doc"}, server.WithTextString("body", "hello world"), server.WithTextPeer("origin"))
		})).To(Succeed())
		Eventually(func() string { return bodyText(widgetAt(n.srv, "w1")) }).Should(Equal("hello world"))

		n.part.Cut()
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1"}, nil, nil, server.WithTextString("body", "hello "), server.WithTextPeer("origin"))
		})).To(Succeed())
		time.Sleep(10 * time.Millisecond)
		Expect(o.srv.CollectGarbage(ctx, convWS)).To(Succeed())
		compacted, err := o.srv.CompactText(ctx, convWS)
		Expect(err).NotTo(HaveOccurred())
		Expect(compacted).To(BeNumerically(">", 0), "the origin compacted the document")

		_, err = n.ws().Patch(ctx, &testdatav1.Widget{Id: "w1"}, nil, nil, server.WithTextString("body", "hello world!"), server.WithTextPeer("local"))
		Expect(err).NotTo(HaveOccurred())
		n.part.Heal()
		Expect(n.flush(15*time.Second)).To(Succeed(), "status %+v", n.status())
		expectConverged(o, n)

		final := bodyText(widgetAt(o.srv, "w1"))
		var stale int
		for _, r := range n.rejected() {
			Expect(r.Code).To(Equal(server.CodeTextStateStale))
			stale++
		}
		Expect(strings.Contains(final, "!") || stale == 1).To(BeTrue(),
			"the edit is either kept or reported once: text %q, %d reports", final, stale)
	})
})

func copyDB(src, dir string) {
	GinkgoHelper()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		copyFile(src+suffix, filepath.Join(dir, filepath.Base(src)+suffix))
	}
}

func restoreDB(dir, dst string) {
	GinkgoHelper()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dst + suffix)
		copyFile(filepath.Join(dir, filepath.Base(dst)+suffix), dst+suffix)
	}
}

func copyFile(src, dst string) {
	GinkgoHelper()
	in, err := os.Open(src)
	if os.IsNotExist(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
	defer in.Close()
	out, err := os.Create(dst)
	Expect(err).NotTo(HaveOccurred())
	defer out.Close()
	_, err = io.Copy(out, in)
	Expect(err).NotTo(HaveOccurred())
}
