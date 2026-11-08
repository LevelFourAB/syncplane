package upstream_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/client"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
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
		sk := newSkew()
		o, n := sk.link()
		Expect(n.status().SchemaRevision).To(Equal("r2"))
		Expect(n.status().SchemaSkew).To(Equal(&upstream.SchemaSkew{Local: "r1", Origin: "r2", Action: upstream.SkewWarn}))

		// A local PATCH of the field both schemas know, then a concurrent
		// origin edit of the field only the origin knows.
		_, err := n.ws().Patch(ctx, sk.older("g1", "b"), []string{"name"}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())
		n.part.Cut()
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Patch(sk.newer("g1", "", 8), []string{"extra"}, nil)
		})).To(Succeed())
		_, err = n.ws().Patch(ctx, sk.older("g1", "c"), []string{"name"}, nil)
		Expect(err).NotTo(HaveOccurred())
		n.part.Heal()
		Expect(n.flush(10 * time.Second)).To(Succeed())

		want := sk.newer("g1", "c", 8)
		sk.expectRow(o.ts.URL, want, "origin")
		sk.expectRow(n.ts.URL, want, "local")
	})

	It("keeps the fields only the origin's newer schema knows across a local PUT", func() {
		sk := newSkew()
		o, n := sk.link()

		_, err := n.ws().Put(ctx, sk.older("g1", "b"))
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())

		want := sk.newer("g1", "b", 7)
		sk.expectRow(o.ts.URL, want, "origin")
		sk.expectRow(n.ts.URL, want, "local")
	})

	It("holds local writes while OnSchemaSkew blocks, and forwards them once the revisions agree", func() {
		sk := newSkew()
		o := newCOrigin()
		_, err := o.srv.RegisterDescriptorSet(dynamicSet(sk.ents[0]))
		Expect(err).NotTo(HaveOccurred())
		var (
			mu    sync.Mutex
			asked []string
		)
		n := newCNodeWith(o, "n0", func(c *upstream.Config) {
			c.Bindings = append(c.Bindings, upstream.Binding{Entity: "gadget"})
			c.OnSchemaSkew = func(ws, local, origin string) upstream.SkewAction {
				mu.Lock()
				defer mu.Unlock()
				asked = append(asked, ws+" "+local+" "+origin)
				return upstream.SkewBlock
			}
		}, func(srv *server.Server) {
			_, err := srv.RegisterDescriptorSet(dynamicSet(sk.ents[1]))
			Expect(err).NotTo(HaveOccurred())
		})
		addConv(n)
		local, origin := n.srv.SchemaRevision(), o.srv.SchemaRevision()
		Expect(local).NotTo(Equal(origin))
		Expect(n.status().SchemaSkew).To(Equal(&upstream.SchemaSkew{Local: local, Origin: origin, Action: upstream.SkewBlock}))

		_, err = n.ws().Put(ctx, sk.older("g2", "held"))
		Expect(err).NotTo(HaveOccurred())
		Consistently(func() proto.Message { return sk.readKey(o.ts.URL, "g2") }).
			WithTimeout(500*time.Millisecond).Should(BeNil(), "a blocked workspace forwards nothing")
		Expect(n.status().Pending).To(Equal(1))
		_, err = n.ws().Put(upstream.WriteThrough(ctx), sk.older("g3", "through"))
		Expect(err).To(MatchError(upstream.ErrSchemaSkew))

		// The local engine moves to the origin's schema.
		_, err = n.srv.RegisterDescriptorSet(dynamicSet(sk.ents[0]))
		Expect(err).NotTo(HaveOccurred())
		Expect(n.srv.SchemaRevision()).To(Equal(origin))
		Expect(n.flush(10 * time.Second)).To(Succeed())
		Expect(sk.readKey(o.ts.URL, "g2")).NotTo(BeNil())
		Expect(sk.readKey(o.ts.URL, "g3")).To(BeNil(), "the refused write-through write committed nothing")
		Expect(n.status().SchemaSkew).To(BeNil())
		mu.Lock()
		defer mu.Unlock()
		Expect(asked).To(Equal([]string{convWS + " " + local + " " + origin}))
	})

	It("keeps holding local writes while an origin that leaves the revision off stream pages streams them", func() {
		sk := newSkew()
		o := newCOrigin()
		_, err := o.srv.RegisterDescriptorSet(dynamicSet(sk.ents[0]))
		Expect(err).NotTo(HaveOccurred())
		// An origin from before stream pages carried the schema revision: it
		// sets the revision on each Subscribe frame, not on the page inside.
		syncPath, syncHandler := syncplanev1connect.NewSyncServiceHandler(o.srv, connect.WithInterceptors(stripPageRevision{}))
		mux := http.NewServeMux()
		for path, h := range o.srv.Handlers() {
			if path == syncPath {
				h = syncHandler
			}
			mux.Handle(path, h)
		}
		o.front.set(mux)

		n := newCNodeWith(o, "n0", func(c *upstream.Config) {
			c.Bindings = append(c.Bindings, upstream.Binding{Entity: "gadget"})
			c.OnSchemaSkew = func(string, string, string) upstream.SkewAction { return upstream.SkewBlock }
		}, func(srv *server.Server) {
			_, err := srv.RegisterDescriptorSet(dynamicSet(sk.ents[1]))
			Expect(err).NotTo(HaveOccurred())
		})
		addConv(n)
		Expect(n.status().SchemaSkew).NotTo(BeNil())

		_, err = n.ws().Put(ctx, sk.older("g2", "held"))
		Expect(err).NotTo(HaveOccurred())
		origin := o.client("other")
		for i := range 5 {
			_, err := origin.Put(ctx, &testdatav1.Widget{Id: fmt.Sprintf("w%d", i), Label: "from the origin"})
			Expect(err).NotTo(HaveOccurred())
		}
		Eventually(func() proto.Message { return dumpRows(n.srv, convWS)["widget"]["w4"] }).
			WithTimeout(10*time.Second).ShouldNot(BeNil(), "the stream delivers the origin's writes")
		Consistently(func() proto.Message { return sk.readKey(o.ts.URL, "g2") }).
			WithTimeout(500*time.Millisecond).Should(BeNil(), "a page without a revision does not lift the block")
		Expect(n.status().SchemaSkew).NotTo(BeNil())
	})

	It("follows origin edits of a text field only the origin's newer schema knows without bootstrapping again", func() {
		newer := server.DynamicEntity{
			Name: "gadget", Package: "conv.skewtext", KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "name", Number: 2, Kind: protoreflect.StringKind},
				{Name: "notes", Number: 3, Kind: protoreflect.BytesKind, Text: true},
			},
		}
		older := newer
		older.Fields = newer.Fields[:2]
		newerType, err := server.BuildDynamicMessageType(newer)
		Expect(err).NotTo(HaveOccurred())
		olderType, err := server.BuildDynamicMessageType(older)
		Expect(err).NotTo(HaveOccurred())
		row := func(name string, notes []byte) proto.Message {
			m := dynamicpb.NewMessage(newerType.Descriptor())
			fs := newerType.Descriptor().Fields()
			m.Set(fs.ByName("id"), protoreflect.ValueOfString("g1"))
			if name != "" {
				m.Set(fs.ByName("name"), protoreflect.ValueOfString(name))
			}
			if notes != nil {
				m.Set(fs.ByName("notes"), protoreflect.ValueOfBytes(notes))
			}
			return m
		}
		localName := func(n *cnode) string {
			GinkgoHelper()
			m := dynamicpb.NewMessage(olderType.Descriptor())
			Expect(n.srv.View(server.Trusted(ctx), convWS, func(tx *server.ViewTx) error {
				_, _, err := tx.Get(m, "g1")
				return err
			})).To(Succeed())
			return m.Get(olderType.Descriptor().Fields().ByName("name")).String()
		}

		o := newCOrigin(server.WithSchemaRevision("r2"))
		Expect(o.srv.RegisterDynamicEntity(newer)).To(Succeed())
		doc := text.New("origin")
		Expect(doc.Insert(0, "notes")).To(Succeed())
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error { return tx.Put(row("a", doc.Encode())) })).To(Succeed())
		n := newCNodeWith(o, "n0", func(c *upstream.Config) {
			c.Bindings = append(c.Bindings, upstream.Binding{Entity: "gadget"})
		}, func(srv *server.Server) {
			Expect(srv.RegisterDynamicEntity(older)).To(Succeed())
		}, server.WithSchemaRevision("r1"))
		addConv(n)
		Expect(localName(n)).To(Equal("a"))
		before := n.part.bootstraps.Load()

		for i := range 5 {
			Expect(doc.Insert(0, fmt.Sprintf("edit%d ", i))).To(Succeed())
			Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
				return tx.Patch(row("", doc.Encode()), []string{"notes"}, nil)
			})).To(Succeed())
			Eventually(func() uint64 { return n.status().OriginSeq }).Should(Equal(o.head()))
		}
		Expect(o.srv.Apply(ctx, convWS, func(tx *server.ApplyTx) error {
			return tx.Patch(row("b", nil), []string{"name"}, nil)
		})).To(Succeed())
		Eventually(func() string { return localName(n) }).Should(Equal("b"))
		Expect(n.part.bootstraps.Load()-before).To(BeZero(), "bootstraps after the link was caught up")
	})

	It("lets OnSchemaSkew ignore a skew", func() {
		sk := newSkew()
		o := newCOrigin(server.WithSchemaRevision("r2"))
		Expect(o.srv.RegisterDynamicEntity(sk.ents[0])).To(Succeed())
		n := newCNodeWith(o, "n0", func(c *upstream.Config) {
			c.Bindings = append(c.Bindings, upstream.Binding{Entity: "gadget"})
			c.OnSchemaSkew = func(string, string, string) upstream.SkewAction { return upstream.SkewIgnore }
		}, func(srv *server.Server) {
			Expect(srv.RegisterDynamicEntity(sk.ents[1])).To(Succeed())
		}, server.WithSchemaRevision("r1"))
		addConv(n)
		_, err := n.ws().Put(ctx, sk.older("g2", "sent"))
		Expect(err).NotTo(HaveOccurred())
		Expect(n.flush(5 * time.Second)).To(Succeed())
		Expect(sk.readKey(o.ts.URL, "g2")).NotTo(BeNil())
		Expect(n.status().SchemaSkew).To(BeNil())
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

// skew is a gadget entity in two schema revisions: the origin registers the
// newer one, with the extra field, and the linked node the older one.
type skew struct {
	newerType, olderType protoreflect.MessageType
	ents                 [2]server.DynamicEntity
}

func newSkew() *skew {
	GinkgoHelper()
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
	return &skew{newerType: newerType, olderType: olderType, ents: [2]server.DynamicEntity{newer, older}}
}

// link starts an origin at revision r2 holding gadget g1 {name a, extra 7}
// and a node at revision r1 linked to it and caught up.
func (sk *skew) link() (*corigin, *cnode) {
	GinkgoHelper()
	o := newCOrigin(server.WithSchemaRevision("r2"))
	Expect(o.srv.RegisterDynamicEntity(sk.ents[0])).To(Succeed())
	n := newCNodeWith(o, "n0", func(c *upstream.Config) {
		c.Bindings = append(c.Bindings, upstream.Binding{Entity: "gadget"})
	}, func(srv *server.Server) {
		Expect(srv.RegisterDynamicEntity(sk.ents[1])).To(Succeed())
	}, server.WithSchemaRevision("r1"))
	Expect(o.srv.Apply(context.Background(), convWS, func(tx *server.ApplyTx) error {
		return tx.Put(sk.newer("g1", "a", 7))
	})).To(Succeed())
	addConv(n)
	return o, n
}

func (sk *skew) newer(id, name string, extra int64) proto.Message {
	return gadget(sk.newerType, id, name, extra)
}

func (sk *skew) older(id, name string) proto.Message {
	return gadget(sk.olderType, id, name, 0)
}

// read returns gadget g1 as the server at url serves it, decoded with the
// newer schema so that fields the server does not know show up too.
func (sk *skew) read(url string) proto.Message {
	GinkgoHelper()
	return sk.readKey(url, "g1")
}

// readKey returns the gadget key names as the server at url serves it, nil
// when it serves none.
func (sk *skew) readKey(url, key string) proto.Message {
	GinkgoHelper()
	c, err := client.New(url)
	Expect(err).NotTo(HaveOccurred())
	snap, err := c.Workspace(convWS).Bootstrap(context.Background())
	Expect(err).NotTo(HaveOccurred())
	var got proto.Message
	for rc, err := range snap.Rows() {
		Expect(err).NotTo(HaveOccurred())
		if rc.GetEntity() == "gadget" && rc.GetKey() == key {
			got = dynamicpb.NewMessage(sk.newerType.Descriptor())
			Expect(proto.Unmarshal(rc.GetValue(), got)).To(Succeed())
		}
	}
	return got
}

func (sk *skew) expectRow(url string, want proto.Message, side string) {
	GinkgoHelper()
	got := sk.read(url)
	Expect(proto.Equal(got, want)).To(BeTrue(), "%s row %v, want %v", side, got, want)
}

// dynamicSet returns a descriptor set holding spec's file and every file it
// imports, imports first.
func dynamicSet(spec server.DynamicEntity) []byte {
	GinkgoHelper()
	fd, err := server.BuildDynamicFile(spec)
	Expect(err).NotTo(HaveOccurred())
	set := &descriptorpb.FileDescriptorSet{}
	seen := map[string]bool{}
	var add func(protoreflect.FileDescriptor)
	add = func(f protoreflect.FileDescriptor) {
		if seen[f.Path()] {
			return
		}
		seen[f.Path()] = true
		for i := range f.Imports().Len() {
			add(f.Imports().Get(i).FileDescriptor)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(f))
	}
	add(fd)
	raw, err := proto.Marshal(set)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func gadget(mt protoreflect.MessageType, id, name string, extra int64) proto.Message {
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

// stripPageRevision clears the schema revision of every page a Subscribe
// stream sends, as an origin from before pages carried it did.
type stripPageRevision struct{}

func (stripPageRevision) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc { return next }

func (stripPageRevision) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (stripPageRevision) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		return next(ctx, pageRevisionStripper{conn})
	}
}

type pageRevisionStripper struct{ connect.StreamingHandlerConn }

func (c pageRevisionStripper) Send(msg any) error {
	if r, ok := msg.(*syncplanev1.SubscribeResponse); ok && r.GetPage() != nil {
		r.GetPage().SchemaRevision = ""
	}
	return c.StreamingHandlerConn.Send(msg)
}
