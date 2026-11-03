package upstream_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream"
)

var _ = Describe("Confirmed rows", func() {
	var (
		ctx context.Context
		o   *origin
		n   *local
	)

	putNote := func(srv *server.Server, peer, body string) {
		GinkgoHelper()
		Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
			return tx.Put(&todov1.Note{Id: "n1"}, server.WithTextString("body", body), server.WithTextPeer(peer))
		})).To(Succeed())
	}

	noteText := func(srv *server.Server) string {
		GinkgoHelper()
		note := getRow[*todov1.Note](srv, "n1")
		if note == nil {
			return ""
		}
		s, err := text.PlainText(note.GetBody())
		Expect(err).NotTo(HaveOccurred())
		return s
	}

	// record is the row value the link keeps as the confirmed copy of a local
	// row, nil when it keeps none.
	record := func(entity, key string) []byte {
		GinkgoHelper()
		found, value, err := upstream.ConfirmedRecord(ctx, n.store, ws, entity, key)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "the link keeps a record of %s %q", entity, key)
		return value
	}

	// synced waits until the origin's latest write is local.
	sentinels := 0
	synced := func() {
		GinkgoHelper()
		sentinels++
		key := fmt.Sprintf("sentinel-%d", sentinels)
		o.put(&todov1.Todo{Id: key})
		Eventually(func() *todov1.Todo { return n.todo(key) }).NotTo(BeNil())
	}

	BeforeEach(func() {
		ctx = context.Background()
		o = newOrigin(server.WithPermissions(denyForbidden{}))
		o.put(&todov1.Todo{Id: "t1", Title: "a"})
		putNote(o.srv, "origin", "hello")
		n = newLocal(o, nil)
		n.add()
		n.flush()
		synced()
	})

	It("keeps no copy of the value of a row no local write is pending for", func() {
		Expect(record("todo", "t1")).To(BeNil())
		Expect(record("note", "n1")).To(BeNil())
	})

	It("keeps the origin's row while a local write to it is pending, and drops it once the origin confirms the write", func() {
		n.part.Cut()
		n.patch(&todov1.Todo{Id: "t1", Title: "b"}, "title")
		var kept todov1.Todo
		Expect(proto.Unmarshal(record("todo", "t1"), &kept)).To(Succeed())
		Expect(kept.GetTitle()).To(Equal("a"))

		n.part.Heal()
		n.flush()
		Eventually(func() []byte { return record("todo", "t1") }).Should(BeNil())
		Expect(title(n.todo("t1"))).To(Equal("b"))
	})

	It("folds the origin's text edits onto a row it keeps no copy of", func() {
		boots := n.part.bootstraps.Load()
		putNote(o.srv, "origin", "hello world")
		Eventually(func() string { return noteText(n.srv) }).Should(Equal("hello world"))
		Expect(n.part.bootstraps.Load()).To(Equal(boots), "the text edit arrived as a delta the link could fold")
		Expect(record("note", "n1")).To(BeNil())
	})

	It("takes a fresh snapshot when a row it keeps no copy of was rewritten outside the link", func() {
		// A write straight to the store, past the engine and so past the
		// capture: the local row no longer is the row the origin confirmed.
		drift, err := proto.Marshal(&todov1.Todo{Id: "t1", Title: "drift"})
		Expect(err).NotTo(HaveOccurred())
		Expect(n.store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			seq, err := tx.NextSeq()
			if err != nil {
				return err
			}
			return tx.PutRow("todo", "t1", drift, seq, storage.Actor{})
		})).To(Succeed())
		boots := n.part.bootstraps.Load()

		// The origin rejects this write, so the local row goes back to the
		// confirmed row, which the link must not take from the drifted one.
		n.patch(&todov1.Todo{Id: "t1", Title: "forbidden"}, "title")
		Eventually(n.rejected).Should(HaveLen(1))
		Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("a"))
		Expect(n.part.bootstraps.Load()).To(BeNumerically(">", boots))
	})

	It("leaves bound rows out of the local engine's text compaction", func() {
		n.stop()
		n = newLocal(o, nil,
			server.WithGC(time.Millisecond, 0),
			server.WithTextDeltaRetention(time.Millisecond),
			server.WithTextCompaction(1),
		)
		n.add()
		n.flush()
		putNote(o.srv, "origin", "hello world")
		putNote(o.srv, "origin", "hello")
		synced()
		Eventually(func() string { return noteText(n.srv) }).Should(Equal("hello"))
		before := getRow[*todov1.Note](n.srv, "n1").GetBody()

		time.Sleep(5 * time.Millisecond)
		Expect(n.srv.CollectGarbage(ctx, ws)).To(Succeed())
		rewritten, err := n.srv.CompactText(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(rewritten).To(BeZero())
		Expect(getRow[*todov1.Note](n.srv, "n1").GetBody()).To(Equal(before))
	})
})
