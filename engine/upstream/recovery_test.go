package upstream_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream"
)

var _ = Describe("Recovery", func() {
	var ctx context.Context
	BeforeEach(func() { ctx = context.Background() })

	It("bootstraps again when the origin collected tombstones past the cursor, keeping pending writes", func() {
		o := newOrigin(server.WithGC(time.Millisecond, 0))
		o.put(&todov1.Todo{Id: "t1", Title: "doomed"}, &todov1.Todo{Id: "t2", Title: "stays"})
		n := newLocal(o, nil)
		n.add()
		n.flush()

		n.part.Cut()
		Expect(o.srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
			return tx.Delete(&todov1.Todo{}, "t1")
		})).To(Succeed())
		o.put(&todov1.Todo{Id: "t3", Title: "new"})
		time.Sleep(10 * time.Millisecond)
		Expect(o.srv.CollectGarbage(ctx, ws)).To(Succeed())
		n.put(&todov1.Todo{Id: "t7", Title: "pending"})

		n.part.Heal()
		n.flush()
		Expect(n.todo("t1")).To(BeNil())
		Expect(title(n.todo("t3"))).To(Equal("new"))
		Expect(title(n.todo("t7"))).To(Equal("pending"))
		Expect(title(o.todo("t7"))).To(Equal("pending"))
		n.mu.Lock()
		Expect(n.resets).To(Equal([]string{ws}))
		n.mu.Unlock()
	})

	It("finishes a snapshot that was cut off part way", func() {
		o := newOrigin(server.WithChunkSize(2))
		for i := range 20 {
			o.put(&todov1.Todo{Id: fmt.Sprintf("t%02d", i), Title: strings.Repeat(fmt.Sprintf("%x", i*7919), 40)})
		}
		n := newLocal(o, nil)
		n.part.breakBootstrap.Store(400)
		n.add()
		n.flush()
		Expect(n.part.bootstraps.Load()).To(BeNumerically(">=", 2))
		rows, err := upstream.Query[*todov1.Todo](ctx, n.ws(), query.Spec{})
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(20))
	})

	It("stops syncing a workspace the origin revoked", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		n.flush()
		Expect(o.srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		Eventually(func() upstream.State { return n.status().State }).Should(Equal(upstream.StateRevoked))
		n.mu.Lock()
		Expect(n.revoked).To(Equal([]string{ws}))
		n.mu.Unlock()

		_, err := n.ws().Put(ctx, &todov1.Todo{Id: "t1"})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "%v", err)
		Expect(rej.Code).To(Equal(server.CodeWorkspaceAccessRevoked))
		Expect(n.link.Flush(ctx, ws)).To(MatchError(upstream.ErrRevoked))

		n.restart()
		Expect(n.status().State).To(Equal(upstream.StateRevoked))
	})

	It("reports the stored pending writes as soon as a restarted link starts", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		n.flush()

		// Given three local writes the origin cannot receive
		n.part.Cut()
		n.put(&todov1.Todo{Id: "t1", Title: "one"})
		n.put(&todov1.Todo{Id: "t2", Title: "two"})
		n.put(&todov1.Todo{Id: "t3", Title: "three"})
		Eventually(func() int { return n.status().Pending }).Should(Equal(3))

		// When the link restarts while the origin is still cut off
		n.restart()

		// Then its first status already counts them
		Expect(n.status().Pending).To(Equal(3))

		n.part.Heal()
		n.flush()
		Expect(n.status().Pending).To(BeZero())
		Expect(title(o.todo("t3"))).To(Equal("three"))
	})

	It("converges concurrent edits of a collaborative text field", func() {
		o := newOrigin()
		n := newLocal(o, nil)
		n.add()
		n.flush()
		putNote := func(srv *server.Server, peer, body string) {
			GinkgoHelper()
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Put(&todov1.Note{Id: "n1"}, server.WithTextString("body", body), server.WithTextPeer(peer))
			})).To(Succeed())
		}
		putNote(n.srv, "local", "hello")
		n.flush()

		n.part.Cut()
		putNote(n.srv, "local", "hello world")
		putNote(o.srv, "origin", "oh hello")
		n.part.Heal()
		n.flush()

		bodyOf := func(srv *server.Server) string {
			GinkgoHelper()
			d, err := text.Decode(getRow[*todov1.Note](srv, "n1").GetBody(), "")
			Expect(err).NotTo(HaveOccurred())
			return d.PlainText()
		}
		Expect(bodyOf(o.srv)).To(Equal("oh hello world"))
		Expect(bodyOf(n.srv)).To(Equal("oh hello world"))

		// A later origin edit arrives as a fragment the link folds.
		putNote(o.srv, "origin", "oh hello world!")
		Eventually(func() string { return bodyOf(n.srv) }).Should(Equal("oh hello world!"))
	})

	It("keeps a row it cannot store at its confirmed state until the origin decides the write that blocks it", func() {
		folders := server.WithEntities(&accessv1.Folder{}, &accessv1.FolderMember{}, &accessv1.Doc{})
		o := newOrigin(folders, server.WithAuthenticator(server.AuthenticatorFunc(
			func(_ context.Context, h http.Header) (server.Principal, error) {
				return server.Principal{Subject: h.Get("x-user")}, nil
			})))
		Expect(o.srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			for _, m := range []proto.Message{
				&accessv1.Folder{Id: "root"},
				&accessv1.Folder{Id: "a", Parent: &accessv1.Folder{Id: "root"}},
				&accessv1.Folder{Id: "b", Parent: &accessv1.Folder{Id: "root"}},
				&accessv1.FolderMember{Id: "m1", Folder: &accessv1.Folder{Id: "root"}, UserId: "boss", Role: "folder_manager"},
			} {
				if err := tx.Put(m); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		var part *partition
		n := newLocal(o, func(c *upstream.Config) {
			part = c.HTTPClient.(*http.Client).Transport.(*partition)
			c.HTTPClient = &http.Client{Transport: headerTransport{base: part, user: "boss"}}
			c.Bindings = []upstream.Binding{
				{Prototype: &accessv1.Folder{}}, {Prototype: &accessv1.FolderMember{}}, {Prototype: &accessv1.Doc{}},
			}
		}, folders)
		n.add()
		n.flush()
		parent := func(srv *server.Server, id string) string {
			GinkgoHelper()
			f := &accessv1.Folder{}
			Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				_, _, err := tx.Get(f, id)
				return err
			})).To(Succeed())
			return f.GetParent().GetId()
		}
		Expect(parent(n.srv, "b")).To(Equal("root"))

		part.blockPush.Store(true)
		_, err := n.ws().Patch(server.Trusted(ctx), &accessv1.Folder{Id: "a", Parent: &accessv1.Folder{Id: "b"}}, []string{"parent"}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(o.srv.Apply(server.Trusted(ctx), ws, func(tx *server.ApplyTx) error {
			return tx.Patch(&accessv1.Folder{Id: "b", Parent: &accessv1.Folder{Id: "a"}}, []string{"parent"}, nil)
		})).To(Succeed())
		// The origin's move of b would close a cycle with the pending move of
		// a, which the local engine refuses to store.
		head, err := o.srv.CurrentSeq(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() uint64 { return n.status().OriginSeq }).Should(Equal(head))
		Expect(parent(n.srv, "b")).To(Equal("root"))
		Expect(parent(n.srv, "a")).To(Equal("b"))

		part.blockPush.Store(false)
		n.flush()
		Expect(n.rejected()).To(HaveLen(1))
		Expect(parent(n.srv, "a")).To(Equal("root"))
		Expect(parent(n.srv, "b")).To(Equal("a"))
	})
})

// headerTransport names the caller of every request.
type headerTransport struct {
	base http.RoundTripper
	user string
}

func (t headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("x-user", t.user)
	return t.base.RoundTrip(req)
}
