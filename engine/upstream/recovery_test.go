package upstream_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
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

	It("settles a write the deleted origin workspace applied once it bootstraps the restored one", func() {
		o := newOrigin()
		o.put(&todov1.Todo{Id: "t1", Title: "old"})
		n := newLocal(o, nil)
		n.add()
		n.flush()

		// The origin applies t7, and no page confirms it to the link.
		n.part.blockReads.Store(true)
		n.put(&todov1.Todo{Id: "t7", Title: "pending"})
		Eventually(func() string { return title(o.todo("t7")) }).Should(Equal("pending"))
		Expect(o.srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		Expect(o.srv.RestoreWorkspace(ctx, ws)).To(Succeed())
		o.put(&todov1.Todo{Id: "r1", Title: "restored"})

		n.part.blockReads.Store(false)
		Eventually(func() string { return title(n.todo("r1")) }).Should(Equal("restored"))
		// OnReset runs after the snapshot's rows commit.
		Eventually(func() []string {
			n.mu.Lock()
			defer n.mu.Unlock()
			return slices.Clone(n.resets)
		}).Should(Equal([]string{ws}))
		fctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		Expect(n.link.Flush(fctx, ws)).To(Succeed(), "status %+v", n.status())
		Expect(n.status().Pending).To(BeZero())
		Expect(n.todo("t1")).To(BeNil())
		Expect(n.todo("t7")).To(Equal(o.todo("t7")), "the local engine holds what the origin holds")
	})

	It("asks for no freshness above the restored origin workspace's head after bootstrapping it", func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "origin.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)
		floors := &floorRecorder{Adapter: store}
		srv, err := server.New(floors, server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(srv.Close)
		ts := httptest.NewServer(srv.HTTPHandler())
		DeferCleanup(ts.Close)
		o := &origin{srv: srv, ts: ts, store: store}
		n := newLocal(o, nil)
		n.add()
		for i := range 5 {
			n.put(&todov1.Todo{Id: fmt.Sprintf("t%d", i), Title: "old"})
		}
		n.flush()

		n.part.Cut()
		Expect(o.srv.DeleteWorkspace(ctx, ws)).To(Succeed())
		Expect(o.srv.RestoreWorkspace(ctx, ws)).To(Succeed())
		o.put(&todov1.Todo{Id: "r1", Title: "restored"})
		n.part.Heal()
		Eventually(func() string { return title(n.todo("r1")) }).Should(Equal("restored"))
		// OnReset runs after the bootstrap's last page commits, so r1 can
		// show before the reset is recorded.
		Eventually(func() []string {
			n.mu.Lock()
			defer n.mu.Unlock()
			return slices.Clone(n.resets)
		}).Should(Equal([]string{ws}))
		n.flush()

		// The reads of the next start come from the restored workspace's
		// lineage only.
		mark := floors.count()
		n.restart()
		n.flush()
		head, err := o.srv.CurrentSeq(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		after := floors.since(mark)
		Expect(after).NotTo(BeEmpty())
		Expect(slices.Max(after)).To(BeNumerically("<=", head), "floors %v over head %d", after, head)
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

	It("does not drop the writes of a link started over a copy of another link's store", func() {
		o := newOrigin()
		a := newLocal(o, nil)
		a.add()
		a.flush()
		// b starts over a copy of a's store, as a copied data directory
		// does: same client id, same queue.
		a.stop()
		b := newLocal(o, nil)
		b.stop()
		dir := GinkgoT().TempDir()
		copyDB(a.path, dir)
		restoreDB(dir, b.path)
		a.start()
		b.start()
		a.flush()
		b.flush()
		Expect(b.link.ClientID()).To(Equal(a.link.ClientID()))

		a.put(&todov1.Todo{Id: "t1", Title: "from a"})
		a.flush()
		Eventually(func() string { return title(b.todo("t1")) }).Should(Equal("from a"))
		b.put(&todov1.Todo{Id: "t2", Title: "from b"})
		b.flush()
		Expect(title(o.todo("t2")) == "from b" || len(b.rejected()) > 0).To(BeTrue(),
			"b's write neither reached the origin nor was reported: local row %v", b.todo("t2"))
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

// floorRecorder serves every read from the store itself and records the
// freshness floor each read through its ReplicaReader asked for.
type floorRecorder struct {
	storage.Adapter
	mu     sync.Mutex
	floors []uint64
}

func (r *floorRecorder) ReplicaReader() storage.ReplicaReader { return r }

func (r *floorRecorder) WithReadTxMinSeq(ctx context.Context, ws string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	r.mu.Lock()
	r.floors = append(r.floors, minSeq)
	r.mu.Unlock()
	return storage.ReadRouteReplica, r.Adapter.WithReadTx(ctx, ws, fn)
}

func (r *floorRecorder) WithReadTxAnchored(ctx context.Context, ws string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return storage.ReadRouteReplica, r.Adapter.WithReadTx(ctx, ws, fn)
}

func (r *floorRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.floors)
}

func (r *floorRecorder) since(mark int) []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.floors[mark:])
}
