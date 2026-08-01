package server_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Manager", func() {
	noteSpec := server.DynamicEntity{
		Name:      "note",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
		},
	}

	var (
		ctx context.Context
		mt  protoreflect.MessageType
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		mt, err = server.BuildDynamicMessageType(noteSpec)
		Expect(err).NotTo(HaveOccurred())
	})

	// newManager builds a Manager whose apps are closed at spec end — every app
	// server runs background loops, so a leaked Manager leaks goroutines.
	newManager := func(store storage.Adapter, opts ...server.ManagerOption) *server.Manager {
		GinkgoHelper()
		m, err := server.NewManager(ctx, store, opts...)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(m.Close()).To(Succeed()) })
		return m
	}

	// syncClient opens an httptest server over the manager's routed sync handler
	// and returns a SyncService client addressed at "/<appID>".
	syncClient := func(m *server.Manager, appID string) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		ts := httptest.NewServer(m.SyncHandler())
		DeferCleanup(ts.Close)
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL+"/"+appID)
	}
	putNote := func(cl syncplanev1connect.SyncServiceClient, key, title string) {
		GinkgoHelper()
		val := buildNote(mt, map[string]any{"id": key, "title": title})
		_, err := cl.Push(ctx, connect.NewRequest(pushOf(
			ws,
			[]*syncplanev1.Mutation{putMutation("c1", 1, "note", key, val)})))
		Expect(err).NotTo(HaveOccurred())
	}
	pullKeys := func(cl syncplanev1connect.SyncServiceClient) []string {
		GinkgoHelper()
		resp, err := cl.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		Expect(err).NotTo(HaveOccurred())
		var keys []string
		for _, c := range resp.Msg.GetPatch().GetChanges() {
			keys = append(keys, c.GetKey())
		}
		return keys
	}

	It("registers an app and serves CRUD over the routed handler", func() {
		m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))

		res, err := m.Register(ctx, "app1", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Status).To(Equal(server.RegisterCreated))
		Expect(res.Entities).To(Equal([]string{"note"}))

		cl := syncClient(m, "app1")
		putNote(cl, "n1", "hello")
		Expect(pullKeys(cl)).To(ConsistOf("n1"))
	})

	It("404s an unknown app on the routed handler", func() {
		m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))

		cl := syncClient(m, "ghost")
		_, err := cl.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		Expect(err).To(HaveOccurred())
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnimplemented), "404 surfaces as an unimplemented procedure")
	})

	It("isolates two apps that declare the same entity name", func() {
		m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))
		_, err := m.Register(ctx, "a", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		_, err = m.Register(ctx, "b", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())

		putNote(syncClient(m, "a"), "shared", "only in a")
		Expect(pullKeys(syncClient(m, "a"))).To(ConsistOf("shared"))
		Expect(pullKeys(syncClient(m, "b"))).To(BeEmpty(), "app b's note partition is independent")
	})

	It("reloads persisted schemas on restart", func() {
		path := filepath.Join(GinkgoT().TempDir(), "restart.db")

		store1, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		m1 := newManager(store1)
		_, err = m1.Register(ctx, "app1", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		putNote(syncClient(m1, "app1"), "n1", "persisted")
		Expect(store1.Close()).To(Succeed())

		// Reopen the same database: the new Manager re-registers app1 from the
		// persisted descriptor set before serving, and the row is still there.
		store2, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store2.Close()).To(Succeed()) })
		m2 := newManager(store2)
		Expect(pullKeys(syncClient(m2, "app1"))).To(ConsistOf("n1"), "schema and data survive restart")
	})

	Describe("admin service trust", func() {
		adminClient := func(m *server.Manager) syncplanev1connect.AdminServiceClient {
			GinkgoHelper()
			_, h := m.AdminHandler()
			ts := httptest.NewServer(h)
			DeferCleanup(ts.Close)
			return syncplanev1connect.NewAdminServiceClient(ts.Client(), ts.URL)
		}
		register := func(cl syncplanev1connect.AdminServiceClient, token string) error {
			req := connect.NewRequest(&syncplanev1.RegisterRequest{
				AppId:         "app1",
				DescriptorSet: descriptorSetBytes(noteSpec),
			})
			if token != "" {
				req.Header().Set("Authorization", "Bearer "+token)
			}
			_, err := cl.Register(ctx, req)
			return err
		}

		It("denies registration by default (no authorizer)", func() {
			m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))
			Expect(connect.CodeOf(register(adminClient(m), ""))).To(Equal(connect.CodePermissionDenied))
		})

		It("admits a correct bearer token and denies a wrong one", func() {
			m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")),
				server.WithRegisterAuthorizer(server.TokenAuthorizer("s3cret")))
			cl := adminClient(m)

			Expect(connect.CodeOf(register(cl, "wrong"))).To(Equal(connect.CodePermissionDenied))
			Expect(register(cl, "s3cret")).To(Succeed())

			// GetSchema ships the registered bytes back.
			resp, err := cl.GetSchema(ctx, connect.NewRequest(&syncplanev1.GetSchemaRequest{AppId: "app1"}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetFound()).To(BeTrue())
			Expect(resp.Msg.GetDescriptorSet()).To(Equal(descriptorSetBytes(noteSpec)))
		})
	})

	Describe("Close", func() {
		It("stops the hosted apps' background work", func() {
			notifier := &countingNotifier{Local: pubsub.NewLocal()}
			m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")),
				server.WithServerOptions(
					server.WithNotifier(notifier),
					// Wide enough that both pushes land in one cooldown, so a
					// trailing poke is owed when Close runs.
					server.WithNotifyCoalescing(500*time.Millisecond)))
			_, err := m.Register(ctx, "app1", descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
			cl := syncClient(m, "app1")
			for i, key := range []string{"n1", "n2"} {
				_, err := cl.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
					// Distinct mutation ids: a repeat of one already acked is
					// deduplicated, and would never reach the notifier.
					putMutation("c1", uint64(i+1), "note", key, buildNote(mt, map[string]any{"id": key})),
				})))
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(notifier.notifies.Load()).To(BeEquivalentTo(1))

			Expect(m.Close()).To(Succeed())
			Consistently(notifier.notifies.Load, "1s").Should(BeEquivalentTo(1),
				"an app server's timers must not outlive the Manager that created it")
		})

		It("refuses to host an app registered after Close", func() {
			m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))
			Expect(m.Close()).To(Succeed())
			_, err := m.Register(ctx, "late", descriptorSetBytes(noteSpec))
			Expect(err).To(MatchError(ContainSubstring("manager is closed")),
				"a new app server started here would leak its background loops")
		})

		It("keeps serving an app that was already hosted", func() {
			m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))
			_, err := m.Register(ctx, "app1", descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
			cl := syncClient(m, "app1")
			putNote(cl, "n1", "before close")

			Expect(m.Close()).To(Succeed())
			// Close stops background work, not request handling: the store is
			// still open and in-flight traffic must not start failing.
			Expect(pullKeys(cl)).To(ConsistOf("n1"))
		})

		It("is idempotent", func() {
			m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))
			_, err := m.Register(ctx, "app1", descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
			Expect(m.Close()).To(Succeed())
			Expect(m.Close()).To(Succeed())
		})
	})
})

// newRootStore opens a fresh sqlite store and registers its cleanup. It is the
// namespaceable root a Manager partitions per app.
func newRootStore(path string) storage.Adapter {
	GinkgoHelper()
	store, err := sqlitestore.Open(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	return store
}
