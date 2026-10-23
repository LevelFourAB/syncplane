package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

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

	It("stores each app's workspaces under its own prefix", func() {
		// The partition the isolation above rests on, asserted from below: the
		// adapter holds one flat space of workspaces and the app id is part of
		// the id, not a column it knows about.
		store := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
		m := newManager(store)
		_, err := m.Register(ctx, "a", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		putNote(syncClient(m, "a"), "shared", "only in a")

		Expect(store.ListWorkspaceIDs(ctx, storage.Range{})).To(ConsistOf("a/" + ws))
	})

	It("keeps two apps apart on one shared notifier", func() {
		// The reason a Manager can be handed ONE notifier for every app it
		// hosts: each app's server prefixes the ids it publishes, so a write
		// to "ws" in one app does not wake the other app's watchers of the
		// same name.
		notifier := pubsub.NewLocal()
		m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")),
			server.WithServerOptions(server.WithNotifier(notifier)))
		for _, app := range []string{"a", "b"} {
			_, err := m.Register(ctx, app, descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
		}

		woken, cancel := notifier.Subscribe("a/" + ws)
		DeferCleanup(cancel)
		quiet, cancelQuiet := notifier.Subscribe("b/" + ws)
		DeferCleanup(cancelQuiet)

		putNote(syncClient(m, "a"), "n1", "only in a")

		Eventually(woken).Should(Receive())
		Consistently(quiet, "100ms").ShouldNot(Receive())
	})

	It("reports its own workspaces on the activity stream", func() {
		// The activity stream is the one place ids come back from a shared
		// notifier on a channel: a hosted app must see its own writes under
		// the ids it uses, and never the other app's at all.
		notifier := pubsub.NewLocal()
		m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")),
			server.WithServerOptions(server.WithNotifier(notifier)))
		for _, app := range []string{"a", "b"} {
			_, err := m.Register(ctx, app, descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
		}

		ids, cancel, ok := m.App("a").SubscribeActivity()
		Expect(ok).To(BeTrue())
		DeferCleanup(cancel)

		putNote(syncClient(m, "b"), "n1", "only in b")
		Consistently(ids, "100ms").ShouldNot(Receive())

		putNote(syncClient(m, "a"), "n1", "only in a")
		Eventually(ids).Should(Receive(Equal(ws)))
	})

	It("refuses an app id that would forge another app's prefix", func() {
		m := newManager(newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db")))
		_, err := m.Register(ctx, "a/b", descriptorSetBytes(noteSpec))
		Expect(err).To(MatchError(ContainSubstring("app id")))
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

	It("stores the schema on a retry of a registration the catalog failed to store", func() {
		// Given app a, and a catalog that fails the next write
		root := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
		failing := &failingCatalog{Adapter: root, inner: root.(storage.Catalog)}
		m := newManager(failing)
		_, err := m.Register(ctx, "a", descriptorSetBytes(plainSpec("note")))
		Expect(err).NotTo(HaveOccurred())
		failing.fail.Store(true)
		_, err = m.Register(ctx, "a", descriptorSetBytes(plainSpec("label")))
		Expect(err).To(MatchError(ContainSubstring("persist app schema")))
		Expect(entityNamesOf(m.App("a"))).NotTo(ContainElement("label"),
			"the app serves only what the catalog stores")
		failing.fail.Store(false)

		// When the operator retries the registration that failed
		res, err := m.Register(ctx, "a", descriptorSetBytes(plainSpec("label")))

		// Then the retry stores the schema, and a restart serves label
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Status).To(Equal(server.RegisterUpdated))
		Expect(entityNamesOf(newManager(root).App("a"))).To(ContainElement("label"))
	})

	Describe("across nodes over one root adapter", func() {
		// newNode builds a Manager that authenticates callers by header, so
		// that pulls run as a user of the access model.
		newNode := func(root storage.Adapter, opts ...server.ManagerOption) *server.Manager {
			GinkgoHelper()
			return newManager(root, append([]server.ManagerOption{
				server.WithServerOptions(headerAuthenticator()),
			}, opts...)...)
		}
		// pullAs pulls app's workspace ws from an empty cursor as user, and
		// returns the live rows it shows as "entity/key".
		pullAs := func(m *server.Manager, app, user string) []string {
			GinkgoHelper()
			req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-" + user})
			as(user, req.Header())
			resp, err := syncClient(m, app).Pull(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			var rows []string
			for _, rc := range resp.Msg.GetPatch().GetChanges() {
				if !rc.GetTombstone() {
					rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
				}
			}
			return rows
		}
		// seedReader registers app a on node with an org_reader role that
		// reads tasks, and gives u that role and a task t1 to read.
		seedReader := func(node *server.Manager) {
			GinkgoHelper()
			_, err := node.Register(ctx, "a", descriptorSetBytes(readerSpecs(true)...))
			Expect(err).NotTo(HaveOccurred())
			srv := node.App("a")
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				if err := tx.Put(dynamicRow(srv, "org_member", map[string]string{"id": "om-u", "user_id": "u", "role": "org_reader"})); err != nil {
					return err
				}
				return tx.Put(dynamicRow(srv, "task", map[string]string{"id": "t1"}))
			})).To(Succeed())
		}
		// narrowReader makes org_reader stop reading tasks, through node.
		narrowReader := func(node *server.Manager) {
			GinkgoHelper()
			_, err := node.Register(ctx, "a", descriptorSetBytes(readerSpecs(false)...), server.AllowAccessChange())
			Expect(err).NotTo(HaveOccurred())
			Expect(pullAs(node, "a", "u")).NotTo(ContainElement("task/t1"))
		}
		milestone := server.DynamicEntity{
			Name: "milestone", KeyFields: []string{"id"},
			Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
			Access: readWrite("view_projects", "edit_projects"),
		}

		It("serves a schema another node stored once that node pokes it over a shared notifier", func() {
			// Given nodes A and B with one notifier and no catalog poll, where
			// u reads task t1 through org_reader
			root := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
			shared := server.WithServerOptions(server.WithNotifier(pubsub.NewLocal()))
			nodeA := newNode(root, shared, server.WithCatalogPollInterval(0))
			seedReader(nodeA)
			nodeB := newNode(root, shared, server.WithCatalogPollInterval(0))
			Expect(pullAs(nodeB, "a", "u")).To(ContainElement("task/t1"))

			// When node A narrows org_reader so that it no longer reads tasks
			narrowReader(nodeA)

			// Then node B stops showing u the task
			Eventually(func() []string { return pullAs(nodeB, "a", "u") }).
				ShouldNot(ContainElement("task/t1"))
		})

		It("serves a schema another node stored on the next catalog poll", func() {
			// Given nodes A and B with their own notifiers, where u reads task
			// t1 through org_reader
			root := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
			nodeA := newNode(root, server.WithCatalogPollInterval(0))
			seedReader(nodeA)
			nodeB := newNode(root, server.WithCatalogPollInterval(20*time.Millisecond))
			Expect(pullAs(nodeB, "a", "u")).To(ContainElement("task/t1"))

			// When node A narrows org_reader
			narrowReader(nodeA)

			// Then node B stops showing u the task
			Eventually(func() []string { return pullAs(nodeB, "a", "u") }).
				ShouldNot(ContainElement("task/t1"))
		})

		It("hosts an app that another node registered first on the next catalog poll", func() {
			// Given node B, which hosts no app
			root := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
			nodeB := newNode(root, server.WithCatalogPollInterval(20*time.Millisecond))
			nodeA := newNode(root, server.WithCatalogPollInterval(0))

			// When node A registers app a
			_, err := nodeA.Register(ctx, "a", descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())

			// Then node B serves it
			Eventually(func() []string { return entityNamesOf(nodeB.App("a")) }).Should(ContainElement("note"))
		})

		It("merges a registration over the schema another node stored, not over the one it served", func() {
			// Given nodes A and B that do not hear from each other, and node A
			// narrowed org_reader after node B loaded the schema
			root := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
			nodeA := newNode(root, server.WithCatalogPollInterval(0))
			seedReader(nodeA)
			nodeB := newNode(root, server.WithCatalogPollInterval(0))
			narrowReader(nodeA)

			// When node B registers an unrelated new entity, without
			// AllowAccessChange
			_, err := nodeB.Register(ctx, "a", descriptorSetBytes(milestone))
			Expect(err).NotTo(HaveOccurred())

			// Then node B and a node that starts later serve the narrowed role
			Expect(pullAs(nodeB, "a", "u")).NotTo(ContainElement("task/t1"))
			nodeC := newNode(root, server.WithCatalogPollInterval(0))
			Expect(pullAs(nodeC, "a", "u")).NotTo(ContainElement("task/t1"),
				"node B stored its stale schema over node A's, which widened org_reader without AllowAccessChange")
			Expect(entityNamesOf(nodeC.App("a"))).To(ContainElement("milestone"))
		})

		It("keeps every registration of two nodes that register at the same time", func() {
			// Given nodes A and B that do not hear from each other
			root := newRootStore(filepath.Join(GinkgoT().TempDir(), "mgr.db"))
			nodes := []*server.Manager{
				newNode(root, server.WithCatalogPollInterval(0)),
				newNode(root, server.WithCatalogPollInterval(0)),
			}

			// When each node registers four entities of app a, the two nodes
			// at the same time
			var wg sync.WaitGroup
			errs := make([]error, 8)
			for i := range errs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					_, errs[i] = nodes[i%2].Register(ctx, "a", descriptorSetBytes(plainSpec(fmt.Sprintf("e%d", i))))
				}()
			}
			wg.Wait()

			// Then each succeeded, and a node that starts later serves all eight
			for _, err := range errs {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(entityNamesOf(newNode(root, server.WithCatalogPollInterval(0)).App("a"))).
				To(ContainElements("e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7"))
		})
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
			// The registration pokes the other nodes once; only the pushes
			// count here.
			notifier.notifies.Store(0)
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

// failingCatalog is a root adapter whose catalog refuses writes while fail is
// set.
type failingCatalog struct {
	storage.Adapter
	inner storage.Catalog
	fail  atomic.Bool
}

func (c *failingCatalog) Unwrap() storage.Adapter { return c.Adapter }

func (c *failingCatalog) PutCatalogEntry(ctx context.Context, id string, payload []byte, revision string) error {
	if c.fail.Load() {
		return errors.New("catalog write failed")
	}
	return c.inner.PutCatalogEntry(ctx, id, payload, revision)
}

func (c *failingCatalog) SwapCatalogEntry(ctx context.Context, id string, payload []byte, revision, prev string) error {
	if c.fail.Load() {
		return errors.New("catalog write failed")
	}
	return c.inner.SwapCatalogEntry(ctx, id, payload, revision, prev)
}

func (c *failingCatalog) ListCatalogEntries(ctx context.Context) ([]storage.CatalogEntry, error) {
	return c.inner.ListCatalogEntries(ctx)
}

func (c *failingCatalog) DeleteCatalogEntry(ctx context.Context, id string) error {
	return c.inner.DeleteCatalogEntry(ctx, id)
}

// readerSpecs is the org schema with an org_reader role that reads projects,
// and tasks when readsTasks is set.
func readerSpecs(readsTasks bool) []server.DynamicEntity {
	specs := orgSpecs()
	reader := &syncplanev1.Role{Name: "org_reader", Grants: []string{"view_projects"}}
	if readsTasks {
		reader.Grants = append(reader.Grants, "view_tasks")
	}
	g := specNamed(specs, "org_member").Grant
	g.Roles = append(g.Roles, reader)
	return specs
}

// dynamicRow builds a row of entity as srv serves it, with string fields set
// by name.
func dynamicRow(srv *server.Server, entity string, fields map[string]string) proto.Message {
	GinkgoHelper()
	meta, ok := srv.Entity(entity)
	Expect(ok).To(BeTrue(), "entity %q is not registered", entity)
	msg := dynamicpb.NewMessage(meta.Descriptor)
	for name, v := range fields {
		fd := meta.Descriptor.Fields().ByName(protoreflect.Name(name))
		Expect(fd).NotTo(BeNil(), "no field %q", name)
		msg.Set(fd, protoreflect.ValueOfString(v))
	}
	return msg
}

// entityNamesOf returns the names of the entities srv serves, sorted; nil when
// srv is nil.
func entityNamesOf(srv *server.Server) []string {
	if srv == nil {
		return nil
	}
	var out []string
	for _, e := range srv.Entities() {
		out = append(out, e.Name)
	}
	sort.Strings(out)
	return out
}

// newRootStore opens a fresh sqlite store and registers its cleanup. It is the
// namespaceable root a Manager partitions per app.
func newRootStore(path string) storage.Adapter {
	GinkgoHelper()
	store, err := sqlitestore.Open(path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	return store
}
