package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// appPerms is a per-app policy: it admits a principal to a workspace only if
// the membership function says this app grants it, and records every principal
// it saw so a spec can prove which app's policy answered.
type appPerms struct {
	server.AllowAll
	appID  string
	member func(appID string, p server.Principal, ws string) bool

	mu   sync.Mutex
	seen []server.Principal
}

func (a *appPerms) CanAccessWorkspace(_ context.Context, p server.Principal, ws string) error {
	a.mu.Lock()
	a.seen = append(a.seen, p)
	a.mu.Unlock()
	if a.member != nil && !a.member(a.appID, p, ws) {
		return server.DenyWorkspace("not a member of this workspace")
	}
	return nil
}

func (a *appPerms) principals() []server.Principal {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]server.Principal(nil), a.seen...)
}

var _ = Describe("Manager authentication and per-app policy", func() {
	const (
		docsApp    = "docs"
		billingApp = "billing"
		aliceToken = "alice-token"
		aliceWS    = "w-alice"
	)

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
		// policies holds the policy handed to each app, keyed by app id, so a
		// spec can inspect what that app's hooks saw.
		policies map[string]*appPerms
		polMu    sync.Mutex
	)

	// auth is the ONE authentication layer the whole collection shares: alice's
	// token maps to her principal, anything else is rejected before routing
	// reaches an app's handler.
	auth := server.StaticTokenAuthenticator(map[string]server.Principal{
		aliceToken: {Subject: "alice", Issuer: "test"},
	})

	// membership: alice belongs to her workspace in docs, and to nothing in
	// billing. The two apps' namespaces make the same workspace id unrelated
	// data, which is exactly why the policy has to know its app.
	member := func(appID string, p server.Principal, ws string) bool {
		return appID == docsApp && p.Subject == "alice" && ws == aliceWS
	}

	// newManager builds a host with the shared authenticator plus a per-app
	// policy, over a store the caller names so a spec can reopen it.
	newManager := func(path string) *server.Manager {
		GinkgoHelper()
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		m, err := server.NewManager(ctx, store,
			server.WithServerOptions(server.WithAuthenticator(auth)),
			server.WithAppOptions(func(appID string) []server.Option {
				p := &appPerms{appID: appID, member: member}
				polMu.Lock()
				policies[appID] = p
				polMu.Unlock()
				return []server.Option{server.WithPermissions(p)}
			}),
		)
		Expect(err).NotTo(HaveOccurred())
		return m
	}

	policyFor := func(appID string) *appPerms {
		GinkgoHelper()
		polMu.Lock()
		defer polMu.Unlock()
		p := policies[appID]
		Expect(p).NotTo(BeNil(), "app %q was never created", appID)
		return p
	}

	// client addresses one app on the routed handler, carrying token (empty
	// sends no credential at all).
	client := func(m *server.Manager, appID, token string) syncplanev1connect.SyncServiceClient {
		GinkgoHelper()
		ts := httptest.NewServer(m.SyncHandler())
		DeferCleanup(ts.Close)
		var h http.Header
		if token != "" {
			h = http.Header{}
			h.Set("Authorization", "Bearer "+token)
		}
		return syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL+"/"+appID,
			connect.WithInterceptors(headerInterceptor(h)))
	}
	pull := func(cl syncplanev1connect.SyncServiceClient, ws string) error {
		_, err := cl.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c1",
		}))
		return err
	}

	var mgr *server.Manager

	BeforeEach(func() {
		ctx = context.Background()
		policies = map[string]*appPerms{}
		var err error
		mt, err = server.BuildDynamicMessageType(noteSpec)
		Expect(err).NotTo(HaveOccurred())

		mgr = newManager(filepath.Join(GinkgoT().TempDir(), "collection.db"))
		for _, app := range []string{docsApp, billingApp} {
			_, err := mgr.Register(ctx, app, descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Describe("the shared authentication layer", func() {
		It("authenticates every app on the host", func() {
			for _, app := range []string{docsApp, billingApp} {
				err := pull(client(mgr, app, "wrong-token"), aliceWS)
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnauthenticated),
					"app %q must run the shared authenticator", app)
			}
		})

		It("threads one principal into each app's own hooks", func() {
			Expect(pull(client(mgr, docsApp, aliceToken), aliceWS)).To(Succeed())
			_ = pull(client(mgr, billingApp, aliceToken), aliceWS)

			for _, app := range []string{docsApp, billingApp} {
				seen := policyFor(app).principals()
				Expect(seen).To(HaveLen(1), "app %q saw one request", app)
				Expect(seen[0].Subject).To(Equal("alice"))
				Expect(seen[0].Issuer).To(Equal("test"), "the verified principal reaches %q unchanged", app)
			}
		})

		It("rejects an unauthenticated caller before any app policy runs", func() {
			err := pull(client(mgr, docsApp, ""), aliceWS)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnauthenticated))
			Expect(policyFor(docsApp).principals()).To(BeEmpty())
		})
	})

	Describe("per-app policies", func() {
		It("decides independently for the same principal and workspace id", func() {
			// Same caller, same workspace id, different app: the workspace ids
			// name different partitions, and only the app's own policy knows
			// which of them the caller belongs to.
			Expect(pull(client(mgr, docsApp, aliceToken), aliceWS)).To(Succeed())

			err := pull(client(mgr, billingApp, aliceToken), aliceWS)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})

		It("is bound to its own app id", func() {
			Expect(policyFor(docsApp).appID).To(Equal(docsApp))
			Expect(policyFor(billingApp).appID).To(Equal(billingApp))
		})

		It("survives a restart, including apps created by the boot reload", func() {
			path := filepath.Join(GinkgoT().TempDir(), "restart.db")
			m1 := newManager(path)
			_, err := m1.Register(ctx, billingApp, descriptorSetBytes(noteSpec))
			Expect(err).NotTo(HaveOccurred())

			// Reopening re-creates the app during NewManager, which is a
			// different construction path than Register — the per-app options
			// must apply there too, or a restart would silently drop the policy.
			policies = map[string]*appPerms{}
			m2 := newManager(path)
			err = pull(client(m2, billingApp, aliceToken), aliceWS)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
			Expect(policyFor(billingApp).principals()).To(HaveLen(1))
		})
	})

	Describe("option precedence", func() {
		It("lets a per-app option override a shared one", func() {
			store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "override.db"))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

			shared := &appPerms{appID: "shared", member: func(string, server.Principal, string) bool { return false }}
			own := &appPerms{appID: docsApp}
			m, err := server.NewManager(ctx, store,
				server.WithServerOptions(server.WithPermissions(shared)),
				server.WithAppOptions(func(appID string) []server.Option {
					if appID != docsApp {
						return nil // this app keeps the shared policy
					}
					return []server.Option{server.WithPermissions(own)}
				}),
			)
			Expect(err).NotTo(HaveOccurred())
			for _, app := range []string{docsApp, billingApp} {
				_, err = m.Register(ctx, app, descriptorSetBytes(noteSpec))
				Expect(err).NotTo(HaveOccurred())
			}

			Expect(pull(client(m, docsApp, ""), aliceWS)).To(Succeed(), "the per-app policy wins")
			Expect(connect.CodeOf(pull(client(m, billingApp, ""), aliceWS))).
				To(Equal(connect.CodePermissionDenied), "an app with no options of its own keeps the shared policy")
		})
	})

	Describe("data isolation under one host", func() {
		It("keeps an authorized write inside its own app", func() {
			val := buildNote(mt, map[string]any{"id": "n1", "title": "hello"})
			_, err := client(mgr, docsApp, aliceToken).Push(ctx, connect.NewRequest(pushOf(
				aliceWS,
				[]*syncplanev1.Mutation{putMutation("c1", 1, "note", "n1", val)})))
			Expect(err).NotTo(HaveOccurred())

			// The other app's partition is untouched, and its policy denies the
			// read regardless — two independent reasons, both required.
			Expect(connect.CodeOf(pull(client(mgr, billingApp, aliceToken), aliceWS))).
				To(Equal(connect.CodePermissionDenied))
		})
	})
})
