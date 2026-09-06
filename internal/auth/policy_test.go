package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/auth"
)

// The policy is exercised through the engine, not by calling its hooks: the
// spec builds a real server with the Sessions authenticator and a Policy
// installed, signs in as users with different roles and sends RPCs. What it
// asserts is what a sync client would see.
var _ = Describe("Policy through the engine", func() {
	const (
		app = "todo"
		ws  = "team"
	)

	var (
		ctx      context.Context
		store    *auth.Store
		sessions *auth.Sessions
		policy   *auth.Policy
		clk      *clock
		baseURL  string
		httpc    *http.Client
	)

	// signIn creates a user and returns their access token.
	signIn := func(email string, admin bool) (auth.User, string) {
		GinkgoHelper()
		u, err := store.CreateUser(ctx, auth.NewUser{Email: email, Password: "correct horse", Admin: admin})
		Expect(err).NotTo(HaveOccurred())
		tokens, err := sessions.Start(ctx, u)
		Expect(err).NotTo(HaveOccurred())
		return u, tokens.AccessToken
	}

	// clientAs builds a sync client that sends the token on every request; an
	// empty token sends none.
	clientAs := func(token string) syncplanev1connect.SyncServiceClient {
		var opts []connect.ClientOption
		if token != "" {
			opts = append(opts, connect.WithInterceptors(connect.UnaryInterceptorFunc(
				func(next connect.UnaryFunc) connect.UnaryFunc {
					return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
						req.Header().Set("Authorization", "Bearer "+token)
						return next(ctx, req)
					}
				})))
		}
		return syncplanev1connect.NewSyncServiceClient(httpc, baseURL, opts...)
	}

	pushTodo := func(c syncplanev1connect.SyncServiceClient, workspace string, id uint64) (*syncplanev1.WorkspaceResults, error) {
		value, err := proto.Marshal(&todov1.Todo{Id: "t1", Title: "hello"})
		Expect(err).NotTo(HaveOccurred())
		resp, err := c.Push(ctx, connect.NewRequest(&syncplanev1.PushRequest{
			Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: workspace, Mutations: []*syncplanev1.Mutation{{
				ClientId: "c1", MutationId: id,
				Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
					Entity: "todo", Key: "t1", Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value,
				}},
			}}}},
		}))
		if err != nil {
			return nil, err
		}
		Expect(resp.Msg.GetResults()).To(HaveLen(1))
		return resp.Msg.GetResults()[0], nil
	}

	pull := func(c syncplanev1connect.SyncServiceClient, workspace string) error {
		_, err := c.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: workspace, ClientId: "c1"}))
		return err
	}

	expectRevoked := func(err error) {
		GinkgoHelper()
		Expect(err).To(HaveOccurred())
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		var ce *connect.Error
		Expect(err).To(BeAssignableToTypeOf(ce))
		ce = err.(*connect.Error)
		Expect(ce.Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceAccessRevoked))
	}

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		store, err = auth.OpenSQLite(filepath.Join(GinkgoT().TempDir(), "auth.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		clk = &clock{t: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
		sessions, err = auth.NewSessions(ctx, store, auth.SessionConfig{Issuer: issuer, Clock: clk.now})
		Expect(err).NotTo(HaveOccurred())
		policy = auth.NewPolicy(store, app, auth.PolicyConfig{Clock: clk.now})

		syncStore, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "sync.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(syncStore.Close()).To(Succeed()) })
		srv, err := server.New(syncStore,
			server.WithEntities(&todov1.Todo{}),
			server.WithAuthenticator(sessions),
			server.WithPermissions(policy))
		Expect(err).NotTo(HaveOccurred())
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		baseURL, httpc = ts.URL, ts.Client()

		_, err = store.CreateWorkspace(ctx, app, ws, "Team", "")
		Expect(err).NotTo(HaveOccurred())
	})

	It("refuses a request with no token before any policy runs", func() {
		err := pull(clientAs(""), ws)
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnauthenticated))
	})

	It("refuses a signed-in non-member with a settled denial", func() {
		_, token := signIn("outsider@example.com", false)
		expectRevoked(pull(clientAs(token), ws))
		_, err := pushTodo(clientAs(token), ws, 1)
		Expect(err).NotTo(HaveOccurred())
	})

	It("reports a refused push batch as revoked access", func() {
		_, token := signIn("outsider@example.com", false)
		result, err := pushTodo(clientAs(token), ws, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.GetErrorCode()).To(Equal(server.CodeWorkspaceAccessRevoked))
	})

	It("lets a writer read and write", func() {
		u, token := signIn("writer@example.com", false)
		Expect(store.SetMember(ctx, app, ws, u.ID, auth.RoleWriter)).To(Succeed())
		c := clientAs(token)
		Expect(pull(c, ws)).To(Succeed())
		result, err := pushTodo(c, ws, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.GetErrorCode()).To(BeEmpty())
		Expect(result.GetResults()).To(HaveLen(1))
		Expect(result.GetResults()[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
	})

	It("lets a reader read and rejects their writes without retry", func() {
		u, token := signIn("reader@example.com", false)
		Expect(store.SetMember(ctx, app, ws, u.ID, auth.RoleReader)).To(Succeed())
		c := clientAs(token)
		Expect(pull(c, ws)).To(Succeed())
		result, err := pushTodo(c, ws, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.GetErrorCode()).To(BeEmpty(), "the batch is accepted; the mutation inside is what is refused")
		Expect(result.GetResults()).To(HaveLen(1))
		Expect(result.GetResults()[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(result.GetResults()[0].GetErrorCode()).To(Equal(server.CodePermissionDenied))
	})

	It("lets an admin into any workspace that exists, and no other", func() {
		_, token := signIn("root@example.com", true)
		c := clientAs(token)
		Expect(pull(c, ws)).To(Succeed())
		result, err := pushTodo(c, ws, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.GetResults()[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		// An id the Store has never seen would otherwise spring into being
		// in the sync store on first write.
		expectRevoked(pull(c, "unknown"))
	})

	It("keeps apps apart: membership in one app opens nothing in another", func() {
		u, token := signIn("member@example.com", false)
		_, err := store.CreateWorkspace(ctx, "crm", ws, "", u.ID)
		Expect(err).NotTo(HaveOccurred())
		// The server under test serves app "todo"; the membership is in "crm".
		expectRevoked(pull(clientAs(token), ws))
	})

	It("serves a removed member from the cache until the TTL passes", func() {
		u, token := signIn("member@example.com", false)
		Expect(store.SetMember(ctx, app, ws, u.ID, auth.RoleWriter)).To(Succeed())
		c := clientAs(token)
		Expect(pull(c, ws)).To(Succeed())

		Expect(store.RemoveMember(ctx, app, ws, u.ID)).To(Succeed())
		Expect(pull(c, ws)).To(Succeed(), "within the TTL the cached answer stands")

		clk.advance(auth.DefaultCacheTTL)
		expectRevoked(pull(c, ws))
	})

	It("forgets the cache on request", func() {
		u, token := signIn("member@example.com", false)
		Expect(store.SetMember(ctx, app, ws, u.ID, auth.RoleWriter)).To(Succeed())
		c := clientAs(token)
		Expect(pull(c, ws)).To(Succeed())

		Expect(store.RemoveMember(ctx, app, ws, u.ID)).To(Succeed())
		policy.Forget()
		expectRevoked(pull(c, ws))
	})

	It("applies a role change at the next lookup", func() {
		u, token := signIn("member@example.com", false)
		Expect(store.SetMember(ctx, app, ws, u.ID, auth.RoleReader)).To(Succeed())
		c := clientAs(token)
		result, err := pushTodo(c, ws, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.GetResults()[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))

		Expect(store.SetMember(ctx, app, ws, u.ID, auth.RoleWriter)).To(Succeed())
		clk.advance(auth.DefaultCacheTTL)
		result, err = pushTodo(c, ws, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.GetResults()[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
	})
})

var _ = Describe("Policy hooks", func() {
	It("does not cache when disabled", func() {
		ctx := context.Background()
		store, err := auth.OpenSQLite(filepath.Join(GinkgoT().TempDir(), "auth.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		policy := auth.NewPolicy(store, "todo", auth.PolicyConfig{CacheTTL: -1})

		u, err := store.CreateUser(ctx, auth.NewUser{Email: "a@example.com"})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.CreateWorkspace(ctx, "todo", "team", "", u.ID)
		Expect(err).NotTo(HaveOccurred())
		pr := server.Principal{Subject: u.Subject()}

		Expect(policy.CanAccessWorkspace(ctx, pr, "team")).To(Succeed())
		Expect(store.RemoveMember(ctx, "todo", "team", u.ID)).To(Succeed())
		err = policy.CanAccessWorkspace(ctx, pr, "team")
		_, settled := server.AsWorkspaceDenial(err)
		Expect(settled).To(BeTrue(), "%v", err)
	})

	It("treats anonymous and foreign subjects as outsiders", func() {
		ctx := context.Background()
		store, err := auth.OpenSQLite(filepath.Join(GinkgoT().TempDir(), "auth.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		policy := auth.NewPolicy(store, "todo", auth.PolicyConfig{})
		_, err = store.CreateWorkspace(ctx, "todo", "team", "", "")
		Expect(err).NotTo(HaveOccurred())

		for _, pr := range []server.Principal{{}, {Subject: "google:123"}, {Subject: auth.SubjectPrefix}} {
			err := policy.CanAccessWorkspace(ctx, pr, "team")
			_, settled := server.AsWorkspaceDenial(err)
			Expect(settled).To(BeTrue(), "%+v: %v", pr, err)
			err = policy.CanMutate(ctx, pr, "team", server.Write{})
			_, rejected := server.AsRejection(err)
			Expect(rejected).To(BeTrue(), "%+v: %v", pr, err)
		}
		Expect(auth.IsAdmin(server.Principal{Claims: map[string]any{"admin": true}})).To(BeTrue())
		Expect(auth.IsAdmin(server.Principal{Claims: map[string]any{"admin": "true"}})).To(BeFalse())
	})
})
