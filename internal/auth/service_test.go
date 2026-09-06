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

	authv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/auth/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/auth/v1/authv1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/auth"
)

// The service is exercised over HTTP with the generated client, the way a
// browser reaches it, so what the specs assert are the Connect codes and
// messages a client sees.
var _ = Describe("AuthService", func() {
	const (
		app = "todo"
		ws  = "team"
	)

	var (
		ctx      context.Context
		store    *auth.Store
		sessions *auth.Sessions
		policies *auth.PolicySet
		clk      *clock
		baseURL  string
		httpc    *http.Client
	)

	// serve mounts a fresh Service with cfg. Specs that need a non-default
	// configuration call it again; the default is mounted in BeforeEach.
	serve := func(cfg auth.ServiceConfig) {
		GinkgoHelper()
		cfg.Clock = clk.now
		svc := auth.NewService(store, sessions, policies, cfg)
		path, handler := svc.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		baseURL, httpc = ts.URL, ts.Client()
	}

	clientAs := func(token string) authv1connect.AuthServiceClient {
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
		return authv1connect.NewAuthServiceClient(httpc, baseURL, opts...)
	}

	anon := func() authv1connect.AuthServiceClient { return clientAs("") }

	createUser := func(email string, admin bool) auth.User {
		GinkgoHelper()
		u, err := store.CreateUser(ctx, auth.NewUser{Email: email, Name: "Someone", Password: "correct horse", Admin: admin})
		Expect(err).NotTo(HaveOccurred())
		return u
	}

	signIn := func(email string) *authv1.SignInResponse {
		GinkgoHelper()
		resp, err := anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: email, Password: "correct horse"}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	// as creates a user, signs them in and returns a client speaking as them.
	as := func(email string, admin bool) (auth.User, authv1connect.AuthServiceClient) {
		GinkgoHelper()
		u := createUser(email, admin)
		return u, clientAs(signIn(email).GetTokens().GetAccessToken())
	}

	code := func(err error) connect.Code {
		GinkgoHelper()
		Expect(err).To(HaveOccurred())
		return connect.CodeOf(err)
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
		policies = auth.NewPolicySet(store, auth.PolicyConfig{Clock: clk.now})
		serve(auth.ServiceConfig{})
	})

	Describe("sign-in", func() {
		It("returns the user and a verifiable token pair", func() {
			u := createUser("ada@example.com", false)
			resp := signIn("ADA@example.com")
			Expect(resp.GetUser().GetId()).To(Equal(u.ID))
			Expect(resp.GetUser().GetEmail()).To(Equal("ada@example.com"))
			Expect(resp.GetUser().GetAdmin()).To(BeFalse())
			claims, err := sessions.Verify(ctx, resp.GetTokens().GetAccessToken())
			Expect(err).NotTo(HaveOccurred())
			Expect(claims.Subject).To(Equal(u.Subject()))
			Expect(resp.GetTokens().GetAccessExpiresAt().AsTime()).To(Equal(clk.t.Add(auth.DefaultAccessTTL)))
			Expect(resp.GetTokens().GetRefreshToken()).NotTo(BeEmpty())
		})

		It("answers wrong and unknown credentials alike", func() {
			createUser("ada@example.com", false)
			_, err := anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: "ada@example.com", Password: "wrong horse"}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
			_, err = anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: "nobody@example.com", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
		})

		It("refuses a disabled account with permission_denied", func() {
			u := createUser("ada@example.com", false)
			_, err := store.UpdateUser(ctx, u.ID, auth.UserUpdate{Disabled: ptr(true)})
			Expect(err).NotTo(HaveOccurred())
			_, err = anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: "ada@example.com", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied))
		})

		It("locks an address after too many failures and unlocks when the window passes", func() {
			createUser("ada@example.com", false)
			serve(auth.ServiceConfig{SignInAttempts: 3, SignInWindow: time.Minute})
			wrong := connect.NewRequest(&authv1.SignInRequest{Email: "Ada@example.com", Password: "wrong horse"})
			for range 3 {
				_, err := anon().SignIn(ctx, wrong)
				Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
			}
			// The right password is refused too: the lock is on the address.
			_, err := anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: "ada@example.com", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodeResourceExhausted))
			// Another address is unaffected.
			createUser("bob@example.com", false)
			signIn("bob@example.com")

			clk.advance(time.Minute)
			signIn("ada@example.com")
		})

		It("clears the failure count on success", func() {
			createUser("ada@example.com", false)
			serve(auth.ServiceConfig{SignInAttempts: 2, SignInWindow: time.Minute})
			wrong := connect.NewRequest(&authv1.SignInRequest{Email: "ada@example.com", Password: "wrong horse"})
			_, err := anon().SignIn(ctx, wrong)
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
			signIn("ada@example.com")
			_, err = anon().SignIn(ctx, wrong)
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated), "one failure after a success is not a lock")
		})
	})

	Describe("sign-up", func() {
		It("is closed by default", func() {
			_, err := anon().SignUp(ctx, connect.NewRequest(&authv1.SignUpRequest{Email: "new@example.com", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodeFailedPrecondition))
		})

		It("creates a plain account with no workspaces when open", func() {
			serve(auth.ServiceConfig{OpenSignUp: true})
			resp, err := anon().SignUp(ctx, connect.NewRequest(&authv1.SignUpRequest{Email: "New@Example.com", Password: "correct horse", Name: "New"}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetUser().GetEmail()).To(Equal("new@example.com"))
			Expect(resp.Msg.GetUser().GetAdmin()).To(BeFalse())

			c := clientAs(resp.Msg.GetTokens().GetAccessToken())
			list, err := c.ListWorkspaces(ctx, connect.NewRequest(&authv1.ListWorkspacesRequest{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Msg.GetWorkspaces()).To(BeEmpty())

			_, err = anon().SignUp(ctx, connect.NewRequest(&authv1.SignUpRequest{Email: "new@example.com", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodeAlreadyExists))
			_, err = anon().SignUp(ctx, connect.NewRequest(&authv1.SignUpRequest{Email: "other@example.com", Password: "short"}))
			Expect(code(err)).To(Equal(connect.CodeInvalidArgument))
			_, err = anon().SignUp(ctx, connect.NewRequest(&authv1.SignUpRequest{Email: "other@example.com"}))
			Expect(code(err)).To(Equal(connect.CodeInvalidArgument), "an empty password is not a password-less account")
			_, err = anon().SignUp(ctx, connect.NewRequest(&authv1.SignUpRequest{Email: "not an address", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodeInvalidArgument))
		})
	})

	Describe("sessions", func() {
		It("refreshes, retiring the presented token", func() {
			createUser("ada@example.com", false)
			first := signIn("ada@example.com").GetTokens()
			resp, err := anon().Refresh(ctx, connect.NewRequest(&authv1.RefreshRequest{RefreshToken: first.GetRefreshToken()}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetTokens().GetRefreshToken()).NotTo(Equal(first.GetRefreshToken()))
			_, err = sessions.Verify(ctx, resp.Msg.GetTokens().GetAccessToken())
			Expect(err).NotTo(HaveOccurred())

			_, err = anon().Refresh(ctx, connect.NewRequest(&authv1.RefreshRequest{RefreshToken: first.GetRefreshToken()}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
		})

		It("signs out idempotently", func() {
			createUser("ada@example.com", false)
			tokens := signIn("ada@example.com").GetTokens()
			_, err := anon().SignOut(ctx, connect.NewRequest(&authv1.SignOutRequest{RefreshToken: tokens.GetRefreshToken()}))
			Expect(err).NotTo(HaveOccurred())
			_, err = anon().SignOut(ctx, connect.NewRequest(&authv1.SignOutRequest{RefreshToken: tokens.GetRefreshToken()}))
			Expect(err).NotTo(HaveOccurred())
			_, err = anon().SignOut(ctx, connect.NewRequest(&authv1.SignOutRequest{RefreshToken: "garbage"}))
			Expect(err).NotTo(HaveOccurred())
			_, err = anon().Refresh(ctx, connect.NewRequest(&authv1.RefreshRequest{RefreshToken: tokens.GetRefreshToken()}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
		})

		It("requires a bearer token for the account calls", func() {
			_, err := anon().GetCurrentUser(ctx, connect.NewRequest(&authv1.GetCurrentUserRequest{}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
			_, err = clientAs("garbage").ListWorkspaces(ctx, connect.NewRequest(&authv1.ListWorkspacesRequest{}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
		})

		It("returns the current user and drops a disabled or deleted one at once", func() {
			u, c := as("ada@example.com", true)
			resp, err := c.GetCurrentUser(ctx, connect.NewRequest(&authv1.GetCurrentUserRequest{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetUser().GetId()).To(Equal(u.ID))
			Expect(resp.Msg.GetUser().GetAdmin()).To(BeTrue())

			_, err = store.UpdateUser(ctx, u.ID, auth.UserUpdate{Disabled: ptr(true)})
			Expect(err).NotTo(HaveOccurred())
			_, err = c.GetCurrentUser(ctx, connect.NewRequest(&authv1.GetCurrentUserRequest{}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied))

			Expect(store.DeleteUser(ctx, u.ID)).To(Succeed())
			_, err = c.GetCurrentUser(ctx, connect.NewRequest(&authv1.GetCurrentUserRequest{}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
		})

		It("changes the password and ends the other sessions", func() {
			createUser("ada@example.com", false)
			mine := signIn("ada@example.com").GetTokens()
			other := signIn("ada@example.com").GetTokens()
			c := clientAs(mine.GetAccessToken())

			_, err := c.ChangePassword(ctx, connect.NewRequest(&authv1.ChangePasswordRequest{CurrentPassword: "wrong horse", NewPassword: "battery staple"}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied))
			_, err = c.ChangePassword(ctx, connect.NewRequest(&authv1.ChangePasswordRequest{CurrentPassword: "correct horse", NewPassword: "short"}))
			Expect(code(err)).To(Equal(connect.CodeInvalidArgument))
			_, err = c.ChangePassword(ctx, connect.NewRequest(&authv1.ChangePasswordRequest{CurrentPassword: "correct horse", NewPassword: "battery staple"}))
			Expect(err).NotTo(HaveOccurred())

			_, err = anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: "ada@example.com", Password: "correct horse"}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated))
			_, err = anon().SignIn(ctx, connect.NewRequest(&authv1.SignInRequest{Email: "ada@example.com", Password: "battery staple"}))
			Expect(err).NotTo(HaveOccurred())

			_, err = anon().Refresh(ctx, connect.NewRequest(&authv1.RefreshRequest{RefreshToken: other.GetRefreshToken()}))
			Expect(code(err)).To(Equal(connect.CodeUnauthenticated), "the other device is signed out")
			_, err = anon().Refresh(ctx, connect.NewRequest(&authv1.RefreshRequest{RefreshToken: mine.GetRefreshToken()}))
			Expect(err).NotTo(HaveOccurred(), "the device that changed the password stays signed in")
		})
	})

	Describe("workspaces and members", func() {
		var (
			owner  auth.User
			ownerC authv1connect.AuthServiceClient
		)

		BeforeEach(func() {
			owner, ownerC = as("owner@example.com", false)
			_, err := store.CreateWorkspace(ctx, app, ws, "Team", owner.ID)
			Expect(err).NotTo(HaveOccurred())
		})

		It("lists a member's workspaces with their role, and everything for an admin", func() {
			_, err := store.CreateWorkspace(ctx, "crm", "accounts", "Accounts", "")
			Expect(err).NotTo(HaveOccurred())
			resp, err := ownerC.ListWorkspaces(ctx, connect.NewRequest(&authv1.ListWorkspacesRequest{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetWorkspaces()).To(HaveLen(1))
			Expect(resp.Msg.GetWorkspaces()[0].GetAppId()).To(Equal(app))
			Expect(resp.Msg.GetWorkspaces()[0].GetWorkspaceId()).To(Equal(ws))
			Expect(resp.Msg.GetWorkspaces()[0].GetName()).To(Equal("Team"))
			Expect(resp.Msg.GetWorkspaces()[0].GetRole()).To(Equal(authv1.Role_ROLE_OWNER))

			_, adminC := as("root@example.com", true)
			resp, err = adminC.ListWorkspaces(ctx, connect.NewRequest(&authv1.ListWorkspacesRequest{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetWorkspaces()).To(HaveLen(2))
			Expect(resp.Msg.GetWorkspaces()[0].GetAppId()).To(Equal("crm"))
			Expect(resp.Msg.GetWorkspaces()[0].GetRole()).To(Equal(authv1.Role_ROLE_OWNER))
		})

		It("lets an owner add, change and remove members", func() {
			reader := createUser("reader@example.com", false)
			resp, err := ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "Reader@example.com", Role: authv1.Role_ROLE_READER,
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetMember().GetUserId()).To(Equal(reader.ID))
			Expect(resp.Msg.GetMember().GetEmail()).To(Equal("reader@example.com"))
			Expect(resp.Msg.GetMember().GetRole()).To(Equal(authv1.Role_ROLE_READER))

			list, err := ownerC.ListMembers(ctx, connect.NewRequest(&authv1.ListMembersRequest{AppId: app, WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred())
			Expect(list.Msg.GetMembers()).To(HaveLen(2))
			Expect(list.Msg.GetMembers()[0].GetEmail()).To(Equal("owner@example.com"))
			Expect(list.Msg.GetMembers()[1].GetEmail()).To(Equal("reader@example.com"))

			resp, err = ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "reader@example.com", Role: authv1.Role_ROLE_WRITER,
			}))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetMember().GetRole()).To(Equal(authv1.Role_ROLE_WRITER))

			_, err = ownerC.RemoveMember(ctx, connect.NewRequest(&authv1.RemoveMemberRequest{AppId: app, WorkspaceId: ws, UserId: reader.ID}))
			Expect(err).NotTo(HaveOccurred())
			_, err = ownerC.RemoveMember(ctx, connect.NewRequest(&authv1.RemoveMemberRequest{AppId: app, WorkspaceId: ws, UserId: reader.ID}))
			Expect(code(err)).To(Equal(connect.CodeNotFound))
		})

		It("refuses bad member requests with the right codes", func() {
			_, err := ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "nobody@example.com", Role: authv1.Role_ROLE_READER,
			}))
			Expect(code(err)).To(Equal(connect.CodeNotFound))
			_, err = ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "owner@example.com",
			}))
			Expect(code(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("keeps the last owner", func() {
			_, err := ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "owner@example.com", Role: authv1.Role_ROLE_READER,
			}))
			Expect(code(err)).To(Equal(connect.CodeFailedPrecondition))
			_, err = ownerC.RemoveMember(ctx, connect.NewRequest(&authv1.RemoveMemberRequest{AppId: app, WorkspaceId: ws, UserId: owner.ID}))
			Expect(code(err)).To(Equal(connect.CodeFailedPrecondition))

			// With a second owner in place the first may step down.
			createUser("second@example.com", false)
			_, err = ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "second@example.com", Role: authv1.Role_ROLE_OWNER,
			}))
			Expect(err).NotTo(HaveOccurred())
			_, err = ownerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "owner@example.com", Role: authv1.Role_ROLE_READER,
			}))
			Expect(err).NotTo(HaveOccurred())
		})

		It("keeps non-owners out of member management and outsiders out of the list", func() {
			reader := createUser("reader@example.com", false)
			Expect(store.SetMember(ctx, app, ws, reader.ID, auth.RoleReader)).To(Succeed())
			readerC := clientAs(signIn("reader@example.com").GetTokens().GetAccessToken())

			_, err := readerC.ListMembers(ctx, connect.NewRequest(&authv1.ListMembersRequest{AppId: app, WorkspaceId: ws}))
			Expect(err).NotTo(HaveOccurred(), "any member may see who else is in")
			_, err = readerC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "reader@example.com", Role: authv1.Role_ROLE_OWNER,
			}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied))
			_, err = readerC.RemoveMember(ctx, connect.NewRequest(&authv1.RemoveMemberRequest{AppId: app, WorkspaceId: ws, UserId: owner.ID}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied))

			_, outsiderC := as("outsider@example.com", false)
			_, err = outsiderC.ListMembers(ctx, connect.NewRequest(&authv1.ListMembersRequest{AppId: app, WorkspaceId: ws}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied))
			_, err = outsiderC.ListMembers(ctx, connect.NewRequest(&authv1.ListMembersRequest{AppId: app, WorkspaceId: "nope"}))
			Expect(code(err)).To(Equal(connect.CodePermissionDenied), "an unknown workspace looks like any other closed door")
		})

		It("lets an admin manage any workspace that exists", func() {
			_, adminC := as("root@example.com", true)
			createUser("new@example.com", false)
			_, err := adminC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: ws, Email: "new@example.com", Role: authv1.Role_ROLE_WRITER,
			}))
			Expect(err).NotTo(HaveOccurred())
			_, err = adminC.SetMember(ctx, connect.NewRequest(&authv1.SetMemberRequest{
				AppId: app, WorkspaceId: "nope", Email: "new@example.com", Role: authv1.Role_ROLE_WRITER,
			}))
			Expect(code(err)).To(Equal(connect.CodeNotFound))
		})

		It("makes a membership change visible to the policy at once", func() {
			reader := createUser("reader@example.com", false)
			Expect(store.SetMember(ctx, app, ws, reader.ID, auth.RoleReader)).To(Succeed())
			pr := server.Principal{Subject: reader.Subject()}
			policy := policies.For(app)
			Expect(policy.CanAccessWorkspace(ctx, pr, ws)).To(Succeed())

			_, err := ownerC.RemoveMember(ctx, connect.NewRequest(&authv1.RemoveMemberRequest{AppId: app, WorkspaceId: ws, UserId: reader.ID}))
			Expect(err).NotTo(HaveOccurred())
			err = policy.CanAccessWorkspace(ctx, pr, ws)
			_, settled := server.AsWorkspaceDenial(err)
			Expect(settled).To(BeTrue(), "the cache was dropped: %v", err)
		})
	})
})
