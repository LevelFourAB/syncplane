package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/auth/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/auth/v1/authv1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// Sign-in limiter defaults, used when ServiceConfig leaves them zero.
const (
	DefaultSignInAttempts = 10
	DefaultSignInWindow   = 15 * time.Minute
)

// ServiceConfig configures Service.
type ServiceConfig struct {
	// OpenSignUp lets anyone create an account through SignUp. When false,
	// the default, SignUp fails with failed_precondition and accounts are
	// created by an administrator.
	OpenSignUp bool
	// SignInAttempts is how many failed sign-ins an e-mail address may
	// accumulate within SignInWindow before SignIn refuses it with
	// resource_exhausted. Zero means DefaultSignInAttempts.
	SignInAttempts int
	// SignInWindow is the period the attempt count covers. Zero means
	// DefaultSignInWindow.
	SignInWindow time.Duration
	// Clock supplies the current time for the limiter. Nil means time.Now.
	Clock func() time.Time
}

// Service implements syncplane.auth.v1.AuthService over a Store, its
// Sessions and the PolicySet guarding the apps. Mount Handler at the root of
// the public listener. Service is safe for concurrent use.
type Service struct {
	authv1connect.UnimplementedAuthServiceHandler

	store    *Store
	sessions *Sessions
	policies *PolicySet
	cfg      ServiceConfig
	limiter  *limiter
}

// NewService builds the service. policies may be nil on a host that installs
// no Policy; membership changes then invalidate nothing.
func NewService(store *Store, sessions *Sessions, policies *PolicySet, cfg ServiceConfig) *Service {
	if cfg.SignInAttempts == 0 {
		cfg.SignInAttempts = DefaultSignInAttempts
	}
	if cfg.SignInWindow == 0 {
		cfg.SignInWindow = DefaultSignInWindow
	}
	now := cfg.Clock
	if now == nil {
		now = time.Now
	}
	return &Service{
		store:    store,
		sessions: sessions,
		policies: policies,
		cfg:      cfg,
		limiter:  newLimiter(cfg.SignInAttempts, cfg.SignInWindow, now),
	}
}

// Handler returns the mount path and handler for the service.
func (s *Service) Handler() (string, http.Handler) {
	return authv1connect.NewAuthServiceHandler(s)
}

// rpcError maps this package's sentinels to Connect codes. Anything not a
// sentinel is a database failure and reported as internal, with the detail
// kept out of the response.
func rpcError(err error) error {
	switch {
	case errors.Is(err, ErrInvalidCredentials), errors.Is(err, ErrInvalidSession), errors.Is(err, ErrInvalidToken):
		return connect.NewError(connect.CodeUnauthenticated, err)
	case errors.Is(err, ErrDisabled):
		return connect.NewError(connect.CodePermissionDenied, err)
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, ErrEmailTaken), errors.Is(err, ErrWorkspaceExists), errors.Is(err, ErrIdentityLinked):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, ErrInvalidEmail), errors.Is(err, ErrWeakPassword), errors.Is(err, ErrInvalidRole),
		errors.Is(err, ErrInvalidWorkspace), errors.Is(err, ErrInvalidIdentity):
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeInternal, errors.New("auth: internal error"))
}

func denied(msg string) error {
	return connect.NewError(connect.CodePermissionDenied, errors.New(msg))
}

func invalid(msg string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(msg))
}

// caller authenticates the request's bearer token and loads the account
// behind it. The load is what makes a disabled or deleted account lose these
// calls at once rather than at the access token's expiry.
func (s *Service) caller(ctx context.Context, header http.Header) (server.Principal, User, error) {
	pr, err := s.sessions.Authenticate(ctx, header)
	if err != nil {
		return server.Principal{}, User{}, connect.NewError(connect.CodeUnauthenticated, err)
	}
	u, err := s.store.UserBySubject(ctx, pr.Subject)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return server.Principal{}, User{}, connect.NewError(connect.CodeUnauthenticated, ErrInvalidToken)
		}
		return server.Principal{}, User{}, rpcError(err)
	}
	if u.Disabled {
		return server.Principal{}, User{}, connect.NewError(connect.CodePermissionDenied, ErrDisabled)
	}
	return pr, u, nil
}

func wireUser(u User) *authv1.User {
	return &authv1.User{Id: u.ID, Email: u.Email, Name: u.Name, Admin: u.Admin}
}

func wireTokens(t Tokens) *authv1.Tokens {
	return &authv1.Tokens{
		AccessToken:      t.AccessToken,
		AccessExpiresAt:  timestamppb.New(t.AccessExpiresAt),
		RefreshToken:     t.RefreshToken,
		RefreshExpiresAt: timestamppb.New(t.RefreshExpiresAt),
	}
}

func wireRole(r Role) authv1.Role {
	switch r {
	case RoleReader:
		return authv1.Role_ROLE_READER
	case RoleWriter:
		return authv1.Role_ROLE_WRITER
	case RoleOwner:
		return authv1.Role_ROLE_OWNER
	}
	return authv1.Role_ROLE_UNSPECIFIED
}

func roleOf(r authv1.Role) (Role, bool) {
	switch r {
	case authv1.Role_ROLE_READER:
		return RoleReader, true
	case authv1.Role_ROLE_WRITER:
		return RoleWriter, true
	case authv1.Role_ROLE_OWNER:
		return RoleOwner, true
	}
	return "", false
}

func wireMember(m MemberUser) *authv1.Member {
	return &authv1.Member{
		UserId:  m.UserID,
		Email:   m.Email,
		Name:    m.Name,
		Role:    wireRole(m.Role),
		AddedAt: timestamppb.New(m.AddedAt),
	}
}

// limiterKey is the e-mail address as the limiter counts it: normalized the
// way the store compares it, so case does not buy extra attempts.
func limiterKey(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func (s *Service) SignIn(ctx context.Context, req *connect.Request[authv1.SignInRequest]) (*connect.Response[authv1.SignInResponse], error) {
	key := limiterKey(req.Msg.GetEmail())
	if !s.limiter.allow(key) {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("too many failed sign-in attempts; try again later"))
	}
	u, err := s.store.VerifyPassword(ctx, req.Msg.GetEmail(), req.Msg.GetPassword())
	if err != nil {
		if errors.Is(err, ErrInvalidCredentials) {
			s.limiter.fail(key)
		}
		return nil, rpcError(err)
	}
	s.limiter.reset(key)
	tokens, err := s.sessions.Start(ctx, u)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&authv1.SignInResponse{User: wireUser(u), Tokens: wireTokens(tokens)}), nil
}

func (s *Service) SignUp(ctx context.Context, req *connect.Request[authv1.SignUpRequest]) (*connect.Response[authv1.SignUpResponse], error) {
	if !s.cfg.OpenSignUp {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("sign-up is closed on this host"))
	}
	if req.Msg.GetPassword() == "" {
		return nil, rpcError(ErrWeakPassword)
	}
	u, err := s.store.CreateUser(ctx, NewUser{Email: req.Msg.GetEmail(), Name: req.Msg.GetName(), Password: req.Msg.GetPassword()})
	if err != nil {
		return nil, rpcError(err)
	}
	tokens, err := s.sessions.Start(ctx, u)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&authv1.SignUpResponse{User: wireUser(u), Tokens: wireTokens(tokens)}), nil
}

func (s *Service) Refresh(ctx context.Context, req *connect.Request[authv1.RefreshRequest]) (*connect.Response[authv1.RefreshResponse], error) {
	tokens, err := s.sessions.Refresh(ctx, req.Msg.GetRefreshToken())
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&authv1.RefreshResponse{Tokens: wireTokens(tokens)}), nil
}

func (s *Service) SignOut(ctx context.Context, req *connect.Request[authv1.SignOutRequest]) (*connect.Response[authv1.SignOutResponse], error) {
	// Idempotent: a sign-out is the client forgetting its credentials, and
	// it must not fail because the server already had.
	if err := s.sessions.Revoke(ctx, req.Msg.GetRefreshToken()); err != nil && !errors.Is(err, ErrInvalidSession) {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&authv1.SignOutResponse{}), nil
}

func (s *Service) GetCurrentUser(ctx context.Context, req *connect.Request[authv1.GetCurrentUserRequest]) (*connect.Response[authv1.GetCurrentUserResponse], error) {
	_, u, err := s.caller(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&authv1.GetCurrentUserResponse{User: wireUser(u)}), nil
}

func (s *Service) ChangePassword(ctx context.Context, req *connect.Request[authv1.ChangePasswordRequest]) (*connect.Response[authv1.ChangePasswordResponse], error) {
	pr, u, err := s.caller(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if _, err := s.store.VerifyPassword(ctx, u.Email, req.Msg.GetCurrentPassword()); err != nil {
		if errors.Is(err, ErrInvalidCredentials) {
			return nil, denied("current password is incorrect")
		}
		return nil, rpcError(err)
	}
	if err := s.store.SetPassword(ctx, u.ID, req.Msg.GetNewPassword()); err != nil {
		return nil, rpcError(err)
	}
	sid, _ := pr.Claims["sid"].(string)
	if err := s.sessions.RevokeOthers(ctx, u.ID, sid); err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&authv1.ChangePasswordResponse{}), nil
}

func (s *Service) ListWorkspaces(ctx context.Context, req *connect.Request[authv1.ListWorkspacesRequest]) (*connect.Response[authv1.ListWorkspacesResponse], error) {
	_, u, err := s.caller(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	resp := &authv1.ListWorkspacesResponse{}
	if u.Admin {
		all, err := s.store.ListAllWorkspaces(ctx)
		if err != nil {
			return nil, rpcError(err)
		}
		for _, w := range all {
			resp.Workspaces = append(resp.Workspaces, &authv1.Workspace{
				AppId: w.AppID, WorkspaceId: w.ID, Name: w.Name, Role: authv1.Role_ROLE_OWNER,
			})
		}
		return connect.NewResponse(resp), nil
	}
	mine, err := s.store.ListUserWorkspaces(ctx, u.ID)
	if err != nil {
		return nil, rpcError(err)
	}
	for _, w := range mine {
		resp.Workspaces = append(resp.Workspaces, &authv1.Workspace{
			AppId: w.AppID, WorkspaceId: w.ID, Name: w.Name, Role: wireRole(w.Role),
		})
	}
	return connect.NewResponse(resp), nil
}

// membership answers what the caller is in a workspace: their role, or none.
// An administrator counts as an owner everywhere.
func (s *Service) membership(ctx context.Context, u User, appID, workspaceID string) (Role, bool, error) {
	if u.Admin {
		return RoleOwner, true, nil
	}
	m, err := s.store.Member(ctx, appID, workspaceID, u.ID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", false, nil
		}
		return "", false, rpcError(err)
	}
	return m.Role, true, nil
}

func (s *Service) ListMembers(ctx context.Context, req *connect.Request[authv1.ListMembersRequest]) (*connect.Response[authv1.ListMembersResponse], error) {
	_, u, err := s.caller(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	appID, workspaceID := req.Msg.GetAppId(), req.Msg.GetWorkspaceId()
	_, member, err := s.membership(ctx, u, appID, workspaceID)
	if err != nil {
		return nil, err
	}
	if !member {
		return nil, denied("not a member of this workspace")
	}
	members, err := s.store.ListMemberUsers(ctx, appID, workspaceID)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &authv1.ListMembersResponse{}
	for _, m := range members {
		resp.Members = append(resp.Members, wireMember(m))
	}
	return connect.NewResponse(resp), nil
}

// manager checks that the caller may change a workspace's member list.
func (s *Service) manager(ctx context.Context, u User, appID, workspaceID string) error {
	role, member, err := s.membership(ctx, u, appID, workspaceID)
	if err != nil {
		return err
	}
	if !member || !role.CanManage() {
		return denied("only an owner may manage members")
	}
	return nil
}

// lastOwner reports whether userID is the only owner of the workspace, in
// which case demoting or removing them would leave it with no one to manage
// it but an administrator.
func (s *Service) lastOwner(ctx context.Context, appID, workspaceID, userID string) (bool, error) {
	m, err := s.store.Member(ctx, appID, workspaceID, userID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, rpcError(err)
	}
	if m.Role != RoleOwner {
		return false, nil
	}
	n, err := s.store.CountOwners(ctx, appID, workspaceID)
	if err != nil {
		return false, rpcError(err)
	}
	return n == 1, nil
}

func (s *Service) forget(appID string) {
	if s.policies != nil {
		s.policies.Forget(appID)
	}
}

func (s *Service) SetMember(ctx context.Context, req *connect.Request[authv1.SetMemberRequest]) (*connect.Response[authv1.SetMemberResponse], error) {
	_, u, err := s.caller(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	appID, workspaceID := req.Msg.GetAppId(), req.Msg.GetWorkspaceId()
	role, ok := roleOf(req.Msg.GetRole())
	if !ok {
		return nil, invalid("role is required")
	}
	if err := s.manager(ctx, u, appID, workspaceID); err != nil {
		return nil, err
	}
	target, err := s.store.UserByEmail(ctx, req.Msg.GetEmail())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("no account with that e-mail address"))
		}
		return nil, rpcError(err)
	}
	if role != RoleOwner {
		last, err := s.lastOwner(ctx, appID, workspaceID, target.ID)
		if err != nil {
			return nil, err
		}
		if last {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("a workspace keeps at least one owner"))
		}
	}
	if err := s.store.SetMember(ctx, appID, workspaceID, target.ID, role); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("workspace does not exist"))
		}
		return nil, rpcError(err)
	}
	s.forget(appID)
	m, err := s.store.Member(ctx, appID, workspaceID, target.ID)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&authv1.SetMemberResponse{
		Member: wireMember(MemberUser{Member: m, Email: target.Email, Name: target.Name}),
	}), nil
}

func (s *Service) RemoveMember(ctx context.Context, req *connect.Request[authv1.RemoveMemberRequest]) (*connect.Response[authv1.RemoveMemberResponse], error) {
	_, u, err := s.caller(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	appID, workspaceID, userID := req.Msg.GetAppId(), req.Msg.GetWorkspaceId(), req.Msg.GetUserId()
	if err := s.manager(ctx, u, appID, workspaceID); err != nil {
		return nil, err
	}
	last, err := s.lastOwner(ctx, appID, workspaceID, userID)
	if err != nil {
		return nil, err
	}
	if last {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("a workspace keeps at least one owner"))
	}
	if err := s.store.RemoveMember(ctx, appID, workspaceID, userID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("%s is not a member", userID))
		}
		return nil, rpcError(err)
	}
	s.forget(appID)
	return connect.NewResponse(&authv1.RemoveMemberResponse{}), nil
}
