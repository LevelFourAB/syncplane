package auth

import (
	"context"
	"fmt"
)

// The reads here join two tables for the sign-in service, which shows people
// e-mail addresses and workspace names rather than ids. They are read-only
// views over the same rows the single-table calls in store.go write.

// MemberUser is a Member together with the account it names.
type MemberUser struct {
	Member
	Email string
	Name  string
}

// ListMemberUsers returns a workspace's members with their accounts, ordered
// by e-mail address. A workspace that does not exist has no members and is
// not an error.
func (s *Store) ListMemberUsers(ctx context.Context, appID, workspaceID string) ([]MemberUser, error) {
	r, err := s.b.query(ctx, `SELECT m.app_id, m.workspace_id, m.user_id, m.role, m.added_at, u.email, u.name
		FROM auth_members m JOIN auth_users u ON u.id = m.user_id
		WHERE m.app_id = ? AND m.workspace_id = ? ORDER BY u.email`, appID, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("auth: list members: %w", err)
	}
	defer r.Close()
	var out []MemberUser
	for r.Next() {
		var mu MemberUser
		var added int64
		if err := r.Scan(&mu.AppID, &mu.WorkspaceID, &mu.UserID, &mu.Role, &added, &mu.Email, &mu.Name); err != nil {
			return nil, fmt.Errorf("auth: list members: %w", err)
		}
		mu.AddedAt = fromMillis(added)
		out = append(out, mu)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list members: %w", err)
	}
	return out, nil
}

// UserWorkspace is a Workspace together with the role one user holds in it.
type UserWorkspace struct {
	Workspace
	Role Role
}

// ListUserWorkspaces returns every workspace a user is a member of, across
// apps, ordered by app then workspace id.
func (s *Store) ListUserWorkspaces(ctx context.Context, userID string) ([]UserWorkspace, error) {
	r, err := s.b.query(ctx, `SELECT w.app_id, w.workspace_id, w.name, w.created_at, m.role
		FROM auth_members m JOIN auth_workspaces w ON w.app_id = m.app_id AND w.workspace_id = m.workspace_id
		WHERE m.user_id = ? ORDER BY w.app_id, w.workspace_id`, userID)
	if err != nil {
		return nil, fmt.Errorf("auth: list user workspaces: %w", err)
	}
	defer r.Close()
	var out []UserWorkspace
	for r.Next() {
		var uw UserWorkspace
		var created int64
		if err := r.Scan(&uw.AppID, &uw.ID, &uw.Name, &created, &uw.Role); err != nil {
			return nil, fmt.Errorf("auth: list user workspaces: %w", err)
		}
		uw.CreatedAt = fromMillis(created)
		out = append(out, uw)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list user workspaces: %w", err)
	}
	return out, nil
}

// ListAllWorkspaces returns every workspace of every app, ordered by app then
// workspace id. It is the administrator's view.
func (s *Store) ListAllWorkspaces(ctx context.Context) ([]Workspace, error) {
	r, err := s.b.query(ctx, `SELECT `+workspaceColumns+` FROM auth_workspaces ORDER BY app_id, workspace_id`)
	if err != nil {
		return nil, fmt.Errorf("auth: list all workspaces: %w", err)
	}
	defer r.Close()
	var out []Workspace
	for r.Next() {
		w, err := scanWorkspace(r)
		if err != nil {
			return nil, fmt.Errorf("auth: list all workspaces: %w", err)
		}
		out = append(out, w)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list all workspaces: %w", err)
	}
	return out, nil
}

// CountOwners returns how many members of a workspace hold RoleOwner.
func (s *Store) CountOwners(ctx context.Context, appID, workspaceID string) (int, error) {
	var n int
	err := queryOne(ctx, s.b, `SELECT count(*) FROM auth_members WHERE app_id = ? AND workspace_id = ? AND role = ?`,
		[]any{appID, workspaceID, string(RoleOwner)}, &n)
	if err != nil {
		return 0, fmt.Errorf("auth: count owners: %w", err)
	}
	return n, nil
}
