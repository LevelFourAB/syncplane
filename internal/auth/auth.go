// Package auth is the identity core of the standalone syncplaned host: the
// users, passwords, workspaces, memberships and linked external identities
// that let the binary be deployed on its own, with no embedding application to
// supply them.
//
// A Store keeps that state in the same database as the sync data — the SQLite
// file or the PostgreSQL server that -db names — through a connection of its
// own and in tables of its own, all prefixed auth_. Nothing here reaches the
// storage.Adapter seam, so an application that embeds the engine and brings
// its own identity is unaffected by this package and never imports it.
//
// Every user has an opaque, stable id, and User.Subject renders it as the
// subject a server.Principal carries. A user who changes e-mail address is the
// same subject to the permission hooks and on the actor recorded on every row
// they wrote. Workspaces are scoped per app, as they are in the engine:
// ("todo", "team") and ("crm", "team") are two workspaces with two member
// lists.
//
// # Sharing a SQLite file with the sync store
//
// SQLite has one write lock per file, and the sync store already holds a
// writer connection on it. The auth store's write transactions are short — a
// password is hashed before the transaction opens, never inside it — and both
// stores wait out contention with a busy timeout, so in practice they
// interleave. A sync write that holds the lock for longer than that timeout
// fails the auth write with SQLITE_BUSY rather than corrupting anything.
package auth

import (
	"errors"
	"time"
)

// SubjectPrefix starts every subject this package issues. It namespaces local
// users apart from subjects another Authenticator might produce, as
// docs/how-to/authentication-and-permissions.md recommends for multi-issuer
// deployments.
const SubjectPrefix = "user:"

// MinPasswordLength is the shortest password CreateUser and SetPassword
// accept, in bytes. Shorter ones fail with ErrWeakPassword.
const MinPasswordLength = 8

var (
	// ErrNotFound reports that the user, workspace, member or identity a call
	// named does not exist.
	ErrNotFound = errors.New("auth: not found")
	// ErrEmailTaken reports that another user already has the e-mail address.
	// Addresses compare after normalization, so it also fires for a change of
	// case.
	ErrEmailTaken = errors.New("auth: email already in use")
	// ErrInvalidEmail reports an address that is empty or not of the form
	// local@domain.
	ErrInvalidEmail = errors.New("auth: invalid email address")
	// ErrWeakPassword reports a password shorter than MinPasswordLength.
	ErrWeakPassword = errors.New("auth: password too short")
	// ErrInvalidCredentials reports that no enabled user has this e-mail and
	// password. It is deliberately one error for an unknown address, a wrong
	// password and a user with no password set, so a caller cannot probe which.
	ErrInvalidCredentials = errors.New("auth: invalid credentials")
	// ErrDisabled reports that the credentials were correct but the user is
	// disabled and must not be let in.
	ErrDisabled = errors.New("auth: user is disabled")
	// ErrInvalidWorkspace reports a workspace id the sync RPCs would refuse,
	// or an empty app id.
	ErrInvalidWorkspace = errors.New("auth: invalid workspace")
	// ErrWorkspaceExists reports that the app already has a workspace with
	// this id.
	ErrWorkspaceExists = errors.New("auth: workspace already exists")
	// ErrInvalidRole reports a role that is not one of the Role constants.
	ErrInvalidRole = errors.New("auth: invalid role")
	// ErrInvalidIdentity reports an external identity with an empty issuer or
	// subject.
	ErrInvalidIdentity = errors.New("auth: invalid identity")
	// ErrIdentityLinked reports that the external identity is already linked
	// to a different user.
	ErrIdentityLinked = errors.New("auth: identity linked to another user")
	// ErrInvalidSession reports a refresh token that names no live session:
	// malformed, unknown, expired, revoked, or already rotated away. The
	// caller must sign in again.
	ErrInvalidSession = errors.New("auth: invalid session")
	// ErrInvalidToken reports an access token that does not verify:
	// malformed, signed by an unknown key, expired, or issued for another
	// issuer.
	ErrInvalidToken = errors.New("auth: invalid token")
)

// User is an account on the host. ID is opaque and stable for the life of the
// account; Email is stored normalized (trimmed, lower-cased). Admin grants the
// host-wide role that manages users, workspaces and schemas. A Disabled user
// keeps every record but cannot sign in.
type User struct {
	ID        string
	Email     string
	Name      string
	Admin     bool
	Disabled  bool
	CreatedAt time.Time
}

// Subject is the principal subject for this user: SubjectPrefix followed by
// the id. It is what the permission hooks and the row actor see.
func (u User) Subject() string {
	return SubjectPrefix + u.ID
}

// NewUser is the input to Store.CreateUser. Password may be empty for a user
// who signs in only through a linked external identity; SetPassword can add
// one later.
type NewUser struct {
	Email    string
	Name     string
	Password string
	Admin    bool
}

// UserUpdate names the fields Store.UpdateUser changes. A nil field keeps the
// stored value. E-mail and password change through their own calls.
type UserUpdate struct {
	Name     *string
	Admin    *bool
	Disabled *bool
}

// Role is what a member may do in one workspace. Roles are stored as their
// string value, so a new role is a code change and no migration.
type Role string

const (
	// RoleOwner reads, writes and manages the member list.
	RoleOwner Role = "owner"
	// RoleWriter reads and writes.
	RoleWriter Role = "writer"
	// RoleReader reads only.
	RoleReader Role = "reader"
)

// Valid reports whether r is one of the Role constants.
func (r Role) Valid() bool {
	switch r {
	case RoleOwner, RoleWriter, RoleReader:
		return true
	}
	return false
}

// CanWrite reports whether the role may mutate rows.
func (r Role) CanWrite() bool {
	return r == RoleOwner || r == RoleWriter
}

// CanManage reports whether the role may change the member list.
func (r Role) CanManage() bool {
	return r == RoleOwner
}

// Workspace is a sync workspace the host knows about. AppID and ID together
// identify it; ID is the workspace_id the sync RPCs carry. Name is a label for
// people and has no other meaning.
type Workspace struct {
	AppID     string
	ID        string
	Name      string
	CreatedAt time.Time
}

// Member is one user's role in one workspace.
type Member struct {
	AppID       string
	WorkspaceID string
	UserID      string
	Role        Role
	AddedAt     time.Time
}

// Identity is an external account linked to a user: the issuer that
// authenticated it and the subject it gave, an OIDC provider's `iss` and
// `sub`. A user may have several; each (Issuer, Subject) pair links to at
// most one user.
type Identity struct {
	Issuer   string
	Subject  string
	UserID   string
	LinkedAt time.Time
}
