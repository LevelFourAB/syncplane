package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/levelfourab/syncplane/engine/server"
)

// Store persists users, workspaces, memberships and identities. It is safe
// for concurrent use. Open it once per process with OpenSQLite or
// OpenPostgres and Close it when done.
//
// Every method returns the sentinel errors declared in this package for the
// outcomes a caller acts on — ErrNotFound, ErrEmailTaken, ErrInvalidCredentials
// and the rest — and wraps anything else, which is a database failure the
// caller reports rather than handles. Times are UTC with millisecond precision.
type Store struct {
	b backend
}

// OpenSQLite opens the auth tables in the SQLite database at path, creating
// the file and the tables when they do not exist and upgrading them when they
// are from an older binary. Passing the path the sync store uses is the
// intended way to run: the two share the file and nothing else (see the
// package comment).
func OpenSQLite(path string) (*Store, error) {
	b, err := openSQLite(path)
	if err != nil {
		return nil, err
	}
	s := &Store{b: b}
	if err := s.migrate(context.Background()); err != nil {
		_ = b.close()
		return nil, err
	}
	return s, nil
}

// OpenPostgres opens the auth tables on the PostgreSQL server dsn names,
// creating and upgrading them as OpenSQLite does. Nodes that boot at the same
// time serialize their migrations on an advisory lock, so each step runs once.
func OpenPostgres(ctx context.Context, dsn string) (*Store, error) {
	b, err := openPostgres(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{b: b}
	if err := s.migrate(ctx); err != nil {
		_ = b.close()
		return nil, err
	}
	return s, nil
}

// Close releases the database connections. The Store must not be used after.
func (s *Store) Close() error {
	return s.b.close()
}

// migrations[i] takes the auth tables from version i to i+1. Index 0 is the
// baseline. Structural changes append here; the ledger table records which
// steps a database has, and migrate applies the rest in one transaction.
var migrations = []string{`
CREATE TABLE auth_users (
	id            {TEXT} PRIMARY KEY,
	email         {TEXT} NOT NULL UNIQUE,
	name          {TEXT} NOT NULL DEFAULT '',
	admin         BOOLEAN NOT NULL DEFAULT FALSE,
	disabled      BOOLEAN NOT NULL DEFAULT FALSE,
	password_hash {TEXT},
	created_at    BIGINT NOT NULL
);
CREATE TABLE auth_identities (
	issuer    {TEXT} NOT NULL,
	subject   {TEXT} NOT NULL,
	user_id   {TEXT} NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
	linked_at BIGINT NOT NULL,
	PRIMARY KEY (issuer, subject)
);
CREATE INDEX auth_identities_user ON auth_identities (user_id);
CREATE TABLE auth_workspaces (
	app_id       {TEXT} NOT NULL,
	workspace_id {TEXT} NOT NULL,
	name         {TEXT} NOT NULL DEFAULT '',
	created_at   BIGINT NOT NULL,
	PRIMARY KEY (app_id, workspace_id)
);
CREATE TABLE auth_members (
	app_id       {TEXT} NOT NULL,
	workspace_id {TEXT} NOT NULL,
	user_id      {TEXT} NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
	role         {TEXT} NOT NULL,
	added_at     BIGINT NOT NULL,
	PRIMARY KEY (app_id, workspace_id, user_id),
	FOREIGN KEY (app_id, workspace_id)
		REFERENCES auth_workspaces(app_id, workspace_id) ON DELETE CASCADE
);
CREATE INDEX auth_members_user ON auth_members (user_id);
`}

const ledgerDDL = `
CREATE TABLE IF NOT EXISTS auth_migrations (
	version    BIGINT PRIMARY KEY,
	applied_at BIGINT NOT NULL
);`

// migrate brings the tables to len(migrations). The lock, the ledger, the
// version read and every step share one transaction, so a node that loses the
// race finds the work done when it gets the lock and applies nothing.
func (s *Store) migrate(ctx context.Context) error {
	return s.b.tx(ctx, func(q querier) error {
		if err := s.b.lockMigrations(ctx, q); err != nil {
			return fmt.Errorf("auth: take migration lock: %w", err)
		}
		if _, err := q.exec(ctx, ledgerDDL); err != nil {
			return fmt.Errorf("auth: create migration ledger: %w", err)
		}
		var version int
		if err := queryOne(ctx, q, `SELECT COALESCE(MAX(version), 0) FROM auth_migrations`, nil, &version); err != nil {
			return fmt.Errorf("auth: read schema version: %w", err)
		}
		if version > len(migrations) {
			return fmt.Errorf("auth: database schema version %d is newer than this binary (max %d)", version, len(migrations))
		}
		for ; version < len(migrations); version++ {
			if _, err := q.exec(ctx, s.b.ddl(migrations[version])); err != nil {
				return fmt.Errorf("auth: migration %d: %w", version+1, err)
			}
			if _, err := q.exec(ctx, `INSERT INTO auth_migrations (version, applied_at) VALUES (?, ?)`,
				version+1, now()); err != nil {
				return fmt.Errorf("auth: migration %d: record: %w", version+1, err)
			}
		}
		return nil
	})
}

// errNoRows is what scanOne returns for an empty result. Callers turn it into
// ErrNotFound or ErrInvalidCredentials as their contract says.
var errNoRows = errors.New("auth: no rows")

// queryOne runs a query expected to yield at most one row and scans it into
// dest. An empty result is errNoRows.
func queryOne(ctx context.Context, q querier, sql string, args []any, dest ...any) error {
	r, err := q.query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer r.Close()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return err
		}
		return errNoRows
	}
	if err := r.Scan(dest...); err != nil {
		return err
	}
	return r.Err()
}

func now() int64 { return time.Now().UnixMilli() }

func fromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func newID() string { return uuid.Must(uuid.NewV7()).String() }

// normalizeEmail trims and lower-cases the address and checks it has the
// shape local@domain. Nothing stricter: the address is a login name here, and
// whether it delivers mail is not this package's concern.
func normalizeEmail(email string) (string, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	at := strings.IndexByte(email, '@')
	if at <= 0 || at == len(email)-1 || len(email) > 254 || strings.ContainsAny(email, " \t\r\n") {
		return "", ErrInvalidEmail
	}
	return email, nil
}

func checkPassword(password string) error {
	if len(password) < MinPasswordLength {
		return ErrWeakPassword
	}
	return nil
}

func checkWorkspace(appID, workspaceID string) error {
	if appID == "" {
		return ErrInvalidWorkspace
	}
	if err := server.ValidateWorkspaceID(workspaceID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidWorkspace, err)
	}
	return nil
}

// ---- users ----

const userColumns = `id, email, name, admin, disabled, created_at`

func scanUser(r rows) (User, error) {
	var u User
	var created int64
	if err := r.Scan(&u.ID, &u.Email, &u.Name, &u.Admin, &u.Disabled, &created); err != nil {
		return User{}, err
	}
	u.CreatedAt = fromMillis(created)
	return u, nil
}

func (s *Store) userWhere(ctx context.Context, q querier, where string, arg any) (User, error) {
	r, err := q.query(ctx, `SELECT `+userColumns+` FROM auth_users WHERE `+where, arg)
	if err != nil {
		return User{}, fmt.Errorf("auth: load user: %w", err)
	}
	defer r.Close()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return User{}, fmt.Errorf("auth: load user: %w", err)
		}
		return User{}, ErrNotFound
	}
	u, err := scanUser(r)
	if err != nil {
		return User{}, fmt.Errorf("auth: load user: %w", err)
	}
	return u, nil
}

// CreateUser adds a user. It fails with ErrInvalidEmail, ErrWeakPassword when
// a password is given and too short, and ErrEmailTaken.
func (s *Store) CreateUser(ctx context.Context, in NewUser) (User, error) {
	email, err := normalizeEmail(in.Email)
	if err != nil {
		return User{}, err
	}
	var hash *string
	if in.Password != "" {
		if err := checkPassword(in.Password); err != nil {
			return User{}, err
		}
		h, err := hashPassword(in.Password)
		if err != nil {
			return User{}, err
		}
		hash = &h
	}
	u := User{ID: newID(), Email: email, Name: in.Name, Admin: in.Admin, CreatedAt: fromMillis(now())}
	_, err = s.b.exec(ctx, `INSERT INTO auth_users (id, email, name, admin, disabled, password_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		u.ID, u.Email, u.Name, u.Admin, false, hash, u.CreatedAt.UnixMilli())
	if err != nil {
		if s.b.classify(err) == constraintUnique {
			return User{}, ErrEmailTaken
		}
		return User{}, fmt.Errorf("auth: create user: %w", err)
	}
	return u, nil
}

// User loads a user by id. It fails with ErrNotFound.
func (s *Store) User(ctx context.Context, id string) (User, error) {
	return s.userWhere(ctx, s.b, `id = ?`, id)
}

// UserByEmail loads a user by e-mail address, compared after normalization.
// It fails with ErrNotFound; a malformed address is also not found.
func (s *Store) UserByEmail(ctx context.Context, email string) (User, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		return User{}, ErrNotFound
	}
	return s.userWhere(ctx, s.b, `email = ?`, email)
}

// UserBySubject loads the user a principal subject names. A subject without
// SubjectPrefix, or with an unknown id, is ErrNotFound.
func (s *Store) UserBySubject(ctx context.Context, subject string) (User, error) {
	id, ok := strings.CutPrefix(subject, SubjectPrefix)
	if !ok || id == "" {
		return User{}, ErrNotFound
	}
	return s.User(ctx, id)
}

// ListUsers returns every user ordered by e-mail address.
func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	r, err := s.b.query(ctx, `SELECT `+userColumns+` FROM auth_users ORDER BY email`)
	if err != nil {
		return nil, fmt.Errorf("auth: list users: %w", err)
	}
	defer r.Close()
	var users []User
	for r.Next() {
		u, err := scanUser(r)
		if err != nil {
			return nil, fmt.Errorf("auth: list users: %w", err)
		}
		users = append(users, u)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list users: %w", err)
	}
	return users, nil
}

// UpdateUser changes the fields upd names and returns the user as stored
// afterwards. It fails with ErrNotFound.
func (s *Store) UpdateUser(ctx context.Context, id string, upd UserUpdate) (User, error) {
	var out User
	err := s.b.tx(ctx, func(q querier) error {
		u, err := s.userWhere(ctx, q, `id = ?`, id)
		if err != nil {
			return err
		}
		if upd.Name != nil {
			u.Name = *upd.Name
		}
		if upd.Admin != nil {
			u.Admin = *upd.Admin
		}
		if upd.Disabled != nil {
			u.Disabled = *upd.Disabled
		}
		if _, err := q.exec(ctx, `UPDATE auth_users SET name = ?, admin = ?, disabled = ? WHERE id = ?`,
			u.Name, u.Admin, u.Disabled, id); err != nil {
			return fmt.Errorf("auth: update user: %w", err)
		}
		out = u
		return nil
	})
	return out, err
}

// DeleteUser removes a user with their memberships and linked identities. It
// fails with ErrNotFound.
func (s *Store) DeleteUser(ctx context.Context, id string) error {
	n, err := s.b.exec(ctx, `DELETE FROM auth_users WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("auth: delete user: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetPassword replaces the user's password, or gives one to a user who had
// none. It fails with ErrWeakPassword and ErrNotFound.
func (s *Store) SetPassword(ctx context.Context, id, password string) error {
	if err := checkPassword(password); err != nil {
		return err
	}
	// Hashing runs before the write so the transaction, and on SQLite the
	// file's write lock, is held for the UPDATE alone.
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	n, err := s.b.exec(ctx, `UPDATE auth_users SET password_hash = ? WHERE id = ?`, hash, id)
	if err != nil {
		return fmt.Errorf("auth: set password: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// VerifyPassword checks an e-mail and password and returns the user they
// belong to. An unknown address, a wrong password and a user with no password
// all fail with ErrInvalidCredentials and take the same time to do so. A
// correct password for a disabled user fails with ErrDisabled.
func (s *Store) VerifyPassword(ctx context.Context, email, password string) (User, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		verifyDummy(password)
		return User{}, ErrInvalidCredentials
	}
	var u User
	var created int64
	var hash *string
	err = queryOne(ctx, s.b, `SELECT `+userColumns+`, password_hash FROM auth_users WHERE email = ?`,
		[]any{email}, &u.ID, &u.Email, &u.Name, &u.Admin, &u.Disabled, &created, &hash)
	if err != nil {
		if errors.Is(err, errNoRows) {
			verifyDummy(password)
			return User{}, ErrInvalidCredentials
		}
		return User{}, fmt.Errorf("auth: verify password: %w", err)
	}
	if hash == nil {
		verifyDummy(password)
		return User{}, ErrInvalidCredentials
	}
	ok, err := verifyPassword(*hash, password)
	if err != nil {
		return User{}, fmt.Errorf("auth: verify password: %w", err)
	}
	if !ok {
		return User{}, ErrInvalidCredentials
	}
	if u.Disabled {
		return User{}, ErrDisabled
	}
	u.CreatedAt = fromMillis(created)
	return u, nil
}

// ---- workspaces ----

const workspaceColumns = `app_id, workspace_id, name, created_at`

func scanWorkspace(r rows) (Workspace, error) {
	var w Workspace
	var created int64
	if err := r.Scan(&w.AppID, &w.ID, &w.Name, &created); err != nil {
		return Workspace{}, err
	}
	w.CreatedAt = fromMillis(created)
	return w, nil
}

// CreateWorkspace records a workspace of app appID. A non-empty ownerID makes
// that user its first member with RoleOwner, in the same transaction. It
// fails with ErrInvalidWorkspace, ErrWorkspaceExists, and ErrNotFound when
// ownerID names no user.
func (s *Store) CreateWorkspace(ctx context.Context, appID, workspaceID, name, ownerID string) (Workspace, error) {
	if err := checkWorkspace(appID, workspaceID); err != nil {
		return Workspace{}, err
	}
	w := Workspace{AppID: appID, ID: workspaceID, Name: name, CreatedAt: fromMillis(now())}
	err := s.b.tx(ctx, func(q querier) error {
		_, err := q.exec(ctx, `INSERT INTO auth_workspaces (app_id, workspace_id, name, created_at) VALUES (?, ?, ?, ?)`,
			w.AppID, w.ID, w.Name, w.CreatedAt.UnixMilli())
		if err != nil {
			if s.b.classify(err) == constraintUnique {
				return ErrWorkspaceExists
			}
			return fmt.Errorf("auth: create workspace: %w", err)
		}
		if ownerID == "" {
			return nil
		}
		return s.setMember(ctx, q, appID, workspaceID, ownerID, RoleOwner)
	})
	if err != nil {
		return Workspace{}, err
	}
	return w, nil
}

// Workspace loads one workspace. It fails with ErrNotFound.
func (s *Store) Workspace(ctx context.Context, appID, workspaceID string) (Workspace, error) {
	r, err := s.b.query(ctx, `SELECT `+workspaceColumns+` FROM auth_workspaces WHERE app_id = ? AND workspace_id = ?`,
		appID, workspaceID)
	if err != nil {
		return Workspace{}, fmt.Errorf("auth: load workspace: %w", err)
	}
	defer r.Close()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return Workspace{}, fmt.Errorf("auth: load workspace: %w", err)
		}
		return Workspace{}, ErrNotFound
	}
	w, err := scanWorkspace(r)
	if err != nil {
		return Workspace{}, fmt.Errorf("auth: load workspace: %w", err)
	}
	return w, nil
}

// ListWorkspaces returns every workspace of an app ordered by id.
func (s *Store) ListWorkspaces(ctx context.Context, appID string) ([]Workspace, error) {
	r, err := s.b.query(ctx, `SELECT `+workspaceColumns+` FROM auth_workspaces WHERE app_id = ? ORDER BY workspace_id`, appID)
	if err != nil {
		return nil, fmt.Errorf("auth: list workspaces: %w", err)
	}
	defer r.Close()
	var out []Workspace
	for r.Next() {
		w, err := scanWorkspace(r)
		if err != nil {
			return nil, fmt.Errorf("auth: list workspaces: %w", err)
		}
		out = append(out, w)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list workspaces: %w", err)
	}
	return out, nil
}

// RenameWorkspace changes the label. It fails with ErrNotFound.
func (s *Store) RenameWorkspace(ctx context.Context, appID, workspaceID, name string) error {
	n, err := s.b.exec(ctx, `UPDATE auth_workspaces SET name = ? WHERE app_id = ? AND workspace_id = ?`,
		name, appID, workspaceID)
	if err != nil {
		return fmt.Errorf("auth: rename workspace: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteWorkspace forgets a workspace and its member list. The sync data
// under that id is untouched: it lives in the sync store, which this package
// does not reach. It fails with ErrNotFound.
func (s *Store) DeleteWorkspace(ctx context.Context, appID, workspaceID string) error {
	n, err := s.b.exec(ctx, `DELETE FROM auth_workspaces WHERE app_id = ? AND workspace_id = ?`, appID, workspaceID)
	if err != nil {
		return fmt.Errorf("auth: delete workspace: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ---- members ----

const memberColumns = `app_id, workspace_id, user_id, role, added_at`

func scanMember(r rows) (Member, error) {
	var m Member
	var added int64
	if err := r.Scan(&m.AppID, &m.WorkspaceID, &m.UserID, &m.Role, &added); err != nil {
		return Member{}, err
	}
	m.AddedAt = fromMillis(added)
	return m, nil
}

func (s *Store) setMember(ctx context.Context, q querier, appID, workspaceID, userID string, role Role) error {
	if !role.Valid() {
		return ErrInvalidRole
	}
	// added_at keeps the first value on a role change: the row records when
	// the user joined, not when they were last promoted.
	_, err := q.exec(ctx, `INSERT INTO auth_members (app_id, workspace_id, user_id, role, added_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (app_id, workspace_id, user_id) DO UPDATE SET role = excluded.role`,
		appID, workspaceID, userID, string(role), now())
	if err != nil {
		if s.b.classify(err) == constraintForeignKey {
			return ErrNotFound
		}
		return fmt.Errorf("auth: set member: %w", err)
	}
	return nil
}

// SetMember adds a user to a workspace with a role, or changes the role of a
// user already in it. It fails with ErrInvalidRole, and with ErrNotFound when
// the workspace or the user does not exist.
func (s *Store) SetMember(ctx context.Context, appID, workspaceID, userID string, role Role) error {
	return s.setMember(ctx, s.b, appID, workspaceID, userID, role)
}

// RemoveMember takes a user out of a workspace. It fails with ErrNotFound
// when they were not a member.
func (s *Store) RemoveMember(ctx context.Context, appID, workspaceID, userID string) error {
	n, err := s.b.exec(ctx, `DELETE FROM auth_members WHERE app_id = ? AND workspace_id = ? AND user_id = ?`,
		appID, workspaceID, userID)
	if err != nil {
		return fmt.Errorf("auth: remove member: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Member returns a user's role in a workspace. It fails with ErrNotFound when
// they are not a member, which is also the answer for a workspace that does
// not exist. This is the call the workspace gate makes on every RPC, so it is
// one indexed lookup.
func (s *Store) Member(ctx context.Context, appID, workspaceID, userID string) (Member, error) {
	r, err := s.b.query(ctx, `SELECT `+memberColumns+` FROM auth_members WHERE app_id = ? AND workspace_id = ? AND user_id = ?`,
		appID, workspaceID, userID)
	if err != nil {
		return Member{}, fmt.Errorf("auth: load member: %w", err)
	}
	defer r.Close()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return Member{}, fmt.Errorf("auth: load member: %w", err)
		}
		return Member{}, ErrNotFound
	}
	m, err := scanMember(r)
	if err != nil {
		return Member{}, fmt.Errorf("auth: load member: %w", err)
	}
	return m, nil
}

func (s *Store) members(ctx context.Context, what, where string, args ...any) ([]Member, error) {
	r, err := s.b.query(ctx, `SELECT `+memberColumns+` FROM auth_members WHERE `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("auth: %s: %w", what, err)
	}
	defer r.Close()
	var out []Member
	for r.Next() {
		m, err := scanMember(r)
		if err != nil {
			return nil, fmt.Errorf("auth: %s: %w", what, err)
		}
		out = append(out, m)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: %s: %w", what, err)
	}
	return out, nil
}

// ListMembers returns a workspace's members ordered by user id. A workspace
// that does not exist has no members and is not an error.
func (s *Store) ListMembers(ctx context.Context, appID, workspaceID string) ([]Member, error) {
	return s.members(ctx, "list members", `app_id = ? AND workspace_id = ? ORDER BY user_id`, appID, workspaceID)
}

// ListMemberships returns every workspace a user belongs to, across apps,
// ordered by app then workspace id.
func (s *Store) ListMemberships(ctx context.Context, userID string) ([]Member, error) {
	return s.members(ctx, "list memberships", `user_id = ? ORDER BY app_id, workspace_id`, userID)
}

// ---- identities ----

// LinkIdentity ties an external (issuer, subject) to a user. Linking the same
// pair to the same user again is a no-op; to a different user it fails with
// ErrIdentityLinked. It also fails with ErrInvalidIdentity and, when userID
// names no user, ErrNotFound.
func (s *Store) LinkIdentity(ctx context.Context, issuer, subject, userID string) error {
	if issuer == "" || subject == "" {
		return ErrInvalidIdentity
	}
	_, err := s.b.exec(ctx, `INSERT INTO auth_identities (issuer, subject, user_id, linked_at) VALUES (?, ?, ?, ?)`,
		issuer, subject, userID, now())
	if err == nil {
		return nil
	}
	switch s.b.classify(err) {
	case constraintForeignKey:
		return ErrNotFound
	case constraintUnique:
		existing, lookupErr := s.identity(ctx, issuer, subject)
		if lookupErr != nil {
			return lookupErr
		}
		if existing.UserID == userID {
			return nil
		}
		return ErrIdentityLinked
	}
	return fmt.Errorf("auth: link identity: %w", err)
}

func (s *Store) identity(ctx context.Context, issuer, subject string) (Identity, error) {
	var id Identity
	var linked int64
	err := queryOne(ctx, s.b, `SELECT issuer, subject, user_id, linked_at FROM auth_identities WHERE issuer = ? AND subject = ?`,
		[]any{issuer, subject}, &id.Issuer, &id.Subject, &id.UserID, &linked)
	if err != nil {
		if errors.Is(err, errNoRows) {
			return Identity{}, ErrNotFound
		}
		return Identity{}, fmt.Errorf("auth: load identity: %w", err)
	}
	id.LinkedAt = fromMillis(linked)
	return id, nil
}

// UserByIdentity loads the user an external identity is linked to. It fails
// with ErrNotFound.
func (s *Store) UserByIdentity(ctx context.Context, issuer, subject string) (User, error) {
	id, err := s.identity(ctx, issuer, subject)
	if err != nil {
		return User{}, err
	}
	return s.User(ctx, id.UserID)
}

// UnlinkIdentity removes an external identity. It fails with ErrNotFound.
func (s *Store) UnlinkIdentity(ctx context.Context, issuer, subject string) error {
	n, err := s.b.exec(ctx, `DELETE FROM auth_identities WHERE issuer = ? AND subject = ?`, issuer, subject)
	if err != nil {
		return fmt.Errorf("auth: unlink identity: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ListIdentities returns a user's linked identities ordered by issuer then
// subject.
func (s *Store) ListIdentities(ctx context.Context, userID string) ([]Identity, error) {
	r, err := s.b.query(ctx, `SELECT issuer, subject, user_id, linked_at FROM auth_identities WHERE user_id = ? ORDER BY issuer, subject`, userID)
	if err != nil {
		return nil, fmt.Errorf("auth: list identities: %w", err)
	}
	defer r.Close()
	var out []Identity
	for r.Next() {
		var id Identity
		var linked int64
		if err := r.Scan(&id.Issuer, &id.Subject, &id.UserID, &linked); err != nil {
			return nil, fmt.Errorf("auth: list identities: %w", err)
		}
		id.LinkedAt = fromMillis(linked)
		out = append(out, id)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list identities: %w", err)
	}
	return out, nil
}
