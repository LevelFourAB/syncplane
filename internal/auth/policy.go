package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
)

// DefaultCacheTTL is how long Policy trusts a membership answer before asking
// the store again, when PolicyConfig leaves CacheTTL zero.
const DefaultCacheTTL = 5 * time.Second

// PolicyConfig configures a Policy.
type PolicyConfig struct {
	// CacheTTL bounds how long a membership or workspace lookup is reused.
	// Zero means DefaultCacheTTL; negative disables the cache. A member
	// removed from a workspace keeps access for at most this long on each
	// node, and a member added waits at most this long to be let in.
	CacheTTL time.Duration
	// Clock supplies the current time for cache expiry. Nil means time.Now.
	Clock func() time.Time
}

// Policy is the host's server.Permissions for one app: the lightweight
// workspace-membership model behind syncplaned. Install one per app with
// server.WithPermissions, built over the same Store the Sessions
// authenticator uses.
//
// The rules, in the order the engine asks:
//
//   - A caller enters a workspace when it exists in the Store and they are a
//     member of it, or when their access token carries the admin claim. An
//     anonymous caller, a non-member and a workspace the Store does not know
//     are all refused with a settled server.WorkspaceDenial.
//   - A member writes when their Role.CanWrite; a reader's mutation is
//     rejected with server.CodePermissionDenied, an ack the client drops
//     without retrying. Admins write everywhere.
//   - Every member reads every row; there is no row filter, so FilterEpoch
//     is constant.
//
// Membership is read from the Store and cached per (workspace, user) for
// PolicyConfig.CacheTTL. Policy is safe for concurrent use.
type Policy struct {
	store *Store
	appID string
	ttl   time.Duration
	now   func() time.Time

	mu    sync.Mutex
	cache map[cacheKey]cacheEntry
}

type cacheKey struct{ workspace, user string }

// cacheEntry is one remembered answer. A negative answer — not a member, or
// no such workspace — is cached too, so a caller hammering a workspace they
// cannot enter costs one lookup per TTL rather than one per RPC.
type cacheEntry struct {
	role    Role
	member  bool
	exists  bool
	expires time.Time
}

// cacheSweepAt is the size past which an insert evicts expired entries. The
// working set is the live (workspace, user) pairs, which is small; this keeps
// a scan of unknown ids from growing the map without bound.
const cacheSweepAt = 4096

// NewPolicy builds the Policy for one app over store.
func NewPolicy(store *Store, appID string, cfg PolicyConfig) *Policy {
	p := &Policy{store: store, appID: appID, ttl: cfg.CacheTTL, now: cfg.Clock, cache: map[cacheKey]cacheEntry{}}
	if p.ttl == 0 {
		p.ttl = DefaultCacheTTL
	}
	if p.now == nil {
		p.now = time.Now
	}
	return p
}

// IsAdmin reports whether the principal's access token carried the admin
// claim. It is the host's notion of "administrator" for every check that is
// not about one workspace.
func IsAdmin(p server.Principal) bool {
	admin, _ := p.Claims["admin"].(bool)
	return admin
}

// lookup answers "may this user enter the workspace, and as what" from the
// cache or the store. An admin's answer depends only on the workspace
// existing, so admins are cached under an empty user id.
func (p *Policy) lookup(ctx context.Context, workspaceID, userID string, admin bool) (cacheEntry, error) {
	key := cacheKey{workspace: workspaceID}
	if !admin {
		key.user = userID
	}
	now := p.now()
	if p.ttl > 0 {
		p.mu.Lock()
		e, ok := p.cache[key]
		p.mu.Unlock()
		if ok && now.Before(e.expires) {
			return e, nil
		}
	}

	var e cacheEntry
	if admin {
		_, err := p.store.Workspace(ctx, p.appID, workspaceID)
		switch {
		case err == nil:
			e.exists = true
		case errors.Is(err, ErrNotFound):
		default:
			return cacheEntry{}, err
		}
	} else {
		m, err := p.store.Member(ctx, p.appID, workspaceID, userID)
		switch {
		case err == nil:
			// A membership row references the workspace row, so the
			// workspace exists.
			e = cacheEntry{role: m.Role, member: true, exists: true}
		case errors.Is(err, ErrNotFound):
		default:
			return cacheEntry{}, err
		}
	}

	if p.ttl > 0 {
		e.expires = now.Add(p.ttl)
		p.mu.Lock()
		if len(p.cache) >= cacheSweepAt {
			for k, old := range p.cache {
				if !now.Before(old.expires) {
					delete(p.cache, k)
				}
			}
		}
		p.cache[key] = e
		p.mu.Unlock()
	}
	return e, nil
}

// userID extracts the store id from a principal this package issued. It is
// empty for an anonymous caller or a subject from another issuer.
func userID(pr server.Principal) string {
	if len(pr.Subject) <= len(SubjectPrefix) || pr.Subject[:len(SubjectPrefix)] != SubjectPrefix {
		return ""
	}
	return pr.Subject[len(SubjectPrefix):]
}

// CanAccessWorkspace implements server.Permissions.
func (p *Policy) CanAccessWorkspace(ctx context.Context, pr server.Principal, workspaceID string) error {
	uid := userID(pr)
	if uid == "" {
		return server.DenyWorkspace("sign in to access this workspace")
	}
	admin := IsAdmin(pr)
	e, err := p.lookup(ctx, workspaceID, uid, admin)
	if err != nil {
		return fmt.Errorf("auth: workspace access: %w", err)
	}
	if admin {
		if !e.exists {
			return server.DenyWorkspace("workspace does not exist")
		}
		return nil
	}
	// A non-member is told only that: whether the workspace exists is not
	// theirs to learn from the error message.
	if !e.member {
		return server.DenyWorkspace("not a member of this workspace")
	}
	return nil
}

// CanMutate implements server.Permissions.
func (p *Policy) CanMutate(ctx context.Context, pr server.Principal, workspaceID string, _ server.Write) error {
	if IsAdmin(pr) {
		return nil
	}
	uid := userID(pr)
	if uid == "" {
		return server.Reject(server.CodePermissionDenied, "sign in to write to this workspace")
	}
	e, err := p.lookup(ctx, workspaceID, uid, false)
	if err != nil {
		return fmt.Errorf("auth: mutate: %w", err)
	}
	if !e.member {
		return server.Reject(server.CodePermissionDenied, "not a member of this workspace")
	}
	if !e.role.CanWrite() {
		return server.Reject(server.CodePermissionDenied, fmt.Sprintf("role %q may not write to this workspace", e.role))
	}
	return nil
}

// ReadFilter implements server.Permissions: every member reads every row.
func (p *Policy) ReadFilter(context.Context, server.Principal, string, string) (server.RowFilter, error) {
	return nil, nil
}

// FilterEpoch implements server.Permissions. Visibility never changes per
// row, so the epoch is constant and no cursor is ever invalidated by it.
func (p *Policy) FilterEpoch(context.Context, server.Principal, string) (uint64, error) {
	return 0, nil
}

// Forget drops every cached answer, so the next RPC asks the store. A host
// that changes a membership on this node calls it to make the change take
// effect at once rather than after CacheTTL. Other nodes learn at their own
// TTL.
func (p *Policy) Forget() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache = map[cacheKey]cacheEntry{}
}
