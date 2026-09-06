package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
)

// Default lifetimes, used when SessionConfig leaves them zero.
const (
	DefaultAccessTTL  = 15 * time.Minute
	DefaultRefreshTTL = 30 * 24 * time.Hour
)

// SessionConfig configures Sessions.
type SessionConfig struct {
	// Issuer is written into every access token's iss claim and checked on
	// verification. Use the public URL of the host. Required.
	Issuer string
	// AccessTTL is how long an access token verifies. Zero means
	// DefaultAccessTTL. A demoted or disabled user keeps their current
	// access for at most this long.
	AccessTTL time.Duration
	// RefreshTTL is how long a session lives without a refresh. Every
	// successful refresh moves the expiry this far into the future. Zero
	// means DefaultRefreshTTL.
	RefreshTTL time.Duration
	// Clock supplies the current time. Nil means time.Now. Tests set it to
	// move tokens past their expiry without waiting.
	Clock func() time.Time
}

// Tokens is what a sign-in or refresh hands the client: a short-lived access
// token to present as a bearer credential on every RPC, and a refresh token
// that obtains the next pair. Both expiry times are absolute.
type Tokens struct {
	AccessToken      string
	AccessExpiresAt  time.Time
	RefreshToken     string
	RefreshExpiresAt time.Time
}

// Session is one signed-in device or browser. RevokedAt is zero while the
// session is live. A session whose ExpiresAt has passed is dead even without
// RevokedAt set.
type Session struct {
	ID         string
	UserID     string
	CreatedAt  time.Time
	LastUsedAt time.Time
	ExpiresAt  time.Time
	RevokedAt  time.Time
}

// Sessions issues and verifies the credentials a signed-in user holds. It is
// the host's server.Authenticator: install it with server.WithAuthenticator
// and every sync RPC carries the user's principal.
//
// Access tokens are Ed25519-signed JWTs (see Claims). The signing key lives in
// the auth tables, so every node of a Postgres deployment signs and verifies
// with the same key, and verification is a signature check with no database
// read. Refresh tokens are opaque; the store keeps only a hash. A refresh
// rotates the token: the one presented stops working and a new one is
// returned. Presenting a rotated-away token again revokes the whole session,
// on the assumption that the token leaked, so a client must persist the new
// token before it uses it and must never refresh from two places at once.
//
// Sessions is safe for concurrent use.
type Sessions struct {
	store *Store
	cfg   SessionConfig
	now   func() time.Time

	mu      sync.RWMutex
	signKID string
	signKey ed25519.PrivateKey
	public  map[string]ed25519.PublicKey
}

// NewSessions loads the signing key from the store, generating and persisting
// one when there is none yet. It fails when cfg.Issuer is empty.
func NewSessions(ctx context.Context, store *Store, cfg SessionConfig) (*Sessions, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("auth: session issuer is required")
	}
	if cfg.AccessTTL == 0 {
		cfg.AccessTTL = DefaultAccessTTL
	}
	if cfg.RefreshTTL == 0 {
		cfg.RefreshTTL = DefaultRefreshTTL
	}
	s := &Sessions{store: store, cfg: cfg, now: cfg.Clock, public: map[string]ed25519.PublicKey{}}
	if s.now == nil {
		s.now = time.Now
	}
	if err := s.loadKeys(ctx); err != nil {
		return nil, err
	}
	if s.signKey == nil {
		if err := s.createKey(ctx); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// loadKeys reads every signing key. The newest signs; all of them verify, so
// a token signed by another node's freshly created key, or by a key rotated
// out later, still verifies until it expires.
func (s *Sessions) loadKeys(ctx context.Context) error {
	r, err := s.store.b.query(ctx, `SELECT id, private_key FROM auth_signing_keys ORDER BY created_at, id`)
	if err != nil {
		return fmt.Errorf("auth: load signing keys: %w", err)
	}
	defer r.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for r.Next() {
		var kid, encoded string
		if err := r.Scan(&kid, &encoded); err != nil {
			return fmt.Errorf("auth: load signing keys: %w", err)
		}
		seed, err := base64.RawStdEncoding.DecodeString(encoded)
		if err != nil || len(seed) != ed25519.SeedSize {
			return fmt.Errorf("auth: signing key %q is malformed", kid)
		}
		key := ed25519.NewKeyFromSeed(seed)
		s.public[kid] = key.Public().(ed25519.PublicKey)
		s.signKID, s.signKey = kid, key
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("auth: load signing keys: %w", err)
	}
	return nil
}

// createKey generates a key and persists it. Two nodes booting into an empty
// database at once may both do this; the result is two keys, both of which
// every node verifies, which is harmless.
func (s *Sessions) createKey(ctx context.Context) error {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return fmt.Errorf("auth: generate signing key: %w", err)
	}
	kid := newID()
	if _, err := s.store.b.exec(ctx, `INSERT INTO auth_signing_keys (id, private_key, created_at) VALUES (?, ?, ?)`,
		kid, base64.RawStdEncoding.EncodeToString(seed), s.now().UnixMilli()); err != nil {
		return fmt.Errorf("auth: store signing key: %w", err)
	}
	key := ed25519.NewKeyFromSeed(seed)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.public[kid] = key.Public().(ed25519.PublicKey)
	s.signKID, s.signKey = kid, key
	return nil
}

// publicKey finds a verification key, reloading from the store once when the
// id is unknown: another node may have created it since this one booted.
func (s *Sessions) publicKey(ctx context.Context, kid string) (ed25519.PublicKey, bool) {
	s.mu.RLock()
	pub, ok := s.public[kid]
	s.mu.RUnlock()
	if ok {
		return pub, true
	}
	if err := s.loadKeys(ctx); err != nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	pub, ok = s.public[kid]
	return pub, ok
}

// refresh tokens are "<session id>.<secret>": the id finds the row, the
// secret's hash is compared to the stored one.
func newSecret() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("auth: generate refresh token: %w", err)
	}
	return b64.EncodeToString(raw), nil
}

func hashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func splitRefreshToken(token string) (sid, secret string, ok bool) {
	sid, secret, ok = strings.Cut(token, ".")
	return sid, secret, ok && sid != "" && secret != ""
}

// Start opens a session for a user whose credentials the caller has already
// verified, and returns its first token pair.
func (s *Sessions) Start(ctx context.Context, u User) (Tokens, error) {
	secret, err := newSecret()
	if err != nil {
		return Tokens{}, err
	}
	now := s.now()
	sid := newID()
	expires := now.Add(s.cfg.RefreshTTL)
	if _, err := s.store.b.exec(ctx, `INSERT INTO auth_sessions (id, user_id, token_hash, created_at, last_used_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		sid, u.ID, hashSecret(secret), now.UnixMilli(), now.UnixMilli(), expires.UnixMilli()); err != nil {
		if s.store.b.classify(err) == constraintForeignKey {
			return Tokens{}, ErrNotFound
		}
		return Tokens{}, fmt.Errorf("auth: start session: %w", err)
	}
	return s.mint(u, sid, secret, now, expires)
}

// Refresh rotates a session's refresh token and returns a new token pair. The
// token presented stops working. It fails with ErrInvalidSession when the
// token names no live session — including when it was already rotated away,
// which also revokes the session — and with ErrDisabled when the user has
// been disabled since, which revokes the session too.
func (s *Sessions) Refresh(ctx context.Context, refreshToken string) (Tokens, error) {
	sid, secret, ok := splitRefreshToken(refreshToken)
	if !ok {
		return Tokens{}, ErrInvalidSession
	}
	next, err := newSecret()
	if err != nil {
		return Tokens{}, err
	}
	now := s.now()
	expires := now.Add(s.cfg.RefreshTTL)
	// One conditional UPDATE is the whole rotation: it succeeds for exactly
	// one presenter of the current secret, without a row lock either engine
	// would have to spell differently.
	n, err := s.store.b.exec(ctx, `UPDATE auth_sessions SET token_hash = ?, last_used_at = ?, expires_at = ?
		WHERE id = ? AND token_hash = ? AND revoked_at IS NULL AND expires_at > ?`,
		hashSecret(next), now.UnixMilli(), expires.UnixMilli(),
		sid, hashSecret(secret), now.UnixMilli())
	if err != nil {
		return Tokens{}, fmt.Errorf("auth: refresh session: %w", err)
	}
	if n == 0 {
		// A live session whose secret did not match has been presented a
		// token that was rotated away: someone holds an old copy. Revoke
		// it; the legitimate holder signs in again.
		if _, err := s.store.b.exec(ctx, `UPDATE auth_sessions SET revoked_at = ?
			WHERE id = ? AND revoked_at IS NULL AND expires_at > ?`,
			now.UnixMilli(), sid, now.UnixMilli()); err != nil {
			return Tokens{}, fmt.Errorf("auth: revoke reused session: %w", err)
		}
		return Tokens{}, ErrInvalidSession
	}
	var userID string
	if err := queryOne(ctx, s.store.b, `SELECT user_id FROM auth_sessions WHERE id = ?`, []any{sid}, &userID); err != nil {
		return Tokens{}, fmt.Errorf("auth: refresh session: %w", err)
	}
	u, err := s.store.User(ctx, userID)
	if err != nil {
		return Tokens{}, fmt.Errorf("auth: refresh session: %w", err)
	}
	if u.Disabled {
		if err := s.RevokeSession(ctx, sid); err != nil && !errors.Is(err, ErrNotFound) {
			return Tokens{}, err
		}
		return Tokens{}, ErrDisabled
	}
	return s.mint(u, sid, next, now, expires)
}

func (s *Sessions) mint(u User, sid, secret string, now, refreshExpires time.Time) (Tokens, error) {
	accessExpires := now.Add(s.cfg.AccessTTL)
	s.mu.RLock()
	kid, key := s.signKID, s.signKey
	s.mu.RUnlock()
	access, err := signJWT(kid, key, Claims{
		Issuer:    s.cfg.Issuer,
		Subject:   u.Subject(),
		IssuedAt:  now.Unix(),
		ExpiresAt: accessExpires.Unix(),
		Session:   sid,
		Email:     u.Email,
		Name:      u.Name,
		Admin:     u.Admin,
	})
	if err != nil {
		return Tokens{}, fmt.Errorf("auth: sign access token: %w", err)
	}
	return Tokens{
		AccessToken:      access,
		AccessExpiresAt:  accessExpires,
		RefreshToken:     sid + "." + secret,
		RefreshExpiresAt: refreshExpires,
	}, nil
}

// Revoke ends the session a refresh token belongs to: sign-out. A token that
// names no live session is ErrInvalidSession. The session's outstanding
// access tokens stay valid until they expire.
func (s *Sessions) Revoke(ctx context.Context, refreshToken string) error {
	sid, secret, ok := splitRefreshToken(refreshToken)
	if !ok {
		return ErrInvalidSession
	}
	n, err := s.store.b.exec(ctx, `UPDATE auth_sessions SET revoked_at = ?
		WHERE id = ? AND token_hash = ? AND revoked_at IS NULL`,
		s.now().UnixMilli(), sid, hashSecret(secret))
	if err != nil {
		return fmt.Errorf("auth: revoke session: %w", err)
	}
	if n == 0 {
		return ErrInvalidSession
	}
	return nil
}

// RevokeSession ends a session by id, for an administrator or a user
// managing their own devices. It fails with ErrNotFound when no live session
// has the id.
func (s *Sessions) RevokeSession(ctx context.Context, sessionID string) error {
	n, err := s.store.b.exec(ctx, `UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL`,
		s.now().UnixMilli(), sessionID)
	if err != nil {
		return fmt.Errorf("auth: revoke session: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// RevokeAll ends every live session of a user. A user with none is not an
// error.
func (s *Sessions) RevokeAll(ctx context.Context, userID string) error {
	if _, err := s.store.b.exec(ctx, `UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL`,
		s.now().UnixMilli(), userID); err != nil {
		return fmt.Errorf("auth: revoke sessions: %w", err)
	}
	return nil
}

// ListSessions returns a user's live sessions, newest first.
func (s *Sessions) ListSessions(ctx context.Context, userID string) ([]Session, error) {
	r, err := s.store.b.query(ctx, `SELECT id, user_id, created_at, last_used_at, expires_at FROM auth_sessions
		WHERE user_id = ? AND revoked_at IS NULL AND expires_at > ? ORDER BY created_at DESC, id DESC`,
		userID, s.now().UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("auth: list sessions: %w", err)
	}
	defer r.Close()
	var out []Session
	for r.Next() {
		var sess Session
		var created, used, expires int64
		if err := r.Scan(&sess.ID, &sess.UserID, &created, &used, &expires); err != nil {
			return nil, fmt.Errorf("auth: list sessions: %w", err)
		}
		sess.CreatedAt, sess.LastUsedAt, sess.ExpiresAt = fromMillis(created), fromMillis(used), fromMillis(expires)
		out = append(out, sess)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("auth: list sessions: %w", err)
	}
	return out, nil
}

// PruneSessions deletes sessions that expired or were revoked before the
// given time and returns how many. Dead sessions serve no purpose once every
// access token they issued has expired, so a host runs this periodically
// with a cutoff at least AccessTTL in the past.
func (s *Sessions) PruneSessions(ctx context.Context, before time.Time) (int64, error) {
	cutoff := before.UnixMilli()
	n, err := s.store.b.exec(ctx, `DELETE FROM auth_sessions WHERE expires_at < ? OR revoked_at < ?`, cutoff, cutoff)
	if err != nil {
		return 0, fmt.Errorf("auth: prune sessions: %w", err)
	}
	return n, nil
}

// Verify checks an access token and returns its claims. It fails with
// ErrInvalidToken for anything that does not verify: a bad signature, an
// unknown key, another issuer, or an expired token. It reads the database
// only when the token names a signing key this process has not seen.
func (s *Sessions) Verify(ctx context.Context, accessToken string) (Claims, error) {
	t, err := parseJWT(accessToken)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	pub, ok := s.publicKey(ctx, t.kid)
	if !ok {
		return Claims{}, fmt.Errorf("%w: unknown signing key", ErrInvalidToken)
	}
	if !verifyJWT(t, pub) {
		return Claims{}, fmt.Errorf("%w: bad signature", ErrInvalidToken)
	}
	if t.claims.Issuer != s.cfg.Issuer {
		return Claims{}, fmt.Errorf("%w: issuer mismatch", ErrInvalidToken)
	}
	if !s.now().Before(time.Unix(t.claims.ExpiresAt, 0)) {
		return Claims{}, fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	if !strings.HasPrefix(t.claims.Subject, SubjectPrefix) {
		return Claims{}, fmt.Errorf("%w: foreign subject", ErrInvalidToken)
	}
	return t.claims, nil
}

// Authenticate implements server.Authenticator. It reads the bearer token
// from the Authorization header and returns the principal it proves: Subject
// and Issuer from the token, and the claims "email", "name", "admin" (bool)
// and "sid" for policies to read. A request with no bearer token, or with one
// that does not verify, fails with server.ErrUnauthenticated, so the host
// fails closed.
func (s *Sessions) Authenticate(ctx context.Context, header http.Header) (server.Principal, error) {
	token, ok := strings.CutPrefix(header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return server.Principal{}, server.ErrUnauthenticated
	}
	claims, err := s.Verify(ctx, token)
	if err != nil {
		return server.Principal{}, fmt.Errorf("%w: %w", server.ErrUnauthenticated, err)
	}
	return server.Principal{
		Subject: claims.Subject,
		Issuer:  claims.Issuer,
		Claims: map[string]any{
			"email": claims.Email,
			"name":  claims.Name,
			"admin": claims.Admin,
			"sid":   claims.Session,
		},
	}, nil
}
