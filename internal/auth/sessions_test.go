package auth_test

import (
	"context"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/auth"
)

const issuer = "https://sync.example"

// clock is a settable time source the specs move by hand.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func describeSessions(name string, open func() *auth.Store) bool {
	return Describe("Sessions ("+name+")", func() {
		var (
			ctx      context.Context
			store    *auth.Store
			sessions *auth.Sessions
			clk      *clock
			user     auth.User
		)

		newSessions := func(cfg auth.SessionConfig) *auth.Sessions {
			GinkgoHelper()
			if cfg.Issuer == "" {
				cfg.Issuer = issuer
			}
			cfg.Clock = clk.now
			s, err := auth.NewSessions(ctx, store, cfg)
			Expect(err).NotTo(HaveOccurred())
			return s
		}

		bearer := func(token string) http.Header {
			return http.Header{"Authorization": []string{"Bearer " + token}}
		}

		BeforeEach(func() {
			ctx = context.Background()
			store = open()
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
			clk = &clock{t: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)}
			sessions = newSessions(auth.SessionConfig{})
			var err error
			user, err = store.CreateUser(ctx, auth.NewUser{Email: "ada@example.com", Name: "Ada", Password: "correct horse", Admin: true})
			Expect(err).NotTo(HaveOccurred())
		})

		It("requires an issuer", func() {
			_, err := auth.NewSessions(ctx, store, auth.SessionConfig{})
			Expect(err).To(MatchError(ContainSubstring("issuer")))
		})

		It("starts a session and verifies the access token it minted", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			Expect(tokens.AccessExpiresAt).To(Equal(clk.t.Add(auth.DefaultAccessTTL)))
			Expect(tokens.RefreshExpiresAt).To(Equal(clk.t.Add(auth.DefaultRefreshTTL)))

			claims, err := sessions.Verify(ctx, tokens.AccessToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(claims.Issuer).To(Equal(issuer))
			Expect(claims.Subject).To(Equal(user.Subject()))
			Expect(claims.Email).To(Equal("ada@example.com"))
			Expect(claims.Name).To(Equal("Ada"))
			Expect(claims.Admin).To(BeTrue())
			Expect(claims.Session).NotTo(BeEmpty())
			Expect(claims.ExpiresAt).To(Equal(tokens.AccessExpiresAt.Unix()))

			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(HaveLen(1))
			Expect(live[0].ID).To(Equal(claims.Session))
			Expect(live[0].ExpiresAt).To(Equal(tokens.RefreshExpiresAt))
		})

		It("refuses to start a session for a user that does not exist", func() {
			_, err := sessions.Start(ctx, auth.User{ID: "nope"})
			Expect(err).To(MatchError(auth.ErrNotFound))
		})

		It("authenticates a bearer token into a principal and fails closed otherwise", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())

			p, err := sessions.Authenticate(ctx, bearer(tokens.AccessToken))
			Expect(err).NotTo(HaveOccurred())
			Expect(p.Subject).To(Equal(user.Subject()))
			Expect(p.Issuer).To(Equal(issuer))
			Expect(p.Claims).To(HaveKeyWithValue("email", "ada@example.com"))
			Expect(p.Claims).To(HaveKeyWithValue("admin", true))
			Expect(p.Claims).To(HaveKey("sid"))

			_, err = sessions.Authenticate(ctx, http.Header{})
			Expect(err).To(MatchError(server.ErrUnauthenticated))
			_, err = sessions.Authenticate(ctx, bearer("garbage"))
			Expect(err).To(MatchError(server.ErrUnauthenticated))
			_, err = sessions.Authenticate(ctx, http.Header{"Authorization": []string{"Basic abc"}})
			Expect(err).To(MatchError(server.ErrUnauthenticated))
		})

		It("expires the access token and not the session", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())

			clk.advance(auth.DefaultAccessTTL)
			_, err = sessions.Verify(ctx, tokens.AccessToken)
			Expect(err).To(MatchError(auth.ErrInvalidToken))

			renewed, err := sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).NotTo(HaveOccurred())
			_, err = sessions.Verify(ctx, renewed.AccessToken)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rotates the refresh token and slides the expiry", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())

			clk.advance(10 * 24 * time.Hour)
			renewed, err := sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(renewed.RefreshToken).NotTo(Equal(tokens.RefreshToken))
			Expect(renewed.RefreshExpiresAt).To(Equal(clk.t.Add(auth.DefaultRefreshTTL)))

			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(HaveLen(1), "a refresh continues the session rather than opening one")
			Expect(live[0].LastUsedAt).To(Equal(clk.t))
		})

		It("revokes the session when a rotated-away token is presented", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			renewed, err := sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).NotTo(HaveOccurred())

			_, err = sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
			// The legitimate holder is locked out too: the session is gone.
			_, err = sessions.Refresh(ctx, renewed.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(BeEmpty())
		})

		It("lets an idle session expire", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			clk.advance(auth.DefaultRefreshTTL)
			_, err = sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(BeEmpty())
		})

		It("refuses malformed and unknown refresh tokens without side effects", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			for _, bad := range []string{"", "nodot", ".secret", "sid.", "unknown.secret"} {
				_, err := sessions.Refresh(ctx, bad)
				Expect(err).To(MatchError(auth.ErrInvalidSession), "token %q", bad)
			}
			_, err = sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).NotTo(HaveOccurred(), "the real session is untouched")
		})

		It("carries user changes into the next access token", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			_, err = store.UpdateUser(ctx, user.ID, auth.UserUpdate{Admin: ptr(false), Name: ptr("Ada L.")})
			Expect(err).NotTo(HaveOccurred())

			renewed, err := sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).NotTo(HaveOccurred())
			claims, err := sessions.Verify(ctx, renewed.AccessToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(claims.Admin).To(BeFalse())
			Expect(claims.Name).To(Equal("Ada L."))
		})

		It("ends the session of a user who was disabled", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			_, err = store.UpdateUser(ctx, user.ID, auth.UserUpdate{Disabled: ptr(true)})
			Expect(err).NotTo(HaveOccurred())

			_, err = sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).To(MatchError(auth.ErrDisabled))
			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(BeEmpty())
		})

		It("loses the session when the user is deleted", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			Expect(store.DeleteUser(ctx, user.ID)).To(Succeed())
			_, err = sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
		})

		It("signs out with the refresh token", func() {
			tokens, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			Expect(sessions.Revoke(ctx, tokens.RefreshToken)).To(Succeed())
			Expect(sessions.Revoke(ctx, tokens.RefreshToken)).To(MatchError(auth.ErrInvalidSession))
			_, err = sessions.Refresh(ctx, tokens.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
			// Access tokens are not checked against the store, so the one
			// already issued lives out its short lifetime.
			_, err = sessions.Verify(ctx, tokens.AccessToken)
			Expect(err).NotTo(HaveOccurred())
		})

		It("revokes sessions by id and all at once", func() {
			first, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			second, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			third, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())

			firstClaims, err := sessions.Verify(ctx, first.AccessToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(sessions.RevokeSession(ctx, firstClaims.Session)).To(Succeed())
			Expect(sessions.RevokeSession(ctx, firstClaims.Session)).To(MatchError(auth.ErrNotFound))
			_, err = sessions.Refresh(ctx, first.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
			_, err = sessions.Refresh(ctx, second.RefreshToken)
			Expect(err).NotTo(HaveOccurred())

			Expect(sessions.RevokeAll(ctx, user.ID)).To(Succeed())
			_, err = sessions.Refresh(ctx, third.RefreshToken)
			Expect(err).To(MatchError(auth.ErrInvalidSession))
			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(BeEmpty())
			Expect(sessions.RevokeAll(ctx, user.ID)).To(Succeed(), "nothing left to revoke is fine")
		})

		It("prunes dead sessions and keeps live ones", func() {
			dead, err := sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			Expect(sessions.Revoke(ctx, dead.RefreshToken)).To(Succeed())
			clk.advance(time.Hour)
			_, err = sessions.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())

			n, err := sessions.PruneSessions(ctx, clk.t.Add(-auth.DefaultAccessTTL))
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))
			live, err := sessions.ListSessions(ctx, user.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(live).To(HaveLen(1))
		})

		It("verifies tokens signed by another process sharing the store", func() {
			// A second Sessions over the same store is a second node. It loads
			// the key the first one created and signs with it.
			other := newSessions(auth.SessionConfig{})
			tokens, err := other.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			_, err = sessions.Verify(ctx, tokens.AccessToken)
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses a token from another issuer, even one signed with the same key", func() {
			other := newSessions(auth.SessionConfig{Issuer: "https://elsewhere.example"})
			tokens, err := other.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			_, err = sessions.Verify(ctx, tokens.AccessToken)
			Expect(err).To(MatchError(auth.ErrInvalidToken))
		})

		It("honours configured lifetimes", func() {
			short := newSessions(auth.SessionConfig{AccessTTL: time.Minute, RefreshTTL: time.Hour})
			tokens, err := short.Start(ctx, user)
			Expect(err).NotTo(HaveOccurred())
			Expect(tokens.AccessExpiresAt).To(Equal(clk.t.Add(time.Minute)))
			Expect(tokens.RefreshExpiresAt).To(Equal(clk.t.Add(time.Hour)))
		})
	})
}
