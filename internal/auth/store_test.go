package auth_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "modernc.org/sqlite"

	"github.com/levelfourab/syncplane/internal/auth"
)

func ptr[T any](v T) *T { return &v }

func describeStore(name string, open func() *auth.Store) bool {
	return Describe("Store ("+name+")", func() {
		var (
			ctx   context.Context
			store *auth.Store
		)

		BeforeEach(func() {
			ctx = context.Background()
			store = open()
			DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		})

		// create is the spec's shorthand for a user with a password.
		create := func(email string) auth.User {
			GinkgoHelper()
			u, err := store.CreateUser(ctx, auth.NewUser{Email: email, Name: "Someone", Password: "correct horse"})
			Expect(err).NotTo(HaveOccurred())
			return u
		}

		Describe("users", func() {
			It("creates a user with a normalized e-mail and a derived subject", func() {
				u, err := store.CreateUser(ctx, auth.NewUser{Email: "  Ada@Example.COM ", Name: "Ada", Password: "correct horse", Admin: true})
				Expect(err).NotTo(HaveOccurred())
				Expect(u.ID).NotTo(BeEmpty())
				Expect(u.Email).To(Equal("ada@example.com"))
				Expect(u.Name).To(Equal("Ada"))
				Expect(u.Admin).To(BeTrue())
				Expect(u.Disabled).To(BeFalse())
				Expect(u.Subject()).To(Equal(auth.SubjectPrefix + u.ID))
				Expect(u.CreatedAt).To(BeTemporally("~", time.Now(), 5*time.Second))

				got, err := store.User(ctx, u.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(u))

				byEmail, err := store.UserByEmail(ctx, "ADA@example.com")
				Expect(err).NotTo(HaveOccurred())
				Expect(byEmail).To(Equal(u))

				bySubject, err := store.UserBySubject(ctx, u.Subject())
				Expect(err).NotTo(HaveOccurred())
				Expect(bySubject).To(Equal(u))
			})

			It("refuses a second user with the same e-mail in any case", func() {
				create("ada@example.com")
				_, err := store.CreateUser(ctx, auth.NewUser{Email: "Ada@Example.com", Password: "correct horse"})
				Expect(err).To(MatchError(auth.ErrEmailTaken))
			})

			It("refuses malformed e-mail addresses", func() {
				for _, email := range []string{"", "ada", "@example.com", "ada@", "ada @example.com"} {
					_, err := store.CreateUser(ctx, auth.NewUser{Email: email, Password: "correct horse"})
					Expect(err).To(MatchError(auth.ErrInvalidEmail), "email %q", email)
				}
			})

			It("refuses a short password but allows none at all", func() {
				_, err := store.CreateUser(ctx, auth.NewUser{Email: "a@example.com", Password: "short"})
				Expect(err).To(MatchError(auth.ErrWeakPassword))

				u, err := store.CreateUser(ctx, auth.NewUser{Email: "a@example.com"})
				Expect(err).NotTo(HaveOccurred())
				// Without a password there is nothing to sign in with.
				_, err = store.VerifyPassword(ctx, u.Email, "")
				Expect(err).To(MatchError(auth.ErrInvalidCredentials))
			})

			It("reports not found for unknown ids, e-mails and subjects", func() {
				_, err := store.User(ctx, "nope")
				Expect(err).To(MatchError(auth.ErrNotFound))
				_, err = store.UserByEmail(ctx, "nobody@example.com")
				Expect(err).To(MatchError(auth.ErrNotFound))
				_, err = store.UserByEmail(ctx, "not an address")
				Expect(err).To(MatchError(auth.ErrNotFound))
				_, err = store.UserBySubject(ctx, "other:123")
				Expect(err).To(MatchError(auth.ErrNotFound))
				_, err = store.UserBySubject(ctx, auth.SubjectPrefix)
				Expect(err).To(MatchError(auth.ErrNotFound))
			})

			It("lists users by e-mail", func() {
				create("carol@example.com")
				create("alice@example.com")
				create("bob@example.com")
				users, err := store.ListUsers(ctx)
				Expect(err).NotTo(HaveOccurred())
				var emails []string
				for _, u := range users {
					emails = append(emails, u.Email)
				}
				Expect(emails).To(Equal([]string{"alice@example.com", "bob@example.com", "carol@example.com"}))
			})

			It("updates only the fields named", func() {
				u := create("ada@example.com")
				got, err := store.UpdateUser(ctx, u.ID, auth.UserUpdate{Disabled: ptr(true)})
				Expect(err).NotTo(HaveOccurred())
				Expect(got.Disabled).To(BeTrue())
				Expect(got.Name).To(Equal("Someone"))
				Expect(got.Admin).To(BeFalse())

				got, err = store.UpdateUser(ctx, u.ID, auth.UserUpdate{Name: ptr("Ada"), Admin: ptr(true)})
				Expect(err).NotTo(HaveOccurred())
				Expect(got.Name).To(Equal("Ada"))
				Expect(got.Admin).To(BeTrue())
				Expect(got.Disabled).To(BeTrue(), "an unnamed field keeps its value")

				stored, err := store.User(ctx, u.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(stored).To(Equal(got))

				_, err = store.UpdateUser(ctx, "nope", auth.UserUpdate{Name: ptr("x")})
				Expect(err).To(MatchError(auth.ErrNotFound))
			})

			It("deletes a user together with memberships and identities", func() {
				u := create("ada@example.com")
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", u.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(store.LinkIdentity(ctx, "https://idp.example", "sub-1", u.ID)).To(Succeed())

				Expect(store.DeleteUser(ctx, u.ID)).To(Succeed())
				Expect(store.DeleteUser(ctx, u.ID)).To(MatchError(auth.ErrNotFound))

				_, err = store.User(ctx, u.ID)
				Expect(err).To(MatchError(auth.ErrNotFound))
				members, err := store.ListMembers(ctx, "todo", "team")
				Expect(err).NotTo(HaveOccurred())
				Expect(members).To(BeEmpty())
				_, err = store.UserByIdentity(ctx, "https://idp.example", "sub-1")
				Expect(err).To(MatchError(auth.ErrNotFound))
				// The workspace itself outlives its last member.
				_, err = store.Workspace(ctx, "todo", "team")
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Describe("passwords", func() {
			It("verifies the right password and refuses the rest", func() {
				u := create("ada@example.com")

				got, err := store.VerifyPassword(ctx, "ADA@example.com", "correct horse")
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(u))

				_, err = store.VerifyPassword(ctx, u.Email, "wrong horse")
				Expect(err).To(MatchError(auth.ErrInvalidCredentials))
				_, err = store.VerifyPassword(ctx, "nobody@example.com", "correct horse")
				Expect(err).To(MatchError(auth.ErrInvalidCredentials))
				_, err = store.VerifyPassword(ctx, "not an address", "correct horse")
				Expect(err).To(MatchError(auth.ErrInvalidCredentials))
			})

			It("replaces a password", func() {
				u := create("ada@example.com")
				Expect(store.SetPassword(ctx, u.ID, "short")).To(MatchError(auth.ErrWeakPassword))
				Expect(store.SetPassword(ctx, u.ID, "battery staple")).To(Succeed())

				_, err := store.VerifyPassword(ctx, u.Email, "correct horse")
				Expect(err).To(MatchError(auth.ErrInvalidCredentials))
				_, err = store.VerifyPassword(ctx, u.Email, "battery staple")
				Expect(err).NotTo(HaveOccurred())

				Expect(store.SetPassword(ctx, "nope", "battery staple")).To(MatchError(auth.ErrNotFound))
			})

			It("gives a password to a user created without one", func() {
				u, err := store.CreateUser(ctx, auth.NewUser{Email: "ada@example.com"})
				Expect(err).NotTo(HaveOccurred())
				Expect(store.SetPassword(ctx, u.ID, "battery staple")).To(Succeed())
				_, err = store.VerifyPassword(ctx, u.Email, "battery staple")
				Expect(err).NotTo(HaveOccurred())
			})

			It("tells a disabled user apart only after the password is right", func() {
				u := create("ada@example.com")
				_, err := store.UpdateUser(ctx, u.ID, auth.UserUpdate{Disabled: ptr(true)})
				Expect(err).NotTo(HaveOccurred())

				_, err = store.VerifyPassword(ctx, u.Email, "correct horse")
				Expect(err).To(MatchError(auth.ErrDisabled))
				_, err = store.VerifyPassword(ctx, u.Email, "wrong horse")
				Expect(err).To(MatchError(auth.ErrInvalidCredentials))
			})
		})

		Describe("workspaces", func() {
			It("creates a workspace with its owner as first member", func() {
				owner := create("ada@example.com")
				w, err := store.CreateWorkspace(ctx, "todo", "team", "Team", owner.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(w.AppID).To(Equal("todo"))
				Expect(w.ID).To(Equal("team"))
				Expect(w.Name).To(Equal("Team"))

				got, err := store.Workspace(ctx, "todo", "team")
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(w))

				m, err := store.Member(ctx, "todo", "team", owner.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(m.Role).To(Equal(auth.RoleOwner))
			})

			It("creates a workspace without members when no owner is given", func() {
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", "")
				Expect(err).NotTo(HaveOccurred())
				members, err := store.ListMembers(ctx, "todo", "team")
				Expect(err).NotTo(HaveOccurred())
				Expect(members).To(BeEmpty())
			})

			It("rolls the workspace back when the owner does not exist", func() {
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", "nope")
				Expect(err).To(MatchError(auth.ErrNotFound))
				_, err = store.Workspace(ctx, "todo", "team")
				Expect(err).To(MatchError(auth.ErrNotFound))
			})

			It("refuses a duplicate id within an app and allows it across apps", func() {
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", "")
				Expect(err).NotTo(HaveOccurred())
				_, err = store.CreateWorkspace(ctx, "todo", "team", "", "")
				Expect(err).To(MatchError(auth.ErrWorkspaceExists))
				_, err = store.CreateWorkspace(ctx, "crm", "team", "", "")
				Expect(err).NotTo(HaveOccurred())
			})

			It("refuses ids the sync RPCs would refuse", func() {
				for _, id := range []string{"", "-x", "a/b", "a b", "*"} {
					_, err := store.CreateWorkspace(ctx, "todo", id, "", "")
					Expect(err).To(MatchError(auth.ErrInvalidWorkspace), "id %q", id)
				}
				_, err := store.CreateWorkspace(ctx, "", "team", "", "")
				Expect(err).To(MatchError(auth.ErrInvalidWorkspace))
			})

			It("keeps member lists apart per app", func() {
				u := create("ada@example.com")
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", u.ID)
				Expect(err).NotTo(HaveOccurred())
				_, err = store.CreateWorkspace(ctx, "crm", "team", "", "")
				Expect(err).NotTo(HaveOccurred())

				_, err = store.Member(ctx, "crm", "team", u.ID)
				Expect(err).To(MatchError(auth.ErrNotFound))
			})

			It("lists and renames an app's workspaces", func() {
				for _, id := range []string{"b", "a"} {
					_, err := store.CreateWorkspace(ctx, "todo", id, "", "")
					Expect(err).NotTo(HaveOccurred())
				}
				_, err := store.CreateWorkspace(ctx, "crm", "c", "", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(store.RenameWorkspace(ctx, "todo", "a", "Alpha")).To(Succeed())
				Expect(store.RenameWorkspace(ctx, "todo", "zzz", "x")).To(MatchError(auth.ErrNotFound))

				list, err := store.ListWorkspaces(ctx, "todo")
				Expect(err).NotTo(HaveOccurred())
				Expect(list).To(HaveLen(2))
				Expect(list[0].ID).To(Equal("a"))
				Expect(list[0].Name).To(Equal("Alpha"))
				Expect(list[1].ID).To(Equal("b"))
			})

			It("deletes a workspace with its member list", func() {
				u := create("ada@example.com")
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", u.ID)
				Expect(err).NotTo(HaveOccurred())

				Expect(store.DeleteWorkspace(ctx, "todo", "team")).To(Succeed())
				Expect(store.DeleteWorkspace(ctx, "todo", "team")).To(MatchError(auth.ErrNotFound))
				_, err = store.Member(ctx, "todo", "team", u.ID)
				Expect(err).To(MatchError(auth.ErrNotFound))
				memberships, err := store.ListMemberships(ctx, u.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(memberships).To(BeEmpty())
			})
		})

		Describe("members", func() {
			var owner, other auth.User

			BeforeEach(func() {
				owner = create("owner@example.com")
				other = create("other@example.com")
				_, err := store.CreateWorkspace(ctx, "todo", "team", "", owner.ID)
				Expect(err).NotTo(HaveOccurred())
			})

			It("adds a member and changes their role in place", func() {
				Expect(store.SetMember(ctx, "todo", "team", other.ID, auth.RoleReader)).To(Succeed())
				first, err := store.Member(ctx, "todo", "team", other.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(first.Role).To(Equal(auth.RoleReader))

				Expect(store.SetMember(ctx, "todo", "team", other.ID, auth.RoleWriter)).To(Succeed())
				second, err := store.Member(ctx, "todo", "team", other.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(second.Role).To(Equal(auth.RoleWriter))
				Expect(second.AddedAt).To(Equal(first.AddedAt), "a role change is not a new membership")

				members, err := store.ListMembers(ctx, "todo", "team")
				Expect(err).NotTo(HaveOccurred())
				Expect(members).To(HaveLen(2))
			})

			It("refuses roles it does not know", func() {
				Expect(store.SetMember(ctx, "todo", "team", other.ID, auth.Role("god"))).To(MatchError(auth.ErrInvalidRole))
				Expect(store.SetMember(ctx, "todo", "team", other.ID, "")).To(MatchError(auth.ErrInvalidRole))
			})

			It("reports not found for a missing workspace or user", func() {
				Expect(store.SetMember(ctx, "todo", "nope", other.ID, auth.RoleReader)).To(MatchError(auth.ErrNotFound))
				Expect(store.SetMember(ctx, "todo", "team", "nope", auth.RoleReader)).To(MatchError(auth.ErrNotFound))
				_, err := store.Member(ctx, "todo", "nope", owner.ID)
				Expect(err).To(MatchError(auth.ErrNotFound))
			})

			It("removes a member", func() {
				Expect(store.RemoveMember(ctx, "todo", "team", owner.ID)).To(Succeed())
				Expect(store.RemoveMember(ctx, "todo", "team", owner.ID)).To(MatchError(auth.ErrNotFound))
				_, err := store.Member(ctx, "todo", "team", owner.ID)
				Expect(err).To(MatchError(auth.ErrNotFound))
			})

			It("lists a user's memberships across apps", func() {
				_, err := store.CreateWorkspace(ctx, "crm", "accounts", "", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(store.SetMember(ctx, "crm", "accounts", owner.ID, auth.RoleReader)).To(Succeed())

				memberships, err := store.ListMemberships(ctx, owner.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(memberships).To(HaveLen(2))
				Expect(memberships[0].AppID).To(Equal("crm"))
				Expect(memberships[0].WorkspaceID).To(Equal("accounts"))
				Expect(memberships[0].Role).To(Equal(auth.RoleReader))
				Expect(memberships[1].AppID).To(Equal("todo"))
				Expect(memberships[1].Role).To(Equal(auth.RoleOwner))

				none, err := store.ListMemberships(ctx, other.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(none).To(BeEmpty())
			})
		})

		Describe("identities", func() {
			const issuer = "https://idp.example"

			It("links an identity and finds the user through it", func() {
				u := create("ada@example.com")
				Expect(store.LinkIdentity(ctx, issuer, "sub-1", u.ID)).To(Succeed())
				got, err := store.UserByIdentity(ctx, issuer, "sub-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(u))

				ids, err := store.ListIdentities(ctx, u.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(ids).To(HaveLen(1))
				Expect(ids[0].Issuer).To(Equal(issuer))
				Expect(ids[0].Subject).To(Equal("sub-1"))
				Expect(ids[0].UserID).To(Equal(u.ID))
			})

			It("treats relinking to the same user as a no-op and to another as a conflict", func() {
				a := create("a@example.com")
				b := create("b@example.com")
				Expect(store.LinkIdentity(ctx, issuer, "sub-1", a.ID)).To(Succeed())
				Expect(store.LinkIdentity(ctx, issuer, "sub-1", a.ID)).To(Succeed())
				Expect(store.LinkIdentity(ctx, issuer, "sub-1", b.ID)).To(MatchError(auth.ErrIdentityLinked))
				// The same subject at another issuer is another identity.
				Expect(store.LinkIdentity(ctx, "https://other.example", "sub-1", b.ID)).To(Succeed())
			})

			It("refuses empty identities and unknown users", func() {
				u := create("ada@example.com")
				Expect(store.LinkIdentity(ctx, "", "sub-1", u.ID)).To(MatchError(auth.ErrInvalidIdentity))
				Expect(store.LinkIdentity(ctx, issuer, "", u.ID)).To(MatchError(auth.ErrInvalidIdentity))
				Expect(store.LinkIdentity(ctx, issuer, "sub-1", "nope")).To(MatchError(auth.ErrNotFound))
			})

			It("unlinks an identity", func() {
				u := create("ada@example.com")
				Expect(store.LinkIdentity(ctx, issuer, "sub-1", u.ID)).To(Succeed())
				Expect(store.UnlinkIdentity(ctx, issuer, "sub-1")).To(Succeed())
				Expect(store.UnlinkIdentity(ctx, issuer, "sub-1")).To(MatchError(auth.ErrNotFound))
				_, err := store.UserByIdentity(ctx, issuer, "sub-1")
				Expect(err).To(MatchError(auth.ErrNotFound))
			})
		})
	})
}

var _ = Describe("Store (sqlite) lifecycle", func() {
	It("keeps its data across reopen and applies the migrations once", func() {
		ctx := context.Background()
		path := filepath.Join(GinkgoT().TempDir(), "auth.db")

		store, err := auth.OpenSQLite(path)
		Expect(err).NotTo(HaveOccurred())
		u, err := store.CreateUser(ctx, auth.NewUser{Email: "ada@example.com", Password: "correct horse"})
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Close()).To(Succeed())

		store, err = auth.OpenSQLite(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		got, err := store.User(ctx, u.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(u))

		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
		var applied int
		Expect(db.QueryRow(`SELECT count(*) FROM auth_migrations`).Scan(&applied)).To(Succeed())
		Expect(applied).To(Equal(1))
	})

	It("refuses a database written by a newer binary", func() {
		path := filepath.Join(GinkgoT().TempDir(), "auth.db")
		store, err := auth.OpenSQLite(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Close()).To(Succeed())

		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(`INSERT INTO auth_migrations (version, applied_at) VALUES (999, 0)`)
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Close()).To(Succeed())

		_, err = auth.OpenSQLite(path)
		Expect(err).To(MatchError(ContainSubstring("newer than this binary")))
	})
})
