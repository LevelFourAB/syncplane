package server_test

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/server"
)

// What the WithCallerAccess hook returns decides which grant rows apply to a
// caller. Its errors are failures to decide, and its subjects are opaque
// strings that only an exact match on a grant row's subject may admit.
var _ = Describe("The WithCallerAccess hook", func() {
	It("fails a request with code internal when the hook returns a workspace denial", func() {
		// Given a hook whose identity lookup fails with an error that wraps a
		// workspace denial
		f := newSchemaFixtureOn(newTestStore(), teamEntities(),
			server.WithCallerAccess(func(context.Context, server.Principal, string) (server.CallerAccess, error) {
				return server.CallerAccess{}, errors.Join(errors.New("identity cache miss"), server.DenyWorkspace("unknown user"))
			}))
		f.seedSquads()

		// When dana pulls
		_, err := f.pullErr("dana")

		// Then the request fails as internal, which a client retries, and not
		// as a denial, which a client reads as a revocation and drops its data
		Expect(err).To(HaveOccurred())
		Expect(connect.CodeOf(err)).To(Equal(connect.CodeInternal),
			"the hook's error reached the client as %v", err)
	})

	Describe("subjects", func() {
		var f *accessFixture

		BeforeEach(func() {
			// Given the team schema, where "design" is both a squad key and a
			// subject of a user, and grant rows name subjects that start with
			// "eve" and go on past a separator
			f = newSchemaFixtureOn(newTestStore(), teamEntities(), subjectsBy(func(p server.Principal) []string {
				switch p.Subject {
				case "eve-nul":
					return []string{"eve\u0000r3"}
				default:
					return []string{p.Subject}
				}
			}))
			f.seedSquads()
			f.seed(
				repoUser("rm-design-user", "r2", "design", "repo_reader"),
				repoUser("rm-eve", "r2", "eve", "repo_reader"),
				repoUser("rm-eve-slash", "r1", "eve/r1", "repo_reader"),
				repoUser("rm-eve-nul", "r3", "eve\u0000r3", "repo_reader"),
				repoUser("rm-eve-colon", "r3", "eve:r3", "repo_reader"),
			)
		})

		It("gives a user whose subject equals a squad key only the user's own grants", func() {
			// When the user "design" pulls
			rows := f.pull("design")

			// Then it reads r2, which its own row grants, and nothing that
			// squad design holds
			Expect(rows).To(ContainElement("issue/i2"))
			Expect(rows).NotTo(ContainElements("issue/i1", "issue/i3"))
		})

		It("matches a subject exactly, whatever separator follows it on a grant row", func() {
			// When eve pulls
			rows := f.pull("eve")

			// Then eve reads r2, and none of the repos granted to subjects
			// that begin with "eve"
			Expect(rows).To(ContainElement("issue/i2"))
			Expect(rows).NotTo(ContainElements("issue/i1", "issue/i3"))

			// And the caller whose subject holds a NUL reads its own repo only
			Expect(f.pull("eve-nul")).To(SatisfyAll(ContainElement("issue/i3"), Not(ContainElements("issue/i1", "issue/i2"))))
		})
	})
})
