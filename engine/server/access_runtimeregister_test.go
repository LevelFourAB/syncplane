package server_test

import (
	"context"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// RegisterEntity accepts an entity that leaves the model incomplete, because
// a later call can complete it. An entity that breaks a rule no later
// RegisterEntity can repair is a different case: a server that serves a
// valid schema must refuse it, or every request fails from then on while the
// call that caused it reported success.
var _ = Describe("Runtime registration of one entity into a served access model", func() {
	ctx := context.Background()

	// serving reports how the server answers a pull by a caller that holds
	// no grant: the gate refuses it with PermissionDenied while the model is
	// valid, and every request fails with FailedPrecondition while it is not.
	serving := func(srv *server.Server) connect.Code {
		GinkgoHelper()
		_, err := srv.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		Expect(err).To(HaveOccurred())
		return connect.CodeOf(err)
	}

	It("refuses an entity that declares a role another entity declares, and keeps serving", func() {
		// Given a server that serves the org schema, where org_member declares
		// the org_admin role
		srv := newBareServer()
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(orgSpecs()...))
		Expect(err).NotTo(HaveOccurred())
		Expect(serving(srv)).To(Equal(connect.CodePermissionDenied))

		// When RegisterDynamicEntity adds a guest grant entity that declares
		// org_admin as well, which no later RegisterEntity can undo
		guest := orgSpecs()[0]
		guest.Name = "guest"
		guest.Grant.Roles = []*syncplanev1.Role{{Name: "org_admin", Grants: []string{"view_tasks"}}}
		err = srv.RegisterDynamicEntity(guest)

		// Then the call fails, and the server keeps serving the org schema
		served := serving(srv)
		Expect(err).To(MatchError(ContainSubstring(`role "org_admin" is declared by both`)), "served after: %v", served)
		Expect(served).To(Equal(connect.CodePermissionDenied))
	})

	It("refuses an entity without access under WithStrictAccess, and keeps serving", func() {
		// Given a server built with WithStrictAccess that serves the org schema
		srv := newBareServer(server.WithStrictAccess())
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(orgSpecs()...))
		Expect(err).NotTo(HaveOccurred())
		Expect(serving(srv)).To(Equal(connect.CodePermissionDenied))

		// When RegisterDynamicEntity adds an entity that declares no access
		err = srv.RegisterDynamicEntity(plainSpec("label"))

		// Then the call fails, and the server keeps serving the org schema
		served := serving(srv)
		Expect(err).To(MatchError(ContainSubstring(`entity "label" declares no access option`)), "served after: %v", served)
		Expect(served).To(Equal(connect.CodePermissionDenied))
	})
})
