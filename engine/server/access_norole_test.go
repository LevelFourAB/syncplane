package server_test

import (
	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// Note and Setting have only rules that admit every admitted caller, so a
// schema of the two declares no role, and no caller can hold a permission.
var _ = Describe("An access model that declares no role", func() {
	var f *accessFixture

	BeforeEach(func() {
		gate := &gatePerms{verdict: func(p server.Principal, _ string) error {
			if p.Subject == "mallory" {
				return server.DenyWorkspace("not a member")
			}
			return nil
		}}
		f = newSchemaFixtureOn(newTestStore(), server.WithEntities(&accessv1.Note{}, &accessv1.Setting{}),
			server.WithPermissions(gate))
		f.seed(&accessv1.Note{Id: "n1", Body: "seeded"})
	})

	It("admits every caller the workspace hook admits", func() {
		Expect(f.pull("ann")).To(ConsistOf("note/n1"))

		r := f.push("ann", "c1", putMutation("c1", 1, "note", "n2", &accessv1.Note{Id: "n2", Body: "posted"}))[0]
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
		Expect(f.pull("bob")).To(ConsistOf("note/n1", "note/n2"))
	})

	It("refuses a caller the workspace hook refuses", func() {
		_, err := f.pullErr("mallory")

		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		Expect(err.(*connect.Error).Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceAccessRevoked))
	})

	It("admits an in-process caller the workspace hook admits", func() {
		ctx := server.WithPrincipal(f.ctx, server.Principal{Subject: "ann"})

		Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.Note{Id: "n2", Body: "posted"})
		})).To(Succeed())
	})

	It("holds a rule with a subject check to the caller's own rows", func() {
		r := f.push("ann", "c1", putMutation("c1", 1, "setting", "ann", &accessv1.Setting{UserId: "ann", Theme: "dark"}))[0]
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())

		r = f.push("ann", "c2", putMutation("c2", 1, "setting", "bob", &accessv1.Setting{UserId: "bob", Theme: "dark"}))[0]
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodePermissionDenied))
		Expect(f.pull("bob")).To(ConsistOf("note/n1"))
	})
})
