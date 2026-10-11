package server_test

import (
	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A grant can change while a long read runs. The snapshot ships rows under the
// grants it began with, and the pull from its Begin cursor must leave the
// client with what the grants after the change show.
var _ = Describe("Access model bootstrap across a revoked grant", func() {
	It("removes the rows the snapshot shipped below a grant revoked after the snapshot began", func() {
		// Given guest reads p1 and p2 through two project grants
		hooked := &hookedReadStore{Adapter: newTestStore(), entity: "task"}
		f := newAccessFixtureOn(hooked)
		f.seedOrg()
		f.seed(&accessv1.ProjectMember{Id: "pm-guest-p2", Project: project("p2"), UserId: "guest", Role: "project_reader"})

		// And the p1 grant is revoked after the snapshot begins, before it
		// reads a page
		hooked.mu.Lock()
		hooked.sawBegin, hooked.sawEntity = false, false
		hooked.afterBegin = func() {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&accessv1.ProjectMember{}, "pm-guest")
			})).To(Succeed())
		}
		hooked.mu.Unlock()

		// When guest bootstraps, and pulls from the Begin cursor
		req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-guest"})
		as("guest", req.Header())
		stream, err := f.sync.Bootstrap(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		guest := newAccessReplica(f, "guest")
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				guest.cursor = body.Begin.GetCursor()
				guest.access = body.Begin.GetCallerAccess()
			case *syncplanev1.BootstrapResponse_Chunk_:
				for _, rc := range body.Chunk.GetRows() {
					guest.rows[rc.GetEntity()+"/"+rc.GetKey()] = true
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		hooked.mu.Lock()
		Expect(hooked.afterBegin).To(BeNil(), "the grant was not revoked during the snapshot")
		hooked.mu.Unlock()
		guest.sync()

		// Then guest holds what a fresh pull shows it: p2 and t2, and nothing
		// of p1
		Expect(guest.view()).NotTo(ContainElements("task/t1", "project/p1"))
		Expect(guest.view()).To(Equal(f.pull("guest")))
	})
})
