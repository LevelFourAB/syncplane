package server_test

import (
	"context"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A caller whose last grant is revoked holds no permission, so the workspace
// gate refuses it. These specs pin what reaches its client on each read path.
var _ = Describe("Access model sync when the last grant goes", func() {
	revokeGuest := func(f *accessFixture) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&accessv1.ProjectMember{}, "pm-guest")
		})).To(Succeed())
	}

	It("tombstones the rows an open stream sent before it drops the scope with NO_ACCESS", func() {
		// Given guest, whose only grant is pm-guest at p1, holds task t1
		// and has a stream open whose heartbeat does not come in this spec
		f := newAccessFixture(server.WithHeartbeat(time.Hour))
		f.seedOrg()
		guest := newAccessReplica(f, "guest")
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t1"))
		req := connect.NewRequest(&syncplanev1.SubscribeRequest{
			ClientId: "c-guest",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws, Cursor: guest.cursor}},
		})
		as("guest", req.Header())
		ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
		defer cancel()
		stream, err := f.sync.Subscribe(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		for stream.Receive() && !stream.Msg().GetCaughtUp() {
		}

		// When pm-guest is deleted
		revokeGuest(f)

		// Then the stream sends a tombstone for t1
		gone := false
		for !gone && stream.Receive() {
			Expect(stream.Msg().GetDropped()).To(BeNil(), "the stream dropped the scope first")
			for _, rc := range stream.Msg().GetPage().GetPatch().GetChanges() {
				if rc.GetEntity() == "task" && rc.GetKey() == "t1" && rc.GetTombstone() {
					gone = true
				}
			}
		}
		Expect(gone).To(BeTrue())
	})

	It("tombstones the rows a pulling client holds when its last grant is revoked, as a stream does", func() {
		// Given guest, whose only grant is pm-guest at p1, holds task t1
		f := newAccessFixture()
		f.seedOrg()
		guest := newAccessReplica(f, "guest")
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t1"))

		// When pm-guest is deleted and guest pulls
		revokeGuest(f)
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest", Cursor: guest.cursor})
		as("guest", req.Header())
		resp, err := f.sync.Pull(f.ctx, req)

		// Then the pull tells the client to drop t1, which guest no longer
		// reads
		Expect(err).NotTo(HaveOccurred())
		var tombstoned []string
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetTombstone() {
				tombstoned = append(tombstoned, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
		Expect(tombstoned).To(ContainElement("task/t1"))
		// ... with a summary that holds nothing, and has_more, so the client
		// pulls again at once ...
		Expect(resp.Msg.GetCallerAccess()).NotTo(BeNil())
		Expect(resp.Msg.GetCallerAccess().GetAnchorGroups()).To(BeEmpty())
		Expect(resp.Msg.GetCallerAccess().GetRootPermissions()).To(BeEmpty())
		Expect(resp.Msg.GetHasMore()).To(BeTrue())

		// ... and that pull is refused as one of a caller that holds no
		// permission
		next := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest", Cursor: resp.Msg.GetNextCursor()})
		as("guest", next.Header())
		_, err = f.sync.Pull(f.ctx, next)
		Expect(err).To(HaveOccurred())
		Expect(err.(*connect.Error).Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceNoAccess))
	})

	It("tombstones the rows a pulling client holds when its last grant is revoked, where an application hook runs first", func() {
		// Given guest holds task t1, on a server whose application hook makes
		// the gate read the grants apart from the pull's own read
		f := newAccessFixture(server.WithPermissions(&recordingPolicy{}))
		f.seedOrg()
		guest := newAccessReplica(f, "guest")
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t1"))

		// When pm-guest is deleted and guest pulls
		revokeGuest(f)
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest", Cursor: guest.cursor})
		as("guest", req.Header())
		resp, err := f.sync.Pull(f.ctx, req)

		// Then the pull tombstones t1, and the next one is refused
		Expect(err).NotTo(HaveOccurred())
		var tombstoned []string
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetTombstone() {
				tombstoned = append(tombstoned, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
		Expect(tombstoned).To(ContainElement("task/t1"))
		next := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest", Cursor: resp.Msg.GetNextCursor()})
		as("guest", next.Header())
		_, err = f.sync.Pull(f.ctx, next)
		Expect(err).To(HaveOccurred())
		Expect(err.(*connect.Error).Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceNoAccess))
	})

	It("refuses a pull from a cursor issued while the caller held no permission, and sends nothing", func() {
		// Given guest, whose last grant was revoked, and a pull that sent the
		// revocation resync
		f := newAccessFixture()
		f.seedOrg()
		guest := newAccessReplica(f, "guest")
		guest.sync()
		revokeGuest(f)
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest", Cursor: guest.cursor})
		as("guest", req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// When rows change and guest pulls from the cursor that resync issued
		f.seed(&accessv1.Task{Id: "t-new", Project: project("p1"), Title: "new"})
		again := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest", Cursor: resp.Msg.GetNextCursor()})
		as("guest", again.Header())
		_, err = f.sync.Pull(f.ctx, again)

		// Then the pull is refused, as one that waits for a first grant
		Expect(err).To(HaveOccurred())
		Expect(err.(*connect.Error).Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceNoAccess))
	})
})
