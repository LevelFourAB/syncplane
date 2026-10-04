package server_test

import (
	"slices"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Widget declares no access. Beside the org schema it is outside the model:
// the workspace gate still decides who enters, but no rule and no anchor
// applies to its rows.
var _ = Describe("An entity outside the access model", func() {
	var f *accessFixture

	BeforeEach(func() {
		f = newAccessFixture(server.WithEntities(&testdatav1.Widget{}))
		f.seedOrg()
		f.seed(&testdatav1.Widget{Id: "w1", Label: "shared"})
	})

	It("shows every row to a caller below the root", func() {
		rows := f.pull("guest")
		Expect(rows).To(ContainElements("widget/w1", "task/t1"))
		Expect(rows).NotTo(ContainElement("task/t2"))
	})

	It("shows every row to a role at the root", func() {
		Expect(f.pull("member")).To(ContainElement("widget/w1"))
	})

	It("still refuses a caller who holds no permission", func() {
		_, err := f.pullErr("stranger")
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		Expect(err.(*connect.Error).Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceAccessRevoked))
	})

	It("answers the Query RPC with every row for a caller below the root", func() {
		req := connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: ws, Entity: "widget"})
		as("guest", req.Header())
		resp, err := f.sync.Query(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Msg.GetRows()).To(HaveLen(1))
		Expect(resp.Msg.GetRows()[0].GetKey()).To(Equal("w1"))
	})

	It("lets a caller below the root create, change and delete its rows", func() {
		results := f.push("guest", "c1",
			putMutation("c1", 1, "widget", "w2", &testdatav1.Widget{Id: "w2", Label: "new"}),
			patchMutation("c1", 2, "widget", "w1", &testdatav1.Widget{Label: "renamed"}, "label"),
			deleteMutation("c1", 3, "widget", "w1"),
		)
		for _, r := range results {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
		}
		Expect(f.pull("guest")).To(ContainElement("widget/w2"))
		Expect(f.pull("guest")).NotTo(ContainElement("widget/w1"))
	})
})

// A cursor records every entity's read rule, an entity outside the model
// included, so an entity that joins or leaves the model resets the clients
// that hold its rows, and an entity added outside the model does not.
var _ = Describe("Access model sync across an entity joining or leaving the model", func() {
	var store storage.Adapter

	// withoutLabelAccess edits the access file so Label declares no access,
	// and no role grants the permission its rule used.
	withoutLabelAccess := func(file *descriptorpb.FileDescriptorProto) {
		label := messageNamed(file, "Label")
		eo := proto.GetExtension(label.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
		eo.Access = nil
		for _, m := range file.GetMessageType() {
			eo := proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			for _, role := range eo.GetGrant().GetRoles() {
				role.Grants = slices.DeleteFunc(role.Grants, func(p string) bool { return p == "view_labels" })
			}
		}
	}

	// running returns a server on store whose schema is the access file after
	// change has edited it.
	running := func(change func(*descriptorpb.FileDescriptorProto)) *accessFixture {
		GinkgoHelper()
		f := newSchemaFixtureOn(store, server.WithEntities())
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(change), server.AllowAccessChange())
		Expect(err).NotTo(HaveOccurred())
		return f
	}

	synced := func(f *accessFixture) *accessReplica {
		GinkgoHelper()
		r := newAccessReplica(f, "guest")
		r.sync()
		return r
	}

	movedTo := func(r *accessReplica, f *accessFixture) *accessReplica {
		moved := newAccessReplica(f, r.user)
		moved.cursor = r.cursor
		return moved
	}

	BeforeEach(func() {
		store = newTestStore()
	})

	// seedOrg writes the org through the compiled schema, whose Go types
	// EntityTx.Put takes; the fixtures under test read the same store.
	seedOrg := func() {
		GinkgoHelper()
		newAccessFixtureOn(store).seedOrg()
	}

	It("resets a cursor when an entity it knows joins the model", func() {
		seedOrg()
		before := running(withoutLabelAccess)
		guest := synced(before)
		Expect(guest.view()).To(ContainElement("label/l1"))

		after := running(nil)
		Expect(movedTo(guest, after).pullOnce(0).GetReset_()).To(BeTrue())
	})

	It("resets a cursor when an entity it knows leaves the model", func() {
		seedOrg()
		before := running(nil)
		guest := synced(before)
		Expect(guest.view()).NotTo(ContainElement("label/l1"))

		after := running(withoutLabelAccess)
		Expect(movedTo(guest, after).pullOnce(0).GetReset_()).To(BeTrue())
	})

	It("keeps a cursor across an entity added outside the model", func() {
		before := newAccessFixtureOn(store)
		before.seedOrg()
		guest := synced(before)

		after := newAccessFixtureOn(store, server.WithEntities(&testdatav1.Widget{}))
		after.seed(&testdatav1.Widget{Id: "w1"})
		moved := movedTo(guest, after)
		moved.rows = guest.rows
		moved.sync()
		Expect(moved.view()).To(ContainElement("widget/w1"))
		Expect(moved.view()).To(Equal(after.pull("guest")))
	})

	It("registers the compiled org schema beside an entity outside the model", func() {
		_, err := server.New(store, server.WithEntities(
			&accessv1.OrgMember{}, &accessv1.Project{}, &accessv1.ProjectMember{},
			&accessv1.Task{}, &accessv1.Label{}, &accessv1.Note{}, &testdatav1.Widget{},
		))
		Expect(err).NotTo(HaveOccurred())
	})
})
