package server_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// A registration upserts the entities of its descriptor set and keeps the
// others, so what an app serves is every set it registered. A restart must
// serve the same access model, or a caller that the model refused before the
// restart enters the workspace after it.
var _ = Describe("Access model of a hosted app across a restart", func() {
	ctx := context.Background()

	pull := func(m *server.Manager, appID string) error {
		GinkgoHelper()
		ts := httptest.NewServer(m.SyncHandler())
		DeferCleanup(ts.Close)
		cl := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL+"/"+appID)
		_, err := cl.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws}))
		return err
	}

	It("keeps refusing a caller without a grant after a restart that follows a registration of some entities", func() {
		path := filepath.Join(GinkgoT().TempDir(), "restart.db")
		note := server.DynamicEntity{
			Name: "note", KeyFields: []string{"id"},
			Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
		}

		// Given an app whose schema declares the org_admin role, so the gate
		// refuses an anonymous caller that holds no grant
		store1, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		m1, err := server.NewManager(ctx, store1)
		Expect(err).NotTo(HaveOccurred())
		_, err = m1.Register(ctx, "app1", descriptorSetBytes(orgSpecs()...))
		Expect(err).NotTo(HaveOccurred())
		Expect(connect.CodeOf(pull(m1, "app1"))).To(Equal(connect.CodePermissionDenied))

		// And a later registration that adds the note entity alone, which the
		// app serves beside the org entities
		_, err = m1.Register(ctx, "app1", descriptorSetBytes(note))
		Expect(err).NotTo(HaveOccurred())
		Expect(connect.CodeOf(pull(m1, "app1"))).To(Equal(connect.CodePermissionDenied))
		Expect(m1.Close()).To(Succeed())
		Expect(store1.Close()).To(Succeed())

		// When the host restarts over the same store
		store2, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store2.Close()).To(Succeed()) })
		m2, err := server.NewManager(ctx, store2)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(m2.Close()).To(Succeed()) })

		// Then the gate still refuses the caller, as it did before the restart
		err = pull(m2, "app1")
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied), "%v", err)
	})

	It("reports a set registered again as unchanged, before and after a restart", func() {
		path := filepath.Join(GinkgoT().TempDir(), "again.db")
		note := server.DynamicEntity{
			Name: "note", KeyFields: []string{"id"},
			Fields: []server.DynamicField{{Name: "id", Number: 1, Kind: protoreflect.StringKind}},
		}

		// Given an app that registered the org schema, and then note alone
		store1, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		m1, err := server.NewManager(ctx, store1)
		Expect(err).NotTo(HaveOccurred())
		_, err = m1.Register(ctx, "app1", descriptorSetBytes(orgSpecs()...))
		Expect(err).NotTo(HaveOccurred())
		first, err := m1.Register(ctx, "app1", descriptorSetBytes(note))
		Expect(err).NotTo(HaveOccurred())
		Expect(first.Status).To(Equal(server.RegisterUpdated))
		Expect(first.Entities).To(Equal([]string{"note"}))

		// When the app registers note again
		again, err := m1.Register(ctx, "app1", descriptorSetBytes(note))

		// Then nothing changes
		Expect(err).NotTo(HaveOccurred())
		Expect(again.Status).To(Equal(server.RegisterUnchanged))
		Expect(again.Revision).To(Equal(first.Revision))
		Expect(m1.Close()).To(Succeed())
		Expect(store1.Close()).To(Succeed())

		// And after a restart, the app serves the same revision, and the same
		// set registers as unchanged
		store2, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store2.Close()).To(Succeed()) })
		m2, err := server.NewManager(ctx, store2)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(m2.Close()).To(Succeed()) })
		Expect(m2.App("app1").SchemaRevision()).To(Equal(first.Revision))
		after, err := m2.Register(ctx, "app1", descriptorSetBytes(note))
		Expect(err).NotTo(HaveOccurred())
		Expect(after.Status).To(Equal(server.RegisterUnchanged))
	})
})
