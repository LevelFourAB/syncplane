package server_test

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// A pull that reads a cursor-state entry while its read transaction holds a
// connection needs two connections of the reader pool at once. Enough such
// pulls at the same time hold every connection and wait for one more, until
// their deadlines end them. With one reader connection, the first such read
// waits forever, so these specs fail on the deadline instead of by chance.
var _ = Describe("Access model sync on a store with one reader connection", func() {
	var f *accessFixture

	BeforeEach(func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"),
			sqlitestore.WithReadConnections(1))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		f = newAccessFixtureOn(store)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		DeferCleanup(cancel)
		f.ctx = ctx
		f.seedOrg()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("pulls again under the access the cursor names", func() {
		guest := newAccessReplica(f, "guest")
		guest.sync()
		f.seed(&accessv1.Task{Id: "t1", Project: project("p1"), Title: "edited"})
		guest.sync()
		Expect(guest.resets).To(BeZero())
	})

	It("resyncs the rows below a grant the caller gains", func() {
		guest := newAccessReplica(f, "guest")
		guest.sync()
		f.seed(&accessv1.ProjectMember{Id: "pm-guest2", Project: project("p2"), UserId: "guest", Role: "project_reader"})
		guest.sync()
		Expect(guest.view()).To(ContainElement("task/t2"))
		Expect(guest.resets).To(BeZero())
	})

	It("resumes a snapshot under the access its cursor names", func() {
		// bootstrap runs a Bootstrap as guest from resume, and returns the
		// Begin frame and the resume token of the first chunk.
		bootstrap := func(resume *syncplanev1.BootstrapResume) (*syncplanev1.BootstrapResponse_Begin, []byte) {
			GinkgoHelper()
			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-guest", Resume: resume})
			as("guest", req.Header())
			stream, err := f.sync.Bootstrap(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(stream.Close()).To(Succeed()) }()
			var begin *syncplanev1.BootstrapResponse_Begin
			for stream.Receive() {
				switch body := stream.Msg().GetBody().(type) {
				case *syncplanev1.BootstrapResponse_Begin_:
					begin = body.Begin
				case *syncplanev1.BootstrapResponse_Chunk_:
					return begin, body.Chunk.GetResumeToken()
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			return begin, nil
		}

		first, token := bootstrap(nil)
		Expect(token).NotTo(BeEmpty())
		second, _ := bootstrap(&syncplanev1.BootstrapResume{Cursor: first.GetCursor(), Token: token})
		Expect(second.GetResumed()).To(BeTrue())
	})

	It("sends a change of caller access as a delta", func() {
		// Grants at eight more projects, so the summary is larger than the
		// delta and the server sends the delta.
		for i := range 8 {
			id := fmt.Sprintf("pw%d", i)
			f.seed(
				&accessv1.Project{Id: id},
				&accessv1.ProjectMember{Id: "pm-writer-" + id, Project: project(id), UserId: "writer", Role: "project_reader"},
			)
		}
		writer := newAccessReplica(f, "writer")
		writer.sync()
		f.seed(&accessv1.ProjectMember{Id: "pm-writer2", Project: project("p2"), UserId: "writer", Role: "project_admin"})
		Expect(writer.syncDelta()).NotTo(BeNil())
	})
})
