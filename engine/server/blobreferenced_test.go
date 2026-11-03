package server_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("ReferencedBlobs", func() {
	const ws = "ws-referenced"
	var (
		ctx context.Context
		srv *server.Server
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv = mustNewServer(newTestStore(), blobEntities(), server.WithBlobStore(memstore.New()))
		DeferCleanup(srv.Close)
	})

	It("reports the digests live rows name, in every entity, with no access rule", func() {
		_, top := blobOf(1, 10)
		_, hidden := blobOf(2, 10)
		_, tree := blobOf(3, 10)
		_, gone := blobOf(4, 10)
		_, never := blobOf(5, 10)
		putRows(ctx, srv, ws,
			&testdatav1.BlobHolder{Id: "a", Photo: refTo(top)},
			// A row no caller reads still counts.
			&testdatav1.BlobHolder{Id: "b", Hidden: true, Secret: refTo(hidden)},
			&testdatav1.BlobTree{Id: "t", Root: &testdatav1.BlobNode{Icon: refTo(tree)}},
			&testdatav1.BlobHolder{Id: "c", Photo: refTo(gone)},
		)
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Delete(&testdatav1.BlobHolder{}, "c")
		})).To(Succeed())

		got, err := srv.ReferencedBlobs(ctx, ws, []blob.Digest{top.Digest, hidden.Digest, tree.Digest, gone.Digest, never.Digest}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[blob.Digest]bool{top.Digest: true, hidden.Digest: true, tree.Digest: true}))
	})

	It("counts only the entities the filter accepts", func() {
		_, top := blobOf(1, 10)
		_, tree := blobOf(3, 10)
		putRows(ctx, srv, ws,
			&testdatav1.BlobHolder{Id: "a", Photo: refTo(top)},
			&testdatav1.BlobTree{Id: "t", Root: &testdatav1.BlobNode{Icon: refTo(tree)}},
		)
		got, err := srv.ReferencedBlobs(ctx, ws, []blob.Digest{top.Digest, tree.Digest}, func(name string) bool { return name == "blob_tree" })
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[blob.Digest]bool{tree.Digest: true}))
	})

	It("answers an empty request with an empty map and refuses a bad workspace id", func() {
		got, err := srv.ReferencedBlobs(ctx, ws, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeEmpty())
		_, err = srv.ReferencedBlobs(ctx, "", []blob.Digest{"sp1:00"}, nil)
		Expect(err).To(HaveOccurred())
	})
})
