package server_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// testdataSetWithout returns a self-contained descriptor set of the testdata
// file with field removed from message, and with no other entity declared
// than message and the blob messages it nests.
func testdataSetWithout(message, field string) []byte {
	GinkgoHelper()
	fd := testdatav1.File_syncplane_testdata_v1_testdata_proto
	var files []*descriptorpb.FileDescriptorProto
	seen := map[string]bool{}
	var walk func(f protoreflect.FileDescriptor)
	walk = func(f protoreflect.FileDescriptor) {
		if seen[f.Path()] {
			return
		}
		seen[f.Path()] = true
		for i := range f.Imports().Len() {
			walk(f.Imports().Get(i).FileDescriptor)
		}
		files = append(files, protodesc.ToFileDescriptorProto(f))
	}
	walk(fd)
	for _, f := range files {
		for _, m := range f.MessageType {
			if m.GetName() != message {
				if f.GetName() == fd.Path() && m.GetName() != "BlobAlbum" && m.GetName() != "BlobPage" {
					m.Options = nil
				}
				continue
			}
			kept := m.Field[:0]
			for _, fl := range m.Field {
				if fl.GetName() != field {
					kept = append(kept, fl)
				}
			}
			m.Field = kept
		}
	}
	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: files})
	Expect(err).NotTo(HaveOccurred())
	return raw
}

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

	// Blob GC keeps a blob by this answer, so a row that still names a blob
	// must count however the row was last written. A node on a schema that
	// does not declare the field keeps the stored reference through its PUT.
	It("reports a digest a row still names after a node without the field rewrote it", func() {
		store := newTestStore()
		blobs := memstore.New()
		newer := mustNewServer(store, blobEntities(), server.WithBlobStore(blobs))
		DeferCleanup(newer.Close)
		older := mustNewServer(store, server.WithBlobStore(blobs))
		DeferCleanup(older.Close)
		_, err := older.RegisterDescriptorSet(testdataSetWithout("BlobHolder", "photo"))
		Expect(err).NotTo(HaveOccurred())

		// Given a row the newer node wrote, naming photo
		_, photo := blobOf(1, 10)
		_, file := blobOf(2, 10)
		putRows(ctx, newer, ws, &testdatav1.BlobHolder{Id: "a", Title: "t1", Photo: refTo(photo)})
		Expect(newer.BuildIndexes(ctx, ws)).To(Succeed())

		// When the older node PUTs the row without photo
		putRows(ctx, older, ws, &testdatav1.BlobHolder{Id: "a", Title: "t2", Files: []*syncplanev1.BlobRef{refTo(file)}})
		var row testdatav1.BlobHolder
		Expect(newer.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			_, _, err := tx.Get(&row, "a")
			return err
		})).To(Succeed())
		Expect(row.GetPhoto().GetDigest()).To(Equal(string(photo.Digest)), "the stored row still names photo")

		// Then the newer node still counts photo as referenced
		got, err := newer.ReferencedBlobs(ctx, ws, []blob.Digest{photo.Digest}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(map[blob.Digest]bool{photo.Digest: true}))
	})

	It("answers an empty request with an empty map and refuses a bad workspace id", func() {
		got, err := srv.ReferencedBlobs(ctx, ws, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeEmpty())
		_, err = srv.ReferencedBlobs(ctx, "", []blob.Digest{"sp1:00"}, nil)
		Expect(err).To(HaveOccurred())
	})
})
