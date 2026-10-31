package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// blobOf returns size bytes of a blob, distinct per seed, and their manifest.
func blobOf(seed byte, size int) ([]byte, blob.Manifest) {
	GinkgoHelper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i%251) ^ seed
	}
	m, err := blob.Compute(bytes.NewReader(data))
	Expect(err).NotTo(HaveOccurred())
	return data, m
}

// chunkOf returns chunk index of data.
func chunkOf(data []byte, index int) []byte {
	start := index * blob.ChunkSize
	return data[start:min(start+blob.ChunkSize, len(data))]
}

func beginUpload(ctx context.Context, srv *server.Server, ws string, m blob.Manifest) (*syncplanev1.BeginUploadResponse, error) {
	res, err := srv.BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
		WorkspaceId: ws, Digest: string(m.Digest), Size: m.Size, ChunkHashes: m.ChunkHashes,
	}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func putChunk(ctx context.Context, srv *server.Server, ws string, d blob.Digest, index int, data []byte) (syncplanev1.BlobState, error) {
	res, err := srv.PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
		WorkspaceId: ws, Digest: string(d), Index: uint32(index), Data: data,
	}))
	if err != nil {
		return 0, err
	}
	return res.Msg.GetState(), nil
}

// uploadBlob sends every chunk BeginUpload reports missing.
func uploadBlob(ctx context.Context, srv *server.Server, ws string, data []byte, m blob.Manifest) {
	GinkgoHelper()
	res, err := beginUpload(ctx, srv, ws, m)
	Expect(err).NotTo(HaveOccurred())
	for _, i := range res.GetMissingChunks() {
		_, err := putChunk(ctx, srv, ws, m.Digest, int(i), chunkOf(data, int(i)))
		Expect(err).NotTo(HaveOccurred())
	}
}

func statBlob(ctx context.Context, srv *server.Server, ws string, d blob.Digest) *syncplanev1.BlobStatus {
	GinkgoHelper()
	res, err := srv.StatBlobs(ctx, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws, Digests: []string{string(d)}}))
	Expect(err).NotTo(HaveOccurred())
	Expect(res.Msg.GetBlobs()).To(HaveLen(1))
	return res.Msg.GetBlobs()[0]
}

func getChunk(ctx context.Context, srv *server.Server, ws string, d blob.Digest, index int) ([]byte, error) {
	res, err := srv.GetChunk(ctx, connect.NewRequest(&syncplanev1.GetChunkRequest{WorkspaceId: ws, Digest: string(d), Index: uint32(index)}))
	if err != nil {
		return nil, err
	}
	return res.Msg.GetData(), nil
}

func refTo(m blob.Manifest) *syncplanev1.BlobRef {
	return &syncplanev1.BlobRef{Digest: string(m.Digest), Size: m.Size, MediaType: "application/octet-stream"}
}

func putRows(ctx context.Context, srv *server.Server, ws string, rows ...proto.Message) {
	GinkgoHelper()
	Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
		for _, r := range rows {
			if err := tx.Put(r); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
}

// writeTxCounter counts the write transactions the engine opens.
type writeTxCounter struct {
	storage.Adapter
	n atomic.Int64
}

func (c *writeTxCounter) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	c.n.Add(1)
	return c.Adapter.WithWriteTx(ctx, ws, fn)
}

// failingBlobStore fails every Stat with an error that names a file path.
type failingBlobStore struct{ *memstore.Store }

func (failingBlobStore) Stat(context.Context, string, blob.Digest) (blob.Info, error) {
	return blob.Info{}, errors.New("open /srv/private/blobs/ws/ab/cd: input/output error")
}

// uploadPolicy refuses uploads of more than 50 bytes by "bob", and fails to
// decide for "carol".
type uploadPolicy struct{ server.AllowAll }

func (uploadPolicy) CanUploadBlob(_ context.Context, p server.Principal, _ string, size int64) error {
	switch {
	case p.Subject == "bob" && size > 50:
		return server.Reject(server.CodePermissionDenied, "uploads above 50 bytes are not allowed")
	case p.Subject == "carol":
		return errors.New("quota service unreachable")
	}
	return nil
}

func blobEntities() server.Option {
	return server.WithEntities(&testdatav1.BlobHolder{}, &testdatav1.BlobTree{}, &testdatav1.BlobLedger{})
}

var _ = Describe("Blobs", func() {
	const ws = "ws-blobs"
	var (
		ctx   context.Context
		store *memstore.Store
		alice context.Context
		bob   context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		store = memstore.New()
		alice = server.WithPrincipal(ctx, server.Principal{Subject: "alice"})
		bob = server.WithPrincipal(ctx, server.Principal{Subject: "bob"})
	})

	newServer := func(opts ...server.Option) *server.Server {
		GinkgoHelper()
		srv := mustNewServer(newTestStore(),
			append([]server.Option{blobEntities(), server.WithBlobStore(store)}, opts...)...)
		DeferCleanup(srv.Close)
		return srv
	}

	Describe("without a blob store", func() {
		It("fails every call with unimplemented, also over HTTP", func() {
			srv := mustNewServer(newTestStore(), blobEntities())
			DeferCleanup(srv.Close)
			h := httptest.NewServer(srv.HTTPHandler())
			DeferCleanup(h.Close)
			client := syncplanev1connect.NewBlobServiceClient(h.Client(), h.URL)
			_, m := blobOf(1, 10)

			_, err := client.BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{WorkspaceId: ws, Digest: string(m.Digest), Size: m.Size, ChunkHashes: m.ChunkHashes}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnimplemented))
			_, err = client.PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{WorkspaceId: ws, Digest: string(m.Digest)}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnimplemented))
			_, err = client.StatBlobs(ctx, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnimplemented))
			_, err = client.GetChunk(ctx, connect.NewRequest(&syncplanev1.GetChunkRequest{WorkspaceId: ws, Digest: string(m.Digest)}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnimplemented))
		})
	})

	Describe("uploads", func() {
		var srv *server.Server
		BeforeEach(func() { srv = newServer() })

		It("stores a blob of several chunks and serves each one back", func() {
			data, m := blobOf(2, 2*blob.ChunkSize+1000)
			res, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_UPLOADING))
			Expect(res.GetMissingChunks()).To(Equal([]uint32{0, 1, 2}))

			for i, want := range []syncplanev1.BlobState{
				syncplanev1.BlobState_BLOB_STATE_UPLOADING,
				syncplanev1.BlobState_BLOB_STATE_UPLOADING,
				syncplanev1.BlobState_BLOB_STATE_COMPLETE,
			} {
				state, err := putChunk(alice, srv, ws, m.Digest, i, chunkOf(data, i))
				Expect(err).NotTo(HaveOccurred())
				Expect(state).To(Equal(want), "chunk %d", i)
			}

			putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Photo: refTo(m)})
			st := statBlob(alice, srv, ws, m.Digest)
			Expect(st.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(st.GetSize()).To(Equal(m.Size))
			Expect(st.GetChunkHashes()).To(Equal(m.ChunkHashes))
			for i := range 3 {
				got, err := getChunk(alice, srv, ws, m.Digest, i)
				Expect(err).NotTo(HaveOccurred())
				Expect(bytes.Equal(got, chunkOf(data, i))).To(BeTrue(), "chunk %d", i)
			}
			_, err = getChunk(alice, srv, ws, m.Digest, 3)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeOutOfRange))
		})

		It("resumes with the chunks that are still missing", func() {
			data, m := blobOf(3, 2*blob.ChunkSize+10)
			_, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			_, err = putChunk(alice, srv, ws, m.Digest, 1, chunkOf(data, 1))
			Expect(err).NotTo(HaveOccurred())

			res, err := beginUpload(bob, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_UPLOADING))
			Expect(res.GetMissingChunks()).To(Equal([]uint32{0, 2}))
		})

		It("answers a repeated BeginUpload and a repeated chunk the same way", func() {
			data, m := blobOf(4, 100)
			first, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			second, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(proto.Equal(first, second)).To(BeTrue())

			Expect(putChunk(alice, srv, ws, m.Digest, 0, data)).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(putChunk(alice, srv, ws, m.Digest, 0, data)).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			res, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(res.GetMissingChunks()).To(BeEmpty())
		})

		It("completes an empty blob at BeginUpload", func() {
			_, m := blobOf(5, 0)
			res, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(res.GetMissingChunks()).To(BeEmpty())
			_, err = store.Stat(ctx, ws, m.Digest)
			Expect(err).NotTo(HaveOccurred())
			putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Photo: refTo(m)})
			st := statBlob(alice, srv, ws, m.Digest)
			Expect(st.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(st.GetSize()).To(BeZero())
			Expect(st.GetChunkHashes()).To(BeEmpty())
		})

		It("completes at BeginUpload a staged upload that already holds every chunk", func() {
			data, m := blobOf(13, blob.ChunkSize+7)
			// What a crash between the last chunk and the commit leaves.
			Expect(store.Begin(ctx, ws, m)).To(Succeed())
			Expect(store.PutChunk(ctx, ws, m.Digest, 0, bytes.NewReader(chunkOf(data, 0)))).To(Succeed())
			Expect(store.PutChunk(ctx, ws, m.Digest, 1, bytes.NewReader(chunkOf(data, 1)))).To(Succeed())

			res, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(res.GetMissingChunks()).To(BeEmpty())
			_, err = store.Stat(ctx, ws, m.Digest)
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses PutChunk with failed_precondition once the staged upload is gone", func() {
			data, m := blobOf(14, blob.ChunkSize+7)
			_, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(store.Abort(ctx, ws, m.Digest)).To(Succeed())
			_, err = putChunk(alice, srv, ws, m.Digest, 0, chunkOf(data, 0))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeFailedPrecondition))

			res, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetMissingChunks()).To(Equal([]uint32{0, 1}))
		})

		It("answers PutChunk on a committed blob as for an unknown one, unless the chunk proves the bytes", func() {
			data, m := blobOf(15, blob.ChunkSize+7)
			uploadBlob(alice, srv, ws, data, m)
			_, unknown := blobOf(16, blob.ChunkSize+7)

			_, committedErr := putChunk(bob, srv, ws, m.Digest, 0, []byte("guess"))
			_, unknownErr := putChunk(bob, srv, ws, unknown.Digest, 0, []byte("guess"))
			Expect(connect.CodeOf(committedErr)).To(Equal(connect.CodeFailedPrecondition))
			var ce, ue *connect.Error
			Expect(errors.As(committedErr, &ce)).To(BeTrue())
			Expect(errors.As(unknownErr, &ue)).To(BeTrue())
			Expect(ce.Message()).To(Equal(strings.ReplaceAll(ue.Message(), string(unknown.Digest), string(m.Digest))))

			Expect(putChunk(bob, srv, ws, m.Digest, 1, chunkOf(data, 1))).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
		})

		It("opens no write transaction for BeginUpload on a committed blob without a mark", func() {
			counter := &writeTxCounter{Adapter: newTestStore()}
			counted := mustNewServer(counter, blobEntities(), server.WithBlobStore(store))
			DeferCleanup(counted.Close)
			data, m := blobOf(17, 10)
			uploadBlob(alice, counted, ws, data, m)

			before := counter.n.Load()
			res, err := beginUpload(bob, counted, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(counter.n.Load()).To(Equal(before))
		})

		It("does not tell a client what the blob store reported", func() {
			failing := mustNewServer(newTestStore(), blobEntities(), server.WithBlobStore(failingBlobStore{memstore.New()}))
			DeferCleanup(failing.Close)
			_, m := blobOf(18, 10)
			_, err := beginUpload(alice, failing, ws, m)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInternal))
			Expect(err.Error()).NotTo(ContainSubstring("/srv/private"))
		})

		It("asks the upload policy at BeginUpload", func() {
			gated := newServer(server.WithPermissions(uploadPolicy{}))
			_, small := blobOf(30, 50)
			data, big := blobOf(31, 51)

			_, err := beginUpload(bob, gated, ws, small)
			Expect(err).NotTo(HaveOccurred())
			_, err = beginUpload(bob, gated, ws, big)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
			var ce *connect.Error
			Expect(errors.As(err, &ce)).To(BeTrue())
			Expect(ce.Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodePermissionDenied))
			Expect(ce.Message()).To(Equal("uploads above 50 bytes are not allowed"))

			_, err = beginUpload(server.WithPrincipal(ctx, server.Principal{Subject: "carol"}), gated, ws, small)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInternal))

			// Also for a blob that is already stored, so a refused caller
			// learns nothing about what is.
			uploadBlob(alice, gated, ws, data, big)
			_, err = beginUpload(bob, gated, ws, big)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))

			_, err = beginUpload(server.Trusted(bob), gated, ws, big)
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses a wrong chunk with invalid_argument and stores nothing", func() {
			data, m := blobOf(6, blob.ChunkSize+10)
			_, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())

			wrong := bytes.Clone(chunkOf(data, 0))
			wrong[0] ^= 0xff
			_, err = putChunk(alice, srv, ws, m.Digest, 0, wrong)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			_, err = putChunk(alice, srv, ws, m.Digest, 1, chunkOf(data, 0)[:5])
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			_, err = putChunk(alice, srv, ws, m.Digest, 2, chunkOf(data, 1))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))

			_, held, err := store.Staged(ctx, ws, m.Digest)
			Expect(err).NotTo(HaveOccurred())
			Expect(held).To(BeEmpty())
		})

		It("refuses a malformed manifest with invalid_argument", func() {
			_, m := blobOf(7, 100)
			for name, req := range map[string]*syncplanev1.BeginUploadRequest{
				"bad digest":   {WorkspaceId: ws, Digest: "sp1:XYZ", Size: m.Size, ChunkHashes: m.ChunkHashes},
				"wrong count":  {WorkspaceId: ws, Digest: string(m.Digest), Size: blob.ChunkSize + 1, ChunkHashes: m.ChunkHashes},
				"wrong hashes": {WorkspaceId: ws, Digest: string(m.Digest), Size: m.Size, ChunkHashes: [][]byte{make([]byte, blob.HashSize)}},
				"negative":     {WorkspaceId: ws, Digest: string(m.Digest), Size: -1},
			} {
				_, err := srv.BeginUpload(alice, connect.NewRequest(req))
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument), name)
			}
		})

		It("refuses a blob above MaxBlobBytes with resource_exhausted", func() {
			small := newServer(server.WithLimits(server.Limits{MaxBlobBytes: 99}))
			_, m := blobOf(8, 100)
			_, err := beginUpload(alice, small, ws, m)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeResourceExhausted))
			_, m = blobOf(8, 99)
			_, err = beginUpload(alice, small, ws, m)
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses PutChunk with no upload started with failed_precondition", func() {
			data, m := blobOf(9, 100)
			_, err := putChunk(alice, srv, ws, m.Digest, 0, data)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeFailedPrecondition))
		})

		It("refuses StatBlobs above the batch limit and a malformed digest", func() {
			digests := make([]string, 257)
			for i := range digests {
				_, m := blobOf(byte(i), 1)
				digests[i] = string(m.Digest)
			}
			_, err := srv.StatBlobs(alice, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws, Digests: digests}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
			_, err = srv.StatBlobs(alice, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws, Digests: []string{"sha256:nope"}}))
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInvalidArgument))
		})

		It("reports a referenced blob that is part-way uploaded as uploading", func() {
			data, m := blobOf(10, blob.ChunkSize+5)
			putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Photo: refTo(m)})
			Expect(statBlob(alice, srv, ws, m.Digest).GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_MISSING))
			_, err := beginUpload(alice, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			_, err = putChunk(alice, srv, ws, m.Digest, 0, chunkOf(data, 0))
			Expect(err).NotTo(HaveOccurred())
			st := statBlob(alice, srv, ws, m.Digest)
			Expect(st.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_UPLOADING))
			Expect(st.GetSize()).To(Equal(m.Size))
			Expect(st.GetChunkHashes()).To(BeEmpty())
			_, err = getChunk(alice, srv, ws, m.Digest, 0)
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeNotFound))
		})

		It("pokes the workspace once, when the last chunk lands", func() {
			notifier := &countingNotifier{Local: pubsub.NewLocal()}
			poked := newServer(server.WithNotifier(notifier), server.WithNotifyCoalescing(0))
			data, m := blobOf(11, blob.ChunkSize+5)

			_, err := beginUpload(alice, poked, ws, m)
			Expect(err).NotTo(HaveOccurred())
			_, err = putChunk(alice, poked, ws, m.Digest, 0, chunkOf(data, 0))
			Expect(err).NotTo(HaveOccurred())
			Expect(notifier.notifies.Load()).To(BeZero())
			_, err = putChunk(alice, poked, ws, m.Digest, 1, chunkOf(data, 1))
			Expect(err).NotTo(HaveOccurred())
			Expect(notifier.notifies.Load()).To(BeEquivalentTo(1))
			// A repeated chunk of a complete blob commits nothing new.
			_, err = putChunk(alice, poked, ws, m.Digest, 1, chunkOf(data, 1))
			Expect(err).NotTo(HaveOccurred())
			Expect(notifier.notifies.Load()).To(BeEquivalentTo(1))
		})
	})

	// Run with indexes and without: an index only changes how the rows that
	// name a digest are found, never which rows they are.
	for _, mode := range []string{"indexes", "scans"} {
		Describe("read authorization with "+mode, func() {
			var srv *server.Server
			// One blob per place a BlobRef can sit.
			var (
				photoData, coverData, fileData, scanData, mapData, secretData, shelfData, iconData []byte
				photo, cover, file, scan, byName, secret, shelf, icon                              blob.Manifest
			)

			BeforeEach(func() {
				var opts []server.Option
				if mode == "scans" {
					opts = append(opts, server.WithLimits(server.Limits{DisableIndexes: true}))
				}
				srv = newServer(opts...)
				photoData, photo = blobOf(20, 30)
				coverData, cover = blobOf(21, 31)
				fileData, file = blobOf(22, 32)
				scanData, scan = blobOf(23, 33)
				mapData, byName = blobOf(24, 34)
				secretData, secret = blobOf(25, 35)
				shelfData, shelf = blobOf(26, 36)
				iconData, icon = blobOf(27, 37)
				for _, b := range []struct {
					data []byte
					m    blob.Manifest
				}{{photoData, photo}, {coverData, cover}, {fileData, file}, {scanData, scan}, {mapData, byName}, {secretData, secret}, {shelfData, shelf}, {iconData, icon}} {
					uploadBlob(bob, srv, ws, b.data, b.m)
				}
				_, other := blobOf(29, 3)
				putRows(ctx, srv, ws,
					&testdatav1.BlobHolder{
						Id:      "h1",
						OwnerId: "alice",
						Photo:   refTo(photo),
						Album:   &testdatav1.BlobAlbum{Name: "a", Cover: refTo(cover)},
						Files:   []*syncplanev1.BlobRef{refTo(other), refTo(file)},
						Pages:   []*testdatav1.BlobPage{{Id: "p1"}, {Id: "p2", Scan: refTo(scan)}},
						ByName:  map[string]*syncplanev1.BlobRef{"x": refTo(byName)},
						Secret:  refTo(secret),
					},
					&testdatav1.BlobTree{
						Id:      "t1",
						Shelves: []*testdatav1.BlobShelf{{Id: "s1", Files: []*syncplanev1.BlobRef{refTo(shelf)}}},
						Root: &testdatav1.BlobNode{Children: []*testdatav1.BlobNode{
							{Children: []*testdatav1.BlobNode{{Icon: refTo(icon)}}},
						}},
					},
				)
				if mode == "indexes" {
					Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
				}
			})

			readable := func(c context.Context, m blob.Manifest, data []byte) {
				GinkgoHelper()
				st := statBlob(c, srv, ws, m.Digest)
				Expect(st.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
				Expect(st.GetChunkHashes()).To(Equal(m.ChunkHashes))
				got, err := getChunk(c, srv, ws, m.Digest, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(data))
			}
			// unreadable checks that the answer is the one for a digest the
			// server does not have, word for word.
			unreadable := func(c context.Context, m blob.Manifest) {
				GinkgoHelper()
				_, absent := blobOf(99, 7)
				for _, d := range []blob.Digest{m.Digest, absent.Digest} {
					st := statBlob(c, srv, ws, d)
					Expect(proto.Equal(st, &syncplanev1.BlobStatus{Digest: string(d), State: syncplanev1.BlobState_BLOB_STATE_MISSING})).To(BeTrue(), "%v", st)
				}
				_, err := getChunk(c, srv, ws, m.Digest, 0)
				_, absentErr := getChunk(c, srv, ws, absent.Digest, 0)
				Expect(connect.CodeOf(err)).To(Equal(connect.CodeNotFound))
				Expect(connect.CodeOf(absentErr)).To(Equal(connect.CodeNotFound))
				var ce, ae *connect.Error
				Expect(errors.As(err, &ce)).To(BeTrue())
				Expect(errors.As(absentErr, &ae)).To(BeTrue())
				Expect(ce.Message()).To(Equal(fmt.Sprintf("blob %s is not found or not complete", m.Digest)))
				Expect(ae.Message()).To(Equal(fmt.Sprintf("blob %s is not found or not complete", absent.Digest)))
			}

			It("serves a blob named in each place a BlobRef can sit", func() {
				readable(bob, photo, photoData)
				readable(bob, cover, coverData)
				readable(bob, file, fileData)
				readable(bob, scan, scanData)
				readable(bob, byName, mapData)
				readable(bob, shelf, shelfData)
				readable(bob, icon, iconData)
			})

			It("answers several digests in request order", func() {
				_, absent := blobOf(98, 7)
				res, err := srv.StatBlobs(bob, connect.NewRequest(&syncplanev1.StatBlobsRequest{
					WorkspaceId: ws,
					Digests:     []string{string(icon.Digest), string(absent.Digest), string(photo.Digest), string(icon.Digest)},
				}))
				Expect(err).NotTo(HaveOccurred())
				want := []*syncplanev1.BlobStatus{
					{Digest: string(icon.Digest), State: syncplanev1.BlobState_BLOB_STATE_COMPLETE, Size: icon.Size, ChunkHashes: icon.ChunkHashes},
					{Digest: string(absent.Digest), State: syncplanev1.BlobState_BLOB_STATE_MISSING},
					{Digest: string(photo.Digest), State: syncplanev1.BlobState_BLOB_STATE_COMPLETE, Size: photo.Size, ChunkHashes: photo.ChunkHashes},
					{Digest: string(icon.Digest), State: syncplanev1.BlobState_BLOB_STATE_COMPLETE, Size: icon.Size, ChunkHashes: icon.ChunkHashes},
				}
				Expect(res.Msg.GetBlobs()).To(HaveLen(len(want)))
				for i, b := range res.Msg.GetBlobs() {
					Expect(proto.Equal(b, want[i])).To(BeTrue(), "entry %d: %v", i, b)
				}
			})

			It("hides a blob that only a field the caller may not read names", func() {
				readable(alice, secret, secretData)
				unreadable(bob, secret)
			})

			It("hides a blob that only a row the caller may not read names", func() {
				putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Hidden: true, Photo: refTo(photo)})
				unreadable(bob, photo)
				unreadable(alice, secret)
			})

			It("hides a blob that only a deleted row named", func() {
				Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
					return tx.Delete(&testdatav1.BlobHolder{}, "h1")
				})).To(Succeed())
				unreadable(bob, photo)
				unreadable(bob, scan)
			})

			It("hides a blob once its row stops naming it", func() {
				putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Photo: refTo(photo)})
				readable(bob, photo, photoData)
				unreadable(bob, cover)
				unreadable(bob, file)
			})

			It("reads every blob a live row names for a Trusted caller", func() {
				putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Hidden: true, Photo: refTo(photo), Secret: refTo(secret)})
				readable(server.Trusted(bob), photo, photoData)
				readable(server.Trusted(ctx), secret, secretData)
			})

			It("hides a blob that only a server-only row names, except from a Trusted caller", func() {
				putRows(ctx, srv, ws, &testdatav1.BlobLedger{Id: "l1", Receipt: refTo(photo)}, &testdatav1.BlobHolder{Id: "h1"})
				if mode == "indexes" {
					Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
				}
				unreadable(bob, photo)
				readable(server.Trusted(ctx), photo, photoData)
			})

			It("serves a blob a second row names after the first stops naming it", func() {
				putRows(ctx, srv, ws,
					&testdatav1.BlobHolder{Id: "h1"},
					&testdatav1.BlobHolder{Id: "h2", ByName: map[string]*syncplanev1.BlobRef{"y": refTo(photo)}})
				readable(bob, photo, photoData)
			})
		})
	}

	Describe("garbage collection", func() {
		var (
			srv *server.Server
			mu  sync.Mutex
			now time.Time
		)
		clock := func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		}
		setClock := func(t time.Time) {
			mu.Lock()
			defer mu.Unlock()
			now = t
		}

		BeforeEach(func() {
			setClock(time.Now())
			store = memstore.New(memstore.WithClock(clock))
			srv = newServer(server.WithGC(time.Hour, 0), server.WithBlobRetention(time.Hour))
		})

		markOf := func(d blob.Digest) time.Time {
			GinkgoHelper()
			info, err := store.Stat(ctx, ws, d)
			Expect(err).NotTo(HaveOccurred())
			return info.UnreferencedSince
		}

		It("marks an unreferenced blob and deletes it once the mark is older than the retention", func() {
			data, m := blobOf(40, 10)
			uploadBlob(alice, srv, ws, data, m)

			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).NotTo(BeZero())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).NotTo(BeZero(), "a mark younger than the retention keeps the blob")

			Expect(store.SetUnreferencedSince(ctx, ws, m.Digest, time.Now().Add(-2*time.Hour))).To(Succeed())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			_, err := store.Stat(ctx, ws, m.Digest)
			Expect(err).To(MatchError(blob.ErrNotFound))
		})

		It("keeps a referenced blob and clears its mark, whatever its age", func() {
			data, m := blobOf(41, 10)
			uploadBlob(alice, srv, ws, data, m)
			Expect(store.SetUnreferencedSince(ctx, ws, m.Digest, time.Now().Add(-2*time.Hour))).To(Succeed())
			putRows(ctx, srv, ws, &testdatav1.BlobTree{Id: "t1", Root: &testdatav1.BlobNode{Icon: refTo(m)}})

			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).To(BeZero())
		})

		It("counts a server-only row as a reference", func() {
			data, m := blobOf(52, 10)
			uploadBlob(alice, srv, ws, data, m)
			putRows(ctx, srv, ws, &testdatav1.BlobLedger{Id: "l1", Receipt: refTo(m)})
			Expect(store.SetUnreferencedSince(ctx, ws, m.Digest, time.Now().Add(-2*time.Hour))).To(Succeed())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).To(BeZero())
		})

		It("counts a row no caller can read as a reference", func() {
			data, m := blobOf(42, 10)
			uploadBlob(alice, srv, ws, data, m)
			putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Hidden: true, Secret: refTo(m)})
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).To(BeZero())
		})

		It("marks a blob again once its last row is deleted", func() {
			data, m := blobOf(43, 10)
			uploadBlob(alice, srv, ws, data, m)
			putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Files: []*syncplanev1.BlobRef{refTo(m)}})
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).To(BeZero())
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.BlobHolder{}, "h1")
			})).To(Succeed())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			Expect(markOf(m.Digest)).NotTo(BeZero())
		})

		It("restarts the clock when BeginUpload names a committed blob", func() {
			data, m := blobOf(44, 10)
			uploadBlob(alice, srv, ws, data, m)
			Expect(store.SetUnreferencedSince(ctx, ws, m.Digest, time.Now().Add(-2*time.Hour))).To(Succeed())

			res, err := beginUpload(bob, srv, ws, m)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(markOf(m.Digest)).To(BeZero())
			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			_, err = store.Stat(ctx, ws, m.Digest)
			Expect(err).NotTo(HaveOccurred())
		})

		It("aborts a staged upload older than the retention", func() {
			_, old := blobOf(45, blob.ChunkSize+1)
			_, young := blobOf(46, blob.ChunkSize+1)
			setClock(time.Now().Add(-2 * time.Hour))
			_, err := beginUpload(alice, srv, ws, old)
			Expect(err).NotTo(HaveOccurred())
			setClock(time.Now())
			_, err = beginUpload(alice, srv, ws, young)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
			_, _, err = store.Staged(ctx, ws, old.Digest)
			Expect(err).To(MatchError(blob.ErrNotFound))
			_, _, err = store.Staged(ctx, ws, young.Digest)
			Expect(err).NotTo(HaveOccurred())
		})

		It("deletes nothing while a live row of an entity outside the catalog exists", func() {
			rows := newTestStore()
			full := mustNewServer(rows, blobEntities(), server.WithBlobStore(store))
			DeferCleanup(full.Close)
			data, m := blobOf(51, 10)
			uploadBlob(alice, full, ws, data, m)
			putRows(ctx, full, ws, &testdatav1.BlobHolder{Id: "h1", Photo: refTo(m)})

			// The same rows, served by a deploy that dropped blob_holder.
			narrow := mustNewServer(rows, server.WithEntities(&testdatav1.BlobTree{}), server.WithBlobStore(store),
				server.WithGC(time.Hour, 0), server.WithBlobRetention(time.Hour))
			DeferCleanup(narrow.Close)
			Expect(store.SetUnreferencedSince(ctx, ws, m.Digest, time.Now().Add(-2*time.Hour))).To(Succeed())
			Expect(narrow.CollectGarbage(ctx, ws)).To(Succeed())
			_, err := store.Stat(ctx, ws, m.Digest)
			Expect(err).NotTo(HaveOccurred())

			// Once the row is gone, the blob goes too.
			Expect(full.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&testdatav1.BlobHolder{}, "h1")
			})).To(Succeed())
			Expect(store.SetUnreferencedSince(ctx, ws, m.Digest, time.Now().Add(-2*time.Hour))).To(Succeed())
			Expect(narrow.CollectGarbage(ctx, ws)).To(Succeed())
			_, err = store.Stat(ctx, ws, m.Digest)
			Expect(err).To(MatchError(blob.ErrNotFound))
		})

		It("deletes a workspace's blobs with the workspace", func() {
			data, m := blobOf(47, 10)
			uploadBlob(alice, srv, ws, data, m)
			_, staged := blobOf(48, blob.ChunkSize+1)
			_, err := beginUpload(alice, srv, ws, staged)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.DeleteWorkspace(ctx, ws)).To(Succeed())
			_, err = store.Stat(ctx, ws, m.Digest)
			Expect(err).To(MatchError(blob.ErrNotFound))
			_, _, err = store.Staged(ctx, ws, staged.Digest)
			Expect(err).To(MatchError(blob.ErrNotFound))
		})

		It("deletes in a background pass what a deleted workspace still holds", func() {
			looping := newServer(server.WithGC(time.Hour, 10*time.Millisecond))
			putRows(ctx, looping, ws, &testdatav1.BlobHolder{Id: "h1"})
			Expect(looping.DeleteWorkspace(ctx, ws)).To(Succeed())
			// What an upload admitted before the deletion commits after it.
			data, m := blobOf(49, 10)
			Expect(store.Begin(ctx, ws, m)).To(Succeed())
			Expect(store.PutChunk(ctx, ws, m.Digest, 0, bytes.NewReader(data))).To(Succeed())
			Expect(store.Complete(ctx, ws, m.Digest)).To(Succeed())

			Eventually(func() error {
				_, err := store.Stat(ctx, ws, m.Digest)
				return err
			}, "2s").Should(MatchError(blob.ErrNotFound))
		})

		It("marks blobs in a background pass of a workspace with no rows", func() {
			looping := newServer(server.WithGC(time.Hour, 10*time.Millisecond))
			data, m := blobOf(50, 10)
			uploadBlob(alice, looping, ws, data, m)
			Eventually(func() time.Time { return markOf(m.Digest) }, "2s").ShouldNot(BeZero())
		})
	})
})
