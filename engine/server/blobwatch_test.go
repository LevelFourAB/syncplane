package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// bearerSubject takes the bearer token as the subject, unverified.
var bearerSubject = server.AuthenticatorFunc(func(_ context.Context, h http.Header) (server.Principal, error) {
	subject, _ := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
	return server.Principal{Subject: subject}, nil
})

// revocablePolicy denies "bob" the workspace once revoked is set.
type revocablePolicy struct {
	server.AllowAll
	revoked *atomic.Bool
}

func (p revocablePolicy) CanAccessWorkspace(_ context.Context, pr server.Principal, _ string) error {
	if pr.Subject == "bob" && p.revoked.Load() {
		return server.DenyWorkspace("bob was removed")
	}
	return nil
}

// watchFrame is one received message, or how the stream ended.
type watchFrame struct {
	blobs []*syncplanev1.BlobStatus
	done  bool
	err   error
}

var _ = Describe("WatchBlobs", func() {
	const ws = "ws-watch"
	var (
		ctx   context.Context
		store *memstore.Store
		srv   *server.Server
		alice context.Context
	)

	start := func(opts ...server.Option) syncplanev1connect.BlobServiceClient {
		GinkgoHelper()
		srv = mustNewServer(newTestStore(), append([]server.Option{
			blobEntities(), server.WithBlobStore(store), server.WithAuthenticator(bearerSubject),
			server.WithNotifyCoalescing(5 * time.Millisecond),
		}, opts...)...)
		DeferCleanup(srv.Close)
		h := httptest.NewServer(srv.HTTPHandler())
		DeferCleanup(h.Close)
		return syncplanev1connect.NewBlobServiceClient(h.Client(), h.URL)
	}

	// watch opens a stream as subject and returns its frames on a channel.
	watch := func(client syncplanev1connect.BlobServiceClient, subject string, digests ...blob.Digest) <-chan watchFrame {
		raw := make([]string, len(digests))
		for i, d := range digests {
			raw[i] = string(d)
		}
		req := connect.NewRequest(&syncplanev1.WatchBlobsRequest{WorkspaceId: ws, Digests: raw})
		req.Header().Set("Authorization", "Bearer "+subject)
		sctx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		frames := make(chan watchFrame, 64)
		go func() {
			defer GinkgoRecover()
			stream, err := client.WatchBlobs(sctx, req)
			if err != nil {
				frames <- watchFrame{err: err}
				return
			}
			defer stream.Close()
			for stream.Receive() {
				frames <- watchFrame{blobs: stream.Msg().GetBlobs()}
			}
			frames <- watchFrame{done: true, err: stream.Err()}
		}()
		return frames
	}
	next := func(frames <-chan watchFrame) watchFrame {
		GinkgoHelper()
		var f watchFrame
		Eventually(frames, "3s").Should(Receive(&f))
		return f
	}
	// nextBlobs skips keepalives.
	nextBlobs := func(frames <-chan watchFrame) watchFrame {
		GinkgoHelper()
		for {
			f := next(frames)
			if f.done || f.err != nil || len(f.blobs) > 0 {
				return f
			}
		}
	}
	states := func(f watchFrame) map[string]syncplanev1.BlobState {
		out := map[string]syncplanev1.BlobState{}
		for _, b := range f.blobs {
			out[b.GetDigest()] = b.GetState()
		}
		return out
	}

	BeforeEach(func() {
		ctx = context.Background()
		store = memstore.New()
		alice = server.WithPrincipal(ctx, server.Principal{Subject: "alice"})
	})

	It("fails with unimplemented without a blob store", func() {
		srv := mustNewServer(newTestStore(), blobEntities())
		DeferCleanup(srv.Close)
		h := httptest.NewServer(srv.HTTPHandler())
		DeferCleanup(h.Close)
		_, m := blobOf(60, 5)
		f := next(watch(syncplanev1connect.NewBlobServiceClient(h.Client(), h.URL), "bob", m.Digest))
		Expect(connect.CodeOf(f.err)).To(Equal(connect.CodeUnimplemented))
	})

	It("refuses no digests, too many and a malformed one with invalid_argument", func() {
		client := start()
		many := make([]blob.Digest, 257)
		for i := range many {
			_, m := blobOf(byte(i), 2)
			many[i] = m.Digest
		}
		for _, digests := range [][]blob.Digest{nil, many, {"sp1:nope"}} {
			f := next(watch(client, "bob", digests...))
			Expect(connect.CodeOf(f.err)).To(Equal(connect.CodeInvalidArgument), "%d digests", len(digests))
		}
	})

	It("sends every state first, in request order, and the changes after, then ends", func() {
		client := start()
		doneData, done := blobOf(61, 10)
		laterData, later := blobOf(62, blob.ChunkSize+3)
		_, absent := blobOf(63, 10)
		uploadBlob(alice, srv, ws, doneData, done)
		_, err := beginUpload(alice, srv, ws, later)
		Expect(err).NotTo(HaveOccurred())
		putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Files: []*syncplanev1.BlobRef{refTo(done), refTo(later), refTo(absent)}})

		frames := watch(client, "bob", later.Digest, absent.Digest, done.Digest, later.Digest)
		first := next(frames)
		Expect(first.err).NotTo(HaveOccurred())
		var order []string
		var got []syncplanev1.BlobState
		for _, b := range first.blobs {
			order = append(order, b.GetDigest())
			got = append(got, b.GetState())
		}
		Expect(order).To(Equal([]string{string(later.Digest), string(absent.Digest), string(done.Digest), string(later.Digest)}))
		Expect(got).To(Equal([]syncplanev1.BlobState{
			syncplanev1.BlobState_BLOB_STATE_UPLOADING, syncplanev1.BlobState_BLOB_STATE_MISSING,
			syncplanev1.BlobState_BLOB_STATE_COMPLETE, syncplanev1.BlobState_BLOB_STATE_UPLOADING,
		}))
		Expect(first.blobs[2].GetChunkHashes()).To(Equal(done.ChunkHashes))

		// Another caller finishes the upload: one message with that digest.
		for _, i := range []int{0, 1} {
			_, err := putChunk(alice, srv, ws, later.Digest, i, chunkOf(laterData, i))
			Expect(err).NotTo(HaveOccurred())
		}
		f := nextBlobs(frames)
		Expect(f.blobs).To(HaveLen(1))
		Expect(f.blobs[0].GetDigest()).To(Equal(string(later.Digest)))
		Expect(f.blobs[0].GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
		Expect(f.blobs[0].GetSize()).To(Equal(later.Size))
		Expect(f.blobs[0].GetChunkHashes()).To(Equal(later.ChunkHashes))

		// The absent one arrives, and the stream ends with OK.
		absentData, _ := blobOf(63, 10)
		uploadBlob(alice, srv, ws, absentData, absent)
		f = nextBlobs(frames)
		Expect(states(f)).To(Equal(map[string]syncplanev1.BlobState{string(absent.Digest): syncplanev1.BlobState_BLOB_STATE_COMPLETE}))
		f = nextBlobs(frames)
		Expect(f.done).To(BeTrue())
		Expect(f.err).NotTo(HaveOccurred())
	})

	It("ends at once when every digest is complete in the first message", func() {
		client := start()
		data, m := blobOf(64, 10)
		uploadBlob(alice, srv, ws, data, m)
		putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", Photo: refTo(m)})
		frames := watch(client, "bob", m.Digest)
		Expect(states(next(frames))).To(Equal(map[string]syncplanev1.BlobState{string(m.Digest): syncplanev1.BlobState_BLOB_STATE_COMPLETE}))
		f := next(frames)
		Expect(f.done).To(BeTrue())
		Expect(f.err).NotTo(HaveOccurred())
	})

	It("reports a complete blob when a row the caller reads starts naming it", func() {
		client := start()
		data, m := blobOf(65, 10)
		uploadBlob(alice, srv, ws, data, m)
		frames := watch(client, "bob", m.Digest)
		Expect(states(next(frames))).To(Equal(map[string]syncplanev1.BlobState{string(m.Digest): syncplanev1.BlobState_BLOB_STATE_MISSING}))

		putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", ByName: map[string]*syncplanev1.BlobRef{"k": refTo(m)}})
		f := nextBlobs(frames)
		Expect(states(f)).To(Equal(map[string]syncplanev1.BlobState{string(m.Digest): syncplanev1.BlobState_BLOB_STATE_COMPLETE}))
	})

	It("keeps a blob the caller cannot read missing through its completion, and sends nothing for it", func() {
		client := start()
		secretData, secret := blobOf(66, 10)
		openData, open := blobOf(67, 10)
		putRows(ctx, srv, ws, &testdatav1.BlobHolder{Id: "h1", OwnerId: "alice", Secret: refTo(secret), Photo: refTo(open)})
		frames := watch(client, "bob", secret.Digest, open.Digest)
		Expect(next(frames).err).NotTo(HaveOccurred())

		uploadBlob(alice, srv, ws, secretData, secret)
		uploadBlob(alice, srv, ws, openData, open)
		// The open blob may pass through UPLOADING on the way; the secret one
		// never shows up.
		for {
			f := nextBlobs(frames)
			Expect(f.err).NotTo(HaveOccurred())
			Expect(states(f)).NotTo(HaveKey(string(secret.Digest)))
			if states(f)[string(open.Digest)] == syncplanev1.BlobState_BLOB_STATE_COMPLETE {
				break
			}
		}
		Consistently(frames, "200ms").ShouldNot(Receive(HaveField("blobs", Not(BeEmpty()))))
	})

	It("sends a keepalive at the heartbeat interval", func() {
		client := start(server.WithHeartbeat(30 * time.Millisecond))
		_, m := blobOf(68, 10)
		frames := watch(client, "bob", m.Digest)
		Expect(next(frames).blobs).To(HaveLen(1))
		f := next(frames)
		Expect(f.err).NotTo(HaveOccurred())
		Expect(f.done).To(BeFalse())
		Expect(f.blobs).To(BeEmpty())
	})

	It("ends with the gate's error when the caller loses the workspace", func() {
		revoked := &atomic.Bool{}
		client := start(server.WithHeartbeat(30*time.Millisecond), server.WithPermissions(revocablePolicy{revoked: revoked}))
		_, m := blobOf(69, 10)
		frames := watch(client, "bob", m.Digest)
		Expect(next(frames).err).NotTo(HaveOccurred())
		revoked.Store(true)
		f := nextBlobs(frames)
		Expect(f.done).To(BeTrue())
		Expect(connect.CodeOf(f.err)).To(Equal(connect.CodePermissionDenied))
		var ce *connect.Error
		Expect(errors.As(f.err, &ce)).To(BeTrue())
		Expect(ce.Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceAccessRevoked))
	})

	It("refuses a caller the gate refuses before it subscribes", func() {
		revoked := &atomic.Bool{}
		revoked.Store(true)
		client := start(server.WithPermissions(revocablePolicy{revoked: revoked}))
		_, m := blobOf(70, 10)
		f := next(watch(client, "bob", m.Digest))
		Expect(connect.CodeOf(f.err)).To(Equal(connect.CodePermissionDenied))
	})
})
