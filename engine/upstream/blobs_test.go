package upstream_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	"github.com/levelfourab/syncplane/engine/client"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream"
	"github.com/levelfourab/syncplane/engine/upstream/blobbridge"
)

// blobSwitch cuts the blob bridge off from the origin's BlobService, apart
// from the link's own partition.
type blobSwitch struct {
	base  http.RoundTripper
	down  atomic.Bool
	tries atomic.Int64
}

func (s *blobSwitch) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.down.Load() {
		s.tries.Add(1)
		return nil, errors.New("blob service unreachable")
	}
	return s.base.RoundTrip(req)
}

var _ = Describe("Blobs", func() {
	var ctx context.Context
	BeforeEach(func() { ctx = context.Background() })

	It("uploads a blob named by a row outside the subscribed scope after the row left the local engine", func() {
		originBlobs := memstore.New()
		o := newOrigin(server.WithEntities(&testdatav1.BlobHolder{}), server.WithBlobStore(originBlobs))
		sw := &blobSwitch{base: &http.Transport{}}
		bc, err := client.New(o.ts.URL, client.WithHTTPClient(&http.Client{Transport: sw}))
		Expect(err).NotTo(HaveOccurred())
		bridge := blobbridge.New(memstore.New(), bc.BlobService(),
			blobbridge.WithBackoff(10*time.Millisecond, 50*time.Millisecond),
			blobbridge.WithSweepInterval(50*time.Millisecond))
		n := newLocal(o, func(c *upstream.Config) {
			c.Bindings = append(c.Bindings, upstream.Binding{Prototype: &testdatav1.BlobHolder{}})
		}, server.WithEntities(&testdatav1.BlobHolder{}), server.WithBlobStore(bridge))
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = bridge.Run(rctx, n.srv)
		}()
		DeferCleanup(func() {
			cancel()
			<-done
		})
		Expect(n.link.AddWorkspace(ctx, ws, &syncplanev1.Subscription{Entity: "blob_holder", Predicate: query.Eq("title", "mine")})).To(Succeed())
		n.flush()

		// Offline, the app stores a blob and a row outside the scope that
		// names it.
		sw.down.Store(true)
		n.part.Cut()
		data := bytes.Repeat([]byte("attachment"), 1000)
		m, err := blob.Compute(bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())
		tctx := server.Trusted(ctx)
		res, err := n.srv.BeginUpload(tctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
			WorkspaceId: ws, Digest: string(m.Digest), Size: m.Size, ChunkHashes: m.ChunkHashes,
		}))
		Expect(err).NotTo(HaveOccurred())
		for _, i := range res.Msg.GetMissingChunks() {
			start := int(i) * blob.ChunkSize
			_, err := n.srv.PutChunk(tctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
				WorkspaceId: ws, Digest: string(m.Digest), Index: i, Data: data[start:min(start+blob.ChunkSize, len(data))],
			}))
			Expect(err).NotTo(HaveOccurred())
		}
		_, err = n.ws().Put(ctx, &testdatav1.BlobHolder{Id: "b1", Title: "shared",
			Photo: &syncplanev1.BlobRef{Digest: string(m.Digest), Size: m.Size, MediaType: "application/octet-stream"}})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sw.tries.Load).Should(BeNumerically(">=", 1))

		// The link reconnects before the bridge does: the origin confirms
		// the row, and the row leaves the local engine.
		n.part.Heal()
		Eventually(func() string { return getRow[*testdatav1.BlobHolder](o.srv, "b1").GetPhoto().GetDigest() }).
			Should(Equal(string(m.Digest)))
		// An in-scope origin change carries the link's watermark, so the
		// link learns of the confirmation now.
		Expect(o.srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
			return tx.Put(&testdatav1.BlobHolder{Id: "other", Title: "mine"})
		})).To(Succeed())
		Eventually(func() *testdatav1.BlobHolder { return getRow[*testdatav1.BlobHolder](n.srv, "b1") }).Should(BeNil())
		n.flush()

		sw.down.Store(false)
		Eventually(func() error {
			_, err := originBlobs.Stat(ctx, ws, m.Digest)
			return err
		}).WithTimeout(3*time.Second).Should(Succeed(), "the origin's row names a blob the origin does not hold")
	})
})
