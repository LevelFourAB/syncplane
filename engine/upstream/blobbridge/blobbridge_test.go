package blobbridge_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/blobtest"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	"github.com/levelfourab/syncplane/engine/client"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream/blobbridge"
)

var _ = Describe("Bridge", func() {
	var o *origin

	BeforeEach(func() { o = newOrigin() })

	Describe("uploads", func() {
		It("uploads a local blob when the row that names it commits after it", func() {
			l := newLocal(o.client(), memstore.New(), "", nil)
			data, m := blobOf(1, 1000)
			upload(l.srv, data, m)
			Consistently(func() []byte { return read(o.blobs, m.Digest) }, "200ms").Should(BeNil())

			putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)})
			Eventually(func() []byte { return read(o.blobs, m.Digest) }).Should(Equal(data))
		})

		It("uploads a local blob when it commits after the row that names it", func() {
			l := newLocal(o.client(), memstore.New(), "", nil)
			data, m := blobOf(2, blob.ChunkSize+10)
			putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Files: []*syncplanev1.BlobRef{refTo(m)}})
			upload(l.srv, data, m)
			Eventually(func() []byte { return read(o.blobs, m.Digest) }).Should(Equal(data))
		})

		It("sends no chunks for a blob the origin already holds", func() {
			data, m := blobOf(3, 1000)
			upload(o.srv, data, m)
			before := o.count("PutChunk")
			l := newLocal(o.client(), memstore.New(), "", nil)
			putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)})
			upload(l.srv, data, m)
			Eventually(func() int { return o.count("BeginUpload") }).Should(BeNumerically(">=", 1))
			Consistently(func() int { return o.count("PutChunk") }, "200ms").Should(Equal(before))
		})

		It("skips blobs that only rows of other entities name", func() {
			l := newLocal(o.client(), memstore.New(), "", nil, blobbridge.WithEntities("blob_holder"))
			kept, mk := blobOf(4, 100)
			other, mo := blobOf(5, 100)
			putRows(l.srv,
				&testdatav1.BlobHolder{Id: "h", Photo: refTo(mk)},
				&testdatav1.BlobTree{Id: "t", Root: &testdatav1.BlobNode{Icon: refTo(mo)}},
			)
			upload(l.srv, other, mo)
			upload(l.srv, kept, mk)
			Eventually(func() []byte { return read(o.blobs, mk.Digest) }).Should(Equal(kept))
			Consistently(func() []byte { return read(o.blobs, mo.Digest) }, "200ms").Should(BeNil())
		})

		It("waits out an offline origin and uploads when it is back", func() {
			o.offline.Store(true)
			l := newLocal(o.client(), memstore.New(), "", nil)
			data, m := blobOf(6, 1000)
			putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)})
			upload(l.srv, data, m)
			Consistently(func() []byte { return read(o.blobs, m.Digest) }, "300ms").Should(BeNil())

			o.offline.Store(false)
			Eventually(func() []byte { return read(o.blobs, m.Digest) }, "5s").Should(Equal(data))
		})

		It("finds blobs left from before a restart and resumes a broken upload", func() {
			inner := memstore.New()
			path := dbPath()
			data, m := blobOf(7, blob.ChunkSize+10)

			// The first run breaks after one chunk reaches the origin.
			o.failPutChunkAfter.Store(1)
			l := newLocal(o.client(), inner, path, nil)
			putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)})
			upload(l.srv, data, m)
			Eventually(func() []int {
				_, held, _ := o.blobs.Staged(context.Background(), ws, m.Digest)
				return held
			}).Should(HaveLen(1))
			l.stop()
			Expect(read(o.blobs, m.Digest)).To(BeNil())

			// A blob that commits while no bridge runs.
			later, ml := blobOf(8, 500)
			store, err := sqlitestore.Open(path)
			Expect(err).NotTo(HaveOccurred())
			plain, err := server.New(store, serverOptions(inner)...)
			Expect(err).NotTo(HaveOccurred())
			putRows(plain, &testdatav1.BlobHolder{Id: "later", Photo: refTo(ml)})
			upload(plain, later, ml)
			Expect(plain.Close()).To(Succeed())
			Expect(store.Close()).To(Succeed())

			o.failPutChunkAfter.Store(-1)
			sent := o.count("PutChunk")
			newLocal(o.client(), inner, path, nil)
			Eventually(func() []byte { return read(o.blobs, m.Digest) }).Should(Equal(data))
			Eventually(func() []byte { return read(o.blobs, ml.Digest) }).Should(Equal(later))
			// One chunk of the broken upload and the one of the later blob.
			Expect(o.count("PutChunk") - sent).To(Equal(2))
		})

		It("keeps a blob named by a live row through local GC while the origin is offline", func() {
			o.offline.Store(true)
			l := newLocal(o.client(), memstore.New(), "", []server.Option{
				server.WithGC(time.Hour, 0),
				server.WithBlobRetention(time.Millisecond),
			})
			named, mn := blobOf(9, 100)
			loose, ml := blobOf(10, 100)
			putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(mn)})
			upload(l.srv, named, mn)
			upload(l.srv, loose, ml)
			for range 2 {
				Expect(l.srv.CollectGarbage(context.Background(), ws)).To(Succeed())
				time.Sleep(5 * time.Millisecond)
			}
			Expect(read(l.inner, mn.Digest)).To(Equal(named))
			Expect(read(l.inner, ml.Digest)).To(BeNil())

			o.offline.Store(false)
			Eventually(func() []byte { return read(o.blobs, mn.Digest) }, "5s").Should(Equal(named))
			Consistently(func() []byte { return read(o.blobs, ml.Digest) }, "200ms").Should(BeNil())
		})
	})

	Describe("read-through", func() {
		// atOrigin uploads a blob to the origin and names it in a row on both
		// engines, as the sync link leaves them.
		atOrigin := func(l *local, seed byte, size int) ([]byte, blob.Manifest) {
			data, m := blobOf(seed, size)
			upload(o.srv, data, m)
			row := &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)}
			putRows(o.srv, row)
			putRows(l.srv, row)
			return data, m
		}

		It("serves a blob uploaded at the origin through the local BlobService", func() {
			l := newLocal(o.client(), memstore.New(), "", nil, blobbridge.WithReadWait(5*time.Second))
			data, m := atOrigin(l, 11, blob.ChunkSize+10)

			st := statOf(l.srv, m.Digest)
			Expect(st.GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(download(l.srv, st)).To(Equal(data))
			Expect(read(l.inner, m.Digest)).To(Equal(data))
		})

		It("reports a blob it fetches as uploading, and completes a watch when it arrives", func() {
			l := newLocal(o.client(), memstore.New(), "", nil)
			data, m := atOrigin(l, 12, 1000)

			ts := httptest.NewServer(l.srv.HTTPHandler())
			DeferCleanup(ts.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := syncplanev1connect.NewBlobServiceClient(ts.Client(), ts.URL).WatchBlobs(ctx,
				connect.NewRequest(&syncplanev1.WatchBlobsRequest{WorkspaceId: ws, Digests: []string{string(m.Digest)}}))
			Expect(err).NotTo(HaveOccurred())
			defer stream.Close()
			var states []syncplanev1.BlobState
			for stream.Receive() {
				for _, st := range stream.Msg().GetBlobs() {
					states = append(states, st.GetState())
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			Expect(len(states)).To(BeNumerically(">=", 2))
			Expect(states[0]).To(Equal(syncplanev1.BlobState_BLOB_STATE_UPLOADING))
			Expect(states[len(states)-1]).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
			Expect(read(l.inner, m.Digest)).To(Equal(data))
		})

		It("fetches a blob once for concurrent readers", func() {
			l := newLocal(o.client(), memstore.New(), "", nil, blobbridge.WithReadWait(5*time.Second))
			data, m := atOrigin(l, 13, 1000)

			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					Expect(statOf(l.srv, m.Digest).GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_COMPLETE))
				}()
			}
			wg.Wait()
			Expect(o.count("GetChunk")).To(Equal(1))
			Expect(read(l.inner, m.Digest)).To(Equal(data))
		})

		It("fetches nothing for a digest no local row names", func() {
			l := newLocal(o.client(), memstore.New(), "", nil, blobbridge.WithReadWait(time.Second))
			data, m := blobOf(14, 1000)
			upload(o.srv, data, m)
			putRows(o.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)})

			Expect(statOf(l.srv, m.Digest).GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_MISSING))
			Consistently(func() []byte { return read(l.inner, m.Digest) }, "200ms").Should(BeNil())
			Expect(o.count("GetChunk")).To(BeZero())
		})

		It("retries a fetch while the origin is offline", func() {
			l := newLocal(o.client(), memstore.New(), "", nil)
			o.offline.Store(true)
			data, m := atOrigin(l, 15, 1000)
			Expect(statOf(l.srv, m.Digest).GetState()).To(Equal(syncplanev1.BlobState_BLOB_STATE_MISSING))
			Consistently(func() []byte { return read(l.inner, m.Digest) }, "200ms").Should(BeNil())

			o.offline.Store(false)
			Eventually(func() []byte { return read(l.inner, m.Digest) }, "5s").Should(Equal(data))
		})

		It("does not store a chunk that does not match its hash", func() {
			tamper := &tamperingOrigin{BlobServiceClient: o.client()}
			tamper.bad.Store(true)
			l := newLocal(tamper, memstore.New(), "", nil)
			data, m := atOrigin(l, 16, 1000)
			statOf(l.srv, m.Digest)
			Eventually(func() int { return o.count("GetChunk") }).Should(BeNumerically(">=", 2))
			Expect(read(l.inner, m.Digest)).To(BeNil())

			tamper.bad.Store(false)
			Eventually(func() []byte { return read(l.inner, m.Digest) }, "5s").Should(Equal(data))
		})

		It("lets the Go client download an origin blob on the first try", func() {
			l := newLocal(o.client(), memstore.New(), "", nil)
			data, m := atOrigin(l, 19, blob.ChunkSize+10)
			ts := httptest.NewServer(l.srv.HTTPHandler())
			DeferCleanup(ts.Close)
			c, err := client.New(ts.URL)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var out bytes.Buffer
			_, err = c.Workspace(ws).DownloadBlob(ctx, string(m.Digest), &out)
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Bytes()).To(Equal(data))
		})

		It("prefetches blobs that new rows name", func() {
			l := newLocal(o.client(), memstore.New(), "", nil, blobbridge.WithPrefetch())
			data, m := atOrigin(l, 17, 1000)
			Eventually(func() []byte { return read(l.inner, m.Digest) }).Should(Equal(data))
		})
	})

	Describe("work per workspace", func() {
		It("reads only the workspaces that change, also across a restart", func() {
			const n = 60
			path := dbPath()
			ctx := context.Background()
			seed, err := sqlitestore.Open(path)
			Expect(err).NotTo(HaveOccurred())
			plain, err := server.New(seed, serverOptions(memstore.New())...)
			Expect(err).NotTo(HaveOccurred())
			for i := range n {
				putRowsIn(plain, fmt.Sprintf("ws-%02d", i), &testdatav1.BlobHolder{Id: "h"})
			}
			Expect(plain.Close()).To(Succeed())
			Expect(seed.Close()).To(Succeed())

			inner := memstore.New()
			start := func() (*local, *readCounter) {
				store, err := sqlitestore.Open(path)
				Expect(err).NotTo(HaveOccurred())
				rc := &readCounter{Adapter: store}
				rc.start()
				return newLocalOn(o.client(), inner, rc, store.Close, nil), rc
			}
			caughtUp := func(l *local) func() []string {
				return func() []string {
					ids, err := l.srv.ConsumerPendingWorkspaces(ctx, blobbridge.DefaultConsumerName, "", n+1)
					Expect(err).NotTo(HaveOccurred())
					return ids
				}
			}

			l, rc := start()
			Eventually(caughtUp(l), "10s").Should(BeEmpty())

			// Idle sweeps read no workspace.
			rc.start()
			Consistently(rc.workspaces, "300ms").Should(BeEmpty())

			// A write is read in its own workspace alone.
			putRowsIn(l.srv, "ws-07", &testdatav1.BlobHolder{Id: "h2"})
			Eventually(caughtUp(l)).Should(BeEmpty())
			Consistently(rc.workspaces, "200ms").Should(ConsistOf("ws-07"))

			// A restart reads no workspace that has nothing to do.
			l.stop()
			_, rc = start()
			Consistently(rc.workspaces, "300ms").Should(BeEmpty())
		})

		It("uploads every blob found at start, in many batches", func() {
			inner := memstore.New()
			ctx := context.Background()
			path := dbPath()
			store, err := sqlitestore.Open(path)
			Expect(err).NotTo(HaveOccurred())
			plain, err := server.New(store, serverOptions(inner)...)
			Expect(err).NotTo(HaveOccurred())
			var ms []blob.Manifest
			for row := range 6 {
				h := &testdatav1.BlobHolder{Id: fmt.Sprintf("h%d", row)}
				for i := range 100 {
					data, m := blobOf(byte(i), 64+row*100+i)
					Expect(inner.Begin(ctx, ws, m)).To(Succeed())
					Expect(inner.PutChunk(ctx, ws, m.Digest, 0, bytes.NewReader(data))).To(Succeed())
					Expect(inner.Complete(ctx, ws, m.Digest)).To(Succeed())
					h.Files = append(h.Files, refTo(m))
					ms = append(ms, m)
				}
				putRows(plain, h)
			}
			Expect(plain.Close()).To(Succeed())
			Expect(store.Close()).To(Succeed())

			newLocal(o.client(), inner, path, nil)
			Eventually(func() int {
				n := 0
				for _, m := range ms {
					if _, err := o.blobs.Stat(ctx, ws, m.Digest); err == nil {
						n++
					}
				}
				return n
			}, "20s").Should(Equal(len(ms)))
		})
	})

	It("refuses a consumer name the engine reserves", func() {
		store, err := sqlitestore.Open(":memory:")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)
		br := blobbridge.New(memstore.New(), o.client(), blobbridge.WithConsumerName("syncplane.blobs"))
		srv, err := server.New(store, serverOptions(br)...)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(srv.Close)
		Expect(br.Run(context.Background(), srv)).To(MatchError(ContainSubstring("reserves")))
	})

	It("refuses a second Run while one runs", func() {
		l := newLocal(o.client(), memstore.New(), "", nil)
		data, m := blobOf(18, 100)
		putRows(l.srv, &testdatav1.BlobHolder{Id: "h", Photo: refTo(m)})
		upload(l.srv, data, m)
		// The upload shows that the first Run is in progress.
		Eventually(func() []byte { return read(o.blobs, m.Digest) }).Should(Equal(data))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		Expect(l.bridge.Run(ctx, l.srv)).To(MatchError(blobbridge.ErrRunning))
	})
})

// The wrapper with a bridge running behind it still keeps the Store contract.
var _ = blobtest.Conformance("blobbridge", func(now func() time.Time) blob.Store {
	o := newOrigin()
	l := newLocal(o.client(), memstore.New(memstore.WithClock(now)), "", nil)
	return l.bridge
})

// tamperingOrigin flips a byte of every chunk it downloads while bad is set.
type tamperingOrigin struct {
	syncplanev1connect.BlobServiceClient
	bad atomic.Bool
}

func (t *tamperingOrigin) GetChunk(ctx context.Context, req *connect.Request[syncplanev1.GetChunkRequest]) (*connect.Response[syncplanev1.GetChunkResponse], error) {
	res, err := t.BlobServiceClient.GetChunk(ctx, req)
	if err == nil && t.bad.Load() && len(res.Msg.Data) > 0 {
		res.Msg.Data[0] ^= 0xff
	}
	return res, err
}
