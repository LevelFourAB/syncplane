package blobbridge_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	"github.com/levelfourab/syncplane/engine/client"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream/blobbridge"
)

func TestBlobBridge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Blob Bridge Suite")
}

const ws = "ws-1"

var quiet = slog.New(slog.DiscardHandler)

// serverOptions are the options both engines run with.
func serverOptions(blobs blob.Store, extra ...server.Option) []server.Option {
	return append([]server.Option{
		server.WithEntities(&testdatav1.BlobHolder{}, &testdatav1.BlobTree{}),
		server.WithBlobStore(blobs),
		server.WithHeartbeat(50 * time.Millisecond),
		server.WithNotifyCoalescing(0),
		server.WithLogger(quiet),
	}, extra...)
}

// origin is an origin engine served over HTTP. Its handler can be taken
// offline, and counts the blob calls it serves.
type origin struct {
	srv     *server.Server
	blobs   *memstore.Store
	url     string
	offline atomic.Bool
	// failPutChunk makes every PutChunk after the first n fail while set.
	failPutChunkAfter atomic.Int64
	mu                sync.Mutex
	calls             map[string]int
}

func newOrigin() *origin {
	GinkgoHelper()
	o := &origin{blobs: memstore.New(), calls: map[string]int{}}
	o.failPutChunkAfter.Store(-1)
	store, err := sqlitestore.Open(":memory:")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(store.Close)
	o.srv, err = server.New(store, serverOptions(o.blobs)...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(o.srv.Close)
	h := o.srv.HTTPHandler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o.offline.Load() {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		o.mu.Lock()
		o.calls[method]++
		n := o.calls[method]
		o.mu.Unlock()
		if after := o.failPutChunkAfter.Load(); method == "PutChunk" && after >= 0 && int64(n) > after {
			http.Error(w, "broken", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	}))
	DeferCleanup(ts.Close)
	o.url = ts.URL
	return o
}

func (o *origin) count(method string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[method]
}

// client returns a BlobService client of the origin, as an app passes one to
// the bridge.
func (o *origin) client() syncplanev1connect.BlobServiceClient {
	GinkgoHelper()
	c, err := client.New(o.url)
	Expect(err).NotTo(HaveOccurred())
	return c.BlobService()
}

// local is a local engine whose blob store is a bridge to an origin.
type local struct {
	srv    *server.Server
	inner  *memstore.Store
	bridge *blobbridge.Bridge
	stop   func()
}

// newLocal starts a local engine over inner and dbPath (":memory:" when
// empty), with a bridge built from opts that is running until the spec ends
// or stop is called.
func newLocal(o syncplanev1connect.BlobServiceClient, inner *memstore.Store, dbPath string, srvOpts []server.Option, opts ...blobbridge.Option) *local {
	GinkgoHelper()
	if dbPath == "" {
		dbPath = ":memory:"
	}
	store, err := sqlitestore.Open(dbPath)
	Expect(err).NotTo(HaveOccurred())
	return newLocalOn(o, inner, store, store.Close, srvOpts, opts...)
}

// newLocalOn is newLocal over store, which the local engine closes with
// closeStore when it stops.
func newLocalOn(o syncplanev1connect.BlobServiceClient, inner *memstore.Store, store storage.Adapter, closeStore func() error, srvOpts []server.Option, opts ...blobbridge.Option) *local {
	GinkgoHelper()
	var err error
	l := &local{inner: inner}
	opts = append([]blobbridge.Option{
		blobbridge.WithLogger(quiet),
		blobbridge.WithBackoff(10*time.Millisecond, 50*time.Millisecond),
		blobbridge.WithSweepInterval(50 * time.Millisecond),
	}, opts...)
	l.bridge = blobbridge.New(inner, o, opts...)
	l.srv, err = server.New(store, serverOptions(l.bridge, srvOpts...)...)
	Expect(err).NotTo(HaveOccurred())
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer GinkgoRecover()
		Expect(l.bridge.Run(ctx, l.srv)).To(Succeed())
	}()
	var once sync.Once
	l.stop = func() {
		once.Do(func() {
			cancel()
			<-stopped
			Expect(l.srv.Close()).To(Succeed())
			Expect(closeStore()).To(Succeed())
		})
	}
	DeferCleanup(l.stop)
	return l
}

// blobOf returns size bytes, distinct per seed, and their manifest.
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

func refTo(m blob.Manifest) *syncplanev1.BlobRef {
	return &syncplanev1.BlobRef{Digest: string(m.Digest), Size: m.Size, MediaType: "application/octet-stream"}
}

// upload stores data in srv through its BlobService, as a client does.
func upload(srv *server.Server, data []byte, m blob.Manifest) {
	GinkgoHelper()
	ctx := server.Trusted(context.Background())
	res, err := srv.BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
		WorkspaceId: ws, Digest: string(m.Digest), Size: m.Size, ChunkHashes: m.ChunkHashes,
	}))
	Expect(err).NotTo(HaveOccurred())
	for _, i := range res.Msg.GetMissingChunks() {
		start := int(i) * blob.ChunkSize
		_, err := srv.PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
			WorkspaceId: ws, Digest: string(m.Digest), Index: i, Data: data[start:min(start+blob.ChunkSize, len(data))],
		}))
		Expect(err).NotTo(HaveOccurred())
	}
}

func putRows(srv *server.Server, rows ...proto.Message) {
	GinkgoHelper()
	putRowsIn(srv, ws, rows...)
}

func putRowsIn(srv *server.Server, workspaceID string, rows ...proto.Message) {
	GinkgoHelper()
	Expect(srv.Mutate(server.Trusted(context.Background()), workspaceID, func(tx *server.EntityTx) error {
		for _, r := range rows {
			if err := tx.Put(r); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
}

// read returns the committed bytes of d in st, or nil.
func read(st blob.Store, d blob.Digest) []byte {
	r, err := st.Open(context.Background(), ws, d)
	if err != nil {
		return nil
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())
	return b
}

// statOf asks srv's BlobService for d as an unauthenticated caller.
func statOf(srv *server.Server, d blob.Digest) *syncplanev1.BlobStatus {
	GinkgoHelper()
	res, err := srv.StatBlobs(context.Background(), connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws, Digests: []string{string(d)}}))
	Expect(err).NotTo(HaveOccurred())
	return res.Msg.GetBlobs()[0]
}

// download reads every chunk of d from srv's BlobService.
func download(srv *server.Server, st *syncplanev1.BlobStatus) []byte {
	GinkgoHelper()
	var out []byte
	for i := range blob.ChunkCount(st.GetSize()) {
		res, err := srv.GetChunk(context.Background(), connect.NewRequest(&syncplanev1.GetChunkRequest{WorkspaceId: ws, Digest: st.GetDigest(), Index: uint32(i)}))
		Expect(err).NotTo(HaveOccurred())
		out = append(out, res.Msg.GetData()...)
	}
	return out
}

// dbPath returns a path for a SQLite file that outlives one local engine.
func dbPath() string { return filepath.Join(GinkgoT().TempDir(), "local.db") }

// readCounter records the workspaces whose read transactions it opens.
type readCounter struct {
	storage.Adapter
	mu    sync.Mutex
	reads map[string]int
}

func (c *readCounter) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	c.mu.Lock()
	if c.reads != nil {
		c.reads[ws]++
	}
	c.mu.Unlock()
	return c.Adapter.WithReadTx(ctx, ws, fn)
}

// start makes the counter record from now on, forgetting earlier reads.
func (c *readCounter) start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = map[string]int{}
}

// workspaces returns the ids of the workspaces read since start.
func (c *readCounter) workspaces() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for ws := range c.reads {
		out = append(out, ws)
	}
	return out
}
