package server

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/fsstore"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/prefixstore"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

func newBlobTestServer(t *testing.T, opts ...Option) *Server {
	t.Helper()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "blobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	srv, err := New(store, append([]Option{WithEntities(&testdatav1.BlobHolder{}, &testdatav1.BlobTree{})}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// TestBlobIndexesAtRegistration pins which BlobRef paths get an index and
// which entity has to be read row by row.
func TestBlobIndexesAtRegistration(t *testing.T) {
	srv := newBlobTestServer(t)
	holder := srv.catalogPtr().entities["blob_holder"]
	want := []string{"__blob_album.cover", "__blob_by_name", "__blob_files", "__blob_pages.scan", "__blob_photo", "__blob_secret"}
	got := slices.Clone(holder.blobs.indexes)
	slices.Sort(got)
	if !slices.Equal(got, want) || holder.blobs.unindexed {
		t.Fatalf("blob_holder: indexes %v unindexed %v, want %v and false", got, holder.blobs.unindexed, want)
	}
	tree := srv.catalogPtr().entities["blob_tree"]
	if !tree.blobs.unindexed {
		t.Fatalf("blob_tree: a list in a list and a recursive message must leave it unindexed")
	}
	if srv.catalogPtr().entities["blob_holder"].blobs == nil {
		t.Fatal("blob_holder has no blob references")
	}
}

// TestBlobLookupUsesReadyIndexes checks that a lookup on an entity whose blob
// indexes are built is served from them, and that it reads rows otherwise.
// The Ginkgo suite checks that both give the same answer.
func TestBlobLookupUsesReadyIndexes(t *testing.T) {
	ctx := context.Background()
	srv := newBlobTestServer(t)
	const ws = "ws"
	d := blob.Digest("sp1:" + string(bytes.Repeat([]byte("a"), 64)))
	err := srv.Mutate(Trusted(ctx), ws, func(tx *EntityTx) error {
		return tx.Put(&testdatav1.BlobHolder{Id: "h1", Pages: []*testdatav1.BlobPage{{Id: "p", Scan: &syncplanev1.BlobRef{Digest: string(d)}}}})
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := func() ([]string, bool) {
		var keys []string
		var served bool
		err := srv.store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			keys, served, err = blobCandidates(tx, srv.catalogPtr().entities["blob_holder"], map[blob.Digest]bool{d: true}, srv.limits)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return keys, served
	}
	if _, served := lookup(); served {
		t.Fatal("served from indexes that are not built")
	}
	if err := srv.BuildIndexes(ctx, ws); err != nil {
		t.Fatal(err)
	}
	keys, served := lookup()
	if !served || !slices.Equal(keys, []string{"h1"}) {
		t.Fatalf("served %v keys %v, want true [h1]", served, keys)
	}
}

// TestBlobStoreNamesCarryTheAppPrefix checks that a server a Manager hosts
// names its workspaces in the blob store with its share of the id space, and
// that its GC pass leaves another app's workspaces alone.
func TestBlobStoreNamesCarryTheAppPrefix(t *testing.T) {
	ctx := context.Background()
	// Every upload starts two hours ago, past the one-hour retention.
	st := memstore.New(memstore.WithClock(func() time.Time { return time.Now().Add(-2 * time.Hour) }))
	srv := newBlobTestServer(t, WithBlobStore(st), withWorkspacePrefix("one/"), WithBlobRetention(time.Hour))
	m, err := blob.Compute(bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Begin(ctx, "two/ws", m); err != nil {
		t.Fatal(err)
	}
	if err := srv.blobs.Begin(ctx, srv.blobKey("ws"), m); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Staged(ctx, "one/ws", m.Digest); err != nil {
		t.Fatalf("one/ws: %v", err)
	}
	if err := srv.collectAllBlobs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Staged(ctx, "one/ws", m.Digest); err == nil {
		t.Fatal("one/ws: the pass did not abort this app's old upload")
	}
	if _, _, err := st.Staged(ctx, "two/ws", m.Digest); err != nil {
		t.Fatalf("two/ws: the pass reached another app's upload: %v", err)
	}
}

// TestBlobDeleteSkipsAfterAWrite checks that the delete step leaves every
// blob alone when a write landed in the workspace after the snapshot its
// reference check read.
func TestBlobDeleteSkipsAfterAWrite(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	srv := newBlobTestServer(t, WithBlobStore(st))
	const ws = "ws"
	m, err := blob.Compute(bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Begin(ctx, ws, m); err != nil {
		t.Fatal(err)
	}
	if err := st.PutChunk(ctx, ws, m.Digest, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	if err := st.Complete(ctx, ws, m.Digest); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := st.SetUnreferencedSince(ctx, ws, m.Digest, old); err != nil {
		t.Fatal(err)
	}
	var seq uint64
	if err := srv.store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		var err error
		seq, err = tx.CurrentSeq()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Mutate(Trusted(ctx), ws, func(tx *EntityTx) error {
		return tx.Put(&testdatav1.BlobHolder{Id: "h1", Photo: &syncplanev1.BlobRef{Digest: string(m.Digest)}})
	}); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(-time.Hour)
	n, err := srv.deleteUnchanged(ctx, ws, seq, []blob.Digest{m.Digest}, cutoff)
	if err != nil || n != 0 {
		t.Fatalf("deleted %d (%v) after a write, want 0", n, err)
	}
	if _, err := st.Stat(ctx, ws, m.Digest); err != nil {
		t.Fatalf("blob gone: %v", err)
	}
	if err := srv.store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		var err error
		seq, err = tx.CurrentSeq()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := srv.deleteUnchanged(ctx, ws, seq, []blob.Digest{m.Digest}, cutoff); err != nil || n != 1 {
		t.Fatalf("deleted %d (%v) with no write since the snapshot, want 1", n, err)
	}
}

// TestAppsSharingAFileStoreStayApart builds two servers the way a Manager
// builds two apps over one root adapter and one fsstore, and checks that the
// blob one app stores is not readable or collectable by the other.
func TestAppsSharingAFileStoreStayApart(t *testing.T) {
	ctx := context.Background()
	root, err := sqlitestore.Open(filepath.Join(t.TempDir(), "root.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	blobs, err := fsstore.Open(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	app := func(id string) *Server {
		prefix := appPrefix(id)
		srv, err := New(prefixstore.Wrap(root, prefix), withWorkspacePrefix(prefix),
			WithEntities(&testdatav1.BlobHolder{}), WithBlobStore(blobs))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		return srv
	}
	one, two := app("one"), app("two")
	data := []byte("app one's bytes")
	m, err := blob.Compute(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := one.BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
		WorkspaceId: "ws", Digest: string(m.Digest), Size: m.Size, ChunkHashes: m.ChunkHashes,
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := one.PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
		WorkspaceId: "ws", Digest: string(m.Digest), Data: data,
	})); err != nil {
		t.Fatal(err)
	}
	for _, srv := range []*Server{one, two} {
		if err := srv.Mutate(Trusted(ctx), "ws", func(tx *EntityTx) error {
			return tx.Put(&testdatav1.BlobHolder{Id: "h1", Photo: &syncplanev1.BlobRef{Digest: string(m.Digest)}})
		}); err != nil {
			t.Fatal(err)
		}
	}
	stat := func(srv *Server) syncplanev1.BlobState {
		res, err := srv.StatBlobs(ctx, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: "ws", Digests: []string{string(m.Digest)}}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetBlobs()[0].GetState()
	}
	if got := stat(one); got != syncplanev1.BlobState_BLOB_STATE_COMPLETE {
		t.Fatalf("app one: %v", got)
	}
	if got := stat(two); got != syncplanev1.BlobState_BLOB_STATE_MISSING {
		t.Fatalf("app two reads app one's blob: %v", got)
	}
	if err := two.DeleteWorkspace(ctx, "ws"); err != nil {
		t.Fatal(err)
	}
	if got := stat(one); got != syncplanev1.BlobState_BLOB_STATE_COMPLETE {
		t.Fatalf("app two's workspace delete reached app one: %v", got)
	}
}
