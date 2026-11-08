package client_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}

// startServer serves the example app (todo, note, the example mutators and
// permission policy) plus the test entities the specs write, over a SQLite
// store in a temp dir, and returns the server and its URL.
func startServer() (*server.Server, string) {
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "client.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(store.Close)
	return serve(store)
}

// serve is startServer over the given store.
func serve(store storage.Adapter) (*server.Server, string) {
	perms := exampleapp.NewPermissions()
	srv, err := server.New(store, append(exampleapp.Options(perms),
		server.WithPermissions(perms),
		server.WithEntities(
			&testdatav1.Widget{},
			&testdatav1.Pair{},
			&testdatav1.KeyedTextEntity{},
			&testdatav1.BlobHolder{},
		),
		server.WithBlobStore(memstore.New()),
		server.WithHeartbeat(50*time.Millisecond),
		server.WithNotifyCoalescing(0),
		server.WithLogger(slog.New(slog.DiscardHandler)),
	)...)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(srv.Close)
	ts := httptest.NewServer(srv.HTTPHandler())
	DeferCleanup(ts.Close)
	return srv, ts.URL
}

// withReplica is a store whose reads may run on a second store, a standby
// that holds only the writes a spec copies to it.
type withReplica struct {
	storage.Adapter
	replica storage.Adapter
}

func (s withReplica) ReplicaReader() storage.ReplicaReader { return laggingReader(s) }

// laggingReader serves a read from the replica when the replica has reached
// the caller's floor, and from the primary otherwise, as pgstore's replica
// routing does.
type laggingReader withReplica

func (r laggingReader) WithReadTxMinSeq(ctx context.Context, ws string, minSeq uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	stale := false
	err := r.replica.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		cur, err := tx.CurrentSeq()
		if err != nil {
			return err
		}
		if cur < minSeq {
			stale = true
			return nil
		}
		return fn(tx)
	})
	if stale {
		return storage.ReadRouteFallbackStale, r.Adapter.WithReadTx(ctx, ws, fn)
	}
	return storage.ReadRouteReplica, err
}

func (r laggingReader) WithReadTxAnchored(ctx context.Context, ws string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	var seq uint64
	if err := r.Adapter.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		var err error
		seq, err = tx.CurrentSeq()
		return err
	}); err != nil {
		return storage.ReadRouteFallbackError, err
	}
	return r.WithReadTxMinSeq(ctx, ws, seq, fn)
}
