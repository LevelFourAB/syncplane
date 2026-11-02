package client_test

import (
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
