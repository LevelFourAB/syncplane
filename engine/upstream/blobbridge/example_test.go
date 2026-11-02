package blobbridge_test

import (
	"context"
	"log"

	"github.com/levelfourab/syncplane/engine/blob/fsstore"
	"github.com/levelfourab/syncplane/engine/client"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream/blobbridge"
)

// A local engine whose blobs reach the origin, and whose readers get the
// origin's blobs.
func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	origin, err := client.New("https://sync.example.com")
	if err != nil {
		log.Fatal(err)
	}
	blobs, err := fsstore.Open("/var/lib/app/blobs")
	if err != nil {
		log.Fatal(err)
	}
	bridge := blobbridge.New(blobs, origin.BlobService(),
		blobbridge.WithEntities("blob_holder"),
	)
	defer bridge.Close()

	store, err := sqlitestore.Open("/var/lib/app/local.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	local, err := server.New(store,
		server.WithEntities(&testdatav1.BlobHolder{}),
		server.WithBlobStore(bridge),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer local.Close()

	go func() {
		if err := bridge.Run(ctx, local); err != nil {
			log.Print(err)
		}
	}()
	// Serve local.HTTPHandler() to the app's clients.
}
