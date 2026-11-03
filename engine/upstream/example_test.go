package upstream_test

import (
	"context"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// Example links a local engine to an origin, writes through the local
// engine, and reads the row back once the origin confirmed it. The origin
// runs in-process here; in an app it is a remote server.
func Example() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "upstream-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	originStore, err := sqlitestore.Open(filepath.Join(dir, "origin.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer originStore.Close()
	origin, err := server.New(originStore, server.WithEntities(&todov1.Todo{}))
	if err != nil {
		log.Fatal(err)
	}
	defer origin.Close()
	ts := httptest.NewServer(origin.HTTPHandler())
	defer ts.Close()

	// The link is built first: the local engine needs its options, and the
	// link needs the engine.
	link, err := upstream.New(upstream.Config{
		OriginURL: ts.URL,
		Bindings:  []upstream.Binding{{Prototype: &todov1.Todo{}}},
	})
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(dir, "local.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	srv, err := server.New(store, append([]server.Option{server.WithEntities(&todov1.Todo{})}, link.ServerOptions()...)...)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()
	if err := link.Start(ctx, srv); err != nil {
		log.Fatal(err)
	}
	defer link.Close()
	if err := link.AddWorkspace(ctx, "demo"); err != nil {
		log.Fatal(err)
	}

	w := link.Workspace("demo")
	receipt, err := w.Put(ctx, &todov1.Todo{Id: "t1", Title: "buy milk"})
	if err != nil {
		log.Fatal(err)
	}
	if err := receipt.Wait(ctx); err != nil {
		log.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		log.Fatal(err)
	}
	row, _, err := upstream.Get[*todov1.Todo](ctx, w, "t1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(row.Value.GetTitle())
	// Output: buy milk
}
