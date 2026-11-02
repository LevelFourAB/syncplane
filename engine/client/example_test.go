package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"

	"github.com/levelfourab/syncplane/engine/client"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// Example writes rows to a workspace on a remote server and reads them back.
// An in-process server stands in for the remote one.
func Example() {
	ctx := context.Background()
	url, stop := exampleServer()
	defer stop()

	c, err := client.New(url)
	if err != nil {
		log.Fatal(err)
	}
	ws := c.Workspace("acme")

	// Each write returns once the server has applied or rejected it.
	res, err := ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "Write the report"})
	if err != nil {
		log.Fatal(err)
	}

	// A compare-and-swap against the version the put left.
	_, err = ws.Patch(ctx, &todov1.Todo{Id: "t1", Completed: true}, []string{"completed"},
		client.WithExpectedVersion(res.Seq))
	if err != nil {
		log.Fatal(err)
	}

	// The same guard again is stale now, and the server rejects it.
	_, err = ws.Patch(ctx, &todov1.Todo{Id: "t1", Title: "Late edit"}, []string{"title"},
		client.WithExpectedVersion(res.Seq))
	var rej *client.RejectedError
	if errors.As(err, &rej) {
		fmt.Println("rejected:", rej.Code)
	}

	// Several operations as one atomic mutation.
	if _, err := ws.Apply(ctx,
		client.Put(&todov1.Todo{Id: "t2", Title: "Send the invoice"}),
		client.Delete(&todov1.Todo{}, "t1"),
	); err != nil {
		log.Fatal(err)
	}

	for row, err := range client.QueryAll[*todov1.Todo](ctx, ws, query.Spec{Where: query.Eq("completed", false)}) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(row.Key, row.Value.GetTitle())
	}
	// Output:
	// rejected: conflict
	// t2 Send the invoice
}

func exampleServer() (url string, stop func()) {
	dir, err := os.MkdirTemp("", "syncplane-client-example-*")
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		log.Fatal(err)
	}
	srv, err := server.New(store,
		server.WithEntities(&todov1.Todo{}),
		server.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		log.Fatal(err)
	}
	ts := httptest.NewServer(srv.HTTPHandler())
	return ts.URL, func() {
		ts.Close()
		_ = srv.Close()
		_ = store.Close()
		_ = os.RemoveAll(dir)
	}
}
