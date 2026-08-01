package server_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// Example is the whole server side of a sync app: open a store, declare the
// schema and the server-authoritative mutations, serve the handler. A client
// pushes a mutation and pulls the result back, standing in for the TypeScript
// client's background coordinator.
func Example() {
	dir, err := os.MkdirTemp("", "syncplane-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// The store is the caller's to close: it may outlive the server, and one
	// adapter can back several of them.
	store, err := sqlitestore.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	srv, err := server.New(store,
		// Entities are ordinary proto messages carrying (syncplane.v1.entity).
		server.WithEntities(&todov1.Todo{}),
		// A mutator is a named write the server owns: clients ask for it by
		// name, and it runs inside the push transaction.
		server.WithMutator("complete_all", &todov1.CompleteAllArgs{},
			func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
				var todos []*todov1.Todo
				if err := tx.Scan(&todov1.Todo{}, func(msg proto.Message) error {
					todos = append(todos, msg.(*todov1.Todo))
					return nil
				}); err != nil {
					return err
				}
				for _, todo := range todos {
					todo.Completed = true
					if err := tx.Put(todo); err != nil {
						return err
					}
				}
				return nil
			}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// In a real main this is http.ListenAndServe(":8787", srv.HTTPHandler()).
	ts := httptest.NewServer(srv.HTTPHandler())
	defer ts.Close()
	client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

	ctx := context.Background()
	value, err := proto.Marshal(&todov1.Todo{Id: "t1", Title: "buy milk"})
	if err != nil {
		log.Fatal(err)
	}
	args, err := anypb.New(&todov1.CompleteAllArgs{})
	if err != nil {
		log.Fatal(err)
	}
	// One workspace batch: create a todo, then complete every todo. Each
	// mutation carries the client's own monotonic id, which is how the server
	// acks it exactly once however often the client retries.
	if _, err := client.Push(ctx, connect.NewRequest(&syncplanev1.PushRequest{
		Batches: []*syncplanev1.WorkspaceBatch{{
			WorkspaceId: "demo",
			Mutations: []*syncplanev1.Mutation{
				{ClientId: "c1", MutationId: 1, Payload: &syncplanev1.Mutation_Crud{
					Crud: &syncplanev1.CrudMutation{
						Entity: "todo", Key: "t1",
						Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value,
					},
				}},
				{ClientId: "c1", MutationId: 2, Payload: &syncplanev1.Mutation_Custom{
					Custom: &syncplanev1.CustomMutation{Name: "complete_all", Args: args},
				}},
			},
		}},
	})); err != nil {
		log.Fatal(err)
	}

	// Pull from an empty cursor: the client's first read of the workspace.
	resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: "demo"}))
	if err != nil {
		log.Fatal(err)
	}
	for _, change := range resp.Msg.GetPatch().GetChanges() {
		var todo todov1.Todo
		if err := proto.Unmarshal(change.GetValue(), &todo); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %q completed=%t\n", change.GetKey(), todo.GetTitle(), todo.GetCompleted())
	}
	// Output:
	// t1: "buy milk" completed=true
}

// ExampleServer_Handler mounts the sync service alongside an app's own routes.
// HTTPHandler is the shortcut for when it is the only thing on the listener.
func ExampleServer_Handler() {
	srv, err := server.New(newExampleStore(), server.WithEntities(&todov1.Todo{}))
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	mux := http.NewServeMux()
	path, handler := srv.Handler()
	mux.Handle(path, handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	fmt.Println(path)
	// Output:
	// /syncplane.v1.SyncService/
}

// newExampleStore opens a throwaway store for the examples that only need a
// server to exist.
func newExampleStore() *sqlitestore.Store {
	dir, err := os.MkdirTemp("", "syncplane-example-*")
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		log.Fatal(err)
	}
	return store
}
