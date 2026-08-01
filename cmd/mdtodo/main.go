// mdtodo demonstrates layering the sync engine over an external source of
// truth: the todo app's entities live as Markdown files in a folder, kept
// bidirectionally converged with the engine by a connect.Connector.
//
// Run it next to the todo web app: client edits materialize as .md files;
// editing, adding or deleting a file shows up in every connected client.
// External edits are picked up by the periodic reconcile (-reconcile); a
// file watcher could call Connector.Kick instead for immediacy.
package main

import (
	"context"
	"flag"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/connect/sqlitestate"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/clilog"
)

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	var (
		dbPath    = flag.String("db", "mdtodo.db", "engine SQLite database path")
		statePath = flag.String("state", "mdtodo-connector.db", "connector state database path")
		dir       = flag.String("dir", "todos", "folder the Markdown files live in")
		addr      = flag.String("addr", "127.0.0.1:8787", "listen address")
		reconcile = flag.Duration("reconcile", 2*time.Second, "how often to scan the folder for external edits")
	)
	buildLogger := clilog.Flags(flag.CommandLine)
	flag.Parse()
	logger := buildLogger()

	store, err := sqlitestore.Open(*dbPath)
	if err != nil {
		clilog.Fatal(logger, "open store", err)
	}
	defer store.Close()

	// The shared notes document syncs as a plain Markdown file per note
	// (<dir>/<ws>/notes/<id>.md); external edits merge with concurrent
	// client edits through the text CRDT. Annotations stay engine-side.
	srv, err := server.New(store,
		server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}),
		server.WithLogger(logger))
	if err != nil {
		clilog.Fatal(logger, "create server", err)
	}
	defer srv.Close()

	state, err := sqlitestate.Open(*statePath)
	if err != nil {
		clilog.Fatal(logger, "open connector state", err)
	}
	conn, err := connect.New(connect.Config{
		Server:            srv,
		Source:            &mdSource{root: *dir},
		State:             state,
		Entities:          []proto.Message{&todov1.Todo{}, &todov1.Note{}},
		ReconcileInterval: *reconcile,
		Logger:            logger,
		// One folder is served by one process, so the folder identifies the
		// writer: stable across restarts (no dead peer per run in every note
		// document) and distinct between two mdtodo instances on two folders.
		WriterID: "mdtodo:" + *dir,
	})
	if err != nil {
		clilog.Fatal(logger, "create connector", err)
	}
	defer conn.Close()
	if err := conn.Start(context.Background()); err != nil {
		clilog.Fatal(logger, "start connector", err)
	}
	// Make "wrong database" failures obvious: sqlitestore.Open silently
	// creates an empty database at a fresh path, which would leave the
	// connector with nothing to export.
	if ids, err := srv.WorkspaceIDs(context.Background()); err == nil {
		if len(ids) == 0 {
			logger.Warn("no workspaces yet — if you expected existing data, check the -db path (a bare file copy misses the -wal)", "db", *dbPath)
		} else {
			logger.Info("syncing workspaces", "count", len(ids), "workspaces", ids)
		}
	}

	path, handler := srv.Handler()
	mux := http.NewServeMux()
	mux.Handle(path, handler)

	logger.Info("mdtodo listening", "addr", *addr, "db", *dbPath, "files", *dir)
	clilog.Fatal(logger, "http server stopped", http.ListenAndServe(*addr, cors(mux)))
}
