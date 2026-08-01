// todoserver backs the examples/todo web app: the sync engine with the todo
// entity, the complete_all mutator, the example permission policy, and
// permissive CORS so the Vite dev server can reach it.
package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/clilog"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

// cors is permissive on purpose — demo only. Wildcard headers cover the
// Connect protocol headers (Connect-Protocol-Version, Connect-Timeout-Ms).
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
		dbPath      = flag.String("db", "todo.db", "SQLite database path")
		addr        = flag.String("addr", "127.0.0.1:8787", "listen address")
		heartbeat   = flag.Duration("heartbeat", 30*time.Second, "Subscribe stream heartbeat interval")
		gcRetention = flag.Duration("gc-retention", 30*24*time.Hour, "tombstone retention")
		gcInterval  = flag.Duration("gc-interval", time.Hour, "tombstone GC interval (0 disables the background pass)")
	)
	buildLogger := clilog.Flags(flag.CommandLine)
	flag.Parse()
	logger := buildLogger()

	store, err := sqlitestore.Open(*dbPath)
	if err != nil {
		clilog.Fatal(logger, "open store", err)
	}
	defer store.Close()

	perms := exampleapp.NewPermissions()
	srv, err := server.New(store, append(exampleapp.Options(perms),
		server.WithHeartbeat(*heartbeat),
		server.WithPermissions(perms),
		server.WithGC(*gcRetention, *gcInterval),
		server.WithLogger(logger),
	)...)
	if err != nil {
		clilog.Fatal(logger, "create server", err)
	}
	defer srv.Close()

	logger.Info("todoserver listening", "addr", *addr, "db", *dbPath)
	clilog.Fatal(logger, "http server stopped", http.ListenAndServe(*addr, cors(srv.HTTPHandler())))
}
