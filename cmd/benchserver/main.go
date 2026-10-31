// benchserver runs the sync engine with the syncplane.bench.v1 corpus for the
// browser benchmark suite (client/test/perf). It is the testserver's sibling:
// same permissive CORS, same LISTENING line, but registering the bench entities
// and carrying the endpoints a benchmark needs — seeding a corpus server-side,
// driving writes that a measured client then has to catch up with, and
// reporting what crossed the wire.
//
// It is not a test server. Nothing here asserts anything; every endpoint exists
// to put the system into a state whose cost the browser then measures.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/benchapp"
	"github.com/levelfourab/syncplane/internal/clilog"
)

func main() {
	var (
		dbPath    = flag.String("db", "", "SQLite database path, or a postgres:// DSN (default: temp file)")
		dbReset   = flag.Bool("db-reset", false, "drop and recreate the database a postgres:// -db names before opening it")
		addr      = flag.String("addr", "127.0.0.1:0", "listen address")
		heartbeat = flag.Duration("heartbeat", 30*time.Second, "Subscribe stream heartbeat interval")
		// The engine's default coalescing window would fold a benchmark's
		// server-side write into a later frame and count the delay as sync
		// latency. Benchmarks that want coalescing set it explicitly.
		notifyWindow = flag.Duration("notify-window", 0, "change-notification coalescing window (0 disables)")
		// Bootstrap paging: the default is the engine's, because how a corpus
		// pages is part of what the bootstrap benchmarks are measuring.
		chunkSize = flag.Int("chunk-size", 0, "bootstrap chunk size in rows (0 = engine default)")
		// Read by the harness before it decides whether a cached corpus is
		// still the corpus this build would generate.
		printVersion = flag.Bool("corpus-version", false, "print the corpus generator version and exit")
	)
	buildLogger := clilog.Flags(flag.CommandLine)
	flag.Parse()
	if *printVersion {
		fmt.Println(benchapp.GeneratorVersion)
		return
	}
	logger := buildLogger()

	var store storage.Adapter
	if isPostgresDSN(*dbPath) {
		if *dbReset {
			if err := resetDatabase(context.Background(), *dbPath); err != nil {
				clilog.Fatal(logger, "reset database", err)
			}
		}
		pg, err := pgstore.Open(context.Background(), *dbPath)
		if err != nil {
			clilog.Fatal(logger, "open store", err)
		}
		defer pg.Close()
		store = pg
	} else {
		if *dbReset {
			clilog.Fatal(logger, "flags", fmt.Errorf("-db-reset requires -db to be a postgres:// DSN"))
		}
		if *dbPath == "" {
			dir, err := os.MkdirTemp("", "syncbench-*")
			if err != nil {
				clilog.Fatal(logger, "create temp dir", err)
			}
			defer os.RemoveAll(dir)
			*dbPath = filepath.Join(dir, "bench.db")
		}
		sqlite, err := sqlitestore.Open(*dbPath)
		if err != nil {
			clilog.Fatal(logger, "open store", err)
		}
		defer sqlite.Close()
		store = sqlite
	}

	opts := append(benchapp.Options(),
		server.WithHeartbeat(*heartbeat),
		server.WithNotifyCoalescing(*notifyWindow),
		server.WithLogger(logger),
		// Configured but never scheduled: the re-bootstrap benchmark advances
		// the horizon by asking for a pass at the moment it wants one, and a
		// timer firing mid-measurement would be noise in every other run.
		server.WithGC(time.Millisecond, 0),
	)
	if *chunkSize > 0 {
		opts = append(opts, server.WithChunkSize(*chunkSize))
	}
	srv, err := server.New(store, opts...)
	if err != nil {
		clilog.Fatal(logger, "create server", err)
	}
	defer srv.Close()

	stats := newWireStats()
	mux := http.NewServeMux()
	for path, handler := range srv.Handlers() {
		mux.Handle(path, stats.wrap(handler))
	}
	mux.Handle("/debug/stats", stats)
	mux.Handle("/debug/tiers", tiersHandler())
	mux.Handle("/debug/seed", seedHandler(srv, logger))
	mux.Handle("/debug/seed-search", seedSearchHandler(srv))
	mux.Handle("/debug/write", writeHandler(srv))
	mux.Handle("/debug/gc", gcHandler(srv))
	// Go's profiler, for attributing a server-side number the stats endpoint
	// can only size: `go tool pprof <url>/debug/pprof/profile?seconds=30`
	// while a scenario runs.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		clilog.Fatal(logger, "listen", err)
	}
	// Parsed by client/test/support/benchserver.ts; keep the format stable.
	fmt.Printf("LISTENING http://%s\n", ln.Addr().String())
	os.Stdout.Sync()

	clilog.Fatal(logger, "http server stopped", http.Serve(ln, cors(mux)))
}

// isPostgresDSN tells a Postgres DSN from a SQLite path by URI scheme, exactly
// as syncplaned does for its -db flag.
func isPostgresDSN(db string) bool {
	return strings.HasPrefix(db, "postgres://") || strings.HasPrefix(db, "postgresql://")
}

// resetDatabase drops and recreates the database `dsn` names, connecting to the
// server's maintenance database with the same credentials. A benchmark run
// writes to its corpus, so each run starts from an empty database it then seeds,
// the way a SQLite run starts from a fresh copy of the cached file.
func resetDatabase(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	name := cfg.Database
	if name == "" || name == "postgres" {
		return fmt.Errorf("-db-reset refuses to reset database %q", name)
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoted+" WITH (FORCE)"); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "CREATE DATABASE "+quoted)
	return err
}

// cors is permissive because this binary only ever serves the benchmark
// harness, which Vite serves from a different port — so every Connect call from
// it is cross-origin.
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
