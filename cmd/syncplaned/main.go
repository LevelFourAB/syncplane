// syncplaned runs the sync engine as a multi-app host: a server.Manager routes
// the public sync surface by a leading "/<appID>" path segment, while a
// separate, private listener serves the trusted AdminService for runtime
// schema registration (gated by a bearer TokenAuthorizer).
//
// Apps have no compiled entities here — their schema arrives at runtime as a
// FileDescriptorSet over AdminService.Register, is persisted, and is reloaded
// on restart. The integration suite spawns this and reads both addresses from
// stdout.
//
// # Trust posture
//
// This binary is a demo/development host and belongs behind a trusted boundary.
// It installs no Authenticator and no Permissions, so every caller is anonymous
// and the default AllowAll admits them into any workspace of any app; the admin
// listener is gated only by its bearer token and must stay on a private
// interface.
//
// A production host embeds the same Manager and supplies both, sharing one
// authenticator across apps and giving each app its own policy:
//
//	server.WithServerOptions(server.WithAuthenticator(auth))
//	server.WithAppOptions(func(appID string) []server.Option { ... })
//
// Those seams are not exposed as flags here because a policy is code, not
// configuration.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/levelfourab/syncplane/engine/notify/pgnotify"
	"github.com/levelfourab/syncplane/engine/pglisten"
	"github.com/levelfourab/syncplane/engine/presence/pgpresence"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/otelstore"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/internal/admin"
	"github.com/levelfourab/syncplane/internal/clilog"
)

// isPostgresDSN reports whether -db names a PostgreSQL database rather than a
// SQLite file. The two are told apart by URI scheme because a Postgres DSN
// cannot be mistaken for a path and a path cannot be mistaken for a URI, which
// keeps one flag doing one job — and leaves every existing invocation meaning
// exactly what it did before.
func isPostgresDSN(db string) bool {
	return strings.HasPrefix(db, "postgres://") || strings.HasPrefix(db, "postgresql://")
}

func main() {
	var (
		dbPath       = flag.String("db", "", "SQLite database path, or a postgres:// DSN for multi-node hosting (default: temp file)")
		dbReplica    = flag.String("db-replica", "", "postgres:// DSN of a read replica of -db; requires a postgres -db")
		dbPartitions = flag.Int("db-partitions", 0, "hash-partition the workspace-scoped tables into this many partitions (a power of two, applied only when creating the schema); requires a postgres -db")
		dbMigrate    = flag.Bool("db-migrate", true, "apply outstanding migrations at boot; -db-migrate=false verifies the schema instead, for a deployment whose DDL is a deploy step; requires a postgres -db")
		addr         = flag.String("addr", "127.0.0.1:0", "public sync listen address")
		adminAddr    = flag.String("admin-addr", "127.0.0.1:0", "private admin listen address")
		token        = flag.String("token", "", "bearer token for AdminService (empty denies all registration)")
		heartbeat    = flag.Duration("heartbeat", 30*time.Second, "Subscribe stream heartbeat interval")
		notifyWindow = flag.Duration("notify-window", 50*time.Millisecond, "change-notification coalescing window (0 disables)")
		chunkSize    = flag.Int("chunk-size", 256, "bootstrap chunk size in rows")
		otelEnabled  = flag.Bool("otel", false, "export OpenTelemetry traces and metrics over OTLP (configured by the OTEL_EXPORTER_OTLP_* environment variables)")
		otelService  = flag.String("otel-service-name", "syncplaned", "service.name reported on exported telemetry")
	)
	buildLogger := clilog.Flags(flag.CommandLine)
	flag.Parse()
	logger := buildLogger()

	if *dbPath == "" {
		dir, err := os.MkdirTemp("", "syncplaned-*")
		if err != nil {
			clilog.Fatal(logger, "create temp dir", err)
		}
		defer os.RemoveAll(dir)
		*dbPath = filepath.Join(dir, "sync.db")
	}
	postgres := isPostgresDSN(*dbPath)

	// Off by default: an exporter that cannot reach a collector is noise, and a
	// demo host should run with no infrastructure at all. With the flag unset
	// the engine's instruments resolve to the API's no-ops.
	if *otelEnabled {
		shutdown, err := setupTelemetry(context.Background(), *otelService)
		if err != nil {
			clilog.Fatal(logger, "set up telemetry", err)
		}
		// Batched telemetry is lost unless the providers are flushed. This
		// defer only runs on a clean return, which the serve loops below never
		// take — a production host would drain it from a signal handler.
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				logger.Warn("flushing telemetry failed", "err", err)
			}
		}()
	}

	if *dbReplica != "" && !postgres {
		clilog.Fatal(logger, "configure store",
			fmt.Errorf("-db-replica requires -db to be a postgres:// DSN"))
	}
	if *dbPartitions != 0 && !postgres {
		clilog.Fatal(logger, "configure store",
			fmt.Errorf("-db-partitions requires -db to be a postgres:// DSN"))
	}
	if !*dbMigrate && !postgres {
		clilog.Fatal(logger, "configure store",
			fmt.Errorf("-db-migrate=false requires -db to be a postgres:// DSN"))
	}

	var store storage.Adapter
	if postgres {
		var pgOpts []pgstore.Option
		if *dbReplica != "" {
			pgOpts = append(pgOpts, pgstore.WithReadReplica(*dbReplica))
		}
		if *dbPartitions != 0 {
			pgOpts = append(pgOpts, pgstore.WithPartitions(*dbPartitions))
		}
		if !*dbMigrate {
			pgOpts = append(pgOpts, pgstore.WithoutMigrations())
		}
		pg, err := pgstore.Open(context.Background(), *dbPath, pgOpts...)
		if err != nil {
			clilog.Fatal(logger, "open store", err)
		}
		store = pg
	} else {
		sqlite, err := sqlitestore.Open(*dbPath)
		if err != nil {
			clilog.Fatal(logger, "open store", err)
		}
		store = sqlite
	}
	defer store.Close()

	// Wrapped once at the root: the Manager partitions this adapter per app
	// with a workspace-id prefix, which forwards to what it wraps, so every app
	// is instrumented by this single call.
	observed, err := otelstore.Wrap(store)
	if err != nil {
		clilog.Fatal(logger, "instrument store", err)
	}

	managerOpts := []server.ManagerOption{
		server.WithRegisterAuthorizer(server.TokenAuthorizer(*token)),
		server.WithServerOptions(
			server.WithHeartbeat(*heartbeat),
			server.WithNotifyCoalescing(*notifyWindow),
			server.WithChunkSize(*chunkSize),
			server.WithLogger(logger),
		),
	}

	// A Postgres store is the deployment that can have a second node, and on a
	// second node both in-process fan-outs stop reaching half the clients: the
	// notifier leaves them polling for changes, and the presence bus shows each
	// of them only the participants its own node is serving. The same database
	// already carries LISTEN/NOTIFY, so there is nothing further to configure
	// and no reason to make the operator ask for either.
	//
	// One of each for every hosted app: each app's server prefixes the
	// workspace ids it puts on them, so two apps that both have a workspace
	// "team" stay apart without a fan-out apiece.
	if postgres {
		// One transport for both: they carry different payloads on different
		// channels, but a LISTEN connection can hold both subscriptions and a
		// publisher can issue both notifications, which halves what this
		// process adds to the database's connection budget.
		link, err := pglisten.Open(context.Background(), *dbPath, pglisten.WithLogger(logger))
		if err != nil {
			clilog.Fatal(logger, "open notify transport", err)
		}
		defer link.Close()
		notifier, err := pgnotify.New(context.Background(), link)
		if err != nil {
			clilog.Fatal(logger, "open notifier", err)
		}
		defer notifier.Close()
		bus, err := pgpresence.New(context.Background(), link)
		if err != nil {
			clilog.Fatal(logger, "open presence bus", err)
		}
		defer bus.Close()
		managerOpts = append(managerOpts, server.WithServerOptions(
			server.WithNotifier(notifier),
			server.WithPresenceBus(bus),
		))
	}

	mgr, err := server.NewManager(context.Background(), observed, managerOpts...)
	if err != nil {
		clilog.Fatal(logger, "create manager", err)
	}
	defer mgr.Close()

	// Public surface: routed sync handler. Mounted at root so "/<appID>/..."
	// reaches the right app.
	syncLn, err := net.Listen("tcp", *addr)
	if err != nil {
		clilog.Fatal(logger, "listen (sync)", err)
	}
	syncMux := http.NewServeMux()
	syncMux.Handle("/", mgr.SyncHandler())

	// Private surface: the trusted admin service, on its own listener so it can
	// bind to a private interface.
	adminLn, err := net.Listen("tcp", *adminAddr)
	if err != nil {
		clilog.Fatal(logger, "listen (admin)", err)
	}
	adminPath, adminHandler := mgr.AdminHandler()
	adminMux := http.NewServeMux()
	adminMux.Handle(adminPath, adminHandler) // Connect control plane (schema registration)
	// Built-in admin web UI, on the same private listener. ServeMux
	// longest-prefix routing keeps them apart: Connect owns
	// "/syncplane.v1.AdminService/...", the UI owns "/".
	adminMux.Handle("/", admin.Handler(mgr))

	// Parseable by the integration harness; keep the format stable.
	fmt.Printf("SYNC http://%s\n", syncLn.Addr().String())
	fmt.Printf("ADMIN http://%s\n", adminLn.Addr().String())
	os.Stdout.Sync()

	go func() { clilog.Fatal(logger, "admin server stopped", http.Serve(adminLn, adminMux)) }()
	clilog.Fatal(logger, "sync server stopped", http.Serve(syncLn, syncMux))
}
