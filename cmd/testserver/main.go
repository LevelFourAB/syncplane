// testserver runs the sync engine with the test entities (Todo, Widget),
// the complete_all mutator and a deny-rule permission hook. The integration
// suites spawn it on port 0 and read the address from stdout. With -access it
// serves the access-model test schema instead, and /debug/trusted to seed its
// grant rows.
//
// It also serves permissive CORS and a /debug/rpc counter endpoint so the
// browser e2e suite can reach it from the harness origin and assert what did
// (and did not) reach the server; see debug.go.
//
// Blobs are kept in memory, so BlobService works in either schema; the
// default one carries the entities that refer to them (blob_holder,
// blob_tree, blob_ledger).
//
// A second listener answers each TCP connection as a tunnel (engine/tunnel)
// to the same handlers, and its address is printed as "TUNNEL tcp://…" before
// the LISTENING line.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/levelfourab/syncplane/engine/blob/memstore"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/tunnel"
	"github.com/levelfourab/syncplane/internal/clilog"
	"github.com/levelfourab/syncplane/internal/exampleapp"
)

func main() {
	var (
		dbPath       = flag.String("db", "", "SQLite database path (default: temp file)")
		addr         = flag.String("addr", "127.0.0.1:0", "listen address")
		heartbeat    = flag.Duration("heartbeat", 30*time.Second, "Subscribe stream heartbeat interval")
		notifyWindow = flag.Duration("notify-window", 50*time.Millisecond, "change-notification coalescing window (0 disables)")
		chunkSize    = flag.Int("chunk-size", 256, "bootstrap chunk size in rows")
		gcRetention  = flag.Duration("gc-retention", 0, "tombstone retention (used with -gc-interval)")
		gcInterval   = flag.Duration("gc-interval", 0, "tombstone GC interval (0 disables GC)")
		gcTextDelta  = flag.Duration("gc-text-delta-retention", time.Millisecond,
			"collaborative-text delta log retention; the default makes a write compactable as soon as a suite asks")
		roomAnnounce = flag.Duration("room-announce", 500*time.Millisecond,
			"presence re-announce interval; a record nobody re-announces expires after three of these")
		gcTextPurge = flag.Int("gc-text-compaction", 1,
			"items a text field must give up before compaction rewrites its row (0 disables)")
		access = flag.Bool("access", false,
			"serve the access-model test schema instead of the default one; see access.go")
		maxAccessDepth = flag.Int("max-access-depth", 0,
			"Limits.MaxAccessDepth: the most access parent hops a permission reaches down (0 is the default)")
		maxBlobBytes = flag.Int64("max-blob-bytes", 0,
			"Limits.MaxBlobBytes: the largest blob BeginUpload accepts (0 is the default, 4 GiB)")
		blobRetention = flag.Duration("blob-retention", 0,
			"how long blob GC keeps an unreferenced blob or a staged upload (0 is the default, 30 days); /debug/gc runs a pass")
		tunnelAddr = flag.String("tunnel-addr", "127.0.0.1:0",
			"listen address for tunnel connections, each a byte stream of length-prefixed frames (empty disables)")
	)
	buildLogger := clilog.Flags(flag.CommandLine)
	flag.Parse()
	logger := buildLogger()

	if *dbPath == "" {
		dir, err := os.MkdirTemp("", "synctest-*")
		if err != nil {
			clilog.Fatal(logger, "create temp dir", err)
		}
		defer os.RemoveAll(dir)
		// The suites stop the server with a signal, and the serve loop never
		// returns, so the defer above does not run. Without this every run
		// leaves its database behind in the temp directory.
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
		go func() {
			<-stop
			os.RemoveAll(dir)
			os.Exit(0)
		}()
		*dbPath = filepath.Join(dir, "sync.db")
	}

	store, err := sqlitestore.Open(*dbPath)
	if err != nil {
		clilog.Fatal(logger, "open store", err)
	}
	defer store.Close()

	opts := []server.Option{
		server.WithHeartbeat(*heartbeat),
		server.WithRoomAnnounce(*roomAnnounce),
		server.WithNotifyCoalescing(*notifyWindow),
		server.WithChunkSize(*chunkSize),
		server.WithLogger(logger),
		server.WithLimits(server.Limits{MaxAccessDepth: *maxAccessDepth, MaxBlobBytes: *maxBlobBytes}),
		server.WithBlobStore(memstore.New()),
		server.WithBlobRetention(*blobRetention),
	}
	if *access {
		opts = append(opts, accessOptions()...)
	} else {
		opts = append(opts, defaultOptions()...)
	}
	if *gcInterval > 0 || *gcRetention > 0 {
		opts = append(opts, server.WithGC(*gcRetention, *gcInterval))
	} else {
		// Nothing collects on its own (interval 0), but /debug/gc can run a pass
		// when a suite asks; the retention is short so the suite's own writes are
		// in reach of it.
		opts = append(opts, server.WithGC(time.Millisecond, 0))
	}
	opts = append(opts,
		server.WithTextDeltaRetention(*gcTextDelta),
		server.WithTextCompaction(*gcTextPurge),
		// One record per write, rather than the five-minute default that folds a
		// run of writes to one row into one. A suite's writes all land inside any
		// realistic window and under one actor, so the default would collapse
		// every history spec to a single record and leave the per-write shape —
		// which mask a write records, which before-state, how a restore reads it
		// — unobservable from this side. Folding is decided entirely on the Go
		// write path and is covered by the engine's own suite.
		server.WithHistorySession(server.HistorySessionOff),
		// Write attribution needs an identity to attribute to, and a suite has
		// to be able to choose one per spec. A bearer token is taken at face
		// value as the subject — nothing is verified, which is only tolerable
		// because this binary exists to be driven by the suites. A request
		// without one stays anonymous, which is what every other spec expects.
		server.WithAuthenticator(server.AuthenticatorFunc(
			func(_ context.Context, h http.Header) (server.Principal, error) {
				subject, ok := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
				if !ok || subject == "" {
					return server.Principal{}, nil
				}
				return server.Principal{Subject: subject}, nil
			})),
	)
	srv, err := server.New(store, opts...)
	if err != nil {
		clilog.Fatal(logger, "create server", err)
	}
	defer srv.Close()

	stats := newRPCStats()
	mux := http.NewServeMux()
	// Every service, each behind the stats wrapper: it keys on the last path
	// segment, so mounting the presence service here is what makes Watch and
	// Publish visible to /debug/rpc.
	for path, handler := range srv.Handlers() {
		mux.Handle(path, stats.wrap(handler))
	}
	// Server-side view of what the clients are doing, for the multi-tab e2e
	// suite; see rpcStats.
	mux.Handle("/debug/rpc", stats)
	// Trusted server-side string writes to Widget text fields, so the
	// integration suite can exercise cross-language convergence of the
	// string-boundary write path.
	mux.Handle("/debug/textstring", textStringHandler(srv))
	// One garbage-collection pass on demand, so suites can decide when
	// collaborative-text compaction happens.
	mux.Handle("/debug/gc", gcHandler(srv))
	// Workspace deletion on demand, so suites can watch a syncing client meet
	// the denial a deleted workspace answers with.
	mux.Handle("/debug/delete-workspace", deleteWorkspaceHandler(srv))
	if *access {
		mux.Handle("/debug/trusted", trustedWriteHandler(srv))
	}

	if *tunnelAddr != "" {
		// The same mux over the tunnel, so a suite can drive one client through
		// it and another over HTTP against the same store.
		tln, err := net.Listen("tcp", *tunnelAddr)
		if err != nil {
			clilog.Fatal(logger, "listen for tunnels", err)
		}
		go serveTunnels(tln, mux)
		// Printed before LISTENING, so it is in the output by the time the
		// harness has seen that line.
		fmt.Printf("TUNNEL tcp://%s\n", tln.Addr().String())
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		clilog.Fatal(logger, "listen", err)
	}
	// Parseable by the integration harness; keep the format stable.
	fmt.Printf("LISTENING http://%s\n", ln.Addr().String())
	os.Stdout.Sync()

	clilog.Fatal(logger, "http server stopped", http.Serve(ln, cors(mux)))
}

// serveTunnels answers every connection on ln as one tunnel to h.
func serveTunnels(ln net.Listener, h http.Handler) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() { _ = tunnel.Serve(context.Background(), tunnel.NewStreamConn(conn), h) }()
	}
}

// defaultOptions is the schema and permission policy every suite but the
// access-model one runs against.
func defaultOptions() []server.Option {
	perms := exampleapp.NewPermissions()
	return append(exampleapp.Options(perms),
		server.WithPermissions(perms),
		server.WithEntities(

			// Widget.body carries (syncplane.v1.field_behavior).text, so
			// registration auto-installs the convergent text merger —
			// integration tests verify that concurrent PATCHes to the same text
			// field merge without interleaving instead of last-writer-wins
			// clobbering.
			&testdatav1.Widget{},
			// Composite-key entity: integration tests verify both sides derive
			// the same escaped row key when key fields contain the "/" separator.
			&testdatav1.Pair{},
			// Reference entities: integration tests verify typed-reference
			// projection and reference-following subscriptions (issue→project,
			// project→issues).
			&testdatav1.Project{}, &testdatav1.User{}, &testdatav1.Issue{},
			// Task holds workspace-qualified references (UserRef): integration
			// tests verify a stub naming another workspace round-trips, and
			// that the server's follow and matches() stay inside one workspace.
			&testdatav1.Task{},
			// Polymorphic references: a oneof of targets on the holder
			// (OneofRefEntity), inside a reference message (Memo's OwnerRef) and
			// a holder oneof mixing both with a plain member (Attachment).
			&testdatav1.OneofRefEntity{}, &testdatav1.Memo{}, &testdatav1.Attachment{},
			// Historied opts into field history (and carries a collaborative
			// text field), which also brings the engine's server-only history
			// entity into the catalog: integration tests read it back through
			// queryServer and restore from it with an ordinary PATCH.
			&testdatav1.Historied{},
			// Guarded keeps fields from every client with a field guard, and
			// exampleapp's policy protects one more for the subject "viewer":
			// integration tests verify that a PUT keeps the stored value of a
			// guarded field and that the half only the server knows arrives as
			// a rejection ack.
			&testdatav1.Guarded{},
			// Article declares full-text search (a searchable mark plus an
			// EntityOptions.search spec): integration tests verify a match
			// predicate and a relevance ordering return the same rows in the
			// same order locally and through queryServer, and that cursors
			// minted by one side page the other.
			&testdatav1.Article{},
			// Collaborative text inside keyed-list elements and map values:
			// integration tests verify that both sides address one document
			// per element by instance path ("notes.<id>.body") and merge
			// concurrent edits element by element.
			&testdatav1.KeyedTextEntity{},
			// Rows that refer to blobs: blob_holder from each place an index
			// reaches (photo, album.cover, files, pages[].scan, by_name), with
			// rows hidden while `hidden` is set and `secret` read only by the
			// subject `owner_id` names; blob_tree from places only a row scan
			// reaches; blob_ledger is server-only, so no client reads a blob
			// through its `receipt`. Integration tests verify which blobs
			// BlobService serves to which caller.
			&testdatav1.BlobHolder{}, &testdatav1.BlobTree{}, &testdatav1.BlobLedger{},
		),
	)
}
