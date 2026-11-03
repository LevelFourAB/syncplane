package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream"
	"github.com/levelfourab/syncplane/internal/clilog"
)

// runForward serves the forwarding mode (-forward): an origin engine with the
// default schema, and a local engine linked to it through engine/upstream over
// loopback HTTP, so the integration suite can sync a TS client against the
// local engine while other writers use the origin.
//
// The origin listens on an address of its own, printed as "ORIGIN http://…"
// before the LISTENING line, and serves the same services and debug endpoints
// as the default mode. The LISTENING address is the local engine. It declares
// todo and widget only, both bound to the origin in the link's default write
// mode (write-behind), with no permission policy and no mutators: the origin
// alone rejects a todo titled "forbidden", and runs complete_all and the other
// example mutators, which the local engine forwards. Besides its services the
// local listener serves /debug/rpc and the /debug/upstream/ endpoints (see
// forwarder.mount). The tunnel listener answers with the local engine.
func runForward(
	logger *slog.Logger, originStore *sqlitestore.Store, localPath string,
	originOpts, localOpts []server.Option, addr, tunnelAddr string,
) {
	origin, err := server.New(originStore, originOpts...)
	if err != nil {
		clilog.Fatal(logger, "create origin server", err)
	}
	defer origin.Close()
	originMux := http.NewServeMux()
	mountEngine(originMux, origin, newRPCStats())

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		clilog.Fatal(logger, "parse listen address", err)
	}
	oln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		clilog.Fatal(logger, "listen for the origin", err)
	}
	originURL := "http://" + oln.Addr().String()
	go func() { clilog.Fatal(logger, "origin http server stopped", http.Serve(oln, cors(originMux))) }()

	f := &forwarder{
		logger:     logger,
		originURL:  originURL,
		localPath:  localPath,
		localOpts:  slices.Concat(localOpts, []server.Option{server.WithEntities(&todov1.Todo{}, &testdatav1.Widget{})}),
		net:        newPartition(),
		stats:      newRPCStats(),
		rejections: map[string][]upstream.Rejection{},
	}
	if err := f.start(); err != nil {
		clilog.Fatal(logger, "start local engine", err)
	}
	defer f.stop()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		clilog.Fatal(logger, "listen", err)
	}
	f.localURL = "http://" + ln.Addr().String()
	mux := http.NewServeMux()
	f.mount(mux)

	if tunnelAddr != "" {
		tln, err := net.Listen("tcp", tunnelAddr)
		if err != nil {
			clilog.Fatal(logger, "listen for tunnels", err)
		}
		go serveTunnels(tln, mux)
		fmt.Printf("TUNNEL tcp://%s\n", tln.Addr().String())
	}
	fmt.Printf("ORIGIN %s\n", originURL)
	// Parseable by the integration harness; keep the format stable.
	fmt.Printf("LISTENING %s\n", f.localURL)
	os.Stdout.Sync()

	clilog.Fatal(logger, "http server stopped", http.Serve(ln, cors(mux)))
}

// forwarder owns the local engine and its link, which /debug/upstream/restart
// replaces with a fresh pair over the same store file.
type forwarder struct {
	logger    *slog.Logger
	originURL string
	localURL  string
	localPath string
	localOpts []server.Option
	net       *partition
	// stats outlives restarts, so its counts cover every engine instance.
	stats *rpcStats

	// handler serves the current engine's services.
	handler atomic.Pointer[http.Handler]

	// mu serializes start, stop and restart, and guards the fields below.
	mu    sync.Mutex
	store *sqlitestore.Store
	srv   *server.Server
	link  *upstream.Link

	rmu        sync.Mutex
	rejections map[string][]upstream.Rejection
}

// start opens the store, builds the engine with the link installed and
// starts the link. The caller holds mu, or nothing else runs yet.
func (f *forwarder) start() error {
	link, err := upstream.New(upstream.Config{
		OriginURL:  f.originURL,
		HTTPClient: &http.Client{Transport: f.net},
		Bindings: []upstream.Binding{
			{Prototype: &todov1.Todo{}},
			{Prototype: &testdatav1.Widget{}},
		},
		OnRejected: func(r upstream.Rejection) {
			f.rmu.Lock()
			defer f.rmu.Unlock()
			f.rejections[r.Workspace] = append(f.rejections[r.Workspace], r)
		},
		// A heal must show within a test's patience; the default backoff
		// waits up to 30 seconds.
		RetryMin: 20 * time.Millisecond,
		RetryMax: 200 * time.Millisecond,
		Logger:   f.logger,
	})
	if err != nil {
		return err
	}
	store, err := sqlitestore.Open(f.localPath)
	if err != nil {
		return err
	}
	srv, err := server.New(store, slices.Concat(f.localOpts, link.ServerOptions())...)
	if err != nil {
		_ = store.Close()
		return err
	}
	if err := link.Start(context.Background(), srv); err != nil {
		_ = srv.Close()
		_ = store.Close()
		return err
	}
	mux := http.NewServeMux()
	for path, h := range srv.Handlers() {
		mux.Handle(path, f.stats.wrap(h))
	}
	var h http.Handler = mux
	f.handler.Store(&h)
	f.store, f.srv, f.link = store, srv, link
	return nil
}

// stop closes the link, the engine and the store, in that order.
func (f *forwarder) stop() {
	_ = f.link.Close()
	_ = f.srv.Close()
	_ = f.store.Close()
}

func (f *forwarder) currentLink() *upstream.Link {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.link
}

// mount serves the local engine and these endpoints on mux:
//
//   - GET /debug/upstream/info: {"origin", "local"} base URLs.
//   - POST /debug/upstream/link {"workspace"}: link a workspace. The local
//     engine refuses a write to a workspace that is not linked.
//   - POST /debug/upstream/partition and /debug/upstream/heal: cut the link's
//     connections to the origin, in-flight calls and the live stream
//     included, and refuse new ones; then allow them again.
//   - POST /debug/upstream/flush {"workspace", "timeoutMs"}: Link.Flush; 504
//     when the timeout (default 10s) passes first.
//   - GET /debug/upstream/status?workspace=: the link's status for the
//     workspace and every rejection reported for it since the process started.
//   - POST /debug/upstream/restart: close the link and the local engine and
//     start fresh ones over the same store file.
func (f *forwarder) mount(mux *http.ServeMux) {
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(*f.handler.Load()).ServeHTTP(w, r)
	}))
	mux.Handle("/debug/rpc", f.stats)
	mux.HandleFunc("GET /debug/upstream/info", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"origin": f.originURL, "local": f.localURL})
	})
	mux.HandleFunc("POST /debug/upstream/link", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Workspace string `json:"workspace"`
		}
		if !readJSON(w, r, &req) {
			return
		}
		if err := f.currentLink().AddWorkspace(r.Context(), req.Workspace); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /debug/upstream/partition", func(w http.ResponseWriter, _ *http.Request) {
		f.net.cut()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /debug/upstream/heal", func(w http.ResponseWriter, _ *http.Request) {
		f.net.heal()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /debug/upstream/flush", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Workspace string `json:"workspace"`
			TimeoutMs int    `json:"timeoutMs"`
		}
		if !readJSON(w, r, &req) {
			return
		}
		timeout := 10 * time.Second
		if req.TimeoutMs > 0 {
			timeout = time.Duration(req.TimeoutMs) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		err := f.currentLink().Flush(ctx, req.Workspace)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("GET /debug/upstream/status", func(w http.ResponseWriter, r *http.Request) {
		ws := r.URL.Query().Get("workspace")
		type rejection struct {
			Code    string            `json:"code"`
			Message string            `json:"message"`
			Mutator string            `json:"mutator"`
			Rows    []upstream.RowRef `json:"rows"`
		}
		out := struct {
			Linked     bool        `json:"linked"`
			State      string      `json:"state"`
			Pending    int         `json:"pending"`
			OriginSeq  uint64      `json:"originSeq"`
			LastError  string      `json:"lastError"`
			Rejections []rejection `json:"rejections"`
		}{Rejections: []rejection{}}
		if st, ok := f.currentLink().Status(ws); ok {
			out.Linked = true
			out.State = st.State.String()
			out.Pending = st.Pending
			out.OriginSeq = st.OriginSeq
			if st.LastError != nil {
				out.LastError = st.LastError.Error()
			}
		}
		f.rmu.Lock()
		for _, r := range f.rejections[ws] {
			out.Rejections = append(out.Rejections, rejection{r.Code, r.Message, r.Mutator, r.Rows})
		}
		f.rmu.Unlock()
		writeJSON(w, out)
	})
	mux.HandleFunc("POST /debug/upstream/restart", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.stop()
		if err := f.start(); err != nil {
			// The engine is gone and every later call would fail obscurely.
			clilog.Fatal(f.logger, "restart local engine", err)
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// errPartitioned is what a call to the origin fails with while the link is
// partitioned.
var errPartitioned = errors.New("testserver: link to the origin is partitioned")

// partition is the link's HTTP transport to the origin, with a switch that
// cuts it. Cutting cancels every call in flight, a live stream's included,
// and fails new calls before they dial, which is how a lost network looks to
// the link.
type partition struct {
	base *http.Transport

	mu   sync.Mutex
	down bool
	// gen ends when the current connections are cut.
	gen    context.Context
	cancel context.CancelFunc
}

func newPartition() *partition {
	p := &partition{base: http.DefaultTransport.(*http.Transport).Clone()}
	p.gen, p.cancel = context.WithCancel(context.Background())
	return p
}

func (p *partition) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	p.cancel()
}

func (p *partition) heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.down {
		return
	}
	p.down = false
	p.gen, p.cancel = context.WithCancel(context.Background())
}

func (p *partition) RoundTrip(req *http.Request) (*http.Response, error) {
	p.mu.Lock()
	down, gen := p.down, p.gen
	p.mu.Unlock()
	if down {
		return nil, errPartitioned
	}
	ctx, cancel := context.WithCancel(req.Context())
	// AfterFunc runs at once when gen ended between the check and here.
	stop := context.AfterFunc(gen, cancel)
	res, err := p.base.RoundTrip(req.WithContext(ctx))
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	res.Body = &cutBody{ReadCloser: res.Body, done: func() { stop(); cancel() }}
	return res, nil
}

// cutBody releases a call's cut hook when its body is closed.
type cutBody struct {
	io.ReadCloser
	once sync.Once
	done func()
}

func (b *cutBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}
