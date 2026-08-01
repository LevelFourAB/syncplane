package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// cors is permissive on purpose — this binary only ever serves test suites.
// The browser e2e harness is served by Vite on a different port, so every
// Connect call from it is cross-origin; wildcard headers cover the protocol
// headers the web client sends (Connect-Protocol-Version, Connect-Timeout-Ms)
// and the ones it reads back off error responses.
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

// rpcStats counts Connect calls by bare method name ("Push", "Subscribe", ...),
// exposed as JSON at /debug/rpc.
//
// The browser e2e suite asserts cross-tab invariants that no single tab can
// observe: that an account holds exactly ONE Subscribe stream however many tabs
// it has open, that leadership moves to a peer when the holder's tab hides, and
// that a reloaded tab reads its existing IndexedDB store instead of
// re-bootstrapping. Each of those is a statement about what does or does not
// reach the server, so the server is what counts it.
//
// `active` holds calls in flight, which for a server-streaming method means the
// life of the stream — that is what makes leader election visible. `total` only
// grows, so a test can assert that something did NOT happen across an interval.
//
// `bytesIn`/`bytesOut` are the wire sizes of the request and response bodies,
// which is what makes a payload-size claim testable: the delta-state text
// suites assert that an edit to a large document ships a small PATCH, and no
// client-side measurement could tell a small request from a small request the
// server ignored.
//
// `pushBytesIn` and `pullBytesOut` are the same counts keyed by workspace
// instead of by method — request bytes for Push, response bytes for Pull, the
// direction each carries payload in. One testserver process serves every
// integration worker at once, so the per-method totals move under a test that
// is only watching its own traffic; a workspace is unique per test, so it is
// the only key that isolates.
type rpcStats struct {
	mu           sync.Mutex
	active       map[string]int
	total        map[string]int
	bytesIn      map[string]int64
	bytesOut     map[string]int64
	pushBytesIn  map[string]int64
	pullBytesOut map[string]int64
}

func newRPCStats() *rpcStats {
	return &rpcStats{
		active:       map[string]int{},
		total:        map[string]int{},
		bytesIn:      map[string]int64{},
		bytesOut:     map[string]int64{},
		pushBytesIn:  map[string]int64{},
		pullBytesOut: map[string]int64{},
	}
}

// wrap counts every call passing through it. Mount it inside the CORS
// middleware so preflights are not counted as calls.
func (s *rpcStats) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
		s.enter(method)
		counting := &countingWriter{ResponseWriter: w}
		var workspaces []string
		if method == "Push" || method == "Pull" {
			// Buffering is only safe because these unary bodies are bounded and
			// this binary only ever serves tests; the streaming methods keep
			// their bodies untouched.
			raw, err := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			workspaces = requestWorkspaces(method, raw, r.Header)
			r.Body = io.NopCloser(bytes.NewReader(raw))
		}
		body := &countingReader{ReadCloser: r.Body}
		r.Body = body
		defer func() {
			s.exit(method, body.n, counting.n, workspaces)
		}()
		next.ServeHTTP(counting, r)
	})
}

// requestWorkspaces reads the workspace ids out of a request body so the call's
// wire size can be attributed to them. Best effort: an encoding this does not
// recognize simply yields no attribution, which shows up as a stalled counter
// rather than as a wrong number.
func requestWorkspaces(method string, raw []byte, header http.Header) []string {
	payload := raw
	if strings.Contains(header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil
		}
		defer func() { _ = zr.Close() }()
		if payload, err = io.ReadAll(zr); err != nil {
			return nil
		}
	}
	json := strings.Contains(header.Get("Content-Type"), "json")
	unmarshal := func(m proto.Message) bool {
		if json {
			return protojson.Unmarshal(payload, m) == nil
		}
		return proto.Unmarshal(payload, m) == nil
	}
	if method == "Pull" {
		var req syncplanev1.PullRequest
		if !unmarshal(&req) {
			return nil
		}
		return []string{req.GetWorkspaceId()}
	}
	var req syncplanev1.PushRequest
	if !unmarshal(&req) {
		return nil
	}
	out := make([]string, 0, len(req.GetBatches()))
	for _, b := range req.GetBatches() {
		out = append(out, b.GetWorkspaceId())
	}
	return out
}

// countingReader tallies the request-body bytes actually consumed by the
// handler. Connect reads the whole body for unary calls, so the count is the
// full request payload.
type countingReader struct {
	io.ReadCloser
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n += int64(n)
	return n, err
}

// countingWriter tallies response-body bytes. It deliberately implements only
// Write and Flush: the testserver's handlers need streaming (Subscribe), not
// hijacking.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n += int64(n)
	return n, err
}

func (c *countingWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *rpcStats) enter(method string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[method]++
	s.total[method]++
}

func (s *rpcStats) exit(method string, in, out int64, workspaces []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[method]--
	s.bytesIn[method] += in
	s.bytesOut[method] += out
	// Whole-body attribution per named workspace: a push naming several would
	// over-count each, which no caller of this needs to care about because the
	// suites that read it push one workspace at a time. A pull names exactly
	// one, and its payload is the response.
	for _, ws := range workspaces {
		if method == "Pull" {
			s.pullBytesOut[ws] += out
			continue
		}
		s.pushBytesIn[ws] += in
	}
}

// textStringHandler applies a trusted server-side string write to a Widget
// text field (EntityTx with WithTextString) — the integration suite's
// stand-in for a connector or admin-form writer, so cross-language
// convergence of string writes is testable over the real wire. POST JSON:
// {"workspace", "key", "field", "value"}. The row is Put when absent,
// Patched when present.
func textStringHandler(srv *server.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Workspace string `json:"workspace"`
			Key       string `json:"key"`
			Field     string `json:"field"`
			Value     string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		opts := []server.WriteOption{
			server.WithTextString(req.Field, req.Value),
			// One fixed identity: writes are serialized per workspace and
			// each reads the stored row in its own transaction, which is
			// exactly the reuse the text peer contract allows.
			server.WithTextPeer("testserver:textstring"),
		}
		err := srv.Mutate(server.Trusted(r.Context()), req.Workspace, func(tx *server.EntityTx) error {
			var cur testdatav1.Widget
			found, err := tx.Get(&cur, req.Key)
			if err != nil {
				return err
			}
			if found {
				return tx.Patch(&testdatav1.Widget{Id: req.Key}, nil, nil, opts...)
			}
			return tx.Put(&testdatav1.Widget{Id: req.Key}, opts...)
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// gcHandler runs one garbage-collection pass on demand — tombstones, idle
// clients, the text delta log, and collaborative-text compaction (see
// server.WithTextCompaction). The suites need compaction to happen at a moment
// they choose rather than on a timer, and a pass is stateless and re-runnable,
// so exposing it costs nothing. POST JSON: {"workspace"}.
func gcHandler(srv *server.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Workspace string `json:"workspace"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := srv.CollectGarbage(r.Context(), req.Workspace); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *rpcStats) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	snapshot := struct {
		Active       map[string]int   `json:"active"`
		Total        map[string]int   `json:"total"`
		BytesIn      map[string]int64 `json:"bytesIn"`
		BytesOut     map[string]int64 `json:"bytesOut"`
		PushBytesIn  map[string]int64 `json:"pushBytesIn"`
		PullBytesOut map[string]int64 `json:"pullBytesOut"`
	}{
		Active:       maps.Clone(s.active),
		Total:        maps.Clone(s.total),
		BytesIn:      maps.Clone(s.bytesIn),
		BytesOut:     maps.Clone(s.bytesOut),
		PushBytesIn:  maps.Clone(s.pushBytesIn),
		PullBytesOut: maps.Clone(s.pullBytesOut),
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(snapshot)
}
