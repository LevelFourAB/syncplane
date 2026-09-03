package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/internal/benchapp"
)

// seedHandler generates a tier's corpus into its workspace and builds its
// indexes. POST JSON: {"tier", "seed"}; responds with the row counts written
// and how long both steps took.
//
// Seeding the largest tier is minutes of work, which is why it is an endpoint
// rather than something a benchmark does for itself: the harness seeds once
// into a database it then caches, and every later run starts from the copy.
func seedHandler(srv *server.Server, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Tier string `json:"tier"`
			Seed uint64 `json:"seed"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tier, ok := benchapp.Tiers[req.Tier]
		if !ok {
			http.Error(w, fmt.Sprintf("unknown tier %q", req.Tier), http.StatusBadRequest)
			return
		}
		ws := benchapp.WorkspaceID(tier.Name)
		started := time.Now()
		// Progress goes to the log rather than the response: the response
		// cannot say anything until the whole corpus is written, and the
		// largest tier is long enough that a silent process looks hung.
		var lastLog time.Time
		progress := func(entity string, done, total int) {
			if time.Since(lastLog) < time.Second && done != total {
				return
			}
			lastLog = time.Now()
			logger.Info("seeding", "workspace", ws, "entity", entity, "done", done, "total", total)
		}
		counts, err := benchapp.Seed(r.Context(), srv, ws, tier, req.Seed, progress)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Info("seeded", "workspace", ws, "rows", counts.Total, "took", time.Since(started))

		// Seeding leaves every index unready, and the background pass would
		// rebuild the whole corpus a minute into each run — measuring a server
		// that is mid-backfill rather than the one a deployment actually runs.
		// Building here folds the backfill into the cached database, so a run
		// starts from the steady state.
		buildStarted := time.Now()
		if err := srv.BuildIndexes(r.Context(), ws); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Info("indexes built", "workspace", ws, "took", time.Since(buildStarted))

		elapsed := time.Since(started)
		writeJSON(w, struct {
			Workspace string          `json:"workspace"`
			Counts    benchapp.Counts `json:"counts"`
			TookMs    int64           `json:"tookMs"`
		}{ws, counts, elapsed.Milliseconds()})
	})
}

// tiersHandler reports the corpus tiers and their row counts, so the specs can
// scale expectations to the corpus without keeping a second copy of the table
// that would drift from the one the seeder actually used.
func tiersHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		type tierView struct {
			benchapp.Tier
			Total     int    `json:"total"`
			Workspace string `json:"workspace"`
		}
		out := make([]tierView, 0, len(benchapp.TierNames))
		for _, name := range benchapp.TierNames {
			tier := benchapp.Tiers[name]
			out = append(out, tierView{tier, tier.Total(), benchapp.WorkspaceID(name)})
		}
		writeJSON(w, out)
	})
}

// writeHandler mutates a seeded corpus from the server side, so a benchmark can
// price catching up with changes it did not make itself. POST JSON:
// {"workspace", "op", "count", "tier"}.
//
//   - "patch" touches `count` existing issues, newest first, titling each with
//     the request's "marker" (default "touched"). Each write moves the row's
//     recency, so a client holding a recency window sees membership move rather
//     than merely seeing a row change.
//   - "insert" appends `count` new issues past the seeded range — rows that
//     enter every window that is not already full at a better rank.
//   - "delete" removes `count` issues from the newest end, which is how a
//     tombstone reaches a client.
//   - "flip" rewrites `count` issues' state and priority, alternating by index
//     between the pair relFlipState/relFlipPriority names and a pair that does
//     not. A client watching a query whose predicate crosses a reference to
//     these rows sees its MEMBERSHIP move without any row it holds being
//     touched — the case "patch" cannot produce, since a title moves no
//     predicate.
//
// Writes go through Mutate as one transaction per batch, so a benchmark that
// asks for 10,000 changes gets a workspace history it can be behind by.
func writeHandler(srv *server.Server) http.Handler {
	// Where the next inserted issue's key starts, per workspace. Inserted keys
	// have to be new EVERY time: deriving them from the request's own count
	// would make two inserts of different sizes overlap, so the second would
	// quietly overwrite the first's rows and report itself as an insert.
	var mu sync.Mutex
	inserted := map[string]int{}
	nextInsert := func(ws string, count int) int {
		mu.Lock()
		defer mu.Unlock()
		base := inserted[ws]
		inserted[ws] += count
		return base
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Workspace string `json:"workspace"`
			Op        string `json:"op"`
			Count     int    `json:"count"`
			Tier      string `json:"tier"`
			// Prefix for the titles a "patch" writes. A caller that waits for a
			// change to arrive needs to be able to tell THIS write from the one
			// before it — with a fixed title, the second round of a latency
			// measurement matches the first round's value and reports zero.
			Marker string `json:"marker"`
			// Index of the first issue to touch, overriding the op's default
			// range. What lets a caller delete exactly the rows an earlier
			// insert created: the insert's response carries the index it chose.
			First *int `json:"first"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tier, ok := benchapp.Tiers[req.Tier]
		if !ok {
			http.Error(w, fmt.Sprintf("unknown tier %q", req.Tier), http.StatusBadRequest)
			return
		}
		if req.Count <= 0 {
			http.Error(w, "count must be positive", http.StatusBadRequest)
			return
		}
		started := time.Now()
		// Where the touched range starts. "patch" and "delete" work backwards
		// from the newest seeded issue — the end of the id range is where
		// recency windows and the hot comment threads already are, so a write
		// there lands inside whatever scope a partial-sync client is holding.
		// "insert" writes past the seeded range, at keys nothing has used yet.
		var first int
		switch {
		case req.First != nil:
			// An explicit range is the caller's responsibility: it may name
			// inserted rows past the seeded range, so no tier bound applies.
			// Not offered for "insert" — inserted keys are allocated here, and a
			// caller-chosen one could collide with a later allocation.
			if req.Op != "patch" && req.Op != "delete" && req.Op != "flip" {
				http.Error(w, fmt.Sprintf("first is only valid for patch, delete and flip, not %q", req.Op),
					http.StatusBadRequest)
				return
			}
			if *req.First < 0 {
				http.Error(w, "first must not be negative", http.StatusBadRequest)
				return
			}
			first = *req.First
		case req.Op == "patch" || req.Op == "delete" || req.Op == "flip":
			first = tier.Issues - req.Count
			if first < 0 {
				http.Error(w, fmt.Sprintf("tier %s holds %d issues, cannot touch %d",
					tier.Name, tier.Issues, req.Count), http.StatusBadRequest)
				return
			}
		case req.Op == "insert":
			first = tier.Issues + nextInsert(req.Workspace, req.Count)
		default:
			http.Error(w, fmt.Sprintf("unknown op %q", req.Op), http.StatusBadRequest)
			return
		}
		marker := req.Marker
		if marker == "" {
			marker = "touched"
		}
		if err := applyWrites(r, srv, req.Workspace, req.Op, first, req.Count, marker); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, struct {
			Written int   `json:"written"`
			First   int   `json:"first"`
			TookMs  int64 `json:"tookMs"`
		}{req.Count, first, time.Since(started).Milliseconds()})
	})
}

// writeBatch is how many rows share one transaction, and therefore one
// workspace sequence. Smaller than the seeder's batch: a benchmark measuring
// catch-up wants a history with many versions in it, not one enormous one.
const writeBatch = 200

// The state/priority pair "flip" moves issues onto. A benchmark watching a
// query whose semi-join names this pair is what turns a flip into a membership
// move; client/test/perf/fixtures.ts spells the same values, and the two must
// agree or the flip moves nothing the watch can see.
const (
	relFlipState    = "review"
	relFlipPriority = int32(4)
)

// applyWrites writes `count` issues starting at index `first`; the caller has
// already decided what that index means for the op it asked for.
func applyWrites(
	r *http.Request, srv *server.Server, ws, op string, first, count int, marker string,
) error {
	for start := 0; start < count; start += writeBatch {
		end := min(start+writeBatch, count)
		lo, hi := first+start, first+end
		err := srv.Mutate(server.Trusted(r.Context()), ws, func(tx *server.EntityTx) error {
			for i := lo; i < hi; i++ {
				key := benchapp.IssueKey(i)
				switch op {
				case "patch":
					// A field mask so this is a PATCH, not a whole-row replace:
					// what a client applies (and what the wire carries) is the
					// named fields, which is the shape ordinary traffic has.
					msg := &benchv1.Issue{
						Id:          key,
						Title:       fmt.Sprintf("%s %d", marker, i),
						UpdatedAtMs: touchedAtMs(i),
					}
					if err := tx.Patch(msg, []string{"title", "updated_at_ms"}, nil); err != nil {
						return err
					}
				case "insert":
					if err := tx.Put(&benchv1.Issue{
						Id:          key,
						Title:       fmt.Sprintf("inserted %d", i),
						State:       "open",
						Project:     &benchv1.Project{Id: "project-00000000"},
						UpdatedAtMs: touchedAtMs(i),
						CreatedAtMs: touchedAtMs(i),
					}); err != nil {
						return err
					}
				case "flip":
					// Alternating by index so one call moves membership in both
					// directions: a benchmark that only ever added keys would
					// never price what a removal costs.
					state, priority := relFlipState, relFlipPriority
					if i%2 != 0 {
						state, priority = "open", int32(1)
					}
					msg := &benchv1.Issue{
						Id:          key,
						State:       state,
						Priority:    priority,
						UpdatedAtMs: touchedAtMs(i),
					}
					if err := tx.Patch(msg, []string{"state", "priority", "updated_at_ms"}, nil); err != nil {
						return err
					}
				case "delete":
					if err := tx.Delete(&benchv1.Issue{}, key); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unknown op %q", op)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// touchedAtMs stamps a server-side write far past the seeded corpus's range, so
// a touched row sorts to the top of every recency ordering regardless of which
// row it was.
func touchedAtMs(i int) int64 {
	return 2_000_000_000_000 + int64(i)
}

// gcHandler runs one garbage-collection pass on demand. The re-bootstrap
// benchmark needs the tombstone horizon to advance at a moment it chooses:
// a client behind the horizon is the expensive path, and there is no other way
// to put one there deliberately. POST JSON: {"workspace"}.
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

// wireStats counts Connect calls and their body sizes by bare method name
// ("Pull", "Bootstrap", ...), exposed as JSON at /debug/stats.
//
// It is deliberately simpler than testserver's equivalent, which attributes
// bytes to workspaces by parsing request bodies. The benchmark tier runs
// single-worker and serially, and every scenario brackets its own work, so a
// process-wide counter read before and after is already attributed: nothing
// else is running.
//
// What it buys is the other half of a sync measurement. The client can say how
// long a bootstrap took and how many rows it wrote; only the server can say how
// many bytes it took to get them there — and bytes are the part of a benchmark
// that does NOT change when the same client runs over a real network.
//
// # Why the compressed sizes are measured too
//
// The bytes this counts are uncompressed. connect-web refuses a compressed
// Connect frame outright (it throws on the compressed flag), so the browser
// never negotiates protocol-level compression and the engine ships raw
// protobuf. A real deployment does not: browsers always send `Accept-Encoding`,
// and any reverse proxy or CDN in front of the engine compresses the response
// body transparently, streaming ones included.
//
// So the raw count answers "what did the engine produce" and the gzip count
// answers "what would have crossed a real link" — and a data-volume estimate
// built on the first alone would be several times too pessimistic. Both are
// reported because the difference between them is itself worth seeing: it is
// the compression a deployment gets for free by putting anything in front of
// this server.
//
// gzip runs at BestSpeed, so the ratio is a floor — a CDN at default gzip, and
// certainly one doing brotli, does better.
type wireStats struct {
	mu       sync.Mutex
	total    map[string]int
	bytesIn  map[string]int64
	bytesOut map[string]int64
	gzipIn   map[string]int64
	gzipOut  map[string]int64
	// Responses the handler had already compressed itself.
	encodedOut map[string]int
	// What a real link would carry: the body as sent when it was already
	// compressed, and its gzipped size when it was not. This is the number a
	// data-volume estimate should use — the raw counts describe the engine, not
	// the network.
	wireIn  map[string]int64
	wireOut map[string]int64
	// Handler time, from the request reaching the mux to its response
	// finishing — the server's share of a round trip a client clocks, so a
	// slow sync can be attributed to one side or the other.
	serverMs    map[string]float64
	serverMaxMs map[string]float64
}

func newWireStats() *wireStats {
	return &wireStats{
		total:       map[string]int{},
		bytesIn:     map[string]int64{},
		bytesOut:    map[string]int64{},
		gzipIn:      map[string]int64{},
		gzipOut:     map[string]int64{},
		encodedOut:  map[string]int{},
		wireIn:      map[string]int64{},
		wireOut:     map[string]int64{},
		serverMs:    map[string]float64{},
		serverMaxMs: map[string]float64{},
	}
}

func (s *wireStats) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
		counting := &countingWriter{ResponseWriter: w, gz: newGzipCounter()}
		body := &countingReader{ReadCloser: r.Body, gz: newGzipCounter()}
		r.Body = body
		started := time.Now()
		defer func() {
			elapsed := float64(time.Since(started)) / float64(time.Millisecond)
			// Closed here rather than as the bodies drain: gzip only knows its
			// final size once its last block is flushed, and for a streaming
			// method that is when the stream ends.
			out := counting.gz.close()
			in := body.gz.close()
			// Whether the handler had already compressed what it wrote. Connect
			// compresses a UNARY response through the ordinary HTTP
			// Content-Encoding header, which the browser transparently decodes
			// — so Pull arrives compressed — while a STREAMING response cannot
			// use it and connect-web refuses protocol-level frame compression,
			// so Bootstrap arrives raw. Measuring instead of assuming is the
			// only way to tell those apart, and gzipping an already-gzipped
			// body reports ~1.0x and means nothing.
			encodedOut := w.Header().Get("Content-Encoding") != ""
			encodedIn := r.Header.Get("Content-Encoding") != ""
			s.mu.Lock()
			s.total[method]++
			s.bytesIn[method] += body.n
			s.bytesOut[method] += counting.n
			s.gzipIn[method] += in
			s.gzipOut[method] += out
			if encodedOut {
				s.encodedOut[method]++
				s.wireOut[method] += counting.n
			} else {
				s.wireOut[method] += out
			}
			if encodedIn {
				s.wireIn[method] += body.n
			} else {
				s.wireIn[method] += in
			}
			s.serverMs[method] += elapsed
			s.serverMaxMs[method] = max(s.serverMaxMs[method], elapsed)
			s.mu.Unlock()
		}()
		next.ServeHTTP(counting, r)
	})
}

func (s *wireStats) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	snapshot := struct {
		Total      map[string]int   `json:"total"`
		BytesIn    map[string]int64 `json:"bytesIn"`
		BytesOut   map[string]int64 `json:"bytesOut"`
		GzipIn     map[string]int64 `json:"gzipIn"`
		GzipOut    map[string]int64 `json:"gzipOut"`
		EncodedOut map[string]int   `json:"encodedOut"`
		WireIn     map[string]int64 `json:"wireIn"`
		WireOut    map[string]int64 `json:"wireOut"`
		// Handler time in milliseconds, summed and the single slowest call.
		ServerMs    map[string]float64 `json:"serverMs"`
		ServerMaxMs map[string]float64 `json:"serverMaxMs"`
	}{
		Total:       maps.Clone(s.total),
		BytesIn:     maps.Clone(s.bytesIn),
		BytesOut:    maps.Clone(s.bytesOut),
		GzipIn:      maps.Clone(s.gzipIn),
		GzipOut:     maps.Clone(s.gzipOut),
		EncodedOut:  maps.Clone(s.encodedOut),
		WireIn:      maps.Clone(s.wireIn),
		WireOut:     maps.Clone(s.wireOut),
		ServerMs:    maps.Clone(s.serverMs),
		ServerMaxMs: maps.Clone(s.serverMaxMs),
	}
	s.mu.Unlock()
	writeJSON(w, snapshot)
}

type countingWriter struct {
	http.ResponseWriter
	n  int64
	gz *gzipCounter
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	w.gz.write(p[:n])
	return n, err
}

// Flush keeps the streaming methods streaming: without it the wrapper hides the
// underlying http.Flusher and a Subscribe frame would sit in the buffer until
// the stream ended, which is precisely the latency the realtime benchmarks
// measure.
func (w *countingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type countingReader struct {
	io.ReadCloser
	n  int64
	gz *gzipCounter
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.n += int64(n)
	r.gz.write(p[:n])
	return n, err
}

// gzipCounter compresses everything written to it and keeps only the size.
//
// It exists because the interesting question about a payload is how big it
// would be on a link that compresses, and the only honest way to answer that is
// to compress it. The output is discarded — nothing here needs the bytes, just
// the count.
//
// BestSpeed because this runs inline with the response the benchmark is timing.
// A slower level would compress better and make the server look worse at the
// same time, which is the wrong trade for a number that is already a floor.
type gzipCounter struct {
	w    *gzip.Writer
	sink *byteCounter
}

func newGzipCounter() *gzipCounter {
	sink := &byteCounter{}
	w, err := gzip.NewWriterLevel(sink, gzip.BestSpeed)
	if err != nil {
		// Only returned for an invalid level, and BestSpeed is a constant.
		panic(err)
	}
	return &gzipCounter{w: w, sink: sink}
}

func (g *gzipCounter) write(p []byte) {
	if len(p) == 0 {
		return
	}
	_, _ = g.w.Write(p)
}

// close flushes the final block and returns the compressed size. A gzip stream
// only knows its size once it is closed, so a count read before this is short
// by whatever is still in the window.
func (g *gzipCounter) close() int64 {
	_ = g.w.Close()
	return g.sink.n
}

type byteCounter struct{ n int64 }

func (c *byteCounter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}
