package server

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
)

// Response compression for the two server-streaming procedures, applied at the
// HTTP layer rather than through Connect.
//
// Connect has its own compression, and connect-go enables gzip by default —
// but the two halves of the protocol negotiate it differently. Unary responses
// use the standard Content-Encoding/Accept-Encoding pair, which every browser
// sends unprompted, so Push, Pull and Query already come back gzipped and
// nothing here touches them. Streaming responses negotiate on
// Connect-Accept-Encoding instead, and a browser client cannot ask: fetch
// forbids setting the header itself, and connect-web implements no compression
// at all. So Bootstrap — the largest response the server ever writes, a whole
// snapshot of every row a client can see — went out uncompressed to exactly
// the clients that need it compressed most.
//
// Gzipping the whole HTTP body sidesteps the negotiation. Content-Encoding is
// decoded by the browser's network stack before connect-web sees a byte, and it
// is decoded incrementally, so the frames still arrive as a stream rather than
// after the body completes. A connect-go client is unaffected: it negotiates
// Connect-level compression as before, and never sends the Accept-Encoding this
// path keys on.
//
// The rows in a snapshot are the same message shape over and over, so this is
// worth roughly 4x on the wire — see issues/120 for the measurements and for
// the per-row encoding work that would compound with it.

// streamingProcedures are the procedures whose responses this compresses. Both
// are server-streaming; unary is left to connect-go, which already handles it
// and would double-encode if this joined in.
var streamingProcedures = map[string]bool{
	syncplanev1connect.SyncServiceBootstrapProcedure: true,
	syncplanev1connect.SyncServiceSubscribeProcedure: true,
}

// gzipWriterPool amortizes the deflate state (a few hundred KB per writer)
// across streams. Subscribe holds one for the life of the stream, so a server
// with many subscribers holds many; the pool keeps that from also being an
// allocation per stream.
var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(io.Discard) },
}

// compressStreams wraps the sync handler so a streaming response to a client
// that accepts gzip is gzipped.
func compressStreams(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !streamingProcedures[r.URL.Path] || !acceptsGzip(r.Header.Get("Accept-Encoding")) {
			next.ServeHTTP(w, r)
			return
		}
		// A client that negotiated Connect-level compression is already served
		// by connect-go, per message. Compressing the body around that would
		// cost a second pass over the same bytes for nothing — and this is not
		// hypothetical: Go's http.Transport attaches Accept-Encoding: gzip to
		// every request without being asked, so a connect-go client looks
		// exactly like a browser on the header this path keys on.
		if negotiatedStreamCompression(r.Header) {
			next.ServeHTTP(w, r)
			return
		}
		// Added even when the response turns out not to be compressed: the
		// header describes what the response could have varied on, and the
		// decision below depends on the request.
		w.Header().Add("Vary", "Accept-Encoding")
		gw := &gzipResponseWriter{ResponseWriter: w}
		defer gw.close()
		next.ServeHTTP(gw, r)
	})
}

// gzipResponseWriter compresses the response body, deciding on the first write
// whether to and passing everything through untouched if not.
type gzipResponseWriter struct {
	http.ResponseWriter
	zw       *gzip.Writer
	decided  bool
	compress bool
}

// Unwrap exposes the underlying writer to http.ResponseController, so write
// deadlines and any other controller call reach the real connection instead of
// failing against this wrapper.
func (g *gzipResponseWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }

func (g *gzipResponseWriter) WriteHeader(status int) {
	g.decide(status)
	g.ResponseWriter.WriteHeader(status)
}

func (g *gzipResponseWriter) Write(p []byte) (int, error) {
	g.decide(http.StatusOK)
	if !g.compress {
		return g.ResponseWriter.Write(p)
	}
	return g.zw.Write(p)
}

// Flush flushes the gzip writer before the underlying one. connect-go requires
// a server-streaming handler's ResponseWriter to implement http.Flusher and
// calls Flush after every message; a gzip writer that did not flush with it
// would hold the message in its window and stall Subscribe's live tail until
// enough later messages arrived to fill it — a poke delivered late is the one
// failure this whole path must not introduce.
func (g *gzipResponseWriter) Flush() {
	if g.compress {
		_ = g.zw.Flush()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// decide settles compression for this response, once, before anything is
// written.
func (g *gzipResponseWriter) decide(status int) {
	if g.decided {
		return
	}
	g.decided = true
	// A streaming procedure still answers with a unary-shaped error when it
	// fails before the stream opens — an unknown workspace, a rejected
	// authentication — and connect-go may have compressed that itself. Encoding
	// an already-encoded body would leave a client unable to read either layer.
	if status != http.StatusOK || g.Header().Get("Content-Encoding") != "" {
		return
	}
	g.compress = true
	g.Header().Set("Content-Encoding", "gzip")
	// Any length already set describes the identity encoding, not what goes out.
	g.Header().Del("Content-Length")
	zw, _ := gzipWriterPool.Get().(*gzip.Writer)
	zw.Reset(g.ResponseWriter)
	g.zw = zw
}

// close finishes the gzip stream and returns the writer to the pool. Called
// once the handler has returned, so nothing writes through afterwards.
func (g *gzipResponseWriter) close() {
	if !g.compress {
		return
	}
	_ = g.zw.Close()
	gzipWriterPool.Put(g.zw)
	g.zw = nil
	g.compress = false
}

// negotiatedStreamCompression reports whether the request asked for
// Connect-level (per-message) compression of the stream, on either the Connect
// or the gRPC-Web protocol. Browsers cannot send these headers — fetch forbids
// setting them for compression purposes and connect-web never tries — so this
// distinguishes a native client from the one this path exists for.
func negotiatedStreamCompression(header http.Header) bool {
	return acceptsGzip(header.Get("Connect-Accept-Encoding")) ||
		acceptsGzip(header.Get("Grpc-Accept-Encoding"))
}

// acceptsGzip reports whether an Accept-Encoding header asks for gzip. An
// explicit q=0 is a refusal, which is the one weight worth honouring: browsers
// send a bare "gzip", and a proxy that strips compression says so this way.
func acceptsGzip(header string) bool {
	for _, entry := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(entry, ";")
		if !strings.EqualFold(strings.TrimSpace(name), "gzip") {
			continue
		}
		return qualityOf(params) > 0
	}
	return false
}

// qualityOf reads the q parameter of one Accept-Encoding entry, treating
// anything absent or unparseable as full quality.
func qualityOf(params string) float64 {
	for _, param := range strings.Split(params, ";") {
		key, value, ok := strings.Cut(param, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
			continue
		}
		q, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return 1
		}
		return q
	}
	return 1
}
