package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"sync"

	"google.golang.org/protobuf/proto"

	tunnelv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/tunnel/v1"
)

// DefaultRequestBuffer is the request buffer Serve uses without
// WithRequestBuffer: 32 MiB, twice the sync engine's default request limit.
const DefaultRequestBuffer = 32 << 20

// Option configures Serve.
type Option func(*serveOptions)

type serveOptions struct {
	requestBuffer int
	logger        *slog.Logger
}

// WithRequestBuffer bounds the request-body bytes, per call, that Serve holds
// for a handler that has not read them yet. A call that goes above n fails:
// its handler's context ends and the client gets an error. Zero or a negative
// n means DefaultRequestBuffer.
func WithRequestBuffer(n int) Option {
	return func(o *serveOptions) {
		if n > 0 {
			o.requestBuffer = n
		}
	}
}

// WithLogger sets where Serve reports handler panics. The default is
// slog.Default(). A nil logger keeps the default.
func WithLogger(l *slog.Logger) Option {
	return func(o *serveOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// Serve answers the calls that arrive on conn with h, until conn closes or ctx
// ends.
//
// Each call runs h on its own goroutine. The request's context ends when the
// client cancels the call, the channel closes or ctx ends. The request has no
// RemoteAddr or TLS state; Host comes from a Host header when the client sent
// one, and ContentLength from Content-Length, or -1 without it. The
// ResponseWriter implements http.Flusher, and a flush sends the bytes written
// so far.
//
// A handler that panics fails only its own call, and the client sees a network
// failure. The panic is logged unless it is http.ErrAbortHandler.
//
// Serve blocks. Before it returns, it cancels every call in flight, waits for
// their handlers to return and closes conn. It returns nil when the channel
// reaches io.EOF or ctx ends. Otherwise it returns the error that ended it: a
// Receive or Send failure, or an error that wraps ErrProtocol.
func Serve(ctx context.Context, conn Conn, h http.Handler, opts ...Option) error {
	o := serveOptions{requestBuffer: DefaultRequestBuffer, logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	parent := ctx
	ctx, stop := context.WithCancel(ctx)
	s := &server{
		conn:  conn,
		h:     h,
		opts:  o,
		ctx:   ctx,
		stop:  stop,
		calls: make(map[uint32]*serverCall),
	}
	// Closing conn is the only way to unblock a Receive, so ending ctx and a
	// failed Send both reach the loop below through it.
	unwatch := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer unwatch()

	err := s.receive()
	parentDone := parent.Err() != nil
	stop()
	s.wg.Wait()
	_ = conn.Close()

	s.sendMu.Lock()
	sendErr := s.sendErr
	s.sendMu.Unlock()
	switch {
	case sendErr != nil:
		return sendErr
	case errors.Is(err, ErrProtocol):
		return err
	case parentDone, errors.Is(err, io.EOF):
		return nil
	default:
		return err
	}
}

type server struct {
	conn Conn
	h    http.Handler
	opts serveOptions
	ctx  context.Context
	stop context.CancelFunc

	sendMu  sync.Mutex
	sendErr error

	mu    sync.Mutex
	calls map[uint32]*serverCall

	wg sync.WaitGroup
}

type serverCall struct {
	id     uint32
	ctx    context.Context
	cancel context.CancelFunc
	body   *bodyBuffer

	// Guarded by server.mu. cancelled is set when the client sent Cancel, and
	// the call then sends nothing more; failure is set when the server failed
	// the call itself.
	cancelled bool
	failure   string
}

func protocolError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// receive reads frames until the channel fails or carries something that is
// not a valid frame. It never sends: a Send that blocks on a full channel must
// not stop the frames that would unblock it, such as a Cancel.
func (s *server) receive() error {
	for {
		raw, err := s.conn.Receive()
		if err != nil {
			return err
		}
		f := new(tunnelv1.Frame)
		if err := proto.Unmarshal(raw, f); err != nil {
			return protocolError("malformed frame: %v", err)
		}
		if err := s.dispatch(f); err != nil {
			return err
		}
	}
}

func (s *server) dispatch(f *tunnelv1.Frame) error {
	id := f.GetCall()
	switch kind := f.GetKind().(type) {
	case *tunnelv1.Frame_Request:
		if id == 0 {
			return protocolError("request with call id 0")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.calls[id]; ok {
			return protocolError("request for call %d, which is in flight", id)
		}
		ctx, cancel := context.WithCancel(s.ctx)
		c := &serverCall{id: id, ctx: ctx, cancel: cancel, body: newBodyBuffer(s.opts.requestBuffer)}
		s.calls[id] = c
		s.wg.Add(1)
		go s.run(c, kind.Request)
	case *tunnelv1.Frame_Body:
		if c := s.lookup(id); c != nil && !c.body.push(kind.Body) {
			s.fail(c, fmt.Sprintf("request body exceeds the %d bytes the server buffers", s.opts.requestBuffer))
		}
	case *tunnelv1.Frame_End:
		if c := s.lookup(id); c != nil {
			if reason := kind.End.GetError(); reason != "" {
				c.body.finish(fmt.Errorf("%w: %s", ErrCallFailed, reason))
			} else {
				c.body.finish(nil)
			}
		}
	case *tunnelv1.Frame_Cancel:
		if c := s.lookup(id); c != nil {
			s.mu.Lock()
			c.cancelled = true
			s.mu.Unlock()
			c.cancel()
			c.body.finish(context.Canceled)
		}
	case *tunnelv1.Frame_Response:
		return protocolError("response frame from the client")
	default:
		// A kind this version does not know. Ignoring it is what lets the
		// protocol add kinds without breaking older servers.
	}
	return nil
}

func (s *server) lookup(id uint32) *serverCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[id]
}

func (s *server) fail(c *serverCall, reason string) {
	s.mu.Lock()
	if c.failure == "" {
		c.failure = reason
	}
	s.mu.Unlock()
	c.cancel()
	c.body.finish(errors.New("tunnel: " + reason))
}

func (s *server) run(c *serverCall, head *tunnelv1.RequestHead) {
	defer s.wg.Done()
	defer c.cancel()

	w := &responseWriter{s: s, call: c, header: make(http.Header)}
	failure := s.handle(c, head, w)
	_ = c.body.Close()

	s.mu.Lock()
	// Removed before End goes out, so a client that reuses the id as soon as it
	// sees End never finds it still in flight.
	delete(s.calls, c.id)
	cancelled := c.cancelled
	if failure == "" {
		failure = c.failure
	}
	s.mu.Unlock()

	if cancelled {
		return
	}
	if failure == "" {
		_ = w.FlushError()
	}
	_ = s.send(&tunnelv1.Frame{
		Call: c.id,
		Kind: &tunnelv1.Frame_End{End: &tunnelv1.End{Error: failure}},
	})
}

// handle runs the handler and returns why the call failed, or "" when the
// handler returned normally.
func (s *server) handle(c *serverCall, head *tunnelv1.RequestHead, w *responseWriter) (failure string) {
	req, err := buildRequest(c, head)
	if err != nil {
		return err.Error()
	}
	defer func() {
		if r := recover(); r != nil {
			if r != http.ErrAbortHandler {
				s.opts.logger.Error("tunnel: handler panicked",
					"target", head.GetTarget(), "panic", r, "stack", string(debug.Stack()))
			}
			failure = "handler aborted"
		}
	}()
	s.h.ServeHTTP(w, req)
	return ""
}

func buildRequest(c *serverCall, head *tunnelv1.RequestHead) (*http.Request, error) {
	target := head.GetTarget()
	// ParseRequestURI rather than the URL parsing in NewRequest: it reads a
	// target the way a server reads a request line, so "//host/path" stays a
	// path instead of naming a host.
	u, err := url.ParseRequestURI(target)
	if err != nil || u.Scheme != "" || len(target) == 0 || target[0] != '/' {
		return nil, fmt.Errorf("invalid request target %q", target)
	}
	// NewRequest validates the method; the URL it parses is replaced below.
	req, err := http.NewRequestWithContext(c.ctx, head.GetMethod(), "/", nil)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	req.URL = u
	req.RequestURI = target
	req.Header = decodeHeaders(head.GetHeaders())
	// net/http moves Host out of the header map on the server side; do the
	// same so handlers and proxies see it where they look for it.
	req.Host = req.Header.Get("Host")
	req.Header.Del("Host")
	req.Body = c.body
	req.ContentLength = -1
	if cl := req.Header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid Content-Length %q", cl)
		}
		req.ContentLength = n
	}
	if req.ContentLength == 0 {
		req.Body = http.NoBody
	}
	return req, nil
}

// responseWriter sends a handler's response as frames. It buffers body bytes
// until a flush, the end of the handler, or a full chunk.
type responseWriter struct {
	s           *server
	call        *serverCall
	header      http.Header
	wroteHeader bool
	buf         []byte
	err         error
}

func (w *responseWriter) Header() http.Header { return w.header }

func (w *responseWriter) WriteHeader(code int) {
	if code < 100 || code > 599 {
		panic(fmt.Sprintf("tunnel: invalid WriteHeader code %d", code))
	}
	// The protocol carries one final head; informational 1xx responses have
	// nowhere to go and are dropped.
	if w.wroteHeader || code < 200 {
		return
	}
	w.wroteHeader = true
	_ = w.send(&tunnelv1.Frame{Call: w.call.id, Kind: &tunnelv1.Frame_Response{Response: &tunnelv1.ResponseHead{
		Status:  uint32(code),
		Headers: encodeHeaders(w.header),
	}}})
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	w.buf = append(w.buf, p...)
	for len(w.buf) >= chunkSize {
		if err := w.send(w.body(w.buf[:chunkSize])); err != nil {
			return 0, err
		}
		w.buf = w.buf[chunkSize:]
	}
	return len(p), nil
}

// FlushError sends the head, if not sent yet, and the buffered body bytes. It
// is the method http.ResponseController calls.
func (w *responseWriter) FlushError() error {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if len(w.buf) > 0 {
		err := w.send(w.body(w.buf))
		w.buf = w.buf[:0]
		return err
	}
	return w.err
}

func (w *responseWriter) Flush() { _ = w.FlushError() }

func (w *responseWriter) body(p []byte) *tunnelv1.Frame {
	return &tunnelv1.Frame{Call: w.call.id, Kind: &tunnelv1.Frame_Body{Body: p}}
}

func (w *responseWriter) send(f *tunnelv1.Frame) error {
	if w.err != nil {
		return w.err
	}
	if err := w.call.ctx.Err(); err != nil {
		w.err = err
		return err
	}
	if err := w.s.send(f); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (s *server) send(f *tunnelv1.Frame) error {
	b, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	if err := s.conn.Send(b); err != nil {
		// The session ends before Serve closes conn, so a Send that fails once
		// it has ended failed because of that close and is not the cause.
		if s.ctx.Err() == nil {
			s.sendErr = err
			s.stop()
		}
		return err
	}
	return nil
}
