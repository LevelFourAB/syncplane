package tunnel_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	tunnelv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/tunnel/v1"
	"github.com/levelfourab/syncplane/engine/tunnel"
)

// served is a handler behind Serve, reached through a Client.
type served struct {
	http       *http.Client
	client     *tunnel.Client
	clientConn *memConn
	serverConn *memConn
	cancel     context.CancelFunc
	done       chan error
}

func serve(h http.Handler, opts ...tunnel.Option) *served {
	clientConn, serverConn := memPipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	opts = append([]tunnel.Option{tunnel.WithLogger(slog.New(slog.DiscardHandler))}, opts...)
	go func() { done <- tunnel.Serve(ctx, serverConn, h, opts...) }()
	c := tunnel.NewClient(clientConn)
	s := &served{
		http:       &http.Client{Transport: c},
		client:     c,
		clientConn: clientConn,
		serverConn: serverConn,
		cancel:     cancel,
		done:       done,
	}
	DeferCleanup(func() {
		cancel()
		_ = c.Close()
	})
	return s
}

// raw is a hand-driven client end of a channel, for specs that pin the frames
// themselves rather than what a Client makes of them.
type raw struct {
	conn *memConn
	done chan error
}

func serveRaw(h http.Handler) *raw {
	clientConn, serverConn := memPipe()
	done := make(chan error, 1)
	go func() {
		done <- tunnel.Serve(context.Background(), serverConn, h, tunnel.WithLogger(slog.New(slog.DiscardHandler)))
	}()
	DeferCleanup(func() { _ = clientConn.Close() })
	return &raw{conn: clientConn, done: done}
}

func (r *raw) send(f *tunnelv1.Frame) {
	b, err := proto.Marshal(f)
	Expect(err).NotTo(HaveOccurred())
	Expect(r.conn.Send(b)).To(Succeed())
}

func (r *raw) receive() *tunnelv1.Frame {
	b, err := r.conn.Receive()
	Expect(err).NotTo(HaveOccurred())
	f := new(tunnelv1.Frame)
	Expect(proto.Unmarshal(b, f)).To(Succeed())
	return f
}

func request(call uint32, method, target string) *tunnelv1.Frame {
	return &tunnelv1.Frame{Call: call, Kind: &tunnelv1.Frame_Request{Request: &tunnelv1.RequestHead{
		Method: method, Target: target,
	}}}
}

func end(call uint32) *tunnelv1.Frame {
	return &tunnelv1.Frame{Call: call, Kind: &tunnelv1.Frame_End{End: &tunnelv1.End{}}}
}

var _ = Describe("Serve and Client", func() {
	It("carries a request and its response", func() {
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			w.Header().Set("X-Echo", r.Header.Get("X-Test"))
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, "%s %s host=%s len=%d body=%s",
				r.Method, r.RequestURI, r.Host, r.ContentLength, body)
		}))
		req, err := http.NewRequest(http.MethodPost, "http://ignored/echo?q=1", strings.NewReader("hello"))
		Expect(err).NotTo(HaveOccurred())
		req.Host = "app.local"
		req.Header.Set("X-Test", "yes")

		resp, err := s.http.Do(req)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusCreated))
		Expect(resp.Header.Get("X-Echo")).To(Equal("yes"))
		body, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(body)).To(Equal("POST /echo?q=1 host=app.local len=5 body=hello"))
	})

	It("gives a request without a body a zero ContentLength", func() {
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%d %t", r.ContentLength, r.Body == http.NoBody)
		}))
		resp, err := s.http.Get("http://ignored/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		Expect(string(body)).To(Equal("0 true"))
	})

	It("carries bodies larger than one frame intact, in frames under 64 KiB", func() {
		payload := make([]byte, 1<<20)
		_, _ = rand.Read(payload)
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sum := sha256.New()
			_, _ = io.Copy(sum, r.Body)
			w.Header().Set("X-Sum", fmt.Sprintf("%x", sum.Sum(nil)))
			_, _ = w.Write(payload)
		}))

		resp, err := s.http.Post("http://ignored/", "application/octet-stream", bytes.NewReader(payload))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.Header.Get("X-Sum")).To(Equal(fmt.Sprintf("%x", sha256.Sum256(payload))))
		got, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))

		Expect(s.clientConn.largest.Load()).To(BeNumerically("<=", 64<<10))
		Expect(s.serverConn.largest.Load()).To(BeNumerically("<=", 64<<10))
	})

	It("sends flushed bytes before the handler returns", func() {
		release := make(chan struct{})
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "first")
			w.(http.Flusher).Flush()
			<-release
			_, _ = io.WriteString(w, "second")
		}))
		resp, err := s.http.Get("http://ignored/")
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		buf := make([]byte, 5)
		_, err = io.ReadFull(resp.Body, buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(buf)).To(Equal("first"))

		close(release)
		rest, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rest)).To(Equal("second"))
	})

	It("supports http.ResponseController flushing", func() {
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "x")
			Expect(http.NewResponseController(w).Flush()).To(Succeed())
		}))
		resp, err := s.http.Get("http://ignored/")
		Expect(err).NotTo(HaveOccurred())
		_ = resp.Body.Close()
	})

	DescribeTable("ends the handler's context when the client abandons the call",
		func(abandon func(cancel context.CancelFunc, body io.Closer)) {
			stopped := make(chan struct{})
			s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(stopped)
			}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://ignored/", nil)
			resp, err := s.http.Do(req)
			Expect(err).NotTo(HaveOccurred())

			abandon(cancel, resp.Body)
			Eventually(stopped).Should(BeClosed())
			_, err = io.ReadAll(resp.Body)
			Expect(err).To(HaveOccurred())
		},
		Entry("by ending the request context", func(cancel context.CancelFunc, _ io.Closer) { cancel() }),
		Entry("by closing the response body", func(_ context.CancelFunc, body io.Closer) { _ = body.Close() }),
	)

	It("returns the request context's error when it ends before the response", func() {
		s := serve(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://ignored/", nil)
		_, err := s.http.Do(req)
		Expect(err).To(MatchError(context.DeadlineExceeded))
	})

	It("fails only the call whose handler panics", func() {
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/panic":
				panic("boom")
			case "/abort":
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}
			_, _ = io.WriteString(w, "ok")
		}))

		_, err := s.http.Get("http://ignored/panic")
		Expect(err).To(MatchError(tunnel.ErrCallFailed))

		resp, err := s.http.Get("http://ignored/abort")
		Expect(err).NotTo(HaveOccurred())
		_, err = io.ReadAll(resp.Body)
		Expect(err).To(MatchError(tunnel.ErrCallFailed))

		resp, err = s.http.Get("http://ignored/fine")
		Expect(err).NotTo(HaveOccurred())
		body, _ := io.ReadAll(resp.Body)
		Expect(string(body)).To(Equal("ok"))
	})

	It("keeps answering other calls while one stream stays open", func() {
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/stream" {
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				return
			}
			_, _ = io.WriteString(w, "unary")
		}))
		stream, err := s.http.Get("http://ignored/stream")
		Expect(err).NotTo(HaveOccurred())
		defer stream.Body.Close()

		for range 20 {
			resp, err := s.http.Get("http://ignored/unary")
			Expect(err).NotTo(HaveOccurred())
			body, _ := io.ReadAll(resp.Body)
			Expect(string(body)).To(Equal("unary"))
		}
	})

	It("fails a call whose unread request body goes above the buffer", func() {
		stopped := make(chan struct{})
		s := serve(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			close(stopped)
		}), tunnel.WithRequestBuffer(64<<10))

		_, err := s.http.Post("http://ignored/", "application/octet-stream", bytes.NewReader(make([]byte, 1<<20)))
		Expect(err).To(MatchError(tunnel.ErrCallFailed))
		Expect(err.Error()).To(ContainSubstring("exceeds"))
		Eventually(stopped).Should(BeClosed())
	})

	It("ends every call and returns nil when ctx ends", func() {
		stopped := make(chan struct{})
		s := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(stopped)
		}))
		resp, err := s.http.Get("http://ignored/")
		Expect(err).NotTo(HaveOccurred())

		s.cancel()
		var serveErr error
		Eventually(s.done).Should(Receive(&serveErr))
		Expect(serveErr).NotTo(HaveOccurred())
		// Serve waits for the handler before it returns.
		Expect(stopped).To(BeClosed())

		_, err = io.ReadAll(resp.Body)
		Expect(err).To(MatchError(tunnel.ErrClosed))
		_, err = s.http.Get("http://ignored/")
		Expect(err).To(MatchError(tunnel.ErrClosed))
	})

	It("returns nil when the client closes the channel", func() {
		s := serve(http.NotFoundHandler())
		Expect(s.client.Close()).To(Succeed())
		var serveErr error
		Eventually(s.done).Should(Receive(&serveErr))
		Expect(serveErr).NotTo(HaveOccurred())
	})

	It("returns the error of a Send that fails", func() {
		failing := errors.New("send failed")
		clientConn, serverConn := memPipe()
		done := make(chan error, 1)
		go func() {
			done <- tunnel.Serve(context.Background(), &failSend{memConn: serverConn, err: failing},
				http.NotFoundHandler())
		}()
		c := tunnel.NewClient(clientConn)
		defer c.Close()
		_, err := (&http.Client{Transport: c}).Get("http://ignored/")
		Expect(err).To(HaveOccurred())
		var serveErr error
		Eventually(done).Should(Receive(&serveErr))
		Expect(serveErr).To(MatchError(failing))
	})
})

type failSend struct {
	*memConn
	err error
}

func (f *failSend) Send([]byte) error { return f.err }

var _ = Describe("the frame protocol", func() {
	hello := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Hello", "1")
		_, _ = io.WriteString(w, "hi")
	})

	It("answers a call with a response head, body frames and an end", func() {
		r := serveRaw(hello)
		r.send(request(7, http.MethodGet, "/"))
		r.send(end(7))

		head := r.receive()
		Expect(head.GetCall()).To(Equal(uint32(7)))
		Expect(head.GetResponse().GetStatus()).To(Equal(uint32(200)))
		Expect(head.GetResponse().GetHeaders()).To(ContainElement(
			HaveField("Name", "X-Hello")))
		body := r.receive()
		Expect(body.GetBody()).To(Equal([]byte("hi")))
		fin := r.receive()
		Expect(fin.GetEnd()).NotTo(BeNil())
		Expect(fin.GetEnd().GetError()).To(BeEmpty())
	})

	It("ignores frames of a kind it does not know, and frames for calls it does not know", func() {
		r := serveRaw(hello)
		r.send(&tunnelv1.Frame{Call: 3})
		r.send(&tunnelv1.Frame{Call: 99, Kind: &tunnelv1.Frame_Body{Body: []byte("stray")}})
		r.send(end(99))
		r.send(&tunnelv1.Frame{Call: 99, Kind: &tunnelv1.Frame_Cancel{Cancel: &tunnelv1.Cancel{}}})
		r.send(request(1, http.MethodGet, "/"))
		r.send(end(1))
		Expect(r.receive().GetResponse()).NotTo(BeNil())
	})

	It("sends nothing more for a call the client cancels", func() {
		r := serveRaw(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			<-req.Context().Done()
			_, _ = io.WriteString(w, "late")
		}))
		r.send(request(1, http.MethodGet, "/"))
		r.send(&tunnelv1.Frame{Call: 1, Kind: &tunnelv1.Frame_Cancel{Cancel: &tunnelv1.Cancel{}}})
		r.send(request(2, http.MethodGet, "/"))
		r.send(&tunnelv1.Frame{Call: 2, Kind: &tunnelv1.Frame_Cancel{Cancel: &tunnelv1.Cancel{}}})
		Consistently(func() bool {
			select {
			case f := <-r.conn.in:
				Fail(fmt.Sprintf("unexpected frame %v", f))
			default:
			}
			return true
		}, 100*time.Millisecond).Should(BeTrue())
	})

	It("fails a call whose target is not an absolute path", func() {
		r := serveRaw(hello)
		for i, target := range []string{"", "relative", "http://host/x"} {
			id := uint32(i + 1)
			r.send(request(id, http.MethodGet, target))
			f := r.receive()
			Expect(f.GetCall()).To(Equal(id))
			Expect(f.GetEnd().GetError()).To(ContainSubstring("invalid request target"))
		}
	})

	It("reads a target that starts with two slashes as a path", func() {
		r := serveRaw(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			fmt.Fprintf(w, "host=%q path=%q", req.URL.Host, req.URL.Path)
		}))
		r.send(request(1, http.MethodGet, "//evil/x"))
		r.send(end(1))
		Expect(r.receive().GetResponse()).NotTo(BeNil())
		Expect(string(r.receive().GetBody())).To(Equal(`host="" path="//evil/x"`))
	})

	DescribeTable("closes the channel and returns ErrProtocol",
		func(frame []byte) {
			r := serveRaw(hello)
			Expect(r.conn.Send(frame)).To(Succeed())
			var err error
			Eventually(r.done).Should(Receive(&err))
			Expect(err).To(MatchError(tunnel.ErrProtocol))
			_, err = r.conn.Receive()
			Expect(err).To(MatchError(io.EOF))
		},
		Entry("for bytes that are not a frame", []byte{0xff, 0xff, 0xff}),
		Entry("for a request with call id 0", mustMarshal(request(0, http.MethodGet, "/"))),
		Entry("for a response frame from the client", mustMarshal(&tunnelv1.Frame{
			Call: 1, Kind: &tunnelv1.Frame_Response{Response: &tunnelv1.ResponseHead{Status: 200}},
		})),
	)

	It("returns ErrProtocol for a request that reuses a call id in flight", func() {
		r := serveRaw(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
			<-req.Context().Done()
		}))
		r.send(request(1, http.MethodGet, "/"))
		r.send(request(1, http.MethodGet, "/"))
		var err error
		Eventually(r.done).Should(Receive(&err))
		Expect(err).To(MatchError(tunnel.ErrProtocol))
	})
})

func mustMarshal(f *tunnelv1.Frame) []byte {
	b, err := proto.Marshal(f)
	if err != nil {
		panic(err)
	}
	return b
}
