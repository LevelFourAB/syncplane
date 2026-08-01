package server_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
)

// These specs speak the Connect wire protocol by hand rather than through the
// generated client. That is the whole point: a connect-go client negotiates
// Connect-level compression (Connect-Accept-Encoding) which a browser has no
// way to send, so it would take a path that was never broken and never touch
// the one that was. What is emulated here is connect-web — enveloped frames
// over a plain POST, with the Accept-Encoding the browser attaches itself.
var _ = Describe("Streaming response compression (issue 120)", func() {
	const ws = "w1"

	var (
		ctx     context.Context
		baseURL string
		client  syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		path, handler := newTestServer().Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		baseURL = ts.URL
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		// Enough rows of one shape for compression to have something to find:
		// a snapshot's redundancy is across rows, not within one.
		muts := make([]*syncplanev1.Mutation, 0, 64)
		for i := range 64 {
			key := fmt.Sprintf("t%02d", i)
			muts = append(muts, putMutation("writer", uint64(i+1), "todo", key,
				&todov1.Todo{Id: key, Title: fmt.Sprintf("write the documentation, part %d", i)}))
		}
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, muts)))
		Expect(err).NotTo(HaveOccurred())
		Expect(pushResults(resp.Msg)).To(HaveLen(len(muts)))
	})

	// bootstrap issues one browser-shaped Bootstrap request and returns the
	// response together with its raw (still encoded) body. extra, when
	// non-nil, adds headers a browser would not send.
	bootstrapWith := func(acceptEncoding string, extra http.Header) (*http.Response, []byte) {
		GinkgoHelper()
		body := envelope(mustMarshal(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c1"}))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+syncplanev1connect.SyncServiceBootstrapProcedure, bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/connect+proto")
		req.Header.Set("Connect-Protocol-Version", "1")
		for name, values := range extra {
			for _, v := range values {
				req.Header.Add(name, v)
			}
		}
		if acceptEncoding != "" {
			// Setting it explicitly also stops net/http from adding its own and
			// transparently decoding the response, so the spec sees the wire.
			req.Header.Set("Accept-Encoding", acceptEncoding)
		}
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		return resp, raw
	}

	bootstrap := func(acceptEncoding string) (*http.Response, []byte) {
		GinkgoHelper()
		return bootstrapWith(acceptEncoding, nil)
	}

	It("gzips the snapshot for a client that asks with Accept-Encoding", func() {
		resp, raw := bootstrap("gzip, deflate, br")
		Expect(resp.Header.Get("Content-Encoding")).To(Equal("gzip"))
		Expect(resp.Header.Values("Vary")).To(ContainElement("Accept-Encoding"))

		plain := gunzip(raw)
		Expect(len(raw)).To(BeNumerically("<", len(plain)/2),
			"64 rows of one shape should compress by more than half")

		// The frames survive the encoding: a Begin, at least one Chunk holding
		// every row pushed, and an End.
		rows := 0
		var sawBegin, sawEnd bool
		for _, msg := range unenvelope(plain) {
			var out syncplanev1.BootstrapResponse
			Expect(proto.Unmarshal(msg, &out)).To(Succeed())
			switch body := out.GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				sawBegin = true
			case *syncplanev1.BootstrapResponse_Chunk_:
				rows += len(body.Chunk.GetRows())
			case *syncplanev1.BootstrapResponse_End_:
				sawEnd = true
			}
		}
		Expect(sawBegin).To(BeTrue())
		Expect(sawEnd).To(BeTrue())
		Expect(rows).To(Equal(64))
	})

	It("leaves the snapshot alone for a client that does not accept gzip", func() {
		resp, raw := bootstrap("identity")
		Expect(resp.Header.Get("Content-Encoding")).To(BeEmpty())
		Expect(unenvelope(raw)).NotTo(BeEmpty())
	})

	It("honours an explicit refusal of gzip", func() {
		resp, _ := bootstrap("gzip;q=0, identity")
		Expect(resp.Header.Get("Content-Encoding")).To(BeEmpty())
	})

	It("stands aside for a client that negotiated Connect-level compression", func() {
		// Go's http.Transport attaches Accept-Encoding: gzip to every request
		// on its own, so a connect-go client arrives looking like a browser.
		// What separates them is Connect-Accept-Encoding, which only a native
		// client sends — and compressing its stream here would be a second
		// pass over bytes connect-go already compressed per message.
		extra := http.Header{"Connect-Accept-Encoding": []string{"gzip"}}
		resp, _ := bootstrapWith("gzip", extra)
		Expect(resp.Header.Get("Content-Encoding")).To(BeEmpty())

		extra = http.Header{"Grpc-Accept-Encoding": []string{"gzip"}}
		resp, _ = bootstrapWith("gzip", extra)
		Expect(resp.Header.Get("Content-Encoding")).To(BeEmpty())
	})

	It("delivers a live Subscribe frame without waiting for the gzip window", func() {
		// The failure this guards against is silent and only shows up in
		// production: a gzip writer that is not flushed alongside the
		// underlying one holds each frame until enough later frames fill its
		// window, so a poke arrives whenever the NEXT write happens to come.
		// On a quiet workspace that is unbounded.
		body := envelope(mustMarshal(&syncplanev1.SubscribeRequest{
			ClientId: "c1",
			Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
		}))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+syncplanev1connect.SyncServiceSubscribeProcedure, bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/connect+proto")
		req.Header.Set("Connect-Protocol-Version", "1")
		req.Header.Set("Accept-Encoding", "gzip")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("Content-Encoding")).To(Equal("gzip"))

		// gzip.NewReader itself blocks until the header is flushed, so
		// reaching this line already proves the stream is not held whole.
		frames := make(chan *syncplanev1.SubscribeResponse, 64)
		go func() {
			defer GinkgoRecover()
			zr, err := gzip.NewReader(resp.Body)
			if err != nil {
				return
			}
			for {
				msg, ok := readEnvelope(zr)
				if !ok {
					close(frames)
					return
				}
				var out syncplanev1.SubscribeResponse
				if proto.Unmarshal(msg, &out) == nil {
					frames <- &out
				}
			}
		}()

		// Catch-up drains first; the live tail starts after caught_up.
		Eventually(frames, "5s").Should(Receive(WithTransform(
			func(f *syncplanev1.SubscribeResponse) bool { return f.GetCaughtUp() }, BeTrue())))

		// A second client id, so the mutation id starts its own watermark
		// rather than leaving a gap after the 64 the setup pushed.
		pushed, err := client.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			putMutation("live-writer", 1, "todo", "live", &todov1.Todo{Id: "live", Title: "pushed live"}),
		})))
		Expect(err).NotTo(HaveOccurred())
		Expect(pushResults(pushed.Msg)[0].GetStatus()).
			To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		// Short on purpose: the point is promptness, not eventual arrival.
		var live *syncplanev1.SubscribeResponse
		Eventually(frames, "2s").Should(Receive(&live, WithTransform(
			func(f *syncplanev1.SubscribeResponse) int { return len(f.GetPage().GetPatch().GetChanges()) },
			BeNumerically(">", 0))))
		Expect(live.GetPage().GetPatch().GetChanges()[0].GetKey()).To(Equal("live"))
	})

	It("leaves unary responses to Connect's own compression", func() {
		// connect-go compresses these itself off the same Accept-Encoding.
		// Encoding them again here would leave a browser holding a body it
		// decodes once and cannot parse.
		body := mustMarshal(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c1"})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+syncplanev1connect.SyncServicePullProcedure, bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/proto")
		req.Header.Set("Connect-Protocol-Version", "1")
		req.Header.Set("Accept-Encoding", "gzip")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("Content-Encoding")).To(Equal("gzip"))

		var out syncplanev1.PullResponse
		Expect(proto.Unmarshal(gunzip(raw), &out)).To(Succeed(),
			"one gzip layer, not two")
		Expect(out.GetPatch().GetChanges()).To(HaveLen(64))
	})
})

// envelope wraps a message in the Connect streaming frame header: a flags byte
// followed by the big-endian message length.
func envelope(msg []byte) []byte {
	out := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(out[1:5], uint32(len(msg)))
	return append(out, msg...)
}

// unenvelope splits a Connect stream body into its messages, dropping the
// end-of-stream frame (flag bit 1), which carries trailers as JSON rather than
// a response message.
func unenvelope(body []byte) [][]byte {
	GinkgoHelper()
	var msgs [][]byte
	for len(body) > 0 {
		Expect(len(body)).To(BeNumerically(">=", 5), "truncated envelope header")
		flags := body[0]
		size := binary.BigEndian.Uint32(body[1:5])
		Expect(uint32(len(body)-5)).To(BeNumerically(">=", size), "truncated envelope payload")
		if flags&0x02 == 0 {
			msgs = append(msgs, body[5:5+size])
		}
		body = body[5+size:]
	}
	return msgs
}

// readEnvelope reads one Connect frame from a stream still in flight, blocking
// until it arrives. Reports false at end of stream or on the end-of-stream
// frame.
func readEnvelope(r io.Reader) ([]byte, bool) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, false
	}
	msg := make([]byte, binary.BigEndian.Uint32(head[1:5]))
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, false
	}
	if head[0]&0x02 != 0 {
		return nil, false
	}
	return msg, true
}

func gunzip(raw []byte) []byte {
	GinkgoHelper()
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	Expect(err).NotTo(HaveOccurred())
	out, err := io.ReadAll(zr)
	Expect(err).NotTo(HaveOccurred())
	Expect(zr.Close()).To(Succeed())
	return out
}

var _ = Describe("compressStreams wiring", func() {
	It("keeps the handler usable by a connect-go client", func() {
		// The wrapper sits in front of every procedure, so the default client
		// path — Connect-level compression, no Accept-Encoding of its own —
		// has to keep working unchanged.
		path, handler := newTestServer().Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)

		client := syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		stream, err := client.Bootstrap(context.Background(),
			connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: "w1", ClientId: "c1"}))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(stream.Close)
		var begins, ends int
		for stream.Receive() {
			switch stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				begins++
			case *syncplanev1.BootstrapResponse_End_:
				ends++
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		Expect(begins).To(Equal(1))
		Expect(ends).To(Equal(1))
	})
})
