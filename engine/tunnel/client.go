package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"google.golang.org/protobuf/proto"

	tunnelv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/tunnel/v1"
)

// Client sends HTTP requests through a tunnel to a peer running Serve. It
// implements http.RoundTripper and multiplexes every request over one Conn.
//
// A Client is safe for concurrent use. Once its channel closes, by Close, by
// the peer or by a failure, every call in flight fails and so does every later
// RoundTrip, with an error that wraps ErrClosed or ErrProtocol. Build a new
// Client over a new Conn to reconnect.
type Client struct {
	conn Conn

	sendMu sync.Mutex

	mu    sync.Mutex
	calls map[uint32]*clientCall
	next  uint32
	err   error

	closeOnce sync.Once
	closeErr  error
}

type clientCall struct {
	id   uint32
	body *bodyBuffer

	// ready is closed once head or err is set. Both, and resolved, are guarded
	// by Client.mu.
	ready    chan struct{}
	resolved bool
	head     *tunnelv1.ResponseHead
	err      error

	// stopWatch stops the context watch; closeReq closes the request body.
	// Both are safe to call more than once.
	stopWatch func() bool
	closeReq  func()
}

// NewClient returns a Client that owns conn and reads from it until it closes.
// Close the Client to release conn.
func NewClient(conn Conn) *Client {
	c := &Client{conn: conn, calls: make(map[uint32]*clientCall)}
	go c.receive()
	return c
}

// RoundTrip implements http.RoundTripper.
//
// The request URL's scheme and host are ignored: the call goes to the path and
// query on the peer's handler. A non-empty req.Host goes as a Host header. The
// response body streams as frames arrive and must be closed; closing it before
// the end cancels the call on the peer, and so does ending the request
// context. Response bytes are held in memory, without bound, until the caller
// reads them.
//
// A call the peer fails returns an error that wraps ErrCallFailed: from
// RoundTrip before the response head arrives, from a body Read after.
func (c *Client) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	var closeReqOnce sync.Once
	closeReq := func() {
		closeReqOnce.Do(func() {
			if req.Body != nil {
				_ = req.Body.Close()
			}
		})
	}
	if err := ctx.Err(); err != nil {
		closeReq()
		return nil, err
	}

	call, err := c.register(closeReq)
	if err != nil {
		closeReq()
		return nil, err
	}

	header := req.Header.Clone()
	if header == nil {
		header = make(http.Header)
	}
	if req.Host != "" {
		header.Set("Host", req.Host)
	}
	hasBody := req.Body != nil && req.Body != http.NoBody
	switch {
	case !hasBody:
		header.Set("Content-Length", "0")
	case req.ContentLength > 0:
		header.Set("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	if err := c.send(&tunnelv1.Frame{Call: call.id, Kind: &tunnelv1.Frame_Request{Request: &tunnelv1.RequestHead{
		Method:  method,
		Target:  req.URL.RequestURI(),
		Headers: encodeHeaders(header),
	}}}); err != nil {
		c.remove(call)
		closeReq()
		return nil, err
	}
	go c.sendBody(call, req.Body, hasBody)

	stopWatch := context.AfterFunc(ctx, func() { c.cancel(call, ctx.Err()) })
	c.mu.Lock()
	call.stopWatch = stopWatch
	_, live := c.calls[call.id]
	c.mu.Unlock()
	if !live {
		// The call ended before the watch was in place, so remove has
		// already run and could not stop it.
		stopWatch()
	}

	<-call.ready
	if call.err != nil {
		return nil, call.err
	}
	code := int(call.head.GetStatus())
	resp := &http.Response{
		Status:        fmt.Sprintf("%d %s", code, http.StatusText(code)),
		StatusCode:    code,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        decodeHeaders(call.head.GetHeaders()),
		Body:          call.body,
		ContentLength: -1,
		Request:       req,
	}
	if n, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64); err == nil && n >= 0 {
		resp.ContentLength = n
	}
	return resp, nil
}

// Close closes the channel and fails every call in flight with an error that
// wraps ErrClosed. It returns the error from closing the Conn, the same on
// every call.
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	return c.closeErr
}

func (c *Client) register(closeReq func()) (*clientCall, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	for {
		c.next++
		if _, used := c.calls[c.next]; c.next != 0 && !used {
			break
		}
	}
	call := &clientCall{
		id:        c.next,
		body:      newBodyBuffer(0),
		ready:     make(chan struct{}),
		stopWatch: func() bool { return false },
		closeReq:  closeReq,
	}
	call.body.onEarlyClose = func() { c.cancel(call, context.Canceled) }
	c.calls[call.id] = call
	return call, nil
}

// remove takes call out of flight and reports whether it was still in flight.
func (c *Client) remove(call *clientCall) bool {
	c.mu.Lock()
	_, live := c.calls[call.id]
	delete(c.calls, call.id)
	stopWatch := call.stopWatch
	c.mu.Unlock()
	stopWatch()
	// A request body may still be sending; closing it stops sendBody, and a
	// RoundTripper must close the body in any case.
	call.closeReq()
	return live
}

// resolve settles RoundTrip's wait with err, or with the head when err is nil,
// and reports whether this call did it. Caller holds c.mu.
func (call *clientCall) resolve(head *tunnelv1.ResponseHead, err error) bool {
	if call.resolved {
		return false
	}
	call.resolved, call.head, call.err = true, head, err
	close(call.ready)
	return true
}

// fail ends call with err, wherever it is: before the head, RoundTrip returns
// err; after, the next body Read does.
func (c *Client) fail(call *clientCall, err error) {
	c.mu.Lock()
	call.resolve(nil, err)
	c.mu.Unlock()
	call.body.finish(err)
}

// cancel abandons call on the client side and tells the peer, unless the call
// has already ended.
func (c *Client) cancel(call *clientCall, err error) {
	if !c.remove(call) {
		return
	}
	c.fail(call, err)
	_ = c.send(&tunnelv1.Frame{Call: call.id, Kind: &tunnelv1.Frame_Cancel{Cancel: &tunnelv1.Cancel{}}})
}

func (c *Client) sendBody(call *clientCall, body io.Reader, hasBody bool) {
	var reason string
	if hasBody {
		buf := make([]byte, chunkSize)
		for {
			n, err := body.Read(buf)
			if n > 0 {
				if !c.live(call) {
					return
				}
				// Marshal copies the bytes, so buf is free to reuse.
				if c.send(&tunnelv1.Frame{Call: call.id, Kind: &tunnelv1.Frame_Body{Body: buf[:n]}}) != nil {
					return
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				reason = err.Error()
				break
			}
		}
		call.closeReq()
	}
	if c.live(call) {
		_ = c.send(&tunnelv1.Frame{Call: call.id, Kind: &tunnelv1.Frame_End{End: &tunnelv1.End{Error: reason}}})
	}
}

func (c *Client) live(call *clientCall) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.calls[call.id]
	return ok
}

func (c *Client) lookup(id uint32) *clientCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[id]
}

func (c *Client) send(f *tunnelv1.Frame) error {
	b, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	closed := c.err
	c.mu.Unlock()
	if closed != nil {
		return closed
	}
	if err := c.conn.Send(b); err != nil {
		err = fmt.Errorf("%w: %v", ErrClosed, err)
		go c.shutdown(err)
		return err
	}
	return nil
}

// receive reads frames until the channel fails. Like the server's loop, it
// never sends, so a full channel cannot stop the frames that drain it.
func (c *Client) receive() {
	for {
		raw, err := c.conn.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.shutdown(ErrClosed)
			} else {
				c.shutdown(fmt.Errorf("%w: %v", ErrClosed, err))
			}
			return
		}
		f := new(tunnelv1.Frame)
		if err := proto.Unmarshal(raw, f); err != nil {
			c.shutdown(protocolError("malformed frame: %v", err))
			return
		}
		if err := c.dispatch(f); err != nil {
			c.shutdown(err)
			return
		}
	}
}

func (c *Client) dispatch(f *tunnelv1.Frame) error {
	call := c.lookup(f.GetCall())
	switch kind := f.GetKind().(type) {
	case *tunnelv1.Frame_Response:
		if call == nil {
			return nil
		}
		c.mu.Lock()
		first := call.resolve(kind.Response, nil)
		c.mu.Unlock()
		if !first {
			return protocolError("second response head for call %d", call.id)
		}
	case *tunnelv1.Frame_Body:
		if call == nil {
			return nil
		}
		c.mu.Lock()
		resolved := call.resolved
		c.mu.Unlock()
		if !resolved {
			return protocolError("body before the response head for call %d", call.id)
		}
		call.body.push(kind.Body)
	case *tunnelv1.Frame_End:
		if call == nil {
			return nil
		}
		c.remove(call)
		var err error
		if reason := kind.End.GetError(); reason != "" {
			err = fmt.Errorf("%w: %s", ErrCallFailed, reason)
		}
		c.mu.Lock()
		if !call.resolved && err == nil {
			err = fmt.Errorf("%w: ended without a response", ErrCallFailed)
		}
		call.resolve(nil, err)
		c.mu.Unlock()
		call.body.finish(err)
	case *tunnelv1.Frame_Request, *tunnelv1.Frame_Cancel:
		return protocolError("client-only frame from the server")
	default:
		// A kind this version does not know; see the server's dispatch.
	}
	return nil
}

// shutdown closes the channel once and fails every call in flight with err.
func (c *Client) shutdown(err error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.err = err
		calls := make([]*clientCall, 0, len(c.calls))
		watches := make([]func() bool, 0, len(c.calls))
		for _, call := range c.calls {
			calls = append(calls, call)
			// Read under the lock: RoundTrip sets stopWatch under it.
			watches = append(watches, call.stopWatch)
		}
		c.calls = make(map[uint32]*clientCall)
		c.mu.Unlock()
		c.closeErr = c.conn.Close()
		for i, call := range calls {
			watches[i]()
			call.closeReq()
			c.fail(call, err)
		}
	})
}
