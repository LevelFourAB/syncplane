package tunnel

import (
	"errors"
	"net/http"
	"slices"

	tunnelv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/tunnel/v1"
)

// ErrProtocol is returned by Serve, and by calls through a Client, when the
// other end sends something that is not a valid tunnel frame or breaks the
// order the protocol requires. The channel is closed when it happens.
var ErrProtocol = errors.New("tunnel: protocol violation")

// ErrCallFailed is returned for a call that the other end ended with an error,
// such as a handler that aborted mid-response. The returned error wraps it and
// carries the reason the other end gave.
var ErrCallFailed = errors.New("tunnel: call failed")

// ErrClosed is returned for a call through a Client whose channel closed before
// the call finished, and for any call started after that.
var ErrClosed = errors.New("tunnel: channel closed")

// chunkSize bounds the body bytes in one frame, below the 64 KiB the protocol
// asks senders to stay under.
const chunkSize = 32 << 10

func encodeHeaders(h http.Header) []*tunnelv1.Header {
	// Sorted so the same headers always encode to the same bytes.
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]*tunnelv1.Header, 0, len(names))
	for _, name := range names {
		out = append(out, &tunnelv1.Header{Name: name, Values: h[name]})
	}
	return out
}

func decodeHeaders(in []*tunnelv1.Header) http.Header {
	h := make(http.Header, len(in))
	for _, header := range in {
		for _, v := range header.GetValues() {
			h.Add(header.GetName(), v)
		}
	}
	return h
}
