package tunnel_test

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTunnel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tunnel Suite")
}

// memConn is one end of an in-memory message channel: the shape of a host's
// stream API, with no framing of its own. Closing either end closes both.
type memConn struct {
	in   <-chan []byte
	out  chan<- []byte
	done chan struct{}
	once *sync.Once

	// largest is the size of the largest frame this end has sent.
	largest atomic.Int64
}

func memPipe() (a, b *memConn) {
	ab := make(chan []byte, 64)
	ba := make(chan []byte, 64)
	done := make(chan struct{})
	once := &sync.Once{}
	return &memConn{in: ba, out: ab, done: done, once: once},
		&memConn{in: ab, out: ba, done: done, once: once}
}

func (c *memConn) Send(frame []byte) error {
	if n := int64(len(frame)); n > c.largest.Load() {
		c.largest.Store(n)
	}
	cp := append([]byte(nil), frame...)
	select {
	case <-c.done:
		return io.ErrClosedPipe
	default:
	}
	select {
	case c.out <- cp:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	}
}

func (c *memConn) Receive() ([]byte, error) {
	// Frames sent before the close still arrive, as on a socket.
	select {
	case f := <-c.in:
		return f, nil
	default:
	}
	select {
	case f := <-c.in:
		return f, nil
	case <-c.done:
		return nil, io.EOF
	}
}

func (c *memConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}
