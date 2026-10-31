package tunnel

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// Conn is one message channel between a tunnel client and a server.
//
// Send transmits one frame as one message. Receive blocks until the next
// message arrives and returns it whole; it returns io.EOF, or another error,
// once the channel has closed. Messages must arrive complete, in the order
// they were sent, and without loss.
//
// The tunnel never calls Send concurrently with another Send, nor Receive
// concurrently with another Receive, but it calls Send, Receive and Close
// concurrently with each other. Close must unblock a pending Receive and a
// pending Send, and may be called more than once. The tunnel does not modify a frame after it passes
// it to Send, and does not keep a slice that Receive returned.
type Conn interface {
	Send(frame []byte) error
	Receive() ([]byte, error)
	Close() error
}

// MaxFrameSize is the largest frame, in bytes, that a Conn from NewStreamConn
// sends or receives. The tunnel itself never produces a frame near it.
const MaxFrameSize = 4 << 20

// ErrFrameTooLarge is returned by a Conn from NewStreamConn for a frame above
// MaxFrameSize.
var ErrFrameTooLarge = errors.New("tunnel: frame exceeds MaxFrameSize")

// NewStreamConn returns a Conn that carries frames over a byte stream, such as
// stdio, a pipe or a socket. Each frame is written as its length, a 4-byte
// big-endian unsigned integer, followed by the frame itself. The TypeScript
// fromByteStream adapter uses the same framing.
//
// Receive returns io.EOF when the stream ends between two frames, and
// io.ErrUnexpectedEOF when it ends inside one. A frame above MaxFrameSize makes
// Send and Receive return ErrFrameTooLarge; after Receive does, the stream is
// out of step and must be closed.
//
// The Conn owns rwc: Close closes it. Send is safe for concurrent use.
func NewStreamConn(rwc io.ReadWriteCloser) Conn {
	return &streamConn{rwc: rwc, r: bufio.NewReader(rwc)}
}

type streamConn struct {
	rwc io.ReadWriteCloser
	r   *bufio.Reader

	writeMu sync.Mutex
}

func (c *streamConn) Send(frame []byte) error {
	if len(frame) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	// One write per frame: a reader on the other end of a pipe sees the
	// length and the frame together, and two writers can never interleave.
	buf := make([]byte, 4+len(frame))
	binary.BigEndian.PutUint32(buf, uint32(len(frame)))
	copy(buf[4:], frame)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.rwc.Write(buf)
	return err
}

func (c *streamConn) Receive() ([]byte, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(c.r, prefix[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	frame := make([]byte, n)
	if _, err := io.ReadFull(c.r, frame); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return frame, nil
}

func (c *streamConn) Close() error {
	return c.rwc.Close()
}
