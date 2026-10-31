package tunnel

import (
	"io"
	"net/http"
	"sync"
)

// bodyBuffer is the body of one call as it arrives in frames: the request body
// on the server side, the response body on the client side. The receive loop
// pushes chunks without blocking, so one call whose reader stalls cannot hold
// up the frames of every other call on the channel.
type bodyBuffer struct {
	mu     sync.Mutex
	cond   sync.Cond
	chunks [][]byte
	size   int
	// limit bounds the unread bytes; 0 means no bound.
	limit int
	// done is set once no more chunks will arrive; err is what Read returns
	// after the last chunk.
	done bool
	err  error
	// closed is set when the reader closes the body.
	closed bool
	// onEarlyClose runs once if the reader closes the body before done.
	onEarlyClose func()
}

func newBodyBuffer(limit int) *bodyBuffer {
	b := &bodyBuffer{limit: limit}
	b.cond.L = &b.mu
	return b
}

// push appends a chunk. It reports false when the chunk would take the unread
// bytes above the limit; the chunk is then dropped. A chunk that arrives after
// the body finished or closed is dropped and reported as accepted.
func (b *bodyBuffer) push(p []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done || b.closed || len(p) == 0 {
		return true
	}
	if b.limit > 0 && b.size+len(p) > b.limit {
		return false
	}
	b.chunks = append(b.chunks, p)
	b.size += len(p)
	b.cond.Broadcast()
	return true
}

// finish marks the end of the body. Read returns err after the last chunk, or
// io.EOF when err is nil. Only the first call has an effect.
func (b *bodyBuffer) finish(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return
	}
	if err == nil {
		err = io.EOF
	}
	b.done, b.err = true, err
	b.cond.Broadcast()
}

func (b *bodyBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.chunks) == 0 && !b.done && !b.closed {
		b.cond.Wait()
	}
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	if len(b.chunks) == 0 {
		return 0, b.err
	}
	n := copy(p, b.chunks[0])
	if n == len(b.chunks[0]) {
		b.chunks[0] = nil
		b.chunks = b.chunks[1:]
	} else {
		b.chunks[0] = b.chunks[0][n:]
	}
	b.size -= n
	return n, nil
}

func (b *bodyBuffer) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.chunks, b.size = nil, 0
	early := !b.done
	b.cond.Broadcast()
	b.mu.Unlock()
	if early && b.onEarlyClose != nil {
		b.onEarlyClose()
	}
	return nil
}
