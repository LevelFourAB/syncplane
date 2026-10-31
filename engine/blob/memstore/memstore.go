// Package memstore is a blob.Store that keeps everything in memory. It is for
// tests and for servers whose blobs need not outlive the process.
package memstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
	"sync"
	"time"

	"github.com/levelfourab/syncplane/engine/blob"
)

// Store is an in-memory blob.Store. The zero value is not usable; call New.
type Store struct {
	mu      sync.Mutex
	now     func() time.Time
	blobs   map[key]*committed
	uploads map[key]*staged
}

type key struct {
	ws     string
	digest blob.Digest
}

type committed struct {
	info blob.Info
	data []byte
}

type staged struct {
	upload blob.Upload
	chunks map[int][]byte
}

// Option configures New.
type Option func(*Store)

// WithClock makes the Store read the time from now instead of time.Now. Tests
// use it to move CommittedAt and StartedAt.
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// New returns an empty Store.
func New(opts ...Option) *Store {
	s := &Store{now: time.Now, blobs: map[key]*committed{}, uploads: map[key]*staged{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ blob.Store = (*Store)(nil)

func (s *Store) Begin(_ context.Context, ws string, m blob.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{ws, m.Digest}
	if _, ok := s.blobs[k]; ok {
		return nil
	}
	if _, ok := s.uploads[k]; ok {
		return nil
	}
	m.ChunkHashes = cloneHashes(m.ChunkHashes)
	s.uploads[k] = &staged{upload: blob.Upload{Manifest: m, StartedAt: s.now()}, chunks: map[int][]byte{}}
	return nil
}

func (s *Store) Staged(_ context.Context, ws string, digest blob.Digest) (blob.Upload, []int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.uploads[key{ws, digest}]
	if !ok {
		return blob.Upload{}, nil, blob.ErrNotFound
	}
	have := make([]int, 0, len(u.chunks))
	for i := range u.chunks {
		have = append(have, i)
	}
	slices.Sort(have)
	out := u.upload
	out.Manifest.ChunkHashes = cloneHashes(out.Manifest.ChunkHashes)
	return out, have, nil
}

func (s *Store) PutChunk(_ context.Context, ws string, digest blob.Digest, index int, r io.Reader) error {
	s.mu.Lock()
	u, ok := s.uploads[key{ws, digest}]
	var m blob.Manifest
	if ok {
		m = u.upload.Manifest
	}
	s.mu.Unlock()
	if !ok {
		return blob.ErrNotFound
	}
	n := blob.ChunkLen(m.Size, index)
	p := make([]byte, n)
	if got, err := io.ReadFull(r, p); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: chunk %d is %d bytes, not %d", blob.ErrInvalid, index, got, n)
		}
		return err
	}
	if err := m.CheckChunk(index, p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// The upload may have been completed or aborted while the chunk was read.
	u, ok = s.uploads[key{ws, digest}]
	if !ok {
		return blob.ErrNotFound
	}
	u.chunks[index] = p
	return nil
}

func (s *Store) Complete(_ context.Context, ws string, digest blob.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{ws, digest}
	if _, ok := s.blobs[k]; ok {
		return nil
	}
	u, ok := s.uploads[k]
	if !ok {
		return blob.ErrNotFound
	}
	n := len(u.upload.Manifest.ChunkHashes)
	if len(u.chunks) < n {
		return blob.ErrIncomplete
	}
	var buf bytes.Buffer
	buf.Grow(int(u.upload.Manifest.Size))
	for i := range n {
		buf.Write(u.chunks[i])
	}
	s.blobs[k] = &committed{
		info: blob.Info{Manifest: u.upload.Manifest, CommittedAt: s.now()},
		data: buf.Bytes(),
	}
	delete(s.uploads, k)
	return nil
}

func (s *Store) Abort(_ context.Context, ws string, digest blob.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.uploads, key{ws, digest})
	return nil
}

func (s *Store) ListStaged(_ context.Context, ws string) iter.Seq2[blob.Upload, error] {
	s.mu.Lock()
	var out []blob.Upload
	for k, u := range s.uploads {
		if k.ws == ws {
			up := u.upload
			up.Manifest.ChunkHashes = cloneHashes(up.Manifest.ChunkHashes)
			out = append(out, up)
		}
	}
	s.mu.Unlock()
	return func(yield func(blob.Upload, error) bool) {
		for _, u := range out {
			if !yield(u, nil) {
				return
			}
		}
	}
}

func (s *Store) Stat(_ context.Context, ws string, digest blob.Digest) (blob.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[key{ws, digest}]
	if !ok {
		return blob.Info{}, blob.ErrNotFound
	}
	info := b.info
	info.Manifest.ChunkHashes = cloneHashes(info.Manifest.ChunkHashes)
	return info, nil
}

func (s *Store) Open(_ context.Context, ws string, digest blob.Digest) (io.ReadSeekCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[key{ws, digest}]
	if !ok {
		return nil, blob.ErrNotFound
	}
	// The bytes of a committed blob are never written again, so the reader
	// can share them.
	return nopCloser{bytes.NewReader(b.data)}, nil
}

func (s *Store) SetUnreferencedSince(_ context.Context, ws string, digest blob.Digest, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[key{ws, digest}]
	if !ok {
		return blob.ErrNotFound
	}
	b.info.UnreferencedSince = t
	return nil
}

func (s *Store) Delete(_ context.Context, ws string, digest blob.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blobs, key{ws, digest})
	return nil
}

func (s *Store) List(_ context.Context, ws string) iter.Seq2[blob.Digest, error] {
	s.mu.Lock()
	var out []blob.Digest
	for k := range s.blobs {
		if k.ws == ws {
			out = append(out, k.digest)
		}
	}
	s.mu.Unlock()
	return func(yield func(blob.Digest, error) bool) {
		for _, d := range out {
			if !yield(d, nil) {
				return
			}
		}
	}
}

func (s *Store) ListWorkspaces(context.Context) iter.Seq2[string, error] {
	s.mu.Lock()
	seen := map[string]struct{}{}
	for k := range s.blobs {
		seen[k.ws] = struct{}{}
	}
	for k := range s.uploads {
		seen[k.ws] = struct{}{}
	}
	s.mu.Unlock()
	return func(yield func(string, error) bool) {
		for ws := range seen {
			if !yield(ws, nil) {
				return
			}
		}
	}
}

func (s *Store) DeleteWorkspace(_ context.Context, ws string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.blobs {
		if k.ws == ws {
			delete(s.blobs, k)
		}
	}
	for k := range s.uploads {
		if k.ws == ws {
			delete(s.uploads, k)
		}
	}
	return nil
}

// Close does nothing. The Store stays usable.
func (s *Store) Close() error { return nil }

func cloneHashes(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i, h := range in {
		out[i] = slices.Clone(h)
	}
	return out
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
