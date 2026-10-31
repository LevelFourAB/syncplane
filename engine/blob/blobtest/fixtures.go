package blobtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
)

// clock is the fake time source the suite hands a Store. It starts at a time
// with a non-zero nanosecond part, so a Store that drops precision fails.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock {
	return &clock{t: time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fixture is a blob the specs store: its bytes and manifest.
type fixture struct {
	data []byte
	m    blob.Manifest
}

func (f fixture) digest() blob.Digest { return f.m.Digest }

func (f fixture) chunk(i int) []byte {
	start := i * blob.ChunkSize
	return f.data[start : start+blob.ChunkLen(f.m.Size, i)]
}

func makeFixture(size int, seed byte) fixture {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i%251) ^ seed
	}
	m, err := blob.Compute(bytes.NewReader(data))
	if err != nil {
		panic(err)
	}
	return fixture{data: data, m: m}
}

// The fixtures are built once per process: the multi-chunk ones take tens of
// milliseconds to hash.
var fixtures = sync.OnceValue(func() map[string]fixture {
	return map[string]fixture{
		"empty": makeFixture(0, 0),
		"small": makeFixture(1000, 1),
		"other": makeFixture(1000, 2),
		"two":   makeFixture(blob.ChunkSize+10, 3),
		"three": makeFixture(2*blob.ChunkSize+1000, 4),
	}
})

func fix(name string) fixture { return fixtures()[name] }

// helpers binds the spec helpers to the store under test.
type helpers struct {
	ctx context.Context
}

func (h helpers) begin(s blob.Store, ws string, f fixture) {
	GinkgoHelper()
	Expect(s.Begin(h.ctx, ws, f.m)).To(Succeed())
}

func (h helpers) put(s blob.Store, ws string, f fixture, i int) {
	GinkgoHelper()
	Expect(s.PutChunk(h.ctx, ws, f.digest(), i, bytes.NewReader(f.chunk(i)))).To(Succeed())
}

func (h helpers) putAll(s blob.Store, ws string, f fixture) {
	GinkgoHelper()
	for i := range f.m.ChunkHashes {
		h.put(s, ws, f, i)
	}
}

func (h helpers) commit(s blob.Store, ws string, f fixture) {
	GinkgoHelper()
	h.begin(s, ws, f)
	h.putAll(s, ws, f)
	Expect(s.Complete(h.ctx, ws, f.digest())).To(Succeed())
}

func (h helpers) read(s blob.Store, ws string, d blob.Digest) []byte {
	GinkgoHelper()
	r, err := s.Open(h.ctx, ws, d)
	Expect(err).NotTo(HaveOccurred())
	defer func() { Expect(r.Close()).To(Succeed()) }()
	b, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func (h helpers) staged(s blob.Store, ws string, d blob.Digest) (blob.Upload, []int) {
	GinkgoHelper()
	u, have, err := s.Staged(h.ctx, ws, d)
	Expect(err).NotTo(HaveOccurred())
	return u, have
}

func (h helpers) stat(s blob.Store, ws string, d blob.Digest) blob.Info {
	GinkgoHelper()
	info, err := s.Stat(h.ctx, ws, d)
	Expect(err).NotTo(HaveOccurred())
	return info
}

func (h helpers) list(s blob.Store, ws string) []blob.Digest {
	GinkgoHelper()
	return collect(s.List(h.ctx, ws))
}

func (h helpers) listStaged(s blob.Store, ws string) []blob.Digest {
	GinkgoHelper()
	var out []blob.Digest
	for _, u := range collect(s.ListStaged(h.ctx, ws)) {
		out = append(out, u.Manifest.Digest)
	}
	return out
}

func (h helpers) workspaces(s blob.Store) []string {
	GinkgoHelper()
	return collect(s.ListWorkspaces(h.ctx))
}

func (h helpers) expectGone(s blob.Store, ws string, d blob.Digest) {
	GinkgoHelper()
	_, err := s.Stat(h.ctx, ws, d)
	Expect(err).To(MatchError(blob.ErrNotFound), "Stat")
	_, err = s.Open(h.ctx, ws, d)
	Expect(err).To(MatchError(blob.ErrNotFound), "Open")
	Expect(h.list(s, ws)).NotTo(ContainElement(d), "List")
}

func (h helpers) expectNotStaged(s blob.Store, ws string, d blob.Digest) {
	GinkgoHelper()
	_, _, err := s.Staged(h.ctx, ws, d)
	Expect(err).To(MatchError(blob.ErrNotFound), "Staged")
	Expect(h.listStaged(s, ws)).NotTo(ContainElement(d), "ListStaged")
}

func collect[T any](seq func(func(T, error) bool)) []T {
	GinkgoHelper()
	var out []T
	for v, err := range seq {
		Expect(err).NotTo(HaveOccurred())
		out = append(out, v)
	}
	return out
}

// expectManifest compares manifests by value. A nil and an empty hash list
// are the same manifest.
func expectManifest(got, want blob.Manifest) {
	GinkgoHelper()
	Expect(got.Digest).To(Equal(want.Digest))
	Expect(got.Size).To(Equal(want.Size))
	Expect(got.ChunkHashes).To(HaveLen(len(want.ChunkHashes)))
	for i := range want.ChunkHashes {
		Expect(got.ChunkHashes[i]).To(Equal(want.ChunkHashes[i]), "chunk hash %d", i)
	}
}

// upperDigest is d with its hex digits in upper case: a malformed digest that
// a case-insensitive file system could confuse with d.
func upperDigest(d blob.Digest) blob.Digest {
	s := []byte(d)
	for i := 4; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'f' {
			s[i] -= 'a' - 'A'
		}
	}
	return blob.Digest(s)
}

func sorted[T interface{ ~string }](in []T) []T {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}

var errBoom = errors.New("blobtest: reader failed")

// failingReader fails every read with err.
type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }
