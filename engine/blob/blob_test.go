package blob_test

import (
	"bytes"
	"errors"
	"io"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
)

const validHex = "9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50"

var _ = Describe("ParseDigest", func() {
	It("accepts sp1: and 64 lowercase hex digits", func() {
		d, err := blob.ParseDigest("sp1:" + validHex)
		Expect(err).NotTo(HaveOccurred())
		Expect(d).To(Equal(blob.Digest("sp1:" + validHex)))
		Expect(d.String()).To(Equal("sp1:" + validHex))
	})

	DescribeTable("refuses a malformed digest",
		func(s string) {
			d, err := blob.ParseDigest(s)
			Expect(err).To(MatchError(blob.ErrInvalid))
			Expect(d).To(BeZero())
		},
		Entry("empty", ""),
		Entry("no prefix", validHex),
		Entry("other prefix", "sp2:"+validHex),
		Entry("upper-case prefix", "SP1:"+validHex),
		Entry("prefix only", "sp1:"),
		Entry("one digit short", "sp1:"+validHex[:63]),
		Entry("one digit long", "sp1:"+validHex+"0"),
		Entry("upper-case hex", "sp1:"+strings.ToUpper(validHex)),
		Entry("non-hex digit", "sp1:"+validHex[:63]+"g"),
		Entry("path traversal", "sp1:../"+validHex[:61]),
		Entry("leading space", " sp1:"+validHex),
		Entry("trailing newline", "sp1:"+validHex+"\n"),
	)
})

var _ = Describe("ChunkCount", func() {
	DescribeTable("counts chunks",
		func(size int64, want int) {
			Expect(blob.ChunkCount(size)).To(Equal(want))
		},
		Entry("negative", int64(-1), 0),
		Entry("empty", int64(0), 0),
		Entry("one byte", int64(1), 1),
		Entry("one chunk minus one", int64(blob.ChunkSize-1), 1),
		Entry("exactly one chunk", int64(blob.ChunkSize), 1),
		Entry("one chunk plus one", int64(blob.ChunkSize+1), 2),
		Entry("exactly three chunks", int64(3*blob.ChunkSize), 3),
		Entry("above 4 GiB", int64(5<<30)+1, 641),
	)
})

var _ = Describe("ChunkLen", func() {
	DescribeTable("sizes a chunk",
		func(size int64, index, want int) {
			Expect(blob.ChunkLen(size, index)).To(Equal(want))
		},
		Entry("empty blob", int64(0), 0, 0),
		Entry("negative size", int64(-5), 0, 0),
		Entry("negative index", int64(10), -1, 0),
		Entry("only chunk of a small blob", int64(10), 0, 10),
		Entry("past the only chunk", int64(10), 1, 0),
		Entry("exactly one chunk", int64(blob.ChunkSize), 0, blob.ChunkSize),
		Entry("first of two", int64(blob.ChunkSize+1), 0, blob.ChunkSize),
		Entry("one-byte tail", int64(blob.ChunkSize+1), 1, 1),
		Entry("past the tail", int64(blob.ChunkSize+1), 2, 0),
		Entry("full last chunk", int64(2*blob.ChunkSize), 1, blob.ChunkSize),
	)

	It("sums to the size", func() {
		for _, size := range []int64{0, 1, blob.ChunkSize, 2*blob.ChunkSize + 7} {
			var sum int64
			for i := range blob.ChunkCount(size) {
				sum += int64(blob.ChunkLen(size, i))
			}
			Expect(sum).To(Equal(size))
		}
	})
})

var _ = Describe("Compute", func() {
	It("returns the reader's error", func() {
		boom := errors.New("boom")
		_, err := blob.Compute(io.MultiReader(strings.NewReader("abc"), &failing{err: boom}))
		Expect(err).To(MatchError(boom))
	})

	It("gives an empty reader no chunks", func() {
		m, err := blob.Compute(bytes.NewReader(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(m.Size).To(BeZero())
		Expect(m.ChunkHashes).To(BeEmpty())
		Expect(m.Digest).To(Equal(blob.DigestOf(0, nil)))
	})
})

type failing struct{ err error }

func (f *failing) Read([]byte) (int, error) { return 0, f.err }

// manifestOf returns the manifest of p.
func manifestOf(p []byte) blob.Manifest {
	GinkgoHelper()
	m, err := blob.Compute(bytes.NewReader(p))
	Expect(err).NotTo(HaveOccurred())
	return m
}

var _ = Describe("Manifest", func() {
	two := make([]byte, blob.ChunkSize+3)
	for i := range two {
		two[i] = byte(i % 7)
	}

	Describe("Validate", func() {
		It("accepts a computed manifest", func() {
			Expect(manifestOf(two).Validate()).To(Succeed())
			Expect(manifestOf(nil).Validate()).To(Succeed())
		})

		DescribeTable("refuses an inconsistent manifest",
			func(edit func(*blob.Manifest)) {
				m := manifestOf(two)
				edit(&m)
				Expect(m.Validate()).To(MatchError(blob.ErrInvalid))
			},
			Entry("zero digest", func(m *blob.Manifest) { m.Digest = "" }),
			Entry("malformed digest", func(m *blob.Manifest) { m.Digest = "sp1:xyz" }),
			Entry("negative size", func(m *blob.Manifest) {
				m.Size = -1
				m.ChunkHashes = nil
				m.Digest = blob.DigestOf(0, nil)
			}),
			Entry("size with one chunk fewer", func(m *blob.Manifest) { m.Size = blob.ChunkSize }),
			Entry("size with one chunk more", func(m *blob.Manifest) { m.Size = 2*blob.ChunkSize + 1 }),
			Entry("missing hash", func(m *blob.Manifest) {
				m.ChunkHashes = m.ChunkHashes[:1]
				m.Digest = blob.DigestOf(m.Size, m.ChunkHashes)
			}),
			Entry("short hash", func(m *blob.Manifest) {
				m.ChunkHashes[1] = m.ChunkHashes[1][:blob.HashSize-1]
				m.Digest = blob.DigestOf(m.Size, m.ChunkHashes)
			}),
			Entry("long hash", func(m *blob.Manifest) {
				m.ChunkHashes[1] = append(bytes.Clone(m.ChunkHashes[1]), 0)
				m.Digest = blob.DigestOf(m.Size, m.ChunkHashes)
			}),
			Entry("hashes in the wrong order", func(m *blob.Manifest) {
				m.ChunkHashes[0], m.ChunkHashes[1] = m.ChunkHashes[1], m.ChunkHashes[0]
			}),
			Entry("digest of other bytes", func(m *blob.Manifest) { m.Digest = manifestOf([]byte("x")).Digest }),
		)

		It("refuses a wrong size that keeps the chunk count", func() {
			// The digest commits to the size, so a manifest cannot claim
			// another length for the last chunk and block the real upload.
			m := manifestOf(two)
			m.Size++
			Expect(m.Validate()).To(MatchError(blob.ErrInvalid))
		})
	})

	Describe("CheckChunk", func() {
		m := manifestOf(two)

		It("accepts every chunk", func() {
			Expect(m.CheckChunk(0, two[:blob.ChunkSize])).To(Succeed())
			Expect(m.CheckChunk(1, two[blob.ChunkSize:])).To(Succeed())
		})

		DescribeTable("refuses a chunk that does not match",
			func(index int, p func() []byte) {
				Expect(m.CheckChunk(index, p())).To(MatchError(blob.ErrInvalid))
			},
			Entry("negative index", -1, func() []byte { return two[:blob.ChunkSize] }),
			Entry("index past the end", 2, func() []byte { return two[blob.ChunkSize:] }),
			Entry("another chunk's bytes", 1, func() []byte { return two[:blob.ChunkSize] }),
			Entry("short", 1, func() []byte { return two[blob.ChunkSize : len(two)-1] }),
			Entry("long", 1, func() []byte { return append(bytes.Clone(two[blob.ChunkSize:]), 0) }),
			Entry("one byte changed", 1, func() []byte {
				p := bytes.Clone(two[blob.ChunkSize:])
				p[0] ^= 1
				return p
			}),
			Entry("nil", 0, func() []byte { return nil }),
		)

		It("refuses every chunk of an empty blob", func() {
			Expect(manifestOf(nil).CheckChunk(0, nil)).To(MatchError(blob.ErrInvalid))
		})
	})
})
