package blob_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
)

// vectorsPath is the shared cross-language conformance file; the TS client
// hashes the same cases.
const vectorsPath = "../../testdata/blob/vectors.json"

type vectorFile struct {
	ChunkSize int `json:"chunkSize"`
	Cases     []struct {
		Name        string   `json:"name"`
		Size        int64    `json:"size"`
		Pattern     string   `json:"pattern"`
		Text        string   `json:"text"`
		Digest      string   `json:"digest"`
		ChunkHashes []string `json:"chunkHashes"`
	} `json:"cases"`
}

// loadVectors reads the vector file at tree-construction time, so each case
// becomes its own spec.
func loadVectors() vectorFile {
	b, err := os.ReadFile(vectorsPath)
	if err != nil {
		panic(err)
	}
	var v vectorFile
	if err := json.Unmarshal(b, &v); err != nil {
		panic(err)
	}
	return v
}

// mod251 yields byte i = i % 251 for size bytes without holding them all.
type mod251 struct{ pos, size int64 }

func (r *mod251) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if rest := r.size - r.pos; n > rest {
		n = rest
	}
	for i := range n {
		p[i] = byte((r.pos + i) % 251)
	}
	r.pos += n
	return int(n), nil
}

var _ = Describe("digest vectors", func() {
	v := loadVectors()

	It("has cases over the package's chunk size", func() {
		Expect(v.Cases).NotTo(BeEmpty())
		Expect(v.ChunkSize).To(Equal(blob.ChunkSize))
	})

	for _, c := range v.Cases {
		Describe(c.Name, func() {
			source := func() io.Reader {
				switch c.Pattern {
				case "mod251":
					return &mod251{size: c.Size}
				case "text":
					Expect(int64(len(c.Text))).To(Equal(c.Size), "text and size disagree")
					return bytes.NewReader([]byte(c.Text))
				}
				Fail("unknown pattern " + c.Pattern)
				return nil
			}
			wantHashes := func() [][]byte {
				out := make([][]byte, len(c.ChunkHashes))
				for i, h := range c.ChunkHashes {
					b, err := hex.DecodeString(h)
					Expect(err).NotTo(HaveOccurred())
					out[i] = b
				}
				return out
			}

			It("computes the manifest", func() {
				m, err := blob.Compute(source())
				Expect(err).NotTo(HaveOccurred())
				Expect(m.Digest).To(Equal(blob.Digest(c.Digest)))
				Expect(m.Size).To(Equal(c.Size))
				Expect(m.ChunkHashes).To(HaveLen(len(c.ChunkHashes)))
				for i, h := range wantHashes() {
					Expect(m.ChunkHashes[i]).To(Equal(h), "chunk %d", i)
				}
				Expect(m.Validate()).To(Succeed())
			})

			It("derives the digest from the chunk hashes", func() {
				Expect(blob.DigestOf(c.Size, wantHashes())).To(Equal(blob.Digest(c.Digest)))
			})

			It("has a chunk count that matches its size", func() {
				Expect(blob.ChunkCount(c.Size)).To(Equal(len(c.ChunkHashes)))
			})
		})
	}
})
