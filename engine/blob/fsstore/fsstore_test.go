package fsstore_test

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/fsstore"
)

func fixture(size int, seed byte) ([]byte, blob.Manifest) {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i%251) ^ seed
	}
	m, err := blob.Compute(bytes.NewReader(data))
	Expect(err).NotTo(HaveOccurred())
	return data, m
}

func chunk(data []byte, m blob.Manifest, i int) []byte {
	start := i * blob.ChunkSize
	return data[start : start+blob.ChunkLen(m.Size, i)]
}

// files returns the path of every regular file below dir, relative to dir.
func files(dir string) []string {
	GinkgoHelper()
	var out []string
	Expect(filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, err := filepath.Rel(dir, p)
			Expect(err).NotTo(HaveOccurred())
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})).To(Succeed())
	return out
}

// stagedFile returns the path of the only file named name below dir, or ""
// when there is none.
func stagedFile(dir, name string) string {
	GinkgoHelper()
	var found []string
	for _, f := range files(dir) {
		if filepath.Base(f) == name {
			found = append(found, filepath.Join(dir, f))
		}
	}
	Expect(len(found)).To(BeNumerically("<=", 1), "files named %s", name)
	if len(found) == 0 {
		return ""
	}
	return found[0]
}

// committedFile returns the path of the only committed blob file below dir.
func committedFile(dir string) string {
	GinkgoHelper()
	var found []string
	for _, f := range files(dir) {
		if filepath.Ext(f) == ".blob" {
			found = append(found, filepath.Join(dir, f))
		}
	}
	Expect(found).To(HaveLen(1))
	return found[0]
}

var _ = Describe("fsstore", func() {
	var (
		ctx  context.Context
		dir  string
		now  time.Time
		clk  = func() time.Time { return now }
		data []byte
		m    blob.Manifest
	)

	BeforeEach(func() {
		ctx = context.Background()
		dir = filepath.Join(GinkgoT().TempDir(), "blobs")
		now = time.Date(2026, 5, 6, 7, 8, 9, 10, time.UTC)
		data, m = fixture(blob.ChunkSize+100, 7)
	})

	It("creates the directory and its layout", func() {
		s := open(dir, clk)
		Expect(s.Close()).To(Succeed())
		Expect(files(dir)).To(ConsistOf("layout"))
	})

	It("refuses a directory with another layout", func() {
		Expect(os.MkdirAll(dir, 0o700)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "layout"), []byte("syncplane-fsstore 99\n"), 0o600)).To(Succeed())
		_, err := fsstore.Open(dir)
		Expect(err).To(MatchError(ContainSubstring("layout")))
	})

	It("keeps committed blobs and staged chunks when the directory is opened again", func() {
		s := open(dir, clk)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())
		mark := now.Add(time.Hour)
		Expect(s.SetUnreferencedSince(ctx, "ws", m.Digest, mark)).To(Succeed())

		staged, sm := fixture(blob.ChunkSize+5, 9)
		Expect(s.Begin(ctx, "other", sm)).To(Succeed())
		Expect(s.PutChunk(ctx, "other", sm.Digest, 1, bytes.NewReader(chunk(staged, sm, 1)))).To(Succeed())
		started := now
		Expect(s.Close()).To(Succeed())

		now = now.Add(24 * time.Hour)
		s = open(dir, clk)
		DeferCleanup(s.Close)

		info, err := s.Stat(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.CommittedAt).To(BeTemporally("==", started))
		Expect(info.UnreferencedSince).To(BeTemporally("==", mark))
		r, err := s.Open(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		got, err := io.ReadAll(r)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.Close()).To(Succeed())
		Expect(got).To(Equal(data))

		u, have, err := s.Staged(ctx, "other", sm.Digest)
		Expect(err).NotTo(HaveOccurred())
		Expect(u.StartedAt).To(BeTemporally("==", started))
		Expect(have).To(Equal([]int{1}))
		Expect(s.PutChunk(ctx, "other", sm.Digest, 0, bytes.NewReader(chunk(staged, sm, 0)))).To(Succeed())
		Expect(s.Complete(ctx, "other", sm.Digest)).To(Succeed())

		var wss []string
		for ws, err := range s.ListWorkspaces(ctx) {
			Expect(err).NotTo(HaveOccurred())
			wss = append(wss, ws)
		}
		Expect(wss).To(ConsistOf("ws", "other"))
	})

	It("ignores temporary files a crash left and removes old ones on open", func() {
		s := open(dir, clk)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		other, om := fixture(10, 1)
		Expect(s.Begin(ctx, "ws", om)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", om.Digest, 0, bytes.NewReader(other))).To(Succeed())
		Expect(s.Complete(ctx, "ws", om.Digest)).To(Succeed())
		Expect(s.Close()).To(Succeed())

		// Leave what a process that stopped in each kind of write would
		// leave: a half-written blob, upload, mark and id, an upload
		// directory of a Begin that lost, and a Begin that wrote no upload.
		var stagingDir, uploadDir, blobsDir, wsDir string
		for _, f := range files(dir) {
			switch filepath.Base(f) {
			case "upload.json":
				stagingDir = filepath.Join(dir, filepath.Dir(f))
			case "data":
				uploadDir = filepath.Join(dir, filepath.Dir(f))
			case "id":
				wsDir = filepath.Join(dir, filepath.Dir(f))
			}
		}
		Expect(stagingDir).NotTo(BeEmpty())
		Expect(uploadDir).NotTo(BeEmpty())
		blobsDir = filepath.Join(wsDir, "blobs")
		old := time.Now().Add(-2 * time.Hour)
		var gone []string
		for i, d := range []string{stagingDir, uploadDir, blobsDir, wsDir} {
			p := filepath.Join(d, ".tmp-crash"+string(rune('a'+i)))
			Expect(os.WriteFile(p, []byte("partial"), 0o600)).To(Succeed())
			Expect(os.Chtimes(p, old, old)).To(Succeed())
			gone = append(gone, p)
		}
		lost := filepath.Join(stagingDir, "000000000000000000000000")
		Expect(os.Mkdir(lost, 0o700)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(lost, "data"), nil, 0o600)).To(Succeed())
		Expect(os.Chtimes(lost, old, old)).To(Succeed())
		gone = append(gone, lost)
		noUpload := filepath.Join(wsDir, "staging", strings.Repeat("ab", 32))
		Expect(os.Mkdir(noUpload, 0o700)).To(Succeed())
		Expect(os.Chtimes(noUpload, old, old)).To(Succeed())
		gone = append(gone, noUpload)

		live := filepath.Join(blobsDir, ".tmp-live")
		Expect(os.WriteFile(live, []byte("partial"), 0o600)).To(Succeed())
		racing := filepath.Join(stagingDir, "111111111111111111111111")
		Expect(os.Mkdir(racing, 0o700)).To(Succeed())
		Expect(os.MkdirAll(filepath.Join(dir, "trash", "leftover", "x"), 0o700)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "trash", "leftover", "x", "1.chunk"), []byte("x"), 0o600)).To(Succeed())

		s = open(dir, clk)
		DeferCleanup(s.Close)

		for _, p := range gone {
			_, err := os.Lstat(p)
			Expect(err).To(MatchError(fs.ErrNotExist), "%s stays", p)
		}
		Expect(live).To(BeAnExistingFile(), "a temporary file of a live writer stays")
		Expect(racing).To(BeADirectory(), "an upload directory of a live Begin stays")
		entries, err := os.ReadDir(filepath.Join(dir, "trash"))
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(BeEmpty())

		_, have, err := s.Staged(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		Expect(have).To(Equal([]int{0}))
		var digests []blob.Digest
		for d, err := range s.List(ctx, "ws") {
			Expect(err).NotTo(HaveOccurred())
			digests = append(digests, d)
		}
		Expect(digests).To(ConsistOf(om.Digest))
		var uploads []blob.Digest
		for u, err := range s.ListStaged(ctx, "ws") {
			Expect(err).NotTo(HaveOccurred())
			uploads = append(uploads, u.Manifest.Digest)
		}
		Expect(uploads).To(ConsistOf(m.Digest))
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())
		r, err := s.Open(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(r.Close)
		Expect(io.ReadAll(r)).To(Equal(data))
	})

	It("refuses a directory of the earlier layout", func() {
		Expect(os.MkdirAll(dir, 0o700)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "layout"), []byte("syncplane-fsstore 1\n"), 0o600)).To(Succeed())
		_, err := fsstore.Open(dir)
		Expect(err).To(MatchError(ContainSubstring("layout")))
	})

	It("counts a chunk without its marker as missing, whatever the data holds", func() {
		s := open(dir, clk)
		DeferCleanup(s.Close)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())

		Expect(os.Remove(stagedFile(dir, "1.chunk"))).To(Succeed())

		_, have, err := s.Staged(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		Expect(have).To(Equal([]int{0}))
		Expect(s.Complete(ctx, "ws", m.Digest)).To(MatchError(blob.ErrIncomplete))
		_, err = s.Stat(ctx, "ws", m.Digest)
		Expect(err).To(MatchError(blob.ErrNotFound))

		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())
	})

	It("writes each chunk into one staged file at its offset", func() {
		s := open(dir, clk)
		DeferCleanup(s.Close)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())

		staged, err := os.ReadFile(stagedFile(dir, "data"))
		Expect(err).NotTo(HaveOccurred())
		Expect(staged).To(HaveLen(len(data)))
		Expect(staged[blob.ChunkSize:]).To(Equal(chunk(data, m, 1)))
		Expect(stagedFile(dir, "1.chunk")).To(BeAnExistingFile())
		Expect(stagedFile(dir, "0.chunk")).To(BeEmpty())
	})

	It("completes without reading the staged bytes", func() {
		s := open(dir, clk)
		DeferCleanup(s.Close)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())

		if os.Geteuid() == 0 {
			Skip("root reads files whatever their mode")
		}
		// With the data file write-only, a Complete that read it would
		// fail, whatever the blob's size.
		Expect(os.Chmod(stagedFile(dir, "data"), 0o200)).To(Succeed())
		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())

		committed := committedFile(dir)
		Expect(os.Chmod(committed, 0o600)).To(Succeed())
		info, err := os.Stat(committed)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size()).To(BeNumerically("<", int64(len(data))+4096), "Complete appends only a trailer")
		r, err := s.Open(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(r.Close)
		Expect(io.ReadAll(r)).To(Equal(data))
	})

	It("replaces the tail a Complete that a crash cut short left", func() {
		s := open(dir, clk)
		DeferCleanup(s.Close)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		f, err := os.OpenFile(stagedFile(dir, "data"), os.O_WRONLY|os.O_APPEND, 0)
		Expect(err).NotTo(HaveOccurred())
		_, err = f.Write(bytes.Repeat([]byte("torn"), 10000))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Close()).To(Succeed())

		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())

		info, err := s.Stat(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Manifest.Size).To(Equal(m.Size))
		r, err := s.Open(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(r.Close)
		Expect(io.ReadAll(r)).To(Equal(data))
	})

	It("removes the staged state a crash left beside a committed blob", func() {
		s := open(dir, clk)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		stagingDir := filepath.Dir(filepath.Dir(stagedFile(dir, "data")))
		saved := filepath.Join(GinkgoT().TempDir(), "saved")
		Expect(os.CopyFS(saved, os.DirFS(stagingDir))).To(Succeed())
		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())
		Expect(s.Close()).To(Succeed())

		// The state just after the commit's rename: the data file has
		// moved, the rest of the staged state is still there.
		Expect(os.CopyFS(stagingDir, os.DirFS(saved))).To(Succeed())
		Expect(os.Remove(stagedFile(dir, "data"))).To(Succeed())

		s = open(dir, clk)
		DeferCleanup(s.Close)
		_, _, err := s.Staged(ctx, "ws", m.Digest)
		Expect(err).To(MatchError(blob.ErrNotFound))
		r, err := s.Open(ctx, "ws", m.Digest)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(r.Close)
		Expect(io.ReadAll(r)).To(Equal(data))
	})

	It("drops a staged upload whose data a crash and a Delete left without bytes", func() {
		s := open(dir, clk)
		DeferCleanup(s.Close)
		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		Expect(os.Remove(stagedFile(dir, "data"))).To(Succeed())

		Expect(s.Complete(ctx, "ws", m.Digest)).To(MatchError(blob.ErrNotFound))
		_, _, err := s.Staged(ctx, "ws", m.Digest)
		Expect(err).To(MatchError(blob.ErrNotFound), "the uploader must stage the blob again")

		Expect(s.Begin(ctx, "ws", m)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 0, bytes.NewReader(chunk(data, m, 0)))).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", m.Digest, 1, bytes.NewReader(chunk(data, m, 1)))).To(Succeed())
		Expect(s.Complete(ctx, "ws", m.Digest)).To(Succeed())
	})

	It("reports a damaged blob file as an error, not as a short blob", func() {
		s := open(dir, clk)
		DeferCleanup(s.Close)
		other, om := fixture(1000, 3)
		Expect(s.Begin(ctx, "ws", om)).To(Succeed())
		Expect(s.PutChunk(ctx, "ws", om.Digest, 0, bytes.NewReader(other))).To(Succeed())
		Expect(s.Complete(ctx, "ws", om.Digest)).To(Succeed())

		var blobFile string
		for _, f := range files(dir) {
			if filepath.Ext(f) == ".blob" {
				blobFile = filepath.Join(dir, f)
			}
		}
		info, err := os.Stat(blobFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.Truncate(blobFile, info.Size()-1)).To(Succeed())

		_, err = s.Stat(ctx, "ws", om.Digest)
		Expect(err).To(HaveOccurred())
		Expect(err).NotTo(MatchError(blob.ErrNotFound))
		_, err = s.Open(ctx, "ws", om.Digest)
		Expect(err).To(HaveOccurred())
		Expect(err).NotTo(MatchError(blob.ErrNotFound))
	})

	It("keeps every workspace id inside the directory under a fixed-form name", func() {
		parent := filepath.Dir(dir)
		s := open(dir, clk)
		DeferCleanup(s.Close)
		other, om := fixture(10, 5)
		ids := []string{"..", "../..", "../outside", "/abs", "a/../../b", "Case", "case", "ü", "ü", ""}
		for _, ws := range ids {
			Expect(s.Begin(ctx, ws, om)).To(Succeed())
			Expect(s.PutChunk(ctx, ws, om.Digest, 0, bytes.NewReader(other))).To(Succeed())
			Expect(s.Complete(ctx, ws, om.Digest)).To(Succeed())
		}

		entries, err := os.ReadDir(parent)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1), "nothing is written beside the directory")
		names, err := os.ReadDir(filepath.Join(dir, "workspaces"))
		Expect(err).NotTo(HaveOccurred())
		Expect(names).To(HaveLen(len(ids)))
		for _, e := range names {
			Expect(e.Name()).To(MatchRegexp(`^[0-9a-f]{64}$`))
		}
		blobName := regexp.MustCompile(`^workspaces/[0-9a-f]{64}/(id|blobs/[0-9a-f]{64}\.blob)$`)
		for _, f := range files(dir) {
			if f == "layout" {
				continue
			}
			Expect(blobName.MatchString(f)).To(BeTrue(), "unexpected file %q", f)
		}
	})
})
