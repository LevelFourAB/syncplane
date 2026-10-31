// Package fsstore is a blob.Store that keeps blobs in a local directory.
//
// Several Stores, in one process or in several, can share one directory. The
// directory must be on a local file system that renames atomically, supports
// hard links and keeps an open file readable after it is removed, such as
// ext4, XFS, APFS or ZFS. Network file systems such as NFS and SMB do not give
// these guarantees, and a directory on one must not be shared. Every process
// must run as the same user: the Store creates files that only their owner
// can read.
//
// A staged upload is one data file that each chunk is written into at its
// offset, after its hash is checked. Complete does not read or copy the data:
// it appends a trailer that holds the manifest, syncs the file and renames it
// into place. Its cost does not depend on the blob's size, and a reader sees
// no blob or the whole blob, also after a crash. A staged upload survives a
// restart with the chunks it had. A crash can lose the chunks stored shortly
// before it; Staged then reports them missing.
//
// The directory holds:
//
//	layout                                         the layout version
//	workspaces/<ws>/id                             the workspace id
//	workspaces/<ws>/blobs/<hex>.blob               a committed blob: bytes, then trailer
//	workspaces/<ws>/blobs/<hex>.unreferenced       its UnreferencedSince mark
//	workspaces/<ws>/staging/<hex>/upload.json      a staged upload and its upload id
//	workspaces/<ws>/staging/<hex>/<id>/data        the bytes of the chunks stored so far
//	workspaces/<ws>/staging/<hex>/<id>/<i>.chunk   empty marker: chunk i is in data
//	workspaces/<ws>/staging/<hex>/<id>/commit.json the trailer header Complete chose
//	trash/                                         removed state that is not yet deleted
//
// <ws> is the hex SHA-256 of the workspace id and <hex> the hex digits of the
// digest, so names do not depend on how the file system compares case or
// Unicode. <id> is random per Begin. A trailer is the JSON header, its length
// as a big-endian uint32, then the 8 bytes "SPBLOB2\n". Temporary files start
// with ".tmp-".
package fsstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/levelfourab/syncplane/engine/blob"
)

const (
	layoutFile    = "layout"
	layoutVersion = "syncplane-fsstore 2\n"
	workspacesDir = "workspaces"
	trashDir      = "trash"
	idFile        = "id"
	blobsDir      = "blobs"
	stagingDir    = "staging"
	uploadFile    = "upload.json"
	dataFile      = "data"
	commitFile    = "commit.json"
	blobExt       = ".blob"
	unrefExt      = ".unreferenced"
	chunkExt      = ".chunk"
	tempPrefix    = ".tmp-"

	// trailerMagic ends every committed blob file.
	trailerMagic = "SPBLOB2\n"
	// trailerFixed is the size of the trailer without its header.
	trailerFixed = 4 + len(trailerMagic)

	// tempMaxAge is how old a leftover must be before Open removes it. A
	// live writer touches its files far more often than this, so only what a
	// crashed process left behind is this old.
	tempMaxAge = time.Hour
)

// Store is a blob.Store over a local directory. It is safe for concurrent use
// by several goroutines, and by several Stores that share the directory.
type Store struct {
	dir string
	now func() time.Time
}

// Option configures Open.
type Option func(*Store)

// WithClock makes the Store read the time from now instead of time.Now. It
// sets CommittedAt and StartedAt. A nil now keeps time.Now.
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// Open returns a Store over dir, and creates dir when it does not exist.
//
// Open removes what an earlier process left behind when it stopped during a
// change: the contents of trash/, temporary files and abandoned staging state
// older than an hour, and the staged state of blobs that are committed. To
// find them it reads every directory in dir, so its cost grows with the
// number of blobs. It fails when dir holds a layout that this version does
// not know.
func Open(dir string, opts ...Option) (*Store, error) {
	s := &Store{dir: dir, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	for _, d := range []string{dir, filepath.Join(dir, workspacesDir), filepath.Join(dir, trashDir)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("fsstore: %w", err)
		}
	}
	if err := s.checkLayout(); err != nil {
		return nil, err
	}
	s.clean()
	return s, nil
}

var _ blob.Store = (*Store)(nil)

func (s *Store) checkLayout() error {
	p := filepath.Join(s.dir, layoutFile)
	got, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		if err := writeAtomic(s.dir, layoutFile, []byte(layoutVersion)); err != nil {
			return fmt.Errorf("fsstore: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("fsstore: %w", err)
	}
	if string(got) != layoutVersion {
		return fmt.Errorf("fsstore: %s holds layout %q, not %q", s.dir, got, layoutVersion)
	}
	return nil
}

// clean removes leftovers of interrupted changes. It ignores errors: another
// process may clean the same files at the same time, and a leftover that
// stays is removed by the next Open.
func (s *Store) clean() {
	trash := filepath.Join(s.dir, trashDir)
	if entries, err := os.ReadDir(trash); err == nil {
		for _, e := range entries {
			_ = os.RemoveAll(filepath.Join(trash, e.Name()))
		}
	}
	// Modification times come from the wall clock, not from s.now.
	cutoff := time.Now().Add(-tempMaxAge)
	old := func(p string) bool {
		info, err := os.Lstat(p)
		return err == nil && info.ModTime().Before(cutoff)
	}
	root := filepath.Join(s.dir, workspacesDir)
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), tempPrefix) && old(p) {
			_ = os.Remove(p)
		}
		return nil
	})
	wss, _ := os.ReadDir(root)
	for _, w := range wss {
		wsDir := filepath.Join(root, w.Name())
		staged, _ := os.ReadDir(filepath.Join(wsDir, stagingDir))
		for _, e := range staged {
			if !validHex(e.Name()) {
				continue
			}
			dir := filepath.Join(wsDir, stagingDir, e.Name())
			// A crash between the commit's rename and the removal of the
			// staged state leaves both.
			if _, err := os.Stat(filepath.Join(wsDir, blobsDir, e.Name()+blobExt)); err == nil {
				_ = s.removeDir(dir)
				continue
			}
			_, id, err := readUpload(dir, e.Name())
			if errors.Is(err, blob.ErrNotFound) {
				// A Begin that crashed before it wrote upload.json.
				if old(dir) {
					_ = s.removeDir(dir)
				}
				continue
			}
			if err != nil {
				continue
			}
			// Upload directories of Begins that crashed or lost a race.
			subs, _ := os.ReadDir(dir)
			for _, sub := range subs {
				p := filepath.Join(dir, sub.Name())
				if sub.IsDir() && sub.Name() != id && old(p) {
					_ = s.removeDir(p)
				}
			}
		}
	}
}

// Begin stages an upload of the blob m describes. It fails with an error
// wrapping blob.ErrInvalid when m's digest is malformed.
func (s *Store) Begin(ctx context.Context, ws string, m blob.Manifest) error {
	h, err := digestHex(m.Digest)
	if err != nil {
		return err
	}
	// A DeleteWorkspace, Complete or Abort that runs at the same time can
	// move a directory away between two steps. The steps then fail with
	// fs.ErrNotExist, and a new attempt starts over on fresh directories.
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.begin(ws, h, m)
		if err == nil || !errors.Is(err, fs.ErrNotExist) || attempt == 4 {
			return err
		}
	}
}

func (s *Store) begin(ws, h string, m blob.Manifest) error {
	if s.committed(ws, h) {
		return nil
	}
	dir, err := s.mkdirs(ws, stagingDir, h)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, uploadFile)); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	id := randomName()
	upDir := filepath.Join(dir, id)
	if err := os.Mkdir(upDir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(upDir, dataFile), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	rec, err := json.Marshal(uploadRecord{manifestRecord: toRecord(m), StartedAt: s.now(), ID: id})
	if err != nil {
		return err
	}
	won, err := createAtomic(dir, uploadFile, rec)
	if err != nil {
		return err
	}
	if !won {
		// A Begin that ran at the same time staged the upload first.
		return s.removeDir(upDir)
	}
	// A Complete that ran at the same time can have committed the blob after
	// the check above. Its staged state is gone, so this one is the only
	// staged state left, and the blob must not be both staged and committed.
	if s.committed(ws, h) {
		return s.removeDir(dir)
	}
	return nil
}

// Staged returns a staged upload and the indexes of the chunks it holds.
func (s *Store) Staged(_ context.Context, ws string, digest blob.Digest) (blob.Upload, []int, error) {
	h, err := digestHex(digest)
	if err != nil {
		return blob.Upload{}, nil, blob.ErrNotFound
	}
	dir := s.stagingPath(ws, h)
	u, id, err := readUpload(dir, h)
	if err != nil {
		return blob.Upload{}, nil, err
	}
	have, err := chunksIn(filepath.Join(dir, id), len(u.Manifest.ChunkHashes))
	if err != nil {
		return blob.Upload{}, nil, err
	}
	return u, have, nil
}

// chunkBuffers holds ChunkSize buffers for PutChunk, which must hold a whole
// chunk to check its hash before any byte reaches the data file.
var chunkBuffers = sync.Pool{New: func() any {
	b := make([]byte, blob.ChunkSize)
	return &b
}}

// PutChunk stores chunk index of a staged upload. It holds the chunk in
// memory, so each call that runs at the same time uses up to blob.ChunkSize
// bytes.
func (s *Store) PutChunk(ctx context.Context, ws string, digest blob.Digest, index int, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := digestHex(digest)
	if err != nil {
		return blob.ErrNotFound
	}
	dir := s.stagingPath(ws, h)
	u, id, err := readUpload(dir, h)
	if err != nil {
		return err
	}
	m := u.Manifest
	if index < 0 || index >= len(m.ChunkHashes) {
		return fmt.Errorf("%w: chunk %d is outside a blob of %d chunks", blob.ErrInvalid, index, len(m.ChunkHashes))
	}
	want := blob.ChunkLen(m.Size, index)
	bp := chunkBuffers.Get().(*[]byte)
	defer chunkBuffers.Put(bp)
	p := (*bp)[:want]
	if n, err := io.ReadFull(r, p); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: chunk %d is %d bytes, not %d", blob.ErrInvalid, index, n, want)
		}
		return err
	}
	if err := m.CheckChunk(index, p); err != nil {
		return err
	}

	// Every path below names the upload by its id. When an Abort and a new
	// Begin replace the upload while this call runs, the paths no longer
	// exist, so the call cannot write into or mark the new upload.
	upDir := filepath.Join(dir, id)
	marker := filepath.Join(upDir, chunkName(index))
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	f, err := os.OpenFile(filepath.Join(upDir, dataFile), os.O_WRONLY, 0)
	if err != nil {
		return notFoundIfMissing(err)
	}
	// Two calls that store one chunk write the same checked bytes at the
	// same offset, so their writes can interleave in any order.
	_, err = f.WriteAt(p, int64(index)*blob.ChunkSize)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	// The marker is created only after the bytes are on disk, so a marker
	// that survives a crash always has its bytes. The directory is not
	// synced: a marker that a crash loses makes the chunk missing, and the
	// uploader sends it again.
	mf, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return notFoundIfMissing(err)
	}
	return mf.Close()
}

// Complete commits a staged upload that holds every chunk. It does not read
// the staged bytes: they were checked when PutChunk stored them.
func (s *Store) Complete(ctx context.Context, ws string, digest blob.Digest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := digestHex(digest)
	if err != nil {
		return blob.ErrNotFound
	}
	if s.committed(ws, h) {
		return nil
	}
	dir := s.stagingPath(ws, h)
	u, id, err := readUpload(dir, h)
	if err != nil {
		return s.uploadGone(ws, h, err)
	}
	m := u.Manifest
	upDir := filepath.Join(dir, id)
	have, err := chunksIn(upDir, len(m.ChunkHashes))
	if err != nil {
		return err
	}
	if len(have) < len(m.ChunkHashes) {
		return s.missingChunk(ws, h, dir)
	}

	// Every Complete of this upload writes the same trailer, the one the
	// first of them recorded in commit.json. Two Completes that run at the
	// same time then write the same bytes at the same offset of one file,
	// and either rename commits a whole blob.
	hdr, err := s.trailerHeader(upDir, m)
	if err != nil {
		return s.uploadGone(ws, h, err)
	}
	staged := filepath.Join(upDir, dataFile)
	f, err := os.OpenFile(staged, os.O_WRONLY, 0)
	if errors.Is(err, fs.ErrNotExist) && !s.committed(ws, h) {
		// The data is gone but the upload is still staged: a crash after a
		// commit's rename, then a Delete of the blob. Remove the upload, so
		// that the uploader stages and sends it again instead of finding
		// every chunk present.
		if _, cur, rerr := readUpload(dir, h); rerr == nil && cur == id {
			if err := s.removeDir(dir); err != nil {
				return err
			}
		}
		return blob.ErrNotFound
	}
	if err != nil {
		return s.uploadGone(ws, h, err)
	}
	trailer := make([]byte, 0, len(hdr)+trailerFixed)
	trailer = append(trailer, hdr...)
	trailer = binary.BigEndian.AppendUint32(trailer, uint32(len(hdr)))
	trailer = append(trailer, trailerMagic...)
	_, err = f.WriteAt(trailer, m.Size)
	if err == nil {
		// A Complete that a crash cut short can have left a longer tail.
		err = f.Truncate(m.Size + int64(len(trailer)))
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}

	blobs, err := s.mkdirs(ws, blobsDir)
	if err != nil {
		return s.uploadGone(ws, h, err)
	}
	// A mark that a Delete left behind belongs to the deleted blob, not to
	// this one. No mark can be set between this removal and the rename: a
	// blob that is not committed refuses SetUnreferencedSince.
	if err := removeIfExists(filepath.Join(blobs, h+unrefExt)); err != nil {
		return err
	}
	if err := os.Rename(staged, filepath.Join(blobs, h+blobExt)); err != nil {
		return s.uploadGone(ws, h, err)
	}
	if err := syncDir(blobs); err != nil {
		return err
	}
	// Remove the staged state only while it is still this upload's: a
	// Delete and a new Begin can have replaced it since the rename.
	if _, cur, err := readUpload(dir, h); err == nil && cur == id {
		return s.removeDir(dir)
	}
	return nil
}

// trailerHeader returns the trailer header recorded in upDir, and records
// one when there is none.
func (s *Store) trailerHeader(upDir string, m blob.Manifest) ([]byte, error) {
	if b, err := os.ReadFile(filepath.Join(upDir, commitFile)); err == nil {
		return b, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	hdr, err := json.Marshal(blobRecord{manifestRecord: toRecord(m), CommittedAt: s.now()})
	if err != nil {
		return nil, err
	}
	won, err := createAtomic(upDir, commitFile, hdr)
	if err != nil {
		return nil, err
	}
	if won {
		return hdr, nil
	}
	return os.ReadFile(filepath.Join(upDir, commitFile))
}

// missingChunk returns the error for a staged upload in dir that lacks a
// chunk, which depends on whether the upload is still staged.
func (s *Store) missingChunk(ws, h, dir string) error {
	if _, err := os.Stat(filepath.Join(dir, uploadFile)); err != nil {
		return s.uploadGone(ws, h, err)
	}
	return blob.ErrIncomplete
}

// uploadGone returns the error for a Complete whose staged upload is gone:
// nil when a Complete that ran at the same time committed the blob.
func (s *Store) uploadGone(ws, h string, err error) error {
	if !errors.Is(err, blob.ErrNotFound) && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if s.committed(ws, h) {
		return nil
	}
	return blob.ErrNotFound
}

func (s *Store) Abort(_ context.Context, ws string, digest blob.Digest) error {
	h, err := digestHex(digest)
	if err != nil {
		return nil
	}
	return s.removeDir(s.stagingPath(ws, h))
}

func (s *Store) ListStaged(_ context.Context, ws string) iter.Seq2[blob.Upload, error] {
	return func(yield func(blob.Upload, error) bool) {
		root := filepath.Join(s.wsPath(ws), stagingDir)
		entries, err := readDir(root)
		if err != nil {
			yield(blob.Upload{}, err)
			return
		}
		for _, e := range entries {
			if !validHex(e.Name()) {
				continue
			}
			u, _, err := readUpload(filepath.Join(root, e.Name()), e.Name())
			if errors.Is(err, blob.ErrNotFound) {
				continue
			}
			if !yield(u, err) {
				return
			}
		}
	}
}

// Stat returns a committed blob's Info. A blob file that is damaged on disk
// gives an error that does not wrap blob.ErrNotFound.
func (s *Store) Stat(_ context.Context, ws string, digest blob.Digest) (blob.Info, error) {
	h, err := digestHex(digest)
	if err != nil {
		return blob.Info{}, blob.ErrNotFound
	}
	f, rec, err := s.openBlob(ws, h)
	if err != nil {
		return blob.Info{}, err
	}
	_ = f.Close()
	info := blob.Info{Manifest: rec.manifest(), CommittedAt: rec.CommittedAt}
	mark, err := os.ReadFile(filepath.Join(s.wsPath(ws), blobsDir, h+unrefExt))
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return blob.Info{}, err
	default:
		if err := info.UnreferencedSince.UnmarshalText(mark); err != nil {
			return blob.Info{}, fmt.Errorf("fsstore: mark of %s: %w", digest, err)
		}
	}
	return info, nil
}

// Open returns a reader over a committed blob's bytes. The reader stays
// readable after a Delete of the blob, until it is closed. A blob file that is
// damaged on disk gives an error that does not wrap blob.ErrNotFound.
func (s *Store) Open(_ context.Context, ws string, digest blob.Digest) (io.ReadSeekCloser, error) {
	h, err := digestHex(digest)
	if err != nil {
		return nil, blob.ErrNotFound
	}
	f, rec, err := s.openBlob(ws, h)
	if err != nil {
		return nil, err
	}
	return &reader{SectionReader: io.NewSectionReader(f, 0, rec.Size), f: f}, nil
}

func (s *Store) SetUnreferencedSince(_ context.Context, ws string, digest blob.Digest, t time.Time) error {
	h, err := digestHex(digest)
	if err != nil {
		return blob.ErrNotFound
	}
	if !s.committed(ws, h) {
		return blob.ErrNotFound
	}
	blobs := filepath.Join(s.wsPath(ws), blobsDir)
	if t.IsZero() {
		return removeIfExists(filepath.Join(blobs, h+unrefExt))
	}
	text, err := t.MarshalText()
	if err != nil {
		return err
	}
	return notFoundIfMissing(writeAtomic(blobs, h+unrefExt, text))
}

func (s *Store) Delete(_ context.Context, ws string, digest blob.Digest) error {
	h, err := digestHex(digest)
	if err != nil {
		return nil
	}
	blobs := filepath.Join(s.wsPath(ws), blobsDir)
	// The blob file goes first: once it is gone the blob is not committed,
	// whatever happens to the mark.
	if err := removeIfExists(filepath.Join(blobs, h+blobExt)); err != nil {
		return err
	}
	return removeIfExists(filepath.Join(blobs, h+unrefExt))
}

func (s *Store) List(_ context.Context, ws string) iter.Seq2[blob.Digest, error] {
	return func(yield func(blob.Digest, error) bool) {
		entries, err := readDir(filepath.Join(s.wsPath(ws), blobsDir))
		if err != nil {
			yield("", err)
			return
		}
		for _, e := range entries {
			h, ok := strings.CutSuffix(e.Name(), blobExt)
			if !ok || !validHex(h) {
				continue
			}
			if !yield(blob.Digest("sp1:"+h), nil) {
				return
			}
		}
	}
}

// ListWorkspaces yields the id of every workspace that has a committed blob
// or a staged upload. It reads the directories of every workspace.
func (s *Store) ListWorkspaces(context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		root := filepath.Join(s.dir, workspacesDir)
		entries, err := readDir(root)
		if err != nil {
			yield("", err)
			return
		}
		for _, e := range entries {
			if !validHex(e.Name()) {
				continue
			}
			ws, ok, err := s.listedWorkspace(filepath.Join(root, e.Name()), e.Name())
			if err != nil {
				if !yield("", err) {
					return
				}
				continue
			}
			if ok && !yield(ws, nil) {
				return
			}
		}
	}
}

// listedWorkspace returns the id of the workspace in dir, and whether it
// holds a committed blob or a staged upload.
func (s *Store) listedWorkspace(dir, name string) (string, bool, error) {
	id, err := os.ReadFile(filepath.Join(dir, idFile))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if wsName(string(id)) != name {
		return "", false, nil
	}
	blobs, err := readDir(filepath.Join(dir, blobsDir))
	if err != nil {
		return "", false, err
	}
	for _, e := range blobs {
		if h, ok := strings.CutSuffix(e.Name(), blobExt); ok && validHex(h) {
			return string(id), true, nil
		}
	}
	staged, err := readDir(filepath.Join(dir, stagingDir))
	if err != nil {
		return "", false, err
	}
	for _, e := range staged {
		if !validHex(e.Name()) {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, stagingDir, e.Name(), uploadFile)); err == nil {
			return string(id), true, nil
		}
	}
	return "", false, nil
}

func (s *Store) DeleteWorkspace(_ context.Context, ws string) error {
	return s.removeDir(s.wsPath(ws))
}

// Close does nothing. The Store stays usable.
func (s *Store) Close() error { return nil }

func (s *Store) wsPath(ws string) string {
	return filepath.Join(s.dir, workspacesDir, wsName(ws))
}

func (s *Store) stagingPath(ws, h string) string {
	return filepath.Join(s.wsPath(ws), stagingDir, h)
}

// wsName maps a workspace id onto a directory name. Ids are arbitrary
// strings, and a hash gives every id a name of fixed length that is the same
// on every file system, whatever it does with case, Unicode normalization or
// special names.
func wsName(ws string) string {
	sum := sha256.Sum256([]byte(ws))
	return hex.EncodeToString(sum[:])
}

// mkdirs creates the workspace directory and its id file, then the
// directories sub below it, and returns the deepest one. Only the workspace
// directory is created together with its id, so a directory with content
// always has an id: when a DeleteWorkspace moves the workspace directory away
// in between, creating sub fails with fs.ErrNotExist instead of making a
// workspace directory without an id.
func (s *Store) mkdirs(ws string, sub ...string) (string, error) {
	dir := s.wsPath(ws)
	if _, err := os.Stat(filepath.Join(dir, idFile)); errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
		if err := writeAtomic(dir, idFile, []byte(ws)); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	for _, name := range sub {
		dir = filepath.Join(dir, name)
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return "", err
		}
	}
	return dir, nil
}

// committed reports whether the blob file of digest hex h exists.
func (s *Store) committed(ws, h string) bool {
	_, err := os.Stat(filepath.Join(s.wsPath(ws), blobsDir, h+blobExt))
	return err == nil
}

// openBlob opens the blob file of digest hex h and reads its trailer.
func (s *Store) openBlob(ws, h string) (*os.File, blobRecord, error) {
	f, err := os.Open(filepath.Join(s.wsPath(ws), blobsDir, h+blobExt))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, blobRecord{}, blob.ErrNotFound
	}
	if err != nil {
		return nil, blobRecord{}, err
	}
	rec, err := readTrailer(f, h)
	if err != nil {
		_ = f.Close()
		return nil, blobRecord{}, err
	}
	return f, rec, nil
}

// readTrailer reads the trailer at the end of a committed blob file and
// checks that the bytes before it are as long as the blob.
func readTrailer(f *os.File, h string) (blobRecord, error) {
	info, err := f.Stat()
	if err != nil {
		return blobRecord{}, err
	}
	end := info.Size()
	if end < int64(trailerFixed) {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: file is %d bytes, too short for a trailer", h, end)
	}
	var fixed [trailerFixed]byte
	if _, err := f.ReadAt(fixed[:], end-int64(trailerFixed)); err != nil {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: trailer: %w", h, err)
	}
	if string(fixed[4:]) != trailerMagic {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: no trailer", h)
	}
	n := int64(binary.BigEndian.Uint32(fixed[:4]))
	start := end - int64(trailerFixed) - n
	if start < 0 {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: trailer header of %d bytes does not fit", h, n)
	}
	hdr := make([]byte, n)
	if _, err := f.ReadAt(hdr, start); err != nil {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: trailer: %w", h, err)
	}
	var rec blobRecord
	if err := json.Unmarshal(hdr, &rec); err != nil {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: trailer: %w", h, err)
	}
	if err := rec.check(h); err != nil {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: %w", h, err)
	}
	if rec.Size != start {
		return blobRecord{}, fmt.Errorf("fsstore: blob %s: %d bytes before the trailer, not %d", h, start, rec.Size)
	}
	return rec, nil
}

// readUpload reads the staged upload in dir, of digest hex h, and returns it
// with its upload id. It returns blob.ErrNotFound when no upload is staged
// there.
func readUpload(dir, h string) (blob.Upload, string, error) {
	b, err := os.ReadFile(filepath.Join(dir, uploadFile))
	if errors.Is(err, fs.ErrNotExist) {
		return blob.Upload{}, "", blob.ErrNotFound
	}
	if err != nil {
		return blob.Upload{}, "", err
	}
	var rec uploadRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return blob.Upload{}, "", fmt.Errorf("fsstore: upload %s: %w", h, err)
	}
	if err := rec.check(h); err != nil {
		return blob.Upload{}, "", fmt.Errorf("fsstore: upload %s: %w", h, err)
	}
	if !validID(rec.ID) {
		return blob.Upload{}, "", fmt.Errorf("fsstore: upload %s: upload id %q", h, rec.ID)
	}
	return blob.Upload{Manifest: rec.manifest(), StartedAt: rec.StartedAt}, rec.ID, nil
}

// chunksIn returns the indexes below n of the chunk markers in dir, in
// ascending order.
func chunksIn(dir string, n int) ([]int, error) {
	entries, err := readDir(dir)
	if err != nil {
		return nil, err
	}
	have := []int{}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), chunkExt)
		if !ok {
			continue
		}
		i, err := strconv.Atoi(name)
		if err != nil || i < 0 || i >= n || chunkName(i) != e.Name() {
			continue
		}
		have = append(have, i)
	}
	slices.Sort(have)
	return have, nil
}

func chunkName(i int) string { return strconv.Itoa(i) + chunkExt }

// randomName returns 24 random lowercase hex digits.
func randomName() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func validID(id string) bool {
	if len(id) != 24 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}

// removeDir moves dir into trash/ and deletes it there. The move is atomic,
// so no other call sees a half-deleted directory. A dir that does not exist
// is not an error.
func (s *Store) removeDir(dir string) error {
	dst := filepath.Join(s.dir, trashDir, randomName())
	if err := os.Rename(dir, dst); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	// What stays behind after an error here is removed by the next Open.
	_ = os.RemoveAll(dst)
	return nil
}

// writeTemp writes data to a new synced temporary file in dir and returns its
// path.
func writeTemp(dir string, data []byte) (string, error) {
	f, err := os.CreateTemp(dir, tempPrefix+"*")
	if err != nil {
		return "", err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// writeAtomic writes data to the file name in dir: it writes a temporary
// file, syncs it, renames it over name and syncs dir.
func writeAtomic(dir, name string, data []byte) error {
	tmp, err := writeTemp(dir, data)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

// createAtomic writes data to the file name in dir unless the file exists,
// and reports whether it wrote it. A reader of name sees no file or the whole
// file: the file is written in full under a temporary name, then hard-linked
// to name, which fails when name exists.
func createAtomic(dir, name string, data []byte) (bool, error) {
	tmp, err := writeTemp(dir, data)
	if err != nil {
		return false, err
	}
	defer func() { _ = os.Remove(tmp) }()
	if err := os.Link(tmp, filepath.Join(dir, name)); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// readDir is os.ReadDir that reads a missing directory as empty.
func readDir(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return entries, err
}

func removeIfExists(p string) error {
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func notFoundIfMissing(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return blob.ErrNotFound
	}
	return err
}

// digestHex returns the hex digits of d, or an error wrapping
// blob.ErrInvalid when d is malformed. The digits are safe as a file name.
func digestHex(d blob.Digest) (string, error) {
	if _, err := blob.ParseDigest(string(d)); err != nil {
		return "", err
	}
	return string(d)[len("sp1:"):], nil
}

func validHex(name string) bool {
	_, err := blob.ParseDigest("sp1:" + name)
	return err == nil
}

// manifestRecord is a blob.Manifest as the files store it.
type manifestRecord struct {
	Digest      blob.Digest `json:"digest"`
	Size        int64       `json:"size"`
	ChunkHashes [][]byte    `json:"chunkHashes"`
}

func toRecord(m blob.Manifest) manifestRecord {
	return manifestRecord{Digest: m.Digest, Size: m.Size, ChunkHashes: m.ChunkHashes}
}

func (r manifestRecord) manifest() blob.Manifest {
	return blob.Manifest{Digest: r.Digest, Size: r.Size, ChunkHashes: r.ChunkHashes}
}

// check fails unless the record is a valid manifest of digest hex h.
func (r manifestRecord) check(h string) error {
	if r.Digest != blob.Digest("sp1:"+h) {
		return fmt.Errorf("record is of %s", r.Digest)
	}
	return r.manifest().Validate()
}

// uploadRecord is the content of upload.json.
type uploadRecord struct {
	manifestRecord
	StartedAt time.Time `json:"startedAt"`
	// ID names the directory that holds the upload's data and markers.
	ID string `json:"id"`
}

// blobRecord is the header of a committed blob's trailer.
type blobRecord struct {
	manifestRecord
	CommittedAt time.Time `json:"committedAt"`
}

// reader reads a committed blob's bytes from its open file.
type reader struct {
	*io.SectionReader
	f *os.File
}

func (r *reader) Close() error { return r.f.Close() }
