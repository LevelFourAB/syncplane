// Package blob defines how the engine names and stores blobs: the immutable
// bytes, such as images, PDFs and videos, that rows refer to with a
// syncplane.v1.BlobRef.
//
// A blob is named by its Digest and belongs to one workspace. The bytes move
// in chunks of ChunkSize, and the digest is computed from the size and the
// chunk hashes, so a client can hash, upload and resume one chunk at a time.
//
// Store is the seam a deployment implements to keep the bytes. The fsstore
// package keeps them in a local directory. The blobtest package holds the
// conformance suite every Store must pass.
package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"
)

// ChunkSize is the size in bytes of every chunk of a blob except the last.
// It is part of the digest, so it cannot change without a new digest prefix.
const ChunkSize = 8 << 20

// HashSize is the size in bytes of one chunk hash.
const HashSize = sha256.Size

// digestPrefix marks the digest algorithm: SHA-256 over the size and the
// SHA-256 of each ChunkSize chunk.
const digestPrefix = "sp1:"

// ErrNotFound is returned when a Store has no committed blob, or no staged
// upload, under the requested name.
var ErrNotFound = errors.New("blob: not found")

// ErrIncomplete is returned by Store.Complete when a staged upload does not
// have every chunk yet.
var ErrIncomplete = errors.New("blob: upload incomplete")

// ErrInvalid is wrapped by every error that reports a malformed digest, a
// manifest that does not match its digest, or a chunk that does not match its
// hash.
var ErrInvalid = errors.New("blob: invalid")

// Digest names a blob by its content, in the form "sp1:" followed by 64
// lowercase hex digits. The zero Digest is not valid.
type Digest string

// ParseDigest returns s as a Digest. It fails with an error wrapping
// ErrInvalid unless s has the "sp1:" prefix followed by exactly 64 lowercase
// hex digits.
func ParseDigest(s string) (Digest, error) {
	hexPart, ok := strings.CutPrefix(s, digestPrefix)
	if !ok || len(hexPart) != 2*sha256.Size {
		return "", fmt.Errorf("%w: digest %q is not sp1:<64 hex digits>", ErrInvalid, s)
	}
	for i := 0; i < len(hexPart); i++ {
		c := hexPart[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("%w: digest %q is not sp1:<64 lowercase hex digits>", ErrInvalid, s)
		}
	}
	return Digest(s), nil
}

// String returns the digest in its wire form.
func (d Digest) String() string { return string(d) }

// ChunkCount returns the number of chunks a blob of size bytes has. An empty
// blob has no chunks. A negative size returns 0.
func ChunkCount(size int64) int {
	if size <= 0 {
		return 0
	}
	return int((size + ChunkSize - 1) / ChunkSize)
}

// ChunkLen returns the size in bytes of chunk index of a blob of size bytes:
// ChunkSize for every chunk except the last. It returns 0 for an index outside
// the blob.
func ChunkLen(size int64, index int) int {
	n := ChunkCount(size)
	if index < 0 || index >= n {
		return 0
	}
	if index < n-1 {
		return ChunkSize
	}
	return int(size - int64(index)*ChunkSize)
}

// HashChunk returns the hash of one chunk's bytes.
func HashChunk(p []byte) [HashSize]byte {
	return sha256.Sum256(p)
}

// DigestOf returns the digest of a blob of size bytes from its chunk hashes,
// in order: the SHA-256 of the size as an 8-byte unsigned big-endian integer
// followed by the chunk hashes. It does not check the chunk count or the
// length of each hash; Manifest.Validate does.
func DigestOf(size int64, chunkHashes [][]byte) Digest {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(size))
	h.Write(n[:])
	for _, c := range chunkHashes {
		h.Write(c)
	}
	return Digest(digestPrefix + hex.EncodeToString(h.Sum(nil)))
}

// Compute reads r to the end and returns the blob's manifest. It reads one
// chunk at a time, so its memory use is ChunkSize whatever the blob's size.
func Compute(r io.Reader) (Manifest, error) {
	buf := make([]byte, ChunkSize)
	var m Manifest
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			sum := HashChunk(buf[:n])
			m.ChunkHashes = append(m.ChunkHashes, sum[:])
			m.Size += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return Manifest{}, err
		}
	}
	m.Digest = DigestOf(m.Size, m.ChunkHashes)
	return m, nil
}

// Manifest describes a blob: its digest, size and the hash of each chunk.
type Manifest struct {
	Digest      Digest
	Size        int64
	ChunkHashes [][]byte
}

// Validate reports an error wrapping ErrInvalid when the manifest is not
// consistent: a malformed digest, a negative size, a chunk-hash count that
// does not match the size, a hash that is not HashSize bytes, or a size and
// chunk hashes that do not produce the digest.
func (m Manifest) Validate() error {
	if _, err := ParseDigest(string(m.Digest)); err != nil {
		return err
	}
	if m.Size < 0 {
		return fmt.Errorf("%w: negative size %d", ErrInvalid, m.Size)
	}
	if want := ChunkCount(m.Size); len(m.ChunkHashes) != want {
		return fmt.Errorf("%w: %d chunk hashes for a size that has %d chunks", ErrInvalid, len(m.ChunkHashes), want)
	}
	for i, c := range m.ChunkHashes {
		if len(c) != HashSize {
			return fmt.Errorf("%w: chunk hash %d is %d bytes, not %d", ErrInvalid, i, len(c), HashSize)
		}
	}
	if got := DigestOf(m.Size, m.ChunkHashes); got != m.Digest {
		return fmt.Errorf("%w: size and chunk hashes produce %s, not %s", ErrInvalid, got, m.Digest)
	}
	return nil
}

// CheckChunk reports an error wrapping ErrInvalid when p is not chunk index of
// the blob m describes: the index is outside the blob, the length is wrong, or
// the hash differs.
func (m Manifest) CheckChunk(index int, p []byte) error {
	if index < 0 || index >= len(m.ChunkHashes) {
		return fmt.Errorf("%w: chunk %d is outside a blob of %d chunks", ErrInvalid, index, len(m.ChunkHashes))
	}
	if want := ChunkLen(m.Size, index); len(p) != want {
		return fmt.Errorf("%w: chunk %d is %d bytes, not %d", ErrInvalid, index, len(p), want)
	}
	sum := HashChunk(p)
	if !bytes.Equal(sum[:], m.ChunkHashes[index]) {
		return fmt.Errorf("%w: chunk %d does not match its hash", ErrInvalid, index)
	}
	return nil
}

// Info describes a committed blob.
type Info struct {
	Manifest Manifest
	// CommittedAt is when Store.Complete committed the blob.
	CommittedAt time.Time
	// UnreferencedSince is the time Store.SetUnreferencedSince last recorded,
	// or the zero time when no time is recorded.
	UnreferencedSince time.Time
}

// Upload describes a staged upload.
type Upload struct {
	Manifest Manifest
	// StartedAt is when Store.Begin first staged the upload.
	StartedAt time.Time
}

// Store keeps blob bytes and the little metadata the engine needs about them.
// Every name is scoped to a workspace: the same digest in two workspaces is
// two blobs, whether or not the Store keeps one copy of the bytes.
//
// A blob is first staged, then committed. Begin stages an upload, PutChunk
// stores its chunks, and Complete commits it. Only a committed blob can be
// read. A committed blob does not change, except for its UnreferencedSince
// mark, until Delete removes it.
//
// Implementations must be safe for concurrent use, also by several processes
// that share one Store. Calls that race on one name must leave a state that
// one of them alone could have produced.
type Store interface {
	// Begin stages an upload of the blob m describes. m must be valid (see
	// Manifest.Validate); the Store need not check it again. Staging a
	// digest that is already staged keeps the first upload and its chunks.
	// Begin on a committed blob does nothing and is not an error.
	Begin(ctx context.Context, workspaceID string, m Manifest) error
	// Staged returns a staged upload and the indexes of the chunks it holds,
	// in ascending order. It returns ErrNotFound when no upload of the digest
	// is staged.
	Staged(ctx context.Context, workspaceID string, digest Digest) (Upload, []int, error)
	// PutChunk stores chunk index of a staged upload, reading exactly the
	// chunk's length from r. It checks the bytes against the chunk's hash and
	// stores nothing when they differ or when r ends early, returning an error
	// wrapping ErrInvalid. Storing a chunk that is already stored replaces it with the
	// same bytes. It returns ErrNotFound when no upload of the digest is
	// staged.
	PutChunk(ctx context.Context, workspaceID string, digest Digest, index int, r io.Reader) error
	// Complete commits a staged upload that holds every chunk and removes the
	// staged state. It returns ErrIncomplete when a chunk is missing, and
	// ErrNotFound when no upload of the digest is staged. Complete on a blob
	// that is already committed returns nil. The commit is atomic: a reader
	// sees either no blob or the whole blob.
	Complete(ctx context.Context, workspaceID string, digest Digest) error
	// Abort removes a staged upload and its chunks. Aborting an upload that
	// is not staged is not an error. A committed blob is not changed.
	Abort(ctx context.Context, workspaceID string, digest Digest) error
	// ListStaged yields every staged upload of the workspace, in no fixed
	// order. A call that changes the Store while the iteration runs may or
	// may not be seen by it.
	ListStaged(ctx context.Context, workspaceID string) iter.Seq2[Upload, error]

	// Stat returns a committed blob's Info, or ErrNotFound.
	Stat(ctx context.Context, workspaceID string, digest Digest) (Info, error)
	// Open returns a reader over a committed blob's bytes, or ErrNotFound.
	// The reader supports Seek, so a caller can read one chunk or any range.
	// The caller closes it.
	Open(ctx context.Context, workspaceID string, digest Digest) (io.ReadSeekCloser, error)
	// SetUnreferencedSince records t on a committed blob, or removes the
	// mark when t is the zero time. It returns ErrNotFound when the blob is
	// not committed.
	SetUnreferencedSince(ctx context.Context, workspaceID string, digest Digest, t time.Time) error
	// Delete removes a committed blob. Deleting a blob that is not committed
	// is not an error. A staged upload of the same digest is not changed.
	Delete(ctx context.Context, workspaceID string, digest Digest) error
	// List yields the digest of every committed blob of the workspace, in no
	// fixed order. A call that changes the Store while the iteration runs may
	// or may not be seen by it.
	List(ctx context.Context, workspaceID string) iter.Seq2[Digest, error]
	// ListWorkspaces yields the id of every workspace that has a committed
	// blob or a staged upload, in no fixed order.
	ListWorkspaces(ctx context.Context) iter.Seq2[string, error]
	// DeleteWorkspace removes every committed blob and staged upload of the
	// workspace. Deleting a workspace that holds nothing is not an error.
	DeleteWorkspace(ctx context.Context, workspaceID string) error

	// Close releases the Store's resources.
	Close() error
}
