package client

import (
	"context"
	"fmt"
	"io"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/blob"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// UploadBlob uploads the size bytes of r into the workspace's blob store and
// returns the reference a row stores to point at them. mediaType is carried
// in the reference as given.
//
// It reads r twice: once to compute the digest, then again for each chunk
// the server is missing. A blob the server already holds sends no chunks, and
// an interrupted upload resumes where it stopped. Memory use is one chunk
// (blob.ChunkSize bytes). The server's errors arrive as *connect.Error, for
// example ResourceExhausted for a blob above its size limit.
func (w *Workspace) UploadBlob(ctx context.Context, r io.ReaderAt, size int64, mediaType string) (*syncplanev1.BlobRef, error) {
	if size < 0 {
		return nil, fmt.Errorf("client: negative blob size %d", size)
	}
	m, err := blob.Compute(io.NewSectionReader(r, 0, size))
	if err != nil {
		return nil, fmt.Errorf("client: read blob: %w", err)
	}
	if m.Size != size {
		return nil, fmt.Errorf("client: blob reader holds %d bytes, not %d", m.Size, size)
	}
	ref := &syncplanev1.BlobRef{Digest: m.Digest.String(), Size: m.Size, MediaType: mediaType}
	begin, err := w.c.blob.BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
		WorkspaceId: w.id,
		Digest:      ref.Digest,
		Size:        m.Size,
		ChunkHashes: m.ChunkHashes,
	}))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, min(int64(blob.ChunkSize), size))
	for _, index := range begin.Msg.GetMissingChunks() {
		n := blob.ChunkLen(m.Size, int(index))
		buf = buf[:n]
		if _, err := r.ReadAt(buf, int64(index)*blob.ChunkSize); err != nil && err != io.EOF {
			return nil, fmt.Errorf("client: read blob chunk %d: %w", index, err)
		}
		if err := m.CheckChunk(int(index), buf); err != nil {
			return nil, fmt.Errorf("client: blob changed while uploading: %w", err)
		}
		if _, err := w.c.blob.PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
			WorkspaceId: w.id,
			Digest:      ref.Digest,
			Index:       index,
			Data:        buf,
		})); err != nil {
			return nil, err
		}
	}
	return ref, nil
}

// DownloadBlob writes the blob that digest names to dst, checking each chunk
// against its hash, and returns the number of bytes written.
//
// When the server reports the blob as being uploaded, DownloadBlob blocks
// until the upload completes or ctx ends; it then returns ctx's error. It
// fails with a *connect.Error of code NotFound when no row the caller can
// read refers to the blob, when the server has no bytes for it, and when an
// upload it waits for is removed. On an error dst may hold part of the blob.
func (w *Workspace) DownloadBlob(ctx context.Context, digest string, dst io.Writer) (int64, error) {
	stat, err := w.c.blob.StatBlobs(ctx, connect.NewRequest(&syncplanev1.StatBlobsRequest{
		WorkspaceId: w.id,
		Digests:     []string{digest},
	}))
	if err != nil {
		return 0, err
	}
	sts := stat.Msg.GetBlobs()
	if len(sts) != 1 {
		return 0, notAvailable(digest)
	}
	st := sts[0]
	if st.GetState() == syncplanev1.BlobState_BLOB_STATE_UPLOADING {
		if st, err = w.awaitBlob(ctx, digest); err != nil {
			return 0, err
		}
	}
	if st.GetState() != syncplanev1.BlobState_BLOB_STATE_COMPLETE {
		return 0, notAvailable(digest)
	}
	m := blob.Manifest{Digest: blob.Digest(digest), Size: st.GetSize(), ChunkHashes: st.GetChunkHashes()}
	if err := m.Validate(); err != nil {
		return 0, fmt.Errorf("client: blob %s: %w", digest, err)
	}
	var written int64
	for i := range m.ChunkHashes {
		resp, err := w.c.blob.GetChunk(ctx, connect.NewRequest(&syncplanev1.GetChunkRequest{
			WorkspaceId: w.id,
			Digest:      digest,
			Index:       uint32(i),
		}))
		if err != nil {
			return written, err
		}
		data := resp.Msg.GetData()
		if err := m.CheckChunk(i, data); err != nil {
			return written, fmt.Errorf("client: blob %s: %w", digest, err)
		}
		n, err := dst.Write(data)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// awaitBlob watches the blob until the server reports it in a state other
// than uploading, and returns that status. It returns ctx's error when ctx
// ends first.
func (w *Workspace) awaitBlob(ctx context.Context, digest string) (*syncplanev1.BlobStatus, error) {
	stream, err := w.c.blob.WatchBlobs(ctx, connect.NewRequest(&syncplanev1.WatchBlobsRequest{
		WorkspaceId: w.id,
		Digests:     []string{digest},
	}))
	if err != nil {
		return nil, watchErr(ctx, err)
	}
	defer stream.Close()
	for stream.Receive() {
		for _, st := range stream.Msg().GetBlobs() {
			if st.GetDigest() == digest && st.GetState() != syncplanev1.BlobState_BLOB_STATE_UPLOADING {
				return st, nil
			}
		}
	}
	if err := stream.Err(); err != nil {
		return nil, watchErr(ctx, err)
	}
	// The server ends a watch only once it has reported every blob complete,
	// so a stream that ends without that report was cut off.
	return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("client: watch of blob %s ended before it completed", digest))
}

// watchErr returns ctx's error when ctx ended, and err otherwise.
func watchErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func notAvailable(digest string) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("client: blob %s is not available", digest))
}
