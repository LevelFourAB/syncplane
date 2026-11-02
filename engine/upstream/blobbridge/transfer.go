package blobbridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/blob"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// checkBatch is how many digests one check run covers. It is the most a
// StatBlobs request may name.
const checkBatch = 256

// work runs due jobs until ctx ends.
func (b *Bridge) work(ctx context.Context, st *runState) {
	for {
		j := st.sched.next(ctx)
		if j == nil {
			return
		}
		var o outcome
		switch j.key.kind {
		case kindCheck:
			o = b.runCheck(ctx, st, j)
		case kindUpload:
			o = b.runUpload(ctx, j.key.ws, j.key.digest)
		case kindFetch:
			o = b.runFetch(ctx, st, j)
		}
		st.sched.finish(j, o)
	}
}

// runCheck checks a batch of the digests of check job j: each blob that is
// committed locally, named by a live row that counts, and not complete at the
// origin is queued for upload. With prefetch on, each blob that a row names
// and the local store lacks is queued for a fetch.
func (b *Bridge) runCheck(ctx context.Context, st *runState, j *job) outcome {
	ws := j.key.ws
	batch := st.sched.takeDigests(j, checkBatch)
	if len(batch) == 0 {
		return done
	}
	fail := func(msg string, err error) outcome {
		st.sched.returnDigests(j, batch)
		b.logFailure(ctx, msg, ws, "", err)
		return retry
	}
	var local, absent []blob.Digest
	for _, d := range batch {
		_, err := b.inner.Stat(ctx, ws, d)
		switch {
		case err == nil:
			local = append(local, d)
		case errors.Is(err, blob.ErrNotFound):
			if b.cfg.prefetch {
				absent = append(absent, d)
			}
		default:
			return fail("read local blob", err)
		}
	}
	if len(local)+len(absent) == 0 {
		return done
	}
	named, err := st.srv.ReferencedBlobs(ctx, ws, append(local, absent...), b.entityFilter())
	if err != nil {
		return fail("find rows that name blobs", err)
	}
	for _, d := range absent {
		if named[d] {
			st.sched.want(ws, d, b.cfg.patience)
		}
	}
	var ask []string
	for _, d := range local {
		if named[d] {
			ask = append(ask, d.String())
		}
	}
	if len(ask) == 0 {
		return done
	}
	res, err := b.origin.StatBlobs(ctx, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws, Digests: ask}))
	if err != nil {
		return fail("ask the origin for blobs", err)
	}
	complete := map[string]bool{}
	for _, s := range res.Msg.GetBlobs() {
		if s.GetState() == syncplanev1.BlobState_BLOB_STATE_COMPLETE {
			complete[s.GetDigest()] = true
		}
	}
	for _, d := range ask {
		if !complete[d] {
			st.sched.upload(ws, blob.Digest(d))
		}
	}
	return done
}

// runUpload sends the chunks of a local blob that the origin is missing.
func (b *Bridge) runUpload(ctx context.Context, ws string, d blob.Digest) outcome {
	info, err := b.inner.Stat(ctx, ws, d)
	if errors.Is(err, blob.ErrNotFound) {
		// Deleted since the check; no row that counts names it any more.
		return done
	}
	if err != nil {
		b.logFailure(ctx, "read local blob", ws, d, err)
		return retry
	}
	m := info.Manifest
	begin, err := b.origin.BeginUpload(ctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
		WorkspaceId: ws,
		Digest:      d.String(),
		Size:        m.Size,
		ChunkHashes: m.ChunkHashes,
	}))
	if err != nil {
		return b.originFailure(ctx, "start upload to the origin", ws, d, err)
	}
	missing := begin.Msg.GetMissingChunks()
	if begin.Msg.GetState() == syncplanev1.BlobState_BLOB_STATE_COMPLETE || len(missing) == 0 {
		return done
	}
	r, err := b.inner.Open(ctx, ws, d)
	if errors.Is(err, blob.ErrNotFound) {
		return done
	}
	if err != nil {
		b.logFailure(ctx, "open local blob", ws, d, err)
		return retry
	}
	defer r.Close()
	buf := make([]byte, 0, min(int64(blob.ChunkSize), m.Size))
	for _, index := range missing {
		i := int(index)
		buf = buf[:blob.ChunkLen(m.Size, i)]
		if _, err := r.Seek(int64(i)*blob.ChunkSize, io.SeekStart); err != nil {
			b.logFailure(ctx, "read local blob", ws, d, err)
			return retry
		}
		if _, err := io.ReadFull(r, buf); err != nil {
			b.logFailure(ctx, "read local blob", ws, d, err)
			return retry
		}
		if err := m.CheckChunk(i, buf); err != nil {
			// The local store holds bytes that do not match the blob; sending
			// them would only be refused.
			b.cfg.log.LogAttrs(ctx, slog.LevelError, "blob bridge: local blob is corrupt",
				slog.String("workspace", ws), slog.String("digest", d.String()), slog.Any("err", err))
			return done
		}
		if _, err := b.origin.PutChunk(ctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
			WorkspaceId: ws,
			Digest:      d.String(),
			Index:       index,
			Data:        buf,
		})); err != nil {
			return b.originFailure(ctx, "upload chunk to the origin", ws, d, err)
		}
	}
	return done
}

// runFetch copies a blob from the origin into the local engine, when a live
// local row that counts names it.
func (b *Bridge) runFetch(ctx context.Context, st *runState, j *job) outcome {
	ws, d := j.key.ws, j.key.digest
	if _, err := b.inner.Stat(ctx, ws, d); err == nil {
		return done
	} else if !errors.Is(err, blob.ErrNotFound) {
		b.logFailure(ctx, "read local blob", ws, d, err)
		return retry
	}
	named, err := st.srv.ReferencedBlobs(ctx, ws, []blob.Digest{d}, b.entityFilter())
	if err != nil {
		b.logFailure(ctx, "find rows that name blobs", ws, d, err)
		return retry
	}
	if !named[d] {
		// A row that names it can still arrive from the origin.
		return retry
	}
	res, err := b.origin.StatBlobs(ctx, connect.NewRequest(&syncplanev1.StatBlobsRequest{WorkspaceId: ws, Digests: []string{d.String()}}))
	if err != nil {
		b.logFailure(ctx, "ask the origin for a blob", ws, d, err)
		return retry
	}
	stats := res.Msg.GetBlobs()
	if len(stats) != 1 || stats[0].GetState() != syncplanev1.BlobState_BLOB_STATE_COMPLETE {
		return retry
	}
	m := blob.Manifest{Digest: d, Size: stats[0].GetSize(), ChunkHashes: stats[0].GetChunkHashes()}
	if err := m.Validate(); err != nil {
		b.logFailure(ctx, "origin reports a manifest that does not match the digest", ws, d, err)
		return retry
	}
	// The blob goes in through the local engine's own upload calls, so the
	// engine applies its size limit and notifies the workspace on commit.
	// The marker keeps the bridge's store methods from treating these calls
	// as reads or uploads of their own.
	lctx := server.Trusted(bridgeCall(ctx))
	begin, err := st.srv.BeginUpload(lctx, connect.NewRequest(&syncplanev1.BeginUploadRequest{
		WorkspaceId: ws,
		Digest:      d.String(),
		Size:        m.Size,
		ChunkHashes: m.ChunkHashes,
	}))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeResourceExhausted {
			b.logFailure(ctx, "blob is over the local size limit", ws, d, err)
			return done
		}
		b.logFailure(ctx, "stage local blob", ws, d, err)
		return retry
	}
	st.sched.markStaged(j)
	for _, index := range begin.Msg.GetMissingChunks() {
		chunk, err := b.origin.GetChunk(ctx, connect.NewRequest(&syncplanev1.GetChunkRequest{
			WorkspaceId: ws,
			Digest:      d.String(),
			Index:       index,
		}))
		if err != nil {
			b.logFailure(ctx, "download chunk from the origin", ws, d, err)
			return retry
		}
		data := chunk.Msg.GetData()
		if err := m.CheckChunk(int(index), data); err != nil {
			b.logFailure(ctx, "origin sent a chunk that does not match its hash", ws, d, err)
			return retry
		}
		if _, err := st.srv.PutChunk(lctx, connect.NewRequest(&syncplanev1.PutChunkRequest{
			WorkspaceId: ws,
			Digest:      d.String(),
			Index:       index,
			Data:        data,
		})); err != nil {
			b.logFailure(ctx, "store chunk locally", ws, d, err)
			return retry
		}
	}
	return done
}

// originFailure logs a failed call to the origin and decides whether to
// retry it. A refusal that a retry cannot change ends the job.
func (b *Bridge) originFailure(ctx context.Context, msg, ws string, d blob.Digest, err error) outcome {
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument, connect.CodeResourceExhausted, connect.CodePermissionDenied:
		b.cfg.log.LogAttrs(ctx, slog.LevelError, "blob bridge: origin refused the upload; it is tried again at the next start",
			slog.String("workspace", ws), slog.String("digest", d.String()), slog.Any("err", err))
		return done
	}
	b.logFailure(ctx, msg, ws, d, err)
	return retry
}

// logFailure logs a failure that is retried. A failure because the Run is
// ending is not logged.
func (b *Bridge) logFailure(ctx context.Context, msg, ws string, d blob.Digest, err error) {
	if ctx.Err() != nil {
		return
	}
	attrs := []slog.Attr{slog.String("workspace", ws), slog.Any("err", err)}
	if d != "" {
		attrs = append(attrs, slog.String("digest", d.String()))
	}
	b.cfg.log.LogAttrs(ctx, slog.LevelWarn, fmt.Sprintf("blob bridge: %s", msg), attrs...)
}
