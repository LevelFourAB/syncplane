package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/blob"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

var _ syncplanev1connect.BlobServiceHandler = (*Server)(nil)

// blobRequestBytes is the smallest request bound the blob service runs with:
// one chunk, plus room for the rest of a PutChunk request.
const blobRequestBytes = blob.ChunkSize + 64<<10

// maxStatBlobs caps the digests one StatBlobs request may name.
const maxStatBlobs = 256

// defaultBlobRetention is how long blob GC keeps an unreferenced blob and a
// staged upload when WithBlobRetention says nothing.
const defaultBlobRetention = 30 * 24 * time.Hour

// WithBlobStore makes the server keep blobs in st and serve
// syncplane.v1.BlobService from it. Without it every BlobService call fails
// with unimplemented.
//
// The server does not close st. A Manager that passes one store to every app
// it hosts keeps the apps apart: each app names its workspaces in st with its
// own prefix.
func WithBlobStore(st blob.Store) Option {
	return func(s *Server) error {
		s.blobs = st
		return nil
	}
}

// WithBlobRetention sets how long blob GC keeps a committed blob that no live
// row refers to, and a staged upload that has not completed. It takes effect
// only together with WithBlobStore and WithGC. A non-positive d keeps the
// default, 30 days.
//
// A blob is deleted on the first GC pass after it has been unreferenced for
// d, so a row that refers to it again within d keeps it. The clock restarts
// when BeginUpload names the blob.
func WithBlobRetention(d time.Duration) Option {
	return func(s *Server) error {
		s.blobRetention = d
		return nil
	}
}

// blobRetentionOrDefault is the configured retention, or the default.
func (s *Server) blobRetentionOrDefault() time.Duration {
	if s.blobRetention <= 0 {
		return defaultBlobRetention
	}
	return s.blobRetention
}

// blobKey is the name a workspace has in the blob store. A Manager's apps may
// share one store, so the server's share of the workspace-id space goes in
// front, as it does for storage and the notifier.
func (s *Server) blobKey(workspaceID string) string { return s.wsPrefix + workspaceID }

// blobStore returns the server's store, or unimplemented when it has none.
func (s *Server) blobStore() (blob.Store, error) {
	if s.blobs == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server stores no blobs"))
	}
	return s.blobs, nil
}

// blobStoreError maps an error of the blob store onto the wire. The store's
// own message can name files and hosts, so it goes to the log and the client
// gets a generic one.
func (s *Server) blobStoreError(ctx context.Context, err error) error {
	if connect.CodeOf(err) != connect.CodeUnknown {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeCanceled, errors.New("the request ended before the blob store answered"))
	}
	s.log().LogAttrs(ctx, slog.LevelError, "blob store failed", slog.Any("err", err))
	return connect.NewError(connect.CodeInternal, errors.New("the blob store failed"))
}

// admitBlobUpload runs the workspace gate for an upload. An upload needs
// admission to the workspace and nothing else: which rows may name the blob is
// decided when they are written, and who may read it when it is read.
func (s *Server) admitBlobUpload(ctx context.Context, workspaceID string) error {
	if err := naming.ValidateWorkspaceID(workspaceID); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if isTrusted(ctx) {
		return nil
	}
	_, err := s.enterWorkspace(ctx, workspaceID)
	return err
}

// BeginUpload starts or resumes the upload of one blob; see
// syncplanev1connect.BlobServiceHandler for the wire contract.
//
// When the server's Permissions implements BlobPermissions, BeginUpload asks
// CanUploadBlob after the request is found valid, also for a blob the server
// already stores. A ctx marked with Trusted skips the workspace gate and that
// hook.
func (s *Server) BeginUpload(
	ctx context.Context,
	req *connect.Request[syncplanev1.BeginUploadRequest],
) (*connect.Response[syncplanev1.BeginUploadResponse], error) {
	st, err := s.blobStore()
	if err != nil {
		return nil, err
	}
	msg := req.Msg
	ws := msg.GetWorkspaceId()
	if err := s.admitBlobUpload(ctx, ws); err != nil {
		return nil, err
	}
	if msg.GetSize() > s.limits.MaxBlobBytes {
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf("blob size %d bytes, over MaxBlobBytes (%d); raise it with server.WithLimits",
			msg.GetSize(), s.limits.MaxBlobBytes))
	}
	m := blob.Manifest{Digest: blob.Digest(msg.GetDigest()), Size: msg.GetSize(), ChunkHashes: msg.GetChunkHashes()}
	if err := m.Validate(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.checkUploadPolicy(ctx, ws, m.Size); err != nil {
		return nil, err
	}
	committed, err := s.reviveBlob(ctx, ws, m.Digest)
	if err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	if committed {
		return connect.NewResponse(&syncplanev1.BeginUploadResponse{State: syncplanev1.BlobState_BLOB_STATE_COMPLETE}), nil
	}
	key := s.blobKey(ws)
	// Twice at most: a staged upload can vanish between Begin and Staged when
	// another caller completes it or GC aborts it, and only the second needs
	// staging again.
	for attempt := 0; ; attempt++ {
		if err := st.Begin(ctx, key, m); err != nil {
			return nil, s.blobStoreError(ctx, err)
		}
		up, held, err := st.Staged(ctx, key, m.Digest)
		if errors.Is(err, blob.ErrNotFound) {
			if _, serr := st.Stat(ctx, key, m.Digest); serr == nil {
				return connect.NewResponse(&syncplanev1.BeginUploadResponse{State: syncplanev1.BlobState_BLOB_STATE_COMPLETE}), nil
			} else if !errors.Is(serr, blob.ErrNotFound) {
				return nil, s.blobStoreError(ctx, serr)
			}
			if attempt == 0 {
				continue
			}
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("upload of %s was removed while it started; retry", m.Digest))
		}
		if err != nil {
			return nil, s.blobStoreError(ctx, err)
		}
		missing := missingChunks(blob.ChunkCount(up.Manifest.Size), held)
		if len(missing) > 0 {
			return connect.NewResponse(&syncplanev1.BeginUploadResponse{
				State:         syncplanev1.BlobState_BLOB_STATE_UPLOADING,
				MissingChunks: missing,
			}), nil
		}
		// Nothing left to send: an empty blob, or an upload whose last chunk
		// landed without the commit that should have followed it.
		state, err := s.completeBlob(ctx, ws, m.Digest)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&syncplanev1.BeginUploadResponse{State: state}), nil
	}
}

// checkUploadPolicy asks BlobPermissions.CanUploadBlob, when the server's
// Permissions implements it, whether the caller may upload a blob of size
// bytes. A trusted call is not asked about.
func (s *Server) checkUploadPolicy(ctx context.Context, workspaceID string, size int64) error {
	policy, ok := s.perms.(BlobPermissions)
	if !ok || isTrusted(ctx) {
		return nil
	}
	err := policy.CanUploadBlob(ctx, principalOf(ctx), workspaceID, size)
	if err == nil {
		return nil
	}
	if r, ok := AsRejection(err); ok {
		ce := connect.NewError(connect.CodePermissionDenied, errors.New(r.Message))
		ce.Meta().Set(ErrorCodeMetadataKey, r.Code)
		return ce
	}
	if connect.CodeOf(err) != connect.CodeUnknown {
		return err
	}
	return connect.NewError(connect.CodeInternal, fmt.Errorf("deciding whether the upload is allowed: %w", err))
}

// missingChunks returns the indexes below n that held, in ascending order,
// does not contain.
func missingChunks(n int, held []int) []uint32 {
	var out []uint32
	j := 0
	for i := 0; i < n; i++ {
		if j < len(held) && held[j] == i {
			j++
			continue
		}
		out = append(out, uint32(i))
	}
	return out
}

// reviveBlob reports whether the workspace has the blob committed, and
// removes its UnreferencedSince mark when it does, so an uploader that dedupes
// against an old blob restarts its GC clock.
//
// Clearing a mark holds the workspace's write exclusion, which blob GC also
// holds while it re-reads the mark and deletes (see deleteUnchanged). Without it GC could read the
// old mark, this call clear it and report the blob complete, and GC then
// delete it.
func (s *Server) reviveBlob(ctx context.Context, workspaceID string, d blob.Digest) (bool, error) {
	key := s.blobKey(workspaceID)
	info, err := s.blobs.Stat(ctx, key, d)
	if errors.Is(err, blob.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.UnreferencedSince.IsZero() {
		// GC deletes only a blob whose mark it reads as old inside the
		// exclusion, and a mark it sets is the time of that pass. A blob with
		// no mark now cannot be deleted before a later pass marks it and the
		// whole retention runs out, so it needs no exclusion here.
		return true, nil
	}
	var committed bool
	err = s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		committed = false
		// The exclusion starts at the first call on tx, not at the open.
		if _, err := tx.CurrentSeq(); err != nil {
			return err
		}
		info, err := s.blobs.Stat(ctx, key, d)
		if errors.Is(err, blob.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.UnreferencedSince.IsZero() {
			err := s.blobs.SetUnreferencedSince(ctx, key, d, time.Time{})
			if errors.Is(err, blob.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
		}
		committed = true
		return nil
	})
	return committed, err
}

// completeBlob commits a staged upload that holds every chunk, and pokes the
// workspace's subscribers so a client waiting for the blob checks again. It
// returns UPLOADING when a chunk is still missing.
func (s *Server) completeBlob(ctx context.Context, workspaceID string, d blob.Digest) (syncplanev1.BlobState, error) {
	key := s.blobKey(workspaceID)
	err := s.blobs.Complete(ctx, key, d)
	switch {
	case err == nil:
		s.notifier.Notify(workspaceID)
		return syncplanev1.BlobState_BLOB_STATE_COMPLETE, nil
	case errors.Is(err, blob.ErrIncomplete):
		return syncplanev1.BlobState_BLOB_STATE_UPLOADING, nil
	case errors.Is(err, blob.ErrNotFound):
		// Another caller committed it first, and poked for it. The caller
		// sent a manifest or a matching chunk to get here.
		return s.committedOr(ctx, workspaceID, d, nil)
	default:
		return 0, s.blobStoreError(ctx, err)
	}
}

// committedOr returns COMPLETE when the blob is committed and proof accepts
// its manifest, and the error for a PutChunk that names no started upload
// otherwise. A nil proof accepts every manifest.
//
// A PutChunk caller names a digest and nothing else, so without the proof the
// answer would tell any member of the workspace whether the server stores a
// blob. A chunk that matches the committed manifest shows that the caller
// holds its bytes, which BeginUpload's manifest shows too.
func (s *Server) committedOr(ctx context.Context, workspaceID string, d blob.Digest, proof func(blob.Manifest) error) (syncplanev1.BlobState, error) {
	notStarted := connect.NewError(connect.CodeFailedPrecondition,
		fmt.Errorf("no upload of %s is started; call BeginUpload first", d))
	info, err := s.blobs.Stat(ctx, s.blobKey(workspaceID), d)
	if errors.Is(err, blob.ErrNotFound) {
		return 0, notStarted
	}
	if err != nil {
		return 0, s.blobStoreError(ctx, err)
	}
	if proof != nil && proof(info.Manifest) != nil {
		return 0, notStarted
	}
	return syncplanev1.BlobState_BLOB_STATE_COMPLETE, nil
}

// PutChunk stores one chunk of a blob whose upload BeginUpload started; see
// syncplanev1connect.BlobServiceHandler for the wire contract. The call that
// stores the last missing chunk commits the blob and pokes the workspace's
// subscribers. When no upload is staged, it reports a committed blob as
// complete only when the chunk matches it, and otherwise fails as for a
// digest the server does not have.
//
// A ctx marked with Trusted skips the workspace gate.
func (s *Server) PutChunk(
	ctx context.Context,
	req *connect.Request[syncplanev1.PutChunkRequest],
) (*connect.Response[syncplanev1.PutChunkResponse], error) {
	st, err := s.blobStore()
	if err != nil {
		return nil, err
	}
	msg := req.Msg
	ws := msg.GetWorkspaceId()
	if err := s.admitBlobUpload(ctx, ws); err != nil {
		return nil, err
	}
	d, err := blob.ParseDigest(msg.GetDigest())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	respond := func(state syncplanev1.BlobState, err error) (*connect.Response[syncplanev1.PutChunkResponse], error) {
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&syncplanev1.PutChunkResponse{State: state}), nil
	}
	index := int(msg.GetIndex())
	proof := func(m blob.Manifest) error { return m.CheckChunk(index, msg.GetData()) }
	key := s.blobKey(ws)
	up, _, err := st.Staged(ctx, key, d)
	if errors.Is(err, blob.ErrNotFound) {
		return respond(s.committedOr(ctx, ws, d, proof))
	}
	if err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	// Checked here as well as by the store, so a wrong chunk fails the same
	// way whatever the store does with it.
	if err := up.Manifest.CheckChunk(index, msg.GetData()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	err = st.PutChunk(ctx, key, d, index, bytes.NewReader(msg.GetData()))
	switch {
	case errors.Is(err, blob.ErrNotFound):
		return respond(s.committedOr(ctx, ws, d, proof))
	case errors.Is(err, blob.ErrInvalid):
		// The chunk passed CheckChunk above, so this is the store's own
		// account, whose message is not the client's to read.
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("chunk %d of %s does not match its hash", index, d))
	case err != nil:
		return nil, s.blobStoreError(ctx, err)
	}
	_, held, err := st.Staged(ctx, key, d)
	if errors.Is(err, blob.ErrNotFound) {
		return respond(s.committedOr(ctx, ws, d, proof))
	}
	if err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	if len(held) < blob.ChunkCount(up.Manifest.Size) {
		return respond(syncplanev1.BlobState_BLOB_STATE_UPLOADING, nil)
	}
	return respond(s.completeBlob(ctx, ws, d))
}

// StatBlobs reports the state of blobs; see
// syncplanev1connect.BlobServiceHandler for the wire contract. A digest the
// caller cannot read reports BLOB_STATE_MISSING: the caller reads a blob when
// it reads a live row that names the digest in a syncplane.v1.BlobRef field,
// with the fields a guard hides from it cleared, as View would return the row.
//
// It fails with invalid_argument for a malformed digest, and for a request
// that names more than 256 digests. A ctx marked with Trusted reads every
// blob a live row names.
func (s *Server) StatBlobs(
	ctx context.Context,
	req *connect.Request[syncplanev1.StatBlobsRequest],
) (*connect.Response[syncplanev1.StatBlobsResponse], error) {
	if _, err := s.blobStore(); err != nil {
		return nil, err
	}
	msg := req.Msg
	ws := msg.GetWorkspaceId()
	if err := naming.ValidateWorkspaceID(ws); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	digests, err := parseDigests(msg.GetDigests(), true)
	if err != nil {
		return nil, err
	}
	out, err := s.blobStatuses(ctx, ws, digests)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&syncplanev1.StatBlobsResponse{Blobs: out}), nil
}

// parseDigests parses the digests of a StatBlobs or WatchBlobs request. It
// fails with invalid_argument for a malformed digest, for more than
// maxStatBlobs digests, and for none unless allowNone is set.
func parseDigests(raw []string, allowNone bool) ([]blob.Digest, error) {
	if len(raw) > maxStatBlobs {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("the request names %d digests, the limit is %d", len(raw), maxStatBlobs))
	}
	if len(raw) == 0 && !allowNone {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("the request names no digests"))
	}
	digests := make([]blob.Digest, 0, len(raw))
	for _, r := range raw {
		d, err := blob.ParseDigest(r)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		digests = append(digests, d)
	}
	return digests, nil
}

// blobStatuses returns the status of each digest, in order, as the caller of
// ctx may see it: the store's state of a blob the caller reads, and MISSING
// for every other digest.
//
// The store is asked first, so the read check covers only the digests the
// store has something for. The workspace gate runs either way.
func (s *Server) blobStatuses(ctx context.Context, workspaceID string, digests []blob.Digest) ([]*syncplanev1.BlobStatus, error) {
	key := s.blobKey(workspaceID)
	stored := map[blob.Digest]*syncplanev1.BlobStatus{}
	var present []blob.Digest
	for _, d := range digests {
		if _, seen := stored[d]; seen {
			continue
		}
		stored[d] = nil
		info, err := s.blobs.Stat(ctx, key, d)
		if err == nil {
			stored[d] = &syncplanev1.BlobStatus{Digest: string(d), State: syncplanev1.BlobState_BLOB_STATE_COMPLETE,
				Size: info.Manifest.Size, ChunkHashes: info.Manifest.ChunkHashes}
			present = append(present, d)
			continue
		}
		if !errors.Is(err, blob.ErrNotFound) {
			return nil, s.blobStoreError(ctx, err)
		}
		up, _, err := s.blobs.Staged(ctx, key, d)
		if errors.Is(err, blob.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, s.blobStoreError(ctx, err)
		}
		stored[d] = &syncplanev1.BlobStatus{Digest: string(d), State: syncplanev1.BlobState_BLOB_STATE_UPLOADING, Size: up.Manifest.Size}
		present = append(present, d)
	}
	readable, err := s.readableBlobs(ctx, workspaceID, present)
	if err != nil {
		return nil, err
	}
	out := make([]*syncplanev1.BlobStatus, 0, len(digests))
	for _, d := range digests {
		if st := stored[d]; st != nil && readable[d] {
			out = append(out, proto.CloneOf(st))
			continue
		}
		out = append(out, &syncplanev1.BlobStatus{Digest: string(d), State: syncplanev1.BlobState_BLOB_STATE_MISSING})
	}
	return out, nil
}

// GetChunk returns one chunk of a complete blob; see
// syncplanev1connect.BlobServiceHandler for the wire contract. It fails with
// not_found for a blob the caller cannot read, by the rule StatBlobs states,
// in the same way as for a blob the server does not have.
//
// It fails with invalid_argument for a malformed digest. A ctx marked with
// Trusted reads every blob a live row names.
func (s *Server) GetChunk(
	ctx context.Context,
	req *connect.Request[syncplanev1.GetChunkRequest],
) (*connect.Response[syncplanev1.GetChunkResponse], error) {
	st, err := s.blobStore()
	if err != nil {
		return nil, err
	}
	msg := req.Msg
	ws := msg.GetWorkspaceId()
	if err := naming.ValidateWorkspaceID(ws); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	d, err := blob.ParseDigest(msg.GetDigest())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	notFound := connect.NewError(connect.CodeNotFound, fmt.Errorf("blob %s is not found or not complete", d))
	readable, err := s.readableBlobs(ctx, ws, []blob.Digest{d})
	if err != nil {
		return nil, err
	}
	if !readable[d] {
		return nil, notFound
	}
	key := s.blobKey(ws)
	info, err := st.Stat(ctx, key, d)
	if errors.Is(err, blob.ErrNotFound) {
		return nil, notFound
	}
	if err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	index := int(msg.GetIndex())
	if index >= blob.ChunkCount(info.Manifest.Size) {
		return nil, connect.NewError(connect.CodeOutOfRange,
			fmt.Errorf("chunk %d is past the last chunk of %s, which has %d", index, d, blob.ChunkCount(info.Manifest.Size)))
	}
	r, err := st.Open(ctx, key, d)
	if errors.Is(err, blob.ErrNotFound) {
		return nil, notFound
	}
	if err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	defer r.Close()
	if _, err := r.Seek(int64(index)*blob.ChunkSize, io.SeekStart); err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	data := make([]byte, blob.ChunkLen(info.Manifest.Size, index))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, s.blobStoreError(ctx, err)
	}
	return connect.NewResponse(&syncplanev1.GetChunkResponse{Data: data}), nil
}

// readableBlobs returns the digests of want that the caller of ctx reads in
// the workspace: those a live row it reads names in a BlobRef field once the
// fields a guard hides from it are cleared.
//
// It authorizes as View does, through the same gate and the same row and
// field filters, so a blob is readable exactly when View would return a row
// that names it. Unlike View it leaves out server-only entities unless the
// call is trusted, since no client can receive their rows.
func (s *Server) readableBlobs(ctx context.Context, workspaceID string, want []blob.Digest) (found map[blob.Digest]bool, err error) {
	enforced := !isTrusted(ctx)
	var adm admission
	if enforced {
		if adm, err = s.enterWorkspaceServed(ctx, workspaceID, true); err != nil {
			return nil, err
		}
		defer func() {
			if gerr := adm.verdict.refusal(ctx, err); gerr != nil {
				found, err = nil, gerr
			}
		}()
	}
	if len(want) == 0 {
		return map[blob.Digest]bool{}, nil
	}
	visible, project, _, bind, err := s.readGate(ctx, workspaceID, adm, enforced)
	if err != nil {
		return nil, err
	}
	admit := func(info *entityInfo, row storage.Row, msg proto.Message) (bool, error) {
		if visible != nil {
			ok, err := visible(row)
			if err != nil || !ok {
				return false, err
			}
		}
		return true, project.apply(info, row.Key, msg)
	}
	err = s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		if err := bind(tx); err != nil {
			return err
		}
		var err error
		// No client receives a server-only row, so none reads a blob through
		// one, whatever the entity's read rules admit.
		found, err = s.referencedBlobs(tx, want, admit, !enforced)
		return err
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}
