package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Blob GC.
//
// A blob is kept while a live row names it. Once none does, a pass marks it
// with the time it found it unreferenced (blob.Info.UnreferencedSince), and a
// later pass deletes it when the mark is older than the retention. A pass
// that finds it referenced again removes the mark. The mark is what lets the
// retention run across passes without a table of its own, and what gives a
// client that writes the row after the upload time to do so.
//
// The reference check reads no access rule: a row any caller can see keeps
// the blob for every caller, and so does a row of a server-only entity.

// blobGCBatch is how many digests one reference check covers: one read
// transaction, or one write transaction for the deletes.
const blobGCBatch = 256

// collectAllBlobs runs blob GC for every workspace the blob store holds
// something for, each under its lease. A failing workspace does not stop the
// pass; the errors come back joined.
func (s *Server) collectAllBlobs(ctx context.Context) error {
	var ids []string
	for key, err := range s.blobs.ListWorkspaces(ctx) {
		if err != nil {
			return fmt.Errorf("blob store: list workspaces: %w", err)
		}
		// A store shared by a Manager's apps lists every app's workspaces;
		// this server collects its own share alone.
		if id, ok := strings.CutPrefix(key, s.wsPrefix); ok {
			ids = append(ids, id)
		}
	}
	var errs []error
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		release, run := s.tryLease(ctx, leaseRoleBlobs, id)
		if !run {
			continue
		}
		err := s.collectBlobs(ctx, id)
		release()
		if err != nil {
			errs = append(errs, fmt.Errorf("workspace %s: blobs: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// collectBlobs runs blob GC for one workspace: it removes every blob of a
// deleted workspace, aborts the staged uploads older than the retention,
// and marks, unmarks and deletes committed blobs as the file comment
// describes.
func (s *Server) collectBlobs(ctx context.Context, workspaceID string) error {
	key := s.blobKey(workspaceID)
	deleted, err := s.store.WorkspaceDeleted(ctx, workspaceID)
	if err != nil {
		return err
	}
	if deleted {
		// DeleteWorkspace removes them itself; this catches what it could not,
		// and what an upload admitted before the deletion committed after it.
		return s.blobs.DeleteWorkspace(ctx, key)
	}
	now := time.Now()
	cutoff := now.Add(-s.blobRetentionOrDefault())

	var stale []blob.Digest
	for up, err := range s.blobs.ListStaged(ctx, key) {
		if err != nil {
			return err
		}
		if !up.StartedAt.After(cutoff) {
			stale = append(stale, up.Manifest.Digest)
		}
	}
	for _, d := range stale {
		if err := s.blobs.Abort(ctx, key, d); err != nil {
			return err
		}
	}

	var digests []blob.Digest
	for d, err := range s.blobs.List(ctx, key) {
		if err != nil {
			return err
		}
		digests = append(digests, d)
	}
	var expired []blob.Digest
	marked := 0
	for start := 0; start < len(digests); start += blobGCBatch {
		batch := digests[start:min(start+blobGCBatch, len(digests))]
		var referenced map[blob.Digest]bool
		if err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
			var err error
			referenced, err = s.referencedBlobs(tx, batch, nil, true)
			return err
		}); err != nil {
			return err
		}
		for _, d := range batch {
			info, err := s.blobs.Stat(ctx, key, d)
			if errors.Is(err, blob.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			mark := info.UnreferencedSince
			switch {
			case referenced[d]:
				if !mark.IsZero() {
					err = s.blobs.SetUnreferencedSince(ctx, key, d, time.Time{})
				}
			case mark.IsZero():
				err = s.blobs.SetUnreferencedSince(ctx, key, d, now)
				marked++
			case !mark.After(cutoff):
				expired = append(expired, d)
			}
			if err != nil && !errors.Is(err, blob.ErrNotFound) {
				return err
			}
		}
	}
	removed, err := s.deleteExpiredBlobs(ctx, workspaceID, expired, cutoff)
	for _, c := range []struct {
		kind string
		n    int
	}{
		{"blob_uploads", len(stale)},
		{"blobs", removed},
	} {
		if c.n > 0 {
			s.tel().gcCollected.Add(ctx, int64(c.n), metric.WithAttributes(telemetry.AttrGCKind.String(c.kind)))
		}
	}
	s.log().LogAttrs(ctx, slog.LevelDebug, "collected blobs",
		slog.String("component", "gc"),
		slog.String("workspace", workspaceID),
		slog.Int("uploads_aborted", len(stale)),
		slog.Int("blobs_marked", marked),
		slog.Int("blobs_removed", removed),
	)
	return err
}

// deleteExpiredBlobs deletes each blob of digests that no live row names and
// whose mark is still at or before cutoff, and returns how many it deleted.
// It deletes nothing in a workspace that holds a live row of an entity the
// catalog does not know, because such a row can name any blob.
//
// The checks read a snapshot; the deletes happen in a write transaction that
// first proves no write landed since (see deleteUnchanged). The checks can
// read every row of the workspace, so they must not hold the write exclusion.
func (s *Server) deleteExpiredBlobs(ctx context.Context, workspaceID string, digests []blob.Digest, cutoff time.Time) (int, error) {
	removed := 0
	for start := 0; start < len(digests); start += blobGCBatch {
		batch := digests[start:min(start+blobGCBatch, len(digests))]
		var (
			seq        uint64
			unknown    string
			unreferred []blob.Digest
		)
		err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
			var err error
			if seq, err = tx.CurrentSeq(); err != nil {
				return err
			}
			if unknown, err = s.uncataloguedEntity(tx); err != nil || unknown != "" {
				return err
			}
			referenced, err := s.referencedBlobs(tx, batch, nil, true)
			if err != nil {
				return err
			}
			for _, d := range batch {
				if !referenced[d] {
					unreferred = append(unreferred, d)
				}
			}
			return nil
		})
		if err != nil {
			return removed, err
		}
		if unknown != "" {
			s.log().LogAttrs(ctx, slog.LevelWarn, "blob GC deletes nothing in a workspace that holds rows of an entity the schema does not declare",
				slog.String("component", "gc"),
				slog.String("workspace", workspaceID),
				slog.String("entity", unknown))
			return removed, nil
		}
		n, err := s.deleteUnchanged(ctx, workspaceID, seq, unreferred, cutoff)
		removed += n
		if err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// uncataloguedEntity returns the name of an entity outside the catalog that
// has a live row in the workspace, or "" when there is none. The engine's own
// entities do not count: none of them holds a blob reference.
//
// It reads every row of the workspace, tombstones included, since the storage
// API names no entity it is not asked about. It stops at the first such row.
func (s *Server) uncataloguedEntity(tx storage.ReadTx) (string, error) {
	cat := s.catalogPtr()
	var found string
	err := tx.ScanChanges(0, "", "", 0, func(row storage.Row) error {
		if row.Deleted || naming.IsReservedEntityName(row.Entity) {
			return nil
		}
		if _, ok := cat.entities[row.Entity]; ok {
			return nil
		}
		found = row.Entity
		return errBlobsSettled
	})
	if errors.Is(err, errBlobsSettled) {
		err = nil
	}
	return found, err
}

// deleteUnchanged deletes each blob of digests whose mark is still at or
// before cutoff, when the workspace is still at sequence seq, and returns how
// many it deleted. The caller found at seq that no live row names them.
//
// Every row write moves the sequence, so an unchanged sequence means the
// reference check still holds. The mark is re-read inside the write
// exclusion, the same exclusion reviveBlob clears a mark in: a BeginUpload
// that cleared it first leaves a zero mark here, and one that comes after
// finds the blob gone and stages a new upload.
func (s *Server) deleteUnchanged(ctx context.Context, workspaceID string, seq uint64, digests []blob.Digest, cutoff time.Time) (int, error) {
	if len(digests) == 0 {
		return 0, nil
	}
	key := s.blobKey(workspaceID)
	n := 0
	err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		n = 0
		now, err := tx.CurrentSeq()
		if err != nil || now != seq {
			// A write landed after the check; the next pass checks again.
			return err
		}
		for _, d := range digests {
			info, err := s.blobs.Stat(ctx, key, d)
			if errors.Is(err, blob.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if info.UnreferencedSince.IsZero() || info.UnreferencedSince.After(cutoff) {
				continue
			}
			if err := s.blobs.Delete(ctx, key, d); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}
