package server

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Bootstrap streams a full workspace snapshot: Begin, Chunk*, End. The
// snapshot is paged: the Begin cursor comes from one short read tx, then
// each page of rows is read in its own short read tx, so a large workspace
// never pins a database snapshot — and never holds back whatever reclamation
// that blocks in the store — for the duration of the stream.
//
// Paging means the stream is NOT one consistent snapshot — and that is
// fine. Rows only ever gain versions, snapshots never ship tombstones, and
// the client follows Bootstrap with a Pull from the Begin cursor: anything
// written after Begin was issued is re-delivered (or tombstoned) by that
// pull, overwriting whatever mixture of pages the bootstrap saw. A row
// deleted mid-bootstrap either vanishes from a later page (then the pull's
// tombstone is a no-op) or ships in an earlier page (then the pull's
// tombstone removes it).
//
// Rows go out entity by entity, in the order Begin announces — priority
// entities, then the subscription set, then the rest (see bootstrapEntities).
// A cold client applies chunks as they arrive, so that order is what it paints
// first; the announced list is also how it tells an entity still queued from
// one that shipped no rows at all.
//
// A request carrying `resume` continues an interrupted snapshot instead of
// starting a new one: entities that already shipped in full are skipped, the
// one that was mid-scan restarts after its last delivered key, and the Begin
// cursor is re-issued unchanged so the healing pull still covers the whole
// span. Begin announces the full entity order either way — by the end of a
// resumed stream every entity on it is complete in the client's staging,
// whichever attempt delivered it. See bootstrapresume.go.
//
// With subscriptions, only subscribed entities are scanned and only rows
// matching their predicates ship; read-filtered rows are skipped the same
// way, and a windowed subscription ships exactly its window's members. The
// Begin cursor records the subscription set and filter epoch so a later change
// of either is detected by pull, which then delta-resyncs the visible-set
// difference (or resets when it cannot); it also records the window and
// closure key-sets, so the first pull can diff against what the snapshot
// actually shipped.
func (s *Server) Bootstrap(ctx context.Context, req *connect.Request[syncplanev1.BootstrapRequest], stream *connect.ServerStream[syncplanev1.BootstrapResponse]) error {
	msg := req.Msg
	if err := naming.ValidateWorkspaceID(msg.GetWorkspaceId()); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	principal, err := s.enterWorkspace(ctx, msg.GetWorkspaceId())
	if err != nil {
		return err
	}
	wantHash, err := subscriptionHash(msg.GetSubscriptions())
	if err != nil {
		return err
	}
	emitter, err := newRowEmitter(ctx, s, msg.GetWorkspaceId(), msg.GetSubscriptions())
	if err != nil {
		return err
	}
	epoch, err := s.perms.FilterEpoch(ctx, principal, msg.GetWorkspaceId())
	if err != nil {
		return err
	}
	// A resume the scope no longer matches is dropped here rather than served:
	// what survives is a snapshot this request could actually continue (see
	// bootstrapresume.go). Whether the cursor is still servable needs the
	// workspace position, so that half waits for the tx below.
	resume, err := s.decodeBootstrapResume(ctx, msg.GetWorkspaceId(), msg.GetResume())
	if err != nil {
		return err
	}
	if resume != nil && !resume.acceptable(wantHash, epoch) {
		resume = nil
	}

	// The order rows go out in, announced on Begin so the client can tell an
	// entity that has not started from one that shipped nothing (see
	// bootstrapEntities).
	order := emitter.bootstrapEntities(msg.GetSubscriptions(), msg.GetPriorityEntities())

	// Short tx #1: pin the Begin cursor (and the client's ack watermark), and
	// compute the bounded membership sets — windows and the reference-following
	// closure — so the snapshot scan below ships exactly their members and the
	// Begin cursor carries both key-sets.
	begin := &syncplanev1.BootstrapResponse_Begin{
		SchemaRevision: s.catalogPtr().schemaRev,
		Entities:       order,
	}
	if err := s.store.WithReadTx(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
		current, err := tx.CurrentSeq()
		if err != nil {
			return err
		}
		// Stamp the store epoch so the next pull's provenance guard matches;
		// without it a freshly bootstrapped cursor would look foreign and
		// reset-loop.
		storeEpoch, err := tx.StoreEpoch()
		if err != nil {
			return err
		}
		emitter.windowMembers, err = emitter.buildWindows(tx)
		if err != nil {
			return err
		}
		var closureSet *syncplanecursorv1.ClosureSet
		if emitter.followEntities != nil {
			emitter.closure, err = emitter.buildClosure(tx, msg.GetSubscriptions())
			if err != nil {
				return err
			}
			closureSet = emitter.closure.keySet()
		}
		// A resumed snapshot keeps the cursor it began with: that cursor is what
		// the client's follow-up pull reconciles against, and re-minting it here
		// would drop everything written between the two attempts. The membership
		// sets above are still rebuilt against current data — they decide which
		// rows the REMAINING scan ships — and where they have moved since, the
		// pull's diff against the sets the cursor records is what settles it.
		if resume != nil {
			horizon, err := tx.TombstoneHorizon()
			if err != nil {
				return err
			}
			if resume.servable(horizon, current, storeEpoch) {
				begin.Cursor = resume.rawCursor
				begin.Seq = resume.cur.seq
				begin.Resumed = true
			} else {
				resume = nil
			}
		}
		if resume == nil {
			st := cursorState{
				seq:        current,
				subHash:    wantHash,
				epoch:      epoch,
				storeEpoch: storeEpoch,
				subs:       msg.GetSubscriptions(),
				closure:    closureSet,
				windows:    emitter.windowStates(),
			}
			begin.Cursor = s.encodeCursor(ctx, msg.GetWorkspaceId(), &st)
			begin.Seq = current
		}
		if msg.GetClientId() != "" {
			acked, err := tx.ClientLastMutationID(msg.GetClientId())
			if err != nil {
				return err
			}
			begin.LastAckedMutationId = acked
		}
		return nil
	}); err != nil {
		return err
	}
	if err := stream.Send(&syncplanev1.BootstrapResponse{
		Body: &syncplanev1.BootstrapResponse_Begin_{Begin: begin},
	}); err != nil {
		return err
	}

	rowsSent := 0
	chunk := make([]*syncplanev1.RowChange, 0, s.chunkSize)
	// Where the scan has got to, sent with every chunk so an interrupted client
	// can hand it back and be served the rest (see bootstrapresume.go).
	var pos bootstrapPos
	pos.resumedFrom(resume)
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		err := stream.Send(&syncplanev1.BootstrapResponse{
			Body: &syncplanev1.BootstrapResponse_Chunk_{
				Chunk: &syncplanev1.BootstrapResponse_Chunk{Rows: chunk, ResumeToken: pos.token()},
			},
		})
		chunk = make([]*syncplanev1.RowChange, 0, s.chunkSize)
		return err
	}

	for _, entity := range order {
		// Delivered in full by an earlier attempt of this same snapshot.
		if resume != nil && resume.done[entity] {
			continue
		}
		afterKey := ""
		if resume != nil && entity == resume.entity {
			afterKey = resume.afterKey
		}
		for {
			// Short tx per page: collect raw rows only; classification and
			// stream writes happen outside so the tx stays brief.
			var page []storage.Row
			if err := s.store.WithReadTx(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
				return tx.ScanEntityAfter(entity, afterKey, s.chunkSize, func(row storage.Row) error {
					page = append(page, row)
					return nil
				})
			}); err != nil {
				return err
			}
			for _, row := range page {
				class, err := emitter.classify(row)
				if err != nil {
					return err
				}
				if class != rowVisible {
					continue // snapshots never need tombstones
				}
				chunk = append(chunk, rowChange(row))
				rowsSent++
				pos.row(entity, row.Key)
				if len(chunk) >= s.chunkSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			// ScanEntityAfter excludes tombstones before applying the limit
			// (pinned by the conformance suite), so a short page proves the
			// entity is exhausted.
			if len(page) < s.chunkSize {
				break
			}
			afterKey = page[len(page)-1].Key
		}
		pos.entityDone(entity)
	}
	if err := flush(); err != nil {
		return err
	}
	s.reqLog(ctx, "bootstrap", msg.GetWorkspaceId(), msg.GetClientId()).LogAttrs(ctx, slog.LevelDebug,
		"bootstrap snapshot streamed", slog.Int("rows", rowsSent), slog.Uint64("seq", begin.Seq),
		slog.Bool("resumed", begin.GetResumed()))
	return stream.Send(&syncplanev1.BootstrapResponse{
		Body: &syncplanev1.BootstrapResponse_End_{End: &syncplanev1.BootstrapResponse_End{}},
	})
}
