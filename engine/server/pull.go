package server

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
)

// cursorState is the decoded pull position: the sequence watermark plus the
// subscription set and filter epoch the cursor was issued under. Legacy
// cursors (bare seq) decode with zero hash/epoch, which match a full-sync
// client of an epoch-less app.
type cursorState struct {
	seq     uint64
	subHash uint64
	epoch   uint64
	// storeEpoch pins the cursor to the store lineage that issued it (see
	// storage.ReadTx.StoreEpoch). 0 on legacy/pre-write cursors; the server
	// stamps the workspace's current epoch onto every cursor it issues.
	storeEpoch uint64
	// partialEntity/partialKey mark a sub-position inside a same-version group
	// when a pull page split one: seq names the split version and these name
	// the last row delivered within it, in (entity, key) order. Empty on a
	// clean version boundary. See ScanChanges' contract.
	partialEntity string
	partialKey    string
	// subs is the subscription set subHash hashes. It rides inside the
	// (opaque) cursor so a later scope change can reconstruct the client's
	// old visibility and resync only the difference. Empty on cursors from
	// older servers and for full-sync clients (subHash 0).
	subs []*syncplanev1.Subscription
	// resync is non-nil while a delta resync is paging; see ResyncState.
	resync *syncplanecursorv1.ResyncState
	// closure is the reference-following closure key-set the cursor was issued
	// under (Cursor.closure): the exact set of rows the client holds via follow
	// beyond its base scope. The next following pull rebuilds the closure and
	// diffs it against this set. nil on legacy cursors and non-following
	// subscriptions ⇒ empty ⇒ behaves as today.
	closure *syncplanecursorv1.ClosureSet
	// window is the same thing for windowed subscriptions (Cursor.window): the
	// exact rows the client holds via Subscription.window. nil on legacy
	// cursors and non-windowed subscriptions.
	window *syncplanecursorv1.ClosureSet
}

func decodeCursor(raw []byte) (cursorState, error) {
	if len(raw) == 0 {
		return cursorState{}, nil
	}
	var c syncplanecursorv1.Cursor
	if err := proto.Unmarshal(raw, &c); err != nil {
		return cursorState{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("bad cursor: %w", err))
	}
	return cursorState{
		seq:           c.GetSeq(),
		subHash:       c.GetSubHash(),
		epoch:         c.GetFilterEpoch(),
		storeEpoch:    c.GetStoreEpoch(),
		partialEntity: c.GetPartialEntity(),
		partialKey:    c.GetPartialKey(),
		subs:          c.GetSubscriptions(),
		resync:        c.GetResync(),
		closure:       c.GetClosure(),
		window:        c.GetWindow(),
	}, nil
}

func encodeCursor(st cursorState) []byte {
	raw, err := proto.Marshal(&syncplanecursorv1.Cursor{
		Seq:           st.seq,
		SubHash:       st.subHash,
		FilterEpoch:   st.epoch,
		StoreEpoch:    st.storeEpoch,
		PartialEntity: st.partialEntity,
		PartialKey:    st.partialKey,
		Subscriptions: st.subs,
		Resync:        st.resync,
		Closure:       st.closure,
		Window:        st.window,
	})
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	return raw
}

// resetReason names why a pull forces the client to re-bootstrap, so the log
// line can choose a level: a provenance mismatch is suspicious (likely a
// swapped/restored database) and warns; a horizon miss is the expected fate of
// a long-offline client; a resync fallback is a routine scope-change edge.
type resetReason string

const (
	resetNone              resetReason = ""
	resetHorizon           resetReason = "cursor_below_horizon"
	resetProvenance        resetReason = "store_provenance_mismatch"
	resetStaleResync       resetReason = "resync_invalidated_by_scope_change"
	resetResyncUnavailable resetReason = "old_visibility_unreconstructable"
)

// logReset emits the re-bootstrap event at the level its reason warrants.
func logReset(ctx context.Context, logger *slog.Logger, reason resetReason) {
	if reason == resetNone {
		return
	}
	level := slog.LevelInfo
	switch reason {
	case resetProvenance:
		level = slog.LevelWarn
	case resetStaleResync, resetResyncUnavailable:
		level = slog.LevelDebug
	}
	logger.LogAttrs(ctx, level, "client reset; re-bootstrap required", slog.String("reason", string(reason)))
}

func (s *Server) Pull(ctx context.Context, req *connect.Request[syncplanev1.PullRequest]) (*connect.Response[syncplanev1.PullResponse], error) {
	msg := req.Msg
	if err := naming.ValidateWorkspaceID(msg.GetWorkspaceId()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	principal, err := s.enterWorkspace(ctx, msg.GetWorkspaceId())
	if err != nil {
		return nil, err
	}
	cur, err := decodeCursor(msg.GetCursor())
	if err != nil {
		return nil, err
	}
	wantHash, err := subscriptionHash(msg.GetSubscriptions())
	if err != nil {
		return nil, err
	}
	emitter, err := newRowEmitter(ctx, s, msg.GetWorkspaceId(), msg.GetSubscriptions())
	if err != nil {
		return nil, err
	}
	emitter.noDeltaEntities = noDeltaEntitySet(msg.GetNoDeltaEntities())

	// Resolve every Permissions hook OUTSIDE the read snapshot, mirroring
	// bootstrap.go: FilterEpoch up front, then warm the emitter's per-entity
	// read filters (memoized via filterOf) so classify() inside the scan is a
	// pure cache hit. A slow ReadFilter/FilterEpoch therefore never pins the
	// snapshot — only the pure predicate application runs inside WithReadTx.
	epoch, err := s.perms.FilterEpoch(ctx, principal, msg.GetWorkspaceId())
	if err != nil {
		return nil, err
	}
	for _, e := range emitter.entities() {
		if _, err := emitter.filterOf(e); err != nil {
			return nil, err
		}
	}

	// Classify the pull path from the cursor alone (no tx needed) and, when a
	// delta resync applies, reconstruct the cursor's old visibility now — that
	// resolution calls ReadFilter/ReadFilterAt (resync.go), which must also run
	// outside the snapshot. The scan/resync dispatch still happens inside the
	// tx, but by then every filter is memoized. staleResync and !resyncOK both
	// fall through to a reset inside the tx (after LastAckedMutationId is set).
	var (
		oldEm       *rowEmitter
		resyncOK    bool
		doResync    bool
		dropEnts    []string
		staleResync bool
	)
	switch {
	case cur.resync != nil:
		// Mid-resync: a FURTHER scope change invalidates the diff in flight.
		if cur.resync.GetToSubHash() != wantHash || cur.resync.GetToFilterEpoch() != epoch {
			staleResync = true
		} else {
			oldEm, resyncOK, err = s.oldVisibility(ctx, msg.GetWorkspaceId(), cur, epoch, emitter)
			if err != nil {
				return nil, err
			}
			doResync = true
		}
	case len(msg.GetCursor()) > 0 && (cur.subHash != wantHash || cur.epoch != epoch):
		oldEm, resyncOK, err = s.oldVisibility(ctx, msg.GetWorkspaceId(), cur, epoch, emitter)
		if err != nil {
			return nil, err
		}
		if resyncOK {
			// Entities that left the subscription set drop wholesale, once, on
			// the first resync page; the scan covers the rest.
			dropEnts = s.leavingEntities(oldEm, emitter)
		}
		doResync = true
	}

	limit := s.limits.MaxPullChanges
	if req := int(msg.GetLimit()); req > 0 && req < limit {
		limit = req
	}

	var resp *syncplanev1.PullResponse
	var resetSeen resetReason // logged after the tx commits, never inside it
	err = s.store.WithReadTx(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
		// A delta resync (or a stale one) takes precedence over the normal
		// forward page, but still runs the shared reset checks first: a cursor
		// below the horizon or from a foreign store resets rather than resyncs.
		// An empty cursor never resyncs: it decodes to seq 0 with no prior
		// scope to diff against, so it either replays the workspace from the
		// beginning or resets on the horizon (see pullPreamble). A non-empty
		// cursor whose subscription set or filter epoch changed enters a delta
		// resync (classified above), falling back to reset when the old scope
		// cannot be reconstructed. Mid-resync the client's rows are a mixture of
		// old and new scope; a FURTHER change invalidates the diff in flight,
		// and only a re-bootstrap recovers.
		if staleResync || doResync {
			resp = &syncplanev1.PullResponse{Patch: &syncplanev1.Patch{}}
			_, _, doReset, err := s.pullPreamble(tx, cur, msg.GetClientId(), msg.GetCursor(), resp)
			if err != nil {
				return err
			}
			if doReset != resetNone || staleResync || !resyncOK {
				resp.Reset_ = true
				resp.NextCursor = msg.GetCursor()
				switch {
				case doReset != resetNone:
					resetSeen = doReset
				case staleResync:
					resetSeen = resetStaleResync
				default:
					resetSeen = resetResyncUnavailable
				}
				return nil
			}
			if cur.resync != nil {
				pos := resyncPos{entity: cur.resync.GetEntity(), afterKey: cur.resync.GetAfterKey()}
				return s.resyncStep(tx, cur, oldEm, emitter, wantHash, epoch, msg.GetSubscriptions(), pos, limit, resp)
			}
			resp.Patch.DropEntities = dropEnts
			return s.resyncStep(tx, cur, oldEm, emitter, wantHash, epoch, msg.GetSubscriptions(), resyncPos{}, limit, resp)
		}
		page, _, reason, err := s.pullPage(tx, cur, emitter, wantHash, epoch, msg.GetSubscriptions(), msg.GetClientId(), msg.GetCursor(), limit)
		if err != nil {
			return err
		}
		resetSeen = reason
		resp = page
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Advertised on every pull regardless of path (normal, resync, reset) so a
	// client learns of skew even when the response carries no rows.
	resp.SchemaRevision = s.catalogPtr().schemaRev
	plog := s.reqLog(ctx, "pull", msg.GetWorkspaceId(), msg.GetClientId())
	logReset(ctx, plog, resetSeen)
	// A reset is the expensive outcome — the client throws its store away and
	// re-downloads the workspace — and it is invisible at the RPC layer, which
	// sees an ordinary successful pull. cursor_below_horizon in particular says
	// GC retention is shorter than how long clients actually stay offline, so
	// it is the number that tunes WithGC.
	if resetSeen != resetNone {
		s.tel().pullResets.Add(ctx, 1, metric.WithAttributes(
			telemetry.AttrResetReason.String(string(resetSeen))))
	}
	s.tel().pullChanges.Record(ctx, int64(len(resp.GetPatch().GetChanges())))
	plog.LogAttrs(ctx, slog.LevelDebug, "pull served",
		slog.Int("changes", len(resp.GetPatch().GetChanges())),
		slog.Bool("has_more", resp.GetHasMore()),
		slog.Bool("reset", resp.GetReset_()),
		slog.Uint64("seq", resp.GetSeq()),
	)
	return connect.NewResponse(resp), nil
}

// pullPreamble reads the workspace position, stamps last_acked onto resp, and
// runs the horizon + store-epoch reset checks shared by every pull path. reset
// is true when the cursor cannot be served incrementally and the caller must
// reply reset=true. current and storeEpoch are the live workspace position
// (meaningful only when reset is false).
func (s *Server) pullPreamble(tx storage.ReadTx, cur cursorState, clientID string, rawCursor []byte, resp *syncplanev1.PullResponse) (current, storeEpoch uint64, reset resetReason, err error) {
	horizon, err := tx.TombstoneHorizon()
	if err != nil {
		return 0, 0, resetNone, err
	}
	current, err = tx.CurrentSeq()
	if err != nil {
		return 0, 0, resetNone, err
	}
	storeEpoch, err = tx.StoreEpoch()
	if err != nil {
		return 0, 0, resetNone, err
	}
	if clientID != "" {
		acked, err := tx.ClientLastMutationID(clientID)
		if err != nil {
			return 0, 0, resetNone, err
		}
		resp.LastAckedMutationId = acked
	}
	// A cursor older than the tombstone horizon may have missed
	// garbage-collected deletions; only a re-bootstrap recovers those.
	//
	// The empty cursor decodes to seq 0 and is judged by the same rule, which
	// bounds the "pull from the beginning" path: it serves a whole workspace
	// only until that workspace's first tombstone is collected, and resets
	// forever after. That is deliberate rather than an oversight — a stateless
	// client could in principle be replayed from 0, but the page it gets back
	// resumes from a cursor still below the horizon, so the replay could not
	// survive its own pagination without a second staleness rule tracking how
	// far GC advanced mid-replay. Bootstrap is the entry point for a client
	// with no state; replay-from-0 is a convenience for young and GC-less
	// workspaces (and for tests), not a substitute for it.
	if cur.seq < horizon {
		return current, storeEpoch, resetHorizon, nil
	}
	// Cursor provenance: a non-empty cursor that this store could not have
	// issued means the URL now serves a different database (swapped,
	// recreated empty, or restored from backup). Two impossible-by-
	// construction signals, either of which forces a re-bootstrap instead
	// of a silent splice or a regressed cursor that wedges the client:
	//
	//  1. seq ahead of CurrentSeq — sequences are per-workspace monotonic,
	//     so no pull this store served ever returned a higher seq. Catches
	//     the empty/recreated and restore-from-backup cases, where the new
	//     store's seq sits below the client's cursor.
	//  2. store epoch mismatch — covers the case the seq check cannot: a
	//     DIFFERENT lineage whose seq happens to sit at or above the
	//     client's, where scanning from cur.seq would graft the foreign
	//     store's rows onto the client's.
	//
	// A legacy/pre-epoch cursor carries storeEpoch 0; it is accepted and
	// stamped with the live epoch on the way out (every issued cursor
	// below sets next.storeEpoch), so existing clients are not reset en
	// masse on upgrade — they simply gain epoch protection from their next
	// pull onward. The seq check still guards their empty/restore cases in
	// the meantime.
	if len(rawCursor) > 0 && (cur.seq > current || (cur.storeEpoch != 0 && cur.storeEpoch != storeEpoch)) {
		return current, storeEpoch, resetProvenance, nil
	}
	return current, storeEpoch, resetNone, nil
}

// sameFollowSpecs reports whether two subscription sets carry the same follow
// specs, position by position. Only the follow half is compared, because it is
// the only half sub_hash leaves open — and the caller has already matched that.
//
// Unequal means "cannot tell", not only "differs": a cursor issued before the
// server recorded subscription sets carries none, and a client that reorders an
// otherwise identical set hashes the same but lines up differently. Both cost
// one rebuild, and the cursor that pull issues records the set as sent, so the
// next pull can skip.
func sameFollowSpecs(a, b []*syncplanev1.Subscription) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i].GetFollow(), b[i].GetFollow()) {
			return false
		}
	}
	return true
}

// pullPage serves one forward pull page from cur under a read snapshot: the
// shared reset checks + last_acked (pullPreamble), the closure rebuild, the
// ScanChanges + classify pass, the closure diff at a clean version boundary,
// and the next cursor. It does NOT handle delta resync — callers dispatch that
// before invoking pullPage. The returned cursorState is the position the page
// advanced to (meaningful only when the response is neither reset nor closes
// the stream early); Subscribe carries it forward across notifications.
func (s *Server) pullPage(tx storage.ReadTx, cur cursorState, emitter *rowEmitter, wantHash, epoch uint64, subs []*syncplanev1.Subscription, clientID string, rawCursor []byte, limit int) (*syncplanev1.PullResponse, cursorState, resetReason, error) {
	resp := &syncplanev1.PullResponse{Patch: &syncplanev1.Patch{}}
	current, storeEpoch, reset, err := s.pullPreamble(tx, cur, clientID, rawCursor, resp)
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	if reset != resetNone {
		resp.Reset_ = true
		resp.NextCursor = rawCursor
		return resp, cur, reset, nil
	}
	// An idle poll must not pay for a rebuild. Both bounded membership sets —
	// the window and the follow closure — are functions of the workspace's rows
	// and this request's read filters, and neither can have moved since the
	// cursor was issued: it already sits at the current sequence, so no row has
	// been written, and a visibility change would have bumped FilterEpoch and
	// diverted this pull into a resync before it reached here. So the sets the
	// cursor carries are the sets a rebuild would produce, and the scan below
	// has nothing to deliver either — every row is at or below cur.seq.
	//
	// This is the case a FOLLOWING subscription pays most often. The closure is
	// recomputed statelessly on every following pull (there is no row history to
	// diff against), so without this a polling client on a quiet workspace paid
	// a full seed pass plus a graph walk, once per poll interval, to be told
	// nothing had happened.
	//
	// Two things the sequence alone does not settle:
	//
	//   - A sub-position inside a same-version group means rows AT cur.seq are
	//     still undelivered, so the page is not empty and classify below still
	//     needs both membership sets.
	//   - The follow spec is excluded from sub_hash on purpose (subhash.go), so
	//     a cursor at the head can arrive carrying a DIFFERENT spec, which only
	//     the closure diff can reconcile. Everything else about the subscription
	//     set is already settled — an unequal sub_hash resyncs before pullPage.
	if cur.seq == current && cur.partialEntity == "" && sameFollowSpecs(cur.subs, subs) {
		// Carried forward as issued, restamped with what this request resolved:
		// a legacy cursor picks up the store epoch and its subscription set here
		// exactly as it would on the normal path below.
		next := cur
		next.subHash, next.epoch, next.storeEpoch, next.subs = wantHash, epoch, storeEpoch, subs
		resp.NextCursor = encodeCursor(next)
		resp.Seq = next.seq
		return resp, next, resetNone, nil
	}
	// Windowed subscriptions: compute each window's membership BEFORE the scan
	// so classify is window-aware, and before the closure below, which seeds
	// from window members. See window.go.
	emitter.windowMembers, err = emitter.buildWindows(tx)
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	// Reference-following: rebuild the closure against current rows BEFORE
	// the scan so classify is closure-aware (a changed follow-target must
	// not be tombstoned). The closure is populated only on this normal path
	// — never during resync, where it stays empty. See closure.go.
	if emitter.followEntities != nil {
		emitter.closure, err = emitter.buildClosure(tx, subs)
		if err != nil {
			return nil, cursorState{}, resetNone, err
		}
	}
	// Collaborative-text fields of rows written past the cursor ship as delta
	// state where the recipient's prior state is knowable (see emitChange). Set
	// per page, because a Subscribe stream reuses one emitter across pages at
	// different cursors, and cleared after the scan so the membership diffs
	// below — which emit rows AT or BELOW the cursor — stay full-state.
	emitter.deltas = true
	emitter.deltaAfter = cur.seq
	// Scan one row past the limit: seeing it proves truncation without
	// emitting it.
	scanned := 0
	var lastScanned storage.Row
	if err := tx.ScanChanges(cur.seq, cur.partialEntity, cur.partialKey, limit+1, func(row storage.Row) error {
		scanned++
		if scanned > limit {
			resp.HasMore = true
			return nil
		}
		// The cursor tracks the last SCANNED row, not the last emitted
		// one: filtered or unsubscribed rows must still advance it or the
		// client's pagination loop would never progress past them. Hidden
		// rows consume limit slots as tombstones, keeping pagination
		// monotone. The (version, entity, key) of this row is the resume
		// point if the page truncates inside a same-version group.
		lastScanned = row
		class, err := emitter.classify(row)
		if err != nil {
			return err
		}
		switch class {
		case rowVisible:
			change, err := emitter.emitChange(tx, row)
			if err != nil {
				return err
			}
			resp.Patch.Changes = append(resp.Patch.Changes, change)
		case rowHidden:
			resp.Patch.Changes = append(resp.Patch.Changes, hiddenChange(row))
		case rowUnsubscribed:
			// Skip: the client holds no state for this entity.
		}
		return nil
	}); err != nil {
		emitter.deltas = false
		return nil, cursorState{}, resetNone, err
	}
	emitter.deltas = false
	next := cursorState{subHash: wantHash, epoch: epoch, storeEpoch: storeEpoch, subs: subs}
	if resp.HasMore {
		// Resume just past the last delivered row. The (entity, key)
		// sub-position lets the next pull pick up the rest of a group
		// sharing this version instead of skipping it. The closure work is
		// deferred to a clean boundary, so carry the prior closure forward
		// unchanged (the incremental scan already covered version>seq
		// members above).
		next.seq = lastScanned.Version
		next.partialEntity = lastScanned.Entity
		next.partialKey = lastScanned.Key
		next.closure = cur.closure
		next.window = cur.window
	} else {
		next.seq = current
		// Clean boundary: reconcile both bounded membership sets. Each emits
		// the at-or-below-seq delta the incremental scan could not (members
		// that entered; members that left and nothing else holds), then stamps
		// its new key-set. See emitMembershipDiff.
		if emitter.followEntities != nil {
			if err := emitter.emitMembershipDiff(tx, cur.closure, emitter.closure, cur.seq, resp.Patch); err != nil {
				return nil, cursorState{}, resetNone, err
			}
			next.closure = emitter.closure.keySet()
		}
		if len(emitter.windows) > 0 {
			if err := emitter.emitMembershipDiff(tx, cur.window, emitter.windowMembers, cur.seq, resp.Patch); err != nil {
				return nil, cursorState{}, resetNone, err
			}
			next.window = emitter.windowMembers.keySet()
		}
	}
	resp.NextCursor = encodeCursor(next)
	resp.Seq = next.seq
	return resp, next, resetNone, nil
}
