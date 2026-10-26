package server

import (
	"context"
	"log/slog"
	"sort"

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
// With subscriptions, only subscribed entities are visited and only rows
// matching their predicates ship; read-filtered rows are skipped the same
// way, and a windowed subscription ships exactly its window's members.
//
// How an entity is visited is decided per entity, and the two ways produce the
// same rows in the same order:
//
//   - CANDIDATES, when the entity's visible rows are named by rules the server
//     can enumerate — a predicate an index serves or one pinning the whole row
//     key, a window's membership, a follow closure. The snapshot then reads
//     those rows and nothing else, so a partial scope costs what it ships (see
//     bootstrapCandidates).
//   - a SCAN of the entity, for everything else: full sync, a scope covering
//     the whole entity, a predicate neither an index nor a pinned key reaches,
//     or a candidate set too large to be worth narrowing.
//
// The resume position means the same thing on both — the last key shipped for
// the entity, in key order — so an attempt that narrowed candidates and one
// that scanned can follow each other without skipping or repeating a row.
//
// The
// Begin cursor records the subscription set and filter epoch so a later change
// of either is detected by pull, which then delta-resyncs the visible-set
// difference (or resets when it cannot); it also records the window and
// closure key-sets, so the first pull can diff against what the snapshot
// actually shipped.
func (s *Server) Bootstrap(ctx context.Context, req *connect.Request[syncplanev1.BootstrapRequest], stream *connect.ServerStream[syncplanev1.BootstrapResponse]) (err error) {
	msg := req.Msg
	if err := naming.ValidateWorkspaceID(msg.GetWorkspaceId()); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := checkClientID(msg.GetClientId()); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	adm, err := s.enterWorkspaceServed(ctx, msg.GetWorkspaceId(), false)
	if err != nil {
		return err
	}
	defer func() { err = adm.verdict.settle(ctx, err) }()
	principal := adm.principal
	wantHash, err := subscriptionHash(msg.GetSubscriptions())
	if err != nil {
		return err
	}
	emitter, err := newRowEmitter(ctx, s, msg.GetWorkspaceId(), msg.GetSubscriptions())
	if err != nil {
		return err
	}
	// callerAccessDeltas stays false, so the Begin cursor stores no
	// CallerAccess: BootstrapRequest has no caller_access_version, so nothing
	// says the client applies deltas. A stored summary that the client never
	// uses can evict a ReadAccess entry from a bounded cursor-state store,
	// and losing that entry resets a client. The first pull that names a
	// version stores the summary, so only an access change before that pull
	// costs the whole summary.
	emitter.admit(adm)
	epoch, err := s.perms.FilterEpoch(ctx, principal, msg.GetWorkspaceId())
	if err != nil {
		return err
	}
	// The windows, the closure and every snapshot page classify rows inside a
	// read transaction, so the filters they consult resolve here, after the
	// epoch they are stamped with, and no hook pins a snapshot.
	if err := emitter.warmFilters(); err != nil {
		return err
	}
	// A resume the scope no longer matches is dropped here rather than served:
	// what survives is a snapshot this request could actually continue (see
	// bootstrapresume.go). Whether the cursor is still servable needs the
	// workspace position, so that half waits for the tx below.
	resume, err := s.decodeBootstrapResume(ctx, msg.GetWorkspaceId(), msg.GetResume(), adm.verdict)
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
	// Anchored: the Begin cursor's seq becomes the client's resume point, so it
	// must not sit behind writes the client has already been acked — and the
	// anchor is at least any resume's own floor.
	if err := s.readTxAnchored(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
		// The whole snapshot is filtered under the access read here, with the
		// Begin position: every later page reads at or above it. First, so a
		// grant check the gate left to this transaction comes before any
		// other read.
		if err := emitter.bindAccess(tx); err != nil {
			return err
		}
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
		if emitter.caller != nil && emitter.caller.model.moving {
			// Every page classifies rows with the parents they have at or
			// after this point.
			if _, emitter.movesThrough, _, err = readAccessMoves(tx, ^uint64(0), 0); err != nil {
				return err
			}
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
		// would drop everything written between the two attempts. The remaining
		// scan therefore ships the members of the sets that cursor records, not
		// of the sets above: a row that is a member only of a rebuilt set is in
		// neither set the pull's diff compares if the membership moves again
		// before that pull, so nothing would ever remove it. Where the sets have
		// moved since, the pull's diff against the cursor's sets settles it.
		if resume != nil {
			horizon, err := tx.TombstoneHorizon()
			if err != nil {
				return err
			}
			// A snapshot is filtered by one access throughout, so one begun
			// under other grants cannot be continued. One behind more moves
			// than a pull reads would reset on the pull that follows it.
			movesFit, err := s.movesFit(tx, emitter, resume.cur)
			if err != nil {
				return err
			}
			sameAccess, err := emitter.accessUnchanged(resume.cur)
			if err != nil {
				return err
			}
			if sameAccess && resume.servable(horizon, current, storeEpoch) && movesFit {
				begin.Cursor = resume.rawCursor
				begin.Seq = resume.cur.seq
				begin.Resumed = true
				if len(emitter.windows) > 0 {
					emitter.windowMembers = closureRows{}
					for ck := range resume.cur.windowSet() {
						emitter.windowMembers[ck] = storage.Row{}
					}
				}
				if emitter.followEntities != nil {
					emitter.closure = closureSetRows(resume.cur.closure)
				}
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
			emitter.stampAccess(&st, cursorState{})
			begin.Cursor = s.encodeCursor(ctx, msg.GetWorkspaceId(), &st)
			begin.Seq = current
		}
		// Sent on a resumed snapshot too: an earlier attempt's Begin may not
		// have reached the client's store, and the CallerAccess may have
		// changed since. The resumed cursor then names an older one, which
		// only stops a later pull from sending a delta.
		begin.CallerAccess, _ = emitter.callerAccessUpdate(cursorState{}, false, current)
		if msg.GetClientId() != "" {
			acked, err := tx.ClientLastMutationID(watermarkKey(principal, msg.GetClientId()))
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
	// Where delivery has got to, sent with every chunk so an interrupted client
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

	pins := chainPins{through: begin.GetSeq()}
	// visibleOf renders the rows of a page the client may see, without the
	// fields it may not read. It runs inside the page's read transaction,
	// which is where the parent chains of the access model are read;
	// snapshots never need tombstones, so it drops every other row.
	visibleOf := func(tx storage.ReadTx, page []storage.Row) ([]*syncplanev1.RowChange, error) {
		emitter.readChainsFrom(tx)
		if err := s.pinChains(tx, emitter, begin.GetSeq(), &pins); err != nil {
			return nil, err
		}
		var out []*syncplanev1.RowChange
		for _, row := range page {
			class, err := emitter.classify(row)
			if err != nil {
				return nil, err
			}
			if class != rowVisible {
				continue
			}
			rc, err := emitter.rowChange(row)
			if err != nil {
				return nil, err
			}
			out = append(out, rc)
		}
		return out, nil
	}

	// ship buffers one visible row and records the position that a resumed
	// attempt continues from. Both delivery paths below go through it, which
	// is what keeps their output — and their resume tokens — indistinguishable.
	ship := func(entity string, rc *syncplanev1.RowChange) error {
		chunk = append(chunk, rc)
		rowsSent++
		pos.row(entity, rc.GetKey())
		if len(chunk) >= s.chunkSize {
			return flush()
		}
		return nil
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
		candidates, narrowed, err := s.bootstrapCandidates(ctx, msg.GetWorkspaceId(), begin.GetSeq(), emitter, entity)
		if err != nil {
			return err
		}
		if narrowed {
			// Only the part of the candidate set an earlier attempt did not
			// reach. Candidates are in key order and so is the position, so
			// everything at or below it has already been offered.
			candidates = candidates[sort.Search(len(candidates), func(i int) bool { return candidates[i] > afterKey }):]
			for start := 0; start < len(candidates); start += maxRowBatch {
				batch := candidates[start:min(start+maxRowBatch, len(candidates))]
				var (
					page    []storage.Row
					visible []*syncplanev1.RowChange
				)
				if err := s.readTx(ctx, msg.GetWorkspaceId(), begin.GetSeq(), func(tx storage.ReadTx) error {
					page = page[:0]
					if err := tx.GetRows(entity, batch, func(row storage.Row) error {
						// GetRows yields tombstones and skips keys with no row
						// at all. A snapshot ships neither, for the same reason
						// the scan below never sees one.
						if !row.Deleted {
							page = append(page, row)
						}
						return nil
					}); err != nil {
						return err
					}
					var err error
					visible, err = visibleOf(tx, page)
					return err
				}); err != nil {
					return err
				}
				for _, rc := range visible {
					if err := ship(entity, rc); err != nil {
						return err
					}
				}
			}
			pos.entityDone(entity)
			continue
		}
		for {
			// Short tx per page: stream writes happen outside so the tx stays
			// brief.
			var (
				page    []storage.Row
				visible []*syncplanev1.RowChange
			)
			// Floored at the Begin seq: pages of one snapshot must not read
			// from before the position the Begin frame promised.
			if err := s.readTx(ctx, msg.GetWorkspaceId(), begin.GetSeq(), func(tx storage.ReadTx) error {
				page = page[:0]
				if err := tx.ScanEntityAfter(entity, afterKey, s.chunkSize, func(row storage.Row) error {
					page = append(page, row)
					return nil
				}); err != nil {
					return err
				}
				var err error
				visible, err = visibleOf(tx, page)
				return err
			}); err != nil {
				return err
			}
			for _, rc := range visible {
				if err := ship(entity, rc); err != nil {
					return err
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

// bootstrapCandidates names the rows of one entity a snapshot may have to ship,
// in key order, or reports narrowed=false for an entity it must scan instead.
//
// The enumeration takes a read transaction of its own, at the snapshot's Begin
// floor like every page that follows it — and, like every page that follows it,
// it is not part of one pinned snapshot. A row that enters a predicate's match
// set after this ran is written with a version above the Begin seq, so the
// client's first pull from the Begin cursor delivers it; that is the same
// guarantee the scan path relies on for rows written mid-stream (see the
// Bootstrap doc comment).
//
// The caller's access is a second source of candidates. A row the access model
// hides is hidden whatever the scope says, closure and window members
// included, since they were filtered when built. So when both sources narrow,
// the candidates are the keys both name.
func (s *Server) bootstrapCandidates(ctx context.Context, ws string, beginSeq uint64, e *rowEmitter, entity string) (keys []string, narrowed bool, err error) {
	info, registered := s.catalogPtr().entities[entity]
	if !registered {
		return nil, false, nil
	}
	scope, scoped := bootstrapNarrowable(e, entity)
	if !scoped && !e.accessNarrows(info) {
		return nil, false, nil
	}
	err = s.readTx(ctx, ws, beginSeq, func(tx storage.ReadTx) error {
		accessKeys, byAccess, err := e.accessCandidates(tx, info, s.limits.MaxResyncScan)
		if err != nil {
			return err
		}
		if !scoped {
			keys, narrowed = accessKeys, byAccess
			return nil
		}
		keys, narrowed, err = candidateKeys(tx, info,
			[]*entityScope{scope},
			// A closure member and a window member are visible whatever the
			// base scope says, so both join the candidates. Unlike a resync,
			// which diffs with an empty closure, a snapshot resolves one and
			// can therefore treat it as the explicit key set it is.
			[]closureRows{e.closure, e.windowMembers},
			s.limits.MaxResyncScan, s.limits)
		if err != nil {
			return err
		}
		switch {
		case byAccess && narrowed:
			keys = intersectSorted(keys, accessKeys)
		case byAccess:
			keys, narrowed = accessKeys, true
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return keys, narrowed, nil
}

// intersectSorted returns the keys present in both sorted slices, sorted.
func intersectSorted(a, b []string) []string {
	out := make([]string, 0, min(len(a), len(b)))
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

// bootstrapNarrowable reports this entity's base scope when a snapshot's
// visible rows of it are exactly the predicate matches, closure members and
// window members candidateKeys collects. ok is false when they are not, and the
// entity must be scanned.
//
// Walking classify with a live row of the entity that matches no predicate, is
// named by no key set and is in neither membership set gives the same verdict
// every time under these rules: an entity with no base scope of its own answers
// rowHiddenNonMember (it is reached only through a follow spec, so the closure
// is its whole visibility), a purely windowed or keyed scope answers
// rowHiddenNonMember, and a scope with predicates answers rowHidden. None of
// the three is rowVisible, so the candidates hold every row the snapshot could
// ship.
//
// A nil scope with ok=true is the follow-only entity above. Full sync and a
// scope covering the whole entity are the declines: every live row is visible
// there, which the scan reaches more cheaply than any index.
func bootstrapNarrowable(e *rowEmitter, entity string) (*entityScope, bool) {
	if e.scopes == nil {
		return nil, false // full sync: every row is visible
	}
	scope := e.scopes[entity]
	if scope == nil {
		return nil, true
	}
	if scope.whole {
		return nil, false
	}
	return scope, true
}

// pinChains makes the parent-chain walker bound on e read each row that moved
// after seq with the parents, and what within entries read of it, that it had
// at seq, and read a row created after seq as one that does not exist. A
// snapshot that reads its pages in transactions of their own classifies every
// page as of its Begin position this way, which is the position the pull from
// its cursor compares the moves after it with (see compareMoves). With more
// moves than Limits.MaxResyncScan after seq it pins no more moves: that pull
// resets. When rows moved after seq, or the pins overflowed, it also binds a
// walker over the rows as tx reads them (rowEmitter.nowChains), and the
// emitter's filters then admit a row or a guarded unit only when both walkers
// do.
//
// pins carries what the pages before this one read of the log, so that each
// page reads only the moves after the newest one an earlier page read. Writes
// to a workspace commit in sequence order, so no move can appear later below
// that one.
func (s *Server) pinChains(tx storage.ReadTx, e *rowEmitter, seq uint64, pins *chainPins) error {
	if e.caller == nil || !e.caller.model.moving || e.chains == nil {
		return nil
	}
	if !pins.overflow {
		if err := s.pinMoves(tx, e, seq, pins); err != nil {
			return err
		}
	}
	// The pinned chains admit a row that has moved out of what the caller
	// reads since seq, and the page ships what was written to it after the
	// move. So the page also walks the chains as they are now, and a row
	// ships only when both admit it. That is safe for the pull from the
	// Begin cursor: it tombstones a row that moved out, which the client
	// does not hold, and ships a row that moved in. The walker is new on
	// each page because an earlier page memoized the chains of an earlier
	// transaction. After an overflow the pinned chains miss moves, and their
	// memo can be as old as the first page.
	e.nowChains = nil
	if len(e.chains.overrides) > 0 || len(e.chains.withinOverrides) > 0 || pins.overflow {
		e.nowChains = newAccessChains(e.chains.model, tx, e.chains.maxDepth, e.chains.report)
	}
	return nil
}

// pinMoves reads the moves after pins.through and pins the rows they name on
// the chains bound on e (see pinChains).
func (s *Server) pinMoves(tx storage.ReadTx, e *rowEmitter, seq uint64, pins *chainPins) error {
	moves, newest, overflow, err := readAccessMoves(tx, pins.through, s.limits.MaxResyncScan-pins.read)
	if err != nil {
		return err
	}
	if overflow {
		pins.overflow = true
		return nil
	}
	pins.through = max(pins.through, newest)
	pins.read += len(moves)
	// A row keeps the state from before its first move after seq, which an
	// earlier page has already pinned when the row moved again since.
	if e.chains.overrides == nil {
		e.chains.overrides = map[anchorRow]chainEdge{}
		e.chains.withinOverrides = map[anchorRow]rowWithin{}
	}
	for _, m := range moves {
		if _, seen := e.chains.overrides[m.row]; !seen {
			e.chains.overrides[m.row] = m.from
		}
	}
	for row, st := range firstWithins(e.caller.model, moves) {
		if _, seen := e.chains.withinOverrides[row]; !seen {
			e.chains.withinOverrides[row] = st
		}
	}
	e.chains.createdAfter = seq
	return nil
}

// chainPins is what pinChains has read of the access move log for one
// snapshot.
type chainPins struct {
	// through is the newest move read, or the Begin position before any.
	through uint64
	// read counts the moves read after the Begin position.
	read     int
	overflow bool
}

// movesFit reports whether a pull from cur reads every access move after it,
// rather than reset on more than Limits.MaxResyncScan of them (see
// compareMoves).
func (s *Server) movesFit(tx storage.ReadTx, e *rowEmitter, cur cursorState) (bool, error) {
	if e.caller == nil || !e.caller.model.moving {
		return true, nil
	}
	_, _, overflow, err := readAccessMoves(tx, max(cur.seq, cur.movesThrough), s.limits.MaxResyncScan)
	return !overflow, err
}
