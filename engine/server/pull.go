package server

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/cursorstate"
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
	// lineage is the mark of the write that allocated seq (see
	// storage.ReadTx.SeqLineage and stampLineage). 0 when no mark is known.
	lineage uint64
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
	// beyond its base scope. The next following pull brings the closure up to
	// date against current rows — maintaining it from this set where it can,
	// walking it again where it cannot — and diffs the result against this set.
	// nil on legacy cursors and non-following subscriptions ⇒ empty.
	closure *syncplanecursorv1.ClosureSet
	// closureRebuild marks a closure that was walked under a different
	// subscription set than subs (Cursor.closure_rebuild): a completed delta
	// resync carries the old scope's closure forward. It still names what the
	// client holds, so it diffs exactly, but it cannot be maintained; the next
	// following pull walks it again and clears this at a clean boundary.
	closureRebuild bool
	// resynced marks the cursor a delta resync completed with
	// (Cursor.resynced). The client holds the new scope as of that resync's seq
	// only, so every page of the catch-up pull that follows ships every row as
	// full state; the page that ends the catch-up at a clean boundary issues a
	// cursor without it.
	resynced bool
	// window is the windowed membership as ONE set (Cursor.window), which is
	// what cursors issued before windows were maintained per window carry.
	// This server issues windows instead, except on a truncated page that
	// delivered members its cursor did not name: the set it then carries is
	// no single window's, and the next page resolves every window again.
	window *syncplanecursorv1.ClosureSet
	// windows is the windowed membership split per window (Cursor.windows),
	// each with the position separating it from the rest of its ordering. That
	// split is what lets the next pull maintain a window from the rows that
	// changed rather than resolve it again. nil on non-windowed subscriptions
	// and on cursors older than the field.
	windows []*syncplanecursorv1.WindowState
	// stateID/stateGen name the cursor-state entry subs, closure, window and
	// windows were loaded from (Cursor.sets_ref); stateBlob keeps that entry's
	// bytes so encodeCursor can tell an unchanged membership — which reuses
	// the ref — from a moved one, which writes a new entry under the same
	// lineage. All zero when the cursor inlined its sets.
	stateID   uint64
	stateGen  uint64
	stateBlob []byte
	// setsKey is the seq key of the entry stateGen names (StoredSets.seq_key),
	// nil when the cursor inlined its sets or the entry has no key. epochTag
	// is Cursor.epoch_tag, which encodeCursor computes again with setsKey.
	setsKey  []byte
	epochTag uint64
	// accessPolicy, accessHash and accessID/accessGen are the cursor's record
	// of what its caller could read (Cursor.access_policy, access_hash and
	// access_ref). accessPending is a ReadAccess encodeCursor must store
	// before issuing the cursor; see stampAccess.
	accessPolicy  uint64
	accessHash    uint64
	accessID      uint64
	accessGen     uint64
	accessPending *syncplanecursorv1.ReadAccess
	// accessTag is Cursor.access_tag. accessKey is the seq key of the entry
	// accessGen names, nil when the cursor was decoded from a client: the
	// tag is then carried as it came.
	accessTag uint64
	accessKey []byte
	// callerAccessHash is the hash of the CallerAccess the client last
	// received (Cursor.caller_access_hash), and callerAccessID/Gen reference
	// the stored copy of it (Cursor.caller_access_ref). callerAccessPending
	// is a CallerAccess encodeCursor must store before issuing the cursor.
	callerAccessHash    uint64
	callerAccessID      uint64
	callerAccessGen     uint64
	callerAccessPending *syncplanev1.CallerAccess
	// movesThrough is the sequence through which the client's rows reflect
	// every access move (Cursor.access_moves_through).
	movesThrough uint64
	// stateEvicted marks a cursor whose referenced entry the store no longer
	// holds. The sets above are then empty and unusable, and pullPreamble
	// turns the cursor into a reset — the same recovery as falling below the
	// tombstone horizon.
	stateEvicted bool
}

// readFloor is the position a read from the cursor must read at or past: its
// seq, minSeq, and the position its access and the parents of the client's
// rows were read at (see compareAccess), and the one a resync in flight
// converges on.
func (c cursorState) readFloor(minSeq uint64) uint64 {
	return max(c.seq, minSeq, c.movesThrough, c.resync.GetToMovesThrough())
}

// windowSet is the window membership the cursor carries, in whichever form it
// carries it. Entity names come from the cursor's own subscription list, which
// is the list its window states index into — never from the request's, which
// may have moved on.
func (c cursorState) windowSet() map[closureKey]bool {
	if len(c.windows) == 0 {
		return closureSetMembership(c.window)
	}
	out := map[closureKey]bool{}
	for _, st := range c.windows {
		i := int(st.GetSubscription())
		if i >= len(c.subs) {
			continue
		}
		entity := c.subs[i].GetEntity()
		for _, k := range st.GetKeys() {
			out[closureKey{entity: entity, key: k}] = true
		}
	}
	return out
}

// windowSeedsMoved is the membership difference between the window set a cursor
// carried and the one this pull resolved, in both directions.
//
// A follow closure seeded from a window moves with it, and it moves for rows
// that were never written: a member evicted by an entering row, or one that
// rose into the window when another left, has the same version it always had
// and so appears in no change scan. Naming those rows is what keeps a
// maintained closure from stranding the followers of a seed that quietly left.
func windowSeedsMoved(old map[closureKey]bool, now closureRows) []closureKey {
	var moved []closureKey
	for ck := range now {
		if !old[ck] {
			moved = append(moved, ck)
		}
	}
	for ck := range old {
		if !now.has(ck.entity, ck.key) {
			moved = append(moved, ck)
		}
	}
	return moved
}

// decodeCursor parses a client's cursor and, when it references a cursor-state
// entry, resolves the entry into the returned state. A reference the store no
// longer answers is not an error: the scalars still decode, stateEvicted is
// set, and pullPreamble converts it into a reset. Only a transient store
// failure surfaces as an error, so the client retries instead of re-downloading
// a workspace it could have kept. A verdict the workspace gate left pending is
// reached before the store is read, and its error is a gateError.
func (s *Server) decodeCursor(ctx context.Context, ws string, raw []byte, verdict *pendingVerdict) (cursorState, error) {
	if len(raw) == 0 {
		return cursorState{}, nil
	}
	var c syncplanecursorv1.Cursor
	if err := proto.Unmarshal(raw, &c); err != nil {
		return cursorState{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("bad cursor: %w", err))
	}
	st := cursorState{
		seq:              c.GetSeq(),
		subHash:          c.GetSubHash(),
		epoch:            c.GetFilterEpoch(),
		storeEpoch:       c.GetStoreEpoch(),
		lineage:          c.GetSeqLineage(),
		partialEntity:    c.GetPartialEntity(),
		partialKey:       c.GetPartialKey(),
		subs:             c.GetSubscriptions(),
		resync:           c.GetResync(),
		closure:          c.GetClosure(),
		closureRebuild:   c.GetClosureRebuild(),
		resynced:         c.GetResynced(),
		window:           c.GetWindow(),
		windows:          c.GetWindows(),
		accessPolicy:     c.GetAccessPolicy(),
		accessHash:       c.GetAccessHash(),
		accessID:         c.GetAccessRef().GetId(),
		accessGen:        c.GetAccessRef().GetGen(),
		accessTag:        c.GetAccessTag(),
		epochTag:         c.GetEpochTag(),
		callerAccessHash: c.GetCallerAccessHash(),
		callerAccessID:   c.GetCallerAccessRef().GetId(),
		callerAccessGen:  c.GetCallerAccessRef().GetGen(),
		movesThrough:     c.GetAccessMovesThrough(),
	}
	ref := c.GetSetsRef()
	if ref.GetId() == 0 {
		return st, nil
	}
	st.stateID, st.stateGen = ref.GetId(), ref.GetGen()
	if s.cursorState == nil {
		// The server was reconfigured without a store; entries it once wrote
		// are unreachable, which to the client is indistinguishable from
		// eviction.
		st.stateEvicted = true
		return st, nil
	}
	// The read is storage work in the workspace, which a caller the gate
	// refuses must not cause.
	if err := verdict.reachApart(ctx); err != nil {
		return cursorState{}, err
	}
	blob, err := s.cursorState.Get(ctx, ws, cursorstate.Ref{ID: st.stateID, Gen: st.stateGen})
	if errors.Is(err, cursorstate.ErrNotFound) {
		s.tel().cursorStateMisses.Add(ctx, 1)
		st.stateEvicted = true
		return st, nil
	}
	if err != nil {
		return cursorState{}, connect.NewError(connect.CodeUnavailable, fmt.Errorf("cursor state: %w", err))
	}
	var sets syncplanecursorv1.StoredSets
	if err := proto.Unmarshal(blob, &sets); err != nil {
		// A corrupt entry cannot be advanced from any more than a missing one.
		s.tel().cursorStateMisses.Add(ctx, 1)
		st.stateEvicted = true
		return st, nil
	}
	st.subs = sets.GetSubscriptions()
	st.closure = sets.GetClosure()
	st.window = sets.GetWindow()
	st.windows = sets.GetWindows()
	st.stateBlob = blob
	st.setsKey = sets.GetSeqKey()
	return st, nil
}

// cursorStateInlineLimit is the sets size below which encodeCursor inlines
// them even with a store installed: a cursor smaller than this costs less to
// restate than the store round trip every later pull would pay to resolve it.
const cursorStateInlineLimit = 1024

// encodeCursor renders the position the caller assembled in st. With a
// cursor-state store the bulky sets travel as a reference: an unchanged
// membership reuses the ref st was decoded with, a moved one writes a new
// entry under the same lineage, and st is updated so the caller can carry the
// new ref into its next page. Without a store — or when the write fails — the
// sets are inlined, which every decode path accepts forever; the indirection
// is an optimization, never a dependency.
func (s *Server) encodeCursor(ctx context.Context, ws string, st *cursorState) []byte {
	c := &syncplanecursorv1.Cursor{
		Seq:            st.seq,
		SubHash:        st.subHash,
		FilterEpoch:    st.epoch,
		StoreEpoch:     st.storeEpoch,
		SeqLineage:     st.lineage,
		PartialEntity:  st.partialEntity,
		PartialKey:     st.partialKey,
		Resync:         st.resync,
		ClosureRebuild: st.closureRebuild,
		Resynced:       st.resynced,
	}
	if !s.storeSets(ctx, ws, st, c) {
		c.Subscriptions = st.subs
		c.Closure = st.closure
		c.Window = st.window
		c.Windows = st.windows
	}
	st.epochTag = 0
	if c.GetSetsRef() != nil && len(st.setsKey) > 0 {
		st.epochTag = epochTag(st.setsKey, st.seq, st.epoch)
		c.EpochTag = st.epochTag
	}
	s.storeAccess(ctx, ws, st, c)
	raw, err := proto.Marshal(c)
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	return raw
}

// storeSets moves st's sets into the cursor-state store and stamps the ref
// onto c, reporting whether it did; false means the caller must inline them.
func (s *Server) storeSets(ctx context.Context, ws string, st *cursorState, c *syncplanecursorv1.Cursor) bool {
	if s.cursorState == nil {
		return false
	}
	sets := &syncplanecursorv1.StoredSets{
		Subscriptions: st.subs,
		Closure:       st.closure,
		Windows:       st.windows,
		Window:        st.window,
		SeqKey:        st.setsKey,
	}
	blob, err := proto.Marshal(sets)
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	// A resync after a change of filter epoch trusts the cursor's seq and
	// epoch only by the tag, and the tag needs the key of a stored entry.
	keyed := s.resyncsEpochs()
	// The sets encode deterministically (each key list sorted — see keySet and
	// windowStates), so equal bytes mean equal membership and the entry the
	// cursor came from still describes it: reissue the ref, write nothing.
	// This is what keeps an idle poll, and every page that only advances seq,
	// free of store writes. An entry without a key is written again when the
	// tag needs one.
	if st.stateID != 0 && st.stateBlob != nil && bytes.Equal(blob, st.stateBlob) && (!keyed || len(st.setsKey) > 0) {
		c.SetsRef = &syncplanecursorv1.StateRef{Id: st.stateID, Gen: st.stateGen}
		return true
	}
	if !keyed && len(blob) <= cursorStateInlineLimit {
		return false
	}
	// A new key for each entry, so a tag computed for one entry is not valid
	// for another one.
	key := make([]byte, setsSeqKeySize)
	if _, err := rand.Read(key); err != nil {
		panic(err) // the platform CSPRNG does not fail
	}
	sets.SeqKey = key
	if blob, err = proto.Marshal(sets); err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	ref := cursorstate.Ref{ID: st.stateID, Gen: randNonZero()}
	if ref.ID == 0 {
		ref.ID = randNonZero()
	}
	if err := s.cursorState.Put(ctx, ws, ref, blob); err != nil {
		s.tel().cursorStatePutFailures.Add(ctx, 1)
		s.log().LogAttrs(ctx, slog.LevelDebug, "cursor state write failed; inlining sets",
			slog.String("workspace", ws), slog.String("error", err.Error()))
		return false
	}
	st.stateID, st.stateGen, st.stateBlob, st.setsKey = ref.ID, ref.Gen, blob, key
	c.SetsRef = &syncplanecursorv1.StateRef{Id: ref.ID, Gen: ref.Gen}
	return true
}

// resyncsEpochs reports whether a change of filter epoch can resync a client
// rather than reset it, which needs Permissions to implement EpochReadFilters.
func (s *Server) resyncsEpochs() bool {
	_, ok := s.perms.(EpochReadFilters)
	return ok
}

// setsSeqKeySize is the size in bytes of a StoredSets.seq_key.
const setsSeqKeySize = 32

// epochTag is the Cursor.epoch_tag of a cursor at seq and filter epoch that
// names the StoredSets entry holding key. Never 0, which a cursor without a
// tag carries.
func epochTag(key []byte, seq, epoch uint64) uint64 {
	return positionTag(key, seq, epoch)
}

// randNonZero returns a uniformly random nonzero uint64 for cursor-state
// identifiers. Zero is excluded because the wire treats it as "no ref".
func randNonZero() uint64 {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(err) // the platform CSPRNG does not fail
		}
		if v := binary.LittleEndian.Uint64(b[:]); v != 0 {
			return v
		}
	}
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
	resetStateEvicted      resetReason = "cursor_state_evicted"
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

func (s *Server) Pull(ctx context.Context, req *connect.Request[syncplanev1.PullRequest]) (res *connect.Response[syncplanev1.PullResponse], err error) {
	msg := req.Msg
	if err := naming.ValidateWorkspaceID(msg.GetWorkspaceId()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := checkClientID(msg.GetClientId()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	adm, err := s.enterWorkspaceServed(ctx, msg.GetWorkspaceId(), false)
	// A caller that held a permission when its cursor was issued and holds
	// none now gets its revocation resync before the refusal (see
	// lastGrantRefusal), so the refusal waits for the read transaction.
	var noAccess error
	if adm.caller != nil && claimsGrants(msg.GetCursor(), adm.caller) {
		if err != nil && errors.Is(err, ErrNoAccess) {
			noAccess, err = err, nil
		}
		if adm.verdict != nil {
			adm.verdict.holdNoAccess = true
		}
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if gerr := adm.verdict.refusal(ctx, err); gerr != nil {
			res, err = nil, gerr
		}
		// A caller the gate refuses learns nothing else of its request.
		if held := cmp.Or(noAccess, adm.verdict.heldNoAccess()); err != nil && held != nil {
			res, err = nil, held
		}
	}()
	principal := adm.principal
	cur, err := s.decodeCursor(ctx, msg.GetWorkspaceId(), msg.GetCursor(), adm.verdict)
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
	emitter.callerAccessDeltas = msg.CallerAccessVersion != nil
	emitter.heldCallerAccess = msg.GetCallerAccessVersion()
	emitter.admit(adm)
	emitter.noAccess = noAccess
	emitter.heldSubs = cur.subs

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
		if _, err := emitter.appFilterOf(e); err != nil {
			return nil, err
		}
	}

	// Route the page from the cursor alone (no tx needed). A delta resync
	// reconstructs the cursor's old visibility here, which calls
	// ReadFilter/ReadFilterAt (resync.go) and so must also run outside the
	// snapshot.
	route, err := s.routePull(ctx, msg.GetWorkspaceId(), cur, msg.GetCursor(), wantHash, epoch, emitter)
	if err != nil {
		return nil, err
	}

	limit := s.limits.MaxPullChanges
	if req := int(msg.GetLimit()); req > 0 && req < limit {
		limit = req
	}

	var resp *syncplanev1.PullResponse
	var resetSeen resetReason // logged after the tx commits, never inside it
	// The snapshot is floored at BOTH positions the client holds. The cursor's
	// seq alone is not enough: a push acked at seq S+1 followed by a pull whose
	// cursor still sits at S may be served a snapshot missing the acked write,
	// and the client would drop its optimism for a mutation the page lacks.
	// min_seq is untrusted wire input whose worst effect is forcing the read
	// onto the primary — the behavior every pull had before replicas.
	err = s.readTx(ctx, msg.GetWorkspaceId(), cur.readFloor(msg.GetMinSeq()), func(tx storage.ReadTx) error {
		var e error
		resp, _, resetSeen, e = s.servePullPage(ctx, tx, route, cur, emitter, wantHash, epoch, msg.GetSubscriptions(), watermarkKey(principal, msg.GetClientId()), msg.GetCursor(), limit)
		return e
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

// pullRoute is the path one pull page takes from its cursor. routePull decides
// it before the read snapshot opens; servePullPage follows it inside one.
type pullRoute struct {
	// resync is set when the cursor's subscription set or filter epoch no
	// longer matches the request, or the cursor is inside a delta resync.
	resync bool
	// reset is why a resync cannot run, or resetNone. The storage checks in
	// pullPreamble still run first, and their reason wins.
	reset resetReason
	// oldEm classifies rows under the visibility the cursor was issued with.
	oldEm *rowEmitter
	// dropEnts are the entities that left the subscription set. The first
	// page of a resync drops them whole.
	dropEnts []string
}

// routePull decides whether the page from cur is a forward page or a delta
// resync step. A resync needs the cursor's old visibility, and rebuilding it
// calls ReadFilterAt, so this runs outside the read snapshot.
//
// An empty cursor never resyncs: it decodes to seq 0 with no prior scope to
// diff against, so it either replays the workspace from the beginning or
// resets on the horizon (see pullPreamble). A non-empty cursor whose
// subscription set or filter epoch changed enters a delta resync, and falls
// back to reset when the old scope cannot be reconstructed. Mid-resync the
// client's rows are a mixture of old and new scope, so a FURTHER change
// invalidates the diff in flight and only a re-bootstrap recovers.
func (s *Server) routePull(ctx context.Context, ws string, cur cursorState, rawCursor []byte, wantHash, epoch uint64, emitter *rowEmitter) (pullRoute, error) {
	switch {
	case cur.resync != nil:
		if cur.resync.GetToSubHash() != wantHash || cur.resync.GetToFilterEpoch() != epoch {
			return pullRoute{resync: true, reset: resetStaleResync}, nil
		}
		oldEm, ok, err := s.oldVisibility(ctx, ws, cur, epoch, emitter)
		if err != nil {
			return pullRoute{}, err
		}
		if !ok {
			return pullRoute{resync: true, reset: resetResyncUnavailable}, nil
		}
		return pullRoute{resync: true, oldEm: oldEm}, nil
	case len(rawCursor) > 0 && (cur.subHash != wantHash || cur.epoch != epoch):
		oldEm, ok, err := s.oldVisibility(ctx, ws, cur, epoch, emitter)
		if err != nil {
			return pullRoute{}, err
		}
		if !ok {
			return pullRoute{resync: true, reset: resetResyncUnavailable}, nil
		}
		// Entities that left the subscription set drop wholesale, once, on the
		// first resync page; the scan covers the rest.
		return pullRoute{resync: true, oldEm: oldEm, dropEnts: s.leavingEntities(oldEm, emitter)}, nil
	}
	return pullRoute{}, nil
}

// servePullPage serves one page along route under tx: a forward page
// (pullPage), a delta resync step, or a reset. The returned cursorState is the
// position the page advanced to; Subscribe carries it forward across pages.
//
// It binds the caller's access in the access model from tx first. A change of
// that access since the cursor was issued also makes the page a resync step,
// or a reset when the cursor's access cannot be read back. A resync still runs
// the shared reset checks first, so a cursor below the horizon or from a
// foreign store resets rather than resyncs.
func (s *Server) servePullPage(ctx context.Context, tx storage.ReadTx, route pullRoute, cur cursorState, emitter *rowEmitter, wantHash, epoch uint64, subs []*syncplanev1.Subscription, ackKey string, rawCursor []byte, limit int) (*syncplanev1.PullResponse, cursorState, resetReason, error) {
	if err := emitter.bindAccess(tx); err != nil {
		return nil, cursorState{}, resetNone, err
	}
	// What the caller can read moves with its grants, which the cursor
	// recorded, so a change of access diverts the page into a resync the same
	// way a change of scope does. Only this snapshot can tell.
	acc, err := s.compareAccess(ctx, emitter.ws, tx, cur, len(rawCursor) > 0, emitter)
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	if err := lastGrantRefusal(emitter, cur, route, acc); err != nil {
		return nil, cursorState{}, resetNone, err
	}
	old, resync, ok := route.oldEm, route.resync, route.resync && route.reset == resetNone
	if acc.reset == resetNone && route.reset != resetStaleResync {
		old, resync, ok, err = s.bindResyncAccess(ctx, emitter.ws, cur, acc, old, resync, ok, emitter)
		if err != nil {
			return nil, cursorState{}, resetNone, err
		}
	}
	if !resync && acc.reset == resetNone {
		return s.pullPage(tx, cur, emitter, wantHash, epoch, subs, ackKey, rawCursor, limit)
	}
	resp := &syncplanev1.PullResponse{Patch: &syncplanev1.Patch{}}
	_, _, reset, err := s.pullPreamble(tx, cur, ackKey, rawCursor, resp)
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	switch {
	case reset != resetNone:
	case route.reset == resetStaleResync:
		reset = resetStaleResync
	case acc.reset != resetNone:
		reset = acc.reset
	case !ok:
		reset = resetResyncUnavailable
	}
	if reset != resetNone {
		if emitter.noAccess != nil {
			return nil, cursorState{}, resetNone, emitter.noAccess
		}
		resp.Reset_ = true
		resp.NextCursor = rawCursor
		return resp, cur, reset, nil
	}
	pos := resyncPos{}
	if cur.resync != nil {
		pos = resyncPos{entity: cur.resync.GetEntity(), afterKey: cur.resync.GetAfterKey()}
	} else {
		resp.Patch.DropEntities = appendMissing(route.dropEnts, old.hiddenEntities)
	}
	next, err := s.resyncStep(tx, cur, old, emitter, wantHash, epoch, subs, pos, limit, resp)
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	if emitter.noAccess != nil {
		// The client pulls again at once and receives the refusal, which the
		// page after the resync's last one answers with.
		resp.HasMore = true
	}
	return resp, next, resetNone, nil
}

// claimsGrants reports whether raw, the cursor of a pull by caller, names an
// access record in which caller holds a permission. It reads only the
// cursor, so a cursor it accepts can still fail to resync (see
// lastGrantRefusal).
func claimsGrants(raw []byte, caller *accessCaller) bool {
	if len(raw) == 0 {
		return false
	}
	var c syncplanecursorv1.Cursor
	if proto.Unmarshal(raw, &c) != nil {
		return false
	}
	return c.GetAccessPolicy() != 0 && c.GetAccessRef().GetGen() != 0 &&
		c.GetAccessHash() != readAccessHash(&syncplanecursorv1.ReadAccess{Subjects: caller.subjects})
}

// lastGrantRefusal returns the refusal a pull answers with when the caller
// holds no permission, or nil when the pull serves the page. A pull serves it
// when its cursor was issued while the caller held a permission and the page
// can resync from it, or when the cursor is inside such a resync: the resync
// tombstones every row the caller no longer reads and sends a CallerAccess that
// holds nothing, as an open stream does before the gate drops its scope. Each
// page of it has has_more set, and the pull after its last page gets the
// refusal. A pull whose cursor recorded no permission, or that would reset, gets
// the refusal at once, so a caller that waits for its first grant keeps its
// local copy.
func lastGrantRefusal(e *rowEmitter, cur cursorState, route pullRoute, acc accessComparison) error {
	if e.noAccess == nil {
		return nil
	}
	if route.reset != resetNone || acc.reset != resetNone {
		return e.noAccess
	}
	if cur.resync != nil || (acc.old != nil && acc.old.any()) {
		return nil
	}
	return e.noAccess
}

// belowHorizon reports whether a change-feed position may have missed a
// collected tombstone. The horizon is the highest version GC has collected a
// tombstone at, so a cursor strictly below it is stale. A cursor AT the horizon
// is stale too when it carries a sub-position: the page that issued it split
// the version group, and the rows of that group after (partialEntity, key)
// were never delivered — a tombstone among them is gone now. Only a cursor at
// the horizon on a clean version boundary has seen every row of that version.
func belowHorizon(seq uint64, partialEntity string, horizon uint64) bool {
	return seq < horizon || (seq == horizon && partialEntity != "")
}

// pullPreamble reads the workspace position, stamps last_acked onto resp, and
// runs the horizon + store-epoch reset checks shared by every pull path.
// ackKey is the caller's watermarkKey; empty stamps no last_acked. reset
// is true when the cursor cannot be served incrementally and the caller must
// reply reset=true. current and storeEpoch are the live workspace position
// (meaningful only when reset is false).
func (s *Server) pullPreamble(tx storage.ReadTx, cur cursorState, ackKey string, rawCursor []byte, resp *syncplanev1.PullResponse) (current, storeEpoch uint64, reset resetReason, err error) {
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
	if ackKey != "" {
		acked, err := tx.ClientLastMutationID(ackKey)
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
	if belowHorizon(cur.seq, cur.partialEntity, horizon) {
		return current, storeEpoch, resetHorizon, nil
	}
	// Cursor provenance: a non-empty cursor that this store could not have
	// issued means the URL now serves a different database (swapped,
	// recreated empty, or restored from backup). Three impossible-by-
	// construction signals, any of which forces a re-bootstrap instead
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
	//  3. lineage mismatch — the case neither of those can see: the SAME
	//     store restored from a backup taken before cur.seq and written back
	//     past it. The epoch came back with the backup and the seq is in
	//     range, but the write the store holds at cur.seq is not the one the
	//     cursor was issued after. See lineageForeign.
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
	foreign, err := lineageForeign(tx, cur)
	if err != nil {
		return 0, 0, resetNone, err
	}
	if foreign {
		return current, storeEpoch, resetProvenance, nil
	}
	// The cursor referenced a state entry the store no longer holds, so the
	// membership sets it was issued under are gone and nothing exact can be
	// diffed against them. Judged after horizon and provenance: those reasons
	// explain WHY a cursor is old enough for its entry to have expired.
	if cur.stateEvicted {
		return current, storeEpoch, resetStateEvicted, nil
	}
	return current, storeEpoch, resetNone, nil
}

// lineageForeign reports whether the store holds cur.seq under another lineage
// mark than the one the cursor carries, which means the history the cursor was
// issued from is not this store's: the store was restored from a backup taken
// before cur.seq and has written past it since.
//
// A zero mark on either side is "unknown" and passes. The cursor's is zero when
// it predates the field; the store's when the sequence was written before it
// recorded marks or GC has pruned the record (see storage.ReadTx.SeqLineage).
// The check therefore covers cursors whose sequence was written within the GC
// retention.
func lineageForeign(tx storage.ReadTx, cur cursorState) (bool, error) {
	if cur.lineage == 0 {
		return false, nil
	}
	mark, err := tx.SeqLineage(cur.seq)
	if err != nil {
		return false, err
	}
	return mark != 0 && mark != cur.lineage, nil
}

// stampLineage records in st the lineage mark of st.seq, so the pull that
// presents the cursor can run lineageForeign against it. Every path that
// issues a cursor at a new position calls it; a cursor carried forward at the
// position it came in at keeps the mark it came with.
func stampLineage(tx storage.ReadTx, st *cursorState) error {
	mark, err := tx.SeqLineage(st.seq)
	if err != nil {
		return err
	}
	st.lineage = mark
	return nil
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

// sameSubscriptions reports whether a cursor's subscription set is this
// request's, position for position. Maintaining a membership set against a
// cursor needs that, and sub_hash does not give it: the hash is
// order-independent and excludes the follow spec, so two requests can hash
// alike while their windows line up differently or reach different rows.
//
// Unequal costs one rebuild rather than a wrong answer, and settles itself —
// the cursor that pull issues records the set as sent, so the pull after it
// lines up.
func sameSubscriptions(a, b []*syncplanev1.Subscription) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// maxMaintainedChanges bounds the rows one pull holds in memory to maintain its
// membership sets from. Past it the page is a backlog rather than a step, and a
// set the size of the client's working set costs no more to rebuild than to
// decide row by row against a change list as long as itself.
const maxMaintainedChanges = 512

// changeSet reads the rows written since the cursor, in ScanChanges order, and
// reports whether that is ALL of them. An incomplete set comes back empty.
//
// Maintaining a membership set is a function of what moved, so it is only sound
// over a complete set: a page that stops short leaves rows unaccounted for that
// the sets would have moved on. Two things make it incomplete — a backlog
// longer than maxMaintainedChanges, and a cursor resuming inside a same-version
// group, whose membership was issued at an earlier position than its own seq
// names, so the rows between the two are in no scan this cursor can start.
//
// A complete set is also the whole page, which is why the rows come back: it
// stopped short of the limit, so nothing follows it and nothing is re-read.
func (s *Server) changeSet(tx storage.ReadTx, cur cursorState, limit int) ([]storage.Row, bool, error) {
	if cur.partialEntity != "" {
		return nil, false, nil
	}
	budget := min(limit, maxMaintainedChanges)
	var rows []storage.Row
	if err := tx.ScanChanges(cur.seq, "", "", budget+1, func(row storage.Row) error {
		rows = append(rows, row)
		return nil
	}); err != nil {
		return nil, false, err
	}
	if len(rows) > budget {
		return nil, false, nil
	}
	return rows, true, nil
}

// deliverPage feeds one page's rows to fn: straight from the change set the
// membership work already read where that set is complete, and from a fresh
// scan otherwise. The scan always offers one row past the limit, because seeing
// it is how truncation is detected — a complete change set stopped short of the
// limit and so has none.
func deliverPage(tx storage.ReadTx, cur cursorState, changes []storage.Row, complete bool, limit int, fn func(storage.Row) error) error {
	if complete {
		for _, row := range changes {
			if err := fn(row); err != nil {
				return err
			}
		}
		return nil
	}
	return tx.ScanChanges(cur.seq, cur.partialEntity, cur.partialKey, limit+1, fn)
}

// pullPage serves one forward pull page from cur under a read snapshot: the
// shared reset checks + last_acked (pullPreamble), the window and closure
// refresh, the classify pass over the page's rows, the membership diffs at a
// clean version boundary, and the next cursor. It does NOT handle delta resync —
// servePullPage routes that before invoking pullPage. The returned cursorState is the position the page
// advanced to (meaningful only when the response is neither reset nor closes
// the stream early); Subscribe carries it forward across notifications.
func (s *Server) pullPage(tx storage.ReadTx, cur cursorState, emitter *rowEmitter, wantHash, epoch uint64, subs []*syncplanev1.Subscription, ackKey string, rawCursor []byte, limit int) (*syncplanev1.PullResponse, cursorState, resetReason, error) {
	resp := &syncplanev1.PullResponse{Patch: &syncplanev1.Patch{}}
	current, storeEpoch, reset, err := s.pullPreamble(tx, cur, ackKey, rawCursor, resp)
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	if reset != resetNone {
		resp.Reset_ = true
		resp.NextCursor = rawCursor
		return resp, cur, reset, nil
	}
	// An idle poll must not pay for either set. Both of them —
	// the window and the follow closure — are functions of the workspace's rows
	// and this request's read filters, and neither can have moved since the
	// cursor was issued: it already sits at the current sequence, so no row has
	// been written, and a visibility change would have bumped FilterEpoch or
	// moved the caller's access, and diverted this pull into a resync before it
	// reached here. So the sets the cursor carries are the sets a rebuild would
	// produce, and the scan below has nothing to deliver either — every row is
	// at or below cur.seq.
	//
	// It stays worth doing even though the sets below are usually maintained
	// rather than rebuilt: maintenance still reads the change scan, resolves the
	// hops it has to and re-encodes both sets onto a cursor, and a poll with
	// nothing to say should cost none of that.
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
	//   - A closure a completed resync carried over from the old scope. It is
	//     what the client holds, but not what the new scope's seeds produce,
	//     and only the closure diff below brings the two together.
	if cur.seq == current && cur.partialEntity == "" && sameFollowSpecs(cur.subs, subs) && !cur.closureRebuild {
		// Carried forward as issued, restamped with what this request resolved:
		// a legacy cursor picks up the store epoch and its subscription set here
		// exactly as it would on the normal path below.
		next := cur
		next.subHash, next.epoch, next.storeEpoch, next.subs = wantHash, epoch, storeEpoch, subs
		// Restamped rather than carried: a cursor older than the field picks
		// up the mark here, as it picks up the store epoch.
		if err := stampLineage(tx, &next); err != nil {
			return nil, cursorState{}, resetNone, err
		}
		emitter.stampAccess(&next, cur)
		resp.CallerAccess, resp.CallerAccessDelta = emitter.callerAccessUpdate(cur, len(rawCursor) > 0, current)
		resp.NextCursor = s.encodeCursor(emitter.ctx, emitter.ws, &next)
		resp.Seq = next.seq
		return resp, next, resetNone, nil
	}
	// Both bounded membership sets move only where rows moved, so read what
	// changed first: a COMPLETE change set is what lets them be maintained
	// rather than resolved from scratch, and it is the page besides.
	//
	// A subscription with neither a window nor a follow spec has no membership
	// to maintain and its pull is already O(change), so it never pays for this
	// — the page is the plain scan it always was.
	var (
		changes  []storage.Row
		complete bool
	)
	if !s.limits.DisableMembershipMaintenance && (len(emitter.windows) > 0 || emitter.followEntities != nil) {
		if changes, complete, err = s.changeSet(tx, cur, limit); err != nil {
			return nil, cursorState{}, resetNone, err
		}
	}
	// Maintaining also needs the cursor's subscription set to line up with this
	// request's: the window states index into it, and a follow spec that moved
	// describes a different closure than the one on the cursor. Either way the
	// answer is to resolve both sets from scratch, which is always legal.
	maintain := complete && sameSubscriptions(cur.subs, subs)

	// Windowed subscriptions: resolve each window's membership BEFORE the scan
	// so classify is window-aware, and before the closure below, which seeds
	// from window members. See window.go.
	oldWindow := cur.windowSet()
	if maintain {
		emitter.windowMembers, err = emitter.refreshWindows(tx, cur.windows, changes)
	} else {
		emitter.windowMembers, err = emitter.buildWindows(tx)
	}
	if err != nil {
		return nil, cursorState{}, resetNone, err
	}
	// Reference-following: bring the closure up to date against current rows
	// BEFORE the scan so classify is closure-aware (a changed follow-target must
	// not be tombstoned). This is the only path that resolves the closure under
	// the request's scope; a resync classifies with the set the cursor carries
	// and leaves the reconciliation to the pull that follows it. See closure.go.
	//
	// A closure the cursor carried over from another scope is what the client
	// holds, so it still diffs exactly below, but it was walked from that
	// scope's seeds and paths: maintaining it from what changed since seq would
	// keep every member the new scope never reaches and miss every one it does.
	if emitter.followEntities != nil {
		maintained := false
		if maintain && !cur.closureRebuild {
			emitter.closure, maintained, err = emitter.maintainClosure(
				tx, cur.closure, subs, changes, windowSeedsMoved(oldWindow, emitter.windowMembers))
			if err != nil {
				return nil, cursorState{}, resetNone, err
			}
		}
		if !maintained {
			if emitter.closure, err = emitter.buildClosure(tx, subs); err != nil {
				return nil, cursorState{}, resetNone, err
			}
		}
	}
	// Collaborative-text fields of rows written past the cursor ship as delta
	// state where the recipient's prior state is knowable (see emitChange). Set
	// per page, because a Subscribe stream reuses one emitter across pages at
	// different cursors, and cleared after the scan so the membership diffs
	// below — which emit rows AT or BELOW the cursor — stay full-state.
	//
	// The catch-up after a delta resync is the exception: the resync delivered
	// the rows the new scope or access shows as of its seq only, so one written
	// past that seq is the client's first sight of it and no fragment can be
	// folded into anything. A page that stops short of the current seq leaves
	// such rows to the next page, so the mark stays on its cursor; the client
	// holds the whole scope only as of the cursor of the page that catches up.
	emitter.deltas = !cur.resynced
	emitter.deltaAfter = cur.seq
	// What the cursor proves the client holds, for the hidden branches below:
	// the exact membership sets it carried (window and closure), and — for
	// scopes with no stored set — the fact that nothing created past its seq
	// can ever have been delivered.
	priorClosure := closureSetMembership(cur.closure)
	// Members this page delivers that the cursor's sets do not name. A page
	// that truncates must carry them on its cursor; see the HasMore branch.
	var shippedWindow, shippedClosure []closureKey
	// Scan one row past the limit: seeing it proves truncation without
	// emitting it.
	scanned := 0
	var lastScanned storage.Row
	deliver := func(row storage.Row) error {
		scanned++
		if scanned > limit {
			resp.HasMore = true
			return nil
		}
		// The cursor tracks the last SCANNED row, not the last emitted
		// one: filtered or unsubscribed rows must still advance it or the
		// client's pagination loop would never progress past them. Hidden
		// rows consume limit slots whether or not they emit a tombstone,
		// keeping pagination monotone. The (version, entity, key) of this row
		// is the resume point if the page truncates inside a same-version
		// group.
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
			ck := closureKey{entity: row.Entity, key: row.Key}
			if emitter.windowMembers.has(ck.entity, ck.key) && !oldWindow[ck] {
				shippedWindow = append(shippedWindow, ck)
			}
			if emitter.closure.has(ck.entity, ck.key) && !priorClosure[ck] {
				shippedClosure = append(shippedClosure, ck)
			}
		case rowHidden:
			// Revoke only what the client can hold. A row created past the
			// cursor was never delivered under it, so a tombstone would only
			// disclose a key the read filter may exist to withhold. An older
			// row stays conservative — CreatedVersion 0 is "unknown" and
			// clears no bar — because without history the server cannot prove
			// the client never saw a state of it that was visible.
			if row.CreatedVersion > cur.seq {
				return nil
			}
			resp.Patch.Changes = append(resp.Patch.Changes, hiddenChange(row))
		case rowHiddenNonMember:
			// Hidden solely by set-membership, and the cursor stores those
			// sets: a prior member left and must drop; anything else was never
			// delivered, so there is nothing to revoke and nothing to
			// disclose.
			ck := closureKey{entity: row.Entity, key: row.Key}
			if !oldWindow[ck] && !priorClosure[ck] {
				return nil
			}
			resp.Patch.Changes = append(resp.Patch.Changes, hiddenChange(row))
		case rowUnsubscribed:
			// Skip: the client holds no state for this entity, unless the
			// cursor's closure names the row. Then an earlier follow spec
			// delivered it, this request's specs no longer reach its entity,
			// and the membership diff below leaves rows past cur.seq to this
			// scan.
			ck := closureKey{entity: row.Entity, key: row.Key}
			if priorClosure[ck] && emitter.syncable(row.Entity) {
				resp.Patch.Changes = append(resp.Patch.Changes, hiddenChange(row))
			}
		}
		return nil
	}
	if err := deliverPage(tx, cur, changes, complete, limit, deliver); err != nil {
		emitter.deltas = false
		return nil, cursorState{}, resetNone, err
	}
	emitter.deltas = false
	next := cursorState{subHash: wantHash, epoch: epoch, storeEpoch: storeEpoch, subs: subs}
	// Continue the cursor's state lineage: pages that carry the sets forward
	// unchanged reuse the entry they were decoded from, and a page that moved
	// them writes its entry under the same id (see storeSets).
	next.stateID, next.stateGen, next.stateBlob, next.setsKey = cur.stateID, cur.stateGen, cur.stateBlob, cur.setsKey
	emitter.stampAccess(&next, cur)
	if resp.HasMore {
		// Resume just past the last delivered row. The (entity, key)
		// sub-position lets the next pull pick up the rest of a group
		// sharing this version instead of skipping it. The membership diff is
		// deferred to a clean boundary, so carry the prior sets forward
		// (the incremental scan already covered version>seq members above)
		// — and with them the mark that says the closure cannot be
		// maintained, since the next page resolves the closure again.
		next.seq = lastScanned.Version
		next.partialEntity = lastScanned.Entity
		next.partialKey = lastScanned.Key
		next.closure = cur.closure
		next.closureRebuild = cur.closureRebuild
		next.window = cur.window
		next.windows = cur.windows
		// This page classified with sets resolved for it, not the cursor's.
		// A row it delivered as a member is in neither the prior set nor, if
		// the membership moves again before the clean boundary, the one that
		// boundary resolves, so its diff would never remove the row. Carry
		// the prior set plus those rows instead. That union is no window's
		// or walk's membership, so it goes in the forms that are diffed but
		// never maintained, and a closure seeded from such a window is
		// walked again too.
		if len(shippedClosure) > 0 {
			next.closure = heldSet(priorClosure, shippedClosure)
			next.closureRebuild = true
		}
		if len(shippedWindow) > 0 {
			next.window = heldSet(oldWindow, shippedWindow)
			next.windows = nil
			if emitter.followEntities != nil {
				next.closureRebuild = true
			}
		}
		next.resynced = cur.resynced
	} else {
		next.seq = current
		// Clean boundary: reconcile both bounded membership sets. Each emits
		// the at-or-below-seq delta the incremental scan could not (members
		// that entered; members that left and nothing else holds), then stamps
		// its new key-set. See emitMembershipDiff.
		if emitter.followEntities != nil {
			if err := emitter.emitMembershipDiff(tx, closureSetMembership(cur.closure), emitter.closure, cur.seq, resp.Patch); err != nil {
				return nil, cursorState{}, resetNone, err
			}
			next.closure = emitter.closure.keySet()
		} else if len(priorClosure) > 0 {
			// This request follows nothing, so its closure is empty: every
			// row the cursor's closure names drops unless base scope holds it.
			// A server-only member never reached the client, so it is left
			// out rather than revoked.
			held := make(map[closureKey]bool, len(priorClosure))
			for ck := range priorClosure {
				if emitter.syncable(ck.entity) {
					held[ck] = true
				}
			}
			if err := emitter.emitMembershipDiff(tx, held, nil, cur.seq, resp.Patch); err != nil {
				return nil, cursorState{}, resetNone, err
			}
		}
		if len(emitter.windows) > 0 {
			if err := emitter.emitMembershipDiff(tx, oldWindow, emitter.windowMembers, cur.seq, resp.Patch); err != nil {
				return nil, cursorState{}, resetNone, err
			}
			next.windows = emitter.windowStates()
		}
	}
	if err := stampLineage(tx, &next); err != nil {
		return nil, cursorState{}, resetNone, err
	}
	resp.CallerAccess, resp.CallerAccessDelta = emitter.callerAccessUpdate(cur, len(rawCursor) > 0, current)
	resp.NextCursor = s.encodeCursor(emitter.ctx, emitter.ws, &next)
	resp.Seq = next.seq
	return resp, next, resetNone, nil
}
