// Delta resync: when a pull's subscription set or filter epoch no longer
// matches its cursor, the server used to reply reset=true and force a full
// re-bootstrap — correct but blunt, since a single permission change would
// stampede every client in the workspace at once. Instead, the server now
// reconstructs the visibility rules the cursor was issued under, compares the
// workspace's live rows under both, and ships only the difference between the
// old visible set and the new one:
//
//	visible → hidden     tombstone (the client drops the row)
//	hidden  → visible    value
//	unchanged            nothing
//	entity left the set  one Patch.drop_entities name, instead of per-row tombstones
//
// The diff covers only rows with Version <= the cursor's seq — exactly the
// rows the client could already hold. Anything written later is the job of
// the catch-up pull that follows the resync (the completed cursor keeps the
// ORIGINAL seq under the new scope), which also makes the paged diff safe
// without a pinned snapshot: a row edited mid-resync gains a version past
// the cursor's seq, and the catch-up pull re-delivers or tombstones it under
// the new visibility, overwriting whatever the diff decided.
//
// Reset remains the fallback whenever the old visibility cannot be
// reconstructed: legacy cursors without an embedded subscription set, epoch
// changes without EpochReadFilters (or past its retained history), and a
// second scope change landing mid-resync (the client's rows are then a
// mixture of old and new scope that no single diff describes).
package server

import (
	"context"
	"errors"
	"math"
	"sort"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// oldVisibility reconstructs the row-visibility rules a cursor was issued
// under: its embedded subscription set plus read filters at its epoch. ok is
// false when reconstruction is impossible and the caller must reset.
func (s *Server) oldVisibility(ctx context.Context, ws string, cur cursorState, currentEpoch uint64, newEm *rowEmitter) (oldEm *rowEmitter, ok bool, err error) {
	var subs []*syncplanev1.Subscription
	if cur.subHash != 0 {
		if len(cur.subs) == 0 {
			return nil, false, nil // pre-upgrade cursor: hash only, set unknown
		}
		hash, err := subscriptionHash(cur.subs)
		if err != nil || hash != cur.subHash {
			return nil, false, nil
		}
		subs = cur.subs
	}
	oldEm, err = newRowEmitter(ctx, s, ws, subs)
	if err != nil {
		// The old set no longer validates (e.g. the entity or a predicate
		// field is gone after a server upgrade).
		return nil, false, nil
	}
	if cur.epoch != currentEpoch {
		erf, implemented := s.perms.(EpochReadFilters)
		if !implemented {
			return nil, false, nil
		}
		oldEm.filterSource = func(entity string) (RowFilter, error) {
			return erf.ReadFilterAt(ctx, principalOf(ctx), ws, entity, cur.epoch)
		}
	}
	// Resolve every filter the scan will consult up front, so an
	// unreconstructable epoch surfaces here as a clean reset rather than
	// mid-scan after deltas were already emitted.
	for _, entity := range newEm.entities() {
		if !oldEm.subscribed(entity) {
			continue
		}
		if _, err := oldEm.filterOf(entity); err != nil {
			if errors.Is(err, ErrEpochUnavailable) {
				return nil, false, nil
			}
			return nil, false, err
		}
	}
	return oldEm, true, nil
}

// leavingEntities returns the registered entities subscribed under the old
// scope but not the new one.
//
// Server-only entities are outside this question entirely: no client ever held
// their rows, so neither scope can have carried them and announcing one as
// leaving would tell the client to drop something it never had.
func (s *Server) leavingEntities(oldEm, newEm *rowEmitter) []string {
	var out []string
	for _, name := range s.catalogPtr().syncEntityNames() {
		if oldEm.subscribed(name) && !newEm.subscribed(name) {
			out = append(out, name)
		}
	}
	return out
}

// resyncPos is a position in the (entity, key)-ordered resync diff.
type resyncPos struct {
	entity   string
	afterKey string
}

// resyncStep advances a delta resync by one pull page and encodes the
// resulting cursor: mid-resync cursors keep the old scope (which still
// describes the bulk of the client's rows) plus the diff position; on
// completion the new scope swaps in at the ORIGINAL seq so the next pull
// catches up on everything written since, under the new visibility.
//
// Both emitters' windows are rebuilt HERE, per page, rather than stored in
// ResyncState. That keeps the cursor small, and it is safe: the diff visits
// each row once in (entity, key) order, rows changed since the cursor carry
// version > cur.seq and belong to the catch-up pull, and a re-emitted row
// applies idempotently. The price is resolving both emitters' windows once per
// page of a scope change, which is a rare event.
func (s *Server) resyncStep(tx storage.ReadTx, cur cursorState, oldEm, newEm *rowEmitter, wantHash, epoch uint64, subs []*syncplanev1.Subscription, pos resyncPos, limit int, resp *syncplanev1.PullResponse) error {
	var err error
	if oldEm.windowMembers, err = oldEm.buildWindows(tx); err != nil {
		return err
	}
	if newEm.windowMembers, err = newEm.buildWindows(tx); err != nil {
		return err
	}
	next, done, err := runResync(tx, oldEm, newEm, cur.seq, pos, limit, s.limits, resp.Patch)
	if err != nil {
		return err
	}
	if done {
		// Keep any same-version sub-position from the cursor that entered the
		// resync: the catch-up pull resumes from the original seq under the
		// new scope and must not skip the undelivered tail of a split group.
		st := cursorState{
			seq:     cur.seq,
			subHash: wantHash,
			epoch:   epoch,
			// cur passed Pull's provenance guard, so its storeEpoch already
			// equals this store's; carry it onto the completion cursor.
			storeEpoch:    cur.storeEpoch,
			partialEntity: cur.partialEntity,
			partialKey:    cur.partialKey,
			subs:          subs,
			// Carry the prior closure through unchanged: the resync diffs only
			// base scope (closure stays empty here), and the catch-up pull that
			// follows rebuilds and reconciles the closure under the new scope.
			closure: cur.closure,
			// The window, by contrast, is REPLACED: the resync just emitted the
			// difference between the old windows and the new ones, so the client
			// now holds the new membership and the cursor must say so. Carrying
			// the old set forward would make the catch-up pull re-diff a
			// membership the client no longer has.
			windows: newEm.windowStates(),
		}
		// Same state lineage, new entry: the sets moved with the scope, but
		// they are still this client's succession of cursors.
		st.stateID, st.stateGen, st.stateBlob = cur.stateID, cur.stateGen, cur.stateBlob
		resp.NextCursor = s.encodeCursor(newEm.ctx, newEm.ws, &st)
	} else {
		st := cur
		st.resync = &syncplanecursorv1.ResyncState{
			Entity:        next.entity,
			AfterKey:      next.afterKey,
			ToSubHash:     wantHash,
			ToFilterEpoch: epoch,
		}
		resp.NextCursor = s.encodeCursor(newEm.ctx, newEm.ws, &st)
	}
	// Either way the client must pull again: mid-resync to continue the
	// diff, after completion to catch up from the original seq.
	resp.HasMore = true
	resp.Seq = cur.seq
	return nil
}

// resyncScanBatch is the number of rows one storage scan call in a resync
// returns at a time. It caps how many rows are held at once, independently of
// how many a pull may examine in total (Limits.MaxResyncScan).
const resyncScanBatch = 4096

// runResync visits the live rows of the new-subscribed entities in
// (entity, key) order, classifies each under both scopes, and appends only the
// differences to patch. Rows with Version > seq are skipped (catch-up pull
// territory).
//
// Which rows it visits per entity is decided per entity, and the two ways of
// deciding produce the same patch:
//
//   - CANDIDATES, when both scopes decide the entity by predicates and window
//     membership alone and every predicate is served by an index. Only a row
//     matching some predicate or holding a place in some window can change
//     visibility, so the diff reads those and nothing else — the cost of a
//     scope change is then the size of the two scopes rather than the size of
//     the workspace (see resyncCandidates).
//   - a SCAN of the entity, for everything else: a scope covering the whole
//     entity, an entity reached through a follow spec, a predicate no index
//     serves, or a candidate set too large to be worth narrowing.
//
// emitLimit bounds the changes appended — the client's page size — and
// Limits.MaxResyncScan the rows examined. The second is much the larger of the
// two: rows visible under both scopes emit nothing, so a budget at the page
// size would end most pages having found no difference at all and charge the
// client a round trip for it. Sized well above the page, the resync pages by
// changes emitted and MaxResyncScan is what still bounds the work of one pull.
// Returns where the next page must resume, or done=true when the diff is
// complete.
//
// The resume position means the same thing on both paths — the last row key
// examined in this entity, in key order — so a page that narrowed candidates
// and one that scanned can follow each other without skipping or repeating a
// row.
func runResync(tx storage.ReadTx, oldEm, newEm *rowEmitter, seq uint64, pos resyncPos, emitLimit int, limits Limits, patch *syncplanev1.Patch) (next resyncPos, done bool, err error) {
	scanLimit := limits.MaxResyncScan
	scanned, emitted := 0, 0
	// emitDiff appends the difference between the two verdicts for one row, if
	// there is one. Rows the client could not hold yet are the catch-up pull's.
	emitDiff := func(row storage.Row) error {
		if row.Version > seq {
			return nil
		}
		oldClass, err := oldEm.classify(row)
		if err != nil {
			return err
		}
		newClass, err := newEm.classify(row)
		if err != nil {
			return err
		}
		oldVis, newVis := oldClass == rowVisible, newClass == rowVisible
		switch {
		case oldVis && !newVis:
			patch.Changes = append(patch.Changes, hiddenChange(row))
			emitted++
		case !oldVis && newVis:
			patch.Changes = append(patch.Changes, rowChange(row))
			emitted++
		}
		return nil
	}

	for _, entity := range newEm.entities() {
		if entity < pos.entity { // entities() is sorted; skip completed ones
			continue
		}
		afterKey := ""
		if entity == pos.entity {
			afterKey = pos.afterKey
		}
		if emitted >= emitLimit || scanned >= scanLimit {
			return resyncPos{entity: entity, afterKey: afterKey}, false, nil
		}
		keys, narrowed, err := resyncCandidates(tx, oldEm, newEm, entity, scanLimit, limits)
		if err != nil {
			return resyncPos{}, false, err
		}
		if narrowed {
			// Only the part of the candidate set this page has not reached.
			keys = keys[sort.Search(len(keys), func(i int) bool { return keys[i] > afterKey }):]
			for start := 0; start < len(keys); start += maxRowBatch {
				if emitted >= emitLimit || scanned >= scanLimit {
					return resyncPos{entity: entity, afterKey: afterKey}, false, nil
				}
				chunk := keys[start:min(start+maxRowBatch, len(keys))]
				rows := make(map[string]storage.Row, len(chunk))
				if err := tx.GetRows(entity, chunk, func(row storage.Row) error {
					rows[row.Key] = row
					return nil
				}); err != nil {
					return resyncPos{}, false, err
				}
				// Keyed by the CANDIDATE rather than by the rows that came
				// back, so the resume position advances over a candidate whose
				// row is gone as well as over one that produced a change.
				for _, key := range chunk {
					scanned++
					afterKey = key
					row, live := rows[key]
					// GetRows yields tombstones, and a window member may be
					// carried on a cursor past its own deletion. A deleted row
					// needs no resync work for the same reason the scan path
					// never sees one.
					if !live || row.Deleted {
						continue
					}
					if err := emitDiff(row); err != nil {
						return resyncPos{}, false, err
					}
					if emitted >= emitLimit {
						return resyncPos{entity: entity, afterKey: afterKey}, false, nil
					}
				}
			}
			continue
		}
		for {
			if emitted >= emitLimit || scanned >= scanLimit {
				return resyncPos{entity: entity, afterKey: afterKey}, false, nil
			}
			// The whole batch is buffered before it is classified, so the
			// batch size — not the pull's scan budget, which is far larger —
			// is what decides peak memory here. Row values are unbounded up to
			// Limits.MaxValueBytes.
			budget := min(scanLimit-scanned, resyncScanBatch)
			var page []storage.Row
			if err := tx.ScanEntityAfter(entity, afterKey, budget, func(row storage.Row) error {
				page = append(page, row)
				return nil
			}); err != nil {
				return resyncPos{}, false, err
			}
			for _, row := range page {
				scanned++
				afterKey = row.Key
				if err := emitDiff(row); err != nil {
					return resyncPos{}, false, err
				}
				if emitted >= emitLimit {
					return resyncPos{entity: entity, afterKey: afterKey}, false, nil
				}
			}
			// ScanEntityAfter excludes tombstones before applying the limit,
			// so a short page proves the entity is exhausted. Deleted rows
			// need no resync work: a deletion at or below the cursor's seq
			// already reached the client as a tombstone (or the client never
			// saw the row), and later ones are the catch-up pull's job.
			if len(page) < budget {
				break
			}
		}
	}
	return resyncPos{}, true, nil
}

// resyncCandidates returns the row keys of one entity whose visibility can
// differ between the two scopes, sorted in key order. ok is false when the
// entity has to be scanned instead, which is always legal and is what every
// decline below means.
//
// Soundness. Take a live row of this entity that matches no predicate of
// either scope and is in neither scope's window membership, and walk classify
// with it under each emitter in turn. The entity is registered and not
// server-only (entities() yields no others). The row is not a tombstone — the
// caller skips those. It is not a closure member: a resync diff leaves closure
// empty, which is what stops followed rows being spuriously tombstoned, and
// resyncNarrowable declines an entity any follow spec can reach. It is not a
// window member, by assumption. What is left is the base scope, which
// resyncNarrowable has pinned to one of two shapes on each side: the side does
// not subscribe to the entity, so classify answers rowUnsubscribed; or it has
// a scope that is neither full sync nor whole, so needPred is true and
// scope.matches is false and classify answers rowHidden or, for a purely
// windowed scope, rowHiddenNonMember. None of the three is rowVisible, so both
// sides agree the row is not visible and the diff emits nothing for it. Every
// row that CAN produce a change is therefore in the union collected here.
//
// Quarantined rows agree between the two paths as well. A row that will not
// decode cannot satisfy a predicate, so classify hides it under both scopes —
// the same verdict the scan reaches — whether or not stale index entries still
// name it as a candidate here.
//
// Read filters do not narrow the candidates and must not: a filter only ever
// hides a row that some predicate or window already admitted, the two sides
// evaluate DIFFERENT filters (the old emitter's is the cursor's epoch), and
// classify applies the right one per side.
func resyncCandidates(tx storage.ReadTx, oldEm, newEm *rowEmitter, entity string, maxCandidates int, limits Limits) (keys []string, ok bool, err error) {
	oldScope, ok := resyncNarrowable(oldEm, entity)
	if !ok {
		return nil, false, nil
	}
	newScope, ok := resyncNarrowable(newEm, entity)
	if !ok {
		return nil, false, nil
	}
	info, registered := newEm.srv.catalogPtr().entities[entity]
	if !registered {
		return nil, false, nil
	}

	found := make(map[string]struct{})
	// Past the cap the narrowing has stopped paying: a predicate matching most
	// of an entity costs more through an index than through the scan, and the
	// point of MaxResyncScan is to bound what one pull holds and examines.
	over := false
	add := func(key string) {
		if _, dup := found[key]; dup {
			return
		}
		if len(found) >= maxCandidates {
			over = true
			return
		}
		found[key] = struct{}{}
	}

	for _, scope := range [2]*entityScope{oldScope, newScope} {
		if scope == nil {
			continue
		}
		for _, pred := range scope.preds {
			// An unbounded page and a want() that never stops: this walk needs
			// every candidate the index names, not a page of them. What it
			// offers is a superset of the predicate's matches, which is all
			// the diff needs — the residual is re-checked by classify, against
			// the right scope and the right filter.
			served, err := queryByIndex(tx, info, pred, nil, pageFor(nil, nil, 0, 0), limits,
				func(row storage.Row, _ *query.Compiled) error {
					add(row.Key)
					return nil
				},
				func() int { return math.MaxInt },
				// Nothing to hand back: a decline abandons the whole candidate
				// set, and the caller scans the entity instead.
				func() {},
			)
			if err != nil {
				return nil, false, err
			}
			if !served || over {
				return nil, false, nil
			}
		}
	}
	// A window's membership is the scope, so its members are candidates
	// whatever the predicates say — and the window-only case is nothing else.
	for _, em := range [2]*rowEmitter{oldEm, newEm} {
		for ck := range em.windowMembers {
			if ck.entity == entity {
				add(ck.key)
			}
		}
	}
	if over {
		return nil, false, nil
	}

	keys = make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	// Key order is what makes the resume position mean the same thing here as
	// it does on the scan path.
	sort.Strings(keys)
	return keys, true, nil
}

// resyncNarrowable reports this entity's base scope under one side of a resync
// when that side's visibility is decided by predicates and window membership
// alone. ok is false when it is not, and the entity must be scanned.
//
// A nil scope with ok=true is the side that does not subscribe to the entity
// at all: nothing of it is visible there, so it contributes no candidates.
func resyncNarrowable(e *rowEmitter, entity string) (*entityScope, bool) {
	if e.scopes == nil {
		return nil, false // full sync: every row is visible
	}
	if e.followEntities[entity] {
		// Reached through a follow spec, so visibility is closure membership,
		// which no predicate describes.
		return nil, false
	}
	if len(e.closure) > 0 {
		// A resync diff runs with an empty closure; anything else is a
		// visibility rule this narrowing does not model.
		return nil, false
	}
	scope := e.scopes[entity]
	if scope == nil {
		return nil, true
	}
	if scope.whole {
		return nil, false // every live row is visible, so every one can differ
	}
	return scope, true
}
