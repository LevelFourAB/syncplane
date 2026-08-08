// Delta resync: when a pull's subscription set or filter epoch no longer
// matches its cursor, the server used to reply reset=true and force a full
// re-bootstrap — correct but blunt, since a single permission change would
// stampede every client in the workspace at once. Instead, the server now
// reconstructs the visibility rules the cursor was issued under, scans the
// workspace's live rows, and ships only the difference between the old
// visible set and the new one:
//
//	visible → hidden     tombstone (the client drops the row)
//	hidden  → visible    value
//	unchanged            nothing
//	entity left the set  one Patch.drop_entities name, instead of per-row tombstones
//
// The scan covers only rows with Version <= the cursor's seq — exactly the
// rows the client could already hold. Anything written later is the job of
// the catch-up pull that follows the resync (the completed cursor keeps the
// ORIGINAL seq under the new scope), which also makes the paged scan safe
// without a pinned snapshot: a row edited mid-resync gains a version past
// the cursor's seq, and the catch-up pull re-delivers or tombstones it under
// the new visibility, overwriting whatever the scan decided.
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

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
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

// resyncPos is a position in the (entity, key)-ordered resync scan.
type resyncPos struct {
	entity   string
	afterKey string
}

// resyncStep advances a delta resync by one pull page and encodes the
// resulting cursor: mid-resync cursors keep the old scope (which still
// describes the bulk of the client's rows) plus the scan position; on
// completion the new scope swaps in at the ORIGINAL seq so the next pull
// catches up on everything written since, under the new visibility.
//
// Both emitters' windows are rebuilt HERE, per page, rather than stored in
// ResyncState. That keeps the cursor small, and it is safe: the scan visits
// each row once in (entity, key) order, rows changed since the cursor carry
// version > cur.seq and belong to the catch-up pull, and a re-emitted row
// applies idempotently. The price is one bounded-memory scan per emitter per
// page during a scope change — a rare event, and the honest cost of having no
// secondary index.
func (s *Server) resyncStep(tx storage.ReadTx, cur cursorState, oldEm, newEm *rowEmitter, wantHash, epoch uint64, subs []*syncplanev1.Subscription, pos resyncPos, limit int, resp *syncplanev1.PullResponse) error {
	var err error
	if oldEm.windowMembers, err = oldEm.buildWindows(tx); err != nil {
		return err
	}
	if newEm.windowMembers, err = newEm.buildWindows(tx); err != nil {
		return err
	}
	next, done, err := runResync(tx, oldEm, newEm, cur.seq, pos, limit, s.limits.MaxPullChanges, resp.Patch)
	if err != nil {
		return err
	}
	if done {
		// Keep any same-version sub-position from the cursor that entered the
		// resync: the catch-up pull resumes from the original seq under the
		// new scope and must not skip the undelivered tail of a split group.
		resp.NextCursor = encodeCursor(cursorState{
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
		})
	} else {
		st := cur
		st.resync = &syncplanecursorv1.ResyncState{
			Entity:        next.entity,
			AfterKey:      next.afterKey,
			ToSubHash:     wantHash,
			ToFilterEpoch: epoch,
		}
		resp.NextCursor = encodeCursor(st)
	}
	// Either way the client must pull again: mid-resync to continue the
	// scan, after completion to catch up from the original seq.
	resp.HasMore = true
	resp.Seq = cur.seq
	return nil
}

// runResync scans live rows of the new-subscribed entities in (entity, key)
// order, classifies each under both scopes, and appends only the differences
// to patch. Rows with Version > seq are skipped (catch-up pull territory).
//
// emitLimit bounds appended changes (the client's page size) and scanLimit
// bounds rows considered, so a low-delta resync over a large workspace still
// does bounded work per pull. Returns where the next page must resume, or
// done=true when the scan is complete.
func runResync(tx storage.ReadTx, oldEm, newEm *rowEmitter, seq uint64, pos resyncPos, emitLimit, scanLimit int, patch *syncplanev1.Patch) (next resyncPos, done bool, err error) {
	scanned, emitted := 0, 0
	for _, entity := range newEm.entities() {
		if entity < pos.entity { // entities() is sorted; skip completed ones
			continue
		}
		afterKey := ""
		if entity == pos.entity {
			afterKey = pos.afterKey
		}
		for {
			if emitted >= emitLimit || scanned >= scanLimit {
				return resyncPos{entity: entity, afterKey: afterKey}, false, nil
			}
			budget := scanLimit - scanned
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
				if row.Version > seq {
					continue // not yet at the client; the catch-up pull delivers it
				}
				oldClass, err := oldEm.classify(row)
				if err != nil {
					return resyncPos{}, false, err
				}
				newClass, err := newEm.classify(row)
				if err != nil {
					return resyncPos{}, false, err
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
