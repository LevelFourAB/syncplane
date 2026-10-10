package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A cursor records what its caller could read when it was issued, so that a
// pull can tell when that changed — a grant added or removed, a root role
// gained, an anchor deleted — and resync the rows under the anchors that
// moved. The record itself lives in the cursor-state store and the cursor
// carries a hash and a reference: a resync tombstones the rows the record
// names, so a client must not be able to write it.

// readAccessOf renders what set holds for the permissions entities read by,
// with the caller's subjects and the model's read rules, as a cursor records
// it.
func readAccessOf(model *accessModel, caller *accessCaller, set *accessSet) *syncplanecursorv1.ReadAccess {
	ra := &syncplanecursorv1.ReadAccess{Subjects: slices.Clone(caller.subjects), Rules: model.readRules}
	for _, perm := range model.readPerms {
		a := set.perms[perm]
		if a == nil || (!a.root && len(a.rows) == 0) {
			continue
		}
		pa := &syncplanecursorv1.PermissionAnchors{Permission: perm, Root: a.root}
		if !a.root {
			entities, keys := anchorRowsByEntity(a.rows)
			for _, entity := range entities {
				pa.Rows = append(pa.Rows, &syncplanecursorv1.ClosureSet_Entry{Entity: entity, Keys: keys[entity]})
			}
		}
		ra.Permissions = append(ra.Permissions, pa)
	}
	return ra
}

// anchorRowsByEntity groups rows by entity, for the deterministic form a
// cursor records: the entities sorted, and each one's keys sorted.
func anchorRowsByEntity(rows map[anchorRow]struct{}) ([]string, map[string][]string) {
	keys := map[string][]string{}
	for at := range rows {
		keys[at.entity] = append(keys[at.entity], at.key)
	}
	entities := make([]string, 0, len(keys))
	for entity, ks := range keys {
		entities = append(entities, entity)
		sort.Strings(ks)
	}
	sort.Strings(entities)
	return entities, keys
}

// readAccessHash fingerprints what a ReadAccess holds: its subjects and
// anchors. The rules are left out, so a change of rules that leaves them
// valid keeps the hash. Never 0, which a cursor reserves for a schema without
// the access model.
func readAccessHash(ra *syncplanecursorv1.ReadAccess) uint64 {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&syncplanecursorv1.ReadAccess{
		Subjects:    ra.GetSubjects(),
		Permissions: ra.GetPermissions(),
	})
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	sum := sha256.Sum256(raw)
	if h := binary.BigEndian.Uint64(sum[:8]); h != 0 {
		return h
	}
	return 1
}

// accessSetOf turns a stored ReadAccess back into the set it was rendered
// from, restricted to the permissions entities read by.
func accessSetOf(ra *syncplanecursorv1.ReadAccess) *accessSet {
	set := &accessSet{perms: map[string]*permAnchors{}}
	for _, pa := range ra.GetPermissions() {
		a := &permAnchors{root: pa.GetRoot()}
		for _, entry := range pa.GetRows() {
			for _, key := range entry.GetKeys() {
				if a.rows == nil {
					a.rows = map[anchorRow]struct{}{}
				}
				a.rows[anchorRow{entity: entry.GetEntity(), key: key}] = struct{}{}
			}
		}
		set.perms[pa.GetPermission()] = a
	}
	return set
}

// boundReadAccess returns the ReadAccess and hash of the access bound on the
// emitter, rendered once per binding.
func (e *rowEmitter) boundReadAccess() (*syncplanecursorv1.ReadAccess, uint64) {
	if e.readAccess == nil {
		e.readAccess = readAccessOf(e.caller.model, e.caller, e.access)
		e.readAccessHash = readAccessHash(e.readAccess)
	}
	return e.readAccess, e.readAccessHash
}

// stampAccess records the emitter's bound access on a cursor about to be
// issued. cur is the cursor the request came with: when its access matches,
// the stored entry it names is reused rather than written again.
func (e *rowEmitter) stampAccess(st *cursorState, cur cursorState) {
	st.accessPolicy, st.accessHash, st.accessID, st.accessGen, st.accessPending = 0, 0, 0, 0, nil
	st.callerAccessHash, st.callerAccessID, st.callerAccessGen, st.callerAccessPending = 0, 0, 0, nil
	st.movesThrough = 0
	if e.caller == nil {
		return
	}
	st.movesThrough = e.movesThrough
	e.stampCallerAccess(st, cur)
	ra, hash := e.boundReadAccess()
	st.accessPolicy = e.caller.model.fingerprint
	st.accessHash = hash
	st.accessID = cur.accessID
	// The stored entry also holds the rules, so new rules need a new entry.
	if cur.accessHash == hash && cur.accessPolicy == st.accessPolicy && cur.accessGen != 0 {
		st.accessGen = cur.accessGen
		return
	}
	st.accessPending = ra
}

// stampCallerAccess records the emitter's bound CallerAccess on st, and the
// stored copy a later CallerAccessDelta is computed from. Only a client that
// applies deltas needs that copy, so for any other client a changed
// CallerAccess writes no entry.
func (e *rowEmitter) stampCallerAccess(st *cursorState, cur cursorState) {
	ca, hash := e.boundCallerAccess()
	st.callerAccessHash = hash
	st.callerAccessID = cur.callerAccessID
	if cur.callerAccessHash == hash && cur.callerAccessGen != 0 {
		st.callerAccessGen = cur.callerAccessGen
		return
	}
	if e.callerAccessDeltas {
		st.callerAccessPending = ca
	}
}

// storeAccess writes a pending ReadAccess to the cursor-state store and
// stamps its reference onto c. Without a store, or when the write fails, the
// cursor carries the hash alone, and a later change of access resets the
// client instead of resyncing it.
func (s *Server) storeAccess(ctx context.Context, ws string, st *cursorState, c *syncplanecursorv1.Cursor) {
	if st.accessPending != nil {
		st.accessID, st.accessGen = s.putAccessEntry(ctx, ws, st.accessID, st.accessPending)
		st.accessPending = nil
	}
	if st.callerAccessPending != nil {
		// The entry carries its version, so a pull that reads it back can
		// tell it is the one the cursor names without hashing it again.
		ca := st.callerAccessPending
		entry := &syncplanev1.CallerAccess{
			Subjects:        ca.GetSubjects(),
			RootPermissions: ca.GetRootPermissions(),
			AnchorGroups:    ca.GetAnchorGroups(),
			Version:         st.callerAccessHash,
		}
		st.callerAccessID, st.callerAccessGen = s.putAccessEntry(ctx, ws, st.callerAccessID, entry)
		st.callerAccessPending = nil
	}
	c.AccessPolicy = st.accessPolicy
	c.AccessHash = st.accessHash
	c.CallerAccessHash = st.callerAccessHash
	c.AccessMovesThrough = st.movesThrough
	if st.accessGen != 0 {
		c.AccessRef = &syncplanecursorv1.StateRef{Id: st.accessID, Gen: st.accessGen}
	}
	if st.callerAccessGen != 0 {
		c.CallerAccessRef = &syncplanecursorv1.StateRef{Id: st.callerAccessID, Gen: st.callerAccessGen}
	}
}

// putAccessEntry writes m as a new entry of lineage id, minting the lineage
// when id is 0, and returns the entry's reference. It returns 0, 0 without a
// store or when the write fails.
func (s *Server) putAccessEntry(ctx context.Context, ws string, id uint64, m proto.Message) (uint64, uint64) {
	if s.cursorState == nil {
		return 0, 0
	}
	blob, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	ref := cursorstate.Ref{ID: id, Gen: randNonZero()}
	if ref.ID == 0 {
		ref.ID = randNonZero()
	}
	if err := s.cursorState.Put(ctx, ws, ref, blob); err != nil {
		s.tel().cursorStatePutFailures.Add(ctx, 1)
		return 0, 0
	}
	return ref.ID, ref.Gen
}

// accessComparison is how the access bound on a request's emitter relates to
// the access its cursor was issued under.
type accessComparison struct {
	// hash is the bound access's hash, 0 for a schema without the model.
	hash uint64
	// reset is set when the cursor cannot be served incrementally.
	reset resetReason
	// old is what the cursor was issued under when that differs from what is
	// bound now, nil when it does not.
	old *accessSet
	// oldChains walks parent chains as the client's rows reflect them: with
	// the parent each row moved since then had, and what within entries read
	// of it, before its first move. Nil when no row moved.
	oldChains *accessChains
	// moved are the rows whose moves change what the caller can read, in the
	// order they first moved.
	moved []anchorRow
}

// resyncs reports whether the client's rows must be resynced: the caller's
// anchors changed, or a row moved in a way that changes what it can read.
func (c accessComparison) resyncs() bool {
	return c.old != nil || len(c.moved) > 0
}

const (
	resetAccessPolicy      resetReason = "access_policy_changed"
	resetAccessUnavailable resetReason = "access_state_unavailable"
	resetAccessMoves       resetReason = "access_moves_overflow"
)

// compareAccess compares the access bound on e with what cur was issued
// under, and finds the access moves after cur that change what the caller can
// read. haveCursor is false for a request without a cursor, which has nothing
// to compare. Runs inside the request's read transaction tx, after
// bindAccess.
func (s *Server) compareAccess(ctx context.Context, ws string, tx storage.ReadTx, cur cursorState, haveCursor bool, e *rowEmitter) (accessComparison, error) {
	var cmp accessComparison
	policy := uint64(0)
	if e.caller != nil {
		policy = e.caller.model.fingerprint
		_, cmp.hash = e.boundReadAccess()
	}
	if !haveCursor {
		if e.caller != nil && e.caller.model.moving {
			// The rows the client is about to receive are classified with the
			// parents rows have now.
			_, newest, _, err := readAccessMoves(tx, ^uint64(0), 0)
			if err != nil {
				return cmp, err
			}
			e.movesThrough = newest
		}
		return cmp, nil
	}
	// A schema that adopted or dropped the model has no rules to compare.
	if (cur.accessPolicy == 0) != (policy == 0) {
		cmp.reset = resetAccessPolicy
		return cmp, nil
	}
	if e.caller == nil {
		return cmp, nil
	}
	if cur.resync != nil && cur.resync.GetToAccessHash() != cmp.hash {
		cmp.reset = resetStaleResync
		return cmp, nil
	}
	if cur.accessPolicy != policy || cur.accessHash != cmp.hash {
		old, err := e.loadReadAccess(cur)
		if err != nil {
			return cmp, err
		}
		switch {
		case old == nil && cur.accessPolicy != policy:
			cmp.reset = resetAccessPolicy
		case old == nil:
			cmp.reset = resetAccessUnavailable
		case cur.accessPolicy != policy && !sameSharedReadRules(old.GetRules(), e.caller.model.readRules):
			cmp.reset = resetAccessPolicy
		case cur.accessHash != cmp.hash:
			cmp.old = accessSetOf(old)
		}
		if cmp.reset != resetNone {
			return cmp, nil
		}
	}
	return cmp, s.compareMoves(ctx, ws, tx, cur, e, &cmp)
}

// compareMoves reads the access moves the client's rows do not reflect yet,
// the ones after the greater of the cursor's seq and its movesThrough, and
// records on cmp the rows whose moves change what the caller can read. It
// sets e.movesThrough to the newest move it read.
//
// A move changes what the caller can read when, for some entity whose rows can
// sit below the moved row, the caller could read it on the moved row's chain
// as the client's rows reflect it and cannot on the chain now, or the other
// way around. The first side reads with the cursor's access and with each
// moved row's parent, and what within entries read of it, from before its
// first move; the second with the access and rows bound now.
func (s *Server) compareMoves(ctx context.Context, ws string, tx storage.ReadTx, cur cursorState, e *rowEmitter, cmp *accessComparison) error {
	model := e.caller.model
	e.movesThrough = cur.movesThrough
	if !model.moving {
		return nil
	}
	floor := max(cur.seq, cur.movesThrough)
	moves, newest, overflow, err := readAccessMoves(tx, floor, s.limits.MaxResyncScan)
	if err != nil {
		return err
	}
	if overflow {
		cmp.reset = resetAccessMoves
		return nil
	}
	e.movesThrough = max(cur.movesThrough, newest)
	if len(moves) == 0 {
		return nil
	}
	report := func(row storage.Row, err error) { s.reportPoison(ctx, ws, row, err) }
	oldSet := cmp.old
	if oldSet == nil {
		oldSet = e.access
	}
	cmp.oldChains = newAccessChains(model, tx, s.limits.maxAccessDepth(), report)
	cmp.oldChains.overrides, _ = firstMoves(moves)
	cmp.oldChains.withinOverrides = firstWithins(model, moves)
	cmp.moved, err = movedForCaller(model, e.caller.subjects, cmp.oldChains, oldSet, e.chains, e.access, moves)
	if err != nil {
		return err
	}
	// A resync in flight has diffed part of the rows against the parents rows
	// had when it began. A later move that changes what the caller reads
	// relative to those parents leaves those rows wrong, even when it undoes
	// an earlier move and changes nothing relative to the cursor. The access
	// itself cannot have changed: compareAccess resets on that first.
	if cur.resync != nil {
		var later []accessMove
		for _, m := range moves {
			if m.version > cur.resync.GetToMovesThrough() {
				later = append(later, m)
			}
		}
		if len(later) > 0 {
			began := newAccessChains(model, tx, s.limits.maxAccessDepth(), report)
			began.overrides, _ = firstMoves(later)
			began.withinOverrides = firstWithins(model, later)
			stale, err := movedForCaller(model, e.caller.subjects, began, e.access, e.chains, e.access, later)
			if err != nil {
				return err
			}
			if len(stale) > 0 {
				cmp.reset = resetStaleResync
			}
		}
	}
	return nil
}

// firstMoves returns, for each row that moves, the parent it had before its
// first move, and the rows in the order they first moved.
func firstMoves(moves []accessMove) (map[anchorRow]chainEdge, []anchorRow) {
	first := map[anchorRow]chainEdge{}
	var rows []anchorRow
	for _, m := range moves {
		if _, seen := first[m.row]; !seen {
			first[m.row] = m.from
			rows = append(rows, m.row)
		}
	}
	return first, rows
}

// firstWithins returns, for each row that moves and whose entity a within
// entry names, what the within checks read of it before its first move.
func firstWithins(model *accessModel, moves []accessMove) map[anchorRow]rowWithin {
	var first map[anchorRow]rowWithin
	for _, m := range moves {
		if len(model.withinOn[m.row.entity]) == 0 {
			continue
		}
		if first == nil {
			first = map[anchorRow]rowWithin{}
		}
		if _, seen := first[m.row]; !seen {
			first[m.row] = m.fromWithin
		}
	}
	return first
}

// movedForCaller returns, in the order they first moved, the rows of moves
// whose chains let the caller read some entity below them on one side and not
// on the other: was and wasSet read the chains before the moves, is and isSet
// now. subjects are the caller's, sorted.
func movedForCaller(
	model *accessModel,
	subjects []string,
	was *accessChains,
	wasSet *accessSet,
	is *accessChains,
	isSet *accessSet,
	moves []accessMove,
) ([]anchorRow, error) {
	_, rows := firstMoves(moves)
	var out []anchorRow
	for _, row := range rows {
		if model.entities[row.entity] == nil {
			continue // the entity left the model, which a change of policy resets
		}
		for _, r := range model.readersBelow(row.entity) {
			changed, err := readsChange(model, subjects, was, wasSet, is, isSet, r, row)
			if err != nil {
				return nil, err
			}
			if changed {
				out = append(out, row)
				break
			}
		}
	}
	return out, nil
}

// readsChange reports whether the rows of an entity read by r, a
// readers.loose result, that sit below row can be read on one side and not on
// the other: was and wasSet read the chains on one side, is and isSet on the
// other, for a caller with subjects. A rule in r.checked admits other rows
// than r.perms do, or needs its permissions together, which can each change
// on their own, so each is compared apart from r.perms and from the others.
//
// A rule with within entries can also change for the rows below row when
// what its entries read at row or above it changes. That shows the rows to a
// caller that holds the rule's permissions where they sit, which can be at a
// row below row as well as at row or above it.
func readsChange(
	model *accessModel,
	subjects []string,
	was *accessChains,
	wasSet *accessSet,
	is *accessChains,
	isSet *accessSet,
	r readers,
	row anchorRow,
) (bool, error) {
	before, err := was.holdsAt(wasSet.readAnchors(r), row)
	if err != nil {
		return false, err
	}
	after, err := is.holdsAt(isSet.readAnchors(r), row)
	if err != nil || before != after {
		return before != after, err
	}
	if before {
		// One of r.perms admits every row on both sides.
		return false, nil
	}
	for _, o := range r.checked {
		perms := o.requires
		before, err := was.holdsAllAt(wasSet.anchorsOf(perms), row)
		if err != nil {
			return false, err
		}
		after, err := is.holdsAllAt(isSet.anchorsOf(perms), row)
		if err != nil || before != after {
			return before != after, err
		}
		if !o.checksAbove() {
			continue
		}
		wasWithin, err := was.withinAllAt(o, subjects, row)
		if err != nil {
			return false, err
		}
		isWithin, err := is.withinAllAt(o, subjects, row)
		if err != nil {
			return false, err
		}
		if wasWithin == isWithin {
			continue
		}
		if before || mayHoldBelow(model, wasSet, perms, row) || mayHoldBelow(model, isSet, perms, row) {
			return true, nil
		}
	}
	return false, nil
}

// mayHoldBelow reports whether set can hold each of perms at some row below
// row: each is held at the workspace root, or at a row of an entity whose rows
// can sit below rows of row's entity. It reads nothing, so it can report true
// for an anchor that sits elsewhere.
func mayHoldBelow(model *accessModel, set *accessSet, perms []string, row anchorRow) bool {
	below := model.below[row.entity]
	for _, p := range perms {
		a := set.perms[p]
		if a == nil {
			return false
		}
		if a.root {
			continue
		}
		found := false
		for at := range a.rows {
			if _, ok := slices.BinarySearch(below, at.entity); ok {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// accessUnchanged reports whether cur was issued under the access model and
// the read access bound on e. It reads nothing, which is all a caller needs
// that restarts rather than resyncs on a difference.
func (e *rowEmitter) accessUnchanged(cur cursorState) bool {
	if e.caller == nil {
		return cur.accessPolicy == 0
	}
	_, hash := e.boundReadAccess()
	return cur.accessPolicy == e.caller.model.fingerprint && cur.accessHash == hash
}

// loadReadAccess reads the ReadAccess a cursor names. It returns nil, nil when
// the entry is missing, damaged, does not match the cursor's hashes, or was
// recorded for other subjects than the caller's: the cursor cannot resync
// from it.
func (e *rowEmitter) loadReadAccess(cur cursorState) (*syncplanecursorv1.ReadAccess, error) {
	ra := &syncplanecursorv1.ReadAccess{}
	ok, err := e.loadAccessEntry(cur.accessID, cur.accessGen, ra)
	if errors.Is(err, cursorstate.ErrNotFound) {
		// Counted here and not in loadAccessEntry: this miss resets the
		// client, which is what the metric reports. A missing CallerAccess
		// entry only costs a whole summary.
		e.srv.tel().cursorStateMisses.Add(e.ctx, 1)
		return nil, nil
	}
	if !ok || err != nil {
		return nil, err
	}
	if !slices.Equal(ra.GetSubjects(), e.caller.subjects) || readAccessHash(ra) != cur.accessHash ||
		readPolicyOf(ra.GetRules()) != cur.accessPolicy {
		return nil, nil
	}
	return ra, nil
}

// loadAccessEntry reads the cursor-state entry (id, gen) into m. It reports
// false when gen is 0, there is no store, or the entry is damaged, and
// returns cursorstate.ErrNotFound when the store does not hold the entry.
func (e *rowEmitter) loadAccessEntry(id, gen uint64, m proto.Message) (bool, error) {
	s := e.srv
	if s.cursorState == nil || gen == 0 {
		return false, nil
	}
	blob, err := s.cursorState.Get(e.ctx, e.ws, cursorstate.Ref{ID: id, Gen: gen})
	if err != nil {
		return false, err
	}
	return proto.Unmarshal(blob, m) == nil, nil
}

// sameAnchors reports whether x and y hold their permissions at the same
// anchors. A nil value holds nothing.
func sameAnchors(x, y *permAnchors) bool {
	xRoot, yRoot := x != nil && x.root, y != nil && y.root
	if xRoot || yRoot {
		return xRoot == yRoot
	}
	var xRows, yRows map[anchorRow]struct{}
	if x != nil {
		xRows = x.rows
	}
	if y != nil {
		yRows = y.rows
	}
	if len(xRows) != len(yRows) {
		return false
	}
	for at := range xRows {
		if _, ok := yRows[at]; !ok {
			return false
		}
	}
	return true
}

// readAccessUnchanged reports whether the two sides of a resync read entity
// under the same anchors, for every permission its read rules and the read
// rules of its guards need. The subjects a caller_is check compares with are
// the same on both sides: a cursor recorded for other subjects cannot resync.
// A when check reads only the row, so it decides the same on both sides.
func readAccessUnchanged(oldEm, newEm *rowEmitter, entity string) bool {
	if newEm.caller == nil {
		return true
	}
	info, ok := newEm.srv.catalogPtr().entities[entity]
	if !ok || info.access == nil {
		return true
	}
	for _, r := range info.access.readerSets() {
		if !sameAnchors(oldEm.access.readAnchors(r), newEm.access.readAnchors(r)) {
			return false
		}
		for _, o := range r.checked {
			for _, p := range o.requires {
				if !sameAnchors(oldEm.access.perms[p], newEm.access.perms[p]) {
					return false
				}
			}
		}
	}
	return true
}

// rootLost returns the entities the caller could read at the workspace root
// under old and cannot under now. A resync drops each of them whole and sends
// what the caller can still read, which costs what the caller keeps rather
// than one tombstone per row it lost.
func rootLost(c *catalog, entities []string, old, now *accessSet) []string {
	var out []string
	for _, name := range entities {
		info := c.entities[name]
		if info == nil || info.access == nil {
			continue
		}
		o, n := old.readAnchors(info.access.read), now.readAnchors(info.access.read)
		if o != nil && o.root && (n == nil || !n.root) {
			out = append(out, name)
		}
	}
	return out
}

// denyAll is the read filter of an entity the caller reads no row of.
func denyAll(string, proto.Message) (bool, error) { return false, nil }

// hideOnOldSide makes oldEm read nothing of the entities named, so a resync
// sends every row of them the new side can read. It must run after the old
// side's access is bound.
func (e *rowEmitter) hideOnOldSide(entities []string) {
	for _, name := range entities {
		e.combined[name] = denyAll
	}
	e.hiddenEntities = entities
}

// accessDeltaCandidates returns, in key order, the keys of the rows of info
// whose visibility, or the fields a guard hides of them, can differ between
// the two sides of a resync whose scopes are the same and whose access
// differs: the anchors of the read permissions of the entity and of its
// guards that one side holds and the other does not, the rows below those
// anchors (see rowsBelow), and both sides' window members. ok is false when
// the entity must be scanned instead: the root was gained or lost for one of
// them, an index is not ready, or the set is larger than maxCandidates.
//
// An entity the old side reads nothing of (see hideOnOldSide) takes the new
// side's candidates instead: every row the new side can read.
func accessDeltaCandidates(tx storage.ReadTx, oldEm, newEm *rowEmitter, info *entityInfo, maxCandidates int) (keys []string, ok bool, err error) {
	if slices.Contains(oldEm.hiddenEntities, info.name) {
		keys, ok, err = newEm.accessCandidates(tx, info, maxCandidates)
		if err != nil || !ok {
			return nil, false, err
		}
		return withWindowMembers(keys, info.name, maxCandidates, newEm.windowMembers)
	}
	changed := map[anchorRow]struct{}{}
	var named []string
	anchored := false
	for _, r := range info.access.readerSets() {
		o, n := oldEm.access.readAnchors(r), newEm.access.readAnchors(r)
		oRoot, nRoot := o != nil && o.root, n != nil && n.root
		if oRoot && nRoot {
			// r admits every row on both sides, wherever its parents are.
			continue
		}
		if oRoot || nRoot {
			return nil, false, nil
		}
		anchored = true
		addChanged(changed, o, n)
		// A read rule in checked reads the rows below the anchors of each of
		// its own permissions that pass its checks. One with a permission that
		// moved to or from the root can show or hide every such row, wherever
		// it sits: the rows that name the caller for a caller_is check, the
		// rows within a row that names the caller for a within entry's
		// caller_is check, and any row otherwise, which no index finds.
		for _, rule := range r.checked {
			rooted := false
			for _, p := range rule.requires {
				ro, rn := oldEm.access.perms[p], newEm.access.perms[p]
				if (ro != nil && ro.root) != (rn != nil && rn.root) {
					rooted = true
					continue
				}
				addChanged(changed, ro, rn)
			}
			if !rooted {
				continue
			}
			var rows []string
			ok := false
			if rule.callerIs != nil {
				rows, ok, err = subjectRows(tx, info, rule.callerIs, newEm.caller.subjects, maxCandidates, newEm.srv.limits)
			} else {
				rows, ok, err = withinRows(tx, newEm.caller.model, info, rule, newEm.caller.subjects, maxCandidates, newEm.srv.limits)
			}
			if err != nil || !ok {
				return nil, false, err
			}
			if named, ok = mergeKeys(named, rows, maxCandidates); !ok {
				return nil, false, nil
			}
		}
	}
	if !anchored {
		// Every row and field is readable on both sides, wherever its parents
		// are.
		return withWindowMembers(nil, info.name, maxCandidates, oldEm.windowMembers, newEm.windowMembers)
	}
	from := make([]anchorRow, 0, len(changed)+len(oldEm.movedRows))
	for at := range changed {
		from = append(from, at)
	}
	// A row below a moved row sits below it on both sides, so the rows below
	// it now are the ones its move can show or hide.
	from = append(from, oldEm.movedRows...)
	keys, ok, err = rowsBelow(tx, newEm.caller.model, info, from, maxCandidates, newEm.srv.limits)
	if err != nil || !ok {
		return nil, false, err
	}
	if keys, ok = mergeKeys(keys, named, maxCandidates); !ok {
		return nil, false, nil
	}
	return withWindowMembers(keys, info.name, maxCandidates, oldEm.windowMembers, newEm.windowMembers)
}

// addChanged adds to changed the rows that one of x and y holds its
// permission at and the other does not. Neither may be held at the root.
func addChanged(changed map[anchorRow]struct{}, x, y *permAnchors) {
	for _, side := range [][2]*permAnchors{{x, y}, {y, x}} {
		if side[0] == nil {
			continue
		}
		for at := range side[0].rows {
			if side[1] == nil {
				changed[at] = struct{}{}
				continue
			}
			if _, held := side[1].rows[at]; !held {
				changed[at] = struct{}{}
			}
		}
	}
}

// withWindowMembers adds the members of entity in the given window
// memberships to keys and sorts them. A window ranks only the rows its side
// can read, so a change of access moves rows into and out of a window that
// sit under no changed anchor.
func withWindowMembers(keys []string, entity string, maxCandidates int, windows ...closureRows) ([]string, bool, error) {
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		seen[k] = struct{}{}
	}
	for _, w := range windows {
		for ck := range w {
			if ck.entity != entity {
				continue
			}
			if _, dup := seen[ck.key]; !dup {
				seen[ck.key] = struct{}{}
				keys = append(keys, ck.key)
			}
		}
	}
	if len(keys) > maxCandidates {
		return nil, false, nil
	}
	sort.Strings(keys)
	return keys, true, nil
}

// keyValuesOf parses a row key back into the values of the entity's key
// fields, in key-field order. ok is false for a key that does not parse as
// the entity's key.
func keyValuesOf(info *entityInfo, key string) ([]protoreflect.Value, bool) {
	if info == nil {
		return nil, false
	}
	parts := []string{key}
	if len(info.keyFields) > 1 {
		parts = splitRowKey(key)
	}
	if len(parts) != len(info.keyFields) {
		return nil, false
	}
	values := make([]protoreflect.Value, len(parts))
	for i, fd := range info.keyFields {
		part := parts[i]
		switch fd.Kind() {
		case protoreflect.StringKind:
			values[i] = protoreflect.ValueOfString(part)
		case protoreflect.BoolKind:
			b, err := strconv.ParseBool(part)
			if err != nil {
				return nil, false
			}
			values[i] = protoreflect.ValueOfBool(b)
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			n, err := strconv.ParseInt(part, 10, 32)
			if err != nil {
				return nil, false
			}
			values[i] = protoreflect.ValueOfInt32(int32(n))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			n, err := strconv.ParseInt(part, 10, 64)
			if err != nil {
				return nil, false
			}
			values[i] = protoreflect.ValueOfInt64(n)
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			n, err := strconv.ParseUint(part, 10, 32)
			if err != nil {
				return nil, false
			}
			values[i] = protoreflect.ValueOfUint32(uint32(n))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			n, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				return nil, false
			}
			values[i] = protoreflect.ValueOfUint64(n)
		case protoreflect.EnumKind:
			n, err := strconv.ParseInt(part, 10, 32)
			if err != nil {
				return nil, false
			}
			values[i] = protoreflect.ValueOfEnum(protoreflect.EnumNumber(n))
		default:
			return nil, false
		}
	}
	return values, true
}

// splitRowKey splits a composite row key into its parts, undoing the escaping
// naming.RowKey applies: a backslash escapes the next character, and an
// unescaped slash separates parts.
func splitRowKey(key string) []string {
	var parts []string
	var b strings.Builder
	escaped := false
	for _, r := range key {
		switch {
		case escaped:
			b.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '/':
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	return append(parts, b.String())
}

// bindResyncAccess sets up the old side of a resync for what the caller's
// access did, inside the request's read transaction. old, resync and ok are
// what the request decided from its cursor alone: the old side of a change of
// scope or filter epoch, and whether that resync is possible. It returns them
// as the access comparison leaves them.
//
// A change of access with nothing else changed starts a resync whose old side
// is the new side's scope with the cursor's access. A change of scope or
// filter epoch whose old side cannot be rebuilt stays a reset: an old side
// built from the new scope would hide that change from the diff. Any old side
// reads with the cursor's access when it differs, and with the current access
// otherwise, and reads nothing of an entity the caller could read at the root
// and no longer can.
func (s *Server) bindResyncAccess(
	ctx context.Context,
	ws string,
	cur cursorState,
	acc accessComparison,
	old *rowEmitter,
	resync, ok bool,
	newEm *rowEmitter,
) (*rowEmitter, bool, bool, error) {
	if old == nil && acc.resyncs() && !resync {
		var err error
		if old, err = newRowEmitter(ctx, s, ws, newEm.subs); err != nil {
			return nil, false, false, err
		}
		// Same scope and the same filter epoch, so the application's filters
		// are the ones the new side resolved before the snapshot opened.
		old.filters, old.filterKnown = newEm.filters, newEm.filterKnown
		old.caller = newEm.caller
		old.accessOnly = true
		resync, ok = true, true
	}
	if old == nil {
		return nil, resync, ok, nil
	}
	if cur.resync != nil && cur.resync.GetToSubHash() == cur.subHash && cur.resync.GetToFilterEpoch() == cur.epoch {
		old.accessOnly = true
	}
	chains := newEm.chains
	if acc.oldChains != nil {
		chains = acc.oldChains
	}
	old.movedRows = acc.moved
	old.movedBelow = map[string]bool{}
	for _, row := range acc.moved {
		for _, name := range newEm.caller.model.below[row.entity] {
			old.movedBelow[name] = true
		}
	}
	if acc.old == nil {
		old.useAccess(newEm.access, chains)
		return old, resync, ok, nil
	}
	old.useAccess(acc.old, chains)
	old.hideOnOldSide(rootLost(s.catalogPtr(), newEm.entities(), acc.old, newEm.access))
	return old, resync, ok, nil
}

// appendMissing appends to list the names it does not already hold.
func appendMissing(list, names []string) []string {
	for _, name := range names {
		if !slices.Contains(list, name) {
			list = append(list, name)
		}
	}
	return list
}

// callerAccessOf renders everything set holds, and the subjects of caller,
// for the client: the permissions held at the root, and each anchored row once,
// in the group of the permissions held there and not at the root.
func callerAccessOf(caller *accessCaller, set *accessSet) *syncplanev1.CallerAccess {
	ca := &syncplanev1.CallerAccess{Subjects: callerSubjects(caller)}
	var anchored []string
	for perm, a := range set.perms {
		switch {
		case a.root:
			ca.RootPermissions = append(ca.RootPermissions, perm)
		case len(a.rows) > 0:
			anchored = append(anchored, perm)
		}
	}
	sort.Strings(ca.RootPermissions)
	sort.Strings(anchored)

	// A row finds its set of permissions by steps, one per permission in
	// sorted order, from the set it holds so far to that set plus the
	// permission. Each step is one map lookup, and each distinct set is built
	// once, already sorted. Set 0 is the empty set.
	type step struct{ from, perm int }
	next := map[step]int{}
	sets := [][]string{nil}
	setOf := map[anchorRow]int{}
	for i, perm := range anchored {
		for row := range set.perms[perm].rows {
			from := setOf[row]
			to, ok := next[step{from, i}]
			if !ok {
				to = len(sets)
				sets = append(sets, append(slices.Clip(sets[from]), perm))
				next[step{from, i}] = to
			}
			setOf[row] = to
		}
	}
	rows := make([][]anchorRow, len(sets))
	for row, s := range setOf {
		rows[s] = append(rows[s], row)
	}
	for s, members := range rows {
		if len(members) == 0 {
			continue
		}
		slices.SortFunc(members, func(a, b anchorRow) int {
			if c := strings.Compare(a.entity, b.entity); c != 0 {
				return c
			}
			return strings.Compare(a.key, b.key)
		})
		g := &syncplanev1.AnchorGroup{Permissions: sets[s]}
		for start := 0; start < len(members); {
			end := start + 1
			for end < len(members) && members[end].entity == members[start].entity {
				end++
			}
			keys := make([]string, end-start)
			for i, row := range members[start:end] {
				keys[i] = row.key
			}
			g.Anchors = append(g.Anchors, &syncplanev1.AnchorRows{Entity: members[start].entity, Keys: keys})
			start = end
		}
		ca.AnchorGroups = append(ca.AnchorGroups, g)
	}
	slices.SortFunc(ca.AnchorGroups, func(a, b *syncplanev1.AnchorGroup) int {
		return slices.Compare(a.Permissions, b.Permissions)
	})
	return ca
}

// boundCallerAccess returns the CallerAccess of the access bound on the
// emitter and its hash, rendered once per binding. The hash leaves out seq,
// so it changes only when what the caller holds does.
func (e *rowEmitter) boundCallerAccess() (*syncplanev1.CallerAccess, uint64) {
	if e.callerAccess == nil {
		e.callerAccess = callerAccessOf(e.caller, e.access)
		e.callerAccessHash, e.callerAccessSize = callerAccessHash(e.callerAccess)
	}
	return e.callerAccess, e.callerAccessHash
}

// callerAccessHash fingerprints ca, which carries no seq and no version, and
// returns its encoded size. The hash is never 0, which a cursor reserves for
// a schema without the access model.
func callerAccessHash(ca *syncplanev1.CallerAccess) (uint64, int) {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(ca)
	if err != nil {
		panic(err) // cannot fail for a message of valid in-memory protos
	}
	sum := sha256.Sum256(raw)
	return binary.BigEndian.Uint64(sum[:8]) | 1, len(raw)
}

// callerAccessUpdate returns what a response sends of the bound
// CallerAccess, current through seq. Both are nil for a schema without the
// access model, and when the client holds the bound one already. A client
// that applies deltas is sent one when the cursor-state store holds the
// summary the client has; every other case sends the whole summary.
// haveCursor is false for a request without a cursor, whose client holds
// nothing.
func (e *rowEmitter) callerAccessUpdate(cur cursorState, haveCursor bool, seq uint64) (*syncplanev1.CallerAccess, *syncplanev1.CallerAccessDelta) {
	if e.caller == nil {
		return nil, nil
	}
	ca, hash := e.boundCallerAccess()
	held := e.heldCallerAccess
	if !e.callerAccessDeltas {
		held = 0
		if haveCursor {
			held = cur.callerAccessHash
		}
	}
	if held == hash {
		return nil, nil
	}
	e.heldCallerAccess = hash
	// The whole summary shares ca's lists, which nothing changes after the
	// binding renders them, so sending it costs no copy.
	whole := &syncplanev1.CallerAccess{
		Seq:             seq,
		Subjects:        ca.GetSubjects(),
		RootPermissions: ca.GetRootPermissions(),
		AnchorGroups:    ca.GetAnchorGroups(),
		Version:         hash,
	}
	if !e.callerAccessDeltas || held == 0 {
		return whole, nil
	}
	base := e.storedCallerAccess(cur, held)
	if base == nil {
		return whole, nil
	}
	delta := callerAccessDeltaOf(base, ca)
	if delta == nil {
		return whole, nil
	}
	delta.Seq, delta.BaseVersion, delta.Version = seq, held, hash
	// When most rows changed, the delta lists most rows, and the rows it
	// removes on top of them. Its size counts its seq and both versions, and
	// callerAccessSize leaves out the seq and version of the whole summary,
	// so a delta must be smaller by more than those few bytes to be sent.
	if proto.Size(delta) >= e.callerAccessSize {
		return whole, nil
	}
	return nil, delta
}

// storedCallerAccess returns the CallerAccess cur's caller_access_ref names
// when its version is version, or nil when the cursor names another one or
// the cursor-state store does not hold it. A store that fails counts as one
// without the entry: the whole summary is always a correct answer.
func (e *rowEmitter) storedCallerAccess(cur cursorState, version uint64) *syncplanev1.CallerAccess {
	if cur.callerAccessHash != version {
		return nil
	}
	stored := &syncplanev1.CallerAccess{}
	if ok, err := e.loadAccessEntry(cur.callerAccessID, cur.callerAccessGen, stored); !ok || err != nil {
		return nil
	}
	if stored.GetVersion() != version {
		return nil
	}
	return stored
}

// callerAccessDeltaOf returns the change from held to now, without seq and
// versions, or nil when no delta can carry it: the subjects or the root
// permissions differ.
func callerAccessDeltaOf(held, now *syncplanev1.CallerAccess) *syncplanev1.CallerAccessDelta {
	if !slices.Equal(held.GetSubjects(), now.GetSubjects()) ||
		!slices.Equal(held.GetRootPermissions(), now.GetRootPermissions()) {
		return nil
	}
	before := map[anchorRow][]string{}
	for _, g := range held.GetAnchorGroups() {
		for _, rows := range g.GetAnchors() {
			for _, key := range rows.GetKeys() {
				before[anchorRow{entity: rows.GetEntity(), key: key}] = g.GetPermissions()
			}
		}
	}
	delta := &syncplanev1.CallerAccessDelta{}
	// Walking now's groups in order keeps the delta's groups, and the rows in
	// each, in the order CallerAccess documents.
	for _, g := range now.GetAnchorGroups() {
		var changed []*syncplanev1.AnchorRows
		for _, rows := range g.GetAnchors() {
			var keys []string
			for _, key := range rows.GetKeys() {
				at := anchorRow{entity: rows.GetEntity(), key: key}
				perms, ok := before[at]
				delete(before, at)
				if !ok || !slices.Equal(perms, g.GetPermissions()) {
					keys = append(keys, key)
				}
			}
			if len(keys) > 0 {
				changed = append(changed, &syncplanev1.AnchorRows{Entity: rows.GetEntity(), Keys: keys})
			}
		}
		if len(changed) > 0 {
			delta.AnchorGroups = append(delta.AnchorGroups, &syncplanev1.AnchorGroup{Permissions: g.GetPermissions(), Anchors: changed})
		}
	}
	if len(before) > 0 {
		removed := make(map[anchorRow]struct{}, len(before))
		for at := range before {
			removed[at] = struct{}{}
		}
		entities, keys := anchorRowsByEntity(removed)
		for _, entity := range entities {
			delta.Removed = append(delta.Removed, &syncplanev1.AnchorRows{Entity: entity, Keys: keys[entity]})
		}
	}
	return delta
}
