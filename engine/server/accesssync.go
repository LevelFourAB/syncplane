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
// pull can tell when that changed — a grant added or removed, a global role
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

// anchorRowsByEntity groups rows by entity, for the deterministic forms a
// cursor and a client receive: the entities sorted, and each one's keys
// sorted.
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
	st.callerAccessHash = 0
	if e.caller == nil {
		return
	}
	_, st.callerAccessHash = e.boundCallerAccess()
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

// storeAccess writes a pending ReadAccess to the cursor-state store and
// stamps its reference onto c. Without a store, or when the write fails, the
// cursor carries the hash alone, and a later change of access resets the
// client instead of resyncing it.
func (s *Server) storeAccess(ctx context.Context, ws string, st *cursorState, c *syncplanecursorv1.Cursor) {
	if st.accessPending != nil {
		pending := st.accessPending
		st.accessPending, st.accessGen = nil, 0
		if s.cursorState == nil {
			st.accessID = 0
		} else {
			blob, err := proto.MarshalOptions{Deterministic: true}.Marshal(pending)
			if err != nil {
				panic(err) // cannot fail for a message of valid in-memory protos
			}
			ref := cursorstate.Ref{ID: st.accessID, Gen: randNonZero()}
			if ref.ID == 0 {
				ref.ID = randNonZero()
			}
			if err := s.cursorState.Put(ctx, ws, ref, blob); err != nil {
				s.tel().cursorStatePutFailures.Add(ctx, 1)
				st.accessID = 0
			} else {
				st.accessID, st.accessGen = ref.ID, ref.Gen
			}
		}
	}
	c.AccessPolicy = st.accessPolicy
	c.AccessHash = st.accessHash
	c.CallerAccessHash = st.callerAccessHash
	if st.accessGen != 0 {
		c.AccessRef = &syncplanecursorv1.StateRef{Id: st.accessID, Gen: st.accessGen}
	}
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
}

const (
	resetAccessPolicy      resetReason = "access_policy_changed"
	resetAccessUnavailable resetReason = "access_state_unavailable"
)

// compareAccess compares the access bound on e with what cur was issued
// under. haveCursor is false for a request without a cursor, which has
// nothing to compare. Runs inside the request's read transaction, after
// bindAccess.
func (s *Server) compareAccess(ctx context.Context, ws string, cur cursorState, haveCursor bool, e *rowEmitter) (accessComparison, error) {
	var cmp accessComparison
	policy := uint64(0)
	if e.caller != nil {
		policy = e.caller.model.fingerprint
		_, cmp.hash = e.boundReadAccess()
	}
	if !haveCursor {
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
	if cur.accessPolicy == policy && cur.accessHash == cmp.hash {
		return cmp, nil
	}
	old, err := s.loadReadAccess(ctx, ws, cur, e.caller)
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
	return cmp, nil
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
func (s *Server) loadReadAccess(ctx context.Context, ws string, cur cursorState, caller *accessCaller) (*syncplanecursorv1.ReadAccess, error) {
	if s.cursorState == nil || cur.accessGen == 0 {
		return nil, nil
	}
	blob, err := s.cursorState.Get(ctx, ws, cursorstate.Ref{ID: cur.accessID, Gen: cur.accessGen})
	if errors.Is(err, cursorstate.ErrNotFound) {
		s.tel().cursorStateMisses.Add(ctx, 1)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ra syncplanecursorv1.ReadAccess
	if err := proto.Unmarshal(blob, &ra); err != nil {
		return nil, nil
	}
	if !slices.Equal(ra.GetSubjects(), caller.subjects) || readAccessHash(&ra) != cur.accessHash ||
		readPolicyOf(ra.GetRules()) != cur.accessPolicy {
		return nil, nil
	}
	return &ra, nil
}

// sameReadAnchors reports whether two sets hold perm at the same anchors.
func sameReadAnchors(a, b *accessSet, perm string) bool {
	x, y := a.perms[perm], b.perms[perm]
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
// under the same anchors.
func readAccessUnchanged(oldEm, newEm *rowEmitter, entity string) bool {
	if newEm.caller == nil {
		return true
	}
	info, ok := newEm.srv.catalogPtr().entities[entity]
	if !ok || info.access == nil || info.access.unrestricted {
		return true
	}
	return sameReadAnchors(oldEm.access, newEm.access, info.access.read)
}

// rootLost returns the entities the caller could read at the workspace root
// under old and cannot under now. A resync drops each of them whole and sends
// what the caller can still read, which costs what the caller keeps rather
// than one tombstone per row it lost.
func rootLost(c *catalog, entities []string, old, now *accessSet) []string {
	var out []string
	for _, name := range entities {
		info := c.entities[name]
		if info == nil || info.access == nil || info.access.unrestricted {
			continue
		}
		o, n := old.perms[info.access.read], now.perms[info.access.read]
		if o != nil && o.root && (n == nil || !n.root) {
			out = append(out, name)
		}
	}
	return out
}

// denyAll is the read filter of an entity the old side of a resync treats as
// holding nothing.
func denyAll(string, proto.Message) bool { return false }

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
// whose visibility can differ between the two sides of a resync whose scopes
// are the same and whose access differs: the anchors of the entity's read
// permission that one side holds and the other does not, the rows below
// those anchors, and both sides' window members. ok is false when the entity
// must be scanned instead: the root was gained, an index is not ready, or the
// set is larger than maxCandidates.
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
	a := info.access
	o, n := oldEm.access.perms[a.read], newEm.access.perms[a.read]
	if (o != nil && o.root) || (n != nil && n.root) {
		return nil, false, nil
	}
	changed := map[anchorRow]struct{}{}
	for _, side := range [][2]*permAnchors{{o, n}, {n, o}} {
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
	found := map[string]struct{}{}
	var probes [][]protoreflect.Value
	parentInfo := newEm.caller.model.parents[info.name]
	for at := range changed {
		switch {
		case at.entity == info.name:
			found[at.key] = struct{}{}
		case a.parent != nil && at.entity == a.parentEntity:
			values, ok := keyValuesOf(parentInfo, at.key)
			if !ok {
				return nil, false, nil
			}
			probes = append(probes, values)
		}
	}
	if len(probes) > 0 {
		ci := info.indexByName(refIndexName(a.parent.ref))
		if ci == nil || newEm.srv.limits.DisableIndexes {
			return nil, false, nil
		}
		if ready, err := ci.ready(tx, info.name); err != nil || !ready {
			return nil, false, err
		}
		// Stops inside the scan, so one anchor with many rows below it costs
		// the bound rather than every key it holds.
		over := errors.New("access candidate bound")
		for _, values := range probes {
			lo, hi, err := ci.probeRefIndex(values)
			if err != nil {
				return nil, false, err
			}
			err = tx.ScanIndex(info.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
				found[rowKey] = struct{}{}
				if len(found) > maxCandidates {
					return over
				}
				return nil
			})
			if errors.Is(err, over) {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
		}
	}
	keys = make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	return withWindowMembers(keys, info.name, maxCandidates, oldEm.windowMembers, newEm.windowMembers)
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
// otherwise, and reads nothing of an entity whose read permission the caller
// held at the root and no longer does.
func (s *Server) bindResyncAccess(
	ctx context.Context,
	ws string,
	cur cursorState,
	acc accessComparison,
	old *rowEmitter,
	resync, ok bool,
	newEm *rowEmitter,
) (*rowEmitter, bool, bool, error) {
	if old == nil && acc.old != nil && !resync {
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
	if acc.old == nil {
		old.useAccess(newEm.access)
		return old, resync, ok, nil
	}
	old.useAccess(acc.old)
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

// callerAccessOf renders everything set holds, for the client.
func callerAccessOf(set *accessSet) *syncplanev1.CallerAccess {
	perms := make([]string, 0, len(set.perms))
	for perm, a := range set.perms {
		if a.root || len(a.rows) > 0 {
			perms = append(perms, perm)
		}
	}
	sort.Strings(perms)
	ca := &syncplanev1.CallerAccess{}
	for _, perm := range perms {
		a := set.perms[perm]
		pa := &syncplanev1.PermissionAccess{Permission: perm, Root: a.root}
		if !a.root {
			entities, keys := anchorRowsByEntity(a.rows)
			for _, entity := range entities {
				pa.Anchors = append(pa.Anchors, &syncplanev1.AnchorRows{Entity: entity, Keys: keys[entity]})
			}
		}
		ca.Permissions = append(ca.Permissions, pa)
	}
	return ca
}

// boundCallerAccess returns the CallerAccess of the access bound on the
// emitter and its hash, rendered once per binding. The hash leaves out seq,
// so it changes only when what the caller holds does.
func (e *rowEmitter) boundCallerAccess() (*syncplanev1.CallerAccess, uint64) {
	if e.callerAccess == nil {
		e.callerAccess = callerAccessOf(e.access)
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(e.callerAccess)
		if err != nil {
			panic(err) // cannot fail for a message of valid in-memory protos
		}
		sum := sha256.Sum256(raw)
		e.callerAccessHash = binary.BigEndian.Uint64(sum[:8]) | 1
	}
	return e.callerAccess, e.callerAccessHash
}

// callerAccessUpdate returns the CallerAccess a response sends: the bound
// one, current through seq, when the client does not hold it yet; nil when it
// does, and for a schema without the access model. haveCursor is false for a
// request without a cursor, whose client holds nothing.
func (e *rowEmitter) callerAccessUpdate(cur cursorState, haveCursor bool, seq uint64) *syncplanev1.CallerAccess {
	if e.caller == nil {
		return nil
	}
	ca, hash := e.boundCallerAccess()
	if haveCursor && cur.callerAccessHash == hash {
		return nil
	}
	out := proto.CloneOf(ca)
	out.Seq = seq
	return out
}
