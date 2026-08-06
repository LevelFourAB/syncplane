// Reference-following subscriptions: a subscription's synced set is its base
// predicate scope UNION a bounded-depth closure reached by walking typed
// reference relations from the base-visible rows. The closure is recomputed
// statelessly on every following pull (there is no row history to diff
// against) and reconciled against the exact key-set the cursor carries
// (Cursor.closure), so a row that LEAVES the closure is dropped precisely —
// following a ref is never a one-way ratchet, and never a permission-
// escalation path: every closure member passes the same read filter as a
// base row.
//
// Two directions, resolved from the reference graph by segment name:
//   - forward  (issue→project): a key point-lookup off the holder's ref stub.
//   - reverse  (project→issues): a range probe of the source entity's
//     per-ref-field secondary index for rows whose ref field points back at
//     the holder, falling back to a predicate scan of that entity when the
//     index is not built yet. Bounded by MaxReverseFanOut either way.
//
// The closure is a SET of (entity, key); a row held by ANY path is in it once,
// which makes multi-path "held by A or B" drop-correctness automatic.
package server

import (
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// closureKey identifies a row participating in a follow closure.
type closureKey struct {
	entity string
	key    string
}

// closureRows maps each member of a bounded membership set to the live
// storage row it resolved to (so the diff can read its version/value without a
// second lookup). Used for both the follow closure and a subscription window.
type closureRows map[closureKey]storage.Row

func (cr closureRows) has(entity, key string) bool {
	_, ok := cr[closureKey{entity, key}]
	return ok
}

// keySet projects the members to the wire ClosureSet (per-entity key lists,
// each entity's keys sorted for a deterministic, comparable cursor). This is
// what makes a later diff exact: the set is STORED, never reconstructed.
func (cr closureRows) keySet() *syncplanecursorv1.ClosureSet {
	if len(cr) == 0 {
		return nil
	}
	byEntity := map[string][]string{}
	for ck := range cr {
		byEntity[ck.entity] = append(byEntity[ck.entity], ck.key)
	}
	entities := make([]string, 0, len(byEntity))
	for e := range byEntity {
		entities = append(entities, e)
	}
	sort.Strings(entities)
	out := &syncplanecursorv1.ClosureSet{Entries: make([]*syncplanecursorv1.ClosureSet_Entry, 0, len(entities))}
	for _, e := range entities {
		keys := byEntity[e]
		sort.Strings(keys)
		out.Entries = append(out.Entries, &syncplanecursorv1.ClosureSet_Entry{Entity: e, Keys: keys})
	}
	return out
}

// closureSetMembership decodes a wire ClosureSet into a membership lookup.
func closureSetMembership(cs *syncplanecursorv1.ClosureSet) map[closureKey]bool {
	out := map[closureKey]bool{}
	for _, e := range cs.GetEntries() {
		for _, k := range e.GetKeys() {
			out[closureKey{entity: e.GetEntity(), key: k}] = true
		}
	}
	return out
}

// ---- reference graph (server mirror of EntityRegistry.buildRefGraph) ----

// forwardRefRel is a forward reference relation surfaced on the source entity,
// named by the field's JSON (camelCase) accessor — the RefPath segment.
type forwardRefRel struct {
	localName    string
	fd           protoreflect.FieldDescriptor
	targetEntity string
	keyFields    []protoreflect.FieldDescriptor // target entity's key fields
}

// reverseRefRel is the inverse, surfaced on the target entity, auto-named
// pluralized-source (or <plural(source)>By<Field> when ambiguous).
type reverseRefRel struct {
	name         string
	sourceEntity string
	viaField     protoreflect.FieldDescriptor
}

type refGraph struct {
	forward map[string]map[string]*forwardRefRel // entity -> localName -> rel
	reverse map[string]map[string]*reverseRefRel // entity -> relName  -> rel
}

func (g *refGraph) forwardBy(entity, seg string) *forwardRefRel { return g.forward[entity][seg] }
func (g *refGraph) reverseBy(entity, seg string) *reverseRefRel { return g.reverse[entity][seg] }

// buildRefGraph mirrors client EntityRegistry.buildRefGraph exactly: forward
// refs keyed by source, reverse relations derived and auto-named, names sorted
// for determinism, a clash is a hard error. A ref whose target entity is not
// registered here is omitted (cannot be followed locally).
//
// The two halves share ONE segment namespace per entity — that is what a
// RefPath segment, an include key and client.db.<entity>.refs all resolve
// against — so a derived reverse name must be unique against BOTH the other
// reverse names and the entity's forward ref fields.
func (c *catalog) buildRefGraph() (*refGraph, error) {
	g := &refGraph{
		forward: map[string]map[string]*forwardRefRel{},
		reverse: map[string]map[string]*reverseRefRel{},
	}
	for _, name := range c.entityNames() {
		info := c.entities[name]
		fwd := map[string]*forwardRefRel{}
		fields := info.msgType.Descriptor().Fields()
		// Sort by proto field name, matching the client's deterministic order.
		ordered := make([]protoreflect.FieldDescriptor, 0, fields.Len())
		for i := 0; i < fields.Len(); i++ {
			ordered = append(ordered, fields.Get(i))
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name() < ordered[j].Name() })
		for _, fd := range ordered {
			kf := lww.RefKeyFields(fd)
			if kf == nil {
				continue
			}
			target := c.byType[fd.Message().FullName()]
			if target == nil {
				continue // target entity not registered here; not followable
			}
			fwd[fd.JSONName()] = &forwardRefRel{
				localName:    fd.JSONName(),
				fd:           fd,
				targetEntity: target.name,
				keyFields:    kf,
			}
		}
		g.forward[name] = fwd
	}
	// Reverse relations: group a source's forward refs by target; a single
	// field to a target ⇒ pluralized source, multiple ⇒ disambiguated.
	for _, source := range c.entityNames() {
		byTarget := map[string][]*forwardRefRel{}
		// Iterate in deterministic (localName) order for stable naming.
		locals := make([]string, 0, len(g.forward[source]))
		for ln := range g.forward[source] {
			locals = append(locals, ln)
		}
		sort.Strings(locals)
		for _, ln := range locals {
			r := g.forward[source][ln]
			byTarget[r.targetEntity] = append(byTarget[r.targetEntity], r)
		}
		for target, group := range byTarget {
			single := len(group) == 1
			for _, r := range group {
				relName := pluralizeEntity(source)
				if !single {
					relName = pluralizeEntity(source) + "By" + capitalizeFirst(r.localName)
				}
				if g.reverse[target] == nil {
					g.reverse[target] = map[string]*reverseRefRel{}
				}
				if _, dup := g.reverse[target][relName]; dup {
					return nil, fmt.Errorf("server: reverse relation %q on entity %q is ambiguous; rename a ref field to disambiguate", relName, target)
				}
				// Forward names are complete by now (the loop above ran over every
				// entity), so this sees the whole namespace the segment resolves
				// against. Shadowing is worse than ambiguity: forward-first
				// resolution would answer the segment with the holder's own ref
				// list instead of the rows pointing back, which is a different row
				// set and no error anywhere.
				if _, shadowed := g.forward[target][relName]; shadowed {
					return nil, fmt.Errorf("server: reverse relation %q on entity %q collides with the reference field of that name; rename a ref field to disambiguate", relName, target)
				}
				g.reverse[target][relName] = &reverseRefRel{
					name:         relName,
					sourceEntity: source,
					viaField:     r.fd,
				}
			}
		}
	}
	return g, nil
}

// refGraphLazy builds the reference graph once, after all entities are
// registered. Build errors (a relation-name clash) surface to following
// requests; non-following requests never touch it.
//
// It cannot run at registration time the way the TS registry's constructor
// does: RegisterEntity is incremental, and a ref to a not-yet-registered entity
// is legitimately omitted from the graph, so any name check before the last
// registration would judge a half-built namespace.
func (c *catalog) refGraphLazy() (*refGraph, error) {
	c.refGraphOnce.Do(func() {
		c.refGraph, c.refGraphErr = c.buildRefGraph()
	})
	return c.refGraph, c.refGraphErr
}

// pluralizeEntity / capitalizeFirst mirror client mutations.ts.
func pluralizeEntity(s string) string {
	n := len(s)
	if n >= 2 && s[n-1] == 'y' && !isVowel(s[n-2]) {
		return s[:n-1] + "ies"
	}
	if hasSuffixAny(s, "s", "x", "z", "ch", "sh") {
		return s + "es"
	}
	return s + "s"
}

func isVowel(b byte) bool {
	switch b {
	case 'a', 'e', 'i', 'o', 'u':
		return true
	}
	return false
}

func hasSuffixAny(s string, suffixes ...string) bool {
	for _, suf := range suffixes {
		if len(s) >= len(suf) && s[len(s)-len(suf):] == suf {
			return true
		}
	}
	return false
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] -= 'a' - 'A'
	}
	return string(b)
}

// ---- closure computation ----

// Both bounds are built per request rather than kept as package vars: the
// message quotes the limit in force, which is per server (server.WithLimits).
func errClosureTooLarge(size, limit int) error {
	return connect.NewError(connect.CodeResourceExhausted, errors.New(limitExceeded(
		fmt.Sprintf("follow closure reached %d rows", size), "MaxClosureSize", limit)))
}

func errReverseFanOut(matched, limit int) error {
	return connect.NewError(connect.CodeResourceExhausted, errors.New(limitExceeded(
		fmt.Sprintf("reverse follow matched %d rows", matched), "MaxReverseFanOut", limit)))
}

// frontierRow is a decoded row used as a follow source during BFS.
type frontierRow struct {
	entity string
	key    string
	msg    protoreflect.Message
}

// followEntitiesOf statically computes every entity a follow spec set can
// reach (intermediate and terminal), so subscribed()/entities() cover them
// and the scan + resync diff can carry them. Pure graph reachability, data-
// independent. depth bounds re-application; entity reachability saturates, so
// a visited entity-set terminates self/cyclic relations.
func (g *refGraph) followEntitiesOf(subs []*syncplanev1.Subscription, maxDepth int) (map[string]bool, error) {
	out := map[string]bool{}
	for _, sub := range subs {
		follow := sub.GetFollow()
		if follow == nil || len(follow.GetPaths()) == 0 {
			continue
		}
		depth := int(follow.GetDepth())
		if depth == 0 {
			depth = 1
		}
		if depth > maxDepth {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
				fmt.Sprintf("follow depth %d", depth), "MaxFollowDepth", maxDepth)))
		}
		frontier := map[string]bool{sub.GetEntity(): true}
		for round := 0; round < depth; round++ {
			next := map[string]bool{}
			for ent := range frontier {
				for _, path := range follow.GetPaths() {
					cur := map[string]bool{ent: true}
					for _, seg := range path.GetSegments() {
						step := map[string]bool{}
						for c := range cur {
							to, err := g.segmentTarget(c, seg)
							if err != nil {
								return nil, err
							}
							out[to] = true
							step[to] = true
						}
						cur = step
					}
					for c := range cur {
						next[c] = true
					}
				}
			}
			frontier = next
		}
	}
	return out, nil
}

// segmentTarget resolves one path segment from an entity to the entity it
// lands on (forward target or reverse source).
func (g *refGraph) segmentTarget(entity, seg string) (string, error) {
	if fr := g.forwardBy(entity, seg); fr != nil {
		return fr.targetEntity, nil
	}
	if rr := g.reverseBy(entity, seg); rr != nil {
		return rr.sourceEntity, nil
	}
	return "", errUnknownSegment(g, entity, seg)
}

// buildClosure recomputes the follow closure for this request against current
// rows in tx: for each subscription carrying a follow spec, seed = its base-
// visible rows, then walk its ref paths depth rounds. Every discovered row is
// read-filtered (permission) before joining the closure. Bounded by
// MaxClosureSize / MaxReverseFanOut.
func (e *rowEmitter) buildClosure(tx storage.ReadTx, subs []*syncplanev1.Subscription) (closureRows, error) {
	g, err := e.srv.catalogPtr().refGraphLazy()
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	limits := e.srv.limits
	cr := closureRows{}
	for _, sub := range subs {
		follow := sub.GetFollow()
		if follow == nil || len(follow.GetPaths()) == 0 {
			continue
		}
		seeds, err := e.closureSeeds(tx, sub)
		if err != nil {
			return nil, err
		}
		depth := int(follow.GetDepth())
		if depth == 0 {
			depth = 1
		}
		// visited is per-subscription: each subscription walks its own path set
		// independently, so a row seeded by two subscriptions with different
		// follow specs must be expanded under each. Within a subscription it
		// terminates cycles and bounds depth re-application. The shared closure
		// member set (cr) is the union across subscriptions.
		visited := map[closureKey]bool{}
		frontier := seeds
		for round := 0; round < depth && len(frontier) > 0; round++ {
			var next []frontierRow
			for _, holder := range frontier {
				hk := closureKey{holder.entity, holder.key}
				if visited[hk] {
					continue
				}
				visited[hk] = true
				for _, path := range follow.GetPaths() {
					ends, err := e.walkPath(tx, g, holder, path.GetSegments(), cr, limits)
					if err != nil {
						return nil, err
					}
					next = append(next, ends...)
				}
			}
			frontier = next
		}
	}
	return cr, nil
}

// closureSeeds returns a subscription's base-visible, readable rows (decoded)
// as the follow roots.
//
// The seeds are the expensive half of a following pull, because there are as
// many of them as the base scope has rows and each one is walked. Two things
// keep that from being a full scan and decode of the entity every time: a
// windowed subscription seeds from rows already in hand, and a predicated one
// can narrow through an index.
func (e *rowEmitter) closureSeeds(tx storage.ReadTx, sub *syncplanev1.Subscription) ([]frontierRow, error) {
	info, ok := e.srv.catalogPtr().entities[sub.GetEntity()]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("subscription names unknown entity %q", sub.GetEntity()))
	}
	// A windowed subscription seeds from its WINDOW, not from every predicate
	// match: the predicate selects candidates for the window, so following from
	// all of them would expand the closure right back past the working set the
	// window exists to bound.
	if sub.GetWindow() != nil {
		return e.windowSeeds(info), nil
	}
	filter, err := e.filterOf(sub.GetEntity())
	if err != nil {
		return nil, err
	}
	var pred *query.Compiled
	if sub.GetPredicate() != nil {
		pred, err = query.Compile(info.msgType.Descriptor(), sub.GetPredicate())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	var seeds []frontierRow
	consider := func(row storage.Row, p *query.Compiled) error {
		if row.Deleted {
			return nil
		}
		msg, ok := e.decodeRow(info, row)
		if !ok {
			return nil // quarantined: it seeds nothing
		}
		m := msg.ProtoReflect()
		if filter != nil && !filter(row.Key, msg) {
			return nil
		}
		if p != nil && !p.Eval(m) {
			return nil
		}
		seeds = append(seeds, frontierRow{entity: info.name, key: row.Key, msg: m})
		return nil
	}
	// The subscription's predicate is a predicate like any other, so an index
	// can narrow the seed set exactly as it narrows a query's — a following
	// subscription anchored on a small slice of a large entity stops paying for
	// the whole entity to find its anchors. With no predicate there is nothing
	// to narrow by and the seeds ARE the entity, so the scan is the answer
	// rather than a fallback, and asking the planner would only cost a
	// readiness read per index to be told so.
	//
	// Honoring the discard contract matters less here than it does for a query
	// (buildClosure's visited set would absorb a duplicated seed), but resting
	// on that would make this the one caller whose correctness depends on a
	// de-dupe somewhere else.
	if pred != nil {
		discard := func() { seeds = nil }
		served, err := queryByIndex(tx, info, pred, nil, page{}, e.srv.limits, consider,
			// No page, so nothing can fill and the walk runs the whole range.
			func() bool { return false }, discard)
		if err != nil {
			return nil, err
		}
		if served {
			return seeds, nil
		}
	}
	if err := tx.ScanEntity(info.name, func(row storage.Row) error {
		return consider(row, pred)
	}); err != nil {
		return nil, err
	}
	return seeds, nil
}

// windowSeeds projects one entity's window membership into follow roots.
//
// buildWindows has already resolved that set against this snapshot and applied
// the entity's read filter to it (see rowEmitter.readable), and it holds the
// rows themselves — so the seeds are those rows, and the only work left is the
// decode the walk needs. Scanning the entity to re-derive a set already in hand
// is what this replaces.
//
// windowMembers is the union across every windowed subscription, which is
// exactly what the client holds: two windows on one entity seed each other's
// members, the same way they are each other's base scope.
//
// Row-key order, matching the scan that used to produce these. Nothing
// downstream depends on the order — the closure is a set — but a stable one
// keeps two runs of the same pull comparable.
func (e *rowEmitter) windowSeeds(info *entityInfo) []frontierRow {
	keys := make([]string, 0, len(e.windowMembers))
	for ck := range e.windowMembers {
		if ck.entity == info.name {
			keys = append(keys, ck.key)
		}
	}
	sort.Strings(keys)
	seeds := make([]frontierRow, 0, len(keys))
	for _, key := range keys {
		row := e.windowMembers[closureKey{entity: info.name, key: key}]
		msg, ok := e.decodeRow(info, row)
		if !ok {
			continue // quarantined: it seeds nothing
		}
		seeds = append(seeds, frontierRow{entity: info.name, key: key, msg: msg.ProtoReflect()})
	}
	return seeds
}

// walkPath walks a path's segments from one holder, adding every resolved row
// to the closure and returning the path's terminal rows (the next depth round
// re-applies the whole path set from these).
func (e *rowEmitter) walkPath(tx storage.ReadTx, g *refGraph, holder frontierRow, segments []string, cr closureRows, limits Limits) ([]frontierRow, error) {
	current := []frontierRow{holder}
	for _, seg := range segments {
		var next []frontierRow
		seen := map[closureKey]bool{}
		for _, h := range current {
			rows, err := e.followSegment(tx, g, h, seg, limits)
			if err != nil {
				return nil, err
			}
			for _, r := range rows {
				ck := closureKey{r.row.Entity, r.row.Key}
				if seen[ck] {
					continue
				}
				seen[ck] = true
				if _, ok := cr[ck]; !ok {
					cr[ck] = r.row
					if len(cr) > limits.MaxClosureSize {
						return nil, errClosureTooLarge(len(cr), limits.MaxClosureSize)
					}
				}
				next = append(next, r.front)
			}
		}
		current = next
	}
	return current, nil
}

// resolved pairs a discovered closure member's storage row with its decoded
// frontier form (for onward walking).
type resolved struct {
	row   storage.Row
	front frontierRow
}

func (e *rowEmitter) followSegment(tx storage.ReadTx, g *refGraph, holder frontierRow, seg string, limits Limits) ([]resolved, error) {
	if fr := g.forwardBy(holder.entity, seg); fr != nil {
		return e.followForward(tx, holder, fr)
	}
	if rr := g.reverseBy(holder.entity, seg); rr != nil {
		return e.followReverse(tx, holder, rr, limits)
	}
	return nil, errUnknownSegment(g, holder.entity, seg)
}

// followForward resolves a holder's forward ref (singular or repeated) to its
// live, readable target rows via key point-lookups. Missing targets (orphans)
// and read-filtered targets are silently dropped.
func (e *rowEmitter) followForward(tx storage.ReadTx, holder frontierRow, fr *forwardRefRel) ([]resolved, error) {
	if !holder.msg.Has(fr.fd) {
		return nil, nil
	}
	targetInfo := e.srv.catalogPtr().entities[fr.targetEntity]
	filter, err := e.filterOf(fr.targetEntity)
	if err != nil {
		return nil, err
	}
	var out []resolved
	resolveStub := func(stub protoreflect.Message) error {
		key := targetInfo.keyOf(stub)
		row, err := tx.GetRow(fr.targetEntity, key)
		if errors.Is(err, storage.ErrNotFound) {
			return nil // orphan: target not synced here / never existed
		}
		if err != nil {
			return err
		}
		if row.Deleted {
			return nil
		}
		msg, ok := e.decodeRow(targetInfo, row)
		if !ok {
			return nil // quarantined: dropped like an orphan, and unfollowable
		}
		if filter != nil && !filter(row.Key, msg) {
			return nil // permission prune — and we cannot follow through it
		}
		out = append(out, resolved{row: row, front: frontierRow{entity: fr.targetEntity, key: key, msg: msg.ProtoReflect()}})
		return nil
	}
	if fr.fd.IsList() {
		list := holder.msg.Get(fr.fd).List()
		for i := 0; i < list.Len(); i++ {
			if err := resolveStub(list.Get(i).Message()); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	if err := resolveStub(holder.msg.Get(fr.fd).Message()); err != nil {
		return nil, err
	}
	return out, nil
}

// followReverse resolves a reverse relation: find the live, readable rows of
// the source entity whose ref field points back at the holder.
//
// This is the hop that every follow spec pays on every pull, once PER HOLDER
// ROW, so it is where a scan hurts most. When the source entity's per-ref-field
// index is ready it becomes a range probe over the encoded holder key, and the
// only rows decoded are the ones that actually point back. Otherwise it scans
// and decodes the whole entity, exactly as before.
//
// Both paths are bounded by MaxReverseFanOut on matches, and both must produce
// the same rows in the same order — index use is an optimization, never a
// change of answer, which is what lets the fallback stay a plain fallback.
func (e *rowEmitter) followReverse(tx storage.ReadTx, holder frontierRow, rr *reverseRefRel, limits Limits) ([]resolved, error) {
	sourceInfo := e.srv.catalogPtr().entities[rr.sourceEntity]
	holderInfo := e.srv.catalogPtr().entities[holder.entity]
	filter, err := e.filterOf(rr.sourceEntity)
	if err != nil {
		return nil, err
	}

	// collect turns one candidate row into a match, applying the same checks in
	// the same order on both paths. It returns false once the fan-out bound is
	// hit, with err set.
	var out []resolved
	matched := 0
	collect := func(row storage.Row, m protoreflect.Message, msg proto.Message) error {
		if filter != nil && !filter(row.Key, msg) {
			return nil
		}
		matched++
		if matched > limits.MaxReverseFanOut {
			return errReverseFanOut(matched, limits.MaxReverseFanOut)
		}
		out = append(out, resolved{row: row, front: frontierRow{entity: rr.sourceEntity, key: row.Key, msg: m}})
		return nil
	}

	// The probe collects as it walks and can still decline afterwards (see the
	// candidate bound there), and the scan below re-collects every one of those
	// rows. Both pieces of state have to go back to where the scan expects to
	// find them: `out` would hold each match twice, and `matched` would count
	// each twice — tripping MaxReverseFanOut at roughly half the real fan-out.
	discard := func() { out, matched = nil, 0 }

	if ok, err := e.followReverseIndexed(tx, holder, holderInfo, sourceInfo, rr, limits, collect, discard); err != nil {
		return nil, err
	} else if ok {
		return out, nil
	}

	if err := tx.ScanEntity(rr.sourceEntity, func(row storage.Row) error {
		if row.Deleted {
			return nil
		}
		msg, ok := e.decodeRow(sourceInfo, row)
		if !ok {
			return nil // quarantined: its ref cannot be read, so it points nowhere
		}
		m := msg.ProtoReflect()
		if !refPointsAt(m, rr.viaField, holderInfo, holder.key) {
			return nil
		}
		return collect(row, m, msg)
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// followReverseIndexed serves one reverse hop from the source entity's
// per-ref-field index, reporting whether it could. A false return means the
// caller must scan — the index does not exist, is not built yet, is disabled,
// or the walk exceeded its candidate bound — and NOT that there were no
// matches. The last of those declines only AFTER collect has run, so discard
// runs on the way out and the scan re-collects exactly those rows. This is the
// same contract queryByIndex keeps, enforced the same way: whichever decline it
// was, the caller is left where it started.
//
// The probe is a prefix of the entry: every component of the index is a key
// field of the holder, so bracketing them all selects exactly the rows whose
// ref equals this holder, whatever their own row keys are. Entries are ordered
// by the encoded key and then by row key, so the hits arrive in row-key order —
// the same order the scan fallback yields them in.
//
// Rows are re-read rather than trusted from the entry: the entry says a row
// pointed here when it was written, and this transaction's snapshot is what
// decides whether it still exists and whether this caller may see it.
func (e *rowEmitter) followReverseIndexed(
	tx storage.ReadTx,
	holder frontierRow,
	holderInfo, sourceInfo *entityInfo,
	rr *reverseRefRel,
	limits Limits,
	collect func(storage.Row, protoreflect.Message, proto.Message) error,
	discard func(),
) (bool, error) {
	decline := func() (bool, error) {
		discard()
		return false, nil
	}
	if limits.DisableIndexes || sourceInfo == nil || holderInfo == nil || holder.msg == nil {
		return decline()
	}
	if rr.viaField.IsList() {
		// A to-many ref stores several target keys in one row, so it needs a
		// multi-entry index. Until there is one, scan.
		return decline()
	}
	ci := sourceInfo.indexByName(refIndexName(rr.viaField))
	if ci == nil {
		return decline()
	}
	if ready, err := ci.ready(tx, rr.sourceEntity); err != nil {
		return false, err
	} else if !ready {
		return decline()
	}

	values := make([]protoreflect.Value, 0, len(holderInfo.keyFields))
	for _, kf := range holderInfo.keyFields {
		values = append(values, holder.msg.Get(kf))
	}
	lo, hi, err := ci.probeRefIndex(values)
	if err != nil {
		return false, err
	}

	// The candidate bound guards the case the index cannot: entries narrow by
	// the ref, but a read filter can still reject nearly all of them, at which
	// point the walk has become a scan with extra reads. Overflow falls back
	// rather than failing — an optimization that gives up must not change the
	// answer.
	seen := 0
	overflow := errors.New("index candidate bound")
	err = tx.ScanIndex(rr.sourceEntity, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
		seen++
		if seen > limits.maxIndexScanCandidates() {
			return overflow
		}
		row, err := tx.GetRow(rr.sourceEntity, rowKey)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if row.Deleted {
			// Entries are cleared on delete, so this only happens if an entry
			// outlived its row — a rebuild will drop it, and skipping keeps the
			// two paths' answers identical meanwhile.
			return nil
		}
		msg, ok := e.decodeRow(sourceInfo, row)
		if !ok {
			return nil // quarantined: its ref cannot be read, so it points nowhere
		}
		return collect(row, msg.ProtoReflect(), msg)
	})
	if errors.Is(err, overflow) {
		return decline()
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// emitMembershipDiff reconciles a cursor's prior membership set (old) against
// the freshly computed one (now), for rows at or below seq ONLY — rows above
// seq are delivered by the incremental scan through membership-aware classify,
// so handling them here too would double-emit. It appends to patch:
//
//	+key (new∖old), version ≤ seq          value   (a member the version>seq
//	                                                scan misses: a re-pointed-to
//	                                                OLD target, or an old row
//	                                                that rose into a window)
//	−key (old∖new), version ≤ seq, !base   tombstone (left the set and is not
//	                                                otherwise base-visible)
//
// This is the exact-drop guarantee, and it is why both the follow closure and
// the subscription window carry their set on the cursor: because the prior
// membership is STORED rather than lossily recomputed, a row that left is
// dropped precisely. Both callers share this one implementation — the sets
// differ, the reconciliation does not.
//
// The base-visibility check uses classify, which consults BOTH memberships, so
// a row that left the window but is still held by the closure (or vice versa)
// is kept rather than tombstoned.
func (e *rowEmitter) emitMembershipDiff(tx storage.ReadTx, old *syncplanecursorv1.ClosureSet, now closureRows, seq uint64, patch *syncplanev1.Patch) error {
	oldSet := closureSetMembership(old)
	// Additions: members now present that the cursor did not carry.
	for ck, row := range now {
		if oldSet[ck] || row.Version > seq {
			continue // already held, or the incremental scan delivered it
		}
		patch.Changes = append(patch.Changes, rowChange(row))
	}
	// Removals: members the cursor carried that the set no longer holds.
	for ck := range oldSet {
		if now.has(ck.entity, ck.key) {
			continue
		}
		row, err := tx.GetRow(ck.entity, ck.key)
		if errors.Is(err, storage.ErrNotFound) {
			continue // never existed; the client holds nothing to drop
		}
		if err != nil {
			return err
		}
		if row.Version > seq {
			continue // changed & left: the incremental scan tombstoned or kept it
		}
		if row.Deleted {
			patch.Changes = append(patch.Changes, rowChange(row)) // tombstone at its version
			continue
		}
		// A still-live row that left the set drops only if base scope does not
		// also hold it (classify's membership checks cannot fire here: ck is in
		// neither the new closure nor the new window).
		class, err := e.classify(row)
		if err != nil {
			return err
		}
		if class != rowVisible {
			patch.Changes = append(patch.Changes, hiddenChange(row))
		}
	}
	return nil
}

// refPointsAt reports whether a source row's ref field (singular or repeated)
// references holderKey, by re-deriving each stub's key with the holder
// entity's key derivation (the same f → f.<key> resolution P1 uses).
func refPointsAt(m protoreflect.Message, viaField protoreflect.FieldDescriptor, holderInfo *entityInfo, holderKey string) bool {
	if !m.Has(viaField) {
		return false
	}
	if viaField.IsList() {
		list := m.Get(viaField).List()
		for i := 0; i < list.Len(); i++ {
			if holderInfo.keyOf(list.Get(i).Message()) == holderKey {
				return true
			}
		}
		return false
	}
	return holderInfo.keyOf(m.Get(viaField).Message()) == holderKey
}
