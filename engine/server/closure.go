// Reference-following subscriptions: a subscription's synced set is its base
// predicate scope UNION a bounded-depth closure reached by walking typed
// reference relations from the base-visible rows. Every following pull brings
// the closure up to date against current rows and reconciles it with the exact
// key-set the cursor carries (Cursor.closure), so a row that LEAVES the closure
// is dropped precisely — following a ref is never a one-way ratchet, and never
// a permission-escalation path: every closure member passes the same read
// filter as a base row.
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
//
// buildClosure walks it from every seed. That is the answer for any shape and
// the fallback for all of them, but it costs a hop per holder however little
// moved, so a pull that can see every row written since its cursor MAINTAINS a
// single-segment closure at depth 1 from those rows instead — either direction
// — see maintainClosure.
package server

import (
	"errors"
	"fmt"
	"math"
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

// closureRows maps each member of a bounded membership set to the live storage
// row it resolved to (so the diff can read its version/value without a second
// lookup), or to the ZERO Row for a member carried over from the cursor without
// being re-read — which maintenance does for every member that did not move.
// Used for both the follow closure and a subscription window.
//
// Membership (has) is answered either way. A caller that needs the row itself
// must go through rowOf; the membership diff never does, because a carried-over
// member is by definition one the cursor already held and so is not part of any
// diff.
type closureRows map[closureKey]storage.Row

func (cr closureRows) has(entity, key string) bool {
	_, ok := cr[closureKey{entity, key}]
	return ok
}

// rowOf resolves a member to its row, reading one that was carried over unread
// and caching it. Reports false for a non-member and for a member whose row has
// since gone — a set maintained against a cursor can name a row this snapshot
// no longer has.
func (cr closureRows) rowOf(tx storage.ReadTx, ck closureKey) (storage.Row, bool, error) {
	row, ok := cr[ck]
	if !ok {
		return storage.Row{}, false, nil
	}
	if row.Entity != "" {
		return row, true, nil
	}
	row, err := tx.GetRow(ck.entity, ck.key)
	if errors.Is(err, storage.ErrNotFound) {
		return storage.Row{}, false, nil
	}
	if err != nil {
		return storage.Row{}, false, err
	}
	cr[ck] = row
	return row, true, nil
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
	// ref is the stored shape: where the stub and, for a reference message,
	// the workspace sit inside the field value.
	ref *lww.Ref
}

// reverseRefRel is the inverse, surfaced on the target entity, auto-named
// pluralized-source (or <plural(source)>By<Field> when ambiguous).
type reverseRefRel struct {
	name         string
	sourceEntity string
	viaField     protoreflect.FieldDescriptor
	// via is the forward reference this reverses, with its stored shape.
	via *lww.Ref
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
			ref := lww.RefOf(fd)
			if ref == nil {
				continue
			}
			target := c.byType[ref.Target.FullName()]
			if target == nil {
				continue // target entity not registered here; not followable
			}
			fwd[fd.JSONName()] = &forwardRefRel{
				localName:    fd.JSONName(),
				fd:           fd,
				targetEntity: target.name,
				keyFields:    ref.KeyFields,
				ref:          ref,
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
					via:          r.ref,
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
		return e.windowSeeds(tx, info)
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
			// No page to fill: every seed is wanted, so the walk runs the whole
			// range and its candidate batches are sized by the cap alone.
			func() int { return math.MaxInt }, discard)
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
// The window has already been resolved against this snapshot and read-filtered
// (see rowEmitter.readable), so the seeds ARE its members and the only work
// left is the decode the walk needs. Scanning the entity to re-derive a set
// already in hand is what this replaces. A member the window maintained without
// re-reading costs one point lookup here (see closureRows.rowOf).
//
// windowMembers is the union across every windowed subscription, which is
// exactly what the client holds: two windows on one entity seed each other's
// members, the same way they are each other's base scope.
//
// Row-key order, matching the scan that used to produce these. Nothing
// downstream depends on the order — the closure is a set — but a stable one
// keeps two runs of the same pull comparable.
func (e *rowEmitter) windowSeeds(tx storage.ReadTx, info *entityInfo) ([]frontierRow, error) {
	keys := make([]string, 0, len(e.windowMembers))
	for ck := range e.windowMembers {
		if ck.entity == info.name {
			keys = append(keys, ck.key)
		}
	}
	sort.Strings(keys)
	seeds := make([]frontierRow, 0, len(keys))
	for _, key := range keys {
		row, ok, err := e.windowMembers.rowOf(tx, closureKey{entity: info.name, key: key})
		if err != nil {
			return nil, err
		}
		if !ok || row.Deleted {
			continue
		}
		msg, ok := e.decodeRow(info, row)
		if !ok {
			continue // quarantined: it seeds nothing
		}
		seeds = append(seeds, frontierRow{entity: info.name, key: key, msg: msg.ProtoReflect()})
	}
	return seeds, nil
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
// live, readable target rows. Missing targets (orphans) and read-filtered
// targets are silently dropped.
//
// The stubs name their targets outright, so a repeated ref is a batch of keys
// known before any of them is read (see rowBatch) rather than a lookup per
// element. Targets come back in the field's order, which is the order the
// caller walks onward in.
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
	batch := &rowBatch{tx: tx, entity: fr.targetEntity, size: maxRowBatch, emit: func(row storage.Row) error {
		// A key with no row at all is an orphan the batch already dropped:
		// a target never synced here, or one that never existed.
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
		out = append(out, resolved{row: row, front: frontierRow{entity: fr.targetEntity, key: row.Key, msg: msg.ProtoReflect()}})
		return nil
	}}
	// A stored value naming another workspace resolves there, not in this
	// one (see ReferenceOptions), so it contributes nothing to this closure.
	addValue := func(value protoreflect.Message) error {
		stub, ok := fr.ref.Stub(value)
		if !ok || !fr.ref.ResolvesIn(value, e.ws) {
			return nil
		}
		return batch.add(targetInfo.keyOf(stub))
	}
	if fr.fd.IsList() {
		list := holder.msg.Get(fr.fd).List()
		for i := 0; i < list.Len(); i++ {
			if err := addValue(list.Get(i).Message()); err != nil {
				return nil, err
			}
		}
	} else if err := addValue(holder.msg.Get(fr.fd).Message()); err != nil {
		return nil, err
	}
	if err := batch.flush(); err != nil {
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
		if !refPointsAt(m, rr.via, holderInfo, holder.key, e.ws) {
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
// decides whether it still exists and whether this caller may see it. The
// re-reads are batched (see rowBatch), so a wide fan-out costs reads
// proportional to matches/batch rather than one per match.
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
	batch := &rowBatch{tx: tx, entity: rr.sourceEntity, size: maxRowBatch, emit: func(row storage.Row) error {
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
		// The index is keyed on the target key alone. A reference message may
		// name another workspace under the same key, and such a row points
		// there rather than here, so the hit is re-checked against the value.
		if rr.via.Qualified() && !refPointsAt(msg.ProtoReflect(), rr.via, holderInfo, holder.key, e.ws) {
			return nil
		}
		return collect(row, msg.ProtoReflect(), msg)
	}}
	err = tx.ScanIndex(rr.sourceEntity, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
		seen++
		if seen > limits.maxIndexScanCandidates() {
			return overflow
		}
		return batch.add(rowKey)
	})
	if errors.Is(err, overflow) {
		// The buffered keys go with it: declining hands everything back, and
		// the scan the caller falls back to re-collects them.
		return decline()
	}
	if err != nil {
		return false, err
	}
	if err := batch.flush(); err != nil {
		return false, err
	}
	return true, nil
}

// ---- incremental maintenance ----

// reverseHop is one follow path that resolves to a single reverse relation: the
// members it contributes are the rows of rr.sourceEntity whose rr.viaField
// points at one of its subscription's seeds.
//
// Its membership rule reads per row: "r is in" is a question about r's own ref
// field and about whether what it names is a seed, neither of which needs the
// other members. Composing segments is what breaks that — each level's
// membership becomes a function of the level before it, and the cursor carries
// one flat key-set rather than per-level sets — so a path of more than one
// segment, and any spec deeper than one round, still fall back to buildClosure.
type reverseHop struct {
	rr         *reverseRefRel
	seedInfo   *entityInfo // the subscription's own entity: what a member points AT
	sourceInfo *entityInfo // the entity members belong to
	// subIndex is the position of the subscription this hop came from. Two hops
	// of one subscription share a seed rule and two of different subscriptions
	// do not, which is what the seed memo is keyed by.
	subIndex int
	// seedPred is the subscription's predicate, nil for a whole-entity or
	// windowed subscription; windowed says the seeds are the window's members.
	seedPred *query.Compiled
	windowed bool
}

// seedMemo identifies one resolved seed question: whether a row is a follow
// root for the subscription that asked.
type seedMemo struct {
	subIndex int
	key      string
}

// forwardHop is one follow path that resolves to a single forward relation: the
// members it contributes are the rows its subscription's seeds name through
// fr.fd.
//
// Membership reads the other way round from a reverseHop — a target is in
// because some HOLDER points at it — so a target's own row does not settle it.
// What makes it maintainable anyway is that the holders' ref index answers the
// question from the target's side, at one probe per target whose membership is
// actually in doubt. See heldForward for when it is, and anySeedPointsAt for
// what the probe costs and when it gives up.
type forwardHop struct {
	fr         *forwardRefRel
	seedInfo   *entityInfo // the subscription's own entity: what holds the ref
	targetInfo *entityInfo // the entity members belong to
	// seedPred is the subscription's predicate, nil for a whole-entity or
	// windowed subscription; windowed says the seeds are the window's members.
	seedPred *query.Compiled
	windowed bool
	// sole says no other hop can put a row of targetInfo in the closure, which
	// is what lets a prior member be carried over without a probe (see
	// heldForward).
	sole bool
}

// closureHops is one maintenance pass's hops, split by the direction each is
// decided in.
type closureHops struct {
	reverse []reverseHop
	forward []forwardHop
}

// resolveHops resolves every following subscription into hops, reporting false
// when any of them is a shape maintenance cannot decide per row: a path of more
// than one segment, a spec deeper than one round, or a to-many reference walked
// forwards, whose index the probe in anySeedPointsAt cannot read.
func (e *rowEmitter) resolveHops(subs []*syncplanev1.Subscription) (closureHops, bool, error) {
	g, err := e.srv.catalogPtr().refGraphLazy()
	if err != nil {
		return closureHops{}, false, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	var hops closureHops
	for subIndex, sub := range subs {
		follow := sub.GetFollow()
		if follow == nil || len(follow.GetPaths()) == 0 {
			continue
		}
		if d := follow.GetDepth(); d > 1 {
			return closureHops{}, false, nil
		}
		seedInfo, ok := e.srv.catalogPtr().entities[sub.GetEntity()]
		if !ok {
			return closureHops{}, false, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("subscription names unknown entity %q", sub.GetEntity()))
		}
		var pred *query.Compiled
		if sub.GetWindow() == nil && sub.GetPredicate() != nil {
			if pred, err = query.Compile(seedInfo.msgType.Descriptor(), sub.GetPredicate()); err != nil {
				return closureHops{}, false, connect.NewError(connect.CodeInvalidArgument, err)
			}
		}
		for _, path := range follow.GetPaths() {
			segs := path.GetSegments()
			if len(segs) != 1 {
				return closureHops{}, false, nil
			}
			if rr := g.reverseBy(sub.GetEntity(), segs[0]); rr != nil {
				// A workspace-qualified ref is decided by its value's workspace
				// as well as its key, which the ref index the probes below read
				// does not carry. Rebuild instead.
				if rr.via.Qualified() {
					return closureHops{}, false, nil
				}
				hops.reverse = append(hops.reverse, reverseHop{
					rr:         rr,
					seedInfo:   seedInfo,
					sourceInfo: e.srv.catalogPtr().entities[rr.sourceEntity],
					subIndex:   subIndex,
					seedPred:   pred,
					windowed:   sub.GetWindow() != nil,
				})
				continue
			}
			fr := g.forwardBy(sub.GetEntity(), segs[0])
			// A segment that names nothing at all is an error buildClosure is
			// the right place to raise.
			if fr == nil {
				return closureHops{}, false, nil
			}
			// A to-many ref stores several target keys in one row, and the
			// probe reads one entry per row (see followReverseIndexed). A
			// workspace-qualified ref is decided by its value's workspace too,
			// which the index does not carry.
			if fr.fd.IsList() || fr.ref.Qualified() {
				return closureHops{}, false, nil
			}
			targetInfo, ok := e.srv.catalogPtr().entities[fr.targetEntity]
			if !ok {
				return closureHops{}, false, nil
			}
			hops.forward = append(hops.forward, forwardHop{
				fr:         fr,
				seedInfo:   seedInfo,
				targetInfo: targetInfo,
				seedPred:   pred,
				windowed:   sub.GetWindow() != nil,
			})
		}
	}
	hops.markSole()
	return hops, true, nil
}

// markSole flags each forward hop that is the only hop able to put a row of its
// target entity in the closure. Where two hops reach one entity the set records
// no provenance — it is a union of keys — so neither may take a member it did
// not place for one of its own.
func (h *closureHops) markSole() {
	reach := map[string]int{}
	for i := range h.reverse {
		reach[h.reverse[i].rr.sourceEntity]++
	}
	for i := range h.forward {
		reach[h.forward[i].targetInfo.name]++
	}
	for i := range h.forward {
		h.forward[i].sole = reach[h.forward[i].targetInfo.name] == 1
	}
}

// errMaintenanceDeclined unwinds a pass that met something it cannot decide: an
// index that cannot serve a probe, or a walk past its candidate bound. It never
// reaches a caller — maintainClosure turns it into the rebuild, which is always
// a legal answer.
var errMaintenanceDeclined = errors.New("closure maintenance declined")

// maintainClosure updates the follow closure from what moved since the cursor
// instead of walking it again from every holder, reporting false when the
// subscription set is a shape it cannot decide (see resolveHops) or when a
// probe below gives up, and buildClosure must run.
//
// Only three things move a member, and all of them are named here rather than
// searched for:
//
//   - the member's own row changed, so a reverse hop's ref may now name a
//     different row, or the row may have been deleted or filtered out. Every
//     such row is in changes.
//   - a seed entered or left. For a reverse hop the rows pointing at it entered
//     or left with it; for a forward hop the rows IT points at did. A seed that
//     moved with its own row is in changes; one that moved without being
//     written — a window refills, and the row that moved up was never written —
//     is in seedsMoved.
//   - a forward hop's holder re-pointed. Its new targets are named by its own
//     ref field, but what it stopped pointing at is not: deciding that needs the
//     holder's PREVIOUS refs, and the cursor carries only the set they produced.
//     So the targets of a hop whose holders moved are re-decided from the
//     target's side instead — see recheckForwardTargets.
//
// Everything else the cursor held is carried forward unread, which is the whole
// saving — a closure the size of the client's working set costs reads
// proportional to what changed.
//
// One bound is deliberately not re-derived: MaxReverseFanOut counts the matches
// of ONE hop from ONE holder, and this walks only the holders that moved, so a
// holder whose fan-out grew without being touched is not re-counted. The set
// stays bounded by MaxClosureSize either way, which is the bound that protects
// the cursor and the client.
func (e *rowEmitter) maintainClosure(tx storage.ReadTx, prev *syncplanecursorv1.ClosureSet, subs []*syncplanev1.Subscription, changes []storage.Row, seedsMoved []closureKey) (closureRows, bool, error) {
	hops, ok, err := e.resolveHops(subs)
	if err != nil || !ok {
		return nil, false, err
	}
	members := closureSetMembership(prev)
	cr := closureRows{}
	for ck := range members {
		cr[ck] = storage.Row{}
	}

	m := &closureMaint{
		e: e, tx: tx, hops: hops, cr: cr, prev: members,
		seeds:       map[seedMemo]bool{},
		decided:     map[closureKey]bool{},
		written:     map[string]bool{},
		windowMoved: map[string]bool{},
	}
	switch err := m.run(changes, seedsMoved); {
	case errors.Is(err, errMaintenanceDeclined):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	if len(cr) > e.srv.limits.MaxClosureSize {
		return nil, false, errClosureTooLarge(len(cr), e.srv.limits.MaxClosureSize)
	}
	return cr, true, nil
}

// closureMaint is one maintenance pass's working state: the hops it decides
// against, the set it is updating, and what it has settled along the way.
type closureMaint struct {
	e    *rowEmitter
	tx   storage.ReadTx
	hops closureHops
	cr   closureRows
	// prev is the membership the cursor carried, which is what says whether a
	// member may be carried over rather than re-decided.
	prev map[closureKey]bool
	// seeds memoizes which rows are follow roots — a hot key when many changed
	// rows point at the same one.
	seeds map[seedMemo]bool
	// decided names the members this pass has already settled, so the forward
	// recheck neither reads nor re-probes a row it just placed.
	decided map[closureKey]bool
	// written names the entities with a row in the change set, windowMoved
	// those whose window membership moved without one. Together they say
	// whether a forward hop's holders can have re-pointed.
	written     map[string]bool
	windowMoved map[string]bool
}

// run makes the pass: what changed is re-decided and re-walked, what the window
// moved is re-walked, and the forward targets that could have lost their last
// holder are re-decided last, once every addition is in.
func (m *closureMaint) run(changes []storage.Row, seedsMoved []closureKey) error {
	for _, row := range changes {
		m.written[row.Entity] = true
	}
	for _, seed := range seedsMoved {
		m.windowMoved[seed.entity] = true
	}
	for _, row := range changes {
		if err := m.reconsider(row); err != nil {
			return err
		}
		if err := m.rewalkSeed(closureKey{row.Entity, row.Key}, false); err != nil {
			return err
		}
	}
	for _, seed := range seedsMoved {
		if err := m.rewalkSeed(seed, true); err != nil {
			return err
		}
	}
	return m.recheckForwardTargets()
}

// reconsider re-decides one row's membership from scratch, under every hop that
// could hold it. A row held by no hop leaves the closure; the diff turns that
// into a tombstone unless base scope also holds it.
func (m *closureMaint) reconsider(row storage.Row) error {
	ck := closureKey{row.Entity, row.Key}
	m.decided[ck] = true
	held, err := m.heldByAny(row)
	if err != nil {
		return err
	}
	if held {
		m.cr[ck] = row
		return nil
	}
	delete(m.cr, ck)
	return nil
}

// heldByAny stops at the first hop that holds the row, because the closure is a
// union: one path is enough, and the cheap reverse test comes before the
// forward probe.
func (m *closureMaint) heldByAny(row storage.Row) (bool, error) {
	for i := range m.hops.reverse {
		if m.hops.reverse[i].rr.sourceEntity != row.Entity {
			continue
		}
		held, err := m.holds(row, &m.hops.reverse[i])
		if err != nil || held {
			return held, err
		}
	}
	for i := range m.hops.forward {
		if m.hops.forward[i].targetInfo.name != row.Entity {
			continue
		}
		held, err := m.heldForward(row, &m.hops.forward[i])
		if err != nil || held {
			return held, err
		}
	}
	return false, nil
}

// holds reports whether one reverse hop puts this row in the closure: the row
// is live and readable, and its ref field names a seed. It is followReverse's
// test read from the member's side rather than the holder's, which is what lets
// it be answered without walking anything.
func (m *closureMaint) holds(row storage.Row, hop *reverseHop) (bool, error) {
	if row.Deleted {
		return false, nil
	}
	ok, err := m.e.readable(row)
	if err != nil || !ok {
		return false, err
	}
	msg, ok := m.e.decodeRow(hop.sourceInfo, row)
	if !ok {
		return false, nil // quarantined: its ref cannot be read, so it points nowhere
	}
	for _, stub := range refStubsIn(msg.ProtoReflect(), hop.rr.via, m.e.ws) {
		seed, err := m.isSeed(hop, hop.seedInfo.keyOf(stub))
		if err != nil {
			return false, err
		}
		if seed {
			return true, nil
		}
	}
	return false, nil
}

// isSeed answers whether one row of the subscription's own entity is a follow
// root, by the same rule closureSeeds applies to the whole entity: a window's
// member, or a live readable row the predicate matches.
func (m *closureMaint) isSeed(hop *reverseHop, key string) (bool, error) {
	memo := seedMemo{subIndex: hop.subIndex, key: key}
	if known, ok := m.seeds[memo]; ok {
		return known, nil
	}
	seed, err := m.resolveSeed(hop, closureKey{hop.seedInfo.name, key})
	if err != nil {
		return false, err
	}
	m.seeds[memo] = seed
	return seed, nil
}

func (m *closureMaint) resolveSeed(hop *reverseHop, ck closureKey) (bool, error) {
	var (
		row storage.Row
		ok  bool
		err error
	)
	if hop.windowed {
		// The window IS the base scope, so a row outside it seeds nothing
		// whatever its own fields say.
		if !m.e.windowMembers.has(ck.entity, ck.key) {
			return false, nil
		}
		row, ok, err = m.e.windowMembers.rowOf(m.tx, ck)
	} else {
		row, err = m.tx.GetRow(ck.entity, ck.key)
		ok = !errors.Is(err, storage.ErrNotFound)
		if !ok {
			err = nil
		}
	}
	if err != nil || !ok || row.Deleted {
		return false, err
	}
	msg, decoded := m.e.decodeRow(hop.seedInfo, row)
	if !decoded {
		return false, nil // quarantined: it seeds nothing
	}
	if hop.windowed {
		return true, nil // membership already applied the read filter
	}
	filter, err := m.e.filterOf(ck.entity)
	if err != nil {
		return false, err
	}
	if filter != nil && !filter(row.Key, msg) {
		return false, nil
	}
	return hop.seedPred == nil || hop.seedPred.Eval(msg.ProtoReflect()), nil
}

// heldForward reports whether one forward hop puts this row in the closure: the
// row is live and readable, and some seed's ref field names it. Only the last
// part needs anything the row itself does not hold, and it is asked of the
// holders' ref index (anySeedPointsAt) — but only when it has to be. Nothing
// can have started or stopped pointing at a target while the hop's holders sat
// still, so a member of a closure whose holders did not move is carried over
// for the price of the liveness check.
//
// That shortcut needs the hop to be the only one reaching this entity (see
// closureHops.markSole): where two hops overlap, the set records no provenance,
// so a member this hop never placed cannot be taken for one of its own.
func (m *closureMaint) heldForward(row storage.Row, hop *forwardHop) (bool, error) {
	if row.Deleted {
		return false, nil
	}
	ok, err := m.e.readable(row)
	if err != nil || !ok {
		return false, err
	}
	if hop.sole && m.prev[closureKey{row.Entity, row.Key}] && !m.holdersMoved(hop) {
		return true, nil
	}
	msg, ok := m.e.decodeRow(hop.targetInfo, row)
	if !ok {
		return false, nil // quarantined: followForward drops it too
	}
	return m.anySeedPointsAt(hop, msg.ProtoReflect())
}

// holdersMoved reports whether anything can have changed what a hop's seeds
// point at: a write to the holder entity, or — where the seeds are a window's
// members — a window that refilled.
func (m *closureMaint) holdersMoved(hop *forwardHop) bool {
	return m.written[hop.seedInfo.name] || (hop.windowed && m.windowMoved[hop.seedInfo.name])
}

// anySeedPointsAt asks the holder entity's ref index whether any follow root
// references this target row. It is a forward hop's whole cost, and it is spent
// per target in doubt rather than per holder, which is what makes it cheaper
// than the walk it replaces.
//
// A windowed hop is answered from the index ENTRIES alone: an entry names the
// row that points here, and a window's seeds are its members, so being one is a
// lookup in a set already in hand. A predicate-scoped hop has to read the
// holders, because being a seed is a question about their own fields.
//
// It declines rather than answering wrongly when the index cannot serve the
// probe, and when the walk runs past MaxIndexScanCandidates — a target with a
// fan-in that wide is one the rebuild settles for less.
func (m *closureMaint) anySeedPointsAt(hop *forwardHop, msg protoreflect.Message) (bool, error) {
	limits := m.e.srv.limits
	if limits.DisableIndexes {
		return false, errMaintenanceDeclined
	}
	ci := hop.seedInfo.indexByName(refIndexName(hop.fr.fd))
	if ci == nil {
		return false, errMaintenanceDeclined
	}
	if ready, err := ci.ready(m.tx, hop.seedInfo.name); err != nil {
		return false, err
	} else if !ready {
		return false, errMaintenanceDeclined
	}
	values := make([]protoreflect.Value, 0, len(hop.targetInfo.keyFields))
	for _, kf := range hop.targetInfo.keyFields {
		values = append(values, msg.Get(kf))
	}
	lo, hi, err := ci.probeRefIndex(values)
	if err != nil {
		return false, err
	}

	errFound := errors.New("seed found")
	var holders []string
	seen := 0
	err = m.tx.ScanIndex(hop.seedInfo.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
		seen++
		if seen > limits.maxIndexScanCandidates() {
			return errMaintenanceDeclined
		}
		if hop.windowed {
			if m.e.windowMembers.has(hop.seedInfo.name, rowKey) {
				return errFound
			}
			return nil
		}
		holders = append(holders, rowKey)
		return nil
	})
	switch {
	case errors.Is(err, errFound):
		return true, nil
	case err != nil:
		return false, err
	case len(holders) == 0:
		return false, nil
	}

	// Rows are re-read rather than trusted from the entry, for the reason
	// followReverseIndexed states: the entry says a row pointed here when it was
	// written, and this snapshot decides whether it still exists and whether
	// this caller may see it.
	found := false
	batch := &rowBatch{tx: m.tx, entity: hop.seedInfo.name, size: maxRowBatch, emit: func(row storage.Row) error {
		if found || row.Deleted {
			return nil
		}
		holder, ok := m.e.decodeRow(hop.seedInfo, row)
		if !ok {
			return nil // quarantined: its ref cannot be read, so it points nowhere
		}
		seed, err := m.seedRow(hop, row, holder)
		if err != nil {
			return err
		}
		found = seed
		return nil
	}}
	for _, key := range holders {
		if err := batch.add(key); err != nil {
			return false, err
		}
	}
	if err := batch.flush(); err != nil {
		return false, err
	}
	return found, nil
}

// seedRow answers whether one live, decoded row of a forward hop's own entity
// is a follow root, by the rule closureSeeds applies to the whole entity: a
// window's member, or a readable row the predicate matches. It is resolveSeed's
// question asked of a row in hand rather than of a key.
func (m *closureMaint) seedRow(hop *forwardHop, row storage.Row, msg proto.Message) (bool, error) {
	if hop.windowed {
		// The window IS the base scope, so a row outside it seeds nothing
		// whatever its own fields say, and membership already applied the read
		// filter.
		return m.e.windowMembers.has(hop.seedInfo.name, row.Key), nil
	}
	filter, err := m.e.filterOf(hop.seedInfo.name)
	if err != nil {
		return false, err
	}
	if filter != nil && !filter(row.Key, msg) {
		return false, nil
	}
	return hop.seedPred == nil || hop.seedPred.Eval(msg.ProtoReflect()), nil
}

// followTargets adds the rows one seed points at. This is where a forward hop
// GAINS members, and it costs no probe: the ref stubs name their targets
// outright, so the walk is the point reads followForward batches.
func (m *closureMaint) followTargets(hop *forwardHop, row storage.Row) error {
	if row.Deleted {
		return nil
	}
	msg, ok := m.e.decodeRow(hop.seedInfo, row)
	if !ok {
		return nil // quarantined: its ref cannot be read, so it points nowhere
	}
	seed, err := m.seedRow(hop, row, msg)
	if err != nil || !seed {
		return err
	}
	targets, err := m.e.followForward(m.tx,
		frontierRow{entity: hop.seedInfo.name, key: row.Key, msg: msg.ProtoReflect()}, hop.fr)
	if err != nil {
		return err
	}
	for _, target := range targets {
		// followForward resolved these against this snapshot and read-filtered
		// them, so they are members and the recheck below can skip them.
		ck := closureKey{target.row.Entity, target.row.Key}
		m.cr[ck] = target.row
		m.decided[ck] = true
	}
	return nil
}

// recheckForwardTargets re-decides the prior members a forward hop could have
// lost. A holder that re-pointed leaves no trace of where it pointed before —
// the one thing neither the change set nor the cursor records — so when
// anything can have moved a hop's holders, every prior member of its target
// entity becomes a candidate and is settled from the target's side instead.
//
// The candidates cannot outnumber the seeds the rebuild would walk: this
// maintains singular references only, so each seed names at most one target and
// there are at most as many members as seeds. A pull where no holder moved asks
// nothing here at all.
func (m *closureMaint) recheckForwardTargets() error {
	entities := map[string]bool{}
	for i := range m.hops.forward {
		if hop := &m.hops.forward[i]; m.holdersMoved(hop) {
			entities[hop.targetInfo.name] = true
		}
	}
	for entity := range entities {
		var keys []string
		for ck := range m.prev {
			if ck.entity == entity && !m.decided[ck] {
				keys = append(keys, ck.key)
			}
		}
		// Row-key order, so two runs of the same pull read in the same order.
		sort.Strings(keys)
		// A candidate leaves unless the recheck puts it back, which is also the
		// answer for a key the batch finds no row for: nothing holds a row that
		// is not there.
		for _, key := range keys {
			delete(m.cr, closureKey{entity, key})
		}
		batch := &rowBatch{tx: m.tx, entity: entity, size: maxRowBatch, emit: m.reconsider}
		for _, key := range keys {
			if err := batch.add(key); err != nil {
				return err
			}
		}
		if err := batch.flush(); err != nil {
			return err
		}
	}
	return nil
}

// rewalkSeed re-walks the closure around one row of a subscription's own entity
// — a seed — after something moved it. Both directions start from the same read
// of that row:
//
//   - reverse: the rows pointing back cannot be named without asking, and asking
//     is a single index probe per hop (a scan of the source entity where the
//     index cannot serve it, which is also what a deleted seed gets — there is
//     no message left to probe with).
//   - forward: the rows it points at are named by its own ref field, so they
//     cost the point reads followForward batches, and nothing at all once it is
//     no longer a seed. What it STOPPED pointing at is not knowable from here;
//     recheckForwardTargets covers that.
//
// windowed says which of the two ways the seed moved, and the hops that care
// differ by direction. A windowed subscription's seeds ARE its window's
// members, so they move when the window does; a predicate-scoped one's move
// only when their own row changes, since being a seed is decided by the row's
// own fields. For a reverse hop that is the whole rule — an edit leaving a row
// in its window leaves the rows pointing at it alone. A forward hop takes both:
// an edit leaving a seed in its window can still re-point it.
func (m *closureMaint) rewalkSeed(seed closureKey, windowed bool) error {
	var (
		cached storage.Row
		exists bool
		read   bool
	)
	rowOf := func() (storage.Row, bool, error) {
		if read {
			return cached, exists, nil
		}
		read = true
		row, err := m.tx.GetRow(seed.entity, seed.key)
		switch {
		case errors.Is(err, storage.ErrNotFound):
		case err != nil:
			return storage.Row{}, false, err
		default:
			cached, exists = row, true
		}
		return cached, exists, nil
	}

	for i := range m.hops.reverse {
		hop := &m.hops.reverse[i]
		if hop.seedInfo.name != seed.entity || hop.windowed != windowed {
			continue
		}
		row, ok, err := rowOf()
		if err != nil {
			return err
		}
		holder := frontierRow{entity: seed.entity, key: seed.key}
		if ok && !row.Deleted {
			if msg, decoded := m.e.decodeRow(hop.seedInfo, row); decoded {
				holder.msg = msg.ProtoReflect()
			}
		}
		matches, err := m.e.followReverse(m.tx, holder, hop.rr, m.e.srv.limits)
		if err != nil {
			return err
		}
		for _, match := range matches {
			if err := m.reconsider(match.row); err != nil {
				return err
			}
		}
	}

	for i := range m.hops.forward {
		hop := &m.hops.forward[i]
		if hop.seedInfo.name != seed.entity || (windowed && !hop.windowed) {
			continue
		}
		row, ok, err := rowOf()
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := m.followTargets(hop, row); err != nil {
			return err
		}
	}
	return nil
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
func (e *rowEmitter) emitMembershipDiff(tx storage.ReadTx, oldSet map[closureKey]bool, now closureRows, seq uint64, patch *syncplanev1.Patch) error {
	// Additions: members now present that the cursor did not carry.
	for ck, row := range now {
		if oldSet[ck] || row.Version > seq {
			continue // already held, or the incremental scan delivered it
		}
		patch.Changes = append(patch.Changes, rowChange(row))
	}
	// Removals: members the cursor carried that the set no longer holds. They
	// are grouped by entity and resolved in batches, because a client whose
	// window or closure turned over wholesale leaves as many of these as the
	// set is wide, and one read per departed member is what makes a large drop
	// expensive on a networked adapter. A key with no row at all never
	// existed, so the batch dropping it is the right answer: the client holds
	// nothing for it.
	byEntity := map[string][]string{}
	for ck := range oldSet {
		if now.has(ck.entity, ck.key) {
			continue
		}
		byEntity[ck.entity] = append(byEntity[ck.entity], ck.key)
	}
	for entity, keys := range byEntity {
		batch := &rowBatch{tx: tx, entity: entity, size: maxRowBatch, emit: func(row storage.Row) error {
			if row.Version > seq {
				return nil // changed & left: the incremental scan tombstoned or kept it
			}
			if row.Deleted {
				patch.Changes = append(patch.Changes, rowChange(row)) // tombstone at its version
				return nil
			}
			// A still-live row that left the set drops only if base scope does
			// not also hold it (classify's membership checks cannot fire here:
			// the key is in neither the new closure nor the new window).
			class, err := e.classify(row)
			if err != nil {
				return err
			}
			if class != rowVisible {
				patch.Changes = append(patch.Changes, hiddenChange(row))
			}
			return nil
		}}
		for _, key := range keys {
			if err := batch.add(key); err != nil {
				return err
			}
		}
		if err := batch.flush(); err != nil {
			return err
		}
	}
	return nil
}

// refPointsAt reports whether a source row's ref field (singular or repeated)
// references holderKey in workspace ws, by re-deriving each stub's key with
// the holder entity's key derivation (the same f → f.<key> resolution P1 uses).
// A stored value naming another workspace points at a different row however
// its key reads.
func refPointsAt(m protoreflect.Message, via *lww.Ref, holderInfo *entityInfo, holderKey, ws string) bool {
	for _, stub := range refStubsIn(m, via, ws) {
		if holderInfo.keyOf(stub) == holderKey {
			return true
		}
	}
	return false
}

// refStubsIn returns the target stubs a source row's ref field holds that
// resolve in workspace ws — one for a singular ref, one per element for a
// repeated one, none for an unset field. Values naming another workspace, and
// reference messages holding no stub, are left out.
func refStubsIn(m protoreflect.Message, via *lww.Ref, ws string) []protoreflect.Message {
	if !m.Has(via.Field) {
		return nil
	}
	var out []protoreflect.Message
	add := func(value protoreflect.Message) {
		if stub, ok := via.Stub(value); ok && via.ResolvesIn(value, ws) {
			out = append(out, stub)
		}
	}
	if via.Field.IsList() {
		list := m.Get(via.Field).List()
		for i := 0; i < list.Len(); i++ {
			add(list.Get(i).Message())
		}
		return out
	}
	add(m.Get(via.Field).Message())
	return out
}
