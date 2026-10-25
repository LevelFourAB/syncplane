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

// readUnread resolves the rows of the named members of one entity that were
// carried over unread, caching them. Keys that are not members are ignored.
//
// This is rowOf for many members at once: a read per maxRowBatch of them rather
// than a point read each, which is what a caller about to walk every member
// wants. A member whose row has since gone stays unread, and so still reports
// gone from rowOf.
func (cr closureRows) readUnread(tx storage.ReadTx, entity string, keys []string) error {
	batch := &rowBatch{tx: tx, entity: entity, size: maxRowBatch, emit: func(row storage.Row) error {
		cr[closureKey{entity, row.Key}] = row
		return nil
	}}
	for _, key := range keys {
		row, ok := cr[closureKey{entity, key}]
		if !ok || row.Entity != "" {
			continue
		}
		if err := batch.add(key); err != nil {
			return err
		}
	}
	return batch.flush()
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

// closureSetRows decodes a wire ClosureSet into a membership set whose rows
// are all unread (the zero Row): what a caller needs to answer has, and what
// maintenance starts from before it re-reads the members that moved.
func closureSetRows(cs *syncplanecursorv1.ClosureSet) closureRows {
	cr := closureRows{}
	for ck := range closureSetMembership(cs) {
		cr[ck] = storage.Row{}
	}
	return cr
}

// heldSet projects held plus added to the wire ClosureSet, in the same
// deterministic form keySet emits.
func heldSet(held map[closureKey]bool, added []closureKey) *syncplanecursorv1.ClosureSet {
	cr := make(closureRows, len(held)+len(added))
	for ck := range held {
		cr[ck] = storage.Row{}
	}
	for _, ck := range added {
		cr[ck] = storage.Row{}
	}
	return cr.keySet()
}

// ---- reference graph (server mirror of EntityRegistry.buildRefGraph) ----

// forwardRefRel is one forward reference relation surfaced on the source
// entity, or one CASE of a polymorphic one: what a holder row reaches through
// one stored reference, named by the RefPath segment a client spells the
// relation as — the field's JSON (camelCase) accessor, or the oneof's for a
// oneof of references on the holder. The cases of one segment share a
// localName and differ in ref; a holder row takes at most one of them.
type forwardRefRel struct {
	localName    string
	fd           protoreflect.FieldDescriptor
	targetEntity string
	keyFields    []protoreflect.FieldDescriptor // target entity's key fields
	// ref is the stored shape: where the stub and, for a reference message,
	// the workspace sit inside the field value.
	ref *lww.Ref
}

// reverseRefRel is the inverse of one forward case, surfaced on the case's
// target entity. Its name is the holder field's reverse_relation, or
// <plural(source)>By<Field> when the field sets none.
type reverseRefRel struct {
	name         string
	sourceEntity string
	viaField     protoreflect.FieldDescriptor
	// via is the forward reference this reverses, with its stored shape.
	via *lww.Ref
}

type refGraph struct {
	// forward holds the cases of every forward relation, one per target it
	// can reach: a single entry for a relation to one entity, one per case for
	// a polymorphic one, in accessor order.
	forward map[string]map[string][]*forwardRefRel // entity -> localName -> cases
	reverse map[string]map[string]*reverseRefRel   // entity -> relName  -> rel
}

// forwardBy returns the cases of a forward relation, or nil when the segment
// names none on the entity.
func (g *refGraph) forwardBy(entity, seg string) []*forwardRefRel { return g.forward[entity][seg] }
func (g *refGraph) reverseBy(entity, seg string) *reverseRefRel   { return g.reverse[entity][seg] }

// buildRefGraph mirrors client EntityRegistry.buildRefGraph exactly: forward
// refs keyed by source, and one reverse relation per forward case, named by
// the holder field alone, so a name never changes when a sibling reference is
// added. Field names are unique, so two default names from different fields
// of one entity never clash; a reverse_relation, two cases of one polymorphic
// reference message on one target, or a forward ref field of the same name can.
// A clash is a hard error. A ref whose target entity is not registered
// here is omitted (cannot be followed locally); a polymorphic relation keeps
// the cases whose targets are.
//
// The two halves share ONE segment namespace per entity — that is what a
// RefPath segment, an include key and client.db.<entity>.refs all resolve
// against — so a reverse name must be unique against BOTH the other reverse
// names and the entity's forward ref fields.
func (c *catalog) buildRefGraph() (*refGraph, error) {
	g := &refGraph{
		forward: map[string]map[string][]*forwardRefRel{},
		reverse: map[string]map[string]*reverseRefRel{},
	}
	for _, name := range c.entityNames() {
		info := c.entities[name]
		fwd := map[string][]*forwardRefRel{}
		fields := info.msgType.Descriptor().Fields()
		// Sort by proto field name, matching the client's deterministic order.
		ordered := make([]protoreflect.FieldDescriptor, 0, fields.Len())
		for i := 0; i < fields.Len(); i++ {
			ordered = append(ordered, fields.Get(i))
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name() < ordered[j].Name() })
		for _, fd := range ordered {
			for _, ref := range lww.RefsOf(fd) {
				target := c.byType[ref.Target.FullName()]
				if target == nil {
					continue // target entity not registered here; not followable
				}
				seg := refSegmentName(ref)
				fwd[seg] = append(fwd[seg], &forwardRefRel{
					localName:    seg,
					fd:           fd,
					targetEntity: target.name,
					keyFields:    ref.KeyFields,
					ref:          ref,
				})
			}
		}
		g.forward[name] = fwd
	}
	// Reverse relations: one per forward case, on the case's target. Sources
	// and their cases run in a fixed order so a clash between three or more
	// relations reports the same pair on every build.
	for _, source := range c.entityNames() {
		locals := make([]string, 0, len(g.forward[source]))
		for ln := range g.forward[source] {
			locals = append(locals, ln)
		}
		sort.Strings(locals)
		for _, ln := range locals {
			for _, r := range g.forward[source][ln] {
				rel := &reverseRefRel{
					name:         reverseRelationName(source, r.fd),
					sourceEntity: source,
					viaField:     r.fd,
					via:          r.ref,
				}
				target := r.targetEntity
				if g.reverse[target] == nil {
					g.reverse[target] = map[string]*reverseRefRel{}
				}
				if prev := g.reverse[target][rel.name]; prev != nil {
					return nil, reverseClashError(rel.name, target, prev, rel)
				}
				// Forward names are complete by now (the loop above ran over every
				// entity), so this sees the whole namespace the segment resolves
				// against. Shadowing is worse than ambiguity: forward-first
				// resolution would answer the segment with the holder's own ref
				// list instead of the rows pointing back, which is a different row
				// set and no error anywhere.
				if _, shadowed := g.forward[target][rel.name]; shadowed {
					return nil, fmt.Errorf("server: reverse relation %q on entity %q collides with the reference field of that name; "+
						"set (syncplane.v1.field_behavior).reverse_relation on %s to name it otherwise", rel.name, target, rel.origin())
				}
				g.reverse[target][rel.name] = rel
			}
		}
	}
	return g, nil
}

// reverseRelationName is the name of the reverse relation a reference field
// of source adds to its target: the field's reverse_relation, else
// <plural(source)>By<Field> in camelCase — issuesByAssignee for issue.assignee,
// taskListsByOwner for task_list.owner. A polymorphic reference message gives
// every case the same name, since each case lands on its own target.
//
// The field part comes from the proto name, not JSONName: a json_name option
// changes the JSON mapping only, and the client derives the same name from the
// proto name without reading it.
func reverseRelationName(source string, fd protoreflect.FieldDescriptor) string {
	if name := fieldBehaviorOf(fd).GetReverseRelation(); name != "" {
		return name
	}
	return pluralizeEntity(protoCamelCase(source)) + "By" + capitalizeFirst(protoCamelCase(string(fd.Name())))
}

// origin spells the forward case a reverse relation reverses as a dotted proto
// path: source entity, holder field and, for a case inside a reference
// message, that case's field.
func (r *reverseRefRel) origin() string {
	s := r.sourceEntity + "." + string(r.viaField.Name())
	if r.via.InnerCase() {
		s += "." + string(r.via.Value.Name())
	}
	return s
}

// reverseClashError reports two forward cases whose reverse relations take one
// name on one target, naming both cases in sorted order so the message does
// not depend on which was seen first. The advice differs for two cases of one
// field: the field gives every case one name, so reverse_relation cannot tell
// them apart.
func reverseClashError(name, target string, a, b *reverseRefRel) error {
	first, second := a.origin(), b.origin()
	if second < first {
		first, second = second, first
	}
	advice := "set (syncplane.v1.field_behavior).reverse_relation on one of them to give it another name"
	if a.sourceEntity == b.sourceEntity && a.viaField == b.viaField {
		advice = fmt.Sprintf("%s.%s gives all its cases one reverse relation name, so cases that point at one entity need fields of their own",
			a.sourceEntity, a.viaField.Name())
	}
	return fmt.Errorf("server: reverse relation %q on entity %q is named by both %s and %s; %s", name, target, first, second, advice)
}

// refSegmentName is the RefPath segment a reference is followed by: the
// holder's oneof when the reference is a member of one — the accessor the
// client's message shape has, and the one name its cases share — else the
// field's own accessor.
func refSegmentName(ref *lww.Ref) string {
	if ref.Group != nil {
		return protoCamelCase(string(ref.Group.Name()))
	}
	return ref.Field.JSONName()
}

// protoCamelCase mirrors the client's accessor derivation for a oneof
// (protobuf-es protoCamelCase, the JSON-name rule): an underscore is dropped
// and capitalizes the next letter; a digit ends the capitalization.
func protoCamelCase(name string) string {
	b := make([]byte, 0, len(name))
	capNext := false
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_':
			capNext = true
		case c >= '0' && c <= '9':
			b = append(b, c)
			capNext = false
		default:
			if capNext {
				capNext = false
				if c >= 'a' && c <= 'z' {
					c -= 'a' - 'A'
				}
			}
			b = append(b, c)
		}
	}
	return string(b)
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
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return s
	}
	return string(s[0]-('a'-'A')) + s[1:]
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
							tos, err := g.segmentTargets(c, seg)
							if err != nil {
								return nil, err
							}
							for _, to := range tos {
								out[to] = true
								step[to] = true
							}
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

// segmentTargets resolves one path segment from an entity to the entities it
// can land on: a forward relation's target, one per case for a polymorphic
// one, or a reverse relation's source.
func (g *refGraph) segmentTargets(entity, seg string) ([]string, error) {
	if cases := g.forwardBy(entity, seg); cases != nil {
		out := make([]string, 0, len(cases))
		for _, fr := range cases {
			out = append(out, fr.targetEntity)
		}
		return out, nil
	}
	if rr := g.reverseBy(entity, seg); rr != nil {
		return []string{rr.sourceEntity}, nil
	}
	return nil, errUnknownSegment(g, entity, seg)
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
			// A round walks each path over the WHOLE frontier rather than each
			// frontier row over each path, so that a forward hop sees every
			// holder's targets at once and can read them in batches. The rows
			// reached are the same set either way; only the order the closure
			// gains them in changes, and it stays deterministic.
			holders := make([]frontierRow, 0, len(frontier))
			for _, holder := range frontier {
				hk := closureKey{holder.entity, holder.key}
				if visited[hk] {
					continue
				}
				visited[hk] = true
				holders = append(holders, holder)
			}
			var next []frontierRow
			for _, path := range follow.GetPaths() {
				ends, err := e.walkPath(tx, g, holders, path.GetSegments(), cr, limits)
				if err != nil {
					return nil, err
				}
				next = append(next, ends...)
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
// many of them as the base scope has rows and each one is walked. Three things
// keep that from being a full scan and decode of the entity every time: a
// windowed subscription seeds from rows already in hand, a keyed one reads the
// rows its set names, and a predicated one asks the shared candidate source to
// name its anchors.
func (e *rowEmitter) closureSeeds(tx storage.ReadTx, sub *syncplanev1.Subscription) ([]frontierRow, error) {
	info, ok := e.srv.catalogPtr().entities[sub.GetEntity()]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("subscription names unknown entity %q", sub.GetEntity()))
	}
	if blind, err := e.blindSeeds(sub); err != nil || blind {
		return nil, err
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
		if ok, err := filter.admits(row.Key, msg); err != nil || !ok {
			return err
		}
		if p != nil && !p.Eval(m) {
			return nil
		}
		seeds = append(seeds, frontierRow{entity: info.name, key: row.Key, msg: m})
		return nil
	}
	// A key set already names its seeds. They are read in key order, the order
	// the scan below would produce them in, and each is still decoded and
	// read-filtered: the set is the client's claim, not a resolved membership
	// like a window's.
	if ks := sub.GetKeySet(); ks != nil {
		batch := &rowBatch{tx: tx, entity: info.name, size: maxRowBatch,
			emit: func(row storage.Row) error { return consider(row, nil) }}
		for _, key := range canonicalKeySet(ks).GetKeys() {
			if err := batch.add(key); err != nil {
				return nil, err
			}
		}
		if err := batch.flush(); err != nil {
			return nil, err
		}
		return seeds, nil
	}
	// The subscription's predicate is a predicate like any other, so the source
	// that names a snapshot's or a resync's rows names the seeds too: a
	// secondary index over the predicate, or the single key an equality on every
	// key field pins. That second one is what serves "the issue with id X and
	// everything around it" — an anchor on the key fields, which carry no index
	// of their own.
	//
	// With no predicate there is nothing to narrow by and the seeds ARE the
	// entity, so the scan is the answer rather than a fallback, and asking the
	// planner would only cost a readiness read per index to be told so.
	//
	// The candidates are a superset — an index entry can outlive its row, and
	// neither the residual nor the read filter narrows them — so each candidate
	// row goes through the same classification the scan applies. Key order is
	// what keeps the seeds in the order the scan would produce them in.
	if pred != nil {
		keys, narrowed, err := candidateKeys(tx, info,
			[]*entityScope{{preds: []scopePred{{compiled: pred, cond: sub.GetPredicate()}}}},
			nil, e.srv.limits.MaxResyncScan, e.srv.limits)
		if err != nil {
			return nil, err
		}
		if narrowed {
			batch := &rowBatch{tx: tx, entity: info.name, size: maxRowBatch,
				emit: func(row storage.Row) error { return consider(row, pred) }}
			for _, key := range keys {
				if err := batch.add(key); err != nil {
					return nil, err
				}
			}
			if err := batch.flush(); err != nil {
				return nil, err
			}
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
// already in hand is what this replaces. The members a window maintained
// without re-reading — which is most of them when a pull maintains its windows
// but still rebuilds its closure — are read in batches before the loop rather
// than one point lookup each (see closureRows.readUnread).
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
	if err := e.windowMembers.readUnread(tx, info.name, keys); err != nil {
		return nil, err
	}
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

// walkPath walks a path's segments from a set of holders at once, adding every
// resolved row to the closure and returning the path's terminal rows (the next
// depth round re-applies the whole path set from these).
//
// The holders take a segment TOGETHER, which is what lets the forward direction
// read in batches (see followForward). The rows are then fanned back out in
// holder order and, within a holder, in the ref field's order — the order a
// walk per holder produced them in, and the one that keeps two runs of the same
// pull comparable.
func (e *rowEmitter) walkPath(tx storage.ReadTx, g *refGraph, holders []frontierRow, segments []string, cr closureRows, limits Limits) ([]frontierRow, error) {
	current := holders
	for _, seg := range segments {
		targets, rows, err := e.followForward(tx, g, current, seg)
		if err != nil {
			return nil, err
		}
		var next []frontierRow
		seen := map[closureKey]bool{}
		join := func(r resolved) error {
			ck := closureKey{r.row.Entity, r.row.Key}
			if seen[ck] {
				return nil
			}
			seen[ck] = true
			if _, ok := cr[ck]; !ok {
				cr[ck] = r.row
				if len(cr) > limits.MaxClosureSize {
					return errClosureTooLarge(len(cr), limits.MaxClosureSize)
				}
			}
			next = append(next, r.front)
			return nil
		}
		for i, h := range current {
			if t := targets[i]; t.forward {
				for _, ck := range t.rows {
					r, ok := rows[ck]
					if !ok {
						continue // orphan, tombstone, quarantined or read-filtered
					}
					if err := join(r); err != nil {
						return nil, err
					}
				}
				continue
			}
			out, err := e.followReverse(tx, h, g.reverseBy(h.entity, seg), limits)
			if err != nil {
				return nil, err
			}
			for _, r := range out {
				if err := join(r); err != nil {
					return nil, err
				}
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

// forwardTargets names the rows one holder's forward ref points at, in the
// ref field's order — each with the entity it lives in, since the cases of a
// polymorphic relation reach different entities. forward is false for a
// holder that takes the segment's REVERSE direction instead.
type forwardTargets struct {
	forward bool
	rows    []closureKey
}

// followForward resolves the forward hop of one segment for every holder that
// takes it, and reports what each holder pointed at so the caller can fan the
// rows back out in holder order.
//
// The stubs name their targets outright, so every holder's keys are known
// before any row is read: one batch per 256 distinct targets replaces a lookup
// per holder, and holders naming the same target — the ~20 projects a window of
// 500 issues points at — cost one read between them. That matters most where a
// read is a network round trip, which is the whole cost of a from-scratch
// closure build on a remote store.
//
// Only live, readable targets reach the returned rows: a missing target
// (orphan), a tombstone, a quarantined row and a read-filtered one are each
// absent from it, and the caller drops the holder's key accordingly.
//
// Returns an error for a segment that names no relation on some holder's
// entity; holders taking the reverse direction are left to the caller, since
// that hop is an index probe per holder.
func (e *rowEmitter) followForward(tx storage.ReadTx, g *refGraph, holders []frontierRow, seg string) ([]forwardTargets, map[closureKey]resolved, error) {
	targets := make([]forwardTargets, len(holders))
	// Distinct target keys per entity, in holder-then-field order, so two runs
	// of the same pull read in the same order.
	var entities []string
	byEntity := map[string][]string{}
	queued := map[closureKey]bool{}
	for i, h := range holders {
		cases := g.forwardBy(h.entity, seg)
		if cases == nil {
			if g.reverseBy(h.entity, seg) == nil {
				return nil, nil, errUnknownSegment(g, h.entity, seg)
			}
			continue
		}
		targets[i].forward = true
		// A holder takes at most one case of a polymorphic relation, but a
		// repeated reference message holds one case PER ELEMENT, so every case
		// is read and the keys keep the field's element order within a case.
		for _, fr := range cases {
			targetInfo := e.srv.catalogPtr().entities[fr.targetEntity]
			if _, known := byEntity[fr.targetEntity]; !known {
				entities = append(entities, fr.targetEntity)
				byEntity[fr.targetEntity] = nil
			}
			for _, key := range forwardTargetKeys(h.msg, fr, targetInfo, e.ws) {
				ck := closureKey{fr.targetEntity, key}
				targets[i].rows = append(targets[i].rows, ck)
				if queued[ck] {
					continue
				}
				queued[ck] = true
				byEntity[fr.targetEntity] = append(byEntity[fr.targetEntity], key)
			}
		}
	}
	rows := map[closureKey]resolved{}
	for _, entity := range entities {
		keys := byEntity[entity]
		if len(keys) == 0 {
			continue
		}
		targetInfo := e.srv.catalogPtr().entities[entity]
		filter, err := e.filterOf(entity)
		if err != nil {
			return nil, nil, err
		}
		batch := &rowBatch{tx: tx, entity: entity, size: maxRowBatch, emit: func(row storage.Row) error {
			// A key with no row at all is an orphan the batch already dropped:
			// a target never synced here, or one that never existed.
			if row.Deleted {
				return nil
			}
			msg, ok := e.decodeRow(targetInfo, row)
			if !ok {
				return nil // quarantined: dropped like an orphan, and unfollowable
			}
			if ok, err := filter.admits(row.Key, msg); err != nil || !ok {
				return err // permission prune — and we cannot follow through it
			}
			rows[closureKey{row.Entity, row.Key}] = resolved{
				row:   row,
				front: frontierRow{entity: entity, key: row.Key, msg: msg.ProtoReflect()},
			}
			return nil
		}}
		for _, key := range keys {
			if err := batch.add(key); err != nil {
				return nil, nil, err
			}
		}
		if err := batch.flush(); err != nil {
			return nil, nil, err
		}
	}
	return targets, rows, nil
}

// forwardTargetKeys names the rows one holder's forward ref (singular or
// repeated) points at, in the field's order.
func forwardTargetKeys(holder protoreflect.Message, fr *forwardRefRel, targetInfo *entityInfo, ws string) []string {
	if !holder.Has(fr.fd) {
		return nil
	}
	if !fr.fd.IsList() {
		if key, ok := forwardTargetKey(holder, fr, targetInfo, ws); ok {
			return []string{key}
		}
		return nil
	}
	list := holder.Get(fr.fd).List()
	keys := make([]string, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		value := list.Get(i).Message()
		// A stored value naming another workspace resolves there, not in this
		// one (see ReferenceOptions), so it contributes nothing to this closure.
		stub, ok := fr.ref.Stub(value)
		if !ok || !fr.ref.ResolvesIn(value, ws) {
			continue
		}
		keys = append(keys, targetInfo.keyOf(stub))
	}
	return keys
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
		if ok, err := filter.admits(row.Key, msg); err != nil || !ok {
			return err
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
	ci := sourceInfo.indexByName(refIndexName(rr.via))
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
// What makes it maintainable anyway is that the question can be answered from
// the target's side for the targets whose membership is actually in doubt: from
// the window's own members where the seeds are a window's, and from the holders'
// ref index otherwise. See heldForward for when a target is in doubt, and
// anySeedPointsAt for what each answer costs and when it gives up.
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
	// windowTargets is the set of target row keys this hop's window members
	// name through fr — the whole membership rule for a windowed hop. Filled by
	// buildWindowTargets on the first target in doubt; nil for a
	// predicate-scoped hop and before that.
	windowTargets map[string]bool
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
			cases := g.forwardBy(sub.GetEntity(), segs[0])
			// A segment that names nothing at all is an error buildClosure is
			// the right place to raise.
			if cases == nil {
				return closureHops{}, false, nil
			}
			// Each case of a polymorphic relation is its own hop: it reaches its
			// own target entity through its own index, and a holder that takes
			// another case holds nothing for it, exactly like an unset field.
			for _, fr := range cases {
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
	// A subscription that seeds nothing under the bound access (see
	// blindSubs) left members in prev that no change names, so the closure is
	// walked again.
	if blind, err := e.blindSubs(); err != nil || blind != nil {
		return nil, false, err
	}
	hops, ok, err := e.resolveHops(subs)
	if err != nil || !ok {
		return nil, false, err
	}
	members := closureSetMembership(prev)
	cr := closureSetRows(prev)

	m := &closureMaint{
		e: e, tx: tx, hops: hops, cr: cr, prev: members,
		seeds:              map[seedMemo]bool{},
		decided:            map[closureKey]bool{},
		written:            map[string]bool{},
		windowMoved:        map[string]bool{},
		windowTargetsBuilt: map[string]bool{},
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
	// windowTargetsBuilt names the seed entities whose windowed forward hops
	// have had their target sets resolved, so the window is read once however
	// many targets are in doubt.
	windowTargetsBuilt map[string]bool
}

// seedWalk is one seed this pass must re-walk, and how it moved. A seed that
// both changed and moved with its window is ONE walk: the two ways of moving
// select different reverse hops but the same forward ones, so walking it twice
// would read and follow it twice for one answer.
type seedWalk struct {
	ck closureKey
	// row is the seed's row where one is in hand — a change set carries it, and
	// so does a window membership that resolved it. read says a lookup has
	// already been made and found nothing, so the zero row is the answer.
	row  storage.Row
	read bool
	// changed says the seed's own row is in the change set; windowMoved that
	// its window's membership moved it without one.
	changed     bool
	windowMoved bool
}

// run makes the pass: what changed is re-decided, every moved seed is re-walked
// in both directions, and the forward targets that could have lost their last
// holder are re-decided last, once every addition is in.
//
// The walks are collected before any of them runs so that the forward half can
// resolve all of their targets together. That is the difference between a query
// per moved seed and a query per batch of targets, and on a pull where a whole
// window moved it is the pass's whole cost.
func (m *closureMaint) run(changes []storage.Row, seedsMoved []closureKey) error {
	for _, row := range changes {
		m.written[row.Entity] = true
	}
	for _, seed := range seedsMoved {
		m.windowMoved[seed.entity] = true
	}
	walks := make([]seedWalk, 0, len(changes)+len(seedsMoved))
	at := make(map[closureKey]int, len(changes)+len(seedsMoved))
	for _, row := range changes {
		if err := m.reconsider(row); err != nil {
			return err
		}
		ck := closureKey{row.Entity, row.Key}
		if i, ok := at[ck]; ok {
			walks[i].row, walks[i].changed = row, true
			continue
		}
		at[ck] = len(walks)
		walks = append(walks, seedWalk{ck: ck, row: row, changed: true})
	}
	for _, seed := range seedsMoved {
		if i, ok := at[seed]; ok {
			walks[i].windowMoved = true
			continue
		}
		at[seed] = len(walks)
		// A member the window still holds may already have its row; one that
		// left the window is a key alone until readSeedRows resolves it.
		walks = append(walks, seedWalk{ck: seed, row: m.e.windowMembers[seed], windowMoved: true})
	}
	if err := m.readSeedRows(walks); err != nil {
		return err
	}
	for i := range walks {
		if err := m.rewalkReverse(&walks[i]); err != nil {
			return err
		}
	}
	if err := m.followSeeds(walks); err != nil {
		return err
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
	if ok, err := filter.admits(row.Key, msg); err != nil || !ok {
		return false, err
	}
	return hop.seedPred == nil || hop.seedPred.Eval(msg.ProtoReflect()), nil
}

// heldForward reports whether one forward hop puts this row in the closure: the
// row is live and readable, and some seed's ref field names it. Only the last
// part needs anything the row itself does not hold, and it is asked of the
// seeds (anySeedPointsAt) — but only when it has to be. Nothing can have started
// or stopped pointing at a target while the hop's holders sat still, so a member
// of a closure whose holders did not move is carried over for the price of the
// liveness check.
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
	return m.anySeedPointsAt(hop, row.Key, msg.ProtoReflect())
}

// holdersMoved reports whether anything can have changed what a hop's seeds
// point at: a write to the holder entity, or — where the seeds are a window's
// members — a window that refilled.
func (m *closureMaint) holdersMoved(hop *forwardHop) bool {
	return m.written[hop.seedInfo.name] || (hop.windowed && m.windowMoved[hop.seedInfo.name])
}

// anySeedPointsAt reports whether any of a forward hop's follow roots references
// this target row. It is a forward hop's whole cost, and it is spent per target
// in doubt rather than per holder, which is what makes it cheaper than the walk
// it replaces.
//
// The two hop shapes are answered from opposite ends, because their seed sets
// are:
//
//   - A WINDOWED hop's seeds are the window's members, which are bounded by
//     MaxWindowSize and already in hand, so the question is a lookup in the set
//     of what they point at (buildWindowTargets).
//   - A PREDICATE-SCOPED hop's seeds are unbounded and decided by the holders'
//     own fields, so it probes the holder entity's ref index for the rows that
//     point here and reads them.
//
// The index probe declines rather than answering wrongly when the index cannot
// serve it, and when the walk runs past MaxIndexScanCandidates — a target with a
// fan-in that wide is one the rebuild settles for less.
func (m *closureMaint) anySeedPointsAt(hop *forwardHop, key string, msg protoreflect.Message) (bool, error) {
	if hop.windowed {
		if err := m.buildWindowTargets(hop.seedInfo.name); err != nil {
			return false, err
		}
		return hop.windowTargets[key], nil
	}
	limits := m.e.srv.limits
	if limits.DisableIndexes {
		return false, errMaintenanceDeclined
	}
	ci := hop.seedInfo.indexByName(refIndexName(hop.fr.ref))
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

	var holders []string
	seen := 0
	err = m.tx.ScanIndex(hop.seedInfo.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
		seen++
		if seen > limits.maxIndexScanCandidates() {
			return errMaintenanceDeclined
		}
		holders = append(holders, rowKey)
		return nil
	})
	switch {
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

// buildWindowTargets resolves windowTargets for every windowed forward hop
// seeded by one entity, in a single pass over that entity's window members.
// Hops seeded by the same window share the pass, so a member is read and decoded
// once however many relations are followed from it.
//
// The set is safe to hold for the whole maintenance pass: pullPage resolves
// every window (refreshWindows) BEFORE it calls maintainClosure, and nothing
// here changes a membership, so the members this reads are the members the pass
// decides against from first target to last.
//
// A member the window carried over unread costs a row read, batched, so the
// whole pass is bounded by MaxWindowSize however wide the fan-in of the targets
// it settles.
func (m *closureMaint) buildWindowTargets(entity string) error {
	if m.windowTargetsBuilt[entity] {
		return nil
	}
	m.windowTargetsBuilt[entity] = true
	var hops []*forwardHop
	for i := range m.hops.forward {
		if hop := &m.hops.forward[i]; hop.windowed && hop.seedInfo.name == entity {
			hop.windowTargets = map[string]bool{}
			hops = append(hops, hop)
		}
	}
	if len(hops) == 0 {
		return nil
	}
	info := hops[0].seedInfo
	add := func(row storage.Row) error {
		if row.Deleted {
			return nil
		}
		msg, ok := m.e.decodeRow(info, row)
		if !ok {
			return nil // quarantined: its ref cannot be read, so it points nowhere
		}
		holder := msg.ProtoReflect()
		for _, hop := range hops {
			if key, ok := forwardTargetKey(holder, hop.fr, hop.targetInfo, m.e.ws); ok {
				hop.windowTargets[key] = true
			}
		}
		return nil
	}
	// Row-key order, so two runs of the same pull read in the same order.
	keys := make([]string, 0, len(m.e.windowMembers))
	for ck := range m.e.windowMembers {
		if ck.entity == entity {
			keys = append(keys, ck.key)
		}
	}
	sort.Strings(keys)
	batch := &rowBatch{tx: m.tx, entity: entity, size: maxRowBatch, emit: func(row storage.Row) error {
		// Cache the read on the membership, so a reverse hop asking about the
		// same member later (see resolveSeed) does not read it again.
		m.e.windowMembers[closureKey{entity, row.Key}] = row
		return add(row)
	}}
	for _, key := range keys {
		row := m.e.windowMembers[closureKey{entity, key}]
		if row.Entity == "" {
			if err := batch.add(key); err != nil {
				return err
			}
			continue
		}
		if err := add(row); err != nil {
			return err
		}
	}
	return batch.flush()
}

// forwardTargetKey names the row a holder's SINGULAR forward reference points
// at, reporting false for an unset field, a value holding no stub, and one
// naming another workspace — the three cases followForward contributes nothing
// for.
func forwardTargetKey(holder protoreflect.Message, fr *forwardRefRel, targetInfo *entityInfo, ws string) (string, bool) {
	if !holder.Has(fr.fd) {
		return "", false
	}
	value := holder.Get(fr.fd).Message()
	stub, ok := fr.ref.Stub(value)
	if !ok || !fr.ref.ResolvesIn(value, ws) {
		return "", false
	}
	return targetInfo.keyOf(stub), true
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
	if ok, err := filter.admits(row.Key, msg); err != nil || !ok {
		return false, err
	}
	return hop.seedPred == nil || hop.seedPred.Eval(msg.ProtoReflect()), nil
}

// readSeedRows resolves the rows of the seeds named by key alone — the members
// a window gained or lost, which no change set carried — one batch per entity
// rather than a point read each.
//
// Only the entities a WINDOWED hop is seeded by are read: a seed that moved
// with its window and reaches no such hop is never asked for its row, and a
// seed whose own row changed already has one.
func (m *closureMaint) readSeedRows(walks []seedWalk) error {
	seeded := map[string]bool{}
	for i := range m.hops.reverse {
		if hop := &m.hops.reverse[i]; hop.windowed {
			seeded[hop.seedInfo.name] = true
		}
	}
	for i := range m.hops.forward {
		if hop := &m.hops.forward[i]; hop.windowed {
			seeded[hop.seedInfo.name] = true
		}
	}
	pending := map[string][]int{}
	for i := range walks {
		w := &walks[i]
		if w.read || w.row.Entity != "" || !seeded[w.ck.entity] {
			continue
		}
		pending[w.ck.entity] = append(pending[w.ck.entity], i)
	}
	entities := make([]string, 0, len(pending))
	for entity := range pending {
		entities = append(entities, entity)
	}
	sort.Strings(entities)
	for _, entity := range entities {
		byKey := make(map[string]int, len(pending[entity]))
		keys := make([]string, 0, len(pending[entity]))
		for _, i := range pending[entity] {
			// A key the batch finds no row for keeps the zero row, which read
			// then makes the settled answer.
			walks[i].read = true
			byKey[walks[i].ck.key] = i
			keys = append(keys, walks[i].ck.key)
		}
		// Row-key order, so two runs of the same pull read in the same order.
		sort.Strings(keys)
		batch := &rowBatch{tx: m.tx, entity: entity, size: maxRowBatch, emit: func(row storage.Row) error {
			i, ok := byKey[row.Key]
			if !ok {
				return nil // a key the batch never asked for settles nothing
			}
			walks[i].row = row
			// Cache the read on the membership, so a later reader of the same
			// member (see resolveSeed, buildWindowTargets) does not read it
			// again.
			if m.e.windowMembers.has(entity, row.Key) {
				m.e.windowMembers[closureKey{entity, row.Key}] = row
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

// seedRowOf resolves one walk's seed row, reading it only where nothing has
// already put one in hand.
func (m *closureMaint) seedRowOf(w *seedWalk) (storage.Row, bool, error) {
	if w.read || w.row.Entity != "" {
		return w.row, w.row.Entity != "", nil
	}
	w.read = true
	row, err := m.tx.GetRow(w.ck.entity, w.ck.key)
	switch {
	case errors.Is(err, storage.ErrNotFound):
	case err != nil:
		return storage.Row{}, false, err
	default:
		w.row = row
	}
	return w.row, w.row.Entity != "", nil
}

// followSeeds adds the rows the moved seeds point at. This is where a forward
// hop GAINS members, and it costs no probe: a ref names its target outright.
//
// So the pass is point reads, and they are spent on the TARGETS rather than on
// the seeds: every seed's target key is collected first and the whole set is
// resolved one batch per hop. Two seeds naming the same row cost one read
// between them, and a window that moved wholesale costs a query per 256 targets
// instead of one per member.
func (m *closureMaint) followSeeds(walks []seedWalk) error {
	if len(m.hops.forward) == 0 {
		return nil
	}
	// Target keys per hop, in seed order, so two runs of the same pull read in
	// the same order.
	targets := make([][]string, len(m.hops.forward))
	var applies []int
	for i := range walks {
		w := &walks[i]
		applies = applies[:0]
		for h := range m.hops.forward {
			hop := &m.hops.forward[h]
			// A seed that moved with its window re-points nothing a
			// predicate-scoped hop follows: being one of ITS seeds is decided by
			// the row's own fields, which only a write can change.
			if hop.seedInfo.name == w.ck.entity && (w.changed || hop.windowed) {
				applies = append(applies, h)
			}
		}
		if len(applies) == 0 {
			continue
		}
		row, ok, err := m.seedRowOf(w)
		if err != nil {
			return err
		}
		if !ok || row.Deleted {
			continue
		}
		// Decoded once for every hop this row seeds: the hops share an entity,
		// so they share the decode.
		msg, decoded := m.e.decodeRow(m.hops.forward[applies[0]].seedInfo, row)
		if !decoded {
			continue // quarantined: its refs cannot be read, so it points nowhere
		}
		holder := msg.ProtoReflect()
		for _, h := range applies {
			hop := &m.hops.forward[h]
			seed, err := m.seedRow(hop, row, msg)
			if err != nil {
				return err
			}
			if !seed {
				continue
			}
			if key, ok := forwardTargetKey(holder, hop.fr, hop.targetInfo, m.e.ws); ok {
				targets[h] = append(targets[h], key)
			}
		}
	}
	for h := range m.hops.forward {
		if err := m.addForwardTargets(&m.hops.forward[h], targets[h]); err != nil {
			return err
		}
	}
	return nil
}

// addForwardTargets resolves one hop's collected target keys and records the
// live, readable ones as members. It applies followForward's per-target rule —
// an orphan, a tombstone, a quarantined row and a read-filtered one each
// contribute nothing — to every seed's targets at once.
func (m *closureMaint) addForwardTargets(hop *forwardHop, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	filter, err := m.e.filterOf(hop.targetInfo.name)
	if err != nil {
		return err
	}
	batch := &rowBatch{tx: m.tx, entity: hop.targetInfo.name, size: maxRowBatch, emit: func(row storage.Row) error {
		if row.Deleted {
			return nil
		}
		msg, ok := m.e.decodeRow(hop.targetInfo, row)
		if !ok {
			return nil // quarantined: followForward drops it too
		}
		if ok, err := filter.admits(row.Key, msg); err != nil || !ok {
			return err // permission prune — and we cannot follow through it
		}
		// Resolved against this snapshot and read-filtered, so these are
		// members and recheckForwardTargets can skip them.
		ck := closureKey{row.Entity, row.Key}
		m.cr[ck] = row
		m.decided[ck] = true
		return nil
	}}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		if err := batch.add(key); err != nil {
			return err
		}
	}
	return batch.flush()
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

// rewalkReverse re-decides the rows that point AT one moved seed. They cannot
// be named without asking, and asking is a single index probe per hop (a scan of
// the source entity where the index cannot serve it, which is also what a
// deleted seed gets — there is no message left to probe with).
//
// Which hops care follows from how the seed moved. A windowed subscription's
// seeds ARE its window's members, so they move when the window does; a
// predicate-scoped one's move only when their own row changes, since being a
// seed is decided by the row's own fields. Either way an edit that leaves a row
// where it was leaves the rows pointing at it alone.
//
// The forward direction is not walked here: its targets are named by the seed's
// own ref field, so every moved seed's are resolved together instead (see
// followSeeds).
func (m *closureMaint) rewalkReverse(w *seedWalk) error {
	for i := range m.hops.reverse {
		hop := &m.hops.reverse[i]
		if hop.seedInfo.name != w.ck.entity {
			continue
		}
		if hop.windowed && !w.windowMoved || !hop.windowed && !w.changed {
			continue
		}
		row, ok, err := m.seedRowOf(w)
		if err != nil {
			return err
		}
		holder := frontierRow{entity: w.ck.entity, key: w.ck.key}
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
		rc, err := e.rowChange(row)
		if err != nil {
			return err
		}
		patch.Changes = append(patch.Changes, rc)
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
