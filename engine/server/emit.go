package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// rowClass is the visibility verdict for one stored row and one client.
type rowClass int

const (
	// rowVisible rows are sent as-is (a value, or a real tombstone in pull).
	rowVisible rowClass = iota
	// rowHidden rows are read-filtered or outside the subscribed predicate.
	// Pull emits them as tombstones so clients that saw the row earlier drop
	// it (the revocation fix); bootstrap just skips them. Tombstones reveal
	// entity + key existence — keys are not secrets.
	rowHidden
	// rowUnsubscribed rows belong to an entity outside the (non-empty)
	// subscription set, or to no registered entity at all; either way nothing
	// is emitted for them.
	rowUnsubscribed
)

// entityScope is the union of every subscription naming one entity: a row
// is in scope if any subscription's predicate matches (no predicate =
// whole entity), or if a windowed subscription's membership holds it.
type entityScope struct {
	whole bool
	preds []*query.Compiled
	// windowed is set by at least one Subscription.window on this entity. Such
	// a subscription contributes NO predicate here — its predicate selects
	// candidates for the window, and admitting every candidate would defeat the
	// window. Membership comes from rowEmitter.windowMembers instead.
	windowed bool
}

func (sc *entityScope) matches(msg protoreflect.Message) bool {
	if sc.whole {
		return true
	}
	for _, p := range sc.preds {
		if p.Eval(msg) {
			return true
		}
	}
	return false
}

// rowEmitter decides row visibility for one request, combining read filters
// (resolved lazily per entity) with subscription predicates and decoding
// each row at most once. Shared by Pull, Bootstrap and Query. Entities with
// no filter and no predicate keep a zero-decode fast path.
type rowEmitter struct {
	srv *Server
	ctx context.Context
	ws  string
	// scopes is nil for an empty subscription list (full sync).
	scopes      map[string]*entityScope
	filters     map[string]RowFilter
	filterKnown map[string]bool
	// filterSource resolves an entity's read filter. Defaults to the current
	// ReadFilter; delta resync overrides it with the cursor's old epoch.
	filterSource func(entity string) (RowFilter, error)
	// followEntities is the static set of entities any subscription's follow
	// spec can reach (intermediate or terminal). They count as subscribed so
	// the scan covers them and the resync diff can carry them, even when they
	// have no base scope of their own. Empty when nothing follows.
	followEntities map[string]bool
	// closure is the reference-following closure for THIS request, populated
	// only on the normal pull/bootstrap path (never during a resync diff, where
	// it must stay empty so followed rows are not spuriously tombstoned). A
	// member is visible regardless of base scope; membership was already read-
	// filtered at build time.
	closure closureRows
	// windows are the compiled Subscription.window specs, one per windowed
	// subscription; empty when nothing is windowed.
	windows []*compiledWindow
	// windowMembers is the union of every window's membership for THIS
	// request. Unlike the closure it is populated on the resync path too: a
	// resync classifies rows under both the old and the new scope, and a
	// window is part of the scope it has to resolve.
	windowMembers closureRows
	// includeServerOnly lets rows of a server_only entity through classify. It
	// is the difference between the two kinds of consumer this type serves:
	// SYNC paths (pull, bootstrap, resync, subscribe) ship rows to a client and
	// must never carry one, so they take the zero value; READ paths (the Query
	// RPC, the trusted read gate) answer a caller who asked for the entity by
	// name and must reach it, so they set this.
	//
	// Excluding by default is deliberate. A read path that forgets to set it
	// answers "no rows" for an entity nobody could otherwise see; a sync path
	// that forgot to clear it would ship a client rows the whole option exists
	// to withhold.
	includeServerOnly bool
	// deltas enables collaborative-text delta state on emitted rows, cut
	// against deltaAfter (the cursor's sequence). Only the forward pull scan
	// sets it: every other path (bootstrap, delta resync, membership diffs,
	// query) emits to a recipient whose prior state is not the workspace as of
	// deltaAfter, which is the assumption delta state rests on.
	deltas     bool
	deltaAfter uint64
	// noDeltaEntities indexes the entities the request's sender said it cannot
	// decode; nil when it named none. Set by the sync entry points from
	// PullRequest/SubscribeRequest.no_delta_entities.
	noDeltaEntities map[string]bool
	// deltaSafeBy memoizes the per-entity verdict of deltaSafe.
	deltaSafeBy map[string]bool
	// historySources memoizes, per source row, whether the caller may read the
	// field-history records describing it (see historySourceReadable). Only the
	// history query populates it.
	historySources map[historySourceKey]bool
}

// newRowEmitter validates the subscription set (unknown entities and
// uncompilable predicates are InvalidArgument).
func newRowEmitter(ctx context.Context, srv *Server, ws string, subs []*syncplanev1.Subscription) (*rowEmitter, error) {
	e := &rowEmitter{
		srv:         srv,
		ctx:         ctx,
		ws:          ws,
		filters:     map[string]RowFilter{},
		filterKnown: map[string]bool{},
	}
	e.filterSource = func(entity string) (RowFilter, error) {
		return srv.perms.ReadFilter(ctx, principalOf(ctx), ws, entity)
	}
	if len(subs) == 0 {
		return e, nil
	}
	e.scopes = make(map[string]*entityScope, len(subs))
	for i, sub := range subs {
		info, ok := srv.catalogPtr().entities[sub.GetEntity()]
		if !ok {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("subscription names unknown entity %q", sub.GetEntity()))
		}
		// A server-only entity would sync nothing whatever this subscription
		// says, and silently syncing nothing is the failure a misconfiguration
		// takes longest to notice. Refusing names it at the request that asked.
		if info.serverOnly {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("subscription names server-only entity %q, which never syncs to clients", info.name))
		}
		sc := e.scopes[info.name]
		if sc == nil {
			sc = &entityScope{}
			e.scopes[info.name] = sc
		}
		if sub.GetWindow() != nil {
			cw, err := compileWindow(info, sub, i, srv.limits)
			if err != nil {
				return nil, err
			}
			e.windows = append(e.windows, cw)
			sc.windowed = true
			continue
		}
		if sub.GetPredicate() == nil {
			sc.whole = true
			continue
		}
		compiled, err := query.Compile(info.msgType.Descriptor(), sub.GetPredicate())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("subscription for %q: %w", info.name, err))
		}
		sc.preds = append(sc.preds, compiled)
	}
	// Resolve the static follow-reachable entity set so they count as
	// subscribed even without a base scope of their own.
	for _, sub := range subs {
		if sub.GetFollow() != nil && len(sub.GetFollow().GetPaths()) > 0 {
			g, err := srv.catalogPtr().refGraphLazy()
			if err != nil {
				return nil, connect.NewError(connect.CodeFailedPrecondition, err)
			}
			e.followEntities, err = g.followEntitiesOf(subs, srv.limits.MaxFollowDepth)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	return e, nil
}

func (e *rowEmitter) subscribed(entity string) bool {
	if e.scopes == nil {
		return true // full sync
	}
	return e.scopes[entity] != nil || e.followEntities[entity]
}

// entities returns the subscribed registered entity names in stable order.
// Follow-reachable entities are included so the scan and resync diff cover
// them. The order is SORTED, which resync depends on to resume a paused diff
// (it skips every entity below its saved position); bootstrap wants a
// different order and asks bootstrapEntities for it instead.
//
// Server-only entities are dropped from BOTH branches. The subscribed branch
// could not name one (newRowEmitter refuses it), but a follow spec can reach
// one through a reference and full sync names every entity there is — and full
// sync is the case a subscription set cannot protect against, since one tab
// that never narrows its subscriptions pulls the whole catalog into a store its
// siblings share.
func (e *rowEmitter) entities() []string {
	names := e.srv.catalogPtr().syncEntityNames()
	if e.scopes == nil {
		return names
	}
	out := make([]string, 0, len(e.scopes)+len(e.followEntities))
	for _, name := range names {
		if e.scopes[name] != nil || e.followEntities[name] {
			out = append(out, name)
		}
	}
	return out
}

// bootstrapEntities returns the same set as entities(), ordered by what the
// client wants to SEE first rather than by name.
//
// A snapshot ships one entity at a time and a cold client reads each one as it
// lands (see the client's doBootstrap), so this order decides what a user looks
// at while the rest of the workspace downloads — and, since an entity that has
// not started is indistinguishable from an empty one, which parts of the UI can
// stop saying "still loading" early. Three tiers, each keeping its own order and
// dropping anything an earlier tier already took:
//
//  1. priority_entities, in request order — the client's explicit "this first".
//  2. the subscription set, in request order — a client that lists its
//     subscriptions in screen order gets that order for free.
//  3. everything else, sorted — full sync's whole catalog, and the entities
//     reachable only by following references, which no request names.
//
// Names outside the scope (unregistered, or unsubscribed) are ignored rather
// than rejected: a request naming what it cannot receive is over-specified, not
// wrong, and an old client's stale entity name must not fail its bootstrap.
func (e *rowEmitter) bootstrapEntities(subs []*syncplanev1.Subscription, priority []string) []string {
	inScope := e.entities()
	remaining := make(map[string]bool, len(inScope))
	for _, name := range inScope {
		remaining[name] = true
	}
	out := make([]string, 0, len(inScope))
	take := func(name string) {
		if remaining[name] {
			delete(remaining, name)
			out = append(out, name)
		}
	}
	for _, name := range priority {
		take(name)
	}
	for _, sub := range subs {
		take(sub.GetEntity())
	}
	for _, name := range inScope {
		take(name)
	}
	return out
}

func (e *rowEmitter) filterOf(entity string) (RowFilter, error) {
	if !e.filterKnown[entity] {
		filter, err := e.filterSource(entity)
		if err != nil {
			return nil, err
		}
		e.filters[entity] = filter
		e.filterKnown[entity] = true
	}
	return e.filters[entity], nil
}

// classify is the decision table:
//
//	entity not registered                → unsubscribed (skip)
//	entity is server-only                → unsubscribed (skip)
//	real tombstone                       → visible (pull passes it through)
//	entity unsubscribed (subs non-empty) → unsubscribed (skip)
//	follow-closure or window member      → visible (already read-filtered)
//	row will not decode                  → hidden (quarantined, see poison.go)
//	ReadFilter denies                    → hidden (pull: tombstone; bootstrap: skip)
//	predicate non-match                  → hidden (pull: tombstone; bootstrap: skip)
//	allowed + match                      → visible
func (e *rowEmitter) classify(row storage.Row) (rowClass, error) {
	// An entity the catalog does not hold has no wire representation at all:
	// nothing here can decode it, filter it or say what its fields mean. Stored
	// rows outlive their registration whenever a build stops declaring an entity,
	// and every other path already ignores them — bootstrap iterates registered
	// entities, resync scans them — so skipping is what makes the forward pull
	// scan agree instead of shipping rows the other two withhold.
	//
	// Skipped rather than tombstoned: the absence of a SCHEMA is not the absence
	// of DATA. Registration is additive, so an entity leaves the catalog by way of
	// a deploy that no longer declares it; tombstoning would delete every client's
	// copy of rows that still exist here, and since those rows sit below every
	// cursor, re-declaring the entity could never deliver them again.
	info, registered := e.srv.catalogPtr().entities[row.Entity]
	if !registered {
		return rowUnsubscribed, nil
	}
	// A server-only entity takes the same skip-don't-tombstone path, for the
	// same reason: no client ever held these rows, so there is nothing to
	// tombstone, and emitting one would only tell a client to delete something
	// it never had. This is what makes the forward pull scan agree with
	// bootstrap and resync, which never scan the entity at all (see entities()).
	// A READ path names the entity outright and is let through (see
	// includeServerOnly) — server-only is "does not sync", not "cannot be read".
	if info.serverOnly && !e.includeServerOnly {
		return rowUnsubscribed, nil
	}
	if !e.subscribed(row.Entity) {
		return rowUnsubscribed, nil
	}
	if row.Deleted {
		// Tombstones pass through unfiltered: they carry only the key.
		return rowVisible, nil
	}
	// A closure member is visible regardless of base scope — its read filter
	// was applied when the closure was built. This is the OR half of
	// "visible if base-scope matches OR follow-reachable".
	if e.closure.has(row.Entity, row.Key) {
		return rowVisible, nil
	}
	// Same shape for a window member, and for the same reason: membership was
	// decided (and read-filtered) at build time. The closure is ADDITIVE to
	// base scope while a window RESTRICTS it, which is why they stay separate
	// sets instead of being folded into entityScope.
	if e.windowMembers.has(row.Entity, row.Key) {
		return rowVisible, nil
	}
	var scope *entityScope
	if e.scopes != nil {
		scope = e.scopes[row.Entity]
		if scope == nil {
			// An active subscription set that names this entity only via a follow
			// spec (no base scope of its own): a row outside the current closure
			// is hidden, so pull tombstones one that has left the closure. Full
			// sync (e.scopes == nil) never reaches here — everything is visible.
			return rowHidden, nil
		}
	}
	if scope != nil && !scope.whole && len(scope.preds) == 0 {
		// A purely windowed scope: the window check above was the whole test,
		// so a non-member is hidden without paying for a decode.
		return rowHidden, nil
	}
	needPred := scope != nil && !scope.whole
	filter, err := e.filterOf(row.Entity)
	if err != nil {
		return 0, err
	}
	if filter == nil && !needPred {
		// Zero-decode fast path.
		return rowVisible, nil
	}
	msg, ok := e.decodeRow(info, row)
	if !ok {
		// Quarantined: nothing that cannot be decoded can be shown to satisfy a
		// filter or a predicate, so it is hidden rather than failing the request
		// for every other row in the workspace (see poison.go).
		return rowHidden, nil
	}
	if filter != nil && !filter(row.Key, msg) {
		return rowHidden, nil
	}
	if needPred && !scope.matches(msg.ProtoReflect()) {
		return rowHidden, nil
	}
	return rowVisible, nil
}

func rowChange(row storage.Row) *syncplanev1.RowChange {
	rc := &syncplanev1.RowChange{
		Entity:  row.Entity,
		Key:     row.Key,
		Version: row.Version,
	}
	if row.Deleted {
		rc.Body = &syncplanev1.RowChange_Tombstone{Tombstone: true}
	} else {
		rc.Body = &syncplanev1.RowChange_Value{Value: row.Value}
	}
	return rc
}

// emitChange builds the RowChange for a visible row, shipping collaborative
// text as delta state where that is both safe and smaller. It falls back to the
// full snapshot silently and per field: every reason to decline is a size
// decision, never a correctness one.
func (e *rowEmitter) emitChange(tx storage.ReadTx, row storage.Row) (*syncplanev1.RowChange, error) {
	rc := rowChange(row)
	// A row at or below the cursor is one the recipient may never have held (a
	// page resuming inside a same-version group delivers the rest of that
	// group), so its state cannot be assumed.
	if !e.deltas || row.Deleted || row.Version <= e.deltaAfter {
		return rc, nil
	}
	info, ok := e.srv.catalogPtr().entities[row.Entity]
	if !ok || len(info.textFields) == 0 {
		return rc, nil
	}
	safe, err := e.deltaSafe(row.Entity)
	if err != nil || !safe {
		return rc, err
	}
	// A row that will not decode is not this path's problem to report: full
	// state has always shipped such a row verbatim (only classify decodes, and
	// only when a filter or predicate needs it), and turning a pull into an
	// error here would be a new failure mode introduced by an optimization.
	msg, err := info.decode(row.Value)
	if err != nil {
		return rc, nil
	}
	var deltas []*syncplanev1.FieldDelta
	for _, tf := range info.textFields {
		cur := chainGetBytes(msg.ProtoReflect(), tf.chain)
		if len(cur) == 0 {
			continue // nothing to save, and nothing the recipient must keep
		}
		frags, complete, err := tx.FieldDeltas(row.Entity, row.Key, tf.path, e.deltaAfter)
		if err != nil {
			return nil, err
		}
		if !complete {
			continue // the log does not cover the span; ship the document
		}
		folded, ok := foldFragments(frags)
		if !ok {
			continue
		}
		// An empty fold means the field did not move over the span — the
		// recipient keeps what it holds, and the document rides nowhere. That is
		// the case a row whose OTHER fields changed hits, and it is the one that
		// saves the most.
		if len(folded) > 0 && float64(len(folded)) >= float64(len(cur))*fragmentSizeRatio {
			continue
		}
		deltas = append(deltas, &syncplanev1.FieldDelta{Field: tf.path, Fragment: folded})
		chainClearBytes(msg.ProtoReflect(), tf.chain)
	}
	if len(deltas) == 0 {
		return rc, nil
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		return rc, nil // same reasoning: fall back rather than fail the pull
	}
	rc.Body = &syncplanev1.RowChange_Value{Value: value}
	rc.TextDeltas = deltas
	return rc, nil
}

// foldFragments unions a span's fragments into one, in log order. ok is false
// when they cannot be folded — corrupt log entries, which are no reason to fail
// the pull when full state says the same thing.
func foldFragments(frags [][]byte) ([]byte, bool) {
	if len(frags) == 0 {
		return nil, true
	}
	folded := frags[0]
	for _, f := range frags[1:] {
		merged, err := text.MergeFragments(folded, f)
		if err != nil {
			return nil, false
		}
		folded = merged
	}
	return folded, true
}

// noDeltaEntitySet indexes a request's no_delta_entities by name, or returns
// nil for the ordinary case of a client that named none. The result is read
// only, so one stream's scopes can share it.
func noDeltaEntitySet(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}

// deltaSafe reports whether a recipient at deltaAfter is guaranteed to hold
// this entity's rows as of that sequence — the premise delta state depends on,
// since a fragment is meaningless against state the recipient never had.
//
// Only an unconditionally visible entity qualifies. A read filter, a predicate,
// a window or a follow closure can each have hidden a row at the cursor that is
// visible now, and the forward scan emits such a row as an ordinary change
// without knowing it is the recipient's first sight of it.
//
// Holding the row is necessary and not sufficient: the recipient must also be
// able to decode it, which only the recipient knows. That half arrives as
// no_delta_entities on the request.
func (e *rowEmitter) deltaSafe(entity string) (bool, error) {
	if verdict, known := e.deltaSafeBy[entity]; known {
		return verdict, nil
	}
	verdict, err := e.computeDeltaSafe(entity)
	if err != nil {
		return false, err
	}
	if e.deltaSafeBy == nil {
		e.deltaSafeBy = map[string]bool{}
	}
	e.deltaSafeBy[entity] = verdict
	return verdict, nil
}

func (e *rowEmitter) computeDeltaSafe(entity string) (bool, error) {
	if e.noDeltaEntities[entity] {
		return false, nil
	}
	if len(e.windows) > 0 || e.followEntities != nil {
		return false, nil
	}
	filter, err := e.filterOf(entity)
	if err != nil {
		return false, err
	}
	if filter != nil {
		return false, nil
	}
	if e.scopes == nil {
		return true, nil // full sync: every row of every entity syncs
	}
	sc := e.scopes[entity]
	return sc != nil && sc.whole && !sc.windowed, nil
}

// hiddenChange turns a live-but-hidden row into a tombstone at the row's
// version, so clients that synced it before it went out of scope delete it.
func hiddenChange(row storage.Row) *syncplanev1.RowChange {
	return &syncplanev1.RowChange{
		Entity:  row.Entity,
		Key:     row.Key,
		Version: row.Version,
		Body:    &syncplanev1.RowChange_Tombstone{Tombstone: true},
	}
}
