package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// rowClass is the visibility verdict for one stored row and one client.
type rowClass int

const (
	// rowVisible rows are sent as-is (a value, or a real tombstone in pull).
	rowVisible rowClass = iota
	// rowHidden rows are read-filtered, outside the subscribed predicate, or
	// quarantined. Pull emits them as tombstones so clients that saw the row
	// earlier drop it (the revocation fix); bootstrap just skips them. Such a
	// tombstone reveals entity + key existence and that the row just changed,
	// so pull suppresses it when the row's CreatedVersion proves the client
	// never held it — for everything older it stays conservative, because the
	// server keeps no history to prove non-possession against.
	rowHidden
	// rowHiddenNonMember rows are hidden SOLELY because they are outside a
	// bounded membership set — a purely windowed or keyed scope, or a
	// follow-only entity's closure. Split from rowHidden because possession is
	// provable here: the cursor stores the window and closure sets and a key
	// set is part of sub_hash, so pull tombstones a non-member only if the
	// cursor's prior set held it, and skips the rest instead of disclosing
	// keys the client may not even be allowed to read. Everything except pull
	// treats it exactly as rowHidden.
	rowHiddenNonMember
	// rowUnsubscribed rows belong to an entity outside the (non-empty)
	// subscription set, or to no registered entity at all; either way nothing
	// is emitted for them.
	rowUnsubscribed
)

// entityScope is the union of every subscription naming one entity: a row
// is in scope if any subscription's predicate matches (no predicate =
// whole entity), if a key set names it, or if a windowed subscription's
// membership holds it.
type entityScope struct {
	whole bool
	preds []scopePred
	// keys is the union of every Subscription.key_set on this entity. A named
	// row is in scope without a decode, but unlike a window or closure member
	// it is NOT read-filtered in advance: the set is whatever the client sent,
	// so classify still applies the filter to it. nil when no subscription
	// names keys; an empty set (a present-but-empty key_set) is non-nil and
	// admits nothing.
	keys map[string]bool
	// windowed is set by at least one Subscription.window on this entity. Such
	// a subscription contributes NO predicate here — its predicate selects
	// candidates for the window, and admitting every candidate would defeat the
	// window. Membership comes from rowEmitter.windowMembers instead.
	windowed bool
}

// scopePred is one subscription's predicate, kept in both the forms the server
// reads it in: the compiled form every row is evaluated against, and the
// condition it compiled from, which candidate narrowing inspects to recognise
// an equality pinning the entity's key fields (see keyRowCandidate).
type scopePred struct {
	compiled *query.Compiled
	cond     *syncplanev1.Condition
}

func (sc *entityScope) matches(msg protoreflect.Message) bool {
	if sc.whole {
		return true
	}
	for _, p := range sc.preds {
		if p.compiled.Eval(msg) {
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
	// ReadFilter; delta resync overrides it with the cursor's old epoch, and
	// filterEpochShifted records that it did, so a resync knows the two sides
	// may disagree on a row even where their subscriptions agree.
	filterSource       func(entity string) (RowFilter, error)
	filterEpochShifted bool
	// caller is the caller's identity in the access model, nil for a schema
	// that does not use it; modelInUse records that the catalog had a model
	// when the emitter was built, so a path that forgot to set caller fails
	// instead of reading without the model. access is what the caller holds,
	// bound inside the read snapshot by bindAccess, and chains walks parent
	// chains over the same rows. combined memoizes, per entity, the model's
	// filter joined with the application's, and is cleared whenever access is
	// bound again. prior is the set the next bindAccess binds when it is held
	// at the position that snapshot reads (see accessIn): the workspace gate's
	// set, then the set bound last.
	caller     *accessCaller
	modelInUse bool
	access     *accessSet
	prior      *accessSet
	chains     *accessChains
	combined   map[string]readFilter
	// readAccess and readAccessHash render access as a cursor records it,
	// once per binding (see boundReadAccess). hiddenEntities are the entities
	// the old side of a resync reads nothing of (see hideOnOldSide).
	readAccess     *syncplanecursorv1.ReadAccess
	readAccessHash uint64
	// callerAccess and callerAccessHash render access for the client, once
	// per binding (see boundCallerAccess).
	callerAccess     *syncplanev1.CallerAccess
	callerAccessHash uint64
	hiddenEntities   []string
	// accessOnly marks the old side of a resync whose scope and filter epoch
	// match the new side's, so only the caller's access differs between them.
	accessOnly bool
	// movesThrough is the sequence through which the rows this emitter
	// classifies reflect every access move: the newest move the read has
	// accounted for. A cursor issued from the emitter records it.
	movesThrough uint64
	// movedBelow holds, on the old side of a resync, the entities whose rows
	// can sit below a row whose move changed what the caller can read, and
	// movedRows those rows. A resync cannot skip such an entity even when its
	// scope and anchors did not change.
	movedBelow map[string]bool
	movedRows  []anchorRow
	// subs is the subscription set this emitter was built from, as given; nil
	// for full sync. A resync compares the two sides' sets per entity to skip
	// the entities whose scope did not change.
	subs []*syncplanev1.Subscription
	// followEntities is the static set of entities any subscription's follow
	// spec can reach (intermediate or terminal). They count as subscribed so
	// the scan covers them and the resync diff can carry them, even when they
	// have no base scope of their own. Empty when nothing follows.
	followEntities map[string]bool
	// closure is the reference-following closure for THIS request. The normal
	// pull/bootstrap path resolves it under the request's scope; a resync diff
	// gives both of its emitters the set the cursor carries instead, so a row
	// the client holds through follow is visible on both sides and is never
	// tombstoned for leaving base scope. A member is visible regardless of base
	// scope; membership was already read-filtered at build time.
	closure closureRows
	// windows are the compiled Subscription.window specs, one per windowed
	// subscription; empty when nothing is windowed.
	windows []*compiledWindow
	// windowMembers is the union of every window's membership for THIS
	// request. Unlike the closure it is resolved under the request's scope on
	// the resync path too: a resync classifies rows under both the old and the
	// new scope, and a window is part of the scope it has to resolve.
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
		modelInUse:  srv.catalogPtr().access != nil,
		combined:    map[string]readFilter{},
	}
	e.filterSource = func(entity string) (RowFilter, error) {
		return srv.perms.ReadFilter(ctx, principalOf(ctx), ws, entity)
	}
	if len(subs) == 0 {
		return e, nil
	}
	e.subs = subs
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
		if sub.GetKeySet() != nil {
			// Key set, predicate and window each pick the rows of one
			// subscription by a different rule; letting two apply at once would
			// leave whether they intersect or union up to the reader.
			if sub.GetPredicate() != nil || sub.GetWindow() != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument,
					fmt.Errorf("subscription for %q: key_set cannot combine with predicate or window", info.name))
			}
			keys := sub.GetKeySet().GetKeys()
			if len(keys) > srv.limits.MaxSubscriptionKeys {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
					fmt.Sprintf("subscription for %q: key_set of %d keys", info.name, len(keys)),
					"MaxSubscriptionKeys", srv.limits.MaxSubscriptionKeys)))
			}
			if sc.keys == nil {
				sc.keys = make(map[string]bool, len(keys))
			}
			for _, key := range keys {
				if err := naming.ValidateRowKey(key); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument,
						fmt.Errorf("subscription for %q: key_set: %w", info.name, err))
				}
				sc.keys[key] = true
			}
			continue
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
		sc.preds = append(sc.preds, scopePred{compiled: compiled, cond: sub.GetPredicate()})
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

// syncable reports whether a client can hold rows of entity at all: the
// entity is registered and is not server-only.
func (e *rowEmitter) syncable(entity string) bool {
	info, ok := e.srv.catalogPtr().entities[entity]
	return ok && !info.serverOnly
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

// warmFilters resolves the application's read filter of every entity in
// scope, so classify inside a read snapshot only applies predicates and never
// calls a hook.
func (e *rowEmitter) warmFilters() error {
	for _, entity := range e.entities() {
		if _, err := e.appFilterOf(entity); err != nil {
			return err
		}
	}
	return nil
}

// readFilter decides whether the caller may read one row. It fails only when
// a read it needs fails, and the read path then fails the request: a row the
// filter could not decide is neither shown nor tombstoned. A nil readFilter
// admits every row.
type readFilter func(key string, row proto.Message) (bool, error)

// admits applies f to one row.
func (f readFilter) admits(key string, row proto.Message) (bool, error) {
	if f == nil {
		return true, nil
	}
	return f(key, row)
}

// filterOf returns the read filter for rows of entity: the access model's
// filter for this caller joined with the application's ReadFilter, or nil when
// neither hides a row. In a schema that uses the access model it fails until
// bindAccess has run.
func (e *rowEmitter) filterOf(entity string) (readFilter, error) {
	if e.caller == nil {
		if e.modelInUse {
			return nil, errAccessUnbound
		}
		app, err := e.appFilterOf(entity)
		return appReadFilter(app), err
	}
	if f, ok := e.combined[entity]; ok {
		return f, nil
	}
	if e.access == nil {
		return nil, errAccessUnbound
	}
	app, err := e.appFilterOf(entity)
	if err != nil {
		return nil, err
	}
	var model readFilter
	if info, ok := e.srv.catalogPtr().entities[entity]; ok {
		model = e.modelReadFilter(info)
	}
	f := andFilters(model, appReadFilter(app))
	e.combined[entity] = f
	return f, nil
}

// bindAccess binds what the caller holds as of the read transaction tx. A read
// path calls it at the start of every read transaction whose rows it
// classifies, so the grants, the parent chains it walks and the rows it
// filters come from one snapshot. The grants are those of an earlier
// transaction of the request when it read the workspace at the position tx
// reads, which holds the same rows, and are read from tx otherwise (see
// accessIn). It is a no-op for a schema that does not use the access model.
func (e *rowEmitter) bindAccess(tx storage.ReadTx) error {
	if e.caller == nil {
		return nil
	}
	set, err := accessIn(tx, e.caller, e.srv.limits, e.prior)
	if err != nil {
		return err
	}
	e.prior = set
	e.useAccess(set, e.chainsOver(tx.GetRow))
	return nil
}

// admit gives the emitter the caller the workspace gate resolved, and the set
// the gate read for bindAccess to use where it holds.
func (e *rowEmitter) admit(adm admission) {
	e.caller, e.prior = adm.caller, adm.access
}

// chainsOver returns a parent-chain walker over the rows get reads, for the
// caller's model.
func (e *rowEmitter) chainsOver(get func(entity, key string) (storage.Row, error)) *accessChains {
	return newAccessChainsFrom(e.caller.model, get, e.srv.limits.maxAccessDepth(), func(row storage.Row, err error) {
		e.srv.reportPoison(e.ctx, e.ws, row, err)
	})
}

// readChainsFrom makes the bound parent-chain walker read from tx. A read path
// whose read transactions are shorter than the binding, such as a bootstrap
// that reads each page in a transaction of its own, calls it in each of them.
// The rows the walker has read already stay memoized.
func (e *rowEmitter) readChainsFrom(tx storage.ReadTx) {
	if e.chains != nil {
		e.chains.get = tx.GetRow
	}
}

// useAccess binds an access set resolved elsewhere, such as the one the
// workspace gate read, and the walker that reads parent chains for it.
func (e *rowEmitter) useAccess(set *accessSet, chains *accessChains) {
	e.access = set
	e.chains = chains
	e.readAccess, e.readAccessHash = nil, 0
	e.callerAccess, e.callerAccessHash = nil, 0
	clear(e.combined)
}

// appFilterOf resolves and memoizes the application's ReadFilter for entity.
// It calls the Permissions hook, so a read path warms it for every entity it
// may read before its read snapshot opens.
func (e *rowEmitter) appFilterOf(entity string) (RowFilter, error) {
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
//	follow-closure or window member      → in scope (skips the base-scope
//	                                       checks below, not the read filter)
//	outside a purely windowed or keyed
//	scope, or a follow-only entity's
//	closure                              → hidden-non-member (pull: tombstone
//	                                       only prior members; bootstrap: skip)
//	row will not decode                  → hidden (quarantined, see poison.go)
//	ReadFilter denies                    → hidden (pull: tombstone; bootstrap: skip)
//	key-set member                       → visible (no predicate to evaluate)
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
	// A closure member is in scope regardless of base scope. This is the OR
	// half of "visible if base-scope matches OR follow-reachable". A window
	// member is in scope the same way. The closure is ADDITIVE to base scope
	// while a window RESTRICTS it, which is why they stay separate sets instead
	// of being folded into entityScope.
	//
	// Membership skips the base-scope checks but not the read filter. A build
	// applies the filter to every member it adds, but the sets classified with
	// here can come off the client's cursor — a resync classifies with the
	// closure the cursor carries — and cursor bytes are the client's to edit.
	member := e.closure.has(row.Entity, row.Key) || e.windowMembers.has(row.Entity, row.Key)
	var scope *entityScope
	if e.scopes != nil && !member {
		scope = e.scopes[row.Entity]
		if scope == nil {
			// An active subscription set that names this entity only via a follow
			// spec (no base scope of its own): a row outside the current closure
			// is hidden, so pull tombstones one that has left the closure. Full
			// sync (e.scopes == nil) never reaches here — everything is visible.
			return rowHiddenNonMember, nil
		}
	}
	// A named key is in scope whatever the row holds, so no predicate applies
	// to it — but the read filter still does, which is why it is not folded
	// into the membership checks above.
	keyed := scope != nil && scope.keys[row.Key]
	if scope != nil && !scope.whole && !keyed && len(scope.preds) == 0 {
		// A purely windowed or keyed scope: the membership checks above were
		// the whole test, so a non-member is hidden without paying for a
		// decode. Non-member rather than hidden because possession is
		// provable: the window sets ride on the cursor, and a key set is part
		// of sub_hash, so under a matching hash no row outside it was ever
		// delivered through this scope.
		return rowHiddenNonMember, nil
	}
	needPred := scope != nil && !scope.whole && !keyed
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
	if ok, err := filter.admits(row.Key, msg); err != nil {
		return 0, err
	} else if !ok {
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
		for _, ti := range tf.instances(msg.ProtoReflect()) {
			cur := ti.bytes()
			if len(cur) == 0 {
				continue // nothing to save, and nothing the recipient must keep
			}
			// The log — and so FieldDelta.field — is keyed by instance path:
			// each element of a keyed list or map is its own document with its
			// own span of entries.
			frags, complete, err := tx.FieldDeltas(row.Entity, row.Key, ti.path, e.deltaAfter)
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
			// An empty fold means the document did not move over the span — the
			// recipient keeps what it holds, and the document rides nowhere. That
			// is the case a row whose OTHER fields changed hits, and it is the one
			// that saves the most.
			if len(folded) > 0 && float64(len(folded)) >= float64(len(cur))*fragmentSizeRatio {
				continue
			}
			deltas = append(deltas, &syncplanev1.FieldDelta{Field: ti.path, Fragment: folded})
			ti.clear()
		}
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
// A predicate, a window, a follow closure or the application's ReadFilter can
// each have hidden a row at the cursor that is visible now, and the forward
// scan emits such a row as an ordinary change without knowing it is the
// recipient's first sight of it, so an entity under any of them does not
// qualify. The access model's filter does not disqualify one. A change of the
// caller's grants since the cursor, or a move above a row that changes what the
// caller reads, turns the pull into a resync, whose catch-up ships whole rows
// (see compareAccess). A write that changes what the row's own fields decide
// leaves a gap in the text delta log, so a span that covers it ships the
// documents whole (see recordReadGaps).
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
	if e.modelInUse && e.access == nil {
		return false, errAccessUnbound
	}
	app, err := e.appFilterOf(entity)
	if err != nil {
		return false, err
	}
	if app != nil {
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
//
// It discloses the row's entity, key and version to a client that may not
// read the row, so every caller owes proof — or at least the absence of
// disproof — that the client could have held it: the pull scan checks
// CreatedVersion against the cursor and prior membership sets, the resync
// diff evaluates the old visibility rules, and the membership diff only
// visits keys the cursor carried.
func hiddenChange(row storage.Row) *syncplanev1.RowChange {
	return &syncplanev1.RowChange{
		Entity:  row.Entity,
		Key:     row.Key,
		Version: row.Version,
		Body:    &syncplanev1.RowChange_Tombstone{Tombstone: true},
	}
}
