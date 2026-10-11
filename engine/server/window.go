// Windowed subscriptions: a subscription can bound its synced set by RANK
// rather than only by attribute — "the 1000 most recently updated open
// issues" instead of "every open issue". Without it the only lever an app has
// over working-set size is a more selective predicate, which for a real view
// (a busy project, "all open", an active label) often is not semantically
// possible.
//
// A window is the same KIND of object a follow closure is: a bounded
// membership set that cannot be decided per row, is resolved against the rows
// of a snapshot, is diffed against an exact key-set carried on the cursor, and
// emits entering rows as values and leaving rows as tombstones-unless-
// otherwise-visible. So it reuses that machinery (closureRows,
// emitMembershipDiff) rather than growing a parallel one; the only difference
// is the membership rule — `rank <= limit` instead of `reachable by a ref
// path`.
//
// Two orderings:
//   - order_by: the entity's own fields, via the same query.Order that backs
//     the Query RPC and ViewTx.Query, so a window means what the equivalent
//     query means.
//   - by_recency: the row's workspace sequence, descending. It sorts without
//     decoding anything, which makes it the cheap window to serve.
//
// What a window bounds first is the CLIENT: what it holds, bootstraps and
// materializes. On the server it is bounded by whatever serves the ordering.
// Resolving an order_by window from scratch is exactly its Query — a range
// read off a serving index, or a scan of the entity into a top-K heap without
// one — and an unfiltered recency window reads `limit` rows and stops, because
// the store can produce one entity's rows newest-first.
//
// Resolving from scratch is the FALLBACK, not the steady state. A pull that
// knows every row written since the cursor maintains the membership from those
// rows instead (see maintain). The cursor carries the members and the position
// of the weakest of them, which turns a changed row's membership into one
// comparison; only a row entering or leaving a full window costs a read, and
// then a bounded ordered walk either side of that position rather than the
// whole window. Both paths answer the same question and must produce the same
// set — the rebuild is what the maintained set is tested against.
package server

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// compiledWindow is one Subscription.window resolved against its entity.
//
// The predicate lives here rather than in the entity scope on purpose: a
// windowed subscription's predicate describes CANDIDATES for the window, not
// visible rows. Letting it join entityScope.preds would make every
// out-of-window match visible and defeat the window entirely.
type compiledWindow struct {
	info *entityInfo
	// subIndex is this window's position in the request's subscription list. It
	// is how a cursor's WindowState finds the window it describes, and it is
	// only meaningful against a subscription list that lines up with the one the
	// cursor was issued under (see sameSubscriptions).
	subIndex int
	// blind is set while the bound access hides a field the window selects or
	// orders by: it holds no member, and a cursor carries no state for it, so
	// it is resolved from scratch once the caller reads the field again (see
	// rowEmitter.blindSubs).
	blind bool
	// pred is always non-nil (query.Compile of a nil condition matches
	// everything); hasPred says whether evaluating it can change the outcome,
	// which is what keeps a predicate-less recency window from decoding rows.
	pred      *query.Compiled
	hasPred   bool
	order     *query.Order
	byRecency bool
	limit     int
	// members is this window's membership for the current request, filled by
	// buildWindows or maintain; nil before that.
	members closureRows
	// ordered is the membership best-first. A rebuild always knows it; only a
	// by_recency window can maintain it, because every row written since the
	// cursor outranks every member there and so enters at the front. nil when
	// it is not known.
	ordered []string
	// boundary separates members from non-members in an order_by window's
	// ordering: every candidate at or ahead of it is a member, every candidate
	// behind it is not. Nothing depends on it being any PARTICULAR position —
	// only on it separating — which is what lets a maintained window move it
	// without re-ranking anything. nil for a by_recency window and an empty one.
	boundary []byte
}

// compileWindow validates and resolves one window spec. Alongside the
// unknown-entity and predicate-compile checks in newRowEmitter, this is where
// a malformed window becomes InvalidArgument rather than a surprise later.
func compileWindow(info *entityInfo, sub *syncplanev1.Subscription, subIndex int, limits Limits) (*compiledWindow, error) {
	w := sub.GetWindow()
	limit := int(w.GetLimit())
	if limit <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("subscription for %q: window limit must be > 0", info.name))
	}
	if limit > limits.MaxWindowSize {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("subscription for %q: window limit %d", info.name, limit),
			"MaxWindowSize", limits.MaxWindowSize)))
	}
	if w.GetByRecency() && len(w.GetOrderBy()) > 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("subscription for %q: window sets both by_recency and order_by", info.name))
	}
	desc := info.msgType.Descriptor()
	pred, err := query.Compile(desc, sub.GetPredicate())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("subscription for %q: %w", info.name, err))
	}
	cw := &compiledWindow{
		info:      info,
		subIndex:  subIndex,
		pred:      pred,
		hasPred:   sub.GetPredicate() != nil,
		limit:     limit,
		byRecency: w.GetByRecency(),
	}
	if cw.byRecency {
		return cw, nil
	}
	// An empty order_by is accepted and means row-key order, matching
	// CompileOrder's contract (every pair ties, so the key tiebreak decides).
	cw.order, err = query.CompileOrder(desc, w.GetOrderBy())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("subscription for %q: %w", info.name, err))
	}
	return cw, nil
}

// buildWindows resolves every window's membership from scratch against the
// rows in tx and returns their union — the set classify treats as visible.
// Each window also keeps its own members, which is what a follow spec on a
// windowed subscription seeds from.
//
// Must run BEFORE the pull scan (classify has to be window-aware) and before
// buildClosure (which seeds from window members).
func (e *rowEmitter) buildWindows(tx storage.ReadTx) (closureRows, error) {
	if len(e.windows) == 0 {
		return nil, nil
	}
	union := closureRows{}
	for _, w := range e.windows {
		if blind, err := w.blindOn(e); err != nil || blind {
			if err != nil {
				return nil, err
			}
			continue
		}
		rows, boundary, err := w.compute(tx, e)
		if err != nil {
			return nil, err
		}
		w.adopt(rows, boundary)
		w.mergeInto(union)
	}
	return union, nil
}

// blindOn records on w whether e's bound access hides a field the window
// selects or orders by, and empties it when it does.
func (w *compiledWindow) blindOn(e *rowEmitter) (bool, error) {
	blind, err := e.blindBase(w.subIndex)
	if err != nil {
		return false, err
	}
	w.blind = blind
	if blind {
		w.adopt(nil, nil)
	}
	return blind, nil
}

// adopt records a resolved membership, best-first, with the position that
// separates it from the rest of the ordering.
func (w *compiledWindow) adopt(rows []storage.Row, boundary []byte) {
	w.members = make(closureRows, len(rows))
	w.ordered = make([]string, 0, len(rows))
	w.boundary = boundary
	for _, row := range rows {
		w.members[closureKey{row.Entity, row.Key}] = row
		w.ordered = append(w.ordered, row.Key)
	}
}

// mergeInto folds this window's members into the union classify consults,
// preferring a member whose row was read this request over the same member
// carried over unread by another window (see closureRows).
func (w *compiledWindow) mergeInto(union closureRows) {
	for ck, row := range w.members {
		if held, ok := union[ck]; ok && held.Entity != "" {
			continue
		}
		union[ck] = row
	}
}

// compute resolves this window's membership: the `limit` predicate-matching,
// readable rows of the entity that sort first, best first, together with the
// weakest one's position.
func (w *compiledWindow) compute(tx storage.ReadTx, e *rowEmitter) ([]storage.Row, []byte, error) {
	if w.byRecency {
		rows, err := w.computeByRecency(tx, e)
		return rows, nil, err
	}
	// The order path is exactly a Query: same scan, same bounded-memory
	// max-heap, same total order — which is what keeps "the window" and "the
	// equivalent query" from drifting apart.
	rows, _, err := queryRows(tx, w.info, w.pred, w.order, pageFor(nil, nil, 0, w.limit), e.readable, e, e.srv.limits)
	if err != nil {
		return nil, nil, err
	}
	out := make([]storage.Row, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.row)
	}
	var boundary []byte
	if len(rows) > 0 {
		weakest := rows[len(rows)-1]
		boundary = w.order.Cursor(weakest.msg.ProtoReflect(), weakest.row.Key)
	}
	return out, boundary, nil
}

// computeByRecency keeps the `limit` highest-versioned matching rows. Sorting
// needs no decode — only the predicate does — so a window with no predicate
// and no read filter reads nothing but the row headers.
//
// The ordering it wants is exactly what the store can already produce, so it
// reads rows newest-first and stops rather than scanning the entity into a
// top-K heap. Two things have to hold for that to be legal, and both are
// contract, not luck:
//
//   - ScanEntityByVersionDesc breaks version ties by key ascending, the same
//     way aheadByRecency does, so the two agree about which of two rows
//     sharing a version is in the window.
//   - The early stop is only sound with nothing left to reject. A predicate or
//     a read filter can reject any prefix of the ordering, so those keep
//     reading until the window is full — still newest-first, so they stop as
//     soon as they can rather than after the whole entity.
func (w *compiledWindow) computeByRecency(tx storage.ReadTx, e *rowEmitter) ([]storage.Row, error) {
	h := &recencyTopK{cap: w.limit}
	// Ordered reads have no way to know how many rows a filter will reject, so
	// only the unfiltered case can bound what it asks for.
	limit := 0
	if !w.hasPred {
		if filter, err := e.filterOf(w.info.name); err != nil {
			return nil, err
		} else if filter == nil {
			limit = w.limit
		}
	}
	err := tx.ScanEntityByVersionDesc(w.info.name, limit, func(row storage.Row) error {
		ok, err := e.readable(row)
		if err != nil || !ok {
			return err
		}
		if w.hasPred {
			decoded, ok := e.decodeRow(w.info, row)
			if !ok {
				return nil // quarantined: no predicate can match it
			}
			if !w.pred.Eval(decoded.ProtoReflect()) {
				return nil
			}
		}
		h.offer(row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// The heap's array is in heap order, not window order. Sorting it is what
	// lets the membership be carried on the cursor best-first, which is the
	// whole state a maintained recency window needs.
	sort.Slice(h.rows, func(i, j int) bool { return aheadByRecency(h.rows[i], h.rows[j]) })
	return h.rows, nil
}

// ---- incremental maintenance ----

// refreshWindows resolves every window's membership for this page, maintaining
// each one from the rows written since the cursor where prev carries the state
// to do it and rebuilding the rest. Returns their union, exactly as
// buildWindows does.
//
// The choice is per window rather than all-or-nothing: windows are independent,
// they resolve against the same snapshot either way, and a window that has to
// rebuild is no reason for its neighbour to.
func (e *rowEmitter) refreshWindows(tx storage.ReadTx, prev []*syncplanecursorv1.WindowState, changes []storage.Row) (closureRows, error) {
	if len(e.windows) == 0 {
		return nil, nil
	}
	states := make(map[int]*syncplanecursorv1.WindowState, len(prev))
	for _, st := range prev {
		states[int(st.GetSubscription())] = st
	}
	union := closureRows{}
	for _, w := range e.windows {
		if blind, err := w.blindOn(e); err != nil || blind {
			if err != nil {
				return nil, err
			}
			continue
		}
		maintained := false
		if st := states[w.subIndex]; st != nil {
			var err error
			if maintained, err = w.maintain(tx, e, st, changes); err != nil {
				return nil, err
			}
		}
		if !maintained {
			rows, boundary, err := w.compute(tx, e)
			if err != nil {
				return nil, err
			}
			w.adopt(rows, boundary)
		}
		w.mergeInto(union)
	}
	return union, nil
}

// maintain updates this window's membership from the rows written since the
// cursor, reporting false when the state it was handed cannot carry the update
// and the caller must resolve the window from scratch instead. That is not an
// error path: the two agree, and every reason to decline is a case where the
// cursor does not say enough about the ordering past the window's tail.
func (w *compiledWindow) maintain(tx storage.ReadTx, e *rowEmitter, prev *syncplanecursorv1.WindowState, changes []storage.Row) (bool, error) {
	if w.byRecency {
		return w.maintainByRecency(e, prev, changes)
	}
	return w.maintainByOrder(tx, e, prev, changes)
}

// maintainByRecency reorders the membership around the rows that changed.
//
// A recency window orders by the workspace sequence, so every row written since
// the cursor outranks every member the cursor carries. Entering rows go to the
// front, the members they displace are the tail of a list the cursor already
// holds in order, and nothing is compared or read. The one thing it cannot do
// is refill: a window that lost members needs the rows immediately past its
// tail, and a version-ordered scan has no way to start from one.
func (w *compiledWindow) maintainByRecency(e *rowEmitter, prev *syncplanecursorv1.WindowState, changes []storage.Row) (bool, error) {
	moved := map[string]bool{}
	var entered []storage.Row
	for _, row := range changes {
		if row.Entity != w.info.name {
			continue
		}
		moved[row.Key] = true
		_, ok, err := w.candidate(e, row)
		if err != nil {
			return false, err
		}
		if ok {
			entered = append(entered, row)
		}
	}
	if len(moved) == 0 {
		w.carryOrdered(prev.GetKeys(), nil)
		return true, nil
	}
	sort.Slice(entered, func(i, j int) bool { return aheadByRecency(entered[i], entered[j]) })
	keys := make([]string, 0, len(prev.GetKeys())+len(entered))
	rows := make(map[string]storage.Row, len(entered))
	for _, row := range entered {
		keys = append(keys, row.Key)
		rows[row.Key] = row
	}
	for _, k := range prev.GetKeys() {
		if moved[k] {
			continue // re-placed at the front above, or no longer a candidate
		}
		keys = append(keys, k)
	}
	if len(keys) < w.limit && len(prev.GetKeys()) == w.limit {
		return false, nil
	}
	if len(keys) > w.limit {
		keys = keys[:w.limit]
	}
	w.carryOrdered(keys, rows)
	return true, nil
}

// maintainByOrder decides each changed row's membership against the boundary
// the cursor carries, then repairs the size.
//
// The invariant it preserves is the one that makes the boundary enough: the
// members are exactly the candidates at or ahead of it. That holds trivially
// through the changed rows — a row is admitted iff it is a candidate at or
// ahead of the boundary — and it is what makes the membership the top `limit`
// rows,
// since every non-member then sorts behind every member. Only the SIZE can
// break, and only two bounded ordered reads repair it: entering rows push the
// boundary forward past what they displaced, leaving ones pull it back over
// whatever fills the gap.
func (w *compiledWindow) maintainByOrder(tx storage.ReadTx, e *rowEmitter, prev *syncplanecursorv1.WindowState, changes []storage.Row) (bool, error) {
	boundary := prev.GetBoundary()
	full := len(prev.GetKeys()) == w.limit
	if full && len(boundary) == 0 {
		return false, nil // a full window whose tail was never located
	}
	members := make(map[string]bool, len(prev.GetKeys()))
	for _, k := range prev.GetKeys() {
		members[k] = true
	}
	rows := map[string]storage.Row{}
	touched := false
	for _, row := range changes {
		if row.Entity != w.info.name {
			continue
		}
		touched = true
		msg, ok, err := w.candidate(e, row)
		if err != nil {
			return false, err
		}
		// Behind the boundary of a full window a candidate is out by
		// definition, and so is a row that stopped being a candidate at all.
		if ok && full && bytes.Compare(w.order.Cursor(msg.ProtoReflect(), row.Key), boundary) > 0 {
			ok = false
		}
		if !ok {
			delete(members, row.Key)
			delete(rows, row.Key)
			continue
		}
		members[row.Key] = true
		rows[row.Key] = row
	}
	if !touched {
		w.carrySet(members, nil, boundary)
		return true, nil
	}

	switch {
	case !full:
		// The window held every candidate there was, so it still does — right
		// up to the point where it fills, and a full window needs a tail that
		// nothing here has located.
		if len(members) >= w.limit {
			return false, nil
		}
		w.carrySet(members, rows, nil)
	case len(members) > w.limit:
		// Trim: walk back from the boundary over the members the entering rows
		// displaced. One row further than the overflow is what names the
		// position that separates what is left.
		over := len(members) - w.limit
		tail, err := w.walkToBoundary(tx, e, boundary, over+1)
		if err != nil {
			return false, err
		}
		i := len(tail) - 1
		for len(members) > w.limit && i >= 0 {
			if members[tail[i].row.Key] {
				delete(members, tail[i].row.Key)
				delete(rows, tail[i].row.Key)
			}
			i--
		}
		// The new boundary has to be a MEMBER's position, or it would not
		// separate the two: walk on past anything the trim left behind.
		for i >= 0 && !members[tail[i].row.Key] {
			i--
		}
		if len(members) > w.limit || i < 0 {
			return false, nil // the walk ran out before the window was down to size
		}
		w.carrySet(members, rows, w.order.Cursor(tail[i].msg.ProtoReflect(), tail[i].row.Key))
	case len(members) < w.limit:
		// Refill: the rows immediately past the boundary are the ones that move
		// up into the gap. Fewer than the deficit means the ordering is
		// exhausted and the window now holds every candidate there is, which
		// needs no boundary at all.
		fill, err := w.walkPastBoundary(tx, e, boundary, w.limit-len(members))
		if err != nil {
			return false, err
		}
		for _, r := range fill {
			members[r.row.Key] = true
			rows[r.row.Key] = r.row
		}
		var next []byte
		if len(members) == w.limit && len(fill) > 0 {
			last := fill[len(fill)-1]
			next = w.order.Cursor(last.msg.ProtoReflect(), last.row.Key)
		}
		w.carrySet(members, rows, next)
	default:
		// Still full, and every member is still at or ahead of the boundary, so
		// the position that separated them still does.
		w.carrySet(members, rows, boundary)
	}
	return true, nil
}

// walkToBoundary reads the n candidates nearest the boundary from ahead of it,
// the boundary's own row included, best first.
//
// A page's before bound is exclusive and the boundary's row is a member, so the
// bound is one byte past the boundary: nothing sorts between a position and
// that position with a trailing \u0000 appended, so it admits exactly that
// one extra row.
func (w *compiledWindow) walkToBoundary(tx storage.ReadTx, e *rowEmitter, boundary []byte, n int) ([]qrow, error) {
	upper := make([]byte, 0, len(boundary)+1)
	upper = append(append(upper, boundary...), 0)
	rows, _, err := queryRows(tx, w.info, w.pred, w.order, pageFor(nil, upper, 0, n), e.readable, e, e.srv.limits)
	return rows, err
}

// walkPastBoundary reads the n candidates immediately behind the boundary, best
// first — the rows that move up when members leave.
func (w *compiledWindow) walkPastBoundary(tx storage.ReadTx, e *rowEmitter, boundary []byte, n int) ([]qrow, error) {
	rows, _, err := queryRows(tx, w.info, w.pred, w.order, pageFor(boundary, nil, 0, n), e.readable, e, e.srv.limits)
	return rows, err
}

// candidate reports whether a row belongs in this window's ordering at all:
// live, readable, and matching the window's predicate — the same three tests
// compute applies, one row at a time. The decoded message comes back with it
// because the order position needs one.
func (w *compiledWindow) candidate(e *rowEmitter, row storage.Row) (proto.Message, bool, error) {
	if row.Deleted {
		return nil, false, nil
	}
	ok, err := e.readable(row)
	if err != nil || !ok {
		return nil, false, err
	}
	msg, ok := e.decodeRow(w.info, row)
	if !ok {
		return nil, false, nil // quarantined: no predicate can match it
	}
	if w.hasPred && !w.pred.Eval(msg.ProtoReflect()) {
		return nil, false, nil
	}
	return msg, true, nil
}

// carryOrdered and carrySet record a maintained membership: the rows this page
// read for the members it touched, and a placeholder for every member carried
// over from the cursor unread (see closureRows). Only carryOrdered knows the
// window order, which is why an order_by window leaves ordered nil and states
// its tail as a boundary instead.
func (w *compiledWindow) carryOrdered(keys []string, rows map[string]storage.Row) {
	w.members = make(closureRows, len(keys))
	for _, k := range keys {
		w.members[closureKey{w.info.name, k}] = rows[k]
	}
	w.ordered = keys
	w.boundary = nil
}

func (w *compiledWindow) carrySet(members map[string]bool, rows map[string]storage.Row, boundary []byte) {
	w.members = make(closureRows, len(members))
	for k := range members {
		w.members[closureKey{w.info.name, k}] = rows[k]
	}
	w.ordered = nil
	w.boundary = boundary
}

// windowStates projects the resolved windows into the per-window membership a
// cursor carries. Keys are sorted when the window order is not known, so that
// two runs of the same pull produce the same cursor bytes.
func (e *rowEmitter) windowStates() []*syncplanecursorv1.WindowState {
	if len(e.windows) == 0 {
		return nil
	}
	out := make([]*syncplanecursorv1.WindowState, 0, len(e.windows))
	for _, w := range e.windows {
		if w.blind {
			continue
		}
		keys := w.ordered
		if keys == nil {
			keys = make([]string, 0, len(w.members))
			for ck := range w.members {
				keys = append(keys, ck.key)
			}
			sort.Strings(keys)
		}
		out = append(out, &syncplanecursorv1.WindowState{
			Subscription: uint32(w.subIndex),
			Keys:         keys,
			Boundary:     w.boundary,
		})
	}
	return out
}

// readable reports whether a row passes its entity's read filter — the
// permission half of classify, with no subscription scope applied.
//
// Window membership is computed against this rather than against classify,
// because for a windowed subscription the window IS the scope: asking
// classify would be circular. Members are therefore already read-filtered by
// the time classify admits them, exactly as closure members are.
func (e *rowEmitter) readable(row storage.Row) (bool, error) {
	if row.Deleted {
		return false, nil
	}
	filter, err := e.filterOf(row.Entity)
	if err != nil {
		return false, err
	}
	if filter == nil {
		return true, nil
	}
	info, ok := e.srv.catalogPtr().entities[row.Entity]
	if !ok {
		return false, nil // unregistered entities are hidden defensively
	}
	msg, ok := e.decodeRow(info, row)
	if !ok {
		return false, nil // quarantined: no filter can clear it
	}
	return filter(row.Key, msg)
}

// recencyTopK keeps the cap rows with the highest versions, ties broken by
// ascending row key. A max-heap whose root is the WORST kept row, so once
// full a candidate is admitted only by beating that root — the same shape as
// topK, over storage rows instead of decoded ones.
type recencyTopK struct {
	cap  int
	rows []storage.Row
}

// aheadByRecency reports whether a sorts strictly ahead of b: newer first,
// then row key ascending. Total (keys are unique within an entity), so the
// kept set does not depend on scan order.
func aheadByRecency(a, b storage.Row) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return a.Key < b.Key
}

func (h *recencyTopK) offer(cand storage.Row) {
	if len(h.rows) < h.cap {
		heap.Push(h, cand)
		return
	}
	if aheadByRecency(cand, h.rows[0]) {
		h.rows[0] = cand
		heap.Fix(h, 0)
	}
}

func (h *recencyTopK) Len() int { return len(h.rows) }

// Less inverts the order so the root is the worst kept row.
func (h *recencyTopK) Less(i, j int) bool { return aheadByRecency(h.rows[j], h.rows[i]) }

func (h *recencyTopK) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *recencyTopK) Push(x any) { h.rows = append(h.rows, x.(storage.Row)) }

func (h *recencyTopK) Pop() any {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[:n-1]
	return x
}
