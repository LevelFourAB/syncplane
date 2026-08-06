// Windowed subscriptions: a subscription can bound its synced set by RANK
// rather than only by attribute — "the 1000 most recently updated open
// issues" instead of "every open issue". Without it the only lever an app has
// over working-set size is a more selective predicate, which for a real view
// (a busy project, "all open", an active label) often is not semantically
// possible.
//
// A window is the same KIND of object a follow closure is: a bounded
// membership set that cannot be decided per row, is recomputed on every pull,
// is diffed against an exact key-set carried on the cursor, and emits entering
// rows as values and leaving rows as tombstones-unless-otherwise-visible. So
// it reuses that machinery (closureRows, emitMembershipDiff) rather than
// growing a parallel one; the only difference is the membership rule —
// `rank <= limit` instead of `reachable by a ref path`.
//
// Two orderings:
//   - order_by: the entity's own fields, via the same query.Order that backs
//     the Query RPC and ViewTx.Query, so a window means what the equivalent
//     query means.
//   - by_recency: the row's workspace sequence, descending. It sorts without
//     decoding anything, which makes it the cheap window to serve.
//
// What this does NOT do is make an order_by window bounded on the SERVER:
// computing one still costs a scan of the entity per pull — the same price
// Query pays — because nothing yet plans a query against a secondary index.
// What the window bounds is the CLIENT: what it holds, bootstraps, and
// materializes. The recency ordering IS bounded on both sides: the store can
// produce one entity's rows newest-first, so an unfiltered recency window
// reads `limit` rows and stops.
package server

import (
	"container/heap"
	"errors"
	"fmt"

	"connectrpc.com/connect"

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
	// pred is always non-nil (query.Compile of a nil condition matches
	// everything); hasPred says whether evaluating it can change the outcome,
	// which is what keeps a predicate-less recency window from decoding rows.
	pred      *query.Compiled
	hasPred   bool
	order     *query.Order
	byRecency bool
	limit     int
	// members is this window's membership for the current request, filled by
	// buildWindows; nil before that.
	members closureRows
}

// compileWindow validates and resolves one window spec. Alongside the
// unknown-entity and predicate-compile checks in newRowEmitter, this is where
// a malformed window becomes InvalidArgument rather than a surprise later.
func compileWindow(info *entityInfo, sub *syncplanev1.Subscription, limits Limits) (*compiledWindow, error) {
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

// buildWindows computes every window's membership against the rows in tx and
// returns their union — the set classify treats as visible. Each window also
// keeps its own members, which is what a follow spec on a windowed
// subscription seeds from.
//
// Must run BEFORE the pull scan (classify has to be window-aware) and before
// buildClosure (which seeds from window members).
func (e *rowEmitter) buildWindows(tx storage.ReadTx) (closureRows, error) {
	if len(e.windows) == 0 {
		return nil, nil
	}
	union := closureRows{}
	for _, w := range e.windows {
		rows, err := w.compute(tx, e)
		if err != nil {
			return nil, err
		}
		w.members = closureRows{}
		for _, row := range rows {
			ck := closureKey{row.Entity, row.Key}
			w.members[ck] = row
			union[ck] = row
		}
	}
	return union, nil
}

// compute resolves this window's membership: the `limit` predicate-matching,
// readable rows of the entity that sort first.
func (w *compiledWindow) compute(tx storage.ReadTx, e *rowEmitter) ([]storage.Row, error) {
	if w.byRecency {
		return w.computeByRecency(tx, e)
	}
	// The order path is exactly a Query: same scan, same bounded-memory
	// max-heap, same total order — which is what keeps "the window" and "the
	// equivalent query" from drifting apart.
	rows, _, err := queryRows(tx, w.info, w.pred, w.order, pageFor(nil, nil, 0, w.limit), e.readable, e, e.srv.limits)
	if err != nil {
		return nil, err
	}
	out := make([]storage.Row, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.row)
	}
	return out, nil
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
	return h.rows, nil
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
	return filter(row.Key, msg), nil
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
