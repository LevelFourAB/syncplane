package server

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Query runs a one-shot predicate query against current server state — the
// push-down path for data outside the synced subset. Read filters always
// apply. Results are value snapshots ordered by order_by (missing values
// last, ties broken by the next key and finally the row key ascending, in
// UTF-16 code-unit order — exactly the client evaluator's compareRows).
func (s *Server) Query(ctx context.Context, req *connect.Request[syncplanev1.QueryRequest]) (*connect.Response[syncplanev1.QueryResponse], error) {
	msg := req.Msg
	if err := naming.ValidateWorkspaceID(msg.GetWorkspaceId()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if _, err := s.enterWorkspace(ctx, msg.GetWorkspaceId()); err != nil {
		return nil, err
	}
	info, ok := s.catalogPtr().entities[msg.GetEntity()]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown entity %q", msg.GetEntity()))
	}
	desc := info.msgType.Descriptor()
	predicate, err := query.Compile(desc, msg.GetPredicate())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	order, err := query.CompileOrder(desc, msg.GetOrderBy())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	limit := s.limits.MaxQueryRows
	if req := int(msg.GetLimit()); req > 0 && req < limit {
		limit = req
	}
	offset := int(msg.GetOffset())
	if offset > s.limits.MaxQueryOffset {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("offset %d", offset), "MaxQueryOffset", s.limits.MaxQueryOffset)))
	}
	if offset > 0 && (len(msg.GetAfter()) > 0 || len(msg.GetBefore()) > 0) {
		// The two describe a page in incompatible terms — a rank and a position
		// — so any resolution here would be an invention. Failing says which of
		// them the caller has to drop.
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("offset cannot be combined with after/before; page with the cursors, or drop them to page by rank"))
	}
	after, err := s.queryCursor(order, msg.GetEntity(), "after", msg.GetAfter())
	if err != nil {
		return nil, err
	}
	before, err := s.queryCursor(order, msg.GetEntity(), "before", msg.GetBefore())
	if err != nil {
		return nil, err
	}
	pg := pageFor(after, before, offset, limit)

	emitter, err := newRowEmitter(ctx, s, msg.GetWorkspaceId(), nil)
	if err != nil {
		return nil, err
	}
	// Query is a READ path: it answers for the one entity the request named,
	// having already resolved it from the catalog above, so a server-only
	// entity is reachable here even though nothing ever syncs it. Read filters
	// still apply — this is not a permission bypass, only a sync one.
	emitter.includeServerOnly = true
	// Resolve the entity's read filter BEFORE opening the snapshot (the hook
	// must not run while the read tx is pinned); classify() inside the scan
	// then hits the memoized filter. Query scans a single entity, so this is
	// the only filter to warm.
	if _, err := emitter.filterOf(info.name); err != nil {
		return nil, err
	}
	// Except for field history, whose records describe OTHER entities' rows and
	// are governed by those entities' filters rather than their own (see
	// Server.historyReadable). Warm every one they could name.
	history := info.name == historyEntityName
	if history {
		if err := s.warmHistorySourceFilters(emitter); err != nil {
			return nil, err
		}
	}

	var (
		rows []qrow
		more bool
	)
	err = s.store.WithReadTx(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
		var err error
		rows, more, err = queryRows(tx, info, predicate, order, pg, func(row storage.Row) (bool, error) {
			class, err := emitter.classify(row)
			if err != nil {
				return false, err
			}
			if class != rowVisible {
				return false, nil
			}
			if !history {
				return true, nil
			}
			return s.historyReadable(tx, emitter, info, row)
		}, emitter, s.limits)
		return err
	})
	if err != nil {
		return nil, err
	}

	resp := &syncplanev1.QueryResponse{
		Rows: make([]*syncplanev1.RowChange, 0, len(rows)),
		// Advertised here too: a query answered by a server whose schema has
		// moved on is otherwise indistinguishable from one that has not.
		SchemaRevision: s.catalogPtr().schemaRev,
		HasMore:        more,
	}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, rowChange(r.row))
	}
	if len(rows) > 0 {
		first, last := rows[0], rows[len(rows)-1]
		resp.StartCursor = order.EncodeCursor(msg.GetEntity(), first.msg.ProtoReflect(), first.row.Key)
		resp.EndCursor = order.EncodeCursor(msg.GetEntity(), last.msg.ProtoReflect(), last.row.Key)
	}
	return connect.NewResponse(resp), nil
}

// queryCursor caps and decodes one of a request's cursors. The cap comes first:
// a cursor is walked component by component to validate it, so an uncapped one
// costs work proportional to whatever the client sent.
func (s *Server) queryCursor(order *query.Order, entity, field string, cursor []byte) ([]byte, error) {
	if len(cursor) == 0 {
		return nil, nil
	}
	if len(cursor) > s.limits.MaxQueryCursorBytes {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("%s cursor of %d bytes", field, len(cursor)),
			"MaxQueryCursorBytes", s.limits.MaxQueryCursorBytes)))
	}
	entry, err := order.DecodeCursor(entity, cursor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %w", field, err))
	}
	return entry, nil
}

type qrow struct {
	row storage.Row
	msg proto.Message
}

// page is the slice of an ordering a query asks for. Exactly one of the two
// paging styles is ever in play: after/before name a POSITION in the ordering
// and offset names a COUNT of rows to discard before it, and a request setting
// both is rejected where it arrives (Query, runQuerySpec) rather than resolved
// here.
type page struct {
	// after and before are decoded cursor entries — Order.DecodeCursor's
	// result, not the framed wire form — or nil for an open end. A row is kept
	// when its own cursor sorts strictly between them.
	after, before []byte
	offset, limit int
	// backward takes the page from the before end rather than the after end,
	// which is what makes `before` alone mean "the page preceding this one".
	// The rows still come back in the query's order.
	backward bool
}

// pageFor is the only place backward is decided, so the flag cannot disagree
// with the bounds it is derived from.
func pageFor(after, before []byte, offset, limit int) page {
	if offset < 0 {
		offset = 0
	}
	return page{
		after:    after,
		before:   before,
		offset:   offset,
		limit:    limit,
		backward: after == nil && before != nil,
	}
}

// bounded reports whether a candidate row has to be tested against the cursors
// at all — one encode per candidate that an unbounded query need not pay.
func (p page) bounded() bool { return p.after != nil || p.before != nil }

// within reports whether a row at this position falls inside the page's
// cursor bounds. Both are EXCLUSIVE, so the row a cursor was minted from is
// never returned again.
func (p page) within(cursor []byte) bool {
	if p.after != nil && bytes.Compare(cursor, p.after) <= 0 {
		return false
	}
	if p.before != nil && bytes.Compare(cursor, p.before) >= 0 {
		return false
	}
	return true
}

// retain is how many rows a bounded run has to keep: the page itself plus the
// one extra row that answers has_more without a second query.
func (p page) retain() int { return p.offset + p.limit + 1 }

// queryRows answers one predicate query against a read snapshot: it scans the
// entity, keeps the rows visible admits and predicate matches, and returns one
// page of the ordering plus whether rows remain past it. Shared by the Query
// RPC (where visible applies the read filter) and ViewTx.Query (trusted, so
// everything live is visible), which is what keeps a predicate meaning the same
// thing to a client and to server-side code.
//
// emitter is the request's row emitter on a client-facing scan and nil on the
// trusted one; it decides what a row that will not decode costs — a quarantined
// row the scan skips, or an error the Go caller sees (see poison.go).
//
// A secondary index narrows which rows are considered when one serves the
// query (see queryByIndex); otherwise the predicate can only be evaluated by
// scanning and decoding every visible row, and the cost is O(entity) whatever
// the limit. What the limit bounds either way is MEMORY: only the rows that
// sort first inside the page's bounds need to be retained, so a max-heap whose
// root is the worst kept row evicts in O(log k) as better rows arrive. A
// non-positive limit means no cap and retains every match; only trusted
// in-process callers can ask for that.
//
// A cursor bound costs one encode per candidate here, since a scan has no way
// to know a row is out of range before it has the row. An index-served ordered
// query pays much less: the walk seeks past the bound and stops once the page
// is full (see queryByIndex).
func queryRows(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	order *query.Order,
	pg page,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
	limits Limits,
) (rows []qrow, more bool, err error) {
	h := &topK{order: order, cap: pg.retain(), unbounded: pg.limit <= 0, worstFirst: pg.backward}
	consider := func(row storage.Row, pred *query.Compiled) error {
		ok, err := visible(row)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		decoded, err := decodeMatchable(emitter, info, row)
		if err != nil || decoded == nil {
			return err
		}
		if !pred.Eval(decoded.ProtoReflect()) {
			return nil
		}
		if pg.bounded() && !pg.within(order.Cursor(decoded.ProtoReflect(), row.Key)) {
			return nil
		}
		h.offer(qrow{row: row, msg: decoded})
		return nil
	}

	// An index narrows which rows are considered; it never changes which of
	// them match, so the rest of this function is identical either way — the
	// same visibility check, the same decode, the same heap, the same sort. A
	// declined plan, an unbuilt index, or an exhausted candidate budget all
	// fall through to the scan below, and h.reset is how a decline hands the
	// heap back empty: the scan re-offers every row the walk already offered,
	// and the heap does not dedupe.
	if served, err := queryByIndex(tx, info, predicate, order, pg, limits, consider, h.full, h.reset); err != nil {
		return nil, false, err
	} else if !served {
		if err := tx.ScanEntity(info.name, func(row storage.Row) error {
			return consider(row, predicate)
		}); err != nil {
			return nil, false, err
		}
	}

	rows = h.rows
	sort.SliceStable(rows, func(i, j int) bool {
		return order.Less(
			rows[i].msg.ProtoReflect(), rows[j].msg.ProtoReflect(),
			rows[i].row.Key, rows[j].row.Key,
		)
	})
	rows, more = pg.trim(rows)
	return rows, more, nil
}

// trim cuts the retained rows down to the page and reports whether any were
// left over. The retained set is one row longer than the page (topK held
// retain()), and which end that extra row sits at is which end the limit
// counted from — so a backward page keeps its LAST limit rows and a forward one
// its first. The result is in the query's order either way.
func (p page) trim(rows []qrow) ([]qrow, bool) {
	if p.limit <= 0 {
		// Unbounded: everything that matched is the answer, so nothing is left
		// over to report.
		return rows[min(p.offset, len(rows)):], false
	}
	if p.backward {
		if len(rows) > p.limit {
			return rows[len(rows)-p.limit:], true
		}
		return rows, false
	}
	if p.offset >= len(rows) {
		return nil, false
	}
	rows = rows[p.offset:]
	if len(rows) > p.limit {
		return rows[:p.limit], true
	}
	return rows, false
}

// topK keeps the cap rows that sort first under order. It is a max-heap: the
// root (rows[0]) is the worst kept row, so once full a candidate is admitted
// only when it sorts ahead of that root, replacing it. order.Compare is a
// total order (ties broken by the unique row key), so the kept set is
// deterministic regardless of scan order.
//
// unbounded turns retention off entirely — every offered row is kept, and the
// heap invariant is never established because the caller sorts at the end
// anyway. It exists for in-process callers that asked for no limit.
type topK struct {
	order *query.Order
	cap   int
	// worstFirst inverts what "first" means, so the heap keeps the cap rows
	// that sort LAST. That is the retention a backward page needs: its limit
	// counts back from the before bound, so the rows nearest that bound are the
	// ones worth keeping.
	worstFirst bool
	unbounded  bool
	rows       []qrow
}

// full reports that the heap cannot admit a better row than it already holds.
// An ordered index walk uses it to stop early: the rows it has are the page,
// and everything past them sorts worse.
func (h *topK) full() bool {
	return !h.unbounded && h.cap > 0 && len(h.rows) >= h.cap
}

// reset drops every row kept so far, leaving the heap as newly built. It is the
// un-offer half of queryByIndex's contract: an index walk that declines after
// already offering rows hands them back this way, and the scan that follows
// offers them again.
func (h *topK) reset() { h.rows = h.rows[:0] }

func (h *topK) offer(cand qrow) {
	if h.unbounded {
		h.rows = append(h.rows, cand)
		return
	}
	if h.cap <= 0 {
		return
	}
	if len(h.rows) < h.cap {
		heap.Push(h, cand)
		return
	}
	if h.less(cand, h.rows[0]) {
		h.rows[0] = cand
		heap.Fix(h, 0)
	}
}

// less reports whether a is the better row to KEEP: ahead of b in the
// requested order, or behind it when the page counts back from its before
// bound.
func (h *topK) less(a, b qrow) bool {
	c := h.order.Compare(
		a.msg.ProtoReflect(), b.msg.ProtoReflect(), a.row.Key, b.row.Key,
	)
	if h.worstFirst {
		return c > 0
	}
	return c < 0
}

func (h *topK) Len() int { return len(h.rows) }

// Less inverts the order so the heap is a max-heap: the root is the worst kept
// row, the first to be evicted.
func (h *topK) Less(i, j int) bool { return h.less(h.rows[j], h.rows[i]) }

func (h *topK) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *topK) Push(x any) { h.rows = append(h.rows, x.(qrow)) }

func (h *topK) Pop() any {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[:n-1]
	return x
}
