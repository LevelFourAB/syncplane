package server

import (
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

	var rows []qrow
	err = s.store.WithReadTx(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
		var err error
		rows, err = queryRows(tx, info, predicate, order, offset, limit, func(row storage.Row) (bool, error) {
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
	}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, rowChange(r.row))
	}
	return connect.NewResponse(resp), nil
}

type qrow struct {
	row storage.Row
	msg proto.Message
}

// queryRows answers one predicate query against a read snapshot: it scans the
// entity, keeps the rows visible admits and predicate matches, and returns the
// page [offset, offset+limit) of the ordering. Shared by the Query RPC (where
// visible applies the read filter) and ViewTx.Query (trusted, so everything
// live is visible), which is what keeps a predicate meaning the same thing to
// a client and to server-side code.
//
// emitter is the request's row emitter on a client-facing scan and nil on the
// trusted one; it decides what a row that will not decode costs — a quarantined
// row the scan skips, or an error the Go caller sees (see poison.go).
//
// A secondary index narrows which rows are considered when one serves the
// query (see queryByIndex); otherwise the predicate can only be evaluated by
// scanning and decoding every visible row, and the cost is O(entity) whatever
// the limit. What the limit bounds either way is MEMORY: only the offset+limit
// rows that sort first need to be retained, so a max-heap whose root is the
// worst kept row evicts in O(log k) as better rows arrive. A non-positive limit
// means no cap and retains every match; only trusted in-process callers can ask
// for that.
func queryRows(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	order *query.Order,
	offset, limit int,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
	limits Limits,
) ([]qrow, error) {
	h := &topK{order: order, cap: offset + limit, unbounded: limit <= 0}
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
	if served, err := queryByIndex(tx, info, predicate, order, limits, consider, h.reset); err != nil {
		return nil, err
	} else if !served {
		if err := tx.ScanEntity(info.name, func(row storage.Row) error {
			return consider(row, predicate)
		}); err != nil {
			return nil, err
		}
	}

	rows := h.rows
	sort.SliceStable(rows, func(i, j int) bool {
		return order.Less(
			rows[i].msg.ProtoReflect(), rows[j].msg.ProtoReflect(),
			rows[i].row.Key, rows[j].row.Key,
		)
	})

	if offset >= len(rows) {
		return nil, nil
	}
	rows = rows[offset:]
	// Bounded runs are already <= limit (the heap held offset+limit); an
	// unbounded run has to be clamped here, and only when a limit was given.
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
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
	order     *query.Order
	cap       int
	unbounded bool
	rows      []qrow
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

// less reports whether a sorts strictly ahead of b in the requested order.
func (h *topK) less(a, b qrow) bool {
	return h.order.Compare(
		a.msg.ProtoReflect(), b.msg.ProtoReflect(), a.row.Key, b.row.Key,
	) < 0
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
