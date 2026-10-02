package server

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

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
	adm, err := s.enterWorkspace(ctx, msg.GetWorkspaceId())
	if err != nil {
		return nil, err
	}
	info, ok := s.catalogPtr().entities[msg.GetEntity()]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown entity %q", msg.GetEntity()))
	}
	desc := info.msgType.Descriptor()
	// A rel condition crosses into another entity, so the predicate resolves in
	// two phases: validation here at the boundary — the rel nodes themselves via
	// ExtractRels, everything else by compiling with each rel stubbed out to the
	// empty membership test it collapses to when nothing matches — and the real
	// rewrite inside the snapshot below, where the target rows are.
	rels, err := query.ExtractRels(desc, msg.GetPredicate())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.checkRelLimit(rels); err != nil {
		return nil, err
	}
	stubbed := make(map[*syncplanev1.RelCond]*syncplanev1.Condition, len(rels))
	for _, node := range rels {
		if _, err := s.relTargetInfo(node); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		stubbed[node.Cond] = query.RelReplacement(node, nil, msg.GetWorkspaceId())
	}
	predicate, err := query.Compile(desc, query.RewriteRels(msg.GetPredicate(), stubbed))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	group, err := compileGrouping(desc, msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	facets, err := s.compileFacets(desc, msg, group != nil)
	if err != nil {
		return nil, err
	}
	order, err := query.CompileOrderWith(desc, msg.GetOrderBy(), predicate)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	limit := s.limits.MaxQueryRows
	if req := int(msg.GetLimit()); req > 0 && req < limit {
		limit = req
	}
	perGroup := 0
	if msg.GetAggregate() == syncplanev1.AggregateOp_AGGREGATE_OP_ROWS {
		// At least one: compileGrouping has already refused a request that asked
		// for no rows per group, and a zero here would divide by it below.
		perGroup = max(1, min(int(msg.GetPerGroup()), s.limits.MaxQueryRows))
		// The response carries limit × perGroup rows, so capping the groups is
		// what keeps a grouped read inside the row budget an ordinary page has.
		// One group always fits: it is the read the caller asked for, cut short.
		limit = min(limit, max(1, s.limits.MaxQueryRows/perGroup))
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
	emitter.caller = adm.caller
	// Resolve the entity's read filter BEFORE opening the snapshot (the hook
	// must not run while the read tx is pinned); classify() inside the scan
	// then hits the memoized filter. Query scans a single entity, so this is
	// the only filter to warm.
	if _, err := emitter.appFilterOf(info.name); err != nil {
		return nil, err
	}
	// A rel pre-pass reads the TARGET entities too, under the same rule, so
	// their filters resolve up front as well.
	for _, node := range rels {
		target, err := s.relTargetInfo(node)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		if _, err := emitter.appFilterOf(target.name); err != nil {
			return nil, err
		}
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
		rows      []qrow
		groups    []query.GroupCount
		rowGroups []*rowGroup
		more      bool
		fold      *facetFold
	)
	// Anchored: query cursors are value-based and carry no seq to floor on,
	// and a caller who just wrote expects the query to see it.
	err = s.readTxAnchored(ctx, msg.GetWorkspaceId(), func(tx storage.ReadTx) error {
		if err := emitter.bindAccess(tx); err != nil {
			return err
		}
		filter, err := emitter.filterOf(info.name)
		if err != nil {
			return err
		}
		// A row a filter or the history rule can hide is a row an index entry
		// may not stand in for; the grouped walk needs that stated, because
		// otherwise it counts entries without reading them.
		filtered := filter != nil || history
		visible := func(row storage.Row) (bool, error) {
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
		}
		pred := predicate
		if len(rels) > 0 {
			// The pre-pass runs inside the same snapshot as the base scan, so
			// the membership sets and the rows they are tested against are one
			// consistent view.
			replacements, err := s.resolveRels(tx, msg.GetWorkspaceId(), rels, visible, emitter)
			if err != nil {
				return err
			}
			pred, err = query.Compile(desc, query.RewriteRels(msg.GetPredicate(), replacements))
			if err != nil {
				return err
			}
		}
		if group != nil {
			if msg.GetAggregate() == syncplanev1.AggregateOp_AGGREGATE_OP_ROWS {
				rowGroups, more, err = groupRows(tx, info, pred, group, order,
					perGroup, limit, msg.GetPerGroupTotal(), visible, emitter, s.limits)
				return err
			}
			groups, more, err = groupCounts(tx, info, pred, group,
				msg.GetAggregate() == syncplanev1.AggregateOp_AGGREGATE_OP_DISTINCT,
				limit, filtered, visible, emitter, s.limits)
			return err
		}
		rows, more, fold, err = queryRowsFaceted(tx, info, pred, order, pg, visible, emitter, s.limits, facets)
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
	if group != nil && msg.GetAggregate() == syncplanev1.AggregateOp_AGGREGATE_OP_ROWS {
		resp.RowGroups = make([]*syncplanev1.QueryRowGroup, 0, len(rowGroups))
		for _, g := range rowGroups {
			out := &syncplanev1.QueryRowGroup{
				Value:   g.value.Proto(),
				Rows:    make([]*syncplanev1.RowChange, 0, len(g.rows)),
				Total:   g.total,
				HasMore: g.hasMore,
			}
			for _, r := range g.rows {
				out.Rows = append(out.Rows, rowChange(r.row))
			}
			resp.RowGroups = append(resp.RowGroups, out)
		}
		return connect.NewResponse(resp), nil
	}
	if group != nil {
		resp.Groups = make([]*syncplanev1.QueryGroup, 0, len(groups))
		for _, g := range groups {
			resp.Groups = append(resp.Groups, &syncplanev1.QueryGroup{
				Value: g.Value.Proto(),
				Count: g.Count,
			})
		}
		return connect.NewResponse(resp), nil
	}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, rowChange(r.row))
	}
	if fold != nil {
		resp.Facets = fold.proto(msg.GetFacets())
	}
	if len(rows) > 0 {
		first, last := rows[0], rows[len(rows)-1]
		resp.StartCursor = order.EncodeCursor(msg.GetEntity(), first.msg.ProtoReflect(), first.row.Key)
		resp.EndCursor = order.EncodeCursor(msg.GetEntity(), last.msg.ProtoReflect(), last.row.Key)
	}
	return connect.NewResponse(resp), nil
}

// compileGrouping resolves a request's grouping key, or returns nil for an
// ordinary row query.
//
// The paging fields describe a page of ROWS and have no counterpart over a set
// of groups — a group has no cursor, and the groups themselves are ordered by
// the grouping path — so a request combining them is refused rather than
// resolved. Saying which field to drop is the whole value of failing here.
//
// order_by is the exception, and only under AGGREGATE_OP_ROWS: there it orders
// the rows INSIDE each group, which is what makes "the first N" mean anything.
func compileGrouping(
	desc protoreflect.MessageDescriptor,
	msg *syncplanev1.QueryRequest,
) (*query.Group, error) {
	path := msg.GetGroupBy()
	op := msg.GetAggregate()
	rows := op == syncplanev1.AggregateOp_AGGREGATE_OP_ROWS
	if len(path) == 0 {
		if op != syncplanev1.AggregateOp_AGGREGATE_OP_UNSPECIFIED {
			return nil, fmt.Errorf("aggregate %s needs a group_by path", op)
		}
		return nil, nil
	}
	switch op {
	case syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
		syncplanev1.AggregateOp_AGGREGATE_OP_DISTINCT,
		syncplanev1.AggregateOp_AGGREGATE_OP_ROWS:
	default:
		return nil, errors.New("group_by needs an aggregate of COUNT, DISTINCT or ROWS")
	}
	switch {
	case rows && msg.GetPerGroup() == 0:
		return nil, errors.New("aggregate ROWS needs per_group; it is how many rows each group returns")
	case !rows && msg.GetPerGroup() > 0:
		return nil, fmt.Errorf("per_group needs aggregate ROWS; %s answers with no rows to cap", op)
	case !rows && len(msg.GetOrderBy()) > 0:
		return nil, errors.New("group_by cannot be combined with order_by; groups come back in ascending value order")
	case msg.GetOffset() > 0:
		return nil, errors.New("group_by cannot be combined with offset; limit caps the groups returned")
	case len(msg.GetAfter()) > 0 || len(msg.GetBefore()) > 0:
		return nil, errors.New("group_by cannot be combined with after/before; a cursor names a row position, which a group has none of")
	}
	return query.CompileGroup(desc, path)
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
	rows, more, _, err = queryRowsFaceted(tx, info, predicate, order, pg, visible, emitter, limits, nil)
	return rows, more, err
}

// queryRowsFaceted is queryRows with the request's facets folded from the
// same walk: every row the predicate and the page's bounds admit reaches the
// fold, whether or not the page keeps it. The fold is nil when no facet was
// asked for, and the walk is then exactly queryRows'.
//
// A facet costs the early stop an ordered index walk otherwise takes once the
// page is full — the fold needs every match, so `want` never reports the page
// settled — and nothing else: the index still narrows which rows are read, and
// the heap still bounds what is retained.
func queryRowsFaceted(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	order *query.Order,
	pg page,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
	limits Limits,
	facets []*query.Group,
) (rows []qrow, more bool, fold *facetFold, err error) {
	h := &topK{order: order, cap: pg.retain(), unbounded: pg.limit <= 0, worstFirst: pg.backward}
	if len(facets) > 0 {
		fold = newFacetFold(facets)
	}
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
		if fold != nil {
			fold.add(decoded.ProtoReflect())
		}
		h.offer(qrow{row: row, msg: decoded})
		return nil
	}
	want, discard := h.want, h.reset
	if fold != nil {
		want = func() int { return math.MaxInt }
		discard = func() {
			h.reset()
			fold.reset()
		}
	}

	// An index narrows which rows are considered; it never changes which of
	// them match, so the rest of this function is identical either way — the
	// same visibility check, the same decode, the same heap, the same sort. A
	// declined plan, an unbuilt index, or an exhausted candidate budget all
	// fall through to the scan below, and discard is how a decline hands the
	// heap (and the fold) back empty: the scan re-offers every row the walk
	// already offered, and neither dedupes.
	//
	// A caller the access model narrows considers only its candidate rows, on
	// both paths.
	narrowed, err := emitter.accessRowsOf(tx, info)
	if err != nil {
		return nil, false, nil, err
	}
	if served, err := queryByIndex(tx, info, predicate, order, pg, limits, narrowed, consider, want, discard); err != nil {
		return nil, false, nil, err
	} else if !served {
		if err := narrowed.scan(tx, info.name, func(row storage.Row) error {
			return consider(row, predicate)
		}); err != nil {
			return nil, false, nil, err
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
	return rows, more, fold, nil
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

// want reports how many more rows the heap can admit before it holds the whole
// page. Zero is the stop signal an ordered index walk reads: the rows it has
// are the page, and everything past them sorts worse.
//
// A positive count is also how far ahead such a walk may read — it resolves
// candidates in batches, and a batch bigger than this fetches rows the page
// cannot use. A heap that retains nothing (unbounded, or no cap at all) bounds
// neither, so it wants an unlimited number.
func (h *topK) want() int {
	if h.unbounded || h.cap <= 0 {
		return math.MaxInt
	}
	return max(0, h.cap-len(h.rows))
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
