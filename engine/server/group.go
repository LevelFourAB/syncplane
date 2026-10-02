package server

import (
	"errors"
	"sort"

	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Answering a grouped read — the counted and previewed surfaces of a list UI.
//
// A grouped read partitions the match set by one path and answers with the
// groups rather than with one page of rows: their values alone (a filter
// dropdown, a facet sidebar's axis), each with the number of matching rows in
// it (a board column's badge), or each with its first rows in the requested
// order (the column's preview). The definition is the scan — bucket every
// visible matching row by query.Group.Values — and everything below is an
// optimization over it that has to agree bucket for bucket or decline.
//
// # The skip-scan
//
// With an index whose components are `[eq-filter fields…, groupPath, …]` the
// walk never touches most of the rows a group holds. It seeks to the first
// entry of the range, cuts that entry at the end of the partition component
// (GroupPrefix), does the group's work inside [prefix, prefix⁺), and then seeks
// to prefix⁺ — the next distinct value. Cost is O(groups) seeks plus, for a
// count, one entry step per row, against O(rows) row reads for the scan.
//
// Because the index stores the partition component in order, groups come out in
// order too, so a limited read stops after limit+1 groups instead of bucketing
// the whole entity. That is what makes a facet sidebar over a large entity
// affordable, and it is why the walk reverses direction for a descending
// component rather than declining one.
//
// The same walk serves the head of each group (groupRows): inside one group the
// entries are in the index's own order, so when that order IS the requested one
// — query.PlanGroupRows declines otherwise — the walk stops after per+1 rows
// and the read costs groups × per rather than the whole entity.
//
// # Reading a count off entries
//
// `countable` below is the condition under which an entry may be counted
// without its row: the range answers the predicate outright, every entry inside
// one group names a distinct row, and nothing else can hide a live row from
// this caller. Otherwise each candidate row is read and re-checked, which still
// beats the scan by every row outside the range.
//
// Even a counted group reads ONE row, to name itself: the entry carries the
// group's encoded value and only the row carries the typed one.
//
// One asymmetry that path inherits, deliberately: an entry whose row will not
// decode is counted, where a scan quarantines that row and drops it
// (poison.go). Decoding every row to catch a corrupt one would tax every
// healthy workspace for a fault none of them have — the same trade the
// zero-decode full-sync path already makes. A group that no row can NAME is a
// different matter and declines the index outright, because that is the case
// where the two answers would differ in their shape and not only in a count.

// errGroupNamed stops a distinct walk that has found the one row it needed.
var errGroupNamed = errors.New("server: group named")

// errGroupUnnamed unwinds a walk over a group whose rows all failed to name it,
// which means the entries no longer describe the rows they point at. Like
// errIndexBudget it means "fall back to the scan", not "fail".
var errGroupUnnamed = errors.New("server: group could not be named")

// groupRead is one grouped read in progress: what it groups by, what it may
// count without looking, and the buckets it has accumulated.
type groupRead struct {
	tx   storage.ReadTx
	info *entityInfo
	// group is the compiled grouping key, whose Values decide which groups a row
	// belongs to.
	group *query.Group
	// distinct drops the count: the read names the values and reads no row it
	// does not need in order to name one.
	distinct bool
	// limit caps the groups returned, 0 for uncapped.
	limit int
	// filtered reports that a row can be hidden by something an index entry
	// cannot see — a read filter, or field history's per-source rule — so an
	// entry never stands in for a row.
	filtered bool
	visible  func(storage.Row) (bool, error)
	emitter  *rowEmitter
	limits   Limits
	// rows narrows the read to the rows the access model lets the caller see,
	// nil when it narrows nothing.
	rows *accessRows

	// byKey buckets groups by GroupValue.Key while they accumulate; the order
	// the surface promises is imposed once, at the end.
	byKey map[string]int
	out   []query.GroupCount
}

func (r *groupRead) reset() {
	r.out, r.byKey = nil, map[string]int{}
}

// bucket adds a group's contribution. A distinct read drops the number — the
// group exists because a row put it there, and that is all it reports — so the
// count is zeroed here rather than at each call site.
func (r *groupRead) bucket(v query.GroupValue, n uint64) {
	if r.distinct {
		n = 0
	}
	key := v.Key()
	if i, ok := r.byKey[key]; ok {
		r.out[i].Count += n
		return
	}
	r.byKey[key] = len(r.out)
	r.out = append(r.out, query.GroupCount{Value: v, Count: n})
}

// groupCounts answers one counted grouped read against a read snapshot: the
// groups of the match set in ascending value order, plus whether the limit left
// any out.
//
// It mirrors queryRows — the same visibility check, the same decode, the same
// predicate — and differs only in what it does with a row it keeps. An index
// narrows the work when one partitions on the grouping path; otherwise every
// visible row is read and bucketed, and the cost is O(entity) whatever the
// limit.
//
// `filtered` is the caller's statement that a row can be hidden by something
// this function cannot see. Passing false when a read filter is in play would
// count rows the caller may not read.
func groupCounts(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	group *query.Group,
	distinct bool,
	limit int,
	filtered bool,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
	limits Limits,
) (groups []query.GroupCount, more bool, err error) {
	r := &groupRead{
		tx: tx, info: info, group: group, distinct: distinct, limit: limit,
		filtered: filtered, visible: visible, emitter: emitter, limits: limits,
		byKey: map[string]int{},
	}
	if r.rows, err = emitter.accessRowsOf(tx, info); err != nil {
		return nil, false, err
	}
	served, err := r.byIndex(predicate)
	if err != nil {
		return nil, false, err
	}
	if !served {
		r.reset()
		if err := r.rows.scan(tx, info.name, func(row storage.Row) error {
			return r.considerRow(row, predicate)
		}); err != nil {
			return nil, false, err
		}
		query.SortGroups(r.out)
	}
	// An index walk visits groups in order and stops one past the limit, so its
	// accumulated set is already ordered and at most one group too long — the
	// same shape the sorted scan produces.
	if limit > 0 && len(r.out) > limit {
		return r.out[:limit], true, nil
	}
	return r.out, false, nil
}

// considerRow buckets one scanned row, under the same visibility and decode
// rules queryRows applies before offering a row to its heap.
//
// A repeated grouping path puts a row in one group per DISTINCT element: a row
// listing the same label twice occupies one index entry, so the scan collapses
// repeats the same way or the two disagree on that row's contribution.
func (r *groupRead) considerRow(row storage.Row, pred *query.Compiled) error {
	ok, err := r.visible(row)
	if err != nil || !ok {
		return err
	}
	decoded, err := decodeMatchable(r.emitter, r.info, row)
	if err != nil || decoded == nil {
		return err
	}
	if !pred.Eval(decoded.ProtoReflect()) {
		return nil
	}
	seen := map[string]bool{}
	for _, gv := range r.group.Values(decoded.ProtoReflect()) {
		key := gv.Key()
		if seen[key] {
			continue
		}
		seen[key] = true
		r.bucket(gv, 1)
	}
	return nil
}

// byIndex answers the read from a secondary index when one partitions on the
// grouping path, reporting whether it did.
//
// Every decline leaves the caller exactly where it started: a group is bucketed
// only once it is complete, and a decline throws away what it accumulated
// rather than handing the scan a half-walked answer to add to.
func (r *groupRead) byIndex(predicate *query.Compiled) (bool, error) {
	if r.limits.DisableIndexes || len(r.info.indexes) == 0 {
		return false, nil
	}
	candidates := make([]query.Index, 0, len(r.info.indexes))
	for i := range r.info.indexes {
		ready, err := r.info.indexes[i].ready(r.tx, r.info.name)
		if err != nil {
			return false, err
		}
		if ready {
			candidates = append(candidates, query.Index{
				Name:    r.info.indexes[i].name,
				Encoder: r.info.indexes[i].encoder,
			})
		}
	}
	plan := query.PlanGroup(candidates, predicate, r.group)
	if plan == nil {
		return false, nil
	}
	// The residual is what the range does not already guarantee. A nil one means
	// the range is exact, and match-everything is the right predicate to
	// re-check with — not the original, which would re-do the range's work.
	residual := plan.Residual
	if residual == nil {
		residual = &query.Compiled{}
	}
	// An entry may stand in for a row only when nothing left can reject it: the
	// range answers the predicate whole, each entry inside a group names a
	// distinct row, and no filter hides one.
	countable := plan.Residual == nil && plan.EntriesAreRows && !r.filtered

	order := storage.ScanAsc
	if plan.Desc {
		// A descending partition component stores its values inverted, so reading
		// the range backward is what yields groups in ascending value order.
		order = storage.ScanDesc
	}
	// One past the limit: the extra group reports that more exist, exactly as
	// the extra row does for a page.
	wantGroups := 0
	if r.limit > 0 {
		wantGroups = r.limit + 1
	}
	walked := 0
	budget := r.limits.maxIndexScanCandidates()
	lo, hi := plan.Range.Lo, plan.Range.Hi
	for wantGroups == 0 || len(r.out) < wantGroups {
		entry, err := seekEntry(r.tx, r.info.name, plan.Index, lo, hi, order)
		if err != nil {
			return false, err
		}
		if entry == nil {
			return true, nil
		}
		prefix, err := plan.GroupPrefix(entry)
		if err != nil {
			return false, err
		}
		next := query.UpperBound(prefix)
		err = r.walkGroup(plan, residual, countable, prefix,
			maxBound(prefix, lo), minBound(next, hi), &walked, budget)
		if err != nil {
			if errors.Is(err, errIndexBudget) || errors.Is(err, errGroupUnnamed) {
				r.reset()
				return false, nil
			}
			return false, err
		}
		if order == storage.ScanAsc {
			if next == nil {
				// The group's prefix is all 0xff, so nothing sorts above it.
				return true, nil
			}
			lo = next
		} else {
			hi = prefix
		}
	}
	return true, nil
}

// groupNameCandidates bounds the rows a counted group reads while looking for
// one that names it. Past the first, every candidate is an entry that disagrees
// with the row it points at, and one more read will not settle a group the
// index cannot describe.
const groupNameCandidates = 8

// walkGroup does one group's work over [lo, hi) — whose entries all begin with
// `prefix` — and buckets it when it holds a matching row.
//
// A group with no matching row is not bucketed, which is how a residual that
// rejects everything in a range makes a group simply not exist, exactly as it
// does for the scan.
func (r *groupRead) walkGroup(
	plan *query.GroupPlan,
	residual *query.Compiled,
	countable bool,
	prefix, lo, hi []byte,
	walked *int,
	budget int,
) error {
	var (
		count uint64
		value query.GroupValue
		named bool
		// names are the row keys a counted group may read to name itself; an
		// uncounted one reads every candidate anyway.
		names []string
		seen  = map[string]struct{}{}
	)
	consider := func(row storage.Row) error {
		if row.Deleted {
			// Entries are cleared on delete, so this is an entry that outlived its
			// row. Skipping keeps the answer identical to the scan's.
			return nil
		}
		if !countable {
			ok, err := r.visible(row)
			if err != nil || !ok {
				return err
			}
		}
		decoded, err := decodeMatchable(r.emitter, r.info, row)
		if err != nil || decoded == nil {
			return err
		}
		if !countable && !residual.Eval(decoded.ProtoReflect()) {
			return nil
		}
		if !named {
			if gv, ok := plan.ValueOf(r.group, decoded.ProtoReflect(), prefix); ok {
				value, named = gv, true
			}
		}
		if !countable {
			count++
		}
		if named && r.distinct {
			return errGroupNamed
		}
		return nil
	}
	batch := &rowBatch{tx: r.tx, entity: r.info.name, size: maxRowBatch, emit: consider}
	err := r.tx.ScanIndex(r.info.name, plan.Index, lo, hi, 0, storage.ScanAsc,
		func(_ []byte, rowKey string) error {
			*walked++
			if *walked > budget {
				return errIndexBudget
			}
			if !r.rows.admits(rowKey) {
				return nil
			}
			if countable {
				count++
				if len(names) < groupNameCandidates {
					names = append(names, rowKey)
				}
				return nil
			}
			// One row can hold several entries of a group when a component past
			// the partition fans out; a scan visits each row once and the two
			// must agree.
			if _, dup := seen[rowKey]; dup {
				return nil
			}
			seen[rowKey] = struct{}{}
			return batch.add(rowKey)
		})
	switch {
	case errors.Is(err, errGroupNamed):
	case err != nil:
		return err
	default:
		if err := batch.flush(); err != nil && !errors.Is(err, errGroupNamed) {
			return err
		}
	}
	if countable && count > 0 {
		if err := r.tx.GetRows(r.info.name, names, consider); err != nil && !errors.Is(err, errGroupNamed) {
			return err
		}
	}
	if count == 0 {
		return nil
	}
	if !named {
		// The group holds rows and none of them describes it, so the entries no
		// longer match the rows. The scan is the only thing that can say what the
		// groups really are.
		return errGroupUnnamed
	}
	r.bucket(value, count)
	return nil
}

// errGroupFilled stops a walk over a group that has the rows it was asked for.
// Like errPageFilled it is a success: the rows in hand are the group's head,
// and every entry past them sorts behind one of them.
var errGroupFilled = errors.New("server: group filled")

// rowGroup is one group of a grouped ROW read: the value its rows share at the
// grouping path, its first rows in the requested order, and what the walk
// learned about the rest of them.
type rowGroup struct {
	value query.GroupValue
	rows  []qrow
	// total is the group's matching rows in all, 0 when the read was not asked
	// to count them. A group holding no matching row does not exist at all, so 0
	// never means "empty".
	total   uint64
	hasMore bool
}

// rowGroupRead is one grouped row read in progress: what it groups by, what
// "first" means inside a group, and the groups it has accumulated.
type rowGroupRead struct {
	tx    storage.ReadTx
	info  *entityInfo
	group *query.Group
	order *query.Order
	// per is how many rows each group returns, 0 for all of them.
	per int
	// limit caps the groups returned, 0 for uncapped.
	limit int
	// counted reports each group's matching rows in all, which costs the walk
	// every row of a group where the rows alone cost its head.
	counted bool
	visible func(storage.Row) (bool, error)
	emitter *rowEmitter
	limits  Limits
	// rows narrows the read to the rows the access model lets the caller see,
	// nil when it narrows nothing.
	rows *accessRows

	// byKey buckets groups by GroupValue.Key while they accumulate; the order
	// the surface promises is imposed once, at the end.
	byKey map[string]int
	out   []*rowGroup
	// heaps hold each group's best rows while a SCAN accumulates, one per entry
	// of out. An index walk visits a group's rows in order and appends them
	// directly, so it leaves these nil.
	heaps []*topK
}

func (r *rowGroupRead) reset() {
	r.out, r.heaps, r.byKey = nil, nil, map[string]int{}
}

// groupRows answers one grouped ROW read against a read snapshot: the groups of
// the match set in ascending value order, each with its first `per` rows in
// `order`, plus whether the limit left any group out.
//
// It is groupCounts with rows kept instead of counted, and inherits its rules:
// groups are the ones the rows produce, a repeated grouping path puts a row in
// one group per element, and an index serves the read only when it can produce
// each group's head in order — otherwise every visible row is read, bucketed
// and sorted, at a cost of O(entity).
//
// Nothing here takes the counted read's `filtered` flag: the answer IS the
// rows, so every one of them is read and offered to `visible` whatever the plan
// was, where a count is the one thing an index entry could have stood in for.
func groupRows(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	group *query.Group,
	order *query.Order,
	per int,
	limit int,
	counted bool,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
	limits Limits,
) (groups []*rowGroup, more bool, err error) {
	r := &rowGroupRead{
		tx: tx, info: info, group: group, order: order, per: per, limit: limit,
		counted: counted, visible: visible, emitter: emitter, limits: limits,
		byKey: map[string]int{},
	}
	if r.rows, err = emitter.accessRowsOf(tx, info); err != nil {
		return nil, false, err
	}
	served, err := r.byIndex(predicate)
	if err != nil {
		return nil, false, err
	}
	if !served {
		r.reset()
		if err := r.rows.scan(tx, info.name, func(row storage.Row) error {
			return r.considerRow(row, predicate)
		}); err != nil {
			return nil, false, err
		}
		r.finishScan()
	}
	if !r.counted {
		for _, g := range r.out {
			// The scan counts a group for free and the index walk stops one row
			// past the page, so both hold a number the caller did not ask for and
			// only one of them is the truth.
			g.total = 0
		}
	}
	// An index walk visits groups in order and stops one past the limit, so its
	// accumulated set is already ordered and at most one group too long — the
	// same shape the sorted scan produces.
	if limit > 0 && len(r.out) > limit {
		return r.out[:limit], true, nil
	}
	return r.out, false, nil
}

// considerRow offers one scanned row to every group it belongs to, under the
// same visibility and decode rules queryRows applies.
//
// A repeated grouping path puts a row in one group per DISTINCT element, for
// the reason groupRead.considerRow collapses repeats: a row listing the same
// label twice occupies one index entry, and the two paths have to agree on that
// row's contribution.
func (r *rowGroupRead) considerRow(row storage.Row, pred *query.Compiled) error {
	ok, err := r.visible(row)
	if err != nil || !ok {
		return err
	}
	decoded, err := decodeMatchable(r.emitter, r.info, row)
	if err != nil || decoded == nil {
		return err
	}
	if !pred.Eval(decoded.ProtoReflect()) {
		return nil
	}
	seen := map[string]bool{}
	for _, gv := range r.group.Values(decoded.ProtoReflect()) {
		key := gv.Key()
		if seen[key] {
			continue
		}
		seen[key] = true
		r.offer(gv, qrow{row: row, msg: decoded})
	}
	return nil
}

// offer adds one row to one group's candidates. The heap keeps the `per` rows
// that sort first, so a scan holds groups × per rows however large the groups
// are — the memory a top-N read is entitled to.
func (r *rowGroupRead) offer(v query.GroupValue, cand qrow) {
	key := v.Key()
	i, held := r.byKey[key]
	if !held {
		i = len(r.out)
		r.byKey[key] = i
		r.out = append(r.out, &rowGroup{value: v})
		r.heaps = append(r.heaps, &topK{order: r.order, cap: r.per, unbounded: r.per <= 0})
	}
	g := r.out[i]
	g.total++
	if r.per > 0 && g.total > uint64(r.per) {
		g.hasMore = true
	}
	r.heaps[i].offer(cand)
}

// finishScan turns the scan's heaps into the answer: each group's kept rows in
// the requested order, and the groups themselves in the order the surface
// promises.
func (r *rowGroupRead) finishScan() {
	for i, g := range r.out {
		rows := r.heaps[i].rows
		sort.SliceStable(rows, func(a, b int) bool {
			return r.order.Less(
				rows[a].msg.ProtoReflect(), rows[b].msg.ProtoReflect(),
				rows[a].row.Key, rows[b].row.Key,
			)
		})
		g.rows = rows
	}
	sort.Slice(r.out, func(i, j int) bool {
		return query.CompareGroupValues(r.out[i].value, r.out[j].value) < 0
	})
	r.heaps = nil
}

// byIndex answers the read from a secondary index when one partitions on the
// grouping path AND stores each group's rows in the requested order, reporting
// whether it did.
//
// Every decline leaves the caller exactly where it started: a group is appended
// only once its walk is complete, and a decline throws away what it accumulated
// rather than handing the scan a half-walked answer to add to.
func (r *rowGroupRead) byIndex(predicate *query.Compiled) (bool, error) {
	if r.limits.DisableIndexes || len(r.info.indexes) == 0 {
		return false, nil
	}
	candidates := make([]query.Index, 0, len(r.info.indexes))
	for i := range r.info.indexes {
		ready, err := r.info.indexes[i].ready(r.tx, r.info.name)
		if err != nil {
			return false, err
		}
		if ready {
			candidates = append(candidates, query.Index{
				Name:    r.info.indexes[i].name,
				Encoder: r.info.indexes[i].encoder,
			})
		}
	}
	plan := query.PlanGroupRows(candidates, predicate, r.group, r.order)
	if plan == nil {
		return false, nil
	}
	// The residual is what the range does not already guarantee. A nil one means
	// the range is exact, and match-everything is the right predicate to
	// re-check with — not the original, which would re-do the range's work.
	residual := plan.Residual
	if residual == nil {
		residual = &query.Compiled{}
	}

	order := storage.ScanAsc
	if plan.Desc {
		// A descending partition component stores its values inverted, so reading
		// the range backward is what yields groups in ascending value order. The
		// rows INSIDE a group are read forward either way: that is the order the
		// plan promised.
		order = storage.ScanDesc
	}
	wantGroups := 0
	if r.limit > 0 {
		wantGroups = r.limit + 1
	}
	walked := 0
	budget := r.limits.maxIndexScanCandidates()
	lo, hi := plan.Range.Lo, plan.Range.Hi
	for wantGroups == 0 || len(r.out) < wantGroups {
		entry, err := seekEntry(r.tx, r.info.name, plan.Index, lo, hi, order)
		if err != nil {
			return false, err
		}
		if entry == nil {
			return true, nil
		}
		prefix, err := plan.GroupPrefix(entry)
		if err != nil {
			return false, err
		}
		next := query.UpperBound(prefix)
		g, err := r.walkRows(plan, residual, prefix,
			maxBound(prefix, lo), minBound(next, hi), &walked, budget)
		if err != nil {
			if errors.Is(err, errIndexBudget) || errors.Is(err, errGroupUnnamed) {
				r.reset()
				return false, nil
			}
			return false, err
		}
		// Appended rather than bucketed: the walk visits each group's sub-range
		// once, so two of them can never be the same group.
		if g != nil {
			r.out = append(r.out, g)
		}
		if order == storage.ScanAsc {
			if next == nil {
				// The group's prefix is all 0xff, so nothing sorts above it.
				return true, nil
			}
			lo = next
		} else {
			hi = prefix
		}
	}
	return true, nil
}

// walkRows reads one group's head over [lo, hi) — whose entries all begin with
// `prefix` — in index order, which the plan promised is the requested order.
//
// It returns nil for a group holding no matching row, which is how a residual
// that rejects everything in a range makes a group simply not exist, exactly as
// it does for the scan.
func (r *rowGroupRead) walkRows(
	plan *query.GroupPlan,
	residual *query.Compiled,
	prefix, lo, hi []byte,
	walked *int,
	budget int,
) (*rowGroup, error) {
	g := &rowGroup{}
	named := false
	// want is how many matching rows the walk needs before it can stop: the page
	// plus the one row that answers hasMore. A read that reports a total has no
	// such bound — it owes a number over the whole group.
	want := 0
	if r.per > 0 && !r.counted {
		want = r.per + 1
	}
	kept := 0
	consider := func(row storage.Row) error {
		if row.Deleted {
			// Entries are cleared on delete, so this is an entry that outlived its
			// row. Skipping keeps the answer identical to the scan's.
			return nil
		}
		ok, err := r.visible(row)
		if err != nil || !ok {
			return err
		}
		decoded, err := decodeMatchable(r.emitter, r.info, row)
		if err != nil || decoded == nil {
			return err
		}
		if !residual.Eval(decoded.ProtoReflect()) {
			return nil
		}
		if !named {
			if gv, ok := plan.ValueOf(r.group, decoded.ProtoReflect(), prefix); ok {
				g.value, named = gv, true
			}
		}
		kept++
		g.total++
		if r.per <= 0 || len(g.rows) < r.per {
			g.rows = append(g.rows, qrow{row: row, msg: decoded})
		} else {
			g.hasMore = true
		}
		if want > 0 && kept >= want {
			return errGroupFilled
		}
		return nil
	}

	batch := &rowBatch{tx: r.tx, entity: r.info.name, size: maxRowBatch, emit: consider}
	err := r.tx.ScanIndex(r.info.name, plan.Index, lo, hi, 0, storage.ScanAsc,
		func(_ []byte, rowKey string) error {
			*walked++
			if *walked > budget {
				return errIndexBudget
			}
			if want > 0 {
				// kept only moves during a flush, and a flush empties the batch, so
				// this is reached with nothing buffered — which is what makes sizing
				// the batch to what is left drop no row the group still wanted.
				room := want - kept
				if room <= 0 {
					return errGroupFilled
				}
				batch.size = min(room, maxRowBatch)
			}
			if !r.rows.admits(rowKey) {
				return nil
			}
			// Each entry of a group names a distinct row: PlanGroupRows declines
			// an index where a component past the partition fans out, so nothing
			// here has to dedupe.
			return batch.add(rowKey)
		})
	switch {
	case errors.Is(err, errGroupFilled):
	case err != nil:
		return nil, err
	default:
		if err := batch.flush(); err != nil && !errors.Is(err, errGroupFilled) {
			return nil, err
		}
	}
	if g.total == 0 {
		return nil, nil
	}
	if !named {
		// The group holds rows and none of them describes it, so the entries no
		// longer match the rows. The scan is the only thing that can say what the
		// groups really are.
		return nil, errGroupUnnamed
	}
	return g, nil
}

// seekEntry is the first entry of [lo, hi) in the given direction, or nil when
// the range is empty. It is what turns an ordered index into a skip-scan: the
// entry it returns names the next group, and the caller then resumes past that
// group's own bound.
func seekEntry(
	tx storage.ReadTx,
	entity, index string,
	lo, hi []byte,
	order storage.ScanOrder,
) ([]byte, error) {
	var out []byte
	err := tx.ScanIndex(entity, index, lo, hi, 1, order, func(entry []byte, _ string) error {
		out = append([]byte(nil), entry...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
