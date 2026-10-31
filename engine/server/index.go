package server

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/search"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Secondary indexes on the write path.
//
// An index is a derived, disposable, workspace- and entity-scoped mapping from
// an encoded sort key to a row key. Three properties make the rest of the
// design simple, and every one of them is load-bearing:
//
//   - It is never on the wire. No request or response names an index, so
//     adding, removing or rebuilding one is invisible to every client.
//   - Losing one costs a rebuild, never correctness. Entries are recomputable
//     from the rows at any time.
//   - Using one is a pure OPTIMIZATION. Every read that consults an index has
//     a legal scan fallback, and the two must produce identical results — which
//     is what lets a read fall back the moment an index is not ready, and what
//     makes "run the suite with indexes off" the primary correctness test
//     rather than a set of index-specific assertions.
//
// Maintenance is synchronous and in the same transaction as the row: entries
// commit with the write that produced them or roll back with it, so a reader
// inside the store never sees a row and its entries disagree. That costs every
// row write one replace-set call — the write amplification the index buys its
// read speed with.
//
// Entries exist for LIVE rows only. A delete clears them, which is what keeps
// a range scan from having to re-check whether each hit is a tombstone. A
// declared index holds every live row; a reference index is sparse and holds
// only the rows that point somewhere (see buildRefIndexes).

// buildRefIndexes derives one index per reference field of the entity, keyed
// by the target key the row stores.
//
// A follow spec resolves reverse edges ("every issue whose project is this
// one") on every pull, and without an index that is a full scan and decode of
// the holder entity per holder row. The engine does not need to be told to
// index that: it already knows which fields are references and which of the
// target's key fields a row stores for each.
//
// A composite-key reference yields a multi-component index, in the target's
// declared key-field order, so a probe on the whole key is a prefix probe.
//
// A to-many reference stores a LIST of target keys, so its row takes one
// position per element — the components fan out over the list together, which
// keeps a composite key's parts belonging to the same element.
//
// A polymorphic reference gets one index per case, keyed by that case's target
// key: a reverse hop asks about one target entity, and the case pointing at
// it is the only one whose entries can answer.
//
// Every reference index is SPARSE: a row whose reference is unset, or whose
// to-many list is empty, has no entry. Each reverse hop probes for one target
// key, and a row that points nowhere is never in that range, so storing it
// costs an entry per row for nothing. In an entity where most rows leave a
// reference empty, that is most of the index. The planner still reads the
// index for a query that bounds the reference to set values, and scans for
// one that does not (isNotSet, or an ordering by the reference alone).
func buildRefIndexes(info *entityInfo) error {
	desc := info.msgType.Descriptor()
	for _, rf := range info.refFields {
		if rf.fd.IsMap() {
			continue
		}
		comps := make([]query.IndexComponent, 0, len(rf.keyFields))
		for _, kf := range rf.keyFields {
			path := append([]string{string(rf.fd.Name())}, rf.ref.KeyPathNames(kf)...)
			c, err := query.CompileIndexComponent(desc, path, false)
			if err != nil {
				return fmt.Errorf("reference field %q: %w", rf.fd.Name(), err)
			}
			comps = append(comps, c)
		}
		enc, err := query.NewSparseIndexEncoder(comps)
		if err != nil {
			return fmt.Errorf("reference field %q: %w", rf.fd.Name(), err)
		}
		info.indexes = append(info.indexes, compiledIndex{
			name:     refIndexName(rf.ref),
			specHash: enc.SpecHash(),
			encoder:  enc,
		})
	}
	return nil
}

// buildDeclaredIndexes desugars the two declaration surfaces into the same
// compiledIndex the engine already maintains for references.
//
// There are two surfaces because they answer different questions. A field-level
// `indexed` mark says "this field is worth looking up by" — it is the one a
// schema author can hold in their head, and it does not couple the schema to
// any particular query. An entity-level `index` names a specific SHAPE, which
// is what a filter-plus-a-different-sort-order needs to be O(limit): a sorted
// key-value store has no posting lists to intersect, so a single-field index
// serving that query walks the sort order re-checking the predicate per
// candidate. Since a field mark IS a one-component spec, supporting both costs
// this desugaring step rather than a second system.
//
// Order matters and is fixed here: field marks first (in field order), then
// declared indexes (in declaration order), so the compiled list is stable
// across runs and a spec fingerprint does not move for free.
func buildDeclaredIndexes(info *entityInfo, eo *syncplanev1.EntityOptions) error {
	desc := info.msgType.Descriptor()
	seen := map[string]bool{}
	for i := range info.indexes {
		seen[info.indexes[i].name] = true
	}

	add := func(name string, comps []query.IndexComponent) error {
		if seen[name] {
			return fmt.Errorf("index %q is declared twice", name)
		}
		enc, err := query.NewIndexEncoder(comps)
		if err != nil {
			return fmt.Errorf("index %q: %w", name, err)
		}
		seen[name] = true
		info.indexes = append(info.indexes, compiledIndex{
			name:     name,
			specHash: query.SpecHash(comps),
			encoder:  enc,
		})
		return nil
	}

	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !fieldBehaviorOf(fd).GetIndexed() {
			continue
		}
		c, err := query.CompileIndexComponent(desc, []string{string(fd.Name())}, false)
		if err != nil {
			return fmt.Errorf("field %q is marked indexed: %w", fd.Name(), err)
		}
		if err := add(fieldIndexName(fd), []query.IndexComponent{c}); err != nil {
			return err
		}
	}

	for _, spec := range eo.GetIndex() {
		name := spec.GetName()
		if name == "" {
			return fmt.Errorf("an index declares no name")
		}
		if strings.HasPrefix(name, reservedIndexPrefix) {
			return fmt.Errorf("index %q: the %q prefix is reserved for indexes the engine derives",
				name, reservedIndexPrefix)
		}
		if err := naming.ValidateEntityName(name); err != nil {
			// Index names share the entity-name rules rather than inventing
			// their own: both end up as opaque identifiers in storage keys on
			// both sides, so the same characters have to be safe.
			return fmt.Errorf("index %q: %w", name, err)
		}
		if len(spec.GetKey()) == 0 {
			return fmt.Errorf("index %q declares no key components", name)
		}
		comps := make([]query.IndexComponent, 0, len(spec.GetKey()))
		for _, k := range spec.GetKey() {
			path := strings.Split(k.GetPath(), ".")
			c, err := query.CompileIndexComponent(desc, path, k.GetDesc())
			if err != nil {
				return fmt.Errorf("index %q: %w", name, err)
			}
			comps = append(comps, c)
		}
		if err := add(name, comps); err != nil {
			return err
		}
	}
	return nil
}

// fieldIndexName is the index a field-level `indexed` mark desugars to. It is
// derived rather than named by the author, so two schemas that mark the same
// field agree, and it lives in the reserved namespace because nothing outside
// the desugaring may claim it.
func fieldIndexName(fd protoreflect.FieldDescriptor) string {
	return fieldIndexPrefix + string(fd.Name())
}

// indexByName returns the entity's index with that name, or nil.
func (info *entityInfo) indexByName(name string) *compiledIndex {
	for i := range info.indexes {
		if info.indexes[i].name == name {
			return &info.indexes[i]
		}
	}
	return nil
}

// updateRowIndexes refreshes every index entry of one row from its current
// value. Called wherever a row is written — PUT, PATCH, and the text
// compaction rewrite — immediately next to the text-floor bookkeeping, which
// is where same-transaction derived state belongs.
//
// The whole set is replaced rather than diffed: the caller rarely has the
// row's PREVIOUS value in hand (a PATCH merges into stored bytes; compaction
// rewrites in place), so a diff would put a read and a decode on the write
// path for a saving no index can repay.
//
// An index over a repeated field contributes one entry per element, so the
// count is no longer bounded by the number of declared indexes — it is bounded
// by the data, which is what makes the per-row limit below load-bearing rather
// than a formality.
func (info *entityInfo) updateRowIndexes(tx storage.WriteTx, limits Limits, key string, msg proto.Message) error {
	if len(info.indexes) == 0 && info.search == nil {
		return nil
	}
	refl := msg.ProtoReflect()
	entries := make([]storage.IndexEntry, 0, len(info.indexes))
	for i := range info.indexes {
		for _, k := range info.indexes[i].encoder.Encode(refl, key) {
			entries = append(entries, storage.IndexEntry{Index: info.indexes[i].name, Key: k})
		}
	}
	if info.search != nil {
		// Bounded on its own by the per-row term cap (an oversized document
		// stores the overflow marker alone), so the shared limit below stays
		// about value indexes plus a constant.
		for _, k := range info.search.decl.SearchEntries(refl, key) {
			entries = append(entries, storage.IndexEntry{Index: info.search.name, Key: k})
		}
	}
	if max := limits.maxIndexEntriesPerRow(); len(entries) > max {
		// A row cannot be allowed to write an unbounded number of entries: the
		// count is what an index costs on every single write of that row, and
		// with a fan-out index it grows with the list.
		//
		// The write FAILS rather than storing a truncated set. A partial index
		// disagrees with the scan it is supposed to be indistinguishable from,
		// which would turn a size limit into wrong query results — so the limit
		// surfaces where it can be acted on, as a rejected write naming the row.
		// A rejection, not a plain error: the same write fails the same way
		// every time, and a push would otherwise retry it until it is
		// dead-lettered.
		return Reject(CodeInvalidArgument, fmt.Sprintf("entity %q row %q: %d index entries exceeds the limit of %d; a repeated field indexed by this entity has too many elements, or too many indexes are declared",
			info.name, key, len(entries), max))
	}
	// Refused for the same reason, and on every store: the bound is what
	// Postgres can hold, and a store that holds more would accept a value
	// that a move to Postgres then breaks.
	for _, e := range entries {
		if len(e.Key) > storage.MaxIndexEntryBytes {
			return Reject(CodeInvalidArgument, fmt.Sprintf("entity %q row %q: an entry of index %q is %d bytes, the limit is %d; a value this index holds is too long",
				info.name, key, e.Index, len(e.Key), storage.MaxIndexEntryBytes))
		}
	}
	return tx.PutIndexEntries(info.name, key, entries)
}

// maskTouchesIndexes reports whether any path of a field mask could change any
// index entry of a row of this entity. It is the guard that lets a PATCH skip
// updateRowIndexes: a mask naming only fields no index reads produces the exact
// same entry set it already has, and rewriting it is pure write amplification.
//
// The cost of asking is a path comparison per (mask path, component) pair
// against descriptors resolved at registration — no decode, no read, no encode
// — against the cost of a delete-and-reinsert of every entry the row occupies.
// That trade is what makes an EAV-shaped index affordable: an entity with 20
// indexed custom fields writes ~47 entries per row, and an edit to its title
// touches none of them.
//
// It is deliberately CONSERVATIVE (see IndexComponent.MaskTouches): every case
// it cannot decide without looking at the data answers "yes, rewrite". A false
// positive costs the rewrite that would have happened anyway; a false negative
// would leave an index disagreeing with the scan it must be indistinguishable
// from, which is a wrong query result rather than a slow one.
//
// PUT does not get this guard, and cannot: it replaces the whole row, and the
// entries it must replace are those of a PREVIOUS value the write path does not
// hold. That asymmetry is fine in practice because the TS client derives a mask
// from a diff, so ordinary edits arrive as PATCH.
func (info *entityInfo) maskTouchesIndexes(paths []string) bool {
	if len(info.indexes) == 0 && info.search == nil {
		return false
	}
	for _, path := range paths {
		segs := lww.SplitPath(path)
		for i := range info.indexes {
			for _, c := range info.indexes[i].encoder.Components() {
				if c.MaskTouches(segs) {
					return true
				}
			}
		}
		if info.search != nil && info.search.decl.MaskTouches(segs) {
			return true
		}
	}
	return false
}

// clearRowIndexes drops every index entry of a row. Called when the row is
// deleted: index entries exist only for live rows, so a tombstone holds none.
//
// It takes nothing but the key, which is what lets the delete paths call it —
// neither the push DELETE nor EntityTx.Delete has a decoded message, and
// reading one to compute which entries to remove would put a read on the
// delete path.
func (info *entityInfo) clearRowIndexes(tx storage.WriteTx, key string) error {
	if len(info.indexes) == 0 && info.search == nil {
		return nil
	}
	return tx.DeleteIndexEntries(info.name, key)
}

// indexReady reports whether an index's stored entries cover the whole entity
// under the CURRENT declaration. Both halves matter: a fingerprint mismatch
// means the entries were built from a spec that no longer applies (the schema
// evolved, or the encoding version moved), and an unfinished build means they
// cover only part of the entity. Either way a read must scan instead.
func (ci *compiledIndex) ready(tx storage.ReadTx, entity string) (bool, error) {
	return indexReadyState(tx, entity, ci.name, ci.specHash)
}

func (si *searchIndex) ready(tx storage.ReadTx, entity string) (bool, error) {
	return indexReadyState(tx, entity, si.name, si.specHash)
}

func indexReadyState(tx storage.ReadTx, entity, name, specHash string) (bool, error) {
	hash, ready, err := tx.IndexState(entity, name)
	if err != nil {
		return false, err
	}
	return ready && hash == specHash, nil
}

// candidateSink takes the row keys an index walk names, in the order it names
// them. A walk answering a query sinks them into a rowBatch, which resolves the
// rows; a walk enumerating candidates (candidateKeysByIndex) sinks them into a
// keySink, which reads nothing.
type candidateSink interface {
	// add takes one key. A sink may buffer it, so a key added is not delivered
	// until flush returns.
	add(rowKey string) error
	// flush delivers whatever add buffered. It must run once after the last
	// add, or those keys are never delivered.
	flush() error
	// hold caps how many keys the sink buffers before delivering them. An
	// ordered walk lowers it to the room its page has left, so reading ahead
	// never resolves rows the page cannot take.
	hold(n int)
}

// keySink is the candidateSink of a walk that needs no rows: it buffers
// nothing and passes each key on as the walk names it.
type keySink func(rowKey string) error

func (k keySink) add(rowKey string) error { return k(rowKey) }
func (k keySink) flush() error            { return nil }
func (k keySink) hold(int)                {}

// rowBatch resolves row keys to rows a batch at a time, so that a walk over k
// candidates costs k/size reads instead of k. That is a rounding error on
// sqlitestore and the dominant cost on any adapter whose point read is a
// network round trip, which is why every path that discovers keys before it
// needs the rows — an index walk, a repeated forward ref, a membership diff —
// goes through this rather than reading each key as it finds it.
//
// Rows reach emit in the order the keys were added, because that order is
// load-bearing at every caller: it is the order a scan fallback yields, and
// therefore the order the two paths have to agree on.
//
// A caller that gives up mid-walk simply stops adding. Keys still buffered were
// never offered to emit, so dropping them leaves the caller exactly where it
// started — the same property the decline paths already rely on.
type rowBatch struct {
	tx     storage.ReadTx
	entity string
	// size is the buffered-key count at which add resolves. It bounds the rows
	// held at once, not the total a walk may resolve, and a caller may lower it
	// mid-walk once it knows it needs fewer.
	size int
	keys []string
	// emit receives every row that exists, tombstones included: whether a
	// tombstone is a result or a row to skip is the caller's question. A key
	// with no row at all never reaches it.
	emit func(storage.Row) error
}

// add buffers one key, resolving the batch when it is full.
func (b *rowBatch) add(key string) error {
	b.keys = append(b.keys, key)
	if len(b.keys) < b.size {
		return nil
	}
	return b.flush()
}

// flush resolves whatever is buffered and empties the batch. It must run once
// after the last add, or that batch's rows are never offered.
func (b *rowBatch) flush() error {
	if len(b.keys) == 0 {
		return nil
	}
	err := b.tx.GetRows(b.entity, b.keys, b.emit)
	b.keys = b.keys[:0]
	return err
}

// hold lowers the count add resolves at, for a walk that has learnt it needs
// fewer rows than a full batch.
func (b *rowBatch) hold(n int) { b.size = n }

// maxRowBatch caps how many rows one batch holds. It bounds the memory a walk
// over a large range needs at once; past it, more keys per read stop buying
// anything a caller notices.
const maxRowBatch = 256

// queryByIndex answers a query from a secondary index when one serves it,
// reporting whether it did. A false return is not "no rows" — it is "the
// caller must scan", which is always a legal thing to do and is what every
// decline in indexWalk means.
//
// The rows it offers are exactly the rows a scan would offer, in whatever
// order the caller's heap and sort then impose. That equivalence is the
// contract: the index changes which rows are CONSIDERED, never which match.
//
// An entry names a row key; the row itself is re-read, because the entry says
// where a row pointed when it was written and this snapshot decides whether it
// still exists. Those re-reads go through a rowBatch, so the walk costs reads
// proportional to candidates/batch rather than one per candidate. consider
// receives each live row together with the predicate it still has to satisfy —
// the part of the query the index range did not already decide.
//
// A key rows does not admit is dropped unread, so the scan a caller falls back
// to must be rows.scan for the two paths to offer the same rows.
func queryByIndex(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	order *query.Order,
	pg page,
	limits Limits,
	rows *accessRows,
	consider func(storage.Row, *query.Compiled) error,
	want func() int,
	discard func(),
) (bool, error) {
	return indexWalk(tx, info, predicate, order, pg, limits, want, discard,
		func(residual *query.Compiled) candidateSink {
			return admitSink{a: rows, candidateSink: &rowBatch{tx: tx, entity: info.name, size: maxRowBatch, emit: func(row storage.Row) error {
				if row.Deleted {
					// Entries are cleared on delete, so this means an entry
					// outlived its row. Skipping keeps the answer identical to
					// the scan's; a rebuild drops the entry.
					return nil
				}
				return consider(row, residual)
			}}}
		})
}

// candidateKeysByIndex reports every row key a predicate's index walk names,
// reading no row at all, and reports whether an index served the predicate —
// false meaning the caller must scan the entity, exactly as it means for
// queryByIndex.
//
// The keys are a SUPERSET of the predicate's matches: nothing re-checks the
// residual, because that needs the row. They also include keys whose row is a
// tombstone or gone, because an index entry can outlive the row it names. A
// caller therefore has to decide each row for itself once it reads it — which
// is what makes this cheap, and what candidateKeys relies on.
//
// Keys arrive in index-walk order, deduplicated by row key within the walk: one
// row appearing at several positions of a repeated-field index, or in several
// ranges of one plan, is reported once.
//
// A decline can come after emit has taken keys — the budget is spent mid-walk —
// so the caller must drop everything it collected: the scan it falls back to
// names those rows again.
func candidateKeysByIndex(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	limits Limits,
	emit func(rowKey string) error,
) (bool, error) {
	// No order, so no plan is Ordered and nothing consults want or the page:
	// this walk wants every candidate the index names, not a page of them.
	return indexWalk(tx, info, predicate, nil, page{}, limits,
		func() int { return math.MaxInt },
		// Nothing to hand back: a caller of this abandons the whole key set on
		// a decline and scans instead.
		func() {},
		func(*query.Compiled) candidateSink { return keySink(emit) })
}

// indexWalk plans predicate against the entity's ready indexes and sinks the
// row key of every candidate the plan names, reporting whether a plan served it
// at all.
//
// One row can be named several times — across the ranges of a plan, which the
// planner does not guarantee disjoint, and within one range, because an index
// over a repeated field holds one entry per element. The sink sees each row key
// once, because a scan visits each row once and the two must agree.
//
// sinkFor is called with the residual predicate once the plan is known, and at
// most once per walk.
//
// Two decline paths deserve naming. An index whose build has not finished
// covers part of the entity, so using it would silently drop rows. And a walk
// whose residual rejects candidate after candidate has become a scan with
// extra reads, so past MaxIndexScanCandidates it gives up and lets the caller
// do the honest thing.
//
// That second path is the one with a contract attached: it can only be reached
// AFTER the sink has delivered something, and the scan that follows offers
// those same rows again, so what the caller accumulated has to go before it
// does. That is what discard is for, and why this function calls it rather than
// trusting each caller to remember — a false return always leaves the caller
// exactly where it started, whichever decline it was.
//
// # Ordered plans
//
// A plan the planner reports Ordered yields rows already in the query's order,
// which buys two things nothing else can. The walk SEEKS to pg's cursor instead
// of starting at the range's own bound, and it STOPS as soon as want reports
// the page can take no more — so an ordered query costs O(limit) rather than
// O(range), cursor or not. want also sizes what the sink holds, so reading
// ahead never overshoots the page by more than the rows it still had room for.
//
// Both are optimizations over a comparison that stays authoritative: a seek
// bound only ever narrows, so it can admit rows the cursor excludes — the
// boundary row, and on an index it could not carry the row key into that row's
// whole tie group (see seekRange) — and the caller rejects those. The planner
// has the same relationship with the scan.
func indexWalk(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	order *query.Order,
	pg page,
	limits Limits,
	want func() int,
	discard func(),
	sinkFor func(residual *query.Compiled) candidateSink,
) (bool, error) {
	// Declining before the sink delivered anything discards nothing, so applying
	// the rule unconditionally costs a no-op and removes the question of which
	// decline this was.
	decline := func() (bool, error) {
		discard()
		return false, nil
	}
	if limits.DisableIndexes {
		return decline()
	}
	// A required match narrows candidates harder than any value range, so the
	// postings walk goes first. Its declines never offered a row (the
	// budget is spent before any candidate is resolved), so falling through to
	// the planner — and from there to the scan — needs nothing undone.
	if served, err := queryBySearch(tx, info, predicate, limits, sinkFor); err != nil || served {
		return served, err
	}
	if len(info.indexes) == 0 {
		return decline()
	}
	candidates := make([]query.Index, 0, len(info.indexes))
	for i := range info.indexes {
		ready, err := info.indexes[i].ready(tx, info.name)
		if err != nil {
			return false, err
		}
		if ready {
			candidates = append(candidates, query.Index{
				Name:    info.indexes[i].name,
				Encoder: info.indexes[i].encoder,
			})
		}
	}
	plan := query.PlanQuery(candidates, predicate, order)
	if plan == nil {
		return decline()
	}

	// The residual is what the range does not already guarantee. A nil one
	// means the range is exact, and match-everything is the right predicate to
	// re-check with — not the original, which would re-do work the range did.
	residual := plan.Residual
	if residual == nil {
		residual = &query.Compiled{}
	}

	// An `in` over a component whose values overlap nothing else is disjoint,
	// but the planner does not guarantee disjointness, and an index over a
	// repeated field holds one entry per element — so dedupe by row key.
	seenRows := make(map[string]struct{})
	seen := 0
	budget := limits.maxIndexScanCandidates()
	// A Reverse plan reads its ranges backward to supply the ordering. A
	// backward page wants the rows nearest its before bound, so an ordered walk
	// has to come at the ranges from that end instead — otherwise stopping
	// early would stop on the wrong rows.
	scanOrder := storage.ScanAsc
	if plan.Ordered && plan.Reverse != pg.backward {
		scanOrder = storage.ScanDesc
	}
	sink := sinkFor(residual)
	// The tie group of the entry walked last, copied out because an adapter
	// may reuse the entry's bytes once the callback returns.
	var lastTie []byte
	// A union plan reads one set of ranges per branch, each from its own
	// index; a single plan is one branch of itself. The row-key dedupe above
	// spans branches, so a row two branches yield is offered once.
	for _, br := range plan.Branches() {
		ranges := br.Ranges
		if plan.Ordered && pg.backward {
			ranges = slices.Clone(ranges)
			slices.Reverse(ranges)
		}
		for _, r := range ranges {
			// The sink carries across ranges, so the fill check at a boundary
			// has to see what the previous range buffered — keys still buffered
			// were never offered, and want would be answering for the page
			// before them.
			if err := sink.flush(); err != nil {
				return false, err
			}
			if plan.Ordered && want() == 0 {
				return true, nil
			}
			lo, hi, err := seekRange(plan, order, pg, r)
			if err != nil {
				return false, err
			}
			err = tx.ScanIndex(info.name, br.Index, lo, hi, 0, scanOrder, func(entry []byte, rowKey string) error {
				if plan.Ordered {
					tie, err := plan.TieGroup(entry)
					if err != nil {
						return err
					}
					// want only moves during a flush, and a flush empties the
					// sink, so this is reached with nothing buffered — which is
					// what makes stopping here drop no row the page still wanted.
					// Holding no more than what is left is the other half: the walk
					// reads ahead by a batch, so a batch larger than the page's
					// remaining room would resolve rows it must then throw away.
					room := want()
					if room == 0 {
						// A full page can still take a row of the tie group the
						// last entry belongs to (see query.Plan.TieGroup), so the
						// walk goes on one row at a time until the group ends.
						// The worst row kept belongs to the last entry's group or
						// to an earlier one, so comparing against the last entry
						// can make the walk read on further, but never stop early.
						if tie == nil || !bytes.Equal(tie, lastTie) {
							return errPageFilled
						}
						room = 1
					}
					if tie != nil {
						lastTie = append(lastTie[:0], tie...)
					}
					sink.hold(min(room, maxRowBatch))
				}
				seen++
				if seen > budget {
					return errIndexBudget
				}
				if _, dup := seenRows[rowKey]; dup {
					return nil
				}
				seenRows[rowKey] = struct{}{}
				return sink.add(rowKey)
			})
			if errors.Is(err, errPageFilled) {
				return true, nil
			}
			if errors.Is(err, errIndexBudget) {
				// Buffered keys are dropped unread: they were never offered, so
				// the scan the caller falls back to is the only thing that will
				// offer them.
				return decline()
			}
			if err != nil {
				return false, err
			}
		}
	}
	if err := sink.flush(); err != nil {
		return false, err
	}
	return true, nil
}

// queryBySearch answers a query from the entity's term-postings index when a
// REQUIRED match condition narrows it, reporting whether it did. Candidates
// come from the most selective query term's postings, narrowed by the other
// terms where that is cheaper than re-reading rows, and unioned with the
// overflow rows, whose term sets outgrew their postings; the sink receives them
// with the FULL predicate as their residual, so the index changes which rows
// are considered, never which match. A relevance ordering needs nothing from
// the walk: the caller's heap and sort rank whatever is offered, exactly as
// they rank a scan's rows.
//
// Because the candidates are only ever narrowed and never completed, dropping
// a narrowing pass is always safe: it leaves more rows to re-check, not fewer
// rows in the answer. That is what lets both passes below bound their reads
// and give up on a term whose postings are not worth what they would save.
//
// Declines — no declaration, no required match, a term-less match (which
// matches every row, so the postings narrow nothing), an unbuilt index, or a
// query whose every term blows the candidate budget — all happen before any
// row is offered, so the caller falls through with nothing to discard.
func queryBySearch(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	limits Limits,
	sinkFor func(residual *query.Compiled) candidateSink,
) (bool, error) {
	si := info.search
	if si == nil || predicate == nil {
		return false, nil
	}
	m := predicate.RequiredMatcher()
	if m == nil || len(m.Terms()) == 0 {
		return false, nil
	}
	ready, err := si.ready(tx, info.name)
	if err != nil || !ready {
		return false, err
	}

	budget := limits.maxIndexScanCandidates()
	// postings reads the row keys a range names, giving up once it has seen
	// more than cap of them. The cap rides the storage limit rather than a
	// counter in the callback so that a range the walk is not going to use
	// costs one bounded read instead of a full one.
	postings := func(lo, hi []byte, max int, keep func(rowKey string)) (bool, error) {
		seen := 0
		err := tx.ScanIndex(info.name, si.name, lo, hi, max+1, storage.ScanAsc, func(_ []byte, rowKey string) error {
			seen++
			if seen > max {
				return errIndexBudget
			}
			keep(rowKey)
			return nil
		})
		if errors.Is(err, errIndexBudget) {
			return false, nil
		}
		return err == nil, err
	}

	candidates, seeded, err := searchSeed(m.Terms(), budget, postings)
	if err != nil || candidates == nil {
		return false, err
	}

	// Narrow with the remaining terms. A row that matches carries every term,
	// so intersecting can only remove candidates — but reading a term's
	// postings to find that out is only worth it while the list is small
	// against the rows it could save re-reading, which is what the cap
	// expresses. A term that blows it is skipped rather than declined.
	for i, t := range m.Terms() {
		if i == seeded || len(candidates) == 0 {
			continue
		}
		lo, hi := query.SearchTermRange(t)
		next := make(map[string]struct{}, len(candidates))
		ok, err := postings(lo, hi, min(len(candidates)*searchNarrowRatio, budget), func(rowKey string) {
			if _, in := candidates[rowKey]; in {
				next[rowKey] = struct{}{}
			}
		})
		if err != nil {
			return false, err
		}
		if ok {
			candidates = next
		}
	}

	lo, hi := query.SearchOverflowRange()
	ok, err := postings(lo, hi, budget, func(rowKey string) {
		candidates[rowKey] = struct{}{}
	})
	if !ok || err != nil {
		return false, err
	}

	// Sorted so the batched reads visit rows in a deterministic order; the
	// caller's heap and sort make the result independent of it either way.
	keys := make([]string, 0, len(candidates))
	for k := range candidates {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sink := sinkFor(predicate)
	for _, k := range keys {
		if err := sink.add(k); err != nil {
			return false, err
		}
	}
	if err := sink.flush(); err != nil {
		return false, err
	}
	return true, nil
}

// searchSeedCaps are the candidate counts the seed pass offers a term, in
// order. They grow geometrically so that the total read stays within a
// constant factor of the winning term's posting list, and they start small
// because the term a search is worth serving from usually has a short one.
var searchSeedCaps = []int{64, 512, 4096}

// searchNarrowRatio is how many index entries a narrowing pass will read per
// candidate it could remove. A candidate costs a row read, a decode and a
// rank; an index entry costs a step of a range already positioned. The ratio
// is the conservative end of that difference, so a term wide enough to be
// skipped was never going to pay for itself.
const searchNarrowRatio = 32

// searchSeed picks the term whose postings bound the walk most tightly and
// returns the rows they name, along with the position of the term it used. A
// nil map means no term is selective enough to be worth a candidate walk at
// all and the caller must scan.
//
// Terms are offered a growing candidate cap rather than read whole, one round
// per cap: the first term to fit under a cap has the shortest list of them
// all, to within the ratio between two caps, and every other term is abandoned
// as soon as it is known not to fit. A query of common words therefore costs a
// bounded read per term before it declines, instead of reading every list to
// the budget to discover the same thing.
func searchSeed(
	terms []search.QueryTerm,
	budget int,
	postings func(lo, hi []byte, max int, keep func(rowKey string)) (bool, error),
) (map[string]struct{}, int, error) {
	caps := searchSeedCaps
	if len(terms) == 1 {
		// One term is its own most selective: there is nothing to compare it
		// against, so the rounds would only read the same list twice.
		caps = nil
	}
	// One round per cap, then a final one at the budget — which is the cap that
	// decides whether the walk happens at all, so it runs however small the
	// budget is configured to be.
	for round := 0; round <= len(caps); round++ {
		max := budget
		if round < len(caps) {
			if caps[round] >= budget {
				continue
			}
			max = caps[round]
		}
		for i, t := range terms {
			lo, hi := query.SearchTermRange(t)
			found := make(map[string]struct{})
			ok, err := postings(lo, hi, max, func(rowKey string) {
				found[rowKey] = struct{}{}
			})
			if err != nil {
				return nil, 0, err
			}
			if ok {
				return found, i, nil
			}
		}
	}
	return nil, 0, nil
}

// seekRange narrows one range of an ordered plan to the part of it the page's
// cursors can still contain. It returns r unchanged for a plan that is not
// Ordered, for a plan with several ranges (which have no shared prefix to
// compose with), for a Reverse plan, and for a page with no cursors. A cursor
// encodes the order keys in the query's directions, which on a Reverse plan
// are the opposite of the index's, so its bytes name no position there; the
// caller's page check alone applies the cursor.
//
// The bound is the plan's equality prefix followed by as much of the cursor as
// the chosen index can take:
//
//   - Plan.OrderSpansIndex — the whole entry, row-key tiebreak included, which
//     is a prefix of the boundary row's entry and of nothing else. The after
//     bound stays INCLUSIVE, so the only position it admits that the page
//     excludes is the boundary row itself; the before bound is the entry
//     exactly, which as an exclusive end leaves the boundary row outside. Both
//     cost one comparison, not one per row of the tie group.
//   - otherwise, the components alone (Order.CursorPrefix): the index carries
//     components past the order keys, and a bound running on into the row key
//     would not be a prefix of any entry in it. What that costs is the tie group
//     at the boundary, which the walk reads and consider rejects — the seek
//     narrows, the comparison decides.
func seekRange(plan *query.Plan, order *query.Order, pg page, r query.Range) (lo, hi []byte, err error) {
	lo, hi = r.Lo, r.Hi
	if !plan.Ordered || plan.Reverse || len(plan.Ranges) != 1 || plan.Prefix == nil || !pg.bounded() {
		return lo, hi, nil
	}
	bound := func(entry []byte) ([]byte, error) {
		if !plan.OrderSpansIndex {
			comps, err := order.CursorPrefix(entry)
			if err != nil {
				return nil, err
			}
			entry = comps
		}
		return append(append([]byte(nil), plan.Prefix...), entry...), nil
	}
	if pg.after != nil {
		at, err := bound(pg.after)
		if err != nil {
			return nil, nil, err
		}
		// Inclusive, because rows tying with the cursor on every order key sit
		// at or after this position and some of them sort after it.
		lo = maxBound(lo, at)
	}
	if pg.before != nil {
		at, err := bound(pg.before)
		if err != nil {
			return nil, nil, err
		}
		if plan.OrderSpansIndex {
			// The entry names one row, and an exclusive end is exactly where
			// that row stops being inside the range.
			hi = minBound(hi, at)
		} else {
			// UpperBound rather than the position itself, for the mirror reason:
			// the tie group at the boundary has to stay inside an exclusive end.
			hi = minBound(hi, query.UpperBound(at))
		}
	}
	return lo, hi, nil
}

// maxBound is the tighter of two inclusive lower bounds; nil means "no bound".
func maxBound(a, b []byte) []byte {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case bytes.Compare(a, b) < 0:
		return b
	default:
		return a
	}
}

// minBound is the tighter of two exclusive upper bounds; nil means "no bound".
func minBound(a, b []byte) []byte {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case bytes.Compare(a, b) > 0:
		return b
	default:
		return a
	}
}

// errIndexBudget unwinds a range walk that has considered more candidates than
// the limit allows. It never escapes queryByIndex: it means "fall back", not
// "fail".
var errIndexBudget = errors.New("index candidate budget exhausted")

// errPageFilled unwinds an ordered walk whose page is complete. Like
// errIndexBudget it never escapes queryByIndex, but it means the opposite: the
// index answered the query, and the rows past this point are ones the caller
// would have discarded anyway.
var errPageFilled = errors.New("query page filled")

// probeRefIndex brackets the entries of a reference index whose target key
// equals the given key-field values, in the target's declared key-field order.
//
// The bound is a PREFIX of the entry, so it matches every row holding that
// reference whatever its own row key is — which is exactly the reverse lookup
// a follow spec asks for.
func (ci *compiledIndex) probeRefIndex(values []protoreflect.Value) (lo, hi []byte, err error) {
	lo, err = ci.encoder.EncodePrefix(values)
	if err != nil {
		return nil, nil, err
	}
	return lo, query.UpperBound(lo), nil
}
