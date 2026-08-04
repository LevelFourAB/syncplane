package server

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/query"
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
// a range scan from having to re-check whether each hit is a tombstone.

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
func buildRefIndexes(info *entityInfo) error {
	desc := info.msgType.Descriptor()
	for _, rf := range info.refFields {
		if rf.fd.IsMap() {
			continue
		}
		comps := make([]query.IndexComponent, 0, len(rf.keyFields))
		for _, kf := range rf.keyFields {
			c, err := query.CompileIndexComponent(desc, []string{string(rf.fd.Name()), string(kf.Name())}, false)
			if err != nil {
				return fmt.Errorf("reference field %q: %w", rf.fd.Name(), err)
			}
			comps = append(comps, c)
		}
		enc, err := query.NewIndexEncoder(comps)
		if err != nil {
			return fmt.Errorf("reference field %q: %w", rf.fd.Name(), err)
		}
		info.indexes = append(info.indexes, compiledIndex{
			name:     refIndexName(rf.fd),
			specHash: query.SpecHash(comps),
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
	if len(info.indexes) == 0 {
		return nil
	}
	refl := msg.ProtoReflect()
	entries := make([]storage.IndexEntry, 0, len(info.indexes))
	for i := range info.indexes {
		for _, k := range info.indexes[i].encoder.Encode(refl, key) {
			entries = append(entries, storage.IndexEntry{Index: info.indexes[i].name, Key: k})
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
		return fmt.Errorf("server: entity %q row %q: %d index entries exceeds the limit of %d; a repeated field indexed by this entity has too many elements, or too many indexes are declared",
			info.name, key, len(entries), max)
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
	if len(info.indexes) == 0 {
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
	if len(info.indexes) == 0 {
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
	hash, ready, err := tx.IndexState(entity, ci.name)
	if err != nil {
		return false, err
	}
	return ready && hash == ci.specHash, nil
}

// queryByIndex answers a query from a secondary index when one serves it,
// reporting whether it did. A false return is not "no rows" — it is "the
// caller must scan", which is always a legal thing to do and is what every
// decline path here means.
//
// The rows it offers are exactly the rows a scan would offer, in whatever
// order the caller's heap and sort then impose. That equivalence is the
// contract: the index changes which rows are CONSIDERED, never which match.
//
// Two decline paths deserve naming. An index whose build has not finished
// covers part of the entity, so using it would silently drop rows. And a walk
// whose residual rejects candidate after candidate has become a scan with
// extra reads, so past MaxIndexScanCandidates it gives up and lets the caller
// do the honest thing.
//
// That second path is the one with a contract attached: it can only be reached
// AFTER consider has been called, and the scan that follows offers those same
// rows again, so what consider accumulated has to go before it does. That is
// what discard is for, and why this function calls it rather than trusting
// each caller to remember — a false return always leaves the caller exactly
// where it started, whichever decline it was.
func queryByIndex(
	tx storage.ReadTx,
	info *entityInfo,
	predicate *query.Compiled,
	order *query.Order,
	limits Limits,
	consider func(storage.Row, *query.Compiled) error,
	discard func(),
) (bool, error) {
	// Declining before consider ever ran discards nothing, so applying the rule
	// unconditionally costs a no-op and removes the question of which decline
	// this was.
	decline := func() (bool, error) {
		discard()
		return false, nil
	}
	if limits.DisableIndexes || len(info.indexes) == 0 {
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

	// One row can appear several times: across ranges (an `in` over a component
	// whose values overlap nothing else is disjoint, but the planner does not
	// guarantee disjointness), and within one range, because an index over a
	// repeated field holds one entry per element. Dedupe by row key — a scan
	// visits each row once and the two must agree.
	seenRows := make(map[string]struct{})
	seen := 0
	budget := limits.maxIndexScanCandidates()
	for _, r := range plan.Ranges {
		err := tx.ScanIndex(info.name, plan.Index, r.Lo, r.Hi, 0, func(_ []byte, rowKey string) error {
			seen++
			if seen > budget {
				return errIndexBudget
			}
			if _, dup := seenRows[rowKey]; dup {
				return nil
			}
			seenRows[rowKey] = struct{}{}
			row, err := tx.GetRow(info.name, rowKey)
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if row.Deleted {
				// Entries are cleared on delete, so this means an entry
				// outlived its row. Skipping keeps the answer identical to the
				// scan's; a rebuild drops the entry.
				return nil
			}
			return consider(row, residual)
		})
		if errors.Is(err, errIndexBudget) {
			return decline()
		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// errIndexBudget unwinds a range walk that has considered more candidates than
// the limit allows. It never escapes queryByIndex: it means "fall back", not
// "fail".
var errIndexBudget = errors.New("index candidate budget exhausted")

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
