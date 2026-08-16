package server

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Compaction floors on the write path.
//
// A text document whose tombstones have been purged (see compact.go) carries a
// floor: the version vector it held at the purge. Every write that merges into
// such a document is measured against it, because a union with state from
// before the purge would put the purged characters back. engine/text does the
// measuring — the engine's job is to load the floor and hand it to the merger
// for the document being written.
//
// Floors are keyed by INSTANCE path — the mask form of the document's
// position, item keys included — because a text field under a keyed list or
// map holds one document per element and each compacts on its own schedule.
//
// Loading costs one keyed lookup per merged document per write, and only for
// writes that merge at all: a PATCH, or a PUT onto a row that already exists.
// Documents that were never compacted (nearly all of them) read back no floor
// and take exactly the code path they did before.

// textWrite is the per-row state one write's text merges need: lazy access to
// the row's compaction floors, and the identity the decoded-document cache
// keys on. Built once per row per write closure by textWriteFor.
//
// The zero value merges without a floor and without a cache, which is exactly
// what a merge that only validates (no stored row) wants.
type textWrite struct {
	cache     *textDocCache
	tx        storage.ReadTx
	workspace string
	entity    string
	key       string
	// floors memoizes floorFor per instance path; an entry may be nil (looked
	// up, no floor).
	floors map[string]floorEntry
}

type floorEntry struct {
	vv map[string]uint64
}

// textWriteFor pairs the transaction floors are read from with the cache
// identity of the row being written. Floors load lazily, per merged document —
// which documents a write touches is not known up front once elements are in
// play.
func (info *entityInfo) textWriteFor(s *Server, ws string, tx storage.ReadTx, key string) (*textWrite, error) {
	return &textWrite{
		cache:     s.textCache,
		tx:        tx,
		workspace: ws,
		entity:    info.name,
		key:       key,
	}, nil
}

// floorFor returns the compaction floor of one document of this row, keyed by
// instance path, memoized for the write. Nil (with no error) means "never
// compacted", and callers keep the plain merge path.
func (tw *textWrite) floorFor(path string) (map[string]uint64, error) {
	if tw.tx == nil {
		return nil, nil
	}
	if e, ok := tw.floors[path]; ok {
		return e.vv, nil
	}
	raw, err := tw.tx.TextFloor(tw.entity, tw.key, path)
	if err != nil {
		return nil, err
	}
	var vv map[string]uint64
	if len(raw) > 0 {
		if vv, err = text.DecodeVV(raw); err != nil {
			return nil, fmt.Errorf("server: entity %q text field %q: compaction floor: %w",
				tw.entity, path, err)
		}
	}
	if tw.floors == nil {
		tw.floors = make(map[string]floorEntry)
	}
	tw.floors[path] = floorEntry{vv: vv}
	return vv, nil
}

// mergeField merges one text document of this row, through the decoded-
// document cache and against the document's floor. path is the instance path.
func (tw *textWrite) mergeField(path string, base, incoming []byte) ([]byte, error) {
	floor, err := tw.floorFor(path)
	if err != nil {
		return nil, err
	}
	if tw.cache == nil {
		return text.MergeWithFloor(base, incoming, floor)
	}
	return tw.cache.merge(
		textCacheKey(tw.workspace, tw.entity, tw.key, path),
		base, incoming, floor,
	)
}

// mergersFor returns the entity's mergers with every text field routed through
// this write — its floors and its cache entries. The registered map keeps the
// path-agnostic text.Merge for validation-only merges; here each text merger
// gains the instance path (Merger.FnAt), which is what keys the per-document
// state. An entity with no text fields gets the registered map unchanged, so
// the non-text path allocates nothing.
func (info *entityInfo) mergersFor(tw *textWrite) lww.Mergers {
	if len(info.textFields) == 0 {
		return info.mergers
	}
	out := make(lww.Mergers, len(info.mergers))
	for path, m := range info.mergers {
		out[path] = m
	}
	for _, tf := range info.textFields {
		m, ok := out[tf.path]
		if !ok {
			continue
		}
		m.FnAt = tw.mergeField
		out[tf.path] = m
	}
	return out
}

// hasNestedText reports whether any text field of the entity sits under a
// keyed list or map — the shape whose per-document state cannot be named
// without looking at the row.
func (info *entityInfo) hasNestedText() bool {
	for _, tf := range info.textFields {
		if tf.nested {
			return true
		}
	}
	return false
}

// textStatePaths returns the instance paths a stored row can hold per-document
// side state under — floors, cache entries, delta-log entries: every fixed
// field path, plus the instance paths msg carries. msg may be nil (an absent
// or undecodable row), which leaves only the fixed paths. Sorted, so callers
// touch storage in a deterministic order.
func (info *entityInfo) textStatePaths(msg protoreflect.Message) []string {
	var paths []string
	seen := make(map[string]bool, len(info.textFields))
	for _, tf := range info.textFields {
		if !tf.nested {
			if !seen[tf.path] {
				seen[tf.path] = true
				paths = append(paths, tf.path)
			}
			continue
		}
		if msg == nil {
			continue
		}
		for _, ti := range tf.instances(msg) {
			if !seen[ti.path] {
				seen[ti.path] = true
				paths = append(paths, ti.path)
			}
		}
	}
	sort.Strings(paths)
	return paths
}

// clearTextFloors drops the floors of the named document paths. Called when
// the row is deleted: its documents go with it, so there is nothing left to
// resurrect, and a row re-created later starts documents whose ids have
// nothing to do with the old floors'.
func (info *entityInfo) clearTextFloors(tx storage.WriteTx, key string, paths []string) error {
	for _, path := range paths {
		if err := tx.PutTextFloor(info.name, key, path, nil); err != nil {
			return err
		}
	}
	return nil
}

// dropRemovedTextInstances clears the floor and cache entry of every NESTED
// document prev holds and next does not: the element was dropped, and the
// document's lifecycle ends with the element's (a same-key element created
// later starts a new document). A floor left behind would silently swallow the
// early operations of that new document whenever a writer re-mints counters
// the old floor already covers.
//
// Fixed fields are exempt: clearing one does not end its document's identity —
// the field can be written again as the same document — so its floor stays,
// exactly as before elements could carry text.
func (info *entityInfo) dropRemovedTextInstances(
	tx storage.WriteTx,
	cache *textDocCache,
	ws, key string,
	prev map[string][]byte,
	next protoreflect.Message,
) error {
	if !info.hasNestedText() || len(prev) == 0 {
		return nil
	}
	live := make(map[string]bool)
	for _, tf := range info.textFields {
		if tf.nested {
			for _, ti := range tf.instances(next) {
				live[ti.path] = true
			}
		} else {
			// A fixed field's document identity survives a clear — the field
			// can be written again as the same document — so its floor stays,
			// exactly as before elements could carry text.
			live[tf.path] = true
		}
	}
	var removed []string
	for path := range prev {
		if !live[path] {
			removed = append(removed, path)
		}
	}
	if len(removed) == 0 {
		return nil
	}
	sort.Strings(removed)
	for _, path := range removed {
		if err := tx.PutTextFloor(info.name, key, path, nil); err != nil {
			return err
		}
	}
	cache.forget(ws, info.name, key, removed)
	return nil
}
