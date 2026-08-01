package server

import (
	"fmt"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Compaction floors on the write path.
//
// A text field whose tombstones have been purged (see compact.go) carries a
// floor: the version vector its document held at the purge. Every write that
// merges into such a field is measured against it, because a union with state
// from before the purge would put the purged characters back. engine/text does
// the measuring — the engine's job is to load the floor and hand it to the
// merger for the row being written.
//
// Loading costs one keyed lookup per text field per write, and only for writes
// that merge at all: a PATCH, or a PUT onto a row that already exists. Rows
// that were never compacted (nearly all of them) read back no floor and take
// exactly the code path they did before.

// textFloors returns the compaction floor of each text field of one row, keyed
// by dotted field path. Fields that were never compacted are absent, so an
// empty result means "no floors" and callers can keep the plain merge path.
func (info *entityInfo) textFloors(tx storage.ReadTx, key string) (map[string]map[string]uint64, error) {
	var out map[string]map[string]uint64
	for _, tf := range info.textFields {
		raw, err := tx.TextFloor(info.name, key, tf.path)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		vv, err := text.DecodeVV(raw)
		if err != nil {
			return nil, fmt.Errorf("server: entity %q text field %q: compaction floor: %w",
				info.name, tf.path, err)
		}
		if out == nil {
			out = make(map[string]map[string]uint64, len(info.textFields))
		}
		out[tf.path] = vv
	}
	return out, nil
}

// textWrite is the per-row state one write's text merges need: the compaction
// floors read from storage, and the identity the decoded-document cache keys
// on. Built once per row per write closure by textWriteFor.
//
// The zero value merges without a floor and without a cache, which is exactly
// what an entity with no text fields wants.
type textWrite struct {
	cache     *textDocCache
	workspace string
	entity    string
	key       string
	floors    map[string]map[string]uint64
}

// textWriteFor reads the row's compaction floors and pairs them with the cache
// identity of the row being written.
func (info *entityInfo) textWriteFor(s *Server, ws string, tx storage.ReadTx, key string) (textWrite, error) {
	floors, err := info.textFloors(tx, key)
	if err != nil {
		return textWrite{}, err
	}
	return textWrite{
		cache:     s.textCache,
		workspace: ws,
		entity:    info.name,
		key:       key,
		floors:    floors,
	}, nil
}

// mergeField merges one text field of this row, through the decoded-document
// cache and against the field's floor.
func (tw textWrite) mergeField(path string, base, incoming []byte) ([]byte, error) {
	if tw.cache == nil {
		return text.MergeWithFloor(base, incoming, tw.floors[path])
	}
	return tw.cache.merge(
		textCacheKey(tw.workspace, tw.entity, tw.key, path),
		base, incoming, tw.floors[path],
	)
}

// mergersFor returns the entity's mergers with every text field routed through
// this write — its floor and its cache entry. An entity with no text fields
// gets the registered map unchanged, so the non-text path allocates nothing.
func (info *entityInfo) mergersFor(tw textWrite) lww.Mergers {
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
		path := tf.path
		m.Fn = func(base, incoming []byte) ([]byte, error) {
			return tw.mergeField(path, base, incoming)
		}
		out[path] = m
	}
	return out
}

// clearTextFloors drops every floor of a row. Called when the row is deleted:
// its documents go with it, so there is nothing left to resurrect, and a row
// re-created later starts a document whose ids have nothing to do with the old
// floor's.
func (info *entityInfo) clearTextFloors(tx storage.WriteTx, key string) error {
	for _, tf := range info.textFields {
		if err := tx.PutTextFloor(info.name, key, tf.path, nil); err != nil {
			return err
		}
	}
	return nil
}
