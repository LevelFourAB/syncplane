package server

import (
	"bytes"
	"container/list"
	"strings"
	"sync"

	"github.com/levelfourab/syncplane/engine/text"
)

// Keeping written text documents decoded between writes.
//
// A text field's write closure does three things: decode the stored document,
// merge the incoming write into it, re-encode. Only the merge is O(edit); the
// decode and the encode are both O(document), and the decode is the larger of
// the two — at 50 000 code points it is roughly two thirds of the closure's
// text cost. That cost is paid while the storage adapter holds write mutual
// exclusion for the whole workspace, so it is not just this row's latency, it
// is every other writer's.
//
// The decode is avoidable for the shape that matters, which is somebody typing:
// consecutive writes to one field, each starting from the document the previous
// one produced. Keeping that document decoded turns the second and every later
// keystroke into merge-and-encode.
//
// # Why a hit is always correct
//
// The encoding is canonical and injective: two documents encode to the same
// bytes exactly when they are the same document. An entry is therefore valid
// if and only if the bytes it was built from equal the bytes being merged
// into, which is a memcmp — ~1.4 µs at 50 KB against ~6 ms for the decode it
// replaces. The row identity in the key decides only WHERE to look and what to
// evict; it never decides whether a hit is right. A stale, rolled-back or
// outright wrong entry cannot corrupt a merge, because it cannot pass the
// comparison in the first place.
//
// # Why entries are taken rather than borrowed
//
// The hazard is aliasing, not staleness. Merging mutates the document in
// place, so a caller that merged into a still-registered entry would leave the
// cache claiming that bytes X decode to X-plus-an-edit. take therefore REMOVES
// the entry and hands over ownership; put registers whatever the write ended up
// producing. A write closure that fails, rejects, or is retried simply never
// puts anything back — the storage contract allows a closure to run more than
// once, and the cost of that is a miss, never a wrong answer.
//
// Write mutual exclusion is per workspace for the whole closure, so two
// closures never contend for one row's entry. The mutex here guards the map and
// the eviction list against concurrent workspaces, nothing more.

// DefaultTextCacheBytes is how much encoded text state a server keeps decoded
// when WithTextCache is not set.
//
// The budget counts ENCODED bytes; a decoded document costs several times that
// in live heap (a node, a child slice and an id per code point), so the real
// ceiling is a multiple of this number. It is deliberately small: the value of
// the cache is almost entirely in the most recent write to each actively edited
// field, so a few megabytes covers every document a workspace is being typed
// into at once, and going wider buys little.
const DefaultTextCacheBytes = 8 << 20

// textDocCache holds recently written text documents in decoded form, keyed by
// field and validated by the bytes they were built from. The zero value is not
// usable; a nil *textDocCache is, and misses everything.
type textDocCache struct {
	mu     sync.Mutex
	budget int64
	used   int64
	byKey  map[string]*list.Element
	// order is most-recently-used first; eviction pops the back.
	order *list.List
}

type textCacheEntry struct {
	key     string
	encoded []byte
	doc     *text.Doc
}

func newTextDocCache(budget int64) *textDocCache {
	if budget <= 0 {
		return nil
	}
	return &textDocCache{
		budget: budget,
		byKey:  make(map[string]*list.Element),
		order:  list.New(),
	}
}

// textCacheKey identifies one text document of one row: field is the instance
// path, so each element of a keyed list or map gets its own entry. Workspace is
// part of it because row keys are only unique within a workspace, and leaving
// it out would make two tenants writing the same key evict each other on every
// write.
func textCacheKey(workspace, entity, key, field string) string {
	var b strings.Builder
	b.Grow(len(workspace) + len(entity) + len(key) + len(field) + 3)
	// The first three cannot contain a NUL: entity names and workspace ids are
	// validated identifiers, and row keys reject control characters. An
	// instance path CAN carry one, by way of a string item key, but a forged
	// collision costs nothing: a hit is validated against the bytes it was
	// built from (see take), so a colliding key only shares an eviction slot.
	b.WriteString(workspace)
	b.WriteByte(0)
	b.WriteString(entity)
	b.WriteByte(0)
	b.WriteString(key)
	b.WriteByte(0)
	b.WriteString(field)
	return b.String()
}

// take removes the entry for key and returns its document, but only when that
// document was built from exactly base. The caller owns the returned document
// and must either put back what it becomes or drop it.
func (c *textDocCache) take(key string, base []byte) *text.Doc {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byKey[key]
	if !ok {
		return nil
	}
	entry := el.Value.(*textCacheEntry)
	// Removed whether or not the bytes match: a mismatch means this row moved
	// on without going through the cache, so the entry is dead weight.
	c.order.Remove(el)
	delete(c.byKey, key)
	c.used -= int64(len(entry.encoded))
	if !bytes.Equal(entry.encoded, base) {
		return nil
	}
	return entry.doc
}

// put records doc as the decoded form of encoded. encoded must not be mutated
// afterwards — it is retained and compared against on the next write.
func (c *textDocCache) put(key string, encoded []byte, doc *text.Doc) {
	if c == nil || doc == nil {
		return
	}
	size := int64(len(encoded))
	if size > c.budget {
		// One document larger than the whole budget would evict everything
		// else and then itself; skip it rather than thrash.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byKey[key]; ok {
		c.used -= int64(len(el.Value.(*textCacheEntry).encoded))
		c.order.Remove(el)
		delete(c.byKey, key)
	}
	c.byKey[key] = c.order.PushFront(&textCacheEntry{key: key, encoded: encoded, doc: doc})
	c.used += size
	for c.used > c.budget {
		back := c.order.Back()
		if back == nil {
			break
		}
		evicted := back.Value.(*textCacheEntry)
		c.order.Remove(back)
		delete(c.byKey, evicted.key)
		c.used -= int64(len(evicted.encoded))
	}
}

// forget drops a row's entries for the named document paths (instance paths —
// see textStatePaths). Called when the row is deleted, and when an element
// carrying a document is dropped, so a document does not sit in memory for a
// position nothing will write again.
func (c *textDocCache) forget(workspace, entity, key string, paths []string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, path := range paths {
		k := textCacheKey(workspace, entity, key, path)
		el, ok := c.byKey[k]
		if !ok {
			continue
		}
		c.used -= int64(len(el.Value.(*textCacheEntry).encoded))
		c.order.Remove(el)
		delete(c.byKey, k)
	}
}

// merge is text.MergeWithFloor with the base decode served from the cache when
// the row's last write left a usable document behind.
//
// Every case it does not handle falls through to the uncached function rather
// than being reimplemented, so the refusals — a fragment as the stored base,
// undecodable state, a write that predates the compaction floor — keep reporting
// exactly what they always did.
func (c *textDocCache) merge(cacheKey string, base, incoming []byte, floor map[string]uint64) ([]byte, error) {
	if c == nil || text.IsFragment(base) {
		return text.MergeWithFloor(base, incoming, floor)
	}
	if !text.IsFragment(incoming) && bytes.Equal(base, incoming) {
		// Nothing to merge, and nothing to cache that is not already there.
		return base, nil
	}
	doc := c.take(cacheKey, base)
	if doc == nil {
		var err error
		if doc, err = text.Decode(base, ""); err != nil {
			return text.MergeWithFloor(base, incoming, floor)
		}
	}
	if err := doc.MergeEncoded(incoming, floor); err != nil {
		// doc may hold a partial merge (see MergeEncoded); it is already out of
		// the cache, so dropping the reference is all that is required.
		return nil, err
	}
	merged := doc.Encode()
	c.put(cacheKey, merged, doc)
	return merged, nil
}

// WithTextCache sets how many bytes of encoded text state the server keeps
// decoded between writes, replacing DefaultTextCacheBytes. Zero disables the
// cache, which makes every write decode the stored document exactly as it did
// before the cache existed — a correct configuration, just a slower one.
//
// The budget bounds ENCODED bytes; live heap is a multiple of that (see
// DefaultTextCacheBytes). Nothing about correctness depends on the value: an
// entry is only ever used when its recorded bytes match the ones being merged
// into, so a smaller cache costs decodes and a larger one costs memory.
func WithTextCache(budgetBytes int64) Option {
	return func(s *Server) error { s.textCacheBytes = budgetBytes; return nil }
}
