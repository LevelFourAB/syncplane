package sqlitestore

import "sync"

// rowKeyCache maps the ids of index_row_keys to their row keys, so that
// ScanIndex can resolve the id form of an entry without a read of that table.
//
// A cached mapping cannot go stale. AUTOINCREMENT gives each id one time only,
// so an id that is committed names the same row key for the life of the file.
// When the row is deleted, its entries go in the same transaction, and no
// entry can name the id again; the cached mapping is then unused, not wrong.
// The cache can learn a mapping only when the mapping is known to be
// committed (see sqliteTx.learnRowKeys): an insert that rolls back also rolls
// back sqlite_sequence, and SQLite can then give the same id to a different
// row key. A migration that drops index_row_keys also resets its sequence,
// but migrations run in Open, before the Store has a cache.
//
// Memory is bounded by rowKeyCacheCap entries. Each entry costs about 50 bytes
// of map slot and string header, plus the bytes of the row key rounded up to
// an allocation size class: about 100 bytes for a 36-byte UUID key. When a
// shard is full it is cleared, which is cheaper than an eviction order and
// costs only point reads until the shard has learned its entries again.
//
// Safe for concurrent use.
type rowKeyCache struct {
	shards   [rowKeyCacheShards]rowKeyCacheShard
	shardCap int
}

const (
	// rowKeyCacheShards spreads the read locks of concurrent scans over
	// different cache lines.
	rowKeyCacheShards = 16
	// rowKeyCacheCap is the number of mappings a Store keeps: about 13 MiB
	// for UUID row keys, which holds the hot rows of the indexes it serves.
	rowKeyCacheCap = 1 << 17
)

type rowKeyCacheShard struct {
	mu sync.RWMutex
	m  map[int64]string
	// The padding fills the shard to 128 bytes, the cache line of Apple
	// silicon, so that a read lock of one shard does not invalidate another.
	_ [96]byte
}

// newRowKeyCache returns an empty cache that holds about capacity mappings.
// A capacity below rowKeyCacheShards still holds one mapping per shard.
func newRowKeyCache(capacity int) *rowKeyCache {
	c := &rowKeyCache{shardCap: max(1, capacity/rowKeyCacheShards)}
	for i := range c.shards {
		c.shards[i].m = make(map[int64]string)
	}
	return c
}

func (c *rowKeyCache) shard(id int64) *rowKeyCacheShard {
	return &c.shards[uint64(id)%rowKeyCacheShards]
}

// get returns the row key with the given id, and false when the cache does not
// hold it.
func (c *rowKeyCache) get(id int64) (string, bool) {
	s := c.shard(id)
	s.mu.RLock()
	key, ok := s.m[id]
	s.mu.RUnlock()
	return key, ok
}

// add records that id names key. The caller must know that the mapping is
// committed.
func (c *rowKeyCache) add(id int64, key string) {
	s := c.shard(id)
	s.mu.Lock()
	if len(s.m) >= c.shardCap {
		clear(s.m)
	}
	s.m[id] = key
	s.mu.Unlock()
}
