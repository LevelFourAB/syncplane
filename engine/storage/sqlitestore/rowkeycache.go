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
type rowKeyCache = keyCache[int64]

// newRowKeyCache returns an empty rowKeyCache that holds about capacity
// mappings.
func newRowKeyCache(capacity int) *rowKeyCache {
	return newKeyCache(capacity, func(id int64) uint64 { return uint64(id) })
}

// postingNum names one row number of one posting scope.
type postingNum struct {
	scope, num int64
}

// postingKeyCache maps the row numbers of posting_nums to their row keys, so
// that ScanPostings can resolve a number without a read of that table.
//
// A committed (scope, number) pair names one row key for the life of the file
// (see postings.go), and the cache learns only committed pairs (see
// sqliteTx.learnRowKeys). A scope id is not given out again either, as
// posting_scopes is an AUTOINCREMENT table. But a number can stay in a page or
// a head after DeletePostings forgot its row, when the caller did not name
// every key the row held, and ScanPostings must then not yield the row. So a
// commit that forgot rows removes their mappings before and after it commits,
// and a read transaction adds a mapping only while no such commit is in
// progress or has happened since it began (see Store.WithWriteTx).
type postingKeyCache = keyCache[postingNum]

// newPostingKeyCache returns an empty postingKeyCache that holds about
// capacity mappings.
func newPostingKeyCache(capacity int) *postingKeyCache {
	return newKeyCache(capacity, func(p postingNum) uint64 { return uint64(p.num) })
}

// keyCache maps an id of type K to a row key.
//
// Memory is bounded by about the capacity given to newKeyCache. Each entry
// costs about 50 bytes of map slot and string header, plus the bytes of the
// row key rounded up to an allocation size class: about 100 bytes for a
// 36-byte UUID key. When a shard is full it is cleared, which is cheaper than
// an eviction order and costs only point reads until the shard has learned its
// entries again.
//
// Safe for concurrent use.
type keyCache[K comparable] struct {
	shards   [rowKeyCacheShards]keyCacheShard[K]
	shardCap int
	shardOf  func(K) uint64
}

const (
	// rowKeyCacheShards spreads the read locks of concurrent scans over
	// different cache lines.
	rowKeyCacheShards = 16
	// rowKeyCacheCap is the number of mappings a Store keeps in each of its
	// caches: about 13 MiB for UUID row keys, which holds the hot rows of the
	// indexes it serves.
	rowKeyCacheCap = 1 << 17
)

type keyCacheShard[K comparable] struct {
	mu sync.RWMutex
	m  map[K]string
	// The padding fills the shard to 128 bytes, the cache line of Apple
	// silicon, so that a read lock of one shard does not invalidate another.
	_ [96]byte
}

// newKeyCache returns an empty cache that holds about capacity mappings. A
// capacity below rowKeyCacheShards still holds one mapping per shard. shardOf
// spreads the ids over the shards.
func newKeyCache[K comparable](capacity int, shardOf func(K) uint64) *keyCache[K] {
	c := &keyCache[K]{shardCap: max(1, capacity/rowKeyCacheShards), shardOf: shardOf}
	for i := range c.shards {
		c.shards[i].m = make(map[K]string)
	}
	return c
}

func (c *keyCache[K]) shard(id K) *keyCacheShard[K] {
	return &c.shards[c.shardOf(id)%rowKeyCacheShards]
}

// get returns the row key with the given id, and false when the cache does not
// hold it.
func (c *keyCache[K]) get(id K) (string, bool) {
	s := c.shard(id)
	s.mu.RLock()
	key, ok := s.m[id]
	s.mu.RUnlock()
	return key, ok
}

// add records that id names key. The caller must know that the mapping is
// committed.
func (c *keyCache[K]) add(id K, key string) {
	c.addIf(id, key, nil)
}

// addIf is add when ok is nil or reports true. ok runs under the lock that
// remove takes, so a remove that follows a change ok observes cannot miss the
// mapping.
func (c *keyCache[K]) addIf(id K, key string, ok func() bool) {
	s := c.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok != nil && !ok() {
		return
	}
	if len(s.m) >= c.shardCap {
		clear(s.m)
	}
	s.m[id] = key
}

// remove drops the mapping of id, if the cache holds one.
func (c *keyCache[K]) remove(id K) {
	s := c.shard(id)
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}
