package pgstore

import "sync"

// scopeCache holds the scope id of each posting index a Store has read, so a
// posting statement can bind the scope as a parameter. A bound scope lets a
// partitioned layout prune to one partition when it plans the statement;
// a scope that comes from a join can only prune when the statement runs,
// after the planner has planned and the executor has opened every partition.
//
// An entry can be stale: another node may have cleared the index, or deleted
// the workspace, and so dropped the scope. Every statement that uses an entry
// therefore travels with a read of the current scope id, and the reader
// throws its result away and reads again when the two differ. The cache is
// safe for concurrent use. The zero of *scopeCache, nil, holds nothing.
type scopeCache struct {
	mu sync.Mutex
	m  map[scopeCacheKey]int32
}

type scopeCacheKey struct{ ws, entity, index string }

// scopeCacheMax bounds the entries. A full cache starts again empty, which
// costs each posting index one round trip to read its scope id again.
const scopeCacheMax = 1 << 16

func newScopeCache() *scopeCache {
	return &scopeCache{m: map[scopeCacheKey]int32{}}
}

func (c *scopeCache) get(ws, entity, index string) (int32, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.m[scopeCacheKey{ws, entity, index}]
	return id, ok
}

func (c *scopeCache) put(ws, entity, index string, id int32) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= scopeCacheMax {
		c.m = map[scopeCacheKey]int32{}
	}
	c.m[scopeCacheKey{ws, entity, index}] = id
}

func (c *scopeCache) drop(ws, entity, index string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, scopeCacheKey{ws, entity, index})
}
