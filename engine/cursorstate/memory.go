package cursorstate

import (
	"container/list"
	"context"
	"fmt"
	"sync"
)

// DefaultMemoryBytes is the Memory store's budget when NewMemory is given a
// non-positive one.
const DefaultMemoryBytes = 64 << 20

// Memory is an in-process Store bounded by total entry bytes, evicting the
// least recently used entries when full. It forgets everything on restart:
// every client of the process then re-bootstraps on its next pull.
type Memory struct {
	mu      sync.Mutex
	max     int64
	used    int64
	entries map[memKey]*list.Element
	// lru holds *memEntry, most recently used at the front.
	lru *list.List
}

type memKey struct {
	ws  string
	ref Ref
}

type memEntry struct {
	key   memKey
	state []byte
}

// NewMemory returns an empty Memory store holding at most maxBytes of entry
// state (DefaultMemoryBytes when maxBytes is not positive). Size the budget
// from the deployment's shape: saturated membership state is bounded by the
// server's MaxClosureSize and MaxWindowSize at roughly the key length per row,
// and each connected client keeps a couple of entries live.
func NewMemory(maxBytes int64) *Memory {
	if maxBytes <= 0 {
		maxBytes = DefaultMemoryBytes
	}
	return &Memory{
		max:     maxBytes,
		entries: make(map[memKey]*list.Element),
		lru:     list.New(),
	}
}

func (m *Memory) Get(_ context.Context, workspaceID string, ref Ref) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.entries[memKey{ws: workspaceID, ref: ref}]
	if !ok {
		return nil, ErrNotFound
	}
	m.lru.MoveToFront(el)
	return el.Value.(*memEntry).state, nil
}

func (m *Memory) Put(_ context.Context, workspaceID string, ref Ref, state []byte) error {
	if int64(len(state)) > m.max {
		// Storing it would evict everything else and the next entry would
		// evict this one: the budget is too small for the workload.
		return fmt.Errorf("cursor state entry of %d bytes exceeds the store's %d byte budget", len(state), m.max)
	}
	key := memKey{ws: workspaceID, ref: ref}
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.entries[key]; ok {
		m.lru.MoveToFront(el)
		m.used += int64(len(state)) - int64(len(el.Value.(*memEntry).state))
		el.Value.(*memEntry).state = state
	} else {
		m.entries[key] = m.lru.PushFront(&memEntry{key: key, state: state})
		m.used += int64(len(state))
	}
	for m.used > m.max {
		el := m.lru.Back()
		entry := el.Value.(*memEntry)
		m.lru.Remove(el)
		delete(m.entries, entry.key)
		m.used -= int64(len(entry.state))
	}
	return nil
}
