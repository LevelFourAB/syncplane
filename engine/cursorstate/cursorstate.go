// Package cursorstate stores the bulky membership tail of pull cursors — the
// subscription set and the windowed/closure key-sets — so the cursor a client
// carries is a fixed-size reference instead of a restatement of everything it
// holds.
//
// Entries are immutable — written once, never updated — and a store may evict
// any entry at any time. The server maps a reference whose entry is gone onto
// the same reset path a cursor below the tombstone horizon takes, so eviction
// costs that client one re-bootstrap: correct always, cheap only if eviction
// is rare. Choose an implementation accordingly. An in-process store
// (NewMemory) forgets everything on restart, which turns a deploy into a
// re-bootstrap of every connected client; a store backed by shared
// infrastructure keeps cursors usable across restarts and across nodes.
//
// Entries hold row keys (not values) of the workspace they are stored under.
// Deployments that treat keys as sensitive should weigh that before pointing
// the store at shared infrastructure.
package cursorstate

import (
	"context"
	"errors"
)

// ErrNotFound reports that a referenced entry is not in the store — evicted,
// expired, or never written. The server treats it as a client reset, never as
// a failure.
var ErrNotFound = errors.New("cursor state not found")

// Ref names one immutable entry. ID is the lineage a client's successive
// cursors share — minted when a snapshot starts and carried forward on every
// reissue — and Gen distinguishes the entries within it. Both are opaque
// random values; only equality and grouping by ID mean anything.
type Ref struct {
	ID  uint64
	Gen uint64
}

// Store holds cursor state entries. Implementations must be safe for
// concurrent use; calls may arrive while the server holds an open read
// snapshot on its storage adapter, so an implementation backed by the same
// database must use its own connection rather than the adapter's transaction
// API.
type Store interface {
	// Get returns the entry stored under (workspaceID, ref), or ErrNotFound.
	// Any other error is treated as transient and fails the request it served,
	// leaving the client to retry. The returned bytes are shared with the
	// store; callers must not modify them.
	//
	// A Get is a signal the entry is live: implementations that evict by
	// recency should count it as use.
	Get(ctx context.Context, workspaceID string, ref Ref) ([]byte, error)

	// Put stores an entry under (workspaceID, ref). Refs are never reused for
	// different bytes, so a Put over an existing entry is a retry of the same
	// write and may keep either copy. The store owns state after the call.
	//
	// An error means the entry cannot be relied on; the server falls back to
	// inlining the state in the cursor, so failing is always safe. A bounded
	// store should fail a Put it would have to evict immediately.
	//
	// A Put MAY prune superseded entries: entries sharing the ref's ID are one
	// client's lineage, and only the recent ones can still be presented — by a
	// retried request or a concurrently pulling tab — so an implementation can
	// drop all but the last few without waiting for a TTL.
	Put(ctx context.Context, workspaceID string, ref Ref, state []byte) error
}

// Provider is implemented by storage adapters that can supply a Store backed
// by their own database. A server constructed without WithCursorStateStore
// asks its adapter for one, so an adapter that implements Provider gives
// every server built on it a store with no configuration.
type Provider interface {
	// CursorStateStore returns the adapter-backed store. Called once, during
	// server construction; the adapter retains ownership and closes it with
	// the adapter itself.
	CursorStateStore() Store
}
