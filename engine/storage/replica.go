package storage

import "context"

// ReadRoute reports where a ReplicaReader served a read: from the replica, or
// from the primary after a fallback. The three values are the complete set,
// stable and bounded, so a caller may record them as a telemetry attribute.
type ReadRoute string

const (
	// ReadRouteReplica means the read ran on the replica.
	ReadRouteReplica ReadRoute = "replica"
	// ReadRouteFallbackStale means the replica was behind the caller's
	// freshness floor, so the read ran on the primary.
	ReadRouteFallbackStale ReadRoute = "fallback_stale"
	// ReadRouteFallbackError means the replica failed before fn ran, so the
	// read ran on the primary.
	ReadRouteFallbackError ReadRoute = "fallback_error"
)

// ReplicaReader serves read transactions that may run on a read replica. A
// replica read is a time-shifted read — a consistent snapshot of the primary
// as of some earlier moment — so its one hazard is freshness, which callers
// bound per read with a workspace-sequence floor.
//
// Both methods share WithReadTx's contract: fn runs AT MOST ONCE over a
// consistent snapshot and is never retried. The replica-or-primary decision
// completes before fn is invoked; a replica failure after fn has started
// surfaces as an error, exactly as a primary read's failure would.
type ReplicaReader interface {
	// WithReadTxMinSeq runs fn over a snapshot whose workspace sequence is at
	// least minSeq, preferring the replica and falling back to the primary
	// when the replica is behind the floor or fails before fn runs. minSeq 0
	// accepts any staleness.
	//
	// minSeq is a routing floor, not a bound on what fn observes: a read that
	// fell back sees whatever the primary holds, which may be far past minSeq.
	// The returned ReadRoute reports where fn ran (or, on an error before fn
	// ran, where the attempt failed); it is meaningful even when err is
	// non-nil.
	WithReadTxMinSeq(ctx context.Context, workspaceID string, minSeq uint64, fn func(ReadTx) error) (ReadRoute, error)
	// WithReadTxAnchored runs fn over a snapshot at least as fresh as the
	// primary's workspace sequence at the moment of the call — read-your-writes
	// for a caller holding no sequence of its own. It costs one point read on
	// the primary before the snapshot opens.
	WithReadTxAnchored(ctx context.Context, workspaceID string, fn func(ReadTx) error) (ReadRoute, error)
}

// ReplicaProvider is the optional capability interface of adapters that can
// serve reads from a replica. Resolve it through As, so wrapping adapters stay
// transparent.
type ReplicaProvider interface {
	// ReplicaReader returns the adapter's replica-capable reader, or nil when
	// no replica is configured. Implementing the interface does not promise
	// the capability — whether a replica exists is configuration, which As
	// cannot see — so callers must check for nil.
	ReplicaReader() ReplicaReader
}
