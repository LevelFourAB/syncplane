package upstream

import "fmt"

// State is where a workspace's sync with the origin stands.
type State int

const (
	// StateConnecting is a workspace the link has not reached the origin for
	// since it started.
	StateConnecting State = iota
	// StateBootstrapping is a workspace whose rows are being replaced with a
	// snapshot of the origin.
	StateBootstrapping
	// StateLive is a workspace that is caught up with the origin and receives
	// its changes as they happen.
	StateLive
	// StateOffline is a workspace whose last call to the origin failed. The
	// link retries with backoff; local writes queue meanwhile.
	StateOffline
	// StateNoAccess is a workspace in which the origin grants the link's
	// caller no permission yet. The link asks again with backoff; local
	// writes queue meanwhile.
	StateNoAccess
	// StateRevoked is a workspace the origin refused for good. The link no
	// longer syncs it.
	StateRevoked
	// StateFailed is a workspace the link cannot sync: its link state does
	// not load, or it was deleted from the local store. Status.LastError says
	// why.
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateBootstrapping:
		return "bootstrapping"
	case StateLive:
		return "live"
	case StateOffline:
		return "offline"
	case StateNoAccess:
		return "no_access"
	case StateRevoked:
		return "revoked"
	case StateFailed:
		return "failed"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// Status is the sync health of one linked workspace.
type Status struct {
	Workspace string
	State     State
	// Pending is how many local writes the origin has not confirmed through
	// a pull yet, those it has applied but not sent back included. A local
	// write counts from when the link notices it, shortly after it commits.
	Pending int
	// OriginSeq is the origin sequence the workspace's confirmed rows are
	// current through.
	OriginSeq uint64
	// OriginHeadSeq is the origin's newest sequence for the workspace, as of
	// the last page or snapshot the link received in this process. 0 means
	// unknown: nothing received yet, or a page from an origin that does not
	// report it.
	// See Behind.
	OriginHeadSeq uint64
	// SchemaRevision is the schema revision the origin advertised last, ""
	// when it advertises none.
	SchemaRevision string
	// SchemaSkew is the standing difference between the local engine's
	// schema revision and the origin's, nil when they agree, when either
	// side advertises none, or when Config.OnSchemaSkew answered SkewIgnore.
	SchemaSkew *SchemaSkew
	// LastError is the last failure, nil after the next success.
	LastError error
	// Bootstrap is how far the snapshot download has got, nil while no
	// bootstrap runs. It is set for the whole of a bootstrap attempt, also
	// while State is StateOffline after a failed call, and goes back to nil
	// when the attempt ends, completed or failed.
	Bootstrap *BootstrapProgress
}

// Behind returns how many sequences of origin writes the confirmed rows trail
// the origin by, as of the last page the link received: OriginHeadSeq minus
// OriginSeq, and 0 when the workspace is up to date or OriginHeadSeq is
// unknown. It counts writes, not rows: one write can change many rows, and a
// write the link's caller may not read still takes a sequence. While the
// origin cannot be reached, it does not grow.
func (s Status) Behind() uint64 {
	if s.OriginHeadSeq <= s.OriginSeq {
		return 0
	}
	return s.OriginHeadSeq - s.OriginSeq
}

// BootstrapProgress is how far a workspace's snapshot download has got.
//
// The numbers survive a resumed snapshot and a restart of the process: an
// attempt that continues an interrupted snapshot starts from what the earlier
// attempts applied. A snapshot the origin starts over (it refused the resume)
// starts from zero.
//
// RowsApplied / RowsEstimated is at most 1 and never goes down during one
// snapshot, so it can drive a progress bar as it is.
type BootstrapProgress struct {
	// RowsApplied is the number of snapshot rows applied to the local store.
	RowsApplied uint64
	// RowsEstimated is the expected size of the snapshot in rows, 0 when the
	// origin gives no estimate. The origin gives none when read permissions or
	// a narrowing subscription hide rows of the shipped entities. Rows written
	// during the download can make it end above or below the estimate. It is
	// never below RowsApplied.
	RowsEstimated uint64
	// EntitiesDone is the number of entities whose rows are all applied.
	EntitiesDone int
	// EntitiesTotal is the number of entities the snapshot ships, 0 until the
	// origin announces them.
	EntitiesTotal int
}

// SchemaSkew is a difference between the schema revision of the local engine
// (server.WithSchemaRevision) and the one the origin advertises.
type SchemaSkew struct {
	Local  string
	Origin string
	// Action is what Config.OnSchemaSkew answered for this pair of
	// revisions.
	Action SkewAction
}

// SkewAction is how a workspace reacts to a schema skew.
type SkewAction int

const (
	// SkewWarn reports the skew in Status and keeps syncing.
	SkewWarn SkewAction = iota
	// SkewIgnore keeps syncing and reports nothing.
	SkewIgnore
	// SkewBlock reports the skew in Status and stops forwarding local writes
	// to the origin. Pulls continue. Local writes queue and are forwarded
	// once the revisions agree, or once Config.OnSchemaSkew answers another
	// action for a new pair. A write-through write fails with ErrSchemaSkew
	// and commits nothing.
	SkewBlock
)

func (a SkewAction) String() string {
	switch a {
	case SkewWarn:
		return "warn"
	case SkewIgnore:
		return "ignore"
	case SkewBlock:
		return "block"
	}
	return fmt.Sprintf("SkewAction(%d)", int(a))
}

// status is the mutable status of a workspace runtime. Its fields are
// guarded by workspace.vmu.
type status struct {
	bootstrapping bool
	live          bool
	offline       bool
	noAccess      bool
	revoked       bool
	failed        bool
	// failErr is why the workspace failed. It is kept apart from lastErr
	// because other goroutines still report into lastErr after the failure,
	// and a failure is terminal.
	failErr   error
	originSeq uint64
	// originHead is the origin's head as of the last page, 0 when unknown.
	originHead uint64
	revision  string
	lastErr   error
	// skew is the pair of revisions Config.OnSchemaSkew decided last and its
	// answer, nil when the revisions agree. It stays until the worker decides
	// again, so a block holds while a new pair waits for its answer.
	skew *SchemaSkew
	// boot is the progress of the running bootstrap, nil when none runs.
	boot *BootstrapProgress
}

func (s *status) state() State {
	switch {
	case s.failed:
		return StateFailed
	case s.revoked:
		return StateRevoked
	case s.noAccess:
		return StateNoAccess
	case s.offline:
		return StateOffline
	case s.bootstrapping:
		return StateBootstrapping
	case s.live:
		return StateLive
	}
	return StateConnecting
}
