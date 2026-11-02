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
	// SchemaRevision is the schema revision the origin advertised last, ""
	// when it advertises none.
	SchemaRevision string
	// LastError is the last failure, nil after the next success.
	LastError error
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
	revision  string
	lastErr   error
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
