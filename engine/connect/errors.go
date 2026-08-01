package connect

import (
	"errors"
	"fmt"
	"time"
)

// The sentinels a Source wraps to tell the connector something it cannot
// infer from an opaque error.
var (
	// ErrCursorInvalid says an incremental cursor can no longer be resolved —
	// IMAP's UIDVALIDITY bumped, a history window expired, the account was
	// rebuilt. Wrap it from Incremental.Changes and the connector falls back
	// to a fresh Checkpoint plus a full List within the same pass.
	ErrCursorInvalid = errors.New("connect: source cursor is invalid")
	// ErrNeedsUserAction says the failure will not heal by retrying: a refresh
	// token expired, consent was revoked, the account was disconnected. Wrap
	// it from any Source method (or from Config.OpenSource) and the connector
	// classifies the workspace's Health as FailureUserAction and drops its
	// cached source, so the next pass rebuilds credentials from scratch.
	ErrNeedsUserAction = errors.New("connect: source needs user action")
)

// retryAfterError carries a source-dictated delay alongside the failure.
type retryAfterError struct {
	err error
	d   time.Duration
}

func (e *retryAfterError) Error() string {
	return fmt.Sprintf("%s (retry after %s)", e.err.Error(), e.d)
}

func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter wraps err with a delay the source asked for — an HTTP
// Retry-After header, a documented quota reset. The runner then waits exactly
// that long before the workspace's next attempt instead of applying its own
// exponential backoff, so a rate-limited API is obeyed rather than guessed
// at. Returns nil when err is nil.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, d: d}
}

// retryAfterOf reports the innermost source-dictated delay carried by err.
func retryAfterOf(err error) (time.Duration, bool) {
	var ra *retryAfterError
	if errors.As(err, &ra) {
		return ra.d, true
	}
	return 0, false
}

// classify buckets a pass failure for Health. Everything the source does not
// explicitly mark as needing user action is treated as transient: an
// unclassified error is far more often a timeout or a 5xx than a permanent
// one, and treating a transient failure as permanent would stop syncing a
// healthy account.
func classify(err error) FailureKind {
	switch {
	case err == nil:
		return FailureNone
	case errors.Is(err, ErrNeedsUserAction):
		return FailureUserAction
	default:
		return FailureTransient
	}
}
