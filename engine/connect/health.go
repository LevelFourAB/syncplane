package connect

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"time"
)

// FailureKind buckets why a workspace's last pass failed. The distinction
// that matters to a product is "wait" versus "ask the user": a network blip
// heals on its own, an expired token never does.
type FailureKind int

const (
	// FailureNone is the zero value: the last pass succeeded.
	FailureNone FailureKind = iota
	// FailureTransient is anything unclassified — network errors, 5xx,
	// timeouts, rate limits. Retried with backoff.
	FailureTransient
	// FailureUserAction is a failure wrapping ErrNeedsUserAction: the account
	// needs re-consent, a new token, reconnecting. Still retried (the user may
	// fix it at any moment), but a product should surface it.
	FailureUserAction
)

func (k FailureKind) String() string {
	switch k {
	case FailureNone:
		return "none"
	case FailureTransient:
		return "transient"
	case FailureUserAction:
		return "user_action"
	default:
		return "unknown"
	}
}

// Health is one workspace's sync status, as of its last pass. Convergence
// failures are logged and never returned from the background loops — the
// process must not fall over because one account's token expired — so this is
// the surface a product renders and alerts on instead.
type Health struct {
	WorkspaceID string
	// LastAttempt is when the most recent pass ran; zero if none has.
	LastAttempt time.Time
	// LastSuccess is when a pass last completed without error; zero if never.
	LastSuccess time.Time
	// LastError is the most recent failure, cleared to nil by any success.
	LastError error
	// Kind classifies LastError.
	Kind FailureKind
	// ConsecutiveFailures counts failures since the last success, and is the
	// backoff exponent.
	ConsecutiveFailures int
	// NextAttempt is the earliest time the runner will retry this workspace.
	// Zero when not backing off — after a success, or when backoff is
	// disabled (Config.RetryMin < 0). Direct calls to Ingress, Reconcile and
	// Drain ignore it: the caller asked.
	NextAttempt time.Time
	// Refused lists, in record mode, the records the connector skips and
	// tries again: the codec refused them, or their locator belongs to
	// another record. A skipped record does not fail the pass. Ordered by
	// Entity, Locator and Key. A record leaves the list when it syncs, when
	// a full list no longer finds it in the store, or when it is deleted. A
	// new Connector starts with an empty list.
	Refused []RecordFailure
}

// RecordFailure is one record in Health.Refused.
type RecordFailure struct {
	// Entity is the root entity of the record type.
	Entity string
	// Key is the record key; empty for a store record new to the connector.
	Key string
	// Locator is where the store holds the record; empty for an engine
	// record the store does not hold yet.
	Locator string
	// Err is the error of the latest attempt.
	Err error
	// Failures counts the attempts that failed since the record last
	// synced.
	Failures int
}

// failureID names one record in Connector.failures: by key when the
// connector knows it, else by locator.
type failureID struct{ entity, key, locator string }

// noteFailure adds a failed attempt of a record to the workspace's Refused.
func (c *Connector) noteFailure(ws string, f RecordFailure) {
	id := failureID{entity: f.Entity, key: f.Key}
	if f.Key == "" {
		id.locator = f.Locator
	}
	c.hmu.Lock()
	defer c.hmu.Unlock()
	m := c.failures[ws]
	if m == nil {
		m = make(map[failureID]RecordFailure)
		c.failures[ws] = m
	}
	f.Failures = m[id].Failures + 1
	m[id] = f
}

// clearFailure removes the record with key, and the new store record at
// locator, from the workspace's Refused. Empty values match nothing.
func (c *Connector) clearFailure(ws, entity, key, locator string) {
	c.hmu.Lock()
	defer c.hmu.Unlock()
	m := c.failures[ws]
	if len(m) == 0 {
		return
	}
	if key != "" {
		delete(m, failureID{entity: entity, key: key})
	}
	if locator != "" {
		delete(m, failureID{entity: entity, locator: locator})
	}
}

// pruneFailures removes the new store records of entity that a full list
// did not find from the workspace's Refused.
func (c *Connector) pruneFailures(ws, entity string, listed map[string]bool) {
	c.hmu.Lock()
	defer c.hmu.Unlock()
	for id := range c.failures[ws] {
		if id.entity == entity && id.key == "" && !listed[id.locator] {
			delete(c.failures[ws], id)
		}
	}
}

// failuresLocked returns the workspace's Refused in order. Caller holds hmu.
func (c *Connector) failuresLocked(ws string) []RecordFailure {
	m := c.failures[ws]
	if len(m) == 0 {
		return nil
	}
	out := make([]RecordFailure, 0, len(m))
	for _, f := range m {
		out = append(out, f)
	}
	slices.SortFunc(out, func(a, b RecordFailure) int {
		return cmp.Or(cmp.Compare(a.Entity, b.Entity), cmp.Compare(a.Locator, b.Locator), cmp.Compare(a.Key, b.Key))
	})
	return out
}

// Health returns the workspace's last known status. The bool is false when
// the connector has never run a pass for it.
func (c *Connector) Health(workspaceID string) (Health, bool) {
	c.hmu.Lock()
	defer c.hmu.Unlock()
	h, ok := c.health[workspaceID]
	return h, ok
}

// HealthAll returns every workspace the connector has run a pass for, ordered
// by workspace id.
func (c *Connector) HealthAll() []Health {
	c.hmu.Lock()
	out := make([]Health, 0, len(c.health))
	for _, h := range c.health {
		out = append(out, h)
	}
	c.hmu.Unlock()
	slices.SortFunc(out, func(a, b Health) int { return cmp.Compare(a.WorkspaceID, b.WorkspaceID) })
	return out
}

// recordPass folds one pass's outcome into the workspace's health, computes
// the next attempt time on failure, and hands the result to Config.OnHealth.
// Every pass goes through here — runner-driven or a direct Ingress /
// Reconcile / Drain call — so a manually driven connector is just as
// observable as a started one.
func (c *Connector) recordPass(workspaceID string, err error) Health {
	now := time.Now()
	c.hmu.Lock()
	h := c.health[workspaceID]
	h.WorkspaceID = workspaceID
	h.LastAttempt = now
	if err == nil {
		h.LastSuccess = now
		h.LastError = nil
		h.Kind = FailureNone
		h.ConsecutiveFailures = 0
		h.NextAttempt = time.Time{}
	} else {
		h.LastError = err
		h.Kind = classify(err)
		h.ConsecutiveFailures++
		if d, ok := c.backoff(err, h.ConsecutiveFailures); ok {
			h.NextAttempt = now.Add(d)
		} else {
			h.NextAttempt = time.Time{}
		}
	}
	if c.rec != nil {
		h.Refused = c.failuresLocked(workspaceID)
	}
	c.health[workspaceID] = h
	c.hmu.Unlock()
	if c.onHealth != nil {
		// Outside the lock: the callback is adopter code and may call back
		// into Health/HealthAll.
		c.onHealth(h)
	}
	return h
}

// forgetHealth drops a workspace's health, so a workspace that comes back
// starts from a clean slate rather than an ancient failure. Called by Forget.
func (c *Connector) forgetHealth(workspaceID string) {
	c.hmu.Lock()
	defer c.hmu.Unlock()
	delete(c.health, workspaceID)
	delete(c.failures, workspaceID)
}

// backoff returns how long to wait before retrying a workspace whose pass
// just failed for the nth consecutive time. A delay the source dictated
// (RetryAfter) is honoured verbatim — it is information the connector does
// not have — otherwise the delay doubles per failure from RetryMin up to
// RetryMax, with ±20% jitter so a fleet of workspaces failing against the
// same API does not retry in lockstep. The bool is false when backoff is
// disabled.
func (c *Connector) backoff(err error, failures int) (time.Duration, bool) {
	if d, ok := retryAfterOf(err); ok {
		return d, true
	}
	if c.retryMin < 0 {
		return 0, false
	}
	d := c.retryMin
	for i := 1; i < failures && d < c.retryMax; i++ {
		d *= 2
	}
	if d > c.retryMax {
		d = c.retryMax
	}
	jittered := time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
	return jittered, true
}

// pace yields between chunks of an ingress pass. A cold-start import of a
// large account is a long run of writes into one workspace, each taking that
// workspace's write lock; pausing every IngestChunk writes hands the lock
// back so interactive client writes are not starved behind the import.
// Unchanged items cost nothing — only actual engine writes count.
func (c *Connector) pace(ctx context.Context, p *pass) error {
	if c.ingestChunk <= 0 || c.ingestPause <= 0 {
		return nil
	}
	p.wrote++
	if p.wrote%c.ingestChunk != 0 {
		return nil
	}
	timer := time.NewTimer(c.ingestPause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
