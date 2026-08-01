package effects

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"time"
)

// runnerKey identifies one unit of work: a consumer's progress through one
// workspace. Consumers are isolated from each other by construction — separate
// positions, separate backoff — so a handler wedged on a bad destination stops
// only itself.
type runnerKey struct {
	consumer  string
	workspace string
}

// retirement records that a pair's health entry entered the retention window,
// stamped with the sequence that owns it. The stamp is what keeps a pair that
// came back and left again from having its live entry trimmed by the older
// record of the same key.
type retirement struct {
	key runnerKey
	seq uint64
}

// Health is one consumer's status on one workspace, as of its last pass.
//
// Failures inside the workers are logged and never returned — a reaction must
// not take the process down — so this is the surface that reports them. It is
// also the only thing that distinguishes a wedged consumer from an idle one: a
// handler failing forever and a handler with nothing to do both look like
// silence from the outside.
//
// # What is remembered
//
// A pair the runtime is actively tracking always has an entry, which includes
// every FAILING pair: a consumer that cannot make progress stays visible for as
// long as that is true. A pair that drained its workspace and went quiet is
// forgotten — first from the runtime, then, once Config.HealthRetention newer
// pairs have finished behind it, from here.
//
// So an absent entry means "not currently tracked, and not recently drained",
// which covers a workspace this consumer has never had work for and one it
// finished with long ago. It never means "failing" — that is the whole point of
// the asymmetry. Read Health to find what is wedged or behind, not to enumerate
// what is healthy.
type Health struct {
	Consumer    string
	WorkspaceID string
	// LastAttempt is when the most recent pass ran; zero if none has.
	LastAttempt time.Time
	// LastSuccess is when a pass last completed without error; zero if never.
	LastSuccess time.Time
	// LastError is the most recent failure, cleared to nil by any success.
	LastError error
	// ConsecutiveFailures counts failures since the last success, and is the
	// backoff exponent.
	ConsecutiveFailures int
	// NextAttempt is the earliest time the runner will retry. Zero when not
	// backing off — after a success, or when backoff is disabled.
	NextAttempt time.Time
	// Position is the feed sequence the consumer has fully acted on, and Head
	// the workspace's current sequence. Head-Position is the lag: it stays put
	// while a consumer is wedged and grows as writes pile up behind it.
	Position uint64
	Head     uint64
}

// Lag is how many sequences behind the workspace head this consumer is.
func (h Health) Lag() uint64 {
	if h.Head < h.Position {
		return 0
	}
	return h.Head - h.Position
}

// Health returns one consumer's status on one workspace. The bool is false when
// the runtime holds no entry for that pair — see Health for what it keeps.
func (rt *Runtime) Health(consumer, workspaceID string) (Health, bool) {
	rt.hmu.Lock()
	defer rt.hmu.Unlock()
	h, ok := rt.health[runnerKey{consumer: consumer, workspace: workspaceID}]
	return h, ok
}

// HealthAll returns every consumer/workspace pair the runtime holds an entry
// for, ordered by consumer then workspace.
func (rt *Runtime) HealthAll() []Health {
	rt.hmu.Lock()
	out := make([]Health, 0, len(rt.health))
	for _, h := range rt.health {
		out = append(out, h)
	}
	rt.hmu.Unlock()
	slices.SortFunc(out, func(a, b Health) int {
		if c := cmp.Compare(a.Consumer, b.Consumer); c != 0 {
			return c
		}
		return cmp.Compare(a.WorkspaceID, b.WorkspaceID)
	})
	return out
}

// Tracked reports how many (consumer, workspace) pairs the runtime is holding
// state for: those with work queued, a pass running, or a failure being retried.
// A pair that has drained its workspace is not among them.
//
// It is the resident-set size — the number that stays flat as a store grows and
// climbs when consumers fall behind — so it belongs on a graph next to the lag
// in Health. A number that tracks the workspace count instead means work is
// arriving faster than the workers drain it.
func (rt *Runtime) Tracked() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return len(rt.pairs)
}

// recordPass folds one pass's outcome into health, computes the next attempt on
// failure, and hands the result to Config.OnHealth. Position and head come from
// the pass itself, so a wedged consumer reports the position it is stuck at
// rather than a stale one.
func (rt *Runtime) recordPass(key runnerKey, position, head uint64, err error) Health {
	now := time.Now()
	rt.hmu.Lock()
	h := rt.health[key]
	h.Consumer, h.WorkspaceID = key.consumer, key.workspace
	h.LastAttempt = now
	h.Position, h.Head = position, head
	if err == nil {
		h.LastSuccess = now
		h.LastError = nil
		h.ConsecutiveFailures = 0
		h.NextAttempt = time.Time{}
	} else {
		h.LastError = err
		h.ConsecutiveFailures++
		if d, ok := rt.backoff(h.ConsecutiveFailures); ok {
			h.NextAttempt = now.Add(d)
		} else {
			h.NextAttempt = time.Time{}
		}
	}
	rt.health[key] = h
	rt.hmu.Unlock()
	if rt.onHealth != nil {
		// Outside the lock: the callback is adopter code and may call back into
		// Health/HealthAll.
		rt.onHealth(h)
	}
	return h
}

// retire moves a pair's health into the retention window, where it survives
// until Config.HealthRetention newer pairs have finished behind it. Called as
// the pair leaves the resident set, under Runtime.mu.
//
// Retention exists because eviction and observability pull opposite ways.
// Keeping an entry per pair ever seen is the accumulation this runtime was
// reshaped to remove; dropping one the moment a pair drains would leave a quiet
// system reporting nothing at all, which reads the same as a runtime that never
// ran. A bounded window of the most recently finished pairs answers "is this
// working" without paying for the ones nobody will ask about.
func (rt *Runtime) retire(key runnerKey) {
	rt.hmu.Lock()
	defer rt.hmu.Unlock()
	if _, ok := rt.health[key]; !ok {
		return // never ran a pass; nothing to remember
	}
	if rt.retention == 0 {
		delete(rt.health, key)
		return
	}
	rt.retireSeq++
	rt.retiredAt[key] = rt.retireSeq
	rt.retired = append(rt.retired, retirement{key: key, seq: rt.retireSeq})
	for len(rt.retired) > rt.retention {
		oldest := rt.retired[0]
		rt.retired = rt.retired[1:]
		// A key whose stamp has moved on came back and left again (or is
		// resident right now); a later record owns its entry.
		if rt.retiredAt[oldest.key] != oldest.seq {
			continue
		}
		delete(rt.retiredAt, oldest.key)
		delete(rt.health, oldest.key)
	}
}

// unretire takes a pair back out of the retention window as it re-enters the
// resident set, so its live health is not trimmed by the record of the pair it
// used to be. The stale slice entry is left to be skipped when it surfaces.
// Called under Runtime.mu.
func (rt *Runtime) unretire(key runnerKey) {
	rt.hmu.Lock()
	defer rt.hmu.Unlock()
	delete(rt.retiredAt, key)
}

// backoff returns how long to wait before retrying a pass that just failed for
// the nth consecutive time: the delay doubles from RetryMin up to RetryMax, with
// ±20% jitter so consumers failing against the same destination do not retry in
// lockstep. The bool is false when backoff is disabled.
func (rt *Runtime) backoff(failures int) (time.Duration, bool) {
	if rt.retryMin < 0 {
		return 0, false
	}
	d := rt.retryMin
	for i := 1; i < failures && d < rt.retryMax; i++ {
		d *= 2
	}
	if d > rt.retryMax {
		d = rt.retryMax
	}
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64())), true
}
