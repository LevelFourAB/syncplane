package auth

import (
	"sync"
	"time"
)

// limiter counts failed sign-in attempts per key within a window and refuses
// the key once they reach the limit. A success clears the key.
//
// It is per process. A multi-node deployment gives an attacker one window per
// node, which bounds the rate but does not divide it; the limit exists to
// make online guessing impractical, not to make it impossible.
type limiter struct {
	attempts int
	window   time.Duration
	now      func() time.Time

	mu       sync.Mutex
	failures map[string]*bucket
}

type bucket struct {
	count   int
	resetAt time.Time
}

// limiterSweepAt is the size past which a failure evicts expired buckets, so
// a spray of distinct addresses cannot grow the map without bound.
const limiterSweepAt = 4096

func newLimiter(attempts int, window time.Duration, now func() time.Time) *limiter {
	return &limiter{attempts: attempts, window: window, now: now, failures: map[string]*bucket{}}
}

// allow reports whether the key may attempt to sign in now.
func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.failures[key]
	if !ok {
		return true
	}
	now := l.now()
	if !now.Before(b.resetAt) {
		delete(l.failures, key)
		return true
	}
	return b.count < l.attempts
}

// fail records a failed attempt for the key.
func (l *limiter) fail(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.failures[key]
	if !ok || !now.Before(b.resetAt) {
		if len(l.failures) >= limiterSweepAt {
			for k, old := range l.failures {
				if !now.Before(old.resetAt) {
					delete(l.failures, k)
				}
			}
		}
		b = &bucket{resetAt: now.Add(l.window)}
		l.failures[key] = b
	}
	b.count++
}

// reset clears the key after a successful sign-in.
func (l *limiter) reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, key)
}
