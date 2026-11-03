package upstream

import (
	"math/rand/v2"
	"time"
)

// backoff is exponential backoff with full jitter: a wait drawn at random up
// to a window that doubles per attempt from min to max. Full jitter spreads
// the reconnects of many links that lost the origin at the same moment.
type backoff struct {
	min, max time.Duration
	attempt  int
}

func newBackoff(min, max time.Duration) *backoff { return &backoff{min: min, max: max} }

func (b *backoff) next() time.Duration {
	window := b.min << min(b.attempt, 20)
	if window > b.max || window <= 0 {
		window = b.max
	}
	b.attempt++
	return time.Duration(rand.Int64N(int64(window) + 1))
}

func (b *backoff) reset() { b.attempt = 0 }
