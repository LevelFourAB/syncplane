package pubsub

import (
	"sync"

	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// Prefixed returns a view of n scoped to one prefix of the workspace-id space.
// The view prepends prefix to every workspace id it passes to n, and removes it
// from every id n hands back: the members a Set reports and the ids on the
// activity stream. An id from n without the prefix belongs to another
// partition, and the view drops it. An empty prefix returns n unchanged.
//
// A host that serves several applications from one notifier gives each
// application's server its own view, so two applications with the same
// workspace id do not wake each other's subscribers. The prefix must end in a
// separator that no workspace id can contain, or two partitions overlap.
//
// The view implements Activity exactly when n does, as SubscribeActivity
// resolves it. It has no Unwrap: unwrapping it would reach ids that still carry
// the prefix.
func Prefixed(n Notifier, prefix string) Notifier {
	if prefix == "" {
		return n
	}
	p := prefixed{inner: n, prefix: prefix}
	if a, ok := activityOf(n); ok {
		return prefixedActivity{prefixed: p, activity: a}
	}
	return p
}

type prefixed struct {
	inner  Notifier
	prefix string
}

func (p prefixed) Notify(workspaceID string) { p.inner.Notify(p.prefix + workspaceID) }

func (p prefixed) Subscribe(workspaceID string) (<-chan struct{}, func()) {
	return p.inner.Subscribe(p.prefix + workspaceID)
}

func (p prefixed) SubscribeSet(workspaceIDs []string) Set {
	scoped := make([]string, len(workspaceIDs))
	for i, ws := range workspaceIDs {
		scoped[i] = p.prefix + ws
	}
	return prefixedSet{Set: p.inner.SubscribeSet(scoped), prefix: p.prefix}
}

// prefixedSet removes the prefix from the members a Set reports.
type prefixedSet struct {
	Set
	prefix string
}

func (s prefixedSet) Changed() []string {
	var out []string
	for _, id := range s.Set.Changed() {
		if ws, mine := naming.CutWorkspacePrefix(id, s.prefix); mine {
			out = append(out, ws)
		}
	}
	return out
}

// prefixedActivity is the view of a notifier that implements Activity.
type prefixedActivity struct {
	prefixed
	activity Activity
}

// SubscribeActivity removes the prefix from each id on the stream. Ids arrive
// on a channel, so this is the one place the view runs a goroutine. A consumer
// holds one activity subscription, not one per workspace, so the cost is one
// goroutine per consumer.
func (p prefixedActivity) SubscribeActivity() (<-chan string, func()) {
	ids, cancel := p.activity.SubscribeActivity()
	out := make(chan string)
	// done releases the goroutine from a send no one is receiving any more:
	// cancelling closes the stream from below, but a consumer that stops
	// reading first would otherwise leave this blocked forever.
	done := make(chan struct{})
	go func() {
		defer close(out)
		for id := range ids {
			ws, mine := naming.CutWorkspacePrefix(id, p.prefix)
			if !mine {
				continue
			}
			select {
			case out <- ws:
			case <-done:
				return
			}
		}
	}()
	var once sync.Once
	return out, func() {
		once.Do(func() { close(done) })
		cancel()
	}
}
