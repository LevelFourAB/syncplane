// Package pubsub provides change-notification fan-out. Notifier is the seam a
// shared implementation plugs into for multi-node deployments;
// pubsubtest.Conformance is the behavior suite such an implementation must
// pass, and engine/notify/pgnotify is one over Postgres LISTEN/NOTIFY.
//
// Local is strictly single-node: a notification published on one server process
// never reaches subscribers on another, so clients connected elsewhere fall
// back to their polling interval. Multi-node deployments must install a
// shared Notifier via server.WithNotifier — which should also implement the
// Activity extension, or background work stays as node-local as the pokes.
package pubsub

import "sync"

// Notifier broadcasts "something changed" signals per workspace. Signals
// are content-free and coalesce: a slow subscriber sees at least one signal
// after any notify, not one per notify.
type Notifier interface {
	Notify(workspaceID string)
	// Subscribe returns a signal channel and a cancel func. The channel is
	// closed after cancel.
	Subscribe(workspaceID string) (<-chan struct{}, func())
	// SubscribeSet subscribes to every workspace in workspaceIDs at once.
	// Duplicates fold into one membership. Membership is fixed for the Set's
	// life; a different set of workspaces is a new subscription.
	SubscribeSet(workspaceIDs []string) Set
}

// Set is one SubscribeSet subscription: a shared wake channel plus the record
// of which members were notified since the consumer last drained.
//
// One Set replaces a Subscribe per workspace for a consumer of many.
// Per-workspace channels put the merge on the consumer — a goroutine and a
// channel per member — where a Set keeps it inside the notifier's fan-out:
// Notify marks the member dirty and pokes the wake slot, the consumer selects
// on one channel and drains with Changed. Nothing blocks and nothing is lost;
// a notify racing a drain either joins the Changed being taken or leaves a
// wake token behind, so the worst case is one wake with an empty drain.
type Set interface {
	// Wake signals that a member may have been notified. The channel has a
	// one-token buffer and is never closed. A token can outlive the drain that
	// took its members, so an empty Changed after a wake is expected.
	Wake() <-chan struct{}
	// Changed returns the members notified since the previous call and clears
	// them, in no particular order; nil when there are none.
	Changed() []string
	// Close releases the subscription. Idempotent.
	Close()
}

// Activity is the optional extension a Notifier implements when it can report
// every workspace that sees a notify through ONE subscription, without the
// subscriber naming those workspaces first.
//
// Subscribe answers "did this workspace change" for a workspace you already
// know of, which is the right shape for a client stream and the wrong one for
// background work: a process reacting to changes across a whole store would
// hold a subscription — a channel, a map entry, and something selecting on it —
// for every workspace ever written, and could not learn of a workspace created
// after it started at all. One activity stream carries all of them at fixed
// cost, which is what keeps a per-workspace consumer's resident footprint
// independent of how many workspaces exist.
//
// Reach it through SubscribeActivity rather than a bare type assertion, so a
// wrapped notifier is unwrapped rather than mistaken for one that cannot.
type Activity interface {
	// SubscribeActivity returns a channel of ids of workspaces that saw a
	// notify, and a cancel func. The channel is closed after cancel.
	//
	// Ids REPEAT: this is a change signal, not a one-time announcement, so a
	// workspace appears again every time it is notified. What the stream must
	// not do is grow with the write rate — a subscriber that is not keeping up
	// sees one id per workspace with a notify outstanding, never one per notify
	// — which is the same coalescing Subscribe gives, spread over every
	// workspace instead of one.
	//
	// Delivery is best-effort in the same way pokes are: it cuts latency and
	// carries no guarantee, so a consumer keeps a periodic sweep as its
	// backstop rather than treating this as complete.
	SubscribeActivity() (<-chan string, func())
}

// SubscribeActivity subscribes to workspace activity through n, unwrapping
// wrappers (Coalescing) on the way down, and reports ok=false when nothing in
// the chain implements Activity — which leaves periodic sweeping the only way
// to find work.
func SubscribeActivity(n Notifier) (ids <-chan string, cancel func(), ok bool) {
	for n != nil {
		if a, is := n.(Activity); is {
			ch, cancel := a.SubscribeActivity()
			return ch, cancel, true
		}
		u, is := n.(interface{ Unwrap() Notifier })
		if !is {
			break
		}
		n = u.Unwrap()
	}
	return nil, nil, false
}

// Local is an in-process Notifier.
type Local struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
	sets map[string]map[*localSet]struct{}
	all  map[*activitySub]struct{}
}

// activitySub is one SubscribeActivity subscription: a queue of workspaces with
// a notify outstanding, drained onto the channel by a pump goroutine.
//
// The queue exists to coalesce without dropping. Notify must never block, and a
// plain buffered channel forces a choice between blocking and losing a wakeup
// once it fills — but an id is only worth queueing once, since it says "look at
// this workspace" rather than carrying a change. Holding at most one entry per
// workspace bounds the queue by the number of ACTIVE workspaces rather than by
// the write rate, so a subscriber that stalls costs memory in the shape of the
// work it has yet to do.
//
// The pump is what makes that safe: it clears an id from pending as it hands it
// over, so a notify landing while the subscriber is mid-handover queues the
// workspace again rather than folding into a delivery that has already
// happened. Repeats are cheap; a lost wakeup is a reaction that waits for the
// next sweep.
type activitySub struct {
	out  chan string
	done chan struct{}
	// wake carries "the queue is no longer empty" to the pump, coalesced into
	// one slot: the queue itself is the state, this is only the poke.
	wake chan struct{}

	mu      sync.Mutex
	queue   []string
	pending map[string]struct{}
}

func NewLocal() *Local {
	return &Local{
		subs: make(map[string]map[chan struct{}]struct{}),
		sets: make(map[string]map[*localSet]struct{}),
		all:  make(map[*activitySub]struct{}),
	}
}

func (l *Local) Notify(workspaceID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subs[workspaceID] {
		select {
		case ch <- struct{}{}:
		default: // already has a pending signal; coalesce
		}
	}
	for s := range l.sets[workspaceID] {
		s.mark(workspaceID)
	}
	for s := range l.all {
		s.push(workspaceID)
	}
}

func (l *Local) SubscribeActivity() (<-chan string, func()) {
	s := &activitySub{
		out:     make(chan string),
		done:    make(chan struct{}),
		wake:    make(chan struct{}, 1),
		pending: make(map[string]struct{}),
	}
	l.mu.Lock()
	l.all[s] = struct{}{}
	l.mu.Unlock()
	go s.pump()

	cancel := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if _, ok := l.all[s]; ok {
			delete(l.all, s)
			// The pump owns closing out — it may be mid-send — so teardown
			// signals through done and lets it finish.
			close(s.done)
		}
	}
	return s.out, cancel
}

// push queues a workspace for delivery, folding into an id already waiting.
// Called under Local's lock, so it must not block.
func (s *activitySub) push(workspaceID string) {
	s.mu.Lock()
	if _, waiting := s.pending[workspaceID]; waiting {
		s.mu.Unlock()
		return
	}
	s.pending[workspaceID] = struct{}{}
	s.queue = append(s.queue, workspaceID)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default: // the pump has an unread poke; the queue tells it the rest
	}
}

// pump hands queued workspaces to the subscriber in the order they were
// notified, and closes the channel when the subscription is cancelled.
func (s *activitySub) pump() {
	defer close(s.out)
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
				continue
			case <-s.done:
				return
			}
		}
		ws := s.queue[0]
		if len(s.queue) == 1 {
			// Drop the backing array with the last entry rather than keep
			// re-slicing into it, so a burst does not pin its strings.
			s.queue = nil
		} else {
			s.queue = s.queue[1:]
		}
		delete(s.pending, ws)
		s.mu.Unlock()
		select {
		case s.out <- ws:
		case <-s.done:
			return
		}
	}
}

func (l *Local) Subscribe(workspaceID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	l.mu.Lock()
	if l.subs[workspaceID] == nil {
		l.subs[workspaceID] = make(map[chan struct{}]struct{})
	}
	l.subs[workspaceID][ch] = struct{}{}
	l.mu.Unlock()

	cancel := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if set, ok := l.subs[workspaceID]; ok {
			if _, ok := set[ch]; ok {
				delete(set, ch)
				close(ch)
				if len(set) == 0 {
					delete(l.subs, workspaceID)
				}
			}
		}
	}
	return ch, cancel
}

// localSet is one SubscribeSet subscription. Unlike activitySub it needs no
// pump goroutine: the channel there carries ids and must not drop them, so
// something has to block on the send — here the dirty map is the state and the
// wake slot only pokes, so a non-blocking send under Notify's lock is already
// lossless.
type localSet struct {
	local *Local
	// members is the deduplicated membership, kept so Close can unregister.
	members []string
	wake    chan struct{}

	mu    sync.Mutex
	dirty map[string]struct{}
}

func (l *Local) SubscribeSet(workspaceIDs []string) Set {
	s := &localSet{
		local: l,
		wake:  make(chan struct{}, 1),
		dirty: make(map[string]struct{}),
	}
	l.mu.Lock()
	for _, ws := range workspaceIDs {
		set := l.sets[ws]
		if set == nil {
			set = make(map[*localSet]struct{})
			l.sets[ws] = set
		}
		if _, dup := set[s]; !dup {
			s.members = append(s.members, ws)
		}
		set[s] = struct{}{}
	}
	l.mu.Unlock()
	return s
}

// mark records a notified member and pokes the wake slot. Called under Local's
// lock, so it must not block.
func (s *localSet) mark(workspaceID string) {
	s.mu.Lock()
	s.dirty[workspaceID] = struct{}{}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default: // an unread token already promises a drain; dirty carries the rest
	}
}

func (s *localSet) Wake() <-chan struct{} { return s.wake }

func (s *localSet) Changed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.dirty) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.dirty))
	for ws := range s.dirty {
		out = append(out, ws)
	}
	clear(s.dirty)
	return out
}

func (s *localSet) Close() {
	s.local.mu.Lock()
	defer s.local.mu.Unlock()
	for _, ws := range s.members {
		if set, ok := s.local.sets[ws]; ok {
			delete(set, s)
			if len(set) == 0 {
				delete(s.local.sets, ws)
			}
		}
	}
}
