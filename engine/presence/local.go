package presence

import (
	"context"
	"sync"
)

// Local is an in-process Bus: events reach subscribers in this process and
// nowhere else. It is the default, and the right one for a single-node
// deployment.
type Local struct {
	mu   sync.Mutex
	subs map[Topic]map[*outbox]struct{}
}

// NewLocal returns an in-process Bus.
func NewLocal() *Local {
	return &Local{subs: make(map[Topic]map[*outbox]struct{})}
}

// Publish hands the event to every subscriber of its topic. It never blocks and
// never fails.
func (l *Local) Publish(_ context.Context, ev Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for o := range l.subs[ev.Topic] {
		o.push(ev)
	}
	return nil
}

// Subscribe returns this topic's event channel and its teardown.
func (l *Local) Subscribe(topic Topic) (<-chan Event, func()) {
	o := newOutbox()
	l.mu.Lock()
	if l.subs[topic] == nil {
		l.subs[topic] = make(map[*outbox]struct{})
	}
	l.subs[topic][o] = struct{}{}
	l.mu.Unlock()

	return o.out, func() {
		l.mu.Lock()
		if set, ok := l.subs[topic]; ok {
			delete(set, o)
			if len(set) == 0 {
				delete(l.subs, topic)
			}
		}
		l.mu.Unlock()
		o.close()
	}
}
