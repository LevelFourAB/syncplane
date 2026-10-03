package presence

import "sync"

// maxQueued bounds one outbox's undelivered backlog.
//
// State never reaches it: a session's queued update is overwritten in place, so
// the state half of the queue is bounded by the number of PARTICIPANTS rather
// than by the publish rate — the same trick pubsub.Local plays with one entry
// per active workspace. Only broadcasts accumulate, and a reader far enough
// behind to hold this many of them has lost the room's timing anyway.
const maxQueued = 256

// stateSlot identifies the queued state of one session in one topic, as one
// subject wrote it. An outbox serving a single topic still keys on the topic,
// because the Registry hands one outbox every topic a stream watches.
//
// The subject is part of the key because a receiver keeps a record for the
// subject that wrote it: state from another subject that names the same session
// is not a newer version of that record, and folding it in would lose the
// owner's update.
type stateSlot struct {
	topic   Topic
	session string
	subject string
}

func slotOf(ev *Event) stateSlot {
	return stateSlot{topic: ev.Topic, session: ev.Participant.SessionID, subject: ev.Participant.Subject}
}

// outbox is a non-blocking, coalescing delivery queue: a queue of undelivered
// events drained onto a channel by a pump goroutine.
//
// It is what keeps a publisher from blocking on a reader that is not reading,
// without forcing the choice a plain buffered channel does between blocking and
// losing the newest event once it fills.
type outbox struct {
	out  chan Event
	done chan struct{}
	// wake carries "the queue is no longer empty" to the pump, coalesced into
	// one slot: the queue itself is the state, this is only the poke.
	wake chan struct{}

	mu    sync.Mutex
	queue []*Event
	// stateAt points at each session's queued state event, so a burst from one
	// session folds into its slot instead of lengthening the queue. Entries are
	// cleared when the event is handed over, and when a leave for that session
	// is queued behind it — otherwise a state published after the leave would
	// fold into a slot that sits in front of it and resurrect the record.
	stateAt map[stateSlot]*Event
	dropped uint64
	closed  bool
}

func newOutbox() *outbox {
	o := &outbox{
		out:     make(chan Event),
		done:    make(chan struct{}),
		wake:    make(chan struct{}, 1),
		stateAt: make(map[stateSlot]*Event),
	}
	go o.pump()
	return o
}

// push queues an event, folding a state update into the one already waiting for
// that session; the higher revision of the two stays. It never blocks, so it is
// safe to call under another lock.
func (o *outbox) push(ev Event) {
	slot := slotOf(&ev)
	o.mu.Lock()
	switch ev.Kind {
	case KindState:
		if waiting, ok := o.stateAt[slot]; ok {
			// Arrival order is not revision order: a Registry re-announces a
			// record after releasing the lock it read it under, so a newer
			// publish can be queued first. The receiver drops a lower revision
			// anyway, and putting one in the slot would lose the higher one.
			if ev.Participant.Revision >= waiting.Participant.Revision {
				*waiting = ev
			}
			o.mu.Unlock()
			return
		}
	case KindLeave:
		delete(o.stateAt, slot)
	}
	if len(o.queue) >= maxQueued {
		o.dropped++
		o.mu.Unlock()
		return // at most once: a reader this far behind loses the event
	}
	queued := &ev
	o.queue = append(o.queue, queued)
	if ev.Kind == KindState {
		o.stateAt[slot] = queued
	}
	o.mu.Unlock()
	select {
	case o.wake <- struct{}{}:
	default: // the pump has an unread poke; the queue tells it the rest
	}
}

// dropCount reports how many events this outbox has discarded for lack of room.
func (o *outbox) dropCount() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dropped
}

// close stops the pump, which closes the event channel. It is idempotent.
func (o *outbox) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	// The pump owns closing out — it may be mid-send — so teardown signals
	// through done and lets it finish.
	close(o.done)
}

// pump hands queued events over in the order they were queued, and closes the
// channel when the outbox is closed.
func (o *outbox) pump() {
	defer close(o.out)
	for {
		o.mu.Lock()
		if len(o.queue) == 0 {
			o.mu.Unlock()
			select {
			case <-o.wake:
				continue
			case <-o.done:
				return
			}
		}
		queued := o.queue[0]
		if len(o.queue) == 1 {
			// Drop the backing array with the last entry rather than keep
			// re-slicing into it, so a burst does not pin its payloads.
			o.queue = nil
		} else {
			o.queue = o.queue[1:]
		}
		slot := slotOf(queued)
		if queued.Kind == KindState && o.stateAt[slot] == queued {
			delete(o.stateAt, slot)
		}
		ev := *queued
		o.mu.Unlock()
		select {
		case o.out <- ev:
		case <-o.done:
			return
		}
	}
}
