package presencebridge

import (
	"sync"
	"time"

	"github.com/levelfourab/syncplane/engine/presence"
)

// sessionKey names one session in one topic.
type sessionKey struct {
	topic   presence.Topic
	session string
}

// localSession is what the bridge remembers about a session published on the
// local deployment: who owns it, and when it was last heard of.
type localSession struct {
	issuer  string
	subject string
	seen    time.Time
}

// localSessions is the set of sessions the local deployment speaks for, per
// topic. It serves two ends: upward it holds each session to its first owner,
// and downward it tells an origin event about a local session (an echo of
// something the bridge forwarded) from one about an origin participant.
type localSessions struct {
	mu       sync.Mutex
	sessions map[sessionKey]*localSession
}

func (l *localSessions) init() {
	l.sessions = make(map[sessionKey]*localSession)
}

// claim records ev's session for its owner and reports whether ev may be
// forwarded: the session is new, or ev comes from the owner that holds it.
func (l *localSessions) claim(ev presence.Event, ttl time.Duration) bool {
	k := sessionKey{topic: ev.Topic, session: ev.Participant.SessionID}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	held, ok := l.sessions[k]
	if ok && now.Sub(held.seen) > ttl {
		// Lapsed but not yet swept: the slot is free, as it is in the
		// registry once the record expired.
		ok = false
	}
	if !ok {
		l.sessions[k] = &localSession{issuer: ev.Participant.Issuer, subject: ev.Participant.Subject, seen: now}
		return true
	}
	if held.issuer != ev.Participant.Issuer || held.subject != ev.Participant.Subject {
		return false
	}
	held.seen = now
	return true
}

// note records a session seen on the inner bus, keeping its existing owner.
func (l *localSessions) note(ev presence.Event) {
	if ev.Participant.Issuer == OriginIssuer || ev.Participant.SessionID == "" {
		return
	}
	k := sessionKey{topic: ev.Topic, session: ev.Participant.SessionID}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	held, ok := l.sessions[k]
	if !ok {
		l.sessions[k] = &localSession{issuer: ev.Participant.Issuer, subject: ev.Participant.Subject, seen: now}
		return
	}
	if held.issuer == ev.Participant.Issuer && held.subject == ev.Participant.Subject {
		held.seen = now
	}
}

// has reports whether the session is local in the topic.
func (l *localSessions) has(topic presence.Topic, session string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.sessions[sessionKey{topic: topic, session: session}]
	return ok
}

// expire forgets the sessions not heard of within ttl.
func (l *localSessions) expire(now time.Time, ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, s := range l.sessions {
		if now.Sub(s.seen) > ttl {
			delete(l.sessions, k)
		}
	}
}
