package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/storage"
)

// roomAccess is what the join gate resolved for one caller and one topic.
type roomAccess struct {
	principal Principal
	// visible filters the roster this caller is shown. Nil shows everyone.
	visible ParticipantFilter
}

// openRoom is the admission gate for one topic, and the single place a caller
// is let into a room from — the Watch stream at join and again on its
// heartbeat, and Publish for a topic the caller is not already watching here.
//
// Ordering mirrors openScope's, and for the same reason: the workspace gate
// runs FIRST, so a refused caller performs no storage work and leaves no trace
// in a workspace they cannot see. The anchor read follows, then the app's own
// hooks. The bus subscription is not taken here at all — the caller takes it
// after this returns, so a refusal never registers one.
//
// It returns the drop reason the client should act on: a settled denial is
// permanent, everything else is worth a retry.
func (s *Server) openRoom(ctx context.Context, t presence.Topic) (roomAccess, syncplanev1.TopicDropped_Reason, error) {
	principal, err := s.enterWorkspace(ctx, t.WorkspaceID)
	if err != nil {
		if _, denied := AsWorkspaceDenial(err); denied {
			return roomAccess{}, syncplanev1.TopicDropped_REASON_ACCESS_REVOKED, err
		}
		return roomAccess{}, syncplanev1.TopicDropped_REASON_ERROR, err
	}

	if t.Anchored() {
		reason, err := s.checkAnchor(ctx, t)
		if err != nil {
			return roomAccess{}, reason, err
		}
	}

	rooms, hasHooks := s.perms.(RoomPermissions)
	if !hasHooks {
		return roomAccess{principal: principal}, syncplanev1.TopicDropped_REASON_UNSPECIFIED, nil
	}
	if err := rooms.CanJoinRoom(ctx, principal, t.WorkspaceID, t); err != nil {
		if _, denied := AsRoomDenial(err); denied {
			return roomAccess{}, syncplanev1.TopicDropped_REASON_ACCESS_REVOKED, err
		}
		return roomAccess{}, syncplanev1.TopicDropped_REASON_ERROR, err
	}
	visible, err := rooms.VisibleParticipants(ctx, principal, t.WorkspaceID, t)
	if err != nil {
		return roomAccess{}, syncplanev1.TopicDropped_REASON_ERROR, err
	}
	return roomAccess{principal: principal, visible: visible}, syncplanev1.TopicDropped_REASON_UNSPECIFIED, nil
}

// checkAnchor enforces the rule that makes an anchored room free to authorize:
// a caller is admitted only if the app's ReadFilter would let them read the row
// the topic names. Room membership then inherits row permissions instead of
// being a second surface an app has to remember to guard.
//
// A missing or deleted row is a denial rather than an error. The row is the
// room's subject, and there is nothing to be present at.
func (s *Server) checkAnchor(ctx context.Context, t presence.Topic) (syncplanev1.TopicDropped_Reason, error) {
	info, ok := s.catalogPtr().entities[t.Entity]
	if !ok {
		return syncplanev1.TopicDropped_REASON_ACCESS_REVOKED,
			fmt.Errorf("room anchor names unknown entity %q", t.Entity)
	}
	// readable() answers the permission question and not this one: a
	// server-only entity has no read filter a client could ever clear, and
	// nothing downstream would notice the anchor was meaningless.
	if info.serverOnly {
		return syncplanev1.TopicDropped_REASON_ACCESS_REVOKED,
			fmt.Errorf("room anchor names server-only entity %q, which never reaches clients", info.name)
	}

	row, err := s.roomAnchors.get(ctx, s, t)
	if errors.Is(err, storage.ErrNotFound) {
		return syncplanev1.TopicDropped_REASON_ACCESS_REVOKED,
			fmt.Errorf("room anchor %s/%s does not exist", t.Entity, t.Key)
	}
	if err != nil {
		return syncplanev1.TopicDropped_REASON_ERROR, err
	}

	// A fresh emitter per gate: its read filters are the CALLER's, so they
	// cannot be shared across streams the way the row read is.
	emitter, err := newRowEmitter(ctx, s, t.WorkspaceID, nil)
	if err != nil {
		return syncplanev1.TopicDropped_REASON_ERROR, err
	}
	ok, err = emitter.readable(row)
	if err != nil {
		return syncplanev1.TopicDropped_REASON_ERROR, err
	}
	if !ok {
		return syncplanev1.TopicDropped_REASON_ACCESS_REVOKED,
			fmt.Errorf("room anchor %s/%s is not readable", t.Entity, t.Key)
	}
	return syncplanev1.TopicDropped_REASON_UNSPECIFIED, nil
}

// anchorKey identifies one anchor row across the streams that gate on it.
type anchorKey struct {
	workspace string
	entity    string
	key       string
}

// anchorCache memoizes the anchor-row READ — never the verdict, which depends
// on the caller's read filter and so differs per stream.
//
// The re-gate runs per stream per heartbeat, and a popular document is watched
// by everyone who has it open. Without this, one document's heartbeat costs one
// read transaction and one unmarshal per watcher; with it, one per interval.
//
// Entries live for one interval, so a row that becomes unreadable is noticed
// within two heartbeats rather than one. That is the deliberate cost: the gate
// is a coarse revocation window either way, and doubling it buys back a
// per-watcher read.
type anchorCache struct {
	ttl time.Duration

	mu      sync.Mutex
	entries map[anchorKey]anchorEntry
}

type anchorEntry struct {
	row storage.Row
	at  time.Time
}

func newAnchorCache(ttl time.Duration) *anchorCache {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &anchorCache{ttl: ttl, entries: make(map[anchorKey]anchorEntry)}
}

// get returns the anchor row, reading it through when no fresh entry is held.
// A failed read is not cached: the next caller retries rather than inheriting
// an outage for the rest of the interval.
func (c *anchorCache) get(ctx context.Context, s *Server, t presence.Topic) (storage.Row, error) {
	k := anchorKey{workspace: t.WorkspaceID, entity: t.Entity, key: t.Key}
	now := time.Now()

	c.mu.Lock()
	if e, ok := c.entries[k]; ok && now.Sub(e.at) < c.ttl {
		c.mu.Unlock()
		return e.row, nil
	}
	c.mu.Unlock()

	var row storage.Row
	// Any staleness serves: the entry is about to be cached for a TTL anyway,
	// so replica lag adds nothing the cache does not already tolerate.
	if err := s.readTx(ctx, t.WorkspaceID, 0, func(tx storage.ReadTx) error {
		var err error
		row, err = tx.GetRow(t.Entity, t.Key)
		return err
	}); err != nil {
		return storage.Row{}, err
	}

	c.mu.Lock()
	c.entries[k] = anchorEntry{row: row, at: now}
	c.mu.Unlock()
	return row, nil
}

// expire drops entries past their TTL. The room sweeper calls it, so the cache
// is bounded by the anchors gated on in one interval rather than by every
// anchor the process has ever seen.
func (c *anchorCache) expire(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if now.Sub(e.at) >= c.ttl {
			delete(c.entries, k)
		}
	}
}

// sessionBindings ties a session id to the principal that opened its Watch
// stream on THIS node, so a Publish naming a session it does not own is
// refused.
//
// It is node-local by construction: a Publish that lands on a node holding no
// stream for the session has nothing to check against and is admitted on its
// own principal, which is the same posture the rest of the plane takes — the
// subject a participant is shown under is always stamped from the publisher's
// own principal, never from the payload, so the worst an unbound publish can do
// is write into a session slot under its own name.
type sessionBindings struct {
	mu    sync.Mutex
	bound map[string]*sessionBinding
}

type sessionBinding struct {
	subject string
	// streams are the session's open Watch streams. Usually one; a client that
	// reconnects before the old stream's teardown lands briefly holds two,
	// which is also why this is a set rather than a single reference.
	streams map[*presence.Watch]struct{}
}

func newSessionBindings() *sessionBindings {
	return &sessionBindings{bound: make(map[string]*sessionBinding)}
}

// bind claims a session for a subject and its stream. It reports false when the
// session is already held by a different subject.
func (b *sessionBindings) bind(sessionID, subject string, w *presence.Watch) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.bound[sessionID]
	if !ok {
		b.bound[sessionID] = &sessionBinding{
			subject: subject,
			streams: map[*presence.Watch]struct{}{w: {}},
		}
		return true
	}
	if held.subject != subject {
		return false
	}
	held.streams[w] = struct{}{}
	return true
}

// release drops one stream's claim, forgetting the session with the last.
func (b *sessionBindings) release(sessionID string, w *presence.Watch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.bound[sessionID]
	if !ok {
		return
	}
	delete(held.streams, w)
	if len(held.streams) == 0 {
		delete(b.bound, sessionID)
	}
}

// check reports whether subject may publish as this session. An unbound session
// is allowed: this node holds no stream for it, so it has nothing to say.
func (b *sessionBindings) check(sessionID, subject string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.bound[sessionID]
	return !ok || held.subject == subject
}

// joined reports whether one of this session's streams on this node has already
// been admitted to the topic.
func (b *sessionBindings) joined(sessionID string, t presence.Topic) bool {
	b.mu.Lock()
	streams := make([]*presence.Watch, 0, 1)
	if held, ok := b.bound[sessionID]; ok {
		for w := range held.streams {
			streams = append(streams, w)
		}
	}
	b.mu.Unlock()
	// Asked outside this lock: Watch.Joined takes its own, and the streams a
	// session holds change only when one opens or closes.
	for _, w := range streams {
		if w.Joined(t) {
			return true
		}
	}
	return false
}

// roomSweeper drives the registry's announce-and-expire pass, and ages the
// anchor cache out with it.
type roomSweeper struct {
	stop chan struct{}
	done chan struct{}
}

func startRoomSweeper(s *Server) *roomSweeper {
	sw := &roomSweeper{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(sw.done)
		t := time.NewTicker(s.rooms.Announce())
		defer t.Stop()
		ctx := context.Background()
		for {
			select {
			case <-sw.stop:
				return
			case now := <-t.C:
				s.rooms.Sweep(ctx)
				s.roomAnchors.expire(now)
			}
		}
	}()
	return sw
}

func (sw *roomSweeper) close() {
	select {
	case <-sw.stop:
	default:
		close(sw.stop)
	}
	<-sw.done
}

// roomLimitError builds the resource_exhausted a room request exceeding a limit
// is refused with.
func roomLimitError(what, limitName string, limit int) error {
	return connect.NewError(connect.CodeResourceExhausted,
		fmt.Errorf("%s", limitExceeded(what, limitName, limit)))
}
