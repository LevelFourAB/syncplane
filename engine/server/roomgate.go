package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
// It returns the drop reason the client should act on: NO_ACCESS for a caller
// who holds no permission in the workspace's access model, which a later
// grant lifts; ACCESS_REVOKED for any other settled denial, which is
// permanent; and ERROR for a gate that could not decide, which is worth a
// retry.
func (s *Server) openRoom(ctx context.Context, t presence.Topic) (roomAccess, syncplanev1.TopicDropped_Reason, error) {
	adm, err := s.enterWorkspace(ctx, t.WorkspaceID)
	principal := adm.principal
	if err != nil {
		// As on Subscribe: a caller who holds no grant yet is not revoked,
		// since a grant written later admits it.
		if errors.Is(err, ErrNoAccess) {
			return roomAccess{}, syncplanev1.TopicDropped_REASON_NO_ACCESS, err
		}
		if _, denied := AsWorkspaceDenial(err); denied {
			return roomAccess{}, syncplanev1.TopicDropped_REASON_ACCESS_REVOKED, err
		}
		return roomAccess{}, syncplanev1.TopicDropped_REASON_ERROR, err
	}

	if t.Anchored() {
		reason, err := s.checkAnchor(ctx, t, adm)
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
// room's subject, and there is nothing to be present at. The denial for a
// missing, a deleted and an unreadable row carries one message, so a drop
// does not tell the caller whether a key it cannot read exists. The server
// log names which of the three it was.
//
// In a schema that uses the access model the row must also be readable under
// what the caller held when the workspace gate read its grants. The anchor row
// comes from a cache rather than from one snapshot with the grants, so the
// gate's reading serves as well as a fresh one.
func (s *Server) checkAnchor(ctx context.Context, t presence.Topic, adm admission) (syncplanev1.TopicDropped_Reason, error) {
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
		return syncplanev1.TopicDropped_REASON_ACCESS_REVOKED, s.anchorRefused(ctx, t, "the row does not exist")
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
	if adm.caller != nil {
		emitter.caller = adm.caller
		// The anchor's ancestors come through the same cache as the anchor,
		// for the same reason: every watcher of a document gates on them.
		emitter.useAccess(adm.access, emitter.chainsOver(func(entity, key string) (storage.Row, error) {
			return s.roomAnchors.get(ctx, s, presence.Topic{WorkspaceID: t.WorkspaceID, Entity: entity, Key: key})
		}))
	}
	ok, err = emitter.readable(row)
	if err != nil {
		return syncplanev1.TopicDropped_REASON_ERROR, err
	}
	if !ok {
		detail := "the caller cannot read the row"
		if row.Deleted {
			detail = "the row is deleted"
		}
		return syncplanev1.TopicDropped_REASON_ACCESS_REVOKED, s.anchorRefused(ctx, t, detail)
	}
	return syncplanev1.TopicDropped_REASON_UNSPECIFIED, nil
}

// anchorRefused logs why the anchor row of t refuses the caller and returns
// the error the caller sees, whose message is the same whatever detail says.
func (s *Server) anchorRefused(ctx context.Context, t presence.Topic, detail string) error {
	s.log().LogAttrs(ctx, slog.LevelInfo, "room anchor refused the caller",
		slog.String("workspace", t.WorkspaceID),
		slog.String("topic", t.String()),
		slog.String("detail", detail))
	return fmt.Errorf("room anchor %s/%s is not readable", t.Entity, t.Key)
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

// sessionBindings records which callers hold a Watch stream for a session id
// on THIS node, so a Publish naming a session that only other callers hold
// here is refused. A caller is its principal's issuer and subject together:
// two issuers' callers with one subject are two callers, as they are for
// watermarkKey.
//
// Holding a session id is not exclusive. The id is echoed to every watcher of
// a room, so any member can learn it, and a first-come claim would let that
// member take the id while its owner's stream is down — a room change or a
// network blip — and lock the owner out when it reconnects. Every caller that
// opens a stream naming the id holds it, and holding it gives a caller nothing
// over records another caller wrote.
//
// It is node-local by construction: a Publish that lands on a node holding no
// stream for the session has nothing to check against and is admitted on its
// own principal. The cluster-wide backstop is in the registry, not here: a
// presence record belongs to its issuer and subject, the registry ignores a
// bus event that would change or withdraw a record under a different owner,
// and it re-announces a record only for a stream of the record's owner (see
// presence.Registry). With that in place the worst a stranger can do with a
// session id is occupy a slot its owner has not yet written in a room, under
// the stranger's own name — never change another caller's record, and never
// keep one alive.
type sessionBindings struct {
	mu    sync.Mutex
	bound map[string]*sessionBinding
}

// bindingOwner identifies the caller that holds a stream for a session.
type bindingOwner struct {
	issuer  string
	subject string
}

// ownerOf returns the bindingOwner of p.
func ownerOf(p Principal) bindingOwner {
	return bindingOwner{issuer: p.Issuer, subject: p.Subject}
}

type sessionBinding struct {
	// streams are the session's open Watch streams, by the caller that opened
	// each. Usually one caller with one stream; a client that reconnects
	// before the old stream's teardown lands briefly holds two, which is also
	// why each caller has a set rather than a single reference.
	streams map[bindingOwner]map[*presence.Watch]struct{}
}

func newSessionBindings() *sessionBindings {
	return &sessionBindings{bound: make(map[string]*sessionBinding)}
}

// bind records that p holds a stream for the session.
func (b *sessionBindings) bind(sessionID string, p Principal, w *presence.Watch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.bound[sessionID]
	if !ok {
		held = &sessionBinding{streams: make(map[bindingOwner]map[*presence.Watch]struct{}, 1)}
		b.bound[sessionID] = held
	}
	owner := ownerOf(p)
	mine, ok := held.streams[owner]
	if !ok {
		mine = make(map[*presence.Watch]struct{}, 1)
		held.streams[owner] = mine
	}
	mine[w] = struct{}{}
}

// release drops one stream's claim, forgetting the session with the last.
func (b *sessionBindings) release(sessionID string, p Principal, w *presence.Watch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.bound[sessionID]
	if !ok {
		return
	}
	owner := ownerOf(p)
	mine := held.streams[owner]
	delete(mine, w)
	if len(mine) == 0 {
		delete(held.streams, owner)
	}
	if len(held.streams) == 0 {
		delete(b.bound, sessionID)
	}
}

// check reports whether p may publish as this session: p holds a stream for
// the session, or nobody on this node does.
func (b *sessionBindings) check(sessionID string, p Principal) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	held, ok := b.bound[sessionID]
	if !ok {
		return true
	}
	_, mine := held.streams[ownerOf(p)]
	return mine
}

// joined reports whether one of p's streams for this session on this node has
// already been admitted to the topic. Another caller's stream that names the
// same session does not count, whatever its subject: its admission says
// nothing about whether p may enter the room.
func (b *sessionBindings) joined(sessionID string, p Principal, t presence.Topic) bool {
	b.mu.Lock()
	streams := make([]*presence.Watch, 0, 1)
	if held, ok := b.bound[sessionID]; ok {
		for w := range held.streams[ownerOf(p)] {
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
