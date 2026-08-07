package server

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Cursor is a durable position in a workspace's change feed. It matches the
// (Version, Entity, Key) order of the underlying change scan: Seq is the
// last fully consumed sequence; Entity/Key mark a sub-position inside a
// same-sequence group when a page split one (empty on a clean boundary).
// The zero Cursor reads the feed from the beginning — but only while the
// workspace has collected no tombstones, since seq 0 sits below any nonzero
// horizon. A first run against an older workspace therefore starts with
// ErrCursorTooOld and the resync it asks for, not with a full replay.
type Cursor struct {
	Seq    uint64
	Entity string
	Key    string
	// Epoch names the store lineage that issued this cursor (see
	// storage.ReadTx.StoreEpoch). Changes stamps it into every cursor it
	// returns and answers one naming another lineage with ErrCursorTooOld, so
	// a consumer whose cursor lives outside the engine store cannot resume
	// against a database swapped or restored underneath it. 0 means the
	// lineage is unknown and is always accepted, so bookkeeping that does not
	// carry the field still works — without the check. The value is opaque and
	// spans all 64 bits; whatever persists it must round-trip every one.
	Epoch uint64
}

// Change is one committed row change observed through the feed.
type Change struct {
	Entity string
	Key    string
	// Seq is the workspace sequence of the mutation that wrote this state.
	// Several rows share a Seq when one mutation wrote them together.
	Seq     uint64
	Deleted bool
	// Message is the decoded row value. Nil when Deleted, and nil when the
	// entity is not registered on this server (consumers still see the
	// Entity/Key/Seq envelope).
	Message proto.Message
	// Actor is who made the write this change reports — the caller's subject
	// for a client push or an in-process write, the label a write made under
	// Trusted/TrustedAs gave itself, or both when a bypass ran inside a
	// caller's request. The zero Actor means the write was unattributed: no
	// authenticated caller made it, which is every push on a server with no
	// Authenticator installed.
	//
	// It is the LAST writer of the row, not the writer of every change that
	// coalesced into this one, because the feed reports state rather than
	// transitions. That is enough to suppress a consumer's own echo and to
	// answer "notify everyone except whoever did this"; it is NOT an audit
	// trail, and a consumer that needs every writer must record its own.
	//
	// Suppressing by Actor therefore drops a genuine trigger when someone
	// else's write is coalesced behind the consumer's own — the same blind spot
	// a sequence watermark has (see engine/connect's Shadow.EngineVersion), and
	// the reason a consumer that must not miss a change reacts to state instead.
	Actor storage.Actor
}

// ErrCursorTooOld reports a feed cursor that cannot be resumed against this
// store: it sits below the workspace's tombstone horizon, above its current
// sequence, or names a different store lineage (see Cursor.Epoch). Deletions
// the consumer never saw may have been garbage-collected, and rows above the
// cursor may belong to a history it never observed, so only a full resync
// against current state recovers. After resyncing, resume from the cursor
// Changes returned alongside this error.
var ErrCursorTooOld = errors.New("server: feed cursor cannot be resumed against this store")

// Changes drains committed row changes after the cursor, in (seq, entity,
// key) order, invoking fn for at most limit rows (limit <= 0 uses the
// MaxPullChanges limit). It returns the advanced cursor and whether more
// changes were left unread.
//
// The feed is the engine's outbox: a consumer that persists the returned
// cursor only after acting on the page gets at-least-once delivery of every
// change, in order, across crashes — dedupe by (entity, key, seq). A consumer
// that reacts by writing back should do so through MutateIdempotent keyed on
// the change, since redelivery is the design and not the exception. Only the
// latest state of a row is observable, not every intermediate write
// (coalescing by row, like pull). The feed is trusted server-side API:
// read filters and subscription scoping do not apply.
//
// When the cursor cannot be resumed — below the tombstone horizon, above the
// current sequence, or from another store lineage — Changes returns
// ErrCursorTooOld along with a cursor at the workspace's current sequence:
// resync against current state first, then resume from that cursor. The
// lineage check does not cover a restore of the SAME store to an earlier point
// that has since been written back past the consumer's position; seq and epoch
// both match there, and the cursor alone cannot tell that history apart from
// one the consumer has yet to read.
func (s *Server) Changes(ctx context.Context, workspaceID string, after Cursor, limit int, fn func(Change) error) (next Cursor, more bool, err error) {
	if workspaceID == "" {
		return Cursor{}, false, fmt.Errorf("server: workspace id is required")
	}
	if fn == nil {
		return Cursor{}, false, fmt.Errorf("server: Changes needs a func")
	}
	if limit <= 0 {
		limit = s.limits.MaxPullChanges
	}
	err = s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		horizon, err := tx.TombstoneHorizon()
		if err != nil {
			return err
		}
		current, err := tx.CurrentSeq()
		if err != nil {
			return err
		}
		epoch, err := tx.StoreEpoch()
		if err != nil {
			return err
		}
		// Lineage before position: a cursor from another store makes its seq
		// meaningless here, so comparing it against this workspace's horizon
		// and head would only pick an arbitrary one of the two errors.
		if after.Epoch != 0 && after.Epoch != epoch {
			next = Cursor{Seq: current, Epoch: epoch}
			return ErrCursorTooOld
		}
		// A cursor older than the tombstone horizon may have missed
		// garbage-collected deletions; only a full resync recovers those.
		if after.Seq < horizon {
			next = Cursor{Seq: current, Epoch: epoch}
			return ErrCursorTooOld
		}
		// A cursor ahead of CurrentSeq cannot have been issued by this store:
		// sequences are per-workspace monotonic, so the store behind this feed
		// was swapped, recreated empty, or restored below the consumer's
		// position. Scanning from after.Seq would silently regress the cursor
		// and strand the consumer; recover the same way as the horizon case —
		// resync against current state, then resume from the returned cursor.
		// This still earns its place next to the epoch check: it is what
		// catches an epoch-less cursor, and a lineage that never minted an
		// epoch because nothing was ever written to it.
		if after.Seq > current {
			next = Cursor{Seq: current, Epoch: epoch}
			return ErrCursorTooOld
		}
		// Scan one row past the limit: seeing it proves truncation without
		// emitting it.
		scanned := 0
		var last storage.Row
		if err := tx.ScanChanges(after.Seq, after.Entity, after.Key, limit+1, func(row storage.Row) error {
			scanned++
			if scanned > limit {
				more = true
				return nil
			}
			last = row
			ch := Change{Entity: row.Entity, Key: row.Key, Seq: row.Version, Deleted: row.Deleted, Actor: row.Actor}
			if !row.Deleted {
				if info, ok := s.catalogPtr().entities[row.Entity]; ok {
					msg, err := info.decode(row.Value)
					if err != nil {
						return err
					}
					ch.Message = msg
				}
			}
			return fn(ch)
		}); err != nil {
			return err
		}
		if more {
			// Resume just past the last delivered row; the (entity, key)
			// sub-position picks up the rest of a group sharing this seq.
			next = Cursor{Seq: last.Version, Entity: last.Entity, Key: last.Key, Epoch: epoch}
		} else {
			next = Cursor{Seq: current, Epoch: epoch}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrCursorTooOld) {
			return next, false, ErrCursorTooOld
		}
		return Cursor{}, false, err
	}
	return next, more, nil
}

// CurrentSeq returns the workspace's current sequence — the head of its change
// feed, and 0 for a workspace that has never been written. A feed consumer's
// distance from it is its lag, which is what tells a stalled consumer apart
// from one with nothing to do.
func (s *Server) CurrentSeq(ctx context.Context, workspaceID string) (uint64, error) {
	if workspaceID == "" {
		return 0, fmt.Errorf("server: workspace id is required")
	}
	var seq uint64
	err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		var err error
		seq, err = tx.CurrentSeq()
		return err
	})
	return seq, err
}

// SubscribeChanges returns a channel that signals after writes commit in the
// workspace (the same wakeup the Subscribe streams ride on) and a cancel func;
// the channel
// closes after cancel. Signals are content-free, coalesced, and in-process
// only — they cut latency, they do not carry guarantees. Consumers drain via
// Changes on their own schedule regardless.
func (s *Server) SubscribeChanges(workspaceID string) (<-chan struct{}, func()) {
	return s.notifier.Subscribe(workspaceID)
}

// SubscribeActivity returns a channel of ids of workspaces that have committed
// a write — INCLUDING workspaces the caller has never heard of — and a cancel
// func; the channel closes after cancel. ok is false when the installed
// notifier cannot report it, which leaves a periodic sweep as the only way to
// notice work.
//
// It is the wake-up a consumer that runs background work across many
// workspaces subscribes to. SubscribeChanges needs the workspace id up front,
// so covering a whole store through it means one subscription per workspace and
// no way to hear about a workspace created later; this carries every workspace
// over one subscription, and keeps carrying each of them — an id arrives every
// time its workspace is notified, not once.
//
// Like every notifier signal it is best-effort and, with the default notifier,
// in-process: a consumer that misses one finds the work on its next sweep,
// later. Treat it as latency, not as an enumeration.
func (s *Server) SubscribeActivity() (ids <-chan string, cancel func(), ok bool) {
	return pubsub.SubscribeActivity(s.notifier)
}
