package server

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/levelfourab/syncplane/engine/storage"
)

// MaxConsumerNameBytes bounds an effect consumer's name. The name is the
// identity of a durable position and is stored per workspace, so the bound is
// what keeps a caller-supplied string from becoming unbounded storage. It is
// generous against anything a human names a consumer.
const MaxConsumerNameBytes = 128

// ReservedConsumerPrefix starts the consumer names the engine keeps for its own
// state. Every method that takes a consumer name refuses one that starts with
// it.
const ReservedConsumerPrefix = "syncplane."

// ReservedWorkspacePrefix starts the ids of workspaces that hold only the
// engine's own state, such as a replication link's list of linked workspaces.
// WorkspaceIDs, ConsumerPendingWorkspaces and PendingWorkspaces leave them out,
// and the wire API refuses them, as it refuses every id that is not a valid
// workspace id.
const ReservedWorkspacePrefix = "#syncplane."

// withoutReserved drops the ids that start with ReservedWorkspacePrefix.
func withoutReserved(ids []string) []string {
	return slices.DeleteFunc(ids, func(id string) bool { return strings.HasPrefix(id, ReservedWorkspacePrefix) })
}

// ConsumerShadowWrite is one row's shadow update produced by a drained page: the
// snapshot a consumer wants to remember for (Entity, Key), or a deletion when
// Snapshot is nil. The bytes are opaque to the engine — the consumer decides
// what a shadow is and is the only thing that reads it back.
type ConsumerShadowWrite struct {
	Entity   string
	Key      string
	Snapshot []byte
}

// ConsumerUpdate is the durable bookkeeping one drained page produces: how far
// the consumer got, and the shadows it derived along the way.
type ConsumerUpdate struct {
	// Cursor is the position the consumer has now fully acted on. Its Epoch
	// and Lineage are not retained and ConsumerPosition reads them back as 0:
	// the position is held in the workspace's own store, so it is swapped or
	// restored along with the changes it points into. AdvanceConsumer checks
	// them against the store before it records the position (see
	// ErrCursorForeign).
	Cursor Cursor
	// Shadows are the per-row snapshot writes to land with that position.
	Shadows []ConsumerShadowWrite
	// From, when set, is the position the caller read before it acted on the
	// page, as ConsumerPosition returned it. AdvanceConsumer then records
	// nothing and returns ErrConsumerMoved unless the stored position is still
	// From. Only Seq, Entity and Key are compared, and a consumer with no
	// position matches the zero cursor. Nil skips the check.
	From *Cursor
	// Retain marks the consumer as retained: garbage collection (see
	// WithGC) never removes it, also when its position stays below the
	// tombstone horizon. The mark is sticky: an update with Retain false
	// keeps it. ResetConsumer removes it, together with the position and the
	// shadows.
	//
	// Set it for a consumer whose shadows stay valid below the horizon and
	// that heals by itself when Changes returns ErrCursorTooOld. A retained
	// consumer that is no longer used stays in each workspace until
	// ResetConsumer removes it.
	Retain bool
}

// ErrConsumerMoved is returned by AdvanceConsumer when ConsumerUpdate.From is
// set and the consumer's stored position is no longer From: another pass
// committed since the caller read it. Nothing is recorded. The caller's
// shadows were derived against the older position, so it reads the position
// again before it continues.
var ErrConsumerMoved = errors.New("server: consumer position moved since the update was read")

// ConsumerPosition returns where a named effect consumer has reached in the
// workspace's change feed, and whether it has ever recorded a position. A
// consumer with none reads as (zero cursor, false, nil) — a first run, which for
// a workspace that has collected tombstones means the first Changes call will
// ask for a resync (see ErrCursorTooOld).
func (s *Server) ConsumerPosition(ctx context.Context, workspaceID, consumer string) (Cursor, bool, error) {
	if err := validateConsumerName(consumer); err != nil {
		return Cursor{}, false, err
	}
	if workspaceID == "" {
		return Cursor{}, false, fmt.Errorf("server: workspace id is required")
	}
	var (
		cur   Cursor
		found bool
	)
	err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		pos, ok, err := tx.ConsumerPosition(consumer)
		if err != nil {
			return err
		}
		cur, found = Cursor{Seq: pos.Seq, Entity: pos.Entity, Key: pos.Key}, ok
		return nil
	})
	if err != nil {
		return Cursor{}, false, err
	}
	return cur, found, nil
}

// ConsumerPositions returns every effect consumer's position in the workspace,
// ordered by name, for reporting how far each has fallen behind CurrentSeq.
func (s *Server) ConsumerPositions(ctx context.Context, workspaceID string) ([]storage.ConsumerPosition, error) {
	if workspaceID == "" {
		return nil, fmt.Errorf("server: workspace id is required")
	}
	var out []storage.ConsumerPosition
	err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		var err error
		out, err = tx.ListConsumerPositions()
		return err
	})
	return out, err
}

// ConsumerPendingWorkspaces returns up to limit workspaces where the named
// consumer has changes it has not acted on, ordered by id and starting strictly
// after `after` — pass "" to start, then the last id of a page to continue.
//
// It answers "where is my work" for a background consumer in one query, which
// is what keeps that question independent of how many workspaces exist: the
// alternative is a read transaction per workspace, almost all of which report
// nothing to do. A workspace the consumer has never acted on is pending, so a
// newly registered consumer discovers everything it has to catch up on here.
//
// The answer is optimistic and a snapshot: a listed workspace may hold nothing
// but changes the consumer filters out, and one written during the call may
// only appear on the next. It is never short, though — a workspace whose
// position was behind when the call ran is always listed — because for a
// consumer that finds work this way, an omission is an effect that never fires.
func (s *Server) ConsumerPendingWorkspaces(ctx context.Context, consumer, after string, limit int) ([]string, error) {
	if err := validateConsumerName(consumer); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("server: limit must be positive, got %d", limit)
	}
	// The reserved ids are dropped after the store's limit, so a page that
	// held one is topped up from after its last id: callers take a short page
	// as the last one.
	var out []string
	for len(out) < limit {
		want := limit - len(out)
		ids, err := s.store.ListPendingWorkspaceIDs(ctx, consumer, storage.Range{After: after}, want)
		if err != nil {
			return nil, err
		}
		if len(ids) > 0 {
			after = ids[len(ids)-1]
		}
		exhausted := len(ids) < want
		out = append(out, withoutReserved(ids)...)
		if exhausted {
			break
		}
	}
	return out, nil
}

// pendingPage is how many workspaces PendingWorkspaces asks the store for per
// step.
const pendingPage = 512

// PendingWorkspaces yields every workspace where the named consumer has changes
// it has not acted on, in id order. It pages through ConsumerPendingWorkspaces
// and keeps its guarantees: a workspace whose position was behind when its page
// was read is always yielded, and a listed workspace may hold only changes the
// consumer filters out. Each page is its own snapshot, so a workspace written
// during the iteration may or may not be yielded.
//
// An error ends the sequence: PendingWorkspaces yields it with an empty id and
// stops. Each page is one store query, run when the loop reaches it.
func (s *Server) PendingWorkspaces(ctx context.Context, consumer string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if err := validateConsumerName(consumer); err != nil {
			yield("", err)
			return
		}
		after := ""
		for {
			ids, err := s.store.ListPendingWorkspaceIDs(ctx, consumer, storage.Range{After: after}, pendingPage)
			if err != nil {
				yield("", err)
				return
			}
			for _, id := range ids {
				if strings.HasPrefix(id, ReservedWorkspacePrefix) {
					continue
				}
				if !yield(id, nil) {
					return
				}
			}
			if len(ids) < pendingPage {
				return
			}
			after = ids[len(ids)-1]
		}
	}
}

// AdvanceConsumer commits a consumer's new feed position together with the
// shadow writes derived from the page it covers, in ONE transaction.
//
// The atomicity is the point, not an optimization. An effect has no reconcile
// pass, so the only recovery from a crash mid-page is redelivery — and
// redelivery only re-fires the handler if the shadows still describe the state
// the consumer acted on last time. A shadow that advanced without its position
// makes the redelivered change compare equal to it, and the trigger is lost
// silently. Committing both together makes every crash a duplicate instead,
// which is what the at-least-once contract already promises.
//
// Callers therefore run handlers FIRST and call this after, never the reverse.
// It opens a write transaction, so it must not be called from inside a mutator;
// a context threaded from one fails fast rather than deadlocking.
//
// It returns ErrCursorForeign and records nothing when up.Cursor names a
// history the store does not hold: a nonzero Epoch or Lineage that the store
// does not report, or a Seq above the current sequence. That is the case when
// the workspace was deleted and restored, or the store swapped or restored,
// after Changes returned the cursor. A cursor with zero Epoch and Lineage is
// checked by its Seq alone.
//
// It returns ErrConsumerMoved and records nothing when up.From is set and the
// stored position differs from it.
func (s *Server) AdvanceConsumer(ctx context.Context, workspaceID, consumer string, up ConsumerUpdate) error {
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: AdvanceConsumer called re-entrantly inside a write transaction (workspace %q): effect handlers run post-commit, never inside the write they react to", workspaceID)
	}
	return s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		// The page was read in an earlier transaction. A position from a
		// history the store no longer holds would skip every change of the
		// new history at or below its sequence, with no error anywhere.
		if foreign, err := cursorForeign(tx, up.Cursor); err != nil {
			return err
		} else if foreign {
			return ErrCursorForeign
		}
		if up.From != nil {
			// The read and this write are in different transactions, and a
			// pass that lost its lease can still be running. Without this
			// check its older position and shadows would replace the newer
			// ones another pass committed in between.
			pos, _, err := tx.ConsumerPosition(consumer)
			if err != nil {
				return err
			}
			if pos.Seq != up.From.Seq || pos.Entity != up.From.Entity || pos.Key != up.From.Key {
				return ErrConsumerMoved
			}
		}
		for _, sh := range up.Shadows {
			if sh.Snapshot == nil {
				if err := tx.DeleteConsumerShadow(consumer, sh.Entity, sh.Key); err != nil {
					return err
				}
				continue
			}
			if err := tx.PutConsumerShadow(consumer, sh.Entity, sh.Key, sh.Snapshot); err != nil {
				return err
			}
		}
		return tx.SetConsumerPosition(consumer, storage.ConsumerPosition{
			Seq:    up.Cursor.Seq,
			Entity: up.Cursor.Entity,
			Key:    up.Cursor.Key,
			Retain: up.Retain,
		})
	})
}

// cursorForeign reports whether cur names a history tx does not hold: another
// store lineage, a sequence above the current one, or a write the store holds
// under another mark. Zero marks are unknown and pass, as in Changes.
func cursorForeign(tx storage.ReadTx, cur Cursor) (bool, error) {
	epoch, err := tx.StoreEpoch()
	if err != nil {
		return false, err
	}
	if cur.Epoch != 0 && cur.Epoch != epoch {
		return true, nil
	}
	current, err := tx.CurrentSeq()
	if err != nil {
		return false, err
	}
	if cur.Seq > current {
		return true, nil
	}
	return lineageForeign(tx, cursorState{seq: cur.Seq, lineage: cur.Lineage})
}

// ConsumerShadow returns the snapshot a consumer recorded for one row, or nil
// if it holds none — which is both "never seen" and "keeps no shadow of it".
func (s *Server) ConsumerShadow(ctx context.Context, workspaceID, consumer, entity, key string) ([]byte, error) {
	if err := validateConsumerName(consumer); err != nil {
		return nil, err
	}
	if workspaceID == "" {
		return nil, fmt.Errorf("server: workspace id is required")
	}
	var snapshot []byte
	err := s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		var err error
		snapshot, err = tx.ConsumerShadow(consumer, entity, key)
		return err
	})
	return snapshot, err
}

// ScanConsumerShadows calls fn for every shadow a consumer holds for one entity
// name in the workspace, in bytewise key order. The entity name is the one the
// shadows were written under and need not be a registered entity. A consumer or
// entity with no shadows calls fn zero times.
//
// The scan runs in ONE read transaction, so fn sees one snapshot of all the
// shadows: a write that commits while the scan runs is not seen. fn runs inside
// that transaction and must not start a write; to read and write shadows
// together, use EntityTx.ScanConsumerShadows inside Mutate. When fn returns an
// error, the scan stops and returns it.
//
// It refuses a consumer name that is empty, longer than MaxConsumerNameBytes,
// or starts with ReservedConsumerPrefix.
func (s *Server) ScanConsumerShadows(ctx context.Context, workspaceID, consumer, entity string, fn func(key string, snapshot []byte) error) error {
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if fn == nil {
		return fmt.Errorf("server: ScanConsumerShadows needs a func")
	}
	return s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		return tx.ScanConsumerShadows(consumer, entity, "", 0, fn)
	})
}

// ResetConsumer drops a consumer's position, its retain mark (see
// ConsumerUpdate.Retain) and every shadow it holds in the workspace, so its
// next pass is a first run.
//
// This is what a consumer does after falling below the tombstone horizon: the
// changes its shadows describe have been collected, so the shadows record a
// past that nothing arriving now can be compared against. Keeping them would
// report field changes against a state the row left long ago.
func (s *Server) ResetConsumer(ctx context.Context, workspaceID, consumer string) error {
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: ResetConsumer called re-entrantly inside a write transaction (workspace %q): it opens a write transaction that would deadlock against the running one", workspaceID)
	}
	return s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		return tx.DeleteConsumer(consumer)
	})
}

func validateConsumerName(consumer string) error {
	if consumer == "" {
		return fmt.Errorf("server: consumer name is required")
	}
	if len(consumer) > MaxConsumerNameBytes {
		return fmt.Errorf("server: consumer name is %d bytes, limit is %d", len(consumer), MaxConsumerNameBytes)
	}
	if strings.HasPrefix(consumer, ReservedConsumerPrefix) {
		return fmt.Errorf("server: consumer name %q starts with %q, which the engine reserves", consumer, ReservedConsumerPrefix)
	}
	return nil
}

// ConsumerShadow returns the snapshot a consumer recorded for one key of one
// entity name, or nil if it holds none, as Server.ConsumerShadow does. It reads
// inside this transaction, so it sees the transaction's own shadow writes.
//
// It refuses a consumer name that is empty, longer than MaxConsumerNameBytes,
// or starts with ReservedConsumerPrefix.
func (e *EntityTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	if err := validateConsumerName(consumer); err != nil {
		return nil, err
	}
	return e.tx.ConsumerShadow(consumer, entity, key)
}

// PutConsumerShadow records snapshot as a consumer's shadow for one key of one
// entity name, and replaces the shadow that was there. The entity name need not
// be a registered entity. The bytes are opaque to the server. An empty snapshot
// can read back as nil, which is also what ConsumerShadow returns when there is
// no shadow.
//
// The shadow commits with the rows the transaction writes, and rolls back with
// them, so a connector can write rows and the shadow that describes them with
// no crash between the two. A shadow is local bookkeeping and not a row: it
// takes no sequence, adds nothing to the change feed, notifies no client, and
// is not passed to the WriteCapture. A transaction that writes only shadows
// takes no sequence and Seq stays 0, but the workspace is registered as after
// any other write.
//
// It refuses a consumer name that is empty, longer than MaxConsumerNameBytes,
// or starts with ReservedConsumerPrefix.
func (e *EntityTx) PutConsumerShadow(consumer, entity, key string, snapshot []byte) error {
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	return e.tx.PutConsumerShadow(consumer, entity, key, snapshot)
}

// DeleteConsumerShadow removes a consumer's shadow for one key of one entity
// name. Removing a shadow that does not exist is not an error. It commits and
// rolls back with the transaction, as PutConsumerShadow does.
//
// It refuses a consumer name that is empty, longer than MaxConsumerNameBytes,
// or starts with ReservedConsumerPrefix.
func (e *EntityTx) DeleteConsumerShadow(consumer, entity, key string) error {
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	return e.tx.DeleteConsumerShadow(consumer, entity, key)
}

// ScanConsumerShadows calls fn for up to limit shadows a consumer holds for one
// entity name with key > afterKey, in bytewise key order. It returns the key of
// the last shadow visited and whether more follow; pass lastKey back as
// afterKey to continue. limit must be positive. The scan sees the
// transaction's own shadow writes.
//
// The page is read in full before fn runs, so fn may read and write shadows
// and rows through this transaction. A shadow fn writes after the page was read
// is seen by the next page when its key sorts after lastKey. When fn returns an
// error, the scan stops and returns it.
//
// It refuses a consumer name that is empty, longer than MaxConsumerNameBytes,
// or starts with ReservedConsumerPrefix.
func (e *EntityTx) ScanConsumerShadows(consumer, entity, afterKey string, limit int, fn func(key string, snapshot []byte) error) (lastKey string, more bool, err error) {
	if err := validateConsumerName(consumer); err != nil {
		return "", false, err
	}
	if limit <= 0 {
		return "", false, fmt.Errorf("server: ScanConsumerShadows limit must be positive, got %d", limit)
	}
	if fn == nil {
		return "", false, fmt.Errorf("server: ScanConsumerShadows needs a func")
	}
	// Buffered because an adapter need not tolerate a write while its scan
	// is open (see the storage package contract), and a caller that rewrites
	// the shadows it scans is the expected use.
	type entry struct {
		key      string
		snapshot []byte
	}
	page := make([]entry, 0, min(limit+1, 256))
	if err := e.tx.ScanConsumerShadows(consumer, entity, afterKey, limit+1, func(key string, snapshot []byte) error {
		page = append(page, entry{key, snapshot})
		return nil
	}); err != nil {
		return "", false, err
	}
	if len(page) > limit {
		page, more = page[:limit], true
	}
	for _, en := range page {
		if err := fn(en.key, en.snapshot); err != nil {
			return "", false, err
		}
		lastKey = en.key
	}
	return lastKey, more, nil
}
