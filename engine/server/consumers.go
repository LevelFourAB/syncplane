package server

import (
	"context"
	"fmt"

	"github.com/levelfourab/syncplane/engine/storage"
)

// MaxConsumerNameBytes bounds an effect consumer's name. The name is the
// identity of a durable position and is stored per workspace, so the bound is
// what keeps a caller-supplied string from becoming unbounded storage. It is
// generous against anything a human names a consumer.
const MaxConsumerNameBytes = 128

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
	// Cursor is the position the consumer has now fully acted on.
	Cursor Cursor
	// Shadows are the per-row snapshot writes to land with that position.
	Shadows []ConsumerShadowWrite
}

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
	return s.store.ListPendingWorkspaceIDs(ctx, consumer, after, limit)
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
		})
	})
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

// ResetConsumer drops a consumer's position and every shadow it holds in the
// workspace, so its next pass is a first run.
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
	return nil
}
