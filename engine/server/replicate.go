package server

import (
	"bytes"
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// upstreamActorName is the label Replicate records on its rows when ctx names
// no writer with TrustedAs.
const upstreamActorName = "upstream"

// Replicate runs fn in a write transaction whose writes store rows exactly as
// given, for a link that keeps this server's workspace converged with another
// server. fn's writes are trusted and are not changed: no permission hook runs,
// no compare-and-swap applies, a collaborative-text field is replaced rather
// than unioned, and a field the caller may not write or a creator field keeps
// the value written. Every structure the server derives from rows is kept as
// after any other write: indexes, search, the text delta log, history, the
// access model, and the change feed with its notifications to clients.
//
// All writes of one call take one workspace sequence, allocated on the first
// write; a call that writes nothing takes none and notifies nobody. Its rows
// are attributed to Actor{System: name} for a ctx marked with TrustedAs(ctx,
// name), and to Actor{System: "upstream"} otherwise. Replicate writes are
// never passed to the WriteCapture.
//
// fn may run more than once (the storage adapter may retry the transaction),
// so keep side effects out of it until Replicate returns nil. An error fn
// returns rolls the transaction back and is returned. Do not call Replicate,
// or any other write call, from inside fn: it deadlocks.
func (s *Server) Replicate(ctx context.Context, workspaceID string, fn func(tx *ReplicaTx) error) error {
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: Replicate called re-entrantly inside a write transaction (workspace %q): a nested write transaction waits for the write lock the enclosing one holds and would deadlock", workspaceID)
	}
	name := trustedName(ctx)
	if name == "" {
		name = upstreamActorName
	}
	ctx = TrustedAs(ctx, name)
	actor, err := actorOf(ctx)
	if err != nil {
		return err
	}
	wrote := false
	err = s.withEntityWriteTx(ctx, workspaceID, func(tx storage.WriteTx, guarded bool) error {
		rtx := &ReplicaTx{srv: s, ctx: ctx, ws: workspaceID, tx: tx, actor: actor, guarded: guarded}
		if err := fn(rtx); err != nil {
			return err
		}
		if rtx.spoiled {
			return errNeedsSavepoints
		}
		wrote = rtx.seq != 0
		return nil
	})
	if err != nil {
		return err
	}
	if wrote {
		s.notifier.Notify(workspaceID)
	}
	return nil
}

// ReadState runs fn in a read-only snapshot of the workspace's storage, for a
// link that keeps state beside the rows (see ReplicaTx.Tx). It is trusted:
// no permission hook runs and no read rule applies. fn runs at most once and
// must not start a write. An error fn returns is returned.
func (s *Server) ReadState(ctx context.Context, workspaceID string, fn func(tx storage.ReadTx) error) error {
	if workspaceID == "" {
		return fmt.Errorf("server: workspace id is required")
	}
	return s.store.WithReadTx(ctx, workspaceID, fn)
}

// ReplicaTx is the transaction a Replicate call's fn writes through. It is
// valid only during fn and is not safe for concurrent use.
//
// A Put or Delete that returns an error writes nothing.
type ReplicaTx struct {
	srv   *Server
	ctx   context.Context
	ws    string
	tx    storage.WriteTx
	actor storage.Actor
	// seq is the sequence every write takes, 0 until the first write.
	seq uint64
	// writes counts the writes that reached writeSeq. Every derived-state write
	// comes after it, so a write that failed with the count unchanged wrote
	// nothing.
	writes uint64
	// guarded and spoiled are EntityTx's: a write runs in its own savepoint
	// only on the attempt that follows one whose failed write left derived
	// state behind.
	guarded bool
	spoiled bool
}

// Workspace returns the workspace the transaction writes to.
func (r *ReplicaTx) Workspace() string { return r.ws }

// Seq returns the sequence the transaction's writes take, or 0 before the
// first write.
func (r *ReplicaTx) Seq() uint64 { return r.seq }

// Tx returns the storage transaction, for state the caller keeps beside the
// rows, such as consumer shadows (see storage.WriteTx.PutConsumerShadow). What
// is written through it commits or rolls back with the call. A row written
// through it skips every structure the server derives from rows; write rows
// with Put and Delete.
func (r *ReplicaTx) Tx() storage.WriteTx { return r.tx }

// Get returns the stored row for entity and key as it stands in this
// transaction, including the transaction's own writes. found is false for a
// row that was never written or whose tombstone was collected; a tombstone is
// found, with Deleted set.
func (r *ReplicaTx) Get(entity, key string) (row storage.Row, found bool, err error) {
	row, err = r.tx.GetRow(entity, key)
	if err == storage.ErrNotFound {
		return storage.Row{}, false, nil
	}
	if err != nil {
		return storage.Row{}, false, err
	}
	return row, true, nil
}

// Put stores value as the row of entity with the given key. Writing the bytes
// the live row already holds does nothing.
//
// It returns an error when entity is not registered, when value does not
// decode as the entity's message, or when its key fields do not derive key. It
// returns a Rejection (see AsRejection) for a key the naming rules refuse, and
// for a row this server cannot store: a keyed list that holds one key twice,
// an invalid grant row, an access chain past Limits.MaxAccessDepth or
// Limits.MaxAccessParents, or more index entries than the Limits allow.
func (r *ReplicaTx) Put(entity, key string, value []byte) error {
	return r.atomically(func() error { return r.put(entity, key, value) })
}

// Delete writes a tombstone for the row of entity with the given key. Deleting
// a row that is absent or already deleted does nothing.
//
// It returns an error when entity is not registered, and a Rejection (see
// AsRejection) for a key the naming rules refuse.
func (r *ReplicaTx) Delete(entity, key string) error {
	return r.atomically(func() error { return r.delete(entity, key) })
}

// atomically runs one write so that a failed write leaves nothing behind,
// exactly as EntityTx.atomically does.
func (r *ReplicaTx) atomically(write func() error) error {
	if !r.guarded {
		writes := r.writes
		err := write()
		if err != nil && r.writes != writes {
			r.spoiled = true
		}
		return err
	}
	seq := r.seq
	err := r.tx.InSavepoint(write)
	if err != nil {
		r.seq = seq
	}
	return err
}

func (r *ReplicaTx) writeSeq() (uint64, error) {
	if r.seq == 0 {
		seq, err := r.tx.NextSeq()
		if err != nil {
			return 0, err
		}
		r.seq = seq
	}
	r.writes++
	return r.seq, nil
}

func (r *ReplicaTx) entity(name string) (*entityInfo, error) {
	info, ok := r.srv.catalogPtr().entities[name]
	if !ok {
		return nil, fmt.Errorf("server: Replicate: entity %q is not registered", name)
	}
	return info, nil
}

func (r *ReplicaTx) put(entity, key string, value []byte) error {
	s := r.srv
	info, err := r.entity(entity)
	if err != nil {
		return err
	}
	if err := info.checkKey(key); err != nil {
		return err
	}
	msg, err := info.decode(value)
	if err != nil {
		return fmt.Errorf("server: Replicate: %w", err)
	}
	if derived := info.keyOf(msg.ProtoReflect()); derived != key {
		return fmt.Errorf("server: Replicate: entity %q: key %q does not match key fields (%q)", info.name, key, derived)
	}
	row, live, err := r.liveRow(info, key)
	if err != nil {
		return err
	}
	if live && bytes.Equal(row.Value, value) {
		return nil
	}
	load := func() (storage.Row, bool, error) { return row, live, nil }
	model := s.catalogPtr().access
	var (
		prevText map[string][]byte
		prevRead readInputs
		prior    priorPostings
	)
	if live {
		if base, decErr := info.decode(row.Value); decErr != nil {
			s.reportPoison(r.ctx, r.ws, row, decErr)
		} else {
			prevText = info.textSnapshot(base)
			prevRead = readInputsOf(model, info, base)
			prior = info.priorOf(base)
		}
	}
	// The checks every write path runs, trusted ones included: they keep what
	// the stored rows may look like, which the readers of this store rely on
	// whoever wrote the row.
	if err := info.checkItemKeys(msg.ProtoReflect()); err != nil {
		return err
	}
	if err := s.checkGrantRow(info, key, syncplanev1.CrudOp_CRUD_OP_PUT, nil, msg); err != nil {
		return err
	}
	if err := s.checkAccessChain(r.ctx, r.ws, r.tx, info, key, load, msg); err != nil {
		return err
	}
	seq, err := r.writeSeq()
	if err != nil {
		return err
	}
	// recordTextDeltas logs a gap marker for a document that does not extend
	// the stored one, which is what a replaced (for example reverted) document
	// is: a reader then gets it whole rather than folding a fragment into
	// state the document no longer holds.
	if err := info.recordTextDeltas(r.tx, key, prevText, msg.ProtoReflect(), seq); err != nil {
		return err
	}
	if err := info.recordReadGaps(r.tx, model, key, prevRead, msg, seq); err != nil {
		return err
	}
	if err := info.dropRemovedTextInstances(r.tx, s.textCache, r.ws, key, prevText, msg.ProtoReflect()); err != nil {
		return err
	}
	if err := info.dropUncoveredTextFloors(r.tx, s.textCache, r.ws, key, prevText, msg); err != nil {
		return err
	}
	if err := info.updateRowIndexes(r.tx, s.limits, key, prior, msg); err != nil {
		return err
	}
	if err := s.captureHistory(r.ctx, r.tx, r.ws, info, key, row, live, msg, nil, seq, r.actor); err != nil {
		return err
	}
	if err := s.recordAccessMove(r.ctx, r.ws, r.tx, info, key, load, msg, seq, r.actor); err != nil {
		return err
	}
	return r.tx.PutRow(info.name, key, value, seq, r.actor)
}

func (r *ReplicaTx) delete(entity, key string) error {
	s := r.srv
	info, err := r.entity(entity)
	if err != nil {
		return err
	}
	if err := info.checkKey(key); err != nil {
		return err
	}
	row, live, err := r.liveRow(info, key)
	if err != nil {
		return err
	}
	if !live {
		return nil
	}
	seq, err := r.writeSeq()
	if err != nil {
		return err
	}
	// The wire DELETE's sequence of derived-state removals; see applyCrud.
	textPaths := info.textStatePaths(nil)
	var prior priorPostings
	if info.hasNestedText() || info.search != nil {
		if decoded, decErr := info.decode(row.Value); decErr == nil {
			if info.hasNestedText() {
				textPaths = info.textStatePaths(decoded.ProtoReflect())
			}
			prior = info.priorOf(decoded)
		}
	}
	if err := info.recordTextDeltaGaps(r.tx, key, textPaths, seq); err != nil {
		return err
	}
	s.textCache.forget(r.ws, info.name, key, textPaths)
	if err := info.clearTextFloors(r.tx, key, textPaths); err != nil {
		return err
	}
	if err := info.clearRowIndexes(r.tx, key, prior); err != nil {
		return err
	}
	if err := s.purgeHistory(r.tx, info, key); err != nil {
		return err
	}
	if err := s.recordAccessMove(r.ctx, r.ws, r.tx, info, key,
		func() (storage.Row, bool, error) { return row, live, nil }, nil, seq, r.actor); err != nil {
		return err
	}
	return r.tx.DeleteRow(info.name, key, seq, r.actor)
}

func (r *ReplicaTx) liveRow(info *entityInfo, key string) (storage.Row, bool, error) {
	row, err := r.tx.GetRow(info.name, key)
	if err != nil && err != storage.ErrNotFound {
		return storage.Row{}, false, err
	}
	return row, err == nil && !row.Deleted, nil
}

// dropUncoveredTextFloors clears the compaction floor of every document of
// next that changed and no longer covers its floor, and drops the cached
// decode of every document that changed. prev is the snapshot of the row
// before the write.
//
// A merge keeps every document a superset of its floor. A replaced document
// need not be: one that lost operations the floor covers would refuse, as
// stale, every write of a peer that allocates its counters from the document,
// and no re-read could repair that. The floor describes a document the row no
// longer holds, so it goes.
func (info *entityInfo) dropUncoveredTextFloors(
	tx storage.WriteTx,
	cache *textDocCache,
	ws, key string,
	prev map[string][]byte,
	next proto.Message,
) error {
	if len(info.textFields) == 0 {
		return nil
	}
	after := info.textSnapshot(next)
	var changed []string
	for _, path := range info.textStatePaths(next.ProtoReflect()) {
		doc := after[path]
		if bytes.Equal(prev[path], doc) {
			continue
		}
		changed = append(changed, path)
		raw, err := tx.TextFloor(info.name, key, path)
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			continue
		}
		floor, err := text.DecodeVV(raw)
		if err != nil {
			return fmt.Errorf("server: entity %q text field %q: compaction floor: %w", info.name, path, err)
		}
		var have map[string]uint64
		if len(doc) > 0 {
			d, err := text.Decode(doc, "")
			if err != nil {
				return fmt.Errorf("server: entity %q text field %q: %w", info.name, path, err)
			}
			have = d.VersionVector()
		}
		for peer, counter := range floor {
			if have[peer] < counter {
				if err := tx.PutTextFloor(info.name, key, path, nil); err != nil {
					return err
				}
				break
			}
		}
	}
	cache.forget(ws, info.name, key, changed)
	return nil
}
