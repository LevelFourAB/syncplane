package server

import (
	"errors"
	"time"

	"github.com/levelfourab/syncplane/engine/storage"
)

// failureTx is a storage.WriteTx that records the first error the storage
// adapter itself returned. A custom mutator body usually hands a failed read or
// write straight back, and the push must report that as the store's failure
// (CodeInternal, retried without a limit) and not as the mutator's own
// (CodeMutationFailed, counted toward dropping the mutation). The error a body
// returns cannot tell the two apart; the transaction it ran over can.
//
// Errors that are answers and not failures are not recorded: ErrNotFound, and
// an error a callback returned, which a scan or savepoint only passes back.
//
// Every method of the interface is overridden, so a failure through any of
// them is seen. A method added to storage.WriteTx is forwarded by the embedded
// interface and is not recorded until it is overridden here.
type failureTx struct {
	storage.WriteTx
	failed error
}

// baseTx returns the transaction tx wraps, or tx itself. Caches keyed on a
// transaction compare through it, so the wrapper reads as the same
// transaction.
func baseTx(tx storage.ReadTx) storage.ReadTx {
	if f, ok := tx.(*failureTx); ok {
		return f.WriteTx
	}
	return tx
}

func (t *failureTx) note(err error) error {
	if err != nil && t.failed == nil && !errors.Is(err, storage.ErrNotFound) {
		t.failed = err
	}
	return err
}

// noteAround records err unless it is the error the callback returned.
func (t *failureTx) noteAround(err, own error) error {
	if err != nil && (own == nil || !errors.Is(err, own)) {
		t.note(err)
	}
	return err
}

func (t *failureTx) CurrentSeq() (uint64, error) {
	v, err := t.WriteTx.CurrentSeq()
	return v, t.note(err)
}

func (t *failureTx) TombstoneHorizon() (uint64, error) {
	v, err := t.WriteTx.TombstoneHorizon()
	return v, t.note(err)
}

func (t *failureTx) StoreEpoch() (uint64, error) {
	v, err := t.WriteTx.StoreEpoch()
	return v, t.note(err)
}

func (t *failureTx) SeqLineage(seq uint64) (uint64, error) {
	v, err := t.WriteTx.SeqLineage(seq)
	return v, t.note(err)
}

func (t *failureTx) GetRow(entity, key string) (storage.Row, error) {
	r, err := t.WriteTx.GetRow(entity, key)
	return r, t.note(err)
}

func (t *failureTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	var own error
	err := t.WriteTx.GetRows(entity, keys, func(r storage.Row) error { own = fn(r); return own })
	return t.noteAround(err, own)
}

func (t *failureTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	var own error
	err := t.WriteTx.ScanEntity(entity, func(r storage.Row) error { own = fn(r); return own })
	return t.noteAround(err, own)
}

func (t *failureTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	var own error
	err := t.WriteTx.ScanEntityAfter(entity, afterKey, limit, func(r storage.Row) error { own = fn(r); return own })
	return t.noteAround(err, own)
}

func (t *failureTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	var own error
	err := t.WriteTx.ScanEntityByVersionDesc(entity, limit, func(r storage.Row) error { own = fn(r); return own })
	return t.noteAround(err, own)
}

func (t *failureTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	var own error
	err := t.WriteTx.ScanChanges(afterSeq, afterEntity, afterKey, limit, func(r storage.Row) error { own = fn(r); return own })
	return t.noteAround(err, own)
}

func (t *failureTx) ClientLastMutationID(clientID string) (uint64, error) {
	v, err := t.WriteTx.ClientLastMutationID(clientID)
	return v, t.note(err)
}

func (t *failureTx) ClientRejections(clientID string) ([]storage.ClientRejection, error) {
	v, err := t.WriteTx.ClientRejections(clientID)
	return v, t.note(err)
}

func (t *failureTx) ClientAppliedMutations(clientID string) ([]storage.AppliedMutation, error) {
	v, err := t.WriteTx.ClientAppliedMutations(clientID)
	return v, t.note(err)
}

func (t *failureTx) IdempotencyRecord(key string) (storage.IdempotencyRecord, bool, error) {
	v, found, err := t.WriteTx.IdempotencyRecord(key)
	return v, found, t.note(err)
}

func (t *failureTx) ConsumerPosition(consumer string) (storage.ConsumerPosition, bool, error) {
	v, found, err := t.WriteTx.ConsumerPosition(consumer)
	return v, found, t.note(err)
}

func (t *failureTx) ListConsumerPositions() ([]storage.ConsumerPosition, error) {
	v, err := t.WriteTx.ListConsumerPositions()
	return v, t.note(err)
}

func (t *failureTx) ConsumerShadow(consumer, entity, key string) ([]byte, error) {
	v, err := t.WriteTx.ConsumerShadow(consumer, entity, key)
	return v, t.note(err)
}

func (t *failureTx) FieldDeltas(entity, key, field string, afterSeq uint64) ([][]byte, bool, error) {
	v, complete, err := t.WriteTx.FieldDeltas(entity, key, field, afterSeq)
	return v, complete, t.note(err)
}

func (t *failureTx) FieldDeltaHorizon() (uint64, error) {
	v, err := t.WriteTx.FieldDeltaHorizon()
	return v, t.note(err)
}

func (t *failureTx) TextFloor(entity, key, field string) ([]byte, error) {
	v, err := t.WriteTx.TextFloor(entity, key, field)
	return v, t.note(err)
}

func (t *failureTx) TextCompactMark() (storage.TextCompactMark, error) {
	v, err := t.WriteTx.TextCompactMark()
	return v, t.note(err)
}

func (t *failureTx) ScanIndex(
	entity, index string, lo, hi []byte, limit int, order storage.ScanOrder,
	fn func(entry []byte, rowKey string) error,
) error {
	var own error
	err := t.WriteTx.ScanIndex(entity, index, lo, hi, limit, order, func(entry []byte, rowKey string) error {
		own = fn(entry, rowKey)
		return own
	})
	return t.noteAround(err, own)
}

func (t *failureTx) ScanIndexRanges(
	entity, index string, ranges []storage.IndexRange,
	fn func(i int, entry []byte, rowKey string) error,
) error {
	var own error
	err := t.WriteTx.ScanIndexRanges(entity, index, ranges, func(i int, entry []byte, rowKey string) error {
		own = fn(i, entry, rowKey)
		return own
	})
	return t.noteAround(err, own)
}

func (t *failureTx) IndexState(entity, index string) (string, bool, error) {
	v, ready, err := t.WriteTx.IndexState(entity, index)
	return v, ready, t.note(err)
}

func (t *failureTx) IndexStates(entity string) ([]storage.IndexStatus, error) {
	v, err := t.WriteTx.IndexStates(entity)
	return v, t.note(err)
}

func (t *failureTx) NextSeq() (uint64, error) {
	v, err := t.WriteTx.NextSeq()
	return v, t.note(err)
}

func (t *failureTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	return t.note(t.WriteTx.PutRow(entity, key, value, version, actor))
}

func (t *failureTx) DeleteRow(entity, key string, version uint64, actor storage.Actor) error {
	return t.note(t.WriteTx.DeleteRow(entity, key, version, actor))
}

func (t *failureTx) SetClientLastMutationID(clientID string, mutationID uint64) error {
	return t.note(t.WriteTx.SetClientLastMutationID(clientID, mutationID))
}

func (t *failureTx) SetClientRejections(clientID string, recs []storage.ClientRejection) error {
	return t.note(t.WriteTx.SetClientRejections(clientID, recs))
}

func (t *failureTx) SetClientAppliedMutations(clientID string, recs []storage.AppliedMutation) error {
	return t.note(t.WriteTx.SetClientAppliedMutations(clientID, recs))
}

func (t *failureTx) InSavepoint(fn func() error) error {
	var own error
	err := t.WriteTx.InSavepoint(func() error { own = fn(); return own })
	return t.noteAround(err, own)
}

func (t *failureTx) CollectTombstones(cutoff time.Time) (uint64, error) {
	v, err := t.WriteTx.CollectTombstones(cutoff)
	return v, t.note(err)
}

func (t *failureTx) PruneSeqLineage(cutoff time.Time) (int, error) {
	v, err := t.WriteTx.PruneSeqLineage(cutoff)
	return v, t.note(err)
}

func (t *failureTx) PurgeEntityRows(entity, keyPrefix string, before time.Time, limit int) (int, bool, error) {
	v, more, err := t.WriteTx.PurgeEntityRows(entity, keyPrefix, before, limit)
	return v, more, t.note(err)
}

func (t *failureTx) ExpireClientRejections(cutoff time.Time) (int, error) {
	v, err := t.WriteTx.ExpireClientRejections(cutoff)
	return v, t.note(err)
}

func (t *failureTx) CollectIdleClients(cutoff time.Time) (int, error) {
	v, err := t.WriteTx.CollectIdleClients(cutoff)
	return v, t.note(err)
}

func (t *failureTx) PutIdempotencyRecord(key string, rec storage.IdempotencyRecord) error {
	return t.note(t.WriteTx.PutIdempotencyRecord(key, rec))
}

func (t *failureTx) CollectIdempotencyKeys(cutoff time.Time) (int, error) {
	v, err := t.WriteTx.CollectIdempotencyKeys(cutoff)
	return v, t.note(err)
}

func (t *failureTx) SetConsumerPosition(consumer string, p storage.ConsumerPosition) error {
	return t.note(t.WriteTx.SetConsumerPosition(consumer, p))
}

func (t *failureTx) PutConsumerShadow(consumer, entity, key string, snapshot []byte) error {
	return t.note(t.WriteTx.PutConsumerShadow(consumer, entity, key, snapshot))
}

func (t *failureTx) DeleteConsumerShadow(consumer, entity, key string) error {
	return t.note(t.WriteTx.DeleteConsumerShadow(consumer, entity, key))
}

func (t *failureTx) DeleteConsumer(consumer string) error {
	return t.note(t.WriteTx.DeleteConsumer(consumer))
}

func (t *failureTx) CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (int, error) {
	v, err := t.WriteTx.CollectIdleConsumers(cutoff, belowSeq)
	return v, t.note(err)
}

func (t *failureTx) PutFieldDelta(entity, key, field string, seq uint64, frag []byte) error {
	return t.note(t.WriteTx.PutFieldDelta(entity, key, field, seq, frag))
}

func (t *failureTx) PruneFieldDeltas(cutoff time.Time) (uint64, error) {
	v, err := t.WriteTx.PruneFieldDeltas(cutoff)
	return v, t.note(err)
}

func (t *failureTx) PutTextFloor(entity, key, field string, vv []byte) error {
	return t.note(t.WriteTx.PutTextFloor(entity, key, field, vv))
}

func (t *failureTx) PutIndexEntries(entity, key string, entries []storage.IndexEntry) error {
	return t.note(t.WriteTx.PutIndexEntries(entity, key, entries))
}

func (t *failureTx) DeleteIndexEntries(entity, key string) error {
	return t.note(t.WriteTx.DeleteIndexEntries(entity, key))
}

func (t *failureTx) SetIndexState(entity, index, specHash string, ready bool) error {
	return t.note(t.WriteTx.SetIndexState(entity, index, specHash, ready))
}

func (t *failureTx) SetIndexBuildMark(mark string) error {
	return t.note(t.WriteTx.SetIndexBuildMark(mark))
}

func (t *failureTx) SetTextCompactMark(mark storage.TextCompactMark) error {
	return t.note(t.WriteTx.SetTextCompactMark(mark))
}
