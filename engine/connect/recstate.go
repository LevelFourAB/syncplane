package connect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// Record mode keeps all its bookkeeping as shadows of the consumer
// Connector.consumer, under these entity names. None of them is a registered
// entity; the prefix keeps them apart from each other and from row mode,
// which keeps its shadows in State.
//
//   - rec:<root>   record key → recShadow, the agreed state of the record.
//   - loc:<root>   locator → record key.
//   - idx:<member> member row key → record key, for the rows of the agreed
//     state. It is what maps a deleted row, whose change carries no message,
//     to its record.
//   - dirty:<root> record key → dirtyMark, for records that wait for egress. They commit with the feed position,
//     so the position can move past a change while its record waits out
//     Records.Quiet, and a crash does not lose it.
//   - gone:<root>  record key → goneMark, for records missing from the store
//     that wait out Records.DeleteGrace.
//   - pend:<root>  record key → pendingWrite, for a store write or delete
//     whose outcome the agreed state does not hold yet.
//   - fp:<root>    agreed fingerprint → JSON list of record keys, at most
//     maxFingerprintKeys. It finds the record a touched new locator was
//     renamed from when the old locator is not reported first.
//   - refused:<root> record key → refusedMark, for records Connector.Refused
//     reported.
//   - kept:<root>  record key → keptContent, the store content the last
//     egress write or delete replaced (see Records.KeepReplaced).
func recEntity(rt *recordType) string   { return "rec:" + rt.rootName }
func locEntity(rt *recordType) string   { return "loc:" + rt.rootName }
func dirtyEntity(rt *recordType) string { return "dirty:" + rt.rootName }
func goneEntity(rt *recordType) string  { return "gone:" + rt.rootName }
func pendEntity(rt *recordType) string  { return "pend:" + rt.rootName }
func fpEntity(rt *recordType) string    { return "fp:" + rt.rootName }
func refusedEntity(rt *recordType) string {
	return "refused:" + rt.rootName
}
func keptEntity(rt *recordType) string { return "kept:" + rt.rootName }
func idxEntity(member string) string   { return "idx:" + member }

// maxFingerprintKeys bounds a fingerprint index entry. More records with one
// content tell no rename apart, and each agreed state rewrites the entry.
const maxFingerprintKeys = 16

// recShadow is the agreed state of one record.
type recShadow struct {
	Locator     string `json:"l"`
	Fingerprint string `json:"f"`
	// Hash is the SHA-256 of the agreed content. Egress compares a render
	// with it, because a fingerprint is the store's and cannot be computed
	// from bytes.
	Hash []byte `json:"h"`
	// Version is the engine sequence the agreed content reflects: a feed
	// change at or below it is already in the content.
	Version uint64 `json:"v"`
	Payload []byte `json:"p,omitempty"`
	// Members are the member row keys of the agreed state, by entity. The
	// shadow is written whole, so each agreed state costs O(members).
	Members map[string][]string `json:"m,omitempty"`
	// Ingested is when the record's last ingest wrote rows, in Unix ns; 0
	// for none.
	Ingested int64 `json:"i,omitempty"`
	// Alt is the state of a store write that the connector cannot tell was
	// done (see settleOne); nil for none. The next agreed state drops it.
	Alt *recAlt `json:"a,omitempty"`
}

// recAlt is the agreed state a store write would have made.
type recAlt struct {
	Payload []byte `json:"p,omitempty"`
}

// alternate returns the state of a write the connector cannot tell was done,
// as MergeInput.Alternate; nil for none.
func (sh *recShadow) alternate() *Agreed {
	if sh == nil || sh.Alt == nil {
		return nil
	}
	return &Agreed{Locator: sh.Locator, Payload: sh.Alt.Payload}
}

func (sh *recShadow) ingested() int64 {
	if sh == nil {
		return 0
	}
	return sh.Ingested
}

func (sh *recShadow) agreed() *Agreed {
	if sh == nil {
		return nil
	}
	return &Agreed{Locator: sh.Locator, Fingerprint: sh.Fingerprint, Payload: sh.Payload}
}

// goneMark is a record missing from the store.
type goneMark struct {
	Locator     string `json:"l"`
	Fingerprint string `json:"f"`
	// Since is when the record was found missing, in Unix ns.
	Since int64 `json:"s"`
}

// pendingWrite is a store write or delete that commits before the store
// call, so that the next pass can tell from the store whether it was done.
type pendingWrite struct {
	// Next is the agreed state after the write, with no fingerprint; nil for
	// a delete.
	Next *recShadow `json:"n,omitempty"`
	// From is the locator a write moves the record from.
	From string `json:"f,omitempty"`
	// Replaces is the hash of the content the store held at Next.Locator
	// before the write; nil when it held none.
	Replaces []byte `json:"r,omitempty"`
	// Delete is the locator a delete removes.
	Delete string `json:"d,omitempty"`
}

func decodePending(raw []byte) (*pendingWrite, error) {
	var pw pendingWrite
	if err := json.Unmarshal(raw, &pw); err != nil {
		return nil, fmt.Errorf("connect: decode pending write: %w", err)
	}
	return &pw, nil
}

func contentHash(content []byte) []byte {
	h := sha256.Sum256(content)
	return h[:]
}

func encodeJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		// The shadow types hold only strings, byte slices and numbers.
		panic(fmt.Sprintf("connect: encode shadow: %v", err))
	}
	return raw
}

func decodeRecShadow(raw []byte) (*recShadow, error) {
	if raw == nil {
		return nil, nil
	}
	var sh recShadow
	if err := json.Unmarshal(raw, &sh); err != nil {
		return nil, fmt.Errorf("connect: decode record shadow: %w", err)
	}
	return &sh, nil
}

func decodeGone(raw []byte) (*goneMark, error) {
	if raw == nil {
		return nil, nil
	}
	var g goneMark
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, fmt.Errorf("connect: decode missing-record mark: %w", err)
	}
	return &g, nil
}

// dirtyMark is a record that waits for egress: last is its latest engine
// change, and first the earliest one the store has not received.
type dirtyMark struct{ last, first time.Time }

// encodeDirty writes a mark as "<last>,<first>" in Unix ns.
func encodeDirty(m dirtyMark) []byte {
	return fmt.Appendf(nil, "%d,%d", m.last.UnixNano(), m.first.UnixNano())
}

// decodeDirty reads a mark. A mark of one number has first equal to last.
func decodeDirty(raw []byte) (dirtyMark, error) {
	last, first, two := strings.Cut(string(raw), ",")
	if !two {
		first = last
	}
	l, err := strconv.ParseInt(last, 10, 64)
	if err != nil {
		return dirtyMark{}, fmt.Errorf("connect: decode dirty mark: %w", err)
	}
	f, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return dirtyMark{}, fmt.Errorf("connect: decode dirty mark: %w", err)
	}
	return dirtyMark{last: time.Unix(0, l), first: time.Unix(0, f)}, nil
}

// due returns when the record's egress is due: Records.Quiet after the last
// change, but no later than Records.MaxDelay after the first.
func (r *records) due(m dirtyMark) time.Time {
	at := m.last.Add(max(r.quiet, 0))
	if r.maxDelay >= 0 {
		if limit := m.first.Add(r.maxDelay); limit.Before(at) {
			at = limit
		}
	}
	return at
}

// readShadow reads one shadow outside a transaction.
func (c *Connector) readShadow(ctx context.Context, ws, entity, key string) ([]byte, error) {
	return c.srv.ConsumerShadow(ctx, ws, c.consumer, entity, key)
}

func (c *Connector) loadRecShadow(ctx context.Context, ws string, rt *recordType, key string) (*recShadow, error) {
	raw, err := c.readShadow(ctx, ws, recEntity(rt), key)
	if err != nil {
		return nil, err
	}
	return decodeRecShadow(raw)
}

// shadowReader reads consumer shadows inside a transaction: a
// *server.EntityTx or a *server.ViewTx.
type shadowReader interface {
	ConsumerShadow(consumer, entity, key string) ([]byte, error)
}

func (c *Connector) txRecShadow(tx shadowReader, rt *recordType, key string) (*recShadow, error) {
	raw, err := tx.ConsumerShadow(c.consumer, recEntity(rt), key)
	if err != nil {
		return nil, err
	}
	return decodeRecShadow(raw)
}

// loadGone reads every missing-record mark of a type, by record key.
func (c *Connector) loadGone(ctx context.Context, ws string, rt *recordType) (map[string]*goneMark, error) {
	out := make(map[string]*goneMark)
	err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, goneEntity(rt), func(key string, raw []byte) error {
		g, err := decodeGone(raw)
		if err != nil {
			return err
		}
		out[key] = g
		return nil
	})
	return out, err
}

// loadRecShadows reads every record shadow of a type, by record key.
func (c *Connector) loadRecShadows(ctx context.Context, ws string, rt *recordType) (map[string]*recShadow, error) {
	out := make(map[string]*recShadow)
	err := c.srv.ScanConsumerShadows(ctx, ws, c.consumer, recEntity(rt), func(key string, raw []byte) error {
		sh, err := decodeRecShadow(raw)
		if err != nil {
			return err
		}
		out[key] = sh
		return nil
	})
	return out, err
}

// shadowMutate runs a transaction that writes only shadows. It takes no
// sequence and adds nothing to the feed.
func (c *Connector) shadowMutate(ctx context.Context, ws string, fn func(tx *server.EntityTx) error) error {
	return c.srv.Mutate(server.TrustedAs(ctx, c.label), ws, fn)
}

// loadRows reads the root row and the member rows of a record.
func (c *Connector) loadRows(tx *server.EntityTx, rt *recordType, key string) (proto.Message, []proto.Message, error) {
	root := rt.root.ProtoReflect().New().Interface()
	found, err := tx.Get(root, key)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		root = nil
	}
	var members []proto.Message
	for _, m := range rt.members {
		if err := tx.Query(m.prototype, query.Spec{Where: query.Eq(m.field, key)}, func(msg proto.Message) error {
			members = append(members, msg)
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("connect: members %s of %s %q: %w", m.entity, rt.rootName, key, err)
		}
	}
	return root, members, nil
}

// memberKeys groups member rows by entity, each sorted.
func (c *Connector) memberKeys(members []proto.Message) (map[string][]string, error) {
	if len(members) == 0 {
		return nil, nil
	}
	out := make(map[string][]string)
	for _, msg := range members {
		entity, key, err := c.srv.EntityKey(msg)
		if err != nil {
			return nil, err
		}
		out[entity] = append(out[entity], key)
	}
	for _, keys := range out {
		slices.Sort(keys)
	}
	return out, nil
}

// putAgreed records next as the agreed state of the record with key, in tx,
// and moves the indexes from old (nil when there was none) to it. It clears
// the record's missing mark, and its dirty mark when clearDirty is set. A
// member row that moves here from another record marks that record dirty.
func (c *Connector) putAgreed(tx *server.EntityTx, rt *recordType, key string, old, next *recShadow, clearDirty bool) error {
	if err := tx.PutConsumerShadow(c.consumer, recEntity(rt), key, encodeJSON(next)); err != nil {
		return err
	}
	if old != nil && old.Locator != next.Locator && old.Locator != "" {
		// Another record can own the old locator by now; only drop the entry
		// when it still points here.
		owner, err := tx.ConsumerShadow(c.consumer, locEntity(rt), old.Locator)
		if err != nil {
			return err
		}
		if string(owner) == key {
			if err := tx.DeleteConsumerShadow(c.consumer, locEntity(rt), old.Locator); err != nil {
				return err
			}
		}
	}
	if old == nil || old.Locator != next.Locator {
		if err := tx.PutConsumerShadow(c.consumer, locEntity(rt), next.Locator, []byte(key)); err != nil {
			return err
		}
	}
	var was map[string][]string
	if old != nil {
		was = old.Members
	}
	for _, m := range rt.members {
		before, after := was[m.entity], next.Members[m.entity]
		for _, k := range before {
			if _, ok := slices.BinarySearch(after, k); !ok {
				if err := c.dropIndex(tx, m.entity, k, key); err != nil {
					return err
				}
			}
		}
		for _, k := range after {
			if _, ok := slices.BinarySearch(before, k); ok {
				continue
			}
			// A row that moves here from another record leaves that record's
			// agreed content stale. The drain cannot see it: the row's change
			// maps to this record now, whose agreed state reflects it.
			owner, err := tx.ConsumerShadow(c.consumer, idxEntity(m.entity), k)
			if err != nil {
				return err
			}
			if owner != nil && string(owner) != key {
				if err := c.markDirty(tx, rt, string(owner)); err != nil {
					return err
				}
			}
			if err := tx.PutConsumerShadow(c.consumer, idxEntity(m.entity), k, []byte(key)); err != nil {
				return err
			}
		}
	}
	if old == nil || old.Fingerprint != next.Fingerprint {
		if old != nil {
			if err := c.indexFingerprint(tx, rt, old.Fingerprint, key, false); err != nil {
				return err
			}
		}
		if err := c.indexFingerprint(tx, rt, next.Fingerprint, key, true); err != nil {
			return err
		}
	}
	if err := tx.DeleteConsumerShadow(c.consumer, goneEntity(rt), key); err != nil {
		return err
	}
	if clearDirty {
		return tx.DeleteConsumerShadow(c.consumer, dirtyEntity(rt), key)
	}
	return nil
}

// markDirty marks the record with key for egress, in tx. A mark it replaces
// keeps its first change.
func (c *Connector) markDirty(tx *server.EntityTx, rt *recordType, key string) error {
	mark, err := c.nextDirty(tx, rt, key)
	if err != nil {
		return err
	}
	return tx.PutConsumerShadow(c.consumer, dirtyEntity(rt), key, mark)
}

// nextDirty returns the dirty mark of an engine change of the record with key
// now: it keeps the first change of the mark the record has.
func (c *Connector) nextDirty(tx shadowReader, rt *recordType, key string) ([]byte, error) {
	now := c.now()
	m := dirtyMark{last: now, first: now}
	raw, err := tx.ConsumerShadow(c.consumer, dirtyEntity(rt), key)
	if err != nil {
		return nil, err
	}
	if raw != nil {
		old, err := decodeDirty(raw)
		if err != nil {
			return nil, err
		}
		if old.first.Before(now) {
			m.first = old.first
		}
	}
	return encodeDirty(m), nil
}

// commitPending agrees on next as putAgreed does, and drops the record's
// pending write.
func (c *Connector) commitPending(tx *server.EntityTx, rt *recordType, key string, old, next *recShadow, clearDirty bool) error {
	if err := c.putAgreed(tx, rt, key, old, next, clearDirty); err != nil {
		return err
	}
	return tx.DeleteConsumerShadow(c.consumer, pendEntity(rt), key)
}

// dropIndex removes a member row's index entry when it still names key: the
// row can have moved to another record, whose agreed state now owns it.
func (c *Connector) dropIndex(tx *server.EntityTx, entity, rowKey, key string) error {
	owner, err := tx.ConsumerShadow(c.consumer, idxEntity(entity), rowKey)
	if err != nil {
		return err
	}
	if string(owner) != key {
		return nil
	}
	return tx.DeleteConsumerShadow(c.consumer, idxEntity(entity), rowKey)
}

// indexFingerprint adds key to the fingerprint index entry of fp, or removes
// it, in tx. An empty fingerprint is not indexed.
func (c *Connector) indexFingerprint(tx *server.EntityTx, rt *recordType, fp, key string, add bool) error {
	if fp == "" {
		return nil
	}
	keys, err := c.txFingerprintKeys(tx, rt, fp)
	if err != nil {
		return err
	}
	i, found := slices.BinarySearch(keys, key)
	switch {
	case add && !found && len(keys) < maxFingerprintKeys:
		keys = slices.Insert(keys, i, key)
	case !add && found:
		keys = slices.Delete(keys, i, i+1)
	default:
		return nil
	}
	if len(keys) == 0 {
		return tx.DeleteConsumerShadow(c.consumer, fpEntity(rt), fp)
	}
	return tx.PutConsumerShadow(c.consumer, fpEntity(rt), fp, encodeJSON(keys))
}

func (c *Connector) txFingerprintKeys(tx *server.EntityTx, rt *recordType, fp string) ([]string, error) {
	raw, err := tx.ConsumerShadow(c.consumer, fpEntity(rt), fp)
	if err != nil {
		return nil, err
	}
	return decodeKeys(raw)
}

func decodeKeys(raw []byte) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	var keys []string
	if err := json.Unmarshal(raw, &keys); err != nil {
		return nil, fmt.Errorf("connect: decode fingerprint index: %w", err)
	}
	return keys, nil
}

// forgetRecord removes every shadow of a record, in tx.
func (c *Connector) forgetRecord(tx *server.EntityTx, rt *recordType, key string, sh *recShadow) error {
	if sh != nil {
		if err := c.indexFingerprint(tx, rt, sh.Fingerprint, key, false); err != nil {
			return err
		}
		owner, err := tx.ConsumerShadow(c.consumer, locEntity(rt), sh.Locator)
		if err != nil {
			return err
		}
		if string(owner) == key {
			if err := tx.DeleteConsumerShadow(c.consumer, locEntity(rt), sh.Locator); err != nil {
				return err
			}
		}
		for entity, keys := range sh.Members {
			for _, k := range keys {
				if err := c.dropIndex(tx, entity, k, key); err != nil {
					return err
				}
			}
		}
	}
	// The kept content stays: a refusal can come after egress deleted the
	// record.
	for _, entity := range []string{recEntity(rt), dirtyEntity(rt), goneEntity(rt), pendEntity(rt), refusedEntity(rt)} {
		if err := tx.DeleteConsumerShadow(c.consumer, entity, key); err != nil {
			return err
		}
	}
	return nil
}

// sameContent reports whether content is what the shadow agreed on.
func (sh *recShadow) sameContent(locator string, content []byte) bool {
	return sh != nil && sh.Locator == locator && bytes.Equal(sh.Hash, contentHash(content))
}
