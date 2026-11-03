package upstream

import (
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The link keeps its state in the local store as consumer shadows under one
// reserved consumer name, so that every state change commits in the same
// transaction as the rows it describes. Shadow entity names that start with
// '#' cannot be entity names, so they never collide with a confirmed row,
// which is kept under the entity's own name.
const (
	consumerName = server.ReservedConsumerPrefix + "upstream"

	shadowMeta    = "#m"  // keys metaQueue and metaSync
	shadowEntry   = "#e"  // pending entries, keyed by idKey
	shadowResult  = "#r"  // origin verdicts of pending entries, keyed by idKey
	shadowTouched = "#t:" // + entity: pending ids that touch a row, keyed by row key

	metaQueue      = "queue"
	metaSync       = "sync"
	metaRejections = "rejections"
	metaRefused    = "refused"
)

// stateVersion is the first byte of every record. A record of another
// version is refused rather than misread.
const stateVersion byte = 1

var errBadRecord = errors.New("upstream: link state record does not decode")

// idKey is the shadow key of a pending id: zero padded, so keys sort in id
// order.
func idKey(id uint64) string { return fmt.Sprintf("%020d", id) }

// queueMeta is the part of a workspace's link state every capture reads and
// writes, kept apart from syncMeta so a capture does not rewrite the cursor.
type queueMeta struct {
	// linked is false once the workspace was removed from the link.
	linked bool
	// revoked records that the origin refused the workspace for good.
	revoked bool
	// lowID is at or below the lowest pending id; nextID is the id the next
	// entry takes. Pending ids lie in [lowID, nextID), with holes where an
	// entry was dropped early.
	lowID, nextID uint64
	// nextRef is the ref the next entry takes. Refs name entries for good;
	// ids change when the link renumbers its queue to match the origin.
	nextRef uint64
	// pushFrom is at or below the lowest id the origin has not answered:
	// every entry in [lowID, pushFrom) has a verdict.
	pushFrom uint64
	// unsettled lists the ids whose verdict is a rejection the link has not
	// retired yet.
	unsettled []uint64
	// purgeRows records a deletion of the workspace's bound rows in
	// progress: it runs in pages, and resumes after a restart. With linked
	// false, the link state goes once the rows are gone.
	purgeRows bool
	// epoch counts renumberings, so a reader of the queue can tell that ids
	// it read before may have moved.
	epoch uint64
}

// syncMeta is the pull side of a workspace's link state.
type syncMeta struct {
	clientID string
	subs     []*syncplanev1.Subscription
	// cursor is the origin cursor confirmed rows are current through; nil
	// before the first complete bootstrap.
	cursor []byte
	// originSeq is the origin sequence cursor captures.
	originSeq uint64
	// lastAcked is the origin's mutation watermark, as of cursor.
	lastAcked uint64
	// ackedSeq is the highest origin sequence a push of this link was
	// applied at, sent as min_seq.
	ackedSeq       uint64
	schemaRevision string
	// gen is the generation of the latest bootstrap; confirmed rows a
	// bootstrap wrote carry it.
	gen uint64
	// boot is the bootstrap in progress, nil when none is.
	boot *bootState
	// adopt makes the first bootstrap push local rows the origin lacks
	// instead of deleting them.
	adopt bool
}

type bootState struct {
	cursor, token []byte
	seq, acked    uint64
	revision      string
	// finishing is set once the snapshot's rows are all applied, while the
	// link retires the rows it did not carry, page by page from entity
	// index entity after key afterKey, then pending ids from afterID.
	finishing bool
	entity    uint64
	afterKey  string
	afterID   uint64
	// rows counts the snapshot rows applied, by every attempt of it.
	rows uint64
	// estimate is the smallest row estimate an attempt's Begin gave, 0 when
	// none gave one. Every Begin counts the whole snapshot as an upper bound,
	// so the smallest is the closest.
	estimate uint64
	// entitiesDone of entitiesTotal entities have all their rows applied.
	entitiesDone, entitiesTotal uint64
}

// progress is the BootstrapProgress that b records.
func (b *bootState) progress() BootstrapProgress {
	est := b.estimate
	if est > 0 && est < b.rows {
		est = b.rows
	}
	return BootstrapProgress{
		RowsApplied:   b.rows,
		RowsEstimated: est,
		EntitiesDone:  int(b.entitiesDone),
		EntitiesTotal: int(b.entitiesTotal),
	}
}

// confirmedRow is a row as the origin last sent it.
type confirmedRow struct {
	present bool
	value   []byte
	version uint64
	// gen is the bootstrap generation the row was written under.
	gen uint64
	// elided is set when the record holds no value because the local row
	// holds it: the local row at localVersion is the confirmed row. A record
	// is elided only while no pending entry touches its row, and gets its
	// value back when the first one is captured (see keepConfirmed), so a
	// row stored twice is a row with pending writes.
	elided       bool
	localVersion uint64
	// stale is set when the value may not be what the origin sent: the
	// local row was rewritten outside the link while the record was elided,
	// and value is that rewritten row. Every transaction that reads a stale
	// record asks for a fresh snapshot, which replaces or retires it.
	stale bool
}

// resultRecord is the origin's verdict on one pushed entry.
type resultRecord struct {
	status  syncplanev1.MutationStatus
	seq     uint64
	code    string
	message string
}

// entry is one local mutation the origin has not confirmed.
type entry struct {
	id       uint64
	ref      uint64
	localSeq uint64
	// custom is the custom mutation to forward, nil for a CRUD entry.
	custom *syncplanev1.CustomMutation
	// ops are the entry's writes to bound entities, in order: the upstream
	// form of a CRUD entry, and the replay ops of either kind.
	ops []entryOp
	// reminted records that the entry's text was written afresh after the
	// origin refused its text ids (see txn.remint).
	reminted bool
	// origin is the downstream client mutation the entry captured, nil for a
	// write no client pushed. A rejection is reported back to that client.
	origin *server.MutationOrigin
}

type entryOp struct {
	entity, key string
	kind        syncplanev1.CrudOp
	value       []byte
	mask        []string
	text        map[string][]byte
	cas         casRef
}

// casMode says how an op's compare-and-swap reaches the origin.
type casMode uint8

const (
	casNone casMode = iota
	// casVersion sends casRef.version as is.
	casVersion
	// casBase sends the origin sequence the entry casRef.base was applied at.
	casBase
)

type casRef struct {
	mode    casMode
	version uint64
	base    uint64
}

// rejection is a verdict the link has not reported yet.
type rejection struct {
	id       uint64
	ref      uint64
	localSeq uint64
	code     string
	message  string
	mutator  string
	rows     []RowRef
}

// rows returns the distinct rows an entry touches, in first-touch order.
func (e *entry) rows() []RowRef {
	out := make([]RowRef, 0, len(e.ops))
	for _, op := range e.ops {
		r := RowRef{Entity: op.entity, Key: op.key}
		if !slices.Contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

// --- encoding ---

type enc struct{ b []byte }

func newEnc() *enc { return &enc{b: []byte{stateVersion}} }

func (e *enc) uint(n protowire.Number, v uint64) {
	if v != 0 {
		e.b = protowire.AppendTag(e.b, n, protowire.VarintType)
		e.b = protowire.AppendVarint(e.b, v)
	}
}

func (e *enc) bool(n protowire.Number, v bool) {
	if v {
		e.uint(n, 1)
	}
}

func (e *enc) bytes(n protowire.Number, v []byte) {
	if v != nil {
		e.b = protowire.AppendTag(e.b, n, protowire.BytesType)
		e.b = protowire.AppendBytes(e.b, v)
	}
}

func (e *enc) str(n protowire.Number, v string) {
	if v != "" {
		e.bytes(n, []byte(v))
	}
}

// fields walks an encoded record, calling fn per field with the varint value
// or the bytes value.
func fields(b []byte, fn func(n protowire.Number, v uint64, bs []byte) error) error {
	for len(b) > 0 {
		n, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return errBadRecord
		}
		b = b[l:]
		switch typ {
		case protowire.VarintType:
			v, l := protowire.ConsumeVarint(b)
			if l < 0 {
				return errBadRecord
			}
			b = b[l:]
			if err := fn(n, v, nil); err != nil {
				return err
			}
		case protowire.BytesType:
			v, l := protowire.ConsumeBytes(b)
			if l < 0 {
				return errBadRecord
			}
			b = b[l:]
			if err := fn(n, 0, v); err != nil {
				return err
			}
		default:
			return errBadRecord
		}
	}
	return nil
}

// record strips and checks the version byte.
func record(b []byte) ([]byte, error) {
	if len(b) == 0 || b[0] != stateVersion {
		return nil, errBadRecord
	}
	return b[1:], nil
}

func clone(b []byte) []byte { return append([]byte{}, b...) }

func (m *queueMeta) encode() []byte {
	e := newEnc()
	e.bool(1, m.linked)
	e.bool(2, m.revoked)
	e.uint(3, m.lowID)
	e.uint(4, m.nextID)
	e.uint(5, m.nextRef)
	e.uint(6, m.pushFrom)
	for _, id := range m.unsettled {
		e.b = protowire.AppendTag(e.b, 7, protowire.VarintType)
		e.b = protowire.AppendVarint(e.b, id)
	}
	e.bool(8, m.purgeRows)
	e.uint(9, m.epoch)
	return e.b
}

func decodeQueueMeta(b []byte) (queueMeta, error) {
	var m queueMeta
	b, err := record(b)
	if err != nil {
		return m, err
	}
	err = fields(b, func(n protowire.Number, v uint64, _ []byte) error {
		switch n {
		case 1:
			m.linked = v != 0
		case 2:
			m.revoked = v != 0
		case 3:
			m.lowID = v
		case 4:
			m.nextID = v
		case 5:
			m.nextRef = v
		case 6:
			m.pushFrom = v
		case 7:
			m.unsettled = append(m.unsettled, v)
		case 8:
			m.purgeRows = v != 0
		case 9:
			m.epoch = v
		}
		return nil
	})
	return m, err
}

func (m *syncMeta) encode() ([]byte, error) {
	e := newEnc()
	e.str(1, m.clientID)
	for _, s := range m.subs {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(s)
		if err != nil {
			return nil, err
		}
		e.bytes(2, b)
	}
	e.bytes(3, m.cursor)
	e.uint(4, m.originSeq)
	e.uint(5, m.lastAcked)
	e.uint(6, m.ackedSeq)
	e.str(7, m.schemaRevision)
	e.uint(8, m.gen)
	e.bool(10, m.adopt)
	if m.boot != nil {
		be := &enc{}
		be.bytes(1, m.boot.cursor)
		be.bytes(2, m.boot.token)
		be.uint(3, m.boot.seq)
		be.uint(4, m.boot.acked)
		be.str(5, m.boot.revision)
		be.bool(6, m.boot.finishing)
		be.uint(7, m.boot.entity)
		be.str(8, m.boot.afterKey)
		be.uint(9, m.boot.afterID)
		be.uint(10, m.boot.rows)
		be.uint(11, m.boot.estimate)
		be.uint(12, m.boot.entitiesDone)
		be.uint(13, m.boot.entitiesTotal)
		e.b = protowire.AppendTag(e.b, 9, protowire.BytesType)
		e.b = protowire.AppendBytes(e.b, be.b)
	}
	return e.b, nil
}

func decodeSyncMeta(b []byte) (syncMeta, error) {
	var m syncMeta
	b, err := record(b)
	if err != nil {
		return m, err
	}
	err = fields(b, func(n protowire.Number, v uint64, bs []byte) error {
		switch n {
		case 1:
			m.clientID = string(bs)
		case 2:
			s := &syncplanev1.Subscription{}
			if err := proto.Unmarshal(bs, s); err != nil {
				return err
			}
			m.subs = append(m.subs, s)
		case 3:
			m.cursor = clone(bs)
		case 4:
			m.originSeq = v
		case 5:
			m.lastAcked = v
		case 6:
			m.ackedSeq = v
		case 7:
			m.schemaRevision = string(bs)
		case 8:
			m.gen = v
		case 10:
			m.adopt = v != 0
		case 9:
			bs2 := &bootState{}
			if err := fields(bs, func(n protowire.Number, v uint64, b []byte) error {
				switch n {
				case 1:
					bs2.cursor = clone(b)
				case 2:
					bs2.token = clone(b)
				case 3:
					bs2.seq = v
				case 4:
					bs2.acked = v
				case 5:
					bs2.revision = string(b)
				case 6:
					bs2.finishing = v != 0
				case 7:
					bs2.entity = v
				case 8:
					bs2.afterKey = string(b)
				case 9:
					bs2.afterID = v
				case 10:
					bs2.rows = v
				case 11:
					bs2.estimate = v
				case 12:
					bs2.entitiesDone = v
				case 13:
					bs2.entitiesTotal = v
				}
				return nil
			}); err != nil {
				return err
			}
			m.boot = bs2
		}
		return nil
	})
	return m, err
}

func (c *confirmedRow) encode() []byte {
	e := newEnc()
	e.bool(1, c.present)
	if c.present && !c.elided {
		e.bytes(2, append([]byte{}, c.value...))
	}
	e.uint(3, c.version)
	e.uint(4, c.gen)
	e.bool(5, c.elided)
	e.uint(6, c.localVersion)
	e.bool(7, c.stale)
	return e.b
}

func decodeConfirmed(b []byte) (confirmedRow, error) {
	var c confirmedRow
	b, err := record(b)
	if err != nil {
		return c, err
	}
	err = fields(b, func(n protowire.Number, v uint64, bs []byte) error {
		switch n {
		case 1:
			c.present = v != 0
		case 2:
			c.value = clone(bs)
		case 3:
			c.version = v
		case 4:
			c.gen = v
		case 5:
			c.elided = v != 0
		case 6:
			c.localVersion = v
		case 7:
			c.stale = v != 0
		}
		return nil
	})
	return c, err
}

func (r *resultRecord) encode() []byte {
	e := newEnc()
	e.uint(1, uint64(r.status))
	e.uint(2, r.seq)
	e.str(3, r.code)
	e.str(4, r.message)
	return e.b
}

func decodeResult(b []byte) (resultRecord, error) {
	var r resultRecord
	b, err := record(b)
	if err != nil {
		return r, err
	}
	err = fields(b, func(n protowire.Number, v uint64, bs []byte) error {
		switch n {
		case 1:
			r.status = syncplanev1.MutationStatus(v)
		case 2:
			r.seq = v
		case 3:
			r.code = string(bs)
		case 4:
			r.message = string(bs)
		}
		return nil
	})
	return r, err
}

func (e *entry) encode() ([]byte, error) {
	w := newEnc()
	w.uint(1, e.id)
	w.uint(2, e.localSeq)
	w.uint(5, e.ref)
	w.bool(6, e.reminted)
	if e.custom != nil {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(e.custom)
		if err != nil {
			return nil, err
		}
		w.bytes(3, b)
	}
	if o := e.origin; o != nil {
		oe := &enc{}
		oe.str(1, o.Client)
		oe.uint(2, o.MutationID)
		oe.str(3, o.Mutator)
		for _, t := range o.Targets {
			te := &enc{}
			te.str(1, t.Entity)
			te.str(2, t.Key)
			oe.bytes(4, te.b)
		}
		w.bytes(7, oe.b)
	}
	for _, op := range e.ops {
		o := &enc{}
		o.str(1, op.entity)
		o.str(2, op.key)
		o.uint(3, uint64(op.kind))
		if op.value != nil {
			o.bytes(4, op.value)
		}
		for _, p := range op.mask {
			o.bytes(5, []byte(p))
		}
		paths := make([]string, 0, len(op.text))
		for p := range op.text {
			paths = append(paths, p)
		}
		slices.Sort(paths)
		for _, p := range paths {
			t := &enc{}
			t.bytes(1, []byte(p))
			t.bytes(2, op.text[p])
			o.bytes(6, t.b)
		}
		o.uint(7, uint64(op.cas.mode))
		o.uint(8, op.cas.version)
		o.uint(9, op.cas.base)
		w.bytes(4, o.b)
	}
	return w.b, nil
}

func decodeEntry(b []byte) (*entry, error) {
	e := &entry{}
	b, err := record(b)
	if err != nil {
		return nil, err
	}
	err = fields(b, func(n protowire.Number, v uint64, bs []byte) error {
		switch n {
		case 1:
			e.id = v
		case 2:
			e.localSeq = v
		case 5:
			e.ref = v
		case 6:
			e.reminted = v != 0
		case 3:
			e.custom = &syncplanev1.CustomMutation{}
			return proto.Unmarshal(bs, e.custom)
		case 7:
			o, err := decodeOrigin(bs)
			e.origin = o
			return err
		case 4:
			var op entryOp
			if err := fields(bs, func(n protowire.Number, v uint64, b []byte) error {
				switch n {
				case 1:
					op.entity = string(b)
				case 2:
					op.key = string(b)
				case 3:
					op.kind = syncplanev1.CrudOp(v)
				case 4:
					op.value = clone(b)
				case 5:
					op.mask = append(op.mask, string(b))
				case 6:
					var path string
					var doc []byte
					if err := fields(b, func(n protowire.Number, _ uint64, b []byte) error {
						if n == 1 {
							path = string(b)
						} else if n == 2 {
							doc = clone(b)
						}
						return nil
					}); err != nil {
						return err
					}
					if op.text == nil {
						op.text = map[string][]byte{}
					}
					op.text[path] = doc
				case 7:
					op.cas.mode = casMode(v)
				case 8:
					op.cas.version = v
				case 9:
					op.cas.base = v
				}
				return nil
			}); err != nil {
				return err
			}
			e.ops = append(e.ops, op)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func decodeOrigin(b []byte) (*server.MutationOrigin, error) {
	o := &server.MutationOrigin{}
	err := fields(b, func(n protowire.Number, v uint64, bs []byte) error {
		switch n {
		case 1:
			o.Client = string(bs)
		case 2:
			o.MutationID = v
		case 3:
			o.Mutator = string(bs)
		case 4:
			var t storage.RejectionTarget
			if err := fields(bs, func(n protowire.Number, _ uint64, b []byte) error {
				switch n {
				case 1:
					t.Entity = string(b)
				case 2:
					t.Key = string(b)
				}
				return nil
			}); err != nil {
				return err
			}
			o.Targets = append(o.Targets, t)
		}
		return nil
	})
	return o, err
}

func (r *rejection) encode() []byte {
	e := newEnc()
	e.uint(1, r.id)
	e.uint(2, r.localSeq)
	e.str(3, r.code)
	e.str(4, r.message)
	e.str(5, r.mutator)
	for _, row := range r.rows {
		o := &enc{}
		o.str(1, row.Entity)
		o.str(2, row.Key)
		e.bytes(6, o.b)
	}
	e.uint(7, r.ref)
	return e.b
}

func decodeRejection(b []byte) (rejection, error) {
	var r rejection
	b, err := record(b)
	if err != nil {
		return r, err
	}
	err = fields(b, func(n protowire.Number, v uint64, bs []byte) error {
		switch n {
		case 1:
			r.id = v
		case 2:
			r.localSeq = v
		case 3:
			r.code = string(bs)
		case 4:
			r.message = string(bs)
		case 5:
			r.mutator = string(bs)
		case 6:
			var row RowRef
			if err := fields(bs, func(n protowire.Number, _ uint64, b []byte) error {
				if n == 1 {
					row.Entity = string(b)
				} else if n == 2 {
					row.Key = string(b)
				}
				return nil
			}); err != nil {
				return err
			}
			r.rows = append(r.rows, row)
		case 7:
			r.ref = v
		}
		return nil
	})
	return r, err
}

func encodeIDs(ids []uint64) []byte {
	e := newEnc()
	for _, id := range ids {
		e.b = protowire.AppendVarint(e.b, id)
	}
	return e.b
}

func decodeIDs(b []byte) ([]uint64, error) {
	b, err := record(b)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for len(b) > 0 {
		v, l := protowire.ConsumeVarint(b)
		if l < 0 {
			return nil, errBadRecord
		}
		out = append(out, v)
		b = b[l:]
	}
	return out, nil
}

// --- typed access to the shadows of one transaction ---

// stateTx reads and writes the link state inside one storage transaction.
// A stateTx from readState only reads.
type stateTx struct {
	r storage.ReadTx
	w storage.WriteTx
}

func writeState(tx storage.WriteTx) stateTx { return stateTx{r: tx, w: tx} }

func readState(tx storage.ReadTx) stateTx { return stateTx{r: tx} }

func (s stateTx) get(entity, key string) ([]byte, error) {
	return s.r.ConsumerShadow(consumerName, entity, key)
}

func (s stateTx) put(entity, key string, b []byte) error {
	return s.w.PutConsumerShadow(consumerName, entity, key, b)
}

func (s stateTx) del(entity, key string) error {
	return s.w.DeleteConsumerShadow(consumerName, entity, key)
}

// queue returns the queue meta, and false when the workspace was never
// linked.
func (s stateTx) queue() (queueMeta, bool, error) {
	b, err := s.get(shadowMeta, metaQueue)
	if err != nil || b == nil {
		return queueMeta{}, false, err
	}
	m, err := decodeQueueMeta(b)
	return m, err == nil, err
}

func (s stateTx) putQueue(m queueMeta) error { return s.put(shadowMeta, metaQueue, m.encode()) }

func (s stateTx) sync() (syncMeta, error) {
	b, err := s.get(shadowMeta, metaSync)
	if err != nil || b == nil {
		return syncMeta{}, err
	}
	return decodeSyncMeta(b)
}

func (s stateTx) putSync(m syncMeta) error {
	b, err := m.encode()
	if err != nil {
		return err
	}
	return s.put(shadowMeta, metaSync, b)
}

func (s stateTx) entry(id uint64) (*entry, error) {
	b, err := s.get(shadowEntry, idKey(id))
	if err != nil || b == nil {
		return nil, err
	}
	return decodeEntry(b)
}

func (s stateTx) putEntry(e *entry) error {
	b, err := e.encode()
	if err != nil {
		return err
	}
	return s.put(shadowEntry, idKey(e.id), b)
}

// entries returns every pending entry in id order.
func (s stateTx) entries(q queueMeta) ([]*entry, error) {
	var out []*entry
	for id := q.lowID; id < q.nextID; id++ {
		e, err := s.entry(id)
		if err != nil {
			return nil, err
		}
		if e != nil {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s stateTx) result(id uint64) (resultRecord, bool, error) {
	b, err := s.get(shadowResult, idKey(id))
	if err != nil || b == nil {
		return resultRecord{}, false, err
	}
	r, err := decodeResult(b)
	return r, err == nil, err
}

func (s stateTx) putResult(id uint64, r resultRecord) error {
	return s.put(shadowResult, idKey(id), r.encode())
}

func (s stateTx) confirmed(entity, key string) (confirmedRow, bool, error) {
	b, err := s.get(entity, key)
	if err != nil || b == nil {
		return confirmedRow{}, false, err
	}
	c, err := decodeConfirmed(b)
	return c, err == nil, err
}

func (s stateTx) putConfirmed(entity, key string, c confirmedRow) error {
	return s.put(entity, key, c.encode())
}

func (s stateTx) touched(entity, key string) ([]uint64, error) {
	b, err := s.get(shadowTouched+entity, key)
	if err != nil || b == nil {
		return nil, err
	}
	return decodeIDs(b)
}

func (s stateTx) putTouched(entity, key string, ids []uint64) error {
	if len(ids) == 0 {
		return s.del(shadowTouched+entity, key)
	}
	return s.put(shadowTouched+entity, key, encodeIDs(ids))
}

// deleteEntry removes an entry, its verdict and its marks on the rows it
// touched.
func (s stateTx) deleteEntry(e *entry) error {
	for _, r := range e.rows() {
		ids, err := s.touched(r.Entity, r.Key)
		if err != nil {
			return err
		}
		ids = slices.DeleteFunc(ids, func(id uint64) bool { return id == e.id })
		if err := s.putTouched(r.Entity, r.Key, ids); err != nil {
			return err
		}
	}
	if err := s.del(shadowEntry, idKey(e.id)); err != nil {
		return err
	}
	return s.del(shadowResult, idKey(e.id))
}

// rejections returns the rejections not reported yet, oldest first.
func (s stateTx) rejections() ([]rejection, error) {
	b, err := s.get(shadowMeta, metaRejections)
	if err != nil || b == nil {
		return nil, err
	}
	b, err = record(b)
	if err != nil {
		return nil, err
	}
	var out []rejection
	err = fields(b, func(_ protowire.Number, _ uint64, bs []byte) error {
		r, err := decodeRejection(bs)
		if err != nil {
			return err
		}
		out = append(out, r)
		return nil
	})
	return out, err
}

func (s stateTx) putRejections(rs []rejection) error {
	if len(rs) == 0 {
		return s.del(shadowMeta, metaRejections)
	}
	e := newEnc()
	for _, r := range rs {
		e.bytes(1, r.encode())
	}
	return s.put(shadowMeta, metaRejections, e.b)
}

// refused returns the rows the local engine refused to store at their
// target state.
func (s stateTx) refused() ([]RowRef, error) {
	b, err := s.get(shadowMeta, metaRefused)
	if err != nil || b == nil {
		return nil, err
	}
	b, err = record(b)
	if err != nil {
		return nil, err
	}
	var out []RowRef
	err = fields(b, func(_ protowire.Number, _ uint64, bs []byte) error {
		var r RowRef
		if err := fields(bs, func(n protowire.Number, _ uint64, v []byte) error {
			if n == 1 {
				r.Entity = string(v)
			} else if n == 2 {
				r.Key = string(v)
			}
			return nil
		}); err != nil {
			return err
		}
		out = append(out, r)
		return nil
	})
	return out, err
}

func (s stateTx) putRefused(rows []RowRef) error {
	if len(rows) == 0 {
		return s.del(shadowMeta, metaRefused)
	}
	e := newEnc()
	for _, r := range rows {
		o := &enc{}
		o.str(1, r.Entity)
		o.str(2, r.Key)
		e.bytes(1, o.b)
	}
	return s.put(shadowMeta, metaRefused, e.b)
}

// entryHead is an entry without its ops, for the in-memory view.
type entryHead struct {
	ref, localSeq uint64
}

// head reads the ref and local sequence of entry id, skipping its ops.
func (s stateTx) head(id uint64) (entryHead, bool, error) {
	b, err := s.get(shadowEntry, idKey(id))
	if err != nil || b == nil {
		return entryHead{}, false, err
	}
	b, err = record(b)
	if err != nil {
		return entryHead{}, false, err
	}
	var h entryHead
	err = fields(b, func(n protowire.Number, v uint64, _ []byte) error {
		switch n {
		case 2:
			h.localSeq = v
		case 5:
			h.ref = v
		}
		return nil
	})
	return h, err == nil, err
}

// hasEntry reports whether id holds an entry, without decoding it.
func (s stateTx) hasEntry(id uint64) (bool, error) {
	b, err := s.get(shadowEntry, idKey(id))
	return b != nil, err
}

// registry returns the list of linked workspaces the registry workspace
// holds.
func (s stateTx) registry() ([]string, error) {
	b, err := s.get(registryEntity, registryKey)
	if err != nil || b == nil {
		return nil, err
	}
	b, err = record(b)
	if err != nil {
		return nil, err
	}
	var out []string
	err = fields(b, func(_ protowire.Number, _ uint64, bs []byte) error {
		out = append(out, string(bs))
		return nil
	})
	return out, err
}

func (s stateTx) putRegistry(ids []string) error {
	e := newEnc()
	for _, id := range ids {
		e.bytes(1, []byte(id))
	}
	return s.put(registryEntity, registryKey, e.b)
}
