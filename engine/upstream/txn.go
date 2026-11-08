package upstream

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

// impossibleVersion is an expected version no row ever has. An entry whose
// compare-and-swap base the origin rejected is pushed with it, so the origin
// rejects the entry with code conflict and its id still advances the origin's
// watermark.
const impossibleVersion = math.MaxUint64

// txResult is what one committed link transaction changed, for the in-memory
// view.
type txResult struct {
	meta     *syncMeta
	ackKnown bool
	maxID    uint64
	seen     []pendRef
	// reload, when non-nil, replaces the whole pending view.
	reload   []pendRef
	verdicts []verdictEvent
	dropped  []uint64
	refusals []error
	reject   bool
	// unreported are the refs of the rejections stored for reporting, when
	// the transaction read or wrote that list; nil otherwise.
	unreported []uint64
	// epoch is the queue's renumbering count the view reflects.
	epoch uint64
	// drifted is set when the transaction read a confirmed row the local row
	// was rewritten from outside the link: only a fresh snapshot restores it.
	drifted bool
}

type pendRef struct{ ref, localSeq uint64 }

type verdictEvent struct {
	ref, localSeq uint64
	err           error
}

// txn is one link transaction on a workspace: a Replicate call, with the
// link state read at its start and written back at its end.
type txn struct {
	ctx  context.Context
	w    *workspace
	b    *bound
	rtx  *server.ReplicaTx
	st   stateTx
	q    queueMeta
	q0   []byte
	meta syncMeta
	// metaDirty makes the transaction write meta back.
	metaDirty bool
	res       *txResult
	cache     map[uint64]*entry
	affected  []RowRef
	affSet    map[RowRef]bool
	// refused are the rows recompute could not bring to their target.
	refused []RowRef
}

// txn runs fn in a link transaction and applies its result to the view after
// it commits. fn may run more than once. It returns ErrNotLinked when the
// workspace's link state is gone.
func (w *workspace) txn(ctx context.Context, fn func(*txn) error) (*txResult, error) {
	b := w.l.bind.Load()
	w.mu.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			w.mu.Unlock()
		}
	}
	defer unlock()
	var t *txn
	err := b.srv.Replicate(ctx, w.id, func(rtx *server.ReplicaTx) error {
		t = &txn{ctx: ctx, w: w, b: b, rtx: rtx, st: writeState(rtx.Tx()), res: &txResult{},
			cache: map[uint64]*entry{}, affSet: map[RowRef]bool{}}
		q, ok, err := t.st.queue()
		if err != nil {
			return err
		}
		if !ok || !q.linked {
			return ErrNotLinked
		}
		t.q, t.q0 = q, q.encode()
		if t.meta, err = t.st.sync(); err != nil {
			return err
		}
		from := max(w.maxID, q.lowID)
		if q.epoch != w.epoch {
			// Entries moved since the view read them (a renumbering inside
			// a capture): read them all again.
			from = q.lowID
			t.res.reload = []pendRef{}
		}
		t.res.epoch = q.epoch
		for id := from; id < q.nextID; id++ {
			h, ok, err := t.st.head(id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			t.res.seen = append(t.res.seen, pendRef{h.ref, h.localSeq})
			if t.res.reload != nil {
				t.res.reload = append(t.res.reload, pendRef{h.ref, h.localSeq})
			}
			// A write-through capture stores the origin's verdict with the
			// entry; the view learns it here.
			r, answered, err := t.st.result(id)
			if err != nil {
				return err
			}
			if answered && r.status != syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
				t.res.verdicts = append(t.res.verdicts, verdictEvent{ref: h.ref, localSeq: h.localSeq})
			}
		}
		t.res.maxID = q.nextID
		if err := fn(t); err != nil {
			return err
		}
		return t.finish()
	})
	if errors.Is(err, server.ErrWorkspaceDeleted) {
		err = fmt.Errorf("%w: %w", ErrNotLinked, err)
	}
	if errors.Is(err, ErrNotLinked) {
		// The link state went from under the workspace (DeleteWorkspace on
		// the local engine): nothing is left to sync.
		unlock()
		w.gone()
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	w.commit(t.res)
	unlock()
	if t.res.reject {
		w.kickDeliver()
	}
	if t.res.drifted {
		w.logger().LogAttrs(ctx, slog.LevelWarn, "a synced row was rewritten outside the link; bootstrapping again")
		w.unready(true, false)
	}
	// Reported outside w.mu, so a status callback may call back into the
	// link.
	w.update(func(*status) {})
	return t.res, nil
}

func (t *txn) finish() error {
	// Rows the local engine refused earlier are tried again in every
	// transaction that recomputes rows: what made them unstorable is often a
	// pending write elsewhere that this transaction retired.
	retry, err := t.st.refused()
	if err != nil {
		return err
	}
	if len(t.affected) > 0 || len(retry) > 0 {
		t.touch(retry...)
		if err := t.recompute(t.affected); err != nil {
			return err
		}
		if len(retry) > 0 || len(t.refused) > 0 {
			if err := t.st.putRefused(t.refused); err != nil {
				return err
			}
		}
	}
	if err := advanceLow(t.st, &t.q); err != nil {
		return err
	}
	if q := t.q.encode(); !bytes.Equal(q, t.q0) {
		if err := t.st.putQueue(t.q); err != nil {
			return err
		}
	}
	if t.metaDirty {
		if err := t.st.putSync(t.meta); err != nil {
			return err
		}
		m := t.meta
		t.res.meta = &m
	}
	if t.res.reload != nil {
		// A renumbering can lower the queue's end.
		t.res.maxID = t.q.nextID
	} else {
		t.res.maxID = max(t.res.maxID, t.q.nextID)
	}
	t.res.epoch = t.q.epoch
	return nil
}

func (t *txn) entry(id uint64) (*entry, error) {
	if e, ok := t.cache[id]; ok {
		return e, nil
	}
	e, err := t.st.entry(id)
	if err != nil {
		return nil, err
	}
	t.cache[id] = e
	return e, nil
}

func (t *txn) touch(rows ...RowRef) {
	for _, r := range rows {
		if !t.affSet[r] {
			t.affSet[r] = true
			t.affected = append(t.affected, r)
		}
	}
}

// pending returns every pending entry in id order.
func (t *txn) pending() ([]*entry, error) {
	var out []*entry
	for id := t.q.lowID; id < t.q.nextID; id++ {
		e, err := t.entry(id)
		if err != nil {
			return nil, err
		}
		if e != nil {
			out = append(out, e)
		}
	}
	return out, nil
}

// confirmedRow returns the row the origin confirmed, as rebase reads it. An
// elided record reads its value from the local row, so it must be read before
// the transaction writes that row.
func (t *txn) confirmedRow(r RowRef) (rebase.Row, confirmedRow, error) {
	c, ok, err := t.st.confirmed(r.Entity, r.Key)
	if err != nil || !ok {
		return rebase.Row{}, c, err
	}
	if c.elided {
		if c, err = t.resolveElided(r, c); err != nil {
			return rebase.Row{}, c, err
		}
	}
	if c.stale {
		t.res.drifted = true
	}
	if !c.present {
		return rebase.Row{}, c, nil
	}
	return rebase.Row{Present: true, Value: c.value}, c, nil
}

// resolveElided fills in the value of an elided record from the local row.
// A local row at another version than the record names was rewritten outside
// the link; its record then keeps that row as a stale value, the best base
// left until a fresh snapshot replaces it.
func (t *txn) resolveElided(r RowRef, c confirmedRow) (confirmedRow, error) {
	cur, found, err := t.rtx.Get(r.Entity, r.Key)
	if err != nil {
		return c, err
	}
	live := found && !cur.Deleted
	if live && cur.Version == c.localVersion {
		c.value = cur.Value
		return c, nil
	}
	c.elided, c.localVersion, c.stale = false, 0, true
	c.present, c.value = live, nil
	if live {
		c.value = cur.Value
	}
	return c, t.st.putConfirmed(r.Entity, r.Key, c)
}

// elide drops the value from the record of a row no pending entry touches,
// once the local row holds the confirmed row. A stale record keeps its value
// and its mark until a snapshot replaces it.
func (t *txn) elide(r RowRef, c confirmedRow) error {
	if c.stale {
		return nil
	}
	cur, found, err := t.rtx.Get(r.Entity, r.Key)
	if err != nil || !found || cur.Deleted {
		return err
	}
	if c.elided && c.localVersion == cur.Version {
		return nil
	}
	c.value, c.elided, c.localVersion = nil, true, cur.Version
	return t.st.putConfirmed(r.Entity, r.Key, c)
}

// opsFor returns the local ops of every pending entry on row r, in queue
// order, and whether any pending entry touches the row.
func (t *txn) opsFor(r RowRef) ([]rebase.Op, bool, error) {
	ids, err := t.st.touched(r.Entity, r.Key)
	if err != nil {
		return nil, false, err
	}
	slices.Sort(ids)
	var ops []rebase.Op
	for _, id := range ids {
		e, err := t.entry(id)
		if err != nil {
			return nil, false, err
		}
		if e == nil {
			continue
		}
		for _, op := range e.ops {
			if op.entity == r.Entity && op.key == r.Key {
				ops = append(ops, op.rebaseOp())
			}
		}
	}
	return ops, len(ids) > 0, nil
}

func (op entryOp) rebaseOp() rebase.Op {
	return rebase.Op{Kind: op.kind, Value: op.value, Mask: op.mask, Text: op.text}
}

// recompute writes each row as the confirmed row with the pending entries
// replayed on top, where it differs from the local row. The record of a row no
// pending entry touches is elided once the local row holds the confirmed row.
//
// A row the local engine refuses to store (a Rejection from Replicate, for
// example an access chain that only the pending writes close into a cycle) is
// written as the origin confirmed it instead, and the pending entries stay
// queued: the origin decides them. A confirmed row it also refuses is left as
// it is. Both are reported as refusals rather than failing the transaction,
// so one row cannot stall the workspace.
func (t *txn) recompute(rows []RowRef) error {
	for _, r := range rows {
		m := t.b.models[r.Entity]
		if m == nil {
			continue
		}
		conf, rec, err := t.confirmedRow(r)
		if err != nil {
			return err
		}
		ops, pending, err := t.opsFor(r)
		if err != nil {
			return err
		}
		target, rerr := m.Replay(conf, ops)
		if rerr != nil {
			t.res.refusals = append(t.res.refusals, fmt.Errorf("upstream: replay %s %q: %w", r.Entity, r.Key, rerr))
			target = conf
		}
		err = t.write(m, r, target)
		if err == nil {
			if !pending && conf.Present {
				if err := t.elide(r, rec); err != nil {
					return err
				}
			}
			continue
		}
		if _, ok := server.AsRejection(err); !ok {
			return err
		}
		t.res.refusals = append(t.res.refusals, fmt.Errorf("upstream: store %s %q: %w", r.Entity, r.Key, err))
		t.refused = append(t.refused, r)
		if m.Equal(target, conf) {
			continue
		}
		if err := t.write(m, r, conf); err != nil {
			if _, ok := server.AsRejection(err); !ok {
				return err
			}
			t.res.refusals = append(t.res.refusals, fmt.Errorf("upstream: store confirmed %s %q: %w", r.Entity, r.Key, err))
		}
	}
	return nil
}

// write makes the local row target, unless it already is.
func (t *txn) write(m *rebase.Model, r RowRef, target rebase.Row) error {
	cur, found, err := t.rtx.Get(r.Entity, r.Key)
	if err != nil {
		return err
	}
	if m.Equal(rebase.Row{Present: found && !cur.Deleted, Value: cur.Value}, target) {
		return nil
	}
	if target.Present {
		return t.rtx.Put(r.Entity, r.Key, target.Value)
	}
	return t.rtx.Delete(r.Entity, r.Key)
}

// applyChange folds one origin change into the confirmed rows.
func (t *txn) applyChange(rc *syncplanev1.RowChange) error {
	m := t.b.models[rc.GetEntity()]
	if m == nil {
		return nil
	}
	r := RowRef{Entity: rc.GetEntity(), Key: rc.GetKey()}
	// Only text deltas fold onto the row before: any other change carries
	// the whole row, and reading an elided record costs a local read and can
	// find drift a snapshot is about to replace anyway.
	var old rebase.Row
	if len(rc.GetTextDeltas()) > 0 && !rc.GetTombstone() {
		var err error
		if old, _, err = t.confirmedRow(r); err != nil {
			return err
		}
	}
	row, err := m.FoldTextDeltas(old, rc)
	if err != nil {
		return err
	}
	t.touch(r)
	if !row.Present {
		return t.st.del(r.Entity, r.Key)
	}
	return t.st.putConfirmed(r.Entity, r.Key, confirmedRow{present: true, value: row.Value, version: rc.GetVersion(), gen: t.meta.gen})
}

// dropEntityPage forgets the confirmed state of one page of entity's local
// rows after afterKey, and recomputes them. It returns the last key it
// handled and whether more rows may follow.
func (t *txn) dropEntityPage(entity, afterKey string) (string, bool, error) {
	if t.b.models[entity] == nil {
		return "", false, nil
	}
	var keys []string
	if err := t.rtx.Tx().ScanEntityAfter(entity, afterKey, scanPage, func(row storage.Row) error {
		keys = append(keys, row.Key)
		return nil
	}); err != nil {
		return "", false, err
	}
	for _, k := range keys {
		if err := t.st.del(entity, k); err != nil {
			return "", false, err
		}
		t.touch(RowRef{Entity: entity, Key: k})
	}
	if len(keys) == 0 {
		return afterKey, false, nil
	}
	return keys[len(keys)-1], len(keys) == scanPage, nil
}

// dropPendingRows forgets the confirmed state of entity's rows that pending
// entries touch: the rows a local delete keeps out of the local scan.
func (t *txn) dropPendingRows(entity string) error {
	if t.b.models[entity] == nil {
		return nil
	}
	pend, err := t.pending()
	if err != nil {
		return err
	}
	for _, e := range pend {
		for _, r := range e.rows() {
			if r.Entity != entity {
				continue
			}
			if err := t.st.del(r.Entity, r.Key); err != nil {
				return err
			}
			t.touch(r)
		}
	}
	return nil
}

// settle retires the entries the origin has decided: a rejected entry is
// dropped and queued for reporting, and an applied one is dropped once the
// confirmed rows include it (its id is at or below the stored watermark).
// Its work is proportional to what it retires.
func (t *txn) settle() error {
	var rejs []rejection
	for _, id := range slices.Clone(t.q.unsettled) {
		e, err := t.entry(id)
		if err != nil {
			return err
		}
		r, ok, err := t.st.result(id)
		if err != nil {
			return err
		}
		if e == nil || !ok || r.status != syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
			continue
		}
		if textCollision(e, r) {
			if err := t.remint(e); err != nil {
				return err
			}
			continue
		}
		rej := rejectionOf(e, r.code, r.message)
		rejs = append(rejs, rej)
		if err := t.drop(e, &rej); err != nil {
			return err
		}
	}
	t.q.unsettled = nil
	hi := min(t.meta.lastAcked, t.q.nextID-1, max(t.q.pushFrom, t.q.lowID)-1)
	for id := t.q.lowID; id <= hi; id++ {
		e, err := t.entry(id)
		if err != nil {
			return err
		}
		if e == nil {
			continue
		}
		r, ok, err := t.st.result(id)
		if err != nil {
			return err
		}
		switch {
		case !ok:
		case r.status == syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED:
			rej := rejectionOf(e, r.code, r.message)
			rejs = append(rejs, rej)
			if err := t.drop(e, &rej); err != nil {
				return err
			}
		default:
			if err := t.drop(e, nil); err != nil {
				return err
			}
		}
	}
	if len(rejs) > 0 {
		return t.queueRejections(rejs)
	}
	return nil
}

func rejectionOf(e *entry, code, message string) rejection {
	rej := rejection{id: e.id, ref: e.ref, localSeq: e.localSeq, code: code, message: message, rows: e.rows()}
	if e.custom != nil {
		rej.mutator = e.custom.GetName()
	}
	return rej
}

// advanceLow moves lowID past the ids no entry holds, and pushFrom up to it.
func advanceLow(st stateTx, q *queueMeta) error {
	for q.lowID < q.nextID {
		ok, err := st.hasEntry(q.lowID)
		if err != nil {
			return err
		}
		if ok {
			break
		}
		q.lowID++
	}
	q.pushFrom = max(q.pushFrom, q.lowID)
	return nil
}

// advancePushFrom moves pushFrom past the entries the origin has answered.
func advancePushFrom(st stateTx, q *queueMeta) error {
	q.pushFrom = max(q.pushFrom, q.lowID)
	for q.pushFrom < q.nextID {
		ok, err := st.hasEntry(q.pushFrom)
		if err != nil {
			return err
		}
		if ok {
			if _, answered, err := st.result(q.pushFrom); err != nil {
				return err
			} else if !answered {
				return nil
			}
		}
		q.pushFrom++
	}
	return nil
}

// reject drops an entry the origin never decided, reporting it with code.
func (t *txn) reject(e *entry, code, message string) error {
	rej := rejectionOf(e, code, message)
	if err := t.drop(e, &rej); err != nil {
		return err
	}
	return t.queueRejections([]rejection{rej})
}

func (t *txn) queueRejections(rejs []rejection) error {
	cur, err := t.st.rejections()
	if err != nil {
		return err
	}
	t.res.reject = true
	all := append(cur, rejs...)
	t.res.unreported = refsOf(all)
	return t.st.putRejections(all)
}

// drop removes an entry, marks its rows for recompute, and resolves the
// compare-and-swap of every later entry that was based on it. rej is nil for
// an applied entry.
func (t *txn) drop(e *entry, rej *rejection) error {
	r, hasResult, err := t.st.result(e.id)
	if err != nil {
		return err
	}
	rows := e.rows()
	if err := t.st.deleteEntry(e); err != nil {
		return err
	}
	t.q.unsettled = slices.DeleteFunc(t.q.unsettled, func(id uint64) bool { return id == e.id })
	t.cache[e.id] = nil
	t.touch(rows...)
	ev := verdictEvent{ref: e.ref, localSeq: e.localSeq}
	if rej != nil {
		ev.err = &RejectedError{Rejection: rej.public(t.w.id)}
		// The downstream client was acked when the entry was captured; this
		// tells it, in the transaction that reverts its rows. The origin
		// always names a code, and RejectLate refuses an empty one, which
		// would fail the whole transaction.
		if e.origin != nil {
			if err := t.rtx.RejectLate(*e.origin, cmp.Or(rej.code, server.CodeInternal), rej.message); err != nil {
				return err
			}
		}
	}
	t.res.verdicts = append(t.res.verdicts, ev)
	t.res.dropped = append(t.res.dropped, e.ref)
	for _, row := range rows {
		ids, err := t.st.touched(row.Entity, row.Key)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if id <= e.id {
				continue
			}
			d, err := t.entry(id)
			if err != nil {
				return err
			}
			if d == nil {
				continue
			}
			changed := false
			for i := range d.ops {
				op := &d.ops[i]
				if op.cas.mode != casBase || op.cas.base != e.id {
					continue
				}
				changed = true
				version := uint64(impossibleVersion)
				if rej == nil && hasResult {
					version = casVersionOf(r, op.cas)
				}
				op.cas = casRef{mode: casVersion, version: version}
			}
			if changed {
				if err := t.st.putEntry(d); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// voidVerdictsAbove clears the applied verdicts of the entries above the
// origin's watermark acked, and moves pushFrom back to the first of them, so
// they are pushed again. A watermark below an applied verdict means the
// history that applied the entry is gone: the origin's workspace was deleted
// and restored, or restored from a backup. Without the resend the entry is
// never pushed and never settled. An entry the origin applied after the
// snapshot began is resent too, and answered as already applied.
func (t *txn) voidVerdictsAbove(acked uint64) error {
	for id := max(t.q.lowID, acked+1); id < t.q.nextID; id++ {
		r, ok, err := t.st.result(id)
		if err != nil {
			return err
		}
		// A refusal is reported and dropped whatever the origin holds now.
		if !ok || r.status == syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
			continue
		}
		if err := t.st.del(shadowResult, idKey(id)); err != nil {
			return err
		}
		t.q.pushFrom = min(t.q.pushFrom, id)
	}
	return nil
}

// establishAck brings the queue in line with the origin's watermark the first
// time this process learns it. An entry this process captured before then
// may hold an id the origin already used, for a write whose local
// transaction did not commit; such entries move above the watermark.
func (t *txn) establishAck(acked uint64) error {
	if t.w.ackKnown.Load() {
		return nil
	}
	t.res.ackKnown = true
	return establishAck(t.st, &t.q, acked, t.w.startNext, t.renumber)
}

// establishAck is txn.establishAck on a bare state transaction, for the
// capture. renumber moves the given entries to the given ids.
func establishAck(st stateTx, q *queueMeta, acked, startNext uint64, renumber func([]*entry, []uint64) error) error {
	var move []*entry
	for id := max(q.lowID, startNext); id < q.nextID; id++ {
		e, err := st.entry(id)
		if err != nil {
			return err
		}
		if e == nil {
			continue
		}
		if len(move) == 0 {
			if _, answered, err := st.result(id); err != nil {
				return err
			} else if answered || id > acked {
				continue
			}
		}
		move = append(move, e)
	}
	if len(move) > 0 {
		shift := acked + 1 - move[0].id
		ids := make([]uint64, len(move))
		for i, e := range move {
			ids[i] = e.id + shift
		}
		if err := renumber(move, ids); err != nil {
			return err
		}
	}
	q.nextID = max(q.nextID, acked+1)
	return nil
}

// compactAbove gives the entries above acked consecutive ids from acked+1,
// clearing their verdicts: after a mutation-id gap the origin holds none of
// them.
func (t *txn) compactAbove(acked uint64) error {
	mapping, err := compactAbove(t.st, &t.q, acked)
	if err != nil || len(mapping) == 0 {
		return err
	}
	return t.reloadView()
}

// compactAbove is txn.compactAbove on a bare state transaction. It returns
// the new id of each entry it moved.
func compactAbove(st stateTx, q *queueMeta, acked uint64) (map[uint64]uint64, error) {
	var move []*entry
	var ids []uint64
	next := acked + 1
	for id := max(q.lowID, acked+1); id < q.nextID; id++ {
		e, err := st.entry(id)
		if err != nil {
			return nil, err
		}
		if e == nil {
			continue
		}
		move = append(move, e)
		ids = append(ids, next)
		next++
	}
	if len(move) == 0 {
		return nil, nil
	}
	mapping := make(map[uint64]uint64, len(move))
	for i, e := range move {
		mapping[e.id] = ids[i]
	}
	if err := renumber(st, q, move, ids); err != nil {
		return nil, err
	}
	return mapping, nil
}

// renumber moves entries to new ids, with their marks on rows and the
// compare-and-swap references between them, and clears their verdicts.
func (t *txn) renumber(move []*entry, ids []uint64) error {
	if err := renumber(t.st, &t.q, move, ids); err != nil {
		return err
	}
	return t.reloadView()
}

// reloadView makes the transaction replace the whole pending view, after
// entries changed ids.
func (t *txn) reloadView() error {
	clear(t.cache)
	pend, err := t.pending()
	if err != nil {
		return err
	}
	t.res.reload = make([]pendRef, 0, len(pend))
	for _, e := range pend {
		t.res.reload = append(t.res.reload, pendRef{e.ref, e.localSeq})
	}
	return nil
}

func renumber(st stateTx, q *queueMeta, move []*entry, ids []uint64) error {
	q.epoch++
	mapping := make(map[uint64]uint64, len(move))
	rows := map[RowRef]bool{}
	var rowList []RowRef
	for i, e := range move {
		mapping[e.id] = ids[i]
		for _, r := range e.rows() {
			if !rows[r] {
				rows[r] = true
				rowList = append(rowList, r)
			}
		}
	}
	// Delete every old record before writing a new one: an old id can be
	// another entry's new id.
	for _, e := range move {
		if err := st.del(shadowEntry, idKey(e.id)); err != nil {
			return err
		}
		if err := st.del(shadowResult, idKey(e.id)); err != nil {
			return err
		}
	}
	q.unsettled = slices.DeleteFunc(q.unsettled, func(id uint64) bool { _, moved := mapping[id]; return moved })
	for _, e := range move {
		e.id = mapping[e.id]
		q.pushFrom = min(q.pushFrom, e.id)
		for i := range e.ops {
			if op := &e.ops[i]; op.cas.mode == casBase {
				if n, ok := mapping[op.cas.base]; ok {
					op.cas.base = n
				}
			}
		}
		if err := st.putEntry(e); err != nil {
			return err
		}
		q.nextID = max(q.nextID, e.id+1)
		q.lowID = min(q.lowID, e.id)
	}
	for _, r := range rowList {
		cur, err := st.touched(r.Entity, r.Key)
		if err != nil {
			return err
		}
		for i, id := range cur {
			if n, ok := mapping[id]; ok {
				cur[i] = n
			}
		}
		slices.Sort(cur)
		if err := st.putTouched(r.Entity, r.Key, cur); err != nil {
			return err
		}
	}
	// The queue ends after the highest entry left in it.
	top := q.lowID
	for id := q.lowID; id < q.nextID; id++ {
		e, err := st.entry(id)
		if err != nil {
			return err
		}
		if e != nil {
			top = id + 1
		}
	}
	q.nextID = top
	return nil
}

// logRefusals logs rows the local engine refused to store.
func (w *workspace) logRefusals(ctx context.Context, res *txResult) {
	for _, err := range res.refusals {
		w.logger().LogAttrs(ctx, slog.LevelWarn, "row kept at its confirmed state", slog.Any("error", err))
	}
	if len(res.refusals) > 0 {
		w.update(func(s *status) { s.lastErr = res.refusals[len(res.refusals)-1] })
	}
}

// textCollision reports the origin's refusal of a text document whose item
// ids it holds with other content. The writer's peer minted those ids in a
// copy of the row that did not hold the ones it minted before: a row the
// local engine does not hold, under a partial subscription.
func textCollision(e *entry, r resultRecord) bool {
	if e.reminted || r.code != server.CodeInvalidArgument || !strings.Contains(r.message, "text: merge conflict") {
		return false
	}
	for _, op := range e.ops {
		if len(op.text) > 0 {
			return true
		}
	}
	return false
}

// remint queues e again, after the writes queued behind it, with each of its
// text documents written afresh under a peer unique to the link and the
// entry: the text it shows, without the formatting marks, as new items the
// origin cannot hold already. Its id changes; its ref stays. The writes
// behind it that touch its rows are folded into it (see rebase.Requeue), so
// it does not undo them; an entry they leave nothing of is dropped as
// applied.
func (t *txn) remint(e *entry) error {
	peer := fmt.Sprintf("upstream:%s:%d", t.w.clientID, e.ref)
	for i := range e.ops {
		op := &e.ops[i]
		for path, doc := range op.text {
			d, err := text.Decode(doc, "")
			if err != nil {
				return err
			}
			fresh := text.New(peer)
			if s := d.PlainText(); s != "" {
				if err := fresh.Insert(0, s); err != nil {
					return err
				}
			}
			op.text[path] = fresh.Encode()
		}
	}
	ops, err := t.requeueOps(e)
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		return t.drop(e, nil)
	}
	rows := e.rows()
	if err := t.st.deleteEntry(e); err != nil {
		return err
	}
	e.ops = ops
	t.cache[e.id] = nil
	t.q.unsettled = slices.DeleteFunc(t.q.unsettled, func(id uint64) bool { return id == e.id })
	id, _ := t.w.l.allocate(t.w.id, t.q.nextID, nil)
	e.id = id
	e.reminted = true
	t.q.nextID = id + 1
	if err := t.st.putEntry(e); err != nil {
		return err
	}
	for _, r := range e.rows() {
		ids, err := t.st.touched(r.Entity, r.Key)
		if err != nil {
			return err
		}
		if err := t.st.putTouched(r.Entity, r.Key, append(ids, id)); err != nil {
			return err
		}
	}
	t.cache[id] = e
	t.touch(rows...)
	return nil
}

// requeueOps returns the ops of e as remint sends them again: each folded
// with the ops that later entries make on its row (see rebase.Requeue), and
// without the ops those leave nothing of. A custom entry keeps its ops: the
// origin runs the mutator, not the ops.
func (t *txn) requeueOps(e *entry) ([]entryOp, error) {
	if e.custom != nil {
		return e.ops, nil
	}
	out := make([]entryOp, 0, len(e.ops))
	for _, op := range e.ops {
		m := t.b.models[op.entity]
		if m == nil {
			out = append(out, op)
			continue
		}
		later, err := t.laterOps(RowRef{Entity: op.entity, Key: op.key}, e.id)
		if err != nil {
			return nil, err
		}
		next, keep, err := m.Requeue(op.rebaseOp(), later)
		if err != nil {
			// A value that does not fold goes as captured, as before the
			// fold: the origin decides it.
			out = append(out, op)
			continue
		}
		if !keep {
			continue
		}
		op.kind, op.value, op.mask, op.text = next.Kind, next.Value, next.Mask, next.Text
		out = append(out, op)
	}
	return out, nil
}

// laterOps returns the ops that the entries after id make on row r, in queue
// order.
func (t *txn) laterOps(r RowRef, id uint64) ([]rebase.Op, error) {
	ids, err := t.st.touched(r.Entity, r.Key)
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	var ops []rebase.Op
	for _, later := range ids {
		if later <= id {
			continue
		}
		e, err := t.entry(later)
		if err != nil {
			return nil, err
		}
		if e == nil {
			continue
		}
		for _, op := range e.ops {
			if op.entity == r.Entity && op.key == r.Key {
				ops = append(ops, op.rebaseOp())
			}
		}
	}
	return ops, nil
}
