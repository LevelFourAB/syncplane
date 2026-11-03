package upstream

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/client"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

// receipt is where a capture leaves what it recorded, for the Go app API.
type receipt struct {
	ref      uint64
	localSeq uint64
	queued   bool
}

type receiptKey struct{}

// wtState is the in-memory write-through bookkeeping of one workspace. A
// write-through push can reach the origin for a local transaction that then
// does not commit, so the ids it used are kept out of later entries.
type wtState struct {
	// floor is the lowest id a new entry may take.
	floor uint64
	// pushed are the write-through pushes, by ascending id, whose local
	// transaction may not have committed. A transaction that runs again (a
	// downstream batch rerun under savepoints, an adapter retry) captures its
	// mutations again in order; each takes the id and verdict of the push its
	// first run made for the same mutation, so the origin applies it once.
	pushed []*wtRecord
}

type wtRecord struct {
	id      uint64
	payload []byte
	result  *resultRecord
}

// capture is the server.WriteCapture the link installs. It turns a local
// mutation into a pending entry in the mutation's own transaction.
func (l *Link) capture(ctx context.Context, cw *server.CapturedWrite) error {
	var ops []server.CapturedOp
	pullOnly := ""
	for _, op := range cw.Ops {
		dir, ok := l.bound[op.Entity]
		if !ok {
			continue
		}
		if dir == PullOnly && pullOnly == "" {
			pullOnly = op.Entity
		}
		ops = append(ops, op)
	}
	if cw.Custom == nil {
		if pullOnly != "" {
			return server.Reject(server.CodePermissionDenied,
				fmt.Sprintf("entity %q is synced from the origin and cannot be written locally", pullOnly))
		}
		if len(ops) == 0 {
			return nil
		}
	} else if len(ops) == 0 && len(cw.Ops) > 0 {
		// A local mutator that wrote only local-only entities.
		return nil
	}
	b := l.bind.Load()
	if b == nil {
		return ErrNotStarted
	}
	st := writeState(cw.Tx)
	q, linked, err := st.queue()
	if err != nil {
		return err
	}
	if !linked || !q.linked {
		return server.Reject(server.CodePermissionDenied,
			fmt.Sprintf("workspace %q is not linked to the origin", cw.Workspace))
	}
	if q.revoked {
		return server.Reject(server.CodeWorkspaceAccessRevoked,
			fmt.Sprintf("the origin revoked access to workspace %q", cw.Workspace))
	}

	e := &entry{localSeq: cw.Seq}
	if cw.Origin != nil {
		o := *cw.Origin
		o.Targets = slices.Clone(o.Targets)
		e.origin = &o
	}
	if cw.Custom != nil {
		e.custom = proto.Clone(cw.Custom).(*syncplanev1.CustomMutation)
	}
	for _, op := range ops {
		model := b.models[op.Entity]
		eop := entryOp{
			entity: op.Entity, key: op.Key, kind: op.Op,
			value: bytes.Clone(op.Value), mask: slices.Clone(op.Mask),
		}
		text, err := model.CaptureText(rebase.Op{Kind: op.Op, Value: op.Value, Mask: op.Mask}, op.After)
		if err != nil {
			return fmt.Errorf("upstream: capture %s %q: %w", op.Entity, op.Key, err)
		}
		eop.text = text
		if op.ExpectedVersion != nil && cw.Custom == nil {
			if eop.cas, err = translateCAS(st, e, op); err != nil {
				return err
			}
		}
		e.ops = append(e.ops, eop)
	}

	w := l.workspace(cw.Workspace)
	through := l.cfg.WriteMode == ModeWriteThrough
	if m, ok := ctx.Value(writeModeKey{}).(WriteMode); ok {
		through = m == ModeWriteThrough
	}
	if through {
		if w == nil {
			return fmt.Errorf("upstream: workspace %q is not running; write-through needs a started link", cw.Workspace)
		}
		if w.blocked() {
			return ErrSchemaSkew
		}
		if !w.ackKnown.Load() {
			if err := l.learnWatermark(ctx, w, st, &q); err != nil {
				return err
			}
		}
	}

	if q.nextRef == 0 {
		q.nextRef = 1
	}
	e.ref = q.nextRef
	q.nextRef++
	payload, err := (&entry{localSeq: e.localSeq, custom: e.custom, ops: e.ops}).encode()
	if err != nil {
		return err
	}
	var known *resultRecord
	e.id, known = l.allocate(cw.Workspace, q.nextID, payload)
	q.nextID = e.id + 1
	if err := st.putEntry(e); err != nil {
		return err
	}
	for _, r := range e.rows() {
		ids, err := st.touched(r.Entity, r.Key)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			if err := keepConfirmed(st, r, ops); err != nil {
				return err
			}
		}
		if !slices.Contains(ids, e.id) {
			ids = append(ids, e.id)
		}
		if err := st.putTouched(r.Entity, r.Key, ids); err != nil {
			return err
		}
	}
	switch {
	case known != nil:
		if err := l.recordKnown(st, &q, e, *known); err != nil {
			return err
		}
		if through {
			if err := verdictErr(*known); err != nil {
				return err
			}
		}
	case through:
		if err := l.writeThrough(ctx, w, b, st, &q, e, payload); err != nil {
			return err
		}
	}
	if err := st.putQueue(q); err != nil {
		return err
	}
	if rc, ok := ctx.Value(receiptKey{}).(*receipt); ok {
		*rc = receipt{ref: e.ref, localSeq: e.localSeq, queued: true}
	}
	if w != nil {
		w.kick()
	}
	return nil
}

// keepConfirmed gives the elided record of row r its value back, as the row
// gets its first pending entry: the record named the local row, which the
// captured write replaced. The value is the row before the first of ops on r.
// A local row at another version than the record names was rewritten outside
// the link, so the record keeps that row as a stale value (see confirmedRow).
func keepConfirmed(st stateTx, r RowRef, ops []server.CapturedOp) error {
	c, ok, err := st.confirmed(r.Entity, r.Key)
	if err != nil || !ok || !c.elided {
		return err
	}
	i := slices.IndexFunc(ops, func(op server.CapturedOp) bool { return op.Entity == r.Entity && op.Key == r.Key })
	if i < 0 {
		return fmt.Errorf("upstream: capture of %s %q holds no write to it", r.Entity, r.Key)
	}
	before := ops[i]
	c.stale = before.Before == nil || before.PrevVersion != c.localVersion
	c.elided, c.localVersion = false, 0
	c.present, c.value = before.Before != nil, bytes.Clone(before.Before)
	return st.putConfirmed(r.Entity, r.Key, c)
}

// translateCAS turns a local compare-and-swap into the origin's terms: the
// origin version of the confirmed row when no pending entry touches the row,
// and otherwise the sequence the latest such entry is applied at.
func translateCAS(st stateTx, e *entry, op server.CapturedOp) (casRef, error) {
	for _, prev := range e.ops {
		if prev.entity == op.Entity && prev.key == op.Key {
			return casRef{}, server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("a compare-and-swap of %s %q after another write to it in the same mutation cannot be forwarded", op.Entity, op.Key))
		}
	}
	ids, err := st.touched(op.Entity, op.Key)
	if err != nil {
		return casRef{}, err
	}
	if len(ids) > 0 {
		return casRef{mode: casBase, base: slices.Max(ids)}, nil
	}
	c, ok, err := st.confirmed(op.Entity, op.Key)
	if err != nil {
		return casRef{}, err
	}
	if !ok || !c.present {
		return casRef{mode: casVersion}, nil
	}
	return casRef{mode: casVersion, version: c.version}, nil
}

// allocate returns the id of a new entry: the id and verdict of an earlier
// write-through push of the same mutation that is not committed yet, or else
// the queue's next id, above every id a write-through push may have used.
func (l *Link) allocate(ws string, next uint64, payload []byte) (uint64, *resultRecord) {
	l.wtMu.Lock()
	defer l.wtMu.Unlock()
	s := l.wt[ws]
	if s == nil {
		return next, nil
	}
	// next counts this transaction's own earlier captures, so a push below
	// it is not a candidate; the pushes stay until the committed queue moves
	// past them (see pruneThrough).
	for _, r := range s.pushed {
		if r.id >= next && bytes.Equal(r.payload, payload) {
			return r.id, r.result
		}
	}
	return max(next, s.floor), nil
}

// pruneThrough forgets the write-through pushes below next, the next id of the
// committed queue: their transactions committed or were replaced.
func (l *Link) pruneThrough(ws string, next uint64) {
	l.wtMu.Lock()
	defer l.wtMu.Unlock()
	if s := l.wt[ws]; s != nil {
		s.pushed = slices.DeleteFunc(s.pushed, func(r *wtRecord) bool { return r.id < next })
	}
}

// recordKnown stores a verdict a push for an earlier run of the transaction
// returned.
func (l *Link) recordKnown(st stateTx, q *queueMeta, e *entry, r resultRecord) error {
	if err := st.putResult(e.id, r); err != nil {
		return err
	}
	if r.status == syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED && !slices.Contains(q.unsettled, e.id) {
		q.unsettled = append(q.unsettled, e.id)
	}
	return advancePushFrom(st, q)
}

func (l *Link) wtState(ws string) *wtState {
	if l.wt == nil {
		l.wt = map[string]*wtState{}
	}
	s := l.wt[ws]
	if s == nil {
		s = &wtState{}
		l.wt[ws] = s
	}
	return s
}

// raiseFloor keeps ids below floor out of new entries.
func (l *Link) raiseFloor(ws string, floor uint64) {
	l.wtMu.Lock()
	defer l.wtMu.Unlock()
	s := l.wtState(ws)
	s.floor = max(s.floor, floor)
}

// resetFloor forgets the write-through bookkeeping of a workspace whose
// queue was realigned with the origin's watermark.
func (l *Link) resetFloor(ws string) {
	l.wtMu.Lock()
	defer l.wtMu.Unlock()
	delete(l.wt, ws)
}

// learnWatermark reads the origin's watermark for a write-through write made
// before the workspace's worker read it, and aligns the queue with it.
func (l *Link) learnWatermark(ctx context.Context, w *workspace, st stateTx, q *queueMeta) error {
	meta, err := st.sync()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, l.cfg.WriteThroughTimeout)
	defer cancel()
	acked, err := l.readWatermark(ctx, w.id, w.clientID, meta.cursor)
	if err != nil {
		return throughErr(err)
	}
	if err := establishAck(st, q, acked, w.startNext, func(move []*entry, ids []uint64) error {
		return renumber(st, q, move, ids)
	}); err != nil {
		return err
	}
	l.raiseFloor(w.id, acked+1)
	return nil
}

// writeThrough pushes the queue up to and including e, and returns the
// origin's verdict on e: nil when applied, a server Rejection when rejected,
// and an error when the origin cannot be reached. A mutation-id gap is
// resolved once, by renumbering the queue onto the origin's watermark.
func (l *Link) writeThrough(ctx context.Context, w *workspace, b *bound, st stateTx, q *queueMeta, e *entry, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.WriteThroughTimeout)
	defer cancel()
	realigned := false
	for {
		r, answered, err := st.result(e.id)
		if err != nil {
			return err
		}
		if answered {
			l.noteThrough(w.id, e.id, payload, &r)
			if r.seq != 0 {
				meta, err := st.sync()
				if err != nil {
					return err
				}
				if r.seq > meta.ackedSeq {
					meta.ackedSeq = r.seq
					if err := st.putSync(meta); err != nil {
						return err
					}
				}
			}
			return verdictErr(r)
		}
		bt, err := buildBatch(b, st, *q, w.clientID, batchLimits{
			maxN: l.cfg.MaxPushMutations, maxBytes: l.cfg.MaxPushBytes, until: e.id,
		})
		if err != nil {
			return err
		}
		if len(bt.ents) == 0 {
			return fmt.Errorf("upstream: write-through: nothing to push for mutation %d", e.id)
		}
		l.noteThrough(w.id, e.id, payload, nil)
		wr, err := w.send(ctx, bt)
		if err == nil {
			if _, _, err := recordResults(st, q, bt, wr); err != nil {
				return err
			}
			continue
		}
		switch code := client.CodeOf(err); {
		case code == client.CodeWorkspaceAccessRevoked:
			w.kick()
			return server.Reject(code, err.Error())
		case code == client.CodeMutationIDGap && !realigned:
			// An earlier write-through push failed in transit and never
			// reached the origin, which left its id unused there.
			realigned = true
			if err := l.realign(ctx, w, st, q, e); err != nil {
				return err
			}
			continue
		}
		w.kick()
		return throughErr(err)
	}
}

// realign renumbers the queue onto the origin's watermark, e included.
func (l *Link) realign(ctx context.Context, w *workspace, st stateTx, q *queueMeta, e *entry) error {
	meta, err := st.sync()
	if err != nil {
		return err
	}
	acked, err := l.readWatermark(ctx, w.id, w.clientID, meta.cursor)
	if err != nil {
		return throughErr(err)
	}
	mapping, err := compactAbove(st, q, acked)
	if err != nil {
		return err
	}
	if id, ok := mapping[e.id]; ok {
		e.id = id
	}
	l.resetFloor(w.id)
	return nil
}

// noteThrough records a write-through push: its id is used at the origin
// whether or not the local transaction commits.
func (l *Link) noteThrough(ws string, id uint64, payload []byte, r *resultRecord) {
	l.wtMu.Lock()
	defer l.wtMu.Unlock()
	s := l.wtState(ws)
	s.floor = max(s.floor, id+1)
	rec := &wtRecord{id: id, payload: payload, result: r}
	i, found := slices.BinarySearchFunc(s.pushed, id, func(r *wtRecord, id uint64) int { return cmp.Compare(r.id, id) })
	if found {
		s.pushed[i] = rec
		return
	}
	s.pushed = slices.Insert(s.pushed, i, rec)
}

func verdictErr(r resultRecord) error {
	if r.status == syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
		return server.Reject(r.code, r.message)
	}
	return nil
}

// ErrOriginUnavailable is wrapped by the error a write-through write fails
// with when the origin did not answer. The write did not commit locally; it
// may have been applied at the origin, and arrives back through a pull then.
var ErrOriginUnavailable = errors.New("upstream: origin unavailable")

func throughErr(err error) error {
	return fmt.Errorf("%w: %w", ErrOriginUnavailable, err)
}
