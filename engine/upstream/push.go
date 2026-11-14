package upstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/levelfourab/syncplane/engine/client"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// batch is one push's worth of entries, in id order, with their wire form.
type batch struct {
	muts []*syncplanev1.Mutation
	ents []*entry
}

// batchLimits bound one push.
type batchLimits struct {
	maxN, maxBytes int
	// single stops after the first entry.
	single bool
	// until stops after the entry with this id; 0 means no bound.
	until uint64
}

// buildBatch collects the entries the origin has not answered, from the head
// of the queue in id order. It stops before an entry whose compare-and-swap
// base is in the batch: that entry's expected version is the origin sequence
// its base is applied at, which only the base's answer tells.
func buildBatch(b *bound, st stateTx, q queueMeta, clientID string, lim batchLimits) (batch, error) {
	var out batch
	size := 0
	in := map[uint64]bool{}
	for id := max(q.lowID, q.pushFrom); id < q.nextID; id++ {
		if lim.until != 0 && id > lim.until {
			break
		}
		e, err := st.entry(id)
		if err != nil {
			return batch{}, err
		}
		if e == nil {
			continue
		}
		if _, answered, err := st.result(id); err != nil {
			return batch{}, err
		} else if answered {
			continue
		}
		m, wait, err := mutationOf(b, st, e, in)
		if err != nil {
			return batch{}, err
		}
		if wait && len(out.ents) > 0 {
			break
		}
		m.ClientId = clientID
		n := proto.Size(m)
		if len(out.ents) > 0 && (len(out.ents) >= lim.maxN || size+n > lim.maxBytes) {
			break
		}
		size += n
		in[id] = true
		out.muts = append(out.muts, m)
		out.ents = append(out.ents, e)
		if lim.single {
			break
		}
	}
	return out, nil
}

// mutationOf builds the wire mutation of an entry. wait reports a
// compare-and-swap base still unanswered in the batch.
func mutationOf(b *bound, st stateTx, e *entry, inBatch map[uint64]bool) (_ *syncplanev1.Mutation, wait bool, _ error) {
	m := &syncplanev1.Mutation{MutationId: e.id}
	if e.custom != nil {
		m.Payload = &syncplanev1.Mutation_Custom{Custom: e.custom}
		return m, false, nil
	}
	ops := b.sendableOps(e.ops)
	if len(ops) == 0 {
		// Only an entry stored under a schema that closed fewer fields gets
		// here: a capture keeps a write with nothing to send out of the
		// queue. Sent as captured, the origin decides it, and its id does
		// not leave a gap.
		ops = e.ops
	}
	cruds := make([]*syncplanev1.CrudMutation, 0, len(ops))
	for _, op := range ops {
		crud := &syncplanev1.CrudMutation{Entity: op.entity, Key: op.key, Op: op.kind}
		if op.kind != syncplanev1.CrudOp_CRUD_OP_DELETE {
			v, err := forwardedValue(b, st, op, b.maxValue)
			if err != nil {
				return nil, false, err
			}
			crud.Value = v
		}
		// A PUT goes with the fields the local caller wrote, so the origin keeps
		// its own values of the rest: the link's principal may write fields
		// that the local caller may not.
		if op.kind == syncplanev1.CrudOp_CRUD_OP_PATCH || (op.kind == syncplanev1.CrudOp_CRUD_OP_PUT && len(op.mask) > 0) {
			crud.FieldMask = &fieldmaskpb.FieldMask{Paths: op.mask}
		}
		switch op.cas.mode {
		case casVersion:
			v := op.cas.version
			crud.ExpectedVersion = &v
		case casBase:
			v, w, err := baseVersion(st, op)
			if err != nil {
				return nil, false, err
			}
			if w || inBatch[op.cas.base] {
				wait = true
			}
			crud.ExpectedVersion = &v
		}
		cruds = append(cruds, crud)
	}
	if len(cruds) == 1 {
		m.Payload = &syncplanev1.Mutation_Crud{Crud: cruds[0]}
	} else {
		m.Payload = &syncplanev1.Mutation_Batch{Batch: &syncplanev1.BatchMutation{Mutations: cruds}}
	}
	return m, wait, nil
}

// forwardedValue is the value the origin receives for a PUT or PATCH op. A
// PATCH whose full text documents make the value larger than maxValue carries
// them as fragments against the confirmed row, when the link holds the
// origin's own state of it.
func forwardedValue(b *bound, st stateTx, op entryOp, maxValue int) ([]byte, error) {
	value := op.value
	model := b.models[op.entity]
	if model != nil {
		// A value that cannot be completed goes as captured; the origin's
		// verdict on it is reported like any other.
		if v, err := model.UpstreamValue(op.rebaseOp()); err == nil {
			value = v
		}
	}
	// Full documents are the default: a fragment cut against a confirmed row
	// the origin replaced since (a delete and a new row) is refused, where
	// the full document merges. A value over the origin's limit is refused
	// in full anyway.
	if len(value) > maxValue && model != nil && model.HasText() && op.kind == syncplanev1.CrudOp_CRUD_OP_PATCH {
		c, ok, err := st.confirmed(op.entity, op.key)
		if err != nil {
			return nil, err
		}
		if ok && c.present && !c.elided && !c.stale {
			if v, err := model.UpstreamFragments(op.rebaseOp(), c.value); err == nil {
				value = v
			}
		}
	}
	if value == nil {
		value = []byte{}
	}
	return value, nil
}

// baseVersion resolves a compare-and-swap whose base is another entry: the
// sequence the origin applied the base at (see casVersionOf), and
// impossibleVersion when it rejected the base or did not say the sequence.
// wait reports a base the origin has not answered; the version is then a
// placeholder.
//
// The current confirmed version is no substitute for an unknown sequence: it
// includes every write to the row after the capture, so a compare-and-swap
// made against the base would apply over a change it never saw. An origin that predates
// sequences on ALREADY_APPLIED, or that no longer holds the sequence, leaves
// it unknown.
func baseVersion(st stateTx, op entryOp) (version uint64, wait bool, err error) {
	r, ok, err := st.result(op.cas.base)
	if err != nil {
		return 0, false, err
	}
	switch {
	case !ok:
		return 0, true, nil
	case r.status == syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED:
		return impossibleVersion, false, nil
	default:
		return casVersionOf(r, op.cas), false, nil
	}
}

// casVersionOf is the version a compare-and-swap based on an applied entry
// expects: the later of the sequence the origin applied it at and the
// confirmed version cas recorded at capture, or impossibleVersion when the
// answer did not say the sequence.
func casVersionOf(r resultRecord, cas casRef) uint64 {
	if r.seq == 0 {
		return impossibleVersion
	}
	return max(r.seq, cas.version)
}

// noteSend counts a push of the batch's entries, and forgets the ids below low,
// the queue's lowest id: no entry holds them. It returns the highest id the
// link may have pushed before this push, for its sent_through.
func (w *workspace) noteSend(bt batch, low uint64) uint64 {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	sent := w.sent
	for _, e := range bt.ents {
		w.sent = max(w.sent, e.id)
	}
	if low > 0 {
		for id := range w.sends {
			if id < low {
				delete(w.sends, id)
			}
		}
	}
	for _, e := range bt.ents {
		w.sends[e.id]++
	}
	return sent
}

// adoptSent counts the ids up to the origin's watermark acked as pushed by
// the link: the link takes the watermark over, and resends an unanswered entry
// at or below it as its own.
func (w *workspace) adoptSent(acked uint64) {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	w.sent = max(w.sent, acked)
}

// firstSend reports a mutation id that no push before the one just answered
// carried: this runtime captured it and sent it once. The origin cannot have
// applied it before, so ALREADY_APPLIED for it means another writer uses the
// client id.
func (w *workspace) firstSend(id uint64) bool {
	if id < w.startNext {
		return false
	}
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.sends[id] == 1
}

// recordResults stores the origin's verdicts on a pushed batch. A resend
// answered ALREADY_APPLIED keeps the verdict an earlier answer recorded. A
// first send answered ALREADY_APPLIED (see firstSend) is recorded as rejected
// with CodeClientIDInUse, and its id is returned in inUse.
func recordResults(st stateTx, q *queueMeta, bt batch, wr *syncplanev1.WorkspaceResults, first func(uint64) bool) (_ []verdictEvent, _ uint64, inUse []uint64, _ error) {
	byID := make(map[uint64]*entry, len(bt.ents))
	for _, e := range bt.ents {
		byID[e.id] = e
	}
	var evs []verdictEvent
	var ackedSeq uint64
	for _, r := range wr.GetResults() {
		e := byID[r.GetMutationId()]
		if e == nil {
			continue
		}
		rec := resultRecord{status: r.GetStatus(), seq: r.GetSeq(), code: r.GetErrorCode(), message: r.GetErrorMessage()}
		if rec.status == syncplanev1.MutationStatus_MUTATION_STATUS_UNSPECIFIED {
			continue
		}
		if rec.status == syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED {
			if _, ok, err := st.result(e.id); err != nil {
				return nil, 0, nil, err
			} else if ok {
				continue
			}
			if first(e.id) {
				// The origin holds another writer's mutation under this id,
				// so this one never applies. Reporting it keeps the loss
				// visible; recording it as applied would drop it silently.
				rec = resultRecord{status: syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED, code: CodeClientIDInUse,
					message: fmt.Sprintf("the origin holds mutation %d of client id %q from another writer", e.id, bt.muts[0].GetClientId())}
				inUse = append(inUse, e.id)
			}
		}
		if err := st.putResult(e.id, rec); err != nil {
			return nil, 0, nil, err
		}
		ackedSeq = max(ackedSeq, rec.seq)
		if rec.status == syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
			if !slices.Contains(q.unsettled, e.id) {
				q.unsettled = append(q.unsettled, e.id)
			}
		} else {
			evs = append(evs, verdictEvent{ref: e.ref, localSeq: e.localSeq})
		}
	}
	return evs, ackedSeq, inUse, advancePushFrom(st, q)
}

// push sends the queue to the origin until nothing unanswered is left.
func (w *workspace) push(ctx context.Context) error {
	for {
		if !w.ackKnown.Load() || w.blocked() {
			return nil
		}
		var bt batch
		var low uint64
		_, err := w.txn(ctx, func(t *txn) error {
			var err error
			bt, err = buildBatch(t.b, t.st, t.q, w.clientID, batchLimits{
				maxN: w.l.cfg.MaxPushMutations, maxBytes: w.l.cfg.MaxPushBytes, single: w.isolate,
			})
			low = t.q.lowID
			return err
		})
		if err != nil {
			return fmt.Errorf("upstream: read queue: %w", err)
		}
		if len(bt.muts) == 0 {
			return nil
		}
		sent := w.noteSend(bt, low)
		wr, err := w.send(ctx, bt, sent)
		if err != nil {
			// A failure the link resolved (a realigned queue, a dropped
			// mutation) leaves the rest of the queue to push.
			if err := w.pushFailed(ctx, bt, sent, err); err != nil {
				return err
			}
			continue
		}
		var inUse []uint64
		res, err := w.txn(ctx, func(t *txn) error {
			evs, ackedSeq, iu, err := recordResults(t.st, &t.q, bt, wr, w.firstSend)
			if err != nil {
				return err
			}
			inUse = iu
			t.res.verdicts = append(t.res.verdicts, evs...)
			if ackedSeq > t.meta.ackedSeq {
				t.meta.ackedSeq, t.metaDirty = ackedSeq, true
			}
			return t.settle()
		})
		if err != nil {
			return fmt.Errorf("upstream: record push results: %w", err)
		}
		for _, id := range inUse {
			w.logger().LogAttrs(ctx, slog.LevelWarn, "another writer uses this link's client id; rejecting a write the origin never applied",
				slog.Uint64("mutation_id", id), slog.String("client_id", w.clientID))
		}
		w.isolate = false
		w.poison = poisonCount{}
		w.logRefusals(ctx, res)
		w.update(func(s *status) { s.offline, s.noAccess, s.lastErr = false, false, nil })
	}
}

// send pushes one batch and returns its workspace result. A batch the origin
// refused as a whole returns a *client.WorkspaceError.
func (w *workspace) send(ctx context.Context, bt batch, sent uint64) (_ *syncplanev1.WorkspaceResults, err error) {
	ctx, span := w.l.mx.span(ctx, "syncplane.upstream.push", w.id)
	defer func() {
		w.l.mx.count(ctx, w.l.mx.pushes, err)
		endSpan(span, err)
	}()
	resp, err := w.l.rpc.Push(ctx, connect.NewRequest(&syncplanev1.PushRequest{
		Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: w.id, Mutations: bt.muts, SentThrough: &sent}},
	}))
	if err != nil {
		return nil, err
	}
	w.observeRevision(resp.Msg.GetSchemaRevision())
	for _, wr := range resp.Msg.GetResults() {
		if wr.GetWorkspaceId() != w.id {
			continue
		}
		if code := wr.GetErrorCode(); code != "" {
			return nil, &client.WorkspaceError{Workspace: w.id, Code: code, Message: wr.GetErrorMessage()}
		}
		return wr, nil
	}
	return nil, fmt.Errorf("upstream: push answered no result for workspace %q", w.id)
}

// pushFailed decides what a failed push means for the queue. sent is the
// sent_through the push carried.
func (w *workspace) pushFailed(ctx context.Context, bt batch, sent uint64, err error) error {
	var we *client.WorkspaceError
	isBatch := errors.As(err, &we)
	code := client.CodeOf(err)
	switch {
	case code == client.CodeWorkspaceAccessRevoked:
		return errors.Join(errRevokedWS, err)
	case code == client.CodeWorkspaceNoAccess:
		return errors.Join(errNoAccess, err)
	case isBatch && code == client.CodeMutationIDGap:
		return w.resolveGap(ctx)
	case isBatch && code == client.CodeClientIDInUse:
		return w.resolveInUse(ctx, sent)
	case isBatch && code == client.CodeInternal:
		w.poison = poisonCount{}
		return err
	case isBatch, connect.CodeOf(err) == connect.CodeInvalidArgument && isConnectErr(err):
		// A verdict on the batch itself, which a resend gets again. Push the
		// head alone to tell which mutation it is, and drop that one once it
		// failed the same way PoisonThreshold times.
		key := code + "\u0000" + err.Error()
		if w.poison.key == key {
			w.poison.n++
		} else {
			w.poison = poisonCount{key: key, n: 1}
		}
		if len(bt.ents) > 1 {
			w.isolate = true
			return err
		}
		if th := w.l.cfg.PoisonThreshold; th > 0 && w.poison.n >= th {
			if code == "" {
				code = client.CodeInvalidArgument
			}
			msg := err.Error()
			if we != nil {
				msg = we.Message
			}
			w.logger().LogAttrs(ctx, slog.LevelWarn, "dropping a write the origin keeps failing",
				slog.Uint64("mutation_id", bt.ents[0].id), slog.String("code", code), slog.String("message", msg))
			head, headID := bt.ents[0].ref, bt.ents[0].id
			res, terr := w.txn(ctx, func(t *txn) error {
				e, err := t.entry(headID)
				if err != nil || e == nil || e.ref != head {
					return err
				}
				return t.reject(e, code, msg)
			})
			if terr != nil {
				return terr
			}
			w.poison = poisonCount{}
			w.isolate = false
			w.logRefusals(ctx, res)
			return nil
		}
		return err
	}
	return err
}

// resolveGap realigns the queue with the origin's watermark after the origin
// reported a mutation-id gap: entries at or below it are resent and answered
// as already applied, and those above it are renumbered to follow it.
func (w *workspace) resolveGap(ctx context.Context) error {
	acked, err := w.watermark(ctx)
	if err != nil {
		return err
	}
	_, err = w.txn(ctx, func(t *txn) error { return t.compactAbove(acked) })
	if err != nil {
		return err
	}
	w.l.resetFloor(w.id)
	return nil
}

// resolveInUse realigns the queue after the origin reported that another
// writer used ids after sent, the highest id the link pushed. Nothing of the
// batch applied, and the origin holds the other writer's mutations at the ids
// above sent, so the entries there move to follow the watermark. An entry at
// or below sent is resent under its id and answered as already applied: the
// link cannot tell its own earlier attempt from the other writer's mutation.
func (w *workspace) resolveInUse(ctx context.Context, sent uint64) error {
	acked, err := w.watermark(ctx)
	if err != nil {
		return err
	}
	w.logger().LogAttrs(ctx, slog.LevelWarn, "another writer uses this link's client id; moving unsent writes past its mutations",
		slog.String("client_id", w.clientID), slog.Uint64("sent_through", sent))
	if acked > sent {
		if _, err := w.txn(ctx, func(t *txn) error {
			mapping, err := compactFrom(t.st, &t.q, sent+1, acked+1)
			if err != nil || len(mapping) == 0 {
				return err
			}
			return t.reloadView()
		}); err != nil {
			return err
		}
	}
	w.adoptSent(acked)
	w.l.resetFloor(w.id)
	return nil
}

// watermark reads the origin's mutation watermark for the link's client id.
func (w *workspace) watermark(ctx context.Context) (uint64, error) {
	w.mu.Lock()
	cursor := w.meta.cursor
	w.mu.Unlock()
	return w.l.readWatermark(ctx, w.id, w.clientID, cursor)
}

func (l *Link) readWatermark(ctx context.Context, ws, clientID string, cursor []byte) (uint64, error) {
	resp, err := l.rpc.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: ws,
		ClientId:    clientID,
		Cursor:      cursor,
		Limit:       1,
	}))
	if err != nil {
		return 0, classify(err)
	}
	return resp.Msg.GetLastAckedMutationId(), nil
}

// observeRevision records the origin's schema revision, and wakes the worker
// to decide on a skew when it changed. An empty revision changes nothing. It reports no status change itself: it
// runs inside a write-through capture, where a status callback must not run.
// The next status change carries it.
func (w *workspace) observeRevision(rev string) {
	if rev == "" {
		// The answer did not say. See applyPage.
		return
	}
	w.vmu.Lock()
	changed := w.st.revision != rev
	w.st.revision = rev
	w.vmu.Unlock()
	if changed {
		w.kick()
	}
}
