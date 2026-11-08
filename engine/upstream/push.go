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
	cruds := make([]*syncplanev1.CrudMutation, 0, len(e.ops))
	for _, op := range e.ops {
		crud := &syncplanev1.CrudMutation{Entity: op.entity, Key: op.key, Op: op.kind}
		if op.kind != syncplanev1.CrudOp_CRUD_OP_DELETE {
			crud.Value = forwardedValue(b, op)
		}
		if op.kind == syncplanev1.CrudOp_CRUD_OP_PATCH {
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

// forwardedValue is the value the origin receives for a PUT or PATCH op.
func forwardedValue(b *bound, op entryOp) []byte {
	value := op.value
	if model := b.models[op.entity]; model != nil {
		// A value that cannot be completed goes as captured; the origin's
		// verdict on it is reported like any other.
		if v, err := model.UpstreamValue(op.rebaseOp()); err == nil {
			value = v
		}
	}
	if value == nil {
		value = []byte{}
	}
	return value
}

// baseVersion resolves a compare-and-swap whose base is another entry: the
// sequence the origin applied the base at, and impossibleVersion when it
// rejected the base or did not say the sequence. wait reports a base the
// origin has not answered; the version is then a placeholder.
//
// The confirmed version is no substitute for an unknown sequence: it includes
// every write to the row after the base, so a compare-and-swap made against
// the base would apply over a change it never saw. An origin that predates
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
		return casVersionOf(r), false, nil
	}
}

// casVersionOf is the version a compare-and-swap based on an applied entry
// expects: the sequence the origin applied it at, or impossibleVersion when
// the answer did not say.
func casVersionOf(r resultRecord) uint64 {
	if r.seq == 0 {
		return impossibleVersion
	}
	return r.seq
}

// recordResults stores the origin's verdicts on a pushed batch. A resend
// answered ALREADY_APPLIED keeps the verdict an earlier answer recorded.
func recordResults(st stateTx, q *queueMeta, bt batch, wr *syncplanev1.WorkspaceResults) ([]verdictEvent, uint64, error) {
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
				return nil, 0, err
			} else if ok {
				continue
			}
		}
		if err := st.putResult(e.id, rec); err != nil {
			return nil, 0, err
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
	return evs, ackedSeq, advancePushFrom(st, q)
}

// push sends the queue to the origin until nothing unanswered is left.
func (w *workspace) push(ctx context.Context) error {
	for {
		if !w.ackKnown.Load() || w.blocked() {
			return nil
		}
		var bt batch
		_, err := w.txn(ctx, func(t *txn) error {
			var err error
			bt, err = buildBatch(t.b, t.st, t.q, w.clientID, batchLimits{
				maxN: w.l.cfg.MaxPushMutations, maxBytes: w.l.cfg.MaxPushBytes, single: w.isolate,
			})
			return err
		})
		if err != nil {
			return fmt.Errorf("upstream: read queue: %w", err)
		}
		if len(bt.muts) == 0 {
			return nil
		}
		wr, err := w.send(ctx, bt)
		if err != nil {
			// A failure the link resolved (a realigned queue, a dropped
			// mutation) leaves the rest of the queue to push.
			if err := w.pushFailed(ctx, bt, err); err != nil {
				return err
			}
			continue
		}
		res, err := w.txn(ctx, func(t *txn) error {
			evs, ackedSeq, err := recordResults(t.st, &t.q, bt, wr)
			if err != nil {
				return err
			}
			t.res.verdicts = append(t.res.verdicts, evs...)
			if ackedSeq > t.meta.ackedSeq {
				t.meta.ackedSeq, t.metaDirty = ackedSeq, true
			}
			return t.settle()
		})
		if err != nil {
			return fmt.Errorf("upstream: record push results: %w", err)
		}
		w.isolate = false
		w.poison = poisonCount{}
		w.logRefusals(ctx, res)
		w.update(func(s *status) { s.offline, s.noAccess, s.lastErr = false, false, nil })
	}
}

// send pushes one batch and returns its workspace result. A batch the origin
// refused as a whole returns a *client.WorkspaceError.
func (w *workspace) send(ctx context.Context, bt batch) (_ *syncplanev1.WorkspaceResults, err error) {
	ctx, span := w.l.mx.span(ctx, "syncplane.upstream.push", w.id)
	defer func() {
		w.l.mx.count(ctx, w.l.mx.pushes, err)
		endSpan(span, err)
	}()
	resp, err := w.l.rpc.Push(ctx, connect.NewRequest(&syncplanev1.PushRequest{
		Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: w.id, Mutations: bt.muts}},
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

// pushFailed decides what a failed push means for the queue.
func (w *workspace) pushFailed(ctx context.Context, bt batch, err error) error {
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
// to decide on a skew when it changed. It reports no status change itself: it
// runs inside a write-through capture, where a status callback must not run.
// The next status change carries it.
func (w *workspace) observeRevision(rev string) {
	w.vmu.Lock()
	changed := w.st.revision != rev
	w.st.revision = rev
	w.vmu.Unlock()
	if changed {
		w.kick()
	}
}
