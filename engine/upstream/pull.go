package upstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/trace"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

func endSpan(span trace.Span, err error) { telemetry.EndSpan(span, err) }

// pullDrain pulls pages from the stored cursor until the origin has no more,
// applying each.
func (w *workspace) pullDrain(ctx context.Context) error {
	w.pullMu.Lock()
	defer w.pullMu.Unlock()
	return w.pullDrainLocked(ctx)
}

func (w *workspace) pullDrainLocked(ctx context.Context) error {
	gen := w.gen()
	for {
		w.mu.Lock()
		meta := w.meta
		w.mu.Unlock()
		noDelta := w.l.noDeltaEntities()
		resp, err := w.l.rpc.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId:     w.id,
			ClientId:        w.clientID,
			Cursor:          meta.cursor,
			Subscriptions:   w.l.effectiveSubs(meta.subs),
			Limit:           w.l.cfg.PageLimit,
			MinSeq:          meta.ackedSeq,
			NoDeltaEntities: noDelta,
		}))
		if err != nil {
			return classify(err)
		}
		page := resp.Msg
		w.observeRevision(page.GetSchemaRevision())
		if page.GetReset_() {
			w.unready(true, true)
			return errReset
		}
		if err := w.applyPage(ctx, page, noDelta); err != nil {
			if errors.Is(err, errFullState) {
				continue
			}
			return err
		}
		if !page.GetHasMore() {
			w.caughtUp(gen)
			return nil
		}
	}
}

// applyPage applies one page of origin changes in one local transaction:
// confirmed rows, the entries the page confirms, the local rows they affect,
// and the cursor. noDelta is the no_delta_entities of the request the page
// answers. A page carrying text delta state for a field the local schema does
// not declare, of an entity not in noDelta, adds the entity to the link's
// noDeltaEntities and returns errFullState. Any other page carrying text delta
// state the confirmed rows cannot take asks for a fresh snapshot and returns
// errReset.
func (w *workspace) applyPage(ctx context.Context, page *syncplanev1.PullResponse, noDelta []string) (err error) {
	ctx, span := w.l.mx.span(ctx, "syncplane.upstream.apply_page", w.id)
	defer func() {
		w.l.mx.count(ctx, w.l.mx.pages, err)
		endSpan(span, err)
	}()
	final := !page.GetHasMore()
	w.observeHead(page.GetHeadSeq())
	// An entity leaving the subscriptions is dropped before the page's
	// changes, in pages of its own: the cursor stays where it was until the
	// page commits, so a stop in between pulls the same page and drops again.
	for _, e := range page.GetPatch().GetDropEntities() {
		for after, more := "", true; more; {
			var next string
			var nextMore bool
			_, err := w.txn(ctx, func(t *txn) error {
				var err error
				next, nextMore, err = t.dropEntityPage(e, after)
				return err
			})
			if err != nil {
				return err
			}
			after, more = next, nextMore
		}
	}
	res, err := w.txn(ctx, func(t *txn) error {
		if err := t.establishAck(page.GetLastAckedMutationId()); err != nil {
			return err
		}
		for _, e := range page.GetPatch().GetDropEntities() {
			if err := t.dropPendingRows(e); err != nil {
				return err
			}
		}
		for _, rc := range page.GetPatch().GetChanges() {
			if err := t.applyChange(rc); err != nil {
				return err
			}
		}
		if c := page.GetNextCursor(); len(c) > 0 {
			t.meta.cursor = c
		}
		if s := page.GetSeq(); s > 0 {
			t.meta.originSeq = s
		}
		// An origin from before stream pages carried the revision sends pages
		// without one. An empty revision there means "not said", and storing
		// it would lift a SkewBlock decision on every page.
		if rev := page.GetSchemaRevision(); rev != "" {
			t.meta.schemaRevision = rev
		}
		// An entry the origin applied is dropped on the last page of a
		// drain only: its row may sit on a later page, and dropping it
		// earlier would revert the row in between.
		if final {
			t.meta.lastAcked = page.GetLastAckedMutationId()
		}
		t.metaDirty = true
		return t.settle()
	})
	var undeclared *rebase.UndeclaredTextError
	if errors.As(err, &undeclared) && !slices.Contains(noDelta, undeclared.Entity) {
		// The same span fetched again with the entity named in
		// no_delta_entities carries the documents whole, as unknown fields the
		// confirmed row keeps. An origin that still sends delta state for an
		// entity the request named falls through to a fresh snapshot, so a
		// page is fetched again at most once per entity.
		w.logger().LogAttrs(ctx, slog.LevelWarn, "origin sends text the local schema does not declare; asking for full rows",
			slog.String("entity", undeclared.Entity), slog.String("field", undeclared.Field))
		w.l.askFullState(undeclared.Entity)
		return errFullState
	}
	if errors.Is(err, rebase.ErrTextDeltaUnusable) {
		w.logger().LogAttrs(ctx, slog.LevelWarn, "page carries text the link cannot rebuild; bootstrapping again",
			slog.Any("error", err))
		w.unready(true, false)
		return errReset
	}
	if err != nil {
		return err
	}
	w.logRefusals(ctx, res)
	return nil
}

// bootstrap replaces the workspace's confirmed rows with a snapshot of the
// origin, resuming an interrupted one where the origin allows. Pending
// entries survive: the rows they touch are replayed on the snapshot.
func (w *workspace) bootstrap(ctx context.Context) (err error) {
	w.pullMu.Lock()
	defer w.pullMu.Unlock()
	ctx, span := w.l.mx.span(ctx, "syncplane.upstream.bootstrap", w.id)
	defer func() {
		w.l.mx.count(ctx, w.l.mx.bootstraps, err)
		endSpan(span, err)
	}()
	w.mu.Lock()
	meta := w.meta
	reset := w.resetBoot
	w.mu.Unlock()
	// What an interrupted attempt of this snapshot applied, so a resume does
	// not report a bar that starts again at zero.
	var start BootstrapProgress
	if meta.boot != nil {
		start = meta.boot.progress()
	}
	w.update(func(s *status) { s.bootstrapping, s.live, s.boot = true, false, &start })
	defer w.endProgress()
	if meta.boot != nil && meta.boot.finishing {
		// The snapshot arrived whole before the process stopped; only the
		// retirement of the rows it did not carry is left.
		return w.finishBootstrap(ctx, reset)
	}
	req := &syncplanev1.BootstrapRequest{
		WorkspaceId:   w.id,
		ClientId:      w.clientID,
		Subscriptions: w.l.effectiveSubs(meta.subs),
	}
	if meta.boot != nil {
		req.Resume = &syncplanev1.BootstrapResume{Cursor: meta.boot.cursor, Token: meta.boot.token}
	}
	stream, err := w.l.rpc.Bootstrap(ctx, connect.NewRequest(req))
	if err != nil {
		return classify(err)
	}
	defer stream.Close()
	if !stream.Receive() {
		return bootErr(stream.Err())
	}
	begin := stream.Msg().GetBegin()
	if begin == nil {
		return errors.New("upstream: bootstrap did not start with Begin")
	}
	w.observeRevision(begin.GetSchemaRevision())
	// Begin's seq is the origin's head when the snapshot was taken.
	w.observeHead(begin.GetSeq())
	// Where each announced entity sits in the delivery order. Rows of one
	// entity arrive contiguously and in this order, so a row of entity i
	// proves the i entities before it complete.
	position := make(map[string]uint64, len(begin.GetEntities()))
	for i, e := range begin.GetEntities() {
		position[e] = uint64(i)
	}
	var prog BootstrapProgress
	_, err = w.txn(ctx, func(t *txn) error {
		if err := t.establishAck(begin.GetLastAckedMutationId()); err != nil {
			return err
		}
		if !begin.GetResumed() || t.meta.boot == nil {
			t.meta.gen++
			t.meta.boot = &bootState{
				cursor:   begin.GetCursor(),
				seq:      begin.GetSeq(),
				acked:    begin.GetLastAckedMutationId(),
				revision: begin.GetSchemaRevision(),
			}
		}
		t.meta.boot.estimate = smallerEstimate(t.meta.boot.estimate, beginEstimate(begin))
		t.meta.boot.entitiesTotal = uint64(len(begin.GetEntities()))
		t.metaDirty = true
		prog = t.meta.boot.progress()
		return nil
	})
	if err != nil {
		return err
	}
	w.progress(prog, true)
	for stream.Receive() {
		msg := stream.Msg()
		if chunk := msg.GetChunk(); chunk != nil {
			_, err := w.txn(ctx, func(t *txn) error {
				boot := t.meta.boot
				if boot == nil {
					return errors.New("upstream: bootstrap state lost")
				}
				for _, rc := range chunk.GetRows() {
					if err := t.applyChange(rc); err != nil {
						return err
					}
					// An entity the announcement did not name proves nothing
					// about the order.
					if at, ok := position[rc.GetEntity()]; ok {
						boot.entitiesDone = max(boot.entitiesDone, at)
					}
				}
				boot.rows += uint64(len(chunk.GetRows()))
				boot.token = chunk.GetResumeToken()
				t.metaDirty = true
				prog = boot.progress()
				return nil
			})
			if err != nil {
				return err
			}
			w.progress(prog, false)
			continue
		}
		if msg.GetEnd() != nil {
			_, err := w.txn(ctx, func(t *txn) error {
				if t.meta.boot == nil {
					return errors.New("upstream: bootstrap state lost")
				}
				t.meta.boot.finishing = true
				t.meta.boot.entitiesDone = t.meta.boot.entitiesTotal
				t.metaDirty = true
				prog = t.meta.boot.progress()
				return nil
			})
			if err != nil {
				return err
			}
			w.progress(prog, true)
			return w.finishBootstrap(ctx, reset)
		}
	}
	return bootErr(stream.Err())
}

// beginEstimate is the snapshot's estimated size in rows from Begin, 0 when
// Begin gives no estimate (see BootstrapResponse.Begin.row_estimates).
func beginEstimate(begin *syncplanev1.BootstrapResponse_Begin) uint64 {
	est := begin.GetRowEstimates()
	if len(est) == 0 || len(est) != len(begin.GetEntities()) {
		return 0
	}
	var total uint64
	for _, n := range est {
		total += n
	}
	return total
}

// smallerEstimate is the smaller of two estimates, where 0 is "unknown" and
// loses to any known one.
func smallerEstimate(a, b uint64) uint64 {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	}
	return min(a, b)
}

func bootErr(err error) error {
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("upstream: bootstrap: %w", classify(err))
}

// finishBootstrap retires every confirmed row the snapshot did not carry and
// makes the snapshot's position the cursor. It works in pages, one local
// transaction each, recording its progress, so a large workspace does not
// hold one transaction for its whole scan, and a restart picks it up where
// it stopped. Until the last page commits, the cursor stays where it was and
// the workspace stays off the stream.
func (w *workspace) finishBootstrap(ctx context.Context, reset bool) error {
	for {
		var done bool
		res, err := w.txn(ctx, func(t *txn) error {
			var err error
			done, err = t.finishStep()
			return err
		})
		if err != nil {
			return err
		}
		w.logRefusals(ctx, res)
		if done {
			break
		}
	}
	w.mu.Lock()
	w.needBoot, w.resetBoot, w.ready = false, false, false
	w.mu.Unlock()
	if reset && w.l.cfg.OnReset != nil {
		w.l.cfg.OnReset(w.id)
	}
	return nil
}

// finishStep handles one page of finishBootstrap: local rows of the bound
// entities in name order, then the rows pending entries touch. It reports
// whether the snapshot is done.
func (t *txn) finishStep() (bool, error) {
	boot := t.meta.boot
	if boot == nil {
		return false, errors.New("upstream: bootstrap state lost")
	}
	t.metaDirty = true
	names := t.w.l.names
	if int(boot.entity) < len(names) {
		name := names[boot.entity]
		var rows []storage.Row
		if err := t.rtx.Tx().ScanEntityAfter(name, boot.afterKey, scanPage, func(r storage.Row) error {
			rows = append(rows, r)
			return nil
		}); err != nil {
			return false, err
		}
		for i := range rows {
			if err := t.retire(RowRef{Entity: name, Key: rows[i].Key}, &rows[i]); err != nil {
				return false, err
			}
		}
		if len(rows) < scanPage {
			boot.entity++
			boot.afterKey = ""
		} else {
			boot.afterKey = rows[len(rows)-1].Key
		}
		return false, nil
	}
	n := 0
	for id := max(boot.afterID+1, t.q.lowID); id < t.q.nextID && n < scanPage; id++ {
		boot.afterID = id
		e, err := t.entry(id)
		if err != nil {
			return false, err
		}
		if e == nil {
			continue
		}
		n++
		for _, r := range e.rows() {
			if err := t.retire(r, nil); err != nil {
				return false, err
			}
		}
	}
	if n == scanPage && boot.afterID+1 < t.q.nextID {
		return false, nil
	}
	if err := t.voidVerdictsAbove(boot.acked); err != nil {
		return false, err
	}
	t.meta.cursor = boot.cursor
	t.meta.originSeq = boot.seq
	t.meta.lastAcked = boot.acked
	// The cursor already reads at boot.seq or later, so a lower floor adds
	// nothing. A higher one can come from the history before a reset (a
	// deleted and restored workspace, a restore below it) and would send
	// every read to the primary until the new history passed it.
	t.meta.ackedSeq = min(t.meta.ackedSeq, boot.seq)
	t.meta.schemaRevision = boot.revision
	t.meta.boot = nil
	t.meta.adopt = false
	return true, t.settle()
}

// retire forgets the confirmed state of a row the snapshot did not carry and
// recomputes it. With AddOptions.AdoptLocalRows, a local row the origin does
// not hold and no pending entry touches is queued as a PUT instead. local is
// the live local row, nil when unknown.
func (t *txn) retire(r RowRef, local *storage.Row) error {
	c, ok, err := t.st.confirmed(r.Entity, r.Key)
	if err != nil {
		return err
	}
	if ok && c.gen >= t.meta.gen {
		return nil
	}
	if ok {
		if err := t.st.del(r.Entity, r.Key); err != nil {
			return err
		}
	}
	if t.meta.adopt && local != nil {
		ids, err := t.st.touched(r.Entity, r.Key)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			if err := t.enqueuePut(r, local); err != nil {
				return err
			}
		}
	}
	t.touch(r)
	return nil
}

// enqueuePut queues a local row as a PUT to forward, as if it had been
// written now.
func (t *txn) enqueuePut(r RowRef, local *storage.Row) error {
	model := t.b.models[r.Entity]
	text, err := model.TextDocuments(local.Value)
	if err != nil {
		return err
	}
	if t.q.nextRef == 0 {
		t.q.nextRef = 1
	}
	id, _ := t.w.l.allocate(t.w.id, t.q.nextID, nil)
	e := &entry{id: id, ref: t.q.nextRef, localSeq: local.Version, ops: []entryOp{{
		entity: r.Entity, key: r.Key, kind: syncplanev1.CrudOp_CRUD_OP_PUT,
		value: bytes.Clone(local.Value), text: text,
	}}}
	t.q.nextRef++
	t.q.nextID = id + 1
	if err := t.st.putEntry(e); err != nil {
		return err
	}
	if err := t.st.putTouched(r.Entity, r.Key, []uint64{id}); err != nil {
		return err
	}
	t.cache[id] = e
	t.res.seen = append(t.res.seen, pendRef{e.ref, e.localSeq})
	return nil
}
