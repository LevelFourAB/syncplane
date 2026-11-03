package upstream

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/client"
)

// Errors that steer a workspace's worker.
var (
	errReset     = errors.New("upstream: origin asked for a fresh snapshot")
	errNoAccess  = errors.New("upstream: origin grants no access to the workspace yet")
	errRevokedWS = errors.New("upstream: origin revoked the workspace")
)

// workspace is the runtime of one linked workspace: a worker goroutine that
// bootstraps, catches up and pushes, plus the in-memory view of its queue
// that Status, Flush and Await read.
type workspace struct {
	l        *Link
	id       string
	clientID string
	// startNext is the queue's next id when this runtime loaded it. Entries
	// at or above it were captured by this process.
	startNext uint64

	done   chan struct{}
	cancel context.CancelFunc
	wake   chan struct{}
	// deliverWake wakes the goroutine that reports rejections.
	deliverWake chan struct{}

	// ackKnown is set once the origin's watermark for this link has been
	// read in this process; pushes wait for it.
	ackKnown atomic.Bool

	// pullMu is held by whatever pulls pages into the workspace, so two
	// sources never apply pages out of order.
	pullMu sync.Mutex

	// mu serializes the link's own transactions on the workspace, and guards
	// the fields below.
	mu sync.Mutex
	// meta mirrors the stored syncMeta after the last committed transaction.
	meta syncMeta
	// ready is set while the workspace is caught up and belongs on the
	// stream; the worker pulls before it sets it.
	ready bool
	// needBoot asks the worker for a fresh snapshot; resetBoot marks it as a
	// reset to report.
	needBoot  bool
	resetBoot bool
	// isolate makes the next push carry one mutation, to find which one
	// fails.
	isolate bool
	poison  poisonCount
	// revokeNow reports a stream that dropped the workspace for good.
	revokeNow bool
	// subGen counts subscription changes. A stream or a pull reports the
	// workspace caught up only for the subscriptions it was opened with.
	subGen uint64

	// vmu guards the view below.
	vmu sync.Mutex
	// maxID is the queue's next id as last read: entries below it are in
	// pend or gone, as long as the queue's epoch is still epoch.
	maxID    uint64
	epoch    uint64
	pend     map[uint64]uint64 // ref -> local seq
	bySeq    map[uint64]uint64 // local seq -> ref
	verdicts map[uint64]error  // ref -> nil (applied) or *RejectedError
	// vorder lists the decided writes in the order verdicts learned them,
	// with the local sequence bySeq holds each one under.
	vorder []pendRef
	// unreported are the refs of the rejections stored for reporting, and
	// handed those of them already handed to Config.OnRejected.
	unreported map[uint64]bool
	handed     map[uint64]bool
	st         status
	last       Status
	// bootReported is when bootstrap progress last reached Config.OnStatus.
	bootReported time.Time
	// bootPending is progress held back by the throttle, which bootTimer
	// reports when the interval has passed. Both are nil when none waits.
	bootPending *BootstrapProgress
	bootTimer   *time.Timer
	// bootAttempt counts ended bootstrap attempts, so that a report the
	// timer took before an attempt ended does not land in the next one.
	bootAttempt uint64
	// changed is closed and replaced on every change of the view.
	changed chan struct{}
}

type poisonCount struct {
	key string
	n   int
}

// maxVerdicts bounds the verdicts a workspace remembers for Await.
const maxVerdicts = 4096

func (l *Link) newWorkspace(id string, meta syncMeta, q queueMeta) *workspace {
	w := &workspace{
		l:           l,
		id:          id,
		clientID:    meta.clientID,
		startNext:   q.nextID,
		done:        make(chan struct{}),
		wake:        make(chan struct{}, 1),
		deliverWake: make(chan struct{}, 1),
		unreported:  map[uint64]bool{},
		handed:      map[uint64]bool{},
		meta:        meta,
		pend:        map[uint64]uint64{},
		bySeq:       map[uint64]uint64{},
		verdicts:    map[uint64]error{},
		changed:     make(chan struct{}),
	}
	if w.clientID == "" {
		w.clientID = l.clientID
	}
	w.st.revoked = q.revoked
	w.st.originSeq = meta.originSeq
	w.st.revision = meta.schemaRevision
	w.last = w.statusLocked()
	return w
}

// kick wakes the worker; a signal already pending absorbs it.
func (w *workspace) kick() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// stop ends the worker and waits for it.
func (w *workspace) stop() {
	if w.cancel != nil {
		w.cancel()
	}
	<-w.done
}

func (w *workspace) logger() *slog.Logger {
	return w.l.log.With(slog.String("workspace", w.id))
}

// run is the worker: it brings the workspace up to date and pushes its
// queue, and backs off after a failure. Rejections are reported from a
// goroutine of their own, so a callback can wait for the worker.
func (w *workspace) run(ctx context.Context) {
	dctx, stopDeliver := context.WithCancel(ctx)
	var delivering sync.WaitGroup
	delivering.Add(1)
	go func() {
		defer delivering.Done()
		w.runDeliver(dctx)
	}()
	defer delivering.Wait()
	defer stopDeliver()
	if w.snapshot().State == StateRevoked {
		w.kickDeliver()
		<-ctx.Done()
		return
	}
	bo := newBackoff(w.l.cfg.RetryMin, w.l.cfg.RetryMax)
	_ = w.refresh(ctx)
	w.kickDeliver()
	for {
		if ctx.Err() != nil {
			return
		}
		err := w.step(ctx)
		switch {
		case err == nil:
			bo.reset()
			w.update(func(s *status) { s.offline, s.noAccess, s.lastErr = false, false, nil })
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
			}
			continue
		case ctx.Err() != nil:
			return
		case errors.Is(err, ErrNotLinked):
			w.gone()
			return
		case errors.Is(err, errRevokedWS):
			w.revoke(ctx)
			<-ctx.Done()
			return
		case errors.Is(err, errReset):
			// The worker bootstraps on its next step.
			continue
		case errors.Is(err, errNoAccess):
			w.update(func(s *status) { s.noAccess, s.lastErr = true, err })
		default:
			w.logger().LogAttrs(ctx, slog.LevelWarn, "sync failed; retrying", slog.Any("error", err))
			w.update(func(s *status) { s.offline, s.lastErr = true, err })
		}
		if !w.backoff(ctx, bo.next()) {
			return
		}
	}
}

// gone stops a workspace whose link state was deleted with the workspace:
// it leaves the stream, its worker ends, and its status says why.
func (w *workspace) gone() {
	w.mu.Lock()
	w.ready = false
	w.mu.Unlock()
	w.update(func(s *status) { s.failed, s.live, s.failErr = true, false, ErrNotLinked })
	w.l.kickStreams()
	w.kick()
}

// backoff waits d, keeping the pending view current as local writes wake
// the worker meanwhile. It returns false when ctx ends.
func (w *workspace) backoff(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		case <-w.wake:
			_ = w.refresh(ctx)
		}
	}
}

// step runs one round of the worker.
func (w *workspace) step(ctx context.Context) error {
	w.mu.Lock()
	// A snapshot that was interrupted is finished before anything else: its
	// rows are already partly applied.
	boot := w.needBoot || w.meta.cursor == nil || w.meta.boot != nil
	ready := w.ready
	revoked := w.revokeNow
	w.mu.Unlock()
	if revoked {
		return errRevokedWS
	}
	if w.snapshot().State == StateFailed {
		return ErrNotLinked
	}
	if boot || !ready {
		// Bootstraps and catch-ups share a bound across workspaces, so a
		// start with many workspaces does not open them all at once.
		if err := w.l.acquireSync(ctx); err != nil {
			return err
		}
		err := w.catchUp(ctx, boot)
		w.l.releaseSync()
		if err != nil {
			return err
		}
		w.mu.Lock()
		w.ready = true
		w.mu.Unlock()
		w.l.kickStreams()
	}
	// After the catch-up, which learns the origin's revision, and before
	// anything is forwarded under a pair no one decided on.
	w.checkSkew()
	return w.push(ctx)
}

// catchUp bootstraps the workspace when boot is set, then pulls until it is
// caught up.
func (w *workspace) catchUp(ctx context.Context, boot bool) error {
	if boot {
		if err := w.bootstrap(ctx); err != nil {
			return err
		}
	}
	return w.pullDrain(ctx)
}

// unready takes the workspace off the stream and wakes the worker to pull
// or bootstrap it back.
func (w *workspace) unready(boot, reset bool) {
	w.mu.Lock()
	w.ready = false
	if boot {
		w.needBoot = true
		w.resetBoot = w.resetBoot || reset
	}
	w.mu.Unlock()
	w.update(func(s *status) { s.live = false })
	w.l.kickStreams()
	w.kick()
}

// isReady reports whether the workspace belongs on the stream.
func (w *workspace) isReady() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ready && !w.needBoot
}

// classify maps an error from a call to the origin onto the worker's
// errors.
func classify(err error) error {
	switch client.CodeOf(err) {
	case client.CodeWorkspaceAccessRevoked:
		return errors.Join(errRevokedWS, err)
	case client.CodeWorkspaceNoAccess:
		return errors.Join(errNoAccess, err)
	}
	return err
}

// revoke records that the origin refused the workspace for good.
func (w *workspace) revoke(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)
	w.mu.Lock()
	w.ready = false
	w.mu.Unlock()
	w.l.kickStreams()
	_, err := w.txn(ctx, func(t *txn) error {
		t.q.revoked = true
		return t.st.putQueue(t.q)
	})
	if err != nil {
		w.logger().LogAttrs(ctx, slog.LevelError, "record revoked workspace", slog.Any("error", err))
	}
	if w.l.cfg.DeleteOnRevoke {
		if err := w.l.purge(ctx, w.l.bind.Load(), w.id, purgeRevoked); err != nil {
			w.logger().LogAttrs(ctx, slog.LevelError, "delete rows of revoked workspace", slog.Any("error", err))
		}
	}
	w.update(func(s *status) { s.revoked, s.live = true, false })
	if fn := w.l.cfg.OnAccessRevoked; fn != nil {
		fn(w.id)
	}
}

// observeHead records the origin's head from a page or a snapshot, without a
// report: the transaction that applies the page reports the status. 0 is an
// origin that does not send one, and makes the head unknown rather than leave
// one from a snapshot the page has passed.
func (w *workspace) observeHead(head uint64) {
	w.vmu.Lock()
	w.st.originHead = head
	w.vmu.Unlock()
}

// update changes the status and reports a change to Config.OnStatus.
func (w *workspace) update(fn func(*status)) {
	w.vmu.Lock()
	fn(&w.st)
	cur := w.statusLocked()
	changed := !sameStatus(cur, w.last)
	w.last = cur
	if changed {
		w.broadcastLocked()
	}
	w.vmu.Unlock()
	if changed && w.l.cfg.OnStatus != nil {
		w.l.cfg.OnStatus(cur)
	}
}

// bootProgressInterval is the shortest time between two reports of bootstrap
// progress alone to Config.OnStatus. A fast origin streams many chunks a
// second, and a report per chunk would flood the callback.
var bootProgressInterval = 250 * time.Millisecond

// progress records how far the running bootstrap has got and reports it to
// Config.OnStatus, at once when force is set or bootProgressInterval has passed
// since the last report of progress, and otherwise when that interval has
// passed. Status keeps the last progress reported until then: storing p
// without a report would not hold the reports back, because the next status
// change of any other kind would report it.
func (w *workspace) progress(p BootstrapProgress, force bool) {
	now := time.Now()
	w.vmu.Lock()
	wait := bootProgressInterval - now.Sub(w.bootReported)
	if !force && wait > 0 {
		// The trailing report: without it, a stream that stops sending
		// chunks for a while leaves the last ones unreported, which shows
		// as a stall that is not there.
		w.bootPending = &p
		if w.bootTimer == nil {
			w.bootTimer = time.AfterFunc(wait, w.reportPending)
		}
		w.vmu.Unlock()
		return
	}
	w.bootReported = now
	w.dropPendingLocked()
	w.vmu.Unlock()
	w.update(func(s *status) { s.boot = &p })
}

// reportPending reports the progress the throttle held back, while the
// bootstrap it belongs to still runs.
func (w *workspace) reportPending() {
	w.vmu.Lock()
	p, attempt := w.bootPending, w.bootAttempt
	w.bootPending, w.bootTimer = nil, nil
	if p != nil {
		w.bootReported = time.Now()
	}
	w.vmu.Unlock()
	if p == nil {
		return
	}
	w.update(func(s *status) {
		// update runs fn under vmu, so the attempt cannot end in between.
		if w.bootAttempt == attempt && s.boot != nil {
			s.boot = p
		}
	})
}

// endProgress clears the bootstrap progress when the attempt ends, and drops
// any held back, so a late timer cannot report it.
func (w *workspace) endProgress() {
	w.vmu.Lock()
	w.dropPendingLocked()
	w.bootAttempt++
	w.vmu.Unlock()
	w.update(func(s *status) { s.bootstrapping, s.boot = false, nil })
}

func (w *workspace) dropPendingLocked() {
	if w.bootTimer != nil {
		w.bootTimer.Stop()
	}
	w.bootPending, w.bootTimer = nil, nil
}

func sameStatus(a, b Status) bool {
	return a.State == b.State && a.Pending == b.Pending && a.OriginSeq == b.OriginSeq &&
		a.OriginHeadSeq == b.OriginHeadSeq &&
		a.SchemaRevision == b.SchemaRevision && sameSkew(a.SchemaSkew, b.SchemaSkew) &&
		errors.Is(a.LastError, b.LastError) && errors.Is(b.LastError, a.LastError) &&
		sameBoot(a.Bootstrap, b.Bootstrap)
}

func sameBoot(a, b *BootstrapProgress) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func sameSkew(a, b *SchemaSkew) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func (w *workspace) statusLocked() Status {
	lastErr := w.st.lastErr
	if w.st.failed {
		lastErr = w.st.failErr
	}
	return Status{
		Workspace:      w.id,
		State:          w.st.state(),
		Pending:        len(w.pend),
		OriginSeq:      w.st.originSeq,
		OriginHeadSeq:  w.st.originHead,
		SchemaRevision: w.st.revision,
		SchemaSkew:     w.st.reportedSkew(),
		LastError:      lastErr,
		Bootstrap:      w.st.reportedBoot(),
	}
}

// reportedBoot is a copy of the bootstrap progress, so that a Status handed
// out never changes.
func (s *status) reportedBoot() *BootstrapProgress {
	if s.boot == nil {
		return nil
	}
	b := *s.boot
	return &b
}

// reportedSkew is the skew Status reports: a copy of the decided one, nil
// when there is none or the answer was SkewIgnore.
func (s *status) reportedSkew() *SchemaSkew {
	if s.skew == nil || s.skew.Action == SkewIgnore {
		return nil
	}
	sk := *s.skew
	return &sk
}

// checkSkew compares the local engine's schema revision with the origin's
// and asks Config.OnSchemaSkew about a pair it has not decided yet. It runs
// on the worker, which holds no lock a callback could wait for.
func (w *workspace) checkSkew() {
	local := w.l.bind.Load().srv.SchemaRevision()
	w.vmu.Lock()
	origin, prev := w.st.revision, w.st.skew
	w.vmu.Unlock()
	if local == "" || origin == "" || local == origin {
		if prev != nil {
			w.update(func(s *status) { s.skew = nil })
		}
		return
	}
	if prev != nil && prev.Local == local && prev.Origin == origin {
		return
	}
	action := SkewWarn
	if fn := w.l.cfg.OnSchemaSkew; fn != nil {
		action = fn(w.id, local, origin)
	}
	w.update(func(s *status) { s.skew = &SchemaSkew{Local: local, Origin: origin, Action: action} })
	if action == SkewBlock {
		w.logger().LogAttrs(context.Background(), slog.LevelWarn, "schema revision differs from the origin's; holding local writes",
			slog.String("local", local), slog.String("origin", origin))
	}
}

// blocked reports whether SkewBlock holds the workspace's local writes.
func (w *workspace) blocked() bool {
	w.vmu.Lock()
	defer w.vmu.Unlock()
	return w.st.skew != nil && w.st.skew.Action == SkewBlock
}

func (w *workspace) snapshot() Status {
	w.vmu.Lock()
	defer w.vmu.Unlock()
	return w.statusLocked()
}

func (w *workspace) broadcastLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}

// commit applies a committed transaction's result to the in-memory view.
func (w *workspace) commit(res *txResult) {
	w.l.pruneThrough(w.id, res.maxID)
	if res.meta != nil {
		w.meta = *res.meta
	}
	if res.ackKnown {
		w.ackKnown.Store(true)
	}
	w.vmu.Lock()
	if res.reload != nil {
		// Refs survive a renumbering, so bySeq stays valid for decided writes
		// too; only the pending set is replaced.
		w.pend = map[uint64]uint64{}
		for _, e := range res.reload {
			w.pend[e.ref] = e.localSeq
			if e.localSeq != 0 {
				w.bySeq[e.localSeq] = e.ref
			}
		}
	}
	for _, e := range res.seen {
		w.pend[e.ref] = e.localSeq
		if e.localSeq != 0 {
			w.bySeq[e.localSeq] = e.ref
		}
	}
	if res.maxID > w.maxID || res.reload != nil {
		w.maxID = res.maxID
	}
	w.epoch = res.epoch
	for _, v := range res.verdicts {
		if _, ok := w.verdicts[v.ref]; !ok {
			w.vorder = append(w.vorder, pendRef{ref: v.ref, localSeq: v.localSeq})
		}
		w.verdicts[v.ref] = v.err
		if v.localSeq != 0 {
			w.bySeq[v.localSeq] = v.ref
		}
	}
	for _, ref := range res.dropped {
		delete(w.pend, ref)
	}
	for len(w.vorder) > maxVerdicts {
		old := w.vorder[0]
		w.vorder = w.vorder[1:]
		delete(w.verdicts, old.ref)
		if w.bySeq[old.localSeq] == old.ref {
			delete(w.bySeq, old.localSeq)
		}
	}
	if res.unreported != nil {
		w.unreported = make(map[uint64]bool, len(res.unreported))
		for _, ref := range res.unreported {
			w.unreported[ref] = true
		}
		for ref := range w.handed {
			if !w.unreported[ref] {
				delete(w.handed, ref)
			}
		}
	}
	if res.meta != nil {
		w.st.originSeq = res.meta.originSeq
		w.st.revision = res.meta.schemaRevision
	}
	// Waiters read more than the status, so wake them whether or not it
	// changed. The caller reports the status.
	w.broadcastLocked()
	w.vmu.Unlock()
}

// kickDeliver wakes the goroutine that reports rejections.
func (w *workspace) kickDeliver() {
	select {
	case w.deliverWake <- struct{}{}:
	default:
	}
}

// runDeliver reports stored rejections to Config.OnRejected, one at a time
// in the order they were stored, and forgets each once its callback
// returned. A rejection whose callback ran but was not forgotten (the
// process stopped in between) is reported again after the next Start.
func (w *workspace) runDeliver(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.deliverWake:
		}
		for ctx.Err() == nil {
			r, ok, err := w.nextRejection(ctx)
			if err != nil || !ok {
				break
			}
			w.l.mx.rejected(ctx, r.code)
			if fn := w.l.cfg.OnRejected; fn != nil {
				fn(r.public(w.id))
			}
			if err := w.forgetRejection(ctx, r.ref); err != nil {
				break
			}
		}
	}
}

// nextRejection returns the oldest stored rejection not handed to the
// callback yet, and marks it handed.
func (w *workspace) nextRejection(ctx context.Context) (rejection, bool, error) {
	var rejs []rejection
	_, err := w.txn(ctx, func(t *txn) error {
		var err error
		rejs, err = t.st.rejections()
		t.res.unreported = refsOf(rejs)
		return err
	})
	if err != nil {
		return rejection{}, false, err
	}
	w.vmu.Lock()
	defer w.vmu.Unlock()
	for _, r := range rejs {
		if !w.handed[r.ref] {
			w.handed[r.ref] = true
			w.broadcastLocked()
			return r, true, nil
		}
	}
	return rejection{}, false, nil
}

// forgetRejection drops a reported rejection from the stored list.
func (w *workspace) forgetRejection(ctx context.Context, ref uint64) error {
	// Not cut short by Close: a callback that ran is forgotten before Close
	// returns, so only a crash reports it again.
	_, err := w.txn(context.WithoutCancel(ctx), func(t *txn) error {
		cur, err := t.st.rejections()
		if err != nil {
			return err
		}
		cur = slices.DeleteFunc(cur, func(r rejection) bool { return r.ref == ref })
		t.res.unreported = refsOf(cur)
		return t.st.putRejections(cur)
	})
	return err
}

func refsOf(rejs []rejection) []uint64 {
	out := make([]uint64, 0, len(rejs))
	for _, r := range rejs {
		out = append(out, r.ref)
	}
	return out
}

func (r rejection) public(ws string) Rejection {
	return Rejection{
		Workspace:  ws,
		LocalSeq:   r.localSeq,
		MutationID: r.id,
		Code:       r.code,
		Message:    r.message,
		Mutator:    r.mutator,
		Rows:       append([]RowRef(nil), r.rows...),
	}
}

// waitView blocks until cond, evaluated under vmu, returns done.
func (w *workspace) waitView(ctx context.Context, cond func() (bool, error)) error {
	for {
		w.vmu.Lock()
		done, err := cond()
		ch := w.changed
		w.vmu.Unlock()
		if done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-w.done:
			w.vmu.Lock()
			done, err := cond()
			w.vmu.Unlock()
			if done || err != nil {
				return err
			}
			return ErrClosed
		}
	}
}

// allReported reports whether every stored rejection was handed to the
// callback. The caller holds vmu.
func (w *workspace) allReported() bool {
	for ref := range w.unreported {
		if !w.handed[ref] {
			return false
		}
	}
	return true
}

// isConnectErr reports whether err came from a call the origin answered or
// that failed in transit.
func isConnectErr(err error) bool {
	var ce *connect.Error
	return errors.As(err, &ce)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// gen returns the workspace's subscription generation.
func (w *workspace) gen() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.subGen
}

// caughtUp marks the workspace live, if its subscriptions are still those of
// generation gen.
func (w *workspace) caughtUp(gen uint64) {
	if w.gen() != gen {
		return
	}
	w.update(func(s *status) { s.live, s.offline, s.noAccess, s.lastErr = true, false, false, nil })
}
