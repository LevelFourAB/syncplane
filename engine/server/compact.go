package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Collaborative-text compaction: the background sweep that lets a long-lived
// document stop growing.
//
// A text field accumulates one item per character ever typed, because a delete
// only sets a tombstone (engine/text). Compaction drops the tombstones nothing
// can still refer to and records the document's version vector as that field's
// COMPACTION FLOOR, which is what stops a replica holding older state from
// merging them back in (see textfloor.go and the text package's compact.go).
//
// Two rules keep the disruption bounded:
//
//   - Only QUIET rows are compacted: those whose last write is at or below the
//     sequence the text delta log has been pruned through. That window is
//     WithTextDeltaRetention (an hour by default), and it is exactly the point
//     where the server has already stopped being able to send such a client a
//     cheap delta. A client that reads the row after this gets the compacted
//     document and carries on; one still holding pre-compaction state keeps
//     working too, until it tries to write text anchored to something purged —
//     then it is refused with CodeTextStateStale and re-applies its edit against
//     fresh state.
//   - The row is only rewritten when compaction frees at least minPurged items,
//     so a pass over documents with nothing to reclaim writes nothing and costs
//     one decode per quiet text row.
//
// The rewrite lands at a fresh sequence, so every client re-fetches the field
// once — the price of the reclaim. It deliberately does NOT notify: the visible
// text is unchanged, so there is nothing to poke anyone about, and the new row
// rides along on the next pull the client makes anyway.
//
// The pass runs on a WINDOW, not on the entity. The delta prune moves a
// workspace's horizon forward; every row that became quiet since the last pass
// is one written in the sequence range between the two, and ScanChanges yields
// exactly that range in exactly that order. So the sweep reads what has been
// edited rather than what exists, and a row it has already considered is
// revisited only if something wrote it again — which is the only thing that can
// give a document new tombstones to reclaim. The mark that records how far it
// got is storage.TextCompactMark.

// compactLoop owns the background text-compaction goroutine. It reuses the
// shape of the index-build loop — ticker, per-workspace lease, paged work query
// — because it has the same requirements, and it is a separate pass from GC for
// a reason the two do not share: tombstone collection and key expiry are cheap
// deletes over indexed ranges, while this is a scan, a decode and a rewrite. In
// one transaction the cheap half would wait behind the expensive one, and on an
// adapter holding a per-workspace lock so would every writer.
type compactLoop struct {
	// minPurged is the minimum number of items a text field must give up before
	// the sweep rewrites its row; <= 0 disables compaction. See
	// WithTextCompaction.
	minPurged int
	interval  time.Duration
	// batch bounds the rows one write transaction considers, for the reason the
	// index build's does: the transaction holds the workspace's write lock, so
	// an unbounded one would stall every writer for the length of a full entity
	// scan.
	batch     int
	cancel    context.CancelFunc
	done      sync.WaitGroup
	closeOnce sync.Once
}

const (
	defaultTextCompactionInterval = 5 * time.Minute
	defaultTextCompactionBatch    = 200
	// compactBatchBytes caps the row values one batch holds at once, since the
	// row count cannot: a text row is as large as the document in it, and large
	// documents are the workload the option exists for. A batch ends at
	// whichever of the two bounds it reaches first.
	compactBatchBytes = 8 << 20
)

// errCompactBatchFull ends a batch's scan from inside its callback, as
// index.go's candidate budget does. It never leaves compactBatch.
var errCompactBatchFull = errors.New("compaction batch full")

// WithTextCompaction enables collaborative-text compaction. minPurged is how
// many tombstoned characters a document must give up before its row is
// rewritten; below that the reclaim does not pay for a pull of the whole field
// by every client. A value <= 0 leaves compaction off, which is the default.
//
// It does nothing without WithGC: the sweep only touches rows the delta prune
// has already quietened, and that prune is a GC collector. A server with
// compaction on and GC off has no horizon to work below and rewrites nothing.
//
// Compaction is not free of consequence, which is why it is opt-in: it is the
// one thing in the engine that can refuse a client's text write outright (see
// CodeTextStateStale), and the recovery costs that client the marks it had
// minted on top of state the server dropped. Enable it for workspaces whose
// documents are edited over months; leave it off if documents are short-lived.
//
// Lowering minPurged re-sweeps every workspace from the beginning, since rows
// an earlier pass skipped may clear the lower bar. Raising it does not, because
// a row skipped under the old threshold is skipped under the higher one too.
func WithTextCompaction(minPurged int) Option {
	return func(s *Server) error {
		s.compact.minPurged = minPurged
		return nil
	}
}

// WithTextCompactionPacing overrides how often the compaction sweep looks for
// workspaces with newly quiet rows, and how many rows one of its write
// transactions considers. It only takes effect together with
// WithTextCompaction.
//
// Neither number changes what compaction RECLAIMS — a row that misses one pass
// is picked up by the next, since the window it belongs to stays unswept — so
// this is about how long a compaction transaction holds a workspace's write
// lock, and how promptly the reclaim happens. batch is a ceiling rather than a
// target: a transaction also ends once the rows it has taken exceed a fixed
// byte budget, so raising it past what the workspace's documents fill changes
// nothing. A non-positive interval disables
// the background pass, leaving compaction to manual CompactText calls; so does
// running GC on manual (WithGC with a non-positive interval), since only a GC
// pass moves the horizon this sweep works below.
//
// Defaults: five minutes, 200 rows per transaction.
func WithTextCompactionPacing(interval time.Duration, batch int) Option {
	return func(s *Server) error {
		s.compact.interval = interval
		s.compact.batch = batch
		return nil
	}
}

func (l *compactLoop) start(s *Server) {
	// Gated on a background GC pass, not merely on GC being configured: the
	// horizon the sweep works below is advanced by PruneFieldDeltas, so with GC
	// on manual there is never a new window to sweep and a ticker here would
	// poll for work that only a CollectGarbage call can create.
	if l.minPurged <= 0 || l.interval <= 0 || s.gc == nil || !s.gc.configured || s.gc.interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	logger := s.log().With("component", "compact")
	l.done.Add(1)
	go func() {
		defer l.done.Done()
		ticker := time.NewTicker(l.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Maintenance must never take the server down: log and retry on
				// the next tick. A cancelled pass (shutdown) is not an error
				// worth logging.
				if err := s.compactAllText(ctx); err != nil && ctx.Err() == nil {
					logger.LogAttrs(ctx, slog.LevelError, "text compaction pass failed", slog.Any("err", err))
				}
			}
		}
	}()
}

func (l *compactLoop) close() {
	l.closeOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
	})
	l.done.Wait()
}

func (l *compactLoop) batchSize() int {
	if l.batch <= 0 {
		return defaultTextCompactionBatch
	}
	return l.batch
}

// compactAllText runs one pass over every workspace whose sweep has fallen
// behind its delta horizon, a page at a time. Like the GC pass, a per-workspace
// failure is collected and the pass moves on, so one bad workspace cannot
// starve those after it; only cancellation aborts it.
//
// A workspace whose documents have gone quiet stops advancing its horizon and
// is not returned, so a deployment past its editing burst pays one round trip
// per interval and nothing else.
func (s *Server) compactAllText(ctx context.Context) (err error) {
	start := time.Now()
	// Recorded from a defer so every exit records exactly once, as the GC
	// pass's is: the pass that could not list workspaces and the one cancelled
	// partway through both happened and both cost time.
	defer func() {
		s.tel().compactDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			telemetry.AttrOutcome.String(telemetry.Classify(err))))
	}()
	minPurged := s.compact.minPurged
	var errs []error
	for after := ""; ; {
		ids, lerr := s.store.ListCompactableWorkspaceIDs(ctx, minPurged, storage.Range{After: after}, maintenancePage)
		if lerr != nil {
			return errors.Join(append(errs, lerr)...)
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			release, run := s.tryLease(ctx, leaseRoleCompact, id)
			if !run {
				// Another node holds the lease and is sweeping this workspace;
				// counted so an operator can see the fleet dividing the pass.
				s.tel().compactWorkspaces.Add(ctx, 1, metric.WithAttributes(
					telemetry.AttrOutcome.String(telemetry.OutcomeSkipped)))
				continue
			}
			_, cerr := s.CompactText(ctx, id)
			release()
			s.tel().compactWorkspaces.Add(ctx, 1, metric.WithAttributes(
				telemetry.AttrOutcome.String(telemetry.Classify(cerr))))
			if cerr != nil {
				errs = append(errs, fmt.Errorf("workspace %s: %w", id, cerr))
			}
		}
		// Paged by workspace id, not by the predicate: a workspace this page
		// swept no longer matches it, and paging by the answer would step over
		// the one that took its place.
		if len(ids) < maintenancePage {
			break
		}
		after = ids[len(ids)-1]
	}
	return errors.Join(errs...)
}

// CompactText runs one workspace's text-compaction sweep to completion,
// returning how many rows it rewrote. It sweeps from wherever the last pass
// stopped up to the workspace's current field-delta horizon, in write
// transactions of at most the configured batch (see WithTextCompactionPacing),
// and reports 0 without touching the store when compaction is off.
//
// It is safe to call at any time: each batch commits its rewrites and its mark
// together, so an interrupted sweep resumes where it stopped rather than
// redoing the workspace, and a rewrite that happens twice costs a sequence and
// nothing else. Callers do not normally need it — the background pass covers
// every workspace — but a test or a maintenance script that wants the reclaim
// now can force it.
//
// Do not call it from inside a mutator: it opens write transactions, which
// would deadlock against the one the mutator already holds.
func (s *Server) CompactText(ctx context.Context, workspaceID string) (int, error) {
	if s.compact.minPurged <= 0 {
		return 0, nil
	}
	if inWriteTx(ctx) {
		return 0, fmt.Errorf("server: CompactText called re-entrantly inside a write transaction (workspace %q): it opens write transactions that would deadlock against the running one", workspaceID)
	}
	total := 0
	for more := true; more; {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		var rewritten int
		if err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
			var err error
			// Reset per attempt: the closure may run more than once (see the
			// storage.Adapter write-closure contract).
			rewritten, more = 0, false
			rewritten, more, err = s.compactBatch(tx)
			return err
		}); err != nil {
			return total, err
		}
		// After the commit, never inside the closure: a count carried over from
		// a rolled-back attempt would report a reclaim that did not happen.
		total += rewritten
		if rewritten > 0 {
			s.tel().compactRows.Add(ctx, int64(rewritten))
		}
	}
	if total > 0 {
		s.log().LogAttrs(ctx, slog.LevelDebug, "compacted collaborative text",
			slog.String("component", "compact"),
			slog.String("workspace", workspaceID),
			slog.Int("rows_compacted", total),
		)
	}
	return total, nil
}

// compactBatch sweeps one window of at most batch rows and advances the
// workspace's mark to where it stopped, reporting whether a further batch may
// remain. Both happen in the caller's transaction, so the rewrites and the
// mark that claims them commit together.
func (s *Server) compactBatch(tx storage.WriteTx) (int, bool, error) {
	minPurged := s.compact.minPurged
	mark, err := tx.TextCompactMark()
	if err != nil {
		return 0, false, err
	}
	horizon, err := tx.FieldDeltaHorizon()
	if err != nil {
		return 0, false, err
	}
	if horizon == 0 {
		// Nothing has been pruned here, so no row is quiet yet. Distinct from
		// "caught up": the mark stays where it is rather than claiming a window
		// that does not exist.
		return 0, false, nil
	}
	if mark.MinPurged > minPurged {
		// The threshold was lowered, so rows an earlier sweep skipped may clear
		// the new one. Only downwards: a row skipped under a lower threshold is
		// skipped under a higher one too, which is what keeps a rolling deploy
		// that RAISES it from re-sweeping every workspace on every node.
		mark = storage.TextCompactMark{}
	}
	// A non-empty entity is a sub-position inside the group AT the horizon,
	// which still has rows to consider; only a clean boundary means caught up.
	if mark.Seq >= horizon && mark.Entity == "" {
		if mark.MinPurged == minPurged {
			return 0, false, nil
		}
		// Nothing to sweep, but the recorded threshold is stale — leaving it
		// would keep the work query returning this workspace forever.
		mark.MinPurged = minPurged
		return 0, false, tx.SetTextCompactMark(mark)
	}

	batch := s.compact.batchSize()
	entities := s.catalogPtr().entities
	var (
		// Collect first: the scan and the rewrites share one transaction, and an
		// adapter is not required to tolerate writes during iteration. Bounded
		// by batch and by compactBatchBytes, which is what keeps this from being
		// the whole entity.
		candidates []storage.Row
		last       storage.TextCompactMark
		seen       int
		held       int
		past       bool
		full       bool
	)
	if err := tx.ScanChanges(mark.Seq, mark.Entity, mark.Key, batch, func(r storage.Row) error {
		seen++
		if r.Version > horizon {
			// Written since the prune, so still inside the window where clients
			// catch up cheaply. Skipped rather than breaking off the scan: a
			// scan stops at its limit, and the engine has no way to bound one by
			// version. Seeing one at all means the window is exhausted, so at
			// most one batch per pass reads rows it discards.
			past = true
			return nil
		}
		last = storage.TextCompactMark{Seq: r.Version, Entity: r.Entity, Key: r.Key}
		if r.Deleted {
			return nil
		}
		// The mark advances over rows of every entity, not just text-bearing
		// ones: it is a position in ScanChanges order, and one that skipped the
		// rows it does not care about would resume before them every batch.
		if info, ok := entities[r.Entity]; ok && len(info.textFields) > 0 {
			candidates = append(candidates, r)
			held += len(r.Value)
			if held >= compactBatchBytes {
				// The row just taken is inside the batch, and last already
				// names it, so stopping here leaves nothing unaccounted for.
				full = true
				return errCompactBatchFull
			}
		}
		return nil
	}); err != nil && !errors.Is(err, errCompactBatchFull) {
		return 0, false, err
	}

	next := last
	more := (seen == batch || full) && !past
	if !more {
		// Everything below the horizon has been considered. Recording the
		// horizon itself rather than the last row's position is what lets the
		// work query answer "caught up" from two columns.
		next = storage.TextCompactMark{Seq: horizon}
	}
	next.MinPurged = minPurged

	var seq uint64
	rewritten := 0
	for _, row := range candidates {
		info := entities[row.Entity]
		done, err := s.compactRow(tx, info, row, minPurged, &seq)
		if err != nil {
			return 0, false, err
		}
		if done {
			rewritten++
		}
	}
	if err := tx.SetTextCompactMark(next); err != nil {
		return 0, false, err
	}
	return rewritten, more, nil
}

// compactRow compacts every text field of one row, rewriting it when the
// reclaim clears the threshold. seq is allocated lazily and shared by every row
// the batch rewrites: they are one maintenance event, and one sequence means one
// page of changes rather than one per row.
func (s *Server) compactRow(
	tx storage.WriteTx,
	info *entityInfo,
	row storage.Row,
	minPurged int,
	seq *uint64,
) (bool, error) {
	msg, err := info.decode(row.Value)
	if err != nil {
		// A row that does not decode is not compaction's problem to report; the
		// write paths already reject anything that could have produced it.
		return false, nil
	}
	floors := make(map[string][]byte, len(info.textFields))
	for _, tf := range info.textFields {
		for _, ti := range tf.instances(msg.ProtoReflect()) {
			b := ti.bytes()
			if len(b) == 0 {
				continue
			}
			// Most rows the sweep sees have accumulated nothing worth a rewrite,
			// and asking the stored bytes is orders of magnitude cheaper than
			// decoding the document to find that out.
			if could, err := text.CouldReclaim(b, minPurged); err != nil || !could {
				continue
			}
			d, err := text.Decode(b, "")
			if err != nil {
				continue
			}
			// Items and mark ops count the same towards the threshold: it asks
			// whether the reclaim earned a row rewrite and a sequence number, and
			// either kind shrinks the row.
			if items, marks := d.Compact(); items+marks < minPurged {
				continue
			}
			ti.set(d.Encode())
			floors[ti.path] = text.EncodeVV(d.VersionVector())
		}
	}
	if len(floors) == 0 {
		return false, nil
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		return false, err
	}
	if *seq == 0 {
		if *seq, err = tx.NextSeq(); err != nil {
			return false, err
		}
	}
	for path, vv := range floors {
		if err := tx.PutTextFloor(info.name, row.Key, path, vv); err != nil {
			return false, err
		}
		// A purge removes items, which no fragment can express: a client folding
		// the delta log across this sequence would keep text the document no
		// longer has. Gap the compacted fields so any span covering this write is
		// served as full state. Fields left alone record nothing and still ship
		// as "unchanged".
		if err := info.putTextDelta(tx, row.Key, path, *seq, nil); err != nil {
			return false, err
		}
	}
	// Every site that writes a row refreshes its index entries, without
	// exception. Compaction rewrites only text fields, which can never BE
	// index components, so the entries it recomputes are byte-identical today
	// — the call is here so that the invariant is "a row write refreshes the
	// indexes" rather than "a row write refreshes the indexes unless someone
	// reasoned it need not", which is the version that rots when what
	// compaction rewrites changes.
	if err := info.updateRowIndexes(tx, s.limits, row.Key, msg); err != nil {
		// A rejection means the row breaks an index limit it was written
		// before. Its components are as stored, so the entries it already has
		// are still the right ones, and the compaction goes on without them
		// being rewritten.
		if _, rejected := AsRejection(err); !rejected {
			return false, err
		}
	}
	// Carry the row's recorded actor through unchanged. Compaction purges
	// tombstones from a document; it makes no semantic change to the row, so
	// stamping the maintenance pass here would erase who last wrote the row and
	// tell every feed consumer that the engine did.
	//
	// For the same reason this is the one row write that records no field
	// history (see history.go): the document reads the same before and after, so
	// a record would say a field changed when nothing did.
	return true, tx.PutRow(info.name, row.Key, value, *seq, row.Actor)
}
