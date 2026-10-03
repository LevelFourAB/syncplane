package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Building an index over rows that already exist.
//
// Maintenance (index.go) keeps an index correct from the moment it is
// declared, but only for rows written AFTER that. Everything already in the
// store has no entries, so a fresh index — or one whose declaration changed —
// covers part of the entity and is useless until something walks the rest.
//
// That walk is what this file does, and the readiness flag is what makes it
// safe: an index is usable only once its build has finished under the CURRENT
// spec fingerprint, and until then every read takes its scan fallback. Nothing
// blocks on the build, nothing is wrong while it runs, and a build that fails
// halfway simply leaves the index unready and is retried on the next pass.
//
// A build is idempotent, so re-running one costs time and nothing else. That
// is what lets it be a background sweep whose only coordination is advisory:
// each workspace's build runs under a per-workspace lease (see WithLeases), so
// nodes sharing a lease-providing store divide the sweep — but the pass may
// still run on several nodes, be interrupted, or overlap itself, and none of
// that is wrong.

// indexBuildLoop periodically brings every workspace's indexes up to date. It
// reuses the shape of the GC loop — a ticker goroutine, cancelled by Close,
// paging a work query — because it has the same requirements: never take the
// server down, never starve a workspace behind a failing one, and cost nothing
// per interval on a deployment with nothing to do.
type indexBuildLoop struct {
	interval time.Duration
	// batch caps the rows one write transaction indexes. A build holds the
	// workspace's write lock for the length of a transaction, so an unbounded
	// one would stall every writer for the length of a full entity scan. It is
	// a cap and not the batch size: txBudget ends a transaction earlier
	// whenever the rows turn out to be expensive.
	batch int
	// txBudget bounds the wall time one build transaction spends indexing rows.
	// Zero means defaultIndexBuildTxBudget; only tests set it.
	txBudget  time.Duration
	cancel    context.CancelFunc
	done      sync.WaitGroup
	closeOnce sync.Once
}

const (
	defaultIndexBuildInterval = time.Minute
	defaultIndexBuildBatch    = 500
	// defaultIndexBuildTxBudget bounds how long one build transaction indexes
	// rows before committing. Every writer of the workspace is blocked for the
	// length of that transaction, and the row cap alone cannot bound it: a
	// row's cost varies with how many index entries it occupies and how loaded
	// the store is, so the same 500 rows are milliseconds on one schema and a
	// fifth of a second on another. 20ms is short enough to sit inside a
	// write's own latency budget and long enough that the commit is a minority
	// of the transaction.
	defaultIndexBuildTxBudget = 20 * time.Millisecond
	// indexBuildYield is how long the walk stands down between transactions.
	indexBuildYield = 2 * time.Millisecond
)

// errIndexBuildBudget ends a scan whose transaction has spent its time budget.
// It never reaches a caller: the walk turns it back into a short batch that
// commits normally and resumes at the last key.
var errIndexBuildBudget = errors.New("index build: batch time budget spent")

// WithIndexBuild overrides how often the background pass looks for indexes to
// backfill, and the most rows one of its write transactions covers. A
// transaction may cover fewer: it also stops on an internal time budget, so
// that the write lock it holds is bounded in time rather than in rows.
//
// Neither number changes what a read ANSWERS — an unbuilt index is simply not
// used — so this is purely about how fast a newly declared index starts paying
// off, against how long a build transaction holds a workspace's write lock. A
// non-positive interval disables the pass entirely, which leaves every index
// unready and every read on its scan fallback; that is a legitimate way to
// turn indexing off without changing a schema, and a trap otherwise.
//
// Defaults: one minute, at most 500 rows per transaction.
func WithIndexBuild(interval time.Duration, batch int) Option {
	return func(s *Server) error {
		s.indexBuild.interval = interval
		s.indexBuild.batch = batch
		return nil
	}
}

func (l *indexBuildLoop) start(s *Server) {
	if l.interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	logger := s.log().With("component", "indexbuild")
	l.done.Add(1)
	go func() {
		defer l.done.Done()
		// First pass on the first tick, not at startup. Nothing waits on a
		// build — an unready index just means reads scan — so an eager pass
		// would buy a minute of speed at the cost of write transactions
		// against every workspace during boot, when the store is busiest.
		// A caller that needs an index NOW calls BuildIndexes.
		ticker := time.NewTicker(l.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.buildAllIndexes(ctx); err != nil && ctx.Err() == nil {
					logger.LogAttrs(ctx, slog.LevelError, "index build pass failed", slog.Any("err", err))
				}
			}
		}
	}()
}

func (l *indexBuildLoop) close() {
	l.closeOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
	})
	l.done.Wait()
}

// buildAllIndexes runs one pass over every workspace whose recorded build mark
// does not match the current catalog's, a page at a time. Like the GC pass, a
// per-workspace failure is collected and the pass moves on; only cancellation
// aborts it.
//
// In the steady state the work query matches nothing and the pass is one round
// trip. A workspace it does return is leased before anything reads its index
// state, so a node that loses the lease pays neither the state reads nor the
// build.
func (s *Server) buildAllIndexes(ctx context.Context) error {
	mark := s.catalogPtr().indexBuildMark()
	var errs []error
	for after := ""; ; {
		ids, err := s.store.ListStaleIndexWorkspaceIDs(ctx, mark, storage.Range{After: after}, maintenancePage)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			release, run := s.tryLease(ctx, leaseRoleIndexes, id)
			if !run {
				continue
			}
			err := s.BuildIndexes(ctx, id)
			release()
			if err != nil {
				errs = append(errs, fmt.Errorf("workspace %s: %w", id, err))
			}
		}
		// Paged by workspace id, not by the predicate: a workspace this page
		// built no longer matches, and paging by the answer would step over the
		// one that took its place.
		if len(ids) < maintenancePage {
			return errors.Join(errs...)
		}
		after = ids[len(ids)-1]
	}
}

// BuildIndexes brings every index of every registered entity up to date for
// one workspace, and marks each ready. Indexes already ready under their
// current spec are skipped, so a repeat pass over an unchanged workspace costs
// one state read per index.
//
// A pass that leaves every index ready records the catalog's build mark on the
// workspace, which is what lets the next background pass skip it without
// reading any of that state (see
// storage.Adapter.ListStaleIndexWorkspaceIDs). Nothing is recorded when an
// index ends the pass unready — a concurrent re-registration is the way that
// happens — so the workspace stays selected until a pass finishes under one
// catalog.
//
// An entity whose indexes cannot be built stays unready, and queries on its
// indexes scan. A stored row past an index limit is the lasting cause (see
// storage.MaxIndexEntryBytes). The other entities still build, and the
// returned error joins one error for each entity that did not.
//
// It is safe to call at any time: the build is idempotent, and readiness is
// only claimed once the whole entity has been walked. Callers do not normally
// need it — the background loop covers every workspace — but a test or a
// migration that wants an index usable immediately can force the pass.
//
// Do not call it from inside a mutator: it opens write transactions, which
// would deadlock against the one the mutator already holds.
func (s *Server) BuildIndexes(ctx context.Context, workspaceID string) error {
	if inWriteTx(ctx) {
		return fmt.Errorf("server: BuildIndexes called re-entrantly inside a write transaction (workspace %q): it opens write transactions that would deadlock against the running one", workspaceID)
	}
	cat := s.catalogPtr()
	allReady := true
	var errs []error
	for _, info := range cat.entities {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ready, err := s.buildEntityIndexes(ctx, workspaceID, info)
		if err != nil {
			// The others still build: an entity that cannot (a stored row
			// past an index limit fails every pass) must not hold back the
			// indexes of entities that can.
			errs = append(errs, fmt.Errorf("entity %q: %w", info.name, err))
			allReady = false
			continue
		}
		allReady = allReady && ready
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if !allReady {
		return nil
	}
	// The mark of the catalog this pass ran under, not of whatever
	// re-registration may have swapped in since: a node holding the newer
	// catalog sees a mark that does not match it and redoes the workspace.
	return s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		return tx.SetIndexBuildMark(cat.indexBuildMark())
	})
}

// buildTarget is one index of an entity as a build pass sees it: the name its
// state row is keyed by, and the fingerprint the running catalog declares it
// under.
type buildTarget struct {
	name     string
	specHash string
}

// buildEntityIndexes walks one entity ONCE and marks every index of it ready.
// It reports whether all of them end the call ready under the running
// catalog's specs — false when a concurrent re-registration moved a fingerprint
// out from under the walk, the one case where a successful call leaves an index
// unusable.
//
// One walk covers every index because a row's entries are written as a set:
// storage replaces all of them together (see entityInfo.updateRowIndexes), so
// visiting a row builds its entry in every declared index whether that index
// needed building or not. A walk per index would rewrite the same set once per
// index and read the entity once per index for no extra coverage.
//
// Two things make the batching sound despite concurrent writers:
//
//   - Maintenance is already live. Any row written while the build runs gets
//     its entries from the write path, so the build never has to catch up to
//     a moving target; it only has to visit rows that existed before it
//     started, and revisiting one is harmless.
//   - Readiness is claimed last, in its own transaction per index. A crash
//     mid-build leaves entries in place and the indexes unready, so reads keep
//     scanning and the next pass redoes the walk over rows that mostly already
//     have their entries.
func (s *Server) buildEntityIndexes(ctx context.Context, ws string, info *entityInfo) (allReady bool, err error) {
	targets := make([]buildTarget, 0, len(info.indexes)+1)
	for i := range info.indexes {
		targets = append(targets, buildTarget{info.indexes[i].name, info.indexes[i].specHash})
	}
	if info.search != nil {
		targets = append(targets, buildTarget{info.search.name, info.search.specHash})
	}
	if len(targets) == 0 {
		return true, nil
	}

	// Until an index is ready, every read that would have used it scans
	// instead — so how long a build takes and how far it has got are the
	// explanation for a query latency regression that no query-side metric can
	// give. Both entity and index names are derived at registration, so they
	// are bounded by the schema.
	start := time.Now()
	attrsOf := func(name string) metric.MeasurementOption {
		return metric.WithAttributes(
			telemetry.AttrEntity.String(info.name),
			telemetry.AttrIndex.String(name),
		)
	}
	// The walk is shared, so every index the pass walks for is charged the
	// whole pass. That is what the per-index duration meant when each index had
	// a walk to itself: the time between "this index is unusable" and "this
	// index answers queries".
	record := func(name, outcome string) {
		s.tel().indexBuildDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			telemetry.AttrEntity.String(info.name),
			telemetry.AttrIndex.String(name),
			telemetry.AttrOutcome.String(outcome),
		))
	}

	// Any staleness serves: a stale "not ready" only re-runs the write path
	// below, whose own transactions re-check against the primary.
	var pending []buildTarget
	if err := s.readTx(ctx, ws, 0, func(tx storage.ReadTx) error {
		pending = pending[:0]
		for _, t := range targets {
			ready, err := indexReadyState(tx, info.name, t.name, t.specHash)
			if err != nil {
				return err
			}
			if !ready {
				pending = append(pending, t)
			}
		}
		return nil
	}); err != nil {
		for _, t := range targets {
			record(t.name, telemetry.Classify(err))
		}
		return false, err
	}
	// A pass over an index that was already ready did no work and should not
	// land in the same distribution as one that walked the entity — most passes
	// are this, and counting them as builds would put the median build at zero.
	for _, t := range targets {
		if !containsTarget(pending, t.name) {
			record(t.name, telemetry.OutcomeSkipped)
		}
	}
	if len(pending) == 0 {
		return true, nil
	}
	// Every index still waiting on the walk shares its fate, so a failure is
	// reported once against each of them.
	failed := func(rest []buildTarget, e error) (bool, error) {
		for _, t := range rest {
			record(t.name, telemetry.Classify(e))
		}
		return false, e
	}

	// Stamp the specs BEFORE the walk. If a declaration changed, this is what
	// stops a reader from trusting entries written under the old one: the
	// state row now names the new spec and says not ready.
	if err := s.store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		for _, t := range pending {
			if err := tx.SetIndexState(info.name, t.name, t.specHash, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return failed(pending, err)
	}

	batch, budget := s.indexBuild.batchSize(), s.indexBuild.txTimeBudget()
	afterKey := ""
	for {
		if ctx.Err() != nil {
			return failed(pending, ctx.Err())
		}
		var (
			lastKey string
			seen    int
			cut     bool
		)
		if err := s.store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			// Reset per attempt: WithWriteTx may re-run the closure.
			lastKey, seen, cut = "", 0, false
			deadline := time.Now().Add(budget)
			err := tx.ScanEntityAfter(info.name, afterKey, batch, func(row storage.Row) error {
				seen++
				lastKey = row.Key
				msg, err := info.decode(row.Value)
				if err != nil {
					// A row that will not decode is already invisible to every
					// read (see poison.go), so it belongs in no index either.
					// Skipping it keeps the build from being blocked forever by
					// one bad row.
					return nil
				}
				if err := info.updateRowIndexes(tx, s.limits, row.Key, msg); err != nil {
					return err
				}
				// Cut the batch on the clock as well as on the row cap. A row's
				// cost varies with the entries it occupies and with how loaded
				// the store is, so the cap bounds work but not the time every
				// writer of this workspace spends waiting behind this
				// transaction — which is the number an operator feels.
				if time.Now().After(deadline) {
					cut = true
					return errIndexBuildBudget
				}
				return nil
			})
			if errors.Is(err, errIndexBuildBudget) {
				return nil
			}
			return err
		}); err != nil {
			return failed(pending, err)
		}
		// After the commit, never inside the closure: WithWriteTx may re-run
		// it, and a counter incremented from a rolled-back attempt would report
		// progress the indexes do not have.
		for _, t := range pending {
			s.tel().indexBuildRows.Add(ctx, int64(seen), attrsOf(t.name))
		}
		if !cut && seen < batch {
			break
		}
		afterKey = lastKey
		// Nothing hands the store's write connection to whoever has waited
		// longest — database/sql wakes an arbitrary waiter, and a build that
		// reopened its transaction immediately would keep taking it back before
		// a blocked writer is scheduled. Standing down is the whole fairness
		// mechanism, and a background build is what should pay for it.
		time.Sleep(indexBuildYield)
	}

	allReady = true
	for i, t := range pending {
		var ready bool
		if err := s.store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			// Re-check the fingerprint at the end: if the schema was
			// re-registered mid-build, the catalog this goroutine holds is
			// stale and marking ready would advertise entries built for a spec
			// nobody uses. The next pass, holding the new catalog, redoes the
			// walk.
			//
			// Reset per attempt, as the counters above are: WithWriteTx may
			// re-run the closure, and a ready carried over from a rolled-back
			// attempt would let the caller record a build mark for an index
			// that has none.
			ready = false
			hash, _, err := tx.IndexState(info.name, t.name)
			if err != nil {
				return err
			}
			if hash != t.specHash {
				return nil
			}
			ready = true
			return tx.SetIndexState(info.name, t.name, t.specHash, true)
		}); err != nil {
			return failed(pending[i:], err)
		}
		// A re-registration that landed mid-walk leaves the index unready, and
		// the walk it got did no good: the next pass, holding the new catalog,
		// redoes it. That is a build that did not happen, not a failed one.
		outcome := telemetry.OutcomeOK
		if !ready {
			outcome = telemetry.OutcomeSkipped
		}
		record(t.name, outcome)
		allReady = allReady && ready
	}
	return allReady, nil
}

func containsTarget(targets []buildTarget, name string) bool {
	for _, t := range targets {
		if t.name == name {
			return true
		}
	}
	return false
}

func (l *indexBuildLoop) batchSize() int {
	if l == nil || l.batch <= 0 {
		return defaultIndexBuildBatch
	}
	return l.batch
}

func (l *indexBuildLoop) txTimeBudget() time.Duration {
	if l == nil || l.txBudget <= 0 {
		return defaultIndexBuildTxBudget
	}
	return l.txBudget
}
