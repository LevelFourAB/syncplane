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

// gcLoop owns the background garbage-collection goroutine.
type gcLoop struct {
	retention time.Duration
	// textRetention bounds the collaborative-text delta log; see
	// WithTextDeltaRetention. Always clamped to retention.
	textRetention time.Duration
	// textCompaction is the minimum number of items a text field must give up
	// before the pass rewrites its row; <= 0 disables compaction. See
	// WithTextCompaction.
	textCompaction int
	// keyRetention is how long claimed idempotency keys are kept; see
	// WithIdempotencyRetention. Independent of retention — it is measured
	// against callers' retry windows, not clients' offline windows.
	keyRetention time.Duration
	interval     time.Duration
	// configured is set by WithGC. WithTextDeltaRetention can create the loop
	// on its own, and a loop nobody asked to collect must not collect.
	configured bool
	cancel     context.CancelFunc
	done       sync.WaitGroup
	closeOnce  sync.Once
}

// defaultTextDeltaRetention is how long the text delta log is kept when
// WithTextDeltaRetention says nothing. It is deliberately short compared to
// tombstone retention: the log only serves readers catching up over a span it
// still covers, and a reader further behind than this saves nothing by
// reconstructing a document one keystroke at a time.
const defaultTextDeltaRetention = time.Hour

// defaultIdempotencyRetention is how long a claimed idempotency key is kept when
// WithIdempotencyRetention says nothing. A day covers the retry windows real
// callers have — a load balancer's, a client library's backoff, an effects
// handler restarted after an outage — without keeping a row per API request
// around for the weeks tombstone retention needs.
const defaultIdempotencyRetention = 24 * time.Hour

// WithGC enables garbage collection. One pass prunes, per workspace and with
// the same retention, both tombstones (clients whose cursor predates a
// collected tombstone get reset=true on the next pull and re-bootstrap) and
// idle client watermark rows (bounding the clients table). The same pass also
// carries the text delta log and the trusted-write idempotency ledger, each on
// its own retention (WithTextDeltaRetention, WithIdempotencyRetention), and
// effect consumers stranded below the horizon the pass just set — a consumer
// still keeping up is never below it, however long it idles.
//
// Retention MUST comfortably exceed the longest expected client offline
// window. The push path's watermark adoption depends on it: a pruned client
// that returns is re-admitted at whatever mutation id it pushes next, which
// is only safe while no pruned client can still be retrying mutations the
// server already applied. Weeks is a sane order of magnitude; minutes is not.
//
// interval > 0 runs a background pass over every workspace each interval;
// interval <= 0 configures retention for manual CollectGarbage calls only.
// Call Close to stop the background goroutine.
//
// Multi-node note: every node may run the pass — it is idempotent — but
// coordinating via an advisory lock would avoid duplicate work (deferred).
func WithGC(retention, interval time.Duration) Option {
	return func(s *Server) error {
		if s.gc == nil {
			s.gc = &gcLoop{}
		}
		s.gc.retention = retention
		s.gc.interval = interval
		s.gc.configured = true
		return nil
	}
}

// WithTextDeltaRetention overrides how long each GC pass keeps the
// collaborative-text delta log — the per-(row, field) fragments a pull folds to
// ship an edit instead of the whole document (see textdelta.go). It only takes
// effect together with WithGC, and never outlives that pass's retention.
//
// The trade-off is pure wire size against storage: a span the log no longer
// covers is served as full state, which is correct, just larger. Shortening it
// costs bytes on the wire for clients that reconnect after a lull; lengthening
// it keeps one small row per text write around for that much longer. Default
// one hour.
func WithTextDeltaRetention(d time.Duration) Option {
	return func(s *Server) error {
		if s.gc == nil {
			s.gc = &gcLoop{}
		}
		s.gc.textRetention = d
		return nil
	}
}

// WithIdempotencyRetention overrides how long each GC pass keeps the
// idempotency keys claimed by MutateIdempotent/InvokeIdempotent. It only takes
// effect together with WithGC — without a pass, keys are kept forever, which is
// safe (a retry is never let through twice) but grows with API traffic.
//
// This is the one retention where too short is a correctness problem, not just
// a cost one: a retry arriving after its key expired re-applies the write, with
// no signal that it did. So it is NOT clamped to WithGC's retention the way the
// text delta log is — set it above the longest retry window any caller uses
// (including an integration resuming after an outage), and remember that the
// bound is on the whole retry chain, not one hop. Default 24 hours.
func WithIdempotencyRetention(d time.Duration) Option {
	return func(s *Server) error {
		if s.gc == nil {
			s.gc = &gcLoop{}
		}
		s.gc.keyRetention = d
		return nil
	}
}

// idempotencyCutoff is the timestamp below which claimed keys are collectable.
func (g *gcLoop) idempotencyCutoff(now time.Time) time.Time {
	d := g.keyRetention
	if d <= 0 {
		d = defaultIdempotencyRetention
	}
	return now.Add(-d)
}

// textDeltaCutoff is the timestamp below which delta-log entries are prunable.
// Clamped to the pass's own retention: a client older than that re-bootstraps,
// so keeping its deltas would serve nobody.
func (g *gcLoop) textDeltaCutoff(now time.Time) time.Time {
	d := g.textRetention
	if d <= 0 {
		d = defaultTextDeltaRetention
	}
	if d > g.retention {
		d = g.retention
	}
	return now.Add(-d)
}

// start launches the ticker goroutine; called from New once options applied.
func (g *gcLoop) start(s *Server) {
	if g.interval <= 0 {
		return
	}
	// The pass runs under this context so close() can cancel an in-flight
	// pass instead of blocking Close for a full all-workspaces sweep.
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	logger := s.log().With("component", "gc")
	g.done.Add(1)
	go func() {
		defer g.done.Done()
		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Maintenance must never take the server down: log and retry
				// on the next tick. A cancelled pass (shutdown) is not an
				// error worth logging.
				if err := s.collectAllGarbage(ctx); err != nil && ctx.Err() == nil {
					logger.LogAttrs(ctx, slog.LevelError, "garbage collection pass failed", slog.Any("err", err))
				}
			}
		}
	}()
}

func (g *gcLoop) close() {
	g.closeOnce.Do(func() {
		if g.cancel != nil {
			g.cancel()
		}
	})
	g.done.Wait()
}

// CollectGarbage runs one manual collection pass (tombstones + idle
// clients) for a single workspace using the retention configured via
// WithGC. The pass runs in one write transaction; it is stateless and safe
// to re-run.
//
// Do not call it from inside a mutator: it opens a write transaction, which
// would deadlock against the one the mutator already holds. When the context
// is threaded from the mutator the call fails fast with an error instead.
func (s *Server) CollectGarbage(ctx context.Context, workspaceID string) error {
	// WithTextDeltaRetention alone creates the loop without configuring a pass;
	// only WithGC decides that garbage is collected at all.
	if s.gc == nil || !s.gc.configured {
		return fmt.Errorf("server: GC is not configured (use WithGC)")
	}
	if inWriteTx(ctx) {
		return fmt.Errorf("server: CollectGarbage called re-entrantly inside a write transaction (workspace %q): it opens a write transaction that would deadlock against the running one", workspaceID)
	}
	now := time.Now()
	cutoff := now.Add(-s.gc.retention)
	textCutoff := s.gc.textDeltaCutoff(now)
	keyCutoff := s.gc.idempotencyCutoff(now)
	var horizon, deltaHorizon uint64
	var idleRemoved, keysRemoved, compacted, consumersRemoved int
	if err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		// Re-zero on retry: the closure may run more than once (see the
		// storage.Adapter write-closure contract).
		horizon, deltaHorizon, idleRemoved, keysRemoved, compacted, consumersRemoved = 0, 0, 0, 0, 0, 0
		h, err := tx.CollectTombstones(cutoff)
		if err != nil {
			return err
		}
		horizon = h
		if idleRemoved, err = tx.CollectIdleClients(cutoff); err != nil {
			return err
		}
		// Effect consumers are collected against the horizon this pass just
		// established: one below it has already missed collected tombstones and
		// resets on its owner's next pass either way. A caught-up consumer sits
		// at the current sequence and is never below it, however long it idles.
		if consumersRemoved, err = tx.CollectIdleConsumers(cutoff, horizon); err != nil {
			return err
		}
		if keysRemoved, err = tx.CollectIdempotencyKeys(keyCutoff); err != nil {
			return err
		}
		if deltaHorizon, err = tx.PruneFieldDeltas(textCutoff); err != nil {
			return err
		}
		// Compaction runs last, on rows the prune just declared quiet (see
		// compact.go); it allocates a sequence only if it rewrites something.
		compacted, err = s.compactText(tx, deltaHorizon)
		return err
	}); err != nil {
		return err
	}
	// What the pass actually reclaimed, by kind. Recorded after the commit, so
	// a retried write closure cannot double-count. stranded_consumers is the
	// kind worth alerting on rather than graphing — see the warning below.
	for _, c := range []struct {
		kind string
		n    int
	}{
		{"idle_clients", idleRemoved},
		{"idempotency_keys", keysRemoved},
		{"text_rows", compacted},
		{"stranded_consumers", consumersRemoved},
	} {
		if c.n > 0 {
			s.tel().gcCollected.Add(ctx, int64(c.n), metric.WithAttributes(telemetry.AttrGCKind.String(c.kind)))
		}
	}
	if consumersRemoved > 0 {
		// Louder than the rest of the pass: a collected consumer is one that
		// stopped keeping up long enough to fall past the horizon, so something
		// that was supposed to be reacting to writes was not.
		s.log().LogAttrs(ctx, slog.LevelWarn, "collected effect consumers stranded below the tombstone horizon",
			slog.String("component", "gc"),
			slog.String("workspace", workspaceID),
			slog.Int("consumers_removed", consumersRemoved),
			slog.Uint64("tombstone_horizon", horizon),
		)
	}
	s.log().LogAttrs(ctx, slog.LevelDebug, "collected garbage",
		slog.String("component", "gc"),
		slog.String("workspace", workspaceID),
		slog.Uint64("tombstone_horizon", horizon),
		slog.Uint64("text_delta_horizon", deltaHorizon),
		slog.Int("idle_clients_removed", idleRemoved),
		slog.Int("idempotency_keys_removed", keysRemoved),
		slog.Int("text_rows_compacted", compacted),
		slog.Int("consumers_removed", consumersRemoved),
	)
	return nil
}

// collectAllGarbage runs one pass over every workspace. A per-workspace
// failure (corrupt row, adapter bug) is collected and the pass moves on, so
// one bad workspace cannot starve every workspace after it in listing order;
// the joined errors are returned for logging. Only context cancellation
// (server shutdown) aborts the pass early.
func (s *Server) collectAllGarbage(ctx context.Context) (err error) {
	start := time.Now()
	// Recorded from a defer so every exit records exactly once: the pass that
	// could not even list workspaces, and the one cancelled partway through by
	// shutdown, both happened and both cost time.
	defer func() {
		s.tel().gcDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			telemetry.AttrOutcome.String(telemetry.Classify(err))))
	}()
	ids, err := s.store.ListWorkspaceIDs(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cerr := s.CollectGarbage(ctx, id)
		// Per-workspace, because the pass swallows individual failures and
		// carries on: without this, one workspace that fails every pass
		// forever is indistinguishable from a clean sweep in everything except
		// the joined error the caller logs.
		s.tel().gcWorkspaces.Add(ctx, 1, metric.WithAttributes(
			telemetry.AttrOutcome.String(telemetry.Classify(cerr))))
		if cerr != nil {
			errs = append(errs, fmt.Errorf("workspace %s: %w", id, cerr))
		}
	}
	s.log().LogAttrs(ctx, slog.LevelInfo, "garbage collection pass complete",
		slog.String("component", "gc"),
		slog.Int("workspaces", len(ids)),
		slog.Int("failed", len(errs)),
		slog.Duration("elapsed", time.Since(start)))
	return errors.Join(errs...)
}

// Close stops background work: the GC goroutine, the index backfill, and the
// notify-coalescing timers. It is idempotent and does NOT close what the
// caller owns — neither the storage adapter nor a notifier installed with
// WithNotifier. Stopping the coalescing timers is what keeps a shared
// notifier from receiving a trailing publish after the process announced it
// was done writing.
func (s *Server) Close() error {
	if s.gc != nil {
		s.gc.close()
	}
	if s.indexBuild != nil {
		s.indexBuild.close()
	}
	if s.coalescer != nil {
		_ = s.coalescer.Close()
	}
	return nil
}
