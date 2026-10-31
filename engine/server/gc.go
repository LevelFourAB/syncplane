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
	// keyRetention is how long claimed idempotency keys are kept; see
	// WithIdempotencyRetention. Independent of retention — it is measured
	// against callers' retry windows, not clients' offline windows.
	keyRetention time.Duration
	// historyRetention is how long field-history records are kept; see
	// WithHistoryRetention. Independent of retention for the same reason
	// keyRetention is: it is measured against how far back an app wants to be
	// able to restore, which has nothing to do with client offline windows.
	historyRetention time.Duration
	// clientRetention is how long an idle client's watermark record is kept;
	// see WithClientRetention. Zero keeps every record, which is the default:
	// the record is what stops a resent mutation from applying twice, and a
	// client's pending queue outlives any retention.
	clientRetention time.Duration
	interval        time.Duration
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
// the rejection verdicts recorded for idle clients (a resend of a refused
// mutation is then answered with a bare ALREADY_APPLIED instead of the
// verdict). The same pass also carries the text delta log and the
// trusted-write idempotency ledger, each on its own retention
// (WithTextDeltaRetention, WithIdempotencyRetention), and effect consumers
// stranded below the horizon the pass just set — a consumer still keeping up
// is never below it, however long it idles. The same pass also expires
// field-history records (WithHistoryRetention), which it hard-deletes rather
// than tombstones because nothing syncs them. On the tombstone retention it
// also deletes the sequence lineage records that let a pull detect a store
// restored from backup: a cursor whose sequence is older than the retention is
// served without that check.
//
// Idle clients' watermark records are kept whatever the retention: they are
// what makes a resend idempotent, and a client's pending queue outlives any
// offline window. WithClientRetention opts into deleting them.
//
// Retention MUST comfortably exceed the longest expected client offline
// window, or clients spend their returns re-bootstrapping. Weeks is a sane
// order of magnitude; minutes is not.
//
// interval > 0 runs a background pass each interval over the workspaces holding
// something these retentions make collectable, which on an idle deployment is
// none of them; interval <= 0 configures retention for manual CollectGarbage
// calls only. Call Close to stop the background goroutine.
//
// Multi-node note: every node may run the pass — it is idempotent — and each
// workspace's collection runs under a per-workspace lease (see WithLeases), so
// nodes sharing a lease-providing store divide the sweep rather than repeat it.
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

// WithClientRetention makes each GC pass delete the watermark records of
// clients that have not pushed for d, bounding the clients table. It only
// takes effect together with WithGC. Zero, the default, keeps every record.
//
// A deleted watermark forgets which mutation ids the client already had
// applied. The client re-enters through watermark adoption at whatever id it
// pushes next — and if that push is a resend of mutations whose response was
// lost, they apply a second time: a stale PUT or DELETE then overwrites what
// other clients wrote in the meantime, and a batch or custom mutator runs
// twice. The client's pending queue is durable, so the resend can arrive
// however long after the loss. Set d well past the longest time a client can
// stay away and still be expected to hold unpushed work, and prefer leaving
// it unset unless the clients table is a measured problem. See
// docs/explanation/limits-and-trade-offs.md.
func WithClientRetention(d time.Duration) Option {
	return func(s *Server) error {
		if s.gc == nil {
			s.gc = &gcLoop{}
		}
		s.gc.clientRetention = d
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

// defaultHistoryRetention is how long field-history records are kept when
// WithHistoryRetention says nothing. It is measured against how far back a
// person wants to reach when restoring, not against how long a client can be
// offline, and it is generous: too short means the value someone came to
// recover is the one that expired.
//
// Finite rather than HistoryForever, because a workspace's history grows with
// every write and nothing else here opts an operator into unbounded growth by
// default.
const defaultHistoryRetention = 90 * 24 * time.Hour

// HistoryForever disables time-based expiry of field-history records: a record
// then lives exactly as long as the row it describes, since a DELETE of that
// row purges its history whatever the retention.
//
// Pass it to WithHistoryRetention when history is app-visible data with no
// natural age limit. The cost is that a hot row's log grows without bound for
// as long as the row lives, so it suits entities whose rows are edited by
// people rather than rewritten by machinery.
const HistoryForever time.Duration = -1

// WithHistoryRetention overrides how long each GC pass keeps the field-history
// records written for entities carrying (syncplane.v1.entity).history. It only
// takes effect together with WithGC — without a pass, records are kept until
// their row is deleted whatever this says. Default 90 days.
//
// Pass HistoryForever to turn time-based expiry off and tie a record's lifetime
// to its row's alone.
//
// A record ages from its LAST write rather than from its recorded_at, so one a
// session folded later writes into survives up to a WithHistorySession window
// past what its timestamp says. Nothing folds under HistorySessionOff, where
// the two coincide.
//
// Unlike the text delta log it is NOT clamped to WithGC's retention: history is
// app-visible data with a lifetime of its own, so a value either side of the
// tombstone window is valid.
//
// The sweep HARD-DELETES, leaving no tombstone — nothing syncs these records,
// so nothing has a copy to be told they are gone. It is also the one collector
// that works in chunks, draining a long-accumulated log across transactions
// instead of inside the pass's own (see historyPurgeChunk).
func WithHistoryRetention(d time.Duration) Option {
	return func(s *Server) error {
		if s.gc == nil {
			s.gc = &gcLoop{}
		}
		s.gc.historyRetention = d
		return nil
	}
}

// historyCutoff is the timestamp below which history records are collectable,
// and whether there is one at all — HistoryForever leaves expiry to the row's
// own deletion. Zero (unset) takes the default rather than meaning forever, so
// that "I never configured this" and "I asked for unbounded growth" stay
// different answers.
func (g *gcLoop) historyCutoff(now time.Time) (time.Time, bool) {
	d := g.historyRetention
	if d < 0 {
		return time.Time{}, false
	}
	if d == 0 {
		d = defaultHistoryRetention
	}
	return now.Add(-d), true
}

// clientCutoff is the timestamp below which idle clients' watermark records
// are collectable, and whether they are collectable at all — unset keeps them
// (see WithClientRetention).
func (g *gcLoop) clientCutoff(now time.Time) (time.Time, bool) {
	if g.clientRetention == 0 {
		return time.Time{}, false
	}
	return now.Add(-g.clientRetention), true
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

// cutoffs is every age threshold a pass at now applies, in the form the work
// query takes them. CollectGarbage derives its own thresholds from the same
// value, so what selects a workspace and what the pass then collects cannot
// drift apart.
func (g *gcLoop) cutoffs(now time.Time) storage.GCCutoffs {
	c := storage.GCCutoffs{
		Tombstones:      now.Add(-g.retention),
		IdempotencyKeys: g.idempotencyCutoff(now),
		FieldDeltas:     g.textDeltaCutoff(now),
	}
	if at, prunes := g.clientCutoff(now); prunes {
		c.Clients = at
	}
	if at, expires := g.historyCutoff(now); expires {
		c.Entities = []storage.EntityCutoff{{Entity: historyEntityName, Before: at}}
	}
	return c
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

// CollectGarbage runs one manual collection pass (tombstones, idle clients'
// verdicts and, with WithClientRetention, their watermarks) for a single
// workspace using the retention configured via WithGC. With WithBlobStore it
// also collects the workspace's blobs (see WithBlobRetention). The pass runs
// in one write transaction, plus one transaction per further chunk of
// expired field history (see historyPurgeChunk); it is stateless and safe to
// re-run, and an interrupted drain resumes on the next pass.
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
	err := s.collectRows(ctx, workspaceID)
	if s.blobs != nil {
		err = errors.Join(err, s.collectBlobs(ctx, workspaceID))
	}
	return err
}

// collectRows is CollectGarbage for everything the storage adapter holds.
func (s *Server) collectRows(ctx context.Context, workspaceID string) error {
	// Through cutoffs rather than the retentions directly, so that what a work
	// query selects a workspace for and what this pass then collects are read
	// off one value (see gcLoop.cutoffs).
	now := time.Now()
	cutoffs := s.gc.cutoffs(now)
	cutoff := cutoffs.Tombstones
	textCutoff := cutoffs.FieldDeltas
	keyCutoff := cutoffs.IdempotencyKeys
	// HistoryForever is the only reason to skip the sweep: it leaves expiry to
	// the row's own deletion. Notably NOT gated on the history entity being in
	// the catalog, even though sweeping a name nothing writes costs a statement
	// per workspace per pass: records are durable state and catalog membership is
	// not, so a deploy that drops the last history: {} leaves records that no
	// write path touches, no read path reaches, and no DELETE purges (see
	// Server.purgeHistory). The age sweep is all they have left, and gating it on
	// the catalog would make them immortal.
	historyCutoff, historyExpires := s.gc.historyCutoff(now)
	clientCutoff, clientsPruned := s.gc.clientCutoff(now)
	var horizon, deltaHorizon uint64
	var verdictsExpired, idleRemoved, keysRemoved, consumersRemoved, historyRemoved, lineageRemoved int
	var historyMore bool
	if err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		// Re-zero on retry: the closure may run more than once (see the
		// storage.Adapter write-closure contract).
		horizon, deltaHorizon, verdictsExpired, idleRemoved, keysRemoved, consumersRemoved = 0, 0, 0, 0, 0, 0
		historyRemoved, historyMore, lineageRemoved = 0, false, 0
		h, err := tx.CollectTombstones(cutoff)
		if err != nil {
			return err
		}
		horizon = h
		// A cursor below the horizon resets, so the access moves it could
		// have needed go with the tombstones.
		if _, err := purgeAccessMoves(tx, horizon); err != nil {
			return err
		}
		// Verdicts go on the tombstone retention; the watermark they sit next to
		// only goes when the operator opted in. The order matters: an expiry
		// must not refresh the record, or the prune below would never see it
		// idle (the storage contract holds adapters to that).
		if verdictsExpired, err = tx.ExpireClientRejections(cutoff); err != nil {
			return err
		}
		if clientsPruned {
			if idleRemoved, err = tx.CollectIdleClients(clientCutoff); err != nil {
				return err
			}
		}
		// Effect consumers are collected against the horizon this pass just
		// established: one below it has already missed collected tombstones and
		// resets on its owner's next pass either way. A caught-up consumer sits
		// at the current sequence and is never below it, however long it idles.
		if consumersRemoved, err = tx.CollectIdleConsumers(cutoff, horizon); err != nil {
			return err
		}
		// On the tombstone retention because both bound the same thing: how
		// long a cursor stays fully checked. A cursor whose sequence lost its
		// lineage record is still served, without the restore check.
		if lineageRemoved, err = tx.PruneSeqLineage(cutoff); err != nil {
			return err
		}
		if keysRemoved, err = tx.CollectIdempotencyKeys(keyCutoff); err != nil {
			return err
		}
		if deltaHorizon, err = tx.PruneFieldDeltas(textCutoff); err != nil {
			return err
		}
		// Hard delete, not a tombstone: no client syncs these rows, so nothing
		// has a copy to be told about and there is no second pass to collect
		// what a tombstone would have left. See WithHistoryRetention.
		//
		// The one collector here that takes a limit. Everything else in this
		// transaction is bounded by rows — tombstones, clients, consumers — while
		// history is bounded by WRITES, so the first sweep after enabling it (or
		// after raising retention) is the largest delete the engine issues, and it
		// would hold this transaction for all of it. What the chunk leaves behind
		// drains below, one transaction at a time.
		if historyExpires {
			if historyRemoved, historyMore, err = tx.PurgeEntityRows(historyEntityName, "", historyCutoff, historyPurgeChunk); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// The backlog the bounded chunk did not reach, in transactions of its own —
	// the point of the bound. Its error is carried to the end rather than
	// returned here: the chunks that did commit are reclaimed either way, and a
	// pass that collected everything else should say so before it reports the
	// drain that stopped short.
	var drainErr error
	if historyMore {
		var drained int
		drained, drainErr = s.drainExpiredHistory(ctx, workspaceID, historyCutoff)
		historyRemoved += drained
	}
	// What the pass actually reclaimed, by kind. Recorded after the commit, so
	// a retried write closure cannot double-count. stranded_consumers is the
	// kind worth alerting on rather than graphing — see the warning below.
	for _, c := range []struct {
		kind string
		n    int
	}{
		{"client_verdicts", verdictsExpired},
		{"idle_clients", idleRemoved},
		{"idempotency_keys", keysRemoved},
		{"history_records", historyRemoved},
		{"stranded_consumers", consumersRemoved},
		{"lineage_records", lineageRemoved},
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
		slog.Int("client_verdicts_expired", verdictsExpired),
		slog.Int("idle_clients_removed", idleRemoved),
		slog.Int("idempotency_keys_removed", keysRemoved),
		slog.Int("history_records_removed", historyRemoved),
		slog.Int("consumers_removed", consumersRemoved),
	)
	return drainErr
}

// historyPurgeChunk is how many expired field-history records one transaction
// removes. Large enough that an ordinary pass drains in one or two chunks —
// history accrues per field-writing mutation, so a busy workspace produces
// thousands between passes — and small enough that no single delete holds a
// write transaction long enough to matter to the writers queued behind it.
//
// A var only so the drain loop is testable without planting a chunk's worth of
// records; nothing changes it at runtime.
var historyPurgeChunk = 2000

// drainExpiredHistory removes what the pass's first bounded purge left behind,
// one transaction per chunk, until the sweep reports the selection empty. It
// takes the pass's cutoff rather than a fresh one so the selection it is
// draining cannot grow under it: records written while it runs are newer than
// the cutoff, which is what makes the loop terminate.
//
// Each chunk commits on its own, so an error (or a cancelled context on
// shutdown) keeps whatever earlier chunks reclaimed and leaves the rest for the
// next pass to resume on — the sweep carries no state between calls.
func (s *Server) drainExpiredHistory(ctx context.Context, workspaceID string, cutoff time.Time) (int, error) {
	total := 0
	for more := true; more; {
		// Checked here rather than left to the adapter so a shutdown mid-drain
		// reports the cancellation itself instead of whatever the store makes of
		// a cancelled transaction.
		if err := ctx.Err(); err != nil {
			return total, err
		}
		var removed int
		if err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
			var err error
			removed, more, err = tx.PurgeEntityRows(historyEntityName, "", cutoff, historyPurgeChunk)
			return err
		}); err != nil {
			return total, err
		}
		total += removed
	}
	return total, nil
}

// maintenancePage is how many workspace ids one page of a background pass's
// work query carries. It bounds the pass's memory and nothing else: a pass
// keeps fetching pages until one comes back short, so a page size does not cap
// what an interval covers.
const maintenancePage = 256

// collectAllGarbage runs one pass over every workspace holding something this
// pass's cutoffs would reclaim, a page at a time. A per-workspace failure
// (corrupt row, adapter bug) is collected and the pass moves on, so one bad
// workspace cannot starve every workspace after it in listing order; the joined
// errors are returned for logging. Only context cancellation (server shutdown)
// aborts the pass early.
//
// The work query is what keeps an idle deployment idle: a workspace with
// nothing collectable is never returned, so it costs neither the lease probe
// nor the write transaction that finds nothing. Workspaces counts what the pass
// visited, which on a healthy deployment is a small fraction of what exists.
func (s *Server) collectAllGarbage(ctx context.Context) (err error) {
	start := time.Now()
	// Recorded from a defer so every exit records exactly once: the pass that
	// could not even list workspaces, and the one cancelled partway through by
	// shutdown, both happened and both cost time.
	defer func() {
		s.tel().gcDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			telemetry.AttrOutcome.String(telemetry.Classify(err))))
	}()
	cutoffs := s.gc.cutoffs(time.Now())
	var errs []error
	visited := 0
	for after := ""; ; {
		ids, lerr := s.store.ListCollectableWorkspaceIDs(ctx, cutoffs, storage.Range{After: after}, maintenancePage)
		if lerr != nil {
			return errors.Join(append(errs, lerr)...)
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			visited++
			release, run := s.tryLease(ctx, leaseRoleGC, id)
			if !run {
				// Another node holds the lease and is collecting this workspace;
				// counted so an operator can see the fleet dividing the sweep.
				s.tel().gcWorkspaces.Add(ctx, 1, metric.WithAttributes(
					telemetry.AttrOutcome.String(telemetry.OutcomeSkipped)))
				continue
			}
			cerr := s.collectRows(ctx, id)
			release()
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
		// Paged by workspace id, not by the predicate: a workspace this page
		// collected no longer matches it, and paging by the answer would step
		// over the one that took its place.
		if len(ids) < maintenancePage {
			break
		}
		after = ids[len(ids)-1]
	}
	if s.blobs != nil {
		if berr := s.collectAllBlobs(ctx); berr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, berr)
		}
	}
	s.log().LogAttrs(ctx, slog.LevelInfo, "garbage collection pass complete",
		slog.String("component", "gc"),
		slog.Int("workspaces", visited),
		slog.Int("failed", len(errs)),
		slog.Duration("elapsed", time.Since(start)))
	return errors.Join(errs...)
}

// Close stops background work: the GC goroutine, the index backfill, the room
// registry's announce sweep, and the notify-coalescing timers. It is idempotent
// and does NOT close what the caller owns — not the storage adapter, not a
// notifier installed with WithNotifier, not a bus installed with
// WithPresenceBus. Stopping the coalescing timers is what keeps a shared
// notifier from receiving a trailing publish after the process announced it was
// done writing; closing the registry likewise stops it announcing presence for
// sessions this process is no longer serving.
func (s *Server) Close() error {
	if s.gc != nil {
		s.gc.close()
	}
	if s.indexBuild != nil {
		s.indexBuild.close()
	}
	if s.roomSweep != nil {
		s.roomSweep.close()
	}
	if s.rooms != nil {
		s.rooms.Close()
	}
	if s.coalescer != nil {
		_ = s.coalescer.Close()
	}
	return nil
}
