package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/internal/telemetry"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Push applies one batch of mutations per workspace, each in its own write
// transaction. Within a batch a business rejection rolls back every row the
// rejected mutation wrote (and its sequence) while the rest of the batch — and
// the watermark bump for the rejected mutation — still commits.
//
// Per-client watermark rules: mutation_id <= watermark is ALREADY_APPLIED, or
// REJECTED again if that id's verdict is still on record (which is what keeps a
// lost push response from turning a rejection into a silent drop);
// mutation_id == watermark+1 applies (or REJECTS — both bump the watermark,
// a rejection is an ack); a gap fails that workspace's batch so the client can
// resend it in order.
//
// Batches are independent. There is no cross-workspace atomicity — the storage
// adapter serializes writes per workspace and nothing spans two partitions — so
// a failure scoped to one workspace is reported in its WorkspaceResults rather
// than as an RPC error. That is what keeps one revoked or wedged workspace from
// blocking every other workspace a client holds. RPC errors stay reserved for
// failures of the request itself: malformed, over limits, unauthenticated.
func (s *Server) Push(ctx context.Context, req *connect.Request[syncplanev1.PushRequest]) (*connect.Response[syncplanev1.PushResponse], error) {
	batches := req.Msg.GetBatches()
	if err := s.validatePush(batches); err != nil {
		return nil, err
	}
	resp := &syncplanev1.PushResponse{
		Results:        make([]*syncplanev1.WorkspaceResults, 0, len(batches)),
		SchemaRevision: s.catalogPtr().schemaRev,
	}
	for _, b := range batches {
		resp.Results = append(resp.Results, s.pushWorkspace(ctx, b))
	}
	return connect.NewResponse(resp), nil
}

// pushWorkspace applies one workspace's batch and always returns a result — it
// converts every failure into a code on the WorkspaceResults rather than
// propagating it, so a sibling workspace in the same request is unaffected.
func (s *Server) pushWorkspace(ctx context.Context, batch *syncplanev1.WorkspaceBatch) *syncplanev1.WorkspaceResults {
	ws := batch.GetWorkspaceId()
	out := &syncplanev1.WorkspaceResults{WorkspaceId: ws}

	// One workspace's batch is the unit that holds a write transaction, and so
	// the unit that contends — which makes it the span and the latency worth
	// recording, not the RPC (which may carry several) and not the individual
	// mutation (which cannot be observed apart from its transaction).
	ctx, span := s.tel().tracer.Start(ctx, "syncplane.push.batch",
		trace.WithAttributes(telemetry.AttrWorkspace.String(ws)))
	start := time.Now()
	defer func() {
		// Every failure in here is converted to a code on the result rather
		// than returned, so the code — not a Go error — is what decides the
		// outcome.
		outcome := telemetry.OutcomeOK
		if code := out.GetErrorCode(); code != "" {
			outcome = telemetry.OutcomeError
			if ctx.Err() != nil {
				outcome = telemetry.OutcomeCanceled
			}
			span.SetAttributes(telemetry.AttrOutcome.String(outcome))
			span.SetStatus(codes.Error, out.GetErrorMessage())
		}
		s.tel().pushBatch.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(telemetry.AttrOutcome.String(outcome)))
		span.End()
	}()

	fail := func(code, format string, args ...any) *syncplanev1.WorkspaceResults {
		out.Results = nil
		out.ErrorCode = code
		out.ErrorMessage = fmt.Sprintf(format, args...)
		return out
	}

	// failInternal reports a fault the caller cannot act on. Only the phase
	// reaches the client; err itself is logged, because a permission hook, a
	// history filter or a storage adapter words its errors for an operator and
	// may name a dependency, a key or a row the caller has no business seeing.
	// The log is the ONLY record: a workspace-scoped failure travels as a code
	// on the result, so the logging interceptor sees this RPC succeed.
	failInternal := func(phase string, err error) *syncplanev1.WorkspaceResults {
		s.reqLog(ctx, "push", ws, "").LogAttrs(ctx, slog.LevelError, "batch failed",
			slog.String("phase", phase),
			slog.String("error", err.Error()))
		return fail(CodeInternal, "workspace %q: %s failed", ws, phase)
	}

	// refused reports the workspace gate's error.
	refused := func(err error) *syncplanev1.WorkspaceResults {
		if errors.Is(err, ErrNoAccess) {
			return fail(CodeWorkspaceNoAccess, "workspace %q: the caller holds no permission", ws)
		}
		if _, denied := AsWorkspaceDenial(err); denied {
			return fail(CodeWorkspaceAccessRevoked, "workspace %q: access denied", ws)
		}
		// Could not decide (a hook outage). Transient: the client retries.
		return failInternal("resolving access", err)
	}

	adm, err := s.enterWorkspaceServed(ctx, ws, true)
	principal := adm.principal
	if err != nil {
		return refused(err)
	}

	// Authorize every mutation BEFORE opening the write transaction. CanMutate
	// is a pure function of (principal, workspaceID, Write) — it is handed no
	// tx — so it provides no atomicity with the write and can run outside the
	// lock. Doing it here means a slow hook never holds the workspace write lock,
	// and the hook runs exactly once per push even when the storage adapter
	// retries the write closure below.
	// A non-rejection (infra) error fails this workspace before any lock is
	// taken; a rejection is stored and replayed inside the tx to produce the
	// REJECTED ack and watermark bump.
	// The gate resolves FieldPermissions and the row check once per entity and
	// is read again by the apply below, so the hooks run out here with the rest
	// of authorization rather than inside the transaction where a write needs
	// their answers.
	gate := s.newWriteGate(ctx, adm, ws, true)
	muts := batch.GetMutations()
	authz := make([]error, len(muts))
	for i, m := range muts {
		if err := s.authorize(ctx, principal, ws, gate, m); err != nil {
			if _, isRejection := AsRejection(err); !isRejection {
				if gerr := adm.verdict.refusal(ctx, err); gerr != nil {
					return refused(gerr)
				}
				return failInternal(fmt.Sprintf("authorizing mutation %d", m.GetMutationId()), err)
			}
			authz[i] = err
		}
	}

	var (
		results []*syncplanev1.MutationResult
		applied bool
		// gap and badArg carry a batch-level verdict out of the write closure.
		// They are deterministic, so they are reported as codes rather than
		// retried; the closure returns nil for them so the adapter does not
		// treat them as infrastructure failures.
		gap    string
		badArg string
	)
	// adopted records returning clients re-admitted by watermark adoption (see
	// the last == 0 branch). Captured here and logged after the tx so a closure
	// retry does not double-log; reset at the top of the closure for the same
	// reason results is.
	type adoption struct {
		client     string
		mutationID uint64
	}
	var adopted []adoption
	// Which mutation may read which row from a shared fetch; a pure function of
	// the wire batch, so it survives a closure retry unchanged.
	owners := batchRowOwners(muts)
	// Whether mutations apply inside savepoints. Read by the closure, never
	// written by it: the storage adapter may invoke the closure again on its own
	// (see the storage.Adapter contract), and such a retry must repeat the same
	// attempt rather than switch mode underneath itself.
	guarded := false
	apply := func(tx storage.WriteTx) error {
		// The adapter may run this closure more than once (see the
		// storage.Adapter contract): reset everything captured from outside
		// so a retried attempt does not duplicate results.
		results = nil
		applied = false
		gap, badArg = "", ""
		adopted = adopted[:0]
		// Watermarks per client, by watermarkKey, loaded lazily and tracked as
		// we bump them. bumped names the clients whose watermark this batch
		// moved, which is what the single write per client after the loop is
		// driven by.
		watermarks := map[string]uint64{}
		bumped := map[string]bool{}
		// Recorded verdicts per client, loaded lazily by rejectionsOf and marked
		// dirty when this batch adds one. Reset with the rest: a retried attempt
		// must re-read rather than build on what an abandoned one collected.
		verdicts := map[string][]storage.ClientRejection{}
		dirty := map[string]bool{}
		// Fresh per attempt: a retried closure must re-read rather than serve
		// what an abandoned one fetched.
		rows := &batchRows{tx: tx, owners: owners, fetched: map[string]map[string]storage.Row{}}
		if err := gate.accessOf().begin(tx); err != nil {
			return err
		}
		rejectionsOf := func(key string) ([]storage.ClientRejection, error) {
			if recs, ok := verdicts[key]; ok {
				return recs, nil
			}
			recs, err := tx.ClientRejections(key)
			if err != nil {
				return nil, err
			}
			verdicts[key] = recs
			return recs, nil
		}
		for i, m := range muts {
			// The wire id is a uint64, but stores hold watermarks as signed
			// 64-bit integers: an id above that is as malformed as 0, and
			// reaching the store with it would fail as an internal error the
			// client retries.
			if m.GetClientId() == "" || m.GetMutationId() == 0 || m.GetMutationId() > math.MaxInt64 {
				badArg = "mutation needs client_id and a mutation_id from 1 to 9223372036854775807"
				results = nil
				return errAbortBatch
			}
			if err := checkClientID(m.GetClientId()); err != nil {
				badArg = err.Error()
				results = nil
				return errAbortBatch
			}
			key := watermarkKey(principal, m.GetClientId())
			last, ok := watermarks[key]
			if !ok {
				var err error
				last, err = tx.ClientLastMutationID(key)
				if err != nil {
					return err
				}
				watermarks[key] = last
			}

			switch {
			case m.GetMutationId() <= last:
				// The watermark already covers this id, so nothing is applied
				// either way — but if it was covered by a REJECTION, the client
				// is resending precisely because it never learned that. Replay
				// the verdict rather than the bare ack that would make the edit
				// disappear unexplained.
				recs, err := rejectionsOf(key)
				if err != nil {
					return err
				}
				result := &syncplanev1.MutationResult{
					MutationId: m.GetMutationId(),
					Status:     syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED,
				}
				if r, ok := findRejection(recs, m.GetMutationId()); ok {
					result.Status = syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED
					result.ErrorCode = r.Code
					result.ErrorMessage = r.Message
				}
				results = append(results, result)
				continue
			case last == 0:
				// Watermark adoption: 0 means either a brand-new client
				// (mutation_id 1, the ordinary case) or a returning client
				// whose watermark record was deleted — which only
				// WithClientRetention does. Adopt mutation_id-1 and apply.
				// Nothing here can tell such a client's fresh work from a
				// resend of mutations it already had applied; that hazard, and
				// the buggy NEW client starting above 1 that is accepted
				// silently, are documented on WithClientRetention.
				if m.GetMutationId() > 1 {
					// A first-seen mutation_id above 1 is the noteworthy case: a
					// returning pruned client (or, less benignly, a buggy one).
					adopted = append(adopted, adoption{client: m.GetClientId(), mutationID: m.GetMutationId()})
				}
			case m.GetMutationId() != last+1:
				// Nothing in this batch applies: the client must resend from
				// its watermark, in order. Roll the whole closure back by
				// dropping the results collected so far.
				gap = fmt.Sprintf("mutation id gap: got %d, want %d", m.GetMutationId(), last+1)
				results = nil
				return errAbortBatch
			}

			var seq uint64
			var applyErr error
			rows.at = i
			if guarded {
				applyErr = tx.InSavepoint(func() error {
					var err error
					seq, err = s.applyMutation(ctx, ws, tx, rows, gate, m, authz[i])
					return err
				})
			} else {
				seq, applyErr = s.applyMutation(ctx, ws, tx, rows, gate, m, authz[i])
			}

			var result *syncplanev1.MutationResult
			switch r, isRejection := AsRejection(applyErr); {
			case applyErr == nil:
				applied = true
				result = &syncplanev1.MutationResult{
					MutationId: m.GetMutationId(),
					Status:     syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED,
					Seq:        seq,
				}
			case isRejection:
				if !guarded {
					// Nothing rewound the rejected mutation's partial writes, so
					// this attempt cannot go on; the caller rolls it back and runs
					// the batch again under savepoints.
					return errNeedsSavepoints
				}
				// The savepoint already rolled the mutation's writes back, and
				// what the caller holds may have been read after them; a
				// rejected mutation consumes no sequence.
				gate.accessOf().invalidate()
				result = &syncplanev1.MutationResult{
					MutationId:   m.GetMutationId(),
					Status:       syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED,
					ErrorCode:    r.Code,
					ErrorMessage: r.Message,
				}
				// Record the verdict so a resend of this id — which is what the
				// client does when it never sees this response — is answered
				// with it. Outside the savepoint, like the watermark bump it
				// accompanies.
				recs, err := rejectionsOf(key)
				if err != nil {
					return err
				}
				verdicts[key] = append(recs, storage.ClientRejection{
					MutationID: m.GetMutationId(),
					Code:       r.Code,
					Message:    r.Message,
				})
				dirty[key] = true
			default:
				return applyErr
			}

			// Rejection is an ack: the watermark advances either way. Tracked in
			// memory here and written after the loop — the batch is one
			// transaction, so the only value ever read back is the highest id
			// each client reached, and a write per mutation would rewrite one
			// row per mutation to get there.
			watermarks[key] = m.GetMutationId()
			bumped[key] = true
			results = append(results, result)
		}
		// One write per client, after the loop and so outside every savepoint:
		// the bump has to survive a rejected mutation's rollback.
		for key := range bumped {
			if err := tx.SetClientLastMutationID(key, watermarks[key]); err != nil {
				return err
			}
		}
		// One write per client, after the loop: a batch that rejects several of
		// one client's mutations records them together.
		for key := range dirty {
			if err := tx.SetClientRejections(key, trimRejections(verdicts[key])); err != nil {
				return err
			}
		}
		return nil
	}
	// A savepoint pair is two statements per mutation, and on a networked
	// adapter every statement is paid for whether or not the database spends any
	// time on it — for a 256-mutation batch the pairs are half of everything the
	// transaction sends. Rejections are rare (a CAS conflict, a refused write, an
	// oversized value), so the batch runs unguarded first and pays for the
	// rewind machinery only in the batches that turn out to need it: the whole
	// attempt is discarded and replayed with every mutation in its own savepoint.
	// Replaying is safe because the closure rebuilds everything it reports from
	// the transaction it is handed.
	err = s.store.WithWriteTx(ctx, ws, apply)
	if errors.Is(err, errNeedsSavepoints) {
		guarded = true
		s.tel().pushSavepointReruns.Add(ctx, 1)
		err = s.store.WithWriteTx(ctx, ws, apply)
	}
	if gerr := adm.verdict.refusal(ctx, err); gerr != nil {
		return refused(gerr)
	}
	switch {
	case gap != "":
		// errAbortBatch only exists to roll the transaction back; the verdict
		// itself is in gap.
		return fail(CodeMutationIDGap, "workspace %q: %s", ws, gap)
	case badArg != "":
		return fail(CodeInvalidArgument, "workspace %q: %s", ws, badArg)
	case err != nil:
		return failInternal("applying the batch", err)
	}
	if applied {
		s.notifier.Notify(ws)
	}
	out.Results = results
	s.recordMutationResults(ctx, muts, results)

	// Post-tx logging (never inside the closure: it may be retried). Rejections
	// are acks, not RPC errors, so they are otherwise invisible — a flood of
	// them signals a client bug, schema skew or abuse. Results align 1:1 with
	// the submitted mutations by index (a gap drops every result, so no partial
	// misalignment). Both at Debug: routine, and an attacker must not be able to
	// flood logs above it.
	plog := s.reqLog(ctx, "push", ws, "")
	for i, r := range results {
		if r.GetStatus() != syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED {
			continue
		}
		plog.LogAttrs(ctx, slog.LevelDebug, "mutation rejected",
			slog.String("client", muts[i].GetClientId()),
			slog.Uint64("mutation_id", r.GetMutationId()),
			slog.String("target", mutationLabel(muts[i])),
			slog.String("code", r.GetErrorCode()),
			slog.String("message", r.GetErrorMessage()),
		)
	}
	for _, a := range adopted {
		plog.LogAttrs(ctx, slog.LevelDebug, "client watermark adopted",
			slog.String("client", a.client),
			slog.Uint64("mutation_id", a.mutationID),
		)
	}
	return out
}

// maxRejectionRecordBytes bounds the code and message text one client's
// recorded verdicts may cost, because both are written by the rejecting hook
// and a client can provoke as many rejections per push as the batch limit
// allows. Verdicts past the bound are dropped oldest-first and their ids fall
// back to a bare ALREADY_APPLIED ack — the behaviour of a client that has
// pushed tens of thousands of refused mutations without ever reading a
// response, which no working client does.
const maxRejectionRecordBytes = 64 << 10

// findRejection looks a mutation id up among a client's recorded verdicts.
func findRejection(recs []storage.ClientRejection, mutationID uint64) (storage.ClientRejection, bool) {
	for _, r := range recs {
		if r.MutationID == mutationID {
			return r, true
		}
	}
	return storage.ClientRejection{}, false
}

// trimRejections drops the oldest verdicts from an ascending list until it fits
// maxRejectionRecordBytes, keeping the newest one whatever it costs: a verdict
// too large to record at all is the only case where a resend would learn
// nothing, and it is the case where the client is still waiting for an answer.
func trimRejections(recs []storage.ClientRejection) []storage.ClientRejection {
	budget := maxRejectionRecordBytes
	for i := len(recs) - 1; i >= 0; i-- {
		budget -= len(recs[i].Code) + len(recs[i].Message)
		if budget < 0 {
			return recs[min(i+1, len(recs)-1):]
		}
	}
	return recs
}

// recordMutationResults accounts one batch's per-mutation outcomes. This is
// the signal the RPC layer structurally cannot report: a rejected mutation is
// an ack, so a push whose every mutation was thrown away is still a 200 OK,
// and a client wedged in a rebase loop looks exactly like a healthy one from
// the outside.
//
// results align 1:1 with muts by index — a gap drops every result, so there is
// no partial misalignment to guard against.
func (s *Server) recordMutationResults(ctx context.Context, muts []*syncplanev1.Mutation, results []*syncplanev1.MutationResult) {
	cat := s.catalogPtr()
	for i, r := range results {
		target := s.mutationAttrs(cat, muts[i])
		switch r.GetStatus() {
		case syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED:
			s.tel().pushMutations.Add(ctx, 1, metric.WithAttributes(
				append(target, telemetry.AttrOutcome.String(telemetry.OutcomeOK))...))
		case syncplanev1.MutationStatus_MUTATION_STATUS_ALREADY_APPLIED:
			// Idempotent replay: the watermark had already passed this one.
			// Counted because a rising share of it means clients are resending
			// — a queue that is not draining, or acks that are not landing.
			s.tel().pushMutations.Add(ctx, 1, metric.WithAttributes(
				append(target, telemetry.AttrOutcome.String(telemetry.OutcomeSkipped))...))
		case syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED:
			s.tel().pushRejections.Add(ctx, 1, metric.WithAttributes(
				append(target, telemetry.AttrRejectionCode.String(r.GetErrorCode()))...))
		}
	}
}

// mutationAttrs describes what a mutation targeted, for metrics.
//
// Entity and mutator names arrive from the wire, so both are resolved against
// the catalog and the mutator table before being recorded: an unregistered
// name is reported as telemetry.Unregistered rather than echoed back. Without
// that, any client could mint unbounded metric streams by pushing mutations
// naming entities that do not exist — the requests would all be rejected, and
// the metrics pipeline would fall over anyway.
//
// A fresh slice per call: the caller appends to it.
func (s *Server) mutationAttrs(cat *catalog, m *syncplanev1.Mutation) []attribute.KeyValue {
	switch payload := m.GetPayload().(type) {
	case *syncplanev1.Mutation_Crud:
		name := payload.Crud.GetEntity()
		if _, ok := cat.entities[name]; !ok {
			name = telemetry.Unregistered
		}
		return []attribute.KeyValue{
			telemetry.AttrMutationKind.String("crud"),
			telemetry.AttrEntity.String(name),
		}
	case *syncplanev1.Mutation_Custom:
		name := payload.Custom.GetName()
		if _, ok := s.mutators[name]; !ok {
			name = telemetry.Unregistered
		}
		return []attribute.KeyValue{
			telemetry.AttrMutationKind.String("custom"),
			telemetry.AttrMutator.String(name),
		}
	case *syncplanev1.Mutation_Batch:
		// A batch's members target whatever they target; the batch itself
		// targets nothing, and flattening it here would double-count against
		// the members the mutator applies.
		return []attribute.KeyValue{telemetry.AttrMutationKind.String("batch")}
	default:
		return []attribute.KeyValue{telemetry.AttrMutationKind.String(telemetry.Unregistered)}
	}
}

// batchRows serves applyCrud's base-row read out of ONE storage read per entity
// for a whole push batch, where a point read per mutation would be a round trip
// per mutation.
//
// A mutation reads from it only when it is the FIRST in the batch to target its
// row. The batch applies in order inside one transaction, so whatever an
// earlier mutation wrote is part of the base a later one must merge onto, and a
// copy taken before the batch would be exactly the stale row that breaks. Every
// other read falls through to the transaction, which sees the batch's own
// writes. Nothing else invalidates the cache and nothing needs to: a row it
// holds is handed to one mutation that runs before any write to that row.
//
// The fetch is lazy per entity, so a batch whose mutations never look at their
// base row (an unguarded PUT of an entity with no text fields and no history)
// pays for no read at all.
type batchRows struct {
	tx storage.WriteTx
	// owners maps a row to the index of the mutation that may read it from the
	// shared fetch. Rows nothing may share are absent.
	owners map[rowRef]int
	// fetched holds one entity's read keyed by row key. An entity present here
	// has been read; a key missing from its map has no stored row.
	fetched map[string]map[string]storage.Row
	// at is the index of the mutation currently applying.
	at int
}

// batchRowOwners picks, for each row a batch targets, the mutation that may
// read its base from a shared fetch: the first one to name it, and only while
// every preceding mutation's writes are still known from the wire. A custom
// mutation writes whatever its mutator decides, so nothing past one qualifies.
// Sub-mutations of a batch mutation write rows without being able to read one
// back, so they only disqualify later readers.
func batchRowOwners(muts []*syncplanev1.Mutation) map[rowRef]int {
	owners := make(map[rowRef]int, len(muts))
	seen := make(map[rowRef]bool, len(muts))
	for i, m := range muts {
		switch payload := m.GetPayload().(type) {
		case *syncplanev1.Mutation_Crud:
			ref := rowRef{entity: payload.Crud.GetEntity(), key: payload.Crud.GetKey()}
			if !seen[ref] {
				owners[ref] = i
			}
			seen[ref] = true
		case *syncplanev1.Mutation_Batch:
			for _, crud := range payload.Batch.GetMutations() {
				seen[rowRef{entity: crud.GetEntity(), key: crud.GetKey()}] = true
			}
		default:
			// A custom mutation, or a payload this build does not know: either
			// way what it writes cannot be read off the wire, so no row after
			// it can be claimed.
			return owners
		}
	}
	return owners
}

// get reads one row's stored state, through the shared fetch where the current
// mutation owns it. It reports an absent row as storage.ErrNotFound, exactly as
// ReadTx.GetRow does.
func (b *batchRows) get(entity, key string) (storage.Row, error) {
	ref := rowRef{entity: entity, key: key}
	if owner, ok := b.owners[ref]; !ok || owner != b.at {
		return b.tx.GetRow(entity, key)
	}
	byKey, fetched := b.fetched[entity]
	if !fetched {
		// Only the rows still ahead in the batch: one an earlier mutation owned
		// has had its chance, and may since have been written.
		keys := make([]string, 0, len(b.owners))
		for r, owner := range b.owners {
			if r.entity == entity && owner >= b.at {
				keys = append(keys, r.key)
			}
		}
		byKey = make(map[string]storage.Row, len(keys))
		if err := b.tx.GetRows(entity, keys, func(r storage.Row) error {
			byKey[r.Key] = r
			return nil
		}); err != nil {
			return storage.Row{}, err
		}
		b.fetched[entity] = byKey
	}
	row, ok := byKey[key]
	if !ok {
		return storage.Row{}, storage.ErrNotFound
	}
	return row, nil
}

// errAbortBatch rolls a push's write transaction back when the batch is
// rejected as a whole (a mutation-id gap). It never escapes pushWorkspace: the
// caller reads the verdict from the captured reason instead, so the adapter
// sees an ordinary closure failure and the client sees a typed code.
var errAbortBatch = errors.New("push batch aborted")

// errNeedsSavepoints rolls a push's first, unguarded write attempt back when a
// mutation is rejected: without a savepoint around it nothing rewound the rows
// it wrote before failing. It never escapes pushWorkspace — the batch is run
// again with every mutation guarded, and that attempt decides the outcome.
var errNeedsSavepoints = errors.New("push batch needs savepoints")

// mutationLabel names a mutation's target for logging: the entity for CRUD, the
// mutator name for a custom mutation, or a batch summary. Best-effort and
// side-effect free.
func mutationLabel(m *syncplanev1.Mutation) string {
	switch payload := m.GetPayload().(type) {
	case *syncplanev1.Mutation_Crud:
		return payload.Crud.GetEntity()
	case *syncplanev1.Mutation_Custom:
		return "mutator:" + payload.Custom.GetName()
	case *syncplanev1.Mutation_Batch:
		return fmt.Sprintf("batch[%d]", len(payload.Batch.GetMutations()))
	default:
		return ""
	}
}

// validatePush enforces request-level shape and Limits before any transaction
// opens. These are properties of the REQUEST, not of any workspace or mutation,
// so they stay RPC errors: a malformed or oversized push is the client's
// batching bug to fix (chunk it), and reporting it per workspace would imply
// the rest of the request was processed. Per-mutation limits are checked inside
// the batch as rejections (see checkLimits) so one oversized mutation cannot
// wedge the whole queue.
//
// The mutation cap is a TOTAL across batches: it bounds the work one request
// can ask for, which is the resource the limit exists to protect. A client
// splitting a large offline burst must therefore count across workspaces.
func (s *Server) validatePush(batches []*syncplanev1.WorkspaceBatch) error {
	if len(batches) == 0 {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("push needs at least one batch"))
	}
	if len(batches) > s.limits.MaxWorkspacesPerRequest {
		return connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("push spans %d workspaces", len(batches)),
			"MaxWorkspacesPerRequest", s.limits.MaxWorkspacesPerRequest)))
	}
	total := 0
	seen := make(map[string]struct{}, len(batches))
	for _, b := range batches {
		if err := naming.ValidateWorkspaceID(b.GetWorkspaceId()); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("batch: %w", err))
		}
		// Two batches for one workspace would race each other's watermarks and
		// make the per-workspace result ambiguous.
		if _, dup := seen[b.GetWorkspaceId()]; dup {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("workspace %q appears in more than one batch", b.GetWorkspaceId()))
		}
		seen[b.GetWorkspaceId()] = struct{}{}
		if len(b.GetMutations()) == 0 {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("workspace %q: batch has no mutations", b.GetWorkspaceId()))
		}
		total += len(b.GetMutations())
	}
	if total > s.limits.MaxMutationsPerPush {
		return connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("push of %d mutations", total),
			"MaxMutationsPerPush", s.limits.MaxMutationsPerPush)))
	}
	return nil
}

// checkLimits returns a Rejection when a mutation violates a per-mutation size
// limit. These failures are deterministic — the identical mutation fails every
// retry — so they must surface as rejections (acks the client drops) rather
// than RPC errors that abort the batch and wedge the queue forever (e.g. after
// a server tightens a limit a client already exceeded).
func (s *Server) checkLimits(m *syncplanev1.Mutation) error {
	switch payload := m.GetPayload().(type) {
	case *syncplanev1.Mutation_Crud:
		return s.checkCrudLimits(payload.Crud)
	case *syncplanev1.Mutation_Custom:
		if n := len(payload.Custom.GetArgs().GetValue()); n > s.limits.MaxValueBytes {
			return Reject(CodeInvalidArgument,
				fmt.Sprintf("args of %d bytes exceed the limit of %d", n, s.limits.MaxValueBytes))
		}
	case *syncplanev1.Mutation_Batch:
		// A batch is one envelope but many writes; cap its sub-mutation count
		// by the same per-push budget so a single batch cannot outweigh a
		// whole push, and size-check each sub-mutation. Both are deterministic,
		// so they reject (the client drops the batch) rather than wedge.
		subs := payload.Batch.GetMutations()
		if len(subs) == 0 {
			return Reject(CodeInvalidArgument, "batch has no mutations")
		}
		if len(subs) > s.limits.MaxMutationsPerPush {
			return Reject(CodeInvalidArgument, limitExceeded(
				fmt.Sprintf("batch of %d mutations", len(subs)),
				"MaxMutationsPerPush", s.limits.MaxMutationsPerPush))
		}
		for _, crud := range subs {
			if err := s.checkCrudLimits(crud); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) checkCrudLimits(crud *syncplanev1.CrudMutation) error {
	if n := len(crud.GetValue()); n > s.limits.MaxValueBytes {
		return Reject(CodeInvalidArgument,
			fmt.Sprintf("value of %d bytes exceeds the limit of %d", n, s.limits.MaxValueBytes))
	}
	if n := len(crud.GetFieldMask().GetPaths()); n > s.limits.MaxFieldMaskPaths {
		return Reject(CodeInvalidArgument,
			fmt.Sprintf("%d field-mask paths exceed the limit of %d", n, s.limits.MaxFieldMaskPaths))
	}
	return nil
}

// applyMutation applies one mutation of a push batch and returns the sequence
// it was assigned. Deterministic validation failures (bad payload,
// unknown entity/mutator/field, oversized value) surface as *Rejection errors
// so the caller acks and rolls them back instead of wedging the queue; only
// genuine infrastructure errors abort the batch.
//
// authErr carries the precomputed CanMutate verdict for this mutation (see
// Push): the hook already ran outside the tx, so a non-nil authErr is replayed
// here — a Rejection rolls the mutation back and acks, a non-rejection aborts
// the batch. checkLimits (cheap, deterministic) still runs first.
//
// g is the same gate authorization warmed, carried in for the CRUD writes. It
// does NOT reach a custom mutation: a mutator body writes under the verdict on
// its invocation, which is the escape hatch a protected field and a row check
// are designed around.
//
// rows serves the CRUD base-row reads; it must be positioned on this mutation's
// index (see batchRows).
func (s *Server) applyMutation(
	ctx context.Context,
	ws string,
	tx storage.WriteTx,
	rows *batchRows,
	g *writeGate,
	m *syncplanev1.Mutation,
	authErr error,
) (uint64, error) {
	if err := s.checkLimits(m); err != nil {
		return 0, err
	}
	if authErr != nil {
		return 0, authErr
	}

	seq, err := tx.NextSeq()
	if err != nil {
		return 0, err
	}

	// Who this mutation is attributed to: the authenticated caller that pushed
	// it. Resolved once per mutation, so every row a batch or a mutator writes
	// carries the same actor as the mutation itself.
	actor, err := actorOf(ctx)
	if err != nil {
		return 0, err
	}

	switch payload := m.GetPayload().(type) {
	case *syncplanev1.Mutation_Crud:
		w := Write{ClientId: m.GetClientId(), MutationId: m.GetMutationId(), Crud: payload.Crud}
		if err := s.applyCrud(ctx, ws, tx, rows, g, w, seq, actor); err != nil {
			return 0, err
		}
	case *syncplanev1.Mutation_Custom:
		if err := s.applyCustom(ctx, ws, tx, g, payload.Custom, seq, actor); err != nil {
			return 0, err
		}
	case *syncplanev1.Mutation_Batch:
		// All sub-mutations share this one sequence and are rewound together by
		// the caller, so the batch is atomic: the first rejection aborts the
		// whole mutation and every row it wrote rolls back. Applying in
		// order lets a later sub-mutation observe earlier ones (e.g. PATCH the
		// row a preceding PUT created). An empty batch is rejected in
		// checkLimits, above.
		for _, crud := range payload.Batch.GetMutations() {
			w := Write{ClientId: m.GetClientId(), MutationId: m.GetMutationId(), Crud: crud}
			if err := s.applyCrud(ctx, ws, tx, rows, g, w, seq, actor); err != nil {
				return 0, err
			}
		}
	default:
		return 0, Reject(CodeInvalidArgument, fmt.Sprintf("mutation %d has no payload", m.GetMutationId()))
	}
	return seq, nil
}

// authorize flattens the envelope into Writes and runs the permission hook on
// each: a batch authorizes once per sub-mutation, so policies never unwrap one
// themselves. It runs before NextSeq and apply, so rejecting any Write leaves
// the whole envelope unapplied (and a batch rolls back atomically). A
// payload-less mutation authorizes vacuously; applyMutation rejects it below.
func (s *Server) authorize(
	ctx context.Context,
	p Principal,
	workspaceID string,
	g *writeGate,
	m *syncplanev1.Mutation,
) error {
	switch payload := m.GetPayload().(type) {
	case *syncplanev1.Mutation_Crud:
		return s.authorizeWrite(ctx, p, workspaceID, g, Write{
			ClientId: m.GetClientId(), MutationId: m.GetMutationId(), Crud: payload.Crud,
		})
	case *syncplanev1.Mutation_Custom:
		return s.authorizeWrite(ctx, p, workspaceID, g, Write{
			ClientId: m.GetClientId(), MutationId: m.GetMutationId(), Custom: payload.Custom,
		})
	case *syncplanev1.Mutation_Batch:
		for _, crud := range payload.Batch.GetMutations() {
			if err := s.authorizeWrite(ctx, p, workspaceID, g, Write{
				ClientId: m.GetClientId(), MutationId: m.GetMutationId(), Crud: crud,
			}); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// applyCrud applies one CRUD write. w is the write as authorized, carrying its
// envelope's ClientId and MutationId so the row check is handed what CanMutate
// was.
func (s *Server) applyCrud(
	ctx context.Context,
	ws string,
	tx storage.WriteTx,
	rows *batchRows,
	g *writeGate,
	w Write,
	seq uint64,
	actor storage.Actor,
) error {
	crud := w.Crud
	info, ok := s.catalogPtr().entities[crud.GetEntity()]
	if !ok {
		return Reject(CodeInvalidArgument, fmt.Sprintf("unknown entity %q", crud.GetEntity()))
	}
	// A server-only entity's rows never reach the client that would be writing
	// them, so a wire write of one could only ever be a misconfiguration — an
	// optimistic edit whose row the client can neither read back nor rebase
	// against. Rejecting is deterministic, so the client acks and drains rather
	// than wedging its queue. In-process writes are unaffected: EntityTx is how
	// the engine's own history capture writes these rows.
	if info.serverOnly {
		return Reject(CodeInvalidArgument,
			fmt.Sprintf("entity %q is server-only and cannot be written from a client", info.name))
	}
	// Authorization ran before the write lock, and passed every write to an
	// entity it did not know: applying it is what refuses an unknown entity.
	// An entity registered while the push waited for the lock is known here, so
	// its schema rules and protected fields are checked again against the info
	// this write applies with. The checks are pure, and the gate already holds
	// the protected fields of every entity authorization knew, so they cost
	// nothing worth skipping when the catalog did not change.
	if g != nil {
		if err := writePolicyOf(info, crud); err != nil {
			return err
		}
		protected, err := g.protected(info)
		if err != nil {
			return err
		}
		if err := refuseProtectedMask(info, crud, protected); err != nil {
			return err
		}
	}
	// The key addresses a row in storage and, on the client, a KV key built by
	// pasting it after the entity prefix; a key breaking the shared naming rules
	// is a client bug that would otherwise be discovered as a missing or
	// colliding row.
	if err := info.checkKey(crud.GetKey()); err != nil {
		return err
	}

	// The stored row as it stands before this mutation, fetched at most once.
	// Five sites inside this one mutation want it — the row check and CAS guard
	// below, the PUT text union, the PATCH merge base and history capture — and
	// each has to treat an absent row and a tombstone alike; one fetch behind
	// one `live` flag is what keeps that handling from drifting apart between
	// them. Nothing here writes the row before the op switch's final
	// PutRow/DeleteRow, so the memo cannot go stale. Lazy, so a DELETE (or an
	// unguarded PUT of an entity that has neither text fields nor history)
	// still reads nothing unless the entity has a row check.
	var (
		cachedRow  storage.Row
		cachedLive bool
		fetched    bool
	)
	currentRow := func() (storage.Row, bool, error) {
		if !fetched {
			row, err := rows.get(info.name, crud.GetKey())
			if err != nil && err != storage.ErrNotFound {
				return storage.Row{}, false, err
			}
			cachedRow, cachedLive, fetched = row, err == nil && !row.Deleted, true
		}
		return cachedRow, cachedLive, nil
	}
	// The stored row decoded, memoized on top of that one fetch. A PUT has two
	// callers for it — the protected-field carry-over and the text union — and
	// both read "no base" the same way: an absent row, a tombstone, and a row
	// that will not decode (poison, reported once here) are alike, because a PUT
	// replaces whole rows and so is itself the repair that clears a quarantine.
	var (
		cachedBase proto.Message
		decodedRow bool
	)
	currentBase := func() (proto.Message, error) {
		if decodedRow {
			return cachedBase, nil
		}
		decodedRow = true
		row, live, err := currentRow()
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, nil
		}
		base, decErr := info.decode(row.Value)
		if decErr != nil {
			s.reportPoison(ctx, ws, row, decErr)
			return nil, nil
		}
		cachedBase = base
		return base, nil
	}

	// Before the compare-and-swap below, so a caller refused here does not
	// learn the row's version from a conflict.
	if err := g.guardRow(info, crud.GetKey(), currentRow, func() (Write, error) {
		return w, nil
	}); err != nil {
		return err
	}
	aw, err := g.beginAccessWrite(tx, info, crud.GetKey(), crud.GetOp(), crud.GetFieldMask().GetPaths(), currentRow)
	if err != nil {
		return err
	}

	// Optional compare-and-swap: apply only if the row is still at the version
	// the client read. An absent or deleted row reads as version 0 (so
	// expected_version 0 means "row must not exist"); deleted and GC'd-to-absent
	// are treated alike, matching the PATCH delete-wins policy below, so the
	// outcome never depends on tombstone GC timing. The check runs before the
	// op switch so it guards PUT, PATCH and DELETE uniformly.
	if crud.ExpectedVersion != nil {
		row, live, err := currentRow()
		if err != nil {
			return err
		}
		var current uint64
		if live {
			current = row.Version
		}
		if current != crud.GetExpectedVersion() {
			return Reject(CodeConflict,
				fmt.Sprintf("entity %q: expected version %d but row %q is at version %d",
					info.name, crud.GetExpectedVersion(), crud.GetKey(), current))
		}
	}

	switch crud.GetOp() {
	case syncplanev1.CrudOp_CRUD_OP_PUT:
		msg, err := info.decode(crud.GetValue())
		if err != nil {
			return Reject(CodeInvalidArgument, err.Error())
		}
		if derived := info.keyOf(msg.ProtoReflect()); derived != crud.GetKey() {
			return Reject(CodeInvalidArgument,
				fmt.Sprintf("entity %q: key %q does not match key fields (%q)", info.name, crud.GetKey(), derived))
		}
		// A PUT names no field, so a protected one is not refused: it keeps what
		// the row already holds, and the rest of the write lands. Done here,
		// before the text guard, so bytes a client sent in a protected
		// collaborative-text field are gone before anything decodes them.
		protected, err := g.protected(info)
		if err != nil {
			return err
		}
		if len(protected) > 0 {
			base, baseErr := currentBase()
			if baseErr != nil {
				return baseErr
			}
			if err := carryProtectedFields(info, protected, base, msg); err != nil {
				return err
			}
			if base == nil {
				stampCreatorFields(info, msg, g.creatorSubject())
			}
		}
		// The fields the access model keeps from this caller get the same
		// treatment, which depends on the caller and the row.
		if err := aw.carryGuarded(tx, msg, nil); err != nil {
			return err
		}
		var prevText map[string][]byte
		if len(info.textFields) > 0 {
			// Ingress guard: text bytes a client sends must decode as full
			// documents (and are canonicalized), or the row would brick every
			// reader. A PUT carries whole rows, so delta state has no meaning
			// here and is refused along with everything else that does not
			// decode.
			if err := info.canonicalizeTextFields(msg.ProtoReflect(), nil); err != nil {
				return rejectTextWrite(err)
			}
			// PUT replaces whole rows EXCEPT text fields, which union with the
			// stored document — same contract as EntityTx.Put. A PUT built
			// from any client state must not reset the document: a concurrent
			// PATCH would union the old document right back in, duplicating
			// the text instead of honoring the PUT.
			base, baseErr := currentBase()
			if baseErr != nil {
				return baseErr
			}
			if base != nil {
				tw, err := info.textWriteFor(s, ws, tx, crud.GetKey())
				if err != nil {
					return err
				}
				prevText = info.textSnapshot(base)
				if err := info.mergeTextFields(base.ProtoReflect(), msg.ProtoReflect(), tw); err != nil {
					return rejectTextWrite(err)
				}
			}
		}
		// A reference stores only its target's key fields; strip any extra fields
		// a fat stub carried before persisting.
		if err := info.projectRefs(msg.ProtoReflect()); err != nil {
			return err
		}
		// On the row as it will be stored, so the access parent the checks
		// read is the projected one.
		if err := s.checkGrantRow(info, crud.GetKey(), crud.GetOp(), nil, msg); err != nil {
			return err
		}
		if err := aw.checkNewParentsRead(tx, msg); err != nil {
			return err
		}
		if err := s.checkAccessChain(ctx, ws, tx, info, crud.GetKey(), currentRow, msg); err != nil {
			return err
		}
		if err := aw.finish(tx, msg); err != nil {
			return err
		}
		value, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		if err := info.checkTextCounterSteps(prevText, msg.ProtoReflect()); err != nil {
			return err
		}
		if err := info.recordTextDeltas(tx, crud.GetKey(), prevText, msg.ProtoReflect(), seq); err != nil {
			return err
		}
		if len(info.textFields) > 0 {
			base, baseErr := currentBase()
			if baseErr != nil {
				return baseErr
			}
			model := s.catalogPtr().access
			if err := info.recordReadGaps(tx, model, crud.GetKey(), readInputsOf(model, info, base), msg, seq); err != nil {
				return err
			}
		}
		if err := info.dropRemovedTextInstances(tx, s.textCache, ws, crud.GetKey(),
			prevText, msg.ProtoReflect()); err != nil {
			return err
		}
		if err := info.updateRowIndexes(tx, s.limits, crud.GetKey(), msg); err != nil {
			return err
		}
		// Capture last, once every rejection is behind us: a record must
		// describe a write that happened. A PUT carries no mask, so capture
		// starts from the whole capture set (nil mask) and narrows it against
		// `msg`, the row about to be stored; the before-state is the memo above,
		// which the text union read for its own reasons and an entity without
		// text fields pays one read for here.
		if info.history != nil {
			row, live, rowErr := currentRow()
			if rowErr != nil {
				return rowErr
			}
			if err := s.captureHistory(ctx, tx, ws, info, crud.GetKey(), row, live, msg, nil, seq, actor); err != nil {
				return err
			}
		}
		if err := s.recordAccessMove(ctx, ws, tx, info, crud.GetKey(), currentRow, msg, seq, actor); err != nil {
			return err
		}
		if err := tx.PutRow(info.name, crud.GetKey(), value, seq, actor); err != nil {
			return err
		}
		g.accessOf().noteWrite(info)
		return nil

	case syncplanev1.CrudOp_CRUD_OP_PATCH:
		// PATCH modifies an existing row; it is NOT an upsert. A partial patch
		// carries only its masked fields — the key fields travel in crud.key,
		// not the value — so merging onto an empty base would write a
		// half-formed ghost whose key fields don't match its storage key.
		// Policy: delete-wins. A PATCH whose target row is absent or a
		// tombstone is rejected as a conflict, so the client rolls the
		// optimistic edit back and surfaces the loss. (Mirrored in
		// client/src/optimistic/rebase.ts.)
		if len(crud.GetFieldMask().GetPaths()) == 0 {
			return Reject(CodeInvalidArgument,
				fmt.Sprintf("entity %q: PATCH requires a field_mask", info.name))
		}
		src, err := info.decode(crud.GetValue())
		if err != nil {
			return Reject(CodeInvalidArgument, err.Error())
		}
		// Same ingress guard as PUT: junk text bytes must never reach storage,
		// including through a mask whose base side is absent (where the merge
		// below would raw-replace without decoding). A PATCH is the one place
		// delta state is allowed, and only for a field the mask names outright
		// — anywhere else the value would be copied through unmerged.
		maskedText := maskNamesPath(crud.GetFieldMask().GetPaths())
		if err := info.canonicalizeTextFields(src.ProtoReflect(), maskedText); err != nil {
			return rejectTextWrite(err)
		}
		row, live, rowErr := currentRow()
		if rowErr != nil {
			return rowErr
		}
		// Merge onto the live row, or onto an empty base when the row is gone
		// purely to validate the mask paths first: a poison/version-skew mask
		// must reject as invalid_argument before the conflict check below,
		// regardless of whether the row exists.
		base := info.msgType.New().Interface()
		if live {
			if base, err = info.decode(row.Value); err != nil {
				// The stored row is poison (see poison.go): a mask can only be
				// merged onto a base that decodes. Deterministic, so it must
				// REJECT — an error here would abort the batch and wedge the
				// client's queue on a row it cannot fix by retrying. Conflict is
				// the same verdict a PATCH of a missing row gets, for the same
				// reason: the client rolls its edit back and re-reads. A PUT of
				// the key repairs the row.
				s.reportPoison(ctx, ws, row, err)
				return Reject(CodeConflict,
					fmt.Sprintf("entity %q: PATCH of %q targets a row that does not decode", info.name, crud.GetKey()))
			}
		}
		// Snapshot the stored documents before the merge folds the patch into
		// them; the delta log below describes the move from one to the other.
		prevText := info.textSnapshot(base)
		model := s.catalogPtr().access
		prevRead := readInputsOf(model, info, base)
		mergers := info.mergers
		if live && len(info.textFields) > 0 {
			tw, err := info.textWriteFor(s, ws, tx, crud.GetKey())
			if err != nil {
				return err
			}
			mergers = info.mergersFor(tw)
		}
		if err := lww.MergeWithMergers(base, src, crud.GetFieldMask().GetPaths(), mergers); err != nil {
			return rejectTextWrite(err)
		}
		if !live {
			// Absent and deleted are treated alike: a GC'd tombstone reads as
			// absent (ErrNotFound), so the outcome must not depend on GC timing.
			return Reject(CodeConflict,
				fmt.Sprintf("entity %q: PATCH of %q targets a deleted or missing row", info.name, crud.GetKey()))
		}
		// A row's identity lives in both its storage key and its embedded key
		// fields; the two must agree. A mask may name a key field, so the merge
		// above can drift the embedded identity away from crud.key. Re-derive
		// from the merged result and reject any change — mirrors the PUT check.
		if derived := info.keyOf(base.ProtoReflect()); derived != crud.GetKey() {
			return Reject(CodeInvalidArgument,
				fmt.Sprintf("entity %q: PATCH would change key fields of %q (to %q)", info.name, crud.GetKey(), derived))
		}
		// Project refs on the MERGED result, so a fat stub in the PATCH value
		// cannot smuggle extra fields past the per-item merge.
		if err := info.projectRefs(base.ProtoReflect()); err != nil {
			return err
		}
		if err := s.checkGrantRow(info, crud.GetKey(), crud.GetOp(), crud.GetFieldMask().GetPaths(), base); err != nil {
			return err
		}
		if err := aw.checkNewParentsRead(tx, base); err != nil {
			return err
		}
		if err := s.checkAccessChain(ctx, ws, tx, info, crud.GetKey(), currentRow, base); err != nil {
			return err
		}
		if err := aw.finish(tx, base); err != nil {
			return err
		}
		value, err := proto.Marshal(base)
		if err != nil {
			return err
		}
		if err := info.checkTextCounterSteps(prevText, base.ProtoReflect()); err != nil {
			return err
		}
		if err := info.recordTextDeltas(tx, crud.GetKey(), prevText, base.ProtoReflect(), seq); err != nil {
			return err
		}
		if err := info.recordReadGaps(tx, model, crud.GetKey(), prevRead, base, seq); err != nil {
			return err
		}
		if err := info.dropRemovedTextInstances(tx, s.textCache, ws, crud.GetKey(),
			prevText, base.ProtoReflect()); err != nil {
			return err
		}
		// Only when the mask can reach one: the entries are derived from the row,
		// so a mask that changed nothing an index reads produces the set already
		// stored, and replacing it would be write amplification for nothing.
		if info.maskTouchesIndexes(crud.GetFieldMask().GetPaths()) {
			if err := info.updateRowIndexes(tx, s.limits, crud.GetKey(), base); err != nil {
				return err
			}
		}
		// `row` is the memo the merge base came from, still holding the encoded
		// before-state: the merge above rewrote `base` in place, so the bytes
		// are the only copy of it left, and `base` is now the after-state the
		// capture diffs against. The mask is the request's, which for a wire
		// PATCH is already the effective one — nothing adds paths on this path
		// the way EntityTx's text options do.
		if info.history != nil {
			if err := s.captureHistory(ctx, tx, ws, info, crud.GetKey(), row, live, base,
				crud.GetFieldMask().GetPaths(), seq, actor); err != nil {
				return err
			}
		}
		if err := s.recordAccessMove(ctx, ws, tx, info, crud.GetKey(), currentRow, base, seq, actor); err != nil {
			return err
		}
		if err := tx.PutRow(info.name, crud.GetKey(), value, seq, actor); err != nil {
			return err
		}
		g.accessOf().noteWrite(info)
		return nil

	case syncplanev1.CrudOp_CRUD_OP_DELETE:
		// Per-document side state is keyed by instance path, and the nested
		// paths a row holds can only be named by looking at it — so a delete of
		// an entity with element-borne text decodes the row it removes. An
		// undecodable row falls back to the fixed paths (nothing nested could
		// have been written through the guarded write paths for it anyway).
		textPaths := info.textStatePaths(nil)
		if info.hasNestedText() {
			base, baseErr := currentBase()
			if baseErr != nil {
				return baseErr
			}
			if base != nil {
				textPaths = info.textStatePaths(base.ProtoReflect())
			}
		}
		if err := info.recordTextDeltaGaps(tx, crud.GetKey(), textPaths, seq); err != nil {
			return err
		}
		s.textCache.forget(ws, info.name, crud.GetKey(), textPaths)
		if err := info.clearTextFloors(tx, crud.GetKey(), textPaths); err != nil {
			return err
		}
		if err := info.clearRowIndexes(tx, crud.GetKey()); err != nil {
			return err
		}
		// The row's history goes with the row: nothing can restore into a
		// tombstone, and a row later re-created under this key must not inherit
		// the old one's log. See Server.purgeHistory.
		if err := s.purgeHistory(tx, info, crud.GetKey()); err != nil {
			return err
		}
		if err := s.recordAccessMove(ctx, ws, tx, info, crud.GetKey(), currentRow, nil, seq, actor); err != nil {
			return err
		}
		if err := tx.DeleteRow(info.name, crud.GetKey(), seq, actor); err != nil {
			return err
		}
		g.accessOf().noteWrite(info)
		return nil

	default:
		return Reject(CodeInvalidArgument, fmt.Sprintf("entity %q: unknown op", info.name))
	}
}

// rejectTextWrite turns a write failure into a rejection, separating the two
// text-specific failures from every other invalid payload. A fragment that does
// not fit is retryable by sending the same edit as a full document; state that
// predates a compaction floor is not retryable at all until the sender re-reads
// the row. Everything else never becomes valid by resending.
//
// An unresolved plain-string value (text.ErrPlainStringUnresolved) is in that
// last group: the server has no writer identity to resolve it with, so the
// sender has to resolve it and send a document.
func rejectTextWrite(err error) error {
	switch {
	case errors.Is(err, text.ErrStateStale):
		return Reject(CodeTextStateStale, err.Error())
	case errors.Is(err, text.ErrFragmentUnusable):
		return Reject(CodeTextFullStateRequired, err.Error())
	default:
		return Reject(CodeInvalidArgument, err.Error())
	}
}

// maskNamesPath reports, for a dotted field path, whether the mask names it
// exactly — as opposed to naming an ancestor (which replaces the whole subtree)
// or nothing at all.
func maskNamesPath(paths []string) func(string) bool {
	return func(path string) bool {
		return slices.Contains(paths, path)
	}
}

// applyCustom runs one custom mutation. g is the push's gate: its access is
// the invoking caller's, which the invocation is checked against (see
// MutatorAccess) and which the body's EntityTx.Can answers from. The body's
// own writes pass no gate, and the writes of its AsCaller view pass g.
func (s *Server) applyCustom(
	ctx context.Context,
	ws string,
	tx storage.WriteTx,
	g *writeGate,
	custom *syncplanev1.CustomMutation,
	seq uint64,
	actor storage.Actor,
) error {
	mi, ok := s.mutators[custom.GetName()]
	if !ok {
		return Reject(CodeInvalidArgument, fmt.Sprintf("unknown mutator %q", custom.GetName()))
	}
	var args proto.Message
	if mi.args != nil {
		args = proto.Clone(mi.args)
		proto.Reset(args)
		if custom.GetArgs() != nil {
			if err := custom.GetArgs().UnmarshalTo(args); err != nil {
				return Reject(CodeInvalidArgument,
					fmt.Sprintf("mutator %q: bad args: %v", mi.name, err))
			}
		}
	}
	if err := s.checkInvocation(ctx, ws, tx, g.accessOf(), mi, args); err != nil {
		return err
	}
	return mi.fn(markWriteTx(ctx), &EntityTx{
		srv: s, ctx: ctx, ws: ws, tx: tx, seq: seq, actor: actor, access: g.accessOf(), caller: g,
	}, args)
}
