package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/text"
)

// Server-side field history.
//
// A row holds one value, one version and one actor, and the change feed
// coalesces by row — so neither storage nor the feed can answer "what did this
// field hold at sequence N". An app cannot close that gap from outside: a
// client-written history row records the client's own stale view of the row it
// is about to overwrite, and a mutator-written one only covers writes that went
// through that mutator.
//
// Capture therefore lives on the server's write path, which is the one place
// the before-state already exists — the same fetch that serves the
// compare-and-swap guard, the PUT text union and the PATCH merge base. Both
// write paths call captureHistory immediately before they write the row:
// push.applyCrud for the wire CRUD path, and EntityTx.Put/Patch for trusted
// writes, connectors and mutator bodies.
//
// Two things keep the log proportional to what a person did rather than to what
// the client sent, which is the whole difficulty: a local-first client writes
// per edit, and for a collaborative-text field an edit is a keystroke whose
// before-state is the entire document.
//
// What is recorded is a CHANGE, not a write. Both paths hold the before-state
// and the row about to replace it, so the capture diffs them and records only
// the fields that moved — a write that stores what was already there leaves no
// record at all. Without that, the ordinary local-first shape (re-PUT the whole
// row on every edit) lands one full-capture-set record per write, each carrying
// a prior identical to the row stored beside it, and the log grows with write
// volume rather than with edits. See historyChanges.
//
// What is recorded is a SESSION, not a keystroke. A run of writes to one row by
// one actor folds into the record the first of them opened, which keeps saying
// what the fields held before the run began and grows a span rather than a row
// (see historyTarget and foldHistory). Without that, typing for a minute lands
// hundreds of records, each carrying a whole document, and the history of a row
// outgrows the row by orders of magnitude. What it gives up is the states in
// between: this answers "what did it hold before that editing session", and
// keystroke granularity belongs to the text delta log (textdelta.go).
//
// A text field's recorded before-state is a PLAIN-STRING VALUE carrying the
// text rather than its encoded document, for reasons that are half size and
// half correctness — see historyPrior. Because that form is a legal value of the
// field, a record decodes as an ordinary partial message of its source entity
// and restores by PATCHing it back under the record's mask, with no per-field
// handling; the write path resolves the text into edits (see takePlainStrings).
//
// The record lands in one generic entity (syncplane.v1.FieldHistory) rather than
// a synthesized entity per source, at the same sequence as the write that
// opened it. ScanChanges already breaks version ties on (entity, key), which is
// exactly the several-rows-at-one-sequence shape a batch produces, so the sync
// paths need no special case: the entity is server-only and they skip it
// wholesale.
//
// Server.Changes is the exception worth knowing about. It is the RAW feed and
// narrows by nothing, so a consumer reading it directly sees a
// syncplane_field_history change beside each captured write, at that write's
// sequence — which for a fold is later than the record's own seq, since the row
// being rewritten is an older one. engine/effects is unaffected — it dispatches
// per registered prototype — and so is any consumer that switches on
// Change.Entity; one that treats every change alike should skip
// historyEntityName itself.

// historyEntityName is the wire name of the built-in history entity. It is
// registered automatically, and only once some entity opts in, so an app that
// wants no history carries no extra entity in its catalog.
const historyEntityName = "syncplane_field_history"

// defaultHistorySession is how long one record stays open to later writes when
// WithHistorySession says nothing. Long enough that an ordinary run of typing
// lands in one record, short enough that a working day is still a timeline
// rather than a single entry.
const defaultHistorySession = 5 * time.Minute

// HistorySessionOff turns session folding off: every write that changes a
// captured field gets a record of its own. Pass it to WithHistorySession when
// each write to a field has to be separately restorable, rather than only the
// state a run of writes began from. On an entity carrying a collaborative-text
// field that is one record per keystroke, each carrying the whole text.
//
// It does not make the log an audit trail. A DELETE is still uncaptured and
// still purges the row's records (see purgeHistory), so the log answers what a
// field held, never what happened to the row.
const HistorySessionOff time.Duration = -1

// WithHistorySession overrides how long one row's field-history record stays
// open to later writes by the same actor. Writes that land inside the window
// fold into it rather than appending: the record keeps saying what the fields
// held before the FIRST of them, and grows a span (see FieldHistory.seq and
// through_seq) instead of a row.
//
// The window runs from when the record was opened, not from the last write, so
// a record never covers more than one window however long the editing runs.
//
// What folding costs is the intermediate states: history answers "what did this
// hold before that editing session", not "replay the session". Pass
// HistorySessionOff to keep every write, weighing it against what per-write
// capture costs on an entity carrying a collaborative-text field. Default five
// minutes.
func WithHistorySession(d time.Duration) Option {
	return func(s *Server) error {
		s.historySession = d
		return nil
	}
}

// historySessionWindow resolves the configured window. Zero (unset) takes the
// default rather than meaning off, so that "I never configured this" and "I
// asked for per-write capture" stay different answers — the same split
// WithHistoryRetention makes.
func (s *Server) historySessionWindow() time.Duration {
	switch {
	case s.historySession < 0:
		return 0
	case s.historySession == 0:
		return defaultHistorySession
	default:
		return s.historySession
	}
}

// historySessionKey identifies the row whose session a note describes.
type historySessionKey struct {
	ws     string
	entity string
	key    string
}

// historySessionNote is where a row's open record is, and when it opened.
type historySessionNote struct {
	seq    uint64
	opened time.Time
}

// historySessions notes, per row, the record a later write may fold into.
//
// It is a HINT and never authority. Every fold re-reads the record a note names
// and re-checks the session against the record's own actor and timestamp (see
// sameHistorySession), so a note that is stale, lost to a restart, or missing
// because a sibling node made the write costs one extra read and opens a fresh
// record — never a wrong one. That is what lets an otherwise multi-node engine
// keep this in memory: the failure mode of a miss is a shorter session, which is
// exactly the behaviour of not having the feature.
//
// The alternative is a lookup on the by_source index, which would be authority
// but puts an index read on every captured write and answers wrongly while that
// index is still building.
//
// Being a hint is also what bounds it. The table is capped and prunes itself on
// a clock, both of which can drop a note whose session is still open — and that
// costs the same shorter session a restart costs, on a path that is already
// allowed to lose one.
type historySessions struct {
	mu   sync.Mutex
	open map[historySessionKey]historySessionNote
	// pruneAt is when the table is next worth walking, so that the walk is
	// paced by the clock notes actually go stale on rather than by write
	// volume. See pruneHistorySessionsLocked.
	pruneAt time.Time
}

// historySessionMax is the most notes the table holds. Each costs a few dozen
// bytes plus its key, so the cap is set where the table's own footprint starts
// to matter on a host serving many workspaces — not where sessions stop being
// worth keeping, since evicting one only shortens a session.
const historySessionMax = 16384

// historySessionKeep is how far below the cap an eviction takes the table. The
// slack is what makes the walk amortized: refilling it takes thousands of
// writes, so a workspace editing more rows at once than the table holds pays a
// bounded walk once per that many writes instead of one per write.
const historySessionKeep = historySessionMax * 3 / 4

// openHistorySession returns the sequence of the record still open for a row.
func (s *Server) openHistorySession(ws, entity, key string, now time.Time) (uint64, bool) {
	window := s.historySessionWindow()
	if window == 0 {
		return 0, false
	}
	s.historyOpen.mu.Lock()
	defer s.historyOpen.mu.Unlock()
	note, ok := s.historyOpen.open[historySessionKey{ws, entity, key}]
	if !ok || now.After(note.opened.Add(window)) {
		return 0, false
	}
	return note.seq, true
}

// noteHistorySession records where a row's open record is. Re-noting an
// extended session keeps the ORIGINAL opening time, which is what bounds a
// record to one window rather than letting continuous typing hold one open
// indefinitely.
func (s *Server) noteHistorySession(ws, entity, key string, seq uint64, now time.Time) {
	window := s.historySessionWindow()
	if window == 0 {
		return
	}
	k := historySessionKey{ws, entity, key}
	s.historyOpen.mu.Lock()
	defer s.historyOpen.mu.Unlock()
	if s.historyOpen.open == nil {
		s.historyOpen.open = make(map[historySessionKey]historySessionNote)
	}
	if prev, ok := s.historyOpen.open[k]; ok && prev.seq == seq {
		return
	}
	if len(s.historyOpen.open) >= historySessionMax || !now.Before(s.historyOpen.pruneAt) {
		s.pruneHistorySessionsLocked(now, window)
	}
	s.historyOpen.open[k] = historySessionNote{seq: seq, opened: now}
}

// pruneHistorySessionsLocked bounds the note table. Called with the mutex held.
//
// Expired notes go first, and cost nothing to lose: the window has passed, so
// no write could fold into one anyway. What is left is live sessions, and a
// workspace can have more rows under simultaneous edit than the table holds —
// so the rest are evicted down to a mark that leaves SLACK. The slack is the
// point: a table pruned only to the cap sits at the cap, and every write after
// that walks it. Evicting a quarter buys the next quarter's worth of writes.
//
// Notes are evicted in map order rather than by age, because there is nothing
// to gain from choosing: a lost note costs one extra read and a fresh record,
// which is what every miss costs (see historySessions).
//
// The walk is also paced independently of the cap, so that a table left full by
// a burst of editing shrinks back once the burst ends rather than waiting for
// the next one. Pacing it at one window means a note outlives its session by at
// most one more window.
func (s *Server) pruneHistorySessionsLocked(now time.Time, window time.Duration) {
	s.historyOpen.pruneAt = now.Add(window)
	for k, note := range s.historyOpen.open {
		if now.After(note.opened.Add(window)) {
			delete(s.historyOpen.open, k)
		}
	}
	for k := range s.historyOpen.open {
		if len(s.historyOpen.open) <= historySessionKeep {
			return
		}
		delete(s.historyOpen.open, k)
	}
}

// HistoryFilterFunc admits, rejects or rewrites one history record before it is
// written. rec is the record the engine built and src describes the entity whose
// row changed. Return the record to write and true to keep it, false to drop it
// silently, or an error to FAIL THE WRITE that triggered it — a hook that cannot
// decide must not let the write through unrecorded.
//
// An error means the hook could NOT DECIDE, so the write stays retryable: the
// batch carrying it fails with CodeInternal and nothing is acked, and a client
// resends once the hook can answer. Its text never reaches the client — it is
// logged instead, because a hook's own wording can name dependencies and rows
// the caller has no business seeing.
//
// To refuse a record PERMANENTLY, return a Rejection built by Reject instead.
// That acks the mutation and the client drops it, which is what a hook failing
// deterministically on a record's SHAPE wants: resending the identical mutation
// would fail identically, so acking drains the queue rather than wedging it. A
// Rejection's code and message are the hook's own and do reach the client.
// The split is CanMutate's, for the same reason — a dependency having a bad
// minute must not cost the caller its write.
//
// It runs inside the write transaction, immediately before the record is
// stored, so it sees the record's final shape (mask, prior value, actor,
// sequence) and its verdict commits or rolls back with the write.
//
// It runs on every captured write, including one that FOLDS into a record an
// earlier write opened — an open session, or a second write to the same row at
// one sequence, which a batch mutation or a mutator body produces and which
// folds whatever the session settings say. rec is then the whole folded record
// rather than the increment: this hook's own earlier output, plus the fields the
// new write moved. A hook that redacts must be idempotent over its output.
//
// Dropping a folded record leaves the stored one as the previous write left it,
// rather than deleting it. Dropping the FIRST record of a session leaves later
// writes nothing to fold into, so the next one opens a record of its own, whose
// prior describes the state after the dropped write and is indistinguishable
// from one covering the session from its start. Dropping on a record's contents
// therefore changes what the records around it say; dropping per entity does not.
//
// It must be a PURE FUNCTION of its arguments — free of observable side effects
// — for the same reason Permissions.CanMutate must: the storage adapter MAY
// re-run the write closure and re-invoke this hook, so a hook that audit-logs
// or meters double-counts. Redaction, sampling and per-entity policy belong
// here; anything that has to happen once belongs after the write commits.
//
// Rewriting is by value, not in place: return a record (typically the one
// handed in, mutated) and the engine writes exactly that. The record's IDENTITY
// is not rewritable — source_entity, source_key and seq are the fields its row
// key derives from, and changing any of them fails the write. Neither is
// row_created, and it fails the write the same way: redacting a creation record
// means dropping it. Redaction, dropping and rewriting anything else are what
// the hook is for.
//
// Redacting a mask down to empty is allowed and leaves an update. The record
// still says a change happened, and later writes in the session fold in and
// record their own fields.
type HistoryFilterFunc func(
	ctx context.Context,
	rec *syncplanev1.FieldHistory,
	src EntityMeta,
) (*syncplanev1.FieldHistory, bool, error)

// WithHistoryFilter installs the admission hook every history record passes
// through. See HistoryFilterFunc for what it may do and the purity it owes.
//
// The filter is construction-time only, like a merger: it is a func, so unlike
// a schema it cannot be provisioned at runtime. New fails if fn is nil.
func WithHistoryFilter(fn HistoryFilterFunc) Option {
	return func(s *Server) error {
		if fn == nil {
			return fmt.Errorf("server: WithHistoryFilter needs a func")
		}
		s.historyFilter = fn
		return nil
	}
}

// ensureHistoryEntity registers syncplane.v1.FieldHistory into the catalog under
// construction if any entity in it opts into history and it is not there yet.
// Called from every path that adds entities, so opting an entity in is all an
// app has to do.
//
// A name collision is an error rather than a silent overwrite: the app declared
// something else under the reserved name, and quietly replacing it would make
// its rows unreadable through the type it registered.
func ensureHistoryEntity(c *catalog) error {
	wanted := false
	for _, info := range c.entities {
		if info.history != nil {
			wanted = true
			break
		}
	}
	if !wanted {
		return nil
	}
	histType := (&syncplanev1.FieldHistory{}).ProtoReflect().Type()
	if cur, ok := c.entities[historyEntityName]; ok {
		if cur.msgType.Descriptor().FullName() != histType.Descriptor().FullName() {
			return fmt.Errorf("server: entity name %q is reserved for the engine's field history, "+
				"but %s is registered under it", historyEntityName, cur.msgType.Descriptor().FullName())
		}
		return nil
	}
	info, err := buildEntityInfo(histType)
	if err != nil {
		return fmt.Errorf("server: field history entity: %w", err)
	}
	c.entities[info.name] = info
	c.byType[info.msgType.Descriptor().FullName()] = info
	return nil
}

// captureHistory appends one FieldHistory record for a write about to land, and
// is a no-op for an entity that has not opted in.
//
// prior/live are the stored row as it stood before this write, exactly as the
// caller's own merge base saw it — captureHistory decodes prior.Value itself
// rather than taking a decoded message, because the PATCH paths merge into
// theirs in place and the bytes are the only copy still holding the before
// state. after is the row as it will be stored, which is the caller's merged
// result: the two together are what makes a record describe a CHANGE rather
// than merely a write (see historyChanges).
//
// mask is the write's EFFECTIVE field mask for a PATCH — the set the merge
// actually applies, text paths a write option added included — or nil for a
// PUT, which carries no mask and starts from the whole capture set instead.
// Either way the diff has the last word on what the record names.
//
// The record it lands in may be one an EARLIER write opened: see historyTarget.
func (s *Server) captureHistory(
	ctx context.Context,
	tx storage.WriteTx,
	ws string,
	info *entityInfo,
	key string,
	prior storage.Row,
	live bool,
	after proto.Message,
	mask []string,
	seq uint64,
	actor storage.Actor,
) error {
	if info.history == nil {
		return nil
	}
	histInfo, ok := s.catalogPtr().entities[historyEntityName]
	if !ok {
		// Unreachable through registration (ensureHistoryEntity runs wherever an
		// opted-in entity enters the catalog), so this is a corrupt catalog
		// rather than a configuration the app can fix by retrying.
		return fmt.Errorf("server: entity %q opts into history but %q is not registered",
			info.name, historyEntityName)
	}

	paths := info.historyPaths(mask)
	if mask != nil && len(paths) == 0 {
		// A PATCH that touched nothing the schema asked to record. There is no
		// before-state worth a row, and writing an empty one would turn every
		// bookkeeping edit into history noise.
		return nil
	}

	now := time.Now()
	rec := &syncplanev1.FieldHistory{
		SourceEntity: info.name,
		SourceKey:    key,
		Seq:          seq,
		ThroughSeq:   seq,
		Writes:       1,
		ActorSubject: actor.Subject,
		ActorSystem:  actor.System,
		RecordedAt:   timestamppb.New(now),
	}
	if live {
		base, err := info.decode(prior.Value)
		if err != nil {
			// The stored row is poison (see poison.go). There is no before-state
			// anything can express, and failing the write would leave the row
			// unrepairable — a PUT of the key is exactly what clears the
			// quarantine. The caller has already reported it.
			return nil
		}
		paths = historyChanges(base, after, paths)
		if len(paths) == 0 {
			// The write moved nothing the schema captures. A record here would
			// carry a prior identical to the value being stored — the shape a
			// client that re-PUTs whole rows produces on every edit, which is
			// the ordinary local-first shape.
			return nil
		}
		value, err := historyPrior(info, base, paths)
		if err != nil {
			return err
		}
		rec.Mask = &fieldmaskpb.FieldMask{Paths: paths}
		rec.Prior = value
	} else {
		// The row did not exist, so mask and prior stay empty and the flag says
		// why. Emptiness alone would not: a filter may redact an update's mask
		// down to empty, and foldHistory reads the flag to tell the two apart.
		rec.RowCreated = true
	}
	rec.WriteGroups = []*syncplanev1.FieldHistoryWriteGroup{{
		Fields:   historyUnits(rec.GetMask().GetPaths()),
		Writes:   1,
		FirstSeq: seq,
		LastSeq:  seq,
	}}

	histKey, stored, err := s.historyTarget(tx, histInfo, info, ws, key, rec, actor, now)
	if err != nil {
		return err
	}
	if stored != nil {
		merged, err := foldHistory(info, stored, rec)
		if err != nil {
			return err
		}
		rec = merged
	}

	if s.historyFilter != nil {
		created := rec.GetRowCreated()
		out, keep, err := s.historyFilter(ctx, rec, entityMeta(info))
		if err != nil {
			// Propagated rather than turned into a rejection: an error means the
			// hook could not decide, and acking on it would drop the write for
			// good over what may be transient. A hook meaning to refuse
			// permanently returns a Rejection, which survives this wrap and acks
			// — see HistoryFilterFunc.
			return fmt.Errorf("history filter for entity %q: %w", info.name, err)
		}
		if !keep {
			return nil
		}
		if out == nil {
			return Reject(CodeInternal,
				fmt.Sprintf("history filter for entity %q kept a nil record", info.name))
		}
		rec = out
		// The record's identity is the engine's, not the hook's. foldHistory
		// already folded against the key derived above and purgeHistory finds a
		// row's records by that key's prefix, so honouring a move would silently
		// clobber the record at the new key and orphan this one from the DELETE
		// cascade. Redaction and dropping are the rewrites this hook is for.
		if moved := historyRowKey(rec.GetSourceEntity(), rec.GetSourceKey(), rec.GetSeq()); moved != histKey {
			return Reject(CodeInternal, fmt.Sprintf(
				"history filter for entity %q moved the record from %q to %q; a filter may "+
					"redact or drop a record, not change source_entity, source_key or seq",
				info.name, histKey, moved))
		}
		// row_created is the engine's too. It is what foldHistory reads to decide a
		// record absorbs the rest of its session, so a hook that set it would stop
		// every later write in that session from recording anything, and one that
		// cleared it would have the fold merge onto a before-state that does not
		// exist. Redacting a creation record means dropping it.
		if rec.GetRowCreated() != created {
			return Reject(CodeInternal, fmt.Sprintf(
				"history filter for entity %q changed row_created on record %q; a filter may "+
					"redact or drop a record, not restate whether the row existed",
				info.name, histKey))
		}
	}

	encoded, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	if err := histInfo.updateRowIndexes(tx, s.limits, histKey, rec); err != nil {
		return err
	}
	if err := tx.PutRow(histInfo.name, histKey, encoded, seq, actor); err != nil {
		return err
	}
	// Stored at the WRITE's sequence, not the record's: a fold rewrites an
	// older record where it stands, so a raw change-feed reader sees the history
	// row move again at the sequence that caused it (see FieldHistory.seq).
	//
	// Marked open only after the row is written, and deliberately not undone if
	// the transaction later rolls back. The note is a hint about which record to
	// probe, never authority that one exists: a stale entry costs one extra read
	// and starts a fresh session, which is what a rolled-back write should leave
	// behind anyway.
	s.noteHistorySession(ws, info.name, key, rec.GetSeq(), now)
	return nil
}

// purgeHistory drops every record describing one row. Called from both DELETE
// paths, inside the deleting transaction, so the records go with the row or
// neither does.
//
// It is gated on the history ENTITY being registered rather than on the source
// entity's current option, because records outlive the option that created them:
// registration is additive, so an entity that drops history: {} in a later
// deploy keeps everything already captured, and gating on info.history would
// leave exactly those records unpurgeable — immortal under every retention, and
// waiting to be inherited by the next row to claim the key. The cost is one
// range delete per DELETE of any entity once some entity has ever opted in,
// which matches nothing for an entity that never captured anything.
//
// The gate does close once NO entity opts in, because the history entity is then
// not in the catalog at all — which is why the GC age sweep is deliberately NOT
// gated the same way (see CollectGarbage): it is the only collector such records
// have left. The pairing leaves one gap, and it is the configuration that asked
// for unbounded growth anyway: under HistoryForever nothing expires by age, so a
// row deleted after the last entity opted out keeps its records.
//
// It runs whatever retention is configured, because a deleted row's history is
// dead weight in every configuration. Nothing can use it: a restore is a PATCH,
// and a PATCH of a missing or tombstoned row is a conflict by the engine's
// delete-wins policy. Worse, keeping it would attach the OLD row's history to a
// new row that later claims the same key — which is not a hypothetical, since
// keys are app-chosen and re-creating one is ordinary.
//
// Paired with HistoryForever this is the whole lifecycle: a record lives exactly
// as long as the row it describes.
//
// The bound is a key-prefix purge rather than a scan: history row keys are
// (source_entity, source_key, seq) joined by the shared row-key rules, whose
// escaping is what makes "everything under this row" a contiguous range instead
// of a guess. One statement, however long the log is.
//
// Unbounded, unlike the GC pass's chunked sweep (see collectHistory): this runs
// inside the transaction deleting the row, and the records must go with the row
// or neither does. A chunk left behind would be history for a row that no longer
// exists, waiting to be inherited by the next row to claim the key — and one
// row's log is bounded by the edits made to it, not by the workspace's write
// volume.
func (s *Server) purgeHistory(tx storage.WriteTx, info *entityInfo, key string) error {
	if _, ok := s.catalogPtr().entities[historyEntityName]; !ok {
		return nil
	}
	return purgeRowHistory(tx, info.name, key)
}

// purgeRowHistory removes one source row's records. Split out from purgeHistory
// so the prefix derivation sits next to the key derivation it has to agree with.
func purgeRowHistory(tx storage.WriteTx, entity, key string) error {
	// naming.RowKey escapes `\` and `/` inside each part before joining with
	// `/`, so the two-part prefix cannot be forged by a source key that happens
	// to contain a separator: ("a/b", "c") and ("a", "b/c") escape apart. The
	// middle segment goes through historyKeyPart for the same reason the
	// capture does, and it answers by source key alone, so one prefix covers a
	// row's whole log however far the sequence has run.
	prefix := naming.RowKey([]string{entity, historyKeyPart(entity, key)}) + "/"
	_, _, err := tx.PurgeEntityRows(historyEntityName, prefix, time.Time{}, 0)
	return err
}

// warmHistorySourceFilters resolves the read filter of every entity a history
// record could describe, before the read snapshot opens — historyReadable then
// hits the memoized filter inside the scan, which is what the Permissions
// contract requires (a hook must not run while a read tx is pinned).
//
// It warms EVERY registered entity rather than the history-enabled ones,
// because a record outlives the option that created it: an entity that has
// since dropped history: {} still has records naming it, and they still owe
// that entity's filter.
func (s *Server) warmHistorySourceFilters(e *rowEmitter) error {
	for name := range s.catalogPtr().entities {
		if name == historyEntityName {
			continue
		}
		if _, err := e.appFilterOf(name); err != nil {
			return err
		}
	}
	return nil
}

// historyReadable decides whether one history record may be shown to the caller
// behind e. It is the read half of the feature's permission story and the ONLY
// gate on it, since Query is the only door a client has to a server-only entity.
//
// A record is a view of another entity's row, so the filter that governs it is
// the SOURCE entity's — never the history entity's own. The history entity has a
// name the app never chose and never registered, so its ReadFilter falls to the
// default (nil, allow all); reading a record through that alone would hand a
// client every prior value, mask and actor of rows its own filter denies it. The
// rule this enforces instead is the one an app would write by hand: you may read
// the history of a row you can currently read.
//
// "Currently" is doing real work. The record's prior value carries only the
// masked fields, so it cannot answer a filter that tests an unrecorded one —
// the live row can, and it is also what a restore would PATCH. A row that is
// gone or tombstoned is therefore unreadable rather than open; in practice the
// DELETE cascade has already taken its records with it.
//
// The source-row half of that is memoized per request — see
// historySourceReadable.
func (s *Server) historyReadable(tx storage.ReadTx, e *rowEmitter, histInfo *entityInfo, row storage.Row) (bool, error) {
	if row.Deleted {
		// A tombstone carries only the key, so there is no source row to
		// authorize against. It would never survive a predicate either (nothing
		// decodes out of an empty value), so this only makes that explicit.
		return false, nil
	}
	msg, ok := e.decodeRow(histInfo, row)
	if !ok {
		return false, nil // quarantined, see poison.go
	}
	rec, ok := msg.(*syncplanev1.FieldHistory)
	if !ok {
		return false, nil
	}
	src, registered := s.catalogPtr().entities[rec.GetSourceEntity()]
	if !registered {
		// The source entity has left the catalog: nothing here can decode its
		// row or say what its filter means, so there is no way to establish that
		// the caller may read it.
		return false, nil
	}
	filter, err := e.filterOf(src.name)
	if err != nil {
		return false, err
	}
	hides, err := e.mayHide(src)
	if err != nil {
		return false, err
	}
	if filter == nil && !hides {
		// Every row of the source entity is readable, so its history is too.
		// Also the fast path: no source row is fetched in the common case where
		// an app does not filter reads at all. The projection still runs, as it
		// is what takes the write groups out.
		return e.projectHistory(src, row.Key, rec, nil), nil
	}
	verdict, err := e.historySourceReadable(tx, src, rec.GetSourceKey(), filter)
	if err != nil || !verdict.readable {
		return false, err
	}
	return e.projectHistory(src, row.Key, rec, verdict.hidden), nil
}

// projectHistory takes out of rec the fields of its source row that a guard
// hides from the caller, and reports whether anything of rec is left to
// show: a record of a row's creation, or one whose mask names a field the
// caller reads. seq, through_seq and writes then count only the write groups
// that changed a field the caller reads, and the groups themselves never
// leave the server. A record it changes is memoized by its key for the
// query's matching and its result (see historyView).
//
// A predicate then matches the record the caller receives, so a condition on
// the mask or on the counts cannot tell the caller that a field it does not
// read changed.
func (e *rowEmitter) projectHistory(src *entityInfo, key string, rec *syncplanev1.FieldHistory, hidden []string) bool {
	paths := rec.GetMask().GetPaths()
	kept := paths
	if len(hidden) > 0 {
		kept = nil
		for _, p := range paths {
			if !unitHidden(hidden, protectedHead(p)) {
				kept = append(kept, p)
			}
		}
		if len(kept) == 0 && !rec.GetRowCreated() {
			return false
		}
	}
	if len(kept) == len(paths) && len(rec.GetWriteGroups()) == 0 {
		return true
	}
	out := proto.CloneOf(rec)
	out.WriteGroups = nil
	if len(hidden) > 0 {
		countVisibleWrites(out, rec.GetWriteGroups(), hidden)
	}
	if len(kept) != len(paths) {
		out.Mask = &fieldmaskpb.FieldMask{Paths: kept}
	}
	if len(kept) != len(paths) && len(rec.GetPrior()) > 0 {
		prior, err := src.decode(rec.GetPrior())
		if err != nil {
			return false
		}
		if err := carryProtectedFields(src, hidden, nil, prior); err != nil {
			return false
		}
		if out.Prior, err = proto.Marshal(prior); err != nil {
			return false
		}
	}
	if e.historyViews == nil {
		e.historyViews = map[string]*syncplanev1.FieldHistory{}
	}
	e.historyViews[key] = out
	return true
}

// countVisibleWrites sets seq, through_seq and writes of out from the groups
// that changed a unit outside hidden, sorted. A group of no units is the
// row's creation, which every caller that reads the row sees. A record without
// groups keeps its counts: nothing says which of its writes changed what.
//
// A hidden write that lands before the first visible one moves seq forward,
// and prior still holds at the new seq: the writes before it changed only
// hidden units, which the projection takes out of mask and prior.
func countVisibleWrites(out *syncplanev1.FieldHistory, groups []*syncplanev1.FieldHistoryWriteGroup, hidden []string) {
	var (
		writes      uint32
		first, last uint64
	)
	for _, g := range groups {
		units := g.GetFields()
		if len(units) > 0 && !slices.ContainsFunc(units, func(u string) bool { return !unitHidden(hidden, u) }) {
			continue
		}
		if writes == 0 || g.GetFirstSeq() < first {
			first = g.GetFirstSeq()
		}
		last = max(last, g.GetLastSeq())
		writes += g.GetWrites()
	}
	if writes == 0 {
		return
	}
	out.Seq, out.ThroughSeq, out.Writes = first, last, writes
}

// unitHidden reports whether unit is in hidden, which is sorted.
func unitHidden(hidden []string, unit string) bool {
	_, found := slices.BinarySearch(hidden, unit)
	return found
}

// historyView returns the history record with key as the caller receives it,
// and whether projectHistory changed it.
func (e *rowEmitter) historyView(key string) (*syncplanev1.FieldHistory, bool) {
	rec, ok := e.historyViews[key]
	return rec, ok
}

// historySourceKey identifies the source row a record describes. The workspace
// is not part of it: a rowEmitter serves one request, which is one workspace.
type historySourceKey struct {
	entity string
	key    string
}

// historySourceVerdict is what the caller reads of a source row: whether it
// reads the row, and the units a guard hides from it, sorted.
type historySourceVerdict struct {
	readable bool
	hidden   []string
}

// historySourceReadable is the source-row half of historyReadable — may the
// caller read this row right now, and which of its fields — memoized for the
// request.
//
// The memo is what keeps the gate proportional to the ROWS a query touches
// rather than to the RECORDS it returns, and the difference is the whole shape
// of the feature's read: a history panel asks for one row's log, so a page of
// results is a page of records naming the same source row. Without it each one
// pays its own source-row read and decode inside the read snapshot.
//
// Sound for the same reason filterOf's memo is: the verdict is a pure function
// of the entity's filter — resolved once per request, before the snapshot opens
// — and a row the snapshot holds still for the duration of the scan.
func (e *rowEmitter) historySourceReadable(
	tx storage.ReadTx,
	src *entityInfo,
	key string,
	filter readFilter,
) (historySourceVerdict, error) {
	memo := historySourceKey{entity: src.name, key: key}
	if verdict, known := e.historySources[memo]; known {
		return verdict, nil
	}
	verdict, err := e.computeHistorySourceReadable(tx, src, key, filter)
	if err != nil {
		// Deliberately not memoized: an error is not a verdict, and a storage
		// failure the scan recovers from must not have decided this row.
		return historySourceVerdict{}, err
	}
	if e.historySources == nil {
		e.historySources = map[historySourceKey]historySourceVerdict{}
	}
	e.historySources[memo] = verdict
	return verdict, nil
}

func (e *rowEmitter) computeHistorySourceReadable(
	tx storage.ReadTx,
	src *entityInfo,
	key string,
	filter readFilter,
) (historySourceVerdict, error) {
	srcRow, err := tx.GetRow(src.name, key)
	if err == storage.ErrNotFound {
		return historySourceVerdict{}, nil
	}
	if err != nil {
		return historySourceVerdict{}, err
	}
	if srcRow.Deleted {
		return historySourceVerdict{}, nil
	}
	srcMsg, ok := e.decodeRow(src, srcRow)
	if !ok {
		return historySourceVerdict{}, nil
	}
	readable, err := filter.admits(srcRow.Key, srcMsg)
	if err != nil || !readable {
		return historySourceVerdict{}, err
	}
	hidden, err := e.hiddenUnits(src, srcRow.Key, srcMsg)
	if err != nil {
		return historySourceVerdict{}, err
	}
	return historySourceVerdict{readable: true, hidden: hidden}, nil
}

// historyKeySeqWidth is the widest a rendered sequence can be — math.MaxUint64
// in base 10. historyKeyPart reserves it whatever sequence the write actually
// carries, so whether a row's records are keyed by its source key or by a
// stand-in is settled by the source key alone. A budget that moved as the
// workspace crossed a power of ten would rekey a row mid-life, and
// purgeRowHistory's single prefix would then cover only part of its log.
const historyKeySeqWidth = len("18446744073709551615")

// historyKeyDigestLen is how many hex characters of a source key's digest a
// stand-in carries: 128 bits, since the digest is the whole of what separates
// two long keys sharing a prefix.
const historyKeyDigestLen = 32

// historyKeyMarker separates a stand-in's readable prefix from its digest.
const historyKeyMarker = "~"

// historyRowKey derives the storage key of the record describing (entity, key)
// at seq. It is what histInfo.keyOf would derive from the record's key fields,
// except for the substitution historyKeyPart makes, so every caller that
// addresses a record — the capture, the session probe, the DELETE purge, the
// admission hook's identity guard — must go through here rather than keyOf.
func historyRowKey(entity, key string, seq uint64) string {
	return naming.RowKey([]string{
		entity,
		historyKeyPart(entity, key),
		strconv.FormatUint(seq, 10),
	})
}

// historyKeyPart renders a source key as the middle segment of a history row
// key: the key itself whenever the joined result fits naming.MaxRowKeyBytes,
// and otherwise a stand-in of as much of the key as fits, `~`, and the leading
// hex of its SHA-256.
//
// The substitution exists so that no source key can make a record unstorable.
// A history key joins the entity name and the sequence onto the source key, so
// without it opting into history would SHORTEN the key length an entity can
// carry, and a row whose app-chosen key no longer left room would have its next
// write rejected — the source write, not merely the record. Capture is an
// observability feature and must not decide whether an application's write
// succeeds.
//
// Nothing reads a source key back out of a row key: a record carries its own
// source_entity and source_key, the by_source index orders on those fields, and
// purgeRowHistory only needs a prefix it can derive the same way. The readable
// head is for whoever is looking at raw storage.
//
// Two long keys collide only if one is literally the other's truncation plus
// its digest, which an application would have to construct on purpose. The
// consequence is bounded: their logs share a key range, so a DELETE of either
// row purges both, and records written at one sequence for both rows (only
// reachable inside a single BATCH) fold into one.
func historyKeyPart(entity, key string) string {
	// What is left once the entity, the sequence and the two `/` separators
	// have taken theirs, measured against the ESCAPED width a part occupies in
	// the joined key rather than its own — a key full of `/` doubles.
	budget := naming.MaxRowKeyBytes - naming.RowKeyPartLen(entity) - historyKeySeqWidth - 2
	if naming.RowKeyPartLen(key) <= budget {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	var b strings.Builder
	room := budget - historyKeyDigestLen - len(historyKeyMarker)
	for _, r := range key {
		// Rune by rune: a stand-in is a row key like any other and has to stay
		// valid UTF-8.
		n := naming.RowKeyPartLen(string(r))
		if room-n < 0 {
			break
		}
		b.WriteRune(r)
		room -= n
	}
	b.WriteString(historyKeyMarker)
	b.WriteString(hex.EncodeToString(sum[:])[:historyKeyDigestLen])
	return b.String()
}

// historyTarget picks the record this capture lands in and reads whatever is
// already stored there — nil for a fresh one. When it folds into a record an
// earlier write opened it also moves rec.Seq back to that record's sequence,
// which is what makes the two agree on a key.
//
// Two things put a record in the way. A BATCH mutation shares one sequence
// across its sub-mutations and may touch the same row twice (the documented
// "PATCH the row a preceding PUT created" shape), as may a mutator body — those
// collide on the key derived below whatever the session settings say. And an
// open SESSION deliberately routes this write into an earlier record.
//
// No key it derives can be invalid, so nothing here rejects: historyKeyPart
// bounds the length, and every caller has already put the source key through
// entityInfo.checkKey — the rules left over (non-empty, UTF-8, no control
// characters) are properties the join preserves.
func (s *Server) historyTarget(
	tx storage.WriteTx,
	histInfo *entityInfo,
	info *entityInfo,
	ws string,
	key string,
	rec *syncplanev1.FieldHistory,
	actor storage.Actor,
	now time.Time,
) (string, *syncplanev1.FieldHistory, error) {
	writeSeq := rec.GetSeq()
	histKey := historyRowKey(info.name, key, writeSeq)
	if seq, ok := s.openHistorySession(ws, info.name, key, now); ok && seq != writeSeq {
		rec.Seq = seq
		sessKey := historyRowKey(info.name, key, seq)
		stored, err := readHistoryRecord(tx, histInfo, sessKey)
		if err != nil {
			return "", nil, err
		}
		if stored != nil && sameHistorySession(stored, actor, now, s.historySessionWindow()) {
			return sessKey, stored, nil
		}
		// The note was stale: the record is gone — a DELETE purged it, or the
		// write that opened it rolled back — or the session is not this writer's
		// to extend. Fall back to a record of this write's own.
		rec.Seq = writeSeq
	}
	stored, err := readHistoryRecord(tx, histInfo, histKey)
	if err != nil {
		return "", nil, err
	}
	return histKey, stored, nil
}

// readHistoryRecord reads one stored record, answering nil for every state that
// is not a record this capture can fold into. A row that will not decode is one
// of them: it is not worth failing a write over, and the capture overwrites it
// with the record it can express.
func readHistoryRecord(tx storage.ReadTx, histInfo *entityInfo, histKey string) (*syncplanev1.FieldHistory, error) {
	row, err := tx.GetRow(histInfo.name, histKey)
	if err == storage.ErrNotFound || (err == nil && row.Deleted) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	msg, err := histInfo.decode(row.Value)
	if err != nil {
		return nil, nil
	}
	rec, ok := msg.(*syncplanev1.FieldHistory)
	if !ok {
		return nil, nil
	}
	return rec, nil
}

// sameHistorySession reports whether a stored record is still open to a write by
// actor at now.
//
// Both halves matter. A record belongs to ONE actor, because its actor fields
// attribute every write folded into it — letting a second writer extend it would
// file their edit under someone else's name. And a session expires a fixed
// window after it OPENED rather than after its last write, so one record spans
// at most that window however long the typing runs: an idle timeout would let a
// single record cover an afternoon, and a panel rendering it could only say
// "someone changed this at some point today".
func sameHistorySession(
	stored *syncplanev1.FieldHistory,
	actor storage.Actor,
	now time.Time,
	window time.Duration,
) bool {
	if storedActor(stored) != actor {
		return false
	}
	opened := stored.GetRecordedAt()
	if opened == nil {
		return false
	}
	return !now.After(opened.AsTime().Add(window))
}

// storedActor reads back the actor a record was written with. It is the inverse
// of the population in captureHistory, kept next to the comparison that depends
// on the two agreeing field for field.
func storedActor(rec *syncplanev1.FieldHistory) storage.Actor {
	return storage.Actor{Subject: rec.GetActorSubject(), System: rec.GetActorSystem()}
}

// foldHistory folds a new record into the one already in its place and returns
// the record to write.
//
// The stored record has to describe the state before the FIRST write it covers,
// because that is what "the value at sequence N" means — so a later write
// contributes only the fields no earlier one already carries, and the record's
// seq, recorded_at and actor are its identity and stand. Only the mask, the
// prior value and the span bookkeeping move.
//
// Bookkeeping moves even when nothing else does. A write that folds in without
// widening the mask still rewrites the record to advance through_seq and
// writes, which is one row rewritten at constant size where an unfolded capture
// would have appended another whole record.
//
// A stored row_created record is ABSORBING: it says the row did not exist when
// the session began, and nothing later gives those fields an earlier value than
// "absent".
//
// Absorbing keys on the flag and not on an empty mask, though a creation record
// has both: a filter that redacts an update's last remaining path leaves an
// empty mask on a record that is not a creation, and reading emptiness here
// would swallow every later write in that session.
func foldHistory(info *entityInfo, stored, rec *syncplanev1.FieldHistory) (*syncplanev1.FieldHistory, error) {
	out := proto.Clone(stored).(*syncplanev1.FieldHistory)
	out.ThroughSeq = rec.GetThroughSeq()
	out.Writes = stored.GetWrites() + 1
	out.WriteGroups = foldWriteGroups(out.GetWriteGroups(), rec.GetWriteGroups())
	if stored.GetRowCreated() {
		return out, nil
	}
	var added []string
	for _, p := range rec.GetMask().GetPaths() {
		if !maskCovers(stored.GetMask().GetPaths(), p) {
			added = append(added, p)
		}
	}
	if len(added) == 0 {
		return out, nil
	}
	// A new path may be an ANCESTOR of one the record already carries — a PUT
	// proposing "settings" folding into a session that recorded "settings.color"
	// — and an ancestor path replaces the whole subtree it names. Those stored
	// paths are therefore split out: they drop off the mask, since the wider path
	// subsumes them, and their values are laid back over the merge below.
	var kept, subsumed []string
	for _, p := range stored.GetMask().GetPaths() {
		if maskCovers(added, p) {
			subsumed = append(subsumed, p)
		} else {
			kept = append(kept, p)
		}
	}
	// Fold the uncovered fields' before-state (as of THIS write, the earliest
	// state still recoverable for them) onto the stored record's prior value.
	//
	// Both merges below run with NO mergers, and must: these are snapshots, and
	// a text field holds a plain-string value on each side rather than a
	// document. Handing them the entity's mergers would send two of those to the
	// text merger, which refuses them by design.
	base := info.msgType.New().Interface()
	if len(stored.GetPrior()) > 0 {
		var err error
		if base, err = info.decode(stored.GetPrior()); err != nil {
			return rec, nil
		}
	}
	src, err := info.decode(rec.GetPrior())
	if err != nil {
		return rec, nil
	}
	var recorded proto.Message
	if len(subsumed) > 0 {
		recorded = proto.Clone(base)
	}
	if err := lww.Merge(base, src, added); err != nil {
		return nil, err
	}
	if len(subsumed) > 0 {
		// The subsumed paths are the record's own, and a record says what its
		// fields held before the session — so what it already recorded outranks
		// the state the folding write found. The rest of the subtree comes from
		// the merge above, which is the earliest state still recoverable for it.
		if err := lww.Merge(base, recorded, subsumed); err != nil {
			return nil, err
		}
	}
	value, err := proto.Marshal(base)
	if err != nil {
		return nil, err
	}
	out.Mask = &fieldmaskpb.FieldMask{Paths: append(kept, added...)}
	out.Prior = value
	return out, nil
}

// foldWriteGroups adds the groups of a folding write to the groups of the
// record it folds into. groups is the record's own clone and is changed in
// place; a group of the same fields only grows, so the count of groups stays
// at the number of distinct field sets the session changed.
func foldWriteGroups(groups, add []*syncplanev1.FieldHistoryWriteGroup) []*syncplanev1.FieldHistoryWriteGroup {
	for _, a := range add {
		i := slices.IndexFunc(groups, func(g *syncplanev1.FieldHistoryWriteGroup) bool {
			return slices.Equal(g.GetFields(), a.GetFields())
		})
		if i < 0 {
			groups = append(groups, proto.CloneOf(a))
			continue
		}
		g := groups[i]
		g.Writes += a.GetWrites()
		g.FirstSeq = min(g.GetFirstSeq(), a.GetFirstSeq())
		g.LastSeq = max(g.GetLastSeq(), a.GetLastSeq())
	}
	return groups
}

// historyUnits returns the units a mask names: the first segment of each path,
// sorted and without duplicates. It is the name a guard's hidden set uses (see
// protectedHead), so a write group compares against that set directly.
func historyUnits(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	units := make([]string, 0, len(paths))
	for _, p := range paths {
		units = append(units, protectedHead(p))
	}
	slices.Sort(units)
	return slices.Compact(units)
}

// maskCovers reports whether a mask already accounts for a path: it names the
// path outright, or an ancestor of it. An ancestor path replaces the whole
// subtree under it (see engine/internal/lww), so "settings" covers
// "settings.size" and there is nothing further to record for it.
func maskCovers(mask []string, path string) bool {
	for _, p := range mask {
		if p == path || strings.HasPrefix(path, p+".") {
			return true
		}
	}
	return false
}

// historyPaths resolves the paths a write PROPOSES to record. A PATCH (mask
// non-nil) keeps the paths whose top-level unit the entity captures; a PUT (mask
// nil) proposes the whole capture set, which is what "PUT replaces whole rows"
// means here. historyChanges then narrows either to what actually moved.
//
// The PUT case CLONES rather than handing out info.historyFields: the slice
// lands on a record the admission hook may rewrite, and a hook that appended to
// or reordered it in place would be editing the entity's registration for every
// write after it.
func (info *entityInfo) historyPaths(mask []string) []string {
	if mask == nil {
		return slices.Clone(info.historyFields)
	}
	out := make([]string, 0, len(mask))
	for _, p := range mask {
		segs := lww.SplitPath(p)
		if len(segs) > 0 && slices.Contains(info.historyFields, segs[0]) {
			out = append(out, p)
		}
	}
	return out
}

// historyChanges narrows proposed paths to those whose value actually moved
// between the stored row and the row replacing it — the difference between
// recording changes and recording writes.
//
// It matters most on the PUT path, and the ordinary local-first client is why:
// one that re-sends the whole row on every edit would otherwise get a
// full-capture-set record per write, each carrying a prior identical to the
// value stored beside it, and a history panel would show N entries where
// nothing changed. Narrowing rather than merely suppressing costs nothing extra
// once the comparison is made, and it is what lets a panel render "title
// changed" from the mask alone.
//
// The comparison is per top-level UNIT — a field, or a oneof group — because
// that is the granularity the merge resolves at, so a mask can always be
// grouped by its first segment. Two paths under one unit therefore stand or
// fall together: a PATCH masking "settings.size" and "settings.color" where
// only one moved records both. That over-captures inside a row that did change,
// which costs a little space and no correctness — the alternative is a
// path-granular walk that would have to restate the merge's own path semantics.
//
// A text field compares as its encoded document. An unchanged document merges
// to identical bytes, so a no-op write suppresses; a real edit adds items and
// records. Bytes are sound HERE only because the encoding is canonical (see
// engine/text) — elsewhere in this file values are compared as messages.
func historyChanges(base, after proto.Message, paths []string) []string {
	bm, am := base.ProtoReflect(), after.ProtoReflect()
	moved := make(map[string]bool, len(paths))
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		segs := lww.SplitPath(p)
		if len(segs) == 0 {
			continue
		}
		unit := segs[0]
		changed, known := moved[unit]
		if !known {
			changed = !lww.EqualUnit(bm, am, unit)
			moved[unit] = changed
		}
		if changed {
			out = append(out, p)
		}
	}
	return out
}

// historyPrior serializes the before-state a record carries: the source entity
// with its key fields set — so a decoded record always says which row it
// describes — plus the masked fields as they stood, and nothing else.
//
// The masked fields are copied by an ordinary LWW merge onto an empty base with
// NO mergers, because this is a snapshot rather than a write: a field must land
// as it was, not unioned with anything.
//
// A text field is then REDUCED to a plain-string value (see FieldHistory.prior
// and text.EncodePlainString). The stored document is the wrong thing to keep
// on both counts. It cannot be restored as itself — writing it back unions it
// with the live document, where the delete markers win and the restore silently
// changes nothing — so what a restore needs is the text, applied as edits by
// whoever restores. And the document carries a peer table, a version vector,
// per-run parents and tombstoned runs holding deleted content, several times
// the text it renders, recorded again for every write that touches the field.
func historyPrior(info *entityInfo, base proto.Message, paths []string) ([]byte, error) {
	prior := info.msgType.New()
	baseRefl := base.ProtoReflect()
	for _, kf := range info.keyFields {
		prior.Set(kf, baseRefl.Get(kf))
	}
	if err := lww.Merge(prior.Interface(), base, paths); err != nil {
		return nil, fmt.Errorf("server: entity %q history: %w", info.name, err)
	}
	if err := reduceHistoryText(info, prior, paths); err != nil {
		return nil, err
	}
	return proto.Marshal(prior.Interface())
}

// reduceHistoryText rewrites every recorded text field of a prior snapshot from
// its encoded document to a plain-string value carrying the document's visible
// text, in place.
//
// It walks the entity's text fields rather than the paths, because the paths a
// record names are top-level units while a text field may sit under one; a field
// whose unit is not recorded is not in the snapshot to begin with and reads back
// empty, so the walk is bounded by the entity's shape and needs no path algebra.
//
// A document that will not decode fails the write rather than being dropped. It
// is unreachable from any path that stores one — every write to a text field
// goes through the CRDT merger, which decodes it first — so reaching here means
// the stored row disagrees with the schema about what the field is, and quietly
// recording an empty before-state would make a later restore wipe the field.
func reduceHistoryText(info *entityInfo, prior protoreflect.Message, paths []string) error {
	for _, tf := range info.textFields {
		for _, ti := range tf.instances(prior) {
			if !maskCovers(paths, ti.path) {
				continue
			}
			doc := ti.bytes()
			if len(doc) == 0 {
				continue
			}
			plain, err := text.PlainText(doc)
			if err != nil {
				return fmt.Errorf("server: entity %q history: text field %q: %w", info.name, ti.path, err)
			}
			if plain == "" {
				// An emptied document still encodes to bytes — its tombstoned runs
				// outlive the text. Clearing keeps "held nothing" indistinguishable
				// from "was never set", which is what a restore of either has to do,
				// and is why the empty plain-string value is not recorded instead.
				ti.clear()
				continue
			}
			value, err := text.EncodePlainString(plain)
			if err != nil {
				return fmt.Errorf("server: entity %q history: text field %q: %w", info.name, ti.path, err)
			}
			ti.set(value)
		}
	}
	return nil
}
