// Package storage defines the pluggable persistence interface for the sync
// engine. Implementations must isolate workspaces from each other; the
// engine never reads or writes across workspace boundaries in one
// transaction. Adapters MAY allow different workspaces to commit
// concurrently (a Postgres adapter should exploit this; SQLite does not).
//
// # Adapter authoring contract
//
// An adapter must provide, in order of importance:
//
//   - Per-workspace write mutual exclusion: WithWriteTx must guarantee that
//     for one workspace at most one fn runs at a time, for the ENTIRE
//     duration of fn — including reads fn performs before its first write.
//     A SQL adapter under MVCC must therefore take a workspace-level lock
//     (e.g. lock the workspace row) BEFORE invoking fn; relying on
//     row-locking at first write loses read-then-write updates such as the
//     push path's watermark check.
//   - Write-tx retries: WithWriteTx MAY invoke fn multiple times (e.g. retry
//     on a serialization failure or deadlock). The engine writes its
//     closures to be re-runnable; adapters must discard all effects of an
//     abandoned attempt.
//   - Read snapshots: WithReadTx runs fn AT MOST ONCE over a consistent
//     snapshot. Streaming callers cannot re-run, so a read tx that fails is
//     surfaced as an error, never retried internally.
//   - Re-entrant reads during a scan: a scan callback MAY issue any ReadTx
//     read against the transaction it is scanning, and those reads see the
//     same snapshot as the scan. This is not hypothetical — the engine
//     re-reads each batch of candidate rows (GetRows) from inside ScanIndex to
//     apply read filters, and reads FieldDeltas from inside ScanChanges to ship
//     a text delta instead of a whole document — so an adapter whose driver cannot
//     interleave statements on one connection (a pgx result set held open, a
//     single-stream remote protocol) must buffer or page the scan rather than
//     hold one result set open across fn. Writes are the other way round: the
//     engine collects a scan's rows and rewrites them after it returns (see
//     Server.compactWorkspace), so an adapter need not tolerate a write during
//     iteration.
//   - Atomicity: a write tx commits iff fn's final attempt returns nil;
//     savepoints (InSavepoint) nest within it.
//   - Lazy workspace registration: a write tx whose closure writes nothing
//     must leave no trace — above all no entry for ListWorkspaceIDs, which
//     nothing ever removes (see WithWriteTx).
//   - Actor round-trip: the Actor handed to PutRow/DeleteRow is stored with the
//     row and returned by EVERY read that yields a Row (GetRow, GetRows, both
//     entity scans, ScanChanges). Uniform because a Row whose Actor is populated on
//     some paths and silently zero on others reads as "unattributed" and
//     misleads the caller rather than failing; an adapter that cannot store it
//     yet must say so, not drop it.
//   - Context: every method honors ctx. A call made with a cancelled or
//     expired context fails without committing — the engine never observes a
//     partial commit from a cancelled write.
//   - The collaborative-text delta log (PutFieldDelta, FieldDeltas,
//     PruneFieldDeltas, FieldDeltaHorizon) is an append-only side table the
//     pull path reads over a sequence span. It exists only to make pulls
//     smaller, so an adapter may
//     always report a span incomplete and lose nothing but wire size; reporting
//     an under-covered span as COMPLETE is the one failure that corrupts
//     documents.
//   - Secondary index entries (PutIndexEntries, DeleteIndexEntries,
//     ScanIndex, IndexState, SetIndexState) exist only for LIVE rows: a
//     DeleteRow is paired with a DeleteIndexEntries by the engine, so a
//     tombstone never has entries. CollectTombstones is a bulk delete of rows
//     that are already gone from the index and needs no index work;
//     PurgeEntityRows removes live rows, so it must drop their entries with
//     them — it is the one bulk delete that does its own index work. The keys
//     are opaque bytes compared BYTEWISE — the engine builds them so that byte
//     order is the row order it wants, and an adapter that applies a collation
//     instead answers ordered queries wrongly. Entries are derived state:
//     losing them costs a rebuild, never correctness.
//   - The idempotency ledger (IdempotencyRecord, PutIdempotencyRecord,
//     CollectIdempotencyKeys) records what a completed trusted write did, keyed
//     by a caller-supplied key. It is read and written inside the SAME
//     transaction as the write it guards, which is what makes the check and the
//     write indivisible; an adapter that answers the read from outside the
//     transaction turns a retry into a double-apply.
//   - Effect-consumer state (ConsumerPosition, SetConsumerPosition,
//     ConsumerShadow, PutConsumerShadow, DeleteConsumer, CollectIdleConsumers)
//     is the durable bookkeeping of whatever reacts to committed changes. Its
//     one hard requirement is that a position and the shadows recorded with it
//     commit TOGETHER, which they do by sharing a transaction; an adapter that
//     splits them turns a crash into a silently skipped effect rather than a
//     redelivered one. ListPendingWorkspaceIDs reads the same state from
//     outside a workspace transaction, so that finding the workspaces a
//     consumer owes work costs one query rather than one transaction each.
//   - Work queries (ListPendingWorkspaceIDs, ListCollectableWorkspaceIDs,
//     ListStaleIndexWorkspaceIDs, ListCompactableWorkspaceIDs) answer "which
//     workspaces owe this background pass work" from outside any workspace
//     transaction, paged by id. They are what keeps a deployment's maintenance
//     floor proportional to the work waiting rather than to the number of
//     workspaces that exist, so an adapter answering one by iterating
//     workspaces itself has implemented the signature and not the point. Each
//     may be optimistic and none may be short.
//
// The conformance suite in storagetest encodes this contract; run it (and
// storagetest.ConformanceNamespaced for multi-tenant adapters) against any
// new implementation.
//
// Two implementations ship: sqlitestore (the default, single-writer, one
// process) and pgstore (PostgreSQL, several nodes over one database, different
// workspaces committing concurrently).
//
// Adapters may additionally implement the optional capability interfaces
// cursorstate.Provider and lease.Provider (both shipped adapters do) and
// ReplicaProvider (pgstore, when a read replica is configured) — all resolved
// by servers through storage.As, so wrapping adapters stay transparent by
// implementing Unwrap.
package storage

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by lookups that reference a missing row.
var ErrNotFound = errors.New("storage: not found")

// Row is a stored entity row or tombstone.
type Row struct {
	Entity string
	Key    string
	// Serialized entity message. Empty when Deleted.
	Value []byte
	// Workspace sequence of the mutation that last wrote this row.
	Version uint64
	Deleted bool
	// Actor attributes the write that produced this state — including a
	// tombstone, which records who deleted the row. Every read that returns a
	// Row populates it; the zero Actor means the write was unattributed — an
	// unauthenticated caller made it, or an adapter stored it before it
	// recorded actors at all.
	Actor Actor
}

// Actor is who a write is attributed to. The engine resolves one per write from
// the calling context and hands it to PutRow/DeleteRow; the adapter stores it
// alongside the row and returns it on every read.
//
// Only the LAST writer of a row is recoverable. A row holds one Actor, replaced
// by each write, and the change feed coalesces by row — so this answers "who
// last touched this" and never "who touched it, in order". A true audit trail
// needs a separate log the engine does not keep.
//
// The zero Actor is "unattributed" and is what an adapter reads back for rows
// written before it recorded actors at all.
type Actor struct {
	// Subject is the verified caller, copied from the engine's Principal. Empty
	// when no authenticated caller made the write.
	//
	// It carries no issuer, so it is unique only to the extent the
	// Authenticator makes it so: a server admitting principals from more than
	// one issuer must give their subjects distinct namespaces, or two callers
	// collapse into one actor here.
	Subject string
	// System is the label a write made under the Trusted bypass gave itself,
	// empty exactly when a caller made the write. Bare Trusted records
	// "system", which a bypass may also name itself.
	//
	// It is an operator's label and never an identity: nothing verifies it and
	// no policy runs against it. Subject is recorded alongside it whenever the
	// ctx carried a principal, which is whose request the bypass ran under.
	System string
}

// AppSchema is a persisted runtime schema registration: the serialized
// google.protobuf.FileDescriptorSet an app registered, keyed by app id. It
// lives in a server-global catalog (no workspace/namespace partition — app_id
// is the partition) so the engine can reload and re-register every app's
// entities on boot before serving.
type AppSchema struct {
	AppID         string
	DescriptorSet []byte
	Revision      string
	UpdatedAt     time.Time
}

// IdempotencyRecord is the durable outcome of a trusted write that ran under an
// idempotency key (see the engine's Server.MutateIdempotent). It is what a retry
// of that write gets back instead of applying again.
type IdempotencyRecord struct {
	// Seq is the workspace sequence the original write took, or 0 when it wrote
	// nothing.
	Seq uint64
	// Result is the caller-defined payload recorded with the key — the response
	// an API hands back on a retry. Opaque to the engine and the adapter; empty
	// when the caller recorded none.
	Result []byte
}

// ClientRejection is the durable verdict on one mutation the server refused. A
// rejection is an ack — it bumps the client's watermark without writing
// anything — so without a record of it, a client that never received the push
// response gets a bare "already applied" on its retry and drops the edit with no
// explanation. The record is what lets the retry be answered with the original
// verdict instead.
//
// Records are stored per client alongside the watermark, so CollectIdleClients
// drops them with it.
type ClientRejection struct {
	// MutationID is the refused mutation's id, unique per client and never
	// reused, which is what makes a record safe to keep past the retry it was
	// written for.
	MutationID uint64
	// Code is the machine-readable rejection code (the engine's CodeConflict,
	// CodePermissionDenied, or one an app's own hook invented).
	Code string
	// Message is the human-readable reason, as the rejecting hook worded it.
	Message string
}

// ConsumerPosition is one effect consumer's durable progress through a
// workspace's change feed: the position it has fully acted on, in the same
// (Seq, Entity, Key) order ScanChanges pages by. Entity and Key are empty on a
// clean sequence boundary and name a sub-position inside a same-sequence group
// that a page split.
//
// It lives in the workspace's own store rather than in consumer-owned state so
// that advancing it commits in the SAME transaction as the shadows derived from
// the page it covers (see PutConsumerShadow). Two separate stores cannot do
// that, and the ordering only ever fails in one direction: a crash must
// redeliver a change, never skip one, because an effect has no reconcile pass
// to heal a missed trigger with.
//
// Consumer is the name the consumer registered under, and that name IS the
// identity of the position — renaming a consumer does not carry its progress
// over, it starts a first run.
type ConsumerPosition struct {
	Consumer string
	Seq      uint64
	Entity   string
	Key      string
	// UpdatedAt is when the position was last written. Writes ignore whatever
	// it holds — the adapter stamps its own clock, which is what
	// CollectIdleConsumers measures its cutoff against — and reads populate it.
	UpdatedAt time.Time
}

// Adapter is the storage seam. Per-workspace data access happens inside a
// transaction scoped to a single workspace; the schema catalog and Namespace
// derivation are server-global.
type Adapter interface {
	// Namespace returns a view of the adapter scoped to an app namespace,
	// partitioning workspaces, rows and client watermarks within one backing
	// store. The server-global schema catalog (PutAppSchema and friends) is
	// shared across namespaces. The default namespace is "". Close on a derived
	// view is a no-op — the root adapter owns the handle.
	Namespace(ns string) Adapter
	// PutAppSchema upserts an app's registered descriptor set and revision in
	// the server-global schema catalog (keyed by appID).
	PutAppSchema(ctx context.Context, appID string, set []byte, revision string) error
	// ListAppSchemas returns every persisted app schema, ordered by app id.
	ListAppSchemas(ctx context.Context) ([]AppSchema, error)
	// DeleteAppSchema removes an app's persisted schema. Removing a missing app
	// is not an error.
	DeleteAppSchema(ctx context.Context, appID string) error

	// WithReadTx runs fn inside a read-only snapshot of the workspace. fn is
	// invoked AT MOST ONCE: it may stream results out as it reads, and the
	// adapter must never retry it. A failed read tx returns the error.
	WithReadTx(ctx context.Context, workspaceID string, fn func(ReadTx) error) error
	// WithWriteTx runs fn inside a write transaction for the workspace and
	// commits iff fn returns nil.
	//
	// Mutual exclusion: while fn runs, no other write tx for the SAME
	// workspace may run — not even its reads. Adapters on MVCC databases
	// must acquire a workspace-level lock before invoking fn (the engine
	// reads, e.g. client watermarks, before its first write and relies on
	// those reads being stable until commit).
	//
	// Retries: the adapter MAY invoke fn multiple times (serialization
	// failure, deadlock). Callers must write fn to be re-runnable: reset any
	// captured state at the top of fn, and never apply side effects outside
	// the transaction until WithWriteTx returns nil.
	//
	// Registration: opening a write transaction must NOT by itself make the
	// workspace known to ListWorkspaceIDs. Adapters that keep per-workspace
	// bookkeeping (a sequence counter, a store epoch) must create it lazily,
	// on the transaction's first actual write. workspace_id is unauthenticated
	// wire input and plenty of write closures legitimately write nothing — a
	// read-only Mutate, a push whose every mutation was already applied — so
	// eager registration lets any caller mint permanent workspaces that every
	// background pass then iterates forever.
	WithWriteTx(ctx context.Context, workspaceID string, fn func(WriteTx) error) error
	// ListWorkspaceIDs returns the ids of all workspaces that hold persisted
	// state, i.e. those some write transaction has actually written to.
	//
	// Unpaged, and therefore for enumeration rather than for finding work: a
	// background pass asks one of the work queries below instead, so that its
	// cost tracks what is waiting rather than what exists.
	ListWorkspaceIDs(ctx context.Context) ([]string, error)
	// ListPendingWorkspaceIDs returns up to limit ids of workspaces where the
	// named consumer has changes it has not acted on, ordered by id and starting
	// strictly after `after` ("" from the start). A limit of zero or less is an
	// error rather than "unlimited": the caller pages precisely because the
	// answer can be every workspace.
	//
	// A workspace is pending when the consumer holds no position there (it has
	// acted on nothing), when its position's sequence is below the workspace's
	// current sequence, or when that position names an entity or key — a
	// sub-position inside a same-sequence group, which has changes after it even
	// once the sequence has reached the head. Only the clean boundary a drained
	// pass records means caught up.
	//
	// It exists so that a background consumer can ask "where is my work" in one
	// query instead of opening a read transaction per workspace to find out that
	// almost none of them have any. That is the difference between O(workspaces)
	// transactions per poll and one, and it is what lets a single process carry
	// a consumer across a large workspace count.
	//
	// The answer may be OPTIMISTIC — a returned workspace whose pending changes
	// are all for entities the consumer filters out has nothing for it to do —
	// but never permanently short: for a consumer relying on this to find work,
	// a workspace that stays omitted is an effect that never fires. Adapters may
	// answer from a bounded-staleness read view (a snapshot, a read replica), so
	// one call may miss a workspace that became pending shortly before it; the
	// workspace must appear once the view reaches that moment — for the polling
	// consumers this serves, a later call rather than not at all.
	ListPendingWorkspaceIDs(ctx context.Context, consumer, after string, limit int) ([]string, error)
	// ListCollectableWorkspaceIDs returns up to limit ids of workspaces holding
	// something a garbage-collection pass at these cutoffs would reclaim,
	// ordered by id and starting strictly after `after` ("" from the start). A
	// limit of zero or less is an error, as it is for ListPendingWorkspaceIDs
	// and for the same reason.
	//
	// It exists so that a maintenance pass costs one query rather than a write
	// transaction against every workspace that exists — on an adapter that takes
	// a per-workspace lock, an idle workspace otherwise pays a lock acquisition
	// and a transaction that finds nothing, once per interval, on every node.
	//
	// The answer may be OPTIMISTIC — a returned workspace whose only candidate
	// is collected by a concurrent pass leaves this one nothing to do — but
	// never short: a workspace that holds a candidate and stays omitted is state
	// that is never reclaimed. Adapters may answer from a bounded-staleness read
	// view on the same terms as ListPendingWorkspaceIDs, since a workspace that
	// only just became collectable is due on the next pass, not this one.
	ListCollectableWorkspaceIDs(ctx context.Context, cutoffs GCCutoffs, after string, limit int) ([]string, error)
	// ListStaleIndexWorkspaceIDs returns up to limit ids of workspaces whose
	// recorded index-build mark differs from mark, ordered by id and starting
	// strictly after `after` ("" from the start). A limit of zero or less is an
	// error.
	//
	// A workspace that has never been marked differs from every mark, so a fresh
	// one is always returned. The engine derives the mark from the fingerprints
	// of every declared index, which is what makes a schema change re-select
	// every workspace exactly once.
	//
	// It replaces reading each index's state per workspace: at a few thousand
	// workspaces and a handful of indexes apiece that is six figures of point
	// reads per interval per node to establish that nothing has changed, against
	// one indexed query that usually returns nothing.
	ListStaleIndexWorkspaceIDs(ctx context.Context, mark, after string, limit int) ([]string, error)
	// ListCompactableWorkspaceIDs returns up to limit ids of workspaces whose
	// text-compaction sweep has rows left to consider at this minPurged
	// threshold, ordered by id and starting strictly after `after` ("" from the
	// start). A limit of zero or less is an error.
	//
	// A workspace qualifies when its recorded mark sits below the field-delta
	// horizon — the sweep has not yet considered every row the delta prune has
	// quietened, which includes a mark AT the horizon that still names an
	// entity, since that is a sub-position inside a group with rows after it —
	// or when the mark was recorded under a HIGHER minPurged than
	// this one, which is the threshold being lowered: rows the earlier sweep
	// skipped may clear the new bar, so the workspace is swept again from the
	// beginning. Raising the threshold selects nothing, because a row skipped
	// under the old one is skipped under the new one too.
	//
	// The horizon only advances while text is being written and pruned, so a
	// workspace whose documents have gone quiet stops being returned — which is
	// what keeps this proportional to text still being edited rather than to
	// text that exists.
	//
	// The answer may be OPTIMISTIC — a returned workspace whose remaining rows
	// all fail the threshold costs one sweep that rewrites nothing — but never
	// short, on the same terms as the two work queries above.
	ListCompactableWorkspaceIDs(ctx context.Context, minPurged int, after string, limit int) ([]string, error)
	Close() error
}

// TextCompactMark is how far a workspace's text-compaction sweep has got: the
// (Seq, Entity, Key) position in ScanChanges order it has considered rows
// through, and the minPurged threshold it did so under.
//
// The position is a full sub-position rather than a bare sequence because the
// sweep pages by it. A mutation writing more rows than one batch holds puts
// several rows at one sequence, and a bare sequence cannot say where the batch
// split that group: naming the group's own sequence resumes past it and leaves
// the rest of its rows unconsidered, naming the one below resumes before the
// rows already swept and never gets past the group. Neither shows up as a
// failure — the sweep reports success either way.
//
// MinPurged is recorded so that lowering the server's threshold re-sweeps
// workspaces an earlier pass swept under a stricter one. The zero mark is a
// workspace no sweep has reached.
type TextCompactMark struct {
	// Seq, Entity and Key are an exclusive position in the (Version, Entity,
	// Key) order ScanChanges yields, and resume through it unchanged: an empty
	// Entity is a clean version boundary, a non-empty one resumes inside a
	// same-sequence group.
	Seq    uint64
	Entity string
	Key    string
	// MinPurged is the WithTextCompaction threshold the sweep that recorded this
	// mark ran under.
	MinPurged int
}

// GCCutoffs are the age thresholds one garbage-collection pass applies, as
// absolute timestamps rather than the retentions they were derived from. A
// zero timestamp selects nothing of its kind — the pass keeps that state
// whatever its age — which is how "unbounded retention" reaches an adapter.
//
// It exists for ListCollectableWorkspaceIDs. The collectors themselves take
// their cutoff one at a time, on a transaction the caller has already opened;
// this is the same set of thresholds asked across workspaces instead.
type GCCutoffs struct {
	// Tombstones bounds deleted rows, idle client watermarks and idle effect
	// consumers, which share one retention.
	Tombstones time.Time
	// IdempotencyKeys bounds claimed trusted-write keys.
	IdempotencyKeys time.Time
	// FieldDeltas bounds the collaborative-text delta log.
	FieldDeltas time.Time
	// Entities bounds LIVE rows of entities that expire by age rather than by
	// deletion (PurgeEntityRows), naming each one's own cutoff. Empty for a pass
	// that purges no entity wholesale.
	Entities []EntityCutoff
}

// EntityCutoff is one entity's age threshold for the wholesale purge of its
// live rows. Entity is a wire entity name as PurgeEntityRows takes it.
type EntityCutoff struct {
	Entity string
	Before time.Time
}

// As returns the first adapter in a's wrapper chain that implements T. It
// checks a itself, then follows Unwrap() Adapter methods until a match or the
// end of the chain. Use it instead of a direct type assertion when probing for
// an optional capability such as cursorstate.Provider: a wrapping adapter
// (instrumentation, caching) forwards the Adapter methods but not the
// capability interfaces of what it wraps, and exposes those by implementing
// Unwrap instead.
func As[T any](a Adapter) (T, bool) {
	for a != nil {
		if t, ok := any(a).(T); ok {
			return t, true
		}
		u, ok := a.(interface{ Unwrap() Adapter })
		if !ok {
			break
		}
		a = u.Unwrap()
	}
	var zero T
	return zero, false
}

// ScanOrder is the direction an index walk yields entries in.
type ScanOrder int

const (
	// ScanAsc yields entries in ascending key order, the zero value.
	ScanAsc ScanOrder = iota
	// ScanDesc yields them in descending key order.
	ScanDesc
)

func (o ScanOrder) String() string {
	if o == ScanDesc {
		return "desc"
	}
	return "asc"
}

// ReadTx is a consistent read view of one workspace.
//
// Its scan methods may be re-entered: a scan callback may issue any read on the
// same transaction, over the same snapshot the scan reads. The engine relies on
// this (ScanIndex re-reads rows, ScanChanges reads FieldDeltas), so an adapter
// that cannot interleave statements has to buffer — see the package contract.
type ReadTx interface {
	// CurrentSeq returns the workspace's last allocated sequence (0 if the
	// workspace has never been written).
	CurrentSeq() (uint64, error)
	// TombstoneHorizon returns the sequence below which tombstones may have
	// been garbage-collected. Cursors older than this require a reset.
	TombstoneHorizon() (uint64, error)
	// StoreEpoch returns a random identity minted by the workspace's first
	// actual write and stable thereafter — it names the store lineage, not the
	// physical file, so a byte copy (backup, clone) preserves it while an
	// independently-created store gets a different one. 0 for a workspace that
	// has never been written. Pull stamps it into every cursor so a cursor
	// presented to a store whose epoch differs (the URL now serves a swapped
	// or restored database) is recognized as foreign and reset, rather than
	// splicing two lineages' histories together — a failure the sequence
	// watermark cannot catch when the foreign store's seq sits above the
	// client's cursor.
	StoreEpoch() (uint64, error)
	// GetRow returns the row (or tombstone) for entity/key.
	// Returns ErrNotFound if the row has never existed.
	GetRow(entity, key string) (Row, error)
	// GetRows yields the rows (or tombstones) for keys of one entity, in the
	// order keys lists them. A key with no row is SKIPPED rather than reported
	// — the batch equivalent of ErrNotFound, and why there is no way to tell
	// which key a yielded row answers other than its own Key. A repeated key
	// yields its row once per occurrence; an empty keys yields nothing.
	//
	// The order is the contract. Callers resolve keys collected from an index
	// walk, whose output order is the row order they must produce, so an
	// adapter that yields storage order instead answers ordered queries wrongly
	// with no error anywhere.
	//
	// It exists because a point read per key is a round trip per key on a
	// networked adapter: the engine resolves index candidates in batches
	// through this so an indexed read costs round trips proportional to
	// batches, not to candidates. An adapter with no batch read may loop over
	// GetRow and lose nothing.
	//
	// fn may issue reads on this transaction, exactly as a scan callback may,
	// so an adapter must not hold a result set open across it.
	GetRows(entity string, keys []string, fn func(Row) error) error
	// ScanEntity yields all live (non-tombstone) rows of an entity in key
	// order. Keys compare by byte (Unicode code point) value — the order Go's
	// < gives — NOT a locale collation; afterKey paging shares this order with
	// the engine, so a divergent collation drops or repeats rows across a page
	// boundary. Used by bootstrap.
	ScanEntity(entity string, fn func(Row) error) error
	// ScanEntityAfter yields live rows of an entity with Key > afterKey, in
	// key order, at most limit rows (limit <= 0 means unlimited). Used by
	// paginated mutator scans.
	ScanEntityAfter(entity, afterKey string, limit int, fn func(Row) error) error
	// ScanEntityByVersionDesc yields live rows of an entity NEWEST FIRST —
	// Version descending, ties broken by Key ascending — at most limit rows
	// (limit <= 0 means unlimited). Used by recency-ordered subscription
	// windows, which want the workspace's most recently written rows of one
	// entity and nothing else.
	//
	// The tie-break is load-bearing, not cosmetic: a window takes the first
	// `limit` rows, so two rows written by the same mutation (hence sharing a
	// version) must be ordered the same way here as by the in-memory top-K
	// that this replaces, or the two disagree about which of them is in the
	// window.
	ScanEntityByVersionDesc(entity string, limit int, fn func(Row) error) error
	// ScanChanges yields rows (including tombstones) after a (Version, Entity,
	// Key) position, ordered by (Version, Entity, Key), at most limit rows
	// (limit <= 0 means unlimited). Used by pull.
	//
	// Entity and Key break version ties bytewise, the same order ScanEntity
	// uses, so the cursor's sub-position resumes identically across adapters.
	//
	// The position is exclusive. afterEntity == "" is a clean version
	// boundary: rows with Version > afterSeq. A non-empty afterEntity resumes
	// inside a same-version group: rows with Version > afterSeq, plus rows at
	// Version == afterSeq whose (Entity, Key) sorts after (afterEntity,
	// afterKey). The (Entity, Key) tie-break makes pagination total, so a page
	// boundary that splits a group of rows sharing one version (a batch, a
	// multi-row mutator) resumes at the remaining rows instead of dropping
	// them. Rows always carry a non-empty Entity, so "" is an unambiguous
	// "no sub-position" sentinel.
	ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(Row) error) error
	// ClientLastMutationID returns the per-client idempotency watermark
	// (0 if the client is unknown).
	ClientLastMutationID(clientID string) (uint64, error)
	// ClientRejections returns the verdicts recorded for clientID, ordered by
	// MutationID ascending. An unknown client, or one that has had nothing
	// refused, has none.
	ClientRejections(clientID string) ([]ClientRejection, error)
	// IdempotencyRecord returns what the trusted write that claimed key did, and
	// whether the key has been claimed at all. Keys are per workspace, exactly
	// like everything else this transaction sees.
	//
	// This is a SEPARATE ledger from the client watermarks above, not a reuse of
	// them: watermarks are contiguity-checked per client and pruned once a client
	// goes idle, neither of which an API caller's retry key can satisfy or
	// survive.
	IdempotencyRecord(key string) (rec IdempotencyRecord, found bool, err error)
	// ConsumerPosition returns one effect consumer's feed position and whether
	// it has one at all. An unknown consumer reads as (zero, false, nil): never
	// having acted is a first run, not an error.
	ConsumerPosition(consumer string) (pos ConsumerPosition, found bool, err error)
	// ListConsumerPositions returns every consumer position recorded for the
	// workspace, ordered by consumer name. It exists to report how far behind
	// each consumer is; no engine write path reads it.
	ListConsumerPositions() ([]ConsumerPosition, error)
	// ConsumerShadow returns the snapshot a consumer recorded for one row the
	// last time it acted on that row, or nil if it holds none — which is both
	// "this consumer has never seen this row" and "it keeps no shadow of it".
	//
	// The bytes are opaque: the consumer chooses what to keep (typically the
	// subset of fields it declared an interest in) and is the only thing that
	// decodes them.
	ConsumerShadow(consumer, entity, key string) ([]byte, error)
	// FieldDeltas returns the collaborative-text delta log for one field of one
	// row: the fragments written at sequences strictly above afterSeq, OLDEST
	// FIRST, so folding them in order reproduces the field's current state from
	// the state it held at afterSeq.
	//
	// complete reports whether that fold is trustworthy. It is false when the
	// span is not fully covered — the log was pruned past afterSeq, or a write
	// in the span recorded a gap marker (an empty fragment, written when the
	// new state was not expressible as a delta of the old). An incomplete span
	// returns no fragments: the caller must ship full state instead. Gap markers
	// are never returned as fragments.
	//
	// A row with no writes in the span yields (nil, true, nil) — "unchanged",
	// which is a meaningful answer, not an absence.
	FieldDeltas(entity, key, field string, afterSeq uint64) (frags [][]byte, complete bool, err error)
	// FieldDeltaHorizon returns the sequence the delta log has been pruned
	// through — the value the last PruneFieldDeltas advanced it to, 0 for a
	// workspace whose log has never been pruned. It never regresses.
	//
	// Text compaction reads it to bound what it may rewrite: a row last written
	// at or below the horizon is one the server has already stopped being able
	// to send as a cheap delta, which is what makes it safe to compact (see
	// engine/server's compact.go).
	FieldDeltaHorizon() (uint64, error)
	// TextFloor returns the compaction floor recorded for one collaborative-text
	// field of one row: the encoded version vector the field's document held
	// when its tombstones were last purged (see engine/text.Compact). Nil for a
	// field that has never been compacted, which is most of them.
	//
	// Unlike the delta log this is NOT an optimization: losing a floor lets a
	// replica holding pre-compaction state put purged tombstones back. An
	// adapter must store it as durably as the row itself.
	TextFloor(entity, key, field string) ([]byte, error)
	// TextCompactMark returns how far the workspace's text-compaction sweep has
	// got (see Adapter.ListCompactableWorkspaceIDs). The zero mark means it has
	// never run, which is both "compaction is off here" and "it has not reached
	// this workspace yet"; the sweep treats them the same and starts from the
	// beginning.
	TextCompactMark() (TextCompactMark, error)
	// ScanIndex yields the entries one ENTITY holds in one secondary index whose
	// key falls in [lo, hi), at most limit entries (limit <= 0 means
	// unlimited). A nil lo starts at the beginning; a nil hi runs to the end.
	//
	// order is the direction of the yield. The bounds keep their meaning under
	// both — lo inclusive, hi exclusive — so ScanDesc yields the exact reverse
	// of what ScanAsc yields over the same range, and only the end limit takes
	// from moves: from lo under ScanAsc, from hi under ScanDesc. The engine
	// reads backwards to page a query from its `before` cursor.
	//
	// entity is part of the identity of an index, not a filter over a shared
	// one. Index names are derived from field names, so two entities routinely
	// claim the same name, and an entry carries a row key that only means
	// anything in its own entity's keyspace — an adapter that scanned by name
	// alone would hand one entity's row keys to the other's reader, which would
	// then read live rows of its own that the query never matched.
	//
	// entry is the stored key and rowKey the row it points at. The entry bytes
	// are opaque: the adapter compares them BYTEWISE and never interprets them.
	// That is the whole contract — the engine builds keys whose byte order is
	// the row order it wants (engine/query.IndexEncoder), so an adapter that
	// applies a collation instead returns rows in the wrong order.
	//
	// fn must not assume the row still exists: it does, within this snapshot,
	// but the caller re-reads it anyway to apply read filters — from INSIDE
	// this callback, which is why a scan has to tolerate re-entrant reads.
	ScanIndex(entity, index string, lo, hi []byte, limit int, order ScanOrder, fn func(entry []byte, rowKey string) error) error
	// IndexState returns the spec fingerprint one entity's stored entries in an
	// index were built under and whether the build finished. A missing index
	// reads as ("", false, nil) — not an error, because an index the engine has
	// not built yet is the normal state after registration.
	//
	// Readiness is what makes index use optional: entries only cover the whole
	// entity once the build says so, and until then every read falls back to a
	// scan. It is per (entity, index) for the reason ScanIndex is: two entities
	// sharing a name have separate entries, built separately, and a single
	// shared flag would let either claim coverage of the other.
	IndexState(entity, index string) (specHash string, ready bool, err error)
}

// WriteTx extends ReadTx with mutations. All writes become visible
// atomically on commit.
type WriteTx interface {
	ReadTx
	// NextSeq allocates and returns the next workspace sequence. Allocation
	// happens inside the transaction: concurrent writers to the SAME
	// workspace serialize; different workspaces need not.
	NextSeq() (uint64, error)
	// PutRow stores a live row snapshot at the given version, attributed to
	// actor (see Row.Actor). The actor is stored as given — a zero Actor
	// records "unattributed" and must not be filled in from anything the
	// adapter knows.
	PutRow(entity, key string, value []byte, version uint64, actor Actor) error
	// DeleteRow writes a tombstone at the given version, attributed to actor:
	// a tombstone records who deleted the row.
	DeleteRow(entity, key string, version uint64, actor Actor) error
	// SetClientLastMutationID bumps the idempotency watermark.
	SetClientLastMutationID(clientID string, mutationID uint64) error
	// SetClientRejections replaces every verdict recorded for clientID with recs
	// (see ReadTx.ClientRejections). An empty slice clears them. The engine
	// bounds what it stores, so an adapter may keep the slice whole.
	//
	// It creates the client's watermark row if it does not exist yet, which also
	// refreshes what CollectIdleClients considers idle — the same coupling
	// SetClientLastMutationID has, and for the same reason: the two halves of a
	// client's state must expire together.
	SetClientRejections(clientID string, recs []ClientRejection) error
	// InSavepoint runs fn inside a nested savepoint. If fn returns an error,
	// every write fn made (including allocated sequences) rolls back and the
	// error propagates; on nil the savepoint is released into the enclosing
	// transaction. Savepoints may nest.
	InSavepoint(fn func() error) error
	// CollectTombstones deletes tombstones last written before cutoff and
	// advances the workspace's tombstone horizon to the highest version it
	// deleted. The horizon never regresses. Returns the resulting horizon.
	CollectTombstones(cutoff time.Time) (horizon uint64, err error)
	// PurgeEntityRows deletes rows of ONE entity — LIVE rows included — together
	// with the secondary-index entries those rows occupied, and returns how many
	// rows were removed and whether the selection may still hold more. It leaves
	// no tombstone and does not move the tombstone horizon.
	//
	// Three narrowings, all optional and all applied:
	//
	//   - keyPrefix keeps only rows whose key starts with it, compared BYTEWISE
	//     like every other key comparison here (see ScanEntity). Empty matches
	//     every row of the entity.
	//   - before keeps only rows last written strictly before it. The ZERO time
	//     means NO age bound — not "before the epoch" — so a caller that wants
	//     only the prefix passes time.Time{}.
	//   - limit caps how many rows ONE call removes; <= 0 removes every match at
	//     once. WHICH matching rows a bounded call takes is unspecified — only
	//     that it takes some and reports the rest through more, so a caller that
	//     keeps calling drains the selection.
	//
	// more means matching rows MAY remain, and an adapter may answer it from
	// nothing better than "this call hit its limit": a call that removed exactly
	// the last limit rows is allowed to report more, and the next one then removes
	// nothing and reports false. Only false is definite, and it always arrives,
	// because the predicate is fixed while rows only leave the selection. An
	// unbounded call never reports more.
	//
	// This is the only delete in the interface that removes a live row outright,
	// and it is sound only for an entity no client ever receives — the engine's
	// server_only entities, of which the field-history log is the one that ships.
	// Used on anything a client syncs it would silently delete rows every replica
	// still holds, with no tombstone to carry the deletion and no way to notice.
	//
	// Implement it as a bulk statement rather than a scan plus DeleteRow: a
	// DeleteRow per expired record would mint a tombstone per record, which is
	// what this exists to avoid. Index entries must go with the rows in the same
	// statement — the engine has no row keys in hand here to pair a
	// DeleteIndexEntries with, and an entry outliving its row leaks.
	//
	// The engine calls it two ways: the GC pass expires old records in bounded
	// chunks it drains across transactions (an age bound, no prefix, a limit), and
	// a DELETE drops the records describing the row it is removing (a prefix, no
	// age bound, no limit — those records must go with the row in its own
	// transaction or not at all). Matching nothing is not an error.
	PurgeEntityRows(entity, keyPrefix string, before time.Time, limit int) (removed int, more bool, err error)
	// CollectIdleClients deletes the workspace's per-client watermark rows
	// not updated since cutoff and returns how many were removed. A pruned
	// client that later returns re-enters via watermark adoption (see the
	// server push path), so cutoff must lag the longest plausible client
	// offline window.
	CollectIdleClients(cutoff time.Time) (removed int, err error)
	// PutIdempotencyRecord claims key for this transaction's write, recording
	// what it did (see ReadTx.IdempotencyRecord). It is an unconditional upsert,
	// so a retried write closure lands the same record rather than failing on
	// its own earlier attempt.
	//
	// The claim is only as durable as this transaction: a closure that rolls back
	// leaves the key unclaimed and the write retryable, which is the intended
	// coupling — the ledger must never record an outcome the store did not keep.
	PutIdempotencyRecord(key string, rec IdempotencyRecord) error
	// CollectIdempotencyKeys deletes the workspace's idempotency records last
	// written before cutoff and returns how many were removed. Unlike tombstones,
	// dropping a key is silent: the retry it would have absorbed simply applies
	// again, so cutoff must lag the longest retry window any caller uses.
	CollectIdempotencyKeys(cutoff time.Time) (removed int, err error)
	// SetConsumerPosition records a consumer's feed position, stamping the
	// adapter's own clock over p.UpdatedAt. An unconditional upsert, so a
	// retried write closure lands the same position rather than failing on the
	// one its own earlier attempt wrote.
	SetConsumerPosition(consumer string, p ConsumerPosition) error
	// PutConsumerShadow records a consumer's snapshot of one row (see
	// ReadTx.ConsumerShadow), replacing any previous one.
	//
	// This and SetConsumerPosition are written in ONE transaction by the effects
	// runtime, which is the whole reason both live here rather than in a side
	// store. A shadow that advanced without its position makes the redelivered
	// change compare equal to it, so the consumer sees "nothing changed" and
	// skips a trigger it was supposed to fire — a silent loss, where the
	// reverse order costs only a duplicate the at-least-once contract already
	// promises.
	PutConsumerShadow(consumer, entity, key string, snapshot []byte) error
	// DeleteConsumerShadow drops one row's shadow for one consumer. Deleting a
	// shadow that does not exist is not an error.
	DeleteConsumerShadow(consumer, entity, key string) error
	// DeleteConsumer removes a consumer's position and every shadow it holds in
	// this workspace, so its next pass is a first run. This is how a consumer
	// resets after falling below the tombstone horizon: the changes its shadows
	// describe are gone, so the shadows describe a past nothing can line up
	// against.
	DeleteConsumer(consumer string) error
	// CollectIdleConsumers deletes consumers that are BOTH stale (position last
	// written before cutoff) and stranded (position strictly below belowSeq,
	// which the engine passes as the tombstone horizon), together with their
	// shadows, and returns how many were removed.
	//
	// Both conditions are load-bearing. Staleness alone would collect a live
	// consumer that is merely caught up — a consumer with nothing to do writes
	// nothing, so its position ages while it is perfectly healthy. The horizon
	// test is what turns "idle" into "gone": a position below it has already
	// missed collected tombstones and will be reset on its owner's next pass, so
	// removing the row only makes explicit what already happened. A caught-up
	// consumer sits at the current sequence, never below the horizon, and
	// survives however long it idles.
	//
	// The consequence is that a workspace which has never collected a tombstone
	// keeps a retired consumer's row indefinitely (horizon 0, nothing below it).
	// That is a bounded leak — one row per consumer name ever used — and the
	// cure is to delete the consumer explicitly rather than to widen this.
	CollectIdleConsumers(cutoff time.Time, belowSeq uint64) (removed int, err error)
	// PutFieldDelta records one entry of a text field's delta log at seq: the
	// fragment carrying the state written at seq beyond the state before it. An
	// EMPTY frag is a gap marker — "this write changed the field but produced no
	// usable delta" — which makes every span containing it incomplete. Writes
	// that leave the field byte-identical record nothing.
	//
	// Entries are per (entity, key, field, seq) and overwrite on conflict, so a
	// retried write closure is idempotent.
	PutFieldDelta(entity, key, field string, seq uint64, frag []byte) error
	// PruneFieldDeltas deletes delta-log entries written before cutoff and
	// advances the workspace's field-delta horizon to the highest sequence it
	// deleted, which is what makes later FieldDeltas calls report an
	// under-covered span as incomplete. The horizon never regresses. Returns the
	// resulting horizon.
	PruneFieldDeltas(cutoff time.Time) (horizon uint64, err error)
	// PutTextFloor records a text field's compaction floor (see
	// ReadTx.TextFloor), replacing any previous one; an empty vv clears it.
	// Successive floors only grow — each compaction observes at least what the
	// last one did — so a write is an unconditional upsert.
	//
	// Clearing is what a DELETE of the row does: the document is gone, so there
	// is nothing left to resurrect, and a row re-created later starts a fresh
	// document whose ids would otherwise be measured against a floor from a
	// document they have nothing to do with.
	PutTextFloor(entity, key, field string, vv []byte) error
	// PutIndexEntries REPLACES the whole set of index entries one row occupies:
	// every entry previously stored for (entity, key) is removed, then entries
	// is stored. Passing an empty slice therefore clears the row's entries,
	// which is what DeleteIndexEntries does by name.
	//
	// Replace-set rather than add/remove because the caller never has the row's
	// PREVIOUS value in hand — a PATCH merges into stored bytes, and a DELETE
	// has nothing but a key — so computing a diff would put a read and a decode
	// on every write path. An adapter needs a by-row lookup to make this cheap.
	PutIndexEntries(entity, key string, entries []IndexEntry) error
	// DeleteIndexEntries removes every index entry of one row, by key alone.
	// Called when the row is deleted: index entries exist only for LIVE rows.
	//
	// Deleting entries for a row that has none is not an error.
	DeleteIndexEntries(entity, key string) error
	// SetIndexState records the spec fingerprint one entity's entries in an index
	// are being built under, and whether the build has finished (see
	// ReadTx.IndexState). An unconditional upsert: a rebuild re-stamps the row
	// rather than failing on the state its own earlier attempt wrote.
	SetIndexState(entity, index, specHash string, ready bool) error
	// SetIndexBuildMark records the opaque mark under which every index of
	// every entity in this workspace is built and ready (see
	// Adapter.ListStaleIndexWorkspaceIDs). The mark is compared for equality
	// and nothing else; adapters must store and return it verbatim.
	//
	// It UPDATES an existing workspace and creates nothing: a workspace with no
	// persisted state has no rows to index, so marking one is a no-op rather
	// than the registration WithWriteTx is forbidden to perform. It gains its
	// mark from the first pass after its first real write.
	SetIndexBuildMark(mark string) error
	// SetTextCompactMark records how far the text-compaction sweep has got in
	// this workspace (see Adapter.ListCompactableWorkspaceIDs). The sweep writes
	// one per batch it commits, so the mark and the rows that batch rewrote
	// become visible together and an interrupted sweep resumes where it stopped
	// rather than redoing the workspace.
	//
	// It UPDATES an existing workspace and creates nothing, exactly as
	// SetIndexBuildMark does and for the same reason: a workspace with no
	// persisted state has no text to compact.
	SetTextCompactMark(mark TextCompactMark) error
}

// IndexEntry is one position a row occupies in one secondary index.
//
// Index is the index's name, unique within the ENTITY the entry's row belongs
// to (PutIndexEntries carries that entity) and scoped per workspace like
// everything else a transaction sees. Key is the entry's sort key: opaque bytes
// the adapter stores and compares bytewise, produced by
// engine/query.IndexEncoder so that byte order equals the row order the index
// serves.
//
// One row may hold SEVERAL entries in the same index: an index over a repeated
// field stores one position per element, which is what makes "some assignee is
// Maya" a range read. So (entity, index, key) identifies an entry and (entity,
// row key) does not — an adapter keyed on the latter would keep only the last
// element written. Every key ends with the row key, so two entries of one row
// are always distinct.
//
// An index is DERIVED, DISPOSABLE and never on the wire. Losing every entry
// costs a rebuild, never correctness, and nothing in a request or response
// mentions an index — which is what lets an adapter treat this table as
// regenerable and, if it must, drop it.
type IndexEntry struct {
	Index string
	Key   []byte
}
