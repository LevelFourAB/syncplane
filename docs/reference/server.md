# Go server API

Package `github.com/levelfourab/syncplane/engine/server`. This page lists the
surface; the package docs at the point of code are authoritative for details.

## Construction

```go
store, _ := sqlitestore.Open("app.db")
defer store.Close()                            // the server does not own the store

srv, err := server.New(store,
    server.WithEntities(&todov1.Todo{}),       // reads (syncplane.v1.entity) options
    server.WithMutator("complete_all", &todov1.CompleteAllArgs{}, completeAll),
    server.WithAuthenticator(myAuth{}),        // optional, default anonymous
    server.WithPermissions(myPermissions{}),   // optional, default allow-all
    server.WithLimits(server.Limits{           // optional; non-positive fields keep defaults
        MaxMutationsPerPush:     256,
        MaxWorkspacesPerRequest: 64,
        MaxValueBytes:           1 << 20,
        MaxFieldMaskPaths:       64,
        MaxPullChanges:          4096,
    }),
    server.WithChunkSize(256),                 // bootstrap chunk rows
    server.WithGC(30*24*time.Hour, time.Hour), // retention, background interval
)
if err != nil {
    log.Fatal(err)                             // a bad schema or mutator declaration
}
defer srv.Close()                              // stops background work (not the store)

http.ListenAndServe(addr, srv.HTTPHandler())   // HTTP/1.1 suffices
```

`New` installs everything the options declare before it returns, so a whole
schema costs one error check and nothing starts unless all of it is good. The
declaration options — `WithEntities`, `WithMutator`, `WithMerger` — are applied
in dependency order rather than in the order written, so a `WithMerger` may
name an entity a later `WithEntities` declares.

**Store ownership stays with the caller.** `srv.Close()` stops the engine's
background work — the GC pass, the index backfill, the text-compaction sweep and
the notify-coalescing timers, so a shared notifier receives no trailing publish after shutdown. It
does not close what the caller owns: neither the adapter, which may outlive the
server (and one adapter backs every app a
[Manager](../how-to/host-multiple-apps.md) hosts), nor a notifier installed
with `WithNotifier`. Two defers, in that order.

`srv.HTTPHandler()` returns the service already mounted at its own path. Use
`path, handler := srv.Handler()` to mount it on a mux carrying the app's own
routes — Connect routes by full method path, so the service cannot live at an
arbitrary root.

## Options

| Option                                      | Default      | Effect                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithAuthenticator(a)`                      | anonymous    | Turns request headers into a `Principal` before every sync RPC.                                                                                                                                                                                                                                                                               |
| `WithPermissions(p)`                        | `AllowAll`   | Workspace access, write and per-row read policy.                                                                                                                                                                                                                                                                                              |
| `WithLimits(l)`                             | see below    | Request bounds; non-positive fields keep their default.                                                                                                                                                                                                                                                                                       |
| `WithChunkSize(n)`                          | 256          | Bootstrap chunk size in rows.                                                                                                                                                                                                                                                                                                                 |
| `WithHeartbeat(d)`                          | 30s          | `Subscribe` heartbeat; also how often a live stream re-checks access.                                                                                                                                                                                                                                                                         |
| `WithNotifyCoalescing(w)`                   | —            | Coalesces commit notifications; 0 disables.                                                                                                                                                                                                                                                                                                   |
| `WithNotifier(n)`                           | in-process   | Replaces the in-process poke fan-out with one shared across nodes — `pgnotify` ships such an implementation over Postgres `LISTEN`/`NOTIFY`. A replacement should implement `pubsub.Activity` too, or background consumers lose the signal that announces a new workspace and fall back to listing on a timer.                                |
| `WithCursorStateStore(st)`                  | adapter      | Where the membership tail of issued cursors lives, so the wire carries a reference instead of restating it. Defaults to a store the adapter provides (both shipped adapters do); `nil` disables the indirection and every cursor inlines its sets. See [Cursor state](#cursor-state).                                                         |
| `WithLeases(l)`                             | adapter      | The `lease.Locker` per-workspace background work dedups through: the GC, index and text-compaction sweeps, and any effects runtime built on the server, skip a workspace whose lease another process holds. Defaults to a locker the adapter provides (both shipped adapters do), falling back to a process-local one. See [Leases](#leases). |
| `WithGC(retention, interval)`               | —            | Tombstone retention and background pass interval; `interval <= 0` is manual-only.                                                                                                                                                                                                                                                             |
| `WithTextDeltaRetention(d)`                 | 1h           | How long the collaborative-text delta log is kept.                                                                                                                                                                                                                                                                                            |
| `WithIdempotencyRetention(d)`               | 24h          | How long claimed `MutateIdempotent` keys are kept.                                                                                                                                                                                                                                                                                            |
| `WithHistoryRetention(d)`                   | 90d          | How long field-history records are kept; `HistoryForever` disables time-based expiry. See [Field history](#field-history).                                                                                                                                                                                                                    |
| `WithTextCompaction(minPurged)`             | off          | Opt-in purge of unneeded text tombstones and mark ops, on a sweep of its own. See [Text compaction](#text-compaction).                                                                                                                                                                                                                        |
| `WithTextCompactionPacing(interval, batch)` | 5m, 200 rows | How often the compaction sweep looks for newly quiet rows, and the most rows one of its transactions considers. `batch` is a ceiling, not a target: a transaction also ends once the rows it has taken exceed a fixed byte budget.                                                                                                            |
| `WithTextCache(budgetBytes)`                | 8 MiB        | Keeps written text documents decoded between writes; 0 disables.                                                                                                                                                                                                                                                                              |
| `WithSchemaRevision(rev)`                   | —            | Advertises an app-defined revision on every frame, push, pull, query and bootstrap.                                                                                                                                                                                                                                                           |
| `WithLogger(l)`                             | —            | An `*slog.Logger`.                                                                                                                                                                                                                                                                                                                            |
| `WithTracerProvider(tp)`                    | global       | OpenTelemetry tracer provider; the global one is no-op until an SDK is installed. See [Telemetry](telemetry.md).                                                                                                                                                                                                                              |
| `WithMeterProvider(mp)`                     | global       | OpenTelemetry meter provider; same fallback.                                                                                                                                                                                                                                                                                                  |
| `WithEntities(msgs...)`                     | —            | Declares syncable entities; repeatable.                                                                                                                                                                                                                                                                                                       |
| `WithMutator(name, args, fn)`               | —            | Declares a server-authoritative named mutation.                                                                                                                                                                                                                                                                                               |
| `WithMerger(proto, path, fn)`               | —            | Declares a merge hook for an opaque field.                                                                                                                                                                                                                                                                                                    |
| `WithHistoryFilter(fn)`                     | —            | Admits, rejects or rewrites every field-history record. See [Field history](#field-history).                                                                                                                                                                                                                                                  |
| `WithHistorySession(d)`                     | 5m           | How long one row's field-history record stays open to later writes by the same actor; `HistorySessionOff` records every write. See [Field history](#field-history).                                                                                                                                                                           |
| `WithPresenceBus(b)`                        | in-process   | Fan-out for the ephemeral plane. The default reaches only this process; a multi-node deployment installs a shared one (`engine/presence/pgpresence`). See [Rooms](#rooms).                                                                                                                                                                    |
| `WithRoomAnnounce(d)`                       | 5s           | How often a node re-announces the presence records it is responsible for, and so how quickly a session that vanished without saying so disappears — three intervals. See [Rooms](#rooms).                                                                                                                                                     |

### Limits

Every field defaults when left non-positive.

| Field                     | Default | Bounds                                                                                                                                                                                                                                                                                |
| ------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MaxRequestBytes`         | 16 MiB  | Decompressed request body, enforced before parsing. The outer wall.                                                                                                                                                                                                                   |
| `MaxMutationsPerPush`     | 256     | Mutations per `Push`, across all its per-workspace batches.                                                                                                                                                                                                                           |
| `MaxWorkspacesPerRequest` | 64      | Push batches / `Subscribe` scopes per request.                                                                                                                                                                                                                                        |
| `MaxValueBytes`           | 1 MiB   | Serialized entity value (`crud.value` / `custom.args`).                                                                                                                                                                                                                               |
| `MaxFieldMaskPaths`       | 64      | Field-mask paths per PATCH.                                                                                                                                                                                                                                                           |
| `MaxPullChanges`          | 4096    | Row changes per `Pull` response; larger results paginate via `has_more`.                                                                                                                                                                                                              |
| `MaxQueryRows`            | 1024    | Rows per `Query` response.                                                                                                                                                                                                                                                            |
| `MaxQueryOffset`          | 16384   | `Query` offset; beyond it is rejected rather than clamped. Offset is the fallback for jumping to an arbitrary rank — `after`/`before` are the default for sequential paging — and it bounds the in-memory working set a scan retains. Setting it alongside either cursor is rejected. |
| `MaxQueryCursorBytes`     | 4096    | Length of an `after`/`before` cursor. A cursor's length follows the order-key values it encodes; the cap bounds the work a malformed one can cost, since validating it walks it component by component.                                                                               |
| `MaxFollowDepth`          | 4       | Rounds a `Subscription.follow` may re-apply its path set.                                                                                                                                                                                                                             |
| `MaxClosureSize`          | 10000   | Rows in one request's reference-following closure.                                                                                                                                                                                                                                    |
| `MaxReverseFanOut`        | 1000    | Source rows one reverse-follow hop may match.                                                                                                                                                                                                                                         |
| `MaxWindowSize`           | 1000    | Rows a single `Subscription.window` may hold. Must stay well under `MaxPullChanges`: a window that swaps wholesale emits up to 2N changes in one page.                                                                                                                                |
| `MaxIndexEntriesPerRow`   | 1024    | Secondary-index entries one row may occupy across its entity's indexes — the per-write cost of indexing that entity. An index over a repeated field stores one entry per element, so this is also the ceiling on how long an indexed list may get. Exceeding it fails the write.      |
| `MaxIndexScanCandidates`  | 10000   | Index entries one range scan may walk while a read filter rejects candidates. Overflow falls back to a scan rather than failing.                                                                                                                                                      |
| `MaxRelConditions`        | 8       | `matches()` conditions one query predicate may carry. Each crosses into another entity and is answered by its own pass over that entity before the query runs, so the count multiplies the price of the request. Conditions selecting the same target rows share one pass.            |
| `MaxRelKeys`              | 50000   | Target rows one `matches()` may select — the size of the membership test it collapses to. Exceeding it fails the query rather than truncating, since a truncated key set would silently drop rows that match.                                                                         |
| `MaxRoomsPerStream`       | 64      | Rooms one `Watch` stream may carry, and separately the sessions it may keep alive on other tabs' behalf. Each costs a bus subscription, a join gate, and — when anchored — a row read per re-gate tick.                                                                               |
| `MaxPresenceStateBytes`   | 4 KiB   | One participant's presence state. Unlike a row it costs per announce interval rather than per write, and it has to fit whatever the presence bus can carry (a Postgres NOTIFY payload caps at 8000 bytes, base64 included).                                                           |
| `MaxBroadcastBytes`       | 4 KiB   | One broadcast message, under the same bus ceiling.                                                                                                                                                                                                                                    |
| `MaxPublishItems`         | 32      | Items in one `Publish` request.                                                                                                                                                                                                                                                       |
| `MaxParticipantsPerRoom`  | 128     | Records a room keeps. Beyond it the room keeps the lowest session ids, reports its snapshot truncated and turns further sessions away — lowest rather than newest so two nodes serving the same over-full room show the same people.                                                  |

Raising `MaxMutationsPerPush` or `MaxValueBytes` means raising
`MaxRequestBytes` in tandem.

Every limit bounds one request or one stream. None bounds request rate or how
many streams a caller holds — see
[the default posture is open](../explanation/limits-and-trade-offs.md#the-default-posture-is-open).

Crossing any of these produces an error naming what was counted, the field it
crossed with the value in force, and the option that moves it — so the message
is enough on its own:

```
push of 300 mutations, over MaxMutationsPerPush (256); raise it with server.WithLimits
```

## Error messages

Which audience an error is for decides how it reads, and the prefix is what
tells them apart.

Errors returned from a **Go call** — `RegisterEntity`, `RegisterMerger`,
`EntityKey`, the `ViewTx`/`ApplyTx`/`EntityTx` methods, the options `New`
applies — are prefixed `server:` and may name Go identifiers, because the
reader is holding this API:

```
server: syncplane.example.v1.Todo is not a registered entity; register it first
with server.WithEntities(&Todo{}) or Server.RegisterEntity
```

Errors returned **over the wire** carry no prefix: a Go package name is noise in
an RPC status, and the reader may not be writing Go at all. They are Connect
errors, and the machine-readable code rides in metadata rather than the text —
see [sync status and codes](sync-status-and-codes.md).

Either way an error that reports a bound names the knob that moves it, and one
about an unregistered entity names the call that registers it. Match on codes,
never on message text: the wording here is documentation, not contract.

## Garbage collection

One GC pass prunes expired tombstones, idle client watermark rows, the
collaborative-text delta log, claimed trusted-write idempotency keys and
field-history records. `srv.CollectGarbage(ctx, workspaceID)` runs it manually.
Compacting collaborative text is a separate sweep — see
[Text compaction](#text-compaction).

Keep the tombstone retention comfortably above the longest client offline
window; a pruned client that returns is re-admitted at whatever mutation id it
pushes next. The four retentions answer different questions:

- **Tombstones** (`WithGC`) — bounded by how long a client may be offline.
  Too short forces a re-bootstrap.
- **Text delta log** (`WithTextDeltaRetention`) — an optimization only. Pruning
  early costs wire size and nothing else.
- **Idempotency keys** (`WithIdempotencyRetention`) — bounded by callers' retry
  windows. **Too short is a correctness problem**: a retry arriving after its
  key expired applies again.
- **Field history** (`WithHistoryRetention`) — bounded by how far back you want
  to be able to restore. Unlike the delta log it is not clamped to the tombstone
  retention: history is app-visible data with its own lifetime, so setting it
  either side of the tombstone window is fine. A record ages from the last write
  folded into it rather than from its `recorded_at`, so it outlives what its
  timestamp says by at most one [session window](#editing-sessions).

History records are **hard-deleted**, leaving no tombstone — nothing syncs them,
so nothing holds a copy to be told they are gone.

They are also the one thing the pass collects in **chunks**. Everything else it
touches is bounded by rows; history is bounded by writes, so the first sweep
after enabling it — or after raising retention — is the largest delete the engine
issues. The pass takes a bounded chunk inside its own transaction and drains the
rest a chunk per transaction, so no single delete holds a write open across the
whole backlog. An interrupted drain needs no recovery: the sweep keeps no state,
and the next pass resumes on what is left.

### History that lives as long as its row

`WithHistoryRetention(server.HistoryForever)` turns time-based expiry off. A
record is then bounded only by its row: **deleting a row purges its history**,
in every retention mode, in the same transaction as the delete.

```go
server.WithGC(30*24*time.Hour, time.Hour),
server.WithHistoryRetention(server.HistoryForever),
```

The cascade runs whatever the retention: nothing can use a deleted row's history
— a restore is a PATCH, and a PATCH of a tombstoned row is a conflict under
delete-wins — and keeping it would attach the old row's log to a new row that
later claims the same key.

It also survives an entity **ceasing** to opt in. Registration is additive, so
dropping `history: {}` in a later deploy leaves everything already captured, and
the cascade keeps running over it.

Drop the **last** `history: {}` and the history entity leaves the catalog
entirely: the records that remain are no longer readable, and deleting their
source rows no longer purges them. The expiry sweep still runs — it is not gated
on the catalog, because records are durable state and catalog membership is not
— so retention drains what is left on its own. Under `HistoryForever` there is
no sweep to drain them, so records left behind that way stay until something
purges them.

The default is a finite 90 days. `HistoryForever` suits entities whose rows are
edited by people; on rows machinery rewrites in a loop, the log grows without
bound for as long as the row lives.

## Text compaction

A collaborative-text field accumulates one item per character ever typed, so a
document edited over months grows without bound.
`WithTextCompaction(minPurged)` opts a server into reclaiming the tombstones
nothing can still refer to; `srv.CompactText(ctx, workspaceID)` runs the sweep
manually and reports how many rows it rewrote.

```go
server.WithGC(30*24*time.Hour, time.Hour),
server.WithTextCompaction(64),
server.WithTextCompactionPacing(5*time.Minute, 200), // optional
```

It is a **separate sweep from GC**, on its own interval and its own
per-workspace lease. The two collect different kinds of thing: GC's collectors
are deletes over indexed ranges, while compaction scans, decodes and rewrites
rows. Sharing a transaction would make the cheap half wait behind the expensive
one, and on an adapter taking a per-workspace lock, so would every writer.

Compaction depends on GC even so, and does nothing without it: it only rewrites
rows the delta prune has already **quietened** — those whose last write is at or
below the sequence the text delta log has been pruned through, which is where
the server has already stopped being able to send a catching-up client a cheap
delta. With GC on manual, no horizon moves and the background sweep does not
run.

Each pass sweeps the window between the last pass's position and the current
horizon, so it reads what has been **edited** rather than what exists: a
workspace whose documents have gone quiet stops being visited at all. Within a
window it works in transactions of `batch` rows, resuming from a recorded
position, so no single transaction holds a workspace's write lock for a full
entity scan. An interrupted sweep needs no recovery — a rewrite that happens
twice costs a sequence and nothing else.

Two consequences worth planning for:

- **A rewrite lands at a fresh sequence**, so every client re-fetches that field
  once. That is the price of the reclaim. A paced sweep spreads those re-fetches
  over several sequences rather than one.
- **It is the one thing in the engine that can refuse a client's text write
  outright.** A client still holding pre-compaction state keeps working until it
  writes text anchored to something purged; that push is refused with
  `CodeTextStateStale` and the client re-applies its edit against fresh state.

`minPurged` is how many items a document must give up before its row is worth
rewriting. **Lowering it re-sweeps every workspace from the beginning**, since
rows an earlier pass skipped may clear the lower bar; raising it does not,
because a row skipped under the old threshold is skipped under the higher one
too.

Enable it for workspaces whose documents are edited over months. Leave it off if
documents are short-lived — the reclaim buys nothing and the refusal above is
pure cost. See [the text explanation](../explanation/collaborative-text.md).

## Registration

Schemas known when the binary is built are declared as options:

```go
server.WithEntities(&todov1.Todo{}, &todov1.Note{})
server.WithMutator(name string, args proto.Message, fn)
server.WithMerger(prototype proto.Message, fieldPath string, fn)  // opaque fields only; see the caveats
```

Schemas that arrive later go to the live server instead, and the catalog swaps
atomically under in-flight requests:

```go
srv.RegisterEntity(&mycorpv1.Thing{})
srv.RegisterDescriptorSet(set []byte)          // the wire form defineSchema(...).toBinary() produces
srv.RegisterDynamicEntities(specs ...server.DynamicEntity)
```

Mutators and mergers have no runtime counterpart: they are Go funcs, so unlike
a schema they cannot be provisioned. That also fixes the mutator table at
construction, which is what lets the push path read it without a lock.

A mutator's `args` follows its registration, not its caller: registered with a
prototype it always receives a non-nil message of that type — zeroed when the
invocation carried none — and registered with `nil` it always receives `nil`.
That holds for a client push, `Invoke` and `InvokeIdempotent` alike, so a body
may type-assert its prototype unconditionally. Passing args to a mutator
registered without a prototype is an error.

For building runtime schemas in Go: `server.BuildDynamicFile`,
`server.DynamicEntity` and `server.DynamicField`. See
[Define schemas in TypeScript](../how-to/define-schemas-in-typescript.md).

Registration rejects an entity whose name is not an identifier, or whose
`key_fields` name a kind the client renders differently — see
[Naming rules](naming.md).

## In-process writes

```go
func (s *Server) Apply(ctx, workspaceID string, fn func(tx *ApplyTx) error) error
func (s *Server) ApplyIdempotent(ctx, workspaceID, key string,
    fn func(tx *ApplyTx) ([]byte, error)) (WriteOutcome, error)
func (s *Server) Mutate(ctx, workspaceID string, fn func(tx *EntityTx) error) error
func (s *Server) Invoke(ctx, workspaceID, mutatorName string, args proto.Message) error
func (s *Server) MutateIdempotent(ctx, workspaceID, key string,
    fn func(tx *EntityTx) ([]byte, error)) (WriteOutcome, error)
func (s *Server) InvokeIdempotent(...) (WriteOutcome, error)
```

`WriteOutcome` reports `Applied` (did this call do the work?), `Seq` and
`Result`.

**`Apply` is the default choice.** Its closure reads from a snapshot and records
writes; the engine then authorizes them and applies them as one atomic mutation.
Nothing you write runs while the workspace write lock is held, no permission
hook runs inside a transaction, and the closure runs exactly once — so unlike
`Mutate` there is no retried-closure contract to observe.

`Mutate` holds the write lock for the whole closure and calls `CanMutate` inside
the transaction. Reach for it when you need a serializable read-then-write, or
writes that stream rather than buffer (a bulk import).

`ApplyTx` offers `Get`, `GetMeta`, `Query`, `ScanPage`, `Put`, `Patch` and
`Delete`. Reads come from the snapshot taken before the closure ran: they do not
see the closure's own recorded writes, and a read of a key you have written is an
error rather than a stale row. `EntityTx` offers `Get`, `GetMeta`, `Put`,
`Patch`, `Delete`, `Scan`, `Query`, `Seq` and
`ScanPage(prototype, afterKey, limit, fn)`. Write options on both:
`WithTextString(field, value)`, `WithTextPeer(peer)` and
`WithExpectedVersion(v)`. A text field of the written message holding a
plain-string value (`text.EncodePlainString`, what a field-history record
carries) resolves the same way and needs the same `WithTextPeer`; on `Patch` it
does not widen the mask.

`WithExpectedVersion` makes a `Put` a compare-and-swap — the stored row must be
at that version, an absent or deleted row reading as version 0 — and is how a
server-side write states create-or-update intent to an entity whose
[write policies](../reference/proto-options.md#delete-policy) restrict either.
`Patch` takes its expected version as an argument instead.

Neither consults a client watermark. See
[Write from server-side code](../how-to/server-side-writes.md).

## Reads

```go
func (s *Server) View(ctx, workspaceID string, fn func(tx *ViewTx) error) error
```

`ViewTx` offers `Get`, `GetMeta`, `Query` and `ScanPage`, each reporting a row's
`version` alongside the message. The caller's `ReadFilter` applies — a filtered
row reads as absent — but no engine-imposed limit caps a result. See
[Query from server-side code](../how-to/server-side-queries.md).

`GetMeta` reports a `RowMeta{Version, Actor}` in place of the bare version:
`Actor` is the `storage.Actor` the engine recorded for the row's last write, so a
per-row ownership check reads the engine's attribution rather than a field the
writer filled in. It is on `EntityTx` and `ApplyTx` too — see
[Check ownership in a mutator](../how-to/authentication-and-permissions.md#check-ownership-in-a-mutator).

## Permissions on the in-process API

`Apply`, `Mutate`, `Invoke` and `View` authorize against the **ctx principal**,
with the same hooks and the same `Write` values the sync surface uses — a policy
cannot tell an in-process write from a client's. A ctx carrying no principal
authorizes as anonymous, which a deny-by-default policy refuses.

```go
srv.Apply(server.WithPrincipal(ctx, p), ws, fn)   // acts as p, policy enforced
srv.Mutate(server.Trusted(ctx), ws, fn)           // system work, hooks skipped
```

`server.Trusted(ctx)` is the marked exit for code with no caller to authorize —
connectors, migrations, cron jobs, the admin UI. Forgetting it fails safe: the
call authorizes as anonymous and a real policy denies it. Use
`server.TrustedAs(ctx, "connect:markdown")` for anything that writes on a
schedule or in a loop: it does the same thing and additionally names the writer,
which is what lets a change-feed consumer recognize its own writes.
`Invoke` authorizes
**once** on the verb (`Write{Custom}` with the mutator name and args); the rows
the mutator body writes are not authorized individually, exactly as on the push
path.

## Field history

An entity carrying `(syncplane.v1.entity).history` gets an append-only log of
what its fields held before each write that changed them, captured on the
server's write path — a write storing what is already there records nothing. The
schema half — how to opt in, how to narrow it per field, what a record contains
— is in [Proto options](proto-options.md#field-history); the restore recipe is
[Restore a field to a previous value](../how-to/restore-a-field.md). Three server
options tune it.

```go
func WithHistoryFilter(fn HistoryFilterFunc) Option
func WithHistoryRetention(d time.Duration) Option
func WithHistorySession(d time.Duration) Option

type HistoryFilterFunc func(ctx context.Context, rec *syncplanev1.FieldHistory,
    src EntityMeta) (*syncplanev1.FieldHistory, bool, error)
```

### Editing sessions

`WithHistorySession(d)` sets how long one row's record stays open to later
writes **by the same actor**. Writes landing inside the window fold into it: the
record keeps saying what the fields held before the first of them, its mask
widens to the union of what the run touched, and `through_seq`/`writes` grow
instead of the log. Default **five minutes**; `server.HistorySessionOff` keeps
every write.

The window runs from when the record **opened**, not from its last write, so one
record never covers more than a window however long the editing runs — a day of
work stays a timeline rather than one entry.

What folding costs is the intermediate states: the log answers "what did the
field hold before that editing session", not "replay the session". Turn it off
when each write has to be separately restorable, weighing that against what
per-write capture costs on an entity carrying a text field — see
[Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-records-editing-sessions-not-keystrokes).

Off is still not an audit trail. A `DELETE` is uncaptured whatever the window,
and purges the row's log — see
[Field history is a restore mechanism, not an audit log](../explanation/limits-and-trade-offs.md#field-history-is-a-restore-mechanism-not-an-audit-log)
and [Recover a deleted row](../how-to/recover-a-deleted-row.md).

Which record a write folds into is tracked in memory and re-validated against
storage before every fold, so it is a hint rather than authority. A restart, or a
sibling node handling the next write in a multi-node deployment, opens a new
record — a shorter session, never a wrong one.

Across nodes that costs one guarantee: **two records of one row can cover
overlapping spans**. The node that opened a record still holds the hint, so a
write it handles later folds in around the record a sibling opened meanwhile.
Every record is still the state before its own `seq`, so restoring from one is
unaffected; a timeline that reads `seq`…`through_seq` as a partition of the row's
writes is not.

### Who can read a record

A record is read under **the source row's** read filter, never the history
entity's own. That is not a knob: `syncplane_field_history` is an entity your app
never declared, so a `ReadFilter` written against your own entity names never
matches it and it would fall to the default — allow all. The rule the engine
applies instead is the one you would write by hand: **you may read the history of
a row you can currently read**.

"Currently" is load-bearing. The filter is applied to the live source row, not to
the record's `prior` — `prior` carries only the masked fields, so it cannot answer
a filter that tests an unrecorded one. A record whose source row is gone, or whose
source entity has left the catalog, is unreadable rather than open. `Query` is the
only door a client has to a `server_only` entity, so this is the whole gate;
trusted in-process reads (`View`, `ViewTx.Query`) are unfiltered as always.

A filter you do register for `syncplane_field_history` still applies on top of
this — the two are an AND — but it is not where the rule comes from, and it is
not needed to get one.

The cost is one `ReadFilter` call per registered entity on a history query — the
records name entities the request does not — and, for entities that do filter, a
lookup of the source row per candidate record.

### The admission hook

`WithHistoryFilter` runs on every record, inside the write transaction,
immediately before the record is stored. Return the record and `true` to keep
it, `false` to drop it, or an error to **fail the write that triggered it** — a
hook that cannot decide must not let a write land unrecorded.

An error means the hook **could not decide**, so the write stays retryable: the
batch fails with `internal`, nothing is acked, and the client resends once the
hook can answer. The error's text is logged, never sent — a hook's wording can
name dependencies and rows the caller has no business seeing.

To refuse a record **permanently**, return `server.Reject(code, message)`
instead. That acks the mutation and the client drops it, which is what a hook
failing on a record's _shape_ wants: resending the identical mutation would fail
identically. A rejection's code and message are the hook's own and do reach the
client. The split is [`CanMutate`](#permissions-and-identity)'s — a dependency
having a bad minute must not cost the caller its write.

A hook may redact, rewrite or drop a record. It may **not** move one: changing
`source_entity`, `source_key` or `seq` fails the write. Those three are the
record's identity — what the row key derives from, what same-sequence merging
folds on, and what the `DELETE` cascade purges by.

Nor may it restate `row_created`, which fails the write the same way. A record
marking a row's creation absorbs the rest of its session, so a hook that set the
flag would silence every write folding in behind it; redacting a creation record
means dropping it. Redacting a mask down to empty is allowed and leaves an
update — the record still says a change happened, and later writes in the
session record their own fields.

```go
server.WithHistoryFilter(func(_ context.Context, rec *syncplanev1.FieldHistory,
	src server.EntityMeta) (*syncplanev1.FieldHistory, bool, error) {
	if src.Name == "session" {
		return nil, false, nil // not worth a log
	}
	if slices.Contains(rec.GetMask().GetPaths(), "ssn") {
		rec.Prior = nil // keep the entry, drop the value
	}
	return rec, true, nil
})
```

It must be a **pure function of its arguments**, exactly as
`Permissions.CanMutate` must and for the same reason: the storage adapter may
re-run the write closure and re-invoke the hook, so a hook that audit-logs or
meters double-counts. Redaction, sampling and per-entity policy belong here;
anything that has to happen once belongs after the write commits.

Like `WithMutator` and `WithMerger` it is construction-time only: it is a func,
so unlike a schema it cannot be provisioned at runtime. Passing `nil` fails
`New`.

`WithHistoryRetention` is covered under [Garbage collection](#garbage-collection).

## Change feed

```go
func (s *Server) Changes(ctx, workspaceID string, after Cursor, limit int,
    fn func(Change) error) (next Cursor, more bool, err error)
func (s *Server) SubscribeChanges(workspaceID string) (<-chan struct{}, func())
func (s *Server) SubscribeActivity() (ids <-chan string, cancel func(), ok bool)
func (s *Server) CurrentSeq(ctx, workspaceID string) (uint64, error)
```

Each `Change` carries `Actor` — the authenticated caller's subject, the label a
trusted write gave itself, or both (see
[the change feed](../how-to/change-feed.md#who-wrote-it)).
Because the feed coalesces by row it reports the row's **last** writer, which is
enough for echo suppression and "notify everyone except the actor", and is not
an audit trail.

If you need "what did this field hold at sequence N", that is what
[field history](#field-history) is for — the feed cannot answer it.

The feed narrows by nothing, **field history included**: a consumer reading
`Changes` directly sees a `syncplane_field_history` change beside each captured
write, at the same sequence. Anything dispatching on `Change.Entity` — including
[effects](../how-to/react-to-changes.md), which dispatch per registered prototype — is
unaffected; a consumer that treats every change alike should skip that entity
itself.

Returns `ErrCursorTooOld` when the cursor cannot be resumed against this store:
below the tombstone horizon, above the current sequence, or stamped with another
store lineage. `Cursor.Epoch` carries that lineage and must survive whatever
persists the cursor. See [Follow committed changes](../how-to/change-feed.md).
`CurrentSeq` is the head of the feed; the distance from a consumer's position to
it is that consumer's lag.

`SubscribeChanges` needs a workspace id up front, which is the right shape for
one stream and the wrong one for background work across a whole store.
`SubscribeActivity` carries **every** workspace that commits a write — including
ones the caller has never heard of — over one subscription, repeating an id each
time its workspace is written. `ok` is false when the installed notifier cannot
report it, which leaves periodic sweeping as the only way to find work. Like
every notifier signal it is best-effort and, by default, in-process.

### Effect-consumer state

```go
func (s *Server) ConsumerPosition(ctx, workspaceID, consumer string) (Cursor, bool, error)
func (s *Server) ConsumerPositions(ctx, workspaceID string) ([]storage.ConsumerPosition, error)
func (s *Server) ConsumerShadow(ctx, workspaceID, consumer, entity, key string) ([]byte, error)
func (s *Server) AdvanceConsumer(ctx, workspaceID, consumer string, up ConsumerUpdate) error
func (s *Server) ResetConsumer(ctx, workspaceID, consumer string) error
func (s *Server) ConsumerPendingWorkspaces(ctx, consumer, after string, limit int) ([]string, error)
```

Durable bookkeeping for feed consumers, kept in the workspace's own store and
keyed by consumer name. `AdvanceConsumer` commits a new position **together with**
the shadow writes derived from the page it covers, in one transaction — so a
crash redelivers a change rather than silently skipping it. Handlers run first;
this is called after.

`ConsumerPendingWorkspaces` answers "where is my work" for a whole store in one
query, paging by workspace id from an exclusive `after` cursor. A workspace the
consumer has never acted on is pending, so a newly registered consumer finds
everything it has to catch up on. The answer is optimistic — a listed workspace
may hold only changes the consumer filters out — but never short.

`ResetConsumer` drops a consumer's position and shadows, which is both what a
consumer does after `ErrCursorTooOld` and how you retire one for good. The GC
pass collects consumers stranded below the tombstone horizon.

Most callers reach these through [the effects runtime](../how-to/react-to-changes.md)
rather than directly.

## Permissions and identity

```go
type Authenticator interface {
    Authenticate(ctx context.Context, h http.Header) (Principal, error)
}
```

- `server.ErrUnauthenticated` — wrap it to reject as 401; any other error is a
  500.
- `server.StaticTokenAuthenticator` — fixed API keys.
- `server.WithPrincipal(ctx, p)` — inject an identity from your own middleware.

Permission hooks: `CanAccessWorkspace`, `CanMutate`, `ReadFilter`,
`FilterEpoch` (plus `EpochReadFilters` for delta resync on permission changes,
and `FieldPermissions` for per-caller field protection). Embed
`server.AllowAll` to implement a subset.

```go
type FieldPermissions interface {
    ProtectedFields(ctx context.Context, p Principal, workspaceID, entity string) ([]string, error)
}
```

Optional and type-asserted. It returns the top-level field names this caller may
not write, on top of the ones the entity's schema marks
`(syncplane.v1.field_behavior).write_policy` — it widens that set and can never
narrow it. A PATCH whose mask reaches a protected field is rejected with
`CodePermissionDenied`; a PUT keeps the field's stored value and applies the
rest. Resolved at most once per entity per request, before the write transaction
opens. A returned name matching no field, or matching a key field, fails the
request.

Error constructors: `server.DenyWorkspace(msg)` (a settled denial) and
`server.Reject(code, msg)`. Rejection codes stay free-form on the wire so an app
can invent its own; the engine ships constants for the ones it uses itself:
`CodePermissionDenied`, `CodeInvalidArgument`, `CodeConflict`,
`CodeWorkspaceAccessRevoked`, `CodeMutationIDGap`, `CodeInternal`,
`CodeTextFullStateRequired` and `CodeTextStateStale`.

See [Authentication and permissions](../how-to/authentication-and-permissions.md).

## Rooms

The ephemeral plane — presence and broadcast — served by `PresenceService`
alongside the sync service. It is mounted by `HTTPHandler()` and by
`Handlers()`, so a deployment that mounts either gets it without asking.
Nothing it carries is stored: see [Rooms](../explanation/rooms.md).

```go
path, h := srv.PresenceHandler()   // one service
for path, h := range srv.Handlers() { mux.Handle(path, h) } // all of them
```

`Watch` streams a session's rooms; `Publish` writes state, sends broadcasts and
announces departures. `Publish` is unary, so only receiving needs a stream.

### The join gate

Every topic passes the same admission check at join, again on the stream's
heartbeat, and on a `Publish` for a topic the caller is not already watching
here:

1. `CanAccessWorkspace` — the workspace gate, first, so a refused caller
   performs no storage work.
2. For an **anchored** topic, the anchor row must exist and pass that entity's
   `ReadFilter`. Room membership therefore inherits row permissions. An anchor
   naming an unregistered or server-only entity is refused.
3. `RoomPermissions.CanJoinRoom`, if the policy implements it.
4. `RoomPermissions.VisibleParticipants`, resolved once and applied to the
   snapshot and to every later frame.

A settled denial drops the topic as `REASON_ACCESS_REVOKED`; a hook that could
not decide drops it as `REASON_ERROR`, so an outage never reads as a
revocation. Other topics on the stream keep running either way.

The anchor row read is memoized across the streams re-gating on it, so a
document everyone has open costs one read per heartbeat interval rather than one
per watcher — at the price of a revocation being noticed within two heartbeats
rather than one.

```go
type RoomPermissions interface {
    CanJoinRoom(ctx context.Context, p Principal, workspaceID string, topic presence.Topic) error
    VisibleParticipants(ctx context.Context, p Principal, workspaceID string, topic presence.Topic) (ParticipantFilter, error)
}
```

Optional, and type-asserted: a `Permissions` that does not implement it admits
every caller who cleared the gate above and shows them everyone. Embed
`server.AllowAllRooms` to implement one hook and inherit the other. Return
`server.DenyRoom(msg)` for a settled refusal.

### Identity

`Participant.Subject` is stamped from the request principal. Nothing in the
payload names the publisher, so presence cannot be used to impersonate.
`session_id` is self-asserted like a client id — the server binds it to the
principal that opened its stream, and refuses a `Publish` naming a session this
node holds for somebody else.

### The bus

```go
type Bus interface {
    Publish(ctx context.Context, ev presence.Event) error
    Subscribe(topic presence.Topic) (<-chan presence.Event, func())
}
```

`WithPresenceBus` installs it; the default is in-process. Delivery is at most
once, and `Publish` must never block on a watcher that is not reading — see the
`engine/presence` package doc for the full contract and
`presencetest.Conformance` for the suite an implementation must pass.

Liveness is the bus's only durability: a node re-announces the records of the
sessions its streams are responsible for every `WithRoomAnnounce` interval, and
drops a record nobody re-announced within three of them. `Publish` returns that
interval, so a publisher holding no stream — which nothing re-announces for —
can refresh its own records on it.

## Hosting many apps

```go
mgr, _ := server.NewManager(ctx, root,
    server.WithRegisterAuthorizer(server.TokenAuthorizer(token)),
    server.WithServerOptions(opts...),          // applied to every app
    server.WithAppOptions(func(appID string) []server.Option { ... }),
)
mgr.SyncHandler()    // public, routed by a leading /<appID> segment
mgr.AdminHandler()   // private control plane
mgr.Close()          // stops every hosted app's *Server (not the root adapter)
```

`server.DenyAll` is the `RegisterAuthorizer` default. See [Host multiple apps](../how-to/host-multiple-apps.md).

## Storage

`sqlitestore.Open(path, sqlitestore.WithNamespace(...))` is the single-process
default; `pgstore.Open(ctx, dsn)` backs several nodes off one database. Either
way `root.Namespace(id)` derives one view per app from a single open store. See
[Storage adapter contract](storage-adapter.md) and
[Run more than one node](../how-to/run-multiple-nodes.md).

## Cursor state

A pull cursor's bulk is membership: a windowed or reference-following client's
cursor names every row it holds, which at the limits is hundreds of kilobytes
riding on every pull request, every pull page and every persisted checkpoint.
With a cursor-state store the server writes those sets once and the cursor
carries a fixed-size reference; the entry is rewritten only when membership
actually moves.

The store is a cache with licensed eviction, not a system of record: a client
presenting a reference whose entry is gone is reset onto a fresh bootstrap,
exactly as one below the tombstone horizon is, and a failed write falls back to
inlining the sets in the cursor — the indirection is an optimization, never a
dependency. What an implementation choice buys:

- **Adapter-provided** (the default; both shipped adapters implement
  `cursorstate.Provider`) — entries live in the same database as the rows,
  survive restarts, and on `pgstore` are shared across nodes. Superseded and
  aged entries are pruned on write; nothing to configure.
- **`cursorstate.NewMemory(maxBytes)`** — in-process, for tests and
  single-node deployments that accept the cost: a restart forgets every entry,
  so every connected client re-bootstraps at once.
- **Your own `cursorstate.Store`** (Redis, memcached, …) — implement two
  methods over immutable entries. Entries hold row keys (not values); weigh
  that before pointing the store at shared infrastructure if keys are
  sensitive in your domain.

`syncplane.cursor_state.misses` is the health signal: every miss is a client
re-download. See [Telemetry](telemetry.md#cursor-state).

## Leases

`Server.Leases()` exposes the `lease.Locker` the server resolved — installed
by `WithLeases`, provided by the adapter (`lease.Provider`; on `pgstore` the
lease space is the shared database, backed by advisory locks), or a
process-local fallback. Background work takes a per-`(role, workspace)`
try-lock around each pass and skips when another process holds it; a locker
error makes the pass run unleased, so a broken lease backend costs duplicated
work, never stopped maintenance.

The engine's roles are `gc`, `indexes` and `effects/<consumer>`. An
application worker leasing its own per-workspace work through
`Server.Leases()` picks role names outside those; the `lease` package
documents the contract — advisory deduplication whose loss is silent, so
leased work stays idempotent.
