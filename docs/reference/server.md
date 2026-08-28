# Go server API

Package `github.com/levelfourab/syncplane/engine/server` provides the Go server implementation for Syncplane. This page documents the server constructor, configuration options, limits, in-process APIs, and background processes.

## Construction

Construct a server with `server.New`:

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

`server.New` validates and applies all options before returning. Declaration options (`WithEntities`, `WithMutator`, `WithMerger`) are evaluated in dependency order rather than registration order. A `WithMerger` option can reference an entity defined in a subsequent `WithEntities` declaration.

The caller retains ownership of the underlying storage adapter. `srv.Close()` stops background maintenance tasks, including garbage collection, index backfills, text compaction sweeps, and notify-coalescing timers. Calling `srv.Close()` does not close the underlying storage adapter (which may back multiple apps in a [Manager](../how-to/host-multiple-apps.md)) or a notifier configured with `WithNotifier`.

`srv.HTTPHandler()` returns an `http.Handler` mounted at its default path. To mount the service on a custom router alongside other routes, use `path, handler := srv.Handler()`. Connect routes RPCs by full method path and cannot be mounted at an arbitrary root prefix.

## Options

The following table lists configuration options for `server.New`:

| Option                                      | Default      | Description                                                                                                                                                                                                              |
| ------------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WithAuthenticator(a)`                      | anonymous    | Authenticates request headers into a `Principal` before each sync RPC.                                                                                                                                                   |
| `WithPermissions(p)`                        | `AllowAll`   | Sets workspace access, write permissions, and per-row read filter policies.                                                                                                                                              |
| `WithLimits(l)`                             | See limits   | Sets request and data size bounds. Non-positive fields retain default values.                                                                                                                                            |
| `WithChunkSize(n)`                          | 256          | Sets bootstrap chunk size in rows.                                                                                                                                                                                       |
| `WithHeartbeat(d)`                          | 30s          | Sets `Subscribe` heartbeat interval and access re-verification frequency for active streams.                                                                                                                             |
| `WithNotifyCoalescing(w)`                   | —            | Coalesces commit notifications across a time window. Setting `0` disables coalescing.                                                                                                                                    |
| `WithNotifier(n)`                           | in-process   | Replaces the in-process fan-out notifier with a shared cross-node implementation, such as `pgnotify` over PostgreSQL `LISTEN`/`NOTIFY`. Replacements should implement `pubsub.Activity` to announce new workspaces.      |
| `WithCursorStateStore(st)`                  | adapter      | Sets the storage backend for cursor membership state. Defaults to the adapter's cursor store. Setting `nil` disables indirection and inlines membership sets into every cursor. See [Cursor state](#cursor-state).       |
| `WithLeases(l)`                             | adapter      | Sets the `lease.Locker` used to deduplicate per-workspace background work (GC, index sweeps, text compaction, effects). Defaults to the adapter's locker, falling back to a process-local locker. See [Leases](#leases). |
| `WithGC(retention, interval)`               | —            | Sets tombstone retention duration and background sweep interval. Setting `interval <= 0` restricts garbage collection to manual execution.                                                                               |
| `WithTextDeltaRetention(d)`                 | 1h           | Sets retention duration for the collaborative-text delta log.                                                                                                                                                            |
| `WithIdempotencyRetention(d)`               | 24h          | Sets retention duration for claimed `MutateIdempotent` keys.                                                                                                                                                             |
| `WithHistoryRetention(d)`                   | 90d          | Sets retention duration for field-history records. `HistoryForever` disables time-based expiration. See [Field history](#field-history).                                                                                 |
| `WithTextCompaction(minPurged)`             | off          | Enables background purging of unreferenced text tombstones and mark operations. See [Text compaction](#text-compaction).                                                                                                 |
| `WithTextCompactionPacing(interval, batch)` | 5m, 200 rows | Sets compaction sweep frequency and the maximum number of rows evaluated per transaction. Transactions also terminate when reaching a fixed byte budget.                                                                 |
| `WithTextCache(budgetBytes)`                | 8 MiB        | Caches decoded collaborative-text documents between writes. Setting `0` disables caching.                                                                                                                                |
| `WithSchemaRevision(rev)`                   | —            | Sets an application-defined schema revision string advertised on frames, pushes, pulls, queries, and bootstraps.                                                                                                         |
| `WithLogger(l)`                             | —            | Sets the `*slog.Logger` instance.                                                                                                                                                                                        |
| `WithTracerProvider(tp)`                    | global       | Sets the OpenTelemetry tracer provider. The global provider is a no-op until an SDK is registered. See [Telemetry](telemetry.md).                                                                                        |
| `WithMeterProvider(mp)`                     | global       | Sets the OpenTelemetry meter provider.                                                                                                                                                                                   |
| `WithEntities(msgs...)`                     | —            | Declares syncable entity message prototypes. Repeatable.                                                                                                                                                                 |
| `WithMutator(name, args, fn)`               | —            | Declares a named server-authoritative mutation.                                                                                                                                                                          |
| `WithMerger(proto, path, fn)`               | —            | Declares a custom merge hook for an opaque field.                                                                                                                                                                        |
| `WithHistoryFilter(fn)`                     | —            | Sets a hook to admit, reject, or rewrite field-history records before commit. See [Field history](#field-history).                                                                                                       |
| `WithHistorySession(d)`                     | 5m           | Sets the duration a field-history record remains open to consecutive writes by the same actor. `HistorySessionOff` records every write separately. See [Field history](#field-history).                                  |
| `WithPresenceBus(b)`                        | in-process   | Sets the bus implementation for the ephemeral plane. Multi-node setups require a shared implementation such as `engine/presence/pgpresence`. See [Rooms](#rooms).                                                        |
| `WithRoomAnnounce(d)`                       | 5s           | Sets the interval for re-announcing active presence records. Sessions disappear after three missed intervals. See [Rooms](#rooms).                                                                                       |

### Limits

The following table lists request and data bounds in `server.Limits`:

| Field                     | Default | Description                                                                                                                                                      |
| ------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MaxRequestBytes`         | 16 MiB  | Maximum decompressed request body size, enforced before parsing.                                                                                                 |
| `MaxMutationsPerPush`     | 256     | Maximum mutations per `Push` across all per-workspace batches.                                                                                                   |
| `MaxWorkspacesPerRequest` | 64      | Maximum push batches or `Subscribe` scopes per request.                                                                                                          |
| `MaxValueBytes`           | 1 MiB   | Maximum serialized entity value size (`crud.value` or `custom.args`).                                                                                            |
| `MaxFieldMaskPaths`       | 64      | Maximum field-mask paths per `PATCH` operation.                                                                                                                  |
| `MaxPullChanges`          | 4096    | Maximum row changes per `Pull` response. Larger result sets paginate with `has_more`.                                                                            |
| `MaxQueryRows`            | 1024    | Maximum rows returned per `Query` response.                                                                                                                      |
| `MaxQueryOffset`          | 16384   | Maximum offset permitted in a `Query`. Requests exceeding this offset are rejected. Specifying an offset alongside an `after` or `before` cursor is rejected.    |
| `MaxQueryCursorBytes`     | 4096    | Maximum byte length of an `after` or `before` query cursor.                                                                                                      |
| `MaxFollowDepth`          | 4       | Maximum recursive hops a `Subscription.follow` can evaluate.                                                                                                     |
| `MaxClosureSize`          | 10000   | Maximum rows in a reference-following closure for a single request.                                                                                              |
| `MaxReverseFanOut`        | 1000    | Maximum source rows matched by a single reverse-follow hop.                                                                                                      |
| `MaxWindowSize`           | 1000    | Maximum rows held in a single `Subscription.window`. Must remain smaller than `MaxPullChanges`.                                                                  |
| `MaxIndexEntriesPerRow`   | 1024    | Maximum secondary-index entries a single row can occupy across all indexes for its entity. Exceeding this limit fails the write.                                 |
| `MaxIndexScanCandidates`  | 10000   | Maximum index entries evaluated during a range scan while a read filter rejects candidates. Exceeding this limit falls back to a table scan.                     |
| `MaxRelConditions`        | 8       | Maximum `matches()` conditions permitted in a single query predicate.                                                                                            |
| `MaxRelKeys`              | 50000   | Maximum target rows selected by a single `matches()` condition. Exceeding this limit fails the query.                                                            |
| `MaxRoomsPerStream`       | 64      | Maximum rooms permitted on a single `Watch` stream, and maximum sessions kept alive on behalf of other clients.                                                  |
| `MaxPresenceStateBytes`   | 4 KiB   | Maximum presence state payload size for one participant.                                                                                                         |
| `MaxBroadcastBytes`       | 4 KiB   | Maximum broadcast message payload size.                                                                                                                          |
| `MaxPublishItems`         | 32      | Maximum items permitted in a single `Publish` request.                                                                                                           |
| `MaxParticipantsPerRoom`  | 128     | Maximum participant records retained per room. When full, the room retains the lowest session IDs, reports snapshot truncation, and rejects additional sessions. |

Increasing `MaxMutationsPerPush` or `MaxValueBytes` requires increasing `MaxRequestBytes` accordingly.

Each limit applies to a single request or stream. Limits do not constrain overall request rates or concurrent stream counts. For details, see [the default posture is open](../explanation/limits-and-trade-offs.md#the-default-posture-is-open).

Exceeding a limit returns an error that names the counted value, the violated field, the current limit, and the configuration option:

```
push of 300 mutations, over MaxMutationsPerPush (256); raise it with server.WithLimits
```

## Error messages

Errors returned by Go API calls (`RegisterEntity`, `RegisterMerger`, `EntityKey`, transaction methods, and options applied in `New`) are prefixed with `server:` and can reference Go identifiers:

```
server: syncplane.example.v1.Todo is not a registered entity; register it first
with server.WithEntities(&Todo{}) or Server.RegisterEntity
```

Errors returned over RPC carry no `server:` prefix. Wire errors are Connect errors with machine-readable error codes in metadata; see [sync status and codes](sync-status-and-codes.md).

Errors reporting bounds violations state the configuration option to modify. Errors referencing unregistered entities specify the registration call. Handle errors by matching error codes rather than error message text.

## Garbage collection

`srv.CollectGarbage(ctx, workspaceID)` manually triggers a garbage collection pass.

A garbage collection pass prunes:

- **Tombstones** (`WithGC`): Pruned based on tombstone retention duration. Tombstone retention must exceed the maximum expected client offline window.
- **Text delta logs** (`WithTextDeltaRetention`): Prunes collaborative-text delta entries.
- **Idempotency keys** (`WithIdempotencyRetention`): Prunes keys claimed by `MutateIdempotent`. Retention must cover caller retry windows to prevent duplicate execution.
- **Field history** (`WithHistoryRetention`): Prunes historical field records. Retention is independent of tombstone retention. A record's age is calculated from its last appended write rather than its initial `recorded_at` timestamp.
- **Idle client watermark rows**: Pruned during the pass.

Field-history records are hard-deleted without creating tombstones.

History records are collected in bounded transactional chunks. The initial pass after enabling history or increasing retention deletes records across multiple transactions to avoid long-running locks. Interrupted sweeps resume from the remaining backlog on the next pass.

### History that lives as long as its row

Setting `WithHistoryRetention(server.HistoryForever)` disables time-based history expiration:

```go
server.WithGC(30*24*time.Hour, time.Hour),
server.WithHistoryRetention(server.HistoryForever),
```

Deleting a row permanently deletes its associated field history within the same transaction, regardless of retention settings.

If an entity removes its `history: {}` schema option, existing records remain stored and continue to be deleted when their source rows are deleted. If all entities remove `history: {}`, the history entity is removed from the catalog. Under standard retention, the background GC pass drains remaining uncataloged records; under `HistoryForever`, orphaned records persist until manually deleted.

The default retention duration is 90 days.

## Text compaction

Collaborative-text fields accumulate tombstones and mark operations as text is modified. `WithTextCompaction(minPurged)` enables background compaction to reclaim unreferenced items. `srv.CompactText(ctx, workspaceID)` runs compaction manually and returns the number of rewritten rows:

```go
server.WithGC(30*24*time.Hour, time.Hour),
server.WithTextCompaction(64),
server.WithTextCompactionPacing(5*time.Minute, 200), // optional
```

Text compaction runs as an independent background sweep with its own interval and per-workspace lease. Compaction operates only on quietened rows whose last write sequence is at or below the sequence through which the text delta log has been pruned by garbage collection. If garbage collection is manual-only, background compaction does not run.

Compaction sweeps evaluate rows modified between the previous pass position and the current horizon. Sweeps process batches of `batch` rows per transaction.

Compaction behavior includes:

- Rewriting a document assigns a new sequence number, causing connected clients to re-fetch that field once.
- Text writes based on purged states are rejected with `CodeTextStateStale`, requiring the client to re-apply changes against the current state.

The `minPurged` argument specifies the minimum number of reclaimable items required to rewrite a row. Lowering `minPurged` causes subsequent sweeps to re-evaluate all workspaces from the beginning. Raising `minPurged` does not trigger a full re-sweep. For details, see [the collaborative text explanation](../explanation/collaborative-text.md).

## Registration

Static schemas and handlers are registered at server construction:

```go
server.WithEntities(&todov1.Todo{}, &todov1.Note{})
server.WithMutator(name string, args proto.Message, fn)
server.WithMerger(prototype proto.Message, fieldPath string, fn)  // opaque fields only; see the caveats
```

Dynamic schemas can be registered on a running server instance:

```go
srv.RegisterEntity(&mycorpv1.Thing{})
srv.RegisterDescriptorSet(set []byte)          // the wire form defineSchema(...).toBinary() produces
srv.RegisterDynamicEntities(specs ...server.DynamicEntity)
```

Mutators and mergers must be registered at construction time and cannot be registered dynamically at runtime.

A mutator receives arguments matching its registered prototype. If registered with a message prototype, the mutator always receives a non-nil message of that type (zeroed if omitted by the caller). If registered with `nil`, the mutator always receives `nil`. Supplying arguments to a mutator registered without a prototype returns an error.

Dynamic schema construction helpers include `server.DynamicField` (`Message`, `Ref`, `KeyField`/`OrderField`, `MapKey`/`MapValue`) with `server.BuildDynamicFile` and `server.RegisterDynamicEntities`, which builds a set of referencing entities into one file like the TypeScript builder. See [Define schemas in TypeScript](../how-to/define-schemas-in-typescript.md).

Schema registration validates that entity names conform to identifier rules and that `key_fields` match supported client types. See [Naming rules](naming.md).

### Registration guarantees

- **Trust**: `RegisterAuthorizer` defaults to deny (`DenyAll`). The built-in `TokenAuthorizer` checks an `Authorization: Bearer <token>` header in constant time. A denied call never touches state.
- **Persistence**: The descriptor set is stored per app and re-registered on boot before serving. A restart leaves every app immediately syncable.
- **Idempotence and evolution**: Re-registering an identical set is a no-op. New entities and appended fields upsert. The manager rejects conflicting changes without partially applying them. Conflicting changes include removed, renumbered, or retyped fields, references retargeted to different entities, or changed `key_fields`. Each app's schema revision is a stable hash of its canonical descriptor bytes, surfaced to clients as `schema_revision` so that a schema change causes clients to re-bootstrap.

## In-process writes

The server provides methods for in-process writes:

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

`WriteOutcome` contains `Applied` (boolean), `Seq` (uint64), and `Result` (`[]byte`).

`ApplyTx` provides `Get`, `GetMeta`, `Query`, `ScanPage`, `Put`, `Patch`, and `Delete`. Reads query the initial snapshot and do not observe uncommitted writes from the same closure. Reading a key already written within the transaction returns an error.

`EntityTx` provides `Get`, `GetMeta`, `Put`, `Patch`, `Delete`, `Scan`, `Query`, `Seq`, and `ScanPage(prototype, afterKey, limit, fn)`.

Write options for `ApplyTx` and `EntityTx`:

- `WithTextString(field, value)`
- `WithTextPeer(peer)`
- `WithExpectedVersion(v)`: Enforces a compare-and-swap check on `Put`. The row must match the expected version, where version 0 represents an absent or deleted row. See [write policies](proto-options.md#write-policies). `Patch` accepts expected version as an explicit parameter.

In-process writes do not update or consult client watermarks. See [Write from server-side code](../how-to/server-side-writes.md).

### Choosing between Apply and Mutate

`Apply` executes its closure against a snapshot, records intended writes, evaluates permissions, and commits all writes in a single atomic mutation. The closure runs once outside the workspace write lock. For a read-modify-write under `Apply`, pass the version you read as `Patch`'s `expected` argument, which turns a concurrent edit into a `conflict` rejection instead of a silent merge. Most operations and request handlers should use `Apply`.

`Mutate` runs its closure inside the write transaction, holding the workspace's write lock for everything the closure does (a storage adapter may hold a wider lock, such as SQLite), and calls `CanMutate` from inside the transaction. `Mutate` may run the closure more than once if the storage adapter retries, so keep side effects out of it.

`Mutate` provides two capabilities that `Apply` does not:

- genuinely serializable read-then-write operations, where nothing can move between the read and the write;
- streaming writes that do not buffer in memory, suitable for bulk imports of many thousands of rows.

### Choosing idempotency keys

Keys are per workspace. Derive keys from data the caller owns, and prefix keys per source when multiple systems write to the same workspace.

Key considerations:

- A successful call claims the key even when the closure wrote nothing. The empty result replays and registers the workspace.
- Keys expire according to `WithIdempotencyRetention` (default 24 hours). Set retention above the longest retry window callers use; a retry arriving after its key has expired applies again.
- Idempotency keys use a separate ledger from client watermarks. Watermarks are contiguity-checked per client and pruned once a client goes idle, which an API caller's retry key cannot satisfy or survive.

## Reads

Execute read-only transactions with `View`:

```go
func (s *Server) View(ctx, workspaceID string, fn func(tx *ViewTx) error) error
```

`ViewTx` provides `Get`, `GetMeta`, `Query`, and `ScanPage`. Each method returns the entity `version` alongside the message. The caller's `ReadFilter` is evaluated for each row; filtered rows return as absent. In-process reads are not constrained by request row limits. See [Query from server-side code](../how-to/server-side-queries.md).

`GetMeta` returns `RowMeta{Version, Actor}`. `Actor` contains the `storage.Actor` recorded during the row's most recent write. `GetMeta` is also available on `ApplyTx` and `EntityTx`. See [Check ownership in a mutator](../how-to/authentication-and-permissions.md#check-ownership-in-a-mutator).

## Permissions on the in-process API

`Apply`, `Mutate`, `Invoke`, and `View` authenticate against the context principal:

```go
srv.Apply(server.WithPrincipal(ctx, p), ws, fn)   // acts as p, policy enforced
srv.Mutate(server.Trusted(ctx), ws, fn)           // system work, hooks skipped
```

A context without a principal authenticates as anonymous.

Use `server.Trusted(ctx)` to bypass permission hooks for system operations, migrations, and administrative tasks. Use `server.TrustedAs(ctx, "actor-name")` to bypass permission hooks while setting a specific actor attribution on written rows.

`Invoke` evaluates permission once for the mutator verb (`Write{Custom}` with mutator name and arguments). Rows written inside the mutator body are not individually authorized.

## Field history

Entities configured with `(syncplane.v1.entity).history` generate append-only logs of field values prior to each modifying write. For schema configuration, see [Proto options](proto-options.md#field-history). For restore operations, see [Restore a field to a previous value](../how-to/restore-a-field.md).

Field history is configured with the following options:

```go
func WithHistoryFilter(fn HistoryFilterFunc) Option
func WithHistoryRetention(d time.Duration) Option
func WithHistorySession(d time.Duration) Option

type HistoryFilterFunc func(ctx context.Context, rec *syncplanev1.FieldHistory,
    src EntityMeta) (*syncplanev1.FieldHistory, bool, error)
```

### Editing sessions

`WithHistorySession(d)` defines the maximum duration a row's history record remains open to coalesce subsequent writes by the same actor. Writes within this window widen the field mask and update `through_seq` and `writes` rather than creating new log entries. The default duration is five minutes. `server.HistorySessionOff` creates a separate history record for every write.

The session window starts from when the record was initially opened.

Sessions are tracked in memory and validated against storage before each write. Process restarts or requests handled by different nodes open new session records.

For design trade-offs, see [Limits and trade-offs](../explanation/limits-and-trade-offs.md#field-history-records-editing-sessions-not-keystrokes), [Field history is a restore mechanism, not an audit log](../explanation/limits-and-trade-offs.md#field-history-is-a-restore-mechanism-not-an-audit-log), and [Recover a deleted row](../how-to/recover-a-deleted-row.md).

### Who can read a record

Field-history records are read under the source row's `ReadFilter`. A caller can read the history of a row if they currently have permission to read that row.

The filter evaluates the current live source row. If the source row is deleted or its entity is removed from the catalog, its history records cannot be read via `Query`. Trusted in-process reads (`View`, `ViewTx.Query`) are unfiltered.

Any `ReadFilter` registered specifically for `syncplane_field_history` is evaluated in addition to the source row filter.

### The admission hook

`WithHistoryFilter` executes within the write transaction before a record is committed. The hook must be a pure function.

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

Hook return behaviors:

- `(rec, true, nil)`: Retains the record (optionally modified or redacted).
- `(nil, false, nil)`: Discards the history record while allowing the underlying write to commit.
- `(nil, false, err)`: Fails the write transaction with an `internal` error. The error message is logged but not sent to the client. The client can retry the write.
- `(nil, false, server.Reject(code, message))`: Rejects the write with a permanent application code and message sent to the client.

A hook must not modify `source_entity`, `source_key`, or `seq`. A hook must not set `row_created` on an update record. Modifying these fields fails the write transaction.

`WithHistoryRetention` configuration is documented in [Garbage collection](#garbage-collection).

### Edge cases

- **`rowCreated`**: Indicates that the row did not exist before that write. Its `mask` and `prior` are empty, so restoring from it does nothing. Test the `rowCreated` flag rather than checking for an empty mask, because an admission hook [history filter](#the-admission-hook) that redacts all paths also leaves an empty mask.
- **Row deletion**: A `DELETE` operation is not captured in field history and purges the row's log. A row created later under the same key starts with a new history. To make deletions recoverable, model deletion as a field change instead. See [Recover a deleted row](../how-to/recover-a-deleted-row.md).
- **Key fields**: Key fields are never included in the mask, preventing a restore from altering a row's identity. Decoded records retain key fields to identify the row.
- **Record retention**: History records expire according to the retention policy (default is 90 days, configured via `WithHistoryRetention`). To retain records indefinitely until the row is deleted, configure `server.WithHistoryRetention(server.HistoryForever)`.

## Change feed

The server provides change feed methods:

```go
func (s *Server) Changes(ctx, workspaceID string, after Cursor, limit int,
    fn func(Change) error) (next Cursor, more bool, err error)
func (s *Server) SubscribeChanges(workspaceID string) (<-chan struct{}, func())
func (s *Server) SubscribeActivity() (ids <-chan string, cancel func(), ok bool)
func (s *Server) CurrentSeq(ctx, workspaceID string) (uint64, error)
```

The feed outputs all workspace changes, including `syncplane_field_history` records. Effects and handlers dispatching by prototype ignore unhandled entity types; consumers processing all changes directly can filter `syncplane_field_history`. See [effects](../how-to/react-to-changes.md).

`Changes` returns `ErrCursorTooOld` if the cursor is below the tombstone horizon, ahead of the current sequence, or contains a mismatched store epoch (`Cursor.Epoch`). See [Follow committed changes](../how-to/change-feed.md).

`CurrentSeq` returns the current head sequence of the workspace feed.

### Semantics

- **At-least-once delivery**: The feed delivers changes at least once when you persist the cursor after processing the page. Deduplicate by `(entity, key, seq)`.
- **Per-workspace order**: Changes are ordered within a workspace by `(seq, entity, key)`.
- **Latest state only**: Only the latest state of a row is observable, not every intermediate write.
- **State feed, not an event log**: The feed reports current state rather than an event history.
- **Full documents**: Text-bearing rows carry full documents, never delta fragments.
- **Trusted server-side API**: Permission read filters do not apply to the change feed.
- **Actor attribution**: Every change carries an actor in `ch.Actor`.

### Actor attribution

`ch.Actor` (a `storage.Actor`) reports the writer in two fields:

- `Subject`: The authenticated caller subject, copied from its [`Principal`](#permissions-and-identity). Empty when no authenticated caller made the write.
- `System`: The label a write made under [`Trusted`/`TrustedAs`](#permissions-on-the-in-process-api) gave itself (`"system"` for a bare `Trusted`). Empty when an external caller made the write.

When both fields are set, a bypass ran under that caller's request. A zero actor means the write was unattributed.

Attribution limitations:

- **Last writer only**: The feed reports state rather than transitions. Writes that coalesce into one change leave only the final actor. Suppressing based on actor drops triggers if someone else's write coalesced behind your write.
- **Authentication requirement**: Caller writes are attributed only if you install an `Authenticator`. By default, client pushes record a zero actor with an empty `Subject`. The `client_id` is not recorded or substituted.
- **System is a label**: `System` is not verified and no policy runs against it. It indicates which pass made the write, not whether the write was permitted.

### Low-latency subscriptions

- `SubscribeChanges`: Exposes the in-process commit wakeup used by the live client stream to reduce latency. It carries no delivery guarantees; consumers must still drain on their own schedule. Requires a workspace ID.
- `SubscribeActivity`: Reports every workspace that commits a write, including new workspaces. Used by consumers monitoring multiple workspaces. If the configured notifier does not support activity streams, `SubscribeActivity` returns `ok = false`.

### Effect-consumer state

The server provides methods to track durable consumer state:

```go
func (s *Server) ConsumerPosition(ctx, workspaceID, consumer string) (Cursor, bool, error)
func (s *Server) ConsumerPositions(ctx, workspaceID string) ([]storage.ConsumerPosition, error)
func (s *Server) ConsumerShadow(ctx, workspaceID, consumer, entity, key string) ([]byte, error)
func (s *Server) AdvanceConsumer(ctx, workspaceID, consumer string, up ConsumerUpdate) error
func (s *Server) ResetConsumer(ctx, workspaceID, consumer string) error
func (s *Server) ConsumerPendingWorkspaces(ctx, consumer, after string, limit int) ([]string, error)
```

`AdvanceConsumer` commits an updated cursor position and shadow state in a single transaction.

`ConsumerPendingWorkspaces` returns workspace IDs with unprocessed changes for a consumer, paginated after an exclusive `after` workspace ID.

`ResetConsumer` removes stored positions and shadow states for a consumer. Consumers below the tombstone horizon are pruned by garbage collection. See [the effects runtime](../how-to/react-to-changes.md).

## Permissions and identity

Authentication and permission interfaces:

```go
type Authenticator interface {
    Authenticate(ctx context.Context, h http.Header) (Principal, error)
}

type FieldPermissions interface {
    ProtectedFields(ctx context.Context, p Principal, workspaceID, entity string) ([]string, error)
}
```

Built-in authenticators and helpers:

- `server.ErrUnauthenticated`: Wrap this error to return an HTTP 401 response. Unwrapped errors return HTTP 500.
- `server.StaticTokenAuthenticator`: Authenticates requests using fixed API tokens.
- `server.WithPrincipal(ctx, p)`: Sets the principal on a context.
- `server.AllowAll`: Default implementation permitting all operations.

Permission interfaces include `CanAccessWorkspace`, `CanMutate`, `ReadFilter`, `FilterEpoch`, `EpochReadFilters`, and `FieldPermissions`.

`FieldPermissions.ProtectedFields` returns top-level field names that the caller cannot modify. Fields declared as protected via schema options `(syncplane.v1.field_behavior).write_policy` remain protected. A `PATCH` touching a protected field is rejected with `CodePermissionDenied`. A `PUT` ignores incoming values for protected fields and preserves stored values. Protected field names matching no known schema field or matching a key field fail the request.

Error constructors and rejection codes:

- `server.DenyWorkspace(msg)`: Settled denial for workspace access.
- `server.Reject(code, msg)`: Settled denial for a mutation.
- Built-in error codes: `CodePermissionDenied`, `CodeInvalidArgument`, `CodeConflict`, `CodeWorkspaceAccessRevoked`, `CodeMutationIDGap`, `CodeInternal`, `CodeTextFullStateRequired`, and `CodeTextStateStale`.

See [Authentication and permissions](../how-to/authentication-and-permissions.md).

## Rooms

The ephemeral presence and broadcast plane is served by `PresenceService` alongside sync endpoints:

```go
path, h := srv.PresenceHandler()   // one service
for path, h := range srv.Handlers() { mux.Handle(path, h) } // all of them
```

`Watch` streams room updates. `Publish` sends presence state, broadcast messages, and departure notices. Ephemeral data is not written to durable storage. See [Rooms](../explanation/rooms.md).

### The join gate

Room admission is evaluated when joining, at each stream heartbeat, and on `Publish` calls for unwatched topics:

1. `CanAccessWorkspace`: Verifies workspace access.
2. **Anchor verification**: For anchored topics, the target row must exist and pass `ReadFilter`. Anchors referencing unregistered or `server_only` entities are rejected.
3. `RoomPermissions.CanJoinRoom`: Evaluates room join permission if implemented.
4. `RoomPermissions.VisibleParticipants`: Resolves the participant visibility filter for the initial snapshot and subsequent frames.

Settled denials close the topic with `REASON_ACCESS_REVOKED`. Temporary errors close the topic with `REASON_ERROR`. Other topics on the stream continue uninterrupted.

Anchor row lookups are memoized across streams sharing the same anchor.

```go
type RoomPermissions interface {
    CanJoinRoom(ctx context.Context, p Principal, workspaceID string, topic presence.Topic) error
    VisibleParticipants(ctx context.Context, p Principal, workspaceID string, topic presence.Topic) (ParticipantFilter, error)
}
```

Implement `RoomPermissions` to enforce room policies. Embed `server.AllowAllRooms` to provide default permit-all behaviors. Use `server.DenyRoom(msg)` for settled refusals.

### Identity

`Participant.Subject` is derived from the authenticated request principal. `session_id` is assigned by the client and bound by the server to the opening principal. A `Publish` request referencing a session ID owned by another principal on that node is rejected.

### The bus

```go
type Bus interface {
    Publish(ctx context.Context, ev presence.Event) error
    Subscribe(topic presence.Topic) (<-chan presence.Event, func())
}
```

`WithPresenceBus` sets the presence bus implementation. The default is in-process.

Nodes re-announce active sessions every `WithRoomAnnounce` interval. Records not re-announced within three intervals are discarded. `Publish` returns the announcement interval to allow standalone publishers to refresh records.

## Hosting many apps

Use `server.NewManager` to run multiple applications on a shared root adapter:

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

The default authorizer is `server.DenyAll`. See [Host multiple apps](../how-to/host-multiple-apps.md).

## Storage

Single-process deployments use `sqlitestore.Open(path, sqlitestore.WithNamespace(...))`. Multi-node deployments use `pgstore.Open(ctx, dsn)`. `root.Namespace(id)` derives isolated application views from a single storage instance. See [Storage adapter contract](storage-adapter.md) and [Run more than one node](../how-to/run-multiple-nodes.md).

## Cursor state

A cursor-state store caches membership state for windowed and reference-following subscriptions, allowing pull cursors to carry compact identifiers rather than full row key sets. If a cursor reference expires or is evicted, the client receives a reset and performs a bootstrap.

Available backends:

- **Adapter-provided** (default): Uses the underlying storage adapter (`cursorstate.Provider`). Entries are shared across nodes on `pgstore` and persist across restarts.
- **`cursorstate.NewMemory(maxBytes)`**: In-memory storage. Restarts clear state and cause connected clients to re-bootstrap.
- **Custom `cursorstate.Store`**: Custom implementation for external caches (such as Redis or Memcached). Entries store row keys.

The metric `syncplane.cursor_state.misses` records cache misses. See [Telemetry](telemetry.md#cursor-state).

## Leases

`Server.Leases()` returns the resolved `lease.Locker` configured via `WithLeases` or provided by the storage adapter (`lease.Provider`).

Background maintenance acquires a non-blocking lease for each `(role, workspace)` pair:

- `gc`: Garbage collection sweeps.
- `indexes`: Secondary index backfills.
- `effects/<consumer>`: Background effect consumers.

If a lease is held by another process, the sweep is skipped. If the lease provider returns an error, the task executes without a lease. Application workers leasing custom work through `Server.Leases()` must use role names outside the engine namespaces.
