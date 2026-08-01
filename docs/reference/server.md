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
background work — the GC pass, the index backfill, and the notify-coalescing
timers, so a shared notifier receives no trailing publish after shutdown. It
does not close what the caller owns: neither the adapter, which may outlive the
server (and one adapter backs every app a
[Manager](../how-to/host-multiple-apps.md) hosts), nor a notifier installed
with `WithNotifier`. Two defers, in that order.

`srv.HTTPHandler()` returns the service already mounted at its own path. Use
`path, handler := srv.Handler()` to mount it on a mux carrying the app's own
routes — Connect routes by full method path, so the service cannot live at an
arbitrary root.

## Options

| Option                          | Default    | Effect                                                                                                                                                                                                                                                                                                         |
| ------------------------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithAuthenticator(a)`          | anonymous  | Turns request headers into a `Principal` before every sync RPC.                                                                                                                                                                                                                                                |
| `WithPermissions(p)`            | `AllowAll` | Workspace access, write and per-row read policy.                                                                                                                                                                                                                                                               |
| `WithLimits(l)`                 | see below  | Request bounds; non-positive fields keep their default.                                                                                                                                                                                                                                                        |
| `WithChunkSize(n)`              | 256        | Bootstrap chunk size in rows.                                                                                                                                                                                                                                                                                  |
| `WithHeartbeat(d)`              | 30s        | `Subscribe` heartbeat; also how often a live stream re-checks access.                                                                                                                                                                                                                                          |
| `WithNotifyCoalescing(w)`       | —          | Coalesces commit notifications; 0 disables.                                                                                                                                                                                                                                                                    |
| `WithNotifier(n)`               | in-process | Replaces the in-process poke fan-out with one shared across nodes — `pgnotify` ships such an implementation over Postgres `LISTEN`/`NOTIFY`. A replacement should implement `pubsub.Activity` too, or background consumers lose the signal that announces a new workspace and fall back to listing on a timer. |
| `WithGC(retention, interval)`   | —          | Tombstone retention and background pass interval; `interval <= 0` is manual-only.                                                                                                                                                                                                                              |
| `WithTextDeltaRetention(d)`     | 1h         | How long the collaborative-text delta log is kept.                                                                                                                                                                                                                                                             |
| `WithIdempotencyRetention(d)`   | 24h        | How long claimed `MutateIdempotent` keys are kept.                                                                                                                                                                                                                                                             |
| `WithTextCompaction(minPurged)` | off        | Opt-in purge of unneeded text tombstones and mark ops on the GC pass.                                                                                                                                                                                                                                          |
| `WithTextCache(budgetBytes)`    | 8 MiB      | Keeps written text documents decoded between writes; 0 disables.                                                                                                                                                                                                                                               |
| `WithSchemaRevision(rev)`       | —          | Advertises an app-defined revision on every frame, push, pull, query and bootstrap.                                                                                                                                                                                                                            |
| `WithLogger(l)`                 | —          | An `*slog.Logger`.                                                                                                                                                                                                                                                                                             |
| `WithTracerProvider(tp)`        | global     | OpenTelemetry tracer provider; the global one is no-op until an SDK is installed. See [Telemetry](telemetry.md).                                                                                                                                                                                               |
| `WithMeterProvider(mp)`         | global     | OpenTelemetry meter provider; same fallback.                                                                                                                                                                                                                                                                   |
| `WithEntities(msgs...)`         | —          | Declares syncable entities; repeatable.                                                                                                                                                                                                                                                                        |
| `WithMutator(name, args, fn)`   | —          | Declares a server-authoritative named mutation.                                                                                                                                                                                                                                                                |
| `WithMerger(proto, path, fn)`   | —          | Declares a merge hook for an opaque field.                                                                                                                                                                                                                                                                     |

### Limits

Every field defaults when left non-positive.

| Field                     | Default | Bounds                                                                                                                                                                                                                                                                           |
| ------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MaxRequestBytes`         | 16 MiB  | Decompressed request body, enforced before parsing. The outer wall.                                                                                                                                                                                                              |
| `MaxMutationsPerPush`     | 256     | Mutations per `Push`, across all its per-workspace batches.                                                                                                                                                                                                                      |
| `MaxWorkspacesPerRequest` | 64      | Push batches / `Subscribe` scopes per request.                                                                                                                                                                                                                                   |
| `MaxValueBytes`           | 1 MiB   | Serialized entity value (`crud.value` / `custom.args`).                                                                                                                                                                                                                          |
| `MaxFieldMaskPaths`       | 64      | Field-mask paths per PATCH.                                                                                                                                                                                                                                                      |
| `MaxPullChanges`          | 4096    | Row changes per `Pull` response; larger results paginate via `has_more`.                                                                                                                                                                                                         |
| `MaxQueryRows`            | 1024    | Rows per `Query` response.                                                                                                                                                                                                                                                       |
| `MaxQueryOffset`          | 16384   | `Query` offset; beyond it is rejected rather than clamped. It bounds the in-memory working set a scan retains, so it stays relevant however a query is served.                                                                                                                   |
| `MaxFollowDepth`          | 4       | Rounds a `Subscription.follow` may re-apply its path set.                                                                                                                                                                                                                        |
| `MaxClosureSize`          | 10000   | Rows in one request's reference-following closure.                                                                                                                                                                                                                               |
| `MaxReverseFanOut`        | 1000    | Source rows one reverse-follow hop may match.                                                                                                                                                                                                                                    |
| `MaxWindowSize`           | 1000    | Rows a single `Subscription.window` may hold. Must stay well under `MaxPullChanges`: a window that swaps wholesale emits up to 2N changes in one page.                                                                                                                           |
| `MaxIndexEntriesPerRow`   | 1024    | Secondary-index entries one row may occupy across its entity's indexes — the per-write cost of indexing that entity. An index over a repeated field stores one entry per element, so this is also the ceiling on how long an indexed list may get. Exceeding it fails the write. |
| `MaxIndexScanCandidates`  | 10000   | Index entries one range scan may walk while a read filter rejects candidates. Overflow falls back to a scan rather than failing.                                                                                                                                                 |

Raising `MaxMutationsPerPush` or `MaxValueBytes` means raising
`MaxRequestBytes` in tandem.

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
collaborative-text delta log and claimed trusted-write idempotency keys, and —
when `WithTextCompaction` asks for it — compacts quiet collaborative-text
documents. `srv.CollectGarbage(ctx, workspaceID)` runs it manually.

Keep the tombstone retention comfortably above the longest client offline
window; a pruned client that returns is re-admitted at whatever mutation id it
pushes next. The three retentions answer different questions:

- **Tombstones** (`WithGC`) — bounded by how long a client may be offline.
  Too short forces a re-bootstrap.
- **Text delta log** (`WithTextDeltaRetention`) — an optimization only. Pruning
  early costs wire size and nothing else.
- **Idempotency keys** (`WithIdempotencyRetention`) — bounded by callers' retry
  windows. **Too short is a correctness problem**: a retry arriving after its
  key expired applies again.

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

`ApplyTx` offers `Get`, `Query`, `ScanPage`, `Put`, `Patch` and `Delete`. Reads
come from the snapshot taken before the closure ran: they do not see the
closure's own recorded writes, and `Get` on a key you have written is an error
rather than a stale row. `EntityTx` offers `Get`, `Put`, `Patch`, `Delete`,
`Scan`, `Query`, `Seq` and `ScanPage(prototype, afterKey, limit, fn)`. Write
options on both: `WithTextString(field, value)` and `WithTextPeer(peer)`.

Neither consults a client watermark. See
[Write from server-side code](../how-to/server-side-writes.md).

## Reads

```go
func (s *Server) View(ctx, workspaceID string, fn func(tx *ViewTx) error) error
```

`ViewTx` offers `Get`, `Query` and `ScanPage`, each reporting a row's `version`
alongside the message. The caller's `ReadFilter` applies — a filtered row reads
as absent — but no engine-imposed limit caps a result. See
[Query from server-side code](../how-to/server-side-queries.md).

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

## Change feed

```go
func (s *Server) Changes(ctx, workspaceID string, after Cursor, limit int,
    fn func(Change) error) (next Cursor, more bool, err error)
func (s *Server) SubscribeChanges(workspaceID string) (<-chan struct{}, func())
func (s *Server) SubscribeActivity() (ids <-chan string, cancel func(), ok bool)
func (s *Server) CurrentSeq(ctx, workspaceID string) (uint64, error)
```

Each `Change` carries `Actor` — the principal, `client_id` or named system actor
behind the write (see [the change feed](../how-to/change-feed.md#who-wrote-it)).
Because the feed coalesces by row it reports the row's **last** writer, which is
enough for echo suppression and "notify everyone except the actor", and is not
an audit trail.

Returns `ErrCursorTooOld` when the cursor predates the tombstone horizon. See
[Follow committed changes](../how-to/change-feed.md). `CurrentSeq` is the head of
the feed; the distance from a consumer's position to it is that consumer's lag.

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
`FilterEpoch` (plus `EpochReadFilters` for delta resync on permission changes).
Embed `server.AllowAll` to implement a subset.

Error constructors: `server.DenyWorkspace(msg)` (a settled denial) and
`server.Reject(code, msg)`. Rejection codes stay free-form on the wire so an app
can invent its own; the engine ships constants for the ones it uses itself:
`CodePermissionDenied`, `CodeInvalidArgument`, `CodeConflict`,
`CodeWorkspaceAccessRevoked`, `CodeMutationIDGap`, `CodeInternal`,
`CodeTextFullStateRequired` and `CodeTextStateStale`.

See [Authentication and permissions](../how-to/authentication-and-permissions.md).

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

`sqlitestore.Open(path, sqlitestore.WithNamespace(...))`, and
`root.Namespace(id)` to derive one view per app from a single open file. See
[Storage adapter contract](storage-adapter.md).
