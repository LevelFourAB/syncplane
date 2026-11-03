# Upstream link

Package `github.com/levelfourab/syncplane/engine/upstream` keeps a local syncplane engine converged with an origin server. A `Link` binds one local `*server.Server` to one origin. Subpackages `presencebridge` and `blobbridge` carry presence and blob bytes across the same hop.

For the model, see [Chained sync](../explanation/chained-sync.md). For walkthroughs, see [Embed sync in a Go desktop app](../how-to/embed-sync-in-a-go-desktop-app.md), [Forward sync to browsers](../how-to/forward-sync-to-browsers.md) and [Read and write the local engine from Go](../how-to/read-and-write-the-local-engine-from-go.md). For a compiling example, see `Example` in `engine/upstream/example_test.go`.

## Lifecycle

```go
link, err := upstream.New(cfg)                                     // validates; does not connect
srv, err := server.New(store, append(opts, link.ServerOptions()...)...)
err = link.Start(ctx, srv)                                         // loads state; syncs in the background
err = link.AddWorkspace(ctx, "acme")
defer link.Close()
```

A `*Link` is safe for concurrent use.

| Method                            | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `New(cfg) (*Link, error)`         | Returns a link that is not started. Fails when `OriginURL` is not absolute, when `Bindings` is empty, when a binding names no entity or one entity twice, or an unknown direction, and when `ClientID` breaks the client-id rules.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `ServerOptions() []server.Option` | `server.WithWriteCapture`, `server.WithUnknownMutatorForwarding`, and `server.WithoutTextCompaction` for every bound entity. Pass them to `server.New` of the engine later given to `Start`. A link installs on one engine. With them, the engine refuses a local write to a bound entity made before `Start` (`ErrNotStarted`), and one to a workspace the link does not sync (`permission_denied`).                                                                                                                                                                                                                                                                 |
| `Start(ctx, srv) error`           | Binds the link to `srv` and starts syncing every workspace linked before, with its stored subscriptions. Returns once the link state is loaded; reads each workspace of the local store once. It finds the linked workspaces in the link's own list, read from the primary store, so a store whose workspace list lags (a read replica) cannot hide one. Syncing runs until `Close` or until `ctx` ends. A workspace whose state does not load reports `StateFailed`, and the others sync. Fails when called twice, when `srv` is nil, or when a bound entity is not registered on `srv` or its schema cannot be replayed. A `Start` that failed can be called again. |
| `Close() error`                   | Ends the origin stream, waits for in-flight calls and state writes, and returns. Local writes after `Close` queue for the next `Start`. Safe to call more than once.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `ClientID() string`               | The client id the link writes to the origin under, `""` before `Start`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |

Shut down in this order: cancel the context of `blobbridge`'s `Run` and wait for it to return, `Close` the link, `Close` the engine, `Close` the `presencebridge` and `blobbridge` stores the engine was built with, then `Close` the storage adapter. `Link.Close` tolerates an engine or store closed first: its in-flight state writes fail, and it returns once its goroutines stopped.

## Workspaces

| Method                                          | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AddWorkspace(ctx, id, subs...) error`          | Links a workspace and syncs it in the background. No subscriptions means every row of every bound entity. The link stores the workspace and its subscriptions, so a later `Start` resumes it. Adding a linked workspace sets its subscriptions. Adding a revoked or failed one starts it over. Fails with `ErrNotStarted` before `Start`, `ErrClosed` after `Close`, and an error for an invalid id or a subscription to an entity that is not bound. |
| `AddWorkspaceWith(ctx, id, AddOptions) error`   | `AddWorkspace` with options; see below.                                                                                                                                                                                                                                                                                                                                                                                                               |
| `SetSubscriptions(ctx, id, subs...) error`      | Replaces the subscriptions of a linked workspace. The origin sends what enters the new scope, and the link removes what leaves it. A local write to a row outside the scope is forwarded; the row shows locally until the origin confirms the write, then leaves the local engine. The status leaves `StateLive` until the new scope is caught up. Returns `ErrNotLinked` for a workspace the link does not sync.                                     |
| `RemoveWorkspace(ctx, id, RemoveOptions) error` | Stops syncing the workspace and deletes its link state, pending writes included: writes the origin has not received are lost, so call `Flush` first to keep them. Local writes to its bound entities are refused afterwards. Does nothing for a workspace that is not linked.                                                                                                                                                                         |
| `Workspaces() []string`                         | The linked workspace ids, sorted.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `Status(id) (Status, bool)`                     | The status of a linked workspace; `false` when the link does not sync it.                                                                                                                                                                                                                                                                                                                                                                             |
| `Flush(ctx, id) error`                          | Blocks until the workspace has no write the origin has not confirmed, every rejection went to `OnRejected`, and the workspace is caught up. Can be called from a callback. Returns `ErrNotLinked`, the status's `LastError` for a `StateFailed` workspace (`ErrNotLinked` when its state was deleted), `ErrRevoked`, or `ctx`'s error.                                                                                                                |
| `Await(ctx, id, seq) error`                     | Blocks until the origin decided the local write at local sequence `seq`. Returns `nil` when applied and `*RejectedError` when rejected. Before a verdict it returns `ErrUnknownWrite` when the link has no record of a forwarded write at `seq`, `ErrRevoked` once the origin revoked the workspace, `ErrNotLinked` when the link does not sync it (any more), `ErrClosed` when the link closed, or `ctx`'s error.                                    |
| `Workspace(id) *Workspace`                      | A typed handle for Go code; see [`Workspace`](#workspace). The id is not checked until a call uses it.                                                                                                                                                                                                                                                                                                                                                |

### `AddOptions`

| Field            | Meaning                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Subscriptions`  | Scope the workspace, as for `AddWorkspace`.                                                                                                                                                                                                                                                                                                                                                                     |
| `AdoptLocalRows` | The first snapshot pushes every local row of a bound entity that the origin does not hold, as a PUT, instead of deleting it. A row the origin holds takes the origin's value either way. Applies to a workspace that is not linked yet, until its first snapshot is applied. Without it, the first snapshot deletes every local row of a bound entity that it does not carry and that no pending write touches. |

### `RemoveOptions`

| Field        | Meaning                                                                                                                                                                                              |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DeleteRows` | Also deletes the workspace's local rows of every bound entity, in pages of one local transaction each. A removal stopped part way finishes after the next `Start`. Rows of local-only entities stay. |

## `Config`

| Field                             | Default                | Meaning                                                                                                                                                                                                                                                                 |
| --------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OriginURL`                       | required               | Base URL of the origin, for example `https://sync.example.com`. The sync service is expected at its standard path below it.                                                                                                                                             |
| `HTTPClient`                      | `http.DefaultClient`   | Carries every call to the origin. A client `Timeout` also ends the live stream; leave it unset.                                                                                                                                                                         |
| `Interceptors`                    | none                   | Connect interceptors added to every call, in order. The link has one identity at the origin: every workspace syncs as the caller these establish.                                                                                                                       |
| `ConnectOptions`                  | none                   | Connect client options added to every call, for example `connect.WithGRPC`.                                                                                                                                                                                             |
| `Bindings`                        | required, at least one | The entities that sync with the origin. An entity no binding names is local-only. A local mutation that writes only local-only entities is not forwarded, a custom mutation the local engine runs included.                                                             |
| `WriteMode`                       | `ModeWriteBehind`      | The write mode of a local write whose context names none.                                                                                                                                                                                                               |
| `ClientID`                        | stored or random       | The client id the link writes to the origin under. Empty means the id stored with the workspaces, or a new random id when none is stored. Two links must never share an id. 1 to 128 bytes, no control characters (`\u0000` to `\u001f`).                               |
| `OnRejected`                      | none                   | Called for each local write the origin rejected. See [Callbacks](#callbacks).                                                                                                                                                                                           |
| `OnReset`                         | none                   | Called after the link replaced a workspace's rows with a fresh snapshot, for a reason other than the first sync.                                                                                                                                                        |
| `OnAccessRevoked`                 | none                   | Called when the origin refused a workspace for good. The link stops syncing it and refuses local writes to its bound entities.                                                                                                                                          |
| `OnStatus`                        | none                   | Called with a workspace's status each time it changes.                                                                                                                                                                                                                  |
| `OnSchemaSkew`                    | `SkewWarn`             | Decides how a workspace reacts when the local engine's schema revision differs from the origin's. See [Schema skew](#schema-skew).                                                                                                                                      |
| `DeleteOnRevoke`                  | false                  | Deletes the local rows of the bound entities of a revoked workspace, as `RemoveOptions.DeleteRows` does.                                                                                                                                                                |
| `RetryMin`, `RetryMax`            | 1s, 30s                | Backoff after a failed call to the origin: a random wait up to a window that doubles from `RetryMin` to `RetryMax`. 0 keeps the default.                                                                                                                                |
| `ConcurrentSyncs`                 | 4                      | How many workspaces bootstrap or catch up at once; the others wait.                                                                                                                                                                                                     |
| `StreamIdleTimeout`               | adaptive               | How long the live stream may deliver nothing before the link opens another. 0 means three times the longest gap seen between the origin's heartbeats, from 2s to 90s. Until two heartbeats were seen, it is 10s, doubled up to 90s each time a stream ends for silence. |
| `PageLimit`                       | origin's default       | The most changes the link asks for per pull or stream page.                                                                                                                                                                                                             |
| `MaxPushMutations`                | 256                    | The most mutations one push carries.                                                                                                                                                                                                                                    |
| `MaxPushBytes`                    | 4 MiB                  | The encoded size at which a push stops adding mutations. A single larger mutation goes alone.                                                                                                                                                                           |
| `WriteThroughTimeout`             | 10s                    | Bounds the call a write-through write makes to the origin.                                                                                                                                                                                                              |
| `PoisonThreshold`                 | 5                      | How many times in a row the origin may fail the first pending write the same way (`mutation_failed`, `invalid_argument` or an unknown code) before the link drops it and reports it as rejected with that code. Negative never drops.                                   |
| `Logger`                          | `slog.Default()`       | Receives the link's background failures.                                                                                                                                                                                                                                |
| `TracerProvider`, `MeterProvider` | global                 | The OpenTelemetry providers the link records on. See [Telemetry](#telemetry).                                                                                                                                                                                           |

### `Binding` and `Direction`

```go
type Binding struct {
	Prototype proto.Message // a message of the entity; or
	Entity    string        // the entity's wire name, for an entity without a generated message
	Direction Direction     // zero value: Both
}
```

One of `Prototype` and `Entity` is required. When both are set they must name the same entity.

| `Direction` | Behavior                                                                                                                               |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Both`      | The link pulls the entity and forwards local writes to it.                                                                             |
| `PullOnly`  | The link pulls the entity. A local write to it fails with `permission_denied`, except inside a custom mutation, which the origin runs. |

### `WriteMode`

| Mode               | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ModeWriteBehind`  | A local write commits at once and goes to the origin later. A write the origin rejects is reverted locally and reported through `OnRejected`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `ModeWriteThrough` | A local write goes to the origin, after every earlier pending write, before it commits locally. It fails with the origin's rejection (a `server.Rejection` with the origin's code), or with an error that wraps `ErrOriginUnavailable` when the origin cannot be reached, and commits nothing then. The call holds the local store's write lock for the whole round trip: every other write to the workspace waits, the link's own included, and on a store with one writer (`sqlitestore`) every write to every workspace waits, so the workspace's pulls and live stream stall too. A downstream client's push uses this mode as well: when the origin cannot be reached, the push's whole batch fails with `internal`, which the client retries. |

`WriteThrough(ctx)` and `WriteBehind(ctx)` return a context whose local writes use that mode, whatever `Config.WriteMode` says. A push from a downstream client uses `Config.WriteMode`.

## Callbacks

The callbacks run on the link's goroutines, and two of them can run at the same time. A callback can call the link and write to the local engine. The workspace's sync waits while one runs.

| Callback          | Delivery                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `OnRejected`      | After the link reverted the write's rows. Per workspace one call at a time, in the order the rejections arrived, on a goroutine of its own. At least once: a rejection is forgotten when its call returns; one whose call ran when the process crashed is reported again after the next `Start`, with the same `MutationID`. `Close` waits for a running call, then forgets its rejection. |
| `OnReset`         | After the fresh snapshot is applied.                                                                                                                                                                                                                                                                                                                                                       |
| `OnAccessRevoked` | When the origin refused the workspace for good.                                                                                                                                                                                                                                                                                                                                            |
| `OnStatus`        | Each time the workspace's status changes.                                                                                                                                                                                                                                                                                                                                                  |
| `OnSchemaSkew`    | On the workspace's worker, once for each new pair of local and origin revisions, before the workspace next forwards local writes.                                                                                                                                                                                                                                                          |

### `Rejection`

| Field        | Meaning                                                                                    |
| ------------ | ------------------------------------------------------------------------------------------ |
| `Workspace`  | The workspace of the write.                                                                |
| `LocalSeq`   | The local sequence the write took; 0 for a custom mutation that wrote no local row.        |
| `MutationID` | The id the link pushed the write under.                                                    |
| `Code`       | The origin's rejection code, for example `conflict` or `permission_denied`.                |
| `Message`    | Human-readable detail for logs.                                                            |
| `Mutator`    | The custom mutation's name, `""` for a CRUD write.                                         |
| `Rows`       | The bound rows (`RowRef{Entity, Key}`) the write touched locally, which the link reverted. |

`*RejectedError` embeds a `Rejection`. `Await` and `Receipt.Wait` return it.

## `Status`

| Field            | Meaning                                                                                                                                                                                                                                                                 |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Workspace`      | The workspace id.                                                                                                                                                                                                                                                       |
| `State`          | See below. `State.String()` gives the lowercase name in the second column.                                                                                                                                                                                              |
| `Pending`        | Local writes the origin has not confirmed through a pull yet, those it applied but did not send back included. A write counts from shortly after it commits.                                                                                                            |
| `OriginSeq`      | The origin sequence the confirmed rows are current through.                                                                                                                                                                                                             |
| `OriginHeadSeq`  | The origin's newest sequence, from the last page or snapshot the link received; `0` when unknown (nothing received yet, or an origin that does not report it). `Behind()` returns `OriginHeadSeq - OriginSeq`, or `0` when it is not ahead. It counts writes, not rows. |
| `SchemaRevision` | The schema revision the origin advertised last, `""` when none.                                                                                                                                                                                                         |
| `SchemaSkew`     | The standing skew (`*SchemaSkew{Local, Origin, Action}`), `nil` when the revisions agree, when either side advertises none, or when the answer was `SkewIgnore`.                                                                                                        |
| `LastError`      | The last failure; `nil` after the next success.                                                                                                                                                                                                                         |
| `Bootstrap`      | The snapshot download progress (`*BootstrapProgress`), `nil` while no bootstrap runs. Set for the whole attempt, also while `State` is `offline` after a failed call.                                                                                                   |

`OnStatus` receives a change of `Bootstrap` alone at most every 250 ms, and the newest one within 250 ms. It always receives the start and the end of the download.

| `BootstrapProgress` field | Meaning                                                                                                                                                                                      |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RowsApplied`             | Snapshot rows applied to the local store, by every attempt of the snapshot.                                                                                                                  |
| `RowsEstimated`           | Expected size of the snapshot in rows, `0` when the origin gives no estimate (read permissions or a narrowing subscription hide rows, or the store cannot count). Never below `RowsApplied`. |
| `EntitiesDone`            | Entities whose rows are all applied.                                                                                                                                                         |
| `EntitiesTotal`           | Entities the snapshot ships, `0` until the origin announces them.                                                                                                                            |

The progress is stored with the bootstrap state, so a resumed snapshot continues from what the earlier attempts applied, also after a restart. A snapshot the origin starts again starts from 0. `RowsApplied / RowsEstimated` is at most 1 and does not go down during one snapshot. See [Show download progress for the first sync](../how-to/show-bootstrap-progress.md).

| `State`              | Name            | Meaning                                                                                                                         |
| -------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `StateConnecting`    | `connecting`    | The link has not reached the origin for the workspace since it started.                                                         |
| `StateBootstrapping` | `bootstrapping` | The workspace's rows are being replaced with a snapshot of the origin.                                                          |
| `StateLive`          | `live`          | Caught up; changes arrive as they happen.                                                                                       |
| `StateOffline`       | `offline`       | The last call to the origin failed. The link retries with backoff; local writes queue.                                          |
| `StateNoAccess`      | `no_access`     | The origin grants the link's caller no permission in the workspace yet. The link asks again with backoff; local writes queue.   |
| `StateRevoked`       | `revoked`       | The origin refused the workspace for good. The link no longer syncs it.                                                         |
| `StateFailed`        | `failed`        | The link cannot sync the workspace: its link state does not load, or it was deleted from the local store. `LastError` says why. |

## Schema skew

The link compares the local engine's revision (`server.WithSchemaRevision`, or the hash of a registered descriptor set) with the revision the origin advertises. It compares before each round of forwarding: after a local write, a `Flush`, a retry, or a change of the origin's revision. When either side advertises no revision, the link does not compare.

For each new pair of revisions, the link calls `Config.OnSchemaSkew(workspace, local, origin)` and applies the `SkewAction` it returns:

| `SkewAction` | Behavior                                                                                                                                                                                                                                                                                           |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SkewWarn`   | The default. `Status.SchemaSkew` reports the skew, and the workspace keeps syncing.                                                                                                                                                                                                                |
| `SkewIgnore` | The workspace keeps syncing, and `Status.SchemaSkew` stays `nil`.                                                                                                                                                                                                                                  |
| `SkewBlock`  | `Status.SchemaSkew` reports the skew, and the link stops forwarding local writes. Pulls continue. Local writes queue, and `Flush` waits. A write-through write fails with `ErrSchemaSkew`. The block lifts when the revisions agree, or when `OnSchemaSkew` answers another action for a new pair. |

Downstream clients compare their revision with the revision of the local engine, not of the origin.

## `Workspace`

`*Workspace` is the Go app's handle on one workspace of the local engine. Its writes go through the local engine's write path, so the link captures and forwards them like any other local write. Its reads see the local rows, pending writes included. Handles are cheap and safe for concurrent use.

Calls authorize against `ctx` as `server.Apply` and `server.View` do; mark `ctx` with `server.Trusted` for system work. A write's mode follows `Config.WriteMode` unless `ctx` was made with `WriteThrough` or `WriteBehind`. Every call fails with `ErrNotStarted` before `Start`.

| Function or method                                             | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Put(ctx, msg, opts...) (Receipt, error)`                      | Writes `msg` as a whole row, as `server.ApplyTx.Put`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `Patch(ctx, msg, mask, expected, opts...) (Receipt, error)`    | Merges the fields `mask` names, as `server.ApplyTx.Patch`. A non-nil `expected` is a compare-and-swap against the row's local version.                                                                                                                                                                                                                                                                                                                                                                                                |
| `Delete(ctx, prototype, key) (Receipt, error)`                 | Deletes the row.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `Apply(ctx, fn func(*server.ApplyTx) error) (Receipt, error)`  | Runs `fn` as `server.Apply` does; one receipt for every write it recorded.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `Invoke(ctx, name, args) (Receipt, error)`                     | Runs a custom mutation, as `server.Invoke`. A mutator the local engine declares runs locally as a prediction that the origin's result replaces; one it does not declare is forwarded and writes nothing locally.                                                                                                                                                                                                                                                                                                                      |
| `Flush(ctx) error`                                             | `Link.Flush` for the workspace.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `Watch(ctx, from *server.Cursor) iter.Seq2[ChangePage, error]` | Committed changes after `from`, page by page as they commit, origin changes and local writes alike. Each `ChangePage` holds `Changes` and `Cursor`, the feed position after them with the store lineage the feed stamped on it; pass it back as `from` to resume. `nil` starts at the current head. Ends without an error when `ctx` ends, or after the first error; `server.ErrCursorTooOld` reports a `from` the feed can no longer serve, from another store lineage included. Changes to one row between two pages arrive as one. |
| `Get[T](ctx, w, key) (Row[T], bool, error)`                    | One local row. `false` when absent, deleted or hidden from the caller.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `Query[T](ctx, w, spec) ([]Row[T], error)`                     | Every local row of `T`'s entity that `spec` matches, in `spec`'s order. Build `spec` with the `engine/query` constructors. The whole result is decoded into one slice in one read: bound it with `spec.Limit` and page with `spec.Offset`. Without a secondary index that covers the predicate and order, the read scans the whole entity.                                                                                                                                                                                            |
| `ID() string`                                                  | The workspace id.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

`T` is a pointer to a generated message type. `Row[T]` has `Key`, `Version` (the local sequence that last wrote the row) and `Value`.

### `Receipt`

| Member            | Meaning                                                                                                                                                                                                                                                                                                                                                         |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Workspace`       | The workspace of the write.                                                                                                                                                                                                                                                                                                                                     |
| `Seq`             | The local sequence the write took; 0 when it wrote no row.                                                                                                                                                                                                                                                                                                      |
| `Forwarded`       | The write was queued for the origin. A write to local-only entities is not.                                                                                                                                                                                                                                                                                     |
| `Wait(ctx) error` | Blocks until the origin decided the write. `nil` when applied or not forwarded, `*RejectedError` when rejected. Before a verdict: `ErrRevoked` once the origin revoked the workspace, `ErrNotLinked` when the link does not sync it (any more), `ErrClosed` when the link closed, `ErrUnknownWrite` when the link has no record of the write, or `ctx`'s error. |

## Errors

| Error                  | Returned when                                                                                                                                                                            |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ErrNotStarted`        | A call that needs a started link, `Workspace` calls included, and a local write to a bound entity made before `Start`.                                                                   |
| `ErrClosed`            | A call after `Close`.                                                                                                                                                                    |
| `ErrNotLinked`         | A workspace the link does not sync.                                                                                                                                                      |
| `ErrRevoked`           | `Flush`, `Await` and `Receipt.Wait` for a workspace the origin refused for good.                                                                                                         |
| `ErrUnknownWrite`      | `Await` and `Receipt.Wait` for a write with no record: a write that forwarded nothing, one confirmed before the process started, or one confirmed long enough ago to be forgotten.       |
| `ErrSchemaSkew`        | A write-through write to a workspace that `SkewBlock` holds. The write did not commit locally and did not reach the origin.                                                              |
| `ErrOriginUnavailable` | Wrapped by the error of a write-through write when the origin did not answer. The write did not commit locally; the origin may have applied it, and then it arrives back through a pull. |
| `*RejectedError`       | `Await` and `Receipt.Wait` for a write the origin rejected.                                                                                                                              |

A local write that the link refuses fails with a `server.Rejection` (test with `server.AsRejection`):

| Code                       | Cause                                                                                                    |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| `permission_denied`        | A CRUD write to a `PullOnly` entity, or a write to a bound entity of a workspace the link does not sync. |
| `workspace_access_revoked` | A write to a bound entity of a workspace the origin revoked.                                             |
| `invalid_argument`         | A compare-and-swap on a row that an earlier operation of the same mutation wrote.                        |
| the origin's code          | A write-through write the origin rejected.                                                               |

A push from a downstream client gets the same codes as `REJECTED` results.

## Storage

The link keeps its state in the local store as consumer shadows under the consumer name `syncplane.upstream`, in the same transactions as the rows. Names that start with `server.ReservedConsumerPrefix` (`syncplane.`) belong to the engine; every method that takes a consumer name refuses them. The state holds one record per present row of a bound entity, the pending writes, and the cursor. A record holds the origin version of the row. It also holds a copy of the confirmed row while pending writes touch the row; otherwise the local row is the confirmed row.

## Telemetry

| Instrument                      | Type    | Unit          | Attributes                 |
| ------------------------------- | ------- | ------------- | -------------------------- |
| `syncplane.upstream.pushes`     | counter | `{push}`      | `syncplane.outcome`        |
| `syncplane.upstream.pages`      | counter | `{page}`      | `syncplane.outcome`        |
| `syncplane.upstream.bootstraps` | counter | `{bootstrap}` | `syncplane.outcome`        |
| `syncplane.upstream.rejections` | counter | `{mutation}`  | `syncplane.rejection_code` |
| `syncplane.upstream.pending`    | gauge   | `{mutation}`  | —                          |

`pending` sums `Status.Pending` over every linked workspace. Spans `syncplane.upstream.push`, `syncplane.upstream.apply_page` and `syncplane.upstream.bootstrap` carry `syncplane.workspace_id`.

## `presencebridge`

Package `engine/upstream/presencebridge` joins the rooms of a local engine to the rooms of the origin. A `*Bridge` is a `presence.Bus`; install it with `server.WithPresenceBus`.

```go
func New(inner presence.Bus, origin syncplanev1connect.PresenceServiceClient, opts ...Option) *Bridge
```

`inner` is the bus the local engine would use without the bridge; `nil` takes `presence.NewLocal()`. `origin` carries the bridge's identity at the origin, for example `client.Client.PresenceService()`. `New` starts the bridge's goroutines, does not contact the origin, and panics when `origin` is nil. A `*Bridge` is safe for concurrent use.

| Method                   | Behavior                                                                                                                                                                                                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Publish(ctx, ev) error` | Hands `ev` to `inner`. When `ev`'s workspace is bridged and `inner` accepted it, queues it for the origin. Returns `inner`'s error. Never blocks on the origin: when the origin queue is full, the event is not forwarded.                                         |
| `Subscribe(topic)`       | Events from `inner` and, for a bridged workspace, the origin's participants and broadcasts. The first subscription to a bridged topic starts watching it on the origin; the cancel of the last one stops. Origin events reach only the subscribers of this bridge. |
| `Close() error`          | Stops watching and publishing to the origin and withdraws every origin participant from the local rooms. Does not close `inner`. Blocks until the goroutines stop. Idempotent. Close the engine first.                                                             |

| Option                                  | Default                    | Effect                                                                                                                                                                                                     |
| --------------------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithWorkspaces(ids...)`                | every workspace            | Bridges the named workspaces only. Replaces an earlier `WithWorkspaces` or `WithWorkspaceFilter`.                                                                                                          |
| `WithWorkspaceFilter(fn)`               | every workspace            | Bridges the workspaces for which `fn` returns true. `fn` runs on every publish and room subscription, from many goroutines. An open room keeps the answer `fn` gave when it opened, for its downward half. |
| `WithAnnounce(d)`                       | `presence.DefaultAnnounce` | How often origin participants are re-announced locally. Must be at most the local engine's `server.WithRoomAnnounce`.                                                                                      |
| `WithOriginLimits(maxTopics, maxItems)` | 64, 32                     | The origin's `MaxRoomsPerStream` and `MaxPublishItems`. The bridge opens one origin stream per `maxTopics` watched rooms.                                                                                  |
| `WithReconnectBackoff(min, max)`        | 250ms, 30s                 | Wait before a retry of a failed origin call; doubles per failure, with jitter.                                                                                                                             |
| `WithLogger(l)`                         | `slog.Default`             | Logger for failed origin calls and lost streams.                                                                                                                                                           |

`OriginIssuer` (`syncplane.upstream`) is the issuer of every participant and broadcast sender relayed from the origin. An `Authenticator` must not issue it.

Behavior across the hop:

- Upward, every presence update, departure and broadcast in a bridged workspace is published to the origin under its local session id. The origin stamps them with the bridge's principal. The first local principal that publishes a session id claims it; publishes for that session from another local principal are not forwarded.
- Downward, origin participants appear locally with the subject the origin reports. Sessions the local engine published are left out.
- The workspace sequence on a presence update is cleared on both hops.
- Delivery is at most once on both hops. A broadcast sent while the origin is unreachable is lost.
- A room the origin refuses stays local and is retried with backoff.
- A bridge serves one engine built with `server.New`, not a `server.Manager`. On several local nodes over a shared inner bus, install a bridge on every node.

## `blobbridge`

Package `engine/upstream/blobbridge` moves blob bytes between a local engine and the origin. A `*Bridge` is a `blob.Store` that wraps the local engine's store. Pass it to `server.WithBlobStore`, then call `Run`.

```go
func New(inner blob.Store, origin syncplanev1connect.BlobServiceClient, opts ...Option) *Bridge
func (b *Bridge) Run(ctx context.Context, srv *server.Server) error
```

The bridge owns `inner`: `Close` closes it. The local server must use the bridge as its only blob store and must not be hosted by a `server.Manager`. The local workspace id is the origin workspace id. Without a `Run` in progress, the bridge is `inner` and nothing more. A `*Bridge` is safe for concurrent use.

`Run` moves blobs until `ctx` ends, waits for the transfers in progress, and returns `nil`. It returns `ErrRunning` when another `Run` of the bridge has not returned, and the server's error for an invalid consumer name.

- **Uploads.** A blob is uploaded when it commits locally and when a row that names it commits. `Run` follows each local workspace's change feed from a position it keeps under its consumer name. At start it also checks every committed local blob, a bounded number at a time, and uploads those that a live row names and the origin does not report complete. An interrupted upload resumes. Failed transfers retry with backoff. A refusal by the origin (`invalid_argument`, `resource_exhausted`, `permission_denied`) is logged and retried at the next `Run`.
- **Read-through.** A read (`Stat`) of a blob that `inner` does not hold, and that a live local row names, starts a fetch from the origin. Concurrent reads share one fetch. Each chunk is checked against its hash. `Stat` waits up to the read wait for the blob to commit, then up to `StageWait` (10s) for it to be staged; a staged blob reads as being uploaded, and the local engine notifies the workspace when it commits.

| Option                   | Default                              | Effect                                                                                                             |
| ------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `WithEntities(names...)` | every entity                         | Counts only blobs that rows of these entities name. Use the bound entities.                                        |
| `WithPrefetch()`         | off                                  | Fetches a blob as soon as a row that names it commits locally, while `Run` runs.                                   |
| `WithReadWait(d)`        | 0                                    | How long a read waits for the fetch to commit the blob. 0 waits only for the fetch to stage it.                    |
| `WithFetchPatience(d)`   | 10m (`DefaultFetchPatience`)         | How long the bridge keeps trying to fetch a blob after the last read or prefetch that asked for it.                |
| `WithConcurrency(n)`     | 4 (`DefaultConcurrency`)             | Transfers and checks that run at the same time. Below 1 means 1.                                                   |
| `WithBackoff(min, max)`  | 1s, 1m                               | Wait before a retry of a failed transfer or check; doubles per failure, with jitter.                               |
| `WithSweepInterval(d)`   | 1m (`DefaultSweepInterval`)          | How often `Run` lists local workspaces whose change feed has unread rows, to find writes no notification reported. |
| `WithConsumerName(name)` | `blobbridge` (`DefaultConsumerName`) | The change-feed consumer name `Run` keeps its positions under. No other consumer of the local engine may use it.   |
| `WithLogger(l)`          | `slog.Default()`                     | Logger for transfer failures.                                                                                      |

## Server options the link uses

`link.ServerOptions()` returns these. They are documented with the server in [Chained sync options](server.md#chained-sync).

- `server.WithWriteCapture(c)`
- `server.WithUnknownMutatorForwarding()`

The link also uses `Server.Replicate`, `Server.ReadState` and `Server.WorkspaceIDs`, and keeps its list of linked workspaces in a workspace whose id starts with `server.ReservedWorkspacePrefix`.
