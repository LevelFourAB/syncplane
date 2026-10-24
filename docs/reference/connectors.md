# Connectors

The `engine/connect` package provides port interfaces, configuration options, error sentinels, and health monitoring for synchronizing data between external sources and the engine. For a step-by-step guide, see [Sync an external store](../how-to/sync-an-external-store.md).

## The port

A source implements `Source`, along with any optional interfaces it supports. The connector type-asserts for optional interfaces:

```go
// Required.
type Source interface {
    List(ctx context.Context, workspaceID string, fn func(Item) error) error
}

// Required when any entity is bound Bidirectional (the default).
type Writer interface {
    Write(ctx context.Context, workspaceID string, msg proto.Message) (fingerprint string, err error)
    Delete(ctx context.Context, workspaceID string, prototype proto.Message, key string) error
}

// Optional: a source with a native change cursor.
type Incremental interface {
    Checkpoint(ctx context.Context, workspaceID string) (cursor string, err error)
    Changes(ctx context.Context, workspaceID, cursor string) (Delta, error)
}

// Optional, on the value returned by Config.OpenSource: io.Closer.
```

Every method accepts `workspaceID`, even when `OpenSource` creates one source per workspace.

Delivery is at-least-once in both directions. `Write` must be an idempotent upsert. `Delete` must treat a missing item as a no-op.

### Item

The `Item` struct contains the following fields:

| Field         | Description                                                                                                                                                                                                                                                                |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Msg`         | The record decoded into a registered entity message with key fields populated.                                                                                                                                                                                             |
| `Fingerprint` | A fingerprint of the source content, such as a content hash, modification time and size, or row version. Equal content must yield equal fingerprints between `List` and `Write`. The connector uses fingerprint equality to distinguish echoed writes from external edits. |
| `TextFields`  | Plain-string values of the entity collaborative-text fields, keyed by top-level protobuf field name. Leave the field bytes unset on `Msg`.                                                                                                                                 |

The `PlainTextField(msg, field)` helper renders a collaborative-text field of an engine message back to its visible string representation.

### Delta and Deletion

The `Delta` and `Deletion` types represent incremental changes:

```go
type Delta struct {
    Items      []Item
    Deletions  []Deletion // applied after Items; page to interleave
    NextCursor string
    More       bool       // another page is ready now — do not wait for the tick
}

type Deletion struct {
    Prototype proto.Message // any message of the entity type
    Key       string        // the engine row key
}
```

### Direction and Binding

The `Direction` and `Binding` types define the synchronization direction for entities:

```go
type Direction int
const (
    Bidirectional Direction = iota // default
    IngressOnly                    // source → engine only; nothing is written back
)

type Binding struct {
    Prototype proto.Message
    Direction Direction
}
```

`Config.Entities` binds entities bidirectionally. `Config.Bindings` defines bindings with an explicit `Direction`. The connector merges both fields. Binding the same entity in both fields causes an error.

## Config

The `Config` struct contains the following fields:

| Field                         | Default                | Description                                                                                                                                                                                                                         |
| ----------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Server`                      | Required               | The engine instance for ingress and egress operations.                                                                                                                                                                              |
| `Source`                      | Optional               | A single source shared across all workspaces. Specify either `Source` or `OpenSource`.                                                                                                                                              |
| `OpenSource`                  | Optional               | A function with signature `func(ctx, workspaceID) (Source, error)` that constructs and caches one source per workspace. Specify either `Source` or `OpenSource`.                                                                    |
| `State`                       | Required               | Durable state storage. The `sqlitestate.Open` function provides the shipped implementation. The connector takes ownership of `State` after `New`.                                                                                   |
| `Entities`                    | None                   | Prototypes bound bidirectionally.                                                                                                                                                                                                   |
| `Bindings`                    | None                   | Prototypes bound with an explicit `Direction`.                                                                                                                                                                                      |
| `BatchSize`                   | `256`                  | The number of rows per change-feed page during egress.                                                                                                                                                                              |
| `Workers`                     | `16`                   | The number of concurrent passes across all workspaces.                                                                                                                                                                              |
| `ReconcileInterval`           | `1m`                   | The interval between workspace passes and discovery sweeps. A negative value disables periodic passes while discovery sweeps continue at `1m` intervals. For an `Incremental` source, this sets the polling interval for `Changes`. |
| `FullReconcileInterval`       | `1h`                   | The maximum time an `Incremental` source runs using cursor changes before a full `List` pass runs. A negative value runs a full pass only on `ErrCursorInvalid` or a missing cursor. Non-incremental sources ignore this setting.   |
| `RetryMin` / `RetryMax`       | `5s` / `5m`            | The per-workspace backoff bounds after a failed pass. A negative `RetryMin` disables backoff.                                                                                                                                       |
| `IngestChunk` / `IngestPause` | `0` / `0` (disabled)   | The pause duration after every `IngestChunk` writes during an ingress pass. Only writes count toward the chunk count.                                                                                                               |
| `OnHealth`                    | `nil`                  | A callback invoked with the workspace `Health` status after each pass on the calling goroutine.                                                                                                                                     |
| `Workspaces`                  | Engine workspace list  | The list of workspaces that `Start` manages, re-evaluated on every sweep. The connector drops any workspace omitted from the list. Provide this function for source-first workspaces.                                               |
| `WriterID`                    | Per-instance unique ID | The collaborative-text writer identity for the connector. Set a fixed value to maintain identity across restarts. Do not share this value between connectors running concurrently.                                                  |
| `Logger`                      | `slog.Default()`       | The logger for background loops.                                                                                                                                                                                                    |

## Driving a connector

The connector provides the following methods:

| Method                | Description                                                                                                                                            |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Start(ctx)`          | Launches background loops, including the worker pool, activity subscription, and discovery sweep.                                                      |
| `Kick(workspaceID)`   | Schedules an immediate pass and coalesces with pending passes. Starts managing a workspace if the engine has not yet tracked it.                       |
| `Ingress(ctx, ws)`    | Runs one source-to-engine pass. Uses incremental synchronization if the source supports it and the cursor is valid; otherwise runs a full `Reconcile`. |
| `Reconcile(ctx, ws)`  | Runs a full source-to-engine pass by enumerating records, ingesting changes, and deleting rows no longer present in the source.                        |
| `Drain(ctx, ws)`      | Egresses committed engine changes until the change feed is empty. Skips entities configured as `IngressOnly`.                                          |
| `Forget(workspaceID)` | Stops managing the workspace, cancels running passes, and releases the source instance. Preserves shadows and cursors.                                 |
| `Tracked()`           | Returns the number of workspaces with active schedule state.                                                                                           |
| `Close()`             | Stops worker goroutines, closes open source instances, and closes `State`.                                                                             |

`Ingress`, `Reconcile`, and `Drain` run immediately, even within a backoff window. Background schedules wait for backoff. Direct calls and background passes for the same workspace run sequentially.

## Errors

The package defines the following error sentinels and helpers:

```go
var ErrCursorInvalid   = errors.New("connect: source cursor is invalid")
var ErrNeedsUserAction = errors.New("connect: source needs user action")

func RetryAfter(err error, d time.Duration) error
```

The connector handles errors as follows:

| Error                | Source                     | Effect                                                                                                       |
| -------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `ErrCursorInvalid`   | `Incremental.Changes`      | Triggers a fresh `Checkpoint` call and a full `List` pass within the same pass.                              |
| `ErrNeedsUserAction` | Any method or `OpenSource` | Sets `Health.Kind` to `FailureUserAction` and drops the cached source so the next pass rebuilds credentials. |
| `RetryAfter(err, d)` | Any method                 | Pauses the workspace for duration `d` instead of applying standard backoff.                                  |

## Health

The package provides the following health monitoring types and methods:

```go
type Health struct {
    WorkspaceID         string
    LastAttempt         time.Time
    LastSuccess         time.Time
    LastError           error       // nil once a pass succeeds
    Kind                FailureKind // FailureNone | FailureTransient | FailureUserAction
    ConsecutiveFailures int
    NextAttempt         time.Time   // zero when not backing off
}

func (c *Connector) Health(workspaceID string) (Health, bool)
func (c *Connector) HealthAll() []Health
```

The connector updates the `Health` entry for a workspace after every scheduled or direct pass. When a workspace is removed from `Config.Workspaces` or stopped with `Forget`, the connector deletes its health record. The connector classifies any error not marked with `ErrNeedsUserAction` as `FailureTransient`.

## State

`State` provides reconstructible cache storage. If state data is lost, a full reconciliation pass restores it without data loss.

The `State` interface defines the following methods:

```go
type State interface {
    Shadow(ctx, workspaceID, entity, key string) (*Shadow, error)
    PutShadow(ctx, workspaceID, entity, key string, s Shadow) error
    DeleteShadow(ctx, workspaceID, entity, key string) error
    ScanShadows(ctx, workspaceID string, fn func(entity, key string, s Shadow) error) error

    Cursor(ctx, workspaceID string) (server.Cursor, error)
    SetCursor(ctx, workspaceID string, c server.Cursor) error

    SourceSync(ctx, workspaceID string) (SourceSync, error)
    SetSourceSync(ctx, workspaceID string, s SourceSync) error

    Close() error
}
```

The state store manages the following types:

- `Shadow`: Contains `SourceFP`, `EngineVersion`, `Base`, and `BaseText`. Represents the last known matching state for both sides of a row. For bidirectional rows, this is the merged row. For ingress-only rows, `Base` and `BaseText` contain the source view to prevent client-only fields from appearing as source edits.
- `SourceSync`: Contains `Cursor` and `LastFullSync`. Tracks feed progress for an incremental source and the timestamp of the last full enumeration.

Implementations do not require cross-row transactions. The connector orders write operations so recovery from crashes occurs during redelivery or reconciliation.

Store the egress `server.Cursor` value in full, including `Epoch` and across all 64 bits. The `Epoch` field identifies the engine store instance; see [Falling behind](../explanation/effects-and-the-change-feed.md#tombstone-horizons-and-falling-behind). A cursor value of `0` indicates an unknown lineage and skips validation.
