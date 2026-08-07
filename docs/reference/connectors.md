# Connectors

Everything `engine/connect` exposes: the port interfaces an adopter
implements, every `Config` field, the error sentinels a source wraps, and the
health surface a product renders. For the task-shaped version, see
[Sync an external store](../how-to/sync-an-external-store.md).

## The port

A source implements `Source`, plus whichever optional interfaces it can
support. The connector type-asserts for the optional ones.

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

Every method takes `workspaceID`, including when `OpenSource` already mints
one source per workspace, so one implementation can serve both wirings.

Delivery is at-least-once in both directions: `Write` must be an idempotent
upsert and `Delete` must treat a missing item as a no-op.

### Item

| Field         | Meaning                                                                                                                                                                                                                                   |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Msg`         | The record decoded into a registered entity message, key fields populated.                                                                                                                                                                |
| `Fingerprint` | A cheap stamp of the source's content — content hash, mtime+size, rowversion. Equal content **must** yield equal fingerprints between `List` and `Write`; that equality is how the connector tells its own echoed writes from real edits. |
| `TextFields`  | Plain-string values of the entity's collaborative-text fields, keyed by top-level proto field name. Leave the field bytes unset on `Msg`.                                                                                                 |

`PlainTextField(msg, field)` renders a text field of an engine message back to
its visible string — the egress half of `TextFields`.

### Delta and Deletion

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

`Config.Entities` is the all-`Bidirectional` shorthand; `Config.Bindings` is
the explicit form. They merge, and binding one entity through both is an
error.

## Config

| Field                         | Default          | Meaning                                                                                                                                                                                                                       |
| ----------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Server`                      | required         | The engine to ingress into and egress out of.                                                                                                                                                                                 |
| `Source`                      | one of two       | A single source shared by every workspace.                                                                                                                                                                                    |
| `OpenSource`                  | one of two       | `func(ctx, workspaceID) (Source, error)`; one source per workspace, built lazily and reused.                                                                                                                                  |
| `State`                       | required         | Durable bookkeeping; `sqlitestate.Open` is the shipped implementation. Owned by the connector after `New`.                                                                                                                    |
| `Entities`                    | —                | Prototypes bound bidirectionally.                                                                                                                                                                                             |
| `Bindings`                    | —                | Prototypes bound with an explicit direction.                                                                                                                                                                                  |
| `BatchSize`                   | 256              | Rows per change-feed page during egress.                                                                                                                                                                                      |
| `Workers`                     | 16               | How many passes run at once, across every workspace. The only bound between a fleet of workspaces and the source.                                                                                                             |
| `ReconcileInterval`           | 1m               | How often each workspace runs a pass, and how often workspaces are discovered and dropped. Negative disables periodic passes; discovery still runs, at 1m. For an `Incremental` source this is how often `Changes` is polled. |
| `FullReconcileInterval`       | 1h               | How long an `Incremental` source may ride its cursor before a full `List` runs as a safety net. Negative means only on `ErrCursorInvalid` or a missing cursor. Ignored for non-incremental sources.                           |
| `RetryMin` / `RetryMax`       | 5s / 5m          | Per-workspace backoff bounds after a failed pass. Negative `RetryMin` disables backoff.                                                                                                                                       |
| `IngestChunk` / `IngestPause` | 0 / 0 (off)      | After every `IngestChunk` engine writes, an ingress pass sleeps `IngestPause`. Only writes count.                                                                                                                             |
| `OnHealth`                    | nil              | Called with the workspace's `Health` after every pass. Runs on the calling goroutine.                                                                                                                                         |
| `Workspaces`                  | engine's list    | Which workspaces `Start` manages, re-read every sweep — one it stops naming is dropped. Supply this for source-first workspaces.                                                                                              |
| `WriterID`                    | per instance     | The connector's collaborative-text writer identity. Pin it to keep one identity across restarts; never share it between connectors that can run at the same time.                                                             |
| `Logger`                      | `slog.Default()` | Where the background loops log.                                                                                                                                                                                               |

## Driving a connector

| Call                  | Does                                                                                                                  |
| --------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `Start(ctx)`          | Launches the background loops: a pool of `Workers`, the activity subscription, and the discovery sweep.               |
| `Kick(workspaceID)`   | Schedules a near-immediate pass, coalescing with one already pending. Manages a workspace the engine has never seen.  |
| `Ingress(ctx, ws)`    | One source→engine pass: incremental when the source supports it and the cursor is live, a full `Reconcile` otherwise. |
| `Reconcile(ctx, ws)`  | One full source→engine pass: enumerate, ingress every change, tombstone rows the source no longer holds.              |
| `Drain(ctx, ws)`      | Egresses committed engine changes until the feed is dry. Skips `IngressOnly` entities.                                |
| `Forget(workspaceID)` | Drops the workspace, cancelling any pass in flight, and releases its source. Shadows and cursors survive.             |
| `Tracked()`           | How many workspaces the connector holds schedule state for.                                                           |
| `Close()`             | Stops the workers, closes any opened sources, closes `State`.                                                         |

`Ingress`, `Reconcile` and `Drain` all attempt immediately, even inside a
backoff window — the caller asked. Only the background schedule waits. They do
still take the workspace's turn, so a direct call and a background pass for one
workspace run one after the other rather than interleaving their writes.

## Errors

```go
var ErrCursorInvalid   = errors.New("connect: source cursor is invalid")
var ErrNeedsUserAction = errors.New("connect: source needs user action")

func RetryAfter(err error, d time.Duration) error
```

| Wrap                 | From                        | Effect                                                                                                             |
| -------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `ErrCursorInvalid`   | `Incremental.Changes`       | A fresh `Checkpoint` plus a full `List`, in the same pass.                                                         |
| `ErrNeedsUserAction` | any method, or `OpenSource` | `Health.Kind` becomes `FailureUserAction`, and the cached source is dropped so the next pass rebuilds credentials. |
| `RetryAfter(err, d)` | any method                  | The workspace waits exactly `d` instead of its own backoff.                                                        |

## Health

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

One entry per workspace, updated after every pass — scheduled or a direct
call — so a manually driven connector is just as observable. A workspace that
stops being managed (`Forget`, or dropping out of `Config.Workspaces`) loses
its entry with the rest of its state. Anything the
source does not explicitly mark as needing user action classifies as
`FailureTransient`.

## State

`State` is a reconstructible cache, not a ledger: losing it costs a full
reconcile, never data.

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

- `Shadow` — `SourceFP`, `EngineVersion`, `Base`, `BaseText`: what both sides
  of one row looked like when they were last known equal. For a bidirectional
  row that is the merged row; for an ingress-only row `Base` and `BaseText`
  hold the **source's** view, which is what keeps a client-only field from
  reading as a source edit.
- `SourceSync` — `Cursor` and `LastFullSync`: how far an incremental source's
  feed has been consumed, and when the last full enumeration ran.

Implementations need no cross-row transactionality; the connector orders its
writes so any crash between two `State` calls is healed by redelivery or
reconcile.

Store the egress `server.Cursor` whole, `Epoch` included, and across all 64
bits — it is a random value, so half of them set the high bit. That field names
the engine store the cursor came from, and it is the only thing that stops a
connector from egressing rows out of a database swapped in behind the same
address (see [Falling behind](../how-to/change-feed.md#falling-behind)). Dropping
it is not an error: 0 reads as "lineage unknown" and the check goes quiet.
