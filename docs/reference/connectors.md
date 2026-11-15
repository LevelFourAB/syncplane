# Connectors

The `engine/connect` package provides port interfaces, configuration options, error sentinels, and health monitoring for synchronizing data between external sources and the engine. For a step-by-step guide, see [Sync an external store](../how-to/sync-an-external-store.md).

A connector runs in one of two modes:

- **Row mode** (`Config.Source` or `Config.OpenSource`): one external item syncs with one row. [The port](#the-port) and [State](#state) describe it.
- **Record mode** (`Config.Records`): one external record, such as a file, syncs with a root row and its member rows. See [Record mode](#record-mode) and [Sync a folder of files as records](../how-to/sync-a-folder-of-files-as-records.md).

Scheduling, backoff, `Health`, and the methods in [Driving a connector](#driving-a-connector) are the same in both modes, except where a table names a difference.

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
    EgressOnly                     // engine → store only; record mode only
)

type Binding struct {
    Prototype proto.Message
    Direction Direction
    Fields    []string // fields the source owns; empty means all
}
```

`Config.Entities` binds entities bidirectionally, and the source owns every field. `Config.Bindings` defines bindings with an explicit `Direction` or `Fields`. The connector merges both fields. Binding the same entity in both fields causes an error.

`Fields` names the fields that the source owns, by top-level protobuf field name, or by oneof name for a whole oneof group. The key fields are always owned. With `Fields` set:

- Ingest takes only the owned fields from an `Item`, in `Msg` and in `TextFields`. Every other field keeps its engine value. A new row starts with those fields empty. Use this for a field that another writer computes and that the source cannot hold.
- Egress writes the whole row, but only for a change to an owned field. A collaborative-text field compares by its visible string.

`New` returns an error for a name that the entity does not declare, and for a member of a oneof. `New` refuses `EgressOnly` on a `Binding`. For `Direction` in record mode, see [RecordType](#recordtype).

## Config

The `Config` struct contains the following fields:

| Field                         | Default                                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ----------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Server`                      | Required                                     | The engine instance for ingress and egress operations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `Records`                     | `nil` (row mode)                             | Switches the connector to record mode. See [Record mode](#record-mode). Cannot be used with `Source`, `OpenSource`, `Entities`, `Bindings` or `State`, and requires `WriterID`.                                                                                                                                                                                                                                                                                                                                                      |
| `Source`                      | Row mode: one of `Source` and `OpenSource`   | A single source shared across all workspaces.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `OpenSource`                  | Row mode: one of `Source` and `OpenSource`   | A function with signature `func(ctx, workspaceID) (Source, error)` that constructs and caches one source per workspace.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `State`                       | Required in row mode; refused in record mode | Durable state storage. The `sqlitestate.Open` function provides the shipped implementation. The connector takes ownership of `State` after `New`.                                                                                                                                                                                                                                                                                                                                                                                    |
| `Entities`                    | None                                         | Row mode. Prototypes bound bidirectionally.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `Bindings`                    | None                                         | Row mode. Prototypes bound with an explicit `Direction` or owned `Fields`.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `BatchSize`                   | `256`                                        | The number of rows per change-feed page during egress. In record mode, the number of changes per feed page that a drain reads.                                                                                                                                                                                                                                                                                                                                                                                                       |
| `Workers`                     | `16`                                         | The number of concurrent passes across all workspaces.                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `ReconcileInterval`           | `1m`                                         | The interval between workspace passes and discovery sweeps. A negative value disables periodic passes while discovery sweeps continue at `1m` intervals. For an `Incremental` source, this sets the polling interval for `Changes`.                                                                                                                                                                                                                                                                                                  |
| `FullReconcileInterval`       | `1h`                                         | The maximum time an `Incremental` source runs using cursor changes before a full `List` pass runs. A negative value runs a full pass only on `ErrCursorInvalid` or a missing cursor. Non-incremental sources ignore this setting.                                                                                                                                                                                                                                                                                                    |
| `RetryMin` / `RetryMax`       | `5s` / `5m`                                  | The per-workspace backoff bounds after a failed pass. A negative `RetryMin` disables backoff.                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `IngestChunk` / `IngestPause` | `0` / `0` (disabled)                         | The pause duration after every `IngestChunk` writes during an ingress pass. Only writes count toward the chunk count. In record mode, an ingest or delete transaction that writes rows counts as one write (see `Records.BatchRows`).                                                                                                                                                                                                                                                                                                |
| `OnHealth`                    | `nil`                                        | A callback invoked with the workspace `Health` status after each pass on the calling goroutine.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `Workspaces`                  | Engine workspace list                        | The list of workspaces that `Start` manages, re-evaluated on every sweep. The connector drops any workspace omitted from the list. Provide this function for source-first workspaces.                                                                                                                                                                                                                                                                                                                                                |
| `WriterID`                    | None                                         | The name of the connector. The connector labels its writes on the change feed as `connect:<WriterID>`, or `connect` when empty. Give each instance of one connector the same value, also an origin and its replicas, so that one constant skips all their writes. With a value set, the connector also keeps a copy of its egress position as the engine consumer position `connect:<WriterID>`, which keeps unsent client edits after a lost `State`. Required in record mode, which keeps all its bookkeeping under that consumer. |
| `TextPeer`                    | `connector:<WriterID>`                       | The collaborative-text writer identity, used as given. Keep the same value across restarts. Two connectors that can write the same rows at the same time, such as an origin and its replicas, must have different values. When `WriterID` and `TextPeer` are both empty, each instance gets a new identity.                                                                                                                                                                                                                          |
| `Logger`                      | `slog.Default()`                             | The logger for background loops.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

## Driving a connector

The connector provides the following methods:

| Method                        | Description                                                                                                                                                     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Start(ctx)`                  | Launches background loops, including the worker pool, activity subscription, and discovery sweep.                                                               |
| `Kick(workspaceID)`           | Schedules an immediate pass and coalesces with pending passes. Starts managing a workspace if the engine has not yet tracked it.                                |
| `Touch(workspaceID, locs...)` | Record mode: reports that the store records at the locators changed, and schedules a pass that ingests them. In row mode it is `Kick` and ignores the locators. |
| `Ingress(ctx, ws)`            | Runs one source-to-engine pass. Uses incremental synchronization if the source supports it and the cursor is valid; otherwise runs a full `Reconcile`.          |
| `Reconcile(ctx, ws)`          | Runs a full source-to-engine pass by enumerating records, ingesting changes, and deleting rows no longer present in the source.                                 |
| `Drain(ctx, ws)`              | Egresses committed engine changes until the change feed is empty. Skips entities configured as `IngressOnly`.                                                   |
| `Flush(ctx, ws)`              | Writes the pending egress of the workspace now. In row mode it is `Drain`.                                                                                      |
| `Refused(ctx, ws, rows)`      | Record mode: reports rows whose write the host reverted. See [Refused writes](#refused-writes). Returns an error in row mode.                                   |
| `Forget(workspaceID)`         | Stops managing the workspace, cancels running passes, and releases the source instance. Preserves shadows and cursors.                                          |
| `Tracked()`                   | Returns the number of workspaces with active schedule state.                                                                                                    |
| `Close()`                     | Stops worker goroutines, closes open source instances, and closes `State`.                                                                                      |

`Ingress`, `Reconcile`, `Drain` and `Flush` run immediately, even within a backoff window. Background schedules wait for backoff. Direct calls and background passes for the same workspace run sequentially.

In record mode the methods do the following. Every pass first completes the store writes that an earlier pass left unconfirmed. Then, when the workspace has had no [first pass](#firstpass), a pass runs it, and `Flush` stops and does nothing. Every pass that continues also deletes the rows of records whose delete grace is over.

| Method      | Record mode                                                                                                                                                                                                                                                                              |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Touch`     | Holds the locators in memory until a pass ingests them. A locator of no record type is ignored. Brings an unmanaged workspace into the managed set, as `Kick` does.                                                                                                                      |
| `Ingress`   | Ingests the locators `Touch` reported. Does not list the store. On a connector that is not started, this is the call that ingests touched locators.                                                                                                                                      |
| `Reconcile` | Lists every record of every type. Ingests new records and records whose fingerprint changed, marks missing records for delete after `Records.DeleteGrace`, and writes a record back to the store when its merge kept engine edits.                                                       |
| `Drain`     | Marks the records that the change feed reports changed, and writes those whose egress is due (see `Records.Quiet` and `Records.MaxDelay`). Before a write it reads the store record. When its fingerprint or its content moved since the last agreed state, it ingests the record first. |
| `Flush`     | Drains the feed and writes every record that waits for egress, also those whose quiet period is not over. Does nothing for a workspace with no first pass.                                                                                                                               |
| `Forget`    | Flushes the workspace first, for at most one minute, and logs a flush error.                                                                                                                                                                                                             |
| `Close`     | Flushes each workspace whose records wait for egress, or whose last pass failed, for at most one minute each, and returns the flush errors with the other errors.                                                                                                                        |

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
    Refused             []RecordFailure // record mode only
}

type RecordFailure struct {
    Entity   string // root entity of the record type
    Key      string // empty for a store record new to the connector
    Locator  string // empty for an engine record the store does not hold yet
    Err      error  // the error of the latest attempt
    Failures int    // failed attempts since the record last synced
}

func (c *Connector) Health(workspaceID string) (Health, bool)
func (c *Connector) HealthAll() []Health
```

The connector updates the `Health` entry for a workspace after every scheduled or direct pass. When a workspace is removed from `Config.Workspaces` or stopped with `Forget`, the connector deletes its health record. The connector classifies any error not marked with `ErrNeedsUserAction` as `FailureTransient`.

`Health.Refused` lists the records that the connector skips and tries again: the codec refused them, or their locator belongs to another record. A skipped record does not fail the pass. The list is ordered by `Entity`, `Locator` and `Key`. A record leaves the list when it syncs, when a full list no longer finds it in the store, or when it is deleted. A new `Connector` starts with an empty list.

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

Store the egress `server.Cursor` value in full, including `Epoch` and `Lineage` across all 64 bits. The `Epoch` field identifies the engine store instance, and `Lineage` identifies the write at the cursor sequence; see [Falling behind](../explanation/effects-and-the-change-feed.md#tombstone-horizons-and-falling-behind). A value of `0` in either field means unknown and skips that check.

## Record mode

Set `Config.Records` to sync each external record with a root row and the member rows that name it. A record is what the store reads and writes whole, such as a file, a JSON document or an API resource. For the model, see [Record mode](../explanation/connectors.md#record-mode).

- The **record key** is the key of the root row. It does not change.
- The **locator** is the address of the record in the store, such as a path. It can change on either side.

Record mode keeps all its bookkeeping in the engine's store, as the position and the shadows of the consumer `connect:<WriterID>`: the feed position, the agreed state of each record, an index from member row to record, an index from locator to record, and the records that wait out `Quiet` or `DeleteGrace`. The bookkeeping is backed up and restored with the rows. Garbage collection does not remove it (see `server.ConsumerUpdate.Retain` in the [server reference](server.md#effect-consumer-state)). `server.ResetConsumer` removes it, and the next pass then runs the first pass again.

`New` refuses a record-mode `Config` when one of these is true:

- `Source`, `OpenSource`, `Entities`, `Bindings` or `State` is set.
- `WriterID` is empty.
- `Records.Types` is empty, or `Records.FirstPass` is not `SourceWins` or `EngineWins`.
- A type has no `Root` or no `Codec`, has both or neither of `Store` and `OpenStore`, or has an unknown `Direction`.
- An entity is not registered, or is the root or a member of more than one type.
- A `Member` has no `Prototype`, or its `RecordField` is empty or is not a dotted path of singular fields that ends in a string field.
- A `RowCodec` fails its checks (see [RowCodec](#rowcodec)).

### Records

| Field             | Default            | Description                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Types`           | Required           | The record types. Each entity can be the root or a member of one type only.                                                                                                                                                                                                                                                                                                                                      |
| `FirstPass`       | `SourceWins`       | The [first-pass policy](#firstpass) of every workspace.                                                                                                                                                                                                                                                                                                                                                          |
| `ChooseFirstPass` | `nil`              | `func(ctx, workspaceID) (FirstPass, error)`. Decides the policy of one workspace and overrides `FirstPass`. Called at the start of each first pass of the workspace, also after a first pass that failed, and never on a later pass. Can run for different workspaces at the same time. An error, or a value that is not `SourceWins` or `EngineWins`, fails the pass before it reads the store or writes a row. |
| `Quiet`           | `1s`               | How long a record must have no engine change before egress writes it, so that a run of edits makes one write. Negative writes at the next pass.                                                                                                                                                                                                                                                                  |
| `MaxDelay`        | `10 × Quiet`       | The longest time a record waits for egress after its first engine change that the store has not received, also while changes keep coming faster than `Quiet`. Negative means no bound. No effect when `Quiet` is negative: records then do not wait.                                                                                                                                                             |
| `DeleteGrace`     | `2s`               | How long a record must be missing from the store before its rows are deleted. A record that comes back in this time keeps its rows. A new record with the fingerprint of a missing one is a rename of it. Negative deletes at the end of the pass that found the record missing.                                                                                                                                 |
| `BatchRows`       | `1000`             | The maximum rows that one ingest transaction merges across records. A record with more rows is merged alone. `Config.IngestChunk` and `Config.IngestPause` count these transactions.                                                                                                                                                                                                                             |
| `Overwrite`       | `nil`              | `func(ctx, Overwrite) error`. Called before egress replaces or deletes store content that holds edits the engine does not have. See [Overwrite](#overwrite). An error stops the write and fails the pass. `nil` writes with no call.                                                                                                                                                                             |
| `KeepReplaced`    | `0` (keep nothing) | How long the connector keeps the store content that an egress write or delete replaced, when the record had an ingest within this time before. Lets a `Refused` call that comes after the egress still give the refused content to `Overwrite`. One copy for each record at most, in the engine's store.                                                                                                         |

### FirstPass

The first pass runs when the consumer `connect:<WriterID>` has no position in the workspace: on the first sync, and after `server.ResetConsumer`. A connector that did not run for longer than the tombstone retention keeps its position and marks every record for egress. It does not run the first pass again.

| Value        | Effect                                                                                                                                                                                                                                                                                                                                                            |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SourceWins` | Imports the store. Each record is merged with no base. Root rows and member rows that no store record holds are deleted. When the codec refuses a record, the pass fails and deletes no row.                                                                                                                                                                      |
| `EngineWins` | Rebuilds the store from the rows. Each root row is rendered and written to the store, and `Records.Overwrite` gets each store record it replaces with other content (`OverwriteFirstPass`). A store record with no rows is ingested as a new record. No row is deleted. When the codec cannot render a root row, the pass fails before it ingests a store record. |

A first pass that fails leaves no position, and the next pass runs the first pass again. Records it already agreed on are skipped.

### RecordType

| Field       | Default                        | Description                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Root`      | Required                       | A prototype of the root entity. Its row key is the record key.                                                                                                                                                                                                                                                                                                                                 |
| `Members`   | None                           | The member entities. A type with no members syncs one row as one record.                                                                                                                                                                                                                                                                                                                       |
| `Store`     | One of `Store` and `OpenStore` | A `RecordStore` for the records of every workspace.                                                                                                                                                                                                                                                                                                                                            |
| `OpenStore` | One of `Store` and `OpenStore` | `func(ctx, workspaceID) (RecordStore, error)`. Builds the store of one workspace on its first pass, and later passes reuse it. A store that implements `io.Closer` is closed on `Close`, on `Forget`, and after a pass that fails with `ErrNeedsUserAction`. An error fails the pass with `connect: open <root> store for workspace "<ws>": <err>`, and the next pass calls `OpenStore` again. |
| `Codec`     | Required                       | Maps records to rows and rows to records. A `Codec` or a `RowCodec`.                                                                                                                                                                                                                                                                                                                           |
| `Direction` | `Bidirectional`                | See the table below.                                                                                                                                                                                                                                                                                                                                                                           |

| Direction       | Record mode                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Bidirectional` | Store edits ingress into rows, and engine edits egress to the store.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `IngressOnly`   | Never writes to the store. Engine edits stay in the engine, and ingest does not render the record.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `EgressOnly`    | Never writes a row: the engine is the only source of the records. A store edit is not merged. The next pass that sees it, or egresses the record, writes the engine's render over it, after a call to `Overwrite` with `OverwriteEgressOnly`. A record the store deleted is written again. Missing marks that an earlier run with another direction left are dropped, and the record is written again. A store record that no root row renders to stays in the store. The first pass writes every root row to the store, whatever the `FirstPass` policy is. |

### Member

```go
type Member struct {
    Prototype   proto.Message
    RecordField string // dotted path of the string field that holds the root key
}
```

`RecordField` is a singular string field, such as `"page_id"`, or a path through singular message fields, such as `"page.id"`. The connector finds the members of a record with a query on this field: declare an index on it for records with many rows.

### RecordStore

```go
type RecordStore interface {
    List(ctx context.Context, workspaceID string, fn func(RecordRef) error) error
    Read(ctx context.Context, workspaceID, locator string) (Record, error)
    Write(ctx context.Context, workspaceID string, rec Record, from string) (fingerprint string, err error)
    Delete(ctx context.Context, workspaceID, locator string) error
}

type RecordRef struct {
    Key         string // optional
    Locator     string
    Fingerprint string
}

type Record struct {
    RecordRef
    Content []byte
}

var ErrRecordNotFound = errors.New("connect: record not found")
```

| Method   | Contract                                                                                                                                                                                                  |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `List`   | Yields each record of the type in the workspace, in any order. Must be cheap: the connector reads the content only of records whose fingerprint changed.                                                  |
| `Read`   | Returns the record at the locator. Returns an error that wraps `ErrRecordNotFound` when there is none, also for a locator of another record type.                                                         |
| `Write`  | Puts `rec.Content` at `rec.Locator`, replacing what is there, and returns the fingerprint of the content as stored. `from` is the old locator when the record moved, which the store removes; else empty. |
| `Delete` | Removes the record at the locator. Removing a missing record is not an error.                                                                                                                             |

Delivery is at least once, so each method must be idempotent. Every method takes the workspace ID, so one `Store` serves all workspaces. A store from `OpenStore` gets only the ID of its own workspace. The connector calls the methods of one workspace from one goroutine at a time. Calls for different workspaces can run at the same time.

| `RecordRef` field | Description                                                                                                                                                                                                                                                                                                           |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Key`             | The record key, when the store knows it, such as an ID the content holds. Used for a locator the connector does not know yet, so that a record the store moved keeps its key. A known locator keeps the key of its record. Empty finds the key of a renamed record by `Fingerprint`, or gives a new record a new key. |
| `Locator`         | The address of the record in the store.                                                                                                                                                                                                                                                                               |
| `Fingerprint`     | A stamp of the content, such as a content hash. Equal content must give an equal fingerprint in `List`, `Read` and `Write`: it is how the connector tells its own writes from edits. A content hash also lets the connector find a renamed record by its content.                                                     |

### Codec

```go
type Codec interface {
    Merge(tx *server.EntityTx, in MergeInput) (Merged, error)
    Render(tx *server.EntityTx, in RenderInput) (out Rendered, ok bool, err error)
}
```

The methods run inside a trusted write transaction, under the workspace write lock: keep them free of I/O. The transaction can roll back and run again, also when another record merged in it fails, so the methods must have no effect outside `tx`. `Render` must give the same content for the same rows: the connector writes each render whose content differs from the store's. One `Codec` serves all workspaces and must be safe for concurrent use.

- `Merge` applies a store record to the rows of the record with key `in.Key`, through `tx`. With a base, it applies the changes from the base to the record and keeps engine edits that the record did not change. With no base, it makes the rows equal to the record. A replay of a committed merge with the old base must write nothing: write a row only when it changes.
- `Render` returns the native form of the record from its rows. `ok` false deletes the record from the store. `Render` can write rows, for example to give new rows a key that the native form holds. `Render` is not called when there is no root row: the record is then deleted from the store.

An error from `Merge` or `Render` rolls back the transaction: no row and no shadow of the record changes. The connector logs the error, lists the record in `Health.Refused`, skips it, and tries it again when it next sees a change of it. In a first pass, a refused record can fail the pass instead (see [FirstPass](#firstpass)).

| Type          | Field                                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MergeInput`  | `Key`                                                     | The record key: the key of the root row that the record merges into.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|               | `Record`                                                  | The store record.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               | `Base`                                                    | The state that the store and the engine last agreed on. `nil` when the connector has no shadow of the record.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|               | `Root`                                                    | The root row read in the transaction. `nil` when there is none.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|               | `Members`                                                 | The member rows of each member entity, in the order of `RecordType.Members`, in key order. `Root` and `Members` are read in the transaction. `Merge` must not change them.                                                                                                                                                                                                                                                                                                                                                                        |
|               | `Alternate`                                               | Set when the connector wrote the record to the store after `Base`, and cannot tell whether the write was done before the store changed the record again: the state that write would have agreed on. The store content continues `Base` or `Alternate`. A `Merge` that ignores it merges against `Base`, and then applies the engine edits of a write that was done a second time: collaborative text holds them twice. `nil` when there is no such write.                                                                                         |
|               | `TextPeer`                                                | The collaborative-text peer to write text under. See `Config.TextPeer`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|               | `TextClock`                                               | The highest text counter that the rows of the record have held, also in text that the host reverted since (see [Refused writes](#refused-writes)). `0` for none. Text written under `TextPeer` must take counters above it, or an item can get the ID of a reverted item that a client still holds. Decode the document with `text.Decode` under `TextPeer` and call `ObserveVersion(map[string]uint64{TextPeer: TextClock})` before the edit. `server.ApplyTextStrings` observes only the documents it is given, so it cannot apply `TextClock`. |
| `Merged`      | `Key`                                                     | The key of the root row the record merged into, when `Merge` matched a new record to an existing root row with another key. Empty keeps `MergeInput.Key`. A record with a base cannot change its key.                                                                                                                                                                                                                                                                                                                                             |
|               | `Payload`                                                 | Codec data for the agreed state, such as the row key of each part. Given back as `Agreed.Payload`.                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `RenderInput` | `Key`, `Base`, `Root`, `Members`, `TextPeer`, `TextClock` | As in `MergeInput`. `Root` is never `nil`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `Rendered`    | `Locator`                                                 | Where the record goes. Empty keeps the locator of the base. A record with no base must have one.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               | `Content`                                                 | The native form.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|               | `Payload`                                                 | Codec data for this content.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `Agreed`      | `Locator`, `Fingerprint`, `Payload`                       | The state that the store and the engine last agreed on.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

### RowCodec

`RowCodec` is a `Codec` for records whose parts map to rows. `Decode` turns content into rows, `Encode` turns rows into content, and the connector merges the decoded rows into the engine one row at a time, against the rows as the store held them when the two sides last agreed.

```go
type RowCodec struct {
    Decode func(in DecodeInput) (Decoded, error)  // required, except for EgressOnly
    Encode func(in EncodeInput) (Rendered, error) // required, except for IngressOnly
    Fields map[proto.Message][]string             // owned fields, keyed by an entity prototype
}

type RecordRows struct {
    Root    proto.Message
    Members []proto.Message
}

func (d *Decoded) SetText(row proto.Message, field, value string)
```

Merge rules:

- A field that the record changed since the agreed state takes the record's value. A field that the record did not change keeps the engine's value, also when a client changed it.
- A collaborative-text field that the record changed is merged with the engine's document, so a store edit and an engine edit both stay, also within one field.
- A member row that the record held and no longer holds is deleted. A member row that a client added, and that the record never held, stays, and the next write puts it in the store.
- A member row that `Decode` gives to another record by its key moves to that record. When the store changes both records in one pass, the row keeps the fields that the new record does not own and its text history: the connector deletes a left-out row at the end of the merge transaction, and only when its `RecordField` still names the old record. A row that leaves one record in one pass and joins another in a later pass is deleted and created again.
- When the connector cannot tell whether its last store write was done (see `MergeInput.Alternate`), each text field merges against the ancestor whose text is nearest the store's, by edit distance. A scalar field counts as a store edit only when it differs from both ancestors. `Decode` gets the `Base` whose `Encode` output is nearest the store content.
- With no agreed state, as on the first sync of a record, the rows take the record's values for the owned fields, and the member rows that the record does not hold are deleted.
- Fields that the record does not own always keep their engine value.

`Fields` names the owned fields of each entity of the type, by top-level field name or by oneof name for a whole oneof group, as `Binding.Fields` does. The key fields and the top-level field of `Member.RecordField` are always owned. An entity with no entry owns every field. Do not own a field that `Decode` cannot read back from the content: on the next change of the record, its empty value counts as an edit.

`Decode` and `Encode` run inside the merge transaction, under the workspace write lock: keep them free of I/O. They can be called more than once for one change, after the transaction rolls back, and at the same time for different records, so they must have no effect outside the call and be safe for concurrent use. `Encode` must give the same content for the same rows. A new key from `id.New()` for a new part is allowed. The rows that `Decode` returns for the content that `Encode` made must be equal to the rows `Encode` got, in the owned fields. An error from either refuses the record, as for any `Codec` error. Use a `RowCodec` only as `RecordType.Codec`: its `Merge` and `Render` methods return an error when called directly.

The agreed state of each record holds a copy of its rows, with their collaborative-text documents, and each ingest and each write of the record writes that copy again. The bookkeeping of a record costs about the size of its rows in storage, and that much again in writes on each change.

| Type          | Field     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DecodeInput` | `Key`     | The record key. A root row with an empty key gets it.                                                                                                                                                                                                                                                                                                                                                                                           |
|               | `Record`  | The store record.                                                                                                                                                                                                                                                                                                                                                                                                                               |
|               | `Base`    | The rows as the store held them at the agreed state. `Base.Root` is `nil` when there is no agreed state. Members are in no fixed order. Match the parts of the content to these rows to give each part the key of its row (see [MatchKeys](#matchkeys)). When the connector cannot tell whether its last store write was done, `Base` holds the rows before or after that write, whichever `Encode` makes into content nearer `Record.Content`. |
|               | `Rows`    | The rows of the record in the engine now, members in the order of `RecordType.Members` and in key order. A member row here that `Base` does not hold is a row a client added, or one that an earlier merge of this content wrote: give a part the key of such a row when they are equal, so that a repeated merge writes nothing.                                                                                                               |
|               | `Payload` | Adopter data of the agreed state, from `Decoded.Payload` or the `Rendered.Payload` of `Encode`. `nil` when there is none.                                                                                                                                                                                                                                                                                                                       |
| `Decoded`     | `Root`    | Required. An empty key gets the record key when the root entity has one string key field. Any other key must be the record key.                                                                                                                                                                                                                                                                                                                 |
|               | `Members` | The member rows, with their keys set. A member with an empty `RecordField` gets the record key. One that names another record refuses the record.                                                                                                                                                                                                                                                                                               |
|               | `Payload` | Adopter data for this content.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `EncodeInput` | `Key`     | The record key.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|               | `Locator` | The locator of the agreed state. Empty when the store does not hold the record yet: `Encode` must then return one in `Rendered.Locator`.                                                                                                                                                                                                                                                                                                        |
|               | `Payload` | As in `DecodeInput`.                                                                                                                                                                                                                                                                                                                                                                                                                            |
|               | `Rows`    | The rows of the record. `Rows.Root` is not `nil`. Read a collaborative-text field with `PlainTextField`.                                                                                                                                                                                                                                                                                                                                        |

The rows in `DecodeInput` and `EncodeInput` are not retained after the call returns. Do not change them. Their collaborative-text fields hold documents: read them with `PlainTextField`. In `Decoded`, give a text field its visible value with `SetText(row, field, value)` and leave its bytes unset: the connector merges the value into the engine's document. An owned text field that `SetText` does not set is empty in the record.

`New` refuses a `RowCodec` with no `Decode` on a type that is not `EgressOnly`, with no `Encode` on a type that is not `IngressOnly`, with a `Fields` entry for an entity outside the type, or with a name that the entity does not declare. `ExampleRowCodec` in `engine/connect/example_rowcodec_test.go` is a compiling text-file codec.

### MatchKeys

```go
func MatchKeys[P comparable](prev []P, prevKeys []string, next []P) []string
```

Gives the parts of `next` the keys of the parts of `prev` they continue, for content whose parts carry no key in the store, such as the lines of a text file. `prevKeys[i]` is the key of `prev[i]`.

- A part of `next` that is equal to a part of `prev` in a longest common subsequence of the two takes its key.
- Between two such pairs, the parts of `next` take the keys of the parts of `prev` in order, so an edited part keeps its key.
- A part with no key gets `""`: give it a new key. When the keys in `prevKeys` are distinct, each key goes to one part at most.
- A part that moved past others gets `""`, so a move is a delete and a create.

Panics when `prev` and `prevKeys` differ in length. Memory is proportional to `len(prev)+len(next)`, and time to that sum times the number of parts inserted and deleted.

### Overwrite

```go
type Overwrite struct {
    WorkspaceID string
    Entity      string // the root entity of the record type
    Key         string
    Locator     string
    Reason      OverwriteReason
    Old         []byte // the content the store holds now
    New         []byte // the content that replaces it; nil for a delete
}
```

`Old` and `New` are not retained after the call returns. `OverwriteReason.String` returns `first_pass`, `egress_only` or `refused`, and `unknown` for any other value.

| Reason                | When                                                                                                                                                                                                                                                                |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OverwriteFirstPass`  | A first pass with `EngineWins` replaces a store record with other content.                                                                                                                                                                                          |
| `OverwriteEgressOnly` | A record of an `EgressOnly` type has store content that changed since the connector wrote it.                                                                                                                                                                       |
| `OverwriteRefused`    | A record of rows that `Connector.Refused` reported. `Old` can be content that an earlier egress replaced. `New` is then the content that the store holds now, which can hold store edits made after that egress, and is `nil` when the store holds no record there. |

In all other cases, egress never replaces a store record that the connector does not know. It ingests that record as a new record, and the engine record whose locator it holds waits until the locator is free. The waiting record is listed in `Health.Refused`.

### Refused writes

```go
type RowRef struct {
    Entity string // wire name of the entity
    Key    string
}

func (c *Connector) Refused(ctx context.Context, workspaceID string, rows []RowRef) error
```

`Refused` reports rows whose write the host could not keep and reverted, such as writes of this connector that an origin server refused. On a replica, call it from `upstream.Config.OnRejected` (see `ExampleConnector_Refused`).

- The connector durably marks the record of each row and schedules a pass.
- The next egress of a marked record gives the store content that it replaces or deletes to `Records.Overwrite` with `OverwriteRefused` first.
- When egress wrote the record before the call, the content it replaced is given instead, if `Records.KeepReplaced` kept it. Else nothing is given.
- One refusal can give two contents: the one egress replaced, when it was kept, and the one the store holds.
- A mark is cleared after use, and after a pass that finds its record written and no kept content. A call that comes while a pass uses an earlier mark of the record makes a mark of its own, which the next pass uses.

Rows of entities outside record mode, of types that are not `Bidirectional`, and rows that no record holds are ignored. Calling it more than once for one refusal is safe. `Refused` is safe for concurrent use. It returns an error in row mode, for an empty workspace ID, and when the engine's store fails. Nothing is marked then.
