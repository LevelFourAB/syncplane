# Go client

Package `github.com/levelfourab/syncplane/engine/client` is a Go client for a remote syncplane server. It keeps no rows: every read goes to the server, and every write is pushed before the call returns. It wraps the generated Connect clients of `SyncService`, `BlobService` and `PresenceService`.

For a walkthrough, see [Use the Go client against a remote server](../how-to/use-the-go-client.md). For a compiling example, see `Example` in `engine/client/example_test.go`.

## Construction

```go
func New(baseURL string, opts ...Option) (*Client, error)
```

`baseURL` is the server's base URL, for example `https://sync.example.com`. The services are expected at their standard paths below it. `New` does not connect. It returns an error when `baseURL` is not absolute, or when the `WithClientID` value breaks the client-id rules.

| Option                     | Default              | Effect                                                                                                                                                                                                                    |
| -------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithHTTPClient(c)`        | `http.DefaultClient` | The HTTP client every call goes through. A client `Timeout` also ends `Subscribe` and presence streams.                                                                                                                   |
| `WithInterceptors(i...)`   | none                 | Connect interceptors added to every call, in order.                                                                                                                                                                       |
| `WithConnectOptions(o...)` | none                 | Connect client options added to every call, for example `connect.WithGRPC`.                                                                                                                                               |
| `WithClientID(id)`         | random per `Client`  | The client id that mutations, pulls and streams carry. With a set id, the client reads the server's watermark for it before its first write to a workspace. 1 to 128 bytes, no control characters (`\u0000` to `\u001f`). |
| `WithMaxAttempts(n)`       | 5                    | How many times a write sends its push when it fails in transit or the server reports a transient failure. Values below 1 mean 1.                                                                                          |

## `Client`

A `Client` is safe for concurrent use. Writes to one workspace run one at a time: each waits until the previous one has its outcome. Writes to different workspaces run concurrently.

| Method                                                | Returns                                                                                                                                                                     |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Workspace(id) *Workspace`                            | A handle on one workspace. All handles on one id share the per-workspace write state. The id is not checked until a call sends it.                                          |
| `ClientID() string`                                   | The client id the client sends.                                                                                                                                             |
| `Subscribe(ctx, scopes, opts...)`                     | `iter.Seq2[Event, error]`: one live stream over several workspaces. See [Subscribe](#subscribe).                                                                            |
| `SyncService()`, `BlobService()`, `PresenceService()` | The generated Connect clients, with the client's HTTP client and options. A push through `SyncService` bypasses the mutation-id counter; do not push under the client's id. |

## `Workspace`

A `*Workspace` is safe for concurrent use.

### Writes

| Method                                    | Effect                                                                                                    |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `Put(ctx, msg, opts...) (Result, error)`  | Replaces the whole row `msg` addresses, or creates it.                                                    |
| `Patch(ctx, msg, paths, opts...)`         | Writes the fields `paths` names. Does not create a row: a patch of an absent row fails with `conflict`.   |
| `Delete(ctx, prototype, key, opts...)`    | Deletes the row. Deleting an absent row is not an error.                                                  |
| `Apply(ctx, ops...) (Result, error)`      | Pushes `ops` as one mutation. Several ops apply in order and atomically, at one sequence, or not at all.  |
| `Invoke(ctx, name, args) (Result, error)` | Runs the server's custom mutator `name` with `args` packed as `google.protobuf.Any`. `nil` sends no args. |
| `Confirm(ctx) (Result, error)`            | Sends an unconfirmed write again at once. Zero `Result` and `nil` when none is unconfirmed.               |

Each write blocks until the server answers, and until every earlier write of the client to the workspace has its outcome. An unconfirmed earlier write is sent again first; when its outcome is still unknown, the new write returns that error and sends nothing.

Build ops for `Apply` with these functions. Errors in an op are reported by `Apply`.

| Function                             | Op                     |
| ------------------------------------ | ---------------------- |
| `Put(msg, opts...) Op`               | As `Workspace.Put`.    |
| `Patch(msg, paths, opts...) Op`      | As `Workspace.Patch`.  |
| `Delete(prototype, key, opts...) Op` | As `Workspace.Delete`. |

`paths` are dotted proto field paths, as a `google.protobuf.FieldMask` carries them. A `Put` merges collaborative-text fields with the stored documents and keeps the stored value of every field the caller may not write.

#### Write options

| Option                   | Effect                                                                                                                                                                                                                                                                                                                                              |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithExpectedVersion(v)` | Compare-and-swap: the server applies the op only when the stored row's version is `v`, and rejects it with `conflict` otherwise. An absent or deleted row has version 0. Read `v` from `Row.Version` or `Result.Seq`.                                                                                                                               |
| `WithText(path, value)`  | Sets the collaborative-text field at `path` from a plain string. The write reads the stored row, makes the document read as `value` with the fewest edits, and sends the whole document. A document inside a keyed list or map is named by its instance path (`notes.<key>.body`). On a `Patch` the path joins the mask. Not allowed on a `Delete`. |

#### `Result`

| Field            | Meaning                                                                                            |
| ---------------- | -------------------------------------------------------------------------------------------------- |
| `MutationID`     | The id the write was pushed under.                                                                 |
| `Seq`            | The workspace sequence the write took, the version of every row it wrote. 0 when `AlreadyApplied`. |
| `AlreadyApplied` | The server had applied the mutation before: an earlier attempt landed and its response was lost.   |

### Reads

| Function or method                                                         | Returns                                                                                                                                                                                                                            |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Get[T](ctx, w, key) (Row[T], bool, error)`                                | One row. `false` when the row is absent, deleted or hidden from the caller.                                                                                                                                                        |
| `Query[T](ctx, w, spec) (Page[T], error)`                                  | One page of a query, with the caller's read rules. `spec.Limit` of 0 or less asks for the server's default; the server clamps every limit. Without a covering index, the server scans the whole entity per call.                   |
| `QueryAll[T](ctx, w, spec) iter.Seq2[Row[T], error]`                       | Every matching row, page by page. `spec.Limit` is the page size and `spec.Offset` applies to the first page only. Rows written between two pages can be missed or seen twice. A spec with `Before` but no `After` yields an error. |
| `w.Bootstrap(ctx, opts...) (*Snapshot, error)`                             | A snapshot of every row the caller can read. Range over `Snapshot.Rows` or call `Snapshot.Close` to release the stream.                                                                                                            |
| `w.Pull(ctx, cursor, opts...) iter.Seq2[*syncplanev1.PullResponse, error]` | The pages of changes after `cursor`, until the server reports no more. A `nil` cursor starts at the beginning, which only a workspace with no collected tombstones can serve. Yields `ErrReset` when the cursor cannot continue.   |

`T` is a pointer to a generated message type. Build `spec.Where` and `spec.OrderBy` with the `engine/query` constructors.

`Pull` and `Subscribe` send the highest sequence the client's writes were applied at, so a server that reads from a replica never answers with state older than them.

#### `Row[T]` and `Page[T]`

| Field                                | Meaning                                                                                               |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `Row.Key`                            | The row key.                                                                                          |
| `Row.Version`                        | The workspace sequence that last wrote the row.                                                       |
| `Row.Value`                          | The row. `nil` when `Deleted`. A field in `Hidden`, or sent as a text delta, reads as its zero value. |
| `Row.Hidden`                         | The fields the caller may not read.                                                                   |
| `Row.Deleted`                        | A tombstone. Only rows from a pull or a stream can be tombstones.                                     |
| `Page.Rows`                          | Ordered by the query's `OrderBy`, ties by row key.                                                    |
| `Page.StartCursor`, `Page.EndCursor` | The positions of the first and last row, for `Spec.Before` and `Spec.After`. Empty for an empty page. |
| `Page.HasMore`                       | More rows follow past the end the limit counted from.                                                 |

#### `Snapshot`

| Member                | Meaning                                                               |
| --------------------- | --------------------------------------------------------------------- |
| `Cursor`              | The position the snapshot captures. Pass it to `Pull` or `Subscribe`. |
| `Seq`                 | The workspace sequence `Cursor` captures.                             |
| `LastAckedMutationID` | The server's watermark for the client's id.                           |
| `SchemaRevision`      | The server's schema revision, `""` when it advertises none.           |
| `Entities`            | The entities the snapshot ships rows of, in delivery order.           |
| `Rows()`              | The rows, once. A second range yields an error. Holds no tombstones.  |
| `Close()`             | Releases the stream. Safe to call more than once.                     |

#### Read options

| Option                       | Applies to          | Effect                                                                                                                                                                                               |
| ---------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithSubscriptions(subs...)` | `Bootstrap`, `Pull` | Scopes the read to `subs`. Without it, every entity. `Subscribe` ignores it; set `Scope.Subscriptions`.                                                                                              |
| `WithPageLimit(n)`           | `Pull`, `Subscribe` | The most changes per page. 0 asks for the server's default; the server clamps to its maximum.                                                                                                        |
| `WithNoDeltaEntities(e...)`  | `Pull`, `Subscribe` | Sends every collaborative-text field of the named entities in full. Without it, a page can carry a field as a fragment (`RowChange.text_deltas`), which only a caller that keeps the rows can apply. |

### Subscribe

```go
func (c *Client) Subscribe(ctx context.Context, scopes []Scope, opts ...ReadOption) iter.Seq2[Event, error]
```

Opens one stream over `scopes` and yields catch-up pages, then live changes. Heartbeats are not yielded. The stream does not reconnect. Iteration ends without an error when `ctx` ends or every scope was dropped, and yields an error when the stream fails.

`Scope` has `Workspace`, `Cursor` (`nil` starts at the beginning) and `Subscriptions` (empty means every entity). Exactly one of these `Event` fields is set:

| Field      | Meaning                                                                                                                                 |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `Page`     | Changes for `Event.Workspace`. Store its `NextCursor` once the changes are applied; an empty `NextCursor` keeps the previous one.       |
| `CaughtUp` | The backlog of the workspace is drained; live changes follow.                                                                           |
| `Dropped`  | The workspace left the stream: `ErrReset`, or a `*WorkspaceError` with `workspace_access_revoked`, `workspace_no_access` or `internal`. |

`Event.SchemaRevision` carries the server's schema revision.

### Blobs

| Method                                                                          | Effect                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `UploadBlob(ctx, r io.ReaderAt, size, mediaType) (*syncplanev1.BlobRef, error)` | Uploads `size` bytes of `r` and returns the reference to store in a row. Reads `r` twice: once for the digest, then for each chunk the server is missing. A blob the server holds sends no chunks; an interrupted upload resumes. Uses memory for one chunk (`blob.ChunkSize`).                       |
| `DownloadBlob(ctx, digest, dst io.Writer) (int64, error)`                       | Writes the blob to `dst`, checking each chunk against its hash. Waits while the blob is being uploaded. Fails with `NotFound` when no row the caller can read names the blob, when the server has no bytes, or when the upload it waits for is removed. On an error, `dst` can hold part of the blob. |

Server errors arrive as `*connect.Error`, for example `ResourceExhausted` for a blob above the server's size limit.

### Rooms

| Method                                  | Returns                                                                                                 |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `Room(name) *Room`                      | A session in an unanchored room: workspace access admits a caller. The session id is random per `Room`. |
| `AnchoredRoom(name, entity, key) *Room` | A session in a room anchored to one row: only a caller that can read the row is admitted.               |

A `*Room` is safe for concurrent use. The server stores none of its state.

| Method                         | Effect                                                                                                                                                                                                                                                             |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SetPresence(ctx, state, seq)` | Replaces the session's presence state at the next revision. `seq` is the workspace sequence the state was made at, 0 when it does not matter. Without an open `Watch`, the state expires unless you call `SetPresence` again at the interval the response names.   |
| `Broadcast(ctx, payload)`      | Sends `payload` to every session watching the room now, this one included.                                                                                                                                                                                         |
| `Watch(ctx)`                   | Yields the roster, then updates, departures, broadcasts and at most one `TopicDropped`. Keeps the session's presence alive while open. Does not reconnect. Ends without an error when `ctx` ends, after a dropped frame, or yields an error when the stream fails. |
| `Leave(ctx)`                   | Withdraws the session; watchers see it leave at once.                                                                                                                                                                                                              |
| `SessionID()`                  | The session id the room publishes and watches as.                                                                                                                                                                                                                  |

## Entity names and keys

| Function                                                  | Returns                                                                                                                                                                                                                                |
| --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EntityName(msg) (string, error)`                         | The entity name of `msg`'s type. `msg` can be a nil pointer of a generated type.                                                                                                                                                       |
| `EntityKey(msg) (entity, key string, err error)`          | The entity and row key `msg` addresses, derived from its `(syncplane.v1.entity)` options as the server derives them. Fails for a message without entity options, an unusable key field, or a key the [naming rules](naming.md) refuse. |
| `DecodeRow[T](rc *syncplanev1.RowChange) (Row[T], error)` | One change decoded as `T`. Fails for a change of another entity or a value that does not decode as `T`.                                                                                                                                |

## Errors

| Error                      | Returned when                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*RejectedError`           | The server rejected the mutation. Final: the server wrote nothing and gives the same answer to every resend. Has `Workspace`, `MutationID`, `Code` and `Message`.                                                                                                                                                                                                                                                                                                       |
| `*WorkspaceError`          | The server refused the push as a whole for one workspace, so nothing applied. `Code` is, for example, `workspace_access_revoked`, `workspace_no_access`, `mutation_failed` or `invalid_argument`. A custom mutator that returns a non-rejection error gives `mutation_failed`.                                                                                                                                                                                          |
| `ErrUnconfirmed` (wrapped) | The client could not learn whether the server applied the mutation: the context ended, every attempt failed, or the server refused a resend of a mutation that an earlier attempt may have applied. The error also wraps the last failure, which can be a `*WorkspaceError` or a `*connect.Error`, so test for `ErrUnconfirmed` first. The mutation stays queued and is sent again, under the same id, before the next write to the workspace, or at once by `Confirm`. |
| `ErrClientIDInUse`         | The server reports a new mutation as already applied: another running client uses the same id. The mutation was not applied.                                                                                                                                                                                                                                                                                                                                            |
| `ErrReset`                 | `Pull` or `Subscribe` cannot continue from the cursor. Bootstrap again.                                                                                                                                                                                                                                                                                                                                                                                                 |
| `*connect.Error`           | The server failed the request, which did not apply.                                                                                                                                                                                                                                                                                                                                                                                                                     |

`CodeOf(err) string` returns the code of a `*RejectedError` or `*WorkspaceError`, or the `syncplane-error-code` metadata of a `*connect.Error`, and `""` for none. The constants `CodePermissionDenied`, `CodeAccessDenied`, `CodeInvalidArgument`, `CodeConflict`, `CodeTextFullStateRequired`, `CodeTextStateStale`, `CodeWorkspaceAccessRevoked`, `CodeWorkspaceNoAccess`, `CodeMutationIDGap`, `CodeInternal` and `CodeMutationFailed` name the server's codes. Applications can define more. See [Sync status and failure codes](sync-status-and-codes.md).

## Idempotency

Every mutation carries the client's id and a mutation id from a counter the client keeps per workspace, in memory. A push that fails in transit is sent again under the same id, so the server applies it at most once. A `Client` with a random id starts a new counter; a `Client` with `WithClientID` continues after the server's watermark for that id.
