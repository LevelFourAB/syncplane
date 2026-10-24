# Syncing an external store

Use the `connect` package to synchronize an external store—such as an existing database or a directory of files—bidirectionally with the engine.

The external store remains the source of truth for its own tooling. The engine maintains a mirror and provides ordering, per-field conflict resolution, delta sync, and offline client support. External edits enter through `Mutate` on a `server.Trusted` context. Committed engine changes flow out through the change feed.

## Prerequisites

Before you start, ensure you have the following:

- An active engine server instance (`srv`).
- An external data store to synchronize.
- Generated Protobuf message types for your entities (for example, `&todov1.Todo{}`).

## Steps

1. Implement the store interfaces.

   Implement the `connect.Source` interface to scan data from your external store:

   ```go
   type Source interface {
       List(ctx, workspaceID, fn func(connect.Item) error) error   // scan everything
   }
   ```

   If your external store supports writes, implement the `connect.Writer` interface unless every entity is bound as `connect.IngressOnly`:

   ```go
   // Implement this too unless every entity is bound IngressOnly.
   type Writer interface {
       Write(ctx, workspaceID, msg) (fingerprint string, err error) // idempotent upsert
       Delete(ctx, workspaceID, prototype, key string) error        // missing = no-op
   }
   ```

2. Open the state database.

   Open a state database to store connector bookkeeping metadata:

   ```go
   state, _ := sqlitestate.Open("connector-state.db") // bookkeeping, reconstructible
   ```

3. Initialize the connector.

   Create a connector instance with `connect.New`:

   ```go
   conn, _ := connect.New(connect.Config{
       Server:   srv,
       Source:   mySource,
       State:    state,
       Entities: []proto.Message{&todov1.Todo{}},
   })
   ```

4. Start the sync loop.

   Start the connector to drain commits and sync on an interval. Trigger kicks on external events:

   ```go
   conn.Start(ctx)        // drains on commit, syncs on an interval
   conn.Kick(workspaceID) // wire fsnotify / DB triggers / webhooks here
   ```

## Confirming the sync

Verify that the connector is running and inspect the health of your workspaces:

```go
h, ok := conn.Health(workspaceID)
// h.LastSuccess, h.LastError, h.ConsecutiveFailures, h.NextAttempt
// h.Kind: FailureNone | FailureTransient | FailureUserAction

conn.HealthAll() // every workspace, ordered
```

Check that `h.LastSuccess` updates and `h.LastError` is clear.

Set `Config.OnHealth` to persist health states or trigger alerts. This callback fires after every pass, including manually driven passes.

## Advanced configuration

### Configuring incremental ingestion

By default, `List` enumerates the entire store on each interval (once per minute per workspace). To avoid full scans on stores with change cursors (such as Gmail `historyId`, IMAP `HIGHESTMODSEQ`, or CalDAV sync tokens), implement `connect.Incremental`:

```go
func (s *mySource) Checkpoint(ctx context.Context, ws string) (string, error) {
    // The source's position NOW. Taken before a full List, so a change racing
    // that list is redelivered rather than skipped.
    return s.api.CurrentHistoryID(ctx, ws)
}

func (s *mySource) Changes(ctx context.Context, ws, cursor string) (connect.Delta, error) {
    page, err := s.api.History(ctx, ws, cursor)
    if errors.Is(err, api.ErrHistoryExpired) {
        // Forces a fresh Checkpoint and a full List, inside the same pass.
        return connect.Delta{}, fmt.Errorf("history expired: %w", connect.ErrCursorInvalid)
    }
    if err != nil {
        return connect.Delta{}, err
    }
    return connect.Delta{
        Items:      page.Items,      // decoded as for List
        Deletions:  page.Deletions,  // applied after Items
        NextCursor: page.NextID,
        More:       page.HasMore,    // loop now; do not wait for the tick
    }, nil
}
```

`Connector.Ingress` selects the pass mode automatically:

- Runs full enumeration when no cursor exists, when the source rejects the current cursor, or when `FullReconcileInterval` (default 1 hour) is due.
- Runs `Changes` in all other cases.

Set `ReconcileInterval` to the desired polling frequency. Set `FullReconcileInterval` to the desired safety net interval, or set it to a negative value to run full passes only when the cursor breaks.

Operational behaviors:

- The cursor persists after its page applies. A crash causes that page to replay. Redelivery is a no-op because an unchanged fingerprint short-circuits and a deletion checks the row before writing a tombstone. `Write` must remain idempotent.
- The deletion sweep (detecting rows where a shadow exists but the source did not list the item) runs during the full pass. An incremental pass only processes explicit entries in `Deletions`. If a source deletes items without reporting them, leave the safety net interval enabled.

### Configuring per-workspace credentials

If your workspaces require separate credentials (such as per-user mailboxes), use `Config.OpenSource` instead of `Config.Source`:

```go
conn, _ := connect.New(connect.Config{
    Server: srv,
    OpenSource: func(ctx context.Context, ws string) (connect.Source, error) {
        tok, err := tokens.For(ctx, ws)
        if errors.Is(err, tokens.ErrRevoked) {
            return nil, fmt.Errorf("reconnect required: %w", connect.ErrNeedsUserAction)
        }
        if err != nil {
            return nil, err
        }
        return newMailSource(tok), nil
    },
    State:    state,
    Entities: []proto.Message{&mailv1.Message{}},
})
```

The connector builds the source on the workspace's first pass and reuses it for subsequent passes. Implement `io.Closer` on the source to release connections when done. The connector drops and rebuilds the source in three cases:

- Calling `Close`.
- Calling `Forget(workspaceID)`.
- Encountering a pass failure with an error that wraps `connect.ErrNeedsUserAction`.

Store the new token after re-authentication. The next pass uses the new token without restarting the process.

Use `Forget` when a user disconnects an account. `Forget` drops the workspace and releases the source, but preserves shadows and cursors so reconnection resumes without re-importing. To prevent a workspace from being rediscovered, exclude it from `Config.Workspaces`.

### Configuring read-only sources

If the application never writes back to the external source, declare entities with `Direction: connect.IngressOnly` and omit `Writer`:

```go
Bindings: []connect.Binding{
    {Prototype: &mailv1.Message{}, Direction: connect.IngressOnly},
},
```

Egress paths skip `IngressOnly` entities. Three-way merge leaves unchanged fields intact and applies changed source fields over the engine state. Collaborative text applies source changes as minimal diffs onto the live document (see [How connectors converge](../explanation/connectors.md) and [Collaborative text](../explanation/collaborative-text.md)).

### Handling errors and backoff

Connector loop failures are logged and do not terminate the process.

Classify errors in the source:

- Wrap `connect.ErrNeedsUserAction` when retries cannot resolve the issue (such as an expired token).
- Return standard errors for transient issues. The connector applies exponential backoff between `Config.RetryMin` and `Config.RetryMax`.
- When an API provides a retry delay, wrap the error with `connect.RetryAfter`:

```go
return connect.RetryAfter(fmt.Errorf("rate limited: %w", err), resetIn)
```

While a workspace is backing off, commit signals, kicks, and scheduled ticks collapse into a single deferred pass. Direct calls to `Ingress`, `Reconcile`, and `Drain` execute immediately.

### Pacing cold-start ingestion

Initial synchronization of a large store performs many consecutive writes into a single workspace, holding that workspace's write lock. Configure pacing options to yield periodically:

```go
IngestChunk: 200,                   // after every 200 engine writes…
IngestPause: 50 * time.Millisecond, // …yield for this long
```

Only actual writes count toward `IngestChunk`. Unchanged items do not trigger pauses.

## A worked example

`cmd/mdtodo` is a complete connector that synchronizes the todo app with a folder
of Markdown files, including `Note` rows as Markdown documents.

## Related

- [How connectors converge](../explanation/connectors.md)
- [Connectors reference](../reference/connectors.md) — every interface,
  `Config` field, sentinel and health value.
- [Follow committed changes](change-feed.md) — the egress half, on its own.
- [Collaborative text](../explanation/collaborative-text.md) — why writer
  identities may never be shared.
