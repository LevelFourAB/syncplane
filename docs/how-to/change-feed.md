# Following committed changes

Drain committed row changes in `(seq, entity, key)` order from a durable cursor
using the `Changes` API. Use this guide when you want to build and manage your
own consumer loop for search indexing, data replication, or custom connectors.

If you need managed positions, scheduling across workspaces, backoff, health
checks, and before-state transitions, use the
[effects runtime](react-to-changes.md) instead.

## Prerequisites

Before you follow changes, ensure that you have the following:

- A configured server instance (`srv`).
- A target workspace ID (`workspaceID`).
- A persistent storage mechanism to save and load cursor positions, such as
  `sqlitestate` or a custom `connect.State` implementation.

## Following changes with a cursor

Follow these steps to process committed changes in a workspace.

### 1. Load the stored cursor

Retrieve the last persisted cursor for the workspace from your state store:

```go
cur := loadCursor()                     // persisted by the consumer
```

If the consumer runs for the first time on an existing workspace, start with a
zero cursor.

### 2. Read changes from the workspace

Call `srv.Changes` with your context, the workspace ID, the current cursor, and
a batch page size:

```go
next, more, err := srv.Changes(ctx, workspaceID, cur, 256, func(ch server.Change) error {
    // ch.Entity, ch.Key, ch.Seq, ch.Deleted, ch.Message (nil when deleted),
    // ch.Actor (who wrote it)
    return index(ch)
})
```

The callback function receives each `server.Change` item in `(seq, entity, key)`
order.

### 3. Process the changes

Process each change inside the callback function. You can inspect row data and
actor metadata on `ch`:

- `ch.Entity`, `ch.Key`, and `ch.Seq`: identify the row and its sequence.
- `ch.Deleted`: indicates whether the row was deleted.
- `ch.Message`: contains the full row document, or `nil` if deleted.
- `ch.Actor`: contains the writer attribution.

If you want to filter out self-writes or suppress notifications, inspect
`ch.Actor`:

```go
// Don't react to your own writes.
if ch.Actor.System == "notifier" {
    return nil
}
// Notify everyone except whoever did it.
notifyAllExcept(ch.Actor.Subject, ch)
```

If your consumer writes back to the server in response to a change, use
[`MutateIdempotent`](server-side-writes.md#retrying-writes-idempotently) keyed on
the change.

### 4. Persist the updated cursor

Save the returned `next` cursor only after you successfully process the batch:

```go
saveCursor(next)                        // persist AFTER acting on the page
```

Always persist the cursor whole, including the `Epoch` field.

### 5. Handle expired cursors and resynchronize

If `srv.Changes` returns `ErrCursorTooOld`, the cursor cannot be resumed. When
this occurs:

1. Resynchronize your local state against the current workspace state.
2. Resume reading changes using the new cursor returned by `srv.Changes`.

## Verifying change processing

To confirm that your change consumer is processing updates:

1. Commit a write to the target workspace.
2. Run your change consumer loop.
3. Confirm that the change callback processes the new write and that `saveCursor`
   persists the updated sequence cursor.

## Related

- [Change feed API](../reference/server.md#change-feed)
- [Effects and the change feed](../explanation/effects-and-the-change-feed.md)
- [React to changes (server-side effects)](react-to-changes.md): Managed change
  loop with before-state and failure handling.
- [Sync an external store](sync-an-external-store.md): Change feed and ingress
  patterns.
- [Go server API](../reference/server.md): Complete server API reference.
