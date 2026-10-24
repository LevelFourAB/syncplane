# Reacting to changes

Use the `effects` runtime to run server-side reactions to committed writes, such as webhooks, notifications, search indexing, and background agents. The runtime tracks durable positions in the engine store, schedules passes when workspaces receive writes, retries failing handlers with backoff, and tracks previous field states for transition handlers.

This guide shows you how to configure the effects runtime, register handlers and raw sinks, handle transitions, deletions, and write-backs, test reactions, and shut down cleanly.

## Prerequisites

Before you begin, ensure that you have:

- A configured server instance (`srv`).
- Data models defined for your entities (for example, `*issuev1.Issue`).
- Familiarity with [server-side writes](server-side-writes.md) and the [change feed](change-feed.md).

## Running effects

Follow these steps to configure the runtime, register event handlers, and run reactions.

### 1. Configure and initialize the runtime

Initialize the runtime with `effects.New`, passing your server instance and optional tuning parameters:

```go
cfg := effects.Config{
    Server:    srv,
    Workers:   16,
    Interval:  15 * time.Second,
    BatchSize: 256,
    RetryMin:  5 * time.Second,
    RetryMax:  5 * time.Minute,
    OnHealth: func(h effects.Health) {
        if h.LastError != nil {
            slog.Warn("consumer wedged",
                "consumer", h.Consumer,
                "workspace", h.WorkspaceID,
                "failures", h.ConsecutiveFailures,
                "lag", h.Lag(),
                "err", h.LastError,
            )
        }
    },
}

rt, err := effects.New(cfg)
if err != nil {
    return err
}
```

Tune the following configuration fields for your deployment:

- `Workers` (default `16`): The maximum number of concurrent drain passes across all consumers and workspaces. Increase this value for slow, I/O-bound sinks such as external webhooks. Decrease this value to reduce database read load.
- `Interval` (default `15s`): How often the runtime sweeps for pending work not caught by notifier signals. `Start` always sweeps once. Set `Interval` to a negative duration to disable periodic sweeps and rely only on notifier activity and manual triggers.
- `BatchSize` (default `256`): The maximum number of rows read per change-feed page. This bounds how many changes one crash can redeliver.
- `RetryMin` (default `5s`) and `RetryMax` (default `5m`): The minimum and maximum exponential backoff delays for a failing pass. Set `RetryMin` to a negative duration to disable backoff.
- `OnHealth`: A callback invoked after every pass with the status of that `(consumer, workspace)` pair.

For design details on worker pools and scheduling, see [Effects and the change feed](../explanation/effects-and-the-change-feed.md). For full configuration fields, see the [Effects runtime reference](../reference/effects.md).

### 2. Register typed event handlers

Register your handlers using `effects.On` before you start the runtime. Provide a durable consumer name, the handler function, and optional field watchers:

```go
err = effects.On(rt, "issue-notifications", func(ctx context.Context, ev effects.Event[*issuev1.Issue]) error {
    // Handle row deletion
    if ev.Deleted {
        return unindexIssue(ctx, ev.Workspace, ev.Key)
    }

    // Handle first sightings
    if ev.FirstSeen {
        if ev.After.GetStatus() == issuev1.Status_DONE {
            return notifyClosed(ctx, ev.After)
        }
        return nil
    }

    // Handle observed field transitions
    if ev.DidChange("status") && ev.After.GetStatus() == issuev1.Status_DONE {
        return notifyClosed(ctx, ev.After)
    }

    return nil
}, effects.Watch("status"))
if err != nil {
    return err
}
```

When writing handlers, account for the following conditions:

- **Consumer names**: The name identifies the durable position stored in the database. Keep consumer names stable across restarts. Renaming a consumer resets its position.
- **Deletions (`ev.Deleted`)**: When a row is deleted, `ev.Deleted` is `true` and `ev.After` is zero. Always check `ev.Deleted` before dereferencing `ev.After`. If you configure `Watch`, `ev.Before` holds the watched fields as they stood when this consumer last acted on the row.
- **First sightings (`ev.FirstSeen`)**: `ev.FirstSeen` is `true` when the consumer holds no shadow state for the row. This occurs when a row is created, when a row existed before consumer registration, or after a tombstone horizon reset. `ev.Changed` is empty and `ev.DidChange` returns `false`. Skip `ev.FirstSeen` events to react only to observed transitions, or check `ev.After` to process existing rows.
- **Watched transitions**: `effects.Watch("field")` stores a shadow copy of only the declared fields. `ev.DidChange("field")` reports whether that field changed since the consumer last processed the row.
- **Errors and skipping**: Return `nil` to acknowledge and skip unneeded changes. Return an error to pause the consumer on that workspace and retry with backoff.

### 3. Write back idempotently from a handler

Handlers execute post-commit on a background goroutine outside the write transaction, so you can write back through the server API.

Because the runtime delivers changes with at-least-once semantics, use `server.MutateIdempotent` keyed on the delivery identity (`Workspace`, `Entity`, `Key`, and `Seq`) to prevent duplicate writes on redelivery:

```go
err = effects.On(rt, "issue-indexer", func(ctx context.Context, ev effects.Event[*issuev1.Issue]) error {
    if ev.Deleted {
        return nil
    }

    key := fmt.Sprintf("indexer:%s:%s:%s:%d", ev.Workspace, ev.Entity, ev.Key, ev.Seq)
    _, err := srv.MutateIdempotent(ctx, ev.Workspace, key,
        func(tx *server.EntityTx) ([]byte, error) {
            return nil, tx.Put(&issuev1.IssueSummary{
                Id:    ev.Key,
                Title: ev.After.GetTitle(),
            })
        })
    return err
})
```

The closure returns the bytes that a replay of the key answers with. Return
`nil` to record only that the write applied. The key is claimed even when the
closure writes nothing, and it expires after `WithIdempotencyRetention` (default 24
hours). Keep that retention period longer than your longest retry window.

For more details on server mutations, see [Writing from server-side code](server-side-writes.md).

### 4. Register a raw sink for external destinations

To stream changes across multiple entities or forward raw events to an external system, such as a message broker, register an `effects.Sink` with `rt.Register`:

```go
type brokerSink struct{}

func (b *brokerSink) Deliver(ctx context.Context, ch effects.Change) error {
    return publishToBroker(ctx, ch.Workspace, ch.Entity, ch.Key, ch.Seq, ch.Deleted)
}

// Subscribe to every entity in the workspace:
err = rt.Register("broker-export", &brokerSink{})

// Or limit delivery to specific entity types:
err = rt.Register("audit-export", &brokerSink{}, &issuev1.Issue{}, &issuev1.Comment{})
```

Register all sinks before you start the runtime. Registering a sink after calling `Start` returns an error.

### 5. Start the background runtime

Start the worker routines and background loops:

```go
if err := rt.Start(ctx); err != nil {
    return err
}
```

`Start` subscribes to the engine activity stream, performs an initial sweep across existing workspaces, and launches the worker pool.

### 6. Trigger passes by hand and test with synchronous drains

To trigger passes outside the automatic schedule, use one of the following methods:

- **Trigger an immediate background pass with `Kick`**:
  Call `rt.Kick(workspaceID)` when a workspace changed and you want the runtime to process it immediately rather than waiting for the next sweep:

  ```go
  rt.Kick(workspaceID)
  ```

- **Drain a consumer synchronously with `Drain`**:
  In integration tests or batch jobs, call `rt.Drain` to process pending changes synchronously on the calling goroutine:

  ```go
  err := rt.Drain(ctx, "issue-notifications", workspaceID)
  if err != nil {
      // Handle drain error or effects.ErrLeaseHeld.
  }
  ```

  `Drain` processes all pending changes for that consumer and workspace to exhaustion before returning. If another process holds the advisory lease, `Drain` returns `effects.ErrLeaseHeld`.

### 7. Shut down cleanly

Stop the runtime when your application terminates by calling `rt.Close()`:

```go
err := rt.Close()
if err != nil {
    return err
}
```

`Close` performs the following actions:

- Cancels the runtime context.
- Stops all active retry timers.
- Waits for all in-flight worker passes to complete.
- Unregisters telemetry metrics from the meter provider.

`Close` is idempotent and safe to call multiple times. `Close` does not close caller-provided sinks.

## Verifying consumer health

To verify that consumers are processing changes and to detect stuck handlers:

1. Inspect individual consumer health with `rt.Health`:

   ```go
   h, ok := rt.Health("issue-notifications", workspaceID)
   if ok {
       slog.Info("consumer status",
           "lag", h.Lag(),
           "last_attempt", h.LastAttempt,
           "last_success", h.LastSuccess,
           "consecutive_failures", h.ConsecutiveFailures,
       )
   }
   ```

2. Inspect all active and recently finished pairs with `rt.HealthAll`:

   ```go
   for _, h := range rt.HealthAll() {
       if h.LastError != nil {
           slog.Error("handler failing", "consumer", h.Consumer, "workspace", h.WorkspaceID, "err", h.LastError)
       }
   }
   ```

3. Check the resident set size with `rt.Tracked`:

   ```go
   activePairs := rt.Tracked()
   ```

   `Tracked` reports how many `(consumer, workspace)` pairs hold state in memory (queued, running, or backing off). This count returns to `0` as workspaces drain.

## Related

- [Effects API](../reference/effects.md)
- [Effects and the change feed](../explanation/effects-and-the-change-feed.md)
- [Follow committed changes (change feed)](change-feed.md) — Follow the change feed directly.
- [Write from server-side code](server-side-writes.md) — Use `Mutate` and `MutateIdempotent` in handlers.
- [Sync an external store (connectors)](sync-an-external-store.md) — Keep an external datastore synchronized with changes.
