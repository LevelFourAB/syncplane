# Change feed and effects runtime

This document explains the design, guarantees, and trade-offs of the change feed and the effects runtime. It covers why the feed delivers state rather than events, how failures and leases behave across instances, and how memory and retention scale with workload.

For practical guides and API details, see:

- [Reacting to changes](../how-to/react-to-changes.md) for registering consumers and handling effects.
- [Consuming the change feed](../how-to/change-feed.md) for driving the change feed directly.
- [Effects runtime reference](../reference/effects.md) for configuration fields and types.

## State reporting versus event logging

The change feed reports the current state of changed rows so you can rebuild search indexes, caches, or mirrors. It is not an append-only event log.

The feed does not function as an event history for two reasons:

- **Coalescing**: Multiple writes to the same row between drains collapse into a single change that contains only the final state. Intermediate states are not observable.
- **The tombstone horizon**: Garbage collection (GC) collects changes older than the tombstone horizon. When GC deletes tombstones, a consumer cannot reconstruct missed changes.

Consumers must converge on current state instead of discrete transitions. If an application requires guaranteed delivery of every event without omission, persist each event as an explicit row through `Mutate` and attach a handler to that row.

### Transitions and shadow state

Because the change feed delivers state, the runtime does not store full transition histories by default. When a consumer declares watched fields with `Watch`, the runtime stores a shadow copy containing only those specific fields as they stood when that consumer last processed the row.

When changes arrive, the runtime provides:

- `ev.After`: The complete current row state, or zero when `ev.Deleted` is true.
- `ev.Before`: The watched fields only, reflecting the consumer's last processed state.
- `ev.Changed` and `ev.DidChange`: Indicators of which watched fields changed between the two states.
- `ev.FirstSeen`: True when the consumer had no shadow state for the row.

`FirstSeen` occurs when a row is newly created, when a row existed before consumer registration, or after a horizon reset drops shadows. Because shadows track consumer memory rather than database history, these three situations appear identical. `DidChange` returns `false` for all `FirstSeen` events.

Consumers that declare no watched fields store no shadows and use no extra storage. Consumers that declare watched fields store one shadow row for every processed row.

## Tombstone horizons and falling behind

Workspaces clean up deleted records past a configurable tombstone horizon. The tombstone horizon defines the maximum allowable downtime for a consumer.

When a consumer falls behind past the horizon or presents an invalid cursor, `Changes` returns `ErrCursorTooOld` with a cursor at the current sequence. Three conditions trigger this error:

- The consumer was offline longer than the GC retention period, placing its cursor before the tombstone horizon.
- The store was recreated empty or restored below the consumer position, placing the cursor above the current sequence.
- The database was swapped or restored from a separately created store, placing the cursor in a different store lineage.

Persisting cursors with their `Epoch` field allows store lineage checks. A value of `0` in `Epoch` represents an unknown lineage and bypasses the check. Consumer positions in the effects runtime reside inside the workspace store, so they transfer automatically during a swap or restore.

A consumer that falls behind past the horizon logs a warning, drops its shadow rows, and resumes at the current sequence number. Unread intermediate changes are permanently lost.

## Guarantees provided to handlers

The effects runtime provides three guarantees for handlers:

- **Post-commit execution**: Handlers run on background goroutines outside the write transaction. A handler can issue write-backs using `Mutate`.
- **At-least-once delivery**: If a crash occurs between handler execution and position commit, the change is redelivered on restart. Handlers must be idempotent. Handlers writing back should use `MutateIdempotent` keyed on `(workspace, entity, key, seq)`.
- **Per-workspace ordering**: Handlers process sequentially per workspace in change feed order. Different workspaces and different consumers run independently.

## Failure isolation and wedging

When a handler returns an error, the runtime wedges that specific consumer on that specific workspace:

1. The consumer position does not advance.
2. The runtime retries the failing change with backoff using `RetryMin` and `RetryMax`.
3. All subsequent changes for that consumer on that workspace wait.

Wedging prevents silent data loss. Skipping an unprocessable change leaves no audit trail and no recovery path. Because each consumer maintains an independent position, a failure in one consumer does not delay or block other consumers.

You can detect wedged consumers using `Health` and `HealthAll`. `Health.Lag()` reports the distance in sequence numbers between the consumer position and the workspace head. In a wedged consumer, lag remains constant or increases as new writes arrive.

The runtime tracks active pairs and retains the most recently finished pairs up to `Config.HealthRetention` (default `1024`). Failing pairs remain in health reporting for the duration of the failure, whereas idle, completed pairs are eventually removed.

If you require bounded retries, dead-letter queues, or fan-out patterns, direct changes into an external broker through a sink instead of embedding retry limits in the handler.

## How passes run

A fixed worker pool (`Config.Workers`, default `16`) executes passes across workspaces and consumers. Work enters the queue through three mechanisms:

- **Workspace writes**: The notifier reports write activity across workspaces and schedules passes immediately without polling.
- **Periodic sweeps**: `Start` runs an initial sweep, and `Config.Interval` (default `15s`) triggers regular sweeps to query for workspaces where a consumer lags behind. Sweeps recover lost notifier signals, catch up after process restarts, and register consumers against existing history.
- **Manual triggers**: `Runtime.Kick(workspaceID)` requests an immediate pass when an external process updates a workspace.

Notifier delivery is not guaranteed. Setting a negative `Config.Interval` disables sweeps and relies entirely on notifiers. The default notifier is process-local and only detects writes from the current node. Multi-node deployments require a shared notifier configured with `WithNotifier`.

Passes for a single consumer on a specific workspace never execute concurrently, regardless of worker count.

## Memory scaling

Memory usage in the runtime depends on active work rather than overall store size:

- A `(consumer, workspace)` pair consumes memory only while processing an active pass, waiting in a queue, or backing off during a retry.
- When all pending changes for a workspace finish, the runtime evicts the pair from memory.
- Consumer positions persist in the workspace store, allowing state to reload on the next write or sweep.

`Runtime.Tracked()` returns the resident set size. This value remains stable as the total number of workspaces grows and expands only when consumers lag behind active writes. Sweeps scale with store size because they scan for lagging workspaces once per interval per consumer. Sweep execution time does not increase reaction latency for notifier-driven writes.

## Multi-instance execution and advisory leases

When multiple runtime instances share a store, they distribute workspaces using advisory leases from the server locker (`server.Leases`).

- With `pgstore`, instances coordinate automatically through the shared database locker.
- Adapters without locker support fall back to process-local leases and do not coordinate across separate processes.

Advisory leases guarantee at-least-once processing across instances, not strict single execution. During rolling updates or network interruptions, a lease connection can drop, allowing a second instance to acquire the lease while the first finishes processing. Handlers must remain idempotent to handle this overlap.

Calling `Runtime.Close` waits for in-flight passes to complete before releasing resources, which minimizes lease overlap windows. When an instance cannot acquire a lease, `Drain` returns `effects.ErrLeaseHeld`, leaving the consumer position unchanged for subsequent sweeps or other instances to process.
