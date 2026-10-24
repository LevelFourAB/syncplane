# Telemetry

The engine emits OpenTelemetry traces and metrics. This page lists every
instrument, its attributes, and the rules the engine follows when adding one.
For wiring an SDK and an exporter, see
[Observe the engine](../how-to/observe-the-engine.md).

**Instrument names are a compatibility surface.** Dashboards and alerts use
instrument names, which follow public API compatibility rules.

## Without an SDK installed

The engine depends only on the OpenTelemetry API, not the SDK. Until a process
installs an SDK, every instrument uses the API no-op implementation: an
interface call with no side effects. The engine exports nothing and links no
exporter.

You configure providers per component. Components default to the global provider:

| Component | Configuration                                                   |
| --------- | --------------------------------------------------------------- |
| `server`  | `server.WithTracerProvider(tp)`, `server.WithMeterProvider(mp)` |
| `effects` | `effects.Config.TracerProvider`, `effects.Config.MeterProvider` |
| `connect` | `connect.Config.TracerProvider`, `connect.Config.MeterProvider` |
| `storage` | `otelstore.Wrap(store, otelstore.WithMeterProvider(mp))`        |

Passing an explicit provider allows tests to assert against their own reader
and allows a host to run multiple independent engines.

## RPC layer

[`otelconnect`](https://github.com/connectrpc/otelconnect-go) instruments every
RPC. It follows OpenTelemetry RPC semantic conventions: `rpc.server.duration`,
`rpc.server.request.size`, `rpc.server.response.size`, and related metrics keyed
by `rpc.method` and `rpc.grpc.status_code`. It also extracts W3C trace context
from incoming requests. If a client sends a `traceparent` header, the server
creates spans as children of the client span.

The engine disables peer attributes to prevent unique attribute values per
client on public sync endpoints.

Engine log lines include `trace_id` and `span_id` when a trace records.

## Metrics

Four signals cover most issues that are invisible in standard RPC metrics:

- `syncplane.push.rejections` — A rejected mutation returns a `200 OK` status,
  so a client stuck in a rebase loop does not appear in RPC-level metrics.
- `syncplane.pull.resets` with `reset_reason=cursor_below_horizon` — Clients
  fall behind garbage collection (GC) and re-download entire workspaces.
  Increase `WithGC` retention.
- `syncplane.effects.lag` — A consumer stopped keeping up. Pair it with
  `syncplane.effects.passes{outcome=error}` to distinguish between a stuck
  consumer and a busy consumer.
- `syncplane.gc.collected{gc_kind=stranded_consumers}` — An effect consumer was
  collected below the horizon. The missed data cannot be recovered.

If you configure a read replica with `pgstore.WithReadReplica`, monitor two
additional signals:

- `syncplane.replica.reads{read_route=fallback_error}` — The replica is
  unreachable or broken, and the primary server serves every read.
- Sustained `syncplane.replica.reads{read_route=fallback_stale}` — Replication
  lag exceeds the window between a client push acknowledgment and its next pull,
  concentrating reads on the primary server. Brief bursts after a write spike are
  normal.

### Push

| Instrument                      | Type      | Unit         | Attributes                                               |
| ------------------------------- | --------- | ------------ | -------------------------------------------------------- |
| `syncplane.push.mutations`      | counter   | `{mutation}` | `mutation_kind`, `entity` or `mutator`, `outcome`        |
| `syncplane.push.rejections`     | counter   | `{mutation}` | `mutation_kind`, `entity` or `mutator`, `rejection_code` |
| `syncplane.push.batch.duration` | histogram | `s`          | `outcome`                                                |

`syncplane.push.rejections` reports rejected mutations. Because the server still
acknowledges a rejected mutation, a push where every mutation is rejected
returns HTTP `200 OK`.

On `syncplane.push.mutations`, `outcome` is `ok` for applied mutations and
`skipped` for `ALREADY_APPLIED` (an idempotent replay). A rising `skipped` share
indicates that clients resend mutations because a queue does not drain or
acknowledgments do not arrive. A resend of a rejected mutation increments
`syncplane.push.rejections` each time the server replays the recorded verdict.

`rejection_code` is bounded by the codes passed to `server.Reject`. Do not
interpolate variable data into a rejection code.

`syncplane.push.batch.duration` measures the batch duration rather than the RPC
duration. One workspace batch holds a write transaction and represents the unit
of contention.

### Pull and subscribe

| Instrument                    | Type            | Unit       | Attributes     |
| ----------------------------- | --------------- | ---------- | -------------- |
| `syncplane.pull.changes`      | histogram       | `{change}` | —              |
| `syncplane.pull.resets`       | counter         | `{reset}`  | `reset_reason` |
| `syncplane.subscribe.streams` | up-down counter | `{stream}` | —              |

`syncplane.pull.resets` indicates that a client discarded its local store and
re-downloaded the workspace. A reset appears as a successful pull at the RPC
layer. `reset_reason` has one of the following values:

| Reason                               | Description                                                                                  |
| ------------------------------------ | -------------------------------------------------------------------------------------------- |
| `cursor_below_horizon`               | The client fell behind garbage collection. This value indicates `WithGC` tuning is required. |
| `store_provenance_mismatch`          | The cursor originated from a different store, such as a restored backup.                     |
| `resync_invalidated_by_scope_change` | A scope change occurred during resynchronization.                                            |
| `old_visibility_unreconstructable`   | The previous scope could not be rebuilt.                                                     |
| `cursor_state_evicted`               | The cursor-state store no longer holds the cursor sets.                                      |

`syncplane.subscribe.streams` measures the current number of open subscription
streams.

### Rooms

See [Rooms](../explanation/rooms.md).

| Instrument                       | Type            | Unit            | Attributes        |
| -------------------------------- | --------------- | --------------- | ----------------- |
| `syncplane.rooms.streams`        | up-down counter | `{stream}`      | —                 |
| `syncplane.rooms.joined`         | up-down counter | `{room}`        | —                 |
| `syncplane.rooms.participants`   | histogram       | `{participant}` | —                 |
| `syncplane.rooms.publishes`      | counter         | `{publish}`     | `room_event_kind` |
| `syncplane.rooms.events.dropped` | counter         | `{event}`       | `drop_reason`     |

`syncplane.rooms.streams` tracks concurrent room connections. An application
using rooms holds two long-lived connections per account.

The ratio of `syncplane.rooms.joined` to `syncplane.rooms.streams` represents the
cost of room re-gating. Every live room re-evaluates its join gate on the stream
heartbeat. An anchored room reads its row, memoized across sharing streams.

`syncplane.rooms.events.dropped` tracks lost events under the at-most-once
delivery contract. Presence recovers within one announce interval. Broadcasts do
not recover.

### Cursor state

| Instrument                            | Type    | Unit        | Attributes |
| ------------------------------------- | ------- | ----------- | ---------- |
| `syncplane.cursor_state.misses`       | counter | `{miss}`    | —          |
| `syncplane.cursor_state.put_failures` | counter | `{failure}` | —          |

A miss indicates that the store evicted an entry that a client still references.
This eviction causes a client reset, recorded under `syncplane.pull.resets` as
`cursor_state_evicted`. This occurs when the store capacity is too small, the
retention period is shorter than client absence intervals, or an in-process
store restarted.

A put failure causes the affected cursor to inline its sets. Clients continue
operating, but payloads include the state data that the store normally omits.

### Notify fan-out

| Instrument                   | Type    | Unit             | Attributes |
| ---------------------------- | ------- | ---------------- | ---------- |
| `syncplane.notify.requested` | counter | `{notification}` | —          |
| `syncplane.notify.published` | counter | `{notification}` | —          |

These instruments measure notifications before and after the coalescing
wrapper. The difference between `syncplane.notify.requested` and
`syncplane.notify.published` represents the volume that `WithNotifyCoalescing`
collapses.

### Storage

| Instrument                      | Type      | Unit     | Attributes           |
| ------------------------------- | --------- | -------- | -------------------- |
| `syncplane.storage.tx.duration` | histogram | `s`      | `tx_kind`, `outcome` |
| `syncplane.replica.reads`       | counter   | `{read}` | `read_route`         |

`syncplane.storage.tx.duration` requires `otelstore.Wrap`. The `tx_kind`
attribute is `read` or `write`. The histogram measures duration from the
perspective of the caller, including retries of the write closure.

`syncplane.replica.reads` counts reads routed through the storage adapter read
replica capability (`pgstore.WithReadReplica`). When you configure no replica,
the engine records nothing. The `read_route` attribute has one of the following
values:

- `replica`: Served from the replica.
- `fallback_stale`: The replica fell behind the freshness floor; the read was
  served from the primary. Sustained values indicate that replication lag exceeds
  the window between a client push acknowledgment and its next pull,
  concentrating reads on the primary server. Brief bursts after a write spike are
  normal.
- `fallback_error`: The replica is unreachable, broken, or failed before the
  read started; the primary server served the read.

### Background maintenance

| Instrument                          | Type      | Unit          | Attributes                   |
| ----------------------------------- | --------- | ------------- | ---------------------------- |
| `syncplane.gc.duration`             | histogram | `s`           | `outcome`                    |
| `syncplane.gc.workspaces`           | counter   | `{workspace}` | `outcome`                    |
| `syncplane.gc.collected`            | counter   | `{row}`       | `gc_kind`                    |
| `syncplane.index.build.duration`    | histogram | `s`           | `entity`, `index`, `outcome` |
| `syncplane.index.build.rows`        | counter   | `{row}`       | `entity`, `index`            |
| `syncplane.text.compact.duration`   | histogram | `s`           | `outcome`                    |
| `syncplane.text.compact.workspaces` | counter   | `{workspace}` | `outcome`                    |
| `syncplane.text.compact.rows`       | counter   | `{row}`       | —                            |

A garbage collection (GC) pass records per-workspace failures and continues.
`syncplane.gc.workspaces` with `outcome=error` identifies persistent
single-workspace failures.

`syncplane.gc.workspaces` counts workspaces visited during the pass that
contain collectable data. `outcome=skipped` indicates that another node held the
workspace lease.

`gc_kind` is one of `client_verdicts`, `idle_clients`, `idempotency_keys`,
`history_records`, or `stranded_consumers`. `idle_clients` appears only with
`WithClientRetention`. The value `stranded_consumers` indicates that an effect
consumer collected below the horizon stopped processing writes and cannot
recover missed data.

`syncplane.index.build.duration` and `syncplane.index.build.rows` track
background index builds. Reads scan tables directly until the index build
completes. An `outcome=skipped` value indicates the index was already built.

The engine emits `syncplane.text.compact.*` metrics during collaborative text
compaction when you enable `WithTextCompaction`.
`syncplane.text.compact.workspaces` counts visited workspaces whose delta
horizon moved since the previous pass. `outcome=skipped` indicates that another
node held the lease. Each compacted row generates a new sequence number that
clients re-fetch once.

### Effects

| Instrument                        | Type      | Unit         | Attributes            |
| --------------------------------- | --------- | ------------ | --------------------- |
| `syncplane.effects.passes`        | counter   | `{pass}`     | `consumer`, `outcome` |
| `syncplane.effects.pass.duration` | histogram | `s`          | `consumer`, `outcome` |
| `syncplane.effects.changes`       | counter   | `{change}`   | `consumer`            |
| `syncplane.effects.lag`           | gauge     | `{sequence}` | `consumer`            |
| `syncplane.effects.tracked`       | gauge     | `{pair}`     | —                     |

`syncplane.effects.lag` measures the maximum difference between `Head` and
`Position` across all workspaces for a consumer. A non-zero value indicates that
a consumer stopped keeping up. Pair `syncplane.effects.lag` with
`syncplane.effects.passes{outcome=error}` to distinguish a stuck consumer from a
busy consumer.

`syncplane.effects.tracked` measures the resident set size of tracked
workspace-consumer pairs.

`syncplane.effects.changes` counts delivery attempts. Because delivery is
at-least-once, redelivered changes increment the counter again.

### Connectors

| Instrument                        | Type      | Unit          | Attributes                |
| --------------------------------- | --------- | ------------- | ------------------------- |
| `syncplane.connect.passes`        | counter   | `{pass}`      | `outcome`, `failure_kind` |
| `syncplane.connect.pass.duration` | histogram | `s`           | `outcome`, `failure_kind` |
| `syncplane.connect.tracked`       | gauge     | `{workspace}` | —                         |

`failure_kind` is `none`, `transient`, or `user_action`.

`syncplane.connect.tracked` measures the number of workspaces that the engine
currently converges.

## Spans

| Span                              | Emitted by  | Notable attributes         |
| --------------------------------- | ----------- | -------------------------- |
| `syncplane.push.batch`            | `server`    | `workspace_id`, `outcome`  |
| `syncplane.storage.tx.read/write` | `otelstore` | `tx_kind`, `workspace_id`  |
| `syncplane.effects.pass`          | `effects`   | `consumer`, `workspace_id` |
| `syncplane.connect.pass`          | `connect`   | `workspace_id`             |

Effect and connector passes serve as trace roots because the initiating write
committed in an earlier request.

A canceled context sets the span status to `Unset` rather than `Error`.

## Attribute reference

| Attribute                   | Bounded by                                    | Metrics    |
| --------------------------- | --------------------------------------------- | ---------- |
| `syncplane.entity`          | Registered entities                           | Yes        |
| `syncplane.mutator`         | Declared mutators                             | Yes        |
| `syncplane.mutation_kind`   | `crud`, `custom`, `batch`                     | Yes        |
| `syncplane.outcome`         | `ok`, `error`, `canceled`, `skipped`          | Yes        |
| `syncplane.rejection_code`  | Application rejection codes                   | Yes        |
| `syncplane.reset_reason`    | Fixed enum                                    | Yes        |
| `syncplane.tx_kind`         | `read`, `write`                               | Yes        |
| `syncplane.read_route`      | `replica`, `fallback_stale`, `fallback_error` | Yes        |
| `syncplane.gc_kind`         | Fixed enum                                    | Yes        |
| `syncplane.failure_kind`    | Fixed enum                                    | Yes        |
| `syncplane.consumer`        | Registered consumers                          | Yes        |
| `syncplane.index`           | Derived index names                           | Yes        |
| `syncplane.room_event_kind` | `state`, `leave`, `message`                   | Yes        |
| `syncplane.drop_reason`     | Fixed enum                                    | Yes        |
| `syncplane.workspace_id`    | Deployment workspaces                         | Spans only |
| `syncplane.client_id`       | Installations                                 | Spans only |

## The TypeScript client

The client does not emit OpenTelemetry data and has no OpenTelemetry
dependency. It does not generate trace context. To produce end-to-end traces,
attach the `traceparent` header in a custom Connect interceptor. Instead, it
reports structured events through `onSyncEvent`:

```ts
const client = new SyncClient({
	url,
	entities,
	onSyncEvent: (e) => {
		if (e.type === 'push' && e.rejected > 0) {
			metrics.increment('sync.rejected', e.rejected);
		}
	},
});
```

| Event        | Fields                                                                                                                |
| ------------ | --------------------------------------------------------------------------------------------------------------------- |
| `push`       | `workspaceIds`, `mutations`, `applied`, `rejected`, `durationMs`, `outcome`                                           |
| `pull`       | `workspaceId`, `changes`, `hasMore`, `reset`, `durationMs`, `requestMs`, `applyMs`, `outcome`                         |
| `bootstrap`  | `workspaceId`, `rows`, `resumed`, `durationMs`, `streamMs`, `stageMs`, `swapMs`, `swapWrites`, `outcome`              |
| `rebase`     | `workspaceId`, `acked`, `replayed`, `durationMs`                                                                      |
| `poke`       | `workspaceId`, `changes` (stream-delivered only)                                                                      |
| `stream`     | `state`, `reason`                                                                                                     |
| `leadership` | `leader`                                                                                                              |
| `resequence` | `workspaceId`, `firstId`, `mutations`                                                                                 |
| `indexing`   | `workspaceId`, `rows`, `durationMs`                                                                                   |
| `query`      | `origin`, `entity`, `workspaceId`, `path`, `index`, `declined`, `candidates`, `rows`, `total`, `durationMs`, `waitMs` |

Every event includes an `at` property containing a timestamp from `Date.now()`.
The `outcome` field distinguishes `offline` from `error`.

The `onSyncEvent` constructor option sets a single handler. Call
`client.onSyncEvent(cb)` to attach or detach additional listeners at runtime.
For interactive debugging, see `attachDevtools(client)` in the
[TypeScript client reference](typescript-client.md#devtools).

The `poke` event reports changes received through the real-time stream. The
`pull` event reports changes fetched during poll cycles.

The `resequence` event indicates recovery after the client discards a queued
mutation locally before transmission to the server. Subsequent mutation IDs
adjust to the server watermark.

The `bootstrap` event reports the breakdown of bootstrap timing:

- `streamMs`: Time spent waiting for the server snapshot.
- `stageMs`: Time spent writing snapshot chunks into the local store.
- `swapMs`: Time spent in the transaction that installs staged chunks, including
  mutex wait time.
- `swapWrites`: Number of key-value entries written during the swap transaction.

For a re-bootstrap, the client stages and swaps rows atomically (`swapWrites`
equals the row count). For an initial bootstrap into an empty workspace, the
client writes rows directly during staging (`stageMs`), resulting in minimal
`swapWrites`.

The `resumed` field indicates whether the snapshot continued from an earlier
interrupted download.

### Local reads

The `query` event reports execution metrics for local store reads:

- `origin: 'query'`: Emitted by `query()` and `queryAll()`.
- `origin: 'watch'`: Emitted by the initial read of a watcher subscription.
  Subsequent updates maintain results incrementally and do not emit events.

The `path` field indicates the execution strategy: `index` or `scan`. When
`path` is `scan`, `declined` contains the reason:

| Value       | Meaning                                                                                                                                                                                        |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `no-query`  | A `watch()` call executed without a query filter (reads entire entity).                                                                                                                        |
| `no-index`  | The entity declares no index matching the query.                                                                                                                                               |
| `not-built` | An index is declared but still building for this workspace. A cross-workspace read reports this while any covered workspace's index is still building.                                         |
| `no-plan`   | No declared index matches the query filter and ordering.                                                                                                                                       |
| `budget`    | Index traversal exceeded the row ceiling and fell back to a table scan. A faceted query reports it when the index range it would walk covers too large a share of the entity to beat the scan. |

A cross-workspace read reports `path: 'index'`. The `index` field contains the
index names joined with `+` (for example, `index1+index2`). The engine sets the
`workspaceId` field only when a read targets a single workspace. The
`candidates` field reports the sum of index positions visited across all
covered workspaces. While any covered workspace's index is still building, the
read falls back to a scan with `declined: 'not-built'`.

The `indexing` event fires once per workspace when all declared indexes finish
building, reporting `rows` indexed and `durationMs`. A first (cold) bootstrap
writes its rows already indexed, so its event reports `rows: 0`. A re-bootstrap
of a workspace that already held a snapshot reports the whole snapshot. For query
optimization guidance, see
[Making a query cheap](proto-options.md#secondary-indexes).

On the index path, `candidates` measures visited index positions. `durationMs`
measures elapsed time from invocation to completion. `waitMs` measures time
spent waiting for concurrent write transactions, such as a rebase, bootstrap
swap, or index build. Reference hydration (`include`) queries emit separate
events.

The `get()`, `scan()`, and `queryServer()` methods do not emit `query` events.

### Delivery

Event handlers registered with `onSyncEvent` execute synchronously within the
sync cycle or read operation. The client catches and swallows exceptions thrown
in handlers, but long-running handlers delay sync and query execution.

## Related

- [The cardinality rule](../explanation/limits-and-trade-offs.md#the-cardinality-rule)
