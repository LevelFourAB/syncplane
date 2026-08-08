# Telemetry

The engine emits OpenTelemetry traces and metrics. This page lists every
instrument, its attributes, and the rules the engine follows when adding one.
For wiring an SDK and an exporter, see
[Observe the engine](../how-to/observe-the-engine.md).

**Instrument names are a compatibility surface.** Dashboards and alerts are
written against them, so they change like any other public API.

## Without an SDK installed

The engine depends only on the OpenTelemetry **API**, never the SDK. Until a
process installs an SDK, every instrument is the API's own no-op: an interface
call and nothing else. Nothing is exported, no exporter is linked, and an
adopter who wants no telemetry does not have to opt out of it.

Providers are configured per component and default to the global provider:

| Component | How to configure                                                |
| --------- | --------------------------------------------------------------- |
| `server`  | `server.WithTracerProvider(tp)`, `server.WithMeterProvider(mp)` |
| `effects` | `effects.Config.TracerProvider`, `.MeterProvider`               |
| `connect` | `connect.Config.TracerProvider`, `.MeterProvider`               |
| `storage` | `otelstore.Wrap(store, otelstore.WithMeterProvider(mp))`        |

Passing one explicitly rather than relying on the global is what lets a test
assert against its own reader, and what lets a host run several engines that
report separately.

## The cardinality rule

> Metric attributes must be bounded by the **schema**.
> Span attributes may be bounded by the **traffic**.

A metric stream exists per distinct attribute set, for as long as the process
lives, in the collector and in the time-series database behind it. Entity
names, rejection codes, reset reasons and consumer names come from finite sets
an app declares once, so they are safe to key a metric by.

`workspace_id` and `client_id` are not. `workspace_id` is the engine's hard
partition key and grows with the deployment; `client_id` is an unauthenticated
per-installation handle that grows without bound and is never reused. Both
appear on **spans only** — a span is sampled, has a bounded lifetime, and is
indexed rather than aggregated.

Two consequences worth knowing about:

- **Names from a request are resolved against the catalog first.** A push
  naming an entity that does not exist is rejected, but recording the name it
  asked for would let any client mint unbounded metric streams by pushing
  garbage. Unresolvable names are recorded as `_unregistered`.
- **Per-workspace health is not a metric.** `effects` and `connect` both track
  status per workspace, and both expose it through `OnHealth` / `HealthAll`
  instead. Effect lag is exported as a **maximum per consumer**, which answers
  "is anything falling behind" without one series per workspace; use
  `HealthAll` to find which workspace it is.

If you add an instrument, classify failures into a bounded enum
(`syncplane.outcome`) rather than deriving an attribute from an error.

## RPC layer

Every RPC is instrumented by
[`otelconnect`](https://github.com/connectrpc/otelconnect-go), which follows the
OpenTelemetry RPC semantic conventions: `rpc.server.duration`,
`rpc.server.request.size`, `rpc.server.response.size` and friends, keyed by
`rpc.method` and `rpc.grpc.status_code`. It also **extracts W3C trace context**,
so a traced client and this server's spans are one trace.

Peer attributes are disabled: on a public sync endpoint they would be one
attribute value per client.

The engine's own log lines carry `trace_id` and `span_id` whenever a trace is
recording, so a log line found by grep leads back to the trace it happened in.

## Metrics

### Push

| Instrument                      | Type      | Unit         | Attributes                                               |
| ------------------------------- | --------- | ------------ | -------------------------------------------------------- |
| `syncplane.push.mutations`      | counter   | `{mutation}` | `mutation_kind`, `entity` or `mutator`, `outcome`        |
| `syncplane.push.rejections`     | counter   | `{mutation}` | `mutation_kind`, `entity` or `mutator`, `rejection_code` |
| `syncplane.push.batch.duration` | histogram | `s`          | `outcome`                                                |

`push.rejections` is the signal the RPC layer structurally cannot report: **a
rejected mutation is still an ack**, so a push whose every mutation was thrown
away is a `200 OK`, and a client wedged in a rebase loop looks exactly like a
healthy one from outside. Watch the rejection rate, not the error rate.

On `push.mutations`, `outcome` is `ok` for applied and `skipped` for
`ALREADY_APPLIED` — an idempotent replay. A rising `skipped` share means
clients are resending: a queue that is not draining, or acks that are not
landing. A resend of a mutation that was _rejected_ counts on
`push.rejections` instead, each time the recorded verdict is replayed, so a
client resending a refused mutation inflates the rejection rate rather than the
skip rate.

`rejection_code` is bounded by the codes your app passes to `server.Reject`.
**Do not interpolate variable data into a rejection code** — a code carrying a
row key is a cardinality bomb.

The batch, not the RPC, is the timed unit: one workspace's batch is what holds
a write transaction, and so what contends.

### Pull and subscribe

| Instrument                    | Type            | Unit       | Attributes     |
| ----------------------------- | --------------- | ---------- | -------------- |
| `syncplane.pull.changes`      | histogram       | `{change}` | —              |
| `syncplane.pull.resets`       | counter         | `{reset}`  | `reset_reason` |
| `syncplane.subscribe.streams` | up-down counter | `{stream}` | —              |

`pull.resets` is the one to alert on. A reset means the client throws its store
away and re-downloads the workspace, and it is invisible at the RPC layer —
just another successful pull. `reset_reason` is one of:

| Reason                               | Means                                                     |
| ------------------------------------ | --------------------------------------------------------- |
| `cursor_below_horizon`               | The client fell behind GC. **This tunes `WithGC`.**       |
| `store_provenance_mismatch`          | The cursor came from a different store (restored backup). |
| `resync_invalidated_by_scope_change` | A scope change landed mid-resync. Routine.                |
| `old_visibility_unreconstructable`   | The old scope could not be rebuilt. Routine.              |

`subscribe.streams` is a level, not a rate: browsers cap concurrent connections
per host, so how many streams are held open is what says whether clients have
connection slots left.

### Notify fan-out

| Instrument                   | Type    | Unit             |
| ---------------------------- | ------- | ---------------- |
| `syncplane.notify.requested` | counter | `{notification}` |
| `syncplane.notify.published` | counter | `{notification}` |

Counted on either side of the coalescing wrapper, so the **gap between them is
exactly what `WithNotifyCoalescing` absorbed**. Near-zero collapse means the
window is only adding latency; near-total collapse means it is worth widening.

### Storage

| Instrument                      | Type      | Unit | Attributes           |
| ------------------------------- | --------- | ---- | -------------------- |
| `syncplane.storage.tx.duration` | histogram | `s`  | `tx_kind`, `outcome` |

Requires `otelstore.Wrap`. `tx_kind` is `read` or `write`. Timed from the
caller's point of view — retries of the write closure included — because that
is what the caller waited for. A write duration that climbs while push volume
does not is queueing, not work.

### Background maintenance

| Instrument                       | Type      | Unit          | Attributes                   |
| -------------------------------- | --------- | ------------- | ---------------------------- |
| `syncplane.gc.duration`          | histogram | `s`           | `outcome`                    |
| `syncplane.gc.workspaces`        | counter   | `{workspace}` | `outcome`                    |
| `syncplane.gc.collected`         | counter   | `{row}`       | `gc_kind`                    |
| `syncplane.index.build.duration` | histogram | `s`           | `entity`, `index`, `outcome` |
| `syncplane.index.build.rows`     | counter   | `{row}`       | `entity`, `index`            |

A GC pass collects per-workspace failures and carries on, so `gc.workspaces`
with `outcome=error` is where one persistently broken workspace shows up —
invisible in `gc.duration` alone.

`gc_kind` is `idle_clients`, `idempotency_keys`, `text_rows`, `history_records`
or `stranded_consumers`. **Alert on `stranded_consumers`**: an effect consumer
collected below the horizon stopped reacting to writes long enough to be given
up on, and what it missed cannot be recovered.

Until an index is built, reads that would use it scan instead — so
`index.build.*` is the explanation for a query latency regression no query-side
metric can give. `outcome=skipped` means the index was already ready.

### Effects

| Instrument                        | Type      | Unit         | Attributes            |
| --------------------------------- | --------- | ------------ | --------------------- |
| `syncplane.effects.passes`        | counter   | `{pass}`     | `consumer`, `outcome` |
| `syncplane.effects.pass.duration` | histogram | `s`          | `consumer`, `outcome` |
| `syncplane.effects.changes`       | counter   | `{change}`   | `consumer`            |
| `syncplane.effects.lag`           | gauge     | `{sequence}` | `consumer`            |
| `syncplane.effects.tracked`       | gauge     | `{pair}`     | —                     |

`effects.lag` is the largest `Head - Position` across a consumer's workspaces.
A consumer whose error rate is 100% and whose lag is climbing is **wedged**;
without these it looks exactly like one with nothing to do, because both
deliver nothing.

`effects.tracked` is the resident-set size: flat as the store grows, climbing
when work arrives faster than the workers drain it.

`effects.changes` counts delivery _attempts_ — delivery is at-least-once, so a
redelivered change is counted again. That is what the sink's own load is made
of.

### Connectors

| Instrument                        | Type      | Unit          | Attributes                |
| --------------------------------- | --------- | ------------- | ------------------------- |
| `syncplane.connect.passes`        | counter   | `{pass}`      | `outcome`, `failure_kind` |
| `syncplane.connect.pass.duration` | histogram | `s`           | `outcome`, `failure_kind` |
| `syncplane.connect.tracked`       | gauge     | `{workspace}` | —                         |

`failure_kind` is `none`, `transient` or `user_action`. A rising `user_action`
share is what a product surfaces as "reconnect your account" rather than
retrying silently forever.

`connect.tracked` is the number of workspaces the process is converging. Read
against `pass.duration`, it is what says whether `Config.Workers` is wide
enough: a fleet of that many workspaces takes `tracked / Workers` passes per
round trip to get through one sweep.

## Spans

| Span                              | Emitted by  | Notable attributes         |
| --------------------------------- | ----------- | -------------------------- |
| `syncplane.push.batch`            | `server`    | `workspace_id`, `outcome`  |
| `syncplane.storage.tx.read/write` | `otelstore` | `tx_kind`, `workspace_id`  |
| `syncplane.effects.pass`          | `effects`   | `consumer`, `workspace_id` |
| `syncplane.connect.pass`          | `connect`   | `workspace_id`             |

Effect and connector passes are **trace roots**: the write that triggered them
committed in another request, possibly on another node, so there is no parent
to attach to.

A cancelled context leaves a span `Unset` rather than `Error` — it is the
caller leaving, not a fault, and marking it otherwise makes every shutdown look
like an incident in a backend's error rate.

## Attribute reference

| Attribute                  | Bounded by                        | Metrics?       |
| -------------------------- | --------------------------------- | -------------- |
| `syncplane.entity`         | registered entities               | yes            |
| `syncplane.mutator`        | declared mutators                 | yes            |
| `syncplane.mutation_kind`  | `crud`/`custom`/`batch`           | yes            |
| `syncplane.outcome`        | `ok`/`error`/`canceled`/`skipped` | yes            |
| `syncplane.rejection_code` | codes your app rejects with       | yes            |
| `syncplane.reset_reason`   | fixed enum                        | yes            |
| `syncplane.tx_kind`        | `read`/`write`                    | yes            |
| `syncplane.gc_kind`        | fixed enum                        | yes            |
| `syncplane.failure_kind`   | fixed enum                        | yes            |
| `syncplane.consumer`       | registered consumers              | yes            |
| `syncplane.index`          | derived index names               | yes            |
| `syncplane.workspace_id`   | **the deployment**                | **spans only** |
| `syncplane.client_id`      | **installations, forever**        | **spans only** |

## The TypeScript client

The client emits **no OpenTelemetry** and takes no OpenTelemetry dependency. It
ships to browsers, where every dependency is bytes the app pays for on first
load, and the observability vendor is the app's choice rather than its sync
library's.

Instead it reports structured events through `onSyncEvent`:

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

| Event        | Carries                                                                                                     |
| ------------ | ----------------------------------------------------------------------------------------------------------- |
| `push`       | `workspaceIds`, `mutations`, `applied`, `rejected`, `durationMs`, `outcome`                                 |
| `pull`       | `workspaceId`, `changes`, `hasMore`, `reset`, `durationMs`, `outcome`                                       |
| `bootstrap`  | `workspaceId`, `rows`, `resumed`, `durationMs`, `streamMs`, `stageMs`, `swapMs`, `swapWrites`, `outcome`    |
| `rebase`     | `workspaceId`, `acked`, `replayed`, `durationMs`                                                            |
| `poke`       | `workspaceId`, `changes` (stream-delivered only)                                                            |
| `stream`     | `state`, `reason`                                                                                           |
| `leadership` | `leader`                                                                                                    |
| `resequence` | `workspaceId`, `firstId`, `mutations`                                                                       |
| `query`      | `origin`, `entity`, `workspaceId`, `path`, `index`, `declined`, `candidates`, `rows`, `total`, `durationMs`, `waitMs` |

Every event carries `at` (a `Date.now()` stamp). `outcome` separates `offline`
from `error`, so an error rate does not track connectivity.

Counting `poke` against `pull` is what separates changes that took the realtime
path from ones that waited for the poll floor.

A `resequence` is always a recovery and should be rare: a queued mutation was
discarded locally before the server ever saw it, and the ids above the hole it
left had to move down onto the server's watermark before the workspace could
push again. Until they did, that workspace's queue was frozen — a cycle
reporting `mutation_id_gap` on its status is the same condition seen from the
other side. Repeated ones point at whatever keeps discarding mutations, which is
the poison backstop (`poisonThreshold`) or a server whose watermarks moved
backwards.

A `bootstrap` reports its three phases as well as its total: `streamMs` waiting
on the server's snapshot, `stageMs` writing the chunks it delivered into the
store, `swapMs` in the transaction that installs them (mutex wait included). They
account for `durationMs` up to a few milliseconds of bookkeeping, and they are
what makes a slow bootstrap attributable — "the snapshot downloaded fast and the
store was slow" is a different problem from the reverse. `swapWrites` counts the
KV entries that transaction wrote; against `rows` it is the swap's write
amplification.

The split between the last two phases depends on what the workspace held. A
re-bootstrap stages its rows and swaps them in atomically, so its rows are
counted in `swapWrites`. A bootstrap of a workspace that holds no snapshot yet
writes rows straight into place as they arrive, so they land in `stageMs` and its
swap is a few entries — the same work, moved off the transaction that blocks
every other writer.

`resumed` says the snapshot was continued from where an interrupted attempt
stopped, so `rows` counts what that attempt downloaded rather than the whole
workspace. A run of resumed bootstraps says snapshots are not surviving the
network they are served over; the cost of each retry is bounded to what was still
missing, which is the point, but the retries themselves are worth watching.

### Local reads

Every event above belongs to the sync cycle. `query` belongs to the local store:
what one read cost, once per read — `origin: 'query'` for `query()`/`queryAll()`,
`origin: 'watch'` for a watcher's first read. A watcher maintains its result per
commit afterwards rather than re-reading, so one subscription reports one event
and no more.

`path` is the field to aggregate on. An index answers in time proportional to
the **result**, a scan in time proportional to the **entity**, and nothing about
the rows says which happened — both return the same answer. When `path` is
`scan`, `declined` says why:

| `declined`        | Meaning                                               |
| ----------------- | ----------------------------------------------------- |
| `cross-workspace` | `queryAll`/`watchQueryAll`; no index spans workspaces |
| `no-query`        | a `watch()` with no query, which is the whole entity  |
| `no-index`        | the entity declares none                              |
| `not-built`       | declared, still building for this workspace           |
| `no-plan`         | none matches this filter and ordering                 |
| `budget`          | the walk exceeded its candidate ceiling and fell back |

`not-built` after a bootstrap or a schema change is expected and ends on its own
once the background build finishes; see
[Making a query cheap](../how-to/query-data.md#making-a-query-cheap). `budget` is
the one to alert on: an index served the query until the workspace grew past the
walk's ceiling, and the same code is a full scan from then on.

On the index path, `candidates` counts the positions the walk visited. Against
`rows` it is how much of the walk the result did not use — a filter the range
could not express, or a repeated field putting one position per element on a
row — and a ratio drifting upward is a query on its way to `budget`.

`durationMs` runs from the call to the answer. `waitMs` is the part of it the
read spent waiting for a concurrent commit — a rebase, a bootstrap swap, an index
build — so a `waitMs` that moves under write load is contention rather than query
cost. Reference hydration (`include`) is in neither; the reference queries it
runs report their own events.

Reads never wait on each other, only on writes, but they do interleave on one
thread — so their durations overlap. Six watches mounting in the same tick each
span nearly the whole burst: 136 rows and 88ms of work between them reads as six
queries of ~88ms, and a threshold sized for one read's work fires on all six at
once. Durations summed across a burst therefore exceed the wall time it took, and
the largest may be reporting its neighbours' work. Aggregate `durationMs - waitMs`
rather than thresholding it; to attribute cost to a single query, measure it while
nothing else reads.

Only queries report. `get()` reads one key, `scan()` is the whole entity by
definition, and `queryServer()` is an RPC rather than a local read — none of the
three emits an event.

Leave `debugVerifyIndexReads` (and `SYNC_DEBUG_VERIFY_TOUCHES`) off while
measuring — they run the scan alongside the index path on every read, which is
in `durationMs`.

### Delivery

`onStatusChange` reports the current state for **rendering**; these report
occurrences for **aggregating**. The handler runs synchronously inside the sync
cycle or the read that fired it — a throw is reported and swallowed, but a slow
handler delays that work, so aggregate rather than await.
