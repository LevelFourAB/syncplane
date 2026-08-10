# React to changes (server-side effects)

The `effects` runtime runs server-side reactions to committed writes — webhooks,
notifications, search indexing, agents. It sits on the
[change feed](change-feed.md) and adds the bookkeeping that is easy to get
subtly wrong: a durable position in the engine's own store, scheduling that
wakes a workspace when it is written, backoff and health for a handler that
keeps failing, and before-state for handlers that react to a transition.

```go
rt, err := effects.New(effects.Config{Server: srv})
if err != nil { ... }

err = effects.On(rt, "slack-status",
    func(ctx context.Context, ev effects.Event[*issuev1.Issue]) error {
        if ev.Deleted || ev.FirstSeen || !ev.DidChange("status") {
            return nil
        }
        if ev.After.GetStatus() != issuev1.Status_DONE {
            return nil
        }
        return postToSlack(ctx, ev.After)
    },
    effects.Watch("status"),
)

if err := rt.Start(ctx); err != nil { ... }
defer rt.Close()
```

Register every consumer before `Start`; `Close` stops the workers.

## What a handler is promised

- **Post-commit.** Handlers run on a background goroutine, never inside the
  write transaction they react to. A handler is free to write back through
  [`Mutate`](server-side-writes.md) — it is not running inside anything.
- **At-least-once.** A crash between a handler acting and its position
  committing redelivers the change. Handlers must be idempotent; one that writes
  back should use
  [`MutateIdempotent`](server-side-writes.md#writes-that-may-be-retried) keyed on
  `(workspace, entity, key, seq)`, which identifies a delivery exactly.
- **Ordered per workspace**, matching the feed. One consumer's handler runs
  serially for a workspace; different workspaces and different consumers proceed
  independently.

## Reacting to a transition

The feed reports state, not transitions (see
[the change feed](change-feed.md#semantics)), so "when status _becomes_ Done" is
not answerable from a change alone. `Watch` declares the fields a handler needs
before-state for, and the runtime keeps a shadow of exactly those fields:

| Field                         | Meaning                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| `ev.After`                    | The row's new state, always complete. Zero when `ev.Deleted`.                        |
| `ev.Before`                   | The **watched fields only**, as they stood when this consumer last acted on the row. |
| `ev.Changed` / `ev.DidChange` | Which watched fields differ between the two.                                         |
| `ev.FirstSeen`                | This consumer held no shadow, so nothing was known to compare against.               |

`FirstSeen` is the case worth deciding about explicitly. It covers a row created
just now, a row that existed before the consumer was registered, and the first
sighting after a horizon reset — the shadow is the consumer's memory, not the
row's history, so the three are indistinguishable. Skip them to react only to
observed transitions, or treat them as changes to also react to what is already
there. `DidChange` is false for all of them.

Declaring nothing keeps no shadows and costs nothing, which is why after-state is
the default. Each watched consumer stores one row per row it has seen.

## When a handler fails

A handler returning an error **wedges its consumer on that workspace**: the
position does not advance, the change is retried with backoff
(`RetryMin`/`RetryMax`), and everything behind it waits. Returning `nil` is how a
handler declines a change it does not care about.

This is deliberate. The alternative — skipping the change — loses it with no
record and no repair path, because there is no reconcile pass that can un-send an
email. The wedge is bounded to one consumer: each has its own position, so a bad
destination stops only itself.

Nothing else will tell you it happened, so watch `Health`:

```go
cfg.OnHealth = func(h effects.Health) {
    if h.LastError != nil {
        log.Warn("effect stuck", "consumer", h.Consumer, "workspace", h.WorkspaceID,
            "failures", h.ConsecutiveFailures, "lag", h.Lag(), "err", h.LastError)
    }
}
```

`Health.Lag()` is how many sequences behind the workspace head the consumer is.
It stays put while a consumer is wedged and grows as writes pile up behind it. A
handler failing forever and a handler with nothing to do both look like silence
from the outside; the lag is what tells them apart.

`Health` and `HealthAll` report what the runtime is currently tracking, plus the
most recently finished pairs (`Config.HealthRetention`, default 1024). A failing
pair is always among them, for as long as it keeps failing. A pair that drained
and went quiet eventually is not — so read `Health` to find what is wedged or
behind, not to enumerate what is healthy.

If you want bounded retry, dead-lettering, replay or fan-out to many
destinations, do not build it into a handler. Put a broker behind a `Sink` (see
below) and let it own that policy.

## The sink seam

`On` registers an in-process handler. Underneath, a consumer is a durable name, a
set of entities and a `Sink`:

```go
type Sink interface {
    Deliver(ctx context.Context, ch effects.Change) error
}
```

`Runtime.Register` wires a sink directly, for consumers that are not one typed
handler — publishing every change to a broker, for instance, where retry
schedules and dead-lettering become the broker's job:

```go
rt.Register("publish", mySink)                       // every entity
rt.Register("publish-issues", mySink, &issuev1.Issue{})  // only these
```

## When a pass runs

Work reaches a consumer three ways, all feeding one queue that a fixed pool of
workers drains:

- **The workspace is written.** The notifier reports activity for every
  workspace, including ones nothing is subscribed to, so a write schedules a
  pass immediately — no discovery step, and no wait for a poll.
- **A sweep finds it.** `Start` sweeps once, and `Config.Interval` (default 15s)
  repeats it. A sweep asks the engine which workspaces each consumer is behind
  on and queues those, so it costs one query per consumer rather than a pass per
  workspace. It is the safety net for a signal that was lost, a process that
  restarted, and a consumer registered against a store that already holds
  history.
- **`Runtime.Kick(workspaceID)`** is the manual trigger for anything that knows
  a workspace changed before the engine does.

Notifier signals carry no guarantee, so a negative `Interval` — no periodic
sweep — is a choice to rely on the notifier alone. The default notifier is
in-process, so a workspace written by **another node** is reported only on that
node; a shared notifier (`WithNotifier`) fixes that if it reports activity too.

`Config.Workers` (default 16) bounds how many passes run at once across every
consumer and workspace. It is the only concurrency bound in the runtime, so it
is what to raise for a slow, I/O-bound sink and what to lower to leave more read
capacity to serving clients. Passes for one consumer on one workspace never
overlap whatever it is set to.

## Cost at many workspaces

What the runtime holds tracks the work outstanding, not the size of the store. A
`(consumer, workspace)` pair costs memory only while it has work queued, a pass
running, or a failure being retried; a workspace that has drained is forgotten
until it is written again, since everything durable about the consumer's
progress lives in the workspace's own store. `Runtime.Tracked()` reports that
resident set — it should stay flat as a store grows, and climb only when
consumers fall behind the writes.

The one thing that scales with the store is a sweep, which scans for workspaces
with pending work once per interval per consumer. Reaction latency does not
depend on it.

## Consumer names

The name is the identity of the durable position, so it must be stable across
restarts. Renaming a consumer does not carry its progress over — it starts a
first run, which against a workspace that has collected tombstones means resuming
at the current sequence rather than replaying history.

## What effects cannot do

**The feed is a state feed, not an event log**, in two independent ways:

- Only the latest state of a row is observable. Writes that coalesce leave one
  change, and no shadow can recover the states in between.
- Changes older than the workspace's tombstone horizon are collected. A consumer
  that falls that far behind logs a warning, drops its shadows, and resumes at
  the current sequence, having permanently missed what it never saw — and it
  cannot enumerate what that was.

Neither is repairable for an outbound action: current state cannot tell you which
notifications were never sent. **An effect that must not miss anything needs the
event persisted as a row through `Mutate`**, where it syncs and is retained under
normal row rules, with a handler reacting to that row.

The tombstone horizon is therefore the bound on how long a consumer may be down.
Size [`WithGC`](../reference/server.md) retention accordingly. A consumer
stranded below the horizon is eventually collected by the GC pass, loudly.

## Running more than one instance

Every pass takes a per-`(consumer, workspace)` lease from the server's locker
(`server.Leases`), so instances of the same consumer against one store divide
the workspaces rather than each draining every one. With `pgstore` the lease
space is the shared database and this needs no configuration; with an adapter
that provides no locker the lease is process-local and dedups nothing across
processes.

The lease is advisory, so **single delivery is the common case, not a
guarantee** — at-least-once stands, and handlers stay idempotent. In
particular a **rolling deploy** can still overlap: a lease lost to a dropped
connection frees the workspace while the old holder is mid-pass.
`Runtime.Close` waits for in-flight passes, which narrows the window without
closing it — stop before starting if duplicate delivery is expensive.

A skipped pass leaves the consumer's position untouched, which is what brings
the workspace back on a later sweep if the lease holder dies without
finishing. `Drain` reports the skip as `effects.ErrLeaseHeld`.

## Related

- [Follow committed changes (change feed)](change-feed.md) — the primitive this
  is built on, for consumers that want the loop themselves.
- [Write from server-side code](server-side-writes.md) — including
  `MutateIdempotent`, which a handler that writes back should use.
- [Sync an external store (connectors)](sync-an-external-store.md) — the feed
  plus an ingress path, for keeping an external store converged rather than
  reacting to changes.
