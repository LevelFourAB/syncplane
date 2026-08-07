# Follow committed changes (change feed)

`Changes` drains committed row changes in `(seq, entity, key)` order from a
durable cursor — the primitive under search indexing, connectors, and anything
else that follows the data:

> Most callers want [the effects runtime](react-to-changes.md), which owns this
> loop for you: durable positions, scheduling across workspaces, backoff,
> health, and before-state for reacting to transitions. Use `Changes` directly
> when you want the loop itself.

```go
cur := loadCursor()                     // persisted by the consumer
next, more, err := srv.Changes(ctx, workspaceID, cur, 256, func(ch server.Change) error {
    // ch.Entity, ch.Key, ch.Seq, ch.Deleted, ch.Message (nil when deleted),
    // ch.Actor (who wrote it)
    return index(ch)
})
saveCursor(next)                        // persist AFTER acting on the page
```

## Semantics

- **At-least-once**, when the cursor is persisted after the page is processed.
  Dedupe by `(entity, key, seq)`. A handler that reacts by writing back should
  do so through [`MutateIdempotent`](server-side-writes.md#writes-that-may-be-retried)
  keyed on the change, since redelivery is by design.
- **Per-workspace order.**
- **Only the latest state of a row is observable**, not every intermediate
  write.
- **It is a state feed, not an event log.** See [below](#not-an-event-log) — the
  distinction decides whether it can carry what you want it to.
- Text-bearing rows always carry **full documents**, never delta fragments.
- The feed is **trusted** server-side API: permission read filters do not apply.
- Every change carries an **actor** — see below.

## Who wrote it

`ch.Actor` (a `storage.Actor`) reports the writer in two fields:

- `Subject` — the authenticated caller's subject, copied from its
  [`Principal`](authentication-and-permissions.md). Empty when no authenticated
  caller made the write.
- `System` — the label a write made under
  [`Trusted`/`TrustedAs`](server-side-writes.md) gave itself, `"system"` for a
  bare `Trusted`. Empty exactly when a caller made the write.

Both set means a bypass ran under that caller's request. The zero actor means
the write was unattributed.

It is what makes two common rules expressible:

```go
// Don't react to your own writes.
if ch.Actor.System == "notifier" {
    return nil
}
// Notify everyone except whoever did it.
notifyAllExcept(ch.Actor.Subject, ch)
```

Three limits to design around:

- **It is the last writer of the row, not every writer.** The feed reports state
  rather than transitions, so writes that coalesce into one change leave only
  the final actor. Suppressing on it therefore drops a genuine trigger whenever
  someone else's write is coalesced behind your own — the same blind spot a
  sequence watermark has. A consumer that must not miss a change reacts to state
  instead of to authorship. It is attribution, not an audit trail.
- **A caller's writes are attributed only if you authenticate them.** With no
  `Authenticator` installed — the default — every client push records the zero
  actor, so `Subject` is empty and the suppression rules above no-op for
  anything a client wrote. Per-row attribution is something
  [authentication](authentication-and-permissions.md) buys. The `client_id` a
  push came under is not a substitute and is not recorded: a client picks its
  own and may pick anyone else's.
- **`System` is a label, not an identity.** Nothing verifies it and no policy
  runs against it — the write that set it skipped the hooks. It answers "which
  pass wrote this", never "was this allowed".

## Falling behind

`Changes` returns `ErrCursorTooOld` plus a cursor at the current sequence
whenever the cursor you presented cannot be resumed. Resync against current
state, then resume from the one it handed back. Three things produce it:

- The cursor predates the tombstone horizon — the consumer was offline past GC
  retention.
- The cursor sits above the current sequence — the store behind the feed was
  recreated empty, or restored below the consumer's position.
- The cursor names a different store lineage — the database at this address was
  swapped for another one, or restored from a store created separately. Without
  this check the feed would hand a consumer rows from a history it never read.

**Persist the cursor whole, `Epoch` included.** That field is what makes the
third case detectable, and a consumer whose bookkeeping drops it keeps working
without the check — 0 reads as "lineage unknown" and is always accepted. The
shipped `sqlitestate` keeps it; a `connect.State` you write yourself must too.
Effect consumers need nothing: their positions live in the workspace's own
store, so a swap or restore carries them along with the changes they point into.

A new consumer takes the same path: the zero cursor is below the horizon of any
workspace that has ever collected a tombstone, so a first run against an
existing workspace starts by resyncing rather than by replaying the feed. Write
the resync path first — it is the normal entry point, not the exception.

## Not an event log

The feed reports the current state of changed rows. That is enough to rebuild a
search index, a mirror or a cache, which is what "resync against current state"
repairs. It is **not** an event history, in two independent ways:

- **Coalescing.** Writes to one row between two drains leave a single change
  carrying the final state. The intermediate states were never observable and no
  cursor can go back for them.
- **The horizon.** Changes older than the tombstone horizon are collected. A
  consumer past it has permanently missed changes and cannot enumerate which —
  the tombstones that would have named them are gone.

Neither is repairable for an outbound action: you cannot un-send an email, and
you cannot reconstruct a webhook you failed to deliver from a row's current
state. So the guarantee to design against is "you will converge on current
state", not "you will see every change".

**If you need guaranteed delivery of events, persist the event as a row** through
[`Mutate`](server-side-writes.md) and react to that row. The engine then syncs and
retains it under normal row rules, and the horizon bounds how long a consumer may
be down rather than what it may lose.

## Lower latency

`SubscribeChanges` exposes the in-process commit wakeup the live client stream
rides on. It cuts latency but carries no guarantee, so consumers still drain on
their own schedule.

It needs a workspace id up front. A consumer following many workspaces wants
`SubscribeActivity` instead: one subscription reporting every workspace that
commits a write, including ones it has never heard of. See
[the server API](../reference/server.md#change-feed).

## Related

- [React to changes (server-side effects)](react-to-changes.md) — this loop,
  packaged, with before-state and failure handling.
- [Sync an external store](sync-an-external-store.md) — the change feed plus an
  ingress path, packaged.
- [Go server API](../reference/server.md).
