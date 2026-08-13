# Multi-tab behaviour

Tabs of one app are not independent clients. They share one IndexedDB database
and one `clientId` **per account** — every workspace the account holds lives in
that one store — and they coordinate through browser primitives rather than
through the server.

## One store, one lock

KV writes, mutation-id minting included, are guarded by a **Web Locks** mutex, so
concurrent tabs never mint duplicate ids. That matters because the server
requires per-`(workspace, client_id)` gap-free mutation ids: two tabs minting
independently would break contiguity for the client they share.

Environments without `navigator.locks` — Node, tests — fall back to an
in-process mutex.

## One stream per plane, held by a visible tab

One **visible** tab holds the live `Subscribe` stream, elected via a second Web
Lock, once per account, carrying every workspace any tab needs. It applies the
rows it receives to the shared store; the others see those writes through the
store plus a `BroadcastChannel` carrying the touched `(workspace, entity)` set.
They run no stream of their own.

Leadership is released on `visibilitychange→hidden` (after a short grace),
`pagehide` and `freeze`, and re-contended on `visible`/`resume`. So it always
lives in a visible tab — or nowhere: all-hidden tabs fall back to the per-tab
poll floor, and a suspended background tab never strands the lock.

If the lock request itself fails — Web Locks exists but is unusable, as in some
sandboxed or storage-partitioned embeds — the tab stops contending, logs it, and
runs its own stream for the session, exactly as a browser without Web Locks
does. An extra connection is the cheaper failure than no live updates at all.

## Two leaders, not one

An app using [rooms](rooms.md) holds a **second** long-lived stream per account
— `Watch`, for the ephemeral plane — elected on its own Web Lock. The two
leaders are independent and need not be the same tab.

They are separate because the planes are. A room is useless without a stream
while the data stream is optional (`streamEnabled: false` is a supported
configuration, and the integration suite's default), so rooms cannot ride the
data stream's term. Nesting them would also mean one plane's failure retiring
the other's tab from contention for the rest of the session.

Only _receiving_ needs a leader: `Publish` is unary, so every tab writes its own
presence directly. What the leader does is hold one `Watch` covering the union
of every tab's rooms, and forward the frames over a `BroadcastChannel`.

The union is a live census rather than a durable record. Tabs announce their
room set when it changes, on request from a newly elected leader, and every
couple of seconds; the leader unions what it hears, debounces, and reopens
`Watch` when the set actually moved. A tab it has not heard from is dropped —
the same soft-state rule the server applies to sessions, one level down, and the
reason a crashed tab's cursor does not linger.

The leader also names its followers' **sessions** on the request, because the
server keeps a presence record alive only while it is watching the stream
responsible for it. Without that, a room would empty out to whichever tab
happened to hold the lock.

A handover is invisible to everyone else by design: the outgoing leader
publishes no departure, and the incoming one's stream lands well inside the
expiry window. What other people see is nothing at all.

The same degradations apply as for the data stream. Without Web Locks or
`BroadcastChannel` — a server runtime, an older browser — every tab runs its own
`Watch`, which is correct and costs a connection each.

## Scope is the union, coverage is your own

The coordinator syncs the union of every live tab's subscriptions, so a tab may
hold rows it did not ask for. Coverage still resolves against **this tab's own**
declared scope: the union is a superset, so own-scope coverage errs toward
claiming less completeness than reality, and it does not flap as peer tabs come
and go. `status().scope` reports `own`, `effective` and whether it was `widened`.

## Why this tier has its own tests

None of this has a stand-in outside a browser — `navigator.locks` and
`BroadcastChannel` decide at runtime whether the client shares or goes it alone.
So the claims above are checked by a browser tier of their own,
`client/test/e2e` (`task test:e2e`): real tabs, one real IndexedDB, real locks.

See [Testing](../how-to/testing.md#the-browser-tier).

## Lifecycle

`client.close()` tears everything down and is idempotent. A closed client throws
`"SyncClient is closed"` and cannot be reopened — create a new instance.
