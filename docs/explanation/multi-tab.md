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

## One stream, held by a visible tab

One **visible** tab holds the live `Subscribe` stream, elected via a second Web
Lock, once per account, carrying every workspace any tab needs. It applies the
rows it receives to the shared store; the others see those writes through the
store plus a `BroadcastChannel` carrying the touched `(workspace, entity)` set.
They run no stream of their own.

Leadership is released on `visibilitychange→hidden` (after a short grace),
`pagehide` and `freeze`, and re-contended on `visible`/`resume`. So it always
lives in a visible tab — or nowhere: all-hidden tabs simply fall back to the
per-tab poll floor, and a suspended background tab never strands the lock.

If the lock request itself fails — Web Locks exists but is unusable, as in some
sandboxed or storage-partitioned embeds — the tab stops contending, logs it, and
runs its own stream for the session, exactly as a browser without Web Locks
does. An extra connection is the cheaper failure than no live updates at all.

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
