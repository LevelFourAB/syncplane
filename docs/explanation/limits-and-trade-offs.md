# Limits and trade-offs

What the engine deliberately does not do, and what to know before adopting it.
Each of these is a consequence of [the convergence model](convergence-model.md), not an oversight.

## An unindexed query scans everything

Without an index that serves it, a query scans and decodes **every live row of
the entity** — `client.query` over the synced rows, and `queryServer` / the
`Query` RPC / `ViewTx.Query` over the stored ones. Only memory is bounded (to
`offset + limit`), never the work.

So: a selective predicate beats deep pagination, and a large entity behind a hot
endpoint will be felt. There is no full-text search; `contains` is a substring
test per row and is never index-servable.

`watchQuery` **always** scans, on both a cold start and every commit that
touches its entity. Its snapshot is shared per (entity, workspace) rather than
per query, so it cannot be narrowed to one query's matches without giving each
watcher its own — which is the remaining piece of this work, and the reason a
watched query over a large entity still costs what it always did.

`client.query` is served from an index when one covers it: a workspace-scoped
query whose predicate and ordering the planner can express as a range reads that
range and merges the optimistic overlay over it, instead of reading the entity.
A cross-workspace query declines — the index nests the workspace inside its own
prefix, and reproducing the scan's cross-workspace tie-break through a merge is
not worth the divergence risk.

Two further paths are exceptions, both server-side and neither reachable from a
query:

- A **reverse follow hop** ("every issue whose project is this one") is served
  from an index the engine derives for every reference field, to-one and to-many
  alike, so it costs O(matches) rather than O(source entity). A to-many
  reference stores a list of target keys, so its row occupies one position per
  key.
- The **seeds of a following subscription** — the base-scope rows its closure is
  walked from — go through the same planner as a query, so a follow anchored on
  a predicate costs O(matches) to anchor. With no predicate the seeds _are_ the
  entity and the scan is the answer, not a fallback. A windowed subscription
  seeds from its window, which is already resolved by the time the walk starts.
- An unfiltered **`by_recency` window** reads its `limit` newest rows and stops.
  With a predicate or a read filter it keeps reading, since either can reject any
  prefix of the ordering.

Both fall back to the scan whenever the index is not built — a fresh index, or
one whose declaration changed, is unusable until a background pass has walked the
entity. That fallback is the design, not a degraded mode: **using an index is a
pure optimization, and the indexed and scanning paths must produce identical
results.** `Limits.DisableIndexes` forces the fallback, which is how the property
is tested rather than assumed.

The cost is paid on writes, on both sides. Every write to an entity with indexes
re-encodes and re-stores that row's whole entry set — neither side has the
previous row value in hand on a PATCH or a DELETE, so both replace rather than
diff. An index over a repeated field makes that cost grow with the data rather
than with the schema — one entry per element — so `MaxIndexEntriesPerRow` stops
being a formality: it is the ceiling on how long an indexed list may get, and a
write that would exceed it fails rather than storing a truncated set, because a
partial index would disagree with the scan it is meant to be indistinguishable
from.

The client indexes **confirmed rows only**. The optimistic overlay is bounded by
the pending queue, so a query scans all of it and merges — which keeps the
answer identical to the merged view without maintaining entries for state that a
rebase throws away on the next pull.

Windowed subscriptions bound the client's working set, not the server's scan.

One consequence of an ordering that has to survive a scan is that it must be a
**total** order — every pair of rows resolvable to a definite side. Values rank
`real < NaN < missing`, and the rank is compared before the key's direction is
applied, so `NaN` and missing values both sort to the end under `asc` and `desc`
alike. `NaN` itself takes part in no comparison: `gt`/`gte`/`lt`/`lte` are false
with a `NaN` on either side, as `eq` already is.

## PUT replaces the whole row

**And so does `EntityTx.Put`** — except collaborative-text fields, which union
with the stored document on every PUT path.

A stale writer (an old client binary, or server code deserializing into an
outdated message type) silently drops fields it does not know about. Prefer
PATCH with a field mask for partial updates, and treat entity schema evolution
as additive.

The [schema-revision handshake](../how-to/handle-failures.md#handle-a-version-skew-during-a-deploy)
makes a skew window during a rolling deploy visible and actionable instead of a
silent stream of dropped writes. It does not replace additive evolution.

## The default posture is open

`workspace_id` is self-asserted, and `CanAccessWorkspace` is what makes it a
boundary. The engine gates every workspace-scoped RPC on that hook before doing
any storage work, but the default (`AllowAll`) admits everyone — **a shared
deployment that does not override it has no cross-tenant isolation, and nothing
will say so at compile time.**

`client_id` is likewise unauthenticated. It is an idempotency handle the client
asserts, not an identity — never derive authorization from it. Identity comes
from the `Principal` an `Authenticator` produces.

A live stream re-checks **access**, but not **credentials**: `Subscribe` re-runs
`CanAccessWorkspace` for every open scope on each heartbeat (default 30s), so a
member removed mid-stream is dropped within one interval. Authentication stays a
check at open, so a credential that expires mid-stream rides to the end of the
stream. Bound stream lifetime if that matters.

## Revocation stops future reads; erasing past ones is opt-in

A permanent denial reaches the client as `workspace_access_revoked` and it stops
syncing that workspace. The rows stay in IndexedDB until the app calls
`removeWorkspace(id, { purge: true })` — the client will not delete a user's data
off one server verdict.

A purge also discards whatever was still queued for that workspace; those
mutations can never be acked, so they come back from the call (and through
`onMutationRejected`) for the app to persist if it wants them.

## One node by default, more than one on purpose

The defaults are single-node, and deliberately so: SQLite plus an in-process
notifier is a deployment with no infrastructure at all. Both halves are seams,
and both have a Postgres implementation when you outgrow them.

The **default notifier is in-process only**. It wakes the `Subscribe` streams,
but only for clients on the same node, so clients elsewhere converge via
polling — correctly, but at poll latency. `WithNotifier` takes a shared one;
`pgnotify` is that over `LISTEN`/`NOTIFY`. The live stream scans the shared
store per node, so a notifier only ever carries a content-free "seq advanced"
signal, never the changed rows — which is why its delivery can be best-effort
without costing correctness.

The **SQLite adapter is single-writer** regardless of workspace: every
workspace's writes serialize against every other's, and two processes cannot
share the file. The storage interface has always permitted the concurrency, and
`pgstore` takes it — different workspaces commit in parallel, and any number of
nodes share the database.

What multi-node still does not give you, even with both:

- **Effects fire once per node**, not once. A consumer running on three nodes
  reacts three times, which is silent rather than convergent.
- **GC has no cross-node coordination.** The pass is idempotent so every node
  may run it; they just duplicate the work.

Sizing, single node: one process, one SQLite file, tens of apps at human-paced
write rates. See [the scale envelope](../how-to/host-multiple-apps.md#scale-envelope)
and [Run more than one node](../how-to/run-multiple-nodes.md).

## `app_id` is a coarser partition than `workspace_id`

Under a `Manager`, each app is its own `*Server` over its own storage namespace
and entity catalog; `workspace_id` then partitions data _within_ an app. Two apps
may even declare the same entity without colliding — they live in different
servers over different data partitions.

App identity is a routing and storage concern only; it is **not** encoded in the
proto package. Registration is trusted (`RegisterAuthorizer`), unlike
`client_id`.

## A stored row that will not decode goes invisible

Every write path refuses to store bytes that do not decode as the entity's
message, so this is a fault that arrives from outside them: an older binary, a
trusted writer with a bug, a corrupted store. When a read has to look inside such
a row — a read filter, a subscription predicate, a query, a window, a follow hop
— the server **quarantines** it rather than failing the request: the row is
logged once (`undecodable row quarantined`, at warn, with entity, key and
version) and then treated as absent. Pull sends a tombstone for it, so a client
that already holds it drops it; bootstrap, queries, windows and follow closures
skip it. Everything else in the workspace keeps syncing.

Nothing is deleted. Any PUT of that key replaces the bytes and the row syncs
normally again, which is the repair; a PATCH of it is rejected as a `conflict`
(a mask can only merge onto a base that decodes) rather than wedging the client's
queue.

One asymmetry is deliberate: a full sync with no read filter and no predicate
never decodes a row, and shipping stored bytes unparsed is what makes that path
cheap. A corrupt row still reaches clients there, and the failure surfaces at the
reader instead. The in-process reads (`ViewTx`, `ApplyTx`, `EntityTx`) also do
not quarantine — they return the decode error to the Go caller, since silently
short-changing a server-side job's result set is the worse failure.

## Custom mergers must satisfy the contract

Fields marked `opaque: true` are replaced whole during merges on both sides — the
escape hatch for app-defined blobs. A custom merger registered for one
(`WithMerger` plus a client `MergerSpec`) **must be idempotent, commutative
and associative**: it runs on server apply, on optimistic overlay merge, and
again on every rebase replay, so a non-idempotent merger (e.g. a concat)
corrupts data.

For collaborative prose do not write one — use `text: true`, whose built-in union
merger satisfies the contract by construction.

## Custom-mutation mirrors are best-effort

A TypeScript mirror of a Go mutator exists to keep the UI responsive. Divergence
is expected and self-heals on rebase; never rely on mirror output as truth.

## A local write is not pushed immediately

A write waits `pushDebounceMs` (default 50ms, restarted by each further write and
capped by `pushMaxDelayMs`, default 250ms) before the client pushes. Without it,
a burst of edits — typing into a text field, dragging a sort handle — spends one
push per event, and the server linearizes each one separately.

The delay buys more than round trips. The push path compacts the never-pushed
tail of the queue first, so writes that land inside one window fold into a single
mutation: a keystroke burst reaches the server as one PATCH rather than as one
per character.

What it costs is how soon a _peer_ sees the edit — up to the ceiling, on top of
the round trip. It costs nothing in durability: the write is committed to the
local store and the pending queue before the window opens, so a tab that closes
mid-window pushes on its next start. Inbound changes are never delayed; only
writes wait, and every other trigger (poke, poll, reconnect, scope change) cycles
at once. Call `syncNow()` for the moments where the wait is wrong anyway — a
submit, a blur, a tab going hidden.

## Wire cursors are opaque

Clients must not interpret them.

## Browser connection limits

Browsers cap around six concurrent HTTP/1.1 connections per host. A client's
stream count does not grow with its workspaces — `Subscribe` multiplexes them
onto one connection — so the cap only binds when a page runs several
`SyncClient`s against one host, which means several accounts (a credential is
attached per request, so two accounts cannot share a stream). Serve h2c if you
expect that.

## The option field numbers are unregistered

The custom option field numbers (50100/50101) sit in the range reserved for use
within a single organization. Claim numbers in protobuf's global extension
registry before publishing the protos; moving them afterwards is a breaking
change for every adopter's generated code. See [Proto options](../reference/proto-options.md#a-note-on-the-option-field-numbers).
