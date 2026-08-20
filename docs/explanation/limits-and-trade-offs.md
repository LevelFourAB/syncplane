# Limits and trade-offs

What the engine deliberately does not do, and what to know before adopting it.
Each of these is a consequence of [the convergence model](convergence-model.md), not an oversight.

## An unindexed query scans everything

Without an index that serves it, a query scans and decodes **every live row of
the entity** — `client.query` over the synced rows, and `queryServer` / the
`Query` RPC / `ViewTx.Query` over the stored ones. Only memory is bounded (to
the page), never the work.

So: a selective predicate beats deep pagination, and a large entity behind a hot
endpoint will be felt. `contains` is a substring test per row and is never
index-servable; searching text is what a [match query](search.md) is for, which
brings its own term index.

## A filter can cross a reference, a sort cannot

`matches()` is the one condition that leaves the row: a single hop through a
forward reference. It stays cheap because nothing downstream ever sees it — a
pre-pass runs the inner condition over the target entity, the node collapses to a
membership test on the stored stub, and the planner, the index walk and the
cursors are handed an ordinary row-local query. The full model is in
[relational queries](relational-queries.md).

`orderBy` has no counterpart, because a sort key cannot be folded away like
that. The value it sorts on lives in another row, and no index key reaches into
one, so the only way to answer is to sort the entire match set: scan and decode
the outer entity, look up every distinct target, again on the next page, and
once more for `total`.

Partial sync sharpens the difference. When a predicate cannot see its target the
row drops out and coverage reports the `relation` bound — a failure the reader
can act on. When a sort key cannot see one there is nothing to sort by, so the
row sinks to the end of the result, and under a `limit` the end of the result is
off the page. What comes back is a short answer with nothing to mark it as
short.

So sorting across a reference stays with the two places it already works. For an
ordered slice per holder — the five newest comments on each issue — `include`
takes `$where`, `$orderBy` and `$limit`, applied to each holder inside the pass
that hydrates it. For a table sorted by a related column, hydrate and sort in the
app: those rows are on screen already, and sorting them again is not work the
engine could have taken off you.

## Cursors page, offset jumps to a rank

`after`/`before` are the default way to page: a cursor names a POSITION in the
ordering, so the walk seeks to it and stops once the page is full — O(limit)
rather than O(offset+limit) on an index-served query — and the page is STABLE.
An offset counts rows, so a row inserted ahead of the page between two requests
shifts every rank after it:
the next page repeats a row the reader already saw, and a delete makes it skip
one instead. Neither is an error the reader can detect. A cursor is a value in
the ordering, and inserts and deletes elsewhere do not move it.

What a cursor structurally cannot do is jump to an arbitrary rank. "Row 8000" is
a count, and nothing in a page boundary expresses one — a scrollbar dragged into
the middle of a long list is asking exactly that, and it is what `offset` is for
and why it is capped. The two are mutually exclusive on a
single request, because a rank and a position describe a page in terms that have
no common resolution: picking one silently would be an invention, so the
combination is rejected.

Every result reports `total`, the matches behind it, and locally that is not
free on an indexed query with a `limit`: the walk can stop reading rows once the
page is full, but it still has to reach the end of its range to count. Counting
costs index positions rather than row reads — unless a predicate the range could
not express is left over, in which case only the row can settle a match and the
rest of the range is read too.

`watchQuery` maintains its result rather than recomputing it. Each watcher
holds the rows its query matches, in result order; a commit arrives naming the
rows it changed, and each of those is spliced in or out — whether it matched
before is what the watcher already holds, and where it sat is a binary search
away. So a commit costs the rows it changed, not the size of the entity, and one
that changes nothing the query holds or now admits costs nothing at all. Its
first read is served from an index when one covers it, like `client.query`'s —
though it asks for every match rather than one page, so a very large match set
can exceed the index walk's budget and fall back to the scan.

Two things still cost the whole entity. A commit that reports an entity **without
its rows** — a bootstrap swap, a wholesale overlay clear — forces a rescan,
because "something in here changed" is all it said. And memory tracks the
**match set**: a narrow query over a large entity holds little, but one with no
predicate holds an entry per row, so a watch over an unfiltered large entity is
still a large working set. A `limit` bounds what is delivered, not what is held.

That is the trade the paging surface is built on. Holding the rows past the page
is what lets a deletion inside a full page pull the next one in without a read,
what makes `total` free, and what makes `setWindow` a slice — so a virtualized
list scrolls a large result without touching the store. The price is that the
result is in memory to scroll.

`client.query` is served from an index when one covers it: a workspace-scoped
query whose predicate and ordering the planner can express as a range reads that
range and merges the optimistic overlay over it, instead of reading the entity.
A top-level `or` is served as a union — each branch planned on its own, the
probes deduped by row key — as long as **every** branch has an index; one
unindexed branch declines the whole thing, because a union that scans for one
branch is the scan with extra steps. A union comes back unordered, so an
ordered `or` pays a sort over its matches rather than reading in index order.
A cross-workspace query declines — the index nests the workspace inside its own
prefix, and reproducing the scan's cross-workspace tie-break through a merge is
not worth the divergence risk.

Two further paths are exceptions, both server-side and neither reachable from a
query:

- A **reverse follow hop** ("every issue whose project is this one") is served
  from an index the engine derives for every reference field, so it costs
  O(matches) rather than O(source entity) — through a **to-one** ref. Through a
  **to-many** one the hop scans the source entity and re-derives each stub's
  key: the index holds one position per key in the list, but probing it for a
  single target would need the multi-entry read the reverse path does not do.
  The entries are still written either way, so the write cost below is paid by
  both.
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

Both costs above are what a window or a closure costs to resolve FROM SCRATCH,
which a pull does only when it cannot maintain what the cursor already carries —
see [partial sync](partial-sync-and-coverage.md#what-a-bounded-membership-costs-the-server).
Maintenance is held to the same rule as index use, and tested the same way:
`Limits.DisableMembershipMaintenance` forces the rebuild, and the two must
produce identical pages however the workspace is driven.

The cost is paid on writes, on both sides. A write that maintains indexes
re-encodes and re-stores that row's whole entry set rather than diffing it —
neither side has the previous row value in hand on a PATCH or a DELETE, so
computing a diff would put a read and a decode on every write path.

**PUT and PATCH are not symmetric here.** A PUT replaces the whole row, so it
always replaces the whole entry set. A PATCH carries a field mask, and a mask
naming only fields no index reads cannot move the row in any of them — so the
rewrite is skipped, and the write costs what it would on an entity with no
indexes at all. The test is a path comparison against the compiled components,
so it costs nothing to ask, and it is conservative in the safe direction: a mask
it cannot account for (one naming a oneof group, or addressing one item of a
collection) rewrites. This is why the TS client's `update()` deriving its mask
from a diff matters beyond bandwidth — an edit to a title stops paying for the
index on a field it did not touch.

An index over a repeated field, or over every entry of a map, makes the
remaining cost grow with the data rather than with the schema — one entry per
element or per entry — so `MaxIndexEntriesPerRow` (default 1024) stops being a
formality: it is the ceiling on how many positions one row may occupy, and a
write that would exceed it **fails** rather than storing a truncated set,
because a partial index would disagree with the scan it is meant to be
indistinguishable from.

### What an EAV index over user-defined fields costs

An index over `custom.*` stores one entry per map ENTRY, so a row's cost scales
with the number of custom fields that row **uses**, not with the number the app
has defined. A field-definition table with 200 fields costs nothing on a row that
fills in three of them.

The write cost is linear in entries per row, and it is paid as a
delete-and-reinsert of that row's whole entry set. Three things decide whether
that matters to you:

- **How many entries a row occupies.** Every fan-out multiplies: N custom fields
  is N entries per `*` index, and a multi-select field's entries multiply again
  by its selected values. `MaxIndexEntriesPerRow` is the ceiling, and exceeding
  it fails the write.
- **Whether your writes touch an indexed field at all.** They mostly do not, and
  the PATCH mask guard above is what turns that into no cost: an edit to a title
  writes exactly what it would on an entity with no indexes. This is the single
  biggest lever, and it is free as long as writes arrive as PATCH with an honest
  mask — which is what the TS client's `update()` produces.
- **How fast your storage is.** The index term is roughly constant per entry, so
  it is a small fraction of a slow write and a large multiple of a fast one. The
  ratio on a laptop with a local database flatters what you will see on
  production hardware, so measure the shape rather than trusting a number.

#### One index or two

A map index declares which side of the entry each component stores, so there are
two layouts and they answer different questions:

| Layout         | Serves                                                                 |
| -------------- | ---------------------------------------------------------------------- |
| `[key, value]` | equality, ranges **and** ordering — within one named field             |
| `[value, key]` | equality only — cross-key _or_ named-key (pin the value, then the key) |

`[value, key]` is the more capable of the two **for equality**: it answers both
"does `priority` hold 5" and "which field holds 5". So an app whose custom-field
queries are all equality declares one index, not two — and the second layout is
what you add when you need a range inside one field (`priority > 3`) or an
ordering by one.

What the second index costs, in shape:

- **Entries per row double, exactly.** A row with N filled custom fields occupies
  N entries per map index, so declaring both layouts makes it 2N — and 2N is what
  `MaxIndexEntriesPerRow` is spent against.
- **PUT cost follows the entry count**, roughly linearly once the entry set
  dominates the write.
- **A PATCH that touches no indexed field pays nothing either way.** The mask
  guard is unaffected by how many indexes exist, which is why a wide EAV entity
  stays cheap to edit.
- **Equality query latency does not change.** Either layout pins both components
  from one predicate, so the second index buys the shapes equality cannot express
  — a range inside one named field, an ordering by it — and not a faster
  equality.

And what a map index buys the query it exists for: the scan is linear in the
entity, the index is flat in it, with the crossover well below any corpus worth
indexing. `engine/server/index_bench_test.go` measures both —
`BenchmarkIndexedWrite/eav` for the write side and entries per row,
`BenchmarkCustomFieldQuery` for the read side. Run them on the hardware you care
about rather than trusting a ratio measured on someone else's:

```sh
go test ./engine/server/... -bench 'BenchmarkIndexedWrite/eav' -run '^$'
go test ./engine/server/... -bench BenchmarkCustomFieldQuery -run '^$'
```

#### Deployment notes

- **Declaring both surfaces over one collection doubles its entries.** A field
  that carries `indexed: true` AND appears as a component of a declared index
  writes one set of entries for each. Pick one unless both shapes are actually
  queried.
- **On PostgreSQL, every write leaves one dead tuple per entry it replaced.** A
  row with a dozen custom fields churns a dozen tuples on each edit that touches
  one. If custom fields are your write shape, tune autovacuum for the
  `index_entries` table specifically — `autovacuum_vacuum_scale_factor` at the
  table level is the lever — rather than relying on the cluster default, which
  is sized for tables that do not rewrite a row's worth of tuples per edit.

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

## A read filter hides values, not existence

A `ReadFilter` decides which rows a client may **read**; it does not make their
keys unobservable. Two mechanisms disclose that a denied row exists to a client
whose subscription scope covers the entity:

- **Deletions.** A tombstone carries entity, key and version to every
  subscribed client — the value is gone, so there is nothing left to run the
  filter over.
- **Revocation.** When a write leaves a row filter-denied — say it rewrites the
  member list the filter consults — pull tombstones the row so a client that
  could read it before the write drops its copy. The server stores only each
  row's current state, so it cannot always tell that client apart from one that
  never saw the row. It withholds the tombstone wherever the cursor proves
  non-possession: a row created past the cursor's position was never delivered
  under it, and a windowed or followed scope carries its exact membership on
  the cursor, so a non-member the cursor never held emits nothing. Everywhere
  else it stays conservative and sends the tombstone. Bootstrap discloses
  nothing — hidden rows are skipped, and a client that holds nothing reconciles
  by absence — and a permission-change resync evaluates the cursor's old
  filter, so it tombstones only rows that were visible under it.

The consequence for schema design: **keys must not themselves be secrets**. An
entity keyed by email, slug or invite code hands subscribed clients those
values as rows are deleted or hidden; key by surrogate id and keep the secret
in a field, where the filter covers it.

What remains either way is a timing channel: versions are workspace-global, so
a client can infer from gaps in the versions it does see that writes it cannot
see happened — but not for which rows, and, given surrogate keys, not what
they concern.

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

**Background work dedups by lease, and only by lease.** Each GC pass, index
build, text-compaction sweep and effects drain takes an advisory per-workspace
lease (`pgstore` backs
it with advisory locks, so it comes free with the store), which makes nodes
divide the sweeps and makes single-fire the common case for effects. It is
deliberately not fencing: a lease can vanish with a dropped connection and its
holder keeps working, and a locker error makes the pass run unleased rather
than not at all. The alternative — leases that correctness depends on — needs
fencing tokens checked on every write, heartbeats, and an answer for what a
workspace does while its holder is dead but not yet expired. The engine's
background work is idempotent anyway, so it buys none of that; a duplicated
pass costs time, and a duplicated effect delivery is the at-least-once
contract handlers already carry.

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
that already holds it drops it — unless the row was created past the client's
cursor, in which case nothing was ever delivered and nothing is sent; bootstrap,
queries, windows and follow closures skip it. Everything else in the workspace
keeps syncing.

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

## Field history records editing sessions, not keystrokes

An entity carrying [`history: {}`](../reference/proto-options.md#field-history)
logs what its marked fields held before each change. Two decisions shape what
that log can answer, and both trade fidelity for a size bound.

The log is proportional to **edits**, not to writes. A local-first client
re-sends whole rows, so a per-write log would carry one full-capture-set record
per edit, each holding a `prior` identical to the row stored beside it. The
server compares the stored row with the one replacing it and records only the
fields that moved, which is why a write that stores a field's current value
again leaves nothing behind.

The log is proportional to **sessions**, not to writes within one. A run of
writes to one row by one actor folds into the record the first of them opened.
Without folding, a minute of typing lands hundreds of records, each carrying the
whole document that one keystroke changed, and a row's history outgrows the row
by orders of magnitude. What folding gives up is the states in between: history
answers "what did this field hold before that editing session", not "replay the
session". Keystroke granularity is what the collaborative-text delta log is for,
and it has a much shorter retention.

The same size argument decides two smaller things. A **text field is recorded as
its text** rather than as its encoded document — the document carries a peer
table, a version vector, per-run parents and the tombstoned runs holding every
deleted character, several times the text it renders, and a restore has to go
through the text anyway (writing document bytes back merges them with the stored
document, where the delete markers win). And **capture is explicit** per field
rather than defaulting to the whole message: what a field costs to record spans
orders of magnitude, so an implicit capture set would opt a schema into
recording a whole text document by adding a field — the one schema change the
engine otherwise treats as always safe.

Retention is finite by default (90 days) for the same reason: a workspace's
history grows with every write, and
[`HistoryForever`](../reference/server.md#history-that-lives-as-long-as-its-row)
is a choice to make deliberately rather than to inherit.

## Field history is a restore mechanism, not an audit log

The log answers "what did this field hold before that editing session". It does
not answer "who deleted this row" or "put the row back", and no retention
setting changes that.

A `DELETE` is not a field change, so nothing captures it — and it purges the
row's records in the same transaction, whatever the retention. Both halves are
forced. A restore is a PATCH, and a PATCH of a tombstoned row is a conflict
under delete-wins, so a kept log would be unusable for the one thing it exists
for. And keys are app-chosen, so a kept log would attach the old row's timeline
to whatever row next claims the key — readable by whoever can read that new row,
since a record is served under the **current** source row's filter.

Keeping the records anyway would still not buy an undelete. Capture is explicit
per field and records only the fields a write **moved**, so the log holds
before-states and never the row's final state: that lived on the row, and the
delete dropped it. A record of what a row's marked fields used to be is not a
snapshot of what the row was.

The change feed is not the fallback. It reports state rather than transitions,
coalesces writes to one row into a single change, and collects past the
tombstone horizon — attribution, not history. An audit trail that has to hold up
is [a row you write yourself](../how-to/change-feed.md#not-an-event-log),
retained under ordinary row rules.

What does make a deletion recoverable and attributable is modelling it as a
field. A captured `deleted_at` turns the delete into an ordinary PATCH, which
the log records with its actor and timestamp, and turns the undelete into a
PATCH clearing it, with no tombstone to conflict with. Attribution is only as
good as authentication: a row records the caller's subject, so a server that
admits every request as anonymous records nobody.
[`DELETE_POLICY_TRUSTED_ONLY`](../reference/proto-options.md#delete-policy) then
reserves the real DELETE for erasure, where purging the history is the point
rather than the limitation. See
[Recover a deleted row](../how-to/recover-a-deleted-row.md).

## Write policies classify a write, not its effect

`create_policy`, `update_policy` and `delete_policy` are decided from the
mutation alone — its op and whether it asserts an `expected_version` — before
the write transaction opens. No storage read, no row. That is what lets one
check cover every write path at once and cost nothing on the hot path, and it is
why an unguarded PUT (which could create or replace, and cannot be told apart)
must satisfy both create and update to be admitted. Classifying it as one or the
other would open a hole in the opposite direction: with creates restricted and
updates allowed, calling it an update would let any client create rows with a
plain `put()`.

Two consequences follow from being syntactic rather than semantic.

A batch applies in order inside one savepoint, so `DELETE(k)` followed by
`PUT(k, expected_version: 0)` is authorized as a delete and a create — both
truthfully — and lands as a replacement of the row. `update_policy` constrains
in-place edits; pairing it with `delete_policy` is what closes the loop.

And the policies range over caller class, not over rows. "Only the author may
edit their comment" is not a rule any schema enum can carry, because the verdict
depends on the row and on who is asking. It belongs in `CanMutate` when
ownership is derivable from the key, or in a mutator body when it needs the
stored row — which is the same escape hatch that makes a restricted entity
writable at all.

Two smaller edges fall out of the same rule. Undo inverts a DELETE into an
unguarded PUT, so undoing a delete on an entity that restricts updates is
refused. And collaborative-text edits are PATCHes, so an entity carrying a
`text: true` field needs updates allowed for clients to edit its text at all.

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
is expected and self-heals on rebase; never rely on mirror output as truth. A
mirror that throws fails the write that issued it, but on a later replay costs
only that mutation's optimism — the mutation is queued by then, and the server
applies it regardless.

## A queued mutation a tab cannot replay loses its optimism, not the write

The pending queue belongs to the workspace, so a tab replays entries other tabs
queued — including ones written against a schema it does not have (see
[the convergence model](convergence-model.md#a-client-holds-only-the-entities-it-declares)).
It skips such an entry and reports it over `console.error` rather than failing the
rebase, which would stall the workspace: pulls, rollbacks and bootstrap all
rebuild the optimistic overlay in the transaction that advances the cursor. The
entry stays queued, and the push retires it as usual — an ack, a rejection, or a
dead-letter after `poisonThreshold` identical failures. Until then that tab
shows the confirmed row.

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
onto one connection — but it does grow with the number of **planes** it uses:
an app using [rooms](rooms.md) holds a second long-lived stream per account
(`Watch`), because room membership changes on navigation and folding it into
`Subscribe`'s fixed scope set would force a data-stream reopen every time.

So the budget is one stream per plane per account, up to two. Several
`SyncClient`s against one host means several accounts (a credential is attached
per request, so two accounts cannot share a stream), and with rooms that binds
at roughly three accounts rather than six.

Ways out, in order of preference: serve h2c, which removes the cap entirely;
set `presenceEnabled: false` on the clients that do not render presence, which
trades the second connection for one unary keepalive per open room per announce
interval; or run fewer clients per page.

## The option field numbers are unregistered

The custom option field numbers (50100/50101) sit in the range reserved for use
within a single organization. Claim numbers in protobuf's global extension
registry before publishing the protos; moving them afterwards is a breaking
change for every adopter's generated code. See [Proto options](../reference/proto-options.md#the-option-field-numbers).
