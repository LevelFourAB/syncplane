# Partial sync and coverage

This document explains how partial sync bounds local working sets, how the server maintains window and relation memberships efficiently, and how clients track whether local data provides a complete or partial view of an entity.

## Why windows exist

A window handles cases that a predicate cannot express. A predicate scope, such as "every open to-do", works for small entities but fails for large views. A busy project or an active label routinely matches tens of thousands of rows, and no selective predicate can represent it.

A window is a rank scope: a `limit` plus an ordering. The `orderBy` or `byRecency` parameter decides which rows win the ranks, and ties break on the row key. With a window, working-set size becomes a policy choice rather than entity size.

## Why key sets exist

A key set handles the other case a predicate cannot express: a set of rows chosen by the user rather than by their fields. The typical shape is mixed data. A client syncs the metadata of every document through a predicate scope and holds the bodies of only the documents it works on or has marked for offline use. No field of the document says which device pinned it, so no predicate selects those rows.

A key set is that set, sent as row keys. The server treats every key as a row it can read directly: a bootstrap reads the named rows and nothing else, and a change to the set reads only the rows named on either side of it. One subscription per pinned row would sync the same rows, but each would be a predicate the server has to recognise and resolve on every request, and a single `in()` over the key field is not a shape the server can resolve without walking the entity.

A key set is a claim, not a grant. Every named row still passes the caller's read filter, and a row the caller may not see is withheld rather than refused.

## Why holds exist

A key set is declared: the app decides which rows it names. A hold is the same shape managed at runtime. A row found by a server query, or opened from a link, is outside every declared subscription, and the client cannot edit what it does not hold locally: text handles, watchers, the optimistic overlay and undo all assume a local row. A parallel cache of server snapshots would have none of that. A hold makes the row local instead.

The client keeps one key set per entity of the rows currently held and appends it to the declared subscriptions as one more subscription. Nothing on the server distinguishes a held row from a declared one. Taking or releasing a hold changes the subscription set, and the server reconciles the change with a delta resync that reads the rows named on either side. An entity whose subscriptions did not change is skipped without a read, so a hold on one document costs one point read however large the rest of the scope is.

Holds are shared across tabs the same way declared subscriptions are: every tab publishes its own set and syncs the union. A row one tab holds stays in scope for every tab, and stays when that tab closes as long as another holds it. Holds are not persisted, because a reload has no components holding anything, and the grace period after the last release absorbs a view that closes a row and reopens it.

A held row does not make its entity complete. Reads report the `predicate` bound, as they do for a declared key set. Under full sync there is nothing to add to, and an entity a declared subscription already covers whole gains nothing from naming rows of it, so a hold changes nothing in either case.

## Window maintenance is exact, not best-effort

The cursor records window membership as a key set. Every pull that delivers data brings the window up to date and computes a diff against that set:

- Rows that entered arrive as values.
- Rows that left arrive as tombstones and leave the local store.

This diff includes a row that sank below the rank boundary when no predicate changed and nothing was deleted. Exact window maintenance makes the local store trustworthy. Without exact diffing, a client accumulates rows that no longer belong to any active view.

`byRecency` is the cheapest ordering to maintain because ranking by write sequence requires no field comparisons.

## What a bootstrap of a partial scope costs

A cold bootstrap ships one entity at a time. Where the entity's scope names the rows it admits — a predicate a secondary index serves, a predicate pinning every key field to a literal, a key set, a window's membership, or a reference-following closure — the snapshot reads those rows and nothing else, so it costs the number of rows it ships. Every other shape reads every live row of the entity and keeps the ones the scope admits: full sync, a scope covering a whole entity, a predicate neither an index nor a pinned key reaches, and a candidate set larger than `MaxResyncScan`. Both paths ship the same rows in the same order, and an interrupted snapshot resumes across either. Indexing the fields a subscription filters on is what makes a snapshot of a narrow scope over a large entity cost the size of the scope.

## Changing the scope resyncs the difference

Changing subscriptions delta-resyncs the visible-set difference rather than forcing a full re-bootstrap. The same applies to permission changes when the policy implements `EpochReadFilters`. `FilterEpoch` versions the inputs behind `ReadFilter`, so a revoked label or a changed membership syncs as a diff.

An open `Subscribe` stream resyncs the same way. A stream opened with a cursor from another subscription set or epoch resyncs that cursor before the scope reports `caught_up`. An epoch that moves while the stream is open resyncs the scope before its next page. Where a `Pull` would reset, the stream drops the scope with reason `RESET`.

The resync pages by the differences it emits rather than by the rows it examines. Each `Pull` compares up to `MaxResyncScan` live rows against both the old and the new visibility and returns at most `MaxPullChanges` differences. Rows visible under both scopes emit nothing, so a scope change over a large workspace costs round trips in proportion to the size of the difference rather than the size of the workspace. Lowering `MaxResyncScan` shortens the read transaction each `Pull` holds and adds round trips.

What the entity's own scope is made of decides how many rows the comparison has to read. Where both the old and the new scope select rows by predicates a secondary index serves or a predicate pins the whole row key with, by key sets, or by window membership alone, the only rows that can change visibility are the ones those predicates, key sets and windows name, so the resync reads them and leaves the rest of the entity untouched. Every other shape — a scope covering a whole entity, an entity reached by following references, a predicate no index serves, or a candidate set larger than `MaxResyncScan` — compares every live row of the entity instead. Both produce the same difference; indexing the fields a subscription filters on is what makes a scope change cost the size of the scope rather than the size of the entity.

An entity whose subscriptions are the same on both sides of the change, under the same read filter, is skipped without a read. Both sides would classify every row of it alike, so the diff has nothing to find there. A change to one entity of a large scope therefore costs the rows that entity names, not a walk of every other entity in the set.

Rows the client holds through a `follow` are outside the comparison. The resync treats every member of the cursor's closure as visible under both scopes, so a row that leaves base scope but stays reachable through a reference is kept rather than tombstoned. The pull after the resync walks the closure under the new scope and reconciles it against the set the cursor carried: members the new scope reaches arrive, members it no longer reaches and that base scope does not hold drop. That one pull walks the closure from scratch, because a set walked from the old scope's seeds cannot be brought up to date from the rows that changed since.

A pull whose cursor already sits at the head of the workspace skips recomputation entirely. Because nothing was written, memberships cannot move, and the server carries the cursor key set forward as issued. Polling a quiet workspace costs nothing to return an empty update.

## The cursor references its membership instead of restating it

Membership sets make cursors large. A window at its limit carries that many row keys, and a reference-following closure can carry far more. Exact diffing requires stored membership sets, because a diff needs the previous membership and nothing else can reproduce it. However, the membership does not need to be stored inside the cursor itself.

With a cursor-state store, the server retains sets under an opaque reference. The cursor on the wire stays a few dozen bytes regardless of how large client holdings grow. The server writes a new entry only when membership changes: an idle poll costs one store read and reissues the existing reference.

The store can evict entries. A cursor whose entry was evicted resets the client to a fresh bootstrap. This is the same recovery mechanism as falling below the tombstone garbage collection (GC) horizon, so eviction must remain rare. When the cursor-state store is unavailable or absent, the cursor inlines the sets directly. Indirection degrades to extra payload bytes rather than an incorrect response.

## The cursor is a checkpoint on a stream, not a per-frame receipt

A server running without a cursor-state store produces large cursors. On `Subscribe`, you can avoid this cost because the server tracks the scope position, and the cursor on a frame exists only so the client can resume later.

When a client sets `sparse_cursor`, the server sends the cursor as a periodic checkpoint instead of on every frame. The steady-state cost of a live subscription becomes the changed row rather than the row plus the entire membership set. Between checkpoints, the stored position sits behind rows that the client already applied. A reopened stream resumes from that position and redelivers subsequent rows, which the client stored-sequence guard discards as already applied.

The server withholds a cursor only while the rows sent since the last checkpoint weigh less than the cursor itself. Redelivery after a reconnection never costs more than the avoided cursor restatements. A subscription without a window or follow closure has a cursor smaller than any data page, which never meets this threshold. Those streams checkpoint on every frame, and `sparse_cursor` changes nothing for them.

## What a bounded membership costs the server

A window bounds the client, not the server. A window bounds client-side storage, bootstrapping, and materialization. Bounding what the server does per pull requires additional mechanisms. A window and a follow closure represent memberships that cannot be decided one row at a time. Resolving either from scratch costs the size of the set rather than the size of the change. On a shared workspace with frequent writes where pulls rarely sit at the head, full recomputations require scanning the entire working set of every connected client per write.

A pull that can inspect every row written since its cursor maintains both sets incrementally from those rows:

- A window retains its members and the rank position of its boundary row. Determining membership for a changed row takes one comparison. Only a row entering or leaving a full window requires a database read, consisting of a bounded ordered walk on either side of that boundary position rather than a full window scan. A `byRecency` window requires no reads because its ordering matches workspace write sequence; new rows enter at the front and tail rows fall off the ordered list in the cursor.
- A follow closure over a reverse relation (such as every issue in a project) is decidable from the member's own reference field. A changed row settles itself, and only a seed that enters or leaves the base scope requires re-evaluation. This includes a seed that shifted without being written, such as when a window refills.
- A follow closure over a forward relation (such as the project that an issue belongs to) reads in reverse: a target entity is retained because a holder references it, so changes to the target row alone do not determine membership. The server instead executes one probe of the holder reference index for each target whose membership is in doubt. A target is in doubt only when a write affects the holder scope. When no holders change, the pull carries existing targets forward untouched. When a holder changes, the server probes whether an active seed still points to each held target.

Resolving from scratch remains the fallback for cases that incremental maintenance does not support:

- A page that truncates
- A cursor that resumes inside a same-version group
- A subscription set that does not match the cursor
- A window that must locate a tail it never held
- A follow path through multiple segments or deeper than one round
- A forward relation hop whose reference index cannot answer the probe, such as a to-many reference, an unbuilt index, or a target whose fan-in exceeds `MaxIndexScanCandidates`

Incremental maintenance and full resolution produce identical pages. Setting `Limits.DisableMembershipMaintenance` forces the fallback path so this equivalence can be tested.

The first resolution and the fallback path remain unbounded. Computing an `orderBy` window costs what the equivalent `Query` costs: a range read where a [secondary index](../reference/proto-options.md#secondary-indexes) serves the predicate and the ordering, and a full entity scan where none does.

## Coverage: knowing when the answer is partial

Bounded working sets introduce a key constraint: a client holding the top rows by one order cannot correctly answer a local query ordered by a different field. Furthermore, an empty local result for an unsubscribed entity appears identical to an empty entity.

Every result array carries a `coverage` report:

```ts
if (!rows.coverage.complete) {
	// bounds: ('unsubscribed' | 'predicate' | 'window' | 'hydrating' | 'relation')[],
	// plus limit for a window
}
```

The `relation` bound represents cross-entity queries. When a query predicate crosses a reference with `matches()`, the query resolves against target rows in the local store. If a holder references a target that was never synced, the query treats it as a non-match. For more information on clearing this bound, see [relational queries](relational-queries.md).

`coverage.complete` is `true` only when the local store provably holds every row of the entity.

Coverage resolves against the current tab's declared scope rather than the peer-widened union synced by the coordinator. Because the union is always a superset, scope coverage can claim less completeness than what is locally present, preventing coverage from flapping as peer tabs open and close.

`RowSet<T>` extends standard arrays. Methods such as `.map()`, `.length`, spreads, and destructuring behave normally. The `coverage` property is non-enumerable, so it does not appear in `Object.keys()` or `JSON.stringify()`. The `queryServer` function also returns coverage; server queries inspect the entire entity, so results are complete unless a query-level `limit` truncates the page.

The `total` property on a paged result reflects these same bounds. The count includes only the rows visible to the local read. Under a window or predicate scope, `total` reports what the local device holds rather than the workspace total. Coverage metadata clarifies whether that count represents a partial or complete entity.

### Hydrating: the bound that clears itself

A local result can also be partial during initial data loading. A cold client applies its initial snapshot entity by entity, and data is readable as each entity lands. An entity that has not started downloading returns `[]`, which is indistinguishable from an empty collection. The `hydrating` bound represents this state. It is the only transient bound: it clears automatically as the snapshot arrives, and result sets grow without scope changes.

The `hydrating` state combines with scope bounds. For example, a windowed entity in the middle of downloading reports `['window', 'hydrating']` alongside its `limit`, which indicates the target boundary once hydration completes. Only a cold start reports `hydrating`. A re-bootstrap stages rows and swaps them atomically, so queries read the intact previous snapshot until the new snapshot is ready.

Clients can prioritize entity delivery:

- `bootstrapPriority` specifies entities required for the initial render, which the server delivers first. Subscribed clients without priorities receive entities in `subscriptions` order.
- `status().workspaces[id].hydration` exposes workspace-wide hydration progress, including outstanding entities and transferred snapshot percentages, for use in progress indicators.

## Related

- [Sync only part of a workspace](../how-to/partial-sync.md): API instructions for configuring partial sync.
- [Limits and trade-offs](limits-and-trade-offs.md): Details on indexing constraints and query trade-offs.
