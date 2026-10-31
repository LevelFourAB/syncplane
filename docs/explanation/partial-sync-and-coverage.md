# Partial sync and coverage

This document explains how partial sync bounds local working sets. It explains how the server maintains window and relation memberships, and how clients track whether local data provides a complete or partial view of an entity.

## Why windows exist

A window handles cases that a predicate cannot express. A predicate scope, such as "every open to-do", works for small entities but fails for large views. A busy project or an active label frequently matches tens of thousands of rows, and no selective predicate can represent that set.

A window is a rank scope: a `limit` plus an ordering. The `orderBy` or `byRecency` parameter decides which rows win the ranks. The row key breaks ties. With a window, working-set size becomes a policy choice rather than entity size.

## Why key sets exist

A key set handles cases where the user chooses rows directly rather than by row fields. The typical shape is mixed data. A client syncs the metadata of every document through a predicate scope, and it holds the bodies of only the documents it edits or marks for offline use. No document field records which device pinned a row, so no predicate can select those rows.

One subscription per pinned row would sync the same rows, but each would be a predicate that the server must recognize and resolve on every request. An `in()` over the key field is not a shape the server can resolve without walking the entity.

A key set transmits this set as row keys. The server reads every key directly: a bootstrap reads only the named rows, and a change to the set reads only the rows named on either side of the change.

A key set is a claim, not a grant. Every named row must still pass the caller read filter. The server withholds rows that the caller cannot read rather than refusing the request.

## Why holds exist

A key set is declared: the app specifies which rows it names. A hold is the same shape managed at runtime. A row found by a server query, or opened from a link, sits outside every declared subscription. The client cannot edit what it does not hold locally: text handles, watchers, optimistic overlays, and undo all require a local row. A hold makes the row local.

The client keeps one key set per entity for rows currently held. It appends this set to the declared subscriptions as an additional subscription. The server does not distinguish a held row from a declared row. Taking or releasing a hold changes the subscription set. The server reconciles the change with a delta resync that reads the rows named on either side. The server skips any entity whose subscriptions did not change without reading it. A hold on one document costs one point read regardless of the remaining scope size.

Holds share across tabs in the same way as declared subscriptions: each tab publishes its own set and syncs the union. A row that one tab holds stays in scope for every tab. The row remains in scope when that tab closes if another tab holds it. Holds do not persist across reloads because a new page has no components holding rows. A grace period after the last release absorbs view transitions when a view (a component) closes and opens a row again.

A held row does not make its entity complete. Reads report the `predicate` bound, as they do for a declared key set. Under full sync, there is nothing to add. An entity that a declared subscription already covers in full gains nothing from named rows. A hold changes nothing in either case.

## Window maintenance is exact, not best-effort

The cursor records window membership as a key set. Every pull that delivers data updates the window and computes a diff against that set:

- Rows that entered arrive as values.
- Rows that left arrive as tombstones and leave the local store.

This diff includes any row that sank below the rank boundary when no predicate changed and nothing was deleted. Exact window maintenance keeps the local store accurate. Without exact diffing, a client accumulates rows that no longer belong to an active view.

`byRecency` is the cheapest ordering to maintain because ranking by write sequence requires no field comparisons.

## What a bootstrap of a partial scope costs

A cold bootstrap ships one entity at a time. When an entity scope names the rows it admits, the snapshot reads only those rows and costs the number of rows it ships. This shape includes a predicate that a secondary index serves, a predicate that pins every key field to a literal, a key set, a window membership, or a reference-following closure.

Every other shape reads every live row of the entity and keeps the admitted rows. This includes full sync, a scope covering an entire entity, a predicate that neither an index nor a pinned key reaches, and a candidate set larger than `MaxResyncScan`. Both paths ship the same rows in the same order. An interrupted snapshot can resume across either path. Indexing the fields that a subscription filters on ensures that a snapshot of a narrow scope over a large entity costs only the size of the scope.

## Changing the scope resyncs the difference

Changing subscriptions delta-resyncs the visible-set difference rather than forcing a full re-bootstrap. The same applies to permission changes when the policy implements `EpochReadFilters`. `FilterEpoch` versions the inputs behind `ReadFilter`, so a revoked label or a changed membership syncs as a diff.

An epoch resync trusts two values from the client cursor: the epoch and the sequence. The old side reads rows with the filters at that epoch. The diff reads only rows at or below that sequence. If a client lowers the epoch, rows it never held appear visible on the old side. If a client raises the sequence, the diff reaches rows written after its cursor. In both cases, the resync would send tombstones for rows the client cannot read, revealing their keys. To prevent this, the membership entry of each cursor in the cursor-state store holds a random key that never leaves the server. The cursor carries a tag of its sequence and epoch under that key. When the tag does not match, the client resets. A server without a cursor-state store cannot store the key, so an epoch change resets its clients.

An open `Subscribe` stream resyncs in the same manner. A stream opened with a cursor from another subscription set or epoch resyncs that cursor before the scope reports `caught_up`. If an epoch changes while the stream is open, the stream resyncs the scope before sending the next page. Where a `Pull` would reset, the stream drops the scope with reason `RESET`.

The resync pages by emitted differences rather than examined rows. Each `Pull` compares up to `MaxResyncScan` live rows against both the old and new visibility. It returns at most `MaxPullChanges` differences. Rows visible under both scopes emit nothing. A scope change over a large workspace costs round trips in proportion to the difference size rather than workspace size. Lowering `MaxResyncScan` shortens the read transaction held by each `Pull` and increases round trips.

The structure of an entity scope determines how many rows the comparison must read. When both scopes select rows using secondary-index predicates, pinned full row keys, key sets, or window memberships, only the named rows can change visibility. The resync reads those rows and leaves the rest of the entity untouched. All other shapes compare every live row of the entity: a scope covering an entire entity, an entity reached by following references, an unindexed predicate, or a candidate set larger than `MaxResyncScan`. Both paths produce the same difference. Indexing the fields that a subscription filters on ensures that a scope change costs the size of the scope rather than the size of the entity.

The server skips any entity whose subscriptions and read filters remain identical on both sides of the change, and whose window membership has not moved since the cursor. Both sides classify every row identically, so the diff produces no updates.

The old side of a window is the membership the cursor records, not the old window resolved again over current rows. A window moves for rows that nobody wrote: when a member is deleted, the next row rises into its place, and newer rows push the tail out. Resolving the old window again would hide those moves. The client would then miss a row that rose into the window, or keep a row that left it. Changing one entity in a large scope therefore costs only the rows that entity names, avoiding reads across other entities.

Rows held through a `follow` stay outside the comparison. The resync treats every member of the cursor closure as visible under both scopes. A row that leaves the base scope but remains reachable through a reference is kept rather than tombstoned. The pull after the resync walks the closure under the new scope and reconciles it against the set from the cursor: members reached by the new scope arrive, while members no longer reached (and not held by the base scope) drop. That pull walks the closure from scratch, because a set walked from seeds in the old scope cannot be updated from intervening row changes. Changing the follow specification alone does not resync, but the next pull reconciles the closure in the same way. A scope that follows nothing has an empty closure, so removing the follow specification drops every member that the base scope does not hold.

A pull whose cursor already sits at the head of the workspace skips recomputation entirely. Because no rows changed, memberships cannot move, and the server carries the cursor key set forward as issued. Polling an idle workspace costs nothing and returns an empty update.

## The cursor references its membership instead of restating it

Membership sets make cursors large. A window at its limit carries that many row keys, and a reference-following closure can carry far more. Exact diffing requires stored membership sets because a diff requires the previous membership. However, the server does not need to store the membership inside the cursor itself.

With a cursor-state store, the server retains sets under an opaque reference. The cursor on the wire stays a few dozen bytes regardless of client holding size. The server writes a new entry only when membership changes. An idle poll costs one store read and reissues the existing reference.

The store can evict entries. An evicted cursor entry resets the client to a fresh bootstrap. This recovery mechanism matches the process for falling below the tombstone garbage collection (GC) horizon, so eviction must remain rare. When the cursor-state store is unavailable or absent, the cursor inlines the sets directly. Missing indirection increases payload size rather than returning an incorrect response.

## The cursor is a checkpoint on a stream, not a per-frame receipt

A server running without a cursor-state store produces large cursors. On `Subscribe`, the server tracks the scope position, and the frame cursor exists only so the client can resume later.

When a client sets `sparse_cursor`, the server sends the cursor as a periodic checkpoint instead of on every frame. The steady-state cost of a live subscription becomes the changed row rather than the row plus the full membership set. Between checkpoints, the stored cursor sits behind rows that the client already applied. The client therefore records two sequences: the sequence its rows reach, and the sequence its cursor encodes. A pull or a reopened stream resumes from the cursor. Its pages can report a sequence below the one the rows reach: a resync page reports the cursor's own sequence, and a page that ends inside a large transaction reports the sequence of that transaction. The client applies such a page and stores its cursor, because no other page moves the cursor forward. It keeps each stored row whose version is higher than the version on the page. Only a page below the sequence of the stored cursor is stale, and the client drops it.

The server withholds a cursor only while rows sent since the last checkpoint weigh less than the cursor itself. Redelivery after reconnection never costs more than the avoided cursor restatements. Any subscription can meet that threshold. A frame that carries one small row weighs less than most cursors, and this includes the fixed-size reference that a cursor-state store issues. A cursor that inlines a window or a follow closure meets the threshold most often.

## What a bounded membership costs the server

A window bounds client-side storage, bootstrapping, and materialization, not the server. Bounding server work per pull requires additional mechanisms. A window and a follow closure represent memberships that the server cannot decide one row at a time. Resolving either from scratch costs the size of the set rather than the size of the change. On a shared workspace with frequent writes, full recomputations would scan the entire working set of every connected client on each write.

A pull that can inspect every row written since its cursor maintains both sets incrementally:

- A window retains its members and the rank position of its boundary row. Determining membership for a changed row takes one comparison. Only a row entering or leaving a full window requires a database read. This read performs a bounded ordered walk on either side of the boundary position instead of a full window scan. A `byRecency` window requires no reads because its ordering matches the workspace write sequence. New rows enter at the front, and tail rows fall off the ordered list in the cursor.
- A follow closure over a reverse relation (such as every issue in a project) is decidable from the member reference field. A changed row settles itself. Only a seed that enters or leaves the base scope requires re-evaluation. This includes a seed that shifted without being written, such as when a window refills.
- A follow closure over a forward relation (such as the project that an issue belongs to) reads in reverse. A target entity is retained because a holder references it, so changes to the target row alone do not determine membership. The server executes one probe of the holder reference index for each target whose membership is in doubt. A target is in doubt only when a write affects the holder scope. When no holders change, the pull carries existing targets forward untouched. When a holder changes, the server probes whether an active seed still points to each held target.

Resolving from scratch remains the fallback when incremental maintenance is not supported:

- A page truncates
- A cursor resumes inside a same-version group
- A subscription set does not match the cursor
- A window must locate a tail it never held
- A follow path traverses multiple segments or runs deeper than one round
- A forward relation hop cannot be answered by the reference index, such as a to-many reference, an unbuilt index, or a target whose fan-in exceeds `MaxIndexScanCandidates`

A page that truncates defers the membership diff to the page that ends the catch-up at a clean version boundary. Each page classifies rows against sets it resolves for itself, so a page can deliver a row as a member that the cursor it read does not name. Its cursor then records the prior set plus those rows. If the membership moves again before the clean boundary, the diff there removes a row that was a member only on a middle page. Such a set is not the membership of any one window or walk, so the next page resolves the sets from scratch.

Incremental maintenance and full resolution produce identical pages. Setting `Limits.DisableMembershipMaintenance` forces the fallback path so this equivalence can be tested.

Initial resolution and the fallback path remain unbounded. Computing an `orderBy` window costs what the equivalent `Query` costs: a range read when a [secondary index](../reference/proto-options.md#secondary-indexes) serves the predicate and the ordering, or a full entity scan when no index serves them.

## Coverage: knowing when the answer is partial

Bounded working sets introduce a constraint: a client holding top rows by one order cannot correctly answer a local query ordered by a different field. In addition, an empty local result for an unsubscribed entity appears identical to an empty entity.

Every result array carries a `coverage` report:

```ts
if (!rows.coverage.complete) {
	// bounds: ('unsubscribed' | 'predicate' | 'window' | 'hydrating' | 'relation')[],
	// plus limit for a window
}
```

The `relation` bound represents cross-entity queries. When a query predicate crosses a reference with `matches()`, the query resolves against target rows in the local store. If a holder references a target that was never synced, the query treats it as a non-match. For details on clearing this bound, see [relational queries](relational-queries.md).

`coverage.complete` is `true` only when the local store provably holds every row of the entity.

Coverage resolves against the declared scope of the current tab rather than the union synced across peer tabs. Because the union is always a superset, scope coverage can claim less completeness than what is locally present. This prevents coverage from fluctuating as peer tabs open and close.

`RowSet<T>` extends standard arrays. Methods such as `.map()`, `.length`, spreads, and destructuring behave normally. The `coverage` property is non-enumerable, so it does not appear in `Object.keys()` or `JSON.stringify()`. The `queryServer` function also returns coverage. Server queries inspect the entire entity, so results are complete unless a query-level `limit` truncates the page.

The `total` property on a paged result reflects these same bounds. The count includes only the rows visible to the local read. Under a window or predicate scope, `total` reports what the local device holds rather than the workspace total. Coverage metadata clarifies whether that count represents a partial or complete entity.

### Hydrating: the bound that clears itself

A local result can also be partial during initial data loading. A cold client applies its initial snapshot entity by entity. Data is readable as each entity lands. An entity that has not started downloading returns `[]`, which is indistinguishable from an empty collection. The `hydrating` bound represents this state. It is the only transient bound: it clears automatically as the snapshot arrives, and result sets grow without scope changes.

The `hydrating` state combines with scope bounds. For example, a windowed entity during download reports `['window', 'hydrating']` alongside its `limit`. This value indicates the target boundary once hydration completes. Only a cold start reports `hydrating`. A re-bootstrap stages rows and swaps them atomically, so queries read the previous snapshot until the new snapshot is ready.

Clients can prioritize and monitor entity delivery:

- `bootstrapPriority` specifies entities required for the initial render, which the server delivers first. Subscribed clients without priorities receive entities in `subscriptions` order.
- `status().workspaces[id].hydration` exposes workspace hydration progress, including outstanding entities and transferred snapshot percentages, for use in progress indicators.

## Related

- [Sync only part of a workspace](../how-to/partial-sync.md): API instructions for configuring partial sync.
- [Limits and trade-offs](limits-and-trade-offs.md): Details on indexing constraints and query trade-offs.
