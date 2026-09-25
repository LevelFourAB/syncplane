# Limits and trade-offs

This document explains what the engine deliberately does not do, why these boundaries exist, and the trade-offs you make when designing schemas and applications around it. Each limit is a consequence of [the convergence model](convergence-model.md), not an oversight.

## An unindexed query scans everything

Without an index that serves it, a query scans and decodes every live row of the entity. This applies to `client.query` over synced rows, as well as `queryServer`, the `Query` RPC, and `ViewTx.Query` over stored rows. Where a declared [secondary index](../reference/proto-options.md#secondary-indexes) serves the predicate and the ordering, the read is a range scan over that index. Without an index, every live row is read and decoded, making the query $O(\text{entity})$ regardless of predicate selectivity or limit size. This scan is suitable for a background job, but consider the performance impact before using it in a hot handler over a large entity.

A local scan narrows what each row costs, even though it cannot narrow how many it reads. The client decodes a candidate row through a projection of the entity: a descriptor holding only the fields the query's predicate, ordering and facets read, with every other field skipped in the wire format rather than decoded into an object. The rows that make the page are then decoded whole from the same bytes, so an app still receives complete rows. This applies to a query with a `limit` whose paths the client can enumerate, and to a grouped read (`groupCount`, `distinct`, `groupRows`) whatever its limit, because a grouped limit counts groups rather than rows. A full-text `match` or a relevance ordering reads the entity's declared search fields rather than a named path, so such a query carries those fields in its projection as well; an entity declaring none decodes whole rows. The server decodes whole rows in every case.

Only memory is bounded to the requested page size; the underlying scan work is unbounded. A selective predicate performs better than deep pagination, and querying a large entity behind a frequent endpoint degrades performance.

A composite index reduces query time to $O(\text{limit})$. Without a composite index, a query filtering on `state` and ordering by `rank` must scan the entire matching range to sort the results.

Unlike the RPC, a `Spec` without a `Limit` holds every match in memory. Inside `EntityTx`, the scan also runs while the workspace write lock is held.

### Unsupported index shapes

Secondary indexes do not support the following query shapes:

| Shape                     | Reason                                                                                 |
| ------------------------- | -------------------------------------------------------------------------------------- |
| `contains` / `endsWith`   | Neither operator is a prefix. Use [Search](search.md) for full-text search.            |
| `or`                      | Requires merging multiple ranges and falls back to a scan.                             |
| Collaborative-text fields | Writes would rewrite index entries on every keystroke. Rejected during registration.   |
| Maps or message lists     | Queries cannot compare nested structures. Index a scalar field on the element instead. |
| Ordering by a list        | List elements have multiple values, so rows lack a single sort position.               |

### Index lifecycle and write costs

- **Index availability:** A newly declared index is not available immediately. A background process indexes existing records first. Queries perform table scans until indexing finishes. On the client, the `indexing` event fires after bootstrap when indexing completes (see [Telemetry](../reference/telemetry.md#local-reads)).
- **Query results:** Using an index produces the same results as a full scan.
- **Local queries:** Local indexes cover confirmed rows for workspace-scoped queries. The engine merges the optimistic overlay over the range.
- **Write costs:** Each write updates the row's position in all defined indexes on both client and server. For repeated fields, the engine creates one entry per element, bounded by `MaxIndexEntriesPerRow`. Writes exceeding this limit fail.
- **Diagnostics:** The `onSyncEvent` callback reports a `query` event with the execution `path` (`index` or `scan`), the active index name, and any decline reason. For details on decline reasons, see [Telemetry](../reference/telemetry.md#local-reads).

## A filter can cross a reference, a sort cannot

The `matches()` operator is the only condition that leaves the row through a single hop across a forward reference. It remains fast because downstream components do not process the cross-row reference directly. A pre-pass evaluates the inner condition over the target entity, collapses the node into a membership test on the stored stub, and passes an ordinary row-local query to the planner, index walk, and cursors. For the full model, see [Relational queries](relational-queries.md).

The `orderBy` operator has no equivalent cross-row optimization because a sort key cannot fold away into a stub check. The sort value lives in another row, and index keys do not span across rows. Answering a cross-row sort requires sorting the entire match set: scanning and decoding the outer entity, looking up every distinct target row, repeating this lookup on the next page, and running it again to compute `total`.

Partial sync highlights this difference:

- When a predicate cannot access its target, the row is excluded and coverage reports the `relation` bound, providing a failure you can act on.
- When a sort key cannot access its target, the missing value causes the row to sink to the end of the result. Under a `limit`, the end of the result falls off the page, returning an incomplete result without indicating that it is truncated.

Cross-reference sorting is therefore limited to two supported patterns:

- For an ordered slice per holder (for example, the five newest comments on each issue), `include` accepts `$where`, `$orderBy`, and `$limit`, which apply to each holder during hydration.
- For a table sorted by a related column, hydrate and sort the data in your application. Those rows are already loaded on the client, so re-sorting them in the application avoids unnecessary server overhead.

## A reverse relation is named by its field alone

A reverse relation name is part of the wire contract. A `follow` segment carries it to the server, and a deployed client sends the names it was built with until it is updated. If a name came from the whole schema, for example with the field name added only when two references reach the same target, a new reference would rename an existing relation. Clients that still send the old name would then fail, and an additive schema change would break them.

For this reason, the name comes from the reference field alone: its `reverse_relation`, else `<plural(holder)>By<Field>`. Field names are unique within an entity, so two default names on one target cannot be the same, and a new reference needs no decision. The cost is length: `project.refs.issuesByProject` where one reference would permit `issues`. A shorter name is one `reverse_relation`, set when the field is created and before any client depends on it. For the rules, see [Reverse relations](../reference/proto-options.md#reverse-relations).

A polymorphic reference has one reverse relation per case, not one per relation. Each case has its own derived index, so a reverse hop is one index probe. A relation-wide reverse relation would have to merge the probes of all cases that reach one target, and a client that needs that union can follow two segments. A member of a oneof is its own field, so it takes its own name. A reference message field gives one name to all its cases, which usually land on different targets. Two cases of one reference message that point at the same entity therefore clash, and need fields of their own.

## Cursors page, offset jumps to a rank

Paging uses `after` and `before` cursors by default. A cursor represents a position in the ordering, so the index walk seeks directly to that position and stops once the page is full (`O(limit)` rather than `O(offset + limit)` on an indexed query). This guarantees that the page remains stable.

An offset counts rows instead of identifying a position. If a row is inserted before the current page between two requests, every subsequent rank shifts: the next page repeats a row you already saw, and a deletion causes the next page to skip a row. Neither issue produces an error that you can detect. A cursor is a value in the ordering, so insertions and deletions elsewhere do not shift its position.

Cursors cannot jump to an arbitrary rank (such as row 8000). A count cannot be expressed in a page boundary. Features such as dragging a scrollbar into the middle of a large list require counts, which is why `offset` exists and why it is capped. You cannot combine `offset` and cursors in a single request because a rank and a position have no shared resolution; combining them returns an error.

Every query result reports `total`, representing the number of matches. On an indexed query with a `limit`, computing `total` is not free locally: the index walk stops reading rows once the page is full, but it must still scan the remaining index range to count matches. Counting consumes index positions rather than row reads, unless an unexpressed predicate remains. In that case, remaining rows in the range must be read and decoded to determine matches.

`watchQuery` maintains query results incrementally instead of recomputing them. Each watcher holds matching rows in result order. When a commit arrives with modified rows, each row is spliced in or out using binary search. A commit costs work proportional to the rows it changed, not the total size of the entity. If a commit does not affect the query's match set, it costs nothing. The initial read uses an index when one covers the query, similar to `client.query`. Because `watchQuery` requests the full match set rather than a single page, a very large match set can exceed the index walk's budget and fall back to a full scan.

Two situations require scanning the entire entity:

- A commit that reports an entity without specifying its rows (such as a bootstrap swap or a confirmed-state wipe) forces a full rescan. A rebase names the rows it clears and replays, so an ordinary pull costs each watcher only the rows that moved.
- Memory usage scales with the total match set. A query without a predicate holds an entry for every row in the entity, creating a large working set. A `limit` bounds the delivered results, not the rows held in memory.

Holding rows beyond the active page allows a deletion in a full page to pull in the next row without a storage read, makes `total` free on subsequent updates, and makes `setWindow` an in-memory slice. This lets virtualized lists scroll large result sets without accessing the store, at the cost of keeping the result set in memory.

`client.query` is served from an index when one covers it. A workspace-scoped query whose predicate and ordering can be expressed as a range reads that range and merges the optimistic overlay instead of scanning the entity.

A top-level `or` condition is served as an index union: each branch is planned independently and results are deduplicated by row key, provided that every branch has an index. If any branch lacks an index, the entire query falls back to a scan, because scanning for a single branch in a union adds overhead without benefit. An index union returns unordered results, so an ordered `or` query sorts its matches in memory rather than reading in index order.

To serve a cross-workspace query, the client runs one index walk per workspace and merges the results in memory. A single index range cannot cover several workspaces because the index prefix nests the workspace identifier. The merge orders results by the ordering keys, then by row key, then by workspace, which matches a full scan. Each workspace walk reads all its matching rows because `limit` applies to the merged result. A read that names no workspaces first finds the workspaces holding rows with one key lookup each. If even one workspace is still building its index, the whole read falls back to a scan.

Two server-side traversal paths do not use query planning:

- A reverse follow hop (such as "every issue whose project is this one") uses an engine-derived index for reference fields, running in `O(matches)` time for to-one references. For to-many references, the hop scans the source entity and re-derives each stub's key because probing a single target requires multi-entry reads that the reverse path does not perform. Both reference types write index entries, incurring index write costs.
- The seeds of a following subscription (the base-scope rows from which its closure is walked) are named the way a bootstrap names the rows it ships: from a secondary index over the predicate, or from the single row key a predicate pinning every key field to a literal derives. A follow anchored on a predicate therefore costs `O(matches)` to anchor, and one anchored on the entity key needs no index at all. With no predicate, the seeds consist of the entire entity and require a full scan, as does a candidate set larger than `MaxResyncScan`. A windowed subscription seeds from its window, which is already resolved when the walk starts.
- An unfiltered `by_recency` window reads its `limit` newest rows and stops. With a predicate or a read filter, it continues reading until the limit is satisfied or the ordering ends.

Both paths fall back to a scan when an index is not yet built. A new or modified index remains unusable until a background pass completes the entity walk. This fallback is intentional: index use is strictly an optimization, and indexed and scanning paths produce identical results. `Limits.DisableIndexes` forces this fallback during testing.

These costs represent resolving a window or closure from scratch, which occurs only when a pull cannot maintain existing cursor state; see [Partial sync and coverage](partial-sync-and-coverage.md#what-a-bounded-membership-costs-the-server). Index maintenance is tested similarly: `Limits.DisableMembershipMaintenance` forces a full rebuild, and both paths must produce identical pages.

Index maintenance costs apply to writes on both the client and the server. A write re-encodes and stores the row's complete entry set instead of computing a diff. Neither side keeps the previous row value in memory during a `PATCH` or `DELETE`, so computing a diff would add a read and decode operation to every write.

`PUT` and `PATCH` differ in how they update indexes:

- A `PUT` replaces the entire row, always rewriting the complete entry set.
- A `PATCH` includes a field mask. If the mask contains only fields that no index reads, the index rewrite is skipped, matching the write cost of an unindexed entity. This check performs a path comparison against compiled components. Masks that cannot be verified (such as those addressing a oneof group or a single collection item) trigger a full rewrite. The TypeScript client's `update()` method derives its mask from a diff, ensuring that edits to unindexed fields do not incur index maintenance costs.

An index over a repeated field or map entry causes index entry counts to scale with data size rather than schema size. `MaxIndexEntriesPerRow` (default `1024`) sets the ceiling on index positions per row. Writes exceeding this limit fail immediately, preventing partial indexes that would disagree with full scans.

### What an EAV index over user-defined fields costs

An index over `custom.*` stores one entry per map entry, so write costs scale with the number of custom fields a row uses, not the total defined in the application schema. A schema defining 200 custom fields adds no overhead to a row that populates only three.

Write costs grow linearly with the number of entries per row and require deleting and re-inserting the row's entire entry set. Three factors determine this impact:

- **Entries per row:** Each repeated or multi-select entry multiplies index entries. `MaxIndexEntriesPerRow` caps this total; exceeding it causes the write to fail.
- **Indexed field modifications:** Writes that do not modify indexed fields avoid index rewrites when using a `PATCH` request with a precise field mask (as generated by `update()`).
- **Storage latency:** Index entry writes scale with underlying storage performance. Test performance on representative hardware rather than relying on local benchmarks.

#### One index or two

A map index declares the layout of its components:

| Layout         | Serves                                                                      |
| -------------- | --------------------------------------------------------------------------- |
| `[key, value]` | Equality, ranges, and ordering within one named field                       |
| `[value, key]` | Equality only, across keys or for a named key (pin the value, then the key) |

The `[value, key]` layout serves all equality checks, answering both whether a field contains a value and which field contains a value. If your application only requires equality queries on custom fields, declare this layout alone. Add `[key, value]` only when you need range filters (`priority > 3`) or sorting on a specific field.

Declaring both index layouts has the following costs:

- Entries per row double. A row with $N$ populated custom fields occupies $2N$ index entries, counting against `MaxIndexEntriesPerRow`.
- `PUT` costs increase linearly with total entry count.
- `PATCH` operations that do not touch indexed fields pay no additional cost under either layout.
- Equality query latency remains unchanged because both layouts pin both components.

A map index avoids full entity scans for supported queries. The engine includes benchmarks in `engine/server/index_bench_test.go` (`BenchmarkIndexedWrite/eav` and `BenchmarkCustomFieldQuery`) to measure write and query latency on target hardware:

```sh
go test ./engine/server/... -bench 'BenchmarkIndexedWrite/eav' -run '^$'
go test ./engine/server/... -bench BenchmarkCustomFieldQuery -run '^$'
```

#### Deployment notes

- **Avoid duplicate index declarations:** Declaring an index on a field with `indexed: true` that is also part of a compound index duplicates entries. Choose one layout unless both query shapes are required.
- **PostgreSQL autovacuum tuning:** In PostgreSQL, replacing index entries generates dead tuples on every update. For workloads with high custom field write rates, tune `autovacuum_vacuum_scale_factor` specifically for the `index_entries` table.

The client indexes confirmed rows only. The optimistic overlay is bounded by the pending queue, so queries scan and merge the overlay with confirmed rows. This keeps results consistent without maintaining index entries for optimistic state that may be discarded on the next pull.

Windowed subscriptions limit client memory consumption, but do not reduce the server-side scan work required to resolve the window.

Because query ordering must produce consistent results across scans and indexes, the engine enforces a total order over all values:

- Values rank in the order `real < NaN < missing`.
- This rank is evaluated before applying the sort direction, so `NaN` and missing values sort to the end under both `asc` and `desc` orderings.
- `NaN` values do not match comparison operators: `gt`, `gte`, `lt`, `lte`, and `eq` all return false when comparing against `NaN`.

## PUT replaces the whole row

`PUT` requests and `EntityTx.Put` replace the entire row, with the exception of collaborative-text fields, which merge with the stored document on every `PUT` path.

If an older client or server binary deserializes data into an outdated message definition, a `PUT` drops unknown fields. Use `PATCH` with a field mask for partial updates, and treat schema evolution as additive.

The [schema-revision handshake](../how-to/handle-failures.md#steps) detects version skew during rolling deployments, but does not replace the need for additive schema changes.

## The default posture is open

The `workspace_id` identifier is self-asserted, and `CanAccessWorkspace` defines the authorization boundary. The engine checks this hook before executing workspace-scoped operations, but the default implementation (`AllowAll`) permits all requests. A shared deployment that does not override `CanAccessWorkspace` provides no cross-tenant isolation.

The `client_id` parameter is an unauthenticated idempotency handle, not an identity. Do not base authorization decisions on `client_id`. Identity is determined by the `Principal` produced by an `Authenticator`.

Active streams re-check access permissions periodically, but do not re-verify credentials:

- `Subscribe` re-evaluates `CanAccessWorkspace` for open scopes on each heartbeat (default 30 seconds). A user whose access is revoked is disconnected within one heartbeat interval.
- Authentication occurs only when opening the stream. If credentials expire while a stream is open, the connection remains active until closed. Limit stream lifetimes if strict credential expiration is required.

The engine limits request costs through `Limits`, but does not rate-limit request frequency or cap concurrent streams per caller. Rate limiting and connection limits must be implemented in the HTTP reverse proxy or gateway in front of the engine handlers. The ephemeral plane follows the same model; see [Rooms](rooms.md#what-is-not-guaranteed).

## Revocation stops future reads; erasing past ones is opt-in

When access is revoked permanently, the client receives `workspace_access_revoked` and stops syncing the workspace. Synced rows remain in client-side IndexedDB storage until the application calls `removeWorkspace(id, { purge: true })`. The engine client does not delete local user data automatically based on a server verdict.

Purging a workspace also removes pending mutations. Because these mutations cannot be acknowledged by the server, `removeWorkspace` returns them (and forwards them to `onMutationRejected`) so the application can persist them if needed.

## A read filter hides values, not existence

A `ReadFilter` controls which rows a client can read, but does not hide the existence of row keys. Two mechanisms disclose that a denied row exists if the client's subscription covers that entity:

- **Deletions:** A tombstone containing the entity name, key, and version is sent to all subscribed clients when a row is deleted. Because the value is removed, the read filter cannot be evaluated against it.
- **Revocation:** If an update modifies a row such that it no longer passes the read filter, the server sends a tombstone to ensure clients drop their cached copy. If the server cannot determine whether a client previously held the row, it sends a tombstone conservatively. Bootstrap sync skips denied rows entirely, and permission-change resync evaluates the client's prior filter to tombstone only previously visible rows.

Because row keys are visible in tombstones, keys must not contain sensitive data such as email addresses, slugs, or invite codes. Use surrogate keys (such as UUIDs) and store sensitive data in entity fields protected by the read filter.

A timing channel also exists: versions are workspace-global, allowing clients to infer that hidden writes occurred by observing version gaps, though without disclosing affected rows or field values.

## One node by default, more than one on purpose

The engine defaults to a single-node setup using SQLite and an in-process notifier, requiring no external infrastructure. Both components can be replaced with PostgreSQL adapters when scaling:

- **Notifier:** The default notifier operates in-process and wakes `Subscribe` streams on the local node. Clients on other nodes receive updates through periodic polling. `WithNotifier` configures a shared notifier, such as `pgnotify` using PostgreSQL `LISTEN`/`NOTIFY`. Because streams scan the shared store directly, notification events carry only sequence progression signals without row payloads, allowing best-effort delivery without compromising correctness.
- **Storage concurrency:** The SQLite adapter serializes writes across all workspaces using a single-writer model. The `pgstore` adapter supports concurrent writes across different workspaces and allows multiple server nodes to share the same database.
- **Background work deduplication:** Garbage collection, index builds, text compaction, and effect draining use per-workspace advisory leases (`pgstore` uses PostgreSQL advisory locks). These leases distribute background tasks across nodes without distributed fencing tokens. If a lease expires or a connection drops, tasks continue; duplicate executions remain safe because background operations are idempotent.

For single-node capacity guidelines and multi-node setup, see [Scale envelope](#scale-envelope) and [Run more than one node](../how-to/run-multiple-nodes.md).

## Scale envelope

One process, one SQLite file (a single writer across every app on the host), and an in-process change notifier: apps do not contend for anything but that writer, and a workspace write path is short. This configuration is sized for internal tools with tens of apps and human-paced write rates.

This setup stops fitting when sustained writes saturate the single writer, or when you need more than one node. You can migrate to [`pgstore`](../reference/storage-adapter.md#adapters) plus a shared notifier with `WithNotifier`. (Clients on other nodes otherwise converge by polling, correctly but with poll latency.) Nothing above the storage seam changes when that happens; `Manager`, app partitioning, and authentication wiring are unaffected. See [Run more than one node](../how-to/run-multiple-nodes.md).

## `app_id` is a coarser partition than `workspace_id`

When using a `Manager`, each application runs as an isolated `*Server` instance with its own prefix of the workspace-ID space and its own entity catalog. The `workspace_id` parameter partitions data within an individual application. Two applications can declare identical entity schemas without colliding.

Application identity is used for routing and storage segregation only; it is not encoded in protobuf package names. Application registration (`RegisterAuthorizer`) is trusted.

## A stored row that will not decode goes invisible

Write operations reject bytes that do not decode according to the entity schema. Undecodable rows result from external causes, such as older binaries, bugs in trusted writers, or database corruption.

When a read operation must inspect an undecodable row (for a read filter, subscription predicate, query, window, or follow hop), the server quarantines the row rather than failing the entire request:

- The server logs a warning (`undecodable row quarantined`) containing the entity, key, and version, and treats the row as absent.
- Pull operations send a tombstone for quarantined rows so clients drop their cached copies.
- Bootstrap sync, queries, windows, and follow closures skip quarantined rows, allowing the rest of the workspace to sync normally.

Quarantined rows are not deleted from storage:

- A `PUT` request with valid data replaces the corrupt bytes and restores normal sync.
- A `PATCH` request is rejected with a `conflict` error because a field mask cannot be applied to undecodable base bytes.

Full sync operations without read filters or predicates transmit raw bytes without decoding them, allowing corrupt rows to reach clients where decoding errors surface on read. In-process methods (`ViewTx`, `ApplyTx`, and `EntityTx`) return decode errors directly to Go callers rather than quarantining rows.

## Field history records editing sessions, not keystrokes

Entities configured with [`history: {}`](../reference/proto-options.md#field-history) log prior field values before each modification. This log balances audit detail against storage growth:

- **Changes, not duplicate writes:** The server compares the incoming row with the stored row and records only modified fields. A write that writes unchanged values produces no history entry.
- **Editing sessions, not individual keystrokes:** Consecutive writes to a row by the same actor within an active session collapse into a single history record opened by the initial write. This prevents rapid typing from generating hundreds of redundant snapshots. To capture individual keystrokes, use collaborative text delta logging, which uses shorter retention windows.
- **Plain text representation:** Collaborative text fields are stored in history as plain text rather than CRDT document structures. This avoids storing version vectors and tombstoned deletions in history records while ensuring clean restores.
- **Explicit field capture:** Schema authors must explicitly enable history per field. Capturing all fields by default would unintentionally log large documents when adding new fields.

History retention defaults to 90 days. Retaining history indefinitely with [`HistoryForever`](../reference/server.md#history-that-lives-as-long-as-its-row) must be configured intentionally.

## Field history is a restore mechanism, not an audit log

Field history records prior states for restoration; it does not serve as an immutable audit log or row deletion log:

- A `DELETE` operation is not a field modification and is not captured in history. Deleting a row purges its history records within the same transaction.
- Restoring a row applies a `PATCH`. A `PATCH` on a tombstoned row produces a conflict under delete-wins rules, making history records unusable for restoring deleted rows.
- If a new row reuses a previously deleted row's key, retaining prior history would incorrectly link old records to the new entity.
- Field history records only changed fields before each update, not complete snapshots of the row at deletion time.

Restoring a field is a `patch` operation using the recorded mask, rather than a `put` of the decoded prior value. The recorded prior contains only the fields specified by the mask (in addition to the row's key fields). A `put` operation replaces entire rows and clears unrecorded fields.

Collaborative text fields do not require special handling during restore. A record stores the text value rather than the full document. On the write path, the engine diffs the text against the stored document and applies the changes as real edits. Restored text is inserted as new edits, untouched regions retain their items, and concurrent edits merge without data loss. For more information, see [Collaborative text](collaborative-text.md).

Formatting marks do not survive restoration. Because restored text is re-inserted, formatting applied to that specific span is removed, while formatting on untouched regions remains.

A restore operation is an optimistic mutation: it is queueable offline, undoable, and recorded in history like any other write.

To track deletions and audit events:

- Use a soft-delete field (such as `deleted_at`) updated via `PATCH`. This captures the timestamp and modifying actor in field history and allows restoration by clearing the field.
- Configure [`DELETE_POLICY_TRUSTED_ONLY`](../reference/proto-options.md#write-policies) to reserve hard `DELETE` operations for permanent data erasure.
- For immutable audit trails, write dedicated event log rows; see [Change feed](effects-and-the-change-feed.md#state-reporting-versus-event-logging) and [Recover a deleted row](../how-to/recover-a-deleted-row.md).

## Soft delete trade-offs

A `DELETE` leaves a tombstone and removes the row's data. A restore cannot bring it back: a restore is a `PATCH`, and a `PATCH` on a tombstoned row causes a conflict under delete-wins.

[Field history](../how-to/restore-a-field.md) also does not close this gap. It records what marked fields held before a write, not the row's final state, and a `DELETE` purges the row's history in the same transaction.

A soft delete keeps the row. Mechanisms that work on live rows — history, restore, permissions, and queries — continue to work.

Soft deletes carry the following trade-offs:

- **Removed rows still sync:** Soft-deleted rows count against bootstrap size and partial-sync windows until purged. Use subscription predicates before deleted data outgrows the working set.
- **Every read must filter:** A query that omits `deletedAtMs.eq(0n)` returns removed rows. A `ReadFilter` enforces filtering on the server, but hides the rows from the restore path as well.
- **Erasure requires a job:** Soft-deleted rows are not automatically collected. Configure retention and execute trusted deletes to purge expired rows.

## Write policies classify a write, not its effect

Write policies (`create_policy`, `update_policy`, and `delete_policy`) evaluate mutation syntax and version constraints before opening a storage transaction, without reading the underlying row.

Because policy checks are syntactic rather than semantic:

- An unguarded `PUT` (without `expected_version`) can either create or replace a row. To prevent unauthorized creations when updates are permitted, unguarded `PUT` requests must satisfy both `create_policy` and `update_policy`.
- Inside a batch transaction, `DELETE(k)` followed by `PUT(k, expected_version: 0)` evaluates as a valid delete and create, replacing the row. Use `delete_policy` in combination with `update_policy` to restrict row modifications.
- Policies evaluate caller roles, not row-level ownership rules. Ownership rules (such as "only the author may edit this comment") must be enforced in `CanMutate` (if derivable from the key) or inside custom mutator logic that inspects the stored row.
- Undo operations convert a `DELETE` into an unguarded `PUT`, which fails if the entity restricts updates.
- Collaborative text edits execute as `PATCH` operations and require update permissions on the entity.

## Custom mergers must satisfy the contract

Fields marked `opaque: true` are replaced entirely during merges unless a custom merger is registered (`WithMerger` on the server and `MergerSpec` on the client).

Custom mergers must be idempotent, commutative, and associative. The merger executes during server apply, optimistic overlay merges, and rebase replays. Non-idempotent mergers (such as string concatenation) cause data corruption.

For collaborative prose, use `text: true`, which uses a built-in union merger that satisfies these algebraic properties automatically.

## Custom-mutation mirrors are best-effort

TypeScript mirrors of Go mutators provide immediate optimistic UI updates. Client and server outputs may diverge temporarily and reconcile upon rebase. Never treat mirror output as authoritative state.

If a mirror throws an exception, the local write fails. If an exception occurs during a rebase replay, only the optimistic overlay is discarded; the mutation remains queued and is processed authoritatively by the server.

## A queued mutation a tab cannot replay loses its optimism, not the write

The pending mutation queue is shared across browser tabs within a workspace. If a tab encounters a queued mutation for an entity schema it does not declare (see [The convergence model](convergence-model.md#a-client-holds-only-the-entities-it-declares)), it logs an error via `console.error` and skips optimistic replay for that mutation.

Skipping replay prevents workspace stalls during pulls, rollbacks, and bootstrap operations. The mutation remains in the queue until the push handler acknowledges, rejects, or dead-letters it (after `poisonThreshold` consecutive failures). The tab displays confirmed server state until the mutation resolves.

## A local write is not pushed immediately

Client writes wait for a debounce interval (`pushDebounceMs`, default 50ms; capped by `pushMaxDelayMs`, default 250ms) before pushing to the server. Rapid consecutive edits reset the debounce timer up to the maximum delay ceiling.

This delay allows the client to compact the pending queue, folding bursts of edits (such as typing or drag operations) into a single `PATCH` mutation.

Debouncing delays when peers receive updates by up to the maximum delay plus network latency. It does not risk data loss: mutations are committed to local storage and the pending queue immediately before the debounce timer starts. If a browser tab closes before the timer expires, pending mutations push when the tab restarts. Inbound sync operations, reconnects, and scope changes trigger immediate sync cycles. To push immediately without waiting for debouncing (such as on form submission or blur events), call `syncNow()`.

## Deleting idle client watermarks can apply a resend twice

The server keeps one small record per `(workspace, client_id)`: the highest mutation ID it has applied or rejected for that client. This watermark is the only thing that makes a resend idempotent. A push whose response is lost stays in the client's durable pending queue, and the client resends it on its next sync cycle, which can be weeks later if the device stays offline. The server cannot tell such a resend from new work: a resend carries no marker, and the client cannot add one, because it does not know whether the original push reached the server.

Garbage collection therefore keeps watermark records by default, and the tombstone retention only clears the rejection verdicts stored next to them. The clients table then grows with the number of distinct client IDs that have ever pushed to a workspace. Each client ID is minted once per local database, so this is bounded by installs and sign-outs rather than by traffic.

`WithClientRetention(d)` opts into deleting the records of clients that have not pushed for `d`. A client whose record is gone is re-admitted at whatever mutation ID it pushes next. If that push is a resend, every mutation in it applies a second time:

- A `PUT` or `DELETE` overwrites what other clients wrote in the meantime, because the server applies mutations in arrival order (last writer wins).
- A batch or custom mutator runs twice.
- Nothing reports it. The client receives `APPLIED`, and the rows look like ordinary writes.

Set `d` well past the longest time a client can stay away and still hold unpushed work, and prefer leaving the option unset unless the clients table is a measured problem.

## Wire cursors are opaque

Wire cursors are opaque tokens. Clients must not parse or interpret cursor structure.

## Browser connection limits

Web browsers limit concurrent HTTP/1.1 connections to approximately six per host. Client connections scale with the number of planes in use rather than the number of active workspaces:

- `Subscribe` multiplexes workspace data streams over a single connection.
- Applications using [Rooms](rooms.md) open a second persistent stream per account (`Watch`) because room navigation changes frequently and would otherwise force data stream reconnects.

Each account uses up to two persistent connections (one per plane). If multiple `SyncClient` instances run under different accounts on the same page, connection limits are reached at approximately three accounts.

To mitigate connection limits, consider the following options in order of preference:

1. Serve HTTP/2 (`h2c` or HTTPS), which removes connection concurrency caps.
2. Set `presenceEnabled: false` on clients that do not display presence, replacing the persistent watch stream with unary keepalive requests.
3. Reduce the number of concurrent client instances per page.

## The option field numbers are unregistered

Custom protobuf option field numbers (`50100` and `50101`) use numbers in the private organization range. Register official option numbers in the global protobuf extension registry before publishing schemas publicly. Changing option numbers after publication requires breaking changes across generated client code. For details, see [Proto options](../reference/proto-options.md#option-field-numbers).

## Architecture details

- The `github.com/levelfourab/syncplane` module path and the `engine/gen/syncplane/v1` package layout are part of the public contract because they are baked into generated adopter code.
- `protoc-gen-go` compiles an options-only import into a blank import of the package declared in `go_package`. This blank import pulls in the extension types that the engine compiled against, allowing annotations to resolve to the `(syncplane.v1.entity)` definition that the engine reads.

## The cardinality rule

> Metric attributes must be bounded by the **schema**.
> Span attributes may be bounded by the **traffic**.

A metric stream exists per distinct attribute set for the lifetime of the process in the collector and the underlying time-series database. Entity names, rejection codes, reset reasons, and consumer names come from finite sets declared by the application, making them safe metric attributes.

`workspace_id` and `client_id` are not bounded by the schema:

- `workspace_id` is the partition key and grows with the deployment.
- `client_id` is an unauthenticated per-installation handle that grows without bound and is not reused.

Both attributes appear on spans only. A span is sampled, has a bounded lifetime, and is indexed rather than aggregated.

Key consequences:

- **Names from a request are resolved against the catalog first.** A push naming an entity that does not exist is rejected. Unresolvable names are recorded as `_unregistered` to prevent unbounded metric streams.
- **Per-workspace health is not a metric.** `effects` and `connect` both track status per workspace and expose it through `OnHealth` and `HealthAll`. Effect lag is exported as a maximum per consumer. Use `HealthAll` to locate specific lagging workspaces.

When adding an instrument, classify failures into a bounded enum (`syncplane.outcome`) rather than deriving an attribute value from an error.
