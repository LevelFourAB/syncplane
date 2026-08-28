# Storage adapter contract

`storage.Adapter` is the persistence interface for Syncplane. Syncplane provides two implementations: `engine/storage/sqlitestore` and `engine/storage/pgstore`, described in [Adapters](#adapters).

## Invariants

- **Write mutual exclusion per workspace.** `WithWriteTx` must lock the workspace for the entire closure, including reads before the first write. Under multiversion concurrency control (MVCC), such as in PostgreSQL, lock the workspace row before invoking the closure.
- **Write closure retry.** `WithWriteTx` closures may be retried. `WithReadTx` closures run at most once. The engine writes closures to be re-runnable.
- **Re-entrant reads during scans.** A scan callback may issue any read on the active transaction, and it must see the same snapshot as the scan. `ScanIndex` callbacks re-read candidate rows through `GetRows` to apply read filters, and `ScanChanges` callbacks read `FieldDeltas` to ship text deltas. Drivers that cannot interleave statements on one connection (such as pgx with an open result set) must buffer or page the scan instead of holding a result set open across the callback. Adapters do not need to tolerate writes during iteration; the engine collects rows from a scan and rewrites them after the scan returns.

  `GetRows` also yields rows through a callback. Its callback may read the transaction, and the adapter must not hold statements open across it.
- **Key order in `GetRows`.** `GetRows(entity, keys, fn)` yields rows in the caller's requested `keys` order. It yields the same rows, tombstones, and actors as `GetRow`. The adapter must reorder rows to match the requested `keys` before yielding.

  A key with no row is skipped. A repeated key yields its row once per occurrence.

  In-process adapters (such as SQLite) may implement `GetRows` by looping over `GetRow`. Networked adapters must answer with a single statement.
- **Text delta log.** `PutFieldDelta`, `FieldDeltas`, and `PruneFieldDeltas` operate on an append-only side table keyed by `(entity, key, field, seq)`, read oldest-first over a sequence span. An adapter may report any span as incomplete, causing pulls to ship whole documents. An adapter must never report an under-covered span as complete.
- **Text compaction floors.** `TextFloor` and `PutTextFloor` are keyed by `(entity, key, field)`. Text floors must be as durable as the row. Losing a floor allows a replica with pre-compaction state to restore purged tombstones.
- **Client verdicts.** `ClientRejections` and `SetClientRejections` belong to the same per-client record as the idempotency watermark. `CollectIdleClients` must drop verdicts and watermarks together.
- **Write attribution.** `PutRow` and `DeleteRow` accept a `storage.Actor` with two fields: `Subject` (the authenticated caller) and `System` (the label for a trusted write). The adapter stores both fields with the row and returns them on every read that yields a `Row` (`GetRow`, `GetRows`, `ScanEntity`, `ScanEntityAfter`, and `ScanChanges`). A zero `Actor` means unattributed; adapters must not populate empty fields.

  A row stores one actor, replaced on each write, preserving only the last writer. The engine stores [field history](proto-options.md#field-history) as regular entity rows through `PutRow`.
- **Creation tracking.** The adapter derives `Row.CreatedVersion`. `PutRow` and `DeleteRow` take no argument for it. A write to a key with no stored state stamps the version of that write. Overwrites, tombstones, and recreations preserve the existing `CreatedVersion`. The value resets only when `CollectTombstones` or `PurgeEntityRows` removes the key state entirely. Reads that return `Row` return `CreatedVersion`. A value of `0` indicates the row was stored before the adapter tracked creation versions.
- **`PurgeEntityRows(entity, keyPrefix, before, limit) (more, err)`.** Hard-deletes rows of an entity—including live rows—and their secondary index entries, without creating tombstones or advancing the tombstone horizon. Use bulk delete statements rather than iterative `DeleteRow` calls:
  - `keyPrefix`: Bytewise prefix expressed as a half-open key range (`lo <= key < hi`), not a `LIKE` expression. An empty prefix matches all keys in the entity.
  - `before`: Timestamp bound that matches rows written strictly before the specified timestamp. A zero value represents no age bound.
  - `limit`: Maximum number of rows to delete in one call. A value `<= 0` removes all matching rows without a limit.
  - `more`: Boolean indicating whether additional matching rows remain. An adapter may report `true` conservatively when reaching `limit`. An adapter reports `false` only when no matching rows remain.

  In SQL implementations, share the key selection between the row deletion and index-entry deletion using `key IN (SELECT key FROM rows WHERE ... ORDER BY key LIMIT n)`. The `ORDER BY` clause ensures both deletions operate on the identical set of keys.
- **Secondary index entries.** `PutIndexEntries`, `DeleteIndexEntries`, `ScanIndex`, `IndexState`, and `SetIndexState` map sort keys (`[]byte`) to row keys per workspace and per `(entity, index_name)` pair:
  - **Key entries by entity and index name:** Keys must be scoped to both the entity and the index name. `IndexState` and `SetIndexState` track build state per `(entity, index_name)` pair.
  - **Compare sort keys bytewise:** Keys are raw bytes compared in lexicographical byte order. `ScanIndex` bounds `lo` (inclusive) and `hi` (exclusive) accept `nil` for unbounded ranges.
  - **Support bidirectional scans:** `ScanIndex` accepts `ScanAsc` or `ScanDesc`. A descending scan traverses the range in reverse order, taking `limit` items starting from `hi`.
  - **Replace full entry set on write:** `PutIndexEntries` replaces all index entries for the specified row across all indexes. Passing an empty slice is equivalent to calling `DeleteIndexEntries`.
  - **Store entries for live rows only:** The engine deletes index entries when deleting a row. Tombstones do not hold index entries.

  Index entries are derived and disposable. `IndexState` returns the spec fingerprint and a boolean indicating whether the index build finished. A missing index returns `("", false, nil)`.
- **`ScanEntityByVersionDesc`.** Yields live rows for an entity ordered by version descending, breaking ties by row key ascending.
- **`SetIndexBuildMark`.** Updates the workspace with an opaque string mark representing the index declaration fingerprint. The operation updates existing workspace records and must not insert a record if the workspace does not exist.
- **Work queries.** `ListPendingWorkspaceIDs`, `ListCollectableWorkspaceIDs`, `ListStaleIndexWorkspaceIDs`, and `ListCompactableWorkspaceIDs` page results by workspace ID using an exclusive cursor, treat a limit `<= 0` as an error, and may return false positives but never false negatives:
  - `ListPendingWorkspaceIDs`: Returns workspaces with pending consumer work. A workspace is pending if it has no recorded position for the consumer, if its position sequence is behind the head sequence, or if its position names an entity or key sub-position.
  - `ListCollectableWorkspaceIDs`: Returns workspaces containing data older than the specified absolute timestamp cutoffs. A cutoff timestamp of zero means unbounded retention and matches no records for that collection category.
  - `ListStaleIndexWorkspaceIDs`: Returns workspaces whose index build mark does not match the specified mark, including workspaces with no recorded mark.
  - `ListCompactableWorkspaceIDs`: Returns workspaces where the recorded `TextCompactMark` is below the field-delta horizon, where the mark is at the horizon but contains an entity sub-position, or where the recorded `minPurged` is higher than the requested threshold.

## Conformance

The `storagetest` package provides test suites to verify adapter implementations:

- `storagetest.Conformance`: Verifies core contract invariants.
- `storagetest.ConformanceNamespaced`: Verifies contract invariants under namespacing.

Key test expectations:

- **Byte order:** Keys compare bytewise using standard byte comparison (`<` in Go). Collation must not use locale-specific rules. The test suite evaluates key sets containing ASCII and non-ASCII characters (`A Z _ a z é`) where locale ordering diverges from byte ordering.
- **Re-entrant reads:** Callbacks during scans may execute read queries against the active transaction across batch boundaries.

## Namespaces

An adapter partitions workspaces, rows, and client watermarks by namespace to allow multiple applications to share a single database.

Adapters provide namespace support through `root.Namespace(id)` and the `WithNamespace(id)` option. Calling `Close` on a namespaced store instance is a no-op because the root store owns the database handle.

## Cursor state (optional)

An adapter can implement `cursorstate.Provider` to return a `cursorstate.Store`. When provided, the server persists cursor membership sets in the database instead of inlining them in wire tokens. For more information, see [Cursor state](server.md#cursor-state).

Properties:

- Entries are immutable.
- Entries may be evicted. A cache miss causes the client to re-bootstrap.
- Implementations must not run `Get` or `Put` inside their own transaction API, because the server calls them while holding an open read snapshot on the adapter.
- Test suites: `storagetest.CursorState` and `storagetest.CursorStateNamespaced`.

## Leases (optional)

An adapter can implement `lease.Provider` to return a `lease.Locker`. When provided, processes sharing the store deduplicate background maintenance tasks (garbage collection, index sweeps, and effect drains) using a per-`(role, workspace)` try-lock.

Properties:

- Leases provide work deduplication, not safety. Callers treat locker errors by proceeding without a lease.
- `sqlitestore` implements process-local locking.
- `pgstore` implements session-level advisory locks over a dedicated connection, releasing locks when the connection closes without waiting for a time-to-live (TTL) expiration.
- Test suites: `storagetest.Leases` and `storagetest.LeasesNamespaced`.

## Read replicas (optional)

An adapter can implement `storage.ReplicaProvider` to return a `storage.ReplicaReader`. When provided, the server routes read paths (bootstrap snapshots, pulls, subscribe catch-ups, change-feed drains, and query scans) to the replica reader instead of `WithReadTx`. Reads are recorded on the `syncplane.replica.reads` metric by route.

The reader serves time-shifted reads, which are consistent snapshots bounded by a workspace sequence floor:

- `WithReadTxMinSeq`: Runs the closure against a snapshot at or above the specified sequence floor. It uses the replica if up to date, falls back to the primary if the replica is behind (`fallback_stale`), or falls back if the replica fails before execution (`fallback_error`).
- `WithReadTxAnchored`: Uses the current sequence on the primary as the floor, providing read-your-writes consistency.

Both methods run the closure at most once. Routing decisions occur before closure execution; errors during stream execution return directly to the caller.

When `ReplicaReader` returns `nil`, replica read capabilities are disabled. Callers and `storage.As` probes must check for `nil`. `pgstore` enables this when configured with `WithReadReplica`. `sqlitestore` returns `nil`.

Test suites: `storagetest.Replica` and `storagetest.Conformance` (with a configured replica).

## Wrapping adapters

An adapter wrapper (such as `otelstore`) can forward `storage.Adapter` calls to an underlying adapter.

To expose optional interfaces implemented by the underlying adapter (such as `cursorstate.Provider`, `lease.Provider`, or `storage.ReplicaProvider`), the wrapper must implement:

```go
Unwrap() storage.Adapter
```

The engine uses `storage.As` to traverse the wrapper chain and locate optional capabilities.

## Adapters

Syncplane provides two built-in adapters:

- **`sqlitestore`**: The default storage adapter. Uses SQLite within a single process. A single writer connection serializes write transactions across all workspaces. The database file cannot be shared between processes.
- **`pgstore`**: A PostgreSQL adapter using pgx v5 for multi-process deployments. Multiple nodes share a single database and commit transactions to different workspaces concurrently. It uses `pg_advisory_xact_lock` for workspace write exclusion, `REPEATABLE READ READ ONLY` for read transactions, and server-side cursors for re-entrant scan callbacks.

For multi-node coordination with `pgstore`, configure a shared notifier such as `pgnotify`. For more information, see [Run more than one node](../how-to/run-multiple-nodes.md).

### Tuning `sqlitestore`

`Open` configures default settings for server workloads. Pass configuration options as `Option` values to `Open`. A zero value leaves the underlying default in place.

| Option                    | Default                       | Description                                                                                                                                                                        |
| ------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithCacheSize(bytes)`    | `8 MiB`                       | Page cache size allocated per connection. Total memory usage is this value multiplied by one writer plus the reader connection count.                                              |
| `WithReadConnections(n)`  | `GOMAXPROCS`, clamped to 4..8 | Reader pool size, bounding concurrent read operations, memory usage, and read nesting depth.                                                                                       |
| `WithWALSizeLimit(bytes)` | `64 MiB`                      | Maximum write-ahead log (`-wal`) size retained after a checkpoint.                                                                                                                 |
| `WithOptimizeInterval(d)` | `1 hour`                      | Interval for refreshing query-planner statistics (`PRAGMA optimize`). A value of `0` disables the periodic background loop; `Open` and `Close` still execute an optimization pass. |

Fixed SQLite settings:

- Journal mode: `WAL`
- Locking: `BEGIN IMMEDIATE` on write transactions
- Reader pool mode: `query_only`
- Synchronous mode: `NORMAL`

### Tuning `pgstore`

Pass configuration options as `Option` values to `Open`, `Migrate`, and `Statements`. Options that do not apply to `Migrate` or `Statements` are ignored. A zero or empty value selects the default.

| Option                   | Default            | Description                                                                                                                                                                                              |
| ------------------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithNamespace(ns)`      | `""`               | App namespace for rows, workspaces, and watermarks. Equivalent to `Open(...).Namespace(ns)`.                                                                                                             |
| `WithMaxConns(n)`        | pgx default or DSN | Connection pool size, bounding concurrent write operations.                                                                                                                                              |
| `WithReadReplica(dsn)`   | No replica         | Connection string for a hot standby pool exposed as `storage.ReplicaProvider`. `Open` fails if the standby schema version does not match the binary schema version.                                      |
| `WithReplicaMaxConns(n)` | pgx default or DSN | Connection pool size for the read replica pool, bounding concurrent replica reads.                                                                                                                       |
| `WithPartitions(n)`      | Unpartitioned      | Hash-partitions `rows`, `field_deltas`, `idempotency`, `index_entries`, and `consumer_shadows` by `(namespace, workspace_id)`. Must be a power of two up to 1024. Applied only when creating the schema. |
| `WithoutMigrations()`    | Migrate at boot    | Disables automatic migration during `Open`. `Open` verifies the schema matches the binary version without executing DDL or acquiring migration advisory locks.                                           |

Migration functions:

- `Migrate(ctx, dsn, opts...)`: Applies migrations directly using an advisory lock.
- `Statements(opts...)`: Returns migration DDL statements as SQL.
- `SchemaVersion()`: Returns the schema version this binary targets.
- `CanServe(binaryVersion, schemaVersion)`: Checks whether a binary version can operate against a specified schema version.

`Open` serves a database migrated to a newer schema version if all intermediate migration steps are additive.

#### Connection budget

Each sync node opens the following database connections:

| Connections           | Purpose                                                                                       |
| --------------------- | --------------------------------------------------------------------------------------------- |
| `WithMaxConns`        | The `pgstore` primary pool. A write holds one connection while waiting on its workspace lock. |
| `WithReplicaMaxConns` | The replica pool against the standby. Used only with `WithReadReplica`.                       |
| 1                     | The lease session, dialed on first use and shared by all application namespaces.              |
| 1                     | The `pglisten` `LISTEN` connection, kept open for the process lifetime.                       |
| 1                     | The `pglisten` publisher connection, dialed on the first notification.                        |

`pgnotify` and `pgpresence` share the single `pglisten.Conn`, requiring two connections total rather than four. Calling `pgnotify.Open` and `pgpresence.Open` with separate DSNs doubles this requirement. Neither connection is pooled.

Primary pool connections handle writes, anchor point-reads (one statement per bootstrap and query), and fallback reads from lagging or unreachable replicas.

For high-availability deployment patterns, see [Deploy for high availability](../explanation/multi-node-architecture.md#table-layout-and-partitioning).
