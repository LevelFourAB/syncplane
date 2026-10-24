# Storage adapter contract

`storage.Adapter` is the persistence interface for Syncplane. Syncplane provides two implementations: `engine/storage/sqlitestore` and `engine/storage/pgstore`, described in [Adapters](#adapters).

## Invariants

- **Write mutual exclusion per workspace.** `WithWriteTx` must lock the workspace from before the closure's first read or write until the transaction ends, including reads before the first write. Under multiversion concurrency control (MVCC), such as in PostgreSQL, take a workspace-level lock before the closure's first statement runs: before invoking the closure, or in the same batch as that statement and ahead of it.
- **Write closure retry.** `WithWriteTx` closures can be retried. `WithReadTx` closures run at most once. The engine writes closures to be re-runnable.
- **Re-entrant reads during scans.** A scan callback can execute any read on the active transaction. It must see the same snapshot as the scan. `ScanIndex` callbacks re-read candidate rows through `GetRows` to apply read filters. `ScanChanges` callbacks read `FieldDeltas` to ship text deltas. The same requirement applies to `ScanIndexRanges` callbacks. Drivers that cannot interleave statements on one connection (such as pgx with an open result set) must buffer or page the scan instead of holding an open result set across the callback. Adapters do not need to tolerate writes during iteration. The engine collects rows from a scan and rewrites them after the scan returns.

  `GetRows` also yields rows through a callback. Its callback can read the transaction. The adapter must not hold statements open across the callback.
- **Key order in `GetRows`.** `GetRows(entity, keys, fn)` yields rows in the caller's requested `keys` order. It yields the same rows, tombstones, and actors as `GetRow`. The adapter must reorder rows to match the requested `keys` before yielding.

  A key with no row is skipped. A repeated key yields its row once per occurrence.

  In-process adapters (such as SQLite) may implement `GetRows` by looping over `GetRow`. Networked adapters must answer with a single statement.
- **Text delta log.** `PutFieldDelta`, `FieldDeltas`, and `PruneFieldDeltas` operate on an append-only side table keyed by `(entity, key, field, seq)`. Operations read oldest-first over a sequence span. An adapter can report any span as incomplete, which causes pulls to ship whole documents. An adapter must never report an under-covered span as complete.
- **Text compaction floors.** `TextFloor` and `PutTextFloor` are keyed by `(entity, key, field)`. Text floors must be as durable as the row. Losing a floor allows a replica with pre-compaction state to restore purged tombstones.
- **Client verdicts.** `ClientRejections` and `SetClientRejections` belong to the same per-client record as the idempotency watermark. `ExpireClientRejections` clears verdicts from records last written before its cutoff. It leaves the watermark and the last-written time unchanged, so the record remains idle. `CollectIdleClients` drops verdicts and watermarks together. `GCCutoffs.Tombstones` selects records that hold verdicts. `GCCutoffs.Clients` selects complete client records.
- **Write attribution.** `PutRow` and `DeleteRow` accept a `storage.Actor` with two fields: `Subject` (the authenticated caller) and `System` (the label for a trusted write). The adapter stores both fields with the row and returns them on every read that yields a `Row` (`GetRow`, `GetRows`, `ScanEntity`, `ScanEntityAfter`, and `ScanChanges`). A zero `Actor` means unattributed. Adapters must not populate empty fields.

  A row stores one actor, replaced on each write to preserve only the last writer. The engine stores [field history](proto-options.md#field-history) as regular entity rows through `PutRow`.
- **Creation tracking.** The adapter derives `Row.CreatedVersion`. `PutRow` and `DeleteRow` take no argument for it. A write to a key with no stored state stamps the version of that write. Overwrites, tombstones, and recreations preserve the existing `CreatedVersion`. The value resets only when `CollectTombstones` or `PurgeEntityRows` removes the key state entirely. Reads that return `Row` return `CreatedVersion`. A value of `0` indicates the row was stored before the adapter tracked creation versions.
- **`PurgeEntityRows(entity, keyPrefix, before, limit) (more, err)`.** Hard-deletes rows of an entity—including live rows—and their secondary index entries, without creating tombstones or advancing the tombstone horizon. Use bulk delete statements rather than iterative `DeleteRow` calls:
  - `keyPrefix`: Bytewise prefix expressed as a half-open key range (`lo <= key < hi`), not a `LIKE` expression. An empty prefix matches all keys in the entity.
  - `before`: Timestamp bound that matches rows written strictly before the specified timestamp. A zero value represents no age bound.
  - `limit`: Maximum number of rows to delete in one call. A value `<= 0` removes all matching rows without a limit.
  - `more`: Boolean indicating whether additional matching rows remain. An adapter may report `true` conservatively when reaching `limit`. An adapter reports `false` only when no matching rows remain.

  In SQL implementations, share the key selection between the row deletion and index-entry deletion using `key IN (SELECT key FROM rows WHERE ... ORDER BY key LIMIT n)`. The `ORDER BY` clause ensures both deletions operate on the identical set of keys.
- **Secondary index entries.** `PutIndexEntries`, `DeleteIndexEntries`, `ScanIndex`, `ScanIndexRanges`, `IndexState`, and `SetIndexState` map sort keys (`[]byte`) to row keys per workspace and per `(entity, index_name)` pair:
  - **Key entries by entity and index name:** Keys must be scoped to both the entity and the index name. `IndexState` and `SetIndexState` track build state per `(entity, index_name)` pair.
  - **Compare sort keys bytewise:** Keys are raw bytes compared in lexicographical byte order. `ScanIndex` bounds `lo` (inclusive) and `hi` (exclusive) accept `nil` for unbounded ranges.
  - **Support bidirectional scans:** `ScanIndex` accepts `ScanAsc` or `ScanDesc`. A descending scan traverses the range in reverse order, taking `limit` items starting from `hi`.
  - **Scan many ranges in one call:** `ScanIndexRanges(entity, index, ranges, fn)` yields the entries under each `storage.IndexRange`, range by range in the order of `ranges`. Within one range, it yields the same entries as an ascending `ScanIndex` with no limit. `fn` receives the position of the range in `ranges`. An entry in two ranges is yielded once for each range. An empty `ranges` slice yields nothing. When `fn` returns an error, the scan stops and returns that error unchanged.

    The engine uses `ScanIndexRanges` to read grants for user teams and rows below anchors, which can span thousands of ranges. Networked adapters must read many ranges per round trip. In-process adapters (such as SQLite) may loop over `ScanIndex`. A networked adapter must also page results: rows below an anchor can exceed available memory, and the engine stops reading when it receives enough rows.
  - **Replace full entry set on write:** `PutIndexEntries` replaces all index entries for the specified row across all indexes. Passing an empty slice is equivalent to calling `DeleteIndexEntries`.
  - **Store entries for live rows only:** The engine deletes index entries when deleting a row. Tombstones do not hold index entries.
  - **Hold entries up to the size limit:** A sort key is at most `storage.MaxIndexEntryBytes` (1536) bytes, and the engine never passes a longer one. The adapter must store it together with the longest workspace ID, entity name, index name, and row key. The conformance suite writes such an entry.

  Index entries are derived and disposable. `IndexState` returns the spec fingerprint and a boolean indicating whether the index build finished. A missing index returns `("", false, nil)`.
- **`ScanEntityByVersionDesc`.** Yields live rows for an entity ordered by version descending, breaking ties by row key ascending.
- **`SetIndexBuildMark`.** Updates the workspace with an opaque string mark representing the index declaration fingerprint. The operation updates existing workspace records and must not insert a record if the workspace does not exist.
- **Work queries.** `ListPendingWorkspaceIDs`, `ListCollectableWorkspaceIDs`, `ListStaleIndexWorkspaceIDs`, and `ListCompactableWorkspaceIDs` page results by workspace ID within a `storage.Range`, treat a limit `<= 0` as an error, and may return false positives but never false negatives:
  - `ListPendingWorkspaceIDs`: Returns workspaces with pending consumer work. A workspace is pending if it has no recorded position for the consumer, if its position sequence is behind the head sequence, or if its position names an entity or key sub-position.
  - `ListCollectableWorkspaceIDs`: Returns workspaces containing data older than the specified absolute timestamp cutoffs. A cutoff timestamp of zero means unbounded retention and matches no records for that collection category.
  - `ListStaleIndexWorkspaceIDs`: Returns workspaces whose index build mark does not match the specified mark, including workspaces with no recorded mark.
  - `ListCompactableWorkspaceIDs`: Returns workspaces where the recorded `TextCompactMark` is below the field-delta horizon, where the mark is at the horizon but contains an entity sub-position, or where the recorded `minPurged` is higher than the requested threshold.

## Conformance

The `storagetest` package provides test suites to verify adapter implementations:

- `storagetest.Conformance`: Verifies core contract invariants.
- `storagetest.Catalog`: Verifies the optional `storage.Catalog` capability.

The test suites verify the following requirements:

- **Byte order:** Keys compare bytewise using standard byte comparison (`<` in Go). Collation must not use locale-specific rules. The test suite evaluates key sets containing ASCII and non-ASCII characters (`A Z _ a z é`) where locale ordering diverges from byte ordering.
- **Re-entrant reads:** Callbacks during scans can execute read queries against the active transaction across batch boundaries.

## Workspace ranges

Every workspace-list query takes a `storage.Range`. Both ends are exclusive and compared bytewise, an empty end is unbounded, and the zero value selects every workspace:

```go
type Range struct {
	After  string
	Before string
}
```

An adapter holds one flat space of workspaces. To partition workspaces across applications, use `prefixstore.Wrap(adapter, "<appID>/")`. It prefixes every ID that crosses the seam and narrows every range to the prefix. See [Host multiple apps in one process](../how-to/host-multiple-apps.md).

## Catalog (optional)

An adapter can implement `storage.Catalog` to persist state that belongs to no workspace: opaque payloads keyed by ID, in one flat keyspace shared by every workspace in the store.

`server.Manager` keeps each hosted application's registered schema there and reloads it at boot, so an adapter without a catalog can back a `server.Server` but not a `server.Manager`. Both built-in adapters implement it.

The catalog has the following properties:

- Entries are few and small. Every read is a full listing, ordered by ID.
- `SwapCatalogEntry(ctx, id, payload, revision, prev)` writes only when the stored revision matches `prev`. An empty `prev` string (`""`) indicates that no entry is stored. The check and write are atomic against all other writers. On a revision mismatch, the method writes nothing and returns `storage.ErrCatalogConflict`. Multiple `server.Manager` nodes sharing a store use this method to write the catalog.
- `DeleteCatalogEntry` on a missing ID is not an error.
- A prefixed view from `prefixstore` does not expose the catalog: it belongs to no partition.
- Test suite: `storagetest.Catalog`.

## Cursor state (optional)

An adapter can implement `cursorstate.Provider` to return a `cursorstate.Store`. When provided, the server persists cursor membership sets in the database instead of inlining them in wire tokens. See [Cursor state](server.md#cursor-state).

The cursor store has the following properties:

- Entries are immutable.
- Entries can be evicted. A cache miss causes the client to re-bootstrap.
- Implementations must not run `Get` or `Put` inside their own transaction API, because the server calls them while holding an open read snapshot on the adapter.
- Test suite: `storagetest.CursorState`.

## Leases (optional)

An adapter can implement `lease.Provider` to return a `lease.Locker`. When provided, processes sharing the store deduplicate background maintenance tasks (garbage collection, index sweeps, and effect drains) using a per-`(role, workspace)` try-lock.

Leases have the following properties:

- Leases provide work deduplication, not safety. Callers treat locker errors by proceeding without a lease.
- `sqlitestore` implements process-local locking.
- `pgstore` implements session-level advisory locks over a dedicated connection. It releases locks when the connection closes without waiting for time-to-live (TTL) expiration.
- Test suite: `storagetest.Leases`.

## Read replicas (optional)

An adapter can implement `storage.ReplicaProvider` to return a `storage.ReplicaReader`. When provided, the server routes read paths (bootstrap snapshots, pulls, subscribe catch-ups, change-feed drains, and query scans) to the replica reader instead of `WithReadTx`. Reads are recorded on the `syncplane.replica.reads` metric by route.

The reader serves time-shifted reads as consistent snapshots bounded by a workspace sequence floor:

- `WithReadTxMinSeq`: Runs the closure against a snapshot at or above the specified sequence floor. It uses the replica if up to date. It falls back to the primary if the replica is behind (`fallback_stale`), or if the replica fails before execution (`fallback_error`).
- `WithReadTxAnchored`: Uses the current sequence on the primary as the floor, providing read-your-writes consistency.

Both methods run the closure at most once. Routing decisions occur before closure execution. Errors during stream execution return directly to the caller.

When `ReplicaReader` returns `nil`, replica read capabilities are disabled. Callers and `storage.As` probes must check for `nil`. `pgstore` enables this capability when configured with `WithReadReplica`. `sqlitestore` returns `nil`.

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
- **`pgstore`**: A PostgreSQL adapter using pgx v5 for multi-process deployments. Multiple nodes share a single database and commit transactions to different workspaces concurrently. It uses `pg_advisory_xact_lock` for workspace write exclusion, `REPEATABLE READ READ ONLY` for read transactions, and buffered scans — a single statement where the scan's limit bounds the result, a server-side cursor where it does not — so a scan callback can read the transaction it is scanning.

For multi-node coordination with `pgstore` and a shared notifier such as `pgnotify`, see [Run more than one node](../how-to/run-multiple-nodes.md).

### Tuning `sqlitestore`

`Open` configures default settings for server workloads. Pass configuration options as `Option` values to `Open`. A zero value leaves the underlying default in place.

| Option                    | Default                       | Description                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WithCacheSize(bytes)`    | `8 MiB`                       | Page cache size allocated per connection. Total memory usage is this value multiplied by one writer plus the reader connection count.                                                                                                                                                                                                                                                |
| `WithMmapSize(bytes)`     | `1 GiB`                       | Upper bound on database file bytes each connection reads through a memory mapping instead of a read syscall. The mapping consumes address space rather than memory. All connections share the mapping. A value of `0` disables mapping. An I/O error on a mapped page raises `SIGBUS` instead of returning an error. Set `0` for storage that can disconnect while the process runs. |
| `WithReadConnections(n)`  | `GOMAXPROCS`, clamped to 4..8 | Reader pool size, bounding concurrent read operations, memory usage, and read nesting depth.                                                                                                                                                                                                                                                                                         |
| `WithWALSizeLimit(bytes)` | `64 MiB`                      | Maximum write-ahead log (`-wal`) size retained after a checkpoint.                                                                                                                                                                                                                                                                                                                   |
| `WithOptimizeInterval(d)` | `1 hour`                      | Interval for refreshing query-planner statistics (`PRAGMA optimize`). A value of `0` disables the periodic background loop. `Open` and `Close` still execute an optimization pass.                                                                                                                                                                                                   |

Fixed SQLite settings include:

- Journal mode: `WAL`
- Locking: `BEGIN IMMEDIATE` on write transactions
- Reader pool mode: `query_only`
- Synchronous mode: `NORMAL`

### Tuning `pgstore`

Pass configuration options as `Option` values to `Open`, `Migrate`, and `Statements`. Options that do not apply to `Migrate` or `Statements` are ignored. A zero or empty value selects the default.

| Option                   | Default            | Description                                                                                                                                                                                 |
| ------------------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WithMaxConns(n)`        | pgx default or DSN | Connection pool size, bounding concurrent write operations.                                                                                                                                 |
| `WithReadReplica(dsn)`   | No replica         | Connection string for a hot standby pool exposed as `storage.ReplicaProvider`. `Open` fails if the standby schema version does not match the binary schema version.                         |
| `WithReplicaMaxConns(n)` | pgx default or DSN | Connection pool size for the read replica pool, bounding concurrent replica reads.                                                                                                          |
| `WithPartitions(n)`      | Unpartitioned      | Hash-partitions `rows`, `field_deltas`, `idempotency`, `index_entries`, and `consumer_shadows` by `workspace_id`. Must be a power of two up to 1024. Applied only when creating the schema. |
| `WithoutMigrations()`    | Migrate at boot    | Disables automatic migration during `Open`. `Open` verifies the schema matches the binary version without executing DDL or acquiring migration advisory locks.                              |

The package provides the following migration functions:

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
| 1                     | The lease session, dialed on first use and shared by every application on the node.           |
| 1                     | The `pglisten` `LISTEN` connection, kept open for the process lifetime.                       |
| 1                     | The `pglisten` publisher connection, dialed on the first notification.                        |

`pgnotify` and `pgpresence` share a single `pglisten.Conn`, using two connections total rather than four. Calling `pgnotify.Open` and `pgpresence.Open` with separate DSNs requires four connections. Neither connection is pooled.

Primary pool connections handle writes, anchor point-reads (one statement per bootstrap and query), and fallback reads from lagging or unreachable replicas.

For high-availability deployment patterns, see [Deploy for high availability](../explanation/multi-node-architecture.md#table-layout-and-partitioning).
