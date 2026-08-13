# Storage adapter contract

`storage.Adapter` is the persistence seam. Two implementations ship —
`engine/storage/sqlitestore` and `engine/storage/pgstore`, described under
[Adapters](#adapters). The full contract lives in the `storage` package docs;
this is the part an implementer must not get wrong.

## Invariants

- **Write mutual exclusion per workspace.** `WithWriteTx` must lock the
  workspace for the _entire_ closure, including reads before the first write.
  Under MVCC (Postgres) that means locking the workspace row before invoking the
  closure.
- **Write closures may be retried**; read closures run at most once. The
  engine's closures are written to be re-runnable.
- **Scans must tolerate re-entrant reads.** A scan callback may issue any read
  on the transaction that is scanning, and it must see the same snapshot as the
  scan. The engine does this on its hot paths: `ScanIndex`'s callback re-reads
  its candidate rows through `GetRows` to apply read filters, and
  `ScanChanges`'s callback reads `FieldDeltas` to ship a text delta instead of a
  whole document. SQLite allows several open statements per connection, so it
  satisfies this for free; a driver that cannot interleave statements on one
  connection (pgx with a result set still open, a single-stream remote protocol)
  must **buffer or page the scan** instead of holding one result set open across
  the callback. Writes are the opposite: the engine collects a scan's rows and
  rewrites them after it returns, so an adapter need not tolerate a write during
  iteration.

  `GetRows` is a yielding read as well, so the same rule applies to it: its
  callback may read the transaction, and nothing may be left open across it.
- **`GetRows(entity, keys, fn)` yields in the caller's key order.** It is the
  batch form of `GetRow` — same rows, same tombstones, same actors — and the
  order is the part an adapter can get wrong silently. The engine feeds it keys
  collected from an index walk, whose order _is_ the order the query answers in,
  so an adapter that yields storage order (what `WHERE key = ANY(…)` returns)
  answers ordered queries wrongly with no error anywhere. Reorder against the
  requested keys before yielding.

  A key with no row is **skipped**, not reported — the batch form of
  `ErrNotFound` — and a repeated key yields its row once per occurrence, which
  is what a repeated forward ref naming the same target twice expects.

  It exists to save round trips, so an adapter that has none (SQLite: the
  database is in the process) may loop over `GetRow` and lose nothing. One where
  a point read crosses the network must answer it with a single statement —
  that is why the method is on the interface at all, since an index-served read
  otherwise costs a round trip per candidate row.
- **The text delta log** (`PutFieldDelta` / `FieldDeltas` / `PruneFieldDeltas`)
  is an append-only side table keyed by `(entity, key, field, seq)`, read back
  oldest-first over a sequence span. It is an optimization, so an adapter is
  free to report every span incomplete — pulls then ship whole documents — but
  it must **never report an under-covered span as complete**.
- **Text compaction floors** (`TextFloor` / `PutTextFloor`), keyed by
  `(entity, key, field)`, are the opposite: not an optimization. Losing one lets
  a replica holding pre-compaction state put purged tombstones back, so they must
  be **as durable as the row**.
- **Client verdicts** (`ClientRejections` / `SetClientRejections`) belong to the
  same per-client record as the idempotency watermark, and `CollectIdleClients`
  must drop both together. They are what lets a resend that follows a lost push
  response be answered with the original rejection instead of a bare ack, so
  keeping them past the watermark would answer for a client the watermark no
  longer knows — and dropping them while it remains loses the verdict the resend
  came for.
- **Write attribution.** `PutRow` and `DeleteRow` take a `storage.Actor` — two
  strings, `Subject` (the authenticated caller) and `System` (the label a
  trusted write gave itself) — which is stored with the row and returned by
  **every** read that yields a `Row`: `GetRow`, `GetRows`, `ScanEntity`,
  `ScanEntityAfter`, `ScanChanges`. Store both as given; a zero `Actor` means "unattributed" and
  must not be filled in from anything the adapter knows. Populating it on some
  read paths and not others is the one failure that misleads silently, because a
  missing actor is indistinguishable from an unattributed write.

  A row holds one actor, replaced on each write, so only its **last** writer is
  recoverable. Adapters still need no history table: the engine's
  [field history](proto-options.md#field-history) is ordinary rows of an
  ordinary entity, written through `PutRow` on the same write path as everything
  else. What it does need is `PurgeEntityRows` — see below.
- **`PurgeEntityRows(entity, keyPrefix, before, limit)`** hard-deletes rows of
  one entity — **live rows included** — together with their index entries,
  leaving no tombstone and not moving the tombstone horizon. It is the only
  delete in the interface that removes a live row outright.

  It exists for entities no client ever receives (`server_only`, of which field
  history is the one that ships), and is only sound for those: used on anything
  a client syncs it would delete rows every replica still holds with no
  tombstone to carry the deletion. Make it a bulk statement rather than a scan
  plus `DeleteRow` — a `DeleteRow` per expired record would mint the tombstones
  this exists to avoid. Index entries go in the same statement, because the
  engine has no row keys in hand here to pair a `DeleteIndexEntries` with.

  All three narrowings are optional and all apply. `keyPrefix` is a **bytewise**
  prefix, so express it as a half-open key range rather than a `LIKE` — that is
  the same comparison every other key predicate makes, uses the primary key, and
  needs no wildcard escaping. An empty prefix matches the whole entity. `before`
  keeps rows written strictly before it, and its **zero value means no age bound
  at all** — not "before the epoch", which is what a naive `UnixMilli()` would
  say and would silently delete nothing. `limit` caps how many rows one call
  removes, `<= 0` meaning no cap; which matching rows a bounded call takes is up
  to the adapter, as long as the ones it leaves are reported through `more`.

  `more` is allowed to be conservative — "this call hit its limit" is a fine
  answer, so a call that removed exactly the last `limit` rows may report `more`
  and the next one then removes nothing. Only `false` is definite, and it must
  arrive: the caller drains by looping until it does.

  Both SQL adapters implement the bound as `key IN (SELECT key FROM rows WHERE …
  ORDER BY key LIMIT n)`, shared by the row delete and the index-entry delete.
  The `ORDER BY` is load-bearing rather than cosmetic: the two subqueries must
  select the **same** set, or entries outlive their rows or die before them.

  The engine calls it both ways: the GC pass expires old records with an age
  bound and a limit it drains across transactions, while a `DELETE` drops the
  records describing the row it removes with a prefix and no limit — those must
  go with the row in its own transaction or not at all.
- **Secondary index entries** (`PutIndexEntries` / `DeleteIndexEntries` /
  `ScanIndex` / `IndexState` / `SetIndexState`) map an opaque sort key to a row
  key, per workspace and per **(entity, index name)** pair. Four rules:

  - **Key entries by entity as well as by index name.** An index name is derived
    from a field name, so two entities routinely claim the same one, and an entry
    carries a bare row key that means nothing outside its own entity. An adapter
    that scans by name alone hands one entity's row keys to the other's reader,
    which then reads live rows of its own that the query never matched — a wrong
    answer, not a slow one. `IndexState` and `SetIndexState` are per pair for the
    same reason: two entities sharing a name have separate entries, built
    separately, and one shared flag would let either claim the other's coverage.
  - **Compare the keys bytewise.** They are `[]byte`, and the engine builds them
    so that byte order _is_ the row order the index serves. An adapter that
    stores them as text or applies a collation answers ordered queries wrongly
    with no error anywhere. `ScanIndex` takes `lo` inclusive and `hi` exclusive;
    a nil bound is open on that end.
  - **`ScanIndex` walks in either direction.** Its `order` parameter is
    `ScanAsc` or `ScanDesc`, and the bounds mean the same thing under both — so
    a descending walk yields the exact reverse of the ascending one over the
    same range, and only the end `limit` takes from moves: from `lo` ascending,
    from `hi` descending. Reading backwards is how a query pages back from a
    `before` cursor. An adapter that ignores `order` and always walks ascending
    returns a page of the wrong rows, with no error anywhere.
  - **`PutIndexEntries` replaces the row's whole entry set**, across every
    index — not just the ones named in the call. The caller has no previous row
    value in hand (a PATCH merges into stored bytes, a DELETE has only a key), so
    it cannot compute a diff, which is why a by-row lookup is what makes this
    affordable. An empty slice is the same as `DeleteIndexEntries`.
  - **Entries exist for live rows only.** The engine pairs every `DeleteRow` with
    a `DeleteIndexEntries`, so a tombstone holds none, and `CollectTombstones` —
    a bulk delete of rows already gone from the index — needs no index work.

  Entries are **derived and disposable**: losing them costs a rebuild, never
  correctness, and nothing in a request or response mentions an index. An adapter
  under storage pressure may drop the table. `IndexState` reports the spec
  fingerprint entries were built under and whether the build finished; a missing
  index reads as `("", false, nil)`, which is the normal state after
  registration, not an error.
- **`ScanEntityByVersionDesc`** yields one entity's live rows newest first,
  ties broken by key **ascending**. The tie-break is load-bearing: a recency
  window takes the first `limit` rows, so two rows sharing a version must order
  here exactly as the engine's own comparator orders them, or the window's
  contents depend on where a page happened to end.
- **`ListPendingWorkspaceIDs`** answers "which workspaces owe this consumer
  work", paged by workspace id from an exclusive cursor. It is the one
  cross-workspace read in the interface, and it exists so that a background
  consumer's idle cost is one query rather than a read transaction per
  workspace. Three rules:

  - **It may be optimistic, never short.** Listing a workspace whose pending
    changes the consumer filters out costs a wasted pass; omitting one whose
    position is behind is an effect that never fires.
  - **A position naming an entity or key is pending** whatever its sequence
    says. That is a sub-position inside a same-sequence group — where a failed
    pass committed the prefix it delivered — so the group has changes after it
    even once the sequence has reached the head.
  - **A workspace with no position for the consumer is pending.** Having acted
    on nothing is a first run, which is how a newly registered consumer
    discovers the history it has to catch up on.

  An adapter with a cheaper answer (a dirty set maintained on write) may
  substitute one, as long as it stays complete.

## Conformance

`storagetest.Conformance` and `storagetest.ConformanceNamespaced` encode the
contract. New adapters must pass both.

Two of those specs are worth knowing about before you write an adapter, because
they fail late and for reasons that do not look like storage bugs:

- **Byte order.** Keys compare bytewise, the order Go's `<` gives, and the
  engine shares that order with the adapter across page boundaries. Under a
  locale collation an `afterKey` scan silently drops or repeats rows instead of
  failing. The specs use a key set (`A Z _ a z é`) whose byte order disagrees
  with a typical locale's — which is also why the suite's Postgres container is
  the glibc image and not `-alpine`, whose musl sorts UTF-8 byte-for-byte and so
  would let the bug through.
- **Re-entrant reads inside scan callbacks.** A scan callback may issue any
  read on the transaction it is scanning. One spec scans further than any
  plausible internal page size, so an adapter that reads its result set in
  batches has to serve callbacks _between_ batches, not merely within one.

## Namespaces

An adapter partitions workspaces, rows and client watermarks by namespace so one
database can back several apps. Both adapters expose this as `root.Namespace(id)`
and a `WithNamespace(id)` option; `Close` on a derived store is a no-op, because
the root owns the handle.

## Cursor state (optional)

An adapter may additionally implement `cursorstate.Provider` — one method
returning a `cursorstate.Store` backed by its own database — and a server
constructed on it then stores issued cursors' membership sets instead of
restating them on the wire (see [Cursor state](server.md#cursor-state)). The
seam sits outside the `storage.Adapter` contract because it tolerates far
weaker guarantees than row storage does: entries are immutable, eviction is
licensed, and a miss costs one client one re-bootstrap. Both shipped adapters
implement it; `storagetest.CursorState` and `storagetest.CursorStateNamespaced`
encode the expectations. An adapter without it still works — every cursor
inlines its sets, as all cursors did before the seam existed.

Implementations must not serve `Get`/`Put` through their own transactional
API: the server calls them while it holds an open read snapshot on the same
adapter.

## Leases (optional)

An adapter may implement `lease.Provider` — one method returning a
`lease.Locker` — and every process sharing the store then dedups its
background work out of one lease space: the GC and index sweeps and effects
drains take a per-`(role, workspace)` try-lock around each pass. The seam
tolerates weaker guarantees than row storage: a lease may vanish without its
holder noticing, and callers treat a locker error as "proceed unleased", so an
implementation buys deduplication, never correctness. Both shipped adapters
implement it — `sqlitestore` process-locally (its file admits one process,
which makes that complete) and `pgstore` with session advisory locks on a
dedicated connection, so a node's leases die with its connection rather than
waiting out a TTL. `storagetest.Leases` and `storagetest.LeasesNamespaced`
encode the expectations.

## Read replicas (optional)

An adapter may implement `storage.ReplicaProvider` — one method returning a
`storage.ReplicaReader` — and a server constructed on it routes its heavy read
paths (bootstrap snapshots, pulls, subscribe catch-ups, change-feed drains,
query scans) through that reader instead of `WithReadTx`, counting each read on
`syncplane.replica.reads` by route.

The reader serves _time-shifted reads_: consistent snapshots of the primary as
of an earlier moment, bounded per read by a workspace-sequence floor.
`WithReadTxMinSeq` runs its closure over a snapshot whose workspace sequence is
at least the floor, preferring the replica and falling back to the primary when
it is behind (`fallback_stale`) or failing before the closure runs
(`fallback_error`); `WithReadTxAnchored` floors at the primary's current
sequence, which is read-your-writes for callers holding no sequence of their
own. Both keep `WithReadTx`'s at-most-once contract for the closure, and the
routing decision completes before the closure runs — a replica failure
mid-stream surfaces as an error, exactly as a failover does.

`ReplicaReader` returning nil means the capability is absent: implementing the
interface is not the promise, configuration is, so callers (and `storage.As`
probes) must check for nil. `pgstore` implements it when `WithReadReplica`
names a standby; `sqlitestore` does not — its file admits one process, which
leaves nothing to offload. `storagetest.Replica` encodes the expectations;
running `storagetest.Conformance` with a replica configured (pointed at the
primary itself is enough) must also still pass.

## Wrapping adapters

A wrapper that delegates to an inner adapter (`otelstore` is the shipped
example) satisfies `storage.Adapter` by forwarding, but that does not carry
optional capabilities such as `cursorstate.Provider` — they sit outside the
interface. A wrapper exposes them by implementing `Unwrap() storage.Adapter`;
the engine probes with `storage.As`, which follows the chain, so a capability
keeps working however deeply the adapter is wrapped.

## Adapters

- **`sqlitestore`** — the default, and the one that needs no infrastructure. A
  single writer connection serializes every workspace against every other, and
  the file cannot be shared between processes.
- **`pgstore`** — PostgreSQL over pgx v5, for deployments that outgrow one
  process. Several nodes share one database and different workspaces commit
  concurrently. Its three non-obvious choices are documented at the point of
  code: `pg_advisory_xact_lock` for write exclusion (a row lock cannot work,
  because lazy registration means a first write has no row to lock), `REPEATABLE
  READ READ ONLY` reads, and server-side cursors so a scan's callback can read
  on the same connection.

Choosing `pgstore` is necessary but not sufficient for multi-node: pokes still
need a shared notifier (`pgnotify`). See
[Run more than one node](../how-to/run-multiple-nodes.md).

### Tuning `sqlitestore`

`Open` picks server-shaped defaults rather than SQLite's library-shaped ones.
Each is an `Option`; zero means "leave the default underneath" — SQLite's for
the cache and WAL limit, an unbounded pool for the reader count.

| Option                    | Default                       | What it controls                                                                                                                                                                     |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WithCacheSize(bytes)`    | 8 MiB                         | Page cache. Charged **per connection**, so the store's ceiling is this times one writer plus every reader — raise it together with the next row, not alone.                          |
| `WithReadConnections(n)`  | `GOMAXPROCS`, clamped to 4..8 | Reader pool size, which bounds read concurrency, the memory above, and how deeply reads may nest — a nested read holds its outer read's connection while waiting for one of its own. |
| `WithWALSizeLimit(bytes)` | 64 MiB                        | How large the `-wal` sidecar is left after a checkpoint. Unlimited, it stays at its high-water mark for the life of the file.                                                        |
| `WithOptimizeInterval(d)` | 1 hour                        | How often the writer refreshes query-planner statistics (`PRAGMA optimize`). Zero disables the loop; `Open` and `Close` still run a pass.                                            |

The statistics pass matters more here than in a typical SQLite application:
indexes are created at runtime, when an entity registers one, so the moment a
schema-time `ANALYZE` would have covered never arrives. It is advisory — stale
statistics cost plan quality, never correctness — so its failures are not
reported.

Everything else is fixed. `journal_mode` is WAL, writes take the write lock at
`BEGIN` (`BEGIN IMMEDIATE`) so contention waits out `busy_timeout` instead of
failing with `SQLITE_BUSY_SNAPSHOT`, the reader pool is `query_only`, and
`synchronous` is `NORMAL` — the WAL default, which trades the last commits
before a power loss or kernel panic (not a process crash) for not paying an
fsync per transaction. A client whose mutation is acked and then lost that way
will not resend it, since an ack retires the pending entry.

### Tuning `pgstore`

Each is an `Option`. `Open` takes all of them; `Migrate` and `Statements` accept
the same values and ignore the ones that cannot apply to them. Zero or empty
selects the default.

| Option                   | Default             | What it controls                                                                                                                                                                                 |
| ------------------------ | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WithNamespace(ns)`      | `""`                | The app namespace this store's rows, workspaces and watermarks live under. Same as `Open(...).Namespace(ns)`.                                                                                    |
| `WithMaxConns(n)`        | pgx's, or the DSN's | Pool size, which bounds writers in flight — a write holds a connection while it waits for its workspace's advisory lock.                                                                         |
| `WithReadReplica(dsn)`   | no replica          | A second pool against a hot standby, exposed as `storage.ReplicaProvider`. `Open` fails if the schema there is not at this binary's version.                                                     |
| `WithReplicaMaxConns(n)` | pgx's, or the DSN's | Replica pool size, which bounds concurrent replica reads.                                                                                                                                        |
| `WithPartitions(n)`      | unpartitioned       | Hash-partitions `rows`, `field_deltas`, `idempotency`, `index_entries` and `consumer_shadows` by `(namespace, workspace_id)`. A power of two up to 1024, honoured only when creating the schema. |
| `WithoutMigrations()`    | migrate at boot     | `Open` verifies the ledger is at this binary's version instead of applying it, and fails if it is not. Issues no DDL and takes no advisory lock.                                                 |

`Open` migrates at boot, serialized across nodes by an advisory lock.
`Migrate(ctx, dsn, opts...)` does the same on its own, and `Statements(opts...)`
returns the migrations as SQL for a deployment that applies its own DDL. Pair
either with `WithoutMigrations()` on the nodes, which is what stops a node from
migrating the database behind the deploy step's back — and lets it connect as a
role with no privilege to change the schema at all. What the layout and the
migration ownership cost an operator is in
[Deploy for high availability](../how-to/deploy-for-high-availability.md#table-layout).
