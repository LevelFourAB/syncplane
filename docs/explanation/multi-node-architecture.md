# Multi-node architecture

This document explains how syncplane coordinates operations across multiple nodes, isolates failure across tiers, manages database sessions and leases, and handles best-effort notification delivery.

To configure and operate a multi-node deployment, see [Run multiple nodes](../how-to/run-multiple-nodes.md) and [Deploy for high availability](../how-to/deploy-for-high-availability.md).

## State tiers and failure impact

Only PostgreSQL holds persistent state in a syncplane deployment, plus the blob store when the server keeps [blobs](blobs.md#more-than-one-node). Sync nodes scan the shared store and send content-free sequence advancement signals through the notifier. Leases depend on active PostgreSQL sessions and release as soon as the connection drops.

| Tier       | Replicas | Failure impact                                          |
| ---------- | -------- | ------------------------------------------------------- |
| Sync nodes | N        | Clients reconnect to another node.                      |
| PostgreSQL | HA pair  | Writes pause during the failover window.                |
| Connectors | 1        | External convergence pauses until the process restarts. |

Failure handling behaves as follows:

- **Sync node failure:** When a node stops, its database connections drop and release all held session leases immediately. Clients reconnect to another node and resume from their cursor position. Unacknowledged writes remain in the client queue and retry idempotently.
- **Database failover:** In-flight transactions return errors and clients retry. `pgstore` retries serialization failures and deadlocks. When a `LISTEN` connection drops, the listener re-queries active subscriptions upon reconnect. Periodic sweeps back up this recovery.
- **Rolling deployment:** Effect handlers must be idempotent to handle concurrent executions across versions. The schema-revision handshake prevents client-server version mismatches.

## Schema registration across nodes

A `server.Manager` keeps the schema of each app in the store catalog. Each node also holds the schema that it serves in memory. Each admin `Register` call reaches one node, so nodes can serve different schemas for a short time.

Two rules keep nodes from overwriting each other's registrations:

- **The catalog is the base of every merge.** A registration reads the stored schema. If the stored schema is newer than the node's schema, the node serves the stored schema first. The node then merges the new set over it. The access-change check compares against the stored schema. A node that missed a narrowed role cannot widen that role again without `AllowAccessChange`.
- **The write is a compare-and-swap.** The node stores the merged schema only if the stored revision matches the revision it merged over. If another node wrote in between, the node reads again and merges again. The app serves the merged schema only after the write succeeds. Node memory never runs ahead of the store.

Other nodes learn of a new schema from a poke on the shared notifier, and from periodic catalog reads for lost pokes. A node serves a schema stored by another node without checking the access change again. The node that stored the schema already checked the change against the catalog.

Until a node receives the new schema, it serves the previous schema. After a role narrows, a node can send rows under the old role for up to one poll interval. The poke shortens this window to the notifier's delivery latency.

## Connection pooler restrictions

Running PgBouncer or similar connection poolers in `transaction` mode breaks three syncplane mechanisms:

- **Leases:** Leases rely on PostgreSQL session advisory locks. In transaction pooling mode, locks do not persist across transactions.
- **`LISTEN` notifications:** `LISTEN` binds to a specific database session. In transaction mode, notifications fail to reach listeners.
- **Startup migrations:** Schema migrations hold session-scoped advisory locks across transactions. Transaction pooling allows concurrent migrations from multiple booting nodes.

## Read replica semantics

Replica reads provide consistent snapshots bounded by workspace sequence floors (`last_seq` cursor position, client `min_seq`, or primary sequence). If a read replica lags behind the requested sequence floor, `pgstore` falls back to the primary database.

A pull with no cursor always reads on the primary. A client with no state uses it to learn its mutation watermark before it numbers its next write. No sequence floor makes a replica watermark current, because a rejected mutation moves the watermark and consumes no sequence. A stale watermark makes the next write reuse an id that the server already holds, and the server then answers with the outcome of the older mutation.

## Table layout and partitioning

Partitioning the large tables improves autovacuum maintenance by isolating dead tuples into smaller concurrent tasks. `rows` and `idempotency` are partitioned by `workspace_id`. The value index tables `index_rows` and `index_entries`, the text delta log `field_deltas`, and `consumer_shadows` are partitioned by a workspace number, and the search tables `posting_rows`, `posting_chunks`, and `posting_pages` by a posting scope. In each case, all the data of one workspace or one search index is in one partition.

The index tables hold integer ids in place of names. `index_scopes` gives each workspace, entity, and index an id, and `index_rows` gives each indexed row an id. An index entry then holds ids and the entry bytes. The workspace id, the entity name, the index name, and the row key are not repeated in each entry and in each index of the table.

`field_deltas` and `consumer_shadows` hold ids in the same way. Each entry holds the workspace number, and an id that `text_fields` gives each text field of an entity, or an id that `shadow_sets` gives the shadows of one consumer in one entity. These entries keep the row key as text.

`pgstore` writes the index entries of the rows of a transaction with one statement for each 500 rows. A statement has a fixed cost: PostgreSQL plans it, locks the tables it reads, and starts it. One statement for the rows of a transaction pays this cost one time. One statement for each row pays it for each row.

On a partitioned layout, `pgstore` makes PostgreSQL plan each statement on the index tables with the workspace number of the transaction. PostgreSQL then locks only the partition of that workspace. A plan that PostgreSQL makes one time and uses for all workspaces locks every partition on each use. An exclusive lock on one partition, which some maintenance commands take, then stalls the index writes of all workspaces.

The ids in `index_scopes` are 32-bit integers from one sequence, which the search indexes share. A workspace takes one id, and one more for each entity and each index that holds entries. An id is not used again, so a database can give out 2^31 ids in its life. After that, a write that needs a new id fails.

Tables such as `workspaces`, `clients`, `text_floors`, `index_state`, `index_scopes`, `consumer_positions`, `cursor_state`, and `schemas` remain unpartitioned because they are small or scanned across workspaces.

## Search table growth under row churn

An entity that declares search fields and creates and deletes many rows makes the search tables larger than a new build of the same rows. This also occurs when the number of live rows stays the same. Where the extra bytes are depends on the workspaces:

- **Many small workspaces:** The extra bytes are dead tuples in `posting_pages`. A write to a page record stores the whole record again, and the old version uses space until a vacuum frees it. The table is about 1.3 times the size of a new build when autovacuum keeps up with the writes, and about 2.2 times that size when it does not.
- **One large workspace where most rows live a short time and some rows live long:** The search tables reach about 2.6 times the size of a new build. About three quarters of the extra bytes are in the two b-tree indexes of `posting_rows`, which are `posting_rows_pkey` and `posting_rows_by_num`. Each can be 9 times its size after a rebuild.

A PostgreSQL b-tree becomes sparse when its keys increase with time and most of the keys are deleted later. New entries go after the existing entries. A deletion leaves a leaf page with the few entries that stay, and no new entry belongs on that page. PostgreSQL uses a leaf page again only when the page is empty, and `VACUUM` does not merge pages that are partly full.

The row numbers in `posting_rows_by_num` only increase, so this index becomes sparse with all row keys. The keys in `posting_rows_pkey` increase only when the row keys of the application are ordered by time, such as UUIDv7 or ULID keys. With such keys, the primary key of `rows` becomes sparse in the same way. Rows that are deleted in the order of their creation cause almost no growth, because their leaf pages become empty.

A vacuum makes the space of dead tuples available to later writes. The table files do not shrink, but they stop growing. `REINDEX` writes a new index file and gives the bytes of the old file back to the file system. `VACUUM FULL` does the same for a table and its indexes, but it takes an `ACCESS EXCLUSIVE` lock.

`sqlitestore` needs no maintenance for this workload. SQLite uses freed pages again, and [Schema upgrades in `sqlitestore`](../reference/storage-adapter.md#schema-upgrades-in-sqlitestore) describes how to give them back to the file system.

To monitor the tables and rebuild the indexes, see [Maintain the search tables](../how-to/deploy-for-high-availability.md#7-maintain-the-search-tables).

## Per-workspace leases

Notifications reach every node. A write on any node wakes clients across all nodes. Different workspaces commit concurrently instead of serializing behind a single writer connection.

Background work divides across nodes through per-workspace leases. `pgstore` backs leases with PostgreSQL advisory locks. This design lets servers deduplicate background tasks without extra configuration:

- Garbage collection and index sweeps skip a workspace while another node holds its garbage collection lease.
- An effects runtime leases each `(consumer, workspace)` drain in the same way.
- Blob garbage collection takes a lease of its own per workspace. It deletes a blob under the workspace write lock, so two nodes that collect at once do not delete a blob that a row names.
- When a node crashes, its connection closes and releases held advisory locks immediately. This prevents lease timeout delays.
- Two holders on one node exclude each other. A node holds all of its leases on one connection. Because an advisory lock allows the holding connection to take the lock again, the store tracks held leases in memory and refuses a second holder. For the same reason, a caller that cancels its context never closes the connection. Closing the connection would release leases held by all other callers on that node.

Leases are advisory and do not guarantee single delivery:

- Delivery to an effect handler remains at-least-once.
- A rolling deployment can deliver a change to two processes.
- Effect handlers must remain idempotent.
- Connectors do not take leases. Connectors maintain state (such as cursors and shadows) in local storage, such as a SQLite file. You must run each connector in only one process to prevent instances from echoing writes.

## Best-effort notification delivery

Syncplane uses best-effort notification delivery so that a missed notification causes a delay of one poll interval rather than a lost write. `pgnotify` uses best-effort delivery in three cases:

- A full publish queue drops notifications instead of blocking the write path.
- PostgreSQL does not buffer notifications across a closed `LISTEN` connection. Notifications published while a listener reconnects are lost.
- Oversized payloads are skipped.

When the listener reconnects, it re-notifies every workspace with a live subscription. Unsubscribed workspaces are not re-notified on reconnect. Background consumers rely on periodic sweeps as a backstop.

`pgpresence` handles reconnections differently:

- Presence events cannot be reconstructed from empty notifications.
- Instead of re-notifying on reconnect, each node re-announces its active records at the interval configured by `WithRoomAnnounce`. Rosters on other nodes remain stale for at most one interval.
- Broadcast messages sent during a connection gap are dropped.
- Payloads exceeding the 8000-byte `NOTIFY` limit return an error at `Publish`. Keep application state and broadcast messages within `MaxPresenceStateBytes` and `MaxBroadcastBytes`.

Presence updates fold state per `(topic, session, subject)` in the publish queue before sending to the database, which reduces `pg_notify` round trips. A fold keeps the highest revision, not the latest arrival: a node re-announces a record after it releases the lock it read the record under, so an older revision can arrive after a newer one. Broadcast messages are not folded.
