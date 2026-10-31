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

## Table layout and partitioning

Partitioning five tables (`rows`, `field_deltas`, `idempotency`, `index_entries`, and `consumer_shadows`) improves autovacuum maintenance by isolating dead tuples into smaller concurrent tasks.

Tables such as `workspaces`, `clients`, `text_floors`, `index_state`, `consumer_positions`, `cursor_state`, and `schemas` remain unpartitioned because they are small or scanned across workspaces.

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
