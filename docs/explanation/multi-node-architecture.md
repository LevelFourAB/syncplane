# Multi-node architecture

This document explains how syncplane coordinates operations across multiple nodes, isolates failure across tiers, manages database sessions and leases, and handles best-effort notification delivery.

To configure and operate a multi-node deployment, see [Run multiple nodes](../how-to/run-multiple-nodes.md) and [Deploy for high availability](../how-to/deploy-for-high-availability.md).

## State tiers and failure impact

Only PostgreSQL holds persistent state in a syncplane deployment. Sync nodes scan the shared store and send content-free sequence advancement signals through the notifier. Leases depend on active PostgreSQL sessions and release as soon as the connection drops.

| Tier       | Replicas | Failure impact                                          |
| ---------- | -------- | ------------------------------------------------------- |
| Sync nodes | N        | Clients reconnect to another node.                      |
| PostgreSQL | HA pair  | Writes pause during the failover window.                |
| Connectors | 1        | External convergence pauses until the process restarts. |

Failure handling behaves as follows:

- **Sync node failure:** When a node stops, its database connections drop and release all held session leases immediately. Clients reconnect to another node and resume from their cursor position. Unacknowledged writes remain in the client queue and retry idempotently.
- **Database failover:** In-flight transactions return errors and clients retry. `pgstore` retries serialization failures and deadlocks. Dropped `LISTEN` connections cause the listener to re-query active subscriptions upon reconnect, backed up by periodic sweeps.
- **Rolling deployment:** Effect handlers must be idempotent to handle concurrent executions across versions. The schema-revision handshake prevents client-server version mismatches.

## Schema registration across nodes

A `server.Manager` keeps the schema of each app in the store's catalog. Each node also holds the schema it serves in memory. Each admin `Register` call reaches one node, so the nodes can serve different schemas for a short time.

Two rules keep the nodes from undoing each other's registrations:

- **The catalog is the base of every merge.** A registration reads the stored schema, serves it first if it is newer than the node's own, and merges the new set over it. The access-change check then compares against the stored schema. A node that missed a narrowed role therefore cannot widen it again without `AllowAccessChange`.
- **The write is a compare-and-swap.** The node stores the merged schema only when the stored revision is still the one it merged over. When another node wrote in between, the node reads again and merges again. The app serves the merged schema only after the write succeeds, so the memory of a node never runs ahead of the store.

The other nodes learn of a new schema from a poke on the shared notifier, and from a periodic read of the catalog for the pokes that get lost. A node serves a schema that another node stored without checking the access change again, because the node that stored it already checked the change against the stored schema.

Until a node has the new schema, it serves the previous one. After a role is narrowed, a node can still send rows under the old role for up to one poll interval. The poke shortens this window to the notifier's delivery latency.

## Connection pooler restrictions

Running PgBouncer or similar connection poolers in `transaction` mode breaks three core syncplane mechanisms:

- **Leases:** Leases rely on PostgreSQL session advisory locks. In transaction pooling mode, locks do not persist across transactions.
- **`LISTEN` notifications:** `LISTEN` binds to a specific database session. In transaction mode, notifications fail to reach listeners.
- **Startup migrations:** Schema migrations hold session-scoped advisory locks across transactions. Transaction pooling allows concurrent migrations from multiple booting nodes.

## Read replica semantics

Replica reads provide consistent snapshots bounded by workspace sequence floors (`last_seq` cursor position, client `min_seq`, or primary sequence). If a read replica lags behind the requested sequence floor, `pgstore` falls back to the primary database.

## Table layout and partitioning

Partitioning five core tables (`rows`, `field_deltas`, `idempotency`, `index_entries`, and `consumer_shadows`) improves autovacuum maintenance by isolating dead tuples into smaller concurrent tasks.

Tables such as `workspaces`, `clients`, `text_floors`, `index_state`, `consumer_positions`, `cursor_state`, and `schemas` remain unpartitioned because they are small or scanned across workspaces.

## Per-workspace leases

Notifications reach every node, allowing a write on any node to wake clients across all nodes. Different workspaces commit concurrently instead of serializing behind a single writer connection.

Background work divides across nodes using per-workspace leases. `pgstore` backs leases with PostgreSQL advisory locks, allowing servers to deduplicate background tasks without extra configuration:

- Garbage collection and index sweeps skip workspaces that another node is actively collecting.
- An effects runtime leases each `(consumer, workspace)` drain in the same way.
- A crashing node releases held advisory locks when its connection closes, which eliminates lease timeout delays.
- Two holders on one node exclude each other in the same way. A node holds all of its leases on one connection, and an advisory lock lets the connection that holds it take it again, so the store also keeps a list of the leases it holds and refuses a second holder itself. For the same reason, a caller that gives up (its context is cancelled) never closes that connection, because closing it would release the leases of every other holder on the node.

Leases are advisory and do not guarantee single delivery:

- Delivery to an effect handler remains at-least-once.
- A rolling deployment can deliver a change to two processes.
- Effect handlers must remain idempotent.
- Connectors do not take leases. Connectors maintain state (such as cursors and shadows) in local storage, such as a SQLite file. You must run each connector in only one process to prevent instances from echoing writes.

## Best-effort notification delivery

Syncplane uses best-effort notification delivery so that a missed notification causes a delay of one poll interval rather than a lost write. `pgnotify` uses best-effort delivery in three cases:

- A full publish queue drops notifications instead of blocking the write path.
- Notifications published while a listener reconnects are lost, because PostgreSQL does not buffer notifications across a closed `LISTEN` connection.
- Oversized payloads are skipped.

When the listener reconnects, it re-notifies every workspace with a live subscription. Unsubscribed workspaces are not re-notified on reconnect, so background consumers rely on periodic sweeps as a backstop.

`pgpresence` handles reconnections differently:

- Presence events cannot be reconstructed from empty notifications.
- Instead of re-notifying on reconnect, each node re-announces its active records at the interval configured by `WithRoomAnnounce`. Rosters on other nodes remain stale for at most one interval.
- Broadcast messages sent during a connection gap are dropped.
- Payloads exceeding the 8000-byte `NOTIFY` limit return an error at `Publish`. Keep application state and broadcast messages within `MaxPresenceStateBytes` and `MaxBroadcastBytes`.

Presence updates fold state per `(topic, session, subject)` in the publish queue before sending to the database, which reduces `pg_notify` round trips. A fold keeps the highest revision, not the latest arrival: a node re-announces a record after it releases the lock it read the record under, so an older revision can arrive after a newer one. Broadcast messages are not folded.
