# Rooms: the ephemeral plane

This document explains why syncplane separates ephemeral state from the durable mutation log, how rooms manage transient presence and broadcast messages, and the architectural trade-offs involved. syncplane converges durable state through a server-linearized, per-workspace mutation log (see [the convergence model](convergence-model.md)). Rooms provide a second plane alongside the log for transient state that matters only while users are connected, such as cursors, selections, viewing rosters, typing indicators, and token streams.

The ephemeral plane does not store, replay, or acknowledge state. The room architecture follows this design principle.

## The dividing test

To decide whether state belongs in a table row or in a room, ask one question:

> Would a reader arriving later want this?

If yes, the state belongs in a row. A comment, a document title, and a completed checkbox are data that a user opening the page later expects to find. These updates go through `mutate` and receive every guarantee the mutation log provides.

If no, the state belongs in a room. Your cursor position is useless to someone who arrives after you leave. A typing indicator that outlives typing activity is worse than no indicator.

## Why the mutation log is the wrong home

Routing cursor positions and transient updates through the mutation log is inefficient and conflicts with log design. In the mutation log, every update would:

- **Burn a workspace sequence number.** A document with five users moving cursors advances the sequence faster than the application writes durable data.
- **Drag every subscriber cursor forward.** Every subscriber must reread a position that carries no lasting value.
- **Require storage until garbage collection (GC).** Storage retention must be sized for pointer movements rather than durable edits.
- **Serialize against durable writes.** A workspace commits one write transaction at a time, so cursor updates block durable writes.

The ephemeral plane avoids these costs. It never opens a write transaction, never touches storage, and never advances a sequence number.

## Addressing

A topic is `(workspace_id, name)`, optionally anchored to a row:

```ts
ws.room('lobby'); // unanchored
ws.room(DocumentSchema, 'doc-42', 'cursors'); // anchored to a row
```

The `workspace_id` is the hard partition key across syncplane. There are no cross-workspace rooms.

The anchor is an authorization mechanism, not a label. An anchored topic admits a caller only if the application `ReadFilter` allows them to read that row. A cursor room on a document inherits document permissions and requires no separate policy. Membership cannot be looser than the data it describes.

An unanchored topic requires workspace access, plus the application `RoomPermissions.CanJoinRoom` check if configured.

## Two primitives

The ephemeral plane provides two primitives:

- **Presence:** Soft state with one record per session per room. Every write replaces the record entirely using last-writer-wins based on a client-minted revision. A record is not deleted; it expires.
- **Broadcast:** An ephemeral message delivered only to current watchers when it arrives. It is not stored, replayed, or acknowledged.

## Expiry instead of deletion

A presence record has no reliable owner to remove it. A tab closes, a laptop lid shuts, or a process terminates without sending a disconnect message. The ephemeral plane therefore does not model departure as an event:

- Each record carries a `session_id` and a client-minted, monotonic `revision`.
- The node holding a session's `Watch` stream re-announces its records every announce interval (`WithRoomAnnounce`, default 5 seconds).
- A record that no node re-announces within three announce intervals is dropped locally and reported to watchers as `left`.
- Closing a room publishes an explicit departure. This makes the common case immediate instead of waiting for the timeout window.

A publisher without an open `Watch` stream—such as a client with `presenceEnabled: false` or a non-browser client that only writes—has no node re-announcing for it. Its records lapse three intervals after the last publish. To prevent this, `Publish` returns the announce interval so the client can re-publish its state on schedule. Re-publishing at the same revision is lightweight: the server treats an unchanged revision as a liveness heartbeat and fans out no frames.

This design runs with no time-to-live (TTL) table, no GC horizon, and no storage-adapter involvement. It also makes presence self-healing across lossy transport paths. The next announcement corrects an event dropped by a full queue, lost during a bus reconnect, or discarded by a slow watcher.

A browser shares one `Watch` stream across all tabs for an account. The stream names all sessions it is responsible for, not just its own. This prevents a room from dropping sessions from other tabs. For details, see [multi-tab behavior](multi-tab.md).

## At-most-once delivery and what it costs

Delivery carries no guarantee. Presence survives dropped messages because it re-announces state periodically. Broadcast does not survive dropped messages. A broadcast message reaches only current watchers. If a connection is reconnecting, the message is lost.

This behavior is a deliberate contract, not a limitation to work around. If an application must not lose a message, write a row to the mutation log. If you want the message to appear immediately, publish a broadcast alongside the mutation as a hint.

## Why presence is not synchronized with the data stream

The ephemeral plane and the durable data plane are independent streams with independent leadership. The system does not order a room frame against a mutation in either direction.

Because streams are independent, a peer cursor can reference a row or collaborative-text offset that you have not pulled yet. Presence records carry the workspace `seq` they were authored against. A recipient can compare this sequence number to determine whether they are behind or if the state is invalid, holding the state until the data stream catches up.

Send positions as [text anchors](collaborative-text.md) rather than integer offsets. An anchor stays attached to its character as concurrent edits move it. An integer offset changes meaning as soon as someone edits earlier text.

## Why `Watch` is a separate stream from `Subscribe`

The scope set for `Subscribe` is fixed for the life of the stream. Catch-up and the live tail run under a single scope set, avoiding delta re-synchronizations mid-stream. In contrast, room membership changes whenever a user navigates between documents.

Combining rooms into `Subscribe` would force a data-stream reconnect and full catch-up on every navigation. Reopening `Watch` has low cost: it has no cursor, requires no backlog replay, and immediately returns the current room snapshot.

The trade-off is one additional long-lived connection per account. For details, see [limits and trade-offs](limits-and-trade-offs.md#browser-connection-limits).

## What is not guaranteed

The ephemeral plane does not provide these guarantees:

- **Cross-topic ordering:** No ordering exists across topics or between room frames and the mutation log.
- **History and replay:** The ephemeral plane retains no history, replay, or cursors. Use table rows for durable state.
- **Delivery guarantees:** Messages are delivered at most once.
- **Global roster truth:** A roster represents a single node view, combining local session streams with bus updates. A record published elsewhere arrives within one announce interval. Single-node deployments do not observe this difference.
- **Frequency rate limiting:** Beyond the size and count limits in `Limits`, the plane enforces no message frequency limits. Message sizes remain bounded.

## Multi-node deployments

The default in-process presence provider (`presence.NewLocal`) reaches only one process. In a multi-node deployment, this default only shows clients the participants connected to their specific node. To share presence across nodes, configure `server.WithPresenceBus`. For example, `engine/presence/pgpresence` provides a bus implementation over PostgreSQL `LISTEN` and `NOTIFY`. For instructions, see [run more than one node](../how-to/run-multiple-nodes.md).

The presence bus has three lossy paths: a full publish queue, a reconnect gap, and a payload exceeding the `NOTIFY` size limit. The first two conditions are transient and self-heal on the next announce interval. The third condition fails immediately at `Publish` so the caller detects that the payload is too large.
