# Rooms: the ephemeral plane

syncplane converges durable state through a server-linearized, per-workspace
mutation log — see [the convergence model](convergence-model.md). Rooms are a
**second plane** alongside it, carrying the state that only matters while
somebody is connected: cursors, selections, "who is viewing this", typing
indicators, an agent's in-flight token stream.

Nothing on this plane is stored, replayed or acknowledged. That is the whole
design, and everything below follows from it.

## The dividing test

> Would a reader arriving later want this?

If yes, it is a row. A comment, a document title, a completed checkbox — all
things someone opening the page tomorrow expects to find. They go through
`mutate` and get every guarantee the log provides.

If no, it is a room. Where your cursor is right now is worthless to somebody who
arrives after you left; a "user is typing" indicator that outlives the typing is
worse than no indicator at all.

## Why the mutation log is the wrong home

A cursor position routed through the log is not merely inefficient, it is wrong
on the log's own terms. Every update would:

- **burn a workspace sequence number**, so a document with five people moving
  their mice advances the sequence faster than the app is actually written to;
- **drag every subscriber's cursor forward**, making each of them re-read a
  position that carries nothing they can use;
- **be stored until GC**, so retention has to be sized for pointer movements
  rather than for edits;
- **serialize against real writes**, because a workspace commits in one write
  transaction at a time and a cursor update is a write.

The ephemeral plane costs none of that: it never opens a write transaction,
never touches storage, and never advances a sequence.

## Addressing

A topic is `(workspace_id, name)`, optionally **anchored** to a row:

```ts
ws.room('lobby'); // unanchored
ws.room(DocumentSchema, 'doc-42', 'cursors'); // anchored to a row
```

`workspace_id` is the hard partition key here exactly as it is everywhere else —
there are no cross-workspace rooms.

The anchor is an authorization mechanism, not a label. An anchored topic admits
a caller **only if the app's `ReadFilter` would let them read that row**, so a
cursor room on a document inherits the document's permissions and needs no
policy of its own. Membership can never be looser than the data it is about,
which is the property that would otherwise have to be re-established by hand for
every room an app invents.

An unanchored topic is gated on workspace access, plus the app's own
`RoomPermissions.CanJoinRoom` if it installs one.

## Two primitives

**Presence** is soft state: one record per session per room, replaced whole on
every write, last-writer-wins by a revision the publisher mints. A record is not
deleted, it **expires**.

**Broadcast** is an ephemeral message delivered to whoever is watching at the
moment it arrives. Not stored, not replayed, not acknowledged.

## Expiry instead of deletion

A presence record has no owner that can be relied upon to remove it. A tab is
closed, a laptop lid comes down, a process is killed — none of those send
anything. So the plane does not model departure as an event at all:

- Each record carries a `session_id` and a client-minted, monotonic `revision`.
- The node holding a session's `Watch` stream **re-announces** its records every
  announce interval (`WithRoomAnnounce`, default 5s).
- A record nobody has re-announced within **three announce intervals** is
  dropped locally and reported to watchers as `left`.
- Closing a room publishes an explicit departure, so the common case is
  immediate rather than waiting out the window.

A publisher holding no stream anywhere — `presenceEnabled: false`, or a
non-browser client that only writes — has nobody re-announcing for it, so its
records would lapse three intervals after its last publish. `Publish` returns
the announce interval for exactly that reason, and the client re-publishes its
own state on it. Re-publishing at the _same_ revision is what makes this cheap:
the server takes an unchanged revision as liveness and fans no frame out.

This is what lets the plane run with **no TTL table, no GC horizon and no
storage-adapter involvement**. It also makes every lossy path in the transport
self-healing for presence: an event dropped by a full queue, lost to a bus
reconnect, or discarded by a watcher that was not reading is corrected by the
next announcement.

A browser shares one `Watch` stream across an account's tabs, so the stream
names the sessions it is responsible for and not only its own — otherwise a
room would empty out to whichever tab happened to hold the lock. See
[multi-tab behaviour](multi-tab.md).

## At most once, and what that costs

Delivery carries no guarantee. Presence survives that because it is re-announced
rather than replayed. **Broadcast does not.** A message reaches whoever is
watching at that moment and nobody else; if the connection was between reconnects,
it is gone.

That is the primitive's contract, not a limitation to be worked around. An app
that cannot lose a message writes a row and lets the log deliver it — and, if it
wants the message to feel live, publishes a broadcast alongside as a hint.

## Why presence is not synchronized with the data stream

The two planes are independent streams with independent leadership. Nothing
orders a room frame against a mutation, in either direction.

That means a peer's cursor can name a row — or a position inside a
collaborative-text field — that you have not pulled yet. Presence records
therefore carry the workspace `seq` they were authored against, so a recipient
can tell "I am behind" from "this state is nonsense" and hold the state until it
catches up.

Positions should travel as [text anchors](collaborative-text.md), not integer
offsets: an anchor stays attached to the character it was placed at as
concurrent edits move it, while an offset silently means something else the
moment anyone types above it.

## Why `Watch` is a separate stream from `Subscribe`

`Subscribe`'s scope set is fixed for the life of the stream, deliberately: the
catch-up and the live tail both run under one set, so nothing has to delta-resync
mid-stream. Room membership, by contrast, changes every time a user opens a
document.

Folding rooms into `Subscribe` would therefore force a data-stream reopen — and
its catch-up — on every navigation. Reopening `Watch` costs nothing comparable:
there is no cursor, no backlog, and a snapshot is what a joiner wants anyway.

The cost is one more long-lived connection per account. See
[limits and trade-offs](limits-and-trade-offs.md#browser-connection-limits).

## What is not guaranteed

- **No ordering across topics**, and none between a room frame and the mutation
  log.
- **No history, replay or cursors.** Anything that needs them is a row.
- **No delivery guarantee**, per the contract above.
- **A roster is a node's view, not global truth.** A node knows the sessions
  whose streams it holds plus whatever the bus has told it; a record published
  elsewhere arrives within one announce interval. Single-node deployments
  cannot observe the difference.
- **No rate limiting** beyond the size and count caps in `Limits`. A chatty
  client is bounded by message size, not by frequency.

## Multi-node

The in-process default (`presence.NewLocal`) reaches only one process, so a
two-node deployment on the default shows each client the participants its own
node happens to be serving. `server.WithPresenceBus` replaces it;
`engine/presence/pgpresence` is one over Postgres `LISTEN`/`NOTIFY`. See
[run more than one node](../how-to/run-multiple-nodes.md).

The bus has three lossy paths — a full publish queue, a reconnect gap, and a
payload over the NOTIFY ceiling. The first two are silent and heal within one
announce interval for presence; the third is refused at `Publish` so the caller
learns its state is too large rather than watching it vanish everywhere but its
own node.
