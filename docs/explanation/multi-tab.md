# Multi-tab behavior

This document explains how multiple tabs in the same browser coordinate shared storage, mutation ID allocation, and network streams without communicating through the server.

Tabs of an application are not independent clients. They share one IndexedDB database and one `clientId` per account. Every workspace for that account lives in this single store. Tabs coordinate through browser primitives rather than through the server.

## One store, one lock

A Web Locks mutex guards key-value (KV) writes, including mutation ID minting. This lock prevents concurrent tabs from minting duplicate IDs.

The server requires gap-free mutation IDs per `(workspace, client_id)`. If two tabs mint IDs independently, they break contiguity for their shared client ID.

Environments without `navigator.locks`, such as Node.js and test environments, fall back to an in-process mutex.

## One stream per plane, held by a visible tab

One visible tab holds the live `Subscribe` stream, elected through a second Web Lock. Election occurs once per account. The stream carries updates for every workspace that any open tab needs.

The leader tab writes received rows into the shared IndexedDB store. Follower tabs do not run data streams of their own. Instead, they observe writes through the shared store and receive notifications over a `BroadcastChannel` that carries the touched `(workspace, entity)` set.

The leader tab releases the lock on `visibilitychange` to hidden after a short grace period, on `pagehide`, and on `freeze`. Tabs recontend for leadership on `visible` and `resume` events. As a result, the lock is held by a visible tab or by no tab at all. When all tabs are hidden, tabs fall back to the per-tab polling floor. This behavior ensures that a suspended background tab never strands the lock.

If the lock request fails because Web Locks is unusable, such as in sandboxed or storage-partitioned embedded environments, the tab stops contending, logs the error, and runs its own stream for the session. An extra connection avoids missing live updates.

## Two leaders, not one

An application using [rooms](rooms.md) maintains a second long-lived stream per account: `Watch`, which serves the ephemeral plane. A separate Web Lock elects this leader. The data stream leader and the room stream leader operate independently and do not need to be the same tab.

The leaders are separate because the two planes have different requirements. A room requires a stream to function, whereas the data stream is optional (`streamEnabled: false` is a supported configuration and the default in the integration test suite). Rooms cannot rely on the data stream lifecycle. Coupling the two streams would also mean a failure in one plane disqualifies a tab from leadership contention in the other plane for the rest of the session.

Only receiving updates requires a leader. Because `Publish` is unary, every tab writes its own presence directly to the server. The elected leader maintains one `Watch` stream that covers the union of rooms across all tabs and forwards frames across a `BroadcastChannel`.

The union of rooms is a live census rather than durable state. Tabs announce their room sets when changes occur, when requested by a newly elected leader, and periodically every few seconds. The leader aggregates these announcements, debounces changes, and reopens `Watch` when the room set changes. If the leader stops receiving announcements from a tab, it drops that tab from the union. This matches the soft-state model that the server applies to sessions and prevents cursors from lingering after a tab crashes.

The leader includes the session IDs of its follower tabs in the request. The server maintains presence records only while watching the stream associated with those sessions. Without follower session IDs in the request, presence shows only the tab that holds the lock.

Leader handovers are transparent to other clients. The outgoing leader publishes no departure event, and the incoming leader establishes its stream before the expiration window elapses.

The fallback behavior matches the data stream. In environments without Web Locks or `BroadcastChannel`, such as older browsers or server runtimes, every tab runs its own `Watch` stream. This approach increases connection overhead while preserving correct behavior.

## Scope is the union, coverage is your own

The coordinator synchronizes the union of subscriptions across all active tabs. As a result, a tab can hold rows in storage that it did not explicitly request.

Coverage calculations evaluate against each tab's declared scope rather than the union. When the union is wider than the tab's own scope, own-scope coverage can report less completeness than the union. This design prevents coverage status from changing when peer tabs open or close. `status().scope` reports `own`, `effective`, and a boolean that indicates whether the scope was `widened`.

## Why this tier has its own tests

Multi-tab coordination primitives cannot be mocked accurately outside a browser. `navigator.locks` and `BroadcastChannel` determine at runtime whether the client coordinates or operates independently. Node.js has no shared lock manager. Outside a browser, the client falls back to an in-process mutex, declines stream leadership, and leases peers locally. Multi-tab claims are therefore covered only in this tier.

To verify these behaviors, tests run in a dedicated browser tier in `client/test/e2e` (`task test:e2e`). `task test:e2e` is not part of `task ci`. It drives a real browser, which `task test:e2e:install` downloads once (approximately 100 MB). This tier executes against real browser tabs, a single IndexedDB store, and real Web Locks.

The browser tier covers behavior that only a browser can execute:

- Cross-tab change sharing
- Stream leadership and handover when a tab hides
- Mutation ID minting across tabs
- Rooms sharing one `Watch` stream across an account's tabs
- An IndexedDB store surviving the tab that created it

Specs drive a JSON-only `window.harness`. Tests assert invariants that no tab can observe against the test server's `/debug/rpc` counters.

Collaborative text has the largest share of the browser tier because every part of the feature depends on multi-tab coordination. Writer identities are leased from a device-wide pool over Web Locks, and two tabs of one application share a store, a queue, and a `clientId`. The specs cover:

- Live handles typing at once and a second device reaching the same document
- String writes meeting a typist
- The wire cost of an edit in both directions, asserted against the server byte counters
- A live handle healing when the server compacts the field under it

[Rooms](rooms.md) use the same multi-tab pattern. The account holds one `Watch` stream regardless of how many tabs and rooms are open, elected on its own Web Lock and shared over a `BroadcastChannel`. The specs prove that:

- Exactly one stream is open across three tabs.
- A settled account stops reopening the stream.
- A follower's rooms and presence reach the server through the leader.
- Closing the leader tab hands the stream over without other peers observing the account leaving.

For more information, see [Testing](../how-to/testing.md).

## Related

- [Construction and lifecycle](../reference/typescript-client.md#construction-and-lifecycle)
