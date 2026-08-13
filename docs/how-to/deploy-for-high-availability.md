# Deploy for high availability

[Run more than one node](run-multiple-nodes.md) gets you a fleet. This gets that
fleet through the things that take a fleet down: a node dying mid-stream, a
database failover, a rolling deploy.

## What holds state

Only one tier does. A sync node scans the shared store for the changes it
serves, and the notifier carries a content-free "seq advanced" signal rather
than the rows — so a node that dies took nothing with it, and a client that
reconnects elsewhere resumes from its cursor. Leases are held on a Postgres
session, so they are released by the connection dropping.

That leaves three tiers with three different answers:

| Tier       | Replicas | Losing one costs                       |
| ---------- | -------- | -------------------------------------- |
| Sync nodes | N        | a reconnect                            |
| PostgreSQL | HA pair  | writes for the failover window         |
| Connectors | 1        | external convergence until it restarts |

## Sync nodes

Every node is interchangeable. Any of them serves any workspace, and background
work — GC, index builds, effect drains — spreads across them by per-workspace
lease with no configuration.

**Do not configure session affinity.** It concentrates load and buys nothing;
a client that lands on a different node after a reconnect picks up where it
left off.

**Set the load balancer's idle timeout above the heartbeat.** `Subscribe`
streams are long-lived and quiet workspaces send nothing between heartbeats
(`WithHeartbeat`, default 30s). An LB that closes a 60s-idle connection turns
every quiet workspace into a reconnect loop. Cap stream age with a max
connection lifetime instead, so rebalancing happens when you choose.

**Serve h2c if a page may run several `SyncClient`s against one host.**
`Subscribe` multiplexes a client's workspaces onto one connection, so the
browser's ~6-connection HTTP/1.1 cap only binds when one page holds several
clients — which means several accounts.

**Drain on SIGTERM.** `Close` stops the GC goroutine, the index backfill, the
room registry's announce sweep and the notify-coalescing timers. Stopping the
timers is what keeps a shared notifier from receiving a publish after the
process announced it was done, and closing the registry likewise stops it
announcing presence for sessions this process no longer serves — so the rooms
it was holding empty out on the other nodes within one announce interval rather
than showing ghosts. It
does not close what you own:

```go
sigs := make(chan os.Signal, 1)
signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)
<-sigs

httpServer.Shutdown(drainCtx) // let open streams close
mgr.Close()
notifier.Close()
bus.Close() // when the app uses rooms
store.Close()
```

## PostgreSQL

Run a primary with automatic failover — Patroni, or whatever your platform
calls its HA pair. Two properties of the adapter shape what that can do for
you.

**Standbys are for failover, not for read scaling.** The adapter takes one DSN
and one pool, and read transactions run `REPEATABLE READ READ ONLY` against the
primary.

**A failover is an error, not a corruption risk.** `pgstore` retries
serialization failures and deadlocks, so an in-flight transaction cut short by
a failover surfaces as an RPC error and the client retries it. Writes are
already serialized per workspace by an advisory transaction lock, and no node
holds state that has to be reconciled afterwards.

### Connection budget

Each node opens:

| Connections    | What for                                                                         |
| -------------- | -------------------------------------------------------------------------------- |
| `WithMaxConns` | the `pgstore` pool; a write holds one while waiting on its workspace's lock      |
| 1              | the lease session, dialled on first use and shared by every app namespace        |
| 1              | `pgnotify`'s `LISTEN` connection                                                 |
| 1              | `pgnotify`'s publisher pool — sized by pgx's default but driven by one goroutine |
| 1              | `pgpresence`'s `LISTEN` connection, when the app uses rooms                      |
| 1              | `pgpresence`'s publisher pool, likewise                                          |

Size `WithMaxConns` against your `max_connections` divided by node count, minus
the fixed connections above, and leave headroom for the standby's own clients.
Neither `pgstore` nor `pgnotify` exposes a pool to share, which is why the
presence bus adds its own pair rather than reusing one.

### Connection poolers

**Do not put a transaction-pooling proxy in front of any of this.** Three paths
depend on a session outliving a transaction, and PgBouncer's `transaction` mode
breaks each of them differently:

- **Leases** are Postgres _session_ advisory locks. They stop excluding
  anything, so every node runs every sweep and every effect fires on every node.
- **`LISTEN`** is bound to the session that issued it, so pokes stop arriving
  and clients converge at poll latency.
- **Migration at boot** takes a session-scoped advisory lock and holds it across
  the transactions that apply the migrations. Spread those over different server
  connections and two booting nodes migrate the same database concurrently —
  which is the one thing that lock exists to prevent.

None of the three fails loudly. Run with no pooler (the engine already pools) or
with `pool_mode = session`.

Deployments sharing one database server stay out of each other's lease space by
schema: the lock key hashes `(schema, namespace, role, workspace)`, so separate
`search_path`s are enough. Give them separate notify channels too — see
[Run more than one node](run-multiple-nodes.md#isolating-deployments-that-share-a-database).

## Connectors run alone

A connector keeps its cursors and shadows in connector-owned state, not in the
engine's store, and nothing leases a workspace to one connector. Two instances
converging one workspace both egress, and each sees the other's writes as
external changes.

So run connectors as **their own deployment, one replica, recreate rather than
rolling**. `Close` waits for in-flight passes, which narrows the overlap with an
incoming process without closing it. Keeping them out of the sync fleet is what
lets that fleet scale.

## Registration does not propagate

If your apps register their schema at runtime over `AdminService`, the node that
served the call updates its own `Manager` and persists the descriptor set; every
other node picks it up **when it next boots**. Register as a deploy step and
restart the fleet behind it, or accept that the new schema is live on one node
until the next roll.

## What each failure costs

**A node dies.** Its connections drop, releasing every lease it held at once —
there is no TTL to wait out before another node takes over its sweeps. Its
clients reconnect elsewhere and resume from their cursors; writes that were
in flight are still in each client's pending queue, and mutation-id watermarks
make the re-push idempotent.

**Postgres fails over.** Writes fail for the window and clients retry. Pokes
published into it are lost, since Postgres buffers nothing across a dropped
`LISTEN`; on reconnect the listener re-pokes every workspace it holds a live
subscription for, and the periodic sweep is the backstop for the rest. The cost
is one poll interval of latency, never a write.

**You roll the sync tier.** Two things to have planned for. Effect handlers must
be idempotent — a lease makes single delivery the common case, and a rolling
deploy is exactly the window where one change reaches two processes. And clients
built against the old schema meet nodes running the new one, which is what the
[schema-revision handshake](handle-failures.md#handle-a-version-skew-during-a-deploy)
makes visible instead of a silent stream of dropped fields.

## Checklist

- `pgstore` **and** `pgnotify` — one without the other is a half-deployment, and
  `pgpresence` too if the app uses rooms (an in-process bus splits every roster
  across nodes, and unlike pokes it never converges)
- No session affinity; LB idle timeout above `WithHeartbeat`; h2c if a page runs
  several clients
- SIGTERM → HTTP drain → `Close` the manager, then the notifier and bus, then
  the store
- `WithMaxConns` sized against `max_connections`, plus the fixed connections per
  node
- No transaction-mode pooler anywhere in front of the engine
- Connectors in their own deployment at one replica
- Override `CanAccessWorkspace` — the default `AllowAll` leaves a shared
  deployment with no tenant isolation and nothing says so at compile time
- Alert on the four signals in
  [Observe the engine](observe-the-engine.md#what-to-alert-on)
