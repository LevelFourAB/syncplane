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

**Standbys are for failover — and, opted in, for read scaling.** By default the
adapter takes one DSN and one pool, and every read runs `REPEATABLE READ READ
ONLY` against the primary. `pgstore.WithReadReplica(dsn)` (`syncplaned
-db-replica`) adds a second pool pointed at a hot standby and moves the heavy
read paths — bootstrap snapshots, pulls, subscribe catch-ups, change-feed
drains, query scans — onto it.

A replica read is a _time-shifted read_: the same consistent snapshot the
primary would have served a moment ago, so convergence is never at stake —
only freshness is, and the engine bounds it per read with a workspace-sequence
floor. Each read names the lowest `last_seq` it may be served from — a pull's
cursor position, the client's highest acked seq (carried on the wire as
`min_seq`, so a client is never shown a snapshot older than its own acked
writes), or the primary's current seq for bootstrap and query. The adapter
checks the floor inside the already-open replica transaction and falls back to
the primary when the replica is behind. A lagging replica therefore costs
primary load, never correctness.

**A failover is an error, not a corruption risk.** `pgstore` retries
serialization failures and deadlocks, so an in-flight transaction cut short by
a failover surfaces as an RPC error and the client retries it. Writes are
already serialized per workspace by an advisory transaction lock, and no node
holds state that has to be reconciled afterwards.

**Standby query cancellation surfaces the same way.** A long replica read — a
bootstrap snapshot, a feed drain — conflicts with WAL replay once it holds a
snapshot the primary has vacuumed past, and the standby cancels it (SQLSTATE
57014) when `max_standby_streaming_delay` runs out. The error reaches the
caller mid-stream and the client retries, exactly as it does across a
failover. `hot_standby_feedback = on` prevents the conflicts by making the
primary retain the dead rows the standby's snapshots still see — at the price
of that bloat accumulating on the primary for as long as long replica reads
run.

### Connection budget

Each node opens:

| Connections           | What for                                                                    |
| --------------------- | --------------------------------------------------------------------------- |
| `WithMaxConns`        | the `pgstore` pool; a write holds one while waiting on its workspace's lock |
| `WithReplicaMaxConns` | the replica pool, against the standby; only with `WithReadReplica`          |
| 1                     | the lease session, dialled on first use and shared by every app namespace   |
| 1                     | `pglisten`'s `LISTEN` connection, parked for the process's lifetime         |
| 1                     | `pglisten`'s publisher, dialled on the first notification                   |

The last two are the whole fan-out budget, not one pair per plane: `pgnotify`
and `pgpresence` register different channels on the same `pglisten.Conn`, so a
node running both pays for two connections rather than four. Handing each its
own DSN instead — `pgnotify.Open` and `pgpresence.Open` — doubles the pair.
Neither connection is pooled, so neither needs sizing.

Size `WithMaxConns` against your `max_connections` divided by node count, minus
the fixed connections above, and leave headroom for the standby's own clients.

With a replica configured the two pools size against different loads. The
replica pool carries the slowest concurrent reads — a bootstrap snapshot or a
feed drain holds its connection for the whole streamed read — so size
`WithReplicaMaxConns` by how many of those run at once, against the standby's
own `max_connections`. That work leaves the primary pool, which then sizes by
write rate plus two smaller read shares: the anchor point-reads (one statement
per bootstrap and query, no held connection) and the fallback share — every
read a lagging or unreachable replica sends back. The fallback share is
visible as `syncplane.replica.reads` with `read_route` `fallback_stale` or
`fallback_error`; a sustained rise means the primary pool is quietly carrying
read load you budgeted to the replica.

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

The replica DSN is the one place transaction pooling is tolerable: the replica
pool runs only single-transaction read-only work — no advisory locks, no
`LISTEN`, never migrations. The primary's rules are unchanged.

Deployments sharing one database server stay out of each other's lease space by
schema: the lock key hashes `(schema, namespace, role, workspace)`, so separate
`search_path`s are enough. Give them separate notify channels too — see
[Run more than one node](run-multiple-nodes.md#isolating-deployments-that-share-a-database).

### Table layout

Five tables hold every tenant's data — `rows`, `field_deltas`, `idempotency`,
`index_entries` and `consumer_shadows` — and by default each is one heap shared
by every workspace in the database. `pgstore.WithPartitions(32)` (`syncplaned
-db-partitions 32`) hash-partitions them by `(namespace, workspace_id)`
instead.

**Decide it when you create the database.** The option is honoured only by the
migration that creates the schema; afterwards `Open` and `Migrate` adopt the
layout they find, so a deploy step chooses once and every node boots without
the flag. Converting a database that already holds data is not something the
option can do.

The modulus must be a power of two, and 32 is a reasonable starting point. The
power-of-two rule is what keeps a later split available: Postgres requires
every hash modulus to divide the largest in the set, so one modulus-32
partition can be detached and two modulus-64 partitions attached in its place —
a per-partition rewrite instead of a whole-table one.

What it buys is **maintenance granularity**. GC's tombstone churn makes
autovacuum the operational story on a large deployment; partitioned, it runs as
N short concurrent jobs instead of one long serial one, dead tuples stay inside
one tenant's partition rather than interleaved through index pages every tenant
scans, and an index rebuild runs per partition rather than pinning the
cluster's xmin horizon for hours while every other table bloats.

What it does not buy: read latency (B-tree depth is logarithmic — 32 partitions
saves roughly one page fetch per lookup), a bound on the largest partition
(hash spreads workspaces evenly, not bytes, so one whale tenant still lands
whole in one partition), or cheap tenant deletion. No query changes either way:
every statement already binds `namespace` and `workspace_id` to equality, so
pruning reaches a single partition, and a row never moves between them.

Two operational consequences:

- **Schedule `ANALYZE`.** Autovacuum does not analyze a partitioned parent, so
  its planner statistics go stale unless something runs `ANALYZE` on a
  schedule.
- **Adding an index later is not online.** `CREATE INDEX` on a partitioned
  parent cannot be `CONCURRENTLY`, so a migration that adds one holds a write
  lock for the build. Own the migrations (below) if you need to build it per
  partition instead.

The rest of the tables are left whole on purpose: `workspaces` is scanned
_across_ workspaces by the passes that look for work and has nothing to prune
on, and `clients`, `text_floors`, `index_state`, `consumer_positions`,
`cursor_state` and `schemas` are small enough that the partition count would
cost more than it buys.

### Owning the migrations

`Open` applies whatever the ledger is missing at boot. For a fleet that would
rather run its DDL as a deploy step, or read it before it runs, two entry
points take that over:

- `pgstore.Migrate(ctx, dsn, opts...)` applies the outstanding migrations and
  returns — the same work `Open` does at boot, on its own.
- `pgstore.Statements(opts...)` returns them as SQL, one script per ledger
  step, each ending with its own row in `schema_migrations`. Applying all of
  them to an empty database leaves one `Open` accepts as fully migrated, with
  nothing to stamp afterwards.

`Migrate`'s DSN falls under the pooler rule above: it holds a session-scoped
advisory lock across the transactions it applies, so it must not point at a
transaction-pooling proxy.

**Give the nodes `WithoutMigrations()`** (`syncplaned -db-migrate=false`), or
the step is advisory rather than authoritative: a bare `Open` applies whatever
the ledger is missing, so the first node to boot with a newer binary migrates
production itself. With the option, `Open` reads the ledger and refuses to serve
unless the step has already brought it to this binary's version.

It is also what lets the nodes connect as a role that cannot change the schema.
A node that migrates at boot needs `CREATE` on the schema forever — Postgres
checks the privilege before it checks existence, so even the ledger's
`CREATE TABLE IF NOT EXISTS` is refused without it — while a node opening
without migrations needs nothing beyond `SELECT`, `INSERT`, `UPDATE` and
`DELETE` on the tables it serves.

So the deploy step runs `pgstore.Migrate` as a role that owns the schema, and
every node behind it starts as the restricted one:

```sh
syncplaned -db "$DATABASE_URL" -db-migrate=false
```

**A migration is a one-way door for the fleet.** `Open` refuses a database
whose ledger is ahead of the binary, so once the step has run, no older binary
can boot against that database — including a node that restarts mid-roll and a
rollback to the previous release. Nodes already serving are unaffected, since
the check happens at `Open`. Run the step with the roll it belongs to rather
than long before it, and plan a binary rollback together with a database
restore.

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
- No transaction-mode pooler anywhere in front of the engine, the deploy step's
  DSN included
- Table layout chosen at creation — partitioned deployments also need `ANALYZE`
  on a schedule
- A deploy step that owns the DDL paired with `WithoutMigrations()` on the
  nodes; without it the step is advisory and the nodes still need `CREATE`
- Connectors in their own deployment at one replica
- Override `CanAccessWorkspace` — the default `AllowAll` leaves a shared
  deployment with no tenant isolation and nothing says so at compile time
- Alert on the four signals in
  [Observe the engine](observe-the-engine.md#what-to-alert-on)
