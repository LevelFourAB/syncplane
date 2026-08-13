# Run more than one node

The defaults are single-node on purpose: a SQLite file and an in-process
notifier need no infrastructure. Running several server processes means
replacing both, because each one is single-node for a different reason.

- **Storage** — a SQLite file cannot be shared between processes. Two writer
  connections contend for the one write lock, and a write that outlives the
  other's `busy_timeout` fails. Replace it with `pgstore`.
- **Pokes** — the default notifier fans out in-process, so a client connected
  to node A never hears about a write that landed on node B. It converges by
  polling, which is correct but not realtime. Replace it with `pgnotify`.
- **Rooms** — the default presence bus is in-process too, so each node shows a
  client only the participants that node happens to be serving. Unlike pokes
  this does not converge later, because nothing on the
  [ephemeral plane](../explanation/rooms.md) is durable: a roster split across
  nodes stays split. Replace it with `pgpresence` — and skip it entirely if the
  app uses no rooms.

The three are independent. A Postgres notifier in front of a SQLite store is a
sensible single-node deployment that gains nothing, and a Postgres store with
the default notifier is a correct multi-node deployment whose clients converge
at poll latency. You need all three for multi-node realtime with presence.

## Both, together

```go
ctx := context.Background()

store, err := pgstore.Open(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	log.Fatal(err)
}
defer store.Close()

notifier, err := pgnotify.Open(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	log.Fatal(err)
}
defer notifier.Close()

bus, err := pgpresence.Open(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	log.Fatal(err)
}
defer bus.Close()

srv, err := server.New(
	store,
	server.WithNotifier(notifier),
	server.WithPresenceBus(bus),
	server.WithEntities(&todov1.Todo{}),
)
```

Each of the two `LISTEN`/`NOTIFY` transports holds a connection pool for
publishing plus one dedicated connection parked in `WaitForNotification` —
neither `pgstore` nor the other exposes a pool to share — so budget four
connection sets per process rather than two.

`Open` applies any outstanding migrations, under an advisory lock — so every
node runs the same code at boot and they race safely. There is no separate
migration step to sequence into a deploy.

## Hosting several apps

`Manager` shares one set of server options across every app it hosts, but a
notifier must not be shared as-is: workspace ids are namespaced per app, so two
apps that both have a workspace `team` would wake each other's clients. Derive a
per-app view instead, which tags what it publishes and filters what it receives:

```go
mgr, err := server.NewManager(ctx, store,
	server.WithAppOptions(func(appID string) []server.Option {
		return []server.Option{server.WithNotifier(notifier.Namespace(appID))}
	}),
)
```

The `Manager` namespaces the _store_ per app by itself; only the notifier needs
this.

## With the shipped host

`syncplaned` tells the two apart by URI scheme on its existing `-db` flag, so a
Postgres DSN selects `pgstore` and wires `pgnotify` per app for you:

```sh
syncplaned -db postgres://user:pass@host/syncplane   # Postgres, multi-node ready
syncplaned -db /var/lib/syncplane/sync.db            # SQLite, unchanged
```

The notifier is not a separate flag because there is no useful deployment that
picks one and not the other: a Postgres store is the one that can have a second
node, and on a second node the in-process notifier stops reaching half the
clients.

Note the trust posture — `syncplaned` installs no authenticator and no
permissions, so it belongs behind a trusted boundary whichever store it uses.

## What you get, and what you do not

Pokes reach every node, so a write on any node wakes clients on all of them.
Different workspaces commit concurrently instead of serializing behind one
writer connection.

Background work divides across nodes by per-workspace leases. `pgstore` backs
them with advisory locks, so a server built on it dedups with no
configuration: the GC and index sweeps skip a workspace another node is
collecting, and an effects runtime leases each (consumer, workspace) drain the
same way. A node that crashes releases everything it held with its connection
— there is no lease TTL to wait out. The leases are advisory, which cuts both
ways: work stays correct without them, and none of it may _depend_ on them —
delivery to an effect handler is still at-least-once, and a rolling deploy can
still hand one change to two processes. See
[Limits and trade-offs](../explanation/limits-and-trade-offs.md) for why the
engine dedups rather than fences.

Two things do not change, and are worth planning around rather than
discovering:

- **Effect handlers must stay idempotent.** The lease makes single delivery
  the common case across nodes; it does not make it a guarantee.
- **Connectors do not take leases**, and they keep their cursors and shadows in
  connector-owned state — shipped default, a local SQLite file — which a second
  instance never sees. Two instances converging one workspace echo each other's
  writes, so run a connector in exactly one process.

Surviving the loss of one of those nodes, or of the database under them, is a
separate concern: see
[Deploy for high availability](deploy-for-high-availability.md).

## Delivery is best-effort, on purpose

A missed poke costs one poll interval, never a lost write. `pgnotify` leans on
that in three places, all of them documented on the package: a full publish
queue drops rather than blocks the write path, a notification published while
the listener is reconnecting is gone (Postgres buffers nothing across a dropped
`LISTEN`), and an oversized payload is skipped.

Reconnects are the one visible gap. On reconnect the listener re-pokes every
workspace it has a live subscription for, so watching clients are not left
waiting for a poll — but a workspace nobody was subscribed to is not announced,
which is why a background consumer keeps its periodic sweep as a backstop.

`pgpresence` has the same three lossy paths and heals differently, because a
presence event _was_ the state and cannot be re-sent from nothing the way a
content-free poke can. There is no re-poke on reconnect; instead each node
re-announces the records it is responsible for every `WithRoomAnnounce`
interval, so the other nodes' rosters are stale for at most one interval. A
broadcast sent during the gap is gone, which is the primitive's documented
contract.

The one failure it does **not** swallow is an oversize payload: a presence
event carries the app's own state base64-encoded inside an 8000-byte NOTIFY
payload, so one that will not fit is refused at `Publish` rather than dropped.
The engine caps state and messages well under that
(`MaxPresenceStateBytes`, `MaxBroadcastBytes`), so reaching it means those
limits were raised past what the transport can carry.

Presence updates arrive at a far higher rate than pokes — a cursor moves tens of
times a second — so the publish queue folds state per `(topic, session)` before
it reaches the database: a hundred users moving cursors cost a hundred
`pg_notify` round trips per drain rather than a thousand. Broadcasts are each
distinct and never folded, only dropped.

## Isolating deployments that share a database

One channel carries every namespace. Staging and production on the same
Postgres would wake each other's clients for workspaces they do not have, so
give them separate channels:

```go
pgnotify.Open(ctx, dsn, pgnotify.WithChannel("syncplane_poke_staging"))
pgpresence.Open(ctx, dsn, pgpresence.WithChannel("syncplane_presence_staging"))
```

The two default to different channels (`syncplane_poke` and
`syncplane_presence`) and must stay apart: a node running only the notifier
would otherwise receive presence payloads it cannot decode.

`pgstore` needs no equivalent: point the two at different databases, or at
different schemas via the DSN's `search_path`.

## Testing against a real Postgres

The `pgstore` and `pgnotify` suites run against a container started by
testcontainers, and **skip** when no Docker is reachable — Postgres is not a
build dependency of this repo. To run them against a server you already have:

```sh
SYNCPLANE_TEST_PG_DSN=postgres://user:pass@localhost:5432/syncplane task test:go
```
