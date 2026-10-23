# Running multiple nodes

This guide shows you how to configure and run multiple Syncplane server nodes using PostgreSQL. Use this setup when you need multiple server processes to share storage, fan out realtime pokes, and synchronize room presence across instances.

The default configuration is designed for a single node: a SQLite file for storage, an in-process notifier for pokes, and an in-process presence bus for rooms. To run multiple nodes, you must replace these components:

- **Storage:** SQLite does not support concurrent multi-process writes. Replace it with `pgstore`.
- **Pokes:** The default notifier operates in-process, so writes on one node do not wake clients on another. Replace it with `pgnotify`.
- **Rooms:** The default presence bus operates in-process, splitting participant rosters across nodes. Replace it with `pgpresence` (or omit it if your application does not use rooms).

## Prerequisites

Before configuring multiple nodes, ensure you have:

- A PostgreSQL database instance accessible to all server nodes.
- The database connection string configured in the `DATABASE_URL` environment variable.
- (Optional) A read replica connection string in `DATABASE_REPLICA_URL` if you route reads to a standby replica.

## Configuring a multi-node server

To run multiple nodes in Go, replace the default SQLite store and in-process transports with PostgreSQL components.

1. **Connect to the PostgreSQL store.**
   Call `pgstore.Open` with your database connection URL:

   ```go
   ctx := context.Background()

   store, err := pgstore.Open(ctx, os.Getenv("DATABASE_URL"))
   if err != nil {
   	log.Fatal(err)
   }
   defer store.Close()
   ```

   If you route read traffic to a hot standby replica, pass `pgstore.WithReadReplica`:

   ```go
   store, err := pgstore.Open(ctx, os.Getenv("DATABASE_URL"),
   	pgstore.WithReadReplica(os.Getenv("DATABASE_REPLICA_URL")))
   ```

   `pgstore.Open` applies pending migrations automatically under an advisory lock. If your deployment process manages database migrations separately, run `pgstore.Migrate` or apply `pgstore.Statements` in a deployment step and pass `pgstore.WithoutMigrations()` to `pgstore.Open`. For more information, see [owning the migrations](deploy-for-high-availability.md#2-run-database-migrations) and [PostgreSQL sizing](deploy-for-high-availability.md#3-configure-database-connections-and-read-replicas).

2. **Open the shared listen transport.**
   Call `pglisten.Open` to manage a shared `LISTEN`/`NOTIFY` transport:

   ```go
   link, err := pglisten.Open(ctx, os.Getenv("DATABASE_URL"))
   if err != nil {
   	log.Fatal(err)
   }
   defer link.Close()
   ```

   `pglisten` maintains one unpooled connection waiting for notifications and one issuing `pg_notify`. Sharing this transport allows both `pgnotify` and `pgpresence` to run on two database connections instead of four. Ensure `link` outlives both services.

   If your process runs only one of the two packages, you can instead call `pgnotify.Open` or `pgpresence.Open` directly with a connection string to let the package manage its own transport.

3. **Initialize the notifier and presence bus.**
   Pass the transport link to `pgnotify.New` and `pgpresence.New`:

   ```go
   notifier, err := pgnotify.New(ctx, link)
   if err != nil {
   	log.Fatal(err)
   }
   defer notifier.Close()

   bus, err := pgpresence.New(ctx, link)
   if err != nil {
   	log.Fatal(err)
   }
   defer bus.Close()
   ```

   If your application does not use rooms, omit `pgpresence`. For background on rooms, see the [ephemeral plane documentation](../explanation/rooms.md).

4. **Create the server instance.**
   Pass the store, notifier, and presence bus to `server.New`:

   ```go
   srv, err := server.New(
   	store,
   	server.WithNotifier(notifier),
   	server.WithPresenceBus(bus),
   	server.WithEntities(&todov1.Todo{}),
   )
   ```

## Hosting multiple applications

If you host multiple applications with `server.Manager`, pass one notifier and one presence bus for all of them:

```go
mgr, err := server.NewManager(ctx, store,
	server.WithServerOptions(
		server.WithNotifier(notifier),
		server.WithPresenceBus(bus),
	),
)
```

`server.Manager` partitions the store, the notifier and the bus itself: each application's server prefixes the workspace IDs it puts on them, so workspaces with identical names in different applications never reach each other.

A schema that you register on one node reaches the other nodes over the same notifier, at once. If a notification is lost, the other nodes find the schema when they next read the catalog, every 30 seconds by default. To change the interval, pass `server.WithCatalogPollInterval`:

```go
mgr, err := server.NewManager(ctx, store,
	server.WithServerOptions(server.WithNotifier(notifier)),
	server.WithCatalogPollInterval(10*time.Second),
)
```

You can send a registration to any node. Each node merges the new set over the schema that the database stores. See [Hosting many apps](../reference/server.md#hosting-many-apps).

## Running multiple nodes with syncplaned

If you run the standalone `syncplaned` host binary, pass a PostgreSQL connection URI to the `-db` flag. `syncplaned` automatically selects `pgstore`, `pgnotify` and `pgpresence`:

```sh
syncplaned -db postgres://user:pass@host/syncplane   # Postgres, multi-node ready
syncplaned -db /var/lib/syncplane/sync.db            # SQLite, unchanged

# Postgres with reads served from a hot standby
syncplaned -db postgres://user:pass@host/syncplane \
	-db-replica postgres://user:pass@standby/syncplane
```

**Note:** `syncplaned` includes no built-in authenticator or permissions system. Run `syncplaned` behind a trusted network boundary.

## Isolating deployments on a shared database

If multiple deployments (such as staging and production) share a PostgreSQL database, isolate their channels and schemas:

1. **Configure separate notification channels.**
   Pass `WithChannel` to assign unique notification channels:

   ```go
   pgnotify.New(ctx, link, pgnotify.WithChannel("syncplane_poke_staging"))
   pgpresence.New(ctx, link, pgpresence.WithChannel("syncplane_presence_staging"))
   ```

   The packages default to `syncplane_poke` and `syncplane_presence`. Keep them on distinct channels so each decoder receives only its intended payloads.

2. **Isolate store tables.**
   Configure different databases or set different schemas using the `search_path` parameter in the connection DSN.

3. **Avoid advisory lock contention.**
   PostgreSQL advisory locks are scoped to the database. Leases hash the schema name to stay isolated, but shared databases still contend on:

   - The migration lock (a fixed key), which causes deployments to take turns migrating at startup.
   - The per-workspace write lock (keyed by workspace ID), which serializes writes when two deployments use the same workspace names.

   To eliminate lock contention, use separate databases or separate schemas.

## Verifying the setup

To verify the multi-node packages against a running PostgreSQL instance, run the test suite with `SYNCPLANE_TEST_PG_DSN`:

```sh
SYNCPLANE_TEST_PG_DSN=postgres://user:pass@localhost:5432/syncplane task test:go
```

When no Docker daemon is reachable, the test suite skips `pgstore`, `pglisten`, `pgnotify`, and `pgpresence` tests unless you provide `SYNCPLANE_TEST_PG_DSN`.

## Related

- [Multi-node architecture](../explanation/multi-node-architecture.md)
