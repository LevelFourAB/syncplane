# Deploying for high availability

This guide shows you how to deploy and configure a multi-node sync fleet and PostgreSQL database for high availability (HA). Use these steps to maintain service through sync node restarts, database failovers, and rolling deployments.

## Prerequisites

Before you begin, verify that you have:

- A multi-node deployment environment (see [Running multiple nodes](run-multiple-nodes.md)).
- A PostgreSQL primary database with an automatic failover pair, such as Patroni.

## Steps

### 1. Partition database tables

If you set up a new database, configure hash partitioning for tenant tables:

1. Configure 32 hash partitions by passing `pgstore.WithPartitions(32)` in Go or `-db-partitions 32` to `syncplaned`. This partitions `rows`, `field_deltas`, `idempotency`, `index_entries`, and `consumer_shadows` by `workspace_id`.
2. Apply this setting during initial database creation. Schema migrations apply the partition layout when creating tables. The modulus must be a power of two. You cannot partition an existing database that contains data.
3. Schedule a job to run `ANALYZE` on partitioned parent tables. Autovacuum does not analyze partitioned parent tables automatically.

**Note:** PostgreSQL does not support `CREATE INDEX CONCURRENTLY` on partitioned parent tables. Adding an index to a partitioned parent table requires an exclusive write lock. To add indexes online later, apply index migrations per partition.

### 2. Run database migrations

Run schema migrations in a dedicated deployment step instead of at node startup:

1. Apply outstanding migrations with `pgstore.Migrate(ctx, dsn, opts...)`, or execute the SQL statements from `pgstore.Statements(opts...)`. Connect with a database role that owns the schema and has `CREATE` privileges.
2. If you want to verify compatibility before migrating, call `pgstore.CanServe(binaryVersion, schemaVersion)` with `pgstore.SchemaVersion()`. Additive migrations allow older binary versions to continue running. If a migration removes or narrows schema objects, plan a dedicated release and create a database backup.
3. Configure sync nodes with `WithoutMigrations()` in Go or `-db-migrate=false` in the CLI:

   ```sh
   syncplaned -db "$DATABASE_URL" -db-migrate=false
   ```

4. Connect sync nodes with a restricted database role that has only `SELECT`, `INSERT`, `UPDATE`, and `DELETE` permissions on tables.

### 3. Configure database connections and read replicas

Configure database connection limits and optional read scaling:

1. Connect sync nodes directly to PostgreSQL or through a connection pooler in session mode (`pool_mode = session`). Do not use a transaction-mode pooling proxy in front of the primary database or migration runner.
2. If multiple deployments share a database server, assign each deployment a separate schema (`search_path`) and separate notify channels (see [Running multiple nodes](run-multiple-nodes.md#isolating-deployments-on-a-shared-database)).
3. Size primary pool connections per node with `WithMaxConns`. Divide the total PostgreSQL `max_connections` by the node count. Subtract fixed connections: 1 lease session, 1 `pglisten` `LISTEN` connection, and 1 `pglisten` publisher connection. Leave headroom for standby clients.
4. If you use a hot standby for read scaling, configure the read replica with `pgstore.WithReadReplica(dsn)` in Go or `-db-replica` in the CLI.
5. Size `WithReplicaMaxConns` based on concurrent streamed reads (such as bootstrap snapshots and change-feed drains) against the standby `max_connections`.
6. If long replica reads fail with SQLSTATE 57014 due to write-ahead log (WAL) replay conflicts, set `hot_standby_feedback = on` on PostgreSQL to retain dead rows on the primary during active standby reads.

### 4. Configure load balancing and sync nodes

Configure the load balancer and sync node runtime settings:

1. Disable session affinity on your load balancer. Reconnecting clients resume from their cursor on any node.
2. Set the load balancer idle timeout higher than the heartbeat interval (`WithHeartbeat`, default 30 seconds). Configure a maximum connection lifetime to rebalance connections without triggering reconnect loops on quiet workspaces.
3. If a web page runs multiple `SyncClient` instances against one host, configure the server to support HTTP/2 cleartext (h2c) to avoid browser connection limits.
4. Configure shared notification adapters with `pgstore` and `pgnotify`. If the application uses rooms, also include `pgpresence`.
5. Override `CanAccessWorkspace` in application code to enforce tenant authorization. The default `AllowAll` setting provides no tenant isolation.
6. Handle `SIGTERM` signals in your sync node process to shut down components in order:

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

### 5. Register application schemas

Register application schemas during deployment:

1. Register schemas as a deployment step before restarting the sync node fleet.
2. If your application registers schemas at runtime through `AdminService`, restart all other sync nodes in the fleet to propagate the new schema descriptor set.

### 6. Deploy connectors

Run data connectors in an independent deployment:

1. Deploy connectors in a dedicated service separate from the sync node fleet.
2. Configure connector deployments to run a single replica.
3. Use a recreate deployment strategy instead of a rolling update.

## Confirming the deployment

To confirm that your high-availability deployment is operational:

1. Monitor the `syncplane.replica.reads` metric. Verify that reads route to the replica. Verify that neither the `fallback_stale` nor the `fallback_error` `read_route` tag shows a sustained increase. A sustained increase in either tag alone is a problem.
2. Verify that alerts are active for the high-availability signals described in [Observing the engine](../reference/telemetry.md#metrics).
3. Test sync node failover by stopping a single sync node. Confirm that active clients reconnect to another node and resume from their existing cursors without data loss.
4. Test database failover on your staging cluster. Verify that in-flight client writes retry upon transient errors and succeed once the standby promotes.

## Related

- [Adapters](../reference/storage-adapter.md#adapters)
- [Multi-node architecture](../explanation/multi-node-architecture.md)
