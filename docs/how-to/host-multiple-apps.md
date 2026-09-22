# Hosting multiple apps in one process

This guide shows you how to host multiple applications in one process using workspace-ID prefixes and `server.Manager`. Use this approach to partition a single database between multiple sync servers or to dynamically register and route schemas at runtime.

## Prerequisites

Before hosting multiple applications, ensure you have:

- A root storage adapter (such as SQLite or PostgreSQL).
- An admin authentication token if you register schemas at runtime.

## Steps

1. **Open the root storage adapter:**

   Open the database file once. If you partition a single SQLite file across static servers without dynamic schemas, wrap the root adapter per app with `prefixstore`:

   ```go
   root, _ := sqlitestore.Open("apps.db")
   defer root.Close()                       // the root owns the handle
   srvA, err := server.New(prefixstore.Wrap(root, "app-a/"), server.WithEntities(&appav1.Thing{}))
   srvB, err := server.New(prefixstore.Wrap(root, "app-b/"), server.WithEntities(&appbv1.Widget{}))
   ```

   Each server keeps using bare workspace IDs; the wrapper prefixes every ID that reaches the database, so workspace `team` under `app-a` is stored as `app-a/team`. Calling `Close` on a wrapped adapter is a no-op. The prefix must end in a separator that no workspace ID can contain, such as `/`.

   `server.Manager` applies the same wrapper itself, using `<appID>/`, so the steps below need no storage wiring per app.

2. **Initialize the manager and configure options:**

   Create a `server.Manager` to route requests by a leading `/<appID>` path segment and host dynamic schemas. Configure shared options for the entire host with `server.WithServerOptions` and per-app options with `server.WithAppOptions`:

   ```go
   mgr, _ := server.NewManager(ctx, root,
   	server.WithRegisterAuthorizer(server.TokenAuthorizer(os.Getenv("ADMIN_TOKEN"))),
   	// Shared: one credential check for the whole host.
   	server.WithServerOptions(server.WithAuthenticator(oidcAuth)),
   	// Per app: its own membership policy, bound to its app id.
   	server.WithAppOptions(func(appID string) []server.Option {
   		return []server.Option{server.WithPermissions(policy{app: appID, members: directory})}
   	}),
   )
   ```

   The authenticator runs on the app's handler after routing, so a request is authenticated once and the resulting `Principal` reaches that app's permission hooks (see [single-app server](authentication-and-permissions.md)).

   Bind permission policies to an app ID. Permission hooks receive a principal and a `workspace_id`, not an app ID. Because each app has its own prefix, workspace `team` in `docs` and workspace `team` in `billing` are separate partitions.

   App IDs are restricted to letters, digits, `_`, `-`, and `:`, starting with a letter or a digit, and are at most 64 bytes. `Register` refuses anything else, so no app ID can spell another app's prefix.

   Per-app options run after shared options and override them. An app with no configured policy inherits `AllowAll`.

   If you use PostgreSQL, pass one change notifier and one presence bus for every hosted app:

   ```go
   link, _ := pglisten.Open(ctx, dsn)
   notifier, _ := pgnotify.New(ctx, link)
   bus, _ := pgpresence.New(ctx, link)

   server.WithServerOptions(
   	server.WithNotifier(notifier),
   	server.WithPresenceBus(bus),
   )
   ```

   Each app's server prefixes the workspace IDs it publishes and subscribes to, and its room registry prefixes the topics it puts on the bus, so neither plane needs a view per app. Both fan-outs sit on one `pglisten.Conn`, which owns the connections and must outlive them. `cmd/syncplaned` wires both this way when its data source name (DSN) selects PostgreSQL.

3. **Start the HTTP servers:**

   Mount the public sync handler and private admin handler on separate listeners:

   ```go
   root, _ := sqlitestore.Open("apps.db")
   defer root.Close()
   mgr, _ := server.NewManager(ctx, root,                       // reloads persisted schemas on boot
   	server.WithRegisterAuthorizer(server.TokenAuthorizer(os.Getenv("ADMIN_TOKEN"))))
   defer mgr.Close()                                           // the manager owns its app servers

   http.ListenAndServe(":8787", mgr.SyncHandler())             // public: /<appID>/syncplane.v1.SyncService/...
   adminPath, adminHandler := mgr.AdminHandler()               // private surface
   adminMux := http.NewServeMux(); adminMux.Handle(adminPath, adminHandler)
   http.ListenAndServe("127.0.0.1:8788", adminMux)
   ```

   `cmd/syncplaned` wires this configuration.

   Each app `*Server` runs background work, including garbage collection (GC), index backfill, and notify coalescing. The manager owns those servers. Calling `mgr.Close()` stops all hosted servers and refuses further apps. The root adapter remains open, so close it after closing the manager.

4. **Register an application schema:**

   Register the schema over the network with the private admin transport (see `examples/register`):

   ```ts
   import { registerSchema } from 'syncplane';
   await registerSchema(adminTransport, 'todo', bundle, { token });
   ```

   `bundle` is a serialized `FileDescriptorSet`. See [Define schemas in TypeScript](define-schemas-in-typescript.md) to build one in TypeScript, or use `server.BuildDynamicFile` and `RegisterDynamicEntities` in Go.

## Verifying the result

Point a browser client to `/<appID>` to verify that it can sync create, read, update, and delete (CRUD) operations against the registered schema.

Pass `entities: bundle.descMessages()`, or call `fetchSchema(transport, appId)` to rebuild the descriptors from the server.

## Related

- [Registration](../reference/server.md#registration)
- [Limits and trade-offs](../explanation/limits-and-trade-offs.md)
