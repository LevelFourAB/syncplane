# Hosting multiple apps in one process

This guide shows you how to host multiple applications in one process using storage namespaces and `server.Manager`. Use this approach to partition a single database between multiple sync servers or to dynamically register and route schemas at runtime.

## Prerequisites

Before hosting multiple applications, ensure you have:

- A root storage adapter (such as SQLite or PostgreSQL).
- An admin authentication token if you register schemas at runtime.

## Steps

1. **Open the root storage adapter:**

   Open the database file once. If you partition a single SQLite file across static servers without dynamic schemas, derive namespaces directly from the root adapter:

   ```go
   root, _ := sqlitestore.Open("apps.db")
   defer root.Close()                       // the root owns the handle
   srvA, err := server.New(root.Namespace("app-a"), server.WithEntities(&appav1.Thing{}))
   srvB, err := server.New(root.Namespace("app-b"), server.WithEntities(&appbv1.Widget{}))
   ```

   Calling `Close` on a derived store is a no-op. Calling `sqlitestore.WithNamespace("app-a")` on `Open` performs the same partitioning for a single-app process.

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

   Bind permission policies to an app ID. Permission hooks receive a principal and a `workspace_id`, not an app ID. Because each app is its own storage namespace, workspace `team` in `docs` and workspace `team` in `billing` are separate partitions.

   Per-app options run after shared options and override them. An app with no configured policy inherits `AllowAll`.

   If you use PostgreSQL with a shared change notifier and presence bus, configure them with namespaced options per app:

   ```go
   link, _ := pglisten.Open(ctx, dsn)
   notifier, _ := pgnotify.New(ctx, link)
   bus, _ := pgpresence.New(ctx, link)

   server.WithAppOptions(func(appID string) []server.Option {
   	return []server.Option{
   		server.WithNotifier(notifier.Namespace(appID)),
   		server.WithPresenceBus(bus.Namespace(appID)),
   	}
   })
   ```

   A namespaced view shares the underlying fan-out, and closing a view is a no-op. The value returned by `Open` or `New` owns the fan-out and must outlive every view. Both fan-outs sit on one `pglisten.Conn`, which owns the connections and must outlive them in turn. `cmd/syncplaned` wires both this way when its data source name (DSN) selects PostgreSQL.

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
