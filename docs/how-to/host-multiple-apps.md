# Host multiple apps in one process

Two mechanisms, at two levels. A **namespace** partitions one SQLite file
between several servers. A **`Manager`** goes further: it hosts many apps whose
schemas arrive at runtime, routes their sync surfaces, and exposes a trusted
control plane for registering them.

## Namespaces: several apps, one database

A namespace partitions workspaces, rows and client watermarks inside one SQLite
file. Open the file once and derive a view per app — never open the same file
twice:

```go
root, _ := sqlitestore.Open("apps.db")
defer root.Close()                       // the root owns the handle
srvA, err := server.New(root.Namespace("app-a"), server.WithEntities(&appav1.Thing{}))
srvB, err := server.New(root.Namespace("app-b"), server.WithEntities(&appbv1.Widget{}))
```

`Close` on a derived store is a no-op. `sqlitestore.WithNamespace("app-a")` on
`Open` does the same for a single-app process.

## A manager: many apps, runtime schemas

A `server.Manager` maps **one app → one `*Server` over one storage namespace**
and routes the public sync surface by a leading `/<appID>` path segment. The
trusted control plane is a separate `AdminService`, mounted on its own (private)
listener so it never sits on the surface browsers reach:

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

`cmd/syncplaned` wires all of this up.

Each app's `*Server` runs background work (GC, index backfill, notify
coalescing), and the manager owns those servers: `mgr.Close()` stops all of
them and refuses to host further apps. It leaves the root adapter open — that
handle is still yours, which is why the two defers go in that order.

### Registering a schema

A backend that owns the schema registers it over the wire (see
`examples/register`):

```ts
import { registerSchema } from 'syncplane';
await registerSchema(adminTransport, 'todo', bundle, { token });
```

`bundle` is a serialized `FileDescriptorSet` — see [Define schemas in TypeScript](define-schemas-in-typescript.md) for how to build one, or
`server.BuildDynamicFile` / `RegisterDynamicEntities` to do it from Go.

A browser client then points at `/<appID>` and syncs full CRUD against a schema
that exists only as runtime-registered bytes: pass `entities:
bundle.descMessages()`, or `fetchSchema(transport, appId)` to rebuild the
descriptors from the server's own copy ("the server ships its schema").

### What registration guarantees

- **Trust.** `RegisterAuthorizer` is deny-by-default (`DenyAll`); the built-in
  `TokenAuthorizer` checks an `Authorization: Bearer <token>` header in constant
  time. A denied call never touches state.
- **Persistence.** The descriptor set is stored per app and re-registered on
  boot **before serving**, so a restart leaves every app immediately syncable.
- **Idempotence + evolution.** Re-registering an identical set is a no-op; new
  entities and appended fields upsert; a conflicting change (a removed,
  renumbered or retyped field, a reference retargeted to a different entity, or
  changed `key_fields`) is rejected without partially applying. Each app's schema
  revision is a stable hash of its canonical descriptor bytes, surfaced to
  clients as `schema_revision` so a schema change makes them re-bootstrap.

## Authenticating a collection of apps

A collection of internal tools normally shares **one** identity provider and
does **not** share membership: everyone with a login can reach the host, but who
may open which workspace differs per app. `Manager` splits its options along
that line — `WithServerOptions` for what every app has in common,
`WithAppOptions` for what each app decides for itself:

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

The authenticator runs on the app's handler, after routing, so a request is
authenticated exactly once and the resulting `Principal` reaches that app's
permission hooks — the same seams a [single-app server](authentication-and-permissions.md) uses, installed once for the
collection.

**Binding the policy to an app id is what makes it correct.** Hooks receive a
principal and a `workspace_id`, never an app id, and each app is its own storage
namespace — so workspace `team` in `docs` and workspace `team` in `billing` are
unrelated partitions that a single shared policy would answer identically.

Per-app options are applied after the shared ones and override them, so a shared
default policy plus overrides for the apps that need one also works. An app
given no policy inherits `AllowAll`, which admits every caller into every
workspace of that app — the open default, per app.

## Scale envelope

One process, one SQLite file (a single writer across every app on the host), and
an in-process change notifier: apps do not contend for anything but that writer,
and a workspace's write path is short. That is sized for **internal tools — tens
of apps, human-paced write rates**.

It stops fitting when sustained writes saturate the single writer, or when you
need more than one node. The way out is
[`pgstore`](../reference/storage-adapter.md#adapters) plus a shared notifier via
`WithNotifier` (clients on other nodes otherwise converge by polling, correctly
but with poll latency). Nothing above the storage seam changes when that
happens — `Manager`, namespaces and the authentication wiring are unaffected.
See [Run more than one node](run-multiple-nodes.md).
