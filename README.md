# Syncplane

Syncplane is a sync engine for local-first applications. Clients read and write a local key-value store, so an application works fully offline. A server-authoritative mutation log keeps replicas converged through optimistic rebase. It replays pending mutations on top of server-confirmed state.

## Features

### Sync

- **Partial sync**: Sync only the rows that a user needs. Filter by predicate, keep a bounded window such as the most recently updated rows, or name the rows a device has pinned by key. Every read reports whether it saw complete data.
- **Live updates**: The server pushes changed rows to connected clients, with polling as a fallback.
- **Multi-tab and multi-workspace**: Multiple browser tabs share one local store and one server connection. A user in several workspaces syncs all of them over that same connection.

### Queries and search

- **Unified query language**: A query returns the same rows whether it runs against the local store or on the server. When you declare an index, the query runs as a range read on both sides instead of a scan.
- **Full-text search**: Mark fields as `searchable` and query them with `match`, ranked by relevance. The client ranks synced data the same way the server does.

### Collaboration

- **Collaborative text**: A field can hold a text document that merges concurrent edits. Formatting and comment anchors stay attached to the characters that they cover as other users edit around them.
- **Presence and broadcast**: The server delivers live cursors, presence indicators, and typing indicators without storing them. Attach a room to a row, and any user who can read the row can join the room.
- **Field history**: Record previous field values and user identities before each editing session. You can restore an old value with a standard write.

### Server

- **Authentication and permissions**: Integrate your own authentication provider, manage user permissions per workspace and per write, and filter what users can read.
- **Custom mutations**: In addition to generated `put`, `patch`, and `del` operations, define named mutations in Go that run on the server, with optional optimistic client versions.
- **Effects and connectors**: Run custom code when writes commit, and keep external systems in two-way sync.
- **Runtime schemas**: Register application entities at runtime instead of compiling them into the binary. One server can host multiple applications.
- **Single-node or multi-node deployments**: Start with SQLite and no extra infrastructure, or switch to PostgreSQL to run multiple nodes. Application code does not change.

### Tooling

- **React adapter**: The `syncplane/react` package exposes queries, rows, and mutations as React hooks, with a provider that manages the client.
- **Test kit**: The `syncplane/testing` package runs your client against an in-process fake server. You can simulate offline mode, reject writes, and assert what the user sees.

## Quick start

### Go server

Set up a server with an entity schema:

```go
store, _ := sqlitestore.Open("app.db")
defer store.Close()                  // the server does not own the store

srv, err := server.New(store,
	server.WithEntities(&todov1.Todo{}), // reads (syncplane.v1.entity) options
)
if err != nil {
	log.Fatal(err)
}
defer srv.Close()

http.ListenAndServe(":8787", srv.HTTPHandler()) // HTTP/1.1 suffices
```

An entity is a Protocol Buffers message with a `syncplane.v1.entity` annotation:

```proto
message Todo {
	option (syncplane.v1.entity) = { name: "todo" key_fields: [ "id" ] };

	string id = 1;
	string title = 2;
	bool completed = 3;
}
```

By default, `AllowAll` admits every caller into every workspace. For shared deployments, see [Authentication and permissions](docs/how-to/authentication-and-permissions.md).

### TypeScript client

Connect a client and mutate entities:

```ts
const client = new SyncClient({
	url: 'https://sync.example.com',
	workspaceId: 'demo',
	entities: [ TodoSchema ],
});
await client.start(); // bootstrap + live stream (returns immediately)

// client.db gives each entity a schema-bound API, keyed by its name.
// CRUD is optimistic and offline-first:
const id = await client.db.todo.mutate.create({ title }); // mints an id
await client.db.todo.mutate.patch(id, { completed: true });
await client.db.todo.mutate.del(id);

client.db.todo.watch((rows) => render(rows));
```

### React

Wrap your components in `SyncProvider` and access data through hooks:

```tsx
const { SyncProvider, db } = createReactSync([ TodoSchema ]);

// The provider builds the client, starts it, and closes it on unmount.
<SyncProvider options={ { url, workspaceId, entities: [ TodoSchema ] } }>
	<TodoList />
</SyncProvider>;

function TodoList() {
	const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });
	const { patch, del } = db.todo.useMutate();
	// ...
}
```

## Code examples

Run queries locally against the synced subset, or on the server against all data:

```ts
const open = await client.db.todo.query({
	where: (t) => and(t.completed.eq(false), t.title.contains('milk')),
	orderBy: (t) => [ t.createdAtMs.desc() ],
	limit: 20,
});

const found = await client.db.todo.queryServer({
	where: (t) => t.title.contains('milk'),
});
```

Sync a subset of a workspace by predicate, by recency rank with a limit, or by row key:

```ts
client.setSubscriptions([
	subscription(TodoSchema, (t) => t.completed.eq(false)),
	subscription(CommentSchema, { limit: 500, byRecency: true }),
	subscription(DocSchema, { keys: pinnedDocIds }),
]);
```

Edit a collaborative text field through a live handle:

```ts
const text = await client.db.doc.text(id, 'body');
await text.insert(5, ', world');
text.subscribe((delta, value) => editor.applyDelta(delta));
```

Write from Go code using the same path and permissions as client writes:

```go
err := srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error {
	todo.Completed = true
	return tx.Patch(&todo, []string{"completed"}, &version)
})
```

To run system operations without authorizing a caller, opt out explicitly with `server.Trusted(ctx)`.

## Running the demo

To run the local demo application:

1. Install dependencies and start the server and web client:

   ```sh
   mise install        # go, node, pnpm, buf, task, ginkgo
   task demo:server    # terminal 1: sync server on :8787
   task demo:web       # terminal 2: vite app
   ```

2. Open the printed URL in two browser tabs.

Changes propagate in under one second without polling. The demo shows undo and redo, drag-to-reorder with fractional indexing, partial sync, collaborative notes with comments, and server rejection handling.

## Documentation

Documentation is in the [`docs/`](docs/README.md) directory, organized by Diátaxis documentation type:

- **[Tutorials](docs/README.md#tutorials)**: Build your first sync application and add collaborative text.
- **[How-to guides](docs/README.md#how-to-guides)**: Configure permissions, queries, partial sync, collaborative text, server-side writes, change feeds and effects, external store connectors, field history, multi-app hosting, multi-node setups, observability, React, and failure handling.
- **[Reference](docs/README.md#reference)**: Review the Go server API, TypeScript client API, Protocol Buffers options, connectors, naming rules, sync status and rejection codes, storage adapter contracts, and telemetry.
- **[Explanation](docs/README.md#explanation)**: Read about the convergence model, partial sync coverage, collaborative text CRDTs, multi-tab behavior, and system trade-offs.

## Development

`mise.toml` defines tool versions. `Taskfile.yml` defines development tasks:

```sh
task generate          # regenerate Go + TS from proto/
task ci                # lint, breaking, vet, typecheck, all tests
task test:go           # Ginkgo suites (engine)
task test:ts           # every TS client suite: unit, react, integration
task test:ts:react     # syncplane/react hooks (jsdom)
task test:e2e          # multi-tab browser tests (Playwright; not in `task ci`)
```

For details about test suites and running individual tests, see [Testing](docs/how-to/testing.md).
