# Syncplane

A sync engine for local-first apps. Clients read and write a local KV store, so
an app works fully offline; a server-authoritative mutation log keeps every
replica converged by optimistic rebase — pending mutations replay on top of
server-confirmed state.

## Features

### Sync

- **Partial sync** — sync only the rows a user needs: filter by predicate, or
  keep a bounded window such as the N most recently updated. Every read
  reports whether it saw complete data.
- **Live updates** — the server pushes changed rows to connected clients, with
  polling as a fallback.
- **Multi-tab, multi-workspace** — open the app in any number of tabs and they
  share one local store and one server connection. A user in several
  workspaces syncs them all over that same connection.

### Queries & search

- **One query language** — a query returns the same rows whether it runs
  against the local store or on the server. Declare an index and it becomes a
  range read on both sides instead of a scan.
- **Full-text search** — mark fields `searchable` and query with `match`,
  ranked by relevance. The client ranks whatever it has synced exactly as the
  server would.

### Collaboration

- **Collaborative text** — a field can hold a text document that merges
  concurrent edits. Formatting and comment anchors stay attached to the
  characters they cover as others edit around them.
- **Presence & broadcast** — cursors, "who is here" and typing indicators,
  delivered live and never stored. Attach a room to a row and whoever may
  read the row may join.
- **Field history** — record what chosen fields held before each editing
  session and who changed them; an old value can be restored with a normal
  write.

### Server

- **Authentication & permissions** — plug in your own authentication, then
  decide per workspace and per write what a user may do, and filter what they
  may read.
- **Custom mutations** — beyond the generated `put`/`patch`/`del`, define
  named mutations that run in Go on the server, with optional optimistic
  client versions.
- **Effects & connectors** — run your own code when writes commit, and keep
  an external system in two-way sync.
- **Runtime schemas** — register an app's entities at runtime instead of
  compiling them in; one server can host many apps.
- **One node or many** — start on SQLite with no extra infrastructure; switch
  to Postgres to run several nodes. Application code does not change.

### Tooling

- **React adapter** — `syncplane/react` exposes queries, rows and mutations
  as hooks, with a provider that owns the client.
- **Test kit** — `syncplane/testing` runs your app's real client against an
  in-process fake server: go offline, reject a write, and assert what the
  user sees.

## Quick start

### Server (Go)

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

An entity is an ordinary proto message with an annotation:

```proto
message Todo {
	option (syncplane.v1.entity) = { name: "todo" key_fields: [ "id" ] };

	string id = 1;
	string title = 2;
	bool completed = 3;
}
```

The default posture is open — `AllowAll` admits every caller into every
workspace. See [Authentication and permissions](docs/how-to/authentication-and-permissions.md)
before a shared deployment.

### Client (TypeScript)

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

## A few more tastes

Queries run locally against the synced subset, or on the server against
everything:

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

Sync only part of a workspace — by predicate, or by rank with a window:

```ts
client.setSubscriptions([
	subscription(TodoSchema, (t) => t.completed.eq(false)),
	subscription(CommentSchema, undefined, { limit: 500, byRecency: true }),
]);
```

Edit a collaborative text field through a live handle:

```ts
const text = await client.db.doc.text(id, 'body');
await text.insert(5, ', world');
text.subscribe((delta, value) => editor.applyDelta(delta));
```

Write from your own Go code through the same path clients use — and under the
same permissions, against the principal on the context:

```go
err := srv.Apply(ctx, workspaceID, func(tx *server.ApplyTx) error {
	todo.Completed = true
	return tx.Patch(&todo, []string{"completed"}, &version)
})
```

System work with no caller to authorize opts out explicitly, per call, with
`server.Trusted(ctx)`.

## Try the demo

```sh
mise install        # go, node, pnpm, buf, task, ginkgo
task demo:server    # terminal 1: sync server on :8787
task demo:web       # terminal 2: vite app
```

Open the printed URL in two tabs — changes propagate in under a second without
polling. The demo exercises undo/redo, drag-to-reorder via fractional indexing,
partial sync, collaborative notes with comments, and server rejection.

## Documentation

Full documentation lives in [`docs/`](docs/README.md), organized along
[Diátaxis](https://diataxis.fr):

- **[Tutorials](docs/README.md#tutorials)** — build your first sync app, then
  add collaborative text.
- **[How-to guides](docs/README.md#how-to-guides)** — permissions, queries,
  partial sync, collaborative text, server-side writes, the change feed and
  effects, external-store connectors, field history, hosting many apps, running
  more than one node, observability, React, failure handling.
- **[Reference](docs/README.md#reference)** — the Go server API, the TypeScript
  client API, proto options, connectors, naming rules, sync status and rejection
  codes, the storage adapter contract, telemetry.
- **[Explanation](docs/README.md#explanation)** — the convergence model, partial
  sync and coverage, the collaborative-text CRDT, multi-tab behaviour, and the
  limits and trade-offs you should know before adopting.

## Development

Tools are pinned in `mise.toml`; tasks live in `Taskfile.yml`:

```sh
task generate          # regenerate Go + TS from proto/
task ci                # lint, breaking, vet, typecheck, all tests
task test:go           # Ginkgo suites (engine)
task test:ts           # every TS client suite: unit, react, integration
task test:ts:react     # just the syncplane/react hooks (jsdom)
task test:e2e          # multi-tab browser tests (Playwright; not in `task ci`)
```

See [Testing](docs/how-to/testing.md) for what each tier can observe and how to
run a single test.
