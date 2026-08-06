# syncplane

A sync engine for local-first apps. Clients read and write a local KV store; a
server-authoritative mutation log keeps every replica converged by optimistic
rebase on the client.

It ships as two halves joined by a protobuf wire contract: a Go server
(`engine/`) and a TypeScript client (`client/`, published as `syncplane`).

## Features

- 🗃️ **Local-first** — reads and writes hit a local KV store; the app works
  fully offline and syncs in the background.
- ↩️ **Optimistic rebase** — mutations queue locally and replay on top of
  server-confirmed state; divergence self-heals on the next pull.
- 🔔 **Live change stream** — a server-streaming `Subscribe` pushes changed rows
  straight to connected clients, reconnects with backoff, falls back to polling.
- 🪟 **Partial sync** — subscribe to a subset of rows by predicate _and_ by rank
  ("the 1000 most recently updated open issues"). Every read reports its own
  `coverage`, so partial data is never mistaken for the whole answer.
- 🔎 **Queries, local and server-side** — one predicate language, evaluated
  identically in Go and TypeScript and pinned by shared vectors. Declare a
  secondary index and the same query becomes a range read on both sides;
  without one it falls back to a scan that returns the same rows.
- ✍️ **Collaborative text** — mark a `bytes` field `text: true` and it holds a
  convergent text document, with Peritext-style marks and anchors that travel
  with the characters they cover.
- 🕰️ **Field history** — opt an entity into `history: {}` and the server logs
  what its marked fields held before each editing session, with the actor;
  restoring one is an ordinary `patch`.
- 🔐 **Auth & permission seams** — an `Authenticator` turns headers into a
  `Principal`; `CanAccessWorkspace`, `CanMutate` and `ReadFilter` decide what it
  may do. Rejections come back as results, not RPC errors.
- 🧬 **CRUD + custom mutations** — generated `put`/`patch`/`del` plus named,
  server-authoritative Go mutators with optional TypeScript mirrors. An entity
  can reserve `DELETE` for trusted writes in its own schema.
- 📤 **Server-side reactions** — a durable change feed, an `effects` runtime
  that runs handlers on committed writes with before-state and backoff, and
  `connect` for keeping an external store converged bidirectionally.
- 🗂️ **Multi-tab, multi-workspace** — tabs share one IndexedDB store; one
  visible tab holds the stream (Web Locks leader election) and the rest see its
  writes. One client holds every workspace an account belongs to, over one
  stream and one push.
- ⚛️ **React adapter** — `syncplane/react` mirrors `client.db` as hooks
  (`useQuery`, `useRow`, `useMutate`, `useText`, `useSyncStatus`, `useUndo`,
  `useOnMutationRejected`), with suspense variants, a provider that owns the
  client, and per-workspace scoping.
- 🧩 **Runtime schemas** — an app's entities can arrive as a
  `FileDescriptorSet` at runtime instead of compiled-in Go, and one process can
  host many apps.
- 🖧 **One node or many** — SQLite plus an in-process notifier needs no
  infrastructure; swap in `pgstore` and `pgnotify` and several nodes share one
  Postgres database. Nothing above the storage seam changes.

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
