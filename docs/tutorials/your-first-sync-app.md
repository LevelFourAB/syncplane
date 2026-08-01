# Your first sync app

By the end of this tutorial you will have a todo app running against a real sync
server, watch a change appear in a second browser tab without a refresh, watch
an edit made offline arrive when you reconnect, and add an entity of your own to
the schema.

You will work inside a checkout of this repository, so nothing has to be
published or wired up before you start. Building an app in your _own_ module
follows the same loop; [Define entities in your own module](../how-to/define-entities.md) covers the one extra step that needs.

## Before you start

```sh
mise install     # go, node, pnpm, buf, task, ginkgo — all pinned in mise.toml
```

## 1. Start the server

```sh
task demo:server
```

That runs `cmd/todoserver`, which is about forty lines of wiring. The
part that matters:

```go
store, err := sqlitestore.Open(*dbPath)     // one SQLite file holds everything
defer store.Close()

srv, err := server.New(store, append(
	exampleapp.Options(perms),              // the entities + mutators
	/* options */
)...)
defer srv.Close()

http.ListenAndServe(*addr, cors(srv.HTTPHandler()))
```

There is no schema migration step and no table per entity. The server learns
its schema from the declared proto messages, and rows land in generic storage
keyed by `(workspace, entity, key)`.

## 2. Start the app

In a second terminal:

```sh
task demo:web
```

Open the printed URL, then open it a second time in another tab. Add a todo in
one tab. It appears in the other in well under a second — no polling, no
refresh: the server pushed the changed row down the live `Subscribe` stream to
the tab holding it, and that tab wrote it to the IndexedDB database both tabs
share.

Try a few things:

- **Toggle a todo.** The checkbox flips instantly. The write went to the local
  store first and queued for the server; the round trip happened behind it.
- **Reorder the list by dragging.** Reordering patches one field on one row —
  a fractional index — rather than renumbering the list.
- **Type a title containing `forbidden`.** It appears, then rolls back. The
  server's `CanMutate` hook rejected it, and the client dropped the mutation and
  restored the previous state.
- **Undo and redo.** Inverses push as ordinary mutations, so undo converges the
  same way every other edit does.

## 3. Go offline

Open DevTools, switch the network to _Offline_, and keep editing. Everything
still works: reads come from the local store, and writes queue.

The status line reports pending mutations. Go back online and they drain — the
client pushes the queue, pulls what changed while it was away, and **rebases**:
it applies the server's changes, drops the mutations the server acknowledged,
clears its optimistic overlay, and replays whatever is still pending on top of
the new base.

Two tabs, both offline, both editing the same todo is worth trying too. When
they reconnect, the field each of them touched survives — merges are per field,
not per row. [The convergence model](../explanation/convergence-model.md)
explains exactly who wins when both touch the _same_ field.

## 4. Add an entity of your own

Entities are proto messages carrying an annotation. Open
`proto/examples/example/todo/v1/todo.proto` and add one:

```proto
message Tag {
	option (syncplane.v1.entity) = {
		name: "tag"
		key_fields: [ "id" ]
	};

	string id = 1;
	string label = 2;
	string color = 3;
}
```

`name` is what the entity is called on the wire and in storage; `key_fields`
names the fields whose values form the row key.

Generate both halves of the contract:

```sh
task generate
```

This emits Go into `engine/gen/` and TypeScript into `client/src/gen/`. Never
edit either by hand.

Declare it on the server, in `internal/exampleapp/exampleapp.go` alongside the
existing entities:

```go
server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}, &todov1.Tag{})
```

And hand it to the client, in `examples/todo/src/main.ts`:

```ts
const client = new SyncClient({
	url: SERVER_URL,
	workspaceId: 'demo',
	entities: [ TodoSchema, NoteSchema, AnnotationSchema, TagSchema ],
	// ...
});
```

Restart the server (`task demo:server`) and the app picks up the rest on
reload. You now have a full CRUD surface for tags, with no further code:

```ts
const id = await client.db.tag.mutate.create({
	label: 'urgent',
	color: '#f00',
});
await client.db.tag.mutate.patch(id, { color: '#c00' });
const tags = await client.db.tag.query({ where: (t) => t.label.eq('urgent') });
client.db.tag.watch((rows) => console.log(rows.length, 'tags'));
```

Try it from the browser console with both tabs open — the second tab's watcher
fires.

That is the whole loop: **annotate a message → `task generate` → register on the
server → list it on the client**. Everything else in syncplane is layered on top
of it.

## Where to go next

- [Adding collaborative text](adding-collaborative-text.md) — the next tutorial:
  a field two people can type into at once.
- [Query data](../how-to/query-data.md) — local, live and server-side queries.
- [Authentication and permissions](../how-to/authentication-and-permissions.md)
  — the demo server admits everyone; a real one should not.
- [The convergence model](../explanation/convergence-model.md) — what actually
  happened during step 3.
