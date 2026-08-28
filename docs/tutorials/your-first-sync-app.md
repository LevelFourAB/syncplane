# Building your first sync app

In this tutorial, you run a todo application against a sync server, test real-time updates and offline synchronization, and add a custom entity to the schema.

You work inside a clone of the repository, where the application and server are preconfigured.

## Prerequisites

Before you start, ensure that you have:

- Cloned this repository.
- Installed the required tools (Go, Node.js, pnpm, Buf, Task, and Ginkgo) by running:

```sh
mise install
```

## 1. Starting the server

To start the sync server, run:

```sh
task demo:server
```

This runs `cmd/todoserver`. The server initializes the SQLite store and starts listening on the configured address:

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

The server learns its schema from the declared Protocol Buffers (protobuf) messages, and rows land in generic storage keyed by `(workspace, entity, key)`. There is no schema migration step and no table per entity.

## 2. Starting the app

In a second terminal, start the web application:

```sh
task demo:web
```

To test real-time synchronization:

1. Open the printed URL in a browser tab.
2. Open the same URL in a second browser tab.
3. Add a todo item in one tab.

The item appears in the second tab without refreshing the page. The server pushes the changed row over the live `Subscribe` stream to the second tab, which writes it to the shared IndexedDB database.

Try the following interactions:

- **Toggle a todo:** Select the checkbox. The checkbox updates immediately. The write applies to the local store first and queues for the server.
- **Reorder the list:** Drag an item to reorder the list. Reordering updates a fractional index field on one row rather than renumbering the list.
- **Test rejection:** Add a todo item with a title containing `forbidden`. The item appears briefly and then rolls back. The server's `CanMutate` hook rejects the mutation, and the client drops the mutation and restores the previous state.
- **Undo and redo:** Trigger an undo or redo action. Inverses push as standard mutations, so undo converges the same way as other edits.

## 3. Testing offline synchronization

To test offline editing:

1. Open browser developer tools and set the network condition to **Offline**.
2. Edit a todo item. Reads continue from the local store, and writes queue locally. The status line reports pending mutations.
3. Set the network condition back to **Online**.

The client pushes queued mutations and pulls changes from the server. The client rebases: it applies server changes, drops acknowledged mutations, clears its optimistic overlay, and replays any pending mutations on top of the new base.

To test concurrent offline edits:

1. Set the network condition to **Offline** in both browser tabs.
2. Edit different fields of the same todo item in each tab.
3. Set both tabs back to **Online**.

Both changes persist because merges occur per field rather than per row. For more information about conflict resolution when editing the same field, see [The convergence model](../explanation/convergence-model.md).

## 4. Adding an entity

Entities are protobuf messages that carry an annotation.

1. Open `proto/examples/example/todo/v1/todo.proto` and add the `Tag` message:

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

In this definition, `name` specifies the entity name used on the wire and in storage, and `key_fields` lists the fields that form the row key.

2. Generate the contract code for Go and TypeScript:

```sh
task generate
```

This emits Go code into `engine/gen/` and TypeScript code into `client/src/gen/`. Do not edit generated files manually.

3. Register the entity on the server by editing `internal/exampleapp/exampleapp.go`:

```go
server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}, &todov1.Tag{})
```

4. Register the entity on the client by editing `examples/todo/src/main.ts`:

```ts
const client = new SyncClient({
	url: SERVER_URL,
	workspaceId: 'demo',
	entities: [ TodoSchema, NoteSchema, AnnotationSchema, TagSchema ],
	// ...
});
```

5. Restart the server:

```sh
task demo:server
```

6. Reload the application in your browser. You now have create, read, update, and delete (CRUD) operations for tags.

7. Test the entity from the browser developer console with both tabs open:

```ts
const id = await client.db.tag.mutate.create({
	label: 'urgent',
	color: '#f00',
});
await client.db.tag.mutate.patch(id, { color: '#c00' });
const tags = await client.db.tag.query({ where: (t) => t.label.eq('urgent') });
client.db.tag.watch((rows) => console.log(rows.length, 'tags'));
```

The watcher in the second tab logs the change.

The development workflow follows this loop: annotate a message, run `task generate`, register the entity on the server, and register the entity on the client.

## Next steps

You have built and run a synchronized application, verified real-time and offline updates, and added a custom entity to the schema.

To explore further:

- [Adding collaborative text](adding-collaborative-text.md): Follow the next tutorial to add collaborative text fields.
- [Define entities in your own module](../how-to/define-entities.md): Learn how to define entities in an external module.
- [Query data](../how-to/query-data.md): Run local, live, and server-side queries.
- [Authentication and permissions](../how-to/authentication-and-permissions.md): Configure authentication and access control.
- [The convergence model](../explanation/convergence-model.md): Learn how syncplane handles concurrent mutations.
