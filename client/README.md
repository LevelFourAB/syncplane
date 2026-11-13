# syncplane

The TypeScript client for [Syncplane](https://github.com/LevelFourAB/syncplane), a sync engine for local-first applications. The client reads and writes a local store, so an application works offline. A Syncplane server orders every write, and the client replays its pending writes on top of the rows that the server confirmed.

The package is ESM-only.

## Install

```sh
npm install syncplane
```

The client needs a Syncplane server. To set one up, see the [Go server quick start](https://github.com/LevelFourAB/syncplane#go-server).

## Quick start

Entities are Protocol Buffers messages that the server and the client share. Generate TypeScript for them with [Buf](https://buf.build) and `protoc-gen-es`, or define them in TypeScript with the [schema builder](https://github.com/LevelFourAB/syncplane/blob/main/docs/how-to/define-schemas-in-typescript.md).

Connect a client and write rows:

```ts
import { SyncClient } from 'syncplane';
import { TodoSchema } from './gen/todo/v1/todo_pb.js';

const client = new SyncClient({
	url: 'https://sync.example.com',
	workspaceId: 'demo',
	entities: [ TodoSchema ],
});
await client.start(); // bootstrap + live stream (returns immediately)

// client.db gives each entity a schema-bound API, keyed by its name.
// Writes apply locally at once and sync in the background:
const id = await client.db.todo.mutate.create({ title: 'Buy milk' }); // mints an id
await client.db.todo.mutate.patch(id, { completed: true });
await client.db.todo.mutate.del(id);

client.db.todo.watch((rows) => render(rows));
```

Query the synced rows locally, or all rows on the server:

```ts
import { and } from 'syncplane';

const open = await client.db.todo.query({
	where: (t) => and(t.completed.eq(false), t.title.contains('milk')),
	orderBy: (t) => [ t.createdAtMs.desc() ],
	limit: 20,
});

const found = await client.db.todo.queryServer({
	where: (t) => t.title.contains('milk'),
});
```

## React

`syncplane/react` gives each entity hooks for queries and writes. The provider builds the client, starts it, and closes it on unmount:

```tsx
import { createReactSync } from 'syncplane/react';

const { SyncProvider, db } = createReactSync([ TodoSchema ]);

<SyncProvider options={ { url, workspaceId, entities: [ TodoSchema ] } }>
	<TodoList />
</SyncProvider>;

function TodoList() {
	const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });
	const { patch, del } = db.todo.useMutate();
	// ...
}
```

React 18 or later is an optional peer dependency. Only `syncplane/react` needs it.

## Entry points

| Import              | Contents                                                                                                                                     |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `syncplane`         | `SyncClient`, queries, subscriptions for partial sync, collaborative text, undo and redo, rooms and presence, and the schema builder.        |
| `syncplane/react`   | `createReactSync`, `SyncProvider` and hooks.                                                                                                 |
| `syncplane/testing` | `createTestClient` and `createTestServer`: a real client against an in-process fake server, with offline mode and rejected writes on demand. |
| `syncplane/tunnel`  | `createTunnelTransport`: the client over a message channel instead of HTTP, such as a WebSocket, a `MessagePort` or a byte stream.           |
| `syncplane/gen/*`   | Generated types for the `syncplane.v1` wire contract, such as `syncplane/gen/syncplane/v1/history_pb.js`.                                    |

## Documentation

- [Building your first sync app](https://github.com/LevelFourAB/syncplane/blob/main/docs/tutorials/your-first-sync-app.md)
- [TypeScript client API reference](https://github.com/LevelFourAB/syncplane/blob/main/docs/reference/typescript-client.md)
- [Building a UI with React](https://github.com/LevelFourAB/syncplane/blob/main/docs/how-to/react.md)
- [Testing your app without a server](https://github.com/LevelFourAB/syncplane/blob/main/docs/how-to/test-your-app.md)
- [Syncing part of a workspace](https://github.com/LevelFourAB/syncplane/blob/main/docs/how-to/partial-sync.md)
- [The convergence model](https://github.com/LevelFourAB/syncplane/blob/main/docs/explanation/convergence-model.md)
- [All documentation](https://github.com/LevelFourAB/syncplane/blob/main/docs/README.md)

## License

MIT
