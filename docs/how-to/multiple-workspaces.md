# Holding several workspaces on one client

This guide shows you how to configure and use a single client across multiple workspaces. Use this guide when a user belongs to several workspaces and needs to share storage, a change stream, and push operations across them.

## Prerequisites

Before you begin, ensure you have:

- An account ID (`accountId`) for the user.
- The list of workspace IDs the user belongs to.
- Schema definitions for your entities (for example, `TodoSchema`).

## Configuring the client

1. Initialize and start the `SyncClient` with your account ID, workspace list, and default workspace:

   ```ts
   const client = new SyncClient({
   	url: 'https://sync.example.com',
   	accountId: 'user-42', // the storage scope: one IndexedDB database
   	workspaces: [ 'team', 'personal' ],
   	defaultWorkspace: 'team', // what client.mutate / client.db bind to
   	entities: [ TodoSchema ],
   });
   await client.start();
   ```

   `accountId` names the store and is required when the client holds more than one workspace. For a single-workspace client, `accountId` defaults to the workspace ID. If you have two accounts, create two separate clients because credentials attach per request and cannot share a stream or a `client_id`.

2. Write mutations to the default workspace or to a specific workspace:

   ```ts
   await client.db.todo.mutate.create({ title }); // → 'team', the default
   await client.ws('personal').db.todo.mutate.create({ title });
   ```

   The client-level API binds to the default workspace. To target a different workspace, call `client.ws(id)`.

   **Note:** The server sequences partition operations individually. Operations such as writes, `transact()` batches, undo steps, references, `queryServer`, and bootstraps do not cross workspaces.

## Reading across workspaces

To read data across multiple workspaces, use cross-workspace query methods such as `scanAll`, `queryAll`, or `watchQueryAll`.

Cross-workspace reads are exact, not a fan-out-and-merge operation. Every workspace's rows for an entity sit under one key prefix, so ordering and limits apply to the merged set. Each row includes its `workspaceId`, which routes write operations back to that workspace:

```ts
const mine = await client.ws('personal').db.todo.scan();
const everywhere = await client.queryAll(TodoSchema, {
	where: (t) => t.completed.eq(false),
});
for (const row of everywhere) {
	await client.ws(row.workspaceId).mutate.del(TodoSchema, row.key);
}
```

In React, call `db.todo.useQueryAll(query, opts?)` to create a single subscription across the whole client:

```tsx
const everywhere = db.todo.useQueryAll({ where: (t) => t.completed.eq(false) });
const byWorkspace = Map.groupBy(everywhere, (row) => row.workspaceId);
```

### Naming the workspaces to read

To restrict a query to specific workspaces instead of filtering in the component, pass the `workspaces` option:

```tsx
const followed = db.todo.useQueryAll({ where: (t) => t.completed.eq(false) }, {
	workspaces: [ 'team', 'personal' ],
});
```

The read scans only the specified workspace prefixes, and `followed.total` counts only rows within those workspaces.

If you specify a single workspace, the query runs as a single-workspace query and can use a [secondary index](../reference/proto-options.md#secondary-indexes). Queries spanning multiple workspaces cannot use secondary indexes. An empty array reads nothing and is not shorthand for all workspaces.

You can also pass `workspaces` to one-shot queries:

```ts
client.queryAll(TodoSchema, q, { workspaces });
```

### A predicate that differs per workspace

Rows do not cross workspaces, and referenced IDs (such as member IDs) differ across workspaces. To apply a different filter condition per workspace under a single subscription, use `byWorkspace`:

```tsx
const mine = db.task.useQueryAll({
	orderBy: (t) => [ t.dueAt.asc() ],
	byWorkspace: (workspaceId) => {
		const me = memberIdIn(workspaceId);
		// Returning nothing leaves the workspace out of the read entirely.
		return me && ((t) => t.assignee.eq(me));
	},
}, { workspaces: followedIds });
```

Only the predicate varies per workspace. Query options such as `orderBy`, `limit`, `offset`, and `total` apply to the merged result. Each workspace query leg runs as a single-workspace query and can use an index. The `byWorkspace` option requires `workspaces` and throws an error without it.

To order results by relevance, use [`relevance()`](search.md#searching-across-workspaces). Give each workspace predicate its own `match` call, and the merged result returns best matches first across all workspaces.

### Rows named one by one

To read specific rows instead of querying by predicate, pass row references to `client.watchRowsAll(schema, refs, cb, opts?)` or `db.todo.useRowsAll(refs, opts?)`. Each reference takes `{ workspaceId, key }` and performs a point read rather than a scan:

```tsx
const tasks = db.todo.useRowsAll([
	{ workspaceId: 'personal', key: 't1' },
	{ workspaceId: 'work', key: 't9' },
]);
```

Mutation hooks bind to a single workspace. To write back to the source workspace of a row from a component, call `useSyncClient()` and use `client.ws(row.workspaceId).db.todo.mutate`.

## Adding and removing

To add or remove workspaces dynamically on an active client:

1. Add a new workspace by calling `addWorkspace`:

   ```ts
   client.addWorkspace('archive'); // bootstraps on the next cycle
   ```

2. Remove a workspace and purge local data by calling `removeWorkspace`:

   ```ts
   // Stop syncing + delete locally. `discarded` carries whatever was still queued
   // for the workspace — the only copy left, since the purge deleted the queue.
   const { discarded } = await client.removeWorkspace('archive', {
   	purge: true,
   });
   ```

   A purge drops the rows, overlay, cursor, and queued mutations for that workspace.

   Discarded mutations cannot be acknowledged by the server. The client returns them in `result.discarded`, reading each mutation inside the transaction that deleted it. The client also reports each discarded mutation through `onMutationRejected` with the `workspace_purged` code. Use the return value if your application needs to preserve unsent work.

## Confirming the result

To confirm that multi-workspace sync works:

1. Run a query across your configured workspaces:

   ```ts
   const everywhere = await client.queryAll(TodoSchema, {
   	where: (t) => t.completed.eq(false),
   });
   ```

2. Verify that `everywhere` contains rows from multiple workspaces and that each row contains the expected `workspaceId`.

## Related

- [Workspaces are a hard partition](../explanation/convergence-model.md#workspaces-are-a-hard-partition)
- [Build a UI with React](react.md) — `<WorkspaceProvider workspaceId>` scopes a subtree's hooks to one workspace, and `{ workspaceId }` scopes a single call.
- [Handle failures](handle-failures.md) — losing access to one workspace leaves the others running.
- [Sync status](../reference/sync-status-and-codes.md) — which fields are account-wide and which are per workspace.
