# Hold several workspaces on one client

A user who belongs to several workspaces gets one client, not one per workspace.
They share a store, a change stream and a push; the client-level API stays bound
to a **default workspace**, so single-workspace code is unchanged, and
`client.ws(id)` reaches any other.

```ts
const client = new SyncClient({
	url: 'https://sync.example.com',
	accountId: 'user-42', // the storage scope: one IndexedDB database
	workspaces: [ 'team', 'personal' ],
	defaultWorkspace: 'team', // what client.mutate / client.db bind to
	entities: [ TodoSchema ],
});
await client.start();

await client.db.todo.mutate.create({ title }); // → 'team', the default
await client.ws('personal').db.todo.mutate.create({ title });
```

## Reading across workspaces

```ts
const mine = await client.ws('personal').db.todo.scan();
const everywhere = await client.queryAll(TodoSchema, {
	where: (t) => t.completed.eq(false),
});
for (const row of everywhere) {
	await client.ws(row.workspaceId).mutate.del(TodoSchema, row.key);
}
```

Cross-workspace reads (`scanAll`, `queryAll`, `watchQueryAll`) are exact, not a
fan-out-and-merge: every workspace's rows of an entity sit under one key prefix,
so ordering and limits apply to the merged set. Every row names where it lives,
which is also how a write routes back.

In React that live read is `db.todo.useQueryAll(query, opts?)` — one
subscription over the whole client:

```tsx
const everywhere = db.todo.useQueryAll({ where: (t) => t.completed.eq(false) });
const byWorkspace = Map.groupBy(everywhere, (row) => row.workspaceId);
```

### Naming the workspaces to read

A view over _some_ of the workspaces — "the areas I follow", a filtered
dashboard — takes `workspaces` rather than reading everything and filtering in
the component:

```tsx
const followed = db.todo.useQueryAll({ where: (t) => t.completed.eq(false) }, {
	workspaces: [ 'team', 'personal' ],
});
```

The read then scans only the prefixes it names, and `followed.total` counts what
you asked about — where a component filter leaves a total describing a different
question. Naming a single workspace is worth it on its own: the read is then a
one-workspace query by construction, so a
[secondary index](../reference/proto-options.md#secondary-indexes) can serve it,
which nothing spanning workspaces can. An empty array reads nothing; it is not
shorthand for "all".

The same option is on the one-shot: `client.queryAll(TodoSchema, q, {
workspaces })`.

### A predicate that differs per workspace

A row never crosses a workspace, so anything referenced by id — the member a
task is assigned to — has a **different id in each one**, and "assigned to me"
is a different literal per workspace. One `where` cannot say it. `byWorkspace`
builds one predicate per workspace, still under a single subscription and a
single merged result:

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

Only the predicate varies: order, page and total belong to the merged result, so
`orderBy`, `limit`, `offset` and `total` stay at the top level. Each workspace's
leg is a single-workspace query and can be index-served. `byWorkspace` needs
`workspaces` — the predicates are built from the set it runs over — and throws
without it.

Ordering by [`relevance()`](search.md#searching-across-workspaces) works here as
anywhere else: give each workspace's predicate its own `match`, and the merged
result comes back best-match-first across all of them.

### Rows named one by one

When the set is not a predicate but a list of specific rows — a personal task
list, a pinned set — name them by ref instead. `client.watchRowsAll(schema,
refs, cb, opts?)` and `db.todo.useRowsAll(refs, opts?)` take
`{ workspaceId, key }` and cost one point read per ref rather than a scan:

```tsx
const tasks = db.todo.useRowsAll([
	{ workspaceId: 'personal', key: 't1' },
	{ workspaceId: 'work', key: 't9' },
]);
```

Writing back does not go through a hook: a mutate hook binds to one workspace,
so a component that writes into whichever workspace a row came from takes
`useSyncClient()` and calls `client.ws(row.workspaceId).db.todo.mutate`.

What does **not** cross a workspace is anything the server sequences per
partition — a write, a `transact()` batch, an undo step, a reference,
`queryServer` — nor a bootstrap.

## Adding and removing

```ts
client.addWorkspace('archive'); // bootstraps on the next cycle

// Stop syncing + delete locally. `discarded` carries whatever was still queued
// for the workspace — the only copy left, since the purge deleted the queue.
const { discarded } = await client.removeWorkspace('archive', { purge: true });
```

A purge drops the rows, the overlay, the cursor and anything still queued for
that workspace. The queued mutations can never be acked, so they are discarded —
but not silently: they come back as `result.discarded` (each with its decoded
`mutation`, read inside the transaction that deleted them, so the list is
exactly what was lost) and each is reported through `onMutationRejected` with
code `workspace_purged`. Take the return value if the app wants to keep unsent
work; the callback is what makes ignoring it safe.

## `accountId`

`accountId` names the store. It defaults to the workspace id for a
single-workspace client (so its database stays exactly where it was) and is
**required** once the client holds more than one.

Two accounts need two clients: a credential is attached per request, so they can
never share a stream or a `client_id`.

## Why the partition is hard

`workspace_id` is a hard partition key everywhere and the server sequences per
workspace, so each workspace keeps its own cursor, pending queue and mutation-id
counter — the server requires per-`(workspace, client_id)` gap-free ids. The
sharing is at the transport and storage layers only.

## Related

- [Build a UI with React](react.md) — `<WorkspaceProvider workspaceId>` scopes a
  subtree's hooks to one workspace, and `{ workspaceId }` scopes a single call.
- [Handle failures](handle-failures.md) — losing access to one workspace leaves
  the others running.
- [Sync status](../reference/sync-status-and-codes.md) — which fields are
  account-wide and which are per workspace.
