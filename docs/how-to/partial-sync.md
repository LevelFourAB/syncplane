# Syncing part of a workspace

This guide shows you how to sync a subset of workspace data to a client. Use partial sync to limit bootstrap, pull, and local materialization to the records your application needs.

## Prerequisites

Before you configure partial sync, ensure that you have:

- A `SyncClient` instance configured with your entity schemas.

## Steps

1. Configure subscription scopes when initializing the client.
   Define a `subscriptions` array in `SyncClient` to scope which records sync:

   ```ts
   const client = new SyncClient({
   	// ...
   	subscriptions: [
   		// Predicate scope: every open todo.
   		subscription(TodoSchema, (t) => t.completed.eq(false)),
   		// Rank scope: the 500 most recently changed comments.
   		subscription(CommentSchema, undefined, { limit: 500, byRecency: true }),
   		// Rank scope by a field: the top 100 issues by priority.
   		subscription(IssueSchema, (i) => i.state.eq('open'), {
   			limit: 100,
   			orderBy: (i) => [ i.priority.desc() ],
   		}),
   		// Key scope: the documents pinned on this device, by row key.
   		subscription(DocumentSchema, { keys: pinnedDocumentIds }),
   	],
   });
   ```

   You can scope subscriptions in four ways:
   - **Predicate scope**: Match records that satisfy a condition.
   - **Window scope by recency**: Limit records by recent changes with `{ limit: <count>, byRecency: true }`. Ranking by write sequence is the cheapest scope ordering to maintain, because it needs no field comparison.
   - **Window scope by field**: Limit records ordered by field values with `{ limit: <count>, orderBy: ... }`.
   - **Key scope**: Name records by row key with `{ keys: [...] }`. Use this for sets that no field expresses, such as documents pinned for offline use, alongside a predicate subscription for the metadata of every document.

   A `limit` makes a subscription windowed. The `orderBy` or `byRecency` option determines row rank, and ties break on row key. The server bounds the cap with `MaxWindowSize` (default 1000). A key scope holds only the named rows. The keys match the row keys that `get()` accepts, an empty array names no rows, and the server bounds the set with `MaxSubscriptionKeys` (default 1000). You cannot combine `keys` with `where` or `limit`. The `include` option works with all scopes. An empty subscription set syncs the full workspace.

2. Update subscriptions when the required scope changes.
   If you need to change subscriptions after initialization, call `setSubscriptions`:

   ```ts
   // Changing the set later delta-resyncs the difference, not a re-bootstrap.
   client.setSubscriptions([ subscription(TodoSchema) ]);
   // Pinning one more document delivers that document and nothing else.
   client.setSubscriptions([
   	subscription(DocumentSchema, { keys: [ ...pinnedDocumentIds, newId ] }),
   ]);
   ```

   Changing the subscription set delta-resyncs the difference without re-bootstrapping. For a key scope, the server reads only the rows named on either side of the change.

3. Check coverage on local reads.
   A client with scoped data cannot answer local queries ordered differently from the subscription scope. Check `coverage.complete` on query results:

   ```ts
   const rows = await client.db.issue.query({
   	orderBy: (i) => [ i.createdAtMs.desc() ],
   });
   if (!rows.coverage.complete) {
   	// bounds: ('unsubscribed' | 'predicate' | 'window' | 'hydrating')[],
   	// plus limit for a window
   	const all = await client.db.issue.queryServer({/* ... */});
   }
   ```

   If `rows.coverage.complete` is `false`, handle the bounds:
   - If bounds include `unsubscribed`, `predicate`, or `window`, call `queryServer` to fetch full results from the server. A key scope reports `predicate`.
   - If the bound is `hydrating`, display a loading state instead of querying the server. The initial snapshot lands entity by entity. Readers re-fire when the snapshot fills. `bootstrapPriority` determines which entities land first, and `status().workspaces[id].hydration` reports outstanding entities.

4. Bring one row into scope on demand.
   A row returned by `queryServer` is a snapshot. The client does not store it locally, so you cannot watch or edit it. To sync the row while open, hold it:

   ```ts
   const hits = await client.db.document.queryServer({
   	where: (d) => d.title.match(term),
   });
   const hold = client.db.document.hold(hits[0].key);
   await hold.ready;
   const text = await client.db.document.text(hits[0].key, 'body');
   // ... edit, then:
   await text.close();
   hold.release();
   ```

   The row arrives on the next sync cycle. The row remains until you release the last hold on it, plus `holdGraceMs` (default 30 seconds). In React, `useRow(key, { hold: true })` and `useText(key, field, { hold: true })` hold the row for the lifetime of the component. See [Holds](../reference/typescript-client.md#holds-one-row-in-scope-on-demand).

## Confirming the result

To confirm that partial sync works:

1. Run a local query for an entity within your subscription scope.
2. Verify that returned rows match the expected predicate or window limits.
3. Inspect `rows.coverage.complete` to verify the coverage status of the query.

## Related

- [Partial sync API](../reference/typescript-client.md#partial-sync-and-subscriptions)
- [Partial sync and coverage](../explanation/partial-sync-and-coverage.md) — how
  windows are maintained, and what `coverage.complete` guarantees.
- [Query data](query-data.md)
