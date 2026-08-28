# Querying data

This guide shows you how to query data with Syncplane in TypeScript. Use this guide to execute local queries, subscribe to live query updates, query the server directly, paginate results, filter list fields, and run grouped aggregations.

## Prerequisites

Before querying data, ensure you have:

- An initialized `SyncClient` instance connected to your backend.
- Defined entity schemas in your project.

```ts
// Local: the synced subset, through the optimistic overlay. Offline, instant.
const open = await client.db.todo.query({
	where: (t) => and(t.completed.eq(false), t.title.contains('milk')),
	orderBy: (t) => [ t.createdAtMs.desc() ],
	limit: 20,
});

// Live: re-emitted after every commit that changes the result.
const view = client.db.todo.watchQuery(
	{ where: (t) => t.completed.eq(false) },
	render,
);
// ... view.close() when done.

// Server: everything, including rows this client never synced.
const found = await client.db.todo.queryServer({
	where: (t) => t.title.contains('milk'),
});
```

## Local queries

The `query` and `watchQuery` methods evaluate queries locally against data downloaded by partial sync and merged with pending local mutations.

To run a local query and handle partial sync coverage:

1. Call `client.db.<entity>.query()` with your query criteria:

   ```ts
   const rows = await client.db.issue.query({
   	orderBy: (i) => [ i.createdAtMs.desc() ],
   });
   if (!rows.coverage.complete) {
   	const all = await client.db.issue.queryServer({/* ... */});
   }
   ```

2. Inspect the `rows.coverage` property before treating an empty result as missing data:
   - If `coverage.complete` is `false`, the client holds only a subset of rows. Fetch missing rows from the server with `queryServer()`.
   - If `coverage.hydrating` is `true`, rows are downloading in the client's initial snapshot. Display a loading indicator rather than sending a server request. The flag clears when the snapshot finishes loading.

For more information, see [Sync only part of a workspace](partial-sync.md) and [Partial sync and coverage](../explanation/partial-sync-and-coverage.md).

## Server queries

Use `queryServer` to send a query to the server's `Query` RPC. This method allows your application to access rows that the client did not sync, such as when searching the full dataset or opening an entity ID not present in the local view.

To execute a server query:

1. Call `client.db.<entity>.queryServer()` with your query options.
2. Process the returned results.

Server queries have the following characteristics:

- Results reflect server state. They are not merged with optimistic local mutations, are not stored locally, and do not trigger watchers.
- Local unpushed edits are not reflected in the results.
- Network connectivity is required.
- Read filters always apply on the server.
- Queries without a supporting index perform a full entity scan on the server, bounded in memory to the page size. Prefer selective predicates over deep pagination.

## Paging

To page through results, use `after` and `before` cursors instead of `offset`. Keyset cursors represent a fixed position in the sort order, preventing skipped or duplicated rows when rows are inserted concurrently.

To paginate query results:

1. Pass a `limit` and an optional `after` cursor to `client.db.<entity>.query()` or `client.db.<entity>.queryServer()`:

   ```ts
   let after: QueryCursor | undefined;
   for (;;) {
   	const page = await client.db.issue.query({
   		orderBy: (i) => [ i.createdAtMs.desc() ],
   		limit: 50,
   		after,
   	});
   	render(page);
   	if (!page.hasMore) break;
   	after = page.endCursor;
   }
   ```

2. Read pagination metadata from the result object:
   - `startCursor`: The cursor for the first item in the page. Pass this as `before` to navigate backward.
   - `endCursor`: The cursor for the last item in the page. Pass this as `after` to navigate forward.
   - `hasMore`: A boolean indicating whether more rows remain in the traversal direction.
   - `total`: The total count of matching rows.

3. To step backward, pass `startCursor` to `before`. The `limit` option counts backward and returns the preceding page.
4. To query a bounded range, provide both `after` and `before`. The `total` property counts matches within that range.
5. If your UI does not need the total count, set `total: false` in the query options to skip the count calculation and improve performance on large datasets.

Cursors are base64url-encoded strings suitable for URLs and JSON serialization. Note the following rules when using cursors:

- A cursor is tied to the entity and sort order under which it was issued. Replaying a cursor with a different `orderBy` clause produces an error.
- Both `query` and `queryServer` accept and emit the same cursors.
- Use `offset` only when you must jump to an arbitrary position in the result set. The server caps `offset` at `MaxQueryOffset`.
- Do not combine `offset` with `after` or `before`. Specifying both causes the query to fail.

## Paging a live result

The `offset` and `limit` options inside a query remain fixed for the lifetime of a watch. To page through live query results, pass a `window` option and adjust it dynamically.

To paginate a live query:

1. Initialize a watch query with a `window` object containing `offset` and `limit`:

   ```ts
   const view = client.db.todo.watchQuery(
   	{ orderBy: (t) => [ t.createdAtMs.desc() ] },
   	(rows) => paint(rows, rows.total),
   	{ window: { offset: 0, limit: 50 } },
   );
   onScroll((first) => view.setWindow({ offset: first, limit: 50 }));
   ```

2. Update the visible slice by calling `view.setWindow({ offset, limit })` as the user navigates.
3. Pass `view.setWindow({})` to deliver all matching rows without slicing.
4. When finished, call `view.close()` to stop watching.

The watch maintains the complete matching result set in memory. Calling `setWindow` slices the in-memory set without re-executing a database read, and `rows.total` reports the total number of matched rows.

You can combine cursors with a window. The `after` and `before` cursors constrain which rows match the query, while `window` slices the resulting subset by rank.

## Filtering on a list

To filter by repeated scalar fields or message lists, use list operators in your `where` clause.

### Filtering repeated scalar fields

For repeated scalar fields, use `has`, `hasAny`, `hasAll`, or `isEmpty`:

1. Define the repeated field in your schema. Mark it with `indexed: true` for efficient range queries:

   ```proto
   repeated string labels = 10 [(syncplane.v1.field_behavior) = { indexed: true }];
   ```

2. Query using `has` or related operators:

   ```ts
   await client.db.issue.query({ where: (i) => i.labels.has('bug') });
   ```

### Filtering repeated message lists

For lists of messages or to-many references, use `any()` to scope conditions to a single element:

1. Use `any()` in the `where` predicate:

   ```ts
   await client.db.issue.query({
   	where: (i) => i.assignees.any((a) => a.id.eq('maya')),
   });
   await client.db.order.query({
   	where: (o) => o.lineItems.any((li) => and(li.sku.eq('A-1'), li.qty.gt(3))),
   });
   ```

2. When querying reference lists, address only key fields on the target stub. Accessing non-key fields throws an error. Reference fields are automatically indexed by the engine.

3. For other message lists, declare a composite index specifying the element field path:

   ```proto
   index: {
   	name: "by_sku"
   	key: [ { path: "line_items.sku" } ]
   }
   ```

Composite indexes over list fields fan out by storing one entry per element. When an equality filter pins the list element, the index supplies the remaining sort order, such as `{ key: [ { path: "labels" }, { path: "rank" desc: true } ] }`. Each index can contain only one list field.

## Counting per value — badges, facets, dropdowns

To calculate aggregation counts or extract unique values for badges, facet filters, and dropdown menus, use `groupCount` and `distinct`.

To count or list distinct values:

1. Call `client.db.<entity>.groupCount()` to group matching records and return counts per value:

   ```ts
   // One badge per status, under the filter the board is showing.
   const byStatus = await client.db.issue.groupCount({
   	by: (i) => i.status,
   	where: (i) => i.projectId.eq(projectId),
   });
   // [ { value: 'done', count: 12 }, { value: 'open', count: 4 } ]

   // The assignees this project actually has — before any of them is counted.
   const assignees = await client.db.issue.distinct({
   	by: (i) => i.assignee.id,
   	where: (i) => i.projectId.eq(projectId),
   });
   ```

2. For React applications, use the `useGroupCount` hook:

   ```tsx
   function StatusBadges({ projectId }: { projectId: string }) {
   	const counts = sync.db.issue.useGroupCount({
   		by: (i) => i.status,
   		where: (i) => i.projectId.eq(projectId),
   	});
   	// Zero-count columns come from YOUR domain, not from the read: a status no
   	// row holds is absent rather than 0.
   	return STATUSES.map((s) => (
   		<Badge
   			key={ s }
   			label={ s }
   			count={ counts.find((g) => g.value === s)?.count ?? 0 }
   		/>
   	));
   }
   ```

3. If you need counts over rows not synced locally, call `groupCountServer` or `distinctServer`.

When using grouped reads:

- A repeated `by` path counts memberships rather than rows. An issue with two labels increments both label counts.
- Results return in ascending value order. Rows without a value (`value: undefined`) appear last. The `limit` option truncates from the beginning of this order.
- Declare composite indexes matching `[eq-filter fields…, groupPath, …]` to allow the query planner to perform index skip-scans instead of scanning all rows. For more information, see [Grouped reads](../explanation/grouped-reads.md).

## The first rows of each group — column previews, top-N

To retrieve the top rows of each partition (for example, displaying the first twenty cards in each board column), use `groupRows`.

To fetch the first rows for each group:

1. Call `client.db.<entity>.groupRows()` specifying `by`, `orderBy`, `perGroup`, and any `where` filter:

   ```ts
   const columns = await client.db.issue.groupRows({
   	by: (i) => i.status,
   	orderBy: (i) => [ i.rank.asc() ],
   	perGroup: 20,
   	where: (i) => i.projectId.eq(projectId),
   });
   // [ { value: 'done', rows: [ …20 issues… ], total: 41, hasMore: true }, … ]
   ```

2. For React applications, use `useGroupRows`. For server-side execution across unsynced data, call `groupRowsServer`.
3. Inspect the returned group objects:
   - `rows`: The array of entity records up to `perGroup` items.
   - `total`: The total count of matching rows in that group.
   - `hasMore`: A boolean indicating whether additional rows exist in that group.

To optimize grouped row queries:

- Set `total: false` when your UI displays rows without count badges, skipping the full count traversal.
- Define a composite index where components following the grouping path match the `orderBy` clause:

  ```proto
  index: {
    name: "by_status_rank"
    key: [ { path: "status" }, { path: "rank" } ]
  }
  ```

- If using a repeated `by` path, note that an entity appears in each group corresponding to its elements.

## Confirming the result

To verify that your query works as expected:

1. Confirm that the returned data matches your query filters and ordering criteria.
2. For local queries, check `rows.coverage.complete` to ensure that partial sync returned complete data.
3. For live queries (`watchQuery` or React hooks), mutate an entity record and verify that your callback or UI component re-renders with updated data.
4. To check whether a query used an index or scanned records, inspect the `onSyncEvent` telemetry handler:

   ```ts
   const client = new SyncClient({
   	url,
   	entities,
   	onSyncEvent: (e) => {
   		if (e.type === 'query' && e.declined) {
   			metrics.increment('query.scan', 1, {
   				entity: e.entity,
   				reason: e.declined,
   			});
   		}
   	},
   });
   ```

## Related

- [Secondary indexes](../reference/proto-options.md#secondary-indexes)
- [Limits and trade-offs](../explanation/limits-and-trade-offs.md)
- [TypeScript client API](../reference/typescript-client.md#queries) — Full operator reference per field type.
- [Grouped reads](../explanation/grouped-reads.md) — How skip-scans and grouped aggregations operate.
- [Query from server-side code](server-side-queries.md) — Go query equivalents.
- [Partial sync and coverage](../explanation/partial-sync-and-coverage.md) — Coverage models and sync boundaries.
