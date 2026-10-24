# Searching data

This guide shows you how to declare searchable fields on an entity and search data using relevance matching. Use this guide to query data with full-text search across local or server databases.

Search is case-insensitive and accent-insensitive. Search matches the final term as a prefix while you type. Offline search over the synced local subset works the same as server search over all data.

## Prerequisites

Before you begin, ensure you have:

- An entity schema defined in Protocol Buffers or TypeScript.
- An initialized database client or transaction.

## Declare the search fields

To make an entity searchable, mark its string fields in your schema definition.

1. If you use Protocol Buffers, mark fields with `searchable: true` for individual fields, or add an entity-level `search` block to control field importance and analyzer options:

```proto
message Article {
	option (syncplane.v1.entity) = {
		name: "article"
		key_fields: [ "id" ]
		search: {
			field: [ { path: "summary" }, { path: "body" } ]
		}
	};

	string id = 1;
	string title = 2 [(syncplane.v1.field_behavior) = { searchable: true }];
	string summary = 3;
	bytes body = 4 [(syncplane.v1.field_behavior) = { text: true }];
}
```

Field order determines importance. Fields marked with `searchable` come first in field order, followed by fields in the `search` block in declaration order. In this example, a match in `title` outranks a match in `summary`, which outranks a match in `body`. A collaborative-text field is searched as plain text.

2. If you use a [TS-first schema](define-schemas-in-typescript.md), mark fields with `.searchable()` or add a `search` options block:

```ts
const article = entity('article', {
	id: text().key(),
	title: text().searchable(),
	summary: text(),
}, {
	search: { fields: [ 'summary' ] },
});
```

Search reads singular string fields, including collaborative text. Schema registration rejects repeated fields, numbers, or declarations that name no fields.

## Query

To query searchable data, call `match` at the entity level and sort with `relevance()`:

1. Run a query with `match` in the `where` clause and `relevance()` in `orderBy`:

```ts
const hits = await client.db.article.query({
	where: (a) => a.match('milk cow'),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
});
```

Every term must occur in a searchable field. The query analyzer processes terms the same way it processes fields. For example, `CAFE` matches `Café`. The last term matches as a prefix only when the query ends inside it (search-as-you-type). For example, `mil` matches `milk`. A trailing space makes the term exact. An empty query matches every row.

2. Combine `match` with other filters or use it in a live query:

```ts
const view = client.db.article.watchQuery({
	where: (a) => and(a.match(input.value), a.state.eq('published')),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
}, render);
```

If you do not need relevance ranking, omit `relevance()` to use custom ordering.

3. If you query from Go, use `query.Match` and `query.Relevance`:

```go
tx.Query(&articlev1.Article{}, query.Spec{
    Where:   query.Match("milk cow"),
    OrderBy: []*syncplanev1.OrderKey{query.Relevance()},
    Limit:   20,
}, collect)
```

The `relevance()` ordering requires exactly one `match` condition in the predicate. For pagination, pass the result `endCursor` as `after`. A cursor created locally is valid on the server, and a cursor created on the server is valid locally.

## Searching across workspaces

To query across workspaces, use `queryAll`, `watchQueryAll`, or `useQueryAll`.

1. Specify a workspace predicate with `byWorkspace` and order by `relevance()`:

```ts
const mine = db.doc.useQueryAll({
	orderBy: (d) => [ d.relevance() ],
	byWorkspace: (workspaceId) => {
		const me = memberIdIn(workspaceId);
		return me && ((d) => and(d.match(input.value), d.owner.eq(me)));
	},
}, { workspaces: followedIds });
```

2. Ensure every workspace predicate contains a single `match` condition. A workspace predicate without a `match` condition throws an error naming the workspace.

Across workspaces, each row ranks against the match condition of its own workspace. Ranks are computed from the row alone and remain comparable across workspaces; see [the search model](../explanation/search.md).

## Local, server, or both

A local `query` searches the synced local subset offline. When partial sync is active, the local database might not contain all server records.

1. Execute a local query and check `local.coverage.complete`.
2. If `coverage.complete` is `false`, call `queryServer` with the same query parameters to search all server data:

```ts
const local = await client.db.article.query({
	where: (a) => a.match(input.value),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
});
if (!local.coverage.complete) {
	const all = await client.db.article.queryServer({
		where: (a) => a.match(input.value),
		orderBy: (a) => [ a.relevance() ],
		limit: 20,
	});
}
```

For more information on the coverage pattern, see [Query data](query-data.md).

## Confirming the result

To confirm that your search configuration works:

1. Run a query with a search term that matches known test records.
2. Verify that the query returns matching records in order of relevance.
3. If you use search-as-you-type, verify that prefix queries return expected matches before you enter the complete term.

## Related

- [Query data](query-data.md) — local vs server queries, coverage, paging.
- [Define schemas in TypeScript](define-schemas-in-typescript.md)
- [The search model](../explanation/search.md)
