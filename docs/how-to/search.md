# Search data

Declare which fields an entity is searched by, then query with `match` and
order by relevance. Search is case- and accent-insensitive, matches the final
term as a prefix while the user is typing, and works offline over the synced
subset exactly as it works on the server over everything.

## Declare the search fields

Mark fields in the .proto — `searchable` for the simple case, an entity-level
`search` block when you want to control importance order or analyzer options:

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

Order is importance: `searchable`-marked fields come first (in field order),
then the `search` block's fields (in declaration order). Here a match in
`title` outranks one in `summary`, which outranks one in `body`. A
collaborative-text field is searched as the plain text the user sees.

The same declaration in a [TS-first schema](define-schemas-in-typescript.md):

```ts
const article = entity('article', {
	id: text().key(),
	title: text().searchable(),
	summary: text(),
}, {
	search: { fields: [ 'summary' ] },
});
```

Search reads **singular string fields** (collaborative text included). A
repeated field, a number, or a declaration naming no fields at all is rejected
at registration.

## Query

`match` is entity-level — call it on the query root, not on a field — and
`relevance()` orders best match first:

```ts
const hits = await client.db.article.query({
	where: (a) => a.match('milk cow'),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
});
```

Every term must occur in some searchable field. The query is analyzed exactly
like the fields are, so `CAFE` finds `Café`; and the **final term matches as a
prefix** when the query ends inside it, so wiring an input straight through
works as search-as-you-type — `mil` finds "milk" — while a trailing space
makes the term exact. An empty query matches every row.

`match` is an ordinary condition: combine it with filters, use it in
`watchQuery` for a live result, or skip `relevance()` to keep your own
ordering.

```ts
const view = client.db.article.watchQuery({
	where: (a) => and(a.match(input.value), a.state.eq('published')),
	orderBy: (a) => [ a.relevance() ],
	limit: 20,
}, render);
```

From Go, the same query is `query.Match` and `query.Relevance`:

```go
tx.Query(&articlev1.Article{}, query.Spec{
    Where:   query.Match("milk cow"),
    OrderBy: []*syncplanev1.OrderKey{query.Relevance()},
    Limit:   20,
}, collect)
```

`relevance()` requires exactly one `match` in the predicate — it is that
condition's rank. Paging works as for any other ordering: pass a result's
`endCursor` back as `after`, and a cursor minted locally is valid on the
server and the other way around.

## Local, server, or both

A local `query` searches the synced subset, instantly and offline; under
partial sync that may not be everything. The result's `coverage` report says
so, and `queryServer` runs the same query over everything the server holds —
same analyzer, same ranking, so falling back does not reorder what was already
on screen:

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

See [Query data](query-data.md) for the coverage pattern in general.

## The index behind it

Declaring search fields also declares a **term index**, maintained on every
write on both sides, that a match query uses to narrow its candidates instead
of scanning the entity. Everything that is true of
[secondary indexes](../reference/proto-options.md#secondary-indexes) is true
of it: it is invisible and additive, a new declaration backfills in the
background (queries scan — correctly — until the pass finishes and the
`indexing` event fires), and using it never changes an answer.

Costs to know:

- **Writes pay for it.** A write re-stores one posting per distinct term of
  the row's searchable fields — a few dozen for typical rows. A row with more
  than 256 distinct terms stores a single overflow marker instead and is
  simply considered by every search, so huge documents degrade the narrowing,
  never the results.
- **Common-word queries narrow little.** A term that appears in most rows
  yields most rows as candidates; the query is still correct, just closer to a
  scan. Ranking, not the index, is what puts the good hits first.

Why matching and ranking behave as they do — and what search deliberately does
not attempt — is covered in [the search model](../explanation/search.md).

## Related

- [Query data](query-data.md) — local vs server queries, coverage, paging.
- [Define schemas in TypeScript](define-schemas-in-typescript.md)
- [The search model](../explanation/search.md)
