# Query data

Queries come in two flavours, and the difference is which data they can see.

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

`query` / `watchQuery` evaluate **locally**, so they see only what partial sync
brought down — merged with pending local mutations, which is what makes the UI
feel instant.

Every result carries a `coverage` report saying whether that is the whole entity
or only part of it. Check it before treating an empty result as "there is
nothing there":

```ts
const rows = await client.db.issue.query({
	orderBy: (i) => [ i.createdAtMs.desc() ],
});
if (!rows.coverage.complete) {
	const all = await client.db.issue.queryServer({/* ... */});
}
```

One bound wants a different answer: `hydrating` means the rows are on their way
in the client's first snapshot, so it asks for a loading state rather than a
server round trip, and clears itself when they land.

See [Sync only part of a workspace](partial-sync.md) and [Partial sync and coverage](../explanation/partial-sync-and-coverage.md).

## Server queries

`queryServer` pushes the same query down to the server's `Query` RPC and is how
an app reaches the rows it did **not** sync: search across everything, open an
id that is not in the current view.

Its results are server truth — not merged with the optimistic overlay, not
stored locally, and they never feed watchers — so a row edited locally but not
yet pushed comes back without that edit. It needs connectivity, and read filters
always apply.

Unless an index serves it, the query costs a full entity scan on the server
(bounded in memory to `offset + limit`). Prefer a selective predicate over deep
pagination.

## Making a query cheap

Server-side queries can be served from a secondary index. Two ways to declare
one, and the choice between them is the whole decision:

**Mark the field** you filter or order by. This is the one to reach for: it says
"this field is worth looking up by" without coupling the schema to any
particular query.

```proto
string state = 8 [(syncplane.v1.field_behavior) = { indexed: true }];
```

**Declare a composite** when a query filters on one field and orders by
_another_:

```proto
option (syncplane.v1.entity) = {
	name: "issue"
	key_fields: [ "id" ]
	index: {
		name: "by_state_rank"
		key: [ { path: "state" }, { path: "rank" desc: true } ]
	}
};
```

Why the second exists: a sorted key-value store has no posting lists to
intersect, so `where state = 'open' order by rank desc limit 20` served from a
single `state` index has to walk the whole `state = 'open'` range to sort it,
and served from a single `rank` index has to walk the sort order re-checking
`state` per candidate. Either is far better than decoding every row, and neither
is bounded by the limit. The composite is exactly O(limit).

What an index cannot serve, so don't declare one hoping it will:

| Shape                      | Why                                                                |
| -------------------------- | ------------------------------------------------------------------ |
| `contains` / `endsWith`    | Neither is a prefix, so no range bounds it. Not full-text search.  |
| `or`                       | Needs a merge of several ranges; falls back to a scan.             |
| a collaborative-text field | Every keystroke would rewrite the entry. Rejected at registration. |
| a map, or a message list   | No comparison reaches it. Index a scalar field of the elements.    |
| ordering **by** a list     | A row has many values there, so it has no sort position.           |

Two things to know about how this behaves. **A new index is not usable
immediately** — a background pass walks the entity first, and until it finishes
every query scans, which is correct and merely slower. And **using an index
never changes an answer**: every query has a legal scan fallback and the two
produce identical results, which is what lets either side decline any plan it is
unsure of.

The same indexes serve the **local** `query()` too, for a workspace-scoped query
the planner covers, and the first read of a `watchQuery()` on the same terms; a
cross-workspace one always scans. Locally the index covers confirmed rows, with
the optimistic overlay scanned whole and merged over the range — so a query still
sees your own unpushed writes.

The cost is on writes: every write to the entity re-stores that row's position
in every index it has, on both sides. An index over a repeated field stores one
entry per element, so that cost grows with the list — `MaxIndexEntriesPerRow` is
the ceiling, and a write past it fails rather than storing a partial index.

## Paging a live result

`offset`/`limit` inside a query are fixed for the life of a watch. To page
through a live result, pass a `window` instead and move it:

```ts
const view = client.db.todo.watchQuery(
	{ orderBy: (t) => [ t.createdAtMs.desc() ] },
	(rows) => paint(rows, rows.total),
	{ window: { offset: 0, limit: 50 } },
);
onScroll((first) => view.setWindow({ offset: first, limit: 50 }));
```

The watch maintains the whole match set, so `setWindow` is a slice rather than a
read, and `rows.total` reports how many rows the page was cut from. A move that
lands on the same rows delivers nothing; `setWindow({})` delivers everything.

## Filtering on a list

A repeated scalar answers `has` / `hasAny` / `hasAll` / `isEmpty`, and marking it
`indexed` makes membership a range read:

```proto
repeated string labels = 10 [(syncplane.v1.field_behavior) = { indexed: true }];
```

```ts
await client.db.issue.query({ where: (i) => i.labels.has('bug') });
```

A list of **messages** — including a to-many reference — is asked about with
`any()`, which scopes one element:

```ts
await client.db.issue.query({
	where: (i) => i.assignees.any((a) => a.id.eq('maya')),
});
await client.db.order.query({
	where: (o) => o.lineItems.any((li) => and(li.sku.eq('A-1'), li.qty.gt(3))),
});
```

The element is explicit because two conditions have to be able to describe the
**same** element. As separate terms —
`and(o.lineItems.any((li) => li.sku.eq('A-1')), o.lineItems.any((li) => li.qty.gt(3)))`
— they are a weaker question that two different line items satisfy.

An element of a reference list is the target's stub, so only its key fields are
addressable; asking for anything else throws. Every reference field is already
indexed by the engine, so `assignees.any((a) => a.id.eq('maya'))` is a range read
with nothing declared. For any other message list, name the element field in a
composite index:

```proto
index: {
	name: "by_sku"
	key: [ { path: "line_items.sku" } ]
}
```

Such a component fans out — the row takes one position per element — and it
still supplies the ordering that follows it once an equality has pinned it, so
`{ key: [ { path: "labels" }, { path: "rank" desc: true } ] }` serves "labelled
X, newest first" at O(limit). One list per index: components over two different
lists would pair values from different elements.

## Semantics to know

Ordering is identical in both directions, down to UTF-16 collation, pinned by
vectors both languages run. Two things surprise people:

- A predicate on a leaf that could never satisfy it — comparing or ordering by
  bytes, a message, a map or a repeated field — is **rejected** when the query is
  compiled, naming the operator that does work instead, rather than quietly
  matching nothing.
- An empty or unset list matches nothing, so `any()` over one is false and
  `not(...any())` reads as "no element matches".
- `NaN` is comparable to nothing: `gt`/`gte`/`lt`/`lte` are all false with a `NaN`
  on either side, as `eq` already is. Ordering ranks values `real < NaN <
  missing`, and the rank wins over the direction, so `NaN` and missing values
  both land at the end under `asc` and `desc` alike.

Collaborative text fields query as their visible string:
`where: (d) => d.body.contains('word')` works on both evaluators.

## Related

- [TypeScript client API](../reference/typescript-client.md#4-queries--the-dsl)
  — the full operator surface per field type.
- [Query from server-side code](server-side-queries.md) — the Go equivalents.
