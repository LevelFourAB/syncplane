# Sync only part of a workspace

A client does not have to hold the whole workspace. A subscription set scopes
what syncs — bootstrap, pull, and local materialization all follow it:

```ts
const client = new SyncClient({
	// ...
	subscriptions: [
		// Predicate scope: every open todo.
		subscription(TodoSchema, (t) => t.completed.eq(false)),
		// Rank scope: the 500 most recently changed comments, whatever they are.
		subscription(CommentSchema, undefined, { limit: 500, byRecency: true }),
		// Rank scope by a field: the top 100 issues by priority.
		subscription(IssueSchema, (i) => i.state.eq('open'), {
			limit: 100,
			orderBy: (i) => [ i.priority.desc() ],
		}),
	],
});

// Changing the set later delta-resyncs the difference, not a re-bootstrap.
client.setSubscriptions([ subscription(TodoSchema) ]);
```

An **empty set is full sync**.

## Windows

A `limit` is what makes a subscription **windowed**. `orderBy` (or `byRecency`)
decides which rows win the ranks, ties break on row key, and the cap is bounded
by the server's `MaxWindowSize` (default 1000).

A window is the answer to the case a predicate cannot express: a real view — a
busy project, "all open", an active label — routinely matches tens of thousands
of rows, and no more selective predicate means the same thing. With a window,
working-set size becomes a policy choice rather than the size of the entity.

`byRecency` is the cheapest ordering to maintain, since ranking by write
sequence needs no field comparison.

## Check coverage on every read

A client holding the top N by one order cannot correctly answer a local query
ordered by another, and an empty local result for an unsubscribed entity looks
exactly like "there is nothing there". Every result array carries a `coverage`
report for that reason:

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

`RowSet<T>` is still an array — `.map`, `.length`, spreads and destructuring are
unchanged, and `coverage` is non-enumerable — so adding the signal broke no call
site.

`hydrating` is the one bound that is not about scope: a cold client's first
snapshot lands entity by entity, so an entity that has not arrived reads as `[]`
just like an empty one. It clears itself as the snapshot fills, and readers
re-fire — so the thing to do about it is show a loading state, not fetch from
the server. `bootstrapPriority` decides which entities land first, and
`status().workspaces[id].hydration` reports what is still outstanding.

## What it costs the server

A window bounds the **client**, not the server. With no secondary index over
proto fields, computing an `orderBy` window still costs one entity scan per
pull, the same price `Query` pays.

## Related

- [Partial sync and coverage](../explanation/partial-sync-and-coverage.md) — how
  windows are maintained exactly, and what `coverage.complete` really promises.
- [Query data](query-data.md).
