# Partial sync and coverage

## Why windows exist

A predicate scope — "every open todo" — is enough for small entities and wrong
for real views. A busy project, "all open", an active label routinely match tens
of thousands of rows, and no more selective predicate means the same thing.

A **window** is a rank scope: a `limit` plus an ordering. `orderBy` (or
`byRecency`) decides which rows win the ranks, ties break on row key. With a
window, working-set size becomes a policy choice rather than the size of the
entity.

## Window maintenance is exact, not best-effort

The cursor carries the window's membership as a key-set, and every pull with
anything to deliver recomputes the window and diffs it against that set:

- rows that **entered** arrive as values,
- rows that **left** arrive as tombstones and leave the local store.

That includes a row which merely _sank_ below the boundary — no predicate
changed, nothing was deleted. Precision here is what makes the local store
trustworthy: without it, a client would accumulate rows that no longer belong to
any view it holds.

`byRecency` is the cheapest ordering to maintain, since ranking by write
sequence needs no field comparison.

## Changing the scope resyncs the difference

Changing subscriptions delta-resyncs the visible-set difference rather than
forcing a full re-bootstrap. The same applies to permission changes when the
policy implements `EpochReadFilters`: `FilterEpoch` versions the inputs behind
`ReadFilter`, so a revoked label or a changed membership syncs as a diff.

A pull whose cursor already sits at the head of the workspace skips the
recomputation entirely: nothing has been written, so no membership can have
moved, and the cursor's key-set is carried forward as issued. A polling client
on a quiet workspace therefore costs nothing to tell "nothing happened".

## What a window does not bound

It bounds the **client**, not the server. Computing an `orderBy` window costs
what the equivalent `Query` costs — a range read where an index serves the
predicate and ordering, and a full entity scan where none does.

## Coverage: knowing when the answer is partial

Bounded working sets create a hazard worth naming: a client holding the top N by
one order **cannot correctly answer a local query ordered by another**, and an
empty local result for an unsubscribed entity looks exactly like "there is
nothing there".

So every result array carries a `coverage` report:

```ts
if (!rows.coverage.complete) {
	// bounds: ('unsubscribed' | 'predicate' | 'window' | 'hydrating')[],
	// plus limit for a window
}
```

`coverage.complete` is true only when the local store **provably** holds every
row of the entity.

It resolves against **this tab's own** declared scope, not the peer-widened
union the coordinator actually syncs. The union is always a superset, so
own-scope coverage errs toward claiming _less_ completeness than reality — and,
more importantly, it does not flap as peer tabs come and go.

`RowSet<T>` is still an array (`.map`, `.length`, spreads and destructuring
unchanged; `coverage` is non-enumerable), so adding the signal broke no call
site. `queryServer` reports coverage too: a server query sees the whole entity,
so it is complete unless its own `limit` truncated the page.

### Hydrating: the bound that clears itself

The other way a local result is not the whole answer has nothing to do with
scope: a cold client's first snapshot is applied **entity by entity** and is
readable as it lands, so an entity that has not started downloading reads as
`[]` — indistinguishable from an empty one, and now with rows already on screen
beside it. `hydrating` is that state, and it is the only transient bound: it
clears on its own as the snapshot arrives, and the result grows with nothing
else having changed.

It composes with the scope bounds rather than replacing them — a windowed
entity mid-download reports `['window', 'hydrating']` and keeps its `limit`,
because the cap still says what the entity converges to. Only a **cold** start
reports it. A re-bootstrap stages its rows and swaps atomically, so readers see
the intact previous snapshot throughout and nothing is ever half-arrived.

Which entity arrives first is the client's call: `bootstrapPriority` names what
the first screen renders, and the server delivers those entities before the
rest (a subscribed client that names none is served in `subscriptions` order).
`status().workspaces[id].hydration` is the same fact workspace-wide — what is
still outstanding, and how much of the snapshot is in — for a progress
indicator.

## Related

- [Sync only part of a workspace](../how-to/partial-sync.md) — the API.
- [Limits and trade-offs](limits-and-trade-offs.md) — the missing index, and
  where it is felt.
