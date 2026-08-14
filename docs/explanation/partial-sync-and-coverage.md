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

The cursor records the window's membership as a key-set, and every pull with
anything to deliver brings the window up to date and diffs it against that set:

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

## The cursor references its membership instead of restating it

Membership is what makes a cursor big: a window at its limit carries that many
row keys, and a reference-following closure can carry far more. Exactness
demands that the set be _stored_ — a diff needs the previous membership, and
nothing else can reproduce it — but not that it be stored in the cursor. With
a cursor-state store the server keeps the sets under an opaque reference, the
cursor on the wire stays a few dozen bytes however large the client's holdings
grow, and a new entry is written only when membership actually moves: an idle
poll costs one store read and reissues the reference it was handed.

The store is allowed to forget. A cursor whose entry was evicted resets the
client onto a fresh bootstrap — the same recovery as falling below the
tombstone GC horizon, and the reason eviction must be rare rather than
impossible. When the store is down or absent the cursor inlines the sets
exactly as every cursor once did, so the indirection degrades to extra bytes,
never to a wrong answer.

## The cursor is a checkpoint on a stream, not a per-frame receipt

A server running without a cursor-state store still has fat cursors, and on
`Subscribe` their cost is avoidable even then: the server keeps the scope's
position itself, and the cursor on a frame exists only so the client can
resume from it later.

So a client that sets `sparse_cursor` gets the cursor as a periodic checkpoint
instead of on every frame, and the steady-state cost of a live subscription
becomes the row that changed rather than the row plus a restatement of
everything the client holds. Between checkpoints its stored position sits behind
the rows it has already applied; a reopened stream resumes from that position
and re-delivers what followed, which the client's own stored-sequence guard
discards as already-applied.

The server withholds a cursor only while the rows it has sent since the last
checkpoint weigh less than the cursor itself, so the re-delivery after a
reconnect can never cost more than the restatements it avoided. A subscription
with no window and no follow has a cursor smaller than any page it rides on,
which never clears that bar — those streams checkpoint on every frame, and the
setting changes nothing for them.

## What a bounded membership costs the server

A window bounds the **client** by construction: what it holds, bootstraps and
materializes. Bounding what the SERVER does per pull takes more, because a
window and a follow closure are both memberships that cannot be decided one row
at a time — resolve either from scratch and the cost is the size of the set, not
the size of what changed. On a shared workspace under constant writes, where no
pull ever sits at the head, that is one pass over every connected client's whole
working set per write, which is the wrong shape for the deployment windows exist
for.

So a pull that can see **every** row written since its cursor maintains both
sets from those rows instead:

- A **window** carries its members and the position of the weakest of them, so a
  changed row's membership is one comparison. Only a row entering or leaving a
  full window costs a read, and then a bounded ordered walk either side of that
  position rather than a pass over the window. A `byRecency` window costs no
  read at all: its ordering is the workspace sequence, so anything written since
  the cursor enters at the front and the tail falls off a list the cursor
  already holds in order.
- A **follow closure** over a reverse relation ("every issue of this project")
  is decidable from the member's own reference field, so a changed row settles
  itself and only a seed that entered or left the base scope is walked again —
  including a seed that moved without being written, which is what a window
  refilling does.
- A **follow closure** over a forward relation ("the project this issue belongs
  to") reads the other way round: a target is held because some issue points at
  it, so its own row cannot settle it. What it costs instead is one probe of the
  holders' reference index per target whose membership is actually in doubt, and
  a target is only in doubt once something can have moved the holders. A pull
  where no issue was written carries every project it holds over untouched;
  where one was, the closure asks each project it holds whether a seed still
  points there — never more questions than there are targets, against a rebuild
  that reads every holder.

Resolving from scratch stays the fallback and every case the maintained path
declines takes it: a page that truncates, a cursor resuming inside a
same-version group, a subscription set that no longer lines up with the
cursor's, a window that would have to locate a tail it never had, a follow path
through more than one segment or deeper than one round, and a forward hop whose
reference index cannot answer the probe — a to-many reference, an index not yet
built, or a target with a fan-in wide enough to run past
`MaxIndexScanCandidates`. The two must produce the same page, and
`Limits.DisableMembershipMaintenance` forces the fallback so that property is
tested rather than assumed.

What stays unbounded is the FIRST resolution, and the fallbacks with it:
computing an `orderBy` window from scratch costs what the equivalent `Query`
costs — a range read where an index serves the predicate and ordering, and a
full entity scan where none does.

## Coverage: knowing when the answer is partial

Bounded working sets create a hazard worth naming: a client holding the top N by
one order **cannot correctly answer a local query ordered by another**, and an
empty local result for an unsubscribed entity looks exactly like "there is
nothing there".

So every result array carries a `coverage` report:

```ts
if (!rows.coverage.complete) {
	// bounds: ('unsubscribed' | 'predicate' | 'window' | 'hydrating' | 'relation')[],
	// plus limit for a window
}
```

`relation` is the cross-entity case: a query whose predicate crosses a
reference (`matches()`) answers from the target rows the store holds, and a
holder whose target never synced reads as a non-match. See
[relational queries](relational-queries.md) for the rule that clears it.

`coverage.complete` is true only when the local store **provably** holds every
row of the entity.

It resolves against **this tab's own** declared scope, not the peer-widened
union the coordinator actually syncs. The union is always a superset, so
own-scope coverage errs toward claiming _less_ completeness than reality — and,
more importantly, it does not flap as peer tabs come and go.

`RowSet<T>` is an array — `.map`, `.length`, spreads and destructuring all
behave as they would on one, and `coverage` rides along non-enumerably, so it
never shows up in `Object.keys` or `JSON.stringify`. `queryServer` reports
coverage too: a server query sees the whole entity, so it is complete unless its
own `limit` truncated the page.

The same bounds reach `total`, the match count a paged result carries beside its
coverage. It is counted from the rows the read could see, so under a window or a
predicate scope it reports what this device holds rather than what the workspace
does, and the two can differ by any amount. Nothing in the number says which one
it is — that is what the coverage beside it is for.

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
