# Relational queries: the one-hop semi-join

Every condition in the query DSL is row-local: its paths point into the row's
own message, so a predicate can be evaluated by looking at one row at a time.
That property is load-bearing — it is what lets a scan define the result, an
index reproduce it, a watcher maintain it per commit, and a subscription decide
membership as rows change.

`matches()` is the one deliberate exception. `i.assignee.matches((a) =>
a.teamId.eq(t))` asks about a **different entity's** row: true when at least
one row the reference points at exists and satisfies the condition. This page
explains the shape of that exception — why it is exactly one hop, how both
engines evaluate it without giving up row-locality, and what it means for a
client that holds only part of the data.

## The scan is still the definition

The semantics are stated at the scan level, like every other query construct:
per row, per stored stub, derive the target's key, look the target up in the
same workspace, and evaluate the inner condition against it — true if any
target matches. An unset reference, an orphaned stub and a deleted target all
contribute nothing. This definition is plainly evaluable over decoded rows on
both sides, and the shared conformance vectors (`testdata/queryeval`,
`relatedCases`) pin both implementations against it.

Neither engine executes it that way. Both run a **two-phase rewrite** that must
be indistinguishable from the definition:

1. **Pre-pass**: the inner condition runs as an ordinary query over the target
   entity — one query per `matches` per workspace — collecting the matching
   key set K.
2. **Rewrite**: the `matches` node is replaced by a membership test on the
   stored ref stubs. A single-key ref becomes `path.key ∈ K` — the exact shape
   the reverse-relation push-down already produces, served by the per-ref-field
   index the engine derives anyway. A composite-key ref becomes a disjunction
   of whole-tuple equalities, which is what keeps a matching `id` under the
   wrong `ns` from matching.

After the rewrite the query **is** row-local, so everything downstream —
planner, index walk, cursors, totals, the maintained watch results — is
untouched. The rewrite never appears on the wire: `RelCond` travels as written,
and each side rewrites against its own state.

Two consequences fall out. First, `not(matches(…))` means "no referenced target
matches", which includes rows with no reference at all — the same rule every
condition on a missing value follows. Second, the pre-pass and the base scan
run inside one snapshot, so the membership and the rows it is tested against
are a single consistent view.

## Why one hop, scoped to the row

The restriction set — no `matches` inside `matches`, none inside `any()`, none
through a reverse relation, no ordering by a related field — is not caution for
its own sake. Each nesting level multiplies pre-passes and couples more
entities into every watcher's invalidation set, and an element-scoped
`matches` has no single row to hang its workspace and key derivation off. One
hop from the row covers the demand that motivated the feature ("issues assigned
to anyone on this team") while keeping cost proportional to one target-entity
query per node. Everything excluded is additive later; today each is rejected
with a named error rather than half-supported.

A `matches` through a reverse relation is also mostly unnecessary: "projects
with an open issue" is usually phraseable from the side that holds the
reference, where the semi-join already works.

## Local answers, partial data, and `follow`

A server evaluates `matches` against everything. A local query evaluates it
against **what partial sync brought down**, and a holder row whose target never
synced reads as a non-match — silently, which is the one failure mode coverage
exists to make visible.

So a local `matches` query reports the `relation` coverage bound unless the
crossed reference is provably answerable, per workspace:

- the **target entity's coverage is complete** — every stub can be resolved,
  wherever the holder came from; or
- **every subscription that scopes the outer entity `follow`s the crossed ref
  field** — then each locally-held holder's targets are in the follow closure,
  so each held row evaluates correctly even though the target entity as a
  whole is partial.

The second clause is the precise sense in which reference-following
subscriptions make the semi-join answerable locally: `follow` was built to sync
the rows that hydration needs, and those are exactly the rows the pre-pass
needs. When neither clause holds, the result still comes back — bounded, with
`relation` in its coverage — and `queryServer` accepts the same condition as
the escape hatch.

Subscription predicates themselves stay row-local: membership is decided as
rows change, where a semi-join's membership set has no single moment to be
computed at. The composition that works is a row-local base scope plus `follow`
— sync by what the row says, then bring its neighbourhood along.

## On the server

The Query RPC and the in-process `Query` methods run the same two-phase
rewrite against authoritative state, inside the request's snapshot. The one
rule that matters: the pre-pass reads the target entity **under the same read
filter as any direct read of it**. Without that, a predicate over a filtered
entity becomes an oracle — a caller could probe rows it cannot read by watching
which holders match. This is the same rule `follow` states for closure
members: crossing a reference is never a permission-escalation path.

A watcher learns that a target commit concerns it from the same reachability
walk `include` uses, fed from the compiled predicate rather than an include
tree. What it then costs is [below](#what-a-live-matches-costs).

## What a `matches` costs

The pre-pass is an ordinary query on both sides, so a declared index covering
the inner condition serves it — the difference between O(matches) and a decode
of every target row in scope. An index on the fields a `matches` filters by is
therefore worth as much as one on the fields the outer query filters by, and
more the larger the target entity is. It is the same index either way: the
client builds the entity's declared indexes locally, and declines to the read
that is the definition whenever it cannot promise the same rows.

One index walk covers one workspace. A local query over one workspace or a
named set of them walks a leg each; a query over every workspace the store
holds has no leg to walk, and its pre-pass reads the target entity.

The rewrite's membership test is served by the per-ref-field index the engine
derives, and answered from a hash when the key set is large enough for that to
pay — so it costs the same whether K holds ten keys or ten thousand.

Conditions crossing into the same entity with the same inner condition share a
single pass. On the server two knobs bound the rest (see
[server options](../reference/server.md)): `MaxRelConditions` caps how many
`matches` one predicate may carry, since nothing in the predicate's own size
does, and `MaxRelKeys` caps how many target rows one of them may select.

## What a live `matches` costs

A watch pays the above once, when it opens. After that a commit to the target
entity costs it a membership update, and — only when that moved something — a
second look at the rows the move reached. Neither has to be a pass over an
entity.

The membership is brought up to date from the rows the commit named — a point
read and one condition evaluation each — rather than by asking the target entity
again. Some commits name no rows because they moved too many to enumerate: a
bootstrap installing a snapshot, a scope change dropping an entity, a rebase
replaying the overlay. There is nothing to update from then, and the pass serves
instead. Watches crossing the same relation over the same scope share that work,
so a screenful of views over one relation costs what one of them does.

The second look is at the rows the move reached, not at the result as a whole:
rows that may have stopped matching are looked for among those the watch already
holds, and rows that may have started are read back through the watch's own
query with the entrant keys standing in for the membership test. Two things have
to hold for that, and re-deriving the result from the whole entity is what
serves when either does not.

- **The holder side needs an index whose leading component is the reference
  field** — `key: [{ path: "issue" }, …]` on the entity holding the reference.
  The engine derives one per reference field; a client builds the indexes the
  schema declares and nothing else, so locally this is a declaration an app
  makes. Without it there is no way from a target key back to its holders, and
  the walk that would find them is the re-derivation itself.
- **No `matches` may sit under a `not`.** The narrowing rests on a move being
  one-directional: a row can only start matching because a key entered. Negated,
  it starts matching because a key _left_, and looking at the entrants would miss
  it.

## Related

- [TypeScript client reference](../reference/typescript-client.md) — the
  `matches()` operator and the coverage rule.
- [Partial sync and coverage](partial-sync-and-coverage.md) — the coverage
  model this extends.
- [Limits and trade-offs](limits-and-trade-offs.md) — what the engine
  deliberately does not do.
