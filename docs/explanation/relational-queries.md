# Relational queries: the one-hop semi-join

How does the query engine evaluate relational queries across entities while
preserving row-local execution, watcher efficiency, and partial sync
guarantees?

Every condition in the query domain-specific language (DSL) is row-local: its
paths point into the row's own message, so you can evaluate a predicate by
looking at one row at a time. That property is load-bearing. It allows a scan to
define the result, an index to reproduce it, a watcher to maintain it per
commit, and a subscription to decide membership as rows change.

The `matches()` operator is the one deliberate exception. For example,
`i.assignee.matches((a) => a.teamId.eq(t))` asks about a different entity's row.
The condition is true when at least one referenced row exists and satisfies the
predicate. This document explains the design of that exception: why it spans
only one hop, how engines evaluate it without giving up row-locality, and how
partial sync interacts with relational queries.

## The scan is still the definition

The semantics are defined at the scan level, like every other query construct.
For each row and each stored stub, derive the target's key, look up the target
in the same workspace, and evaluate the inner condition against it. The
condition evaluates to true if any target matches. An unset reference, an
orphaned stub, and a deleted target all contribute nothing.

This definition is evaluable over decoded rows on both client and server. Shared
conformance vectors in `testdata/queryeval` and `relatedCases` pin both
implementations against this scan definition.

Neither engine executes the scan directly. Instead, both engines run a
two-phase rewrite that matches the scan definition:

1. **Pre-pass**: The inner condition runs as an ordinary query over the target
   entity, executing one query per `matches()` per workspace to collect the
   matching key set $K$.
2. **Rewrite**: The engine replaces the `matches()` node with a membership test
   on the stored reference stubs. A single-key reference becomes `path.key ∈ K`,
   which matches the reverse-relation push-down served by the derived
   per-ref-field index. A composite-key reference becomes a disjunction of
   whole-tuple equalities, preventing a matching `id` under the wrong `ns` from
   matching.

After the rewrite, the query is row-local. Downstream components—including the
planner, index walk, cursors, totals, and maintained watch results—remain
untouched. The rewrite never appears on the wire: `RelCond` travels as written,
and each side rewrites against its own state.

This model has two primary consequences:

- `not(matches(…))` means "no referenced target matches", which includes rows
  with no reference at all. This follows the standard rule for conditions on
  missing values.
- The pre-pass and the base scan run inside a single snapshot, ensuring a
  consistent view between the membership set and the rows tested against it.

## Why one hop, scoped to the row

The engine restricts relational queries: no `matches()` inside `matches()`, none
inside `any()`, none through a reverse relation, and no ordering by a related
field.

These restrictions prevent compounding performance costs. Each nesting level
multiplies pre-passes and couples more entities into every watcher's
invalidation set. Furthermore, an element-scoped `matches()` has no single row to
anchor workspace and key derivation.

A single hop from the row satisfies the core use case, such as finding issues
assigned to members of a specific team. At the same time, it keeps execution
cost proportional to one target-entity query per node. Excluded capabilities can
be added later; currently, the engine rejects them with specific errors.

A `matches()` through a reverse relation is also unnecessary in most cases. For
example, you can phrase "projects with an open issue" from the side holding the
reference, where the semi-join already works.

## Local answers, partial data, and `follow`

A server evaluates `matches()` against all authoritative data. A local client
evaluates it against partial sync data. A holder row whose target never synced
evaluates as a non-match, which is the failure mode that coverage reporting
makes visible.

A local `matches()` query reports the `relation` coverage bound unless the
crossed reference is provably answerable for the workspace:

- **The target entity's coverage is complete**: Every stub can be resolved,
  regardless of where the holder originated.
- **Every subscription scoping the outer entity follows the crossed reference field**:
  All targets for locally held rows exist in the `follow` closure, allowing
  correct evaluation even when the target entity as a whole is partial.

Reference-following subscriptions make semi-joins answerable locally because
`follow` syncs the rows needed for hydration, which match the rows the pre-pass
requires. When neither condition holds, the query returns bounded results with
`relation` in its coverage. You can use `queryServer` as an escape hatch.

Subscription predicates remain row-local. Membership is decided as rows change,
whereas a semi-join membership set has no single point in time for evaluation.
The system pairs a row-local base scope with `follow` to sync the primary row
and pull its neighborhood along.

## On the server

The Query remote procedure call (RPC) and in-process `Query` methods run the
same two-phase rewrite against authoritative state inside the request snapshot.

The pre-pass reads the target entity under the same read filter as any direct
read. Without this rule, a predicate over a filtered entity would act as an
oracle, allowing callers to probe inaccessible rows by watching which holders
match. This matches the rule that `follow` applies to closure members: crossing
a reference never escalates permissions.

A watcher learns that a target commit affects it by using the reachability walk
from `include`, driven by the compiled predicate instead of an include tree.
For evaluation costs, see
[What a live `matches()` costs](#what-a-live-matches-costs).

## What a `matches()` costs

The pre-pass runs as a standard query on both client and server. A declared
index covering the inner condition reduces execution cost from a full entity scan
to an index lookup proportional to matches. An index on the fields filtered by
`matches()` provides the same benefit as an index on outer query fields.

The client builds declared indexes locally and falls back to a direct scan
whenever it cannot guarantee complete rows.

Index behavior varies by workspace scope:

- A query over one workspace or a specific set of workspaces performs an index
  walk for each workspace.
- A query across all workspaces in the store performs a direct scan of the
  target entity.

The rewrite membership test uses the per-ref-field index derived by the engine.
When the key set $K$ is large, the engine evaluates membership using a hash
table, keeping the cost uniform across key set sizes.

Queries crossing into the same entity with identical inner conditions share a
single pass. On the server, two configuration options bound resource usage (see
[Server options](../reference/server.md)):

- `MaxRelConditions` caps the number of `matches()` operators in a predicate.
- `MaxRelKeys` caps the number of target rows a condition can select.

## What a live `matches()` costs

A watch pays the initial evaluation cost when opened. Subsequent commits to the
target entity require a membership update and, if keys changed, an evaluation of
the affected rows.

The engine updates membership using the rows modified in the commit rather than
querying the target entity again. When a commit modifies too many rows to
enumerate (such as a bootstrap snapshot, a scope change dropping an entity, or a
rebase overlay replay), the engine runs a full pre-pass instead. Watches
crossing the same relation over the same scope share this computation.

When evaluating affected rows, the engine inspects only the reach of the change:

- Rows that stopped matching are identified from the existing watch results.
- Rows that started matching are evaluated through the watch query using the
  new entrant keys.

This targeted evaluation requires two conditions:

- **The holder entity requires an index with the reference field as its leading component**:
  For example, `key: [{ path: "issue" }, …]`. The engine derives this index on
  the server. On the client, applications must declare this index in the schema.
  Without this index, the engine must re-derive results across the entire entity.
- **No `matches()` can be nested under a `not` operator**:
  Targeted evaluation relies on one-directional state changes where rows match
  because a key entered the set. Under negation, rows match because a key left
  the set, which entrant key inspection does not capture.

When either condition is not met, the engine re-derives the result from the full
entity.

## Related

- [TypeScript client reference](../reference/typescript-client.md) — The
  `matches()` operator and coverage rules.
- [Partial sync and coverage](partial-sync-and-coverage.md) — The coverage
  model used by relational queries.
- [Limits and trade-offs](limits-and-trade-offs.md) — Architectural limits and
  omitted features.
