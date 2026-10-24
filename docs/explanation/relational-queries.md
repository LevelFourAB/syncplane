# Relational queries: the one-hop semi-join

How does the query engine evaluate relational queries across entities while
preserving row-local execution, watcher efficiency, and partial sync
guarantees?

Every condition in the query domain-specific language (DSL) is row-local: its
paths point into the row's own message. You can evaluate a predicate by
inspecting one row at a time. This property allows a scan to define the result,
an index to reproduce it, a watcher to maintain it per commit, and a
subscription to decide membership as rows change.

The `matches()` operator is the deliberate exception. For example,
`i.assignee.matches((a) => a.teamId.eq(t))` queries a different entity's row.
The condition is true when at least one referenced row exists and satisfies the
predicate. This document explains that exception: why it spans only one hop, how
engines evaluate it without losing row-locality, and how partial sync interacts
with relational queries.

## The scan is still the definition

Scan semantics define the operation, as with every other query construct.
For each row and stored stub, derive the target key, find the target in the
workspace where the stub resolves (the holder's workspace, or the workspace
named by a workspace-qualified reference), and evaluate the inner condition.
The condition is true if any target matches. An unset reference, an orphaned
stub, and a deleted target do not match.

The definition can be evaluated over decoded rows on both the client and the
server. Shared conformance vectors in `testdata/queryeval` and `relatedCases`
verify both implementations against this scan definition.

Neither engine executes the scan directly. Both engines run a two-phase rewrite
that matches the scan definition:

1. **Pre-pass**: The inner condition runs as a standard query over the target
   entity. It runs one query per `matches()` per workspace to collect the
   matching key set $K$.
2. **Rewrite**: The engine replaces the `matches()` node with a membership test
   on stored reference stubs. A single-key reference becomes `path.key ∈ K`,
   which matches the reverse-relation push-down served by the derived
   per-ref-field index. A composite-key reference becomes a disjunction of
   whole-tuple equalities, preventing a matching `id` under the wrong `ns` from
   matching.

After the rewrite, the query is row-local. Downstream components—including the
planner, index walk, cursors, totals, and maintained watch results—do not change.
The rewrite never appears on the wire: `RelCond` travels as written, and each
side rewrites against its own state.

This model has two consequences:

- `not(matches(…))` means "no referenced target matches", which includes rows
  with no reference at all. This follows the standard rule for conditions on
  missing values.
- The pre-pass and the base scan run inside a single snapshot. This guarantees a
  consistent view between the membership set and the tested rows.

## Why one hop, scoped to the row

The engine restricts relational queries: no `matches()` inside `matches()`, none
inside `any()`, none through a reverse relation, and no ordering by a related
field.

These restrictions prevent compounding performance costs. Each nesting level
multiplies pre-passes and couples more entities into each watcher's
invalidation set. In addition, an element-scoped `matches()` has no single row to
anchor workspace and key derivation.

A single hop from the row satisfies common use cases, such as finding issues
assigned to members of a specific team. It keeps execution cost proportional to
one target-entity query per node. The engine rejects unsupported forms with
specific errors.

A reverse relation is unnecessary in most cases. The semi-join already works
from the side that holds the reference (for example, "projects with an open
issue").

A polymorphic reference contains cases that point to different target entities.
Because each case has a different target, `matches()` must name a single `oneof`
case (such as `owner.person`) rather than the `oneof`. Naming the `oneof` would
need a pre-pass for every case and a membership test that tracks which case each
stub holds. The inner condition compiles against that target entity, and the
pre-pass queries that target alone. Naming an individual case scopes each
relational node to a single target and preserves row-locality after the rewrite.

## Local answers, partial data, and `follow`

A server evaluates `matches()` against authoritative data. A local client
evaluates it against partial sync data. A holder row whose target never synced
evaluates as a non-match. Coverage reporting makes this failure mode visible.

A local `matches()` query reports the `relation` coverage bound unless the
crossed reference is provably answerable for the workspace:

- **The target entity coverage is complete**: The client can resolve every stub,
  regardless of holder origin.
- **Every subscription scoping the outer entity follows the crossed reference field**:
  All targets for locally held rows exist in the `follow` closure. This enables
  correct evaluation even when the target entity as a whole is partial.

Reference-following subscriptions make semi-joins answerable locally because
`follow` syncs the rows needed for hydration, which match the rows the pre-pass
requires. When neither condition holds, the query returns bounded results with
`relation` in its coverage. You can use `queryServer` to query authoritative
server data.

Subscription predicates remain row-local. The engine evaluates membership as
rows change, whereas a semi-join membership set has no single point in time for
evaluation. The system pairs a row-local base scope with `follow` to sync the
primary row and pull its neighborhood along.

## Workspace-qualified references

A [reference message](../reference/proto-options.md#syncplanev1reference--workspace-qualified-references)
stores the target workspace beside its key, so a stub can point into another
workspace. The two-phase rewrite works because the pre-pass groups its key set
per workspace; only the membership test changes shape.

On the client, the pre-pass for such a node reads the target entity in every
workspace the client holds, regardless of query scope. The rewritten condition
pairs each workspace key set with the stubs that name that workspace: one branch
per workspace with matches. Omitting a workspace names the holder's workspace.
A stub pointing to a workspace that the client does not hold matches nothing.
This outcome matches an unsynced target and reports through the `relation`
coverage bound. That bound clears only when the target entity is complete in
every held workspace. A `follow` specification cannot cover it, because the
server resolves `follow` inside one workspace.

Incremental watch paths do not run for these nodes. A commit that names target
rows inside the query scope provides no information about targets elsewhere. The
entrant-and-leaver splice keys stubs by target key alone. Both mechanisms fall
back to a pass over the target entity and re-derive the match set.

On the server, the pre-pass reads the request workspace alone. The replacement
adds a test that the stub resolves in that workspace by naming that workspace or
naming none. A stub naming another workspace matches nothing on the server, and
never matches a same-key row in the request workspace. The same rule holds for
`include` and `follow`. A pre-pass would have to gate each target workspace
through `CanAccessWorkspace` on every request, which is why the server stays
inside one workspace for now.

## On the server

The `Query` remote procedure call (RPC) and in-process `Query` methods run the
two-phase rewrite against authoritative state inside the request snapshot.

The pre-pass reads the target entity under the same read filter as a direct
read. Without this rule, a predicate over a filtered entity allows callers to
probe inaccessible rows by watching which holders match. This behavior matches
the rule that `follow` applies to closure members: crossing a reference never
escalates permissions.

A watcher determines whether a target commit affects it by using the
reachability walk from `include`, driven by the compiled predicate instead of an
include tree. For evaluation costs, see
[What a live `matches()` costs](#what-a-live-matches-costs).

## What a `matches()` costs

The pre-pass runs as a standard query on both client and server. A declared
index covering the inner condition reduces execution cost from a full entity
scan to an index lookup proportional to matches. An index on the fields filtered
by `matches()` provides the same benefit as an index on outer query fields.

The client builds declared indexes locally. It falls back to a direct scan
whenever it cannot guarantee complete rows.

Index behavior varies by workspace scope:

- A query over one workspace or a specific set of workspaces performs an index
  walk for each workspace.
- A query across all workspaces in the store performs a direct scan of the
  target entity.

The rewrite membership test uses the per-ref-field index derived by the engine.
When the key set $K$ is large, the engine evaluates membership using a hash
table to keep the cost uniform across key set sizes.

Queries crossing into the same entity with identical inner conditions share a
single pass. On the server, two configuration options bound resource usage (see
[Server options](../reference/server.md)):

- `MaxRelConditions` caps the number of `matches()` operators in a predicate.
- `MaxRelKeys` caps the number of target rows a condition can select.

## What a live `matches()` costs

Opening a watch incurs the initial evaluation cost. Subsequent commits to the
target entity update membership. If keys change, the engine evaluates affected
rows.

The engine updates membership using the rows modified in the commit rather than
querying the target entity again. When a commit modifies too many rows to
enumerate (such as a bootstrap snapshot, a scope change dropping an entity, or a
rebase overlay replay), the engine runs a full pre-pass instead. Watches
crossing the same relation over the same scope share this computation.

When evaluating affected rows, the engine inspects only the reach of the change:

- The engine identifies rows that stopped matching from existing watch results.
- The engine evaluates rows that started matching through the watch query using
  the new entrant keys. The planner bounds how many keys one index probe set may
  carry, so large changes evaluate as multiple queries over slices of entrant
  keys. The number of moved targets does not affect whether targeted evaluation
  applies.

Targeted evaluation requires two conditions:

- **The holder entity requires an index with the reference field as its leading component**:
  For example, `key: [{ path: "issue" }, …]`. The engine derives this index on
  the server. On the client, applications must declare this index in the schema.
  Without this index, the engine must re-derive results across the entire entity.
- **No `matches()` operator can be nested under a `not` operator**:
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
