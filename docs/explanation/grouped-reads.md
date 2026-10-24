# Grouped reads: counts and the head of each group

This document explains how the query engine executes grouped reads (`groupCount`, `distinct`, and `groupRows`), why grouping semantics are defined this way, and how index skip-scans avoid full row reads.

## The problem with standard queries

A list user interface (UI) asks two types of questions:

1. "Which rows match a filter?"
2. "How many rows exist per value?"

Standard queries handle the first question. Predicates, ordering, and pagination retrieve rows. But standard queries handle the second question inefficiently. For example, a board with six status columns needs six badge counts. A facet sidebar needs counts per assignee, label, and priority under the active filter. A filter dropdown must find existing assignees before displaying options.

Using standard queries for these needs requires separate queries across the same match set. Six columns require six walks. A facet sidebar requires one query per known value, and cannot discover unknown values. Grouped reads (`groupCount` and `distinct`) answer these questions in a single walk. With an appropriate index, this walk does not read rows.

Boards also ask a third question: "What are the first 20 cards in each column?" Standard queries require either one query per column or a query that reads every card across all columns to keep 20 per column. In both cases, read cost scales with total group size instead of `groups × N`. The `groupRows` read returns the head of each group in a bounded read.

## Group definition

A group partitions matching rows by the value at a single path. The query engine then returns the resulting partitions.

Shared test vectors (`testdata/indexkey`, `groupCases`) pin three grouping decisions:

- **A missing value is a group.** A row whose path resolves to nothing belongs to its own group (`value: undefined` on the client, or an unset `QueryValue` on the wire). The engine does not conflate missing values with empty strings or zeros, and never drops them. Counts cover every matching row, which ensures facet counts match list totals.
- **Groups return in ascending value order.** The engine returns groups in ascending order, placing `NaN` and the no-value group last. This order matches the row order of an ascending `orderBy` on the same path. This also ensures that a `limit` returns a deterministic prefix instead of an arbitrary sample.
- **A repeated path groups per element.** A row that contains three labels joins three groups. Therefore, counts represent group memberships instead of unique row totals. This behavior supports label facets (such as "how many issues are tagged `ui`?"). A row with an empty list joins the no-value group.

## Why zero-count groups are omitted

A grouped read returns only groups present in matching rows. If no rows hold a specific status, that status leaves no trace in the data.

The application schema defines the set of possible values, such as an enum, a status entity, or a saved view configuration. The engine cannot know the full set of possible values. Grouped reads report values that exist in matching data. The application fills in zero-count values by joining against its own domain models.

## The skip-scan index plan

The row planner recognizes one shape (defined in `engine/query/plan.go`):

```text
equality prefix   →   at most one range component   →   ordering suffix
```

A grouped read adds a partition component between the prefix and the trailing components:

```text
equality prefix   →   partition component   →   trailing components
```

The partition component is neither pinned to a single value nor bounded to a specific range.

During a skip-scan, the walk seeks to the first entry that shares the prefix. It cuts the entry at the end of the partition component, which represents the bytes shared by all entries in that group. The engine completes the group's work in the range `[prefix, prefix⁺)` and seeks to `prefix⁺`, the next distinct value.

The execution cost is O(groups × work) instead of O(rows). For `distinct`, per-group work is zero. Identifying 1,000 distinct assignees requires 1,000 seeks and zero row reads. For `groupCount`, work consists of one range count per group, read directly from index entries without decoding rows.

A skip-scan uses an index defined as `[eq-filter fields…, groupPath, …]`. This structure matches the index structure used for ordering by `[status, rank]`. Because the partition component stores values in order, the engine produces groups in sorted order. A query with a `limit` can stop after `limit + 1` groups instead of bucketing the entire entity.

The index walk handles two conditions:

- **Descending components:** A descending component inverts the index payload, so a forward scan yields groups in descending order. The walk reverses direction to produce ascending groups. This behavior allows an index like `[status, rank desc]` to serve a grouping on `rank`.
- **Row-to-entry cardinality:** An index entry corresponds to one row unless a component past the partition fans out over a list. When a fan-out occurs, a single row can appear at multiple positions within the same group. To prevent double counting, the engine reads full rows.

If no suitable index exists, the query declines the skip-scan and falls back to bucketing a full scan. The scan represents the base definition, while the index serves as an optimization.

On the client, a walk that must read rows—because of a residual that the range cannot express, or a fan-out past the partition—must read every row of every group. The client prices this choice before the first read. The client counts the index range against the whole index. The client walks the range when the range covers a clear minority of the entity. When the range does not, the client reports `declined: 'budget'` without reading a row. A `distinct` read is an exception: it stops at the first row of each group that survives the residual. Its cost does not scale with group size, and it keeps the standing candidate ceiling.

## Retrieving the head of a group

The `groupRows` operation uses the same skip-scan walk. Within a group's sub-range, entries are already sorted by the index. A walk that stops after `perGroup + 1` rows retrieves the group page and determines whether more rows exist. This bounds the operation to `groups × perGroup` row reads regardless of group size.

To use an index for `groupRows`, `PlanGroupRows` requires two conditions:

- **Components past the partition must supply the `orderBy` criteria:** The trailing components must match the requested fields and directions. For example, an index storing `[status, rank desc]` serves "highest rank first" but declines "lowest rank first". Without an explicit `orderBy`, rows return in row-key order. In that case, the partition must be the last component in the index.
- **Each index entry must represent a distinct row:** A component past the partition that fans out can produce duplicate rows in a group page, ordered by unrequested values. In this case, the engine cannot use the index.

When an index does not meet these requirements, the engine falls back to scanning the match set, sorting each bucket, and truncating the results.

Calculating `total` per group requires visiting all entries in the group, which is not bounded by `perGroup`. Setting `total: false` disables this calculation.

## Trade-offs of counting index entries

Counting index entries avoids decoding full rows, which introduces two trade-offs:

- **Read filters:** A read filter or field-history rule can hide rows from a caller. Because index entries do not carry visibility metadata, the server reads full rows when filters apply. The walk still skips rows outside the range, but requires per-row reads.
- **Corrupt rows:** Index entry counts include rows whose stored bytes cannot decode. In contrast, a full scan quarantines and drops corrupt rows (defined in `engine/server/poison.go`). This trade-off avoids decoding every row during standard operations. If a row cannot produce a valid group name, the engine declines the index and runs a scan.

## Client-side optimistic overlay

Local reads must account for the optimistic overlay, where pending writes are authoritative for modified rows.

For standard row queries, merging the overlay is a union: the engine walks the index, drops shadowed rows, and adds pending rows.

Grouped reads cannot merge overlay changes with equivalent cost. A shadowed row remains in the index store. Honoring the overlay requires subtracting shadowed entries from their groups and adding pending rows into correct group positions.

Therefore, the client-side index walk declines when writes exist in the overlay and falls back to bucketing merged local data. In the steady state without pending writes, the skip-scan executes normally. When writes are in flight, the read performs a scan bounded by the synced local dataset.

## Facets: the same groups, folded from a list

Facets calculate group counts across multiple axes alongside a row query without running separate grouped reads. When a list view includes a sidebar with counts for assignees, labels, and priorities, running independent `groupCount` queries requires a separate index walk for each axis. Facets fold counts directly from the rows the query matches for its result.

Facets share their group definition with grouped reads. They use the same bucketing keys, the same per-element handling for repeated paths, the same no-value group, and the same sort order. Because both mechanisms share the same grouping logic, a facet and a `groupCount` query with the same predicate never produce conflicting counts.

In a live query, the client maintains facet buckets incrementally from the rows that a commit modified:

- A row entering the match set increments the count for its group.
- A row leaving the match set decrements the count for its group.
- A row changing its field value decrements its previous group and increments its new group.
- The client drops any group whose count reaches zero.

Because the watcher updates facet buckets per moved row, maintaining facets in a live query adds no read overhead beyond the cost of processing the commit's row deltas.

The choice between a grouped read and a facet involves distinct trade-offs:

- **Grouped reads:** A grouped read answers from an index without reading rows. It serves entities too large to hold in local memory and queries where no list of rows is retrieved.
- **Facets:** A facet reads every matching row to extract field values. For a one-shot query, computing facets gives up the early stop that an ordered index walk takes when a page is full. Without that stop, the planner decides between an index and a scan by size: it counts the index range to walk and compares it against the whole index. The planner walks the range when the range covers a clear minority of the entity, and scans when it does not. A predicate that matches most of the entity reports `declined: 'budget'` and reads the entity once instead of walking most rows. A live query pays no overhead beyond the moved rows.
- **Cross-workspace keying:** In cross-workspace queries, facets key groups by workspace ID in addition to the group value, unless an axis opts out. Grouped reads operate only within a single workspace.

## Related documentation

- [Query data](../how-to/query-data.md) — Recipes for querying and counting data.
- [TypeScript client reference](../reference/typescript-client.md) — API reference and contracts.
- [Secondary indexes](../reference/proto-options.md#secondary-indexes) — How to declare secondary indexes for grouped reads.
