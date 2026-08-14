# Grouped reads: counts, and the head of each group

A list UI asks two kinds of question. "Which rows" is a query, and everything
about it — the predicate, the ordering, the page — is built for that. "How many,
per value" is not. A board with six status columns wants six badges; a facet
sidebar wants a count per assignee, per label, per priority, under whatever
filter is currently on; a filter dropdown wants to know which assignees exist
before it can offer any.

Answered with the query surface alone, each of those is a query of its own that
walks the same match set again for a number. Six columns is six walks; a facet
sidebar is one per value it already knows about, and it cannot discover a value
it does not. A grouped read (`groupCount`, `distinct`) answers all of them in one
walk, and with the right index in one walk that never reads a row.

The same board asks a third question the query surface answers just as badly:
"the first twenty cards of each column". One query per column again, or one query
that reads every card of every column to keep twenty of each — the read is
unbounded in the wrong dimension, scaling with the size of the groups where the
answer wanted is `groups × N`. `groupRows` is that read: the same partition, with
each group's head instead of its number.

## What a group is

The definition is stated over rows, like every other query construct: partition
the matching rows by the value at one path, and answer with the partitions.
Everything below is an optimization that has to reproduce it exactly.

Three details of that definition are decisions rather than consequences, and
both implementations pin them in the shared vectors (`testdata/indexkey`,
`groupCases`):

**A missing value is a group.** A row whose path resolves to nothing joins a
group of its own — `value: undefined` on the client, an unset `QueryValue` on the
wire — never conflated with an empty string or a zero, and never dropped. So the
counts always cover every matching row, which is what makes a facet's numbers
add up against the list beside it.

**Groups come back in ascending value order, with NaN and then the no-value
group last.** That is the order an ascending `orderBy` on the same path puts
rows in, so the two surfaces never disagree about where an unset field sits. It
is also what makes `limit` mean something: the first N groups are a stable
prefix rather than an arbitrary sample.

**A repeated path groups per ELEMENT.** A row holding three labels joins three
groups. The counts therefore do NOT sum to the number of matching rows — they
count memberships, not rows. That is exactly what a label facet asks for
("how many issues are tagged `ui`?"), and it is the wrong answer to a different
question ("how many issues are there?"), which is why it is stated here rather
than discovered. A row whose list is empty joins the no-value group, so it is
still somewhere.

## Only what the rows produce

A grouped read cannot return a group with a count of zero. It reads rows; a
status no row currently holds leaves no trace to read.

This is the one place the surface will not give a board UI what it literally
wants, and it is not an oversight. The set of possible values is the app's
schema — an enum, a status entity, a saved view's column list — and only the app
knows it. So the read reports the values that ARE there, and the app joins its
own domain against them, filling in the zeros. Any other design would have the
engine inventing a value set it has no way to know is complete.

## The skip-scan

The row planner recognises one shape (see `engine/query/plan.go`):

    equality prefix   →   at most one range component   →   ordering suffix

A grouped read adds a fourth kind of component between the prefix and whatever
follows: a **partition**, neither pinned to a value nor bounded to one. The walk
seeks to the first entry sharing the prefix, cuts that entry at the end of the
partition component — the bytes every entry of one group shares — does the
group's work inside `[prefix, prefix⁺)`, and then seeks to `prefix⁺`: the next
distinct value.

Cost becomes O(groups × work) instead of O(rows). For `distinct` the per-group
work is nothing at all, so naming a thousand distinct assignees costs a thousand
seeks and no row reads. For a count it is one range count per group, read off
index ENTRIES with no row decode — which is the same walk the row planner's
`total` already pays for, minus the rows.

The index that serves it is `[eq-filter fields…, groupPath, …]`: the same index a
board ordering by `[status, rank]` already wants declared. Because the partition
component stores its values in order, the groups come out in order too, so a
limited read stops after `limit + 1` groups rather than bucketing the entity.

Two things the walk has to get right, and both are visible in the plan:

- **A descending component.** Its payload is inverted, so reading the range
  forward yields groups in DESCENDING value order. The walk reverses direction
  instead of declining, which is what lets `[status, rank desc]` serve a
  grouping by `rank`.
- **Whether one entry is one row.** It is, unless a component PAST the partition
  fans out over a list — then a single row sits at several positions of the same
  group, and counting positions would count it twice. That case reads the rows.

Declining is always correct, exactly as it is for a row query: with no such index
the caller buckets a scan, per the standing rule that an index is an optimization
and the scan is the definition.

## The head of a group

Rows per group ride the same walk. Inside one group's sub-range the entries are
already in the index's own order, so a walk that stops after `perGroup + 1` rows
has the column's page and knows whether there are more — `groups × perGroup` rows
read, whatever the groups hold.

That only works when the index's order IS the requested one, so `PlanGroupRows`
requires two things the counted read does not:

- **The components past the partition supply `orderBy`**, direction included. An
  index storing `[status, rank desc]` serves "highest rank first" and declines
  "lowest rank first" — walking a group backward would have to start at the far
  end of a range whose end is the next group's start. With no `orderBy` at all the
  rows come back in row-key order, which every entry ends with, so the partition
  has to be the LAST component for the walk to be that order.
- **Each entry of a group is a distinct row.** A component past the partition that
  fans out puts one row at several positions of one group, and the page would
  carry duplicates ordered by a value nobody asked about.

Both declines land on the definition — bucket the match set, sort each bucket, cut
it — which reads every matching row. That is the difference declaring
`[status, rank]` makes, and it is why the read is worth a declared index in a way
a one-off count is not.

`total` per group is the one part that is not bounded by `perGroup`: counting a
group means visiting all of it. It is opt-out (`total: false`) for exactly that
reason, and a column that shows a badge is paying for the badge, not for the
preview.

## What a count off entries gives up

Reading a count off index entries means not looking at the rows, and two things
follow.

A **read filter** or field history's per-source rule can hide a live row from a
caller, and no entry can say so. The server reads rows in that case rather than
counting entries — the walk still beats the scan by every row outside the range,
but the per-row read is back.

A row whose stored bytes **will not decode** is counted, where a scan quarantines
it and drops it (see `engine/server/poison.go`). That is the same trade the
zero-decode full-sync path already makes: decoding every row to catch a corrupt
one would tax every healthy workspace for a fault none of them have. A group that
no row can NAME is different and declines the index outright, because that is the
case where the two answers would differ in shape and not only in a number.

## The client's overlay

Every local read owes the optimistic overlay: pending writes are authoritative
for the rows they touch. For a row query the merge is a union — walk the index,
drop the shadowed rows, add the pending ones back.

A grouped read cannot merge that cheaply. A shadowed row's confirmed positions
are still in the store's index, so honouring the overlay would mean subtracting
each one from whichever group it falls in and adding the pending rows' own groups
back — and for `groupRows`, splicing them into the right place in each group's
page. So the client's index walk **declines outright while anything is in the
overlay** and buckets the merged view instead. In the steady state — nothing
pending, which is where a board spends its time — the skip-scan runs; with a
write in flight the read costs a scan of what the client holds, which is bounded
by the synced subset rather than by the entity.

## Related

- [Query data](../how-to/query-data.md) — the recipes, including counted surfaces.
- [TypeScript client reference](../reference/typescript-client.md) — the API and
  its exact contract.
- [Secondary indexes](../reference/proto-options.md#secondary-indexes) — how to
  declare the index a grouped read wants.
